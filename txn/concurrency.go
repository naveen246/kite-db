package txn

import (
	"errors"
	"github.com/naveen246/kite-db/file"
	"github.com/sasha-s/go-deadlock"
	"slices"
	"sync"
)

var ErrLockAbort = errors.New("could not get a lock to read/write data")

type txLock struct {
	txId   TxID
	lkType lockType
}

var lockTbl *lockTable
var once *sync.Once

// The lock table, which provides methods to lock and unlock blocks.
// All txns share the same common lockTable
type lockTable struct {
	mu    deadlock.Mutex
	locks map[file.Block][]txLock
}

// lockTable is initialized only once per DB instance
func getLockTable() *lockTable {
	once.Do(func() {
		lockTbl = &lockTable{
			locks: make(map[file.Block][]txLock),
		}
	})
	return lockTbl
}

func ResetLockTable() {
	once = new(sync.Once)
}

// sLock - Grants sharedLock on the specified block
// multiple txns can hold a sharedLock on a block
// To avoid deadlock we use wait-die method as follows
//
// Suppose T1 requests sLock and another txn T2 holds xLock on this block.
// If T1 is older than T2 then: T1 waits for the lock (repeat for loop).
// Else: return error ErrLockAbort
func (l *lockTable) sLock(block file.Block, txNum TxID) error {
	for {
		otherTxHasXLock := false
		hasOlderTx := false
		l.mu.Lock()
		for _, txLck := range l.locks[block] {
			if txLck.txId == txNum {
				continue
			}
			if txLck.lkType == exclusiveLock {
				otherTxHasXLock = true
				if txLck.txId < txNum {
					hasOlderTx = true
				}
			}
		}

		if !otherTxHasXLock {
			l.locks[block] = append(l.locks[block], txLock{
				txId:   txNum,
				lkType: sharedLock,
			})
			l.mu.Unlock()
			return nil
		} else if hasOlderTx {
			l.mu.Unlock()
			return ErrLockAbort
		}
		l.mu.Unlock()
	}
}

// xLock - Grants exclusiveLock on the specified block
// only 1 txn can hold an exclusiveLock at a given time
// To avoid deadlock we use wait-die method as follows
//
// Suppose T1 requests xLock and another txn holds any lock on this block.
// If T1 is older than all txns holding any lock then: T1 waits for the lock (repeat for loop).
// Else: return error ErrLockAbort
func (l *lockTable) xLock(block file.Block, txNum TxID) error {
	for {
		otherTxHasAnyLock := false
		hasOlderTx := false
		l.mu.Lock()
		for _, txLck := range l.locks[block] {
			if txLck.txId == txNum {
				continue
			}
			otherTxHasAnyLock = true
			if txLck.txId < txNum {
				hasOlderTx = true
			}
		}

		if !otherTxHasAnyLock {
			l.locks[block] = append(l.locks[block], txLock{
				txId:   txNum,
				lkType: exclusiveLock,
			})
			l.mu.Unlock()
			return nil
		} else if hasOlderTx {
			l.mu.Unlock()
			return ErrLockAbort
		}
		l.mu.Unlock()
	}
}

// unlock - Release a lock on the specified block.
// This is generally called when txn.Commit or txn.Rollback is run
func (l *lockTable) unlock(block file.Block, txNum TxID) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.locks[block] = slices.DeleteFunc(l.locks[block], func(txLk txLock) bool {
		return txLk.txId == txNum
	})
	if len(l.locks[block]) == 0 {
		delete(l.locks, block)
	}
}

type lockType int

const (
	sharedLock lockType = iota
	exclusiveLock
)

// concurrencyMgr - Each txn has its own concurrency manager
// The concurrency manager keeps track of which locks the txn currently has,
// and interacts with the global lock table as needed.
type concurrencyMgr struct {
	// the common lockTable shared by all txns
	lockTbl *lockTable
	// locks keeps track of the locks held by the txn for each block
	locks map[file.Block]lockType
}

func newConcurrencyMgr() *concurrencyMgr {
	return &concurrencyMgr{
		lockTbl: getLockTable(),
		locks:   make(map[file.Block]lockType),
	}
}

// sLock Obtain a sharedLock on the block, if necessary.
// The method will ask the lockTable for an sLock if the txn currently has no locks on that block.
func (c *concurrencyMgr) sLock(block file.Block, txNum TxID) error {
	_, ok := c.locks[block]
	if !ok {
		err := c.lockTbl.sLock(block, txNum)
		if err != nil {
			return err
		}

		c.locks[block] = sharedLock
	}
	return nil
}

// Obtain an exclusiveLock on the block, if necessary.
// If the transaction does not have an xLock on that block,
// then the method first gets an sLock on that block (if necessary),
// and then upgrades it to an xLock.
func (c *concurrencyMgr) xLock(block file.Block, txNum TxID) error {
	l, ok := c.locks[block]
	if !ok || l != exclusiveLock {
		err := c.sLock(block, txNum)
		if err != nil {
			return err
		}

		err = c.lockTbl.xLock(block, txNum)
		if err != nil {
			return err
		}

		c.locks[block] = exclusiveLock
	}
	return nil
}

// releaseLocks Release all locks by asking the lock table to unlock each one.
func (c *concurrencyMgr) releaseLocks(txNum TxID) {
	for blk := range c.locks {
		c.lockTbl.unlock(blk, txNum)
	}
	clear(c.locks)
}
