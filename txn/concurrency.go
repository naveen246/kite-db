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
var once sync.Once

type lockTable struct {
	mu    deadlock.RWMutex
	locks map[file.Block][]txLock
}

func getLockTable() *lockTable {
	once.Do(func() {
		lockTbl = &lockTable{
			locks: make(map[file.Block][]txLock),
		}
	})
	return lockTbl
}

func (l *lockTable) sLock(block file.Block, txNum TxID) error {
	for {
		otherTxHasXLock := false
		hasOlderTx := false
		l.mu.RLock()
		txLocks := make([]txLock, len(l.locks[block]))
		copy(txLocks, l.locks[block])
		l.mu.RUnlock()
		for _, txLock := range txLocks {
			if txLock.lkType == exclusiveLock {
				otherTxHasXLock = true
			}
			if txLock.txId < txNum {
				hasOlderTx = true
			}
		}

		if !otherTxHasXLock {
			break
		} else if hasOlderTx {
			return ErrLockAbort
		}
	}

	l.mu.Lock()
	l.locks[block] = append(l.locks[block], txLock{
		txId:   txNum,
		lkType: sharedLock,
	})
	l.mu.Unlock()
	return nil
}

func (l *lockTable) xLock(block file.Block, txNum TxID) error {

	// Wait-die locking rule for deadlock avoidance
	for {
		otherTxHasAnyLock := false
		hasOlderTx := false
		l.mu.RLock()
		txLocks := make([]txLock, len(l.locks[block]))
		copy(txLocks, l.locks[block])
		l.mu.RUnlock()
		for _, txLock := range txLocks {
			if txLock.txId != txNum {
				otherTxHasAnyLock = true
			}
			if txLock.txId < txNum {
				hasOlderTx = true
			}
		}

		if !otherTxHasAnyLock {
			break
		}
		if hasOlderTx {
			return ErrLockAbort
		}
	}

	l.mu.Lock()
	l.locks[block] = append(l.locks[block], txLock{
		txId:   txNum,
		lkType: exclusiveLock,
	})
	l.mu.Unlock()
	return nil
}

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

type concurrencyMgr struct {
	lockTbl *lockTable
	locks   map[file.Block]lockType
}

func newConcurrencyMgr() *concurrencyMgr {
	return &concurrencyMgr{
		lockTbl: getLockTable(),
		locks:   make(map[file.Block]lockType),
	}
}

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

func (c *concurrencyMgr) releaseLocks(txNum TxID) {
	for blk := range c.locks {
		c.lockTbl.unlock(blk, txNum)
	}
	clear(c.locks)
}
