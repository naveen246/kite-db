package txn

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
	"github.com/sasha-s/go-deadlock"
	"log"
	"time"
)

const EndOfFile = -1

type TxID int64

var nextTxNum struct {
	deadlock.Mutex
	txID TxID
}

func nextTxNumber() TxID {
	nextTxNum.Lock()
	defer nextTxNum.Unlock()
	nextTxNum.txID = TxID(time.Now().UnixNano())
	return nextTxNum.txID
}

// Transaction Provide transaction management for clients,
// ensuring that all transactions are serializable, recoverable,
// and in general satisfy the ACID properties.
type Transaction struct {
	TxNum       TxID
	bufferPool  *buffer.BufferPool
	fileMgr     *file.FileMgr
	concurMgr   *concurrencyMgr
	recoveryMgr *RecoveryMgr
	buffers     *BufferList
}

func NewTransaction(fileMgr *file.FileMgr, log *wal.Log, bufferPool *buffer.BufferPool) *Transaction {
	tx := &Transaction{}
	tx.bufferPool = bufferPool
	tx.fileMgr = fileMgr
	tx.TxNum = nextTxNumber()
	tx.concurMgr = newConcurrencyMgr()
	tx.recoveryMgr = NewRecoveryMgr(tx, tx.TxNum, log, bufferPool)
	tx.buffers = NewBufferList(bufferPool)
	return tx
}

// Commit the current transaction.
// Flush all modified buffers (and their log records),
// write and flush a Commit record to the log,
// release all locks, and unpin any pinned buffers.
func (tx *Transaction) Commit() {
	tx.recoveryMgr.commit()
	tx.ReleaseLocks()
	tx.buffers.unpinAll()
}

// Rollback the current transaction.
// Undo any modified values, flush those buffers,
// write and flush a Rollback record to the log,
// release all locks, and unpin any pinned buffers.
func (tx *Transaction) Rollback() error {
	err := tx.recoveryMgr.rollback()
	if err != nil {
		return err
	}
	tx.ReleaseLocks()
	tx.buffers.unpinAll()
	return nil
}

// Recover Flush all modified buffers.
// Then go through the log, rolling back all uncommitted transactions.
// Finally, write a checkpoint record to the log.
func (tx *Transaction) Recover() error {
	tx.bufferPool.FlushAll(int64(tx.TxNum))
	err := tx.recoveryMgr.recover()
	if err != nil {
		return err
	}
	return nil
}

func (tx *Transaction) ReleaseLocks() {
	tx.concurMgr.releaseLocks(tx.TxNum)
}

// Pin the specified block. The transaction manages the buffer for the client.
func (tx *Transaction) Pin(block file.Block) {
	tx.buffers.pin(block)
}

// Unpin the specified block.
// The transaction looks up the buffer pinned to this block, and unpins it.
func (tx *Transaction) Unpin(block file.Block) {
	tx.buffers.unpin(block)
}

// GetInt Return the integer value stored at the specified offset of the specified block.
// The method first obtains an sLock on the block, then it calls the buffer to retrieve the value.
func (tx *Transaction) GetInt(block file.Block, offset int) (int, error) {
	err := tx.concurMgr.sLock(block, tx.TxNum)
	if err != nil {
		return 0, err
	}

	buf := tx.buffers.getBuffer(block)
	val, err := buf.Contents.GetInt(int64(offset))
	if err != nil {
		log.Fatalln("Transaction GetInt err:", err)
	}

	return int(val), nil
}

// GetString Return the string value stored at the specified offset of the specified block.
// The method first obtains an sLock on the block, then it calls the buffer to retrieve the value.
func (tx *Transaction) GetString(block file.Block, offset int) (string, error) {
	err := tx.concurMgr.sLock(block, tx.TxNum)
	if err != nil {
		return "", err
	}

	buf := tx.buffers.getBuffer(block)
	val, err := buf.Contents.GetString(int64(offset))
	if err != nil {
		log.Fatalln("Transaction GetString err:", err)
	}

	return val, nil
}

// SetInt Store an integer at the specified offset of the specified block.
// The method first obtains an xLock on the block.
// It then reads the current value at that offset,
// puts it into an update log record, and writes that record to the log.
// Finally, it calls the buffer to store the value, passing in the LSN of the log record and the transaction's id.
func (tx *Transaction) SetInt(block file.Block, offset int64, val int, okToLog bool) error {
	err := tx.concurMgr.xLock(block, tx.TxNum)
	if err != nil {
		return err
	}

	buf := tx.buffers.getBuffer(block)
	var lsn int64 = -1
	if okToLog {
		lsn = tx.recoveryMgr.setInt(buf, offset)
	}

	err = buf.Contents.SetInt(offset, int64(val))
	if err != nil {
		log.Fatalln("Transaction SetInt err:", err)
	}

	buf.SetModified(int64(tx.TxNum), lsn)
	return nil
}

// SetString Store a string at the specified offset of the specified block.
// The method first obtains an xLock on the block.
// It then reads the current value at that offset,
// puts it into an update log record, and writes that record to the log.
// Finally, it calls the buffer to store the value, passing in the LSN of the log record and the transaction's id.
func (tx *Transaction) SetString(block file.Block, offset int64, val string, okToLog bool) error {
	err := tx.concurMgr.xLock(block, tx.TxNum)
	if err != nil {
		return err
	}

	buf := tx.buffers.getBuffer(block)
	var lsn int64 = -1
	if okToLog {
		lsn = tx.recoveryMgr.setString(buf, offset)
	}

	err = buf.Contents.SetString(int64(offset), val)
	if err != nil {
		log.Fatalln("Transaction SetString err:", err)
	}

	buf.SetModified(int64(tx.TxNum), lsn)
	return nil
}

func (tx *Transaction) AvailableBuffers() int {
	return tx.bufferPool.Available()
}

// Size Return the number of blocks in the specified file.
// This method first obtains an sLock on the "end of the file" (eofBlock),
// before asking the file manager to return the BlockCount.
func (tx *Transaction) Size(filename string) (int, error) {
	eofBlock := file.GetBlock(filename, EndOfFile)
	err := tx.concurMgr.sLock(eofBlock, tx.TxNum)
	if err != nil {
		return 0, err
	}

	return int(tx.fileMgr.BlockCount(filename)), nil
}

// Append a new block to the end of the specified file and returns a reference to it.
// This method first obtains an xLock on the "end of the file" (eofBlock), before performing the append.
func (tx *Transaction) Append(filename string) (file.Block, error) {
	eofBlock := file.GetBlock(filename, EndOfFile)
	err := tx.concurMgr.xLock(eofBlock, tx.TxNum)
	if err != nil {
		return file.Block{}, err
	}

	block, err := tx.fileMgr.Append(filename)
	if err != nil {
		log.Fatalln("Transaction Append err:", err)
	}
	return block, nil
}

func (tx *Transaction) BlockSize() int {
	return int(tx.fileMgr.BlockSize)
}

// ********************** BufferList **********************************

// BufferList Manage the transaction's currently-pinned buffers.
type BufferList struct {
	buffers  map[file.Block]*buffer.Buffer
	pinCount map[file.Block]int
	bufPool  *buffer.BufferPool
}

func NewBufferList(pool *buffer.BufferPool) *BufferList {
	return &BufferList{
		bufPool:  pool,
		buffers:  make(map[file.Block]*buffer.Buffer),
		pinCount: make(map[file.Block]int),
	}
}

// getBuffer Return the buffer pinned to the specified block.
func (b *BufferList) getBuffer(block file.Block) *buffer.Buffer {
	return b.buffers[block]
}

// pin the block and keep track of the buffer internally.
func (b *BufferList) pin(block file.Block) {
	buf := b.bufPool.PinBuffer(block)
	b.buffers[block] = buf
	b.pinCount[block] = b.pinCount[block] + 1
}

// unpin the specified block.
func (b *BufferList) unpin(block file.Block) {
	buf := b.buffers[block]
	b.bufPool.UnpinBuffer(buf)

	b.pinCount[block] = b.pinCount[block] - 1
	if b.pinCount[block] <= 0 {
		delete(b.buffers, block)
		delete(b.pinCount, block)
	}
}

// unpinAll Unpin all buffers still pinned by this transaction.
// if a block has been pinned multiple times (pinCount) by a txn
// then it should be unpinned the same number of times
func (b *BufferList) unpinAll() {
	for block, pinCount := range b.pinCount {
		buf := b.buffers[block]
		for i := 0; i < pinCount; i++ {
			b.bufPool.UnpinBuffer(buf)
		}
	}
	clear(b.buffers)
	clear(b.pinCount)
}
