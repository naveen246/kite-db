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

func (tx *Transaction) Commit() {
	tx.recoveryMgr.commit()
	tx.ReleaseLocks()
	tx.buffers.unpinAll()
}

func (tx *Transaction) Rollback() error {
	err := tx.recoveryMgr.rollback()
	if err != nil {
		return err
	}
	tx.ReleaseLocks()
	tx.buffers.unpinAll()
	return nil
}

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

func (tx *Transaction) Pin(block file.Block) {
	tx.buffers.pin(block)
}

func (tx *Transaction) Unpin(block file.Block) {
	tx.buffers.unpin(block)
}

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

func (tx *Transaction) Size(filename string) (int, error) {
	eofBlock := file.GetBlock(filename, EndOfFile)
	err := tx.concurMgr.sLock(eofBlock, tx.TxNum)
	if err != nil {
		return 0, err
	}

	return int(tx.fileMgr.BlockCount(filename)), nil
}

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
