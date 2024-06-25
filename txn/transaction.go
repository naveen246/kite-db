package txn

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
)

const EndOfFile = -1

type Transaction struct {
	nxtTxNum    int
	recoveryMgr *RecoveryMgr
	bufferPool  *buffer.BufferPool
	fileMgr     file.FileMgr
	buffers     *BufferList
}

func NewTransaction(fileMgr file.FileMgr, log *wal.Log, bufferPool *buffer.BufferPool) *Transaction {
	return &Transaction{
		bufferPool:  bufferPool,
		fileMgr:     fileMgr,
		recoveryMgr: NewRecoveryMgr(log),
		buffers:     NewBufferList(),
	}
}

func (tx *Transaction) Commit() {

}

func (tx *Transaction) Rollback() {

}

func (tx *Transaction) Recover() {

}

func (tx *Transaction) Pin(block file.Block) {

}

func (tx *Transaction) Unpin(block file.Block) {

}

func (tx *Transaction) GetInt(block file.Block, offset int) int {
	return 0
}

func (tx *Transaction) GetString(block file.Block, offset int) string {
	return ""
}

func (tx *Transaction) SetInt(block file.Block, offset int, val int, okToLog bool) {

}

func (tx *Transaction) SetString(block file.Block, offset int, val string, okToLog bool) {

}
