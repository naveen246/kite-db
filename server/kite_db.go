package server

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/txn"
	"github.com/naveen246/kite-db/wal"
)

var (
	logFile = "simpledb.log"
)

type DB struct {
	FileMgr *file.FileMgr
	Log     *wal.Log
	BufPool *buffer.BufferPool
}

func NewDB(dbDir string, blockSize int64, bufferCount int) *DB {
	fileMgr := file.NewFileMgr(dbDir, blockSize)
	log := wal.NewLog(fileMgr, logFile)
	bufferPool := buffer.NewBufferPool(fileMgr, log, bufferCount)
	return &DB{
		FileMgr: fileMgr,
		Log:     log,
		BufPool: bufferPool,
	}
}

func (db *DB) NewTx() *txn.Transaction {
	return txn.NewTransaction(db.FileMgr, db.Log, db.BufPool)
}
