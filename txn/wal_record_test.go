package txn_test

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/server"
	"github.com/naveen246/kite-db/txn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckpointRecord(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	lsn := txn.WriteCheckPointToLog(log)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, file.IntSize)
	expected[7] = txn.CheckPoint
	assert.Equal(t, expected, log.Iterator().Next())
}

func TestStartRecord(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	var txNum txn.TxID = 2
	lsn := txn.WriteStartRecToLog(log, txNum)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, 2*file.IntSize)
	expected[7] = txn.Start
	expected[15] = byte(txNum)
	assert.Equal(t, expected, log.Iterator().Next())
}

func TestCommitRecord(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	var txNum txn.TxID = 2
	lsn := txn.WriteCommitRecToLog(log, txNum)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, 2*file.IntSize)
	expected[7] = txn.Commit
	expected[15] = byte(txNum)
	assert.Equal(t, expected, log.Iterator().Next())
}

func TestRollbackRecord(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	var txNum txn.TxID = 2
	lsn := txn.WriteRollbackRecToLog(log, txNum)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, 2*file.IntSize)
	expected[7] = txn.Rollback
	expected[15] = byte(txNum)
	assert.Equal(t, expected, log.Iterator().Next())
}
