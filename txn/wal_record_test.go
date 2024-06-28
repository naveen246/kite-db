package txn

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	blockTestSize = 400
	logFile       = "simpledb.log"
	dbDir         = "logRecordTest"
	filename      = "testFile"
)

type DB struct {
	FileMgr file.FileMgr
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

func createFile(fileMgr file.FileMgr, filename string) {
	f, _ := os.Create(fileMgr.DbFilePath(filename))
	f.Truncate(1e5)
}

func removeFile(filename string, dbDir string) {
	os.Remove(filename)
	os.Remove(dbDir)
}

func TestCheckpointRecord(t *testing.T) {
	db := NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	lsn := writeCheckPointToLog(log)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, file.IntSize)
	expected[7] = CheckPoint
	assert.Equal(t, expected, log.Iterator().Next())
}

func TestStartRecord(t *testing.T) {
	db := NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	var txNum TxID = 2
	lsn := writeStartRecToLog(log, txNum)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, 2*file.IntSize)
	expected[7] = Start
	expected[15] = byte(txNum)
	assert.Equal(t, expected, log.Iterator().Next())
}

func TestCommitRecord(t *testing.T) {
	db := NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	var txNum TxID = 2
	lsn := writeCommitRecToLog(log, txNum)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, 2*file.IntSize)
	expected[7] = Commit
	expected[15] = byte(txNum)
	assert.Equal(t, expected, log.Iterator().Next())
}

func TestRollbackRecord(t *testing.T) {
	db := NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	log := db.Log
	var txNum TxID = 2
	lsn := writeRollbackRecToLog(log, txNum)
	assert.Equal(t, 1, lsn)

	assert.True(t, log.Iterator().HasNext())
	expected := make([]byte, 2*file.IntSize)
	expected[7] = Rollback
	expected[15] = byte(txNum)
	assert.Equal(t, expected, log.Iterator().Next())
}
