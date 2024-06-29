package txn_test

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/server"
	"github.com/naveen246/kite-db/txn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func initialize(t *testing.T, db *server.DB) {
	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)

	// Add data to block0 and block1
	tx1 := db.NewTx()
	tx2 := db.NewTx()
	tx1.Pin(block0)
	tx2.Pin(block1)
	var pos int64 = 0
	for i := 0; i < 6; i++ {
		tx1.SetInt(block0, pos, int(pos), false)
		tx2.SetInt(block1, pos, int(pos), false)
		pos += file.IntSize
	}
	tx1.SetString(block0, 60, "abc", false)
	tx2.SetString(block1, 60, "def", false)
	tx1.Commit()
	tx2.Commit()

	// Test if the changes are present in block0 and block1
	fm := db.FileMgr
	page0 := file.NewPageWithSize(fm.BlockSize)
	page1 := file.NewPageWithSize(fm.BlockSize)

	fm.Read(block0, page0)
	fm.Read(block1, page1)
	pos = 0
	for i := 0; i < 6; i++ {
		val, _ := page0.GetInt(pos)
		assert.Equal(t, pos, val)
		val, _ = page1.GetInt(pos)
		assert.Equal(t, pos, val)
		pos += file.IntSize
	}

	val, _ := page0.GetString(60)
	assert.Equal(t, "abc", val)
	val, _ = page1.GetString(60)
	assert.Equal(t, "def", val)
}

func TestRollbackAndRecovery(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	initialize(t, db)

	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)

	tx3 := db.NewTx()
	tx4 := db.NewTx()
	tx3.Pin(block0)
	tx4.Pin(block1)
	var pos int64 = 0
	for i := 0; i < 6; i++ {
		tx3.SetInt(block0, pos, int(pos+100), true)
		tx4.SetInt(block1, pos, int(pos+100), true)
		pos += file.IntSize
	}
	tx3.SetString(block0, 60, "uvw", true)
	tx4.SetString(block1, 60, "xyz", true)
	db.BufPool.FlushAll(int64(tx3.TxNum))
	db.BufPool.FlushAll(int64(tx4.TxNum))

	// Test if the changes are present in block0 and block1
	fm := db.FileMgr
	page0 := file.NewPageWithSize(fm.BlockSize)
	page1 := file.NewPageWithSize(fm.BlockSize)

	fm.Read(block0, page0)
	fm.Read(block1, page1)
	pos = 0
	for i := 0; i < 6; i++ {
		val, _ := page0.GetInt(pos)
		assert.Equal(t, pos+100, val)
		val, _ = page1.GetInt(pos)
		assert.Equal(t, pos+100, val)
		pos += file.IntSize
	}

	val, _ := page0.GetString(60)
	assert.Equal(t, "uvw", val)
	val, _ = page1.GetString(60)
	assert.Equal(t, "xyz", val)

	tx3.Rollback()

	// Test if tx3 changes to block0 are rolled back and tx4 changes remain persisted
	fm.Read(block0, page0)
	fm.Read(block1, page1)
	pos = 0
	for i := 0; i < 6; i++ {
		val, _ := page0.GetInt(pos)
		assert.Equal(t, pos, val)
		val, _ = page1.GetInt(pos)
		assert.Equal(t, pos+100, val)
		pos += file.IntSize
	}

	val, _ = page0.GetString(60)
	assert.Equal(t, "abc", val)
	val, _ = page1.GetString(60)
	assert.Equal(t, "xyz", val)

	// Call transaction recover.
	// initially calling recover will throw an error since tx4 is still holding locks
	tx := db.NewTx()
	err := tx.Recover()
	assert.NotNil(t, err)
	assert.Equal(t, txn.ErrLockAbort, err)

	// release locks held by tx4 and then call recover.
	// This should undo tx4 changes since tx4 is not committed or rolled back
	tx4.ConcurMgr.ReleaseLocks(tx4.TxNum)
	err = tx.Recover()
	assert.Nil(t, err)

	fm.Read(block1, page1)
	pos = 0
	for i := 0; i < 6; i++ {
		val, _ := page1.GetInt(pos)
		assert.Equal(t, pos, val)
		pos += file.IntSize
	}
	val, _ = page1.GetString(60)
	assert.Equal(t, "def", val)
}
