package txn_test

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/server"
	"github.com/naveen246/kite-db/txn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRollbackAndRecovery(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	// Initialize data in block0 and block1
	initial := []int64{0, 1, 2, 3, 4, 5}
	tx1, tx2 := setData(db, initial, initial, "abc", "def")
	tx1.Commit()
	tx2.Commit()

	// Test if initial changes are present in block0 and block1
	verifyData(t, db, initial, initial, "abc", "def")

	// Modify data in block0 and block1
	newData := []int64{100, 200, 300, 400, 500, 600}
	tx3, tx4 := setData(db, newData, newData, "uvw", "xyz")
	db.BufPool.FlushAll(int64(tx3.TxNum))
	db.BufPool.FlushAll(int64(tx4.TxNum))

	// Test if modified changes are present in block0 and block1
	verifyData(t, db, newData, newData, "uvw", "xyz")

	// Rollback tx3 changes in block0
	tx3.Rollback()

	// Test if tx3 changes to block0 are rolled back and tx4 changes remain persisted
	verifyData(t, db, initial, newData, "abc", "xyz")

	// Call transaction recover.
	// initially calling recover will throw an error since tx4 is still holding locks
	tx := db.NewTx()
	err := tx.Recover()
	assert.NotNil(t, err)
	assert.Equal(t, txn.ErrLockAbort, err)

	// release locks held by tx4 and then call recover.
	// This should undo tx4 changes since tx4 is not committed or rolled back
	tx4.ReleaseLocks()
	err = tx.Recover()
	assert.Nil(t, err)

	verifyData(t, db, initial, initial, "abc", "def")
}

func setData(db *server.DB, b0Data []int64, b1Data []int64, str1 string, str2 string) (*txn.Transaction, *txn.Transaction) {
	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)
	tx1 := db.NewTx()
	tx2 := db.NewTx()
	tx1.Pin(block0)
	tx2.Pin(block1)

	var pos int64 = 0
	for i := 0; i < len(b0Data); i++ {
		tx1.SetInt(block0, pos, int(b0Data[i]), true)
		tx2.SetInt(block1, pos, int(b1Data[i]), true)
		pos += file.IntSize
	}
	tx1.SetString(block0, 60, str1, true)
	tx2.SetString(block1, 60, str2, true)
	return tx1, tx2
}

func verifyData(t *testing.T, db *server.DB, b0Data []int64, b1Data []int64, str1 string, str2 string) {
	fm := db.FileMgr
	page0 := file.NewPageWithSize(fm.BlockSize)
	page1 := file.NewPageWithSize(fm.BlockSize)

	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)
	fm.Read(block0, page0)
	fm.Read(block1, page1)
	var pos int64 = 0
	for i := 0; i < len(b0Data); i++ {
		val, _ := page0.GetInt(pos)
		assert.Equal(t, b0Data[i], val)
		val, _ = page1.GetInt(pos)
		assert.Equal(t, b1Data[i], val)
		pos += file.IntSize
	}

	val, _ := page0.GetString(60)
	assert.Equal(t, str1, val)
	val, _ = page1.GetString(60)
	assert.Equal(t, str2, val)
}
