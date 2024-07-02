package txn_test

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/server"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	blockTestSize int64 = 400
	dbDir               = "txTest"
	filename            = "testFile"
)

func createFile(fileMgr *file.FileMgr, filename string) {
	f, _ := os.Create(fileMgr.DbFilePath(filename))
	f.Truncate(1e5)
}

func removeFile(filename string, dbDir string) {
	os.Remove(filename)
	os.Remove(dbDir)
}

func TestTxn(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(db.Log.LogFile), dbDir)

	blk := file.GetBlock(filename, 1)

	// tx1: set [intVal = 1, stringVal = "one"] in the block
	tx1 := db.NewTx()
	tx1.Pin(blk)
	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit()

	// tx2: verify that [intVal = 1, stringVal = "one"] as set by tx1
	// and change them to new values [intVal = 2, stringVal = "one!"]
	tx2 := db.NewTx()
	tx2.Pin(blk)
	iVal, _ := tx2.GetInt(blk, 80)
	sVal, _ := tx2.GetString(blk, 40)
	assert.Equal(t, 1, iVal)
	assert.Equal(t, "one", sVal)
	newIVal := iVal + 1
	newSVal := sVal + "!"
	tx2.SetInt(blk, 80, newIVal, true)
	tx2.SetString(blk, 40, newSVal, true)
	tx2.Commit()

	// tx3: verify that we can see the changes [intVal = 2, stringVal = "one!"] made by tx2
	// change int value to [intVal = 9999], verify the change and then rollback
	tx3 := db.NewTx()
	tx3.Pin(blk)
	iVal, _ = tx3.GetInt(blk, 80)
	sVal, _ = tx3.GetString(blk, 40)
	assert.Equal(t, 2, iVal)
	assert.Equal(t, "one!", sVal)
	tx3.SetInt(blk, 80, 9999, true)
	iVal, _ = tx3.GetInt(blk, 80)
	assert.Equal(t, 9999, iVal)
	tx3.Rollback()

	// tx4: verify that tx3 changes were rolled back and we can see the old int value [intVal = 2]
	tx4 := db.NewTx()
	tx4.Pin(blk)
	iVal, _ = tx4.GetInt(blk, 80)
	assert.Equal(t, 2, iVal)
	tx4.Commit()
}
