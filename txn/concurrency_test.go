package txn_test

import (
	"fmt"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/server"
	"sync"
	"testing"
	"time"
)

func TestConcurrency(t *testing.T) {
	db := server.NewDB(dbDir, blockTestSize, 8)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(db.Log.LogFile), dbDir)

	block1 := file.GetBlock(filename, 1)
	block2 := file.GetBlock(filename, 2)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		txA := db.NewTx()
		txA.Pin(block1)
		txA.Pin(block2)
		fmt.Println("Tx A: request sLock on block 1")
		txA.GetInt(block1, 0)
		fmt.Println("Tx A: receive sLock on block 1")
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Tx A: request sLock on block 2")
		txA.GetInt(block2, 0)
		fmt.Println("Tx A: receive sLock on block 2")
		txA.Commit()
		fmt.Println("Tx A: commit")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		txB := db.NewTx()
		txB.Pin(block1)
		txB.Pin(block2)
		fmt.Println("Tx B: request xLock on block 2")
		txB.SetInt(block2, 0, 0, false)
		fmt.Println("Tx B: receive xLock on block 2")
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Tx B: request sLock on block 1")
		txB.GetInt(block1, 0)
		fmt.Println("Tx B: receive sLock on block 1")
		txB.Commit()
		fmt.Println("Tx B: commit")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		txC := db.NewTx()
		txC.Pin(block1)
		txC.Pin(block2)
		time.Sleep(200 * time.Millisecond)
		fmt.Println("Tx C: request xLock on block 1")
		txC.SetInt(block1, 0, 0, false)
		fmt.Println("Tx C: receive xLock on block 1")
		time.Sleep(500 * time.Millisecond)
		fmt.Println("Tx C: request sLock on block 2")
		txC.GetInt(block2, 0)
		fmt.Println("Tx C: receive sLock on block 2")
		txC.Commit()
		fmt.Println("Tx C: commit")
	}()

	wg.Wait()
}
