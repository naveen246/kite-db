package txn_test

import (
	"errors"
	"fmt"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/server"
	"github.com/naveen246/kite-db/txn"
	"github.com/stretchr/testify/assert"
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

	txA := db.NewTx()
	txB := db.NewTx()
	txC := db.NewTx()
	ch := make(chan string, 3)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(txA *txn.Transaction) {
		defer wg.Done()
		txA.Pin(block1)
		txA.Pin(block2)

		fmt.Println("Tx A: request sLock on block1")
		_, err := txA.GetInt(block1, 0)
		assert.NotErrorIs(t, err, txn.ErrLockAbort)
		fmt.Println("Tx A: receive sLock on block1")

		time.Sleep(1000 * time.Millisecond)

		fmt.Println("Tx A: request sLock on block2")
		_, err = txA.GetInt(block2, 0)
		assert.NotErrorIs(t, err, txn.ErrLockAbort)
		fmt.Println("Tx A: receive sLock on block2")

		ch <- "txA commit"
		txA.Commit()
		fmt.Println("Tx A: commit")
	}(txA)

	wg.Add(1)
	go func(txB *txn.Transaction) {
		defer wg.Done()
		txB.Pin(block1)
		txB.Pin(block2)

		fmt.Println("Tx B: request xLock on block2")
		err := txB.SetInt(block2, 0, 0, false)
		assert.NotErrorIs(t, err, txn.ErrLockAbort)
		fmt.Println("Tx B: receive xLock on block2")

		time.Sleep(1000 * time.Millisecond)

		fmt.Println("Tx B: request sLock on block1")
		_, err = txB.GetInt(block1, 0)
		assert.NotErrorIs(t, err, txn.ErrLockAbort)
		fmt.Println("Tx B: receive sLock on block1")

		ch <- "txB commit"
		txB.Commit()
		fmt.Println("Tx B: commit")
	}(txB)

	wg.Add(1)

	// txC is the youngest transaction so it will be rolled back when it does not get a lock
	// after rollback the txn is tried again after some delay
	go func(txC *txn.Transaction) {
		defer wg.Done()
		// for loop to retry txn when txn is rolled back
		for {
			time.Sleep(500 * time.Millisecond)
			txC.Pin(block1)
			txC.Pin(block2)

			fmt.Println("Tx C: request xLock on block1")
			err := txC.SetInt(block1, 0, 0, false)
			if err != nil {
				assert.ErrorIs(t, err, txn.ErrLockAbort)
				fmt.Println("txC failed xLock on block1, rollback txC")
				txC.Rollback()
				continue
			}
			fmt.Println("Tx C: receive xLock on block1")

			time.Sleep(1000 * time.Millisecond)

			fmt.Println("Tx C: request sLock on block2")
			_, err = txC.GetInt(block2, 0)
			if err != nil && errors.Is(err, txn.ErrLockAbort) {
				fmt.Println("txC failed sLock on block2, rollback txC")
				txC.Rollback()
				continue
			}
			fmt.Println("Tx C: receive sLock on block2")
			break
		}
		ch <- "txC commit"
		txC.Commit()
		fmt.Println("Tx C: commit")
	}(txC)

	wg.Wait()
	// verify the order of txn commits
	assert.Equal(t, "txB commit", <-ch)
	assert.Equal(t, "txA commit", <-ch)
	assert.Equal(t, "txC commit", <-ch)
}
