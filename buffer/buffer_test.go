package buffer

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	blockTestSize = 400
	logFile       = "simpledb.log"
	dbDir         = "bufferTest"
	filename      = "testFile"
)

type DB struct {
	FileMgr file.FileMgr
	Log     *wal.Log
	BufPool *BufferPool
}

func NewDB(dbDir string, blockSize int64, bufferCount int) *DB {
	fileMgr := file.NewFileMgr(dbDir, blockSize)
	log := wal.NewLog(fileMgr, logFile)
	bufPool := NewBufferPool(fileMgr, log, bufferCount)
	return &DB{
		FileMgr: fileMgr,
		Log:     log,
		BufPool: bufPool,
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

func TestReuseAllocatedBuffer(t *testing.T) {
	bufferCount := 8
	db := NewDB(dbDir, blockTestSize, bufferCount)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	block := file.GetBlock(filename, 2)
	pos1 := int64(88)

	bufPool := db.BufPool
	bufPool.printStatus()
	assert.Equal(t, bufferCount, bufPool.Available())
	assert.Equal(t, 0, len(bufPool.allocatedBuffers))

	// Pin a buffer to the block, change some content in memory.
	// Notify the buffer that the buffer page is modified and then unpin the buffer.
	buf1 := bufPool.PinBuffer(block)
	bufPool.printStatus()
	assert.Equal(t, bufferCount-1, bufPool.Available())
	assert.Equal(t, 1, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block, true, 1, -1)

	page1 := buf1.Contents
	text := "abcdefghijklm"
	page1.SetString(pos1, text)

	size := file.MaxLen(len(text))
	pos2 := pos1 + size
	page1.SetInt(pos2, -345)
	buf1.SetModified(1, 0)

	bufPool.UnpinBuffer(buf1)
	bufPool.printStatus()
	assert.Equal(t, bufferCount, bufPool.Available())
	assert.Equal(t, 1, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block, false, 0, 1)
	assert.False(t, bufPool.unpinnedBuffers[bufferCount-1].isPinned())
	assert.Equal(t, int64(2), bufPool.unpinnedBuffers[bufferCount-1].Block.Number)

	// If we now try to pin a buffer to the same block,
	// then the buffer that was previously allocated to the same block is selected again.
	buf2 := bufPool.PinBuffer(block)
	bufPool.printStatus()
	assert.Equal(t, bufferCount-1, bufPool.Available())
	assert.Equal(t, 1, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block, true, 1, 1)

	// Verify that the changes done during the first pinning are still visible after second pinning
	// since the same buffer is reused
	page2 := buf2.Contents
	pos1Value, _ := page2.GetString(pos1)
	pos2Value, _ := page2.GetInt(pos2)
	assert.Equal(t, text, pos1Value)
	assert.Equal(t, int64(-345), pos2Value)

	bufPool.UnpinBuffer(buf2)
	bufPool.printStatus()
	verifyAllocatedBuffer(t, bufPool, block, false, 0, 1)
}

func TestBufferPinningAndUnpinning(t *testing.T) {
	bufferCount := 3
	db := NewDB(dbDir, blockTestSize, bufferCount)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	bufPool := db.BufPool
	bufPool.printStatus()
	assert.Equal(t, bufferCount, bufPool.Available())
	assert.Equal(t, 0, len(bufPool.allocatedBuffers))

	block1 := file.GetBlock(filename, 1)
	buf1 := bufPool.PinBuffer(block1)
	bufPool.printStatus()
	assert.Equal(t, bufferCount-1, bufPool.Available())
	assert.Equal(t, 1, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)

	page := buf1.Contents
	n, _ := page.GetInt(80)
	page.SetInt(80, n+1)
	buf1.SetModified(1, 0)

	bufPool.UnpinBuffer(buf1)
	bufPool.printStatus()
	assert.Equal(t, bufferCount, bufPool.Available())
	assert.Equal(t, 1, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block1, false, 0, 1)

	block2 := file.GetBlock(filename, 2)
	buf2 := bufPool.PinBuffer(block2)
	bufPool.printStatus()
	assert.Equal(t, bufferCount-1, bufPool.Available())
	assert.Equal(t, 2, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block1, false, 0, 1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)

	block3 := file.GetBlock(filename, 3)
	bufPool.PinBuffer(block3)
	bufPool.printStatus()
	assert.Equal(t, bufferCount-2, bufPool.Available())
	assert.Equal(t, 3, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block1, false, 0, 1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block3, true, 1, -1)

	block4 := file.GetBlock(filename, 4)
	bufPool.PinBuffer(block4)
	bufPool.printStatus()
	assert.Equal(t, 0, bufPool.Available())
	assert.Equal(t, 3, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block3, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block4, true, 1, -1)

	bufPool.UnpinBuffer(buf2)
	bufPool.printStatus()
	assert.Equal(t, 1, bufPool.Available())
	assert.Equal(t, 3, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block2, false, 0, -1)
	verifyAllocatedBuffer(t, bufPool, block3, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block4, true, 1, -1)

	buf := bufPool.PinBuffer(block1)
	page2 := buf.Contents
	page2.SetInt(80, 9999)
	buf.SetModified(1, 0)
	bufPool.printStatus()
	assert.Equal(t, 0, bufPool.Available())
	assert.Equal(t, 3, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, 1)
	verifyAllocatedBuffer(t, bufPool, block3, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block4, true, 1, -1)
}

func verifyAllocatedBuffer(t *testing.T, bufPool *BufferPool, block file.Block, isPinned bool, pinCount int, txNum int) {
	buf := bufPool.allocatedBuffers[block.String()]
	assert.Equal(t, isPinned, buf.isPinned())
	assert.Equal(t, pinCount, buf.pins)
	assert.Equal(t, txNum, buf.txNum)
}

func TestFailedPinWhenBufferNotFree(t *testing.T) {
	bufferCount := 3
	db := NewDB(dbDir, blockTestSize, bufferCount)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	bufPool := db.BufPool
	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)
	block2 := file.GetBlock(filename, 2)
	block3 := file.GetBlock(filename, 3)
	bufPool.printStatus()
	assert.Equal(t, bufferCount, bufPool.Available())
	assert.Equal(t, 0, len(bufPool.allocatedBuffers))

	bufPool.PinBuffer(block0)
	buf1 := bufPool.PinBuffer(block1)
	buf2 := bufPool.PinBuffer(block2)
	bufPool.printStatus()
	assert.Equal(t, 0, bufPool.Available())
	assert.Equal(t, bufferCount, len(bufPool.allocatedBuffers))
	verifyAllocatedBuffer(t, bufPool, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)

	bufPool.UnpinBuffer(buf1)
	bufPool.PinBuffer(block0)
	bufPool.PinBuffer(block1)
	bufPool.printStatus()
	verifyAllocatedBuffer(t, bufPool, block0, true, 2, -1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)

	// This PinBuffer should fail and should return nil since all buffers are occupied
	assert.Nil(t, bufPool.PinBuffer(block3, true))

	// If we Unpin a buffer and try again, it should succeed
	bufPool.UnpinBuffer(buf2)
	assert.NotNil(t, bufPool.PinBuffer(block3))
	bufPool.printStatus()
	verifyAllocatedBuffer(t, bufPool, block0, true, 2, -1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block3, true, 1, -1)
}

func TestFlushAll(t *testing.T) {
	bufferCount := 3
	db := NewDB(dbDir, blockTestSize, bufferCount)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	bufPool := db.BufPool
	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)
	block2 := file.GetBlock(filename, 2)
	buf0 := bufPool.PinBuffer(block0)
	buf1 := bufPool.PinBuffer(block1)
	buf2 := bufPool.PinBuffer(block2)
	verifyAllocatedBuffer(t, bufPool, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)

	buf0.SetModified(1, 0)
	buf1.SetModified(1, 1)
	buf2.SetModified(2, 0)
	verifyAllocatedBuffer(t, bufPool, block0, true, 1, 1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, 1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, 2)

	bufPool.FlushAll(1)
	// Only buffers with txNum = 1 should be flushed to disk and txNum changed to -1
	verifyAllocatedBuffer(t, bufPool, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, 2)

	buf0.SetModified(3, 0)
	buf1.SetModified(3, 1)
	bufPool.FlushAll(2)
	// Only buffers with txNum = 2 should be flushed to disk and txNum changed to -1
	verifyAllocatedBuffer(t, bufPool, block0, true, 1, 3)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, 3)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)

	buf2.SetModified(3, 2)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, 3)

	bufPool.FlushAll(3)
	// All buffers have txNum = 3 so all buffers should be flushed to disk and txNum changed to -1
	verifyAllocatedBuffer(t, bufPool, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufPool, block2, true, 1, -1)

}
