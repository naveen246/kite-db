package buffer

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/loghandler"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	blockTestSize = 400
	logFile       = "simpledb.log"
	dbDir         = "buffertest"
	filename      = "testfile"
)

type DB struct {
	FileMgr file.FileMgr
	LogMgr  *loghandler.LogMgr
	BufMgr  *BufferMgr
}

func NewDB(dbDir string, blockSize int64, bufferCount int) *DB {
	fileMgr := file.NewFileMgr(dbDir, blockSize)
	logMgr := loghandler.NewLogMgr(fileMgr, logFile)
	bufferMgr := NewBufferMgr(fileMgr, logMgr, bufferCount)
	return &DB{
		FileMgr: fileMgr,
		LogMgr:  logMgr,
		BufMgr:  bufferMgr,
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

	bufMgr := db.BufMgr
	bufMgr.printStatus()
	assert.Equal(t, bufferCount, bufMgr.Available())
	assert.Equal(t, 0, len(bufMgr.allocatedBuffers))

	// Pin a buffer to the block, change some content in memory.
	// Notify the buffer that the buffer page is modified and then unpin the buffer.
	buf1 := bufMgr.PinBuffer(block)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount-1, bufMgr.Available())
	assert.Equal(t, 1, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block, true, 1, -1)

	page1 := buf1.Contents
	text := "abcdefghijklm"
	page1.SetString(pos1, text)

	size := page1.MaxLen(len(text))
	pos2 := pos1 + int64(size)
	page1.SetInt(pos2, -345)
	buf1.SetModified(1, 0)

	bufMgr.UnpinBuffer(buf1)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount, bufMgr.Available())
	assert.Equal(t, 1, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block, false, 0, 1)
	assert.False(t, bufMgr.unpinnedBuffers[bufferCount-1].isPinned())
	assert.Equal(t, int64(2), bufMgr.unpinnedBuffers[bufferCount-1].Block.Number)

	// If we now try to pin a buffer to the same block,
	// then the buffer that was previously allocated to the same block is selected again.
	buf2 := bufMgr.PinBuffer(block)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount-1, bufMgr.Available())
	assert.Equal(t, 1, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block, true, 1, 1)

	// Verify that the changes done during the first pinning are still visible after second pinning
	// since the same buffer is reused
	page2 := buf2.Contents
	pos1Value, _ := page2.GetString(pos1)
	pos2Value, _ := page2.GetInt(pos2)
	assert.Equal(t, text, pos1Value)
	assert.Equal(t, int64(-345), pos2Value)

	bufMgr.UnpinBuffer(buf2)
	bufMgr.printStatus()
	verifyAllocatedBuffer(t, bufMgr, block, false, 0, 1)
}

func TestBufferPinningAndUnpinning(t *testing.T) {
	bufferCount := 3
	db := NewDB(dbDir, blockTestSize, bufferCount)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	bufMgr := db.BufMgr
	bufMgr.printStatus()
	assert.Equal(t, bufferCount, bufMgr.Available())
	assert.Equal(t, 0, len(bufMgr.allocatedBuffers))

	block1 := file.GetBlock(filename, 1)
	buf1 := bufMgr.PinBuffer(block1)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount-1, bufMgr.Available())
	assert.Equal(t, 1, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)

	page := buf1.Contents
	n, _ := page.GetInt(80)
	page.SetInt(80, n+1)
	buf1.SetModified(1, 0)

	bufMgr.UnpinBuffer(buf1)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount, bufMgr.Available())
	assert.Equal(t, 1, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block1, false, 0, 1)

	block2 := file.GetBlock(filename, 2)
	buf2 := bufMgr.PinBuffer(block2)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount-1, bufMgr.Available())
	assert.Equal(t, 2, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block1, false, 0, 1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)

	block3 := file.GetBlock(filename, 3)
	bufMgr.PinBuffer(block3)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount-2, bufMgr.Available())
	assert.Equal(t, 3, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block1, false, 0, 1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block3, true, 1, -1)

	block4 := file.GetBlock(filename, 4)
	bufMgr.PinBuffer(block4)
	bufMgr.printStatus()
	assert.Equal(t, 0, bufMgr.Available())
	assert.Equal(t, 3, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block3, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block4, true, 1, -1)

	bufMgr.UnpinBuffer(buf2)
	bufMgr.printStatus()
	assert.Equal(t, 1, bufMgr.Available())
	assert.Equal(t, 3, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block2, false, 0, -1)
	verifyAllocatedBuffer(t, bufMgr, block3, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block4, true, 1, -1)

	buf := bufMgr.PinBuffer(block1)
	page2 := buf.Contents
	page2.SetInt(80, 9999)
	buf.SetModified(1, 0)
	bufMgr.printStatus()
	assert.Equal(t, 0, bufMgr.Available())
	assert.Equal(t, 3, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, 1)
	verifyAllocatedBuffer(t, bufMgr, block3, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block4, true, 1, -1)
}

func verifyAllocatedBuffer(t *testing.T, bufMgr *BufferMgr, block file.Block, isPinned bool, pinCount int, txNum int) {
	buf := bufMgr.allocatedBuffers[block.String()]
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

	bufMgr := db.BufMgr
	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)
	block2 := file.GetBlock(filename, 2)
	block3 := file.GetBlock(filename, 3)
	bufMgr.printStatus()
	assert.Equal(t, bufferCount, bufMgr.Available())
	assert.Equal(t, 0, len(bufMgr.allocatedBuffers))

	bufMgr.PinBuffer(block0)
	buf1 := bufMgr.PinBuffer(block1)
	buf2 := bufMgr.PinBuffer(block2)
	bufMgr.printStatus()
	assert.Equal(t, 0, bufMgr.Available())
	assert.Equal(t, bufferCount, len(bufMgr.allocatedBuffers))
	verifyAllocatedBuffer(t, bufMgr, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)

	bufMgr.UnpinBuffer(buf1)
	bufMgr.PinBuffer(block0)
	bufMgr.PinBuffer(block1)
	bufMgr.printStatus()
	verifyAllocatedBuffer(t, bufMgr, block0, true, 2, -1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)

	// This PinBuffer should fail and should return nil since all buffers are occupied
	assert.Nil(t, bufMgr.PinBuffer(block3, true))

	// If we Unpin a buffer and try again, it should succeed
	bufMgr.UnpinBuffer(buf2)
	assert.NotNil(t, bufMgr.PinBuffer(block3))
	bufMgr.printStatus()
	verifyAllocatedBuffer(t, bufMgr, block0, true, 2, -1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block3, true, 1, -1)
}

func TestFlushAll(t *testing.T) {
	bufferCount := 3
	db := NewDB(dbDir, blockTestSize, bufferCount)
	createFile(db.FileMgr, filename)
	defer removeFile(db.FileMgr.DbFilePath(filename), dbDir)
	defer removeFile(db.FileMgr.DbFilePath(logFile), dbDir)

	bufMgr := db.BufMgr
	block0 := file.GetBlock(filename, 0)
	block1 := file.GetBlock(filename, 1)
	block2 := file.GetBlock(filename, 2)
	buf0 := bufMgr.PinBuffer(block0)
	buf1 := bufMgr.PinBuffer(block1)
	buf2 := bufMgr.PinBuffer(block2)
	verifyAllocatedBuffer(t, bufMgr, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)

	buf0.SetModified(1, 0)
	buf1.SetModified(1, 1)
	buf2.SetModified(2, 0)
	verifyAllocatedBuffer(t, bufMgr, block0, true, 1, 1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, 1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, 2)

	bufMgr.FlushAll(1)
	// Only buffers with txNum = 1 should be flushed to disk and txNum changed to -1
	verifyAllocatedBuffer(t, bufMgr, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, 2)

	buf0.SetModified(3, 0)
	buf1.SetModified(3, 1)
	bufMgr.FlushAll(2)
	// Only buffers with txNum = 2 should be flushed to disk and txNum changed to -1
	verifyAllocatedBuffer(t, bufMgr, block0, true, 1, 3)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, 3)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)

	buf2.SetModified(3, 2)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, 3)

	bufMgr.FlushAll(3)
	// All buffers have txNum = 3 so all buffers should be flushed to disk and txNum changed to -1
	verifyAllocatedBuffer(t, bufMgr, block0, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block1, true, 1, -1)
	verifyAllocatedBuffer(t, bufMgr, block2, true, 1, -1)

}
