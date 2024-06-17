package log

import (
	"github.com/naveen246/kite-db/file"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

const blockTestSize uint32 = 20

var tempFileName = "temp.log"
var initialText = "abcdefghijkl"

// setup creates file temp_dir/filename
// and adds 1 logRecord which fills the complete first block in the file
func setup(filename string) file.FileMgr {
	fileMgr := file.NewFileMgr("temp_dir", blockTestSize)
	_, err := os.Create(fileMgr.DbFilePath(filename))
	if err != nil {
		log.Fatal(err)
	}

	page := file.NewPageWithSize(blockTestSize)
	page.SetInt(0, 4)
	page.SetString(4, initialText)
	fileMgr.Write(file.GetBlock(tempFileName, 0), page)
	return fileMgr
}

func teardown(filename string, fileMgr file.FileMgr) {
	os.Remove(filename)
	os.Remove(fileMgr.DbDir)
}

func TestNewLogMgr(t *testing.T) {
	fileMgr := file.NewFileMgr("temp_dir", blockTestSize)
	logMgr := NewLogMgr(fileMgr, tempFileName)
	assert.Equal(t, uint32(0), logMgr.currentBlock.Number)
	assert.Equal(t, blockTestSize, logMgr.logPage.Size)
	assert.Equal(t, tempFileName, logMgr.logFile)
	teardown(fileMgr.DbFilePath(tempFileName), fileMgr)

	fileMgr = setup(tempFileName)
	logMgr = NewLogMgr(fileMgr, tempFileName)
	assert.Equal(t, uint32(0), logMgr.currentBlock.Number)
	assert.Equal(t, blockTestSize, logMgr.logPage.Size)
	assert.Equal(t, tempFileName, logMgr.logFile)
	teardown(fileMgr.DbFilePath(tempFileName), fileMgr)
}

func TestLogAppend(t *testing.T) {
	fileMgr := setup(tempFileName)
	defer teardown(fileMgr.DbFilePath(tempFileName), fileMgr)
	logMgr := NewLogMgr(fileMgr, tempFileName)

	tests := []struct {
		text       string
		blockNum   uint32
		lastRecPos uint32
		lsn        int
	}{
		{text: "abcde", blockNum: 1, lastRecPos: 11, lsn: 1},
		{text: "fgh", blockNum: 1, lastRecPos: 4, lsn: 2},
		{text: "ijklmn", blockNum: 2, lastRecPos: 10, lsn: 3},
		{text: "opq", blockNum: 3, lastRecPos: 13, lsn: 4},
	}

	for _, tt := range tests {
		logMgr.Append([]byte(tt.text))
		assert.Equal(t, tt.blockNum, logMgr.currentBlock.Number)
		assert.Equal(t, blockTestSize, logMgr.logPage.Size)
		recordPos, _ := logMgr.lastRecordPos()
		assert.Equal(t, tt.lastRecPos, recordPos)

		data, _ := logMgr.logPage.GetBytes(recordPos)
		assert.Equal(t, tt.text, string(data))
		assert.Equal(t, tt.lsn, logMgr.latestLogSeqNum)
	}
}

func TestLogAppendNewBlock(t *testing.T) {
	fileMgr := setup(tempFileName)
	defer teardown(fileMgr.DbFilePath(tempFileName), fileMgr)
	logMgr := NewLogMgr(fileMgr, tempFileName)

	initialBlockCount := logMgr.fileMgr.BlockCount(tempFileName)
	logMgr.appendNewBlock()
	newBlockCount := logMgr.fileMgr.BlockCount(tempFileName)
	assert.Equal(t, initialBlockCount+1, newBlockCount)

	recordPos, _ := logMgr.lastRecordPos()
	assert.Equal(t, blockTestSize, recordPos)
}

func TestLogFlush(t *testing.T) {
	fileMgr := setup(tempFileName)
	defer teardown(fileMgr.DbFilePath(tempFileName), fileMgr)
	logMgr := NewLogMgr(fileMgr, tempFileName)

	assert.Equal(t, 0, logMgr.latestLogSeqNum)
	assert.Equal(t, 0, logMgr.lastSavedLogSeqNum)
	logMgr.Append([]byte("abcde"))
	assert.Equal(t, 1, logMgr.latestLogSeqNum)
	assert.Equal(t, 0, logMgr.lastSavedLogSeqNum)
	logMgr.Flush(1)
	assert.Equal(t, 1, logMgr.latestLogSeqNum)
	assert.Equal(t, 1, logMgr.lastSavedLogSeqNum)
}

func TestLogIterator(t *testing.T) {
	fileMgr := setup(tempFileName)
	defer teardown(fileMgr.DbFilePath(tempFileName), fileMgr)
	logMgr := NewLogMgr(fileMgr, tempFileName)

	text := []string{"abcde", "fgh", "ijklmn", "opq"}
	for _, t := range text {
		logMgr.Append([]byte(t))
	}

	iter := logMgr.Iterator()
	for i := 3; i >= 0; i-- {
		assert.True(t, iter.HasNext())
		assert.Equal(t, text[i], string(iter.Next()))
	}

	assert.True(t, iter.HasNext())
	assert.Equal(t, initialText, string(iter.Next()))
	assert.False(t, iter.HasNext())
}
