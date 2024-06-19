package loghandler

import (
	"github.com/naveen246/kite-db/file"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

const blockTestSize int64 = 28

var tempFileName = "temp.log"
var initialText = "abcdefghijkl"
var dbDir = "temp_dir"

// createFile creates file temp_dir/filename
// and adds 1 logRecord which fills the complete first block in the file
func createFile(filename string) file.FileMgr {
	fileMgr := file.NewFileMgr(dbDir, blockTestSize)
	_, err := os.Create(fileMgr.DbFilePath(filename))
	if err != nil {
		log.Fatal(err)
	}

	page := file.NewPageWithSize(blockTestSize)
	page.SetInt(0, file.IntSize)
	page.SetString(file.IntSize, initialText)
	fileMgr.Write(file.GetBlock(tempFileName, 0), page)
	return fileMgr
}

func removeFile(filename string, dbDir string) {
	os.Remove(filename)
	os.Remove(dbDir)
}

func TestNewLogMgr(t *testing.T) {
	fileMgr := file.NewFileMgr(dbDir, blockTestSize)
	logMgr := NewLogMgr(fileMgr, tempFileName)
	assert.Equal(t, int64(0), logMgr.currentBlock.Number)
	assert.Equal(t, blockTestSize, logMgr.logPage.Size)
	assert.Equal(t, tempFileName, logMgr.logFile)
	removeFile(fileMgr.DbFilePath(tempFileName), fileMgr.DbDir)

	fileMgr = createFile(tempFileName)
	logMgr = NewLogMgr(fileMgr, tempFileName)
	assert.Equal(t, int64(0), logMgr.currentBlock.Number)
	assert.Equal(t, blockTestSize, logMgr.logPage.Size)
	assert.Equal(t, tempFileName, logMgr.logFile)
	removeFile(fileMgr.DbFilePath(tempFileName), fileMgr.DbDir)
}

func TestLogAppend(t *testing.T) {
	fileMgr := createFile(tempFileName)
	defer removeFile(fileMgr.DbFilePath(tempFileName), fileMgr.DbDir)
	logMgr := NewLogMgr(fileMgr, tempFileName)

	text := []string{"abcde", "fgh", "i", "opq"}
	tests := []struct {
		text       string
		blockNum   int64
		lastRecPos int64
		lsn        int
	}{
		// TODO: These values depend on block size. Remove hardcoded values and calculate values
		{text: text[0], blockNum: 1, lastRecPos: 15, lsn: 1},
		{text: text[1], blockNum: 2, lastRecPos: 17, lsn: 2},
		{text: text[2], blockNum: 2, lastRecPos: 8, lsn: 3},
		{text: text[3], blockNum: 3, lastRecPos: 17, lsn: 4},
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
	fileMgr := createFile(tempFileName)
	defer removeFile(fileMgr.DbFilePath(tempFileName), fileMgr.DbDir)
	logMgr := NewLogMgr(fileMgr, tempFileName)

	initialBlockCount := logMgr.fileMgr.BlockCount(tempFileName)
	logMgr.appendNewBlock()
	newBlockCount := logMgr.fileMgr.BlockCount(tempFileName)
	assert.Equal(t, initialBlockCount+1, newBlockCount)

	recordPos, _ := logMgr.lastRecordPos()
	assert.Equal(t, blockTestSize, recordPos)
}

func TestLogFlush(t *testing.T) {
	fileMgr := createFile(tempFileName)
	defer removeFile(fileMgr.DbFilePath(tempFileName), fileMgr.DbDir)
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
	fileMgr := createFile(tempFileName)
	defer removeFile(fileMgr.DbFilePath(tempFileName), fileMgr.DbDir)
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
