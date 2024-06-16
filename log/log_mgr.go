package log

import (
	"github.com/naveen246/kite-db/file"
	"log"
	"sync"
)

/*
Any changes in the database should be kept track of, so that the change can be reversed.
Each change is stored as a logRecord in a logFile.
New logRecords are appended to the end of the log.
*/

// LogMgr is responsible for writing log records into a log file.
// New records are appended to memory(logPage) and flushed to disk(logFile) when needed
type LogMgr struct {
	sync.Mutex
	fileMgr      file.FileMgr
	logFile      string
	currentBlock file.Block
	logPage      *file.Page

	// latestLogSeqNum is incremented every time logRecord is written to logPage
	latestLogSeqNum int

	// lastSavedLogSeqNum is updated to latestLogSeqNum when the logPage is flushed to disk
	lastSavedLogSeqNum int
}

// NewLogMgr creates manager for specified logFile
// if logFile does not exist, create with an empty first block
func NewLogMgr(fileMgr file.FileMgr, logFile string) *LogMgr {
	page := file.NewPageWithSize(fileMgr.BlockSize)
	logMgr := &LogMgr{
		fileMgr: fileMgr,
		logFile: logFile,
		logPage: page,
	}

	blockCount := fileMgr.BlockCount(logFile)
	if blockCount == 0 {
		// This is a new file so we append new block to file
		logMgr.currentBlock = logMgr.appendNewBlock()
	} else {
		// This is an existing file so we get the last block of the file
		logMgr.currentBlock = file.GetBlock(logFile, blockCount-1)
		err := fileMgr.Read(logMgr.currentBlock, logMgr.logPage)
		if err != nil {
			log.Fatalf("Read failed for block %v\n", logMgr.currentBlock)
		}
	}

	return logMgr
}

// Append logRecord to logPage(memory)
// Log records are written right to left in the logPage.
// Storing the records backwards makes it easy to read latest records first.
// The beginning of logPage contains the location of the last-written record.
func (l *LogMgr) Append(logRecord []byte) int {
	l.Lock()
	defer l.Unlock()
	lastRecordPos, err := l.logPage.GetInt(0)
	if err != nil {
		return l.latestLogSeqNum
	}
	bytesNeeded := len(logRecord) + file.IntSize

	// if logRecord does not fit in current page,
	// then flush current page to disk, append new block to file
	if lastRecordPos-uint32(bytesNeeded) < uint32(file.IntSize) { // logRecord doesn't fit
		l.flush()
		l.currentBlock = l.appendNewBlock()
		lastRecordPos, _ = l.logPage.GetInt(0)
	}

	// calculate position and write the record to log
	recordPos := lastRecordPos - uint32(bytesNeeded)
	err = l.logPage.SetBytes(int(recordPos), logRecord)
	if err != nil {
		log.Fatal("Failed to write Log record to page")
	}

	l.updateLastRecordPos(recordPos)
	l.latestLogSeqNum++
	return l.latestLogSeqNum
}

func (l *LogMgr) appendNewBlock() file.Block {
	block, err := l.fileMgr.Append(l.logFile)
	if err != nil {
		log.Fatalf("Failed to create new block in file %v\n", l.logFile)
	}
	l.updateLastRecordPos(uint32(l.fileMgr.BlockSize))
	err = l.fileMgr.Write(block, l.logPage)
	if err != nil {
		log.Fatalf("Failed to write page to newly created block in file %v\n", l.logFile)
	}
	return block
}

// The first 4 bytes of page holds the position of last written record
func (l *LogMgr) updateLastRecordPos(pos uint32) {
	err := l.logPage.SetInt(0, pos)
	if err != nil {
		log.Fatal("Failed to update last record position")
	}
}

func (l *LogMgr) flush() {
	err := l.fileMgr.Write(l.currentBlock, l.logPage)
	if err != nil {
		log.Fatal(err)
	}
	l.lastSavedLogSeqNum = l.latestLogSeqNum
}

// Flush ensures that log record corresponding to logSeqNum is written to disk
func (l *LogMgr) Flush(logSeqNum int) {
	if logSeqNum > l.lastSavedLogSeqNum {
		l.flush()
	}
}

func (l *LogMgr) Iterator() {

}
