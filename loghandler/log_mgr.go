package loghandler

import (
	"github.com/naveen246/kite-db/common"
	"github.com/naveen246/kite-db/file"
	"log"
	"sync"
)

/*
A Log keeps track of any changes in the database so that the change can be reversed.
Each change is stored as a logRecord(byte slice) in a logFile.
New logRecords are appended to the end of the logFile.

Data is appended in reverse order in each block of the logFile
Below is an example of how the log sequence numbers would look when we append 15 items
+-------------+--------------------+---------------------+
| 3, 2, 1, 0  |  9, 8, 7, 6, 5, 4  |  14, 13, 12, 11, 10 |
+-------------+--------------------+---------------------+
| Block 0     |  Block 1           |  Block 2            |
+-------------+--------------------+---------------------+

logFile is read from latest to oldest data as follows,
Block 2 is read first (14-10)
Block 1 is read next (9-4)
Block 0 is read next (3-0)

Although each block is of same size, we see that Block-0 has 4 items and Block-1 has 6 items.
This is because the dataSize for each item can vary

There will be 1 logPage(in memory) which holds the data of the last block (Block 2 in above example)
The block whose data is held in logPage is called currentBlock

Below is an example of a block of size 30 bytes where we append "abc" first and "defgh" next
The first 4 bytes of all blocks are reserved for the position/offset of the last added record in that block

+===============+==========+==================+=============+=================+============+
| lastRecordPos |  empty   | len(secondValue) | secondValue | len(firstValue) | firstValue |
+===============+==========+==================+=============+=================+============+
| 14            |          | 5                | defgh       | 3               | abc        |
+---------------+----------+------------------+-------------+-----------------+------------+
| 4 bytes       | 10 bytes | 4 bytes          | 5 bytes     | 4 bytes         | 3 bytes    |
+---------------+----------+------------------+-------------+-----------------+------------+

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
// if logFile does not exist, create file with an empty first block
func NewLogMgr(fileMgr file.FileMgr, logFile string) *LogMgr {
	page := file.NewPageWithSize(fileMgr.BlockSize)
	logMgr := &LogMgr{
		fileMgr: fileMgr,
		logFile: logFile,
		logPage: page,
	}

	blockCount := fileMgr.BlockCount(logFile)
	if blockCount == 0 {
		// logFile is a new file so we append new block to file
		logMgr.currentBlock = logMgr.appendNewBlock()
	} else {
		// logFile is an existing file so we get the last block of the file
		// and read the last block contents to logPage
		logMgr.currentBlock = file.GetBlock(logFile, blockCount-1)
		err := fileMgr.Read(logMgr.currentBlock, logMgr.logPage)
		if err != nil {
			log.Fatalf("Read failed for block %v - %v\n", logMgr.currentBlock, err)
		}
	}

	return logMgr
}

// Append logRecord to logPage(memory)
// Log records are written right to left in the logPage.
// Storing the records backwards makes it easy to read latest records first.
func (l *LogMgr) Append(logRecord []byte) int {
	l.Lock()
	defer l.Unlock()

	lastRecordPos, err := l.lastRecordPos()
	if err != nil {
		return l.latestLogSeqNum
	}
	bytesNeeded := len(logRecord) + file.IntSize

	// if logRecord does not fit in current page,
	// then flush logPage(memory) to currentBlock(disk)
	// and append new block to file and make it the currentBlock
	if int(lastRecordPos)-bytesNeeded < file.IntSize { // logRecord doesn't fit
		l.flush()
		l.currentBlock = l.appendNewBlock()
		lastRecordPos, _ = l.lastRecordPos()
	}

	// calculate new record position and write the record to logPage
	recordPos := lastRecordPos - uint32(bytesNeeded)
	err = l.logPage.SetBytes(recordPos, logRecord)
	if err != nil {
		log.Fatalf("Failed to write Log record to page - %v\n", err)
	}

	l.saveLastRecordPos(recordPos)
	l.latestLogSeqNum++
	return l.latestLogSeqNum
}

func (l *LogMgr) appendNewBlock() file.Block {
	block, err := l.fileMgr.Append(l.logFile)
	if err != nil {
		log.Fatalf("Failed to create new block in file %v - %v\n", l.logFile, err)
	}

	l.saveLastRecordPos(l.fileMgr.BlockSize)
	err = l.fileMgr.Write(block, l.logPage)
	if err != nil {
		log.Fatalf("Failed to write page to newly created block in file %v - %v\n", l.logFile, err)
	}
	return block
}

// The first 4 bytes of page holds the position of last written record
func (l *LogMgr) saveLastRecordPos(pos uint32) {
	err := l.logPage.SetInt(0, pos)
	if err != nil {
		log.Fatalf("Failed to save last record position - %v", err)
	}
}

func (l *LogMgr) lastRecordPos() (uint32, error) {
	lastRecordPos, err := l.logPage.GetInt(0)
	if err != nil {
		return 0, err
	}
	return lastRecordPos, nil
}

func (l *LogMgr) flush() {
	err := l.fileMgr.Write(l.currentBlock, l.logPage)
	if err != nil {
		log.Fatalf("Failed to flush to file %v - %v\n", l.logFile, err)
	}
	l.lastSavedLogSeqNum = l.latestLogSeqNum
}

// Flush ensures that log record corresponding to logSeqNum is written to disk
func (l *LogMgr) Flush(logSeqNum int) {
	if logSeqNum > l.lastSavedLogSeqNum {
		l.flush()
	}
}

func (l *LogMgr) Iterator() common.Iterator {
	l.flush()
	return NewIterator(l.fileMgr, l.currentBlock)
}
