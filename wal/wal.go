package wal

import (
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/iter"
	"github.com/sasha-s/go-deadlock"
	log2 "log"
	"sync/atomic"
)

/*
A Log keeps track of any changes in the database so that the change can be reversed.
Each change is stored as a logRecord(byte slice) in a LogFile.
New logRecords are appended to the end of the LogFile.

Data is appended in reverse order in each block of the LogFile
Below is an example of how the log sequence numbers would look when we append 15 items
+-------------+--------------------+---------------------+
| 3, 2, 1, 0  |  9, 8, 7, 6, 5, 4  |  14, 13, 12, 11, 10 |
+-------------+--------------------+---------------------+
| Block 0     |  Block 1           |  Block 2            |
+-------------+--------------------+---------------------+

LogFile is read from latest to oldest data as follows,
Block 2 is read first (14-10)
Block 1 is read next (9-4)
Block 0 is read next (3-0)

Although each block is of same size, we see that Block-0 has 4 items and Block-1 has 6 items.
This is because the dataSize for each item can vary

There will be 1 logPage(in memory) which holds the data of the last block (Block 2 in above example)
The block whose data is held in logPage is called currentBlock

Below is an example of a block of size 40 bytes where we append "abc" first and "defgh" next
The first 8 bytes of all blocks are reserved for the position/offset of the last added record in that block

+===============+==========+==================+=============+=================+============+
| lastRecordPos |  empty   | len(secondValue) | secondValue | len(firstValue) | firstValue |
+===============+==========+==================+=============+=================+============+
| 16            |          | 5                | defgh       | 3               | abc        |
+---------------+----------+------------------+-------------+-----------------+------------+
| 8 bytes       | 8 bytes  | 8 bytes          | 5 bytes     | 8 bytes         | 3 bytes    |
+---------------+----------+------------------+-------------+-----------------+------------+
0               8          16                 24            29                37           40
*/

// Log is responsible for writing log records into a log file.
// New records are appended to memory(logPage) and flushed to disk(LogFile) when needed
type Log struct {
	deadlock.Mutex
	fileMgr      *file.FileMgr
	LogFile      string
	currentBlock file.Block
	logPage      *file.Page

	// latestLogSeqNum is incremented every time logRecord is written to logPage
	latestLogSeqNum atomic.Int64

	// lastSavedLogSeqNum is updated to latestLogSeqNum when the logPage is flushed to disk
	lastSavedLogSeqNum atomic.Int64
}

// NewLog creates manager for specified LogFile
// if LogFile does not exist, create file with an empty first block
func NewLog(fileMgr *file.FileMgr, logFile string) *Log {
	page := file.NewPageWithSize(fileMgr.BlockSize)
	log := &Log{
		fileMgr: fileMgr,
		LogFile: logFile,
		logPage: page,
	}

	blockCount := fileMgr.BlockCount(logFile)
	if blockCount == 0 {
		// LogFile is a new file so we append new block to file
		log.currentBlock = log.appendNewBlock()
	} else {
		// LogFile is an existing file so we get the last block of the file
		// and read the last block contents to logPage
		log.currentBlock = file.GetBlock(logFile, blockCount-1)
		err := fileMgr.Read(log.currentBlock, log.logPage)
		if err != nil {
			log2.Fatalf("Read failed for block %v - %v\n", log.currentBlock, err)
		}
	}

	return log
}

// Append logRecord to logPage(memory), returns logSeqNumber of the appended record
// Log records are written right to left in the logPage.
// Storing the records backwards makes it easy to read latest records first.
func (l *Log) Append(logRecord []byte) int64 {
	l.Lock()
	defer l.Unlock()

	lastRecordPos, err := l.lastRecordPos()
	if err != nil {
		return l.latestLogSeqNum.Load()
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
	recordPos := lastRecordPos - int64(bytesNeeded)
	err = l.logPage.SetBytes(recordPos, logRecord)
	if err != nil {
		log2.Fatalf("Failed to write Log record to page - %v\n", err)
	}

	l.saveLastRecordPos(recordPos)
	l.latestLogSeqNum.Add(1)
	return l.latestLogSeqNum.Load()
}

func (l *Log) appendNewBlock() file.Block {
	block, err := l.fileMgr.Append(l.LogFile)
	if err != nil {
		log2.Fatalf("Failed to create new block in file %v - %v\n", l.LogFile, err)
	}

	l.saveLastRecordPos(l.fileMgr.BlockSize)
	err = l.fileMgr.Write(block, l.logPage)
	if err != nil {
		log2.Fatalf("Failed to write page to newly created block in file %v - %v\n", l.LogFile, err)
	}
	return block
}

// The first 8 bytes of page holds the position of last written record
func (l *Log) saveLastRecordPos(pos int64) {
	err := l.logPage.SetInt(0, pos)
	if err != nil {
		log2.Fatalf("Failed to save last record position - %v", err)
	}
}

func (l *Log) lastRecordPos() (int64, error) {
	lastRecordPos, err := l.logPage.GetInt(0)
	if err != nil {
		return 0, err
	}
	return lastRecordPos, nil
}

func (l *Log) flush() {
	err := l.fileMgr.Write(l.currentBlock, l.logPage)
	if err != nil {
		log2.Fatalf("Failed to flush to file %v - %v\n", l.LogFile, err)
	}
	l.lastSavedLogSeqNum.Store(l.latestLogSeqNum.Load())
}

// Flush ensures that log record corresponding to logSeqNum is written to disk
func (l *Log) Flush(logSeqNum int64) {
	l.Lock()
	defer l.Unlock()
	lastSavedLogSeqNum := l.lastSavedLogSeqNum.Load()
	if logSeqNum > lastSavedLogSeqNum {
		l.flush()
	}
}

func (l *Log) Iterator() iter.Iterator {
	l.Lock()
	defer l.Unlock()
	l.flush()
	return NewIterator(l.fileMgr, l.currentBlock)
}
