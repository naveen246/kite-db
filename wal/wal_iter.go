package wal

import (
	"github.com/naveen246/kite-db/file"
	log2 "log"
)

// LogIterator provides the ability to move from latest to oldest log record
// This becomes easy since data is appended in reverse order in each block of the logFile
type LogIterator struct {
	fileMgr    file.FileMgr
	block      file.Block
	page       *file.Page
	currentPos int64
}

func NewIterator(fileMgr file.FileMgr, block file.Block) *LogIterator {
	page := file.NewPageWithSize(fileMgr.BlockSize)
	iter := &LogIterator{
		fileMgr: fileMgr,
		block:   block,
		page:    page,
	}
	iter.moveToBlock(block)
	return iter
}

func (l *LogIterator) HasNext() bool {
	return l.currentPos < l.fileMgr.BlockSize || l.block.Number > 0
}

// Next Moves to the next log record in the block.
// If there are no more log records in the block,
// then move to the previous block and return the latest log record from there.
func (l *LogIterator) Next() []byte {
	if l.currentPos >= l.fileMgr.BlockSize {
		l.block = file.GetBlock(l.block.Filename, l.block.Number-1)
		l.moveToBlock(l.block)
	}

	record, err := l.page.GetBytes(l.currentPos)
	if err != nil {
		return nil
	}
	l.currentPos += int64(len(record)) + file.IntSize
	return record
}

// moveToBlock Moves to the specified log block
// and positions it at the first record in that block
func (l *LogIterator) moveToBlock(block file.Block) {
	err := l.fileMgr.Read(block, l.page)
	if err != nil {
		log2.Fatalln("Failed to moveToBlock", err)
	}
	l.currentPos, _ = l.page.GetInt(0)
}
