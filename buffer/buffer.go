package buffer

import (
	"fmt"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/loghandler"
)

type Buffer struct {
	fileMgr  file.FileMgr
	logMgr   *loghandler.LogMgr
	Contents *file.Page
	Block    file.Block
	Id       string

	pins      int
	txNum     int
	logSeqNum int
}

func NewBuffer(id string, fileMgr file.FileMgr, logMgr *loghandler.LogMgr) *Buffer {
	page := file.NewPageWithSize(fileMgr.BlockSize)
	return &Buffer{
		Id:       id,
		fileMgr:  fileMgr,
		logMgr:   logMgr,
		Contents: page,
	}
}

func (b *Buffer) SetModified(txNum int, lsn int) {
	b.txNum = txNum
	if lsn >= 0 {
		b.logSeqNum = lsn
	}
}

func (b *Buffer) assignToBlock(block file.Block) {
	b.flush()
	b.Block = block
	b.fileMgr.Read(b.Block, b.Contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txNum >= 0 {
		b.logMgr.Flush(b.logSeqNum)
		b.fileMgr.Write(b.Block, b.Contents)
		b.txNum = -1
	}
}

func (b *Buffer) isPinned() bool {
	return b.pins > 0
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}

func (b *Buffer) String() string {
	return fmt.Sprintf("Buffer %v: [%v] isPinned: %v", b.Id, b.Block, b.isPinned())
}
