package buffer

import (
	"fmt"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
)

// Buffer A Buffer wraps a page and stores information about its status,
// such as the associated disk block, the number of times the buffer has been pinned,
// whether its contents have been modified, and if so, the id and logSequenceNumber of the modifying transaction.
type Buffer struct {
	fileMgr *file.FileMgr
	log     *wal.Log

	// The main content of the buffer which is accessed by clients to read/write data
	Contents *file.Page

	// Disk block allocated to the buffer.
	Block file.Block
	ID    string

	// Pins indicates the number of clients currently accessing the buffer to read/write content
	Pins int
	// TxNum >= 0 indicates that the buffer page is modified in memory by a client and the page
	// has to be flushed to disk at some point.
	// Initial value is -1 when there is no change in the buffer page
	TxNum     int64
	logSeqNum int64
}

func NewBuffer(id string, fileMgr *file.FileMgr, log *wal.Log) *Buffer {
	page := file.NewPageWithSize(fileMgr.BlockSize)
	return &Buffer{
		ID:        id,
		fileMgr:   fileMgr,
		log:       log,
		Contents:  page,
		TxNum:     -1,
		Pins:      0,
		logSeqNum: -1,
	}
}

// SetModified is called when there is modification done in-memory to the buffer page.
// This indicates that the buffer page is dirty and will need to be flushed to disk at some point to persist the changes done.
func (b *Buffer) SetModified(txNum int64, lsn int64) {
	b.TxNum = txNum
	if lsn >= 0 {
		b.logSeqNum = lsn
	}
}

// assignToBlock Reads the contents of the specified file block into the contents of the buffer.
// If the buffer was dirty(modified in-memory), then its previous contents are first flushed to disk.
func (b *Buffer) assignToBlock(block file.Block) error {
	err := b.flush()
	if err != nil {
		return err
	}

	err = b.fileMgr.Read(block, b.Contents)
	if err != nil {
		return err
	}

	b.Block = block
	b.Pins = 0
	return nil
}

// Write the buffer to its disk block if it is dirty.
func (b *Buffer) flush() error {
	if b.TxNum >= 0 {
		b.log.Flush(b.logSeqNum)
		err := b.fileMgr.Write(b.Block, b.Contents)
		if err != nil {
			return err
		}
		b.TxNum = -1
	}
	return nil
}

// IsPinned Return true if the buffer is currently pinned (that is, if it has a nonzero pin count).
// A buffer is said to be pinned if a client is currently accessing it to either read/write data
// Multiple clients can access a buffer.
// The number of pins indicate the number of clients currently accessing the buffer
func (b *Buffer) IsPinned() bool {
	return b.Pins > 0
}

func (b *Buffer) pin() {
	b.Pins++
}

func (b *Buffer) unpin() {
	b.Pins--
}

func (b *Buffer) String() string {
	return fmt.Sprintf("Buffer %v: [%v] IsPinned: %v, txNum: %v, pins: %v", b.ID[len(b.ID)-3:], b.Block, b.IsPinned(), b.TxNum, b.Pins)
}
