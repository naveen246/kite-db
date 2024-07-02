package buffer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
	"github.com/sasha-s/go-deadlock"
	"log"
	"slices"
	"time"
)

/*
BufferMgr manages access to the buffer pages in the Bufferpool.
Bufferpool is an in-memory cache of buffer-pages to store data read from disk

When a client wants to access a disk-block the following steps take place
- The client sends a request to buffer manager.
- The buffer manager selects a buffer-page from the Bufferpool.
- The contents of the disk-block is read(if needed) to the selected buffer-page and the page is returned to the client.
	At this point the buffer-page is said to be pinned to the disk-block by the client.
- The client reads/writes data to the buffer-page(in-memory)
- Once its usage is done, the client requests the buffer manager to unpin the buffer-page

When a client requests a buffer manager for accessing a disk-block
- If a buffer-page holding the contents of the disk-block is present in Bufferpool:
	We use a map "allocatedBuffers" that maps a block to a buffer-page
	The buffer manager checks the map and returns the page if a corresponding buffer-page is present.
- If a buffer-page holding the contents of the disk-block is not present in Bufferpool and at least one unpinned buffer-page is present:
	We have to pick a buffer-page from the list of unpinned buffer-pages. This can be done using LRU, LFU and other strategies
	LRU is implemented using a slice "unpinnedBuffers".
	When a buffer's pin count becomes 0(no longer used by any client), we add the buffer-page to the tail-end of the slice.
	Whenever a buffer is needed, the Least Recently Used buffer-page is present at the head of the list so remove the buffer-page at the head of the list and use it.
*/

// BufferPool Manages the pinning and unpinning of buffers to blocks.
type BufferPool struct {
	deadlock.Mutex
	UnpinnedBuffers []*Buffer

	// AllocatedBuffers maps Block to Buffer
	AllocatedBuffers map[string]*Buffer
}

func NewBufferPool(fileMgr *file.FileMgr, log *wal.Log, bufCount int) *BufferPool {
	buffers := make([]*Buffer, bufCount)
	for i := 0; i < bufCount; i++ {
		buffers[i] = NewBuffer(uuid.NewString(), fileMgr, log)
	}
	return &BufferPool{
		UnpinnedBuffers:  buffers,
		AllocatedBuffers: make(map[string]*Buffer),
	}
}

// Available Returns the number of available (i.e. unpinned) buffers.
func (bm *BufferPool) Available() int {
	bm.Lock()
	defer bm.Unlock()
	return len(bm.UnpinnedBuffers)
}

// FlushAll Flushes the dirty buffers modified by the specified transaction.
func (bm *BufferPool) FlushAll(txNum int64) {
	bm.Lock()
	defer bm.Unlock()
	for _, buf := range bm.AllocatedBuffers {
		if buf.TxNum == txNum {
			err := buf.flush()
			if err != nil {
				log.Printf("Error flushing buffer %v: %v", buf, err)
			}
		}
	}
}

// UnpinBuffer Unpins the specified data buffer.
// If its pin count goes to 0, then it means that no client is accessing the buffer to read/write data
// The client should explicitly unpin the buffer when its work is done
func (bm *BufferPool) UnpinBuffer(buffer *Buffer) {
	bm.Lock()
	defer bm.Unlock()
	buffer.unpin()
	if !buffer.IsPinned() {
		bm.UnpinnedBuffers = append(bm.UnpinnedBuffers, buffer)
	}
}

// PinBuffer Pins a buffer to the specified block, potentially waiting until a buffer becomes available.
// If no buffer becomes available within a fixed time period, then exit with an error
// Caller has an option to skip waiting and return immediately with nil if buffer is not available
func (bm *BufferPool) PinBuffer(block file.Block, skipWait ...bool) *Buffer {
	bm.Lock()
	buf := bm.tryToPin(block)
	bm.Unlock()
	if buf != nil {
		return buf
	}
	if len(skipWait) > 0 && skipWait[0] {
		return nil
	}

	retries := 2
	wait := time.Second * 3
	for i := 0; i < retries; i++ {
		time.Sleep(wait)
		wait *= 2
		bm.Lock()
		buf := bm.tryToPin(block)
		bm.Unlock()
		if buf != nil {
			return buf
		}
	}
	return nil
}

// tryToPin Tries to pin a buffer to the specified block.
// If there is already a buffer allocated to that block then that buffer is used;
// otherwise, an unpinned buffer from the pool is chosen.
// Returns nil if there are no available buffers or if assignToBlock failed.
func (bm *BufferPool) tryToPin(block file.Block) *Buffer {
	buf := bm.prevAllocatedBuffer(block)
	if buf == nil {
		buf = bm.chooseUnpinnedBuffer()
		if buf == nil {
			return nil
		}
		delete(bm.AllocatedBuffers, buf.Block.String())

		err := buf.assignToBlock(block)
		if err != nil {
			return nil
		}

		bm.AllocatedBuffers[block.String()] = buf
	} else {
		if !buf.IsPinned() {
			bm.removeBufferFromUnpinned(buf)
		}
	}
	buf.pin()
	return buf
}

func (bm *BufferPool) prevAllocatedBuffer(block file.Block) *Buffer {
	buf, ok := bm.AllocatedBuffers[block.String()]
	if ok {
		return buf
	}
	return nil
}

func (bm *BufferPool) removeBufferFromUnpinned(buf *Buffer) {
	index := -1
	for i := 0; i < len(bm.UnpinnedBuffers); i++ {
		if bm.UnpinnedBuffers[i].ID == buf.ID {
			index = i
			break
		}
	}
	if index != -1 {
		bm.UnpinnedBuffers = append(bm.UnpinnedBuffers[:index], bm.UnpinnedBuffers[index+1:]...)
	}
}

func (bm *BufferPool) chooseUnpinnedBuffer() *Buffer {
	if len(bm.UnpinnedBuffers) > 0 {
		buf := bm.UnpinnedBuffers[0]
		bm.UnpinnedBuffers = slices.Delete(bm.UnpinnedBuffers, 0, 1)
		return buf
	}
	return nil
}

// for debugging
func (bm *BufferPool) PrintStatus() {
	fmt.Println("Allocated buffers")
	for _, buf := range bm.AllocatedBuffers {
		fmt.Println(buf.String())
	}
	fmt.Println("Unpinned buffers")
	for _, buf := range bm.UnpinnedBuffers {
		fmt.Println(buf.String())
	}
	fmt.Println()
}
