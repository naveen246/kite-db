package buffer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
	"log"
	"slices"
	"sync"
	"time"
)

// BufferPool Manages the pinning and unpinning of buffers to blocks.
type BufferPool struct {
	sync.Mutex
	unpinnedBuffers []*Buffer

	// allocatedBuffers maps Block to Buffer
	allocatedBuffers map[string]*Buffer
}

func NewBufferPool(fileMgr file.FileMgr, log *wal.Log, bufCount int) *BufferPool {
	buffers := make([]*Buffer, bufCount)
	for i := 0; i < bufCount; i++ {
		buffers[i] = NewBuffer(uuid.NewString(), fileMgr, log)
	}
	return &BufferPool{
		unpinnedBuffers:  buffers,
		allocatedBuffers: make(map[string]*Buffer),
	}
}

// Available Returns the number of available (i.e. unpinned) buffers.
func (bm *BufferPool) Available() int {
	bm.Lock()
	defer bm.Unlock()
	return len(bm.unpinnedBuffers)
}

// FlushAll Flushes the dirty buffers modified by the specified transaction.
func (bm *BufferPool) FlushAll(txNum int64) {
	bm.Lock()
	defer bm.Unlock()
	for _, buf := range bm.allocatedBuffers {
		if buf.txNum == txNum {
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
	if !buffer.isPinned() {
		bm.unpinnedBuffers = append(bm.unpinnedBuffers, buffer)
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
		delete(bm.allocatedBuffers, buf.Block.String())

		err := buf.assignToBlock(block)
		if err != nil {
			return nil
		}

		bm.allocatedBuffers[block.String()] = buf
	} else {
		if !buf.isPinned() {
			bm.removeBufferFromUnpinned(buf)
		}
	}
	buf.pin()
	return buf
}

func (bm *BufferPool) prevAllocatedBuffer(block file.Block) *Buffer {
	buf, ok := bm.allocatedBuffers[block.String()]
	if ok {
		return buf
	}
	return nil
}

func (bm *BufferPool) removeBufferFromUnpinned(buf *Buffer) {
	index := -1
	for i := 0; i < len(bm.unpinnedBuffers); i++ {
		if bm.unpinnedBuffers[i].ID == buf.ID {
			index = i
			break
		}
	}
	if index != -1 {
		bm.unpinnedBuffers = append(bm.unpinnedBuffers[:index], bm.unpinnedBuffers[index+1:]...)
	}
}

func (bm *BufferPool) chooseUnpinnedBuffer() *Buffer {
	if len(bm.unpinnedBuffers) > 0 {
		buf := bm.unpinnedBuffers[0]
		bm.unpinnedBuffers = slices.Delete(bm.unpinnedBuffers, 0, 1)
		return buf
	}
	return nil
}

// for debugging
func (bm *BufferPool) printStatus() {
	fmt.Println("Allocated buffers")
	for _, buf := range bm.allocatedBuffers {
		fmt.Println(buf.String())
	}
	fmt.Println("Unpinned buffers")
	for _, buf := range bm.unpinnedBuffers {
		fmt.Println(buf.String())
	}
	fmt.Println()
}
