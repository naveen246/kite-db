package buffer

import (
	"github.com/google/uuid"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/loghandler"
	"log"
	"slices"
	"sync"
	"time"
)

const MAX_WAIT_TIME = 10 * time.Second

type BufferMgr struct {
	sync.Mutex
	prevUnpinnedBuffers []*Buffer
	numAvailable        int

	// allocatedBuffers maps Block to Buffer
	allocatedBuffers map[string]*Buffer
}

func NewBufferMgr(fileMgr file.FileMgr, logMgr *loghandler.LogMgr, bufCount int) *BufferMgr {
	buffers := make([]*Buffer, bufCount)
	for i := 0; i < bufCount; i++ {
		buffers[i] = NewBuffer(uuid.NewString(), fileMgr, logMgr)
	}
	return &BufferMgr{
		prevUnpinnedBuffers: buffers,
		numAvailable:        bufCount,
	}
}

func (bm *BufferMgr) Available() int {
	bm.Lock()
	defer bm.Unlock()
	return bm.numAvailable
}

func (bm *BufferMgr) FlushAll(txNum int) {
	bm.Lock()
	defer bm.Unlock()
	for _, buf := range bm.allocatedBuffers {
		if buf.txNum == txNum {
			buf.flush()
		}
	}
}

func (bm *BufferMgr) UnpinBuffer(buffer Buffer) {
	bm.Lock()
	defer bm.Unlock()
	buffer.unpin()
	if !buffer.isPinned() {
		bm.prevUnpinnedBuffers = append(bm.prevUnpinnedBuffers, &buffer)
		bm.numAvailable++
		// TODO notify ??
	}
}

func (bm *BufferMgr) PinBufferToBlock(block file.Block) *Buffer {
	bm.Lock()
	defer bm.Unlock()

	if buf := bm.tryToPin(block); buf != nil {
		return buf
	}

	retries := 2
	wait := 3 * time.Second
	for i := 0; i < retries; i++ {
		time.Sleep(wait)
		wait *= 2
		if buf := bm.tryToPin(block); buf != nil {
			return buf
		}
	}
	log.Fatalf("Could not pin buffer to block %v\n", block)
	return nil
}

func (bm *BufferMgr) tryToPin(block file.Block) *Buffer {
	buf := bm.prevAllocatedBuffer(block)
	if buf == nil {
		buf = bm.chooseUnpinnedBuffer()
		if buf == nil {
			return nil
		}
		delete(bm.allocatedBuffers, buf.Block.String())
		buf.assignToBlock(block)
		bm.allocatedBuffers[block.String()] = buf
	}
	if !buf.isPinned() {
		bm.numAvailable--
	}
	buf.pin()
	return buf
}

func (bm *BufferMgr) prevAllocatedBuffer(block file.Block) *Buffer {
	buf, ok := bm.allocatedBuffers[block.String()]
	if ok {
		return buf
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for len(bm.prevUnpinnedBuffers) > 0 {
		buf := bm.prevUnpinnedBuffers[0]
		bm.prevUnpinnedBuffers = slices.Delete(bm.prevUnpinnedBuffers, 0, 1)
		if !buf.isPinned() {
			return buf
		}
	}
	return nil
}
