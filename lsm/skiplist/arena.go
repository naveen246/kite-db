package skiplist

import (
	"errors"
	"math"
	"sync/atomic"
	"unsafe"
)

type Align uint8

const (
	Align1 = 0
	Align8 = 7
)

var (
	ErrArenaFull = errors.New("allocation failed because arena is full")
)

type Arena struct {
	size atomic.Uint64
	buf  []byte
}

func NewArena(size uint32) *Arena {
	a := &Arena{}
	a.size.Store(1)
	a.buf = make([]byte, size)
	return a
}

func (a *Arena) Size() uint32 {
	s := a.size.Load()
	if s > math.MaxUint32 {
		// Saturate at MaxUint32.
		return math.MaxUint32
	}
	return uint32(s)
}

func (a *Arena) Cap() uint32 {
	return uint32(len(a.buf))
}

func (a *Arena) Reset() {
	a.size.Store(1)
}

func (a *Arena) Alloc(size, overflow uint32, align Align) (uint32, error) {
	if int(a.size.Load()) > len(a.buf) {
		return 0, ErrArenaFull
	}

	padded := size + uint32(align)
	newSize := a.size.Add(uint64(padded))
	if int(newSize)+int(overflow) > len(a.buf) {
		return 0, ErrArenaFull
	}

	offset := (uint32(newSize) - padded + uint32(align)) & ^uint32(align)
	return offset, nil
}

func (a *Arena) GetBytes(offset uint32, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size]
}

func (a *Arena) GetPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}

	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) GetPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}

	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}
