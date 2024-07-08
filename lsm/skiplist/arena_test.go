package skiplist

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestArena(t *testing.T) {
	a := NewArena(math.MaxUint32)

	// Initial allocation
	offset, err := a.Alloc(math.MaxUint16, 0, Align1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), offset)
	assert.Equal(t, uint32(math.MaxUint16)+1, a.Size())
	assert.Equal(t, uint32(math.MaxUint32), a.Cap())

	// Verify the offset and size after new allocation
	offset, err = a.Alloc(100, 0, Align1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(math.MaxUint16+1), offset)
	assert.Equal(t, uint32(math.MaxUint16+101), a.Size())

	a.Reset()

	// Verify offset and size after reset
	offset, err = a.Alloc(100, 0, Align1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), offset)
	assert.Equal(t, uint32(101), a.Size())
}

func TestArenaSizeOverflow(t *testing.T) {
	a := NewArena(math.MaxUint32)

	// Allocating under the limit throws no error.
	offset, err := a.Alloc(math.MaxUint16, 0, Align1)
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), offset)
	assert.Equal(t, uint32(math.MaxUint16)+1, a.Size())

	// Allocating over the limit could cause an accounting
	// overflow if 32-bit arithmetic was used. It shouldn't.
	_, err = a.Alloc(math.MaxUint32, 0, Align1)
	assert.Equal(t, ErrArenaFull, err)
	assert.Equal(t, uint32(math.MaxUint32), a.Size())

	// Continuing to allocate continues to throw an error.
	_, err = a.Alloc(math.MaxUint16, 0, Align1)
	assert.Equal(t, ErrArenaFull, err)
	assert.Equal(t, uint32(math.MaxUint32), a.Size())
}
