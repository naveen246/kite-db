package file

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetAndGetIntToMemory(t *testing.T) {
	nums := []uint32{1, 2, 3, 4, 5}
	var tests = []struct {
		offset   int
		expected uint32
	}{
		{0, 1},
		{4, 2},
		{8, 3},
		{12, 4},
		{16, 5},
	}

	// Test GetInt
	page := NewPageWithSize(len(nums) * IntSize)
	for i, num := range nums {
		offsetStart := i * IntSize
		offsetEnd := offsetStart + IntSize
		binary.BigEndian.PutUint32(page.Buffer[offsetStart:offsetEnd], num)
	}
	for _, tt := range tests {
		actual, err := page.GetInt(tt.offset)
		assert.NoError(t, err)
		assert.Equal(t, tt.expected, actual)
	}

	// Test SetInt
	page = NewPageWithSize(len(nums) * IntSize)
	for i, num := range nums {
		offsetStart := i * IntSize
		err := page.SetInt(offsetStart, num)
		assert.NoError(t, err)
	}
	for _, tt := range tests {
		offsetEnd := tt.offset + IntSize
		actual := binary.BigEndian.Uint32(page.Buffer[tt.offset:offsetEnd])
		assert.Equal(t, tt.expected, actual)
	}
}

func TestSetAndGetBytesToMemory(t *testing.T) {
	var data []byte
	bytes := [][]byte{[]byte{42}, []byte{42, 42}, []byte{42, 42, 42}}
	offsets := []int{0, 5, 11}

	var tests = []struct {
		offset   int
		expected []byte
	}{
		{0, bytes[0]},
		{5, bytes[1]},
		{11, bytes[2]},
	}

	// Test GetBytes
	for _, b := range bytes {
		data = binary.BigEndian.AppendUint32(data, uint32(len(b)))
		data = append(data, b...)
	}
	page := NewPageWithBytes(data)
	for _, tt := range tests {
		actual, err := page.GetBytes(tt.offset)
		assert.NoError(t, err)
		assert.Equal(t, tt.expected, actual)
	}

	// Test SetBytes
	page = NewPageWithSize(18)
	for i := 0; i < len(bytes); i++ {
		page.SetBytes(offsets[i], bytes[i])
	}

	for _, tt := range tests {
		length := binary.BigEndian.Uint32(page.Buffer[tt.offset : tt.offset+IntSize])
		assert.Equal(t, len(tt.expected), int(length))

		actual := page.Buffer[tt.offset+IntSize : tt.offset+IntSize+int(length)]
		assert.Equal(t, tt.expected, actual)
	}
}

func TestSetAndGetStringToMemory(t *testing.T) {
	data := []string{"a", "bc", "def"}
	offsets := []int{0, 5, 11}

	page := NewPageWithSize(18)
	for i := 0; i < len(data); i++ {
		page.SetString(offsets[i], data[i])
	}
	for i := 0; i < len(data); i++ {
		actual, _ := page.GetString(offsets[i])
		assert.Equal(t, data[i], actual)
	}
}
