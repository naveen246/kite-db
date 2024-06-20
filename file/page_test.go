package file

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetInt(t *testing.T) {
	nums := []int64{10, 22, -7}
	initial := int64(0)
	var tests = []struct {
		offset   int64
		expected int64
	}{
		{initial, nums[0]},
		{initial + IntSize, nums[1]},
		{initial + 2*IntSize, nums[2]},
	}

	page := NewPageWithSize(int64(len(nums)) * IntSize)
	for i, num := range nums {
		offsetStart := i * IntSize
		offsetEnd := offsetStart + IntSize

		bs := Int64ToBytes(num)
		copy(page.Buffer[offsetStart:offsetEnd], bs)
	}
	for _, tt := range tests {
		actual, err := page.GetInt(tt.offset)
		assert.NoError(t, err)
		assert.Equal(t, tt.expected, actual)
	}
}

func TestSetInt(t *testing.T) {
	nums := []int64{1, -22, 300}
	initial := int64(0)
	var tests = []struct {
		offset   int64
		expected int64
	}{
		{initial, nums[0]},
		{initial + IntSize, nums[1]},
		{initial + 2*IntSize, nums[2]},
	}

	page := NewPageWithSize(int64(len(nums) * IntSize))
	for i, num := range nums {
		offsetStart := i * IntSize
		err := page.SetInt(int64(offsetStart), num)
		assert.NoError(t, err)
	}
	for _, tt := range tests {
		offsetEnd := tt.offset + IntSize
		actual, _ := BytesToInt64(page.Buffer[tt.offset:offsetEnd])
		assert.Equal(t, tt.expected, actual)
	}
}

func TestGetBytes(t *testing.T) {
	var data []byte
	initial := int64(0)
	bytes := [][]byte{[]byte{42}, []byte{42, 42}, []byte{42, 42, 42}}
	offsets := []int64{
		initial,
		initial + IntSize + int64(len(bytes[0])),
		initial + 2*IntSize + int64(len(bytes[0])+len(bytes[1])),
	}
	var tests = []struct {
		offset   int64
		expected []byte
	}{
		{offsets[0], bytes[0]},
		{offsets[1], bytes[1]},
		{offsets[2], bytes[2]},
	}

	for _, b := range bytes {
		data = append(data, Int64ToBytes(int64(len(b)))...)
		data = append(data, b...)
	}
	page := NewPageWithBytes(data)
	for _, tt := range tests {
		actual, err := page.GetBytes(tt.offset)
		assert.NoError(t, err)
		assert.Equal(t, tt.expected, actual)
	}
}

func TestSetBytes(t *testing.T) {
	bytes := [][]byte{[]byte{42}, []byte{42, 42}, []byte{42, 42, 42}}
	initial := int64(0)
	offsets := []int64{
		initial,
		initial + IntSize + int64(len(bytes[0])),
		initial + 2*IntSize + int64(len(bytes[0])+len(bytes[1])),
	}
	var tests = []struct {
		offset   int64
		expected []byte
	}{
		{offsets[0], bytes[0]},
		{offsets[1], bytes[1]},
		{offsets[2], bytes[2]},
	}

	lastIndex := len(bytes) - 1
	dataSize := offsets[lastIndex] + IntSize + int64(len(bytes[lastIndex]))
	page := NewPageWithSize(dataSize)
	for i := 0; i < len(bytes); i++ {
		page.SetBytes(offsets[i], bytes[i])
	}

	for _, tt := range tests {
		length, _ := BytesToInt64(page.Buffer[tt.offset : tt.offset+IntSize])
		assert.Equal(t, len(tt.expected), int(length))

		actual := page.Buffer[tt.offset+IntSize : tt.offset+IntSize+int64(int(length))]
		assert.Equal(t, tt.expected, actual)
	}
}

func TestSetAndGetString(t *testing.T) {
	data := []string{"a", "bc", "def"}
	initial := int64(0)
	offsets := []int64{
		initial,
		initial + IntSize + int64(len(data[0])),
		initial + 2*IntSize + int64(len(data[0])+len(data[1])),
	}

	lastIndex := len(data) - 1
	dataSize := offsets[lastIndex] + IntSize + int64(len(data[lastIndex]))
	page := NewPageWithSize(dataSize)
	for i := 0; i < len(data); i++ {
		page.SetString(offsets[i], data[i])
	}
	for i := 0; i < len(data); i++ {
		actual, _ := page.GetString(offsets[i])
		assert.Equal(t, data[i], actual)
	}
}
