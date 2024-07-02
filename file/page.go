package file

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const IntSize = 8

var ErrOutOfBounds = errors.New("offset out of bounds")

// Page is a struct to store data(Int, bytes, string) in memory (Page.Buffer)
// Int - each Int is stored as 8 bytes in memory
// +---------+
// | Int     |
// +---------+
// | 8 bytes |
// +---------+
//
// bytes- first we store len(bytes) as Int and then append actual bytes
// string - convert string to bytes and then store it as bytes as given above
// bytes and string are stored as given below
// +----------+----------------+
// | dataSize | data           |
// +----------+----------------+
// | 8 bytes  | dataSize bytes |
// +----------+----------------+
type Page struct {
	Buffer []byte
	Size   int64
}

func NewPageWithSize(size int64) *Page {
	bytes := make([]byte, size)
	return &Page{
		Buffer: bytes,
		Size:   size,
	}
}

func NewPageWithBytes(bytes []byte) *Page {
	return &Page{
		Buffer: bytes,
		Size:   int64(len(bytes)),
	}
}

func (p *Page) GetInt(offset int64) (int64, error) {
	offsetEnd := offset + IntSize
	if offsetEnd > p.Size {
		return 0, ErrOutOfBounds
	}
	val, err := BytesToInt64(p.Buffer[offset:offsetEnd])
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (p *Page) SetInt(offset int64, value int64) error {
	offsetEnd := offset + IntSize
	if offsetEnd > p.Size {
		return ErrOutOfBounds
	}

	bs := Int64ToBytes(value)
	copy(p.Buffer[offset:offsetEnd], bs)
	return nil
}

func (p *Page) GetBytes(offset int64) ([]byte, error) {
	length, err := p.GetInt(offset)
	if err != nil {
		return nil, err
	}
	offsetStart := offset + IntSize
	offsetEnd := offsetStart + length
	if offsetEnd > p.Size {
		return nil, ErrOutOfBounds
	}
	return p.Buffer[offsetStart:offsetEnd], nil
}

func (p *Page) SetBytes(offset int64, b []byte) error {
	offsetStart := offset + IntSize
	offsetEnd := offsetStart + int64(len(b))
	if offsetEnd > p.Size {
		return ErrOutOfBounds
	}

	err := p.SetInt(offset, int64(len(b)))
	if err != nil {
		return err
	}
	copy(p.Buffer[offsetStart:], b[:])
	return nil
}

func (p *Page) GetString(offset int64) (string, error) {
	bytes, err := p.GetBytes(offset)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (p *Page) SetString(offset int64, value string) error {
	return p.SetBytes(offset, []byte(value))
}

func MaxLen(strLen int) int64 {
	return IntSize + int64(strLen)
}

func Int64ToBytes(value int64) []byte {
	bs := make([]byte, IntSize)
	var a any = value
	switch v := a.(type) {
	case int64:
		binary.BigEndian.PutUint64(bs, uint64(v))
	}
	return bs
}

func BytesToInt64(b []byte) (int64, error) {
	var val int64
	err := binary.Read(bytes.NewReader(b), binary.BigEndian, &val)
	if err != nil {
		return 0, err
	}
	return val, nil
}
