package file

import (
	"encoding/binary"
	"errors"
)

const IntSize = 4

var ErrOutOfBounds = errors.New("offset out of bounds")

// Page is a struct to store data(Int, bytes, string) in memory (Page.Buffer)
// Int - each Int is stored as 4 bytes in memory
// +---------+
// | Int     |
// +---------+
// | 4 bytes |
// +---------+
//
// bytes- first we store len(bytes) as Int and then append actual bytes
// string - convert string to bytes and then store it as bytes as given above
// bytes and string are stored as given below
// +----------+----------------+
// | dataSize | data           |
// +----------+----------------+
// | 4 bytes  | dataSize bytes |
// +----------+----------------+
type Page struct {
	Buffer []byte
	Size   uint32
}

func NewPageWithSize(size uint32) *Page {
	bytes := make([]byte, size)
	return &Page{
		Buffer: bytes,
		Size:   size,
	}
}

func NewPageWithBytes(bytes []byte) *Page {
	return &Page{
		Buffer: bytes,
		Size:   uint32(len(bytes)),
	}
}

func (p *Page) GetInt(offset uint32) (uint32, error) {
	offsetEnd := offset + IntSize
	if offsetEnd > p.Size {
		return 0, ErrOutOfBounds
	}
	return binary.BigEndian.Uint32(p.Buffer[offset:offsetEnd]), nil
}

func (p *Page) SetInt(offset uint32, value uint32) error {
	offsetEnd := offset + IntSize
	if offsetEnd > p.Size {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(p.Buffer[offset:offsetEnd], value)
	return nil
}

func (p *Page) GetBytes(offset uint32) ([]byte, error) {
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

func (p *Page) SetBytes(offset uint32, b []byte) error {
	offsetStart := offset + IntSize
	offsetEnd := offsetStart + uint32(len(b))
	if offsetEnd > p.Size {
		return ErrOutOfBounds
	}
	err := p.SetInt(offset, uint32(len(b)))
	if err != nil {
		return err
	}
	copy(p.Buffer[offsetStart:], b[:])
	return nil
}

func (p *Page) GetString(offset uint32) (string, error) {
	bytes, err := p.GetBytes(offset)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (p *Page) SetString(offset uint32, value string) error {
	return p.SetBytes(offset, []byte(value))
}
