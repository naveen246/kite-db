package file

import (
	"encoding/binary"
	"errors"
)

const IntSize = 4

var ErrOutOfBounds = errors.New("offset out of bounds")

// Page is a struct to store data(Int, bytes, string) in memory (Page.Buffer)
// Int - each Int is stored as 4 bytes in memory
// bytes- first we store len(bytes) as Int and then append actual bytes
// string - convert string to bytes and then store it as bytes as given above
type Page struct {
	Buffer []byte
	size   int
}

func NewPageWithSize(size int) *Page {
	bytes := make([]byte, size)
	return &Page{
		Buffer: bytes,
		size:   size,
	}
}

func NewPageWithBytes(bytes []byte) *Page {
	return &Page{
		Buffer: bytes,
		size:   len(bytes),
	}
}

func (p *Page) GetInt(offset int) (uint32, error) {
	offsetEnd := offset + IntSize
	if offsetEnd > p.size || offset < 0 {
		return 0, ErrOutOfBounds
	}
	return binary.BigEndian.Uint32(p.Buffer[offset:offsetEnd]), nil
}

func (p *Page) SetInt(offset int, value uint32) error {
	offsetEnd := offset + IntSize
	if offsetEnd > p.size || offset < 0 {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(p.Buffer[offset:offsetEnd], value)
	return nil
}

func (p *Page) GetBytes(offset int) ([]byte, error) {
	length, err := p.GetInt(offset)
	if err != nil {
		return nil, err
	}
	offsetStart := offset + IntSize
	offsetEnd := offsetStart + int(length)
	if offsetEnd > p.size {
		return nil, ErrOutOfBounds
	}
	return p.Buffer[offsetStart:offsetEnd], nil
}

func (p *Page) SetBytes(offset int, b []byte) error {
	offsetStart := offset + IntSize
	offsetEnd := offsetStart + len(b)
	if offsetEnd > p.size || offset < 0 {
		return ErrOutOfBounds
	}
	err := p.SetInt(offset, uint32(len(b)))
	if err != nil {
		return err
	}
	copy(p.Buffer[offsetStart:], b[:])
	return nil
}

func (p *Page) GetString(offset int) (string, error) {
	bytes, err := p.GetBytes(offset)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (p *Page) SetString(offset int, value string) error {
	return p.SetBytes(offset, []byte(value))
}
