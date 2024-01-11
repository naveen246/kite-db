package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompareWithSlash(t *testing.T) {
	assert.Equal(t, -1, compareWithSlash([]byte("aaa"), []byte("bbb")))
	assert.Equal(t, 0, compareWithSlash([]byte("aaa"), []byte("aaa")))
	assert.Equal(t, +1, compareWithSlash([]byte("bbb"), []byte("aaa")))

	assert.Equal(t, -1, compareWithSlash([]byte("aaa"), []byte("/aaa")))
	assert.Equal(t, +1, compareWithSlash([]byte("/aaa"), []byte("aaa")))

	assert.Equal(t, -1, compareWithSlash([]byte("aaa/bbb"), []byte("bbb/bbb")))
	assert.Equal(t, +1, compareWithSlash([]byte("bbb/bbb"), []byte("aaa/bbb")))

	assert.Equal(t, 0, compareWithSlash([]byte("aaa/bbb"), []byte("aaa/bbb")))
	assert.Equal(t, -1, compareWithSlash([]byte("aaa/bbb"), []byte("aaa/bbbb")))
	assert.Equal(t, +1, compareWithSlash([]byte("aaa/bbbb"), []byte("aaa/bbb")))

	assert.Equal(t, +1, compareWithSlash([]byte("/a/b/a/a/a"), []byte("/a/b/a/b")))
	assert.Equal(t, +1, compareWithSlash([]byte("aaaaa"), []byte("")))
	assert.Equal(t, -1, compareWithSlash([]byte(""), []byte("aaaaaa")))
	assert.Equal(t, 0, compareWithSlash([]byte(""), []byte("")))
}
