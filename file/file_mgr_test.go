package file

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path/filepath"
	"testing"
)

const blockTestSize = 100

var tempFileName = "temp_file"

// setup creates file temp_dir/filename
// and populates file with 100 bytes each of a, b, c
func setup(filename string) (*os.File, *FileMgr) {
	fileMgr := NewFileMgr("temp_dir", blockTestSize)
	path := filepath.Join(fileMgr.DBDir, filename)
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	chars := []byte("abc")
	for _, c := range chars {
		file.Write(bytes.Repeat([]byte{c}, blockTestSize))
	}
	return file, fileMgr
}

func teardown(file *os.File, fileMgr *FileMgr) {
	file.Close()
	os.Remove(file.Name())
	os.Remove(fileMgr.DBDir)
}

func TestFileMgrRead(t *testing.T) {
	file, fileMgr := setup(tempFileName)
	defer teardown(file, fileMgr)

	tests := []struct {
		blockNum int
		char     string
	}{
		{0, "a"},
		{1, "b"},
		{2, "c"},
	}

	// verify that 1st file block has 100 bytes of "a", 2nd file block has 100 bytes of "b" and so on
	page := NewPageWithSize(blockTestSize)
	for _, tt := range tests {
		block := NewBlock(tempFileName, tt.blockNum)
		fileMgr.Read(block, page)

		expected := bytes.Repeat([]byte(tt.char), blockTestSize)
		assert.Equal(t, string(expected), string(page.Buffer))
	}

}

func TestFileMgrWrite(t *testing.T) {
	file, fileMgr := setup(tempFileName)
	defer teardown(file, fileMgr)

	// intially 2nd file block has 100 bytes of "b", Overwrite with 100 bytes of "o" and verify if its changed
	block := NewBlock(tempFileName, 1)
	expected := bytes.Repeat([]byte("o"), blockTestSize)
	page := NewPageWithBytes(expected)
	fileMgr.Write(block, page)

	actual := make([]byte, 100)
	file.ReadAt(actual, 1*blockTestSize)
	assert.Equal(t, string(expected), string(actual))
}

func TestBlockCount(t *testing.T) {
	file, fileMgr := setup(tempFileName)
	defer teardown(file, fileMgr)

	assert.Equal(t, 3, fileMgr.BlockCount(tempFileName))
}
