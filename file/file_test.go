package file

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

const blockTestSize = 100

var tempFileName = "temp_file"

// createFile creates file temp_dir/filename
// and populates file with 100 bytes each of a, b, c
func createFile(filename string) (*os.File, *FileMgr) {
	fileMgr := NewFileMgr("temp_dir", blockTestSize)
	file, err := os.Create(fileMgr.DbFilePath(filename))
	if err != nil {
		log.Fatal(err)
	}
	chars := []byte("abc")
	for _, c := range chars {
		file.Write(bytes.Repeat([]byte{c}, blockTestSize))
	}
	return file, fileMgr
}

func removeFile(filename string, dbDir string) {
	os.Remove(filename)
	os.Remove(dbDir)
}

func TestFileRead(t *testing.T) {
	file, fileMgr := createFile(tempFileName)
	defer removeFile(file.Name(), fileMgr.DbDir)

	tests := []struct {
		blockNum int64
		char     string
	}{
		{0, "a"},
		{1, "b"},
		{2, "c"},
	}

	// verify that 1st file block has 100 bytes of "a", 2nd file block has 100 bytes of "b" and so on
	page := NewPageWithSize(blockTestSize)
	for _, tt := range tests {
		block := GetBlock(tempFileName, tt.blockNum)
		fileMgr.Read(block, page)

		expected := bytes.Repeat([]byte(tt.char), blockTestSize)
		assert.Equal(t, string(expected), string(page.Buffer))
	}

}

func TestFileWrite(t *testing.T) {
	file, fileMgr := createFile(tempFileName)
	defer removeFile(file.Name(), fileMgr.DbDir)

	// intially 2nd file block has 100 bytes of "b", Overwrite with 100 bytes of "o" and verify if its changed
	block := GetBlock(tempFileName, 1)
	expected := bytes.Repeat([]byte("o"), blockTestSize)
	page := NewPageWithBytes(expected)
	fileMgr.Write(block, page)

	actual := make([]byte, 100)
	file.ReadAt(actual, 1*blockTestSize)
	assert.Equal(t, string(expected), string(actual))
}

func TestFileAppend(t *testing.T) {
	file, fileMgr := createFile(tempFileName)
	defer removeFile(file.Name(), fileMgr.DbDir)

	initialBlockCount := fileMgr.BlockCount(tempFileName)
	fileMgr.Append(tempFileName)
	expectedBlockCount := initialBlockCount + 1
	actualBlockCount := fileMgr.BlockCount(tempFileName)
	assert.Equal(t, expectedBlockCount, actualBlockCount)
}

func TestBlockCount(t *testing.T) {
	file, fileMgr := createFile(tempFileName)
	defer removeFile(file.Name(), fileMgr.DbDir)

	assert.Equal(t, int64(3), fileMgr.BlockCount(tempFileName))

	newTempFile := "new_temp_file"
	assert.Equal(t, int64(0), fileMgr.BlockCount(newTempFile))
	os.Remove(fileMgr.DbFilePath(newTempFile))
}
