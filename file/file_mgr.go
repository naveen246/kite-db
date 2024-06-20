package file

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// Files are conceptually divided into blocks of equal blockSize.
// Each block in a file starts at offset - (Block.Number * FileMgr.BlockSize)

const dirPermission = 0777
const filePermission = 0666

// FileMgr handles Read from file Block to memory(Page)
// and Write from memory(Page) to a file Block
type FileMgr struct {
	DbDir     string
	BlockSize int64
	IsNew     bool
}

func NewFileMgr(dbDir string, blockSize int64) FileMgr {
	fileMgr := FileMgr{
		DbDir:     dbDir,
		BlockSize: blockSize,
		IsNew:     !pathExists(dbDir),
	}

	if fileMgr.IsNew {
		err := os.Mkdir(dbDir, dirPermission)
		if err != nil {
			panic("Could not create DB directory")
		}
	}

	// TODO
	return fileMgr
}

// Read a block from file to Page(memory)
func (f *FileMgr) Read(block Block, page *Page) error {
	file, err := os.Open(f.DbFilePath(block.Filename))
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.ReadAt(page.Buffer, block.Number*f.BlockSize)
	if err != nil {
		return fmt.Errorf("could not read block %v, %v", block, err)
	}
	return nil
}

// Write a Page(memory) to a block in file
func (f *FileMgr) Write(block Block, page *Page) error {
	path := f.DbFilePath(block.Filename)
	file, err := os.OpenFile(path, os.O_RDWR, filePermission)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(page.Buffer, int64(block.Number*f.BlockSize))
	if err != nil {
		return fmt.Errorf("could not write to block %v, %v", block, err)
	}
	// TODO file.Sync() ??
	return nil
}

// Append empty bytes of size f.BlockSize to file
// and create a new block that corresponds to the bytes appended to file
func (f *FileMgr) Append(filename string) (Block, error) {
	newBlockNum := f.BlockCount(filename)
	block := GetBlock(filename, newBlockNum)
	b := bytes.Repeat([]byte{byte(0)}, int(f.BlockSize))

	file, err := os.OpenFile(f.DbFilePath(filename), os.O_APPEND|os.O_WRONLY|os.O_CREATE, filePermission)
	if err != nil {
		return Block{}, err
	}
	defer file.Close()

	_, err = file.Write(b)
	if err != nil {
		return Block{}, err
	}

	return block, nil
}

func (f *FileMgr) BlockCount(filename string) int64 {
	path := f.DbFilePath(filename)

	fileInfo, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, err := os.Create(path); err != nil {
			log.Fatalf("Failed to create file %v, %v\n", path, err)
		}
		return 0
	}
	if err != nil {
		log.Fatalf("Failed to get BlockCount for %v, %v\n", filename, err)
	}
	return fileInfo.Size() / f.BlockSize
}

func (f *FileMgr) DbFilePath(filename string) string {
	return filepath.Join(f.DbDir, filename)
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}
