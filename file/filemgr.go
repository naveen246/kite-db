package file

import (
	"os"
	"path/filepath"
)

// Files are conceptually divided into blocks of equal blockSize.
// Each block in a file starts at offset - (Block.Number * FileMgr.BlockSize)

type FileMgr struct {
	DBDir     string
	BlockSize int
	IsNew     bool
}

func NewFileMgr(dbDir string, blockSize int) *FileMgr {
	fileMgr := &FileMgr{
		DBDir:     dbDir,
		BlockSize: blockSize,
		IsNew:     !pathExists(dbDir),
	}

	if fileMgr.IsNew {
		err := os.Mkdir(dbDir, 0755)
		if err != nil {
			panic("Could not create DB directory")
		}
	}

	// TODO
	return fileMgr
}

// Read a block from file to Page(memory)
func (f *FileMgr) Read(block *Block, page *Page) error {
	path := filepath.Join(f.DBDir, block.Filename)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.ReadAt(page.Buffer, int64(block.Number*f.BlockSize))
	if err != nil {
		return err
	}
	return nil
}

// Write a Page(memory) to a block in file
func (f *FileMgr) Write(block *Block, page *Page) error {
	path := filepath.Join(f.DBDir, block.Filename)
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(page.Buffer, int64(block.Number*f.BlockSize))
	if err != nil {
		return err
	}
	return nil
}

func (f *FileMgr) Append(filename string) {
	// TODO
}

func (f *FileMgr) BlockCount(filename string) int {
	path := filepath.Join(f.DBDir, filename)
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return int(fileInfo.Size() / int64(f.BlockSize))
}

func pathExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	}
	return false
}
