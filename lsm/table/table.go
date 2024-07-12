package table

import (
	"encoding/binary"
	"fmt"
	"github.com/AndreasBriese/bbloom"
	"github.com/edsrzf/mmap-go"
	"github.com/naveen246/kite-db/lsm/common"
	"github.com/naveen246/kite-db/lsm/iterator"
	"github.com/naveen246/kite-db/lsm/utils"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const sstFileSuffix = ".sst"

type keyInfo struct {
	key    []byte
	offset int
	length int
}

type block struct {
	offset int
	data   []byte
}

func (b block) NewIterator() *iterator.BlockIterator {
	return &iterator.BlockIterator{
		Data: b.data,
	}
}

type Table struct {
	sync.Mutex

	file     *os.File
	fileID   uint64
	refCount atomic.Int32

	TableSize   int
	blockIndex  []keyInfo
	loadingMode common.FileLoadingMode
	mmap        mmap.MMap

	SmallestKey []byte
	LargestKey  []byte

	bloom bbloom.Bloom
}

func Open(file *os.File, loadingMode common.FileLoadingMode) (*Table, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	fileID, ok := ParseFileID(fileInfo.Name())
	if !ok {
		file.Close()
		return nil, fmt.Errorf("invalid filename, %v", err)
	}

	t := &Table{}
	t.file = file
	t.fileID = fileID
	t.refCount.Store(1)
	t.loadingMode = loadingMode
	t.TableSize = int(fileInfo.Size())

	err = t.updateMmap(file, loadingMode)
	if err != nil {
		return nil, err
	}

}

func (t *Table) Close() error {
	if t.loadingMode == common.MemoryMap {
		t.mmap.Unmap()
	}
	return t.file.Close()
}

func (t *Table) read(offset int, size int) ([]byte, error) {
	if len(t.mmap) > 0 {
		if len(t.mmap[offset:]) < size {
			return nil, io.EOF
		}
		return t.mmap[offset : offset+size], nil
	}

	bytes := make([]byte, size)
	_, err := t.file.ReadAt(bytes, int64(offset))
	return bytes, err
}

func (t *Table) readNoFail(offset int, size int) []byte {
	bytes, err := t.read(offset, size)
	utils.Check(err)
	return bytes
}

func (t *Table) readIndex() error {
	readPos := t.TableSize

	readPos -= 4
	buf := t.readNoFail(readPos, 4)
	bloomLen := int(binary.BigEndian.Uint32(buf))

	readPos -= bloomLen
	data := t.readNoFail(readPos, bloomLen)
	t.bloom = bbloom.JSONUnmarshal(data)

	readPos -= 4
	buf = t.readNoFail(readPos, 4)
	restartsLen := int(binary.BigEndian.Uint32(buf))

	readPos -= 4 * restartsLen
	buf = t.readNoFail(readPos, 4*restartsLen)
}

func (t *Table) block(idx int) (block, error) {

}

func (t *Table) IncRefCount() {
	t.refCount.Add(1)
}

func (t *Table) DecRefCount() error {
	newRef := t.refCount.Add(-1)
	if newRef > 0 {
		return nil
	}

	if t.loadingMode == common.MemoryMap {
		t.mmap.Unmap()
	}
	filename := t.file.Name()

	err := t.file.Truncate(0)
	if err != nil {
		return err
	}

	err = t.file.Close()
	if err != nil {
		return err
	}

	err = os.Remove(filename)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) Filename() string { return t.file.Name() }

// ID is the table's ID number (used to make the file name).
func (t *Table) ID() uint64 { return t.fileID }

func (t *Table) updateMmap(file *os.File, loadingMode common.FileLoadingMode) error {
	if loadingMode == common.MemoryMap {
		mMap, err := mmap.Map(file, mmap.RDWR, 0)
		if err != nil {
			file.Close()
			return fmt.Errorf("unable to map file %v", err)
		}
		t.mmap = mMap
	} else if loadingMode == common.LoadToMemory {
		t.mmap = make([]byte, t.TableSize)

		read, err := t.file.ReadAt(t.mmap, 0)
		if err != nil || read != t.TableSize {
			file.Close()
			return fmt.Errorf("failed to load file to memory, %v", err)
		}
	}
	return nil
}

type Header struct {
	pointerLen uint16
	keyLen     uint16
	valueLen   uint16
	prev       uint32
}

// ParseFileID returns fileID from filename after removing extension ".sst"
func ParseFileID(name string) (uint64, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, sstFileSuffix) {
		return 0, false
	}

	name = strings.TrimSuffix(name, sstFileSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}

	utils.AssertTrue(id >= 0)
	return uint64(id), true
}
