package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/emirpasic/gods/v2/maps/treemap"
	"io"
	"log"
	"os"
	"path/filepath"
)

type ValueMeta struct {
	position uint64
	length   uint32
}

type KeyDir *treemap.Map[string, ValueMeta]

type BitCask struct {
	// The active append-only log file
	log Log
	// Maps keys to a value position and length in the log file
	keyDir KeyDir
}

// Log A BitCask append-only log file, containing a sequence of key/value
// entries encoded as follows;
//
// - Key length as big-endian uint32.
// - Value length as big-endian int32, or -1 for tombstones.
// - Key as raw bytes.
// - Value as raw bytes.
type Log struct {
	// Path to Log file
	path string
	// Log file
	file *os.File
}

func NewLog(path string) *Log {
	if !exists(filepath.Dir(path)) {
		err := os.Mkdir(filepath.Dir(path), 0755)
		if err != nil {
			log.Fatalf("Failed to create directory for Log file %v", err)
		}
	}

	filename := filepath.Base(path)
	// If the file doesn't exist, create it, or append to the file
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Fatalf("Failed to open Log file %v", err)
	}

	return &Log{path: path, file: f}
}

// BuildKeyDir Builds a KeyDir by scanning the log file
func (l *Log) BuildKeyDir() KeyDir {
	keyDir := treemap.New[string, ValueMeta]()

	fi, err := l.file.Stat()
	if err != nil {
		log.Fatalf("Failed to stat Log file %v", err)
	}
	fileLen := fi.Size()

	_, err = l.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil
	}

	var pos uint64

	// Read file till the end and populate keyDir
	for pos < uint64(fileLen) {
		lenBuf := make([]byte, 8)
		_, err := l.file.ReadAt(lenBuf, int64(pos))
		if err != nil {
			break
		}

		keyLen := binary.BigEndian.Uint32(lenBuf[0:4])
		var valueLen int32
		err = binary.Read(bytes.NewBuffer(lenBuf[4:8]), binary.BigEndian, &valueLen)
		if err != nil {
			break
		}

		keyPos := pos + 8
		valuePos := keyPos + uint64(keyLen)
		if int64(valuePos)+int64(valueLen) > fileLen {
			// Encountered incomplete entry. Truncate file here and break
			err := l.file.Truncate(int64(pos))
			if err != nil {
				log.Printf("Failed to truncate Log file %v", err)
			}
			break
		}

		key := make([]byte, keyLen)
		_, err = l.file.ReadAt(key, int64(keyPos))
		if err != nil {
			log.Fatalf("Failed to read key %v", err)
		}

		if valueLen == -1 {
			// This indicates a tombstone so remove key from keyDir
			keyDir.Remove(string(key))
			pos = valuePos
		} else {
			keyDir.Put(string(key), ValueMeta{
				position: valuePos,
				length:   uint32(valueLen),
			})
			pos = valuePos + uint64(valueLen)
		}
	}

	return keyDir
}

// readValue from the log file
func (l *Log) readValue(valuePos uint64, valueLen uint32) []byte {
	value := make([]byte, valueLen)
	_, err := l.file.ReadAt(value, int64(valuePos))
	if err != nil {
		log.Fatalf("Failed to read value %v", err)
	}
	return value
}

func (l *Log) writeEntry(key []byte, value []byte) {
	keyLen := uint32(len(key))
	totalLen := 4 + 4 + keyLen
	var valueLen int32
	if value == nil {
		valueLen = -1
	} else {
		valueLen = int32(len(value))
		totalLen += uint32(valueLen)
	}
}

// exists returns whether the given file or directory exists
func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}
