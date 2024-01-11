package kv

import "io"

type Factory interface {
	io.Closer

	NewKV(namespace string, shardId int64) (KV, error)

	NewSnapshotLoader(namespace string, shardId int64) (SnapshotLoader, error)
}

type SnapshotLoader interface {
	io.Closer

	AddChunk(filename string, chunkIndex int32, chunkCount int32, content []byte) error

	// Complete signals that the snapshot is now complete
	Complete()
}

type KV interface {
	io.Closer

	NewWriteBatch() WriteBatch

	Get(key string) ([]byte, io.Closer, error)

	KeyRangeScan(lowerBound, upperBound string) (KeyIterator, error)
	KeyRangeScanReverse(lowerBound, upperBound string) (ReverseKeyIterator, error)

	RangeScan(lowerBound, upperBound string) (KeyValueIterator, error)

	Snapshot() (Snapshot, error)

	Flush() error

	Delete() error
}

type WriteBatch interface {
	io.Closer

	Put(key string, value []byte) error
	Delete(key string) error
	Get(key string) ([]byte, io.Closer, error)

	DeleteRange(lowerBound, upperBound string) error
	KeyRangeScan(lowerBound, upperBound string) (KeyIterator, error)

	// Count is the number of transactions that are currently in the batch
	Count() int

	// Size of all the transactions that are currently in the batch
	Size() int

	Commit() error
}

type KeyIterator interface {
	io.Closer

	Valid() bool
	Key() string
	Next() bool
}

type ReverseKeyIterator interface {
	io.Closer

	Valid() bool
	Key() string
	Prev() bool
}

type KeyValueIterator interface {
	KeyIterator

	Value() ([]byte, error)
}

type Snapshot interface {
	io.Closer

	BasePath() string

	Valid() bool
	Chunk() (SnapshotChunk, error)
	Next() bool
}

type SnapshotChunk interface {
	Name() string
	Index() int32
	TotalCount() int32
	Content() []byte
}

type FactoryOptions struct {
	DataDir     string
	CacheSizeMB int64

	// Create a pure in-memory database. Used for unit-tests
	InMemory bool
}

var DefaultFactoryOptions = &FactoryOptions{
	DataDir:     "data",
	CacheSizeMB: 100,
	InMemory:    false,
}
