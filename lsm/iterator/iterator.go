package iterator

import (
	"github.com/naveen246/kite-db/lsm/common"
	"github.com/naveen246/kite-db/lsm/table"
)

type BlockIterator struct {
	Data    []byte
	pos     uint32
	err     error
	baseKey []byte

	kv   common.KV
	init bool

	last table.Header
}
