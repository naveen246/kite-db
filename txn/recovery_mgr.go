package txn

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/wal"
)

type RecoveryMgr struct {
	log     *wal.Log
	bufPool *buffer.BufferPool
	tx      Transaction
}

func NewRecoveryMgr(log *wal.Log) *RecoveryMgr {
	return &RecoveryMgr{}
}
