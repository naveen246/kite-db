package txn

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/wal"
	"log"
	"slices"
)

type RecoveryMgr struct {
	log     *wal.Log
	bufPool *buffer.BufferPool
	tx      *Transaction
	txNum   TxID
}

func NewRecoveryMgr(tx *Transaction, txNum TxID, log *wal.Log, bufPool *buffer.BufferPool) *RecoveryMgr {
	writeStartRecToLog(log, txNum)
	return &RecoveryMgr{log, bufPool, tx, txNum}
}

func (r *RecoveryMgr) commit() {
	r.bufPool.FlushAll(int64(r.txNum))
	lsn := writeCommitRecToLog(r.log, r.txNum)
	r.log.Flush(lsn)
}

func (r *RecoveryMgr) rollback() {
	iter := r.log.Iterator()
	for iter.HasNext() {
		record := createLogRecord(iter.Next())
		if record.txNumber() == r.txNum {
			if record.recordType() == Start {
				return
			}
			record.undo(r.tx)
		}
	}

	r.bufPool.FlushAll(int64(r.txNum))
	lsn := writeRollbackRecToLog(r.log, r.txNum)
	r.log.Flush(lsn)
}

func (r *RecoveryMgr) recover() {
	iter := r.log.Iterator()
	finishedTxs := make([]TxID, 0)
	for iter.HasNext() {
		record := createLogRecord(iter.Next())
		switch record.recordType() {
		case CheckPoint:
			return
		case Commit, Rollback:
			finishedTxs = append(finishedTxs, record.txNumber())
		default:
			if !slices.Contains(finishedTxs, record.txNumber()) {
				record.undo(r.tx)
			}
		}
	}

	r.bufPool.FlushAll(int64(r.txNum))
	lsn := writeCheckPointToLog(r.log)
	r.log.Flush(lsn)
}

func (r *RecoveryMgr) setInt(buf *buffer.Buffer, offset int) int {
	oldVal, err := buf.Contents.GetInt(int64(offset))
	if err != nil {
		log.Fatalln("Failed to write setInt record to log:", err)
	}

	return writeSetIntRecToLog(r.log, r.txNum, buf.Block, offset, int(oldVal))
}

func (r *RecoveryMgr) setString(buf *buffer.Buffer, offset int) int {
	oldVal, err := buf.Contents.GetString(int64(offset))
	if err != nil {
		log.Fatalln("Failed to write setString record to log:", err)
	}

	return writeSetStringRecToLog(r.log, r.txNum, buf.Block, offset, oldVal)
}
