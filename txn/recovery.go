package txn

import (
	"github.com/naveen246/kite-db/buffer"
	"github.com/naveen246/kite-db/wal"
	"log"
)

// RecoveryMgr Each transaction has its own recovery manager
type RecoveryMgr struct {
	log     *wal.Log
	bufPool *buffer.BufferPool
	tx      *Transaction
	txNum   TxID
}

func NewRecoveryMgr(tx *Transaction, txNum TxID, log *wal.Log, bufPool *buffer.BufferPool) *RecoveryMgr {
	WriteStartRecToLog(log, txNum)
	return &RecoveryMgr{log, bufPool, tx, txNum}
}

// commit Write a commit record to the log, and flush it to disk.
func (r *RecoveryMgr) commit() {
	r.bufPool.FlushAll(int64(r.txNum))
	lsn := WriteCommitRecToLog(r.log, r.txNum)
	r.log.Flush(lsn)
}

// rollback Write a rollback record to the log and flush it to disk.
// rollback the transaction, by iterating through the log records until it finds the transaction's Start record,
// calling undo() for each of the transaction's log records.
func (r *RecoveryMgr) rollback() error {
	iter := r.log.Iterator()
	for iter.HasNext() {
		record := createLogRecord(iter.Next())
		if record.txNumber() == r.txNum {
			if record.recordType() == Start {
				break
			}
			err := record.undo(r.tx)
			if err != nil {
				return err
			}
		}
	}

	r.bufPool.FlushAll(int64(r.txNum))
	lsn := WriteRollbackRecToLog(r.log, r.txNum)
	r.log.Flush(lsn)

	return nil
}

// recover uncompleted(neither commit nor rollback) transactions from the log
// and then write a checkpoint record to the log and flush it.
// The method iterates through the log records.
// Whenever it finds a log record for an unfinished transaction, it calls undo() on that record.
// The method stops when it encounters a CheckPoint record or the end of the log.
func (r *RecoveryMgr) recover() error {
	iter := r.log.Iterator()
	finishedTxs := make(map[TxID]bool)
	for iter.HasNext() {
		record := createLogRecord(iter.Next())
		switch record.recordType() {
		case CheckPoint:
			break
		case Commit, Rollback:
			finishedTxs[record.txNumber()] = true
		default:
			if _, ok := finishedTxs[record.txNumber()]; !ok {
				err := record.undo(r.tx)
				if err != nil {
					return err
				}
			}
		}
	}

	r.bufPool.FlushAll(int64(r.txNum))
	lsn := WriteCheckPointToLog(r.log)
	r.log.Flush(lsn)

	return nil
}

// setInt Write a setInt record to the log and return its lsn
func (r *RecoveryMgr) setInt(buf *buffer.Buffer, offset int64) int64 {
	oldVal, err := buf.Contents.GetInt(offset)
	if err != nil {
		log.Fatalln("Failed to write setInt record to log:", err)
	}

	return writeSetIntRecToLog(r.log, r.txNum, buf.Block, offset, int(oldVal))
}

// setString Write a setString record to the log and return its lsn
func (r *RecoveryMgr) setString(buf *buffer.Buffer, offset int64) int64 {
	oldVal, err := buf.Contents.GetString(offset)
	if err != nil {
		log.Fatalln("Failed to write setString record to log:", err)
	}

	return writeSetStringRecToLog(r.log, r.txNum, buf.Block, offset, oldVal)
}
