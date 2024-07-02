package txn

import (
	"fmt"
	"github.com/naveen246/kite-db/file"
	"github.com/naveen246/kite-db/wal"
	log2 "log"
)

const (
	CheckPoint = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

// LogRecord The interface implemented by each type of log record
type LogRecord interface {
	recordType() int
	txNumber() TxID
	undo(tx *Transaction) error
}

func createLogRecord(bytes []byte) LogRecord {
	page := file.NewPageWithBytes(bytes)
	recordType, err := page.GetInt(0)
	if err != nil {
		log2.Fatalln("Failed to create log record: ", err)
	}
	switch recordType {
	case CheckPoint:
		return newCheckpointRecord()
	case Start:
		return newStartRecord(page)
	case Commit:
		return newCommitRecord(page)
	case Rollback:
		return newRollbackRecord(page)
	case SetInt:
		return newSetIntRecord(page)
	case SetString:
		return newSetStringRecord(page)
	}
	return nil
}

/*************** CheckpointRecord ********************************************/

// CheckpointRecord in log ->
// <CheckPoint>
type CheckpointRecord struct {
}

func newCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (c *CheckpointRecord) recordType() int {
	return CheckPoint
}

// Checkpoint records have no associated transaction,
// and so the method returns a "dummy", negative txID.
func (c *CheckpointRecord) txNumber() TxID {
	return -1
}

// Does nothing, because a checkpoint record contains no undo information.
func (c *CheckpointRecord) undo(tx *Transaction) error {
	return nil
}

func (c *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

// WriteCheckPointToLog write a CheckPoint record to the log.
// This log record contains the CheckPoint operator, and nothing else.
// returns lsn of the appended CheckPoint record
func WriteCheckPointToLog(log *wal.Log) int64 {
	record := make([]byte, file.IntSize)
	page := file.NewPageWithBytes(record)
	err := page.SetInt(0, CheckPoint)
	if err != nil {
		log2.Fatalln("Failed to write CheckPoint record to Log: ", err)
	}
	return log.Append(record)
}

/*************** StartRecord *************************************************/

// StartRecord in log ->
// <Start, TxID>
type StartRecord struct {
	txNum TxID
}

func newStartRecord(page *file.Page) *StartRecord {
	txNumber, err := page.GetInt(file.IntSize)
	if err != nil {
		log2.Fatalln("Failed to create start record: ", err)
	}
	return &StartRecord{
		txNum: TxID(txNumber),
	}
}

func (s *StartRecord) recordType() int {
	return Start
}

func (s *StartRecord) txNumber() TxID {
	return s.txNum
}

// Does nothing, because a start record contains no undo information.
func (s *StartRecord) undo(tx *Transaction) error {
	return nil
}

func (s *StartRecord) String() string {
	return fmt.Sprintf("<START %v>", s.txNum)
}

// WriteStartRecToLog write a Start record to the log.
// This log record contains the Start operator, followed by the transaction id.
// returns lsn of the appended Start record
func WriteStartRecToLog(log *wal.Log, txNum TxID) int64 {
	record := make([]byte, 2*file.IntSize)
	page := file.NewPageWithBytes(record)

	err := page.SetInt(0, Start)
	if err != nil {
		log2.Fatalln("Failed to write Start record to Log: ", err)
	}

	err = page.SetInt(file.IntSize, int64(txNum))
	if err != nil {
		log2.Fatalln("Failed to write Start record to Log: ", err)
	}
	return log.Append(record)
}

/*************** CommitRecord ************************************************/

// CommitRecord in log ->
// <Commit, TxID>
type CommitRecord struct {
	txNum TxID
}

func newCommitRecord(page *file.Page) *CommitRecord {
	txNumber, err := page.GetInt(file.IntSize)
	if err != nil {
		log2.Fatalln("Failed to create commit record: ", err)
	}
	return &CommitRecord{
		txNum: TxID(txNumber),
	}
}

func (c *CommitRecord) recordType() int {
	return Commit
}

func (c *CommitRecord) txNumber() TxID {
	return c.txNum
}

// Does nothing, because a commit record contains no undo information.
func (c *CommitRecord) undo(tx *Transaction) error {
	return nil
}

func (c *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %v>", c.txNum)
}

// WriteCommitRecToLog write a Commit record to the log.
// This log record contains the Commit operator, followed by the transaction id.
// returns lsn of the appended Commit record
func WriteCommitRecToLog(log *wal.Log, txNum TxID) int64 {
	record := make([]byte, 2*file.IntSize)
	page := file.NewPageWithBytes(record)

	err := page.SetInt(0, Commit)
	if err != nil {
		log2.Fatalln("Failed to write Commit record to Log: ", err)
	}

	err = page.SetInt(file.IntSize, int64(txNum))
	if err != nil {
		log2.Fatalln("Failed to write Commit record to Log: ", err)
	}
	return log.Append(record)
}

/*************** RollbackRecord **********************************************/

// RollbackRecord in log ->
// <Rollback, TxID>
type RollbackRecord struct {
	txNum TxID
}

func newRollbackRecord(page *file.Page) *RollbackRecord {
	txNumber, err := page.GetInt(file.IntSize)
	if err != nil {
		log2.Fatalln("Failed to create Rollback record: ", err)
	}
	return &RollbackRecord{
		txNum: TxID(txNumber),
	}
}

func (r *RollbackRecord) recordType() int {
	return Rollback
}

func (r *RollbackRecord) txNumber() TxID {
	return r.txNum
}

// Does nothing, because a rollback record contains no undo information.
func (r *RollbackRecord) undo(tx *Transaction) error {
	return nil
}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %v>", r.txNum)
}

// WriteRollbackRecToLog write a Rollback record to the log.
// This log record contains the Rollback operator, followed by the transaction id.
// returns lsn of the appended Rollback record
func WriteRollbackRecToLog(log *wal.Log, txNum TxID) int64 {
	errMsg := "Failed to write Rollback record to Log: "
	record := make([]byte, 2*file.IntSize)
	page := file.NewPageWithBytes(record)

	err := page.SetInt(0, Rollback)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	err = page.SetInt(file.IntSize, int64(txNum))
	if err != nil {
		log2.Fatalln(errMsg, err)
	}
	return log.Append(record)
}

/*************** SetIntRecord ************************************************/

// SetIntRecord in log ->
// <SetInt, TxID, filename, blockNumber, offset, value>
type SetIntRecord struct {
	txNum  TxID
	offset int64
	val    int
	block  file.Block
}

func newSetIntRecord(page *file.Page) *SetIntRecord {
	errMsg := "Failed to create SetInt record: "
	position := int64(file.IntSize)
	txNumber, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	filename, err := page.GetString(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.MaxLen(len(filename))
	blockNum, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	offset, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	val, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	return &SetIntRecord{
		txNum:  TxID(txNumber),
		offset: offset,
		val:    int(val),
		block:  file.GetBlock(filename, blockNum),
	}
}

func (s *SetIntRecord) recordType() int {
	return SetInt
}

func (s *SetIntRecord) txNumber() TxID {
	return s.txNum
}

// undo Replace the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block,
// calls setInt to restore the saved value, and unpins the buffer.
func (s *SetIntRecord) undo(tx *Transaction) error {
	tx.Pin(s.block)
	err := tx.SetInt(s.block, s.offset, s.val, false)
	if err != nil {
		return err
	}
	tx.Unpin(s.block)
	return nil
}

func (s *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %v %v %v %v>", s.txNum, s.block, s.offset, s.val)
}

// writeSetIntRecToLog write a SetInt record to the log.
// This log record contains the SetInt operator,
// followed by transaction id, filename, blockNumber,
// offset of the modified block, and the previous integer value at that offset.
// returns the LSN of the appended SetInt record
func writeSetIntRecToLog(log *wal.Log, txNum TxID, block file.Block, offset int64, val int) int64 {
	errMsg := "Failed to write SetInt record to Log: "
	filenameLen := file.MaxLen(len(block.Filename))
	record := make([]byte, 5*file.IntSize+filenameLen)
	page := file.NewPageWithBytes(record)

	position := int64(0)
	err := page.SetInt(position, SetInt)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetInt(position, int64(txNum))
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetString(position, block.Filename)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += filenameLen
	err = page.SetInt(position, block.Number)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetInt(position, offset)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetInt(position, int64(val))
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	return log.Append(record)
}

/*************** SetStringRecord *********************************************/

// SetStringRecord in log ->
// <SetString, TxID, filename, blockNumber, offset, value>
type SetStringRecord struct {
	txNum  TxID
	offset int64
	val    string
	block  file.Block
}

func newSetStringRecord(page *file.Page) *SetStringRecord {
	errMsg := "Failed to create SetString record: "
	position := int64(file.IntSize)
	txNumber, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	filename, err := page.GetString(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.MaxLen(len(filename))
	blockNum, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	offset, err := page.GetInt(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	val, err := page.GetString(position)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	return &SetStringRecord{
		txNum:  TxID(txNumber),
		offset: offset,
		val:    val,
		block:  file.GetBlock(filename, blockNum),
	}
}

func (s *SetStringRecord) recordType() int {
	return SetString
}

func (s *SetStringRecord) txNumber() TxID {
	return s.txNum
}

// undo Replace the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block,
// calls SetString to restore the saved value, and unpins the buffer.
func (s *SetStringRecord) undo(tx *Transaction) error {
	tx.Pin(s.block)
	err := tx.SetString(s.block, s.offset, s.val, false)
	if err != nil {
		return err
	}
	tx.Unpin(s.block)
	return nil
}

func (s *SetStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %v %v %v %v>", s.txNum, s.block, s.offset, s.val)
}

// writeSetStringRecToLog write a SetString record to the log.
// This log record contains the SetString operator,
// followed by transaction id, filename, blockNumber,
// offset of the modified block, and the previous string value at that offset.
// returns the LSN of the appended SetInt record
func writeSetStringRecToLog(log *wal.Log, txNum TxID, block file.Block, offset int64, val string) int64 {
	errMsg := "Failed to write SetString record to Log: "
	filenameLen := file.MaxLen(len(block.Filename))
	valueLen := file.MaxLen(len(val))
	record := make([]byte, 5*file.IntSize+filenameLen+valueLen)
	page := file.NewPageWithBytes(record)

	position := int64(0)
	err := page.SetInt(position, SetString)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetInt(position, int64(txNum))
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetString(position, block.Filename)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += filenameLen
	err = page.SetInt(position, block.Number)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetInt(position, offset)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	position += file.IntSize
	err = page.SetString(position, val)
	if err != nil {
		log2.Fatalln(errMsg, err)
	}

	return log.Append(record)
}
