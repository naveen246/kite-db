package buffer

//
//import (
//	"github.com/naveen246/kite-db/file"
//	"github.com/naveen246/kite-db/loghandler"
//	"github.com/stretchr/testify/assert"
//	"os"
//	"testing"
//)
//
//const (
//	BlockSize  = 400
//	BufferSize = 8
//	LogFile    = "simpledb.log"
//)
//
//type DB struct {
//	FileMgr file.FileMgr
//	LogMgr  *loghandler.LogMgr
//	BufMgr  *BufferMgr
//}
//
//func NewDB(dbDir string, blockSize uint32, bufferCount int) *DB {
//	fileMgr := file.NewFileMgr(dbDir, blockSize)
//	logMgr := loghandler.NewLogMgr(fileMgr, LogFile)
//	bufferMgr := NewBufferMgr(fileMgr, logMgr, bufferCount)
//	return &DB{
//		FileMgr: fileMgr,
//		LogMgr:  logMgr,
//		BufMgr:  bufferMgr,
//	}
//}
//
//func removeFile(filename string, dbDir string) {
//	os.Remove(filename)
//	os.Remove(dbDir)
//}
//
//func TestBufferFile(t *testing.T) {
//	dbDir := "buffertest"
//	filename := "testfile"
//	db := NewDB(dbDir, 400, 8)
//
//	defer removeFile(filename, dbDir)
//	defer removeFile(db.FileMgr.DbFilePath(LogFile), dbDir)
//
//	bufMgr := db.BufMgr
//	block := file.GetBlock(filename, 2)
//	pos1 := 88
//
//	buf1 := bufMgr.PinBuffer(block)
//	page1 := buf1.Contents
//	text := "abcdefghijklm"
//	page1.SetString(uint32(pos1), text)
//	size := page1.MaxLen(len(text))
//	pos2 := pos1 + size
//	page1.SetInt(uint32(pos2), 345)
//	buf1.SetModified(1, 0)
//	bufMgr.UnpinBuffer(buf1)
//
//	buf2 := bufMgr.PinBuffer(block)
//	page2 := buf2.Contents
//	pos2Value, _ := page2.GetInt(uint32(pos2))
//
//	pos1Value, _ := page2.GetString(uint32(pos1))
//	assert.Equal(t, text, pos1Value)
//	assert.Equal(t, uint32(345), pos2Value)
//	bufMgr.UnpinBuffer(buf2)
//}
//
//func TestNewBufferMgr(t *testing.T) {
//
//}
