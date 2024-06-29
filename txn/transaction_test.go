package txn_test

import (
	"github.com/naveen246/kite-db/file"
	"os"
)

var (
	blockTestSize int64 = 400
	logFile             = "simpledb.log"
	dbDir               = "txTest"
	filename            = "testFile"
)

func createFile(fileMgr file.FileMgr, filename string) {
	f, _ := os.Create(fileMgr.DbFilePath(filename))
	f.Truncate(1e5)
}

func removeFile(filename string, dbDir string) {
	os.Remove(filename)
	os.Remove(dbDir)
}
