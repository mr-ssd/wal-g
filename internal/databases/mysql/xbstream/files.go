package xbstream

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/utility"
)

func safeFileCreate(dataDir string, relFilePath string) (*os.File, error) {
	filePath := filepath.Join(dataDir, relFilePath)
	if !utility.IsInDirectory(filePath, dataDir) {
		tracelog.ErrorLogger.Fatalf("xbstream tries to create file outside destination directory: %v", filePath)
	}

	err := os.MkdirAll(filepath.Dir(filePath), 0777)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0666) // FIXME: permissions
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)
	return file, nil
}
