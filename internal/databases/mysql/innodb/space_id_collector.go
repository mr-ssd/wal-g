package innodb

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"
)

var ErrSpaceIDNotFound = errors.New("SpaceID not found")

type SpaceIDCollector interface {
	// GetFileForSpaceID locates InnoDB file (path relative to dataDir) for requested SpaceID
	GetFileForSpaceID(spaceID SpaceID) (string, error)
}

type spaceIDCollectorImpl struct {
	dataDir   string
	collected map[SpaceID]string
}

var _ SpaceIDCollector = &spaceIDCollectorImpl{}

func NewSpaceIDCollector(dataDir string) (SpaceIDCollector, error) {
	result := &spaceIDCollectorImpl{dataDir: dataDir}
	result.collected = make(map[SpaceID]string)

	// https://github.com/percona/percona-xtrabackup/blob/percona-xtrabackup-8.0.35-30/storage/innobase/xtrabackup/src/xtrabackup.cc#L5321-L5567

	err := filepath.WalkDir(dataDir, func(path string, info fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("error encountered during dataDir traverse %v: %w", path, walkErr)
		}
		if !info.IsDir() && (strings.HasSuffix(info.Name(), ".ibd") || info.Name() == "ibdata1" || info.Name() == "undo_001" || info.Name() == "undo_002") {
			err := result.collect(path)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	tracelog.DebugLogger.Printf("SpaceIDCollector for dir %v collected %v", dataDir, result.collected)

	return result, nil
}

func (c *spaceIDCollectorImpl) collect(filePath string) error {
	// read first FPS page (always first page in the file)
	file, err := os.OpenFile(filePath, os.O_RDONLY|syscall.O_NOFOLLOW, 0) // FIXME: test performance with O_SYNC
	if err != nil {
		return fmt.Errorf("error opening file %v: %w", filePath, err)
	}

	reader, err := NewPageReader(file)
	if err != nil {
		return fmt.Errorf("cannot collect spaceID from file %v: %w", filePath, err)
	}
	if reader == nil {
		return fmt.Errorf("canot read innodb file %v", filePath)
	}
	// FIXME: use os.Root [go 1.24] https://github.com/golang/go/issues/67002
	if !strings.HasPrefix(filePath, c.dataDir) {
		tracelog.ErrorLogger.Fatalf("File %v is out of data dir %v", filePath, c.dataDir)
	}
	fileName := filePath[len(c.dataDir):]
	c.collected[reader.SpaceID] = strings.TrimPrefix(fileName, "/")
	return nil
}

func (c *spaceIDCollectorImpl) GetFileForSpaceID(spaceID SpaceID) (string, error) {
	result, ok := c.collected[spaceID]
	if ok {
		return result, nil
	}
	return "", fmt.Errorf("file for SpaceID %v not found: %w", spaceID, ErrSpaceIDNotFound)
}
