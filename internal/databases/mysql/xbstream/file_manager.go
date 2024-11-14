package xbstream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
)

type diffFileInfos struct {
	dataDir string
	spaces  map[innodb.SpaceID]diffFileInfo
}

type diffFileInfo struct {
	// file name from base backup
	originalName string
	// temporary file name to avoid file name clashes
	intermediateName string
	// new file name (from xbstream)
	targetName string
}

func NewDiffFileInfos(dataDir string, spaces map[innodb.SpaceID]string) (diffFileInfos, error) {
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return diffFileInfos{}, err
	}
	result := diffFileInfos{
		dataDir: dataDir,
		spaces:  make(map[innodb.SpaceID]diffFileInfo),
	}
	for spaceId, name := range spaces {
		result.spaces[spaceId] = diffFileInfo{originalName: name}
	}
	return result, nil
}

func (dfi *diffFileInfos) visit(spaceId innodb.SpaceID, newFileName string) (diffFileInfo, error) {
	space := dfi.spaces[spaceId]

	if space.originalName == "" {
		// new space:
		space.originalName = newFileName
	}

	if newFileName != "" && space.targetName != newFileName {
		tracelog.ErrorLogger.Fatalf("spaceid %d observed twice with different names: '%s' and '%s", spaceId, space.targetName, newFileName)
	}
	space.targetName = newFileName

	_, err := os.Lstat(newFileName)
	if errors.Is(err, os.ErrNotExist) {
		space.intermediateName = newFileName
		return space, nil // ok
	}
	if err != nil {
		return diffFileInfo{}, err
	}
	// If there was a tablespace with matching name and mismatching ID,
	// renames it temporary.
	space.intermediateName = fmt.Sprintf("__walg_#%d.ibd", spaceId)
	return space, nil // ok
}
