package xbstream

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"github.com/wal-g/wal-g/utility"
)

var ErrSinkEOF = errors.New("ErrSinkEOF")

type fileSink interface {
	// Process should read all data in `chunk` before returning from method
	//
	// when chunk.Type == ChunkTypeEOF:
	// * if xbstream.ErrSinkEOF returned - then sink considered as closed
	Process(chunk *Chunk) error
}

type fileSinkFactory struct {
	dataDir          string
	incrementalDir   string
	decompress       bool
	inplace          bool
	spaceIDCollector innodb.SpaceIDCollector
}

func (fsf *fileSinkFactory) MapDataSinkPath(path string) string {
	ext := filepath.Ext(path)
	if fsf.decompress {
		if ext == ".lz4" || ext == ".zst" {
			path = strings.TrimSuffix(path, ext)
			ext = filepath.Ext(path)
		}
		if ext == ".qp" {
			// FIXME: test whether we can have real file with 'qp' extension
			tracelog.ErrorLogger.Fatal("qpress not supported - restart extraction without 'inplace' feature")
		}
	}
	if fsf.inplace {
		if ext == ".delta" {
			path = strings.TrimSuffix(path, ext)
		}
		if ext == ".meta" {
			path = strings.TrimSuffix(path, ext)
		}
	}
	return path
}

func (fsf *fileSinkFactory) NewDataSink(path string) fileSink {
	var err error
	ext := filepath.Ext(path)
	if ext == ".xbcrypt" {
		tracelog.ErrorLogger.Fatalf("xbstream contains encrypted files. We don't support it. Use xbstream instead: %v", path)
	}
	path = fsf.MapDataSinkPath(path)

	filePath := filepath.Join(fsf.dataDir, path)
	if !utility.IsInDirectory(filePath, fsf.dataDir) {
		tracelog.ErrorLogger.Fatalf("xbstream tries to create file outside destination directory: %v", path)
	}

	err = os.MkdirAll(filepath.Dir(filePath), 0777)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0666) // FIXME: premissions
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)

	// FIXME: fadvise POSIX_FADV_SEQUENTIAL
	// FIXME: test O_DIRECT

	if fsf.decompress {
		decompressor := compression.FindDecompressor(ext)
		if decompressor != nil {
			return newFileSinkDecompress(file, decompressor)
		}
	}
	if fsf.inplace {
		return newDiffFileSink(file, fsf.incrementalDir, path)
	}

	return newSimpleFileSink(file)
}
