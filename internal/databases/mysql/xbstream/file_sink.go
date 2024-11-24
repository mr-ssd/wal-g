package xbstream

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
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

func (fsf *fileSinkFactory) MapDataSinkKey(chunkPath string) string {
	ext := filepath.Ext(chunkPath)
	if fsf.decompress {
		if ext == ".lz4" || ext == ".zst" {
			chunkPath = strings.TrimSuffix(chunkPath, ext)
			ext = filepath.Ext(chunkPath)
		}
		if ext == ".qp" {
			tracelog.ErrorLogger.Fatal("qpress not supported - restart extraction without 'decompress' or 'inplace' feature")
		}
	}
	if fsf.inplace {
		if ext == ".delta" {
			chunkPath = strings.TrimSuffix(chunkPath, ext)
		}
		if ext == ".meta" {
			chunkPath = strings.TrimSuffix(chunkPath, ext)
		}
	}
	return chunkPath
}

func (fsf *fileSinkFactory) MapDataSinkPath(chunkPath string) string {
	return fsf.MapDataSinkKey(chunkPath)
}

func (fsf *fileSinkFactory) NewDataSink(chunkPath string) fileSink {
	ext := filepath.Ext(chunkPath)
	if ext == ".xbcrypt" {
		tracelog.ErrorLogger.Fatalf("xbstream contains encrypted files. We don't support it. Use xbstream instead: %v", chunkPath)
	}

	filePath := fsf.MapDataSinkPath(chunkPath)

	var decompressor compression.Decompressor = nil
	if fsf.decompress {
		decompressor = compression.FindDecompressor(ext)
	}

	if fsf.inplace && (strings.HasSuffix(chunkPath, ".meta") || strings.HasSuffix(chunkPath, ".delta")) {
		return newDiffFileSink(fsf.dataDir, fsf.incrementalDir, decompressor, fsf.spaceIDCollector)
	}

	if decompressor != nil {
		return newFileSinkDecompress(filePath, fsf.dataDir, decompressor)
	}
	return newSimpleFileSink(filePath, fsf.dataDir)
}
