package xbstream

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
)

// DiffBackupSink doesn't try to replicate sophisticated xtrabackup logic
// instead, we do following:
// * extract all non-diff files to incrementDir
// * apply diff-files to dataDir 'inplace' + add truncated versions of diff-files to incrementalDir
// * let xtrabackup do its job
func DiffBackupSink(stream *Reader, dataDir string, incrementalDir string) {
	err := os.MkdirAll(dataDir, 0777) // FIXME: permission & UMASK
	tracelog.ErrorLogger.FatalOnError(err)

	factory := fileSinkFactory{
		dataDir:          dataDir,
		incrementalDir:   incrementalDir,
		decompress:       true, // always decompress files when diff-files applied
		inplace:          true,
		spaceIDCollector: innodb.NewSpaceIDCollector(dataDir),
	}

	sinks := make(map[string]fileSink)
	for {
		chunk, err := stream.Next()
		if err == io.EOF {
			break
		}
		tracelog.ErrorLogger.FatalfOnError("Cannot read next chunk: %v", err)

		path := factory.MapDataSinkPath(chunk.Path)
		sink, ok := sinks[path]
		if !ok {
			sink = factory.NewDataSink(chunk.Path)
			sinks[path] = sink
			tracelog.DebugLogger.Printf("Extracting %v", chunk.Path)
		}

		err = sink.Process(chunk)
		if errors.Is(err, ErrSinkEOF) {
			delete(sinks, path)
		} else if err != nil {
			tracelog.ErrorLogger.Printf("Error in chunk %v: %v", chunk.Path, err)
		}
	}

	for path := range sinks {
		tracelog.WarningLogger.Printf("File %v wasn't clossed properly. Probably xbstream is broken", path)
	}
}

func AsyncDiffBackupSink(wg *sync.WaitGroup, stream *Reader, dataDir string, incrementalDir string) {
	defer wg.Done()
	DiffBackupSink(stream, dataDir, incrementalDir)
}
