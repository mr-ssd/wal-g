package xbstream

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
)

// xbstream BackupSink will unpack archive to disk.
// Note: files may be compressed(quicklz,lz4,zstd) / encrypted("NONE", "AES128", "AES192","AES256")
func BackupSink(stream *Reader, output string, decompress bool) {
	err := os.MkdirAll(output, 0777) // FIXME: permission & UMASK
	tracelog.ErrorLogger.FatalOnError(err)

	factory := fileSinkFactory{
		dataDir:          output,
		incrementalDir:   "",
		decompress:       decompress,
		inplace:          false,
		spaceIDCollector: innodb.NewSpaceIDCollector(output),
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

func AsyncBackupSink(wg *sync.WaitGroup, stream *Reader, dataDir string, decompress bool) {
	defer wg.Done()
	BackupSink(stream, dataDir, decompress)
}
