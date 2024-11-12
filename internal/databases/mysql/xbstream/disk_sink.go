package xbstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/splitmerge"
	"github.com/wal-g/wal-g/utility"
)

type dataSink interface {
	// Process should read all data in `chunk` before returning from method
	//
	// when chunk.Type == ChunkTypeEOF:
	// * if xbstream.ErrSinkEOF returned - then sink considered as closed
	Process(chunk *Chunk) error
}

var ErrSinkEOF = errors.New("ErrSinkEOF")

type simpleFileSink struct {
	file *os.File
}

var _ dataSink = &simpleFileSink{}

func newSimpleFileSink(file *os.File) dataSink {
	return &simpleFileSink{file}
}

func (sink *simpleFileSink) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF {
		_ = sink.file.Close() // FIXME: error handling
		return ErrSinkEOF
	}

	_, err := sink.file.Seek(int64(chunk.Offset), io.SeekStart)
	tracelog.ErrorLogger.FatalfOnError("seek: %v", err)

	if len(chunk.SparseMap) == 0 {
		_, err = io.Copy(sink.file, chunk)
		tracelog.ErrorLogger.FatalfOnError("copy %v", err)
	} else {
		for _, schunk := range chunk.SparseMap {
			off, err := sink.file.Seek(int64(schunk.SkipBytes), io.SeekCurrent)
			tracelog.ErrorLogger.FatalfOnError("seek: %v", err)
			err = ioextensions.PunchHole(sink.file, off-int64(schunk.SkipBytes), int64(schunk.SkipBytes))
			if !errors.Is(err, syscall.EOPNOTSUPP) {
				tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
			}
			_, err = io.CopyN(sink.file, chunk, int64(schunk.WriteBytes))
			tracelog.ErrorLogger.FatalfOnError("copyN: %v", err)
		}
	}

	return nil
}

type decompressFileSink struct {
	simpleFileSink
	writeHere     chan []byte
	fileCloseChan chan struct{}
	xbOffset      uint64
}

var _ dataSink = &decompressFileSink{}

func newDecompressFileSink(file *os.File, decompressor compression.Decompressor) dataSink {
	// xbstream is a simple archive format. Compression / encryption / delta-files are xtrabackup features.
	// so, all chunks of one compressed file is a _single_ stream
	// we should combine data from all file chunks in a single io.Reader before passing to Decompressor:
	sink := decompressFileSink{
		simpleFileSink: simpleFileSink{file},
		writeHere:      make(chan []byte),
		fileCloseChan:  make(chan struct{}),
	}
	reader := splitmerge.NewChannelReader(sink.writeHere)
	readHere, err := decompressor.Decompress(reader)
	tracelog.ErrorLogger.FatalfOnError("Cannot decompress: %v", err)

	go func() {
		_, err := io.Copy(file, readHere)
		tracelog.ErrorLogger.FatalfOnError("Cannot copy data: %v", err)
		err = innodb.RepairSparse(file)
		if err != nil {
			tracelog.WarningLogger.Printf("Error during repairSparse(): %v", err)
		}
		utility.LoggedClose(file, "datasink.Close()")
		close(sink.fileCloseChan)
	}()

	return &sink
}

func (sink *decompressFileSink) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF {
		close(sink.writeHere)
		<-sink.fileCloseChan // file will be closed in goroutine, wait for it...
		return ErrSinkEOF
	}

	if len(chunk.SparseMap) != 0 {
		tracelog.ErrorLogger.Fatalf("Found compressed file %v with sparse map", chunk.Path)
	}
	if sink.xbOffset != chunk.Offset {
		tracelog.ErrorLogger.Fatalf("Offset mismatch for file %v: expected=%v, actual=%v", chunk.Path, sink.xbOffset, chunk.Offset)
	}
	sink.xbOffset += chunk.PayloadLen

	// synchronously read data & send it to writer
	buffer := make([]byte, chunk.PayloadLen)
	_, err := io.ReadFull(chunk, buffer)
	tracelog.ErrorLogger.FatalfOnError("ReadFull %v", err)
	sink.writeHere <- buffer
	return nil
}

type diffFileSink struct {
	simpleFileSink
	meta          *diffMetadata
	readHere      io.ReadCloser
	writeHere     chan []byte
	fileCloseChan chan struct{}
}

var _ dataSink = &diffFileSink{}

func newDiffFileSink(file *os.File) dataSink {
	// xbstream is a simple archive format. Compression / encryption / delta-files are xtrabackup features.
	// so, all chunks of one compressed file is a _single_ stream
	// we should combine data from all file chunks in a single io.Reader before passing to Decompressor:
	sink := diffFileSink{
		simpleFileSink: simpleFileSink{file},
		meta:           nil,
		writeHere:      make(chan []byte),
		fileCloseChan:  make(chan struct{}),
	}
	sink.readHere = splitmerge.NewChannelReader(sink.writeHere)

	go func() {
		err := sink.applyDiff()
		tracelog.ErrorLogger.FatalfOnError("Cannot handle diff: %v", err)
		err = innodb.RepairSparse(file)
		if err != nil {
			tracelog.WarningLogger.Printf("Error during repairSparse(): %v", err)
		}
		utility.LoggedClose(file, "sink.Close()")
		close(sink.fileCloseChan)
	}()

	return &sink
}

func (sink diffFileSink) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".meta") {
		return nil // skip
	}
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".delta") {
		close(sink.writeHere)
		<-sink.fileCloseChan // file will be closed in goroutine, wait for it...
		return ErrSinkEOF
	}

	if strings.HasSuffix(chunk.Path, ".meta") {
		rawMeta, _ := io.ReadAll(chunk.Reader) //  FIXME: error handling
		meta, _ := parseDiffMetadata(rawMeta)  //  FIXME: error handling
		sink.meta = &meta
		return nil
	}
	if strings.HasSuffix(chunk.Path, ".delta") {
		// synchronously read data & send it to writer
		buffer := make([]byte, chunk.PayloadLen)
		_, err := io.ReadFull(chunk, buffer)
		tracelog.ErrorLogger.FatalfOnError("ReadFull %v", err)
		sink.writeHere <- buffer
		return nil
	}

	return sink.simpleFileSink.Process(chunk)
}

func (sink diffFileSink) applyDiff() error {
	// check stream format in README.md
	header := make([]byte, sink.meta.PageSize)
	_, err := sink.readHere.Read(header) // FIXME: check bytes?
	if err != nil {
		return err
	}
	if !slices.Equal(header[0:4], []byte("XTRA")) && !slices.Equal(header[0:4], []byte("xtra")) {
		return errors.New("unexpected header in diff file")
	}
	isLast := slices.Equal(header[0:4], []byte("XTRA"))

	pageNums := make([]innodb.PageNumber, 0, sink.meta.PageSize/4)
	for i := uint32(1); i < sink.meta.PageSize/4; i++ {
		pageNums[i] = innodb.PageNumber(binary.BigEndian.Uint32(header[i*4 : i*4+4]))
		if pageNums[i] == 0xFFFFFFFF {
			break
		}
	}

	if uint32(len(pageNums)) != sink.meta.PageSize/4 && !isLast {
		return fmt.Errorf("invalid '.delta' format: number of pages %v doesn't match delta-header type %v", len(pageNums), header[0:4])
	}

	// copy pages:
	for _, pageNum := range pageNums {
		_, err = sink.file.Seek(int64(pageNum)*int64(sink.meta.PageSize), io.SeekStart)
		if err != nil {
			return err
		}
		_, err = io.CopyN(sink.file, sink.readHere, int64(sink.meta.PageSize))
		if err != nil {
			return err
		}
	}
	return nil
}

type dataSinkFactory struct {
	output           string
	decompress       bool
	inplace          bool
	spaceIDCollector innodb.SpaceIDCollector
}

func (dsf *dataSinkFactory) MapDataSinkPath(path string) string {
	ext := filepath.Ext(path)
	if dsf.decompress {
		if ext == ".lz4" || ext == ".zst" {
			path = strings.TrimSuffix(path, ext)
			ext = filepath.Ext(path)
		}
		if ext == ".qp" {
			// FIXME: test whether we can have real file with 'qp' extension
			tracelog.ErrorLogger.Fatal("qpress not supported - restart extraction without 'inplace' feature")
		}
	}
	if dsf.inplace {
		if ext == ".delta" {
			path = strings.TrimSuffix(path, ext)
		}
		if ext == ".meta" {
			path = strings.TrimSuffix(path, ext)
		}
	}
	return path
}

func (dsf *dataSinkFactory) NewDataSink(path string) dataSink {
	var err error
	ext := filepath.Ext(path)
	if ext == ".xbcrypt" {
		tracelog.ErrorLogger.Fatalf("xbstream contains encrypted files. We don't support it. Use xbstream instead: %v", path)
	}
	path = dsf.MapDataSinkPath(path)

	filePath := filepath.Join(dsf.output, path)
	if !utility.IsInDirectory(filePath, dsf.output) {
		tracelog.ErrorLogger.Fatalf("xbstream tries to create file outside destination directory: %v", path)
	}

	err = os.MkdirAll(filepath.Dir(filePath), 0777)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0666)
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)

	// FIXME: fadvise POSIX_FADV_SEQUENTIAL
	// FIXME: test O_DIRECT

	if dsf.decompress {
		decompressor := compression.FindDecompressor(ext)
		if decompressor != nil {
			return newDecompressFileSink(file, decompressor)
		}
	}
	if dsf.inplace {
		return newDiffFileSink(file)
	}

	return newSimpleFileSink(file)
}

// xbstream DiskSink will unpack archive to disk.
// Note: files may be compressed(quicklz,lz4,zstd) / encrypted("NONE", "AES128", "AES192","AES256")
func DiskSink(stream *Reader, output string, decompress bool, inplace bool) {
	err := os.MkdirAll(output, 0777) // FIXME: permission & UMASK
	tracelog.ErrorLogger.FatalOnError(err)

	factory := dataSinkFactory{output, decompress, inplace, innodb.NewSpaceIDCollector(output)}

	sinks := make(map[string]dataSink)
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

func AsyncDiskSink(wg *sync.WaitGroup, stream *Reader, output string, decompress bool, inplace bool) {
	defer wg.Done()
	DiskSink(stream, output, decompress, inplace)
}
