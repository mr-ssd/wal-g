package xbstream

import (
	"encoding/binary"
	"errors"
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
	// * if xbstream.SinkEOF returned - then sink considered as closed
	Process(chunk *Chunk) error
}

var SinkEOF = errors.New("SinkEOF")

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
		return SinkEOF
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
	file          *os.File
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
		file:          file,
		writeHere:     make(chan []byte),
		fileCloseChan: make(chan struct{}),
	}
	reader := splitmerge.NewChannelReader(sink.writeHere)
	readHere, err := decompressor.Decompress(reader)
	tracelog.ErrorLogger.FatalfOnError("Cannot decompress: %v", err)

	go func() {
		_, err := io.Copy(file, readHere)
		tracelog.ErrorLogger.FatalfOnError("Cannot copy data: %v", err)
		err = sink.repairSparse()
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
		return SinkEOF
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

func (sink *decompressFileSink) repairSparse() error {
	if !strings.HasSuffix(sink.file.Name(), "ibd") {
		return nil
	}
	_, err := sink.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	pageReader := innodb.NewPageReader(sink.file)
	pageNumber := 1 // Never compress/decompress the first page (FSP_HDR)
	for {
		page, err := pageReader.ReadRaw(innodb.PageNumber(pageNumber))
		if err == io.EOF {
			return nil
		}
		pageNumber++
		tracelog.ErrorLogger.FatalOnError(err) // FIXME: in future we can ignore such errors

		if page.Header.PageType == innodb.PageTypeCompressed {
			// do punch hole, if possible
			meta := page.Header.GetCompressedData()
			if meta.CompressedSize < pageReader.PageSize {
				offset := int64(page.Header.PageNumber)*int64(pageReader.PageSize) + int64(meta.CompressedSize)
				size := int64(pageReader.PageSize - meta.CompressedSize)
				err = ioextensions.PunchHole(sink.file, offset, size)
				if errors.Is(err, syscall.EOPNOTSUPP) {
					return nil // ok
				}
				tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
			}
		}
	}
}

type diffFileSink struct {
	simpleFileSink
	meta *diffMetadata
}

var _ dataSink = &diffFileSink{}

func newDiffFileSink(file *os.File) dataSink {
	return &diffFileSink{
		simpleFileSink: simpleFileSink{file},
		meta:           nil,
	}
}

func (sink diffFileSink) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".meta") {
		return nil // skip
	}
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".delta") {
		_ = sink.file.Close()
		return SinkEOF
	}

	if strings.HasSuffix(chunk.Path, ".meta") {
		rawMeta, _ := io.ReadAll(chunk.Reader) //  FIXME: error handling
		meta, _ := parseDiffMetadata(rawMeta)  //  FIXME: error handling
		sink.meta = &meta
		return nil
	}
	if strings.HasSuffix(chunk.Path, ".delta") {
		// apply delta:
		// Delta file format:
		// * Header - page_size bytes
		//   'xtra' (4 bytes) or 'XTRA' for final block
		//   N * page_no (N * 4 bytes) - list of page_no (up to page_size / 4 OR  0xFFFFFFFF-terminated-list)
		// * Body
		//   N * <page content>

		header := make([]byte, sink.meta.PageSize)
		_, err := chunk.Reader.Read(header) // FIXME: check bytes?
		if err != nil {
			return err
		}
		if !slices.Equal(header[0:4], []byte("XTRA")) && !slices.Equal(header[0:4], []byte("xtra")) {
			return errors.New("unexpected header in diff file")
		}
		is_last := slices.Equal(header[0:4], []byte("XTRA"))

		page_cnt := uint32(1)
		for i := uint32(1); i < sink.meta.PageSize/4; i++ {
			page_no := binary.BigEndian.Uint32(header[i*4 : i*4+4])
			if page_no == 0xFFFFFFFF {
				break
			}
			page_cnt = page_cnt + 1
		}

		if page_cnt != sink.meta.PageSize/4 && !is_last {
			// error
		}

		_, err = sink.file.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		return nil
	}

	return sink.simpleFileSink.Process(chunk)
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

// xbstream Disk Sink will unpack archive to disk.
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
		if errors.Is(err, SinkEOF) {
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
