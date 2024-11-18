package xbstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"github.com/wal-g/wal-g/internal/splitmerge"
	"github.com/wal-g/wal-g/utility"
)

type diffFileSink struct {
	file             *os.File
	dataDir          string
	incrementalDir   string
	filePath         string // relative to datadir (as in xbstream) but without '.delta.zst'
	meta             *diffMetadata
	readHere         io.ReadCloser
	writeHere        chan []byte
	fileCloseChan    chan struct{}
	spaceIDCollector innodb.SpaceIDCollector
}

var _ fileSink = &diffFileSink{}

func newDiffFileSink(
	dataDir string,
	incrementalDir string,
	decompressor compression.Decompressor,
	spaceIDCollector innodb.SpaceIDCollector,
) fileSink {
	// xbstream is a simple archive format. Compression / encryption / delta-files are xtrabackup features.
	// so, all chunks of one compressed file is a _single_ stream
	// we should combine data from all file chunks in a single io.Reader before passing to Decompressor:
	sink := diffFileSink{
		dataDir:          dataDir,
		incrementalDir:   incrementalDir,
		meta:             nil,
		writeHere:        make(chan []byte),
		fileCloseChan:    make(chan struct{}),
		spaceIDCollector: spaceIDCollector,
	}

	if decompressor != nil {
		readHere, err := decompressor.Decompress(splitmerge.NewChannelReader(sink.writeHere))
		tracelog.ErrorLogger.FatalfOnError("Cannot decompress: %v", err)
		sink.readHere = readHere
	} else {
		sink.readHere = splitmerge.NewChannelReader(sink.writeHere)
	}

	return &sink
}

func (sink *diffFileSink) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".meta") {
		return nil // skip
	}
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".delta") {
		close(sink.writeHere)
		<-sink.fileCloseChan // file will be closed in goroutine, wait for it...
		return ErrSinkEOF
	}

	if strings.HasSuffix(chunk.Path, ".meta") {
		return sink.ProcessMeta(chunk)
	}
	if strings.HasSuffix(chunk.Path, ".delta") {
		// synchronously read data & send it to writer
		buffer := make([]byte, chunk.PayloadLen)
		_, err := io.ReadFull(chunk, buffer)
		tracelog.ErrorLogger.FatalfOnError(fmt.Sprintf("ReadFull on file %v", chunk.Path), err)
		sink.writeHere <- buffer
		return nil
	}

	return fmt.Errorf("unexpected file extension for diff-sink %v", chunk.Path)
}

func (sink *diffFileSink) ProcessMeta(chunk *Chunk) error {
	if sink.meta != nil {
		return fmt.Errorf("unexpected 'meta' file %v - we already seen it", chunk.Path)
	}
	rawMeta, err := io.ReadAll(chunk.Reader)
	if err != nil {
		return err
	}
	meta, err := parseDiffMetadata(rawMeta)
	if err != nil {
		return err
	}
	sink.meta = &meta

	err = sink.writeToFile(sink.incrementalDir, chunk.Path, rawMeta)
	if err != nil {
		return err
	}

	newFilePath := strings.TrimSuffix(chunk.Path, ".meta")
	oldFilePath, err := sink.spaceIDCollector.GetFileForSpaceID(meta.SpaceID)
	if err != nil && !errors.Is(err, innodb.ErrSpaceIDNotFound) {
		return err
	}
	if errors.Is(err, innodb.ErrSpaceIDNotFound) {
		sink.filePath = newFilePath
		tracelog.InfoLogger.Printf("New file for SpaceID %v will be created at %s", meta.SpaceID, newFilePath)
	} else {
		// in any case update old file... xtrabackup will handle renaming for us
		sink.filePath = oldFilePath
		if oldFilePath != newFilePath {
			tracelog.InfoLogger.Printf("File path for SpaceID %v changed from %s to %s", meta.SpaceID, oldFilePath, newFilePath)
		}
	}

	file, err := safeFileCreate(sink.dataDir, sink.filePath)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)
	sink.file = file

	go func() {
		err := sink.applyDiff()
		tracelog.ErrorLogger.FatalfOnError("Cannot handle diff: %v", err)
		err = innodb.RepairSparse(sink.file)
		if err != nil {
			tracelog.WarningLogger.Printf("Error during repairSparse(): %v", err)
		}
		utility.LoggedClose(sink.file, "sink.Close()")
		close(sink.fileCloseChan)
	}()

	return nil
}

func (sink *diffFileSink) applyDiff() error {
	// check stream format in README.md
	header := make([]byte, sink.meta.PageSize)
	_, err := sink.readHere.Read(header)
	if err != nil {
		return err
	}
	if !slices.Equal(header[0:4], []byte("XTRA")) && !slices.Equal(header[0:4], []byte("xtra")) {
		return errors.New("unexpected header in diff file")
	}
	isLast := slices.Equal(header[0:4], []byte("XTRA"))
	isFirst := true

	pageNums := make([]innodb.PageNumber, 0, sink.meta.PageSize/4)
	for i := uint32(1); i < sink.meta.PageSize/4; i++ {
		pageNum := innodb.PageNumber(binary.BigEndian.Uint32(header[i*4 : i*4+4]))
		if pageNum == 0xFFFFFFFF {
			break
		}
		pageNums = append(pageNums, pageNum)
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

		if isFirst {
			firstPage := make([]byte, sink.meta.PageSize)
			_, err = sink.readHere.Read(firstPage)
			if err != nil {
				return err
			}
			// write to data dir:
			_, err = sink.file.Write(firstPage)
			if err != nil {
				return err
			}
			// write to incremental dir:
			raw := sink.buildFakeDiff(header, firstPage)
			err := sink.writeToFile(sink.incrementalDir, sink.filePath+".delta", raw)
			if err != nil {
				return err
			}
		} else {
			_, err = io.CopyN(sink.file, sink.readHere, int64(sink.meta.PageSize))
			if err != nil {
				return err
			}
		}

		isFirst = false
	}

	tracelog.DebugLogger.Printf("%v pages copied to file %v", len(pageNums), sink.file.Name())

	return nil
}

func (sink *diffFileSink) writeToFile(dir string, relFilePath string, bytes []byte) error {
	if !utility.IsInDirectory(relFilePath, dir) {
		tracelog.ErrorLogger.Fatalf("xbstream tries to create file outside incrementalDir: %v", relFilePath)
	}

	file, err := os.OpenFile(
		path.Join(dir, relFilePath),
		os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW,
		0666, // FIXME: permissions
	)
	if err != nil {
		return err
	}

	_, err = file.Write(bytes)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sink *diffFileSink) buildFakeDiff(header []byte, page []byte) []byte {
	// here we are writing fake diff-file to incrementalDir:
	// it consists of:
	// * Header - page_size bytes (page_size - from '.meta' file)
	//   (4 bytes) 'XTRA' (as it last block for this delta file)
	//   (4 byte) page_number
	//   (4 bytes) 0xFFFFFFFF - as page list termination symbol
	//   (page_size - N) 0x0 - filler
	// * Body
	//   1 * <page content>
	//
	// xtrabackup will re-apply this page and do all its magic for us
	raw := make([]byte, 2*sink.meta.PageSize)
	binary.BigEndian.PutUint32(raw[0:4], 0x58545241)
	binary.BigEndian.PutUint32(raw[4:8], binary.BigEndian.Uint32(header[4:8]))
	binary.BigEndian.PutUint32(raw[8:12], 0xFFFFFFFF)
	copy(raw[sink.meta.PageSize:], page)
	return raw
}
