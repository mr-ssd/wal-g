package internal

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

type TarSizeError struct {
	error
}

func newTarSizeError(packedFileSize, expectedSize int64) TarSizeError {
	return TarSizeError{errors.Errorf("packed wrong numbers of bytes %d instead of %d", packedFileSize, expectedSize)}
}

type Bundle struct {
	Directories []string
	Sentinel    *Sentinel // structure that points to important file e.g. pg_control in Postgres

	TarBallComposer TarBallComposer
	TarBallQueue    *TarBallQueue

	Crypter crypto.Crypter

	TarSizeThreshold int64

	ExcludedFilenames map[string]utility.Empty

	FilesFilter FilesFilter
}

func NewBundle(
	directories []string, crypter crypto.Crypter,
	tarSizeThreshold int64, excludedFilenames map[string]utility.Empty) *Bundle {
	return &Bundle{
		Directories:       directories,
		Crypter:           crypter,
		TarSizeThreshold:  tarSizeThreshold,
		ExcludedFilenames: excludedFilenames,
		FilesFilter:       &CommonFilesFilter{},
	}
}

func (bundle *Bundle) StartQueue(tarBallMaker TarBallMaker) error {
	bundle.TarBallQueue = NewTarBallQueue(bundle.TarSizeThreshold, tarBallMaker)
	return bundle.TarBallQueue.StartQueue()
}

func (bundle *Bundle) SetupComposer(composerMaker TarBallComposerMaker) (err error) {
	tarBallComposer, err := composerMaker.Make(bundle)
	if err != nil {
		return err
	}
	bundle.TarBallComposer = tarBallComposer
	return nil
}

func (bundle *Bundle) FinishQueue() error {
	return bundle.TarBallQueue.FinishQueue()
}

func (bundle *Bundle) AddToBundle(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			tracelog.WarningLogger.Println(path, " deleted during filepath walk")
			return nil
		}
		return errors.Wrap(err, "HandleWalkedFSObject: walk failed")
	}

	fileName := info.Name()
	_, excluded := bundle.ExcludedFilenames[fileName]
	isDir := info.IsDir()

	if excluded && !isDir {
		return nil
	}
	fileInfoHeader, err := bundle.createTarFileInfoHeader(path, info)
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	if bundle.FilesFilter.ShouldUploadFile(path) && info.Mode().IsRegular() {
		bundle.TarBallComposer.AddFile(NewComposeFileInfo(path, info, false, false, fileInfoHeader))
	} else {
		err := bundle.TarBallComposer.AddHeader(fileInfoHeader, info)
		if err != nil {
			return err
		}
		if excluded && isDir {
			return filepath.SkipDir
		}
	}

	return nil
}

func (bundle *Bundle) FinishComposing() (TarFileSets, error) {
	return bundle.TarBallComposer.FinishComposing()
}

func (bundle *Bundle) GetFileRelPath(fileAbsPath string) string {
	for _, directory := range bundle.Directories {
		if strings.HasPrefix(fileAbsPath, directory) {
			return utility.PathSeparator + utility.GetSubdirectoryRelativePath(fileAbsPath, directory)
		}
	}
	return fileAbsPath
}

func (bundle *Bundle) createTarFileInfoHeader(path string, info os.FileInfo) (header *tar.Header, err error) {
	header, err = tar.FileInfoHeader(info, path)
	if err != nil {
		return nil, errors.Wrap(err, "addToBundle: could not grab header info")
	}

	header.Name = bundle.GetFileRelPath(path)
	return
}
