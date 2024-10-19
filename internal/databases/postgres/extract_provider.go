package postgres

import (
	"github.com/wal-g/wal-g/internal"
)

type ExtractProvider interface {
	Get(backup Backup, filesToUnwrap map[string]bool, skipRedundantTars bool, dbDataDir string, createNewIncrementalFiles bool) (
		interpreter IncrementalTarInterpreter,
		concurrentTarsToExtract []internal.ReaderMaker,
		sequentialTarsToExtract []internal.ReaderMaker,
		err error)
}

type ExtractProviderImpl struct {
	FilesToExtractProviderImpl
}

func (t ExtractProviderImpl) Get(
	backup Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (IncrementalTarInterpreter, []internal.ReaderMaker, []internal.ReaderMaker, error) {
	interpreter := t.getTarInterpreter(dbDataDir, backup, filesToUnwrap, createNewIncrementalFiles)
	concurrentTarsToExtract, sequentialTarsToExtract, err := t.FilesToExtractProviderImpl.Get(backup, filesToUnwrap, skipRedundantTars)
	return interpreter, concurrentTarsToExtract, sequentialTarsToExtract, err
}

func (t ExtractProviderImpl) getTarInterpreter(dbDataDir string, backup Backup,
	filesToUnwrap map[string]bool, createNewIncrementalFiles bool) IncrementalTarInterpreter {
	return NewFileTarInterpreter(dbDataDir, *backup.SentinelDto, *backup.FilesMetadataDto,
		filesToUnwrap, createNewIncrementalFiles)
}
