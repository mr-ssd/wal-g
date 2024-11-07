package postgres

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres/orioledb"
	"github.com/wal-g/wal-g/utility"
)

func extendExcludedFiles() {
	for _, fname := range []string{"pg_hba.conf", "postgresql.conf", "postgresql.auto.conf"} {
		ExcludedFilenames[fname] = utility.Empty{}
	}
}

// HandleCatchupPush is invoked to perform a wal-g catchup-push
func HandleCatchupPush(ctx context.Context, pgDataDirectory string, fromLSN LSN) {
	uploader, err := internal.ConfigureUploader()
	tracelog.ErrorLogger.FatalOnError(err)

	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)

	fakePreviousBackupSentinelDto := BackupSentinelDto{
		BackupStartLSN: &fromLSN,
	}

	extendExcludedFiles()

	userData, err := internal.GetSentinelUserData()
	tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

	backupArguments := NewBackupArguments(
		uploader, pgDataDirectory, utility.CatchupPath, false,
		false, false, false,
		RegularComposer, NewCatchupDeltaBackupConfigurator(fakePreviousBackupSentinelDto),
		userData, false)
	if orioledb.IsEnabled(pgDataDirectory) {
		tracelog.InfoLogger.Printf("Catchup incremental backup is not implemented for orioledb. Full backup will be performed.")
	}

	backupHandler, err := NewBackupHandler(backupArguments)
	tracelog.ErrorLogger.FatalOnError(err)
	backupHandler.HandleBackupPush(ctx)
}
