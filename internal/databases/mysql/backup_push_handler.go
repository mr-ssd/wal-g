package mysql

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
)

//nolint:funlen
func HandleBackupPush(
	folder storage.Folder,
	uploader internal.Uploader,
	backupCmd *exec.Cmd,
	isPermanent bool,
	isFullBackup bool,
	fifoStreams int,
	userDataRaw string,
	deltaBackupConfigurator DeltaBackupConfigurator,
) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
		tracelog.WarningLogger.Printf("Failed to obtain the OS hostname")
	}

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	version, err := getMySQLVersion(db)
	tracelog.ErrorLogger.FatalOnError(err)

	flavor, err := getMySQLFlavor(db)
	tracelog.ErrorLogger.FatalOnError(err)

	serverUUID, err := getServerUUID(db, flavor)
	tracelog.ErrorLogger.FatalOnError(err)

	gtidStart, err := getMySQLGTIDExecuted(db, flavor)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogStart, err := getLastUploadedBinlogBeforeGTID(folder, gtidStart, flavor)
	tracelog.ErrorLogger.FatalfOnError("failed to get last uploaded binlog: %v", err)
	timeStart := utility.TimeNowCrossPlatformLocal()

	var backupName string
	var prevBackupInfo PrevBackupInfo
	var incrementCount int
	var xtrabackupInfo XtrabackupInfo
	if isXtrabackup(backupCmd) {
		prevBackupInfo, incrementCount, err = deltaBackupConfigurator.Configure(isFullBackup, hostname, serverUUID, version)
		tracelog.ErrorLogger.FatalfOnError("failed to get previous backup for delta backup: %v", err)

		backupName, xtrabackupInfo, err = handleXtrabackupBackup(uploader, backupCmd, isFullBackup, fifoStreams, &prevBackupInfo)
	} else {
		backupName, err = handleRegularBackup(uploader, backupCmd)
	}
	tracelog.ErrorLogger.FatalfOnError("backup create command failed: %v", err)

	binlogEnd, err := getLastUploadedBinlog(folder)
	tracelog.ErrorLogger.FatalfOnError("failed to get last uploaded binlog (after): %v", err)
	timeStop := utility.TimeNowCrossPlatformLocal()

	uploadedSize, err := uploader.UploadedDataSize()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to calc uploaded data size: %v", err)
	}

	rawSize, err := uploader.RawDataSize()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to calc raw data size: %v", err)
	}

	userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
	tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

	var incrementFrom *string
	if (prevBackupInfo != PrevBackupInfo{}) {
		incrementFrom = &prevBackupInfo.name
	}

	var tool = WalgUnspecifiedStreamBackupTool
	if isXtrabackup(backupCmd) {
		tool = WalgXtrabackupTool
	}

	sentinel := StreamSentinelDto{
		Tool:              tool,
		BinLogStart:       binlogStart,
		BinLogEnd:         binlogEnd,
		StartLocalTime:    timeStart,
		StopLocalTime:     timeStop,
		CompressedSize:    uploadedSize,
		UncompressedSize:  rawSize,
		Hostname:          hostname,
		ServerUUID:        serverUUID,
		ServerVersion:     version,
		IsPermanent:       isPermanent,
		IsIncremental:     incrementCount != 0,
		UserData:          userData,
		LSN:               xtrabackupInfo.ToLSN,
		IncrementFromLSN:  xtrabackupInfo.FromLSN,
		IncrementFrom:     incrementFrom,
		IncrementFullName: prevBackupInfo.fullBackupName,
		IncrementCount:    &incrementCount,
	}
	tracelog.InfoLogger.Printf("Backup sentinel: %s", sentinel.String())

	err = internal.UploadSentinel(uploader, &sentinel, backupName)
	tracelog.ErrorLogger.FatalOnError(err)
}

func handleRegularBackup(uploader internal.Uploader, backupCmd *exec.Cmd) (backupName string, err error) {
	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	backupName, err = uploader.PushStream(context.Background(), limiters.NewDiskLimitReader(stdout))
	tracelog.ErrorLogger.FatalfOnError("failed to push backup: %v", err)

	err = backupCmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Backup command output:\n%s", stderr.String())
	}
	return
}

func handleXtrabackupBackup(
	uploader internal.Uploader,
	backupCmd *exec.Cmd,
	isFullBackup bool,
	fifoStreams int,
	prevBackupInfo *PrevBackupInfo,
) (backupName string, backupInfo XtrabackupInfo, err error) {
	if prevBackupInfo == nil {
		tracelog.ErrorLogger.Fatalf("PrevBackupInfo is null")
	}

	tmpDirRoot := "/tmp" // There is no Percona XtraBackup for Windows (c) @PeterZaitsev
	xtrabackupExtraDirectory, err := prepareTemporaryDirectory(tmpDirRoot)
	tracelog.ErrorLogger.FatalfOnError("failed to prepare tmp directory for diff-backup: %v", err)

	enrichBackupArgs(backupCmd, xtrabackupExtraDirectory, isFullBackup, fifoStreams, prevBackupInfo)
	tracelog.InfoLogger.Printf("Command to execute: %v", strings.Join(backupCmd.Args, " "))

	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	if fifoStreams == 1 {
		backupName, err = uploader.PushStream(context.Background(), limiters.NewDiskLimitReader(stdout))
		tracelog.ErrorLogger.FatalfOnError("failed to push backup: %v", err)
	} else {
		backupName = internal.NewBackupStreamName()
		errGroup, groupContext := errgroup.WithContext(context.Background())
		for idx := 0; idx < fifoStreams; idx++ {
			idx := idx
			threadUploader := uploader.Clone()
			fifoFileName := getXtrabackupFifoFileName(idx)
			file, err := openFifoFile(fifoFileName)
			tracelog.ErrorLogger.FatalfOnError("failed to open named pipe: %v", err)

			errGroup.Go(func() error {
				return threadUploader.PushStreamToDestination(
					groupContext,
					limiters.NewDiskLimitReader(file),
					fmt.Sprintf("%s/thread_%v", backupName, idx),
				)
			})
		}
		err = errGroup.Wait()
		tracelog.ErrorLogger.FatalfOnError("failed to push backup: %v", err)
	}

	err = backupCmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Backup command output:\n%s", stderr.String())
	}

	backupInfo, err = readXtrabackupInfo(xtrabackupExtraDirectory)
	if err != nil {
		tracelog.WarningLogger.Printf("failed to read and parse `xtrabackup_checkpoints`: %v", err)
	}

	err = removeTemporaryDirectory(xtrabackupExtraDirectory)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to remove tmp directory from diff-backup: %v", err)
		err = nil // don't crash an app
	}

	return backupName, backupInfo, err
}
