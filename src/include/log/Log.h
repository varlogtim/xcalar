// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LOG_H_
#define _LOG_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "runtime/Spinlock.h"
#include "stat/StatisticsTypes.h"

namespace xcalar
{
namespace internal
{
namespace durable
{
class DurableObject;
}
}
};

class LogLib
{
    friend class LibDurable;
    friend class LogGenTestData;

  public:
    static constexpr const char *RecoveryDirName = "recovery";
    static constexpr const char *UdfWkBookDirName = "udfs";
    static constexpr const char *DataflowWkBookDirName = "dataflows";
    static constexpr uint64_t LogDefaultFileSize = 4 * MB;
    static constexpr unsigned LogDefaultFileCount = 2;

    char resultSetExportPath_[XcalarApiMaxPathLen + 1];
    char recoveryLogPath_[XcalarApiMaxPathLen + 1];
    char wkbkPath_[XcalarApiMaxPathLen + 1];
    char sharedUDFsPath_[XcalarApiMaxPathLen + 1];
    char targetPath_[XcalarApiMaxPathLen + 1];
    char retinaPath_[XcalarApiMaxPathLen + 1];
    char publishedTablePath_[XcalarApiMaxPathLen + 1];
    char appPersistPath_[XcalarApiMaxPathLen + 1];
    char licenseDirPath_[XcalarApiMaxPathLen + 1];
    char jupyterPath_[XcalarApiMaxPathLen + 1];
    char dfStatsDirPath_[XcalarApiMaxPathLen + 1];

    // These enums index an array of DsSource-s (or directories for most
    // practical purposes) and hence must be sequential. The next step is to
    // create the directory in logNew(). Directory creation is done using
    // statically chosen dir names for now. All directories for logging are
    // created under ResultSetExportPath, the standard system root path.
    // Note: if you need a new direcory foo under ResultSetExportPath then
    // add foo here as an enum.
    typedef enum {
        XcalarRootDirIndex,
        SessionsDirIndex,
        ResultSetExportDirIndex,
        RecoveryDirIndex,
        WorkBookDirIndex,
        SharedUDFsDirIndex,
        TargetDirIndex,
        AppPersistDirIndex,
        PublishedDirIndex,
        JupyterDirIndex,
        LicenseDirIndex,
        DataflowStatsHistoryDirIndex,
        MaxDirsDoNotAddAfterThisEnum,
    } DirIndex;

    // Position within log. Fields should be considered private to log module.
    typedef struct Cursor {
        uint32_t fileNum;  // "Index" into backing file set.
        off_t fileOffset;  // Offset within file.
    } Cursor;

    // Represents a single backing file for a particular log. A log consists of
    // a numbered set of backing files and a meta file. The meta file tells
    // which backing file is the "head" of the log.
    typedef struct BackingFile {
        uint32_t fileNum;  // "Index" into backing file set.
        size_t fileSize;   // Current size of this file, 0 if not fixed
        int fd;            // Keep all files open to avoid errors.
        struct BackingFile *prev;
        struct BackingFile *next;
        char fileName[0];
    } BackingFile;

    typedef struct Handle {
        uint64_t nextWriteSeqNum;
        uint64_t lastReadSeqNum;
        bool positionedAtEnd;
        struct Handle *prev;  // For Log.c's list of open log handles.
        struct Handle *next;
        char *filePrefix;

        BackingFile *backingFilesList;
        BackingFile *currentFile;
        int metaFd;

        Cursor head;
        Cursor lastWritten;

        // Log initialization parameters.  Only backingFileSize should ever
        // be updated after log open.
        bool expandIfNeeded;  // Create larger backing files if needed
        DirIndex dirIndex;    // Log file source/directory
        uint32_t backingFileCount;
        size_t backingFileSize;  // Largest backing file size
    } Handle;

    // Log file creation bit flags that can be OR-ed and passed to create().
    // One of FileSeekToStart or FileSeekToLogicalEnd must be passed to
    // create(). Logical end refers to the last correct logical block
    // written.
    typedef enum {
        FileReturnErrorIfExists = 0x1,
        FileSeekToStart = 0x2,
        FileSeekToLogicalEnd = 0x4,
        FileDontPreallocate = 0x8,  // only works with fileCount==1
        FileExpandIfNeeded = 0x10,  // no effect if FileDontPreallocate
        FileNoCreate = 0x20,        // Don't create file if it doesn't exist
    } FileCreateMode;

    typedef uint64_t FileCreateModeMask;

    // ReadFromStart - start reading from the beginning of the file.
    // ReadNext - read from current offset.
    typedef enum { ReadFromStart, ReadNext } ReadMode;

    typedef uint64_t WriteOptionsMask;

    typedef enum {
        // individual flags
        WriteNone = 0x0,
        WriteOverwrite = 0x1,

        // masks
        WriteOptionsMaskDefault = WriteNone,
    } WriteOption;

    static MustCheck Status createSingleton();
    static void deleteSingleton();
    static MustCheck LogLib *get();

    MustCheck Status create(Handle *log,
                            DirIndex dirIndex,
                            const char *filePrefix,
                            FileCreateModeMask modeMask,
                            uint32_t createFileCount,
                            size_t createFileSize);

    void close(Handle *log);

    MustCheck Status fileDelete(DirIndex dirIndex, const char *filePrefix);

    MustCheck Status closeAndDelete(Handle *log);

    MustCheck Status
    writeRecord(Handle *log,
                void *bufIn,
                size_t bufSize,
                WriteOptionsMask optionsMask = WriteOptionsMaskDefault);

    MustCheck Status readRecord(Handle *log,
                                void *bufIn,
                                size_t bufSize,
                                size_t *bytesReadOut,
                                ReadMode logReadMode);

    MustCheck Status readLastLogicalRecord(Handle *log,
                                           void *bufIn,
                                           size_t bufSize,
                                           size_t *bytesReadOut);

    MustCheck Status seekToBeginning(Handle *log);

    MustCheck size_t getMinLogicalBlockAlignment();

    void getLastWritten(Handle *log, Cursor *tail);

    MustCheck Status resetHead(Handle *log, Cursor *head);

    MustCheck const char *getDirPath(DirIndex dirIndex);

    MustCheck bool isEmpty(Handle *log) { return log->nextWriteSeqNum == 1; };

    // XXX: The following methods should be moved to pivate once the LogDump,
    // LogTest LibLogSanity.cpp becomes a friend class of LogLib
    typedef struct {
        // First 32 bits of LogSetMeta must always indicate version.
        uint32_t logSetVersion;
        uint32_t logSetStartFile;
        uint64_t logSetStartOffset;

        uint8_t reserved[240];
    } LogSetMeta;

    enum {
        LogHeaderMagic = 0x524448474f4cull,
        LogFooterMagic = 0x525446474f4cull,
        LogNextFileMagic = 0x54584e474f4cull,
        LogEndMagic = 0x444e45474f4cull,
        LogMaximumEntrySize = 8ull * GB,
        LogDirLength = 1023,

        LogInitLastReadSeqNum = 0,
        LogInitNextWriteSeqNum = LogInitLastReadSeqNum + 1
    };

    typedef struct LogHeader {
        uint64_t magic;
        uint64_t majVersion;
        uint64_t headerSize;
        uint64_t dataSize;
        uint64_t footerSize;
        uint64_t seqNum;
        uint32_t pid;
        uint8_t reserved[12];
    } LogHeader;

    typedef struct LogFooter {
        uint64_t magic;
        uint64_t entrySize;  // hdr.headerSize + hdr.dataSize + hdr.footerSize
        uint8_t reserved[40];
        uint64_t checksum;  // must always be last
    } LogFooter;

    // See comment for DirIndex.
    char logDirPaths[MaxDirsDoNotAddAfterThisEnum][LogDirLength + 1];

    MustCheck Status loadMetaFile(const char *dirPath,
                                  const char *filePrefix,
                                  LogSetMeta *logSetMetaOut);

    MustCheck Status loadBackingFiles(const char *dirPath,
                                      const char *filePrefix,
                                      BackingFile **logSetBackingFilesList,
                                      FileCreateModeMask modeMask,
                                      uint32_t *fileCountOut,
                                      size_t *fileSizeOut);

    MustCheck Status logSeekToLogicalEnd(Handle *log,
                                         Cursor *lastRecordOut,
                                         bool *foundLastRecordOut);

    MustCheck BackingFile *removeHeadFromBackingFileList(
        BackingFile **listHead);

    MustCheck const char *getSessionDirName();
    void setSessionDirName(const char *newDirName);

  private:
    typedef enum {
        LogMajorVersion1 = 1,  // predates well defined log header; ignore
        LogMajorVersion2 = 2,  // checksums w/ hacked crc32
        LogMajorVersion3 = 3,  // weak checksum, never shipped
        LogMajorVersion4 = 4,  // Hardware (SSE4.2) crc32 checksum
    } LogMajorVersion;

    //
    // Types specific to LogTypeFixedCircular.
    //

    typedef enum {
        LogSetVersion1 = 1,
    } LogSetVersion;

    // Denominator of expansion size caluclation.  4 = increase by 25%
    static constexpr const int LogIncreaseFactor = 4;
    // Log file size modulo - all log files should be a multiple of this
    static constexpr const int LogFileSizeQuanta = 4096;

    // Suffix of LogSet metadata file name.
    static constexpr const char *metaStr = "meta";
    static constexpr const char *moduleName = "liblog";

    static constexpr const char *PublishedDirName = "published";
    static constexpr const char *TargetDirName = "targets";
    static constexpr const char *AppPersistDirName = "apps";
    // XXX: resolve any confusion b/w workbooks and sessions in <sharedRoot>
    // The UDFs will be the first ones under WorkBookDirName
    static constexpr const char *WorkBookDirName = "workbooks";
    static constexpr const char *SharedUDFsDirName = "sharedUDFs";
    static constexpr const char *ResultSetExportDirName = "export";
    static constexpr const char *LicenseDirName = "license";
    static constexpr const char *JupyterDirName = "jupyterNotebooks";
    static constexpr const char *DfStatsHistoryDirName = "DataflowStatsHistory";

    // Sessions directory.  Changed by some tests.
    //
    // XXX: This name is now unused except for upgrade to Dionysus (since
    // pre-Dio layouts had this dir). This field can be deleted in Eos
    // (assuming we wouldn't support upgrade from pre-Dionysus to Eos)
    char sessionDirName_[XcalarApiMaxPathLen + 1];
    static constexpr const char *SessionDirNameDefault = "sessions";

    // Returns the minimum overhead space log metadata occupies in a single log
    // set backing file.
    // Final uint64_t is for potential LogNextFileMagic or LogEndMagic.
    static constexpr const size_t LogOverheadSize =
        sizeof(LogHeader) + sizeof(LogFooter) + sizeof(uint64_t);

    MustCheck Status createInternal();
    MustCheck Status readRecordInternal(Handle *log,
                                        void **buf,
                                        size_t *bufSize,
                                        ReadMode logReadMode);
    MustCheck Status logReadNextHeader(Handle *log, LogHeader *header);
    MustCheck bool logIsValidHeader(Handle *log, const LogHeader *headerPtr);

    MustCheck bool logIsValidFooter(const LogFooter *footerPtr,
                                    const LogHeader *headerPtr,
                                    const uint8_t *dataPtr);

    MustCheck Status logInitStats();

    MustCheck Status logLoadOrCreateFiles(Handle *log,
                                          const char *dirPath,
                                          const char *filePrefix,
                                          FileCreateModeMask modeMask,
                                          LogSetMeta *logSetMetaOut,
                                          uint32_t createFileCount,
                                          size_t createFileSize);

    MustCheck Status logCreateInt(Handle *log,
                                  const char *dirPath,
                                  const char *filePrefix,
                                  FileCreateModeMask modeMask,
                                  uint32_t createFileCount,
                                  size_t createFileSize);

    MustCheck Status logSetReadBytes(Handle *log,
                                     uint8_t *buf,
                                     size_t bufSize,
                                     size_t *bytesReadOut);

    void removeFromLogHandleListWithLock(Handle **listHead, Handle *element);
    MustCheck Status createXcalarDirs();

    MustCheck Status fileReallocate(Handle *log,
                                    BackingFile *file,
                                    size_t newSize);

    // metaFileName consists of "<filePrefix>-<metaStr>".
    MustCheck size_t calculateMetaFileNameLen(const char *filePrefix);

    // Verifies that a file operation doesn't overflow the expected boundaries
    // of the current backing file (this ensures that the code is wrapping log
    // files when it should).
    void assertDontOverflowFile(Handle *log, size_t byteCount);

    MustCheck Handle *removeHeadFromLogHandleList(Handle **listHead);

    void logCloseInt(Handle *log);

    MustCheck Status logCheckForHeader(Handle *log);

    MustCheck Status parseBackingFileName(const char *filePrefixWithDelim,
                                          const char *fileName,
                                          uint32_t *fileNumOut);

    MustCheck Status logGetMetaFileName(const char *filePrefix,
                                        char *metaFileNameOut,
                                        size_t metaFileNameOutSize);

    void appendBackingFileList(BackingFile **listHead, BackingFile *element);

    void insertBeforeBackingFileList(BackingFile **listHead,
                                     BackingFile *curElt,
                                     BackingFile *newElt);

    MustCheck Status logSeekToNextFile(Handle *log, size_t bufferSize);

    MustCheck Status logSavePosition(Handle *log, Cursor *cursorOut);

    MustCheck Status logSeek(Handle *log, Cursor *cursor);

    MustCheck Status createMetaFile(const char *dirPath,
                                    const char *filePrefix,
                                    LogSetMeta *logSetMetaOut);

    MustCheck Status createBackingFiles(const char *dirPath,
                                        const char *filePrefix,
                                        BackingFile **logSetBackingFiles,
                                        uint32_t createFileCount,
                                        size_t createFileSize,
                                        FileCreateModeMask modeMask);

    MustCheck Status setCurrentFile(Handle *log, uint32_t fileNum);

    MustCheck Status logFileGetRemainingBytes(Handle *log, size_t *bytes);

    MustCheck Status logWriteBytes(Handle *log, uint8_t *buf, size_t bufSize);

    MustCheck Status convergentRead(int fd,
                                    void *bufIn,
                                    size_t numBytes,
                                    size_t *bytesRead);

    MustCheck Status readLastLogicalRecordInt(Handle *log);

    static bool logIsInit;
    static LogLib *logLib;

    Spinlock openHdlLock;
    Handle *openHandleList;

    const size_t minLogicalBlockAlignment = sizeof(uint64_t);

    // Global stats.
    // XXX Some of these would be more useful per-instance stats.
    StatGroupId statGroupId;
    StatHandle statOpenLogCount;
    StatHandle statOpenLogCountMaxReached;
    StatHandle statLogWrites;
    StatHandle statLogReads;
    StatHandle statLogHeadResets;
    StatHandle statLogFailedReads;
    StatHandle statLogFailedWrites;

    std::string xceVersion;

    LogLib(){};
    ~LogLib(){};

    LogLib(const LogLib &) = delete;
    LogLib &operator=(const LogLib &) = delete;
};

#endif  // _LOG_H_
