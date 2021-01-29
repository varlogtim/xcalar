// Copyright 2015-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <assert.h>
#include <err.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/mman.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "hash/Hash.h"
#include "common/InitTeardown.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "LogGenTestData.h"
#include "LogDump.h"
#include "lmdb.h"
#include "strings/String.h"
#include "kvstore/KvStore.h"
#include "kvstore/KvStoreTypes.h"
#include "util/FileUtils.h"

// Don't worry about this for the LogDump case
constexpr bool enableLmdbApi_ = true;

// Represents a single open and memory mapped backing file from a log set.
typedef struct {
    int fd;
    uint8_t *content;
    size_t size;
} LdOpenLogSetFile;

MustCheck Status dumpLMDB(const char *mdbPath, const char *outdir);

static void
printUsage(void)
{
    fprintf(stdout,
            "Usage:\n\tlogdump --directory <logDirectory> "
            "--prefix <filePrefix> [--seq <seqNum>] [--content] "
            "[--cfg <cfgFile>] [--gentest] [--verbose] "
            "[--printXlrDir] [--dumplmdb]\n");
}

static Status
parseOptions(int argc, char *argv[], LdOptions *options)
{
    int optionIndex = 0;
    int flag;

    struct option longOptions[] = {
        {"directory", required_argument, 0, 'd'},
        {"prefix", required_argument, 0, 'p'},
        {"seq", required_argument, 0, 's'},
        {"cfg", required_argument, 0, 'C'},
        {"content", no_argument, 0, 'c'},
        {"gentest", no_argument, 0, 'g'},
        {"printXlrDir", no_argument, 0, 'P'},
        {"verbose", no_argument, 0, 'v'},
        {"dumplmdb", no_argument, 0, 'D'},
        {0, 0, 0, 0},
    };

    options->directory[0] = '\0';
    options->prefix[0] = '\0';
    strlcpy(options->cfgFile,
            "src/lib/tests/test-config.cfg",
            sizeof(options->cfgFile));
    options->seqNum = 0;
    options->content = false;
    options->genTest = false;
    options->printXlrDir = false;
    options->verbose = false;
    options->dumplmdb = false;

    while ((flag = getopt_long(argc,
                               argv,
                               "cC:gp:d:s:v",
                               longOptions,
                               &optionIndex)) != -1) {
        switch (flag) {
        case 'c':
            options->content = true;
            break;
        case 'C':
            strlcpy(options->cfgFile, optarg, sizeof(options->cfgFile));
            break;
        case 'D':
            options->dumplmdb = true;
            break;
        case 'g':
            options->genTest = true;
            break;
        case 'p':
            strlcpy(options->prefix, optarg, sizeof(options->prefix));
            break;
        case 'P':
            options->printXlrDir = true;
            break;
        case 'd':
            strlcpy(options->directory, optarg, sizeof(options->directory));
            break;
        case 's':
            options->seqNum = strtoull(optarg, NULL, 0);
            break;
        case 'v':
            options->verbose = true;
            break;
        default:
            fprintf(stderr, "Unknown arg %c\n", flag);
            printUsage();
            // @SymbolCheckIgnore
            exit(1);
            break;
        }
    }

    return StatusOk;
}

static void
appendToPath(const char *directory,
             const char *fileName,
             char *pathOut,
             size_t pathOutSize)
{
    snprintf(pathOut, pathOutSize, "%s/%s", directory, fileName);
}

// Map a specific log set backing file into memory.
static uint8_t *
mmapLogSetFile(LogLib::Handle *log,
               uint32_t fileNum,
               const LdOptions *options,
               LdOpenLogSetFile *newFile)
{
    LogLib::BackingFile *current = log->backingFilesList;

    while (current != NULL) {
        if (current->fileNum == fileNum) {
            char path[PATH_MAX];
            struct stat statBuf;

            int fd;
            uint8_t *content;

            strlcpy(path, current->fileName, PATH_MAX);

            int ret = stat(path, &statBuf);
            assert(ret != -1);

            fd = open(path, O_RDONLY);
            assert(fd != -1);

            content = (uint8_t *)
                memMap(NULL, statBuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
            if (content == MAP_FAILED) {
                fprintf(stderr, "Failed to map file '%s'.\n", path);
                assert(false);
            }

            newFile->fd = fd;
            newFile->content = content;
            newFile->size = statBuf.st_size;
            log->currentFile = current;

            fprintf(stdout,
                    "\nFILE: %s [ size:0x%010llx ]\n",
                    path,
                    (unsigned long long) statBuf.st_size);

            return content;
        }

        current = current->next;
        if (current == log->backingFilesList) {
            break;
        }
    }

    // Didn't find fileNum.
    assert(false);
    return NULL;
}

// Map the given log set's next backing file into memory.
static void
mmapNextLogSetFile(LogLib::Handle *log,
                   const LdOptions *options,
                   LdOpenLogSetFile *file)
{
    uint32_t nextFileNum;

    LogLib::BackingFile *nextFile = log->currentFile->next;

    if (nextFile == NULL) {
        nextFileNum = 0;
    } else {
        nextFileNum = nextFile->fileNum;
    }

    // Close previous file.
    memUnmap(file->content, file->size);
    close(file->fd);

    mmapLogSetFile(log, nextFileNum, options, file);
}

// Validates there's room enough to parse a record type.
static inline void
validateCanParseRecordType(LdOpenLogSetFile *currentFile,
                           unsigned long long posInFile)
{
    if (posInFile + sizeof(uint64_t) > currentFile->size) {
        fprintf(stderr,
                "0x%010llx type:*HDR*? Not enough bytes for header.\n",
                posInFile);
        // @SymbolCheckIgnore
        exit(1);
    }
}

// Scans through data portion of file looking for magic numbers which can
// indicate a misplaced header/footer/end.
static inline void
scanDataForMagicNums(const uint8_t *buf,
                     size_t bufLen,
                     unsigned long long fileBase)
{
    LogLib *logLib = LogLib::get();

    const uint64_t *bufAligned = (uint64_t *) buf;
    const size_t alignment = logLib->getMinLogicalBlockAlignment();

    assert((size_t) buf % alignment == 0);

    for (unsigned i = 0; i < bufLen / sizeof(bufAligned[0]); i++) {
        char *blockFound = NULL;
        switch (bufAligned[i]) {
        case LogLib::LogEndMagic:
            blockFound = (char *) "END";
            break;
        case LogLib::LogHeaderMagic:
            blockFound = (char *) "HDR";
            break;
        case LogLib::LogNextFileMagic:
            blockFound = (char *) "NXT";
            break;
        case LogLib::LogFooterMagic:
            blockFound = (char *) "FTR";
            break;
        default:
            continue;
        }

        assert(blockFound != NULL);
        fprintf(stderr,
                "0x%010llx Warning: %s found in data block.\n",
                (unsigned long long) &bufAligned[i] - fileBase,
                blockFound);
    }
}

// Based on given options, should this record be printed?
static inline bool
shouldPrintThis(const LdOptions *options, const LogLib::LogHeader *header)
{
    return options->seqNum == 0 || (options->seqNum == header->seqNum);
}

// Parse record type from memory mapped file.
static inline unsigned long long
parseRecordType(LdOpenLogSetFile *currentFile, unsigned long long posInFile)
{
    validateCanParseRecordType(currentFile, posInFile);

    return *(uint64_t *) &currentFile->content[posInFile];
}

static void
traverseLog(LogLib::Handle *log,
            LogLib::LogSetMeta *meta,
            const LdOptions *options)
{
    LdOpenLogSetFile currentFile;
    unsigned long long posInFile;
    const size_t alignment = LogLib::get()->getMinLogicalBlockAlignment();

    // Variables used to track state from one header to another.
    uint32_t prevPid = 0;
    uint64_t prevSeqNum = 0;

    mmapLogSetFile(log, meta->logSetStartFile, options, &currentFile);
    posInFile = meta->logSetStartOffset;

    while (true) {
        unsigned long long recordType;

        //
        // At beginning of loop, read in and validate log header.
        //

        recordType = parseRecordType(&currentFile, posInFile);

        if (recordType != LogLib::LogHeaderMagic) {
            fprintf(stderr,
                    "0x%010llx type:*HDR*? Expected:(0x%010llx) "
                    "Actual:(0x%010llx)\n",
                    posInFile,
                    (unsigned long long) LogLib::LogHeaderMagic,
                    recordType);
            // @SymbolCheckIgnore
            exit(1);
        }

        LogLib::LogHeader *header =
            (LogLib::LogHeader *) &currentFile.content[posInFile];

        if (prevPid == 0) {
            prevPid = header->pid;
        }

        if (posInFile + header->headerSize > currentFile.size) {
            fprintf(stderr,
                    "0x%010llx type:*HDR*? Not enough bytes for header.\n",
                    posInFile);
            // @SymbolCheckIgnore
            exit(1);
        }

        if (shouldPrintThis(options, header)) {
            fprintf(stdout,
                    "0x%010llx type:%s ver:%llu hdrSize:%llu "
                    "dataSize:%llu ftrSize:%llu pid:%u(%s) seqNum:%u(%s)\n",
                    posInFile,
                    "HDR",
                    (unsigned long long) header->majVersion,
                    (unsigned long long) header->headerSize,
                    (unsigned long long) header->dataSize,
                    (unsigned long long) header->footerSize,
                    (unsigned) header->pid,
                    prevPid == header->pid ? "ok" : "*changed*",
                    (unsigned) header->seqNum,
                    header->seqNum > prevSeqNum ? "ok" : "*out-of-order*");
        }

        prevPid = header->pid;
        prevSeqNum = header->seqNum;

        //
        // Check for data.
        //

        posInFile += header->headerSize;

        if (posInFile + header->dataSize > currentFile.size) {
            fprintf(stderr,
                    "0x%010llx Not enough bytes for data.\n",
                    posInFile);
        }

        scanDataForMagicNums(&currentFile.content[posInFile],
                             header->dataSize,
                             (unsigned long long) currentFile.content);

        // Should this data be printed?
        if (shouldPrintThis(options, header)) {
            if (options->content) {
                // Print each byte of data.
                // XXX Consider truncating lengthy output.
                for (unsigned i = 0; i < header->dataSize; i++) {
                    fprintf(stdout, "%c", currentFile.content[posInFile + i]);
                }
                fprintf(stdout, "\n");
            }
        }

        posInFile += header->dataSize;

        //
        // Expecting footer. Validate.
        //

        recordType = parseRecordType(&currentFile, posInFile);

        if (recordType != LogLib::LogFooterMagic) {
            fprintf(stderr,
                    "0x%010llx type:*FTR*? Expected:(0x%010llx) "
                    "Actual:(0x%010llx)\n",
                    posInFile,
                    (unsigned long long) LogLib::LogFooterMagic,
                    recordType);
            // @SymbolCheckIgnore
            exit(1);
        }

        if (posInFile + header->footerSize > currentFile.size) {
            fprintf(stderr,
                    "0x%010llx type:*HDR*? Not enough bytes for header.\n",
                    posInFile);
            // @SymbolCheckIgnore
            exit(1);
        }

        LogLib::LogFooter *footer =
            (LogLib::LogFooter *) &currentFile.content[posInFile];

        uint32_t checksum = hashCrc32c(0, header, header->headerSize);
        checksum = hashCrc32c(checksum, &header[1], header->dataSize);
        checksum = hashCrc32c(checksum,
                              footer,
                              (header->footerSize - sizeof(footer->checksum)));

        bool footerXSumMatch = checksum == footer->checksum;
        bool footerSizeMatch = (header->headerSize + header->dataSize +
                                header->footerSize) == footer->entrySize;

        if (shouldPrintThis(options, header) || !footerXSumMatch ||
            !footerSizeMatch) {
            fprintf(stdout,
                    "0x%010llx type:%s entrySize:%llu(%s) "
                    "xsum:0x%016lx(%s)\n",
                    posInFile,
                    "FTR",
                    (unsigned long long) footer->entrySize,
                    footerSizeMatch ? "matches" : "*mistmatch*",
                    footer->checksum,
                    footerXSumMatch ? "valid" : "*corrupt*");
        }

        assert(mathIsAligned(header, alignment));
        assert(mathIsAligned(footer, alignment));
        assert(mathIsAligned(&header[1], alignment));

        posInFile += header->footerSize;

        //
        // Parse whatevers after the footer.
        //

        recordType = parseRecordType(&currentFile, posInFile);

        switch (recordType) {
        case LogLib::LogHeaderMagic:
            // At starting point of next iteration of loop.
            break;

        case LogLib::LogEndMagic:
            if (options->seqNum == 0) {
                fprintf(stdout, "0x%010llx type:%s\n", posInFile, "END");
            }
            goto CommonExit;

        case LogLib::LogNextFileMagic:
            fprintf(stdout,
                    "0x%010llx type:%s skipping:0x%010llx\n",
                    posInFile,
                    "NXT",
                    (unsigned long long) currentFile.size - posInFile);

            // Go to next file.
            mmapNextLogSetFile(log, options, &currentFile);
            posInFile = 0;

            if (log->currentFile->fileNum == meta->logSetStartFile) {
                fprintf(stderr,
                        "Wrapped back to log set beginning without "
                        "finding log end.");
                // @SymbolCheckIgnore
                exit(1);
            }
            break;

        default:
            fprintf(stderr,
                    "0x%010llx Unknown data 0x%010llx.",
                    posInFile,
                    recordType);
            // @SymbolCheckIgnore
            exit(1);
            break;
        }

        // At this point, posInFile either points to a potentially valid header
        // or the beginning of the next file. Either way, go back to the start
        // of the loop and expect a header.
    }

CommonExit:
    // Close last file.
    memUnmap(currentFile.content, currentFile.size);
    close(currentFile.fd);
}

static void
printLogMeta(LogLib::LogSetMeta *meta)
{
    fprintf(stdout,
            "LogMeta: version(%d) startFile(%d)\n",
            meta->logSetVersion,
            meta->logSetStartFile);
}

Status
mdbCkHelper(int err, const char *file, const uint64_t line)
{
    if (err == MDB_SUCCESS) {
        return StatusOk;
    }

    fprintf(stderr,
            "CallerFile %s: CallerLine %lu: LMDB Error: %s\n",
            file,
            line,
            mdb_strerror(err));

    assert(false);
    return StatusLMDBError;
}

Status
writeKv(const char *outdir, const char *subdir, MDB_val *mdbKey, MDB_val *data)
{
    Status status;
    char fqpn[XcalarApiMaxPathLen];
    FILE *fh = NULL;
    size_t elmsWritten;
    // Copy the key as we may modify it
    char key[mdbKey->mv_size];

    // Convert any "/"s to "_" in the key as it's being written to the file
    // system as a file.  We don't want the key to be treated as a file path.
    memcpy(key, mdbKey->mv_data, mdbKey->mv_size);
    for (unsigned ii = 0; ii < mdbKey->mv_size; ii++) {
        if (key[ii] == '/') {
            key[ii] = '_';
        }
    }

    if (subdir == NULL) {
        status = strSnprintf(fqpn,
                             sizeof(fqpn),
                             "%s/%.*s.dump",
                             outdir,
                             (int) mdbKey->mv_size,
                             key);
        BailIfFailed(status);
    } else {
        status = strSnprintf(fqpn, sizeof(fqpn), "%s/%s", outdir, subdir);
        BailIfFailed(status);
        status = FileUtils::recursiveMkdir(fqpn, 0750);
        BailIfFailed(status);
        status = strSnprintf(fqpn,
                             sizeof(fqpn),
                             "%s/%s/%.*s.dump",
                             outdir,
                             subdir,
                             (int) mdbKey->mv_size,
                             key);
        BailIfFailed(status);
    }

    fh = fopen(fqpn, "w");
    BailIfNull(fh);
    elmsWritten = fwrite(data->mv_data, data->mv_size, 1, fh);
    if (elmsWritten != 1) {
        status = StatusUnderflow;
        goto CommonExit;
    }

CommonExit:
    if (fh != NULL) {
        fclose(fh);
        fh = NULL;
    }

    return status;
}

Status
dumpLMDBInner(MDB_txn *txn,
              MDB_dbi *dbiInner,
              const char *outdir,
              const char *subdir)
{
    Status status;
    int retval;
    MDB_val mdbKey, data;
    MDB_cursor *cursor = NULL;

    status = mdbCk(mdb_cursor_open(txn, *dbiInner, &cursor));
    BailIfFailed(status);
    while ((retval = mdb_cursor_get(cursor, &mdbKey, &data, MDB_NEXT)) == 0) {
        status = writeKv(outdir, subdir, &mdbKey, &data);
        BailIfFailed(status);
    }

    if (retval != MDB_NOTFOUND) {
        status = mdbCk(retval);
        BailIfFailed(status);
    }

CommonExit:
    if (cursor != NULL) {
        mdb_cursor_close(cursor);
        cursor = NULL;
    }

    return status;
}

Status
dumpLMDB(const char *mdbPath, const char *outdir)
{
    Status status;
    int retval;
    MDB_txn *txn = NULL;
    MDB_env *mdbEnv = NULL;
    MDB_cursor *cursor = NULL;
    MDB_dbi dbi = KvStore::MdbDbiInvalid;
    MDB_dbi dbiInner = KvStore::MdbDbiInvalid;
    MDB_val mdbKey, data;

    if (access(mdbPath, F_OK) == -1) {
        // Avoid LMDB creating a new environment if one doesn't exist already
        status = StatusInval;
        goto CommonExit;
    }

    status = mdbCk(mdb_env_create(&mdbEnv));
    BailIfFailed(status);
    status = mdbCk(mdb_env_set_maxdbs(mdbEnv, KvStore::MaxDBs));
    BailIfFailed(status);
    if (strstr(mdbPath, KvStoreLib::KvGlobalStoreName) != NULL) {
        status = mdbCk(
            mdb_env_set_mapsize(mdbEnv,
                                XcalarConfig::get()->maxKvsSzGlobalMB_ * MB));
        BailIfFailed(status);
    } else {
        status = mdbCk(
            mdb_env_set_mapsize(mdbEnv,
                                XcalarConfig::get()->maxKvsSzWorkbookMB_ * MB));
        BailIfFailed(status);
    }
    status = mdbCk(mdb_env_open(mdbEnv, mdbPath, MDB_RDONLY, 0));
    BailIfFailed(status);
    status = mdbCk(mdb_txn_begin(mdbEnv, NULL, MDB_RDONLY, &txn));
    BailIfFailed(status);

    // Open the outer database, which can contain multiple inner databases
    status = mdbCk(mdb_dbi_open(txn, NULL, 0, &dbi));
    BailIfFailed(status);
    status = mdbCk(mdb_cursor_open(txn, dbi, &cursor));
    BailIfFailed(status);
    while ((retval = mdb_cursor_get(cursor, &mdbKey, &data, MDB_NEXT)) == 0) {
        // Will succeed if the key names a database rather than a value
        int ret =
            mdb_dbi_open(txn, (const char *) mdbKey.mv_data, 0, &dbiInner);
        if (ret == MDB_NOTFOUND) {
            // This key names a value, so just write it out at top-level
            status = writeKv(outdir, NULL, &mdbKey, &data);
            BailIfFailed(status);
            continue;
        } else {
            status = mdbCk(ret);
            BailIfFailed(status);
            // This key names a database, so open it and iterate over it
            status = dumpLMDBInner(txn,
                                   &dbiInner,
                                   outdir,
                                   (const char *) mdbKey.mv_data);
            BailIfFailed(status);
        }
    }

    if (retval != MDB_NOTFOUND) {
        status = mdbCk(retval);
        BailIfFailed(status);
    }

    status = mdbCk(mdb_txn_commit(txn));
    BailIfFailed(status);
    txn = NULL;

CommonExit:
    if (cursor != NULL) {
        mdb_cursor_close(cursor);
        cursor = NULL;
    }

    if (txn != NULL) {
        assert(status != StatusOk);
        mdb_txn_abort(txn);
        txn = NULL;
    }

    if (mdbEnv != NULL) {
        if (dbiInner != KvStore::MdbDbiInvalid) {
            mdb_dbi_close(mdbEnv, dbiInner);
            dbiInner = KvStore::MdbDbiInvalid;
        }

        if (dbi != KvStore::MdbDbiInvalid) {
            mdb_dbi_close(mdbEnv, dbi);
            dbi = KvStore::MdbDbiInvalid;
        }

        mdb_env_close(mdbEnv);
        mdbEnv = NULL;
    }

    return status;
}

int
main(int argc, char *argv[])
{
    Status status;
    LdOptions options;
    LogLib::LogSetMeta meta;
    char path[PATH_MAX];
    LogLib::Handle log;
    unsigned count;
    size_t fileSize;
    size_t directoryLen;
    LogLib *logLib = NULL;
    LogGenTestData dataGen;

    char fullCfgFilePath[PATH_MAX];
    char *xlrDir = getenv("XLRDIR");
    log.backingFilesList = NULL;

    // Load command line arguments.
    verifyOk(parseOptions(argc, argv, &options));

    if (xlrDir) {
        snprintf(fullCfgFilePath,
                 sizeof(fullCfgFilePath),
                 "%s/%s",
                 xlrDir,
                 options.cfgFile);
    } else {
        strlcpy(fullCfgFilePath, options.cfgFile, sizeof(fullCfgFilePath));
    }

    if (options.dumplmdb) {
        status = dumpLMDB(options.prefix, options.directory);
        goto CommonExit;
    }

    status = InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityDiagnostic,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone);
    BailIfFailed(status);

    if (options.printXlrDir) {
        fprintf(stdout, "%s\n", XcalarConfig::get()->xcalarRootCompletePath_);
        goto CommonExit;
    }

    if (options.genTest) {
        if (options.prefix[0] == '\0') {
            fprintf(stderr, "Must specify prefix for test data.\n");
            printUsage();
            status = StatusInval;
            goto CommonExit;
        }
    } else {
        if (options.directory[0] == '\0' || options.prefix[0] == '\0') {
            fprintf(stderr,
                    "Must specify directory and prefix to identify log set.\n");
            printUsage();
            status = StatusInval;
            goto CommonExit;
        }
    }

    directoryLen = strlen(options.directory);
    assert(directoryLen > 0);
    if (options.directory[directoryLen - 1] == '/') {
        options.directory[directoryLen - 1] = '\0';
    }

    appendToPath("/", options.directory, path, sizeof(path));

    if (options.genTest) {
        status = dataGen.genFuncTestData(options.prefix, options.verbose);
        BailIfFailed(status);
        printf("See %s for generated test logs\n",
               XcalarConfig::get()->xcalarRootCompletePath_);
        goto CommonExit;
    }

    logLib = LogLib::get();
    // Attempt to load specified log set.
    status = logLib->loadMetaFile(path, options.prefix, &meta);
    if (status != StatusOk) {
        fprintf(stderr,
                "Failed to load log metadata file '%s': %s.",
                path,
                strGetFromStatus(status));
        goto CommonExit;
    }

    printLogMeta(&meta);

    status = logLib->loadBackingFiles(path,
                                      options.prefix,
                                      &log.backingFilesList,
                                      LogLib::FileSeekToStart,
                                      NULL,
                                      NULL);
    if (status != StatusOk) {
        fprintf(stderr,
                "Failed to load log backing files: %s.",
                strGetFromStatus(status));
        goto CommonExit;
    }

    fprintf(stdout, "Backing files:\n");

    // Loop through and validate backing files.
    LogLib::BackingFile *currentFile;
    count = 0;
    fileSize = 0;
    currentFile = log.backingFilesList;

    while (currentFile != NULL) {
        struct stat statBuf;

        strlcpy(path, currentFile->fileName, PATH_MAX);

        if (currentFile->fileNum != count) {
            fprintf(stderr,
                    "\t%s ERROR Expected file number %d\n",
                    path,
                    count);
            // @SymbolCheckIgnore
            exit(1);
        }
        count++;

        int ret = stat(path, &statBuf);
        if (ret == -1) {
            err(1, "stat(%s)", path);
        }

        if (fileSize != 0) {
            if ((size_t) statBuf.st_size != fileSize) {
                fprintf(stderr,
                        "\t%s ERROR Wrong file size 0x%lx, expected 0x%lx\n",
                        path,
                        statBuf.st_size,
                        fileSize);
                // @SymbolCheckIgnore
                exit(1);
            }
        } else {
            if (statBuf.st_size == 0) {
                fprintf(stderr, "\t%s ERROR Zero length file\n", path);
            }
            fileSize = statBuf.st_size;
        }

        int fd = open(path, O_RDONLY);
        if (fd == -1) {
            err(1, "open(%s)", path);
        }

        close(fd);

        fprintf(stdout, "\t%s VALID\n", path);
        currentFile = currentFile->next;
        if (currentFile == log.backingFilesList) {
            break;
        }
    }

    traverseLog(&log, &meta, &options);

CommonExit:
    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    if (status == StatusOk) {
        return EXIT_SUCCESS;
    } else {
        // XXX: Exit status truncated to 8 bits; avoid aliasing XCE status
        return EXIT_FAILURE;
    }
}
