// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stddef.h>
#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>

#include "StrlFunc.h"
#include "UdfPersist.h"
#include "udf/UserDefinedFunction.h"
#include "log/Log.h"
#include "hash/Hash.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "LibUdfConstants.h"
#include "util/FileUtils.h"
#include "strings/String.h"
#include "libapis/LibApisCommon.h"
#include "session/Sessions.h"

using namespace udf;

// Following struct needed only for upgrade to Dionysus. These are the contents
// of the legacy UDF metafile. The main goal behind the metadata is to protect
// against UDF corruption due to partial writes. However, the new design
// protects against partial writes differently, and so the final UDF source
// module can't ever be partially written (a temp file is first written, and
// if there's a partial write of this file, the operation just fails, leaving
// the source file untouched; and if the temp file is fully written, a rename(2)
// of this file is done to the existing file, and a rename can't result in a
// partially written file since the rename updates a directory entry. So the
// new design can do away with this metafile which simplifies the on-disk UDF
// layout).
//
struct UdfPersistModule {
    char eyeCatcher[4];  // Beginning eye catcher.
    uint16_t size;       // Size of this block.
    uint16_t version;
    uint16_t type;            // UDF type (see LibApisCommon.h).
    uint64_t sourceLen;       // Length of text in source file.
    struct timespec modTime;  // Last modified time.
    uint64_t sourceCrc;       // Crc hash of source.
    uint8_t reserved[40];     // For future use.
    char eyeCatcherEnd[4];    // End eye catcher.
    uint64_t crc;             // Of this block.
};

UdfPersist::UdfPersist() : finishedRestore_(false) {}

UdfPersist::~UdfPersist() {}

Status
UdfPersist::init(bool skipRestore)
{
    Status status = StatusOk;

    if (skipRestore) {
        finishedRestore_ = true;
    }
    return status;
}

void
UdfPersist::destroy()
{
    finishedRestore_ = false;
}

//
// Functions for writing UDFs to disk.
//
Status
UdfPersist::createOrTruncFile(const char *filePath, int *fd, int rwFlag)
{
    Status status = StatusOk;

    assert(rwFlag == O_RDWR || rwFlag == O_RDONLY || rwFlag == O_WRONLY);
    *fd = open(filePath,
               O_CREAT | O_TRUNC | O_CLOEXEC | rwFlag,
               S_IRWXU | S_IRWXG | S_IRWXO);

    // Can't fail with EEXIST (since O_TRUNC is used)
    assert(*fd != -1 || errno != EEXIST);

    if (*fd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist file %s create or trunc failed: %s",
                filePath,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

// Used to determine names of meta and source files for a module. If sizes are
// insufficient, will populate sizes and not touch filenames.
Status
UdfPersist::legacyFileNames(const char *moduleName,
                            UdfType type,
                            XcalarApiUdfContainer *udfContainer,
                            char *fileNameMeta,
                            size_t *fileNameMetaSize,
                            char *fileNameSource,
                            size_t *fileNameSourceSize)
{
    Status status = StatusUnknown;
    assert(fileNameMetaSize != NULL);
    assert(fileNameSourceSize != NULL);
    assert(type == UdfTypePython);
    LogLib *logLib = LogLib::get();
    size_t commonDirPathLen;
    assert(logLib->wkbkPath_[0] != '\0');
    size_t metaDirPathLen;
    size_t sourceDirPathLen;
    size_t fileNameMetaLen;
    size_t fileNameSourceLen;
    size_t moduleNameLen;
    char *metaDirPath = NULL;
    char *sourceDirPath = NULL;

    // Add 1 for each slash in the name
    commonDirPathLen = strlen(logLib->wkbkPath_) + 1 +
                       strlen(udfContainer->userId.userIdName) + 1 +
                       SessionMgr::MaxTextHexIdSize + 1 +
                       strlen(LogLib::UdfWkBookDirName);

    metaDirPathLen = commonDirPathLen;
    sourceDirPathLen = commonDirPathLen + 1 + strlen(sourcesPyName);
    moduleNameLen = strlen(moduleName);

    // We add 1 for '/'
    fileNameMetaLen =
        metaDirPathLen + 1 + moduleNameLen + strlen(metaFileSuffix);
    fileNameSourceLen =
        sourceDirPathLen + 1 + moduleNameLen + strlen(pySourceFileSuffix);

    metaDirPath = (char *) memAlloc(metaDirPathLen + 1);
    BailIfNull(metaDirPath);

    verify(snprintf(metaDirPath,
                    metaDirPathLen + 1,
                    "%s/%s/%lX/%s",
                    logLib->wkbkPath_,
                    udfContainer->userId.userIdName,
                    udfContainer->sessionInfo.sessionId,
                    LogLib::UdfWkBookDirName) < (int) (metaDirPathLen + 1));

    sourceDirPath = (char *) memAlloc(sourceDirPathLen + 1);
    BailIfNull(sourceDirPath);
    verify(snprintf(sourceDirPath,
                    sourceDirPathLen + 1,
                    "%s/%s/%lX/%s/%s",
                    logLib->wkbkPath_,
                    udfContainer->userId.userIdName,
                    udfContainer->sessionInfo.sessionId,
                    LogLib::UdfWkBookDirName,
                    sourcesPyName) < (int) (sourceDirPathLen + 1));

    if (*fileNameMetaSize >= fileNameMetaLen + 1) {
        // Store metadata in "<metaDirPath>/<moduleName>.xlrudf". Suffix and
        // module name validation keeps from colliding with type directories.
        verify(snprintf(fileNameMeta,
                        *fileNameMetaSize,
                        "%s/%s%s",
                        metaDirPath,
                        moduleName,
                        metaFileSuffix) < (int) *fileNameMetaSize);
    } else {
        *fileNameMetaSize = fileNameMetaLen + 1;
    }

    if (*fileNameSourceSize >= fileNameSourceLen + 1) {
        // Store source in "<sourceDirPath>/<moduleName>.py".
        verify(snprintf(fileNameSource,
                        *fileNameSourceSize,
                        "%s/%s%s",
                        sourceDirPath,
                        moduleName,
                        pySourceFileSuffix) < (int) *fileNameSourceSize);
    } else {
        *fileNameSourceSize = fileNameSourceLen + 1;
    }

    status = StatusOk;
CommonExit:
    if (metaDirPath != NULL) {
        memFree(metaDirPath);
    }
    if (sourceDirPath != NULL) {
        memFree(sourceDirPath);
    }
    return status;
}

// Used to determine names of source files for a module (current and temp). If
// sizes are insufficient, will populate sizes and not touch filenames.
Status
UdfPersist::fileNames(const char *moduleName,
                      UdfType type,
                      XcalarApiUdfContainer *udfContainer,
                      char *fileNameSourceTmp,
                      size_t *fileNameSourceTmpSize,
                      char *fileNameSource,
                      size_t *fileNameSourceSize)
{
    Status status = StatusUnknown;
    assert(fileNameSourceTmp == NULL || fileNameSourceTmpSize != NULL);
    assert(fileNameSourceSize != NULL);
    assert(type == UdfTypePython);
    LogLib *logLib = LogLib::get();
    assert(logLib->wkbkPath_[0] != '\0');
    size_t sourceDirPathLen;
    size_t fileNameSourceTmpLen;
    size_t fileNameSourceLen;
    size_t moduleNameLen;
    char *sourceDirPath = NULL;
    char *moduleNameDup = NULL;
    char *moduleNameDup2 = NULL;
    char *sharedDir = NULL;
    const char *relModName =
        NULL;  // relative name (i.e. absolute path is stripped)
    bool sharedSpace = false;
    char sharedPath[MaxUdfPathLen];

    assert(type == UdfTypePython);  // only python UDFs supported currently

    // Add 1 for each slash in the name
    if ((moduleName[0] == '/' &&
         strncmp(moduleName,
                 UserDefinedFunction::SharedUDFsDirPath,
                 strlen(UserDefinedFunction::SharedUDFsDirPath)) == 0) ||
        (moduleName[0] != '/' && strlen(udfContainer->userId.userIdName) == 0 &&
         udfContainer->sessionInfo.sessionNameLength == 0)) {
        // We have either an absolute path to witin the shared UDFs directory or
        // a relative path with an "empty" UDF container.
        sharedSpace = true;
        moduleNameDup = strAllocAndCopy(moduleName);
        BailIfNull(moduleNameDup);
        relModName = basename(moduleNameDup);
        // Create name of shared directory path
        snprintf(sharedPath,
                 sizeof(sharedPath),
                 "%s",
                 UserDefinedFunction::SharedUDFsDirPath);
        sharedDir = sharedPath;
        // skip the "+ 1" for the slash between xcalarRootCompletePath_
        // and sharedDir, since sharedDir has a leading '/' already
        sourceDirPathLen =
            strlen(XcalarConfig::get()->xcalarRootCompletePath_) +
            strlen(sharedDir) + 1 +
            strlen(sourcesPyName);  // assuming python here
        moduleNameLen = strlen(relModName);
    } else {
        assert(moduleName[0] != '/' ||
               strncmp(moduleName,
                       UserDefinedFunction::UdfWorkBookPrefix,
                       strlen(UserDefinedFunction::UdfWorkBookPrefix)) == 0);
        sourceDirPathLen = strlen(logLib->wkbkPath_) + 1 +
                           strlen(udfContainer->userId.userIdName) + 1 +
                           SessionMgr::MaxTextHexIdSize + 1 +
                           strlen(LogLib::UdfWkBookDirName) + 1 +
                           strlen(sourcesPyName);
        if (moduleName[0] == '/') {
            moduleNameDup = strAllocAndCopy(moduleName);
            BailIfNull(moduleNameDup);
            relModName = basename(moduleNameDup);
            moduleNameLen = strlen(relModName);
        } else {
            moduleNameLen = strlen(moduleName);
            relModName = moduleName;
        }
    }

    // We add 1 for '/'
    fileNameSourceLen =
        sourceDirPathLen + 1 + moduleNameLen + strlen(pySourceFileSuffix);

    fileNameSourceTmpLen = fileNameSourceLen + strlen(udfNewVerTmpSufix);

    sourceDirPath = (char *) memAlloc(sourceDirPathLen + 1);
    BailIfNull(sourceDirPath);
    if (sharedSpace) {
        verify(snprintf(sourceDirPath,
                        sourceDirPathLen + 1,
                        "%s%s/%s",  // skip the '/' for sharedDir
                        XcalarConfig::get()->xcalarRootCompletePath_,
                        sharedDir,
                        sourcesPyName) < (int) (sourceDirPathLen + 1));

    } else {
        verify(snprintf(sourceDirPath,
                        sourceDirPathLen + 1,
                        "%s/%s/%lX/%s/%s",
                        logLib->wkbkPath_,
                        udfContainer->userId.userIdName,
                        udfContainer->sessionInfo.sessionId,
                        LogLib::UdfWkBookDirName,
                        sourcesPyName) < (int) (sourceDirPathLen + 1));
    }

    if (fileNameSourceTmpSize != NULL) {
        if (*fileNameSourceTmpSize >= fileNameSourceTmpLen + 1) {
            // Store new tmp file in
            // "<sourceDirPath>/<relModName><udfNewVerTmpSufix>.py"
            verify(snprintf(fileNameSourceTmp,
                            *fileNameSourceTmpSize,
                            "%s/%s%s%s",
                            sourceDirPath,
                            relModName,
                            udfNewVerTmpSufix,
                            pySourceFileSuffix) < (int) *fileNameSourceTmpSize);
        } else {
            *fileNameSourceTmpSize = fileNameSourceTmpLen + 1;
        }
    }

    if (*fileNameSourceSize >= fileNameSourceLen + 1) {
        // Store source in "<sourceDirPath>/<relModName>.py".
        verify(snprintf(fileNameSource,
                        *fileNameSourceSize,
                        "%s/%s%s",
                        sourceDirPath,
                        relModName,
                        pySourceFileSuffix) < (int) *fileNameSourceSize);
    } else {
        *fileNameSourceSize = fileNameSourceLen + 1;
    }

    status = StatusOk;
CommonExit:
    if (sourceDirPath != NULL) {
        memFree(sourceDirPath);
    }
    if (moduleNameDup != NULL) {
        memFree(moduleNameDup);
        moduleNameDup = NULL;
    }
    if (moduleNameDup2 != NULL) {
        memFree(moduleNameDup2);
        moduleNameDup2 = NULL;
    }
    return status;
}

// Return StatusOk if the fileNameSource has contents identical to "sourceCheck"
Status
UdfPersist::bytesCmp(char *fileNameSource, const char *sourceCheck)
{
    Status status;
    int sourceFd = -1;
    uint64_t sourceLen = 0;
    struct stat srcStat;
    char *source = NULL;
    size_t bytesRead;
    int ret;
    const char *errorMessage = NULL;

    sourceFd = open(fileNameSource, O_CLOEXEC | O_RDONLY);
    if (sourceFd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist bytesCmp failed to open %s: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = lseek(sourceFd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist bytesCmp failed to lseek %s: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = lstat(fileNameSource, &srcStat);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist bytesCmp failed to lstat %s: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }
    sourceLen = srcStat.st_size;

    //
    // Construct source file name and read in.
    //
    source = (char *) memAlloc(sourceLen + 1);
    if (source == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist bytesCmp for %s failed: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = FileUtils::convergentRead(sourceFd, source, sourceLen, &bytesRead);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist bytesCmp failed to read %s: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (bytesRead != sourceLen) {
        errorMessage = "invalid source file length";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist bytesCmp of %s failed '%s': %s",
                fileNameSource,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }
    source[sourceLen] = '\0';

    FileUtils::close(sourceFd);
    sourceFd = -1;

    if (strncmp(source, sourceCheck, sourceLen) == 0) {  // file contents match
        status = StatusOk;
    } else {
        status = StatusUDFSourceMismatch;
    }
CommonExit:
    if (source != NULL) {
        memFree(source);
        source = NULL;
    }
    return status;
}

// Update the UDF source ...write out the new source first to a temp file, and
// if successful, move the temp file to the current file. Cleanup any temp files
// during boot, and before writing out the temp file.
Status
UdfPersist::update(const char *moduleName,
                   XcalarApiUdfContainer *udfContainer,
                   UdfType type,
                   const char *source)
{
    Status status;
    int sourceTmpFd = -1;
    bool createdSourceTmp = false;
    char *fnameSourceTmpDup = NULL;
    char *fileNameSourceTmp = NULL;
    char *fileNameSource = NULL;
    int retCode;

    assert(type == UdfTypePython);
    assert(finishedRestore_);

    //
    // Setup file names for the current and new tmp files
    //
    size_t fileNameSourceTmpSize = 0;
    size_t fileNameSourceSize = 0;

    status = fileNames(moduleName,
                       type,
                       udfContainer,
                       NULL,
                       &fileNameSourceTmpSize,
                       NULL,
                       &fileNameSourceSize);
    BailIfFailed(status);

    fileNameSourceTmp = (char *) memAlloc(fileNameSourceTmpSize);
    BailIfNull(fileNameSourceTmp);

    fileNameSource = (char *) memAlloc(fileNameSourceSize);
    BailIfNull(fileNameSource);

    status = fileNames(moduleName,
                       type,
                       udfContainer,
                       fileNameSourceTmp,
                       &fileNameSourceTmpSize,
                       fileNameSource,
                       &fileNameSourceSize);
    BailIfFailed(status);

    //
    // Write new source to a temp file (remove it first if exists). In the
    // event of a crash, we may lose the temp file or it may have partially
    // written data. But during re-boot, ANY temp files are deleted...since
    // the update occurs via the atomic 'rename(2)' operation, the current
    // UDF file's contents are always consistent (modulo file system
    // corruption which Xcalar can't really protect against at the app level).
    //

    fnameSourceTmpDup = strAllocAndCopy(fileNameSourceTmp);
    BailIfNull(fnameSourceTmpDup);

    status = FileUtils::recursiveMkdir(dirname(fnameSourceTmpDup), 0750);
    BailIfFailed(status);

    status = createOrTruncFile(fileNameSourceTmp, &sourceTmpFd, O_WRONLY);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist update %s fileNameSourceTmp %s create "
                "or trunc"
                " failed: %s",
                moduleName,
                fileNameSourceTmp,
                strGetFromStatus(status));
        goto CommonExit;
    } else {
        createdSourceTmp = true;
    }

    assert(sourceTmpFd != -1);

    status = FileUtils::convergentWrite(sourceTmpFd, source, strlen(source));
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist update %s fileNameSourceTmp %s write "
                "failed: %s",
                moduleName,
                fileNameSourceTmp,
                strGetFromStatus(status));
        goto CommonExit;
    }

    retCode = rename(fileNameSourceTmp, fileNameSource);
    if (retCode == -1) {
        int renameErrno = errno;
        // See rename(2) - if file is on NFS (UDF files are typically in Xcalar
        // shared root mounted on NFS), the rename may report failure despite
        // the rename actually having succeeded! Here, do an existence check on
        // fileNameSourceTmp - if it's gone, the rename must've actually
        // succeeded (check the contents of source file and confirm that its
        // contents are equal to "source" passed in to this function) - in which
        // case, return success despite the rename() above having reported
        // failure.
        if (access(fileNameSourceTmp, F_OK) == -1) {
            // tmp file's gone, so rename probably succeeded - check the
            // contents of fileNameSource to make sure.
            createdSourceTmp = false;
            status = bytesCmp(fileNameSource, source);
            if (status == StatusUDFSourceMismatch) {
                status = StatusUDFUpdateFailed;
                xSyslog(ModuleName,
                        XlogErr,
                        "Scalar Function persist update of module %s, rename "
                        "of tmp file "
                        "%s to source file %s failed but now tmp file is gone "
                        "and source file has unexpected contents: %s",
                        moduleName,
                        fileNameSourceTmp,
                        fileNameSource,
                        strGetFromStatus(status));
                assert(0);  // seems impossible - assert fail in DEBUG
            }
            BailIfFailed(status);
        } else {
            // tmp file's still present so rename() definitely failed
            status = sysErrnoToStatus(renameErrno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function persist update %s rename of %s to %s "
                    "failed: %s",
                    moduleName,
                    fileNameSourceTmp,
                    fileNameSource,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        // even if rename succeeds, ensure the source file has the desired
        // source contents (which also serves to open/close the source file's
        // fd that'd sync the NFS client cache for the file to make sure a read
        // from this path on a different node after this operation returns,
        // gets the latest contents written out by this operation/node)
        status = bytesCmp(fileNameSource, source);
        if (status == StatusUDFSourceMismatch) {
            status = StatusUDFUpdateFailed;
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function persist update of module %s, rename of "
                    "tmp file "
                    "%s to source file %s succeeded but now tmp file is gone "
                    "and source file has unexpected contents: %s",
                    moduleName,
                    fileNameSourceTmp,
                    fileNameSource,
                    strGetFromStatus(status));
            assert(0);
        }
        BailIfFailed(status);
    }

    xSyslog(ModuleName,
            XlogDebug,
            "Scalar Function persist update module '%s' saved to '%s'",
            moduleName,
            fileNameSource);

CommonExit:
    if (fnameSourceTmpDup != NULL) {
        memFree(fnameSourceTmpDup);
        fnameSourceTmpDup = NULL;
    }
    if (fileNameSource != NULL) {
        memFree(fileNameSource);
        fileNameSource = NULL;
    }
    if (fileNameSourceTmp != NULL) {
        memFree(fileNameSourceTmp);
        fileNameSourceTmp = NULL;
    }
    if (sourceTmpFd != -1) {
        FileUtils::close(sourceTmpFd);
        sourceTmpFd = -1;
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persisnt update %s failed: %s",
                moduleName,
                strGetFromStatus(status));

        Status statusUnlink;
        if (createdSourceTmp) {
            int ret = unlink(fileNameSourceTmp);
            if (ret == -1) {
                statusUnlink = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogWarn,
                        "Scalar Function persist update %s unable to delete "
                        "%s: %s",
                        moduleName,
                        fileNameSourceTmp,
                        strGetFromStatus(statusUnlink));
            }
        }
    }

    return status;
}

// Delete persistent UDF source filename
void
UdfPersist::del(const char *moduleName,
                XcalarApiUdfContainer *udfContainer,
                UdfType type)
{
    Status status = StatusOk;
    int ret;
    // Load file names.
    size_t fileNameSourceSize = 0;
    char *fileNameSource = NULL;

    status = fileNames(moduleName,
                       type,
                       udfContainer,
                       NULL,
                       NULL,
                       NULL,
                       &fileNameSourceSize);
    BailIfFailed(status);

    fileNameSource = (char *) memAlloc(fileNameSourceSize);
    if (fileNameSource == NULL) {
        xSyslog(ModuleName,
                XlogWarn,
                "Scalar Function module %s delete failed: %s",
                moduleName,
                strGetFromStatus(status));
        status = StatusNoMem;
        goto CommonExit;
    }

    status = fileNames(moduleName,
                       type,
                       udfContainer,
                       NULL,
                       NULL,
                       fileNameSource,
                       &fileNameSourceSize);
    BailIfFailed(status);

    ret = unlink(fileNameSource);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogWarn,
                "Scalar Function module %s delete %s failed: %s",
                moduleName,
                fileNameSource,
                strGetFromStatus(status));
        // Fall through
    }

CommonExit:
    if (fileNameSource != NULL) {
        memFree(fileNameSource);
        fileNameSource = NULL;
    }
}

// Deletes module's persisted state - only used during upgrade - hence the
// legacy prefix. It generates the filenames for the old UDF metafile, and the
// source file, and deletes them
void
UdfPersist::legacyDel(const char *moduleName,
                      XcalarApiUdfContainer *udfContainer,
                      UdfType type)
{
    Status status = StatusOk;
    int ret;

    // Load file names.
    size_t fileNameMetaSize = 0;
    size_t fileNameSourceSize = 0;
    char *fileNameMeta = NULL;
    char *fileNameSource = NULL;

    status = legacyFileNames(moduleName,
                             type,
                             udfContainer,
                             NULL,
                             &fileNameMetaSize,
                             NULL,
                             &fileNameSourceSize);
    BailIfFailed(status);

    fileNameMeta = (char *) memAlloc(fileNameMetaSize);
    if (fileNameMeta == NULL) {
        xSyslog(ModuleName,
                XlogWarn,
                "Scalar Function module %s delete failed: %s",
                moduleName,
                strGetFromStatus(status));
        status = StatusNoMem;
        goto CommonExit;
    }

    fileNameSource = (char *) memAlloc(fileNameSourceSize);
    if (fileNameSource == NULL) {
        xSyslog(ModuleName,
                XlogWarn,
                "Scalar Function module %s delete failed: %s",
                moduleName,
                strGetFromStatus(status));
        status = StatusNoMem;
        goto CommonExit;
    }

    status = legacyFileNames(moduleName,
                             type,
                             udfContainer,
                             fileNameMeta,
                             &fileNameMetaSize,
                             fileNameSource,
                             &fileNameSourceSize);
    BailIfFailed(status);

    ret = unlink(fileNameSource);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogWarn,
                "Scalar Function module %s delete %s failed: %s",
                moduleName,
                fileNameSource,
                strGetFromStatus(status));
        // Fall through
    }

    ret = unlink(fileNameMeta);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogWarn,
                "Scalar Function module %s delete %s failed: %s",
                moduleName,
                fileNameMeta,
                strGetFromStatus(status));
        // Fall through
    }

CommonExit:
    if (fileNameMeta != NULL) {
        memFree(fileNameMeta);
        fileNameMeta = NULL;
    }

    if (fileNameSource != NULL) {
        memFree(fileNameSource);
        fileNameSource = NULL;
    }
}

//
// Functions for reading previously persisted files.
//

// Following routine allows code re-factoring between the legacy, pre-Dionysus
// code to read and add a Python UDF module, and the new Dionysus code. Main
// difference is that legacy code adds the UDF module with the on-disk
// persistent version whereas new code simply starts at version 1 (this is at
// boot so versions don't matter), and the legacy code uses udfMeta for file
// size, whereas new code uses lstat(2).

Status
UdfPersist::readSourceAndAdd(const char *fileNameSource,
                             uint64_t moduleVersion,  // used only for legacy
                             UdfType udfType,
                             XcalarApiUdfContainer *udfContainer,
                             void *udfMeta,  // used only for legacy
                             char *moduleName)

{
    Status status = StatusUnknown;
    int sourceFd = -1;
    int ret = 0;
    UdfModuleSrc *input = NULL;
    uint64_t sourceLen = 0;
    struct stat srcStat;
    char *source;
    size_t bytesRead;
    size_t sourceCrcSize;
    uint64_t sourceCrc;
    UdfPersistModule *udfPstMod = (UdfPersistModule *) udfMeta;
    const char *errorMessage = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize;

    sourceFd = open(fileNameSource, O_CLOEXEC | O_RDONLY);
    if (sourceFd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist readSourceAndAdd failed to open %s: "
                "%s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = lseek(sourceFd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist readSourceAndAdd failed to lseek %s: "
                "%s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (udfMeta != NULL) {
        sourceLen = udfPstMod->sourceLen;
    } else {
        ret = lstat(fileNameSource, &srcStat);
        if (ret == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function persist readSourceAndAdd failed to lstat "
                    "%s: %s",
                    fileNameSource,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sourceLen = srcStat.st_size;
    }

    //
    // Construct source file name and read in.
    //
    input = (UdfModuleSrc *) memAllocAligned(UdfPersistBlockAlign,
                                             sizeof(*input) + sourceLen + 1);
    if (input == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist readSourceAndAdd for %s failed: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    source = (char *) input + offsetof(typeof(*input), source);
    status = FileUtils::convergentRead(sourceFd, source, sourceLen, &bytesRead);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist readSourceAndAdd failed to read %s: "
                "%s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (bytesRead != sourceLen) {
        errorMessage = "invalid source file length";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist readSourceAndAdd failed bytes check "
                "%s: %s",
                fileNameSource,
                strGetFromStatus(status));
        goto CommonExit;
    }
    source[sourceLen] = '\0';

    FileUtils::close(sourceFd);
    sourceFd = -1;

    if (udfPstMod != NULL) {
        // Validate CRC of source. This is legacy only; no need to validate
        // checksum in Dionysus and beyond
        sourceCrcSize =
            udfPstMod->sourceLen - (udfPstMod->sourceLen % HashCrc32Alignment);
        sourceCrc = hashCrc32c(0, source, sourceCrcSize);
        if (udfPstMod->sourceCrc != sourceCrc) {
            errorMessage = "invalid source CRC";
            status = StatusUdfPersistInvalid;
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function persist readSourceAndAdd failed checksum "
                    "%s: %s",
                    fileNameSource,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    //
    // Construct input and add module.
    //
    input->type = udfPstMod ? (UdfType) udfPstMod->type : udfType;
    input->isBuiltin = false;
    input->sourceSize = sourceLen + 1;
    verify(strlcpy(input->moduleName, moduleName, sizeof(input->moduleName)) <
           sizeof(input->moduleName));

    xSyslog(ModuleName,
            XlogInfo,
            "Restoring %s version %lu",
            input->moduleName,
            moduleVersion);
    if (udfMeta != NULL) {  // legacy - so has version
        status = UserDefinedFunction::get()->addUdfInternal(input,
                                                            udfContainer,
                                                            moduleVersion,
                                                            &output,
                                                            &outputSize);
    } else {
        status = UserDefinedFunction::get()->addUdf(input,
                                                    udfContainer,
                                                    &output,
                                                    &outputSize);
    }
CommonExit:
    if (input != NULL) {
        memFree(input);
        input = NULL;
    }
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
    return status;
}

// Read UDF from source filename and add the UDF to the cluster
Status
UdfPersist::addModule(const char *fileNameSource,
                      UdfType udfType,
                      XcalarApiUdfContainer *udfContainer)
{
    Status status = StatusUnknown;
    const char *sourceBaseName = strBasename(fileNameSource);  // has suffix
    const size_t baseNameLen = strlen(sourceBaseName);  // includes suffix
    char *modName = NULL;
    char *suffix = NULL;

    // has suffix - so larger than needed
    modName = (char *) memAllocExt(baseNameLen + 1, ModuleName);
    BailIfNull(modName);

    verify(strlcpy(modName, sourceBaseName, baseNameLen + 1) < baseNameLen + 1);

    if (udfType == UdfTypePython) {
        // strip the source file suffix to finally get module name in modName
        suffix = strstr(modName, pySourceFileSuffix);
        assert(suffix != NULL);
        *suffix = '\0';
    }

    // now "modName" has name of module
    // XXX: check if different nodes call in here...would they all read the
    // same UDFs and try to add them?

    status = readSourceAndAdd(fileNameSource,
                              0,  // version - unused for non-legacy i.e. new
                              udfType,
                              udfContainer,
                              NULL,  // legacy
                              modName);
    BailIfFailed(status);

CommonExit:
    if (modName) {
        memFree(modName);
        modName = NULL;
    }
    return status;
}

// Only for use by upgrade: use the legacy UDF metafile to read UDF and add to
// cluster
Status
UdfPersist::legacyAdd(const char *fileNameMeta,
                      XcalarApiUdfContainer *udfContainer)
{
    Status status = StatusOk;
    int ret;
    UdfModuleSrc *input = NULL;
    int metaFd = -1;
    size_t bytesRead;
    const char *errorMessage = NULL;
    const char *metaBaseName = strBasename(fileNameMeta);
    size_t baseNameLen = strlen(metaBaseName);
    char moduleNameVersioned[baseNameLen + 1];
    char moduleName[baseNameLen + 1];
    char *modulePath = NULL;
    size_t modulePathSize = 0;
    uint64_t crc;
    char *suffix = NULL;
    uint64_t curVersion = UserDefinedFunction::UdfRecord::InvalidVersion;
    bool delCurVersion = false;
    ModuleNameEntry *modEntry = NULL;
    char *fileNameMetaDup = NULL;
    const char *moduleNameTmp;
    const char *moduleVersionTmp;
    //
    // Pull in meta file contents.
    //
    UdfPersistModule module;

    modulePathSize = strlen(fileNameMeta) + strlen(sourcesPyName) + 1;
    modulePath = (char *) memAllocExt(modulePathSize, moduleName);

    verify(strlcpy(moduleNameVersioned,
                   metaBaseName,
                   sizeof(moduleNameVersioned)) < sizeof(moduleNameVersioned));
    suffix = strstr(moduleNameVersioned, metaFileSuffix);
    assert(suffix != NULL);  // Because of meta pattern passed to file iter.
    *suffix = '\0';

    if (strlen(moduleNameVersioned) >= UdfVersionedModuleName) {
        errorMessage = "module name too long";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(strlen(metaFileSuffix) > strlen(pySourceFileSuffix));

    fileNameMetaDup = strAllocAndCopy((char *) fileNameMeta);
    BailIfNull(fileNameMetaDup);

    verify(snprintf(modulePath,
                    modulePathSize,
                    "%s/%s/%s%s",
                    dirname(fileNameMetaDup),
                    sourcesPyName,
                    moduleNameVersioned,
                    pySourceFileSuffix) < (int) modulePathSize);

    memcpy(moduleName, moduleNameVersioned, sizeof(moduleName));
    status = UserDefinedFunction::parseFunctionName(moduleName,
                                                    &moduleNameTmp,
                                                    &moduleVersionTmp,
                                                    NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s parseFunctionName failed: %s",
                fileNameMeta,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Record the current version of the module here.
    curVersion = (uint64_t) atoll(moduleVersionTmp);

    // Figure out the DLM node for this UDF module. Here the idea is to have
    // all nodes in the cluster participate in the UDF restore.
    if (hashStringFast(moduleName) % Config::get()->getActiveNodes() !=
        Config::get()->getMyNodeId()) {
        status = StatusOk;
        goto CommonExit;
    }

    metaFd = open(fileNameMeta, O_CLOEXEC | O_RDONLY);
    if (metaFd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    ret = lseek(metaFd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s lseek failed: %s",
                fileNameMeta,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        FileUtils::convergentRead(metaFd, &module, sizeof(module), &bytesRead);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed: %s",
                fileNameMeta,
                strGetFromStatus(status));
        goto CommonExit;
    }

    FileUtils::close(metaFd);
    metaFd = -1;

    //
    // Validate block.
    //
    if (bytesRead < offsetof(typeof(module), type)) {
        errorMessage = "too few bytes read";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (memcmp(module.eyeCatcher, EyeCatcherBegin, sizeof(module.eyeCatcher)) !=
        0) {
        errorMessage = "invalid eye catcher";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (module.version != UdfPersistVersion1) {
        errorMessage = "unsupported version";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (module.size != sizeof(module)) {
        errorMessage = "invalid size";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (memcmp(module.eyeCatcherEnd,
               EyeCatcherEnd,
               sizeof(module.eyeCatcherEnd)) != 0) {
        errorMessage = "invalid end eye catcher";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    crc =
        hashCrc32c(0, &module, sizeof(module) - offsetof(typeof(module), crc));
    if (module.crc != crc) {
        errorMessage = "invalid metadata crc";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (module.type != UdfTypePython) {
        errorMessage = "unsupported Scalar Function type";
        status = StatusUdfPersistInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add %s read failed with '%s': %s",
                fileNameMeta,
                errorMessage,
                strGetFromStatus(status));
        goto CommonExit;
    }

    modEntry = moduleNameHT_.find(moduleName);
    if (modEntry != NULL) {
        if (modEntry->curVersion ==
            UserDefinedFunction::UdfRecord::InvalidVersion) {
            // NOOP. It's quite possible that the previous attempt to upload
            // a different version of this module failed. So try to upload
            // this version of the module.
        } else if (modEntry->curVersion > curVersion) {
            // We already got a newer version already uploaded, so go ahead and
            // delete this stale version.
            delCurVersion = true;
            status = StatusUdfModuleAlreadyExists;
            goto CommonExit;
        } else if (modEntry->curVersion < curVersion) {
            // Delete the already uploaded stale version. We need to upload a
            // new UDF version.
            // XXX: NOTE! The new higher version (in curVersion) has still
            // not been vetted for partial write corruption - the source
            // checksum validation is done below...if the curVersion file is
            // bad, and the modEntry->curVersion which has been uploaded is
            // deleted below, it'll delete the last known good copy of the
            // UDF, resulting in loss of UDF data during a post-crash re-boot
            // or Dionysus upgrade! This is an existing bug in Chronos and
            // earlier releases...found during code inspection of this routine.
            //
            // Fix should do two things:
            // 0. Defer deletion of the module to the point AFTER the source
            //    checksum has been validated for the new higher version
            // 1. Trigger cleanup process for higher versioned, but bad UDF.
            //    Either this could be done programmatically in this code here
            //    by deleting both the meta and source files with the higher
            //    version, or the error scenario appropriately logged, so an
            //    admin responding to a missing UDF (since this UDF will not
            //    show up in the cluster post-reboot) can do the cleanup
            //    of the bad higher version, manually (which gives humans a
            //    chance to carefully do the rescue). Preference is to do this
            //    programmatically, since the log/admin-intervention seems hard
            //    to document / realize in practice
            status = UserDefinedFunction::get()->deleteUdfInternal(moduleName,
                                                                   udfContainer,
                                                                   true);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Scalar Function persist add %s failed: %s",
                        fileNameMeta,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            assert(0 && "Cannot have the version recur");
        }
    } else {
        modEntry = (ModuleNameEntry *) memAlloc(sizeof(ModuleNameEntry));
        snprintf(modEntry->moduleName,
                 sizeof(modEntry->moduleName),
                 "%s",
                 moduleName);
        verifyOk(moduleNameHT_.insert(modEntry));
    }
    modEntry->curVersion = UserDefinedFunction::UdfRecord::InvalidVersion;

    status = readSourceAndAdd(modulePath,
                              curVersion,
                              UdfTypePython,
                              udfContainer,
                              &module,
                              moduleName);

    if (status == StatusUdfPersistInvalid) {
        // XXX:
        // Fix loss of UDF issue here...if curVersion is bad, it'd have failed
        // in readSourceAndAdd() above...at which point, it should be deleted
        // from on-disk, but the version prior to curVersion should be retained
    }
    BailIfFailed(status);

    assert(status == StatusOk);
    assert(modEntry->curVersion ==
           UserDefinedFunction::UdfRecord::InvalidVersion);
    modEntry->curVersion = curVersion;

CommonExit:
    if (input != NULL) {
        memAlignedFree(input);
    }
    if (metaFd != -1) {
        FileUtils::close(metaFd);
        metaFd = -1;
    }
    if (delCurVersion == true) {
        legacyDel(moduleNameVersioned, udfContainer, (UdfType) module.type);
    }
    if (modulePath != NULL) {
        memFree(modulePath);
        modulePath = NULL;
    }
    if (fileNameMetaDup != NULL) {
        memFree(fileNameMetaDup);
        fileNameMetaDup = NULL;
    }

    if (status != StatusOk) {
        if (errorMessage != NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Persisted Scalar Function %s failed to load (%s): %s",
                    fileNameMeta,
                    strGetFromStatus(status),
                    errorMessage);
        } else {
            xSyslog(ModuleName,
                    XlogErr,
                    "Persisted Scalar Function %s failed to load: %s",
                    fileNameMeta,
                    strGetFromStatus(status));
        }
    }

    return status;
}

// This routine is used during upgrade of pre-Dionysus UDFs.  Iterate through
// the moduleNameHT_ hash table - for each module, remove the corresponding
// metafile and source file, and remove this module name from moduleNameHT_
// hash table. Remove the UDF module from the cluster - this will remove the
// metafile and source file on-disk too

Status
UdfPersist::deleteOldUdfFiles(XcalarApiUdfContainer *udfContainer)
{
    Status status;

    ModuleNameHashTable::iterator it(moduleNameHT_);

    for (; it.get() != NULL; it.next()) {
        status =
            UserDefinedFunction::get()->deleteUdfInternal(it.get()->moduleName,
                                                          udfContainer,
                                                          true);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

// This routine is used during upgrade of pre-Dionysus UDFs.  Copy each old
// versioned python source file to a non-versioned file name in the udfs/python
// directory. Note this is only for pre-Dionysus upgrade - so the legacy source
// files must all be Python source files (with versions).

Status
UdfPersist::copyOldSourcesToNew(DIR *udfDir, char *udfDirPath)
{
    Status status;
    // path name to python sub-dir under udfDir
    char *pyDirPath = NULL;
    size_t pyDirPathSize = 0;
    DIR *pyDir = NULL;

    // pattern to look for the legacy versioned python source files in pyDir
    // Pattern is "*<UdfVersionDelim>*<pySourceFileSuffix>" so 3 chars (leading
    // and intermediate asterisk plus NULL char at end, besides the
    // UdfVersionDelim and pySourceFileSuffix

    char versionedSourcePattern[strlen(pySourceFileSuffix) +
                                strlen(UserDefinedFunction::UdfVersionDelim) +
                                3];

    // all per-file state related variables below
    size_t modNameSize = 0;
    char *modNameVersioned = NULL;
    char *suffix = NULL;
    // path name for old versioned source file, and new source file
    char *versionedSourceFile = NULL;
    char *newSourceFile = NULL;
    size_t absFileNameSize = 0;
    const char *moduleNameTmp = NULL;
    const char *moduleVersionTmp = NULL;

    verify((size_t) snprintf(versionedSourcePattern,
                             sizeof(versionedSourcePattern),
                             "*%s*%s",  // e.g. pattern is "*#*.py"
                             UserDefinedFunction::UdfVersionDelim,
                             pySourceFileSuffix) <
           sizeof(versionedSourcePattern));

    // generate the path to the python directory
    pyDirPathSize = strlen(udfDirPath) + 1 + strlen(sourcesPyName);
    // add 1 to size for NULL terminator when allocating
    pyDirPath = (char *) memAllocExt(pyDirPathSize + 1, moduleName);
    BailIfNull(pyDirPath);

    verify(snprintf(pyDirPath,
                    pyDirPathSize + 1,
                    "%s/%s",
                    udfDirPath,
                    sourcesPyName) < (int) (pyDirPathSize + 1));

    pyDir = opendir(pyDirPath);
    if (pyDir == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    while (true) {
        errno = 0;
        struct dirent *curPyFile = readdir(pyDir);
        if (curPyFile == NULL) {
            assert(errno == 0);
            break;
        }
        if (curPyFile->d_name[0] == '.') {
            continue;
        }
        if (!strMatch(versionedSourcePattern, curPyFile->d_name)) {
            continue;
        }

        // This file name matches the legacy versioned module name. If so,
        // create a new file name stripping out the version. The new file name's
        // string length has to be less than that of the current file name since
        // it'd be stripped of the version number.

        modNameSize = strlen(curPyFile->d_name);
        modNameVersioned = (char *) memAllocExt(modNameSize + 1, ModuleName);
        BailIfNull(modNameVersioned);

        verify(strlcpy(modNameVersioned, curPyFile->d_name, modNameSize + 1) <
               modNameSize + 1);

        suffix = strstr(modNameVersioned, pySourceFileSuffix);
        assert(suffix != NULL);
        *suffix =
            '\0';  // strip off suffix so modNameVersioned has version only

        status = UserDefinedFunction::parseFunctionName(modNameVersioned,
                                                        &moduleNameTmp,
                                                        &moduleVersionTmp,
                                                        NULL);
        BailIfFailed(status);

        absFileNameSize = pyDirPathSize + 1 + strlen(curPyFile->d_name);

        versionedSourceFile =
            (char *) memAllocExt(absFileNameSize + 1, ModuleName);

        BailIfNull(versionedSourceFile);

        newSourceFile = (char *) memAllocExt(absFileNameSize + 1, ModuleName);
        BailIfNull(newSourceFile);

        verify(snprintf(versionedSourceFile,
                        absFileNameSize + 1,
                        "%s/%s",
                        pyDirPath,
                        curPyFile->d_name) < (int) absFileNameSize + 1);

        verify(snprintf(newSourceFile,
                        absFileNameSize + 1,
                        "%s/%s%s",
                        pyDirPath,
                        moduleNameTmp,
                        pySourceFileSuffix) < (int) absFileNameSize + 1);

        // Copy old versioned source file to new source file without version
        status = FileUtils::copyFile(newSourceFile, versionedSourceFile);
        BailIfFailed(status);

        memFree(modNameVersioned);
        modNameVersioned = NULL;
        memFree(versionedSourceFile);
        versionedSourceFile = NULL;
        memFree(newSourceFile);
        newSourceFile = NULL;
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function copyOldSourcesToNew %s to %s failed:"
                " %s",
                versionedSourceFile,
                newSourceFile,
                strGetFromStatus(status));
    }
    if (pyDirPath) {
        memFree(pyDirPath);
        pyDirPath = NULL;
    }
    if (modNameVersioned) {
        memFree(modNameVersioned);
        modNameVersioned = NULL;
    }
    if (versionedSourceFile) {
        memFree(versionedSourceFile);
        pyDirPath = NULL;
    }
    if (newSourceFile) {
        memFree(newSourceFile);
        pyDirPath = NULL;
    }
    if (pyDir != NULL) {
        verify(closedir(pyDir) == 0);
        pyDir = NULL;
    }
    return status;
}

// Scan the python UDFs dir, adding each python file's code as a UDF to cluster
// It also cleans up (i.e. deletes) any tmp files left from a crashed UDF update
void
UdfPersist::processUdfsPyDir(DIR *udfPyDir,
                             char *dirPath,
                             XcalarApiUdfContainer *udfContainer)
{
    // Process the entire directory.  If this becomes a bottleneck, we
    // could either parallelize the processing by splitting the list of
    // files or overlap reading in files with processing them or both.

    Status status;
    char pySourcePattern[strlen(pySourceFileSuffix) + 2];
    char pySourceTmpPattern[strlen(udfNewVerTmpSufix) +
                            strlen(pySourceFileSuffix) + 2];
    int ret = -1;
    size_t sourceFileAbsPathLen = 0;
    char *sourceFileAbsPath = NULL;

    verify((size_t) snprintf(pySourcePattern,
                             sizeof(pySourcePattern),
                             "*%s",
                             pySourceFileSuffix) < sizeof(pySourcePattern));

    verify((size_t) snprintf(pySourceTmpPattern,
                             sizeof(pySourceTmpPattern),
                             "*%s%s",
                             udfNewVerTmpSufix,
                             pySourceFileSuffix) < sizeof(pySourceTmpPattern));

    while (true) {
        errno = 0;
        struct dirent *currentFile = readdir(udfPyDir);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error
            assert(errno == 0);
            // End of directory
            break;
        }

        // Skip entries for the current and higher level directory as well
        // as the unlikely case of "dot-files"
        if (currentFile->d_name[0] == '.') {
            continue;
        }

        // Look for any tmp files with the udfNewVerTmpSufix in this dir
        // (pattern in pySourceTmpPattern) and delete them since they represent
        // an interrupted module update.

        if (!strMatch(pySourceTmpPattern, currentFile->d_name) &&
            !strMatch(pySourcePattern, currentFile->d_name)) {
            // Apply pattern skip
            continue;
        }

        // file is either a tmp file or a real source file...

        // sourceFileAbsPath should be the full file path to a UDF sourcefile:
        //
        //  <sharedRoot>/workbooks/<userName>/<workBookName>/udfs/\
        //      python/<sourceFileName>.py
        //
        //      OR
        //
        //  <sharedRoot>/sharedUDFs/python/<sourceFileName>.py
        //
        // Depending on the context (i.e. udfContainer)
        //
        // NOTE: the file could also be a tmp file (w/ udfNewVerTmpSufix)
        //
        // Add 1s for '/' between each dir and a final 1 for the '\0'
        sourceFileAbsPathLen =
            strlen(dirPath) + 1 + strlen(currentFile->d_name);

        sourceFileAbsPath =
            (char *) memAllocExt(sourceFileAbsPathLen + 1, ModuleName);
        if (sourceFileAbsPath == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "processUdfsPyDir on %s/%s failed due to %s",
                    dirPath,
                    currentFile->d_name,
                    strGetFromStatus(status));
            break;
        }

        verify(snprintf(sourceFileAbsPath,
                        sourceFileAbsPathLen + 1,
                        "%s/%s",
                        dirPath,
                        currentFile->d_name) <
               (int) (sourceFileAbsPathLen + 1));

        if (strMatch(pySourcePattern, currentFile->d_name)) {
            // add any genuine python source files
            status = addModule(sourceFileAbsPath, UdfTypePython, udfContainer);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogInfo,
                        "Attempt to add Scalar Function module %s failed: %s",
                        sourceFileAbsPath,
                        strGetFromStatus(status));
            }
            // Note that failure to add() is non-fatal
            memFree(sourceFileAbsPath);
            sourceFileAbsPath = NULL;
        } else if (strMatch(pySourceTmpPattern, currentFile->d_name)) {
            // delete any tmp files found during restore
            ret = unlink(sourceFileAbsPath);
            if (ret == -1) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogWarn,
                        "Scalar Function persist processUdfsPyDir unable to "
                        "delete %s: %s",
                        sourceFileAbsPath,
                        strGetFromStatus(status));
            }
            // Note that failure to add() is non-fatal
            memFree(sourceFileAbsPath);
            sourceFileAbsPath = NULL;
        } else {
            assert(0);  // imposssible
        }
    }
    moduleNameHT_.removeAll(&ModuleNameEntry::del);
}

// Scan the UDFs directory and load UDFs into the cluster
void
UdfPersist::processUdfsDirPostUpgrade(DIR *udfDir,
                                      char *dirPath,
                                      size_t maxDirNameLen,
                                      XcalarApiUdfContainer *udfContainer)
{
    Status status;

    DIR *cdirIter = NULL;
    char *cdirName = NULL;
    char *cdirPath = NULL;

    cdirName = (char *) memAllocExt(maxDirNameLen, moduleName);
    BailIfNull(cdirName);

    cdirPath = (char *) memAllocExt(MaxUdfDirChildPathLen, moduleName);
    BailIfNull(cdirPath);

    while ((cdirIter = SessionMgr::get()->getNextDirAndName(udfDir,
                                                            dirPath,
                                                            cdirName,
                                                            maxDirNameLen)) !=
           NULL) {
        // If python sub-dir encountered, process python UDFs
        snprintf(cdirPath, MaxUdfDirChildPathLen, "%s/%s", dirPath, cdirName);
        if (strncmp(cdirName, sourcesPyName, maxDirNameLen) == 0) {
            processUdfsPyDir(cdirIter, cdirPath, udfContainer);
        } else {
            // In future, could process other languages here...
            xSyslog(ModuleName,
                    XlogWarn,
                    "Scalar Function persist restore skipping non-Python dir "
                    "%s",
                    cdirPath);
        }
        if (cdirIter != NULL) {
            verify(closedir(cdirIter) == 0);
            cdirIter = NULL;
        }
    }

CommonExit:
    if (cdirName != NULL) {
        memFree(cdirName);
        cdirName = NULL;
    }
    if (cdirPath != NULL) {
        memFree(cdirPath);
        cdirPath = NULL;
    }
    if (cdirIter != NULL) {
        verify(closedir(cdirIter) == 0);
        cdirIter = NULL;
    }
}

// Used for upgrade of pre-Dionysus UDF files. Scans the UDFs dir looking for
// UDF metafiles and uses them to load the UDFs into the cluster - this process
// will cleanup any stale, left-over versions on-disk, using only the highest
// versioned UDF files to load from. After this is over, the legacy sources are
// copied to new source filenames (without the version suffix) and then the
// legacy UDFs which were just loaded, are deleted from the cluster (which also
// removes the persistent, versioned UDF files in legacy format, leaving only
// the new UDF source files on-disk).
void
UdfPersist::processUdfsDirUpgrade(DIR *udfDir,
                                  char *dirPath,
                                  XcalarApiUdfContainer *udfContainer)
{
    Status status;
    char *metaFileAbsPath = NULL;
    // Process the entire directory.  If this becomes a bottleneck, we
    // could either parallelize the processing by splitting the list of
    // files or overlap reading in files with processing them or both.
    char metaPattern[strlen(metaFileSuffix) + 2];
    verify((size_t) snprintf(metaPattern,
                             sizeof(metaPattern),
                             "*%s",
                             metaFileSuffix) < sizeof(metaPattern));

    while (true) {
        errno = 0;
        struct dirent *currentFile = readdir(udfDir);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error
            assert(errno == 0);
            // End of directory
            break;
        }

        // Skip entries for the current and higher level directory as well
        // as the unlikely case of "dot-files"
        if (currentFile->d_name[0] == '.') {
            continue;
        }

        // Skip sources directories.
        if (strcmp(currentFile->d_name, sourcesPyName) == 0) {
            continue;
        }

        // Apply pattern skip
        if (!strMatch(metaPattern, currentFile->d_name)) {
            continue;
        }

        // metaFileAbsPath should be the full file path to a UDF metafile:
        //
        //  <sharedRoot>/workbooks/<userName>/<workBookName>/udfs/\
        //      <moduleName>.xlrudf
        //
        // Add 1s for '/' between each dir and a final 1 for the '\0'
        size_t metaFileAbsPathLen =
            strlen(dirPath) + 1 + strlen(currentFile->d_name);

        metaFileAbsPath =
            (char *) memAllocExt(metaFileAbsPathLen + 1, ModuleName);
        if (metaFileAbsPath == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "processUdfsDir on %s/%s failed due to %s",
                    dirPath,
                    currentFile->d_name,
                    strGetFromStatus(status));
            break;
        }

        verify(snprintf(metaFileAbsPath,
                        metaFileAbsPathLen + 1,
                        "%s/%s",
                        dirPath,
                        currentFile->d_name) < (int) (metaFileAbsPathLen + 1));

        status = legacyAdd(metaFileAbsPath, udfContainer);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Attempt to add Scalar Function module %s failed: %s",
                    metaFileAbsPath,
                    strGetFromStatus(status));
        }
        // Note that failure to legacyAdd() is non-fatal
        // XXX: for upgrade, think about whether a failure to add or any other
        // such failure for a UDF should be handled in a better way?
        memFree(metaFileAbsPath);
        metaFileAbsPath = NULL;
    }
    // Upgrade processing now...
    // 0. First copy all the python source files to non-versioned equivalents

    status = copyOldSourcesToNew(udfDir, dirPath);
    BailIfFailed(status);

    // 1. Once the copy is successful, iterate through the moduleNameHT_ hash
    //    table - for each module, remove the corresponding metafile and source
    //    file, by deleting the UDF from the cluster
    status = deleteOldUdfFiles(udfContainer);
    BailIfFailed(status);

    // XXX: Nice-to-have
    // 2. Assert that there's no metafile or versioned source file left on-disk.
    //    If one is found, there maybe a bug in upgrade logic or something else.
    //    In non-debug code, remove any such persistent file that's found.

    moduleNameHT_.removeAll(&ModuleNameEntry::del);
CommonExit:
    if (metaFileAbsPath) {
        memFree(metaFileAbsPath);
        metaFileAbsPath = NULL;
    }
}

// Main routine which scans the UDFs dir, first upgrading any UDFs it finds in
// pre-Dionysus legacy format, and then loading UDFs into the cluster. The
// upgrade routine is a noop of course, on an upgraded or green field layout.
void
UdfPersist::processUdfsDir(DIR *udfDir,
                           char *dirPath,
                           size_t maxDirNameLen,
                           XcalarApiUdfContainer *udfContainer,
                           bool upgradeToDionysus)
{
    processUdfsDirUpgrade(udfDir, dirPath, udfContainer);
    rewinddir(udfDir);
    processUdfsDirPostUpgrade(udfDir, dirPath, maxDirNameLen, udfContainer);
}

// Called on usrnode init to read all persisted UDFs and add them into the
// system. Only called on DLM node.
//
// Note that upgradeToDionysus is needed purely to code refactor between the
// code needed to load UDFs for Dionysus (and beyond) versus the code needed by
// XCE to do a one-time upgrade when bringing up Dionysus (and beyond) on a
// pre-Dionysus system.
//
// XXX: Whether to use a flag or always do unconditional upgrade is TBD.
// Currently the flag is ignored in processUdfsDir() which does unconditional
// upgrade always - so booting a new cluster on a legacy UDF layout will
// automatically upgrade and load the UDFs during first boot.
//

Status
UdfPersist::addAll(bool upgradeToDionysus)
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    LogLib *logLib = LogLib::get();
    XcalarApiUserId uid;
    size_t maxUserNameLen = 0;
    int maxDirNameLen;
    char *udfPath = NULL;
    XcalarApiUdfContainer *udfContainer = NULL;
    int treeDepth = 0;

    dirIter = opendir(logLib->wkbkPath_);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add all opendir %s failed: %s",
                logLib->wkbkPath_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // The on-disk dir tree for UDFs has either usernames or workbook names for
    // the directories - so the max dir name length is the max of the two
    // The layout is:
    //     logLib->wkbkPath_/<userName>/<workBookName>/udfs/<module>.xlrudf
    //     logLib->wkbkPath_/<userName>/<workBookName>/udfs/python/<module>.py
    //

    maxUserNameLen = sizeof(uid.userIdName);  // ignore the + 1
    maxDirNameLen = XcalarApiSessionNameLen > maxUserNameLen
                        ? XcalarApiSessionNameLen
                        : maxUserNameLen;

    udfPath = (char *) memAllocExt(MaxUdfPathLen, ModuleName);
    BailIfNull(udfPath);

    strlcpy(udfPath, logLib->wkbkPath_, sizeof(logLib->wkbkPath_));

    udfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              ModuleName);
    BailIfNull(udfContainer);

    memset(udfContainer, 0, sizeof(XcalarApiUdfContainer));

    status = SessionMgr::get()
                 ->processWorkbookDirTree(SessionMgr::WorkbookDirType::Udfs,
                                          dirIter,
                                          udfPath,
                                          maxDirNameLen,
                                          udfContainer,
                                          upgradeToDionysus,
                                          &treeDepth);
    BailIfFailed(status);

    verify(closedir(dirIter) == 0);  // close wkbook dir
    dirIter = NULL;

    // Now process the shared UDFs directory

    dirIter = opendir(logLib->sharedUDFsPath_);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function persist add all opendir %s failed: %s",
                logLib->sharedUDFsPath_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    //     XXX: When users are allowed to create new dirs under the sharedUDFs
    //     path, maxDirNameLen MAY need to be revised. MAY because the max file
    //     name is 255 on most (all?) Linux filesystems, and
    //     XcalarApiSessionNameLen is 255 --> so unless the latter is shrunk or
    //     some other bizarre reason, we don't need to over-engineer this. In
    //     any case, we could also always have XD limit the max filename size
    //     when the feature is added, to conform to any size limits XCE adheres
    //     to...

    memset(udfContainer, 0, sizeof(XcalarApiUdfContainer));
    processUdfsDirPostUpgrade(dirIter,
                              logLib->sharedUDFsPath_,
                              maxDirNameLen,
                              udfContainer);
    status = StatusOk;

CommonExit:
    if (dirIter != NULL) {
        verify(closedir(dirIter) == 0);
        dirIter = NULL;
    }

    if (udfPath != NULL) {
        memFree(udfPath);
        udfPath = NULL;
    }
    if (udfContainer != NULL) {
        memFree(udfContainer);
        udfContainer = NULL;
    }
    if (status == StatusOk) {
        finishedRestore_ = true;
    }

    return status;
}

Status
UdfPersist::deleteWorkbookDirectory(const char *userName,
                                    const uint64_t sessionId)
{
    Status status = StatusOk;
    char workbookPath[XcalarApiMaxPathLen + 1];
    int ret;

    ret = snprintf(workbookPath,
                   sizeof(workbookPath),
                   "%s/%s/%lX",
                   LogLib::get()->wkbkPath_,
                   userName,
                   sessionId);
    assert(ret < (int) sizeof(workbookPath));

    status = FileUtils::rmDirRecursiveSafer(workbookPath);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove workbook directory '%s' for user '%s' "
                "(session ID %lX): %s",
                workbookPath,
                userName,
                sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // If the user doesn't have any other workbooks, delete the user's
    // directory.  It will be recreated if/when they create a new
    // session.
    snprintf(workbookPath,
             sizeof(workbookPath),
             "%s/%s",
             LogLib::get()->wkbkPath_,
             userName);
    if (FileUtils::isDirectoryEmpty(workbookPath)) {
        status = FileUtils::rmDirRecursiveSafer(workbookPath);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "failed to delete workbook directory '%s' for user '%s' "
                    "(session ID %lX): %s",
                    workbookPath,
                    userName,
                    sessionId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:

    return status;
}

// Caller must free memory allocated in the 'udfOnDiskPath' return param
Status
UdfPersist::getUdfOnDiskPath(const char *udfLibNsPath, char **udfOnDiskPath)
{
    Status status;
    LogLib *logLib = LogLib::get();
    size_t udfPathLen;
    XcalarApiUserId user;
    char sessionIdStr[SessionMgr::MaxTextHexIdSize];
    const char *userNameBegin;
    const char *userNameEnd;
    size_t userNameLen;
    size_t sessionIdStrLen;
    char fullyQualDefModule[LibNsTypes::MaxPathNameLen + 1];

    assert(udfOnDiskPath != NULL);

    // Convert libNs path:
    //
    // "/workbook/<userName>/<sessId>/udf/<moduleName>"
    // OR
    // "/sharedUDFs/<modN>" (where <modN> isn't default)
    // OR
    // "default:xyz" (not really a libNs path for default module - special case)
    // OR
    // "/sharedUDFs/default:xyz"
    //
    // TO on-disk path:
    //
    // "<sharedRoot>/workbooks/<userName>/<sessId>/udfs/python/<moduleName>.py"
    // OR
    // "<sharedRoot>/sharedUDFs/python/<moduleName>.py"
    // OR
    // "<InstallRoot>/scripts/default.py"

    if (udfOnDiskPath == NULL) {
        status = StatusInval;
        goto CommonExit;
    }
    *udfOnDiskPath = NULL;
    assert(udfLibNsPath[0] == '/' ||
           strncmp(udfLibNsPath,
                   UserDefinedFunction::DefaultModuleName,
                   strlen(UserDefinedFunction::DefaultModuleName)) == 0);

    status = UserDefinedFunction::get()
                 ->getUdfName(fullyQualDefModule,
                              sizeof(fullyQualDefModule),
                              (char *) UserDefinedFunction::DefaultModuleName,
                              NULL,
                              false);
    BailIfFailed(status);

    // if default module
    if (strncmp(udfLibNsPath,
                UserDefinedFunction::DefaultModuleName,
                strlen(UserDefinedFunction::DefaultModuleName)) == 0 ||
        strncmp(udfLibNsPath, fullyQualDefModule, strlen(fullyQualDefModule)) ==
            0) {
        const char *xlrDir = getenv("XLRDIR");
        char defaultModuleFullPath[PATH_MAX];
        char *defaultDirPath = NULL;

        if (xlrDir == NULL) {
            status = StatusFailed;
            xSyslog(ModuleName,
                    XlogCrit,
                    "%s",
                    "XLRDIR environment variable must point to Xcalar install");
            goto CommonExit;
        }
        status = strStrlcpy(defaultModuleFullPath,
                            UserDefinedFunction::DefaultUDFsPath,
                            sizeof(defaultModuleFullPath));
        BailIfFailed(status);
        defaultDirPath = dirname(defaultModuleFullPath);

        udfPathLen = strlen(xlrDir) + 1 + strlen(defaultDirPath);
        *udfOnDiskPath = (char *) memAllocExt(udfPathLen + 1, ModuleName);
        BailIfNull(*udfOnDiskPath);

        if (snprintf(*udfOnDiskPath,
                     udfPathLen + 1,
                     "%s/%s",
                     xlrDir,
                     defaultDirPath) >= (int) (udfPathLen + 1)) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "default Udfs open default udf module '%s/%s' failed: %s",
                    xlrDir,
                    UserDefinedFunction::DefaultUDFsPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (strncmp(udfLibNsPath,
                       UserDefinedFunction::UdfWorkBookPrefix,
                       strlen(UserDefinedFunction::UdfWorkBookPrefix)) == 0) {
        // parse udfLibNsPath to extract userName and sessionId
        userNameBegin = strchr(udfLibNsPath + 1, '/') + 1;
        userNameEnd = strchr(userNameBegin, '/');
        userNameLen = userNameEnd - userNameBegin;

        // udfOnDiskPath for workbook UDFs is as specified above;
        // add 1 for the intervening '/' separator in the path
        udfPathLen = strlen(logLib->wkbkPath_) + 1 + userNameLen + 1 +
                     SessionMgr::MaxTextHexIdSize + 1 +
                     strlen(LogLib::UdfWkBookDirName) + 1 +
                     strlen(sourcesPyName);
        *udfOnDiskPath = (char *) memAllocExt(udfPathLen + 1, ModuleName);
        BailIfNull(*udfOnDiskPath);

        strncpy(user.userIdName, userNameBegin, userNameLen);
        // null terminate since userNameBegin points to the rest of the path
        // after the user name
        user.userIdName[userNameLen] = '\0';

        sessionIdStrLen = strchr(userNameEnd + 1, '/') - userNameEnd - 1;
        strncpy(sessionIdStr, userNameEnd + 1, sessionIdStrLen);
        // null terminate since userNameEnd + 1 points to the rest of the path
        // after the session ID string in the path
        sessionIdStr[sessionIdStrLen] = '\0';

        verify(snprintf(*udfOnDiskPath,
                        udfPathLen + 1,
                        "%s/%s/%s/%s/%s",
                        logLib->wkbkPath_,
                        user.userIdName,
                        sessionIdStr,
                        LogLib::UdfWkBookDirName,
                        sourcesPyName) < (int) (udfPathLen + 1));

    } else if (strncmp(udfLibNsPath,
                       UserDefinedFunction::SharedUDFsDirPath,
                       strlen(UserDefinedFunction::SharedUDFsDirPath)) == 0) {
        // udfOnDiskPath layout for shared UDFs is as specified above;
        // add 1 for the intervening '/' separator in the path
        udfPathLen =
            strlen(logLib->sharedUDFsPath_) + 1 + strlen(sourcesPyName);
        *udfOnDiskPath = (char *) memAllocExt(udfPathLen + 1, ModuleName);
        BailIfNull(*udfOnDiskPath);

        verify(snprintf(*udfOnDiskPath,
                        udfPathLen + 1,
                        "%s/%s",
                        logLib->sharedUDFsPath_,
                        sourcesPyName) < (int) (udfPathLen + 1));
    } else if ((strncmp(udfLibNsPath,
                        UserDefinedFunction::GlobalUDFDirPathForTests,
                        strlen(
                            UserDefinedFunction::GlobalUDFDirPathForTests)) ==
                0) ||
               strncmp(udfLibNsPath,
                       UserDefinedFunction::UdfGlobalDataFlowPrefix,
                       strlen(UserDefinedFunction::UdfGlobalDataFlowPrefix)) ==
                   0) {
        // do nothing - return empty on-disk path
    } else {
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    xSyslog(ModuleName,
            XlogDebug,
            "Scalar Function persist getUdfOnDiskPath generated on-disk path: "
            "'%s'",
            (udfOnDiskPath == NULL ? "" : *udfOnDiskPath));

    if (status != StatusOk) {
        if (udfOnDiskPath != NULL && *udfOnDiskPath != NULL) {
            memFree(*udfOnDiskPath);
            *udfOnDiskPath = NULL;
        }
    }
    return status;
}

// Now, we can persist the UDF, if restores have already finished. Note
// that during bootstrapping, we use the same code path to restore the
// UDFs from the persistent copy.
bool
UdfPersist::needsPersist(UdfModuleSrc *input,
                         XcalarApiUdfContainer *udfContainer)
{
    if (finishedRestoring() && !input->isBuiltin && udfContainer != NULL &&
        // don't persist:
        //     - when adding UDFs during boot - ones being added are from disk
        //     - built-ins
        //     - test UDFs (udfContainer is NULL)
        //     - retina UDFs; they're in the retina files
        !UserDefinedFunction::get()->containerForDataflows(udfContainer)) {
        return true;
    } else {
        return false;
    }
}
