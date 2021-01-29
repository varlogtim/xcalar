// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#if HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef HAVE_ARCHIVE3_H
#include <archive3.h>
#include <archive_entry3.h>
#elif defined(HAVE_ARCHIVE_H)
#include <archive.h>
#include <archive_entry.h>
#endif

#include <new>
#include <jansson.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "util/System.h"
#include "util/MemTrack.h"
#include "util/Archive.h"
#include "bc/BufferCache.h"
#include "sys/XLog.h"
#include "LibUtilConstants.h"

using namespace util;

static BcHandle *archiveManifestBcHandle = NULL;
static BcHandle *archiveFileBcHandle = NULL;
static constexpr const char *moduleName = "archive";

uint64_t
getBcArchiveManifestNumElems()
{
    return NumArchiveManifestInBc;
}

uint64_t
getBcArchiveFileNumElems()
{
    return NumArchiveFilesInBc;
}

Status
archiveInit()
{
    Status status = StatusUnknown;

    archiveManifestBcHandle =
        BcHandle::create(BufferCacheObjects::ArchiveManifest);
    if (archiveManifestBcHandle == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    archiveFileBcHandle = BcHandle::create(BufferCacheObjects::ArchiveFile);
    if (archiveFileBcHandle == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        archiveDestroy();
    }
    return status;
}

void
ArchiveFile::del()
{
    if (this->buf != NULL) {
        memFree(this->buf);
        this->buf = NULL;
    }
    archiveFileBcHandle->freeBuf(this);
}

static void
addArchiveFileToManifest(ArchiveManifest *archiveManifest,
                         ArchiveFile *archiveFile)
{
    assert(archiveManifest != NULL);
    assert(archiveFile != NULL);

    archiveManifest->fileContents->insert(archiveFile);
    archiveManifest->totalEstimatedFileContentsSize +=
        strlen(archiveFile->fileName) + 1 + archiveFile->bufSize +
        sizeof(struct stat);
}

ArchiveManifest *
archiveNewEmptyManifest()
{
    Status status = StatusUnknown;
    ArchiveManifest *archiveManifest;
    void *ptr;

    archiveManifest =
        (ArchiveManifest *) archiveManifestBcHandle->allocBuf(
            XidInvalid, &status);
    if (archiveManifest == NULL) {
        goto CommonExit;
    }

    archiveManifest->fileContents = NULL;

    ptr = memAllocExt(sizeof(*archiveManifest->fileContents), moduleName);
    if (ptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    archiveManifest->fileContents = new (ptr) ArchiveHashTable();

    archiveManifest->totalEstimatedFileContentsSize = 0;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (archiveManifest != NULL) {
            archiveFreeManifest(archiveManifest);
            archiveManifest = NULL;
        }
    }
    return archiveManifest;
}

Status
archiveAddFileToManifest(const char *fileName,
                         void *fileContents,
                         size_t fileSize,
                         ArchiveManifest *manifest,
                         json_t *archiveChecksum)
{
    Status status = StatusUnknown;
    ArchiveFile *archiveFile = NULL;
    size_t ret;

    assert(manifest != NULL);
    if (fileContents == NULL || fileSize == 0) {
        status = StatusEmptyFile;
        goto CommonExit;
    }

    archiveFile = (ArchiveFile *) archiveFileBcHandle->allocBuf(
        XidInvalid, &status);
    if (archiveFile == NULL) {
        goto CommonExit;
    }

    archiveFile->buf = memAllocExt(fileSize, moduleName);
    if (archiveFile->buf == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ret =
        strlcpy(archiveFile->fileName, fileName, sizeof(archiveFile->fileName));
    if (ret >= sizeof(archiveFile->fileName)) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

    archiveFile->bufSize = fileSize;
    memcpy(archiveFile->buf, fileContents, fileSize);
    archiveFile->isFile = true;
    addArchiveFileToManifest(manifest, archiveFile);

    if (archiveChecksum) {
        status = archiveAddChecksum(fileName,
                                    fileContents,
                                    fileSize,
                                    archiveChecksum);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error adding \"%s\": Could not add checksum: %s",
                    fileName,
                    strGetFromStatus(status));
        }
    }

    // addArchiveFileToManifest inserts archiveFile into a hash table, so
    // set it to NULL so we don't delete it in the CommonExit block below.
    archiveFile = NULL;

    status = StatusOk;
CommonExit:
    if (archiveFile != NULL) {
        archiveFile->del();
        archiveFile = NULL;
    }

    return status;
}

Status
archiveRemoveFileFromManifest(const char *fileName,
                              size_t fileSize,
                              ArchiveManifest *manifest)
{
    Status status = StatusUnknown;

    ArchiveFile *archiveFile = manifest->fileContents->remove(fileName);
    if (archiveFile) {
        archiveFile->del();
        archiveFile = NULL;
    } else {
        status = StatusNoEnt;
        goto CommonExit;
    }

    manifest->totalEstimatedFileContentsSize -=
        strlen(fileName) + 1 + fileSize + sizeof(struct stat);

    status = StatusOk;

CommonExit:
    return status;
}

Status
archiveAddDirToManifest(const char *dirName, ArchiveManifest *manifest)
{
    Status status = StatusUnknown;
    ArchiveFile *archiveFile = NULL;
    size_t ret;

    assert(manifest != NULL);
    archiveFile = (ArchiveFile *) archiveFileBcHandle->allocBuf(
        XidInvalid, &status);
    if (archiveFile == NULL) {
        goto CommonExit;
    }
    ret =
        strlcpy(archiveFile->fileName, dirName, sizeof(archiveFile->fileName));
    if (ret >= sizeof(archiveFile->fileName)) {
        status = StatusNameTooLong;
        goto CommonExit;
    }
    archiveFile->buf = NULL;
    archiveFile->bufSize = 0;
    archiveFile->isFile = false;
    addArchiveFileToManifest(manifest, archiveFile);
    // addArchiveFileToManifest inserts archiveFile into a hash table, so
    // set it to NULL so we don't delete it in the CommonExit block below.
    archiveFile = NULL;

    status = StatusOk;

CommonExit:
    if (archiveFile != NULL) {
        archiveFile->del();
        archiveFile = NULL;
    }

    return status;
}

Status
archiveAddChecksum(const char *fileName,
                   const void *fileContents,
                   size_t fileSize,
                   json_t *archiveChecksum)
{
    Status status = StatusUnknown;
    json_t *jsonChecksum = NULL;
    size_t ret = 0;
    char *fileChecksum = NULL;

    status = archiveCreateChecksum(fileContents, fileSize, &fileChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error adding \"%s\": Could not create checksum: %s",
                fileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    jsonChecksum = json_string(fileChecksum);
    ret = json_object_set(archiveChecksum, fileName, jsonChecksum);
    if (ret) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error adding \"%s\": Could not set checksum: %s",
                fileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:

    if (fileChecksum) {
        memFree(fileChecksum);
        fileChecksum = NULL;
    }

    if (jsonChecksum) {
        json_decref(jsonChecksum);
    }

    return status;
}

// Match result should be return by Status
Status
archiveVerifyChecksum(const char *fileName,
                      const void *fileContents,
                      size_t fileSize,
                      const json_t *archiveChecksum)
{
    Status status = StatusUnknown;
    json_t *jsonChecksum = NULL;
    char *fileChecksum = NULL;
    const char *fileChecksumRecord;
    bool checksumOk = true;

    status = archiveCreateChecksum(fileContents, fileSize, &fileChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error extracting \"%s\": Could not create checksum: %s",
                fileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    jsonChecksum = json_object_get(archiveChecksum, fileName);
    fileChecksumRecord = json_string_value(jsonChecksum);

    // Checksum of the current file not available.
    // No need to check workbook version. If a checksum file is provided, it
    // must be version > v1.
    if (!fileChecksumRecord) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Error extracting \"%s\": Checksum not available",
                      fileName);
        status = StatusChecksumNotFound;
        goto CommonExit;
    } else {
        checksumOk = !strcmp(fileChecksum, fileChecksumRecord);
        // Checksum mismatch.
        // No need to check workbook version. If workbook version is v1,
        // there should be no checksum file. Code will not reach current
        // function.
        if (!checksumOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Error extracting \"%s\": Checksum mismatch",
                          fileName);
            status = StatusChecksumMismatch;
            goto CommonExit;
        } else {
            xSyslog(moduleName, XlogDebug, "\"%s\" verified", fileName);
        }
    }

    status = StatusOk;

CommonExit:

    if (fileChecksum) {
        memFree(fileChecksum);
        fileChecksum = NULL;
    }

    return status;
}

Status
archiveCreateChecksum(const void *fileContents,
                      size_t fileSize,
                      char **fileChecksum)
{
    Status status = StatusUnknown;
    const uint32_t digest = hashCrc32c(0, fileContents, fileSize);
    // 1 byte is 8 bits, 1 hex digit is 4 bits.
    size_t digestLength = sizeof digest * 2;

    *fileChecksum = (char *) memAllocExt(digestLength + 1, moduleName);
    BailIfNull(*fileChecksum);
    sprintf(*fileChecksum, "%0*x", (int) digestLength, digest);

    status = StatusOk;

CommonExit:
    return status;
}

Status
archivePack(ArchiveManifest *manifest, void **bufOut, size_t *bufSizeOut)
{
    Status status = StatusUnknown;
    ArchiveFile *archiveFile;
    struct archive *archive = NULL;
    struct archive_entry *archiveEntry = NULL;
    bool archiveOpened = false;
    int ret;
    void *buf, *tmpBuf;
    size_t bufSizeUsed = 0;

    assert(manifest != NULL);

    buf = memAllocExt(manifest->totalEstimatedFileContentsSize, moduleName);
    if (buf == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    archiveEntry = archive_entry_new();
    if (archiveEntry == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    archive = archive_write_new();
    if (archive == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ret = archive_write_add_filter_gzip(archive);
    if (ret != ARCHIVE_OK) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }

    ret = archive_write_set_format_pax_restricted(archive);
    if (ret != ARCHIVE_OK) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }

    ret = archive_write_open_memory(archive,
                                    buf,
                                    manifest->totalEstimatedFileContentsSize,
                                    &bufSizeUsed);
    if (ret != ARCHIVE_OK) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }
    archiveOpened = true;

    for (ArchiveHashTable::iterator it = manifest->fileContents->begin();
         (archiveFile = it.get()) != NULL;
         it.next()) {
        archive_entry_set_pathname(archiveEntry, archiveFile->fileName);
        archive_entry_set_size(archiveEntry, archiveFile->bufSize);
        if (archiveFile->isFile) {
            archive_entry_set_filetype(archiveEntry, AE_IFREG);
            archive_entry_set_perm(archiveEntry, 0644);
        } else {
            archive_entry_set_filetype(archiveEntry, AE_IFDIR);
            archive_entry_set_perm(archiveEntry, 0755);
        }
        archive_entry_set_mtime(archiveEntry, time(NULL), 0);

        // XXX EstimatedFileContentsSize is just an estimate. We might run
        // out of space. Consider adding some code path to re-alloc
        // if the pre-alloc'ed buffer is too small (e.g. by doubling the size,
        // or by alloc'ing a block at a time)
        ret = archive_write_header(archive, archiveEntry);
        if (ret < 0) {
            xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
            status = sysErrnoToStatus(archive_errno(archive));
            goto CommonExit;
        }

        ret =
            archive_write_data(archive, archiveFile->buf, archiveFile->bufSize);
        if (ret < 0) {
            xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
            status = sysErrnoToStatus(archive_errno(archive));
            goto CommonExit;
        }

        archive_entry_clear(archiveEntry);
    }

    // Finalize the write
    archive_write_close(archive);
    archiveOpened = false;
    xSyslog(moduleName,
            XlogDebug,
            "EstimatedSize: %lu bytes "
            "BufSizeUsed: %lu bytes",
            manifest->totalEstimatedFileContentsSize,
            bufSizeUsed);

    // As soon as we add the code path to expand pre-alloc'ed buffer
    // when we run out, we can nuke this assert.
    assert(bufSizeUsed <= manifest->totalEstimatedFileContentsSize);
    tmpBuf = memRealloc(buf, bufSizeUsed);
    if (tmpBuf == NULL) {
        bufSizeUsed = manifest->totalEstimatedFileContentsSize;
    } else {
        buf = tmpBuf;
    }

    *bufOut = buf;
    buf = NULL;
    *bufSizeOut = bufSizeUsed;

    status = StatusOk;
CommonExit:
    if (archiveEntry != NULL) {
        archive_entry_free(archiveEntry);
        archiveEntry = NULL;
    }

    if (archiveOpened) {
        assert(archive != NULL);
        archive_write_close(archive);
        archiveOpened = false;
    }

    if (archive != NULL) {
        archive_write_free(archive);
        archive = NULL;
    }

    if (buf != NULL) {
        assert(status != StatusOk);
        memFree(buf);
        buf = NULL;
    }

    return status;
}

static Status
readArchiveFile(struct archive *archive,
                struct archive_entry *archiveEntry,
                ArchiveFile **archiveFileOut)
{
    ArchiveFile *archiveFile;
    Status status = StatusUnknown;
    ssize_t ret;
    void *buf = NULL;
    ssize_t bufSize;

    assert(archive != NULL);
    assert(archiveFileOut != NULL);

    archiveFile = (ArchiveFile *) archiveFileBcHandle->allocBuf(
        XidInvalid, &status);
    if (archiveFile == NULL) {
        goto CommonExit;
    }

    size_t r;
    r = strlcpy(archiveFile->fileName,
                archive_entry_pathname(archiveEntry),
                sizeof(archiveFile->fileName));
    if (r >= sizeof(archiveFile->fileName)) {
        status = StatusOverflow;
        goto CommonExit;
    }

    bufSize = archive_entry_size(archiveEntry);
    if (bufSize < 0) {
        status = StatusLibArchiveError;
        goto CommonExit;
    }

    if (bufSize > 0) {
        buf = memAllocExt(bufSize, moduleName);
        if (buf == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        ret = archive_read_data(archive, buf, bufSize);
        if (ret != bufSize) {
            status = StatusLibArchiveError;
            goto CommonExit;
        }
    }

    if (archive_entry_filetype(archiveEntry) == AE_IFREG) {
        archiveFile->isFile = true;
    } else {
        assert(archive_entry_filetype(archiveEntry) == AE_IFDIR);
        archiveFile->isFile = false;
    }

    archiveFile->buf = buf;
    archiveFile->bufSize = bufSize;
    buf = NULL;

    *archiveFileOut = archiveFile;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (archiveFile != NULL) {
            archiveFileBcHandle->freeBuf(archiveFile);
            archiveFile = NULL;
        }

        if (buf != NULL) {
            memFree(buf);
            buf = NULL;
        }
    }
    assert(buf == NULL);

    return status;
}

Status
archiveGetFileData(ArchiveManifest *manifest,
                   const char *fileName,
                   void **buf,
                   size_t *bufSize,
                   const json_t *archiveChecksum)
{
    ArchiveFile *archiveFile;
    Status status = StatusUnknown;

    assert(manifest != NULL);
    assert(buf != NULL);
    assert(bufSize != NULL);
    assert(fileName != NULL);

    archiveFile = manifest->fileContents->find(fileName);
    if (archiveFile == NULL) {
        return StatusNoEnt;
    }

    *buf = archiveFile->buf;
    *bufSize = archiveFile->bufSize;

    // Only verify checksum when checksum is provided.
    if (archiveChecksum) {
        status =
            archiveVerifyChecksum(fileName, *buf, *bufSize, archiveChecksum);

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error extracting \"%s\": Checksum verification failed: %s",
                    fileName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = StatusOk;

CommonExit:
    return status;
}

Status
archiveUnpack(void *buf, size_t bufSize, ArchiveManifest **manifestOut)
{
    Status status = StatusUnknown;
    ArchiveManifest *manifest = NULL;
    struct archive *archive = NULL;
    bool archiveOpened = false;
    struct archive_entry *archiveEntry;
    ArchiveFile *archiveFile = NULL;
    int ret;

    assert(buf != NULL);
    assert(manifestOut != NULL);
    assert(bufSize > 0);

    manifest = archiveNewEmptyManifest();
    if (manifest == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    archive = archive_read_new();
    if (archive == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ret = archive_read_support_filter_all(archive);
    if (ret != ARCHIVE_OK) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }

    ret = archive_read_support_format_all(archive);
    if (ret != ARCHIVE_OK) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }

    ret = archive_read_open_memory(archive, buf, bufSize);
    if (ret != ARCHIVE_OK) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }
    archiveOpened = true;

    while ((ret = archive_read_next_header(archive, &archiveEntry)) ==
           ARCHIVE_OK) {
        status = readArchiveFile(archive, archiveEntry, &archiveFile);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(archiveFile != NULL);
        addArchiveFileToManifest(manifest, archiveFile);
        archiveFile = NULL;
    }
    assert(ret != ARCHIVE_OK);
    if (ret != ARCHIVE_EOF) {
        xSyslog(moduleName, XlogErr, "%s", archive_error_string(archive));
        status = sysErrnoToStatus(archive_errno(archive));
        goto CommonExit;
    }

    *manifestOut = manifest;
    status = StatusOk;
CommonExit:
    if (archiveOpened) {
        assert(archive != NULL);
        archive_read_close(archive);
        archiveOpened = false;
    }

    if (archive != NULL) {
        archive_read_free(archive);
        archive = NULL;
    }

    if (status != StatusOk) {
        if (manifest != NULL) {
            archiveFreeManifest(manifest);
            manifest = NULL;
        }

        if (archiveFile != NULL) {
            archiveFile->del();
            archiveFile = NULL;
        }
    }

    assert(archiveFile == NULL);
    return status;
}

void
archiveFreeManifest(ArchiveManifest *manifest)
{
    ArchiveHashTable *fileContents;

    assert(manifest != NULL);

    if (manifest->fileContents != NULL) {
        fileContents = manifest->fileContents;
        fileContents->removeAll(&ArchiveFile::del);

        fileContents->~ArchiveHashTable();
        memFree(fileContents);
        manifest->fileContents = NULL;
    }

    archiveManifestBcHandle->freeBuf(manifest);
    manifest = NULL;
}

void
archiveDestroy()
{
    if (archiveManifestBcHandle != NULL) {
        BcHandle::destroy(&archiveManifestBcHandle);
        archiveManifestBcHandle = NULL;
    }

    if (archiveFileBcHandle != NULL) {
        BcHandle::destroy(&archiveFileBcHandle);
        archiveFileBcHandle = NULL;
    }
}
