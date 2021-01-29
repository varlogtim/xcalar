// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _ARCHIVE_H_
#define _ARCHIVE_H_

#include "util/StringHashTable.h"
#include "operators/GenericTypes.h"

enum { ArchiveNumSlots = 7 };

struct ArchiveFile {
    StringHashTableHook hook;
    const char *getFileName() const { return fileName; };
    char fileName[XcalarApiMaxPathLen + 1];
    void *buf;
    size_t bufSize;
    bool isFile;

    void del();
};

typedef StringHashTable<ArchiveFile,
                        &ArchiveFile::hook,
                        &ArchiveFile::getFileName,
                        ArchiveNumSlots,
                        hashStringFast>
    ArchiveHashTable;

struct ArchiveManifest {
    ArchiveHashTable *fileContents;
    size_t totalEstimatedFileContentsSize;
};

static constexpr uint64_t BcArchiveManifestBufSize = sizeof(ArchiveManifest);
static constexpr uint64_t BcArchiveFileBufSize = sizeof(ArchiveFile);

MustCheck Status archiveInit();
void archiveDestroy();

// *** Functions to create a packed archive *** //
// You almost never have to use this unless you want to create a new manifest
// from scratch
ArchiveManifest *archiveNewEmptyManifest();
MustCheck Status archiveAddFileToManifest(const char *fileName,
                                          void *fileContents,
                                          size_t fileSize,
                                          ArchiveManifest *manifest,
                                          json_t *archiveChecksum);
MustCheck Status archiveRemoveFileFromManifest(const char *fileName,
                                               size_t fileSize,
                                               ArchiveManifest *manifest);
MustCheck Status archiveAddDirToManifest(const char *dirName,
                                         ArchiveManifest *manifest);
MustCheck Status archiveAddChecksum(const char *fileName,
                                    const void *fileContents,
                                    size_t fileSize,
                                    json_t *archiveChecksum);
MustCheck Status archiveVerifyChecksum(const char *fileName,
                                       const void *fileContents,
                                       size_t fileSize,
                                       const json_t *archiveChecksum);
MustCheck Status archiveCreateChecksum(const void *fileContents,
                                       size_t fileSize,
                                       char **fileChecksum);
MustCheck Status archivePack(ArchiveManifest *manifest,
                             void **bufOut,
                             size_t *bufSizeOut);

// *** Functions to read from a packed archive *** //
// Given a buffer containing the byte sequence of compressed files, this
// function will allocate a manifest of the compressed files. You can then
// use the manifest with archiveGetFileData to retrieve the contents of
// each file
MustCheck Status archiveUnpack(void *buf,
                               size_t bufSize,
                               ArchiveManifest **archiveManifestOut);
MustCheck Status archiveGetFileData(ArchiveManifest *manifest,
                                    const char *fileName,
                                    void **buf,
                                    size_t *bufSize,
                                    const json_t *archiveChecksum);
void archiveFreeManifest(ArchiveManifest *archiveManifest);

#endif  // _ARCHIVE_H_
