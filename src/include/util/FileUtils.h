// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _FILEUTILS_H_
#define _FILEUTILS_H_

#include <sys/types.h>
#include "primitives/Primitives.h"

namespace FileUtils
{
MustCheck Status convergentRead(int fd,
                                void *bufIn,
                                size_t numBytes,
                                size_t *bytesRead);

MustCheck Status convergentWrite(int fd, const void *bufIn, size_t count);

MustCheck Status unlinkFiles(const char *dirPath,
                             const char *namePattern,
                             unsigned *numDeleted,
                             unsigned *numFailed);

// dstPath must not exist
MustCheck Status copyFile(const char *dstPath, const char *srcPath);

// If return != StatusOk the dstPath may or may not have files from srcPath
MustCheck Status copyDirectoryFiles(const char *dstDirPath,
                                    const char *srcDirPath);

// Returns false if directory exists and has content (other than . and ..).
// Otherwise returns true.
MustCheck bool isDirectoryEmpty(const char *dirPath);

MustCheck Status fallocate(int fd, int mode, off_t offset, off_t len);

MustCheck Status rmDirRecursive(const char *dirPath);
MustCheck Status rmDirRecursiveSafer(const char *dirPath);

MustCheck Status recursiveMkdir(const char *dirPath, mode_t mode);

// Ignores IO errors
void close(int fd);
};

#endif  // _FILEUTILS_H_
