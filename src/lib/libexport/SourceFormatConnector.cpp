// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <fcntl.h>
#include <new>
#include <libgen.h>

#include "StrlFunc.h"
#include "SourceFormatConnector.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "hash/Hash.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "LibExportConstants.h"
#include "util/FileUtils.h"
#include "util/SystemPathVerify.h"

using namespace Export;

// XXX - this can be removed once DataTargetInt.h is split into several .h
const char *ExDefaultExportTargetName =
    SourceFormatConnector::defaultTargetName;
static bool sfInited = false;

static constexpr char const *moduleName = "libexport";

SourceFormatConnector *SourceFormatConnector::sfConnector = NULL;

// *************************** Helper Functions ***************************** //

/* static */ Status
SourceFormatConnector::init()
{
    Status status;
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(*SourceFormatConnector::sfConnector),
                      __PRETTY_FUNCTION__);
    BailIfNull(ptr);

    SourceFormatConnector::sfConnector = new (ptr) SourceFormatConnector();

    status = SourceFormatConnector::sfConnector->initInternal();
    BailIfFailed(status);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (ptr != NULL) {
            assert(SourceFormatConnector::sfConnector != NULL);
            SourceFormatConnector::sfConnector->~SourceFormatConnector();
            memFree(SourceFormatConnector::sfConnector);
        }
    }
    return status;
}

void
SourceFormatConnector::destroy()
{
    auto &sfc = SourceFormatConnector::getRef();
    sfc.~SourceFormatConnector();
    memFree(&sfc);
}

/* static */ SourceFormatConnector &
SourceFormatConnector::getRef()
{
    assert(sfConnector);
    return *sfConnector;
}

Status
SourceFormatConnector::initInternal()
{
    assert(!sfInited);

    sfInited = true;
    return StatusOk;
}

SourceFormatConnector::~SourceFormatConnector()
{
    this->targetHashTableLock.lock();
    targetHashTable.removeAll(&SfTargetDirectory::del);
    this->targetHashTableLock.unlock();
}

void
SourceFormatConnector::SfTargetDirectory::del()
{
    memFree(this);
}

Status
SourceFormatConnector::getDefaultTargets(int *numTargets,
                                         ExExportTarget **targets)
{
    Status status = StatusOk;
    ExExportTarget *defaultTarget = NULL;
    ExAddTargetSFInput *sfInput;
    size_t sz;

    defaultTarget =
        (ExExportTarget *) memAllocExt(sizeof(*defaultTarget), moduleName);
    BailIfNull(defaultTarget);
    memZero(defaultTarget, sizeof(*defaultTarget));

    // Setup hdr
    defaultTarget->hdr.type = ExTargetSFType;
    static_assert(sizeof(defaultTarget->hdr.name) > defaultTargetNameLen,
                  "Default target must fit in struct");
    verify(strlcpy(defaultTarget->hdr.name,
                   defaultTargetName,
                   sizeof(defaultTarget->hdr.name)) <
           sizeof(defaultTarget->hdr.name));

    // Setup sf specific
    sfInput = &defaultTarget->specificInput.sfInput;
    sz = strlcpy(sfInput->url,
                 XcalarConfig::get()->xcalarRootCompletePath_,
                 sizeof(sfInput->url));
    if (sz >= sizeof(sfInput->url)) {
        status = StatusDsUrlTooLong;
        goto CommonExit;
    }

    sz = strlcat(sfInput->url, defaultTargetDir, sizeof(sfInput->url));
    if (sz >= sizeof(sfInput->url)) {
        status = StatusDsUrlTooLong;
        goto CommonExit;
    }

    *numTargets = 1;
    *targets = defaultTarget;

CommonExit:
    if (status != StatusOk) {
        if (defaultTarget != NULL) {
            memFree(defaultTarget);
            defaultTarget = NULL;
        }
    }
    return status;
}

Status
SourceFormatConnector::shouldPersist(const ExExportTarget *target,
                                     bool *persist)
{
    assert(target->hdr.type == ExTargetSFType);

    // Persist targets other than default
    *persist = strcmp(target->hdr.name, defaultTargetName) != 0;

    return StatusOk;
}

Status
SourceFormatConnector::verifyTarget(const ExExportTarget *target)
{
    Status status = StatusOk;
    bool isShared;
    const char *path = target->specificInput.sfInput.url;
    char *existentDirectory = NULL;

    if (path[0] != '/') {
        status = StatusFailed;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Target '%s' path '%s' must be an absolute path",
                      target->hdr.name,
                      path);
        goto CommonExit;
    }

    status = getLowestExistentDir(path, &existentDirectory);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to extract any existent paths from '%s': %s",
                      path,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    assert(existentDirectory);

    status = PathVerify::verifyPath(existentDirectory, &isShared);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to verify shared storage path '%s' for target "
                      "'%s'",
                      path,
                      target->hdr.name);
        goto CommonExit;
    }
    if (!isShared) {
        status = StatusFailed;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Target '%s' has path '%s' not on shared storage",
                      target->hdr.name,
                      path);
        goto CommonExit;
    }

CommonExit:
    if (existentDirectory) {
        memFree(existentDirectory);
        existentDirectory = NULL;
    }
    return status;
}

Status
SourceFormatConnector::getLowestExistentDir(const char *path, char **directory)
{
    Status status = StatusOk;
    *directory = NULL;
    int pathLen = strlen(path);
    char tmpBuf[pathLen + 1];
    // copy in the initial path; this will be modified in place
    verify(strlcpy(tmpBuf, path, pathLen + 1) == sizeof(tmpBuf) - 1);
    char *tmpPath = tmpBuf;
    bool cont = true;
    int finalPathLen;

    assert(path[0] == '/');

    // Work backwards from path until we get a directory that exists
    do {
        struct stat fileStat;
        if (strlen(path) == 1) {
            // we have reached the top of the path; finish this iteration then
            // exit
            assert(strcmp(path, "/") == 0);
            cont = false;
        }
        int ret = stat(tmpPath, &fileStat);
        if (ret == -1 && errno != ENOENT) {
            // We had a real error other than a portion of the path not existing
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }
        if (ret == -1 && errno == ENOENT) {
            // The path does not exist; backtrack to the parent directory
            tmpPath = dirname(tmpPath);
            continue;
        }
        // The path exists; if it's a file, fail
        if (!S_ISDIR(fileStat.st_mode)) {
            status = StatusFailed;
            xSyslog(moduleName,
                    XlogErr,
                    "Target path '%s' contains '%s',"
                    "which is a file, not a directory",
                    path,
                    tmpPath);
            goto CommonExit;
        }
        break;
    } while (cont);

    finalPathLen = strlen(tmpPath);
    *directory = static_cast<char *>(memAlloc(finalPathLen + 1));
    BailIfNull(*directory);

    strlcpy(*directory, tmpPath, finalPathLen + 1);

CommonExit:
    assert(status != StatusOk || *directory);
    return status;
}

Status
SourceFormatConnector::addTargetLocal(const ExExportTarget *target)
{
    Status status;
    SourceFormatConnector::SfTargetDirectory *dir = NULL;
    const ExAddTargetSpecificInput *input;
    bool allocated = false;

    assert(target->hdr.type == ExTargetSFType);
    input = &target->specificInput;

    this->targetHashTableLock.lock();
    dir = targetHashTable.find(target->hdr.name);
    this->targetHashTableLock.unlock();

    if (dir != NULL) {
        status = StatusExTargetAlreadyExists;
        goto CommonExit;
    }

    dir = (SourceFormatConnector::SfTargetDirectory *)
        memAllocExt(sizeof(SourceFormatConnector::SfTargetDirectory),
                    moduleName);
    if (dir == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    allocated = true;

    dir->target = target->hdr;
    strlcpy(dir->input.url, input->sfInput.url, sizeof(dir->input.url));

    this->targetHashTableLock.lock();
    targetHashTable.insert(dir);
    this->targetHashTableLock.unlock();

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (allocated) {
            memFree(dir);
            dir = NULL;
            allocated = false;
        }
    }
    return status;
}

Status
SourceFormatConnector::removeTargetLocal(const ExExportTargetHdr *hdr)
{
    Status status;
    SourceFormatConnector::SfTargetDirectory *dir = NULL;

    assert(hdr->type == ExTargetSFType);

    this->targetHashTableLock.lock();
    dir = targetHashTable.remove(hdr->name);
    this->targetHashTableLock.unlock();

    if (dir == NULL) {
        status = StatusTargetDoesntExist;
        goto CommonExit;
    }
    memFree(dir);

    status = StatusOk;
CommonExit:
    return status;
}

int
SourceFormatConnector::countTargets(const char *namePattern)
{
    SourceFormatConnector::SfTargetDirectory *dir;
    int numTargets = 0;

    assert(sfInited);

    this->targetHashTableLock.lock();

    for (TargetHashTable::iterator it = targetHashTable.begin();
         (dir = it.get()) != NULL;
         it.next()) {
        if (strMatch(namePattern, dir->target.name)) {
            numTargets++;
        }
    }

    this->targetHashTableLock.unlock();

    return numTargets;
}

Status
SourceFormatConnector::listTargets(ExExportTarget *targets,
                                   int *ii,
                                   int targetsLength,
                                   const char *namePattern)
{
    Status status;
    SourceFormatConnector::SfTargetDirectory *dir;
    int initialii = *ii;

    assert(sfInited);

    this->targetHashTableLock.lock();

    for (TargetHashTable::iterator it = targetHashTable.begin();
         (dir = it.get()) != NULL;
         it.next()) {
        if (strMatch(namePattern, dir->target.name)) {
            if (*ii >= targetsLength) {
                status = StatusExTargetListRace;
                goto CommonExit;
            }
            // Copy the target info into the caller's buffer
            targets[*ii].hdr = dir->target;
            strlcpy(targets[*ii].specificInput.sfInput.url,
                    dir->input.url,
                    sizeof(targets[*ii].specificInput.sfInput.url));
            (*ii)++;
        }
    }

    this->targetHashTableLock.unlock();

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        *ii = initialii;
    }
    return status;
}
