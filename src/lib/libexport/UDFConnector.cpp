// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include <jansson.h>

#include "StrlFunc.h"
#include "DataTargetEnums.h"
#include "UDFConnector.h"
#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "hash/Hash.h"
#include "sys/XLog.h"
#include "LibExportConstants.h"
#include "app/AppMgr.h"
#include "udf/UserDefinedFunction.h"

using namespace Export;

static bool udfInited = false;

static constexpr char const *moduleName = "libexport";

UDFConnector *UDFConnector::udfConnector = NULL;

/* static */ Status
UDFConnector::init()
{
    Status status;
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(*UDFConnector::udfConnector), __PRETTY_FUNCTION__);
    BailIfNull(ptr);

    UDFConnector::udfConnector = new (ptr) UDFConnector();

    status = UDFConnector::udfConnector->initInternal();
    BailIfFailed(status);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (ptr != NULL) {
            assert(UDFConnector::udfConnector != NULL);
            UDFConnector::udfConnector->~UDFConnector();
            memFree(UDFConnector::udfConnector);
        }
    }
    return status;
}

void
UDFConnector::destroy()
{
    auto &udfC = UDFConnector::getRef();
    udfC.~UDFConnector();
    memFree(&udfC);
}

/* static */ UDFConnector &
UDFConnector::getRef()
{
    assert(udfConnector);
    return *udfConnector;
}

Status
UDFConnector::initInternal()
{
    assert(!udfInited);

    udfInited = true;
    return StatusOk;
}

UDFConnector::~UDFConnector()
{
    this->targetHashTableLock.lock();
    targetHashTable.removeAll(&UdfTargetDirectory::del);
    this->targetHashTableLock.unlock();
}

void
UDFConnector::UdfTargetDirectory::del()
{
    memFree(this);
}

Status
UDFConnector::getDefaultTargets(int *numTargets, ExExportTarget **targets)
{
    *numTargets = 0;
    *targets = NULL;
    return StatusOk;
}

Status
UDFConnector::shouldPersist(const ExExportTarget *target, bool *persist)
{
    assert(target->hdr.type == ExTargetUDFType);
    *persist = true;
    return StatusOk;
}

Status
UDFConnector::verifyTarget(const ExExportTarget *target)
{
    return StatusOk;
}

Status
UDFConnector::addTargetLocal(const ExExportTarget *target)
{
    Status status;
    UDFConnector::UdfTargetDirectory *dir = NULL;
    const ExAddTargetSpecificInput *input;
    bool allocated = false;

    assert(target->hdr.type == ExTargetUDFType);
    input = &target->specificInput;

    this->targetHashTableLock.lock();
    dir = targetHashTable.find(target->hdr.name);
    this->targetHashTableLock.unlock();

    if (dir != NULL) {
        status = StatusExTargetAlreadyExists;
        goto CommonExit;
    }

    dir = (UDFConnector::UdfTargetDirectory *)
        memAllocExt(sizeof(UDFConnector::UdfTargetDirectory), moduleName);
    if (dir == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    allocated = true;

    dir->target = target->hdr;
    strlcpy(dir->input.url, input->udfInput.url, sizeof(dir->input.url));
    strlcpy(dir->input.appName,
            input->udfInput.appName,
            sizeof(dir->input.appName));

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
UDFConnector::removeTargetLocal(const ExExportTargetHdr *hdr)
{
    Status status;
    UDFConnector::UdfTargetDirectory *dir = NULL;

    assert(hdr->type == ExTargetUDFType);

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
UDFConnector::countTargets(const char *namePattern)
{
    UDFConnector::UdfTargetDirectory *dir;
    int numTargets = 0;

    assert(udfInited);

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
UDFConnector::listTargets(ExExportTarget *targets,
                          int *ii,
                          int targetsLength,
                          const char *namePattern)
{
    Status status;
    UDFConnector::UdfTargetDirectory *dir;
    int initialii = *ii;

    assert(udfInited);

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
            strlcpy(targets[*ii].specificInput.udfInput.url,
                    dir->input.url,
                    sizeof(targets[*ii].specificInput.udfInput.url));
            strlcpy(targets[*ii].specificInput.udfInput.appName,
                    dir->input.appName,
                    sizeof(targets[*ii].specificInput.udfInput.appName));
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
