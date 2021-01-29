// Copyright 2017 Xcalar, Inc. All rights reserved.
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
#include <sys/mman.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>

#include "primitives/Primitives.h"
#include "util/MemTrack.h"

#include "LogDump.h"

#include "runtime/Runtime.h"
#include "dag/DagLib.h"
#include "dag/DagTypes.h"
#include "DurableVersions.h"
#include "durable/Durable.h"
#include "SessionDurable.h"
#include "kvstore/KvStore.h"
#include "KvStoreDurable.h"
#include "DataTargetDurable.h"

static constexpr const char *moduleName = "LogUpgrade";

using namespace xcalar::internal;

typedef struct UpgradeDurablesArgs {
    LdOptions *options;
    Status status;
} UpgradeDurablesArgs;

MustCheck Status upgradeDataTarget(LogLib::Handle *h);
MustCheck Status upgradeKvStore(const char *kvStoreName);

Status
upgradeDataTarget(LogLib::Handle *h)
{
    Status status;
    int numTargets;
    ExExportTarget *exportTgt = NULL;
    DataTargetManager &targetMgr = DataTargetManager::getRef();

    targetMgr.targetLog_ = *h;
    status = targetMgr.readTargets(&numTargets, &exportTgt);
    BailIfFailed(status);

    status = targetMgr.writeTargets();
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
upgradeKvStore(const char *kvStoreName)
{
    Status status;
    KvStore *kvStore = NULL;
    char mdbFname[XcalarApiMaxPathLen];

    kvStore = (KvStore *) memAllocExt(sizeof(KvStore), moduleName);
    BailIfNull(kvStore);

    new (kvStore) KvStore(0, KvStoreInvalid);

    status = kvStore->setName(kvStoreName);
    BailIfFailed(status);

    status = kvStore->getContainingDir(mdbFname, sizeof(mdbFname));
    BailIfFailed(status);

    if (access(mdbFname, F_OK) != -1) {
        // We already have an LMDB kvstore of this name.  So kvstore is already
        // up to date so don't smash it here.  Someone already did an upgrade
        // or an upgrade is unnecessary.
        status = StatusInval;
        goto CommonExit;
    }

    if (kvStore->canUpgrade()) {
        // Reads in the old kvstore format and writes out in the new format
        status = kvStore->persistedInit(kvStoreName);
        BailIfFailed(status);
    } else {
        status = StatusInval;
    }

CommonExit:
    if (kvStore != NULL) {
        kvStore->~KvStore();
        memFree(kvStore);
        kvStore = NULL;
    }

    return status;
}

void *
upgradeDurablesThr(void *args)
{
    Status status;
    UpgradeDurablesArgs *upgradeArgs = (UpgradeDurablesArgs *) args;
    LogLib *logLib = LogLib::get();
    LogLib::Handle lh;
    bool logOpen = false;

    status = upgradeKvStore(upgradeArgs->options->prefix);
    if (status == StatusOk) {
        goto CommonExit;
    }

    status = SessionMgr::Session::
        upgradeFromLog(upgradeArgs->options->prefix,
                       SessionMgr::Session::LogPrefix::Unknown,
                       NULL,
                       NULL);
    if (status == StatusOk) {
        goto CommonExit;
    }

    // Try the create/open in LogLib::TargetDirIndex
    status =
        logLib->create(&lh,
                       LogLib::TargetDirIndex,
                       upgradeArgs->options->prefix,
                       LogLib::FileSeekToLogicalEnd |
                           LogLib::FileExpandIfNeeded | LogLib::FileNoCreate,
                       LogLib::LogDefaultFileCount,
                       LogLib::LogDefaultFileSize);
    if (status == StatusOk) {
        logOpen = true;

        if (!logLib->isEmpty(&lh)) {
            status = upgradeDataTarget(&lh);
            if (status == StatusOk) {
                goto CommonExit;
            }
        }
    }

    // Without parsing the filename we don't know a-priori which dirIndex a
    // prefix belongs in, so we expect to hit this case
    if (status == StatusNoEnt) {
        status = StatusOk;
    } else {
        status = StatusDurVerError;
    }

CommonExit:
    if (logOpen) {
        logLib->close(&lh);
        logOpen = false;
    }
    upgradeArgs->status = status;

    if (status != StatusOk) {
        fprintf(stderr,
                "Could not load log set %s: %s\n",
                upgradeArgs->options->prefix,
                strGetFromStatus(status));
    }
    return NULL;
}

Status
upgradeDurables(LdOptions *options)
{
    Status status;
    pthread_t threadHandle;
    UpgradeDurablesArgs upgradeArgs;
    upgradeArgs.options = options;

    // DagLib expectes to be running in runtime thread context.
    status = Runtime::get()->createBlockableThread(&threadHandle,
                                                   NULL,
                                                   upgradeDurablesThr,
                                                   &upgradeArgs);

    BailIfFailed(status);

    sysThreadJoin(threadHandle, NULL);
    status = upgradeArgs.status;

CommonExit:
    return status;
}
