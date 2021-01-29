// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "xdb/MergeGvm.h"
#include "strings/String.h"

static constexpr const char *moduleName = "mergeGvm";

MergeGvm *MergeGvm::instance;

MergeGvm *
MergeGvm::get()
{
    return instance;
}

Status
MergeGvm::init()
{
    instance = (MergeGvm *) memAllocExt(sizeof(MergeGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) MergeGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
MergeGvm::destroy()
{
    instance->~MergeGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
MergeGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexMergeOperator;
}

Status
MergeGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    Status status = StatusOk;
    const char *tableName[Operators::MergeNumTables];
    TableNsMgr::IdHandle *idHandle[Operators::MergeNumTables];
    XdbId xdbId[Operators::MergeNumTables];

    switch ((MergeGvm::Action) action) {
    case Action::InitMerge: {
        MergeInitInput *ip = (MergeInitInput *) payload;
        for (unsigned ii = Operators::MergeDeltaTable;
             ii < Operators::MergeNumTables;
             ii++) {
            tableName[ii] = ip->tableInfo[ii].tableName;
            idHandle[ii] = &ip->tableInfo[ii].idHandle;
            xdbId[ii] = ip->tableInfo[ii].xdbId;
        }
        status = Operators::get()->mergeInitLocal(tableName, idHandle, xdbId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergeInitLocal delta table %s metadata, "
                        "target table %s: %s",
                        tableName[Operators::MergeDeltaTable],
                        tableName[Operators::MergeTargetTable],
                        strGetFromStatus(status));
        break;
    }
    case Action::PrepareMerge: {
        MergePrepareInput *ip = (MergePrepareInput *) payload;
        for (unsigned ii = Operators::MergeDeltaTable;
             ii < Operators::MergeNumTables;
             ii++) {
            tableName[ii] = ip->tableInfo[ii].tableName;
            idHandle[ii] = &ip->tableInfo[ii].idHandle;
            xdbId[ii] = ip->tableInfo[ii].xdbId;
        }
        status =
            Operators::get()->mergePrepareLocal(tableName, idHandle, xdbId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergePrepareLocal delta table %s metadata, "
                        "target table %s: %s",
                        tableName[Operators::MergeDeltaTable],
                        tableName[Operators::MergeTargetTable],
                        strGetFromStatus(status));
        break;
    }

    case Action::PostCommitMerge: {
        MergePostCommitInput *ip = (MergePostCommitInput *) payload;
        status = Operators::get()->mergePostCommitLocal(&ip->idHandle);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergePostCommitLocal target table %lu: %s",
                        ip->idHandle.tableId,
                        strGetFromStatus(status));
        break;
    }

    case Action::AbortMerge: {
        MergeAbortInput *ip = (MergeAbortInput *) payload;
        for (unsigned ii = Operators::MergeDeltaTable;
             ii < Operators::MergeNumTables;
             ii++) {
            tableName[ii] = ip->tableInfo[ii].tableName;
            idHandle[ii] = &ip->tableInfo[ii].idHandle;
            xdbId[ii] = ip->tableInfo[ii].xdbId;
        }
        status = Operators::get()->mergeAbortLocal(tableName, idHandle, xdbId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergeAbortLocal delta table %s metadata, "
                        "target table %s: %s",
                        tableName[Operators::MergeDeltaTable],
                        tableName[Operators::MergeTargetTable],
                        strGetFromStatus(status));
        break;
    }

    default:
        assert(0 && "Unknown Merge GVM action");
        return StatusUnimpl;
        break;
    }

CommonExit:
    return status;
}

Status
MergeGvm::MergeTableInfo::init(const char *tableName,
                               TableNsMgr::IdHandle *idHandle,
                               XdbId xdbId)
{
    Status status;
    status =
        strStrlcpy(this->tableName, tableName, XcalarApiMaxTableNameLen + 1);
    BailIfFailed(status);

    memcpy(&this->idHandle, idHandle, sizeof(TableNsMgr::IdHandle));
    this->xdbId = xdbId;
CommonExit:
    return status;
}
