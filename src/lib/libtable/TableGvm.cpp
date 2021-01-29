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
#include "table/TableGvm.h"
#include "table/Table.h"
#include "strings/String.h"

static constexpr const char *moduleName = "tableGvm";

TableGvm *TableGvm::instance;

TableGvm *
TableGvm::get()
{
    return instance;
}

Status
TableGvm::init()
{
    instance = (TableGvm *) memAllocExt(sizeof(TableGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) TableGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
TableGvm::destroy()
{
    instance->~TableGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
TableGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexTable;
}

Status
TableGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    Status status = StatusOk;

    switch ((TableGvm::Action) action) {
    case Action::AddObj: {
        AddTableObj *ip = (AddTableObj *) payload;
        status = TableMgr::get()->addTableObjLocal(ip->tableId,
                                                   ip->dagId,
                                                   ip->dagNodeId,
                                                   ip->fullyQualName,
                                                   &ip->sessionContainer);
        BailIfFailed(status);
        break;
    }

    case Action::RemoveObj: {
        RemoveTableObj *ip = (RemoveTableObj *) payload;
        TableMgr::get()->removeTableObjLocal(ip->tableId);
        break;
    }

    default:
        assert(0 && "Unknown Table GVM action");
        return StatusUnimpl;
        break;
    }

CommonExit:
    return status;
}
