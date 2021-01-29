// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "export/DataTarget.h"
#include "DataTargetGvm.h"

static constexpr const char *moduleName = "libexport";

DataTargetGvm *DataTargetGvm::instance;

DataTargetGvm *
DataTargetGvm::get()
{
    return instance;
}

Status
DataTargetGvm::init()
{
    instance = (DataTargetGvm *) memAllocExt(sizeof(DataTargetGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) DataTargetGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
DataTargetGvm::destroy()
{
    instance->~DataTargetGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
DataTargetGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexDataTargetMgr;
}

Status
DataTargetGvm::localHandler(uint32_t action,
                            void *payload,
                            size_t *outputSizeOut)
{
    Status status;

    switch ((DataTargetGvm::Action) action) {
    case Action::Add:
        status = DataTargetManager::getRef().addLocalHandler(payload);
        break;

    case Action::Remove:
        status = DataTargetManager::getRef().removeLocalHandler(payload);
        break;

    default:
        assert(0 && "Unknown DataTarget GVM action");
        return StatusUnimpl;
        break;
    }

    return status;
}
