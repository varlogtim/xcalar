// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "dag/DagLib.h"
#include "dag/Dag.h"
#include "DagGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "libdag";

DagGvm *DagGvm::instance;

DagGvm *
DagGvm::get()
{
    return instance;
}

Status
DagGvm::init()
{
    instance = (DagGvm *) memAllocExt(sizeof(DagGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) DagGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
DagGvm::destroy()
{
    instance->~DagGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
DagGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexDag;
}

Status
DagGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    Status status;

    switch ((DagGvm::Action) action) {
    case Action::Create:
        status = DagLib::get()->createNewDagLocal(payload);
        break;

    case Action::Delete:
        status = DagLib::get()->destroyDagLocal(payload);
        break;

    case Action::PreDelete:
        status = DagLib::get()->prepareDeleteDagLocal(payload);
        break;

    default:
        assert(0 && "Unknown Dag GVM action");
        return StatusUnimpl;
        break;
    }

    return status;
}
