// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "dag/Dag.h"
#include "DagNodeGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "libdag";

DagNodeGvm *DagNodeGvm::instance;

DagNodeGvm *
DagNodeGvm::get()
{
    return instance;
}

Status
DagNodeGvm::init()
{
    instance = (DagNodeGvm *) memAllocExt(sizeof(DagNodeGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) DagNodeGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
DagNodeGvm::destroy()
{
    instance->~DagNodeGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
DagNodeGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexDagNode;
}

Status
DagNodeGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    return Dag::updateLocalHandler(action, payload);
}
