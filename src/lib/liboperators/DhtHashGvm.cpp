// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "DhtHashGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "operators/Dht.h"

static constexpr const char *moduleName = "dhtHashGvm";

DhtHashGvm *DhtHashGvm::instance;

DhtHashGvm *
DhtHashGvm::get()
{
    return instance;
}

Status
DhtHashGvm::init()
{
    instance = (DhtHashGvm *) memAllocExt(sizeof(DhtHashGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) DhtHashGvm();
    Gvm::get()->registerTarget(instance);

    return StatusOk;
}

void
DhtHashGvm::destroy()
{
    instance->~DhtHashGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
DhtHashGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexDhtHash;
}

Status
DhtHashGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    DhtMgr *dhtMgr = DhtMgr::get();

    switch ((Action) action) {
    case Action::Add:
        return dhtMgr->dhtAddDhtLocal((void *) payload);
        break;
    case Action::Delete:
        return dhtMgr->dhtDeleteDhtLocal((void *) payload);
    default:
        assert(0 && "Invalid GVM action");
        break;
    }

    return StatusGvmInvalidAction;
}
