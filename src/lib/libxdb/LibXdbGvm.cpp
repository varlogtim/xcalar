// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "xdb/Xdb.h"
#include "LibXdbGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "libXdbGvm";

LibXdbGvm *LibXdbGvm::instance;

LibXdbGvm *
LibXdbGvm::get()
{
    return instance;
}

Status
LibXdbGvm::init()
{
    instance = (LibXdbGvm *) memAllocExt(sizeof(LibXdbGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) LibXdbGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
LibXdbGvm::destroy()
{
    instance->~LibXdbGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
LibXdbGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexXdb;
}

Status
LibXdbGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    switch ((Action) action) {
    case Action::Create:
        return XdbMgr::get()->xdbAllocLocal((void *) payload);
        break;
    default:
        assert(0 && "Invalid GVM action");
        break;
    }
    return StatusGvmInvalidAction;
}
