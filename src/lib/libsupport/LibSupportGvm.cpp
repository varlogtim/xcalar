// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "support/SupportBundle.h"
#include "LibSupportGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "libSupportGvm";

LibSupportGvm *LibSupportGvm::instance;

LibSupportGvm *
LibSupportGvm::get()
{
    return instance;
}

Status
LibSupportGvm::init()
{
    instance = (LibSupportGvm *) memAllocExt(sizeof(LibSupportGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) LibSupportGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
LibSupportGvm::destroy()
{
    instance->~LibSupportGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
LibSupportGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexSupport;
}

Status
LibSupportGvm::localHandler(uint32_t action,
                            void *payload,
                            size_t *outputSizeOut)
{
    switch ((Action) action) {
    case Action::Generate:
        return SupportBundle::get()->supportGenerateLocal((void *) payload);
        break;
    default:
        assert(0 && "Invalid GVM action");
        break;
    }
    return StatusGvmInvalidAction;
}
