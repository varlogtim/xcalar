// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "runtime/Runtime.h"
#include "LibRuntimeGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libRuntimeGvm";

LibRuntimeGvm *LibRuntimeGvm::instance;

LibRuntimeGvm *
LibRuntimeGvm::get()
{
    return instance;
}

Status
LibRuntimeGvm::init()
{
    instance = (LibRuntimeGvm *) memAllocExt(sizeof(LibRuntimeGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) LibRuntimeGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
LibRuntimeGvm::destroy()
{
    instance->~LibRuntimeGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
LibRuntimeGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexRuntime;
}

Status
LibRuntimeGvm::localHandler(uint32_t action,
                            void *payload,
                            size_t *outputSizeOut)
{
    Status status = StatusOk;
    Runtime *runtime = Runtime::get();
    XcalarApiRuntimeSetParamInput *input =
        (XcalarApiRuntimeSetParamInput *) payload;

    switch ((Action) action) {
    case Action::ChangeThreads: {
        status = runtime->changeThreadCountLocalWrap(input);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to change runtime threads: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        break;
    }
    default:
        assert(0 && "Invalid GVM action");
        status = StatusGvmInvalidAction;
        break;
    }
CommonExit:
    return status;
}
