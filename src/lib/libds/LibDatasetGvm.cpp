// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "dataset/LibDatasetGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"

static constexpr const char *moduleName = "libds";

LibDatasetGvm *LibDatasetGvm::instance;

LibDatasetGvm *
LibDatasetGvm::get()
{
    return instance;
}

Status
LibDatasetGvm::init()
{
    instance = (LibDatasetGvm *) memAllocExt(sizeof(LibDatasetGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) LibDatasetGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
LibDatasetGvm::destroy()
{
    instance->~LibDatasetGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
LibDatasetGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexDataset;
}

Status
LibDatasetGvm::localHandler(uint32_t action,
                            void *payload,
                            size_t *outputSizeOut)
{
    Status status;

    switch ((LibDatasetGvm::Action) action) {
    case Action::CreateStruct:
        status = Dataset::get()->createDatasetStructLocal(payload);
        break;

    case Action::Unload:
        status = Dataset::get()->unloadDatasetLocal(payload);
        break;

    case Action::Finalize:
        status = Dataset::get()->finalizeDatasetLocal(payload, outputSizeOut);
        break;

    default:
        assert(0 && "Unknown Dataset GVM action");
        return StatusUnimpl;
        break;
    }

    return status;
}
