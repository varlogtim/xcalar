// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "runtime/Mutex.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "LicenseGvm.h"
#include "util/License.h"

static constexpr const char *moduleName = "liblicense";

LicenseGvm *LicenseGvm::instance;

LicenseGvm *
LicenseGvm::get()
{
    return instance;
}

Status
LicenseGvm::init()
{
    instance = (LicenseGvm *) memAllocExt(sizeof(LicenseGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) LicenseGvm();

    Gvm::get()->registerTarget(instance);

    return StatusOk;
}

void
LicenseGvm::destroy()
{
    instance->~LicenseGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
LicenseGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexLicense;
}

Status
LicenseGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    Status status;
    LicenseMgr *licenseMgr = LicenseMgr::get();

    switch ((LicenseGvm::Action) action) {
    case Action::Update:
        status = licenseMgr->updateLicenseLocal(payload);
        break;

    case Action::Revert:
        status = licenseMgr->revertLicenseLocal(payload);
        break;

    default:
        assert(0 && "Unknown License GVM action");
        return StatusUnimpl;
        break;
    }

    return status;
}
