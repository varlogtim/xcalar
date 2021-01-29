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
#include "AppMgrGvm.h"
#include "app/AppMgr.h"

static constexpr const char *moduleName = "libapp";

AppMgrGvm *AppMgrGvm::instance = NULL;

AppMgrGvm *
AppMgrGvm::get()
{
    return instance;
}

Status
AppMgrGvm::init()
{
    assert(instance == NULL);
    instance = (AppMgrGvm *) memAllocExt(sizeof(AppMgrGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) AppMgrGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
AppMgrGvm::destroy()
{
    instance->~AppMgrGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
AppMgrGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexAppMgr;
}

Status
AppMgrGvm::localHandler(uint32_t action, void *payload, size_t *outputSizeOut)
{
    AppMgr *appMgr = AppMgr::get();

    switch ((Action) action) {
    case Action::Create:
        return appMgr->createLocalHandler(payload);
    case Action::Update:
        return appMgr->updateLocalHandler(payload);
    case Action::Remove:
        appMgr->removeLocalHandler(payload);
        return StatusOk;
    }

    return StatusGvmInvalidAction;
}
