// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "operators/Operators.h"
#include "OperatorsGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "OperatorsGvm";

OperatorsGvm *OperatorsGvm::instance;

OperatorsGvm *
OperatorsGvm::get()
{
    return instance;
}

Status
OperatorsGvm::init()
{
    instance = (OperatorsGvm *) memAllocExt(sizeof(OperatorsGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) OperatorsGvm();
    Gvm::get()->registerTarget(instance);

    return StatusOk;
}

void
OperatorsGvm::destroy()
{
    instance->~OperatorsGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
OperatorsGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexOperators;
}

Status
OperatorsGvm::localHandler(uint32_t action,
                           void *payload,
                           size_t *outputSizeOut)
{
    Operators *oper = Operators::get();

    switch ((Action) action) {
    case Action::AddPerTxnInfo:
        return oper->addPerTxnInfo(payload, outputSizeOut);
        break;
    case Action::UpdatePerTxnInfo:
        return oper->updatePerTxnInfo(payload);
        break;
    case Action::DeletePerTxnInfo:
        oper->removePerTxnInfo(payload);
        return StatusOk;
        break;
    default:
        assert(0 && "Invalid GVM action");
        break;
    }

    return StatusGvmInvalidAction;
}
