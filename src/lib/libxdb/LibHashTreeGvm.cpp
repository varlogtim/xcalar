// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "xdb/LibHashTreeGvm.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "xdb/HashTree.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libhashtree";

LibHashTreeGvm *LibHashTreeGvm::instance;

LibHashTreeGvm *
LibHashTreeGvm::get()
{
    return instance;
}

Status
LibHashTreeGvm::init()
{
    instance =
        (LibHashTreeGvm *) memAllocExt(sizeof(LibHashTreeGvm), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) LibHashTreeGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
LibHashTreeGvm::destroy()
{
    instance->~LibHashTreeGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
LibHashTreeGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexHashTree;
}

Status
LibHashTreeGvm::localHandler(uint32_t action,
                             void *payload,
                             size_t *outputSizeOut)
{
    Status status = StatusOk;
    GvmInput *input = (GvmInput *) payload;
    HashTreeMgr *htMgr = HashTreeMgr::get();

    xSyslog(moduleName,
            XlogDebug,
            "Performing action %d on hash tree %lu",
            action,
            input->hashTreeId);

    switch ((LibHashTreeGvm::Action) action) {
    case Action::InitState:
        status = htMgr->initState(input->hashTreeId, input->pubTableName);
        break;

    case Action::Create:
        status = htMgr->createHashTreeLocal(input->srcTable,
                                            input->hashTreeId,
                                            (HashTreeMgr::CreateReason)
                                                input->createReason,
                                            input->pubTableName);
        break;

    case Action::Update: {
        HashTree::UpdateStats updateOutput;
        status = htMgr->updateInMemHashTreeLocal(input->srcTable,
                                                 input->unixTS,
                                                 input->dropSrc,
                                                 input->size,
                                                 input->numRows,
                                                 input->batchId,
                                                 input->hashTreeId,
                                                 (HashTreeMgr::UpdateReason)
                                                     input->updateReason,
                                                 &updateOutput);

        // repurpose the input payload as the output payload
        assert(sizeof(*input) >= sizeof(updateOutput));

        *(HashTree::UpdateStats *) payload = updateOutput;
        *outputSizeOut = sizeof(updateOutput);
        break;
    }
    case Action::Select: {
        GvmSelectInput *selectInput = (GvmSelectInput *) payload;

        status = htMgr->selectHashTreeLocal(selectInput);
        break;
    }
    case Action::Coalesce:
        status =
            htMgr->coalesceHashTreeLocal(input->hashTreeId, input->batchId);
        break;

    case Action::Inactivate:
        htMgr->inactivateHashTreeLocal(input->hashTreeId, input->pubTableName);
        break;

    case Action::Destroy:
        htMgr->destroyHashTreeLocal(input->hashTreeId, input->pubTableName);
        break;

    case Action::Revert:
        htMgr->revertHashTreeLocal(input->batchId, input->hashTreeId);
        break;

    case Action::GetSize:
        htMgr->getSizeLocal(input->hashTreeId, payload, outputSizeOut);
        break;

    case Action::RefreshSize:
        htMgr->refreshTotalSizeLocal(input->hashTreeId,
                                     input->size,
                                     input->numRows);
        break;

    case Action::AddIndex: {
        GvmIndexInput *input = (GvmIndexInput *) payload;

        status = htMgr->addIndexToHashTreeLocal(input->hashTreeId, input->key);
        break;
    }

    case Action::RemoveIndex: {
        GvmIndexInput *input = (GvmIndexInput *) payload;

        htMgr->removeIndexFromHashTreeLocal(input->hashTreeId, input->key);
        break;
    }

    default:
        assert(0 && "Unknown HashTree GVM action");
        return StatusUnimpl;
        break;
    }

    return status;
}
