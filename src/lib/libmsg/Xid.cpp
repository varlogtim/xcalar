// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "msg/Xid.h"
#include "util/MemTrack.h"
#include "util/Atomics.h"

static constexpr const char *moduleName = "libXid";
XidMgr *XidMgr::instance = NULL;

XidMgr *
XidMgr::get()
{
    return instance;
}

Status
XidMgr::init()
{
    assert(instance == NULL);
    instance = (XidMgr *) memAllocExt(sizeof(XidMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) XidMgr();

    // Reserve XIDs for static use across all nodes
    instance->xidGlobal_.xidLsb = XidReservedCount;
    instance->xidGlobal_.nodeId = (unsigned char) Config::get()->getMyNodeId();

    instance->init_ = true;
    return StatusOk;
}

void
XidMgr::destroy()
{
    init_ = false;
    instance->~XidMgr();
    memFree(instance);
    instance = NULL;
}

bool
XidMgr::xidIsLocal(Xid xid)
{
    return xidGetNodeId(xid) == Config::get()->getMyNodeId();
}

NodeId
XidMgr::xidGetNodeId(Xid xidIn)
{
    XidInternal xidTemp;
    xidTemp.xid = xidIn;

    NodeId nodeId = xidTemp.nodeId;

    assert(nodeId < (NodeId) Config::get()->getActiveNodes());

    return nodeId;
}

// Even though the variable xid is local to every node is kept unique in
// the cluster by using the format myNodeId:local generation count, where
// the two numbers are concatinated.
Xid
XidMgr::xidGetNext()
{
    XidInternal xidTemp;

    xidSerializeLock_.lock();
    ++xidGlobal_.xidLsb;
    xidTemp.xid = xidGlobal_.xid;
    xidSerializeLock_.unlock();
    if (unlikely(xidTemp.xidLsb == 0)) {
        // @SymbolCheckIgnore
        panic("Xid has wrapped around");
    }
    return xidTemp.xid;
}
