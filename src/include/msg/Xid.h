// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XID_H_
#define _XID_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "runtime/Spinlock.h"

class XidMgr final
{
  public:
    // XXX - Need to be deleted as soon as jsongen gets rid of it's use
    static constexpr uint32_t XidLeftShift = 54;
    static_assert(XidLeftShift + MaxNodesBitShift ==
                      sizeof(uint64_t) * BitsPerUInt8,
                  "XidLeftShift + MaxNodesBitShift == "
                  "sizeof(uint64_t) * BitsPerUInt8");

    enum ReservedXids {
        // XIDs between 1 and 256 reserved for future use
        XidGlobalKvStore = 1,
        XidGlobalNsId = 2,
        // These are fixed system DHTs reserved to avoid a twoPc LibNs
        // lookup.
        XidSystemUnorderedDht = 3,
        XidSystemAscendingDht = 4,
        XidSystemDescendingDht = 5,
        XidSystemRandomDht = 6,
        XidSystemBroadcastDht = 7,

        XidReservedCount = 256,
    };

    static XidMgr* get();
    static Status init();
    void destroy();

    bool xidIsLocal(Xid xid);
    NodeId xidGetNodeId(Xid xid);
    Xid xidGetNext();

  private:
    static XidMgr* instance;
    bool init_ = false;

    struct XidInternal {
        union {
            Xid xid;
            struct {
                uint64_t xidLsb : XidLeftShift;
                uint64_t nodeId : MaxNodesBitShift;
            };
        };
    };

    // Cmd Seq Num - Globally unique but local generated generation count
    XidInternal xidGlobal_;

    // Xid creation lock
    Spinlock xidSerializeLock_;

    XidMgr() {}
    ~XidMgr() {}
    XidMgr(const XidMgr&) = delete;
    XidMgr(const XidMgr&&) = delete;
    XidMgr& operator=(const XidMgr&) = delete;
    XidMgr& operator=(const XidMgr&&) = delete;
};

#endif  // _XID_H_
