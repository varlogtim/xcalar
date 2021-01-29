// Copyright 2013 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _BUFFERCACHEQUOTA_H_
#define _BUFFERCACHEQUOTA_H_

#include "bc/BufferCache.h"

// - Buffer Cache provides contiguous memory resources for it's clients in big
// chunks. If a client chooses to manage this contiguous memory logically as
// a collection of BufferSize chunks, BcQuota can be used.
// - For instance, a Buffer cache client could have N different subsysytems, say
// SS0, SS1, ... SSn-1 and each of these would be given a quota of Qx BufferSize
// chunks.
// - BcQuota now fails allocation request, if a subsystem SSx exceeds it's given
// quota Qx. This happens even if there is resource left in backing BufferCache
// in it's freelist.
class BcQuota
{
  public:
    // @param bcHandle: BcHandle that requires quota management.
    // @param clientQuota: Array of client quotas.
    // @param clientQuotaLen: Length of client quota array.
    // @return: BcQuota
    MustCheck static BcQuota *quotaCreate(BcHandle *bcHandle,
                                          uint64_t *clientQuota,
                                          uint64_t clientQuotaLen);

    // Delete Buf$ Quota.
    // @param bcQuota: BcQuota to destroy.
    static void quotaDestroy(BcQuota **bcQuota);

    // Allocate based on Quota availability.
    // @param clientQuotaIdx: Index into the quota buckets.
    // @return: Return allocated buffer for success, NULL for failure.
    MustCheck void *quotaAllocBuf(uint64_t clientQuotaIdx);

    // Free back to Quota pool.
    // @param buf: Free buffer back to the quota buckets.
    // @param clientQuotaIdx: Index into the quota buckets.
    void quotaFreeBuf(void *buf, uint64_t clientQuotaIdx);

  private:
    struct Bucket {
        uint64_t total;        // Resource owned.
        Atomic64 outstanding;  // Resource held outstanding here.
    };

    Bucket *buckets = NULL;
    uint64_t bucketCount = 0ULL;

    // XXX Inherit BcQuota from BcHandle instead.
    BcHandle *bcHandle = NULL;

    // Use quotaCreate instead.
    BcQuota() {}

    // Use quotaDestroy instead.
    ~BcQuota() {}

    BcQuota(const BcQuota &) = delete;
    BcQuota &operator=(const BcQuota &) = delete;
};

#endif  // _BUFFERCACHEQUOTA_H_
