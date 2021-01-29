// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/MemTrack.h"
#include "bcquota/BufferCacheQuota.h"

static constexpr const char *moduleName = "libbcquota";

BcQuota *
BcQuota::quotaCreate(BcHandle *bcHandle,
                     uint64_t *clientQuota,
                     uint64_t clientQuotaLen)
{
    uint64_t ii;
    uint64_t numElems;
    Status status = StatusOk;

    assert(clientQuota != NULL);

    BcQuota *bcQuota = (BcQuota *) memAllocExt(sizeof(BcQuota), moduleName);
    if (bcQuota == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    bcQuota->bucketCount = clientQuotaLen;
    bcQuota->buckets =
        (Bucket *) memAllocExt(sizeof(Bucket) * bcQuota->bucketCount,
                               moduleName);
    if (bcQuota->buckets == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0, numElems = 0; ii < clientQuotaLen; ii++, numElems++) {
        assert(clientQuota[ii] != 0);
        bcQuota->buckets[ii].total = clientQuota[ii];
        atomicWrite64(&bcQuota->buckets[ii].outstanding, 0);
    }

    bcQuota->bcHandle = bcHandle;

CommonExit:
    if (status != StatusOk) {
        if (bcQuota != NULL) {
            quotaDestroy(&bcQuota);
            bcQuota = NULL;
        }
    }

    return bcQuota;
}

void
BcQuota::quotaDestroy(BcQuota **bcQuota)
{
    assert(bcQuota != NULL && *bcQuota != NULL);

    (*bcQuota)->bcHandle = NULL;

    if ((*bcQuota)->buckets != NULL) {
        memFree((*bcQuota)->buckets);
        (*bcQuota)->buckets = NULL;
    }
    (*bcQuota)->bucketCount = 0ULL;

    memFree(*bcQuota);
    (*bcQuota) = NULL;
}

void *
BcQuota::quotaAllocBuf(uint64_t clientQuotaIdx)
{
    if (clientQuotaIdx + 1 > this->bucketCount) {
        return NULL;
    }

    // Expect to not spin for long here. i.e. between atomic read and CAS,
    // there should not be too much contention algorithmically.
    while (true) {
        uint64_t quota, tmpQuota;
        quota = atomicRead64(&this->buckets[clientQuotaIdx].outstanding);
        if (quota >= buckets[clientQuotaIdx].total) {
            return NULL;
        }
        tmpQuota = atomicCmpXchg64(&this->buckets[clientQuotaIdx].outstanding,
                                   quota,
                                   quota + 1);
        if (tmpQuota == quota) {
            // Now we have the Quota.
            break;
        }
    }

    void *buf = this->bcHandle->allocBuf(XidInvalid);
    if (buf == NULL) {
        // Put the quota resources back to the bucket, since the bcAlloc failed
        // here.
        uint64_t quota =
            atomicDec64(&this->buckets[clientQuotaIdx].outstanding);
        assert(quota < this->buckets[clientQuotaIdx].total);
    }

    return buf;
}

void
BcQuota::quotaFreeBuf(void *buf, uint64_t clientQuotaIdx)
{
    assert(clientQuotaIdx + 1 <= this->bucketCount);

    uint64_t quota = atomicDec64(&this->buckets[clientQuotaIdx].outstanding);
    assert(quota < this->buckets[clientQuotaIdx].total);

    this->bcHandle->freeBuf(buf);
}
