// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DHT_H
#define _DHT_H

#include "primitives/Primitives.h"
#include "runtime/Mutex.h"
#include "operators/DhtTypes.h"
#include "util/IntHashTable.h"
#include "hash/Hash.h"
#include "util/Random.h"

class BcHandle;

class DhtMgr final
{
    friend class DhtHashGvm;

  public:
    static constexpr const char *DhtSystemDescendingDht = "systemDescendingDht";
    static constexpr const char *DhtSystemAscendingDht = "systemAscendingDht";
    static constexpr const char *DhtSystemUnorderedDht = "systemUnorderedDht";
    static constexpr const char *DhtSystemRandomDht = "systemRandomDht";
    static constexpr const char *DhtSystemBroadcastDht = "systemBroadcastDht";
    // Saved DHT file name prefix
    static constexpr const char *dhtFilePrefix = "Xcalar.DHT.";
    // DHT external block constants
    static constexpr const char *DhtExtEyecStr = "XDHT";
    static constexpr const char *DhtExtEndEyecStr = "ZDHT";

    // Arbitrary number
    static constexpr unsigned MaxNumDhts = 4096;

    // Initializaton and termination
    static MustCheck Status init();
    void destroy();
    MustCheck static DhtMgr *get();

    // Recreate previous DHT environment
    MustCheck Status dhtStateRestore();

    // Create/Delete system DHTs
    MustCheck Status dhtCreateDefaultDhts();
    void dhtDeleteDefaultDhts();

    // Mainline functions
    MustCheck Status dhtGetDhtId(const char *dhtName, DhtId *dhtId);
    MustCheck Status dhtCreate(const char *dhtName,
                               float64_t lowerBound,
                               float64_t upperBound,
                               Ordering ordering,
                               DhtBroadcast broadcast,
                               DhtAutoRemove autoRemove,
                               DhtId *dhtIdOut);
    // Returns a pointer to the Dht associated with the dhtID.  The Dht should
    // be considered "read only" as it remains in the GVM hash table.
    MustCheck Dht *dhtGetDht(DhtId dhtId);

    MustCheck Status dhtDelete(const char *dhtName);

    MustCheck DhtHandle dhtOpen(DhtId dhtId, Status *status);
    MustCheck Status dhtClose(DhtHandle dhtHandle, DhtId dhtId);

    MustCheck Status isValidDhtName(const char *dhtName);
    MustCheck Status dhtMigrate(DhtExternal *oldDht, DhtExternal *newDht);

    MustCheck unsigned dhtHashFloat64(Dht *dht, double in, bool doCrc);
    MustCheck unsigned dhtHashUInt64(Dht *dht, uint64_t in);
    MustCheck unsigned dhtHashInt64(Dht *dht, int64_t in);
    MustCheck unsigned dhtHashString(Dht *dht, const char *in);

    // gets a node id based on a racy round robin algorithm
    unsigned getNextNodeId();

  private:
    static constexpr const char *moduleName = "dhtMgr";
    static constexpr unsigned DhtHTSlots = 17;

    static DhtMgr *instance;

    BcHandle *dhtBc_ = NULL;
    bool defaultDhtsCreated_ = false;
    unsigned numNodes_;
    static thread_local uint64_t randomCounter_;

    MustCheck Status dhtAddViaGvm(const char *dhtName,
                                  const DhtId dhtId,
                                  const float64_t lowerBound,
                                  const float64_t upperBound,
                                  const Ordering ordering,
                                  DhtBroadcast broadcast);
    MustCheck Status dhtDeleteViaGvm(DhtId dhtId);

    // Gvm handlers
    MustCheck Status dhtAddDhtLocal(void *payload);
    MustCheck Status dhtDeleteDhtLocal(void *payload);

    // Hash table managed by Gvm handler to add/delete Dht.  Local access
    // is allowed directly to get Dht.  The Dht must be protected from deletion
    // while being used by opening the Dht path name within LibNs.
    struct DhtIdToDhtHTEntry {
        IntHashTableHook hook;
        DhtId getDhtId() const { return dhtId; };
        DhtId dhtId;
        Dht dht;
        void del();
    };

    IntHashTable<DhtId,
                 DhtIdToDhtHTEntry,
                 &DhtIdToDhtHTEntry::hook,
                 &DhtIdToDhtHTEntry::getDhtId,
                 DhtHTSlots,
                 hashIdentity>
        dhtIdToDhtHT_;

    Mutex dhtIdToDhtHTLock_;

    // Disallow
    DhtMgr() {}   // Use init
    ~DhtMgr() {}  // Use destroy

    DhtMgr(const DhtMgr &) = delete;
    DhtMgr &operator=(const DhtMgr &) = delete;

  public:
    static constexpr unsigned BcDhtBufSize = sizeof(DhtIdToDhtHTEntry);
    static constexpr uint64_t BcDhtNumElems = 128;
};
#endif  // _DHT_H
