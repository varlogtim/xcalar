// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// Library of hash functions for hashing arbitrary types by key

#include <assert.h>
#include <time.h>
#include <fcntl.h>
#include <dirent.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "operators/Dht.h"
#include "config/Config.h"
#include "bc/BufferCache.h"
#include "operators/OperatorsHash.h"
#include "hash/Hash.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "strings/String.h"
#include "ns/LibNs.h"
#include "msg/Xid.h"
#include "DhtHashGvm.h"
#include "gvm/Gvm.h"
#include "log/Log.h"

// Creation flow:
//  * Dht is created and is assigned a dhtId
//  * Replicated to all nodes via GVM along with the dhtId
//    On each node, the Dht is placed into a hash table keyed by dhtId
//  * An object derived from NsObject is created and has the dhtId as an
//    additional variable.
//  * The fully qualified path name of the Dht along with the derived
//    NsObject are published to LibNs.
//
// Access flow:
//  * Fully qualified path name of Dht is used to get the associated dhtId
//  * The dhtId is opened to protect access (keep Dht from being deleted
//    from underneath).
//  * dhtId is used to obtain a pointer to the associated Dht
//  * The Dht is used
//  * The Dht handle, returned from opening the Dht, is closed.  This
//    releases the protection and the Dht may be deleted if there are no
//    other opens.

static constexpr const char *moduleName = "DhtHash";

DhtMgr *DhtMgr::instance = NULL;

thread_local uint64_t DhtMgr::randomCounter_ = 0;

// Return the ID for the specified DHT name.

Status
DhtMgr::dhtGetDhtId(const char *dhtName, DhtId *dhtId)
{
    // Return the NsId associated with the specified DHT name.
    Status status = StatusOk;
    Status status2 = StatusOk;
    LibNs *libNs = LibNs::get();
    char dhtPathName[LibNsTypes::MaxPathNameLen];
    DhtNsObject *dhtNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    bool objDeleted = false;

    *dhtId = DhtInvalidDhtId;

    if (strcmp(dhtName, DhtSystemAscendingDht) == 0) {
        *dhtId = XidMgr::XidSystemAscendingDht;
    } else if (strcmp(dhtName, DhtSystemDescendingDht) == 0) {
        *dhtId = XidMgr::XidSystemDescendingDht;
    } else if (strcmp(dhtName, DhtSystemUnorderedDht) == 0) {
        *dhtId = XidMgr::XidSystemUnorderedDht;
    } else if (strcmp(dhtName, DhtSystemRandomDht) == 0) {
        *dhtId = XidMgr::XidSystemRandomDht;
    } else if (strcmp(dhtName, DhtSystemBroadcastDht) == 0) {
        *dhtId = XidMgr::XidSystemBroadcastDht;
    } else {
        // Create a fully qualified path name using the supplied dht name.
        if (snprintf(dhtPathName,
                     LibNsTypes::MaxPathNameLen,
                     "%s%s",
                     DhtNsObject::PathPrefix,
                     dhtName) >= (int) LibNsTypes::MaxPathNameLen) {
            status = StatusOverflow;
            goto CommonExit;
        }
        // get the DHT name space object
        nsHandle = libNs->open(dhtPathName,
                               LibNsTypes::ReaderShared,
                               (NsObject **) &dhtNsObject,
                               &status);
        if (status != StatusOk) {
            // Convert LibNs errors to more informative DHT errors
            if (status == StatusNsNotFound || status == StatusPendingRemoval) {
                status = StatusDhtNotFound;
            } else if (status == StatusAccess) {
                status = StatusDhtInUse;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to open '%s': %s",
                    dhtPathName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        *dhtId = dhtNsObject->getDhtId();
        status2 = libNs->close(nsHandle, &objDeleted);
        if (status2 != StatusOk) {
            // As we obtained the desired info, syslog and then
            // don't return the close error
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    dhtPathName,
                    strGetFromStatus(status2));
        }
        assert(!objDeleted);
        memFree(dhtNsObject);
    }

CommonExit:

    return status;
}

Status
DhtMgr::dhtCreateDefaultDhts()
{
    Status status;
    bool unorderedDhtCreated = false;
    bool randomDhtCreated = false;
    bool broadcastDhtCreated = false;
    bool ascendingDhtCreated = false;
    bool descendingDhtCreated = false;

    status = dhtCreate(DhtSystemUnorderedDht,
                       (double) INT64_MIN,
                       (double) INT64_MAX,
                       Unordered,
                       DoNotBroadcast,
                       DoNotAutoRemove,
                       NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    unorderedDhtCreated = true;

    status = dhtCreate(DhtSystemRandomDht,
                       (double) INT64_MIN,
                       (double) INT64_MAX,
                       Random,
                       DoNotBroadcast,
                       DoNotAutoRemove,
                       NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    randomDhtCreated = true;

    status = dhtCreate(DhtSystemBroadcastDht,
                       (double) INT64_MIN,
                       (double) INT64_MAX,
                       Unordered,
                       DoBroadcast,
                       DoNotAutoRemove,
                       NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    broadcastDhtCreated = true;

    status = dhtCreate(DhtSystemAscendingDht,
                       (double) INT64_MIN,
                       (double) INT64_MAX,
                       Ascending,
                       DoNotBroadcast,
                       DoNotAutoRemove,
                       NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    ascendingDhtCreated = true;

    status = dhtCreate(DhtSystemDescendingDht,
                       (double) INT64_MIN,
                       (double) INT64_MAX,
                       Descending,
                       DoNotBroadcast,
                       DoNotAutoRemove,
                       NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    descendingDhtCreated = true;

    defaultDhtsCreated_ = true;

CommonExit:
    if (status != StatusOk) {
        Status status2;
        if (unorderedDhtCreated) {
            status2 = dhtDelete(DhtSystemUnorderedDht);
            assert(status2 == StatusOk);
            unorderedDhtCreated = false;
        }

        if (randomDhtCreated) {
            status2 = dhtDelete(DhtSystemRandomDht);
            assert(status2 == StatusOk);
            randomDhtCreated = false;
        }

        if (broadcastDhtCreated) {
            status2 = dhtDelete(DhtSystemBroadcastDht);
            assert(status2 == StatusOk);
            broadcastDhtCreated = false;
        }

        if (ascendingDhtCreated) {
            status2 = dhtDelete(DhtSystemAscendingDht);
            assert(status2 == StatusOk);
            ascendingDhtCreated = false;
        }

        if (descendingDhtCreated) {
            status2 = dhtDelete(DhtSystemDescendingDht);
            assert(status2 == StatusOk);
            descendingDhtCreated = false;
        }
    }

    return status;
}

void
DhtMgr::dhtDeleteDefaultDhts()
{
    Status status;

    if (defaultDhtsCreated_) {
        status = dhtDelete(DhtSystemUnorderedDht);
        assert(status == StatusOk);
        status = dhtDelete(DhtSystemRandomDht);
        assert(status == StatusOk);
        status = dhtDelete(DhtSystemBroadcastDht);
        assert(status == StatusOk);
        status = dhtDelete(DhtSystemAscendingDht);
        assert(status == StatusOk);
        status = dhtDelete(DhtSystemDescendingDht);
        assert(status == StatusOk);
        defaultDhtsCreated_ = false;
    }
}

// This function publishes the DHT name into the global name space after
// distributing the DHT to all nodes via GVM.
Status
DhtMgr::dhtCreate(const char *dhtName,
                  float64_t lowerBound,
                  float64_t upperBound,
                  Ordering ordering,
                  DhtBroadcast broadcast,
                  DhtAutoRemove autoRemove,
                  DhtId *dhtIdOut)
{
    Status status = StatusUnknown;
    LibNs *libNs = LibNs::get();
    char dhtPathName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    DhtId dhtId = DhtInvalidDhtId;
    bool addedToGvm = false;

    assert(dhtName != NULL);

    if (strlen(dhtName) == 0) {
        status = StatusDhtEmptyDhtName;
        goto CommonExit;
    }

    if (upperBound < lowerBound) {
        status = StatusDhtUpperBoundLessThanLowerBound;
        goto CommonExit;
    }

    // Allocate the dhtId (aka Xid).  For system DHTs this is a reserved
    // value.  For non-system DHTs just get the next available one.
    if (strcmp(dhtName, DhtSystemAscendingDht) == 0) {
        dhtId = XidMgr::XidSystemAscendingDht;
    } else if (strcmp(dhtName, DhtSystemDescendingDht) == 0) {
        dhtId = XidMgr::XidSystemDescendingDht;
    } else if (strcmp(dhtName, DhtSystemUnorderedDht) == 0) {
        dhtId = XidMgr::XidSystemUnorderedDht;
    } else if (strcmp(dhtName, DhtSystemRandomDht) == 0) {
        dhtId = XidMgr::XidSystemRandomDht;
    } else if (strcmp(dhtName, DhtSystemBroadcastDht) == 0) {
        dhtId = XidMgr::XidSystemBroadcastDht;
    } else {
        dhtId = XidMgr::get()->xidGetNext();
    }

    // Broadcast the DHT + dhtID (used as HT key) via GVM.
    status = dhtAddViaGvm(dhtName,
                          dhtId,
                          lowerBound,
                          upperBound,
                          ordering,
                          broadcast);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "failed to add Dht '%s' (Id %lu) to GVM: %s",
                dhtName,
                dhtId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    addedToGvm = true;

    {
        // Scoping of object.  It's unneeded once it is published

        DhtNsObject dhtNsObject(dhtId);

        // Create a fully qualified path name using the supplied dht name.
        if (snprintf(dhtPathName,
                     LibNsTypes::MaxPathNameLen,
                     "%s%s",
                     DhtNsObject::PathPrefix,
                     dhtName) >= (int) LibNsTypes::MaxPathNameLen) {
            status = StatusOverflow;
            goto CommonExit;
        }

        if (autoRemove == DoAutoRemove) {
            nsId = libNs->publishWithAutoRemove(dhtPathName,
                                                &dhtNsObject,
                                                &status);
        } else {
            nsId = libNs->publish(dhtPathName, &dhtNsObject, &status);
        }
        if (status != StatusOk) {
            // Convert LibNs error to more informative DHT error
            if (status == StatusExist) {
                status = StatusDhtAlreadyExists;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to publish '%s': %s",
                    dhtPathName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    xSyslog(moduleName, XlogDebug, "DHT created: %s", dhtName);

    assert(status == StatusOk);

    if (dhtIdOut != NULL) {
        *dhtIdOut = dhtId;
    }

CommonExit:

    if (status != StatusOk) {
        if (addedToGvm) {
            // Clean it out
            Status status2 = dhtDeleteViaGvm(dhtId);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to delete Dht '%s' (ID %lu) from GVM: %s",
                        dhtName,
                        dhtId,
                        strGetFromStatus(status2));
            }
        }

        if (nsId != LibNsTypes::NsInvalidId) {
            // Remove from the name space
            bool objDeleted;
            Status status2 = libNs->remove(nsId, &objDeleted);
            assert(objDeleted == true);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to remove '%s' (ID %lu): %s",
                        dhtPathName,
                        nsId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

// This function checks if the specified DHT name exists in the name space.
Status
DhtMgr::isValidDhtName(const char *dhtName)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    char dhtPathName[LibNsTypes::MaxPathNameLen];

    // Create a fully qualified path name using the supplied dht name.
    if (snprintf(dhtPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 DhtNsObject::PathPrefix,
                 dhtName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    nsId = libNs->getNsId(dhtPathName);
    if (nsId == LibNsTypes::NsInvalidId) {
        status = StatusDhtNotFound;
        goto CommonExit;
    }

CommonExit:

    return status;
}

Dht *
DhtMgr::dhtGetDht(DhtId dhtId)
{
    DhtIdToDhtHTEntry *htEntry;
    Dht *dht = NULL;

    dhtIdToDhtHTLock_.lock();

    htEntry = dhtIdToDhtHT_.find(dhtId);
    if (htEntry != NULL) {
        // Note that we're returning a pointer to the entry in the hash
        // table.  This is supposedly protected via a LibNs open() to
        // the path name associated with the Dht.
        dht = &htEntry->dht;
    }

    dhtIdToDhtHTLock_.unlock();

    return dht;
}

DhtHandle
DhtMgr::dhtOpen(DhtId dhtId, Status *status)
{
    DhtHandle dhtHandle;
    *status = StatusOk;
    DhtIdToDhtHTEntry *htEntry;
    Dht *dht = NULL;
    char dhtPathName[LibNsTypes::MaxPathNameLen];

    assert(dhtId != DhtInvalidDhtId);

    if (dhtId == XidMgr::XidSystemAscendingDht ||
        dhtId == XidMgr::XidSystemDescendingDht ||
        dhtId == XidMgr::XidSystemUnorderedDht ||
        dhtId == XidMgr::XidSystemRandomDht ||
        dhtId == XidMgr::XidSystemBroadcastDht) {
        // System Dht.  No need to open it wrt LibNs.
        dhtHandle.nsId = dhtId;
        dhtHandle.version = 1;
        dhtHandle.openFlags = LibNsTypes::ReaderShared;
    } else {
        // Open the name space object.  Have to get the NsId from the GVM
        // hash table.
        dhtIdToDhtHTLock_.lock();
        htEntry = dhtIdToDhtHT_.find(dhtId);
        if (htEntry == NULL) {
            dhtIdToDhtHTLock_.unlock();
            *status = StatusDhtNotFound;
            goto CommonExit;
        }
        dhtIdToDhtHTLock_.unlock();

        dht = &htEntry->dht;

        // Open via path name
        if (snprintf(dhtPathName,
                     LibNsTypes::MaxPathNameLen,
                     "%s%s",
                     DhtNsObject::PathPrefix,
                     dht->dhtName) >= (int) LibNsTypes::MaxPathNameLen) {
            *status = StatusOverflow;
            goto CommonExit;
        }

        dhtHandle =
            LibNs::get()->open(dhtPathName, LibNsTypes::ReaderShared, status);
        if (*status != StatusOk) {
            // Convert LibNs errors to more informative DHT errors.
            if (*status == StatusNsNotFound ||
                *status == StatusPendingRemoval) {
                *status = StatusDhtNotFound;
            } else if (*status == StatusAccess) {
                *status = StatusDhtInUse;
            }
        }
    }

CommonExit:

    return dhtHandle;
}

Status
DhtMgr::dhtClose(DhtHandle dhtHandle, DhtId dhtId)
{
    Status status = StatusOk;
    bool objDeleted;
    LibNs *libNs = LibNs::get();

    if (dhtId == XidMgr::XidSystemAscendingDht ||
        dhtId == XidMgr::XidSystemDescendingDht ||
        dhtId == XidMgr::XidSystemUnorderedDht ||
        dhtId == XidMgr::XidSystemRandomDht ||
        dhtId == XidMgr::XidSystemBroadcastDht) {
        // System Dht.  No need to close it wrt LibNs.
    } else {
        status = libNs->close(dhtHandle, &objDeleted);
        if (status == StatusOk) {
            if (objDeleted) {
                // Delete from GVM
                Status status2 = dhtDeleteViaGvm(dhtId);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "failed to delete Dht (ID %lu) from GVM: %s",
                            dhtId,
                            strGetFromStatus(status2));
                    // Continue on... This leaks the hash table entry which
                    // shouldn't matter (other than the loss of memory) as the
                    // dht ID doesn't get reused.
                }
            }
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "failed to close Dht (ID %lu): %s",
                    dhtHandle.nsId,
                    strGetFromStatus(status));
        }
    }

    return status;
}

Status
DhtMgr::dhtDelete(const char *dhtName)
{
    DhtId dhtId;
    Status status = StatusUnknown;
    char dhtPathName[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    bool objDeleted = false;

    // Create a fully qualified path name using the supplied dht name.
    if (snprintf(dhtPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 DhtNsObject::PathPrefix,
                 dhtName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    status = dhtGetDhtId(dhtName, &dhtId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get Dht ID for '%s':%s",
                dhtName,
                strGetFromStatus(status));
        assert(0 && "Failed to get Dht ID");
        goto CommonExit;
    }

    // Remove the path name from the name space.  If there are opens it will
    // prevent the name from being removed and it will be marked for pending
    // removal.  When the last of the closes is done the name will be removed
    // and the Dht will be deleted.
    status = libNs->remove(dhtPathName, &objDeleted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to remove '%s': %s",
                dhtPathName,
                strGetFromStatus(status));
    }

    if (objDeleted) {
        // Remove the Dht from the GVM
        Status status2 = dhtDeleteViaGvm(dhtId);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete '%s' (ID %lu) from GVM: %s",
                    dhtName,
                    dhtId,
                    strGetFromStatus(status2));
            // This leaks the hash table entry but shouldn't matter (other
            // than the loss of memory) as the dht ID doesn't get reused.
            goto CommonExit;
        }
    }

CommonExit:

    return status;
}

// Migrate an older format external DHT to the current format

Status
DhtMgr::dhtMigrate(DhtExternal *oldDht, DhtExternal *newDht)
{
    DhtBaseExternal *srcDht = NULL;

    if (memcmp(oldDht->dhtExtEyec, DhtExtEyecStr, sizeof(oldDht->dhtExtEyec)) !=
        0) {
        xSyslog(moduleName, XlogErr, "File read does not contain a DHT");
        return StatusInval;
    }

    if (oldDht->dhtExtVer == DhtInvalidDhtId) {
        xSyslog(moduleName, XlogErr, "Invalid DHT version information");
        return StatusInval;
    }

    if (oldDht->dhtExtVer > DhtExtCurrentVer &&
        oldDht->dhtExtVer < DhtExtTestVer) {
        xSyslog(moduleName,
                XlogErr,
                "DHT version %d is not supported by this level of XCE",
                oldDht->dhtExtVer);
        return StatusInval;
    }

    // Set all the constant fields of the new DHT
    memZero(newDht, sizeof(*newDht));
    memcpy(newDht->dhtExtEyec, DhtExtEyecStr, sizeof(newDht->dhtExtEyec));
    memcpy(newDht->dhtExtEndEyec,
           DhtExtEndEyecStr,
           sizeof(newDht->dhtExtEndEyec));
    newDht->dhtExtVer = DhtExtCurrentVer;
    newDht->dhtExtLen = DhtExtBlockSize;

    // Basic check passed, do version specific conversions
    assert(oldDht->dhtExtVer != DhtExtCurrentVer);
    switch (oldDht->dhtExtVer) {
    // XXX add CRC32 calculation whenever the problems with it are fixed
    case DhtExtTestVer:
        srcDht = (DhtBaseExternal *) oldDht;
        // consider validating fields as well
        newDht->upperBound = srcDht->upperBound;
        newDht->lowerBound = srcDht->lowerBound;
        newDht->ordering = srcDht->ordering;
        // The test version does not contain a creation time
        clock_gettime(CLOCK_REALTIME, &newDht->dhtExtCreateTime);
        break;

    default:
        assert(0 && "DHT version checking failed");
        return StatusInval;
        break;
    }

    return StatusOk;
}

Status
DhtMgr::init()
{
    Status status = StatusUnknown;
    bool gvmInited = false;

    assert(sizeof(DhtExternal) == DhtExtBlockSize);

    assert(instance == NULL);
    instance = new (std::nothrow) DhtMgr;
    if (instance == NULL) {
        return StatusNoMem;
    }

    instance->dhtBc_ = BcHandle::create(BufferCacheObjects::Dht);
    if (instance->dhtBc_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    instance->numNodes_ = Config::get()->getActiveNodes();
    instance->randomCounter_ = 0;

    status = DhtHashGvm::init();
    if (status != StatusOk) {
        goto CommonExit;
    }
    gvmInited = true;

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (gvmInited) {
            DhtHashGvm::get()->destroy();
            gvmInited = false;
        }
        if (instance->dhtBc_ != NULL) {
            BcHandle::destroy(&instance->dhtBc_);
            instance->dhtBc_ = NULL;
        }
    }

    return status;
}

DhtMgr *
DhtMgr::get()
{
    return instance;
}

//  DHT state restore - restore all system DHTs that were defined to the cluster
//  from the previous time it was active.  Minimally, the system order
//  preserving and non-order preserving DHTs are created.
//
//  The other user nodes must be up to receive the Two PC messages needed
//  to propagate the saved DHTs across the cluster.  Therefore, this can not
//  be done during the initialization phase by dhtInit.
//
//  Note that DHTs aren't persisted so restore doesn't restore any non-system
//  DHTs that existed from the previous time cluster was active.
Status
DhtMgr::dhtStateRestore()
{
    Status status = StatusUnknown;

    // Create the system DHTs ...
    status = dhtCreateDefaultDhts();
    assert(status == StatusOk);  // Just for testing

    return status;
}

unsigned
DhtMgr::dhtHashFloat64(Dht *dht, float64_t in, bool doCrc)
{
    float64_t lowerBound, upperBound;
    unsigned nodeId;
    assert(dht != NULL);

    if (dht->ordering & SortedFlag) {
        // (in != in) is true if in is NaN
        if (in != in) {
            nodeId = 0;
            goto CommonExit;
        }

        lowerBound = dht->lowerBound;
        if (in < lowerBound) {
            nodeId = 0;
            goto CommonExit;
        }
        assert(in >= lowerBound);

        upperBound = dht->upperBound;
        if (in >= upperBound) {
            nodeId = numNodes_ - 1;
            goto CommonExit;
        }
        assert(in <= upperBound);

        assert(upperBound > lowerBound);

        // Retain precision here.
        nodeId = (unsigned) (((in - lowerBound) * (float64_t) numNodes_) /
                             (upperBound - lowerBound));

        if (unlikely(nodeId >= numNodes_)) {
            // account for precision issues
            nodeId = numNodes_ - 1;
        }
    } else if (dht->ordering & RandomFlag) {
        nodeId = getNextNodeId();
    } else {
        if (doCrc) {
            nodeId = (unsigned) (hashCrc32c(0, &in, sizeof(in)) % numNodes_);
        } else {
            nodeId = ((uint64_t) in) % numNodes_;
        }
    }

CommonExit:
    assert(nodeId < numNodes_);

    if (dht->ordering & DescendingFlag) {
        return numNodes_ - 1 - nodeId;
    } else {
        return nodeId;
    }
}

unsigned
DhtMgr::dhtHashString(Dht *dht, const char *in)
{
    if (dht->ordering) {
        return dhtHashFloat64(dht, (float64_t) operatorsHashByString(in), true);
    } else {
        return hashStringFast(in) % numNodes_;
    }
}

unsigned
DhtMgr::dhtHashUInt64(Dht *dht, uint64_t in)
{
    return dhtHashFloat64(dht, (float64_t) in, true);
}

unsigned
DhtMgr::dhtHashInt64(Dht *dht, int64_t in)
{
    return dhtHashFloat64(dht, (float64_t) in, true);
}

unsigned
DhtMgr::getNextNodeId()
{
    return randomCounter_++ % numNodes_;
}

void
DhtMgr::destroy()
{
    if (instance == NULL) {
        return;
    }

    dhtIdToDhtHTLock_.lock();
    dhtIdToDhtHT_.removeAll(&DhtIdToDhtHTEntry::del);
    dhtIdToDhtHTLock_.unlock();

    if (DhtHashGvm::get()) {
        DhtHashGvm::get()->destroy();
    }

    BcHandle::destroy(&dhtBc_);
    dhtBc_ = NULL;

    delete instance;
    instance = NULL;
}

void
DhtMgr::DhtIdToDhtHTEntry::del()
{
    DhtMgr *dhtMgr = DhtMgr::get();

    dhtMgr->dhtBc_->freeBuf(this);
}

// Add the Dht to all the nodes via Gvm.  The Dht will be inserted into a
// local hash table on each node for local access.
Status
DhtMgr::dhtAddViaGvm(const char *dhtName,
                     const DhtId dhtId,
                     const float64_t lowerBound,
                     const float64_t upperBound,
                     const Ordering ordering,
                     const DhtBroadcast broadcast)
{
    Status status = StatusOk;
    Dht *dht = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(Dht));
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload->init(DhtHashGvm::get()->getGvmIndex(),
                   (uint32_t) DhtHashGvm::Action::Add,
                   sizeof(Dht));
    dht = (Dht *) gPayload->buf;
    dht->dhtId = dhtId;
    strlcpy((char *) &dht->dhtName, dhtName, LibNsTypes::MaxPathNameLen);
    dht->lowerBound = lowerBound;
    dht->upperBound = upperBound;
    dht->ordering = ordering;
    dht->broadcast = broadcast;

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    // Caller syslogs status and does any needed error recovery.
    return status;
}

// Delete the Dht from all nodes via Gvm.
Status
DhtMgr::dhtDeleteViaGvm(const DhtId dhtId)
{
    Status status = StatusOk;
    Dht *dht = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(Dht));
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload->init(DhtHashGvm::get()->getGvmIndex(),
                   (uint32_t) DhtHashGvm::Action::Delete,
                   sizeof(Dht));
    dht = (Dht *) gPayload->buf;
    dht->dhtId = dhtId;
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    // Caller syslogs status and does any needed recovery.

    return status;
}

// Gvm called local handler routines.
Status
DhtMgr::dhtAddDhtLocal(void *payload)
{
    Status status = StatusOk;
    Dht *dhtIn = (Dht *) payload;
    DhtIdToDhtHTEntry *htEntry = NULL;

    htEntry = (DhtIdToDhtHTEntry *) dhtBc_->allocBuf(XidInvalid, &status);
    if (htEntry == NULL) {
        goto CommonExit;
    }

    htEntry->dhtId = dhtIn->dhtId;
    strlcpy((char *) &htEntry->dht.dhtName,
            (char *) &dhtIn->dhtName,
            LibNsTypes::MaxPathNameLen);
    htEntry->dht.lowerBound = dhtIn->lowerBound;
    htEntry->dht.upperBound = dhtIn->upperBound;
    htEntry->dht.ordering = dhtIn->ordering;
    htEntry->dht.broadcast = dhtIn->broadcast;

    dhtIdToDhtHTLock_.lock();

    assert(dhtIdToDhtHT_.find(dhtIn->dhtId) == NULL);

    status = dhtIdToDhtHT_.insert(htEntry);
    assert(status == StatusOk);

    dhtIdToDhtHTLock_.unlock();

CommonExit:

    if (status != StatusOk) {
        if (htEntry != NULL) {
            dhtBc_->freeBuf(htEntry);
        }
    }

    return status;
}

Status
DhtMgr::dhtDeleteDhtLocal(void *payload)
{
    Status status = StatusOk;
    Dht *dhtIn = (Dht *) payload;
    DhtIdToDhtHTEntry *htEntry;

    dhtIdToDhtHTLock_.lock();
    htEntry = dhtIdToDhtHT_.remove(dhtIn->dhtId);
    assert(htEntry != NULL);

    dhtIdToDhtHTLock_.unlock();

    dhtBc_->freeBuf(htEntry);

    return status;
}
