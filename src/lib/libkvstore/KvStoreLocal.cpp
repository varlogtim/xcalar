// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// This file only contains methods for operations on the local persistent and
// in-memory backing key-value stores.  See KvStore.cpp for global KV store
// management.

#include <sys/resource.h>

#include "kvstore/KvStore.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "util/FileUtils.h"
#include "lmdb.h"
#include "common/Version.h"

Mutex KvStore::initLock;

using namespace xcalar::internal;

Status
KvStore::getContainingDir(char *dirPath, size_t dirPathSize) const
{
    const char *xlrRoot = XcalarConfig::get()->xcalarRootCompletePath_;
    return strSnprintf(dirPath,
                       dirPathSize,
                       "%s/%s/%s",
                       xlrRoot,
                       SubdirName,
                       name_);
}

Status
KvStore::persistedInit(const char *kvStoreName)
{
    Status status;
    MDB_txn *txn = NULL;
    int retval;
    char mdbPath[XcalarApiMaxPathLen];
    bool isVALimSet = false;
    size_t MaxMdbBytes;

    disableReopen_ = true;
    enableLmdbApi_ = true;
    assert(mdbEnv_ == NULL);

    // On-disk data allowed to grow to this maximum.
    //
    // Per-open-DB virtual address utilization will always fixed at this
    // maximum.  Note that for RHEL6/7 the VA limit is 128TB per process.
    //
    // Example: if maxKvsSzGlobalMB_ is set to 1000 (1GB), on a fresh install
    // the on-disk usage will start around 32KB (and grow as KV pairs are
    // added), and the VA usage will always be constant 1GB.
    if (type_ == KvStoreSession) {
        MaxMdbBytes = XcalarConfig::get()->maxKvsSzWorkbookMB_ * MB;
    } else {
        MaxMdbBytes = XcalarConfig::get()->maxKvsSzGlobalMB_ * MB;
    }

    status = setName(kvStoreName);
    BailIfFailed(status);

    status = mdbCk(mdb_env_create(&mdbEnv_));
    BailIfFailed(status);
    status = mdbCk(mdb_env_set_maxdbs(mdbEnv_, MaxDBs));
    BailIfFailed(status);
    status = mdbCk(mdb_env_set_mapsize(mdbEnv_, MaxMdbBytes));
    BailIfFailed(status);

    status = getContainingDir(mdbPath, sizeof(mdbPath));
    BailIfFailed(status);
    status = FileUtils::recursiveMkdir(mdbPath, 0750);
    BailIfFailed(status);

    status = changeVaLimit(MaxMdbBytes);
    BailIfFailed(status);
    isVALimSet = true;

    // Standard LMDB locking relies on both pthreads mutex and also POSIX file
    // record locks.  We disable these with MDB_NOLOCK and instead use the
    // kvstore mutex.
    status = mdbCk(mdb_env_open(mdbEnv_, mdbPath, MDB_NOLOCK, 0640));
    BailIfFailed(status);

    status = mdbCk(mdb_txn_begin(mdbEnv_, NULL, 0, &txn));
    BailIfFailed(status);

    retval = mdb_dbi_open(txn, "KvStore", 0, &dbi_);
    if (retval == MDB_NOTFOUND) {
        status = mdbCk(mdb_dbi_open(txn, "KvStore", MDB_CREATE, &dbi_));
        BailIfFailed(status);
        status = mdbInitMetadata(txn);
        BailIfFailed(status);
        status = mdbCk(mdb_txn_commit(txn));
        BailIfFailed(status);
        txn = NULL;

        xSyslog(ModuleName,
                XlogInfo,
                "Created new KVS backing store: %s:%p:%u",
                name_,
                mdbEnv_,
                dbi_);
    } else {
        uint64_t magic;

        status = mdbCk(retval);
        BailIfFailed(status);
        status = mdbGetKv(txn, XceMetadataMagic, &magic);
        BailIfFailed(status);
        if (magic != magicKvStoreLMDB) {
            xSyslog(ModuleName,
                    XlogErr,
                    "%s:%p:%u is not a kvstore (0x%016lx != 0x%016lx)",
                    name_,
                    mdbEnv_,
                    dbi_,
                    magic,
                    magicKvStoreLMDB);
            status = StatusLMDBError;
            goto CommonExit;
        }

        status = persistedLoad(txn);
        BailIfFailed(status);

        status = mdbCk(mdb_txn_commit(txn));
        BailIfFailed(status);
        txn = NULL;

        xSyslog(ModuleName,
                XlogInfo,
                "Opened existing KVS backing store: %s:%p:%u",
                name_,
                mdbEnv_,
                dbi_);
    }

CommonExit:
    if (txn != NULL) {
        assert(status != StatusOk);
        mdb_txn_abort(txn);
        txn = NULL;
    }

    if (status != StatusOk) {
        if (mdbEnv_ != NULL) {
            if (dbi_ != MdbDbiInvalid) {
                mdb_dbi_close(mdbEnv_, dbi_);
                dbi_ = MdbDbiInvalid;
            }

            mdb_env_close(mdbEnv_);
            mdbEnv_ = NULL;
        }

        memFree(name_);
        name_ = NULL;

        if (isVALimSet) {
            verifyOk(changeVaLimit(-MaxMdbBytes));
        }

        isVALimSet = false;
        enableLmdbApi_ = false;
    }

    disableReopen_ = false;
    return status;
}

Status
KvStore::persistedLoad(MDB_txn *const txn)
{
    Status status;
    KvStoreLib *kvs = KvStoreLib::get();
    KvStoreEntry *kvEntryReplaced = NULL;
    KvStoreEntry *kvEntry = NULL;
    MDB_val mdbKey, data;
    MDB_cursor *cursor = NULL;
    int retval;

    status = mdbCk(mdb_cursor_open(txn, dbi_, &cursor));
    BailIfFailed(status);

    while ((retval = mdb_cursor_get(cursor, &mdbKey, &data, MDB_NEXT)) == 0) {
        kvEntry = kvs->kvStoreInitEntry(mdbKey.mv_size, data.mv_size, true);
        BailIfNull(kvEntry);

        status = strStrlcpy((char *) kvEntry->key,
                            (const char *) mdbKey.mv_data,
                            mdbKey.mv_size);
        BailIfFailed(status);
        status = strStrlcpy((char *) kvEntry->value,
                            (char *) data.mv_data,
                            data.mv_size);
        BailIfFailed(status);

        status = kvs->addOrReplaceThenPersist(this,
                                              kvEntry,
                                              KvStoreOptDisablePersist,
                                              &kvEntryReplaced);
        BailIfFailed(status);
        kvEntry = NULL;

        if (kvEntryReplaced != NULL) {
            memFree(kvEntryReplaced);
            kvEntryReplaced = NULL;
        }
    }

    if (retval != MDB_NOTFOUND) {
        status = mdbCk(retval);
        BailIfFailed(status);
    }

    mdb_cursor_close(cursor);
    cursor = NULL;

CommonExit:
    if (kvEntry != NULL) {
        memFree(kvEntry);
        kvEntry = NULL;
    }

    if (cursor != NULL) {
        mdb_cursor_close(cursor);
        cursor = NULL;
    }

    return status;
}

Status
KvStore::persistedGet(const char *key, KvStoreEntry **kvEntry, const bool doDel)
{
    Status status;
    KvStoreLib *kvs = KvStoreLib::get();
    MDB_val mdbKey, data;
    MDB_txn *txn = NULL;
    int retval;

    *kvEntry = NULL;
    mdbKey.mv_size = strlen(key) + 1;
    mdbKey.mv_data = (void *) key;

    status = mdbCk(mdb_txn_begin(mdbEnv_, NULL, 0, &txn));
    BailIfFailed(status);

    retval = mdb_get(txn, dbi_, &mdbKey, &data);
    if (retval == MDB_NOTFOUND) {
        status = StatusKvEntryNotFound;
        goto CommonExit;
    }

    status = mdbCk(retval);
    BailIfFailed(status);

    *kvEntry = kvs->kvStoreInitEntry(mdbKey.mv_size, data.mv_size, true);
    BailIfNull(*kvEntry);

    status = strStrlcpy((char *) (*kvEntry)->key, key, mdbKey.mv_size);
    BailIfFailed(status);
    status = strStrlcpy((char *) (*kvEntry)->value,
                        (char *) data.mv_data,
                        data.mv_size);
    BailIfFailed(status);

    if (doDel) {
        status = mdbCk(mdb_del(txn, dbi_, &mdbKey, NULL));
        BailIfFailed(status);
    }

    status = mdbCk(mdb_txn_commit(txn));
    BailIfFailed(status);
    txn = NULL;

CommonExit:
    if (txn != NULL) {
        assert(status != StatusOk);
        mdb_txn_abort(txn);
        txn = NULL;
    }

    if (status != StatusOk) {
        if (kvEntry != NULL && *kvEntry != NULL) {
            memFree(*kvEntry);
            *kvEntry = NULL;
        }
    }

    return status;
}

Status
KvStore::persistedGet(const char *key, KvStoreEntry **kvEntry)
{
    return persistedGet(key, kvEntry, false);
}

Status
KvStore::persistedDel(const char *key, KvStoreEntry **kvEntry)
{
    return persistedGet(key, kvEntry, true);
}

Status
KvStore::persistedDelAll()
{
    Status status;
    MDB_txn *txn = NULL;

    status = mdbCk(mdb_txn_begin(mdbEnv_, NULL, 0, &txn));
    BailIfFailed(status);

    status = mdbCk(mdb_drop(txn, dbi_, 0));
    BailIfFailed(status);

    status = mdbCk(mdb_txn_commit(txn));
    BailIfFailed(status);
    txn = NULL;

CommonExit:
    if (txn != NULL) {
        assert(status != StatusOk);
        mdb_txn_abort(txn);
        txn = NULL;
    }

    return status;
}

Status
KvStore::persistedPut(KvStoreEntry *kvEntry)
{
    return persistedPut(kvEntry, NULL);
}

Status
KvStore::persistedPut(KvStoreEntry *kvEntry, KvStoreEntry **kvEntryReplaced)
{
    Status status;
    MDB_txn *txn = NULL;

    status = mdbCk(mdb_txn_begin(mdbEnv_, NULL, 0, &txn));
    BailIfFailed(status);

    status = persistedPutHelper(txn, kvEntry, kvEntryReplaced);
    BailIfFailed(status);

    status = mdbCk(mdb_txn_commit(txn));
    BailIfFailed(status);
    txn = NULL;

CommonExit:
    if (txn != NULL) {
        assert(status != StatusOk);
        mdb_txn_abort(txn);
        txn = NULL;
    }

    return status;
}

Status
KvStore::persistedMultiPut(KvStoreEntry **kvEntries,
                           KvStoreEntry ***kvEntryReplaced,
                           const size_t numEnts)
{
    Status status;
    MDB_txn *txn = NULL;

    status = mdbCk(mdb_txn_begin(mdbEnv_, NULL, 0, &txn));
    BailIfFailed(status);

    for (size_t i = 0; i < numEnts; i++) {
        if (kvEntryReplaced == NULL) {
            status = persistedPutHelper(txn, kvEntries[i], NULL);
        } else {
            status = persistedPutHelper(txn, kvEntries[i], kvEntryReplaced[i]);
        }
        BailIfFailed(status);
    }

    status = mdbCk(mdb_txn_commit(txn));
    BailIfFailed(status);
    txn = NULL;

CommonExit:
    if (txn != NULL) {
        assert(status != StatusOk);
        mdb_txn_abort(txn);
        txn = NULL;
    }

    return status;
}

Status
KvStore::persistedPutHelper(MDB_txn *const txn,
                            KvStoreEntry *kvEntry,
                            KvStoreEntry **kvEntryReplaced)
{
    Status status;
    MDB_val mdbKey, data;

    // Include terminating null byte in k/v store
    mdbKey.mv_size = strnlen(kvEntry->key, kvEntry->keySize) + 1;
    mdbKey.mv_data = (void *) kvEntry->key;
    data.mv_size = kvEntry->valueSize;
    data.mv_data = (void *) kvEntry->value;

    if (kvEntryReplaced) {
        int retval = mdb_put(txn, dbi_, &mdbKey, &data, MDB_NOOVERWRITE);
        *kvEntryReplaced = NULL;
        if (retval == MDB_KEYEXIST) {
            KvStoreLib *kvs = KvStoreLib::get();
            *kvEntryReplaced =
                kvs->kvStoreInitEntry(kvEntry->keySize, data.mv_size, true);
            BailIfNull(*kvEntryReplaced);
            status = strStrlcpy((char *) (*kvEntryReplaced)->key,
                                kvEntry->key,
                                kvEntry->keySize);
            BailIfFailed(status);
            status = strStrlcpy((char *) (*kvEntryReplaced)->value,
                                (char *) data.mv_data,
                                data.mv_size);
            BailIfFailed(status);

            data.mv_size = kvEntry->valueSize;
            data.mv_data = (void *) kvEntry->value;
            status = mdbCk(mdb_put(txn, dbi_, &mdbKey, &data, 0));
            BailIfFailed(status);
        } else {
            status = mdbCk(retval);
            BailIfFailed(status);
        }

    } else {
        status = mdbCk(mdb_put(txn, dbi_, &mdbKey, &data, 0));
        BailIfFailed(status);
    }

    status = mdbPutKv(txn, XceMetadataLastWriterVer, versionGetFullStr());
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        if (kvEntryReplaced && (*kvEntryReplaced != NULL)) {
            memFree(*kvEntryReplaced);
            *kvEntryReplaced = NULL;
        }
    }

    return status;
}

void
KvStore::persistedClose(const bool doDelete, const bool doFreeName)
{
    const char *path;
    char *retPath = NULL;
    Status status;
    size_t MaxMdbBytes;

    assert(enableLmdbApi_);

    if (type_ == KvStoreSession) {
        MaxMdbBytes = XcalarConfig::get()->maxKvsSzWorkbookMB_ * MB;
    } else {
        MaxMdbBytes = XcalarConfig::get()->maxKvsSzGlobalMB_ * MB;
    }

    disableReopen_ = true;

    if (mdbEnv_ == NULL) {
        return;
    }

    status = mdbCk(mdb_env_get_path(mdbEnv_, &path));
    if (status == StatusOk && path != NULL) {
        // Make a copy since we're going to tear down the environment next
        retPath = strAllocAndCopy(path);
    }

    mdb_dbi_close(mdbEnv_, dbi_);
    dbi_ = MdbDbiInvalid;
    mdb_env_close(mdbEnv_);
    mdbEnv_ = NULL;

    verifyOk(changeVaLimit(-MaxMdbBytes));

    if (doFreeName && name_ != NULL) {
        memFree(name_);
        name_ = NULL;
    }

    if (retPath != NULL) {
        if (doDelete) {
            Status status = FileUtils::rmDirRecursiveSafer(retPath);
            if (status == StatusOk) {
                xSyslog(ModuleName,
                        XlogInfo,
                        "Deleted KV store backing path: %s",
                        retPath);
            } else {
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to delete KV store backing path: %s",
                        retPath);
            }
        }

        memFree(retPath);
        retPath = NULL;
    }

    enableLmdbApi_ = false;
    disableReopen_ = false;
}

Status
KvStore::mdbCkHelper(int err, const char *file, const uint64_t line)
{
    if (err == MDB_SUCCESS) {
        return StatusOk;
    }

    xSyslog(ModuleName,
            XlogErr,
            "CallerFile %s: CallerLine %lu: DB %s: "
            "LMDB Error: %s:%p:%u: %s (%d)",
            file,
            line,
            name_,
            enableLmdbApi_ ? "open" : "closed",
            mdbEnv_,
            dbi_,
            mdb_strerror(err),
            err);

    assert((enableLmdbApi_ && err != KV_DB_CLOSED) ||
           (!enableLmdbApi_ && err == KV_DB_CLOSED));

    if (!disableReopen_) {
        // See lmdb.h.  For these errors we must shut down the database and
        // reinitialize.
        switch (err) {
        case MDB_PAGE_NOTFOUND:
        case MDB_CORRUPTED:
        case MDB_PANIC:
            if (enableLmdbApi_) {
                persistedClose(false, false);
                assert(!enableLmdbApi_);
            }

            break;
        default:
            break;
        }

        if (!enableLmdbApi_) {
            char *origNamePtr = name_;
            Status status = persistedInit(name_);
            if (status == StatusOk) {
                if (origNamePtr != NULL) {
                    // Init allocs space for a new name; free the old one
                    memFree(origNamePtr);
                    origNamePtr = NULL;
                }
            } else {
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to reopen LMDB: %s",
                        strGetFromStatus(status));
                assert(!enableLmdbApi_);
            }
        }
    }

    assert(enableLmdbApi_);
    assert(false);

    return StatusLMDBError;
}

// Assumes called within an active transaction
Status
KvStore::mdbPutKv(
    MDB_txn *const txn, void *key, size_t keyLen, void *val, size_t valLen)
{
    MDB_val mdbKey, data;

    mdbKey.mv_size = keyLen;
    mdbKey.mv_data = key;
    data.mv_size = valLen;
    data.mv_data = val;

    return mdbCk(mdb_put(txn, dbi_, &mdbKey, &data, 0));
}

Status
KvStore::mdbGetKv(
    MDB_txn *const txn, void *key, size_t keyLen, void **val, size_t *valLen)
{
    Status status;
    MDB_val mdbKey, data;

    *val = NULL;
    mdbKey.mv_size = keyLen;
    mdbKey.mv_data = key;

    status = mdbCk(mdb_get(txn, dbi_, &mdbKey, &data));
    BailIfFailed(status);
    *valLen = data.mv_size;
    *val = data.mv_data;

CommonExit:
    return status;
}

Status
KvStore::mdbGetKv(MDB_txn *const txn, const char *key, uint64_t *val)
{
    Status status;
    void *dbVal;
    size_t len;
    int retval;

    status = mdbGetKv(txn, (void *) key, strlen(key) + 1, &dbVal, &len);
    BailIfFailed(status);
    assert(len == 2 * sizeof(int64_t) + 1);

    retval = sscanf((char *) dbVal, "%016lx", val);
    if (retval != 1) {
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}
Status
KvStore::mdbPutKv(MDB_txn *const txn, const char *key, const char *val)
{
    // Include null terminator in key/value
    return mdbPutKv(txn,
                    (void *) key,
                    strlen(key) + 1,
                    (void *) val,
                    strlen(val) + 1);
}

Status
KvStore::mdbPutKv(MDB_txn *const txn, const char *key, uint64_t val)
{
    return mdbPutKv(txn, (void *) key, strlen(key) + 1, &val, sizeof(val));
}

Status
KvStore::mdbInitMetadata(MDB_txn *const txn)
{
    Status status;
    char buf[2 * sizeof(int64_t) + 1];

    // We only allow null terminated strings in DB
    status = strSnprintf(buf, sizeof(buf), "%016lx", magicKvStoreLMDB);
    BailIfFailed(status);
    status = mdbPutKv(txn, XceMetadataMagic, buf);
    BailIfFailed(status);
    status = mdbPutKv(txn, XceMetadataType, "KvStore");
    BailIfFailed(status);
    status = mdbPutKv(txn, XceMetadataCreatorVer, versionGetFullStr());
    BailIfFailed(status);
    status = mdbPutKv(txn, XceMetadataLastWriterVer, versionGetFullStr());
    BailIfFailed(status);

CommonExit:
    return status;
}

MustCheck Status
KvStore::changeVaLimit(ssize_t delta) const
{
    Status status = StatusOk;
    struct rlimit vaLimit;
    bool isLocked = false;

    // LMDB uses a memory mapped file which doesn't materially affect the
    // actual amount of physical memory consumed, so change the virtual
    // address space limit to accomodate the new or deleted mapping.
    if ((XcalarConfig::get()->enforceVALimit_) ||
        (vaLimit.rlim_cur != RLIM_INFINITY)) {
        initLock.lock();
        isLocked = true;
        if (getrlimit(RLIMIT_AS, &vaLimit)) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }

        if (vaLimit.rlim_cur == RLIM_INFINITY) {
            // Already have an infinite limit
            goto CommonExit;
        }

        // delta can be negative if we removed a mapping
        vaLimit.rlim_cur += delta;

        if (setrlimit(RLIMIT_AS, &vaLimit)) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }
    }

CommonExit:
    if (isLocked) {
        initLock.unlock();
        isLocked = false;
    }

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to change VA limit by %ld bytes: %s",
                delta,
                strGetFromStatus(status));
    }

    return status;
}
