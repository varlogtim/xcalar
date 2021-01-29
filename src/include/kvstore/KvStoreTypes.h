// Copyright 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _KVSTORETYPES_H
#define _KVSTORETYPES_H

#include "log/Log.h"
#include "util/IntHashTable.h"
#include "libapis/LibApisCommon.h"
#include "lmdb.h"
#include "strings/String.h"
#include "runtime/Mutex.h"
#include "runtime/RwLock.h"
#include "util/Atomics.h"

enum {
    KvStoreMaxKeyLen = XcalarApiMaxKeyLen,
    KvStoreMaxPathLen = XcalarApiMaxPathLen,
    NumKvHashSlots = 251,
};

enum KvStoreOps {
    KvStoreOpen,
    KvStoreClose,
    KvStoreAddOrReplace,
    KvStoreMultiAddOrReplace,
    KvStoreAppend,
    KvStoreSetIfEqual,
    KvStoreDelete,
    KvStoreLookup,
    KvStoreList,
    // XXX: Published tables specific modification to KvStore
    KvStoreUpdateImdDependency,
};

enum KvStoreType : uint32_t {
    KvStoreInvalid = 0,
    KvStoreGlobal = 1,
    KvStoreGroup = 2,
    KvStoreUser = 3,
    KvStoreSession = 4,
    // Add all enums before this
    KvStoreVersionTest,
    KvStoreLast
};

typedef Xid KvStoreId;

struct KvStoreEntry {
    StringHashTableHook hook;
    const char *getKey() const { return key; };
    const char *key;
    const char *value;
    size_t keySize;
    size_t valueSize;
    bool persist;
    void del();
};

typedef StringHashTable<KvStoreEntry,
                        &KvStoreEntry::hook,
                        &KvStoreEntry::getKey,
                        NumKvHashSlots,
                        hashStringFast>
    KvHashTable;

// Local persistent and in-memory operations only
struct KvStore {
    friend MustCheck Status dumpLMDB(const char *mdbPath, const char *outdir);

    // Data that is persisted using libdurable
    // XXX: Only for legacy support/upgrade...
    struct KvStorePersistedData {
        // Version of the content of the keyValueBuffer.
        unsigned keyValueBufferVersion;
        uint64_t keyValueBufferSize;
        // key=value\n....
        char keyValueBuffer[0];
    };

    enum class State {
        Init,
        OpenInProgress,
        CloseInProgress,
        Ready,
    };

    KvStore(const KvStoreId kvStoreId, const KvStoreType type)
        : bytesMemory_(0),
          kvStoreId_(kvStoreId),
          type_(type),
          mdbEnv_(NULL),
          dbi_(MdbDbiInvalid),
          enableLmdbApi_(false),
          disableReopen_(false)
    {
        memZero(&hook_, sizeof(IntHashTableHook));
        atomicWrite32(&state_, (int) State::Init);
        atomicWrite32(&ref_, 1);
    };

    MustCheck Status persistedInit(const char *kvStoreName);
    MustCheck Status persistedGet(const char *key, KvStoreEntry **kvEntry);
    MustCheck Status persistedDel(const char *key, KvStoreEntry **kvEntry);
    MustCheck Status persistedDelAll();
    MustCheck Status persistedPut(KvStoreEntry *kvEntry);
    MustCheck Status persistedMultiPut(KvStoreEntry **kvEntry,
                                       KvStoreEntry ***kvEntryReplace,
                                       const size_t numEnts);
    void persistedClose(const bool doDelete, const bool doFreeName = true);
    MustCheck KvStoreId getKvStoreId() const { return kvStoreId_; };
    MustCheck Status setName(const char *name)
    {
        return (name_ = strAllocAndCopy(name)) ? StatusOk : StatusNoMem;
    }
    MustCheck Status getContainingDir(char *dirPath, size_t dirPathSize) const;
    MustCheck Status getLastWriteVer(char **lastWriteVer);

    // In memory key-value mappings.  Superset of the persistent store.  Items
    // in the kvHash but not in the persistent store are ephemeral with the
    // life of the node
    KvHashTable kvHash_;

    IntHashTableHook hook_;
    // Current size of KvStore data in bytes.
    size_t bytesMemory_;

    RwLock lock_;

    bool validState() { return ((int) getState() <= (int) State::Ready); }

    State getState() { return ((State) atomicRead32(&state_)); }

    void setState(State state) { atomicWrite32(&state_, (int) state); }

    void incRef() { atomicInc32(&ref_); }

    void decRef()
    {
        if (!(atomicDec32(&ref_))) {
            delete this;
        }
    }

  private:
    static constexpr const char *ModuleName = "KvStoreDb";
    static constexpr const char *SubdirName = "kvs";

    static constexpr const size_t MaxDBs = 8;
    static constexpr const uint64_t MdbDbiInvalid = 0xdeadbeef;
    static constexpr const uint64_t magicKvStoreLMDB = 0xdead2858a3c51ffaULL;
    static constexpr const char *XceMetadataMagic = "__xceMetadata_Magic";
    static constexpr const char *XceMetadataType = "__xceMetadata_Type";
    static constexpr const char *XceMetadataCreatorVer =
        "__xceMetadata_CreatorVer";
    static constexpr const char *XceMetadataLastWriterVer =
        "__xceMetadata_LastWriterVer";

    static Mutex initLock;

    MustCheck Status persistedLoad(MDB_txn *const txn);
    MustCheck Status persistedGet(const char *key,
                                  KvStoreEntry **kvEntry,
                                  const bool doDel);
    MustCheck Status persistedPut(KvStoreEntry *kvEntry,
                                  KvStoreEntry **kvEntryReplaced);
    MustCheck Status persistedPutHelper(MDB_txn *const txn,
                                        KvStoreEntry *kvEntry,
                                        KvStoreEntry **kvEntryReplaced);

    // Internal DB management stuff
    MustCheck Status mdbCkHelper(int err,
                                 const char *file,
                                 const uint64_t line);
    MustCheck Status mdbInitMetadata(MDB_txn *const txn);
    MustCheck Status mdbPutKv(MDB_txn *const txn,
                              const char *key,
                              const char *val);
    MustCheck Status mdbPutKv(
        MDB_txn *const txn, void *key, size_t keyLen, void *val, size_t valLen);
    MustCheck Status mdbPutKv(MDB_txn *const txn,
                              const char *key,
                              uint64_t val);
    MustCheck Status mdbGetKv(MDB_txn *const txn,
                              void *key,
                              size_t keyLen,
                              void **val,
                              size_t *valLen);
    MustCheck Status mdbGetKv(MDB_txn *const txn,
                              const char *key,
                              size_t keyLen,
                              uint64_t *val,
                              size_t *valLen);
    MustCheck Status mdbGetKv(MDB_txn *const txn,
                              const char *key,
                              uint64_t *val);

    MustCheck Status changeVaLimit(ssize_t delta) const;

    // Global KvStore ID
    KvStoreId kvStoreId_;

    // Intended usage of this KvStore
    KvStoreType type_;
    Atomic32 state_;
    Atomic32 ref_;

    MDB_env *mdbEnv_;
    MDB_dbi dbi_;
    // Name originally passed to kvStoreOpen.
    char *name_;

    // Enable/disable LMDB API for error path
    bool enableLmdbApi_;
    // Protect KvStore::mdbCkHelper error handling against reentry via
    // init/close paths
    bool disableReopen_;

    ~KvStore() { kvHash_.removeAll(&KvStoreEntry::del); }

    KvStore(const KvStore &) = delete;
    KvStore &operator=(const KvStore &) = delete;
};

typedef enum {
    KvStoreOptNone = 0,
    KvStoreOptSync = 0x1,
    KvStoreOptDisablePersist = 0x2,
} KvStoreOpts;
typedef uint64_t KvStoreOptsMask;

// KvStore message header. Must be at beginning of any KvStore message
// dispatched through Msg2pcDlmUpdateKvStore.
struct KvStoreHeader {
    KvStoreId kvStoreId;
    KvStoreOps kvStoreOp;
};

struct PublishDependencyUpdateInput {
    KvStoreHeader hdr;

    bool isDelete;
    char name[XcalarApiMaxTableNameLen + 1];
    unsigned numParents;
    char parentNames[0];
};

struct KvStoreMultiAddOrReplaceInput {
    size_t numKvPairs;
    XcalarApiKeyScope scope;
    bool persist;
    // Variable size value, need to cursor iteratively
    XcalarApiKeyValuePair kvPairs[0];
};

typedef XcalarApiKeyAddOrReplaceInput KvStoreAddOrReplaceInput;
typedef XcalarApiKeyValuePair KvStoreKeyValuePair;
typedef XcalarApiKeyAppendInput KvStoreAppendInput;
typedef XcalarApiKeySetIfEqualInput KvStoreSetIfEqualInput;

#define KV_DB_CLOSED 0xbaddead
static_assert(KV_DB_CLOSED > MDB_LAST_ERRCODE, "Invalid error code");

// If the DB is not open we need to disable the LMDB API
#define mdbCk(mdbCall) \
    mdbCkHelper(enableLmdbApi_ ? mdbCall : KV_DB_CLOSED, __FILE__, __LINE__)

#endif
