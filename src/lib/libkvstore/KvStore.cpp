// Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <stddef.h>
#include <pthread.h>
#include <regex.h>

#include "StrlFunc.h"
#include "kvstore/KvStore.h"
#include "hash/Hash.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "stat/Statistics.h"
#include "runtime/Mutex.h"
#include "runtime/RwLock.h"
#include "msg/Xid.h"
#include "util/Stopwatch.h"
#include "xdb/HashTree.h"

KvStoreLib *KvStoreLib::instance = NULL;

//
// KvStore
// Distributed persistent and ephemeral key value store. Allows creation and
// access to (read/write) multiple KvStores. Used primarily to provide
// xcalar-gui a way to store state.
//
// Synchronization:
// Each KvStore has a DLM node derived from its Id. All KvStore modifications
// (including creation) will be serialized at this node. This imposes an
// ordering on modifications to the same KvStore and prevents 2 nodes from
// creating the "same" KvStore. Persistence and replaying of persisted state is
// also done on the DLM node only.
//

struct KvStoreOpenMsg {
    KvStoreHeader header;
    KvStoreType type;
    char fileName[KvStoreMaxPathLen + 1];
};

struct KvStoreCloseMsg {
    KvStoreHeader header;
    KvStoreCloseFlags flags;
};

struct KvStoreDeleteMsg {
    KvStoreHeader header;
    char key[KvStoreMaxKeyLen + 1];
    KvStoreOptsMask optsMask;
};

struct KvStoreAddOrReplaceMsg {
    KvStoreHeader header;
    KvStoreOptsMask optsMask;
    KvStoreAddOrReplaceInput input;  // must be last
};

struct KvStoreMultiAddOrReplaceMsg {
    KvStoreHeader header;
    KvStoreOptsMask optsMask;
    KvStoreMultiAddOrReplaceInput input;  // must be last
};

struct KvStoreAppendMsg {
    KvStoreHeader header;
    KvStoreOptsMask optsMask;
    KvStoreAppendInput input;  // must be last
};

struct KvStoreSetIfEqualMsg {
    KvStoreHeader header;
    KvStoreOptsMask optsMask;
    size_t msgSize;
    KvStoreSetIfEqualInput input;  // must be last
};

struct KvStoreLookupMsg {
    KvStoreHeader header;
    char key[KvStoreMaxKeyLen + 1];
};

struct KvStoreListMsg {
    KvStoreHeader header;
    char keyRegex[KvStoreMaxKeyLen + 1];
};

struct KvStoreResponse {
    KvStoreHeader header;
    Status status;
    void *output;
    size_t outputSize;
};

KvStoreLib::KvStoreLib() {}

KvStoreLib::~KvStoreLib() {}

Status  // static
KvStoreLib::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) KvStoreLib;
    if (instance == NULL) {
        return StatusNoMem;
    }
    Status status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }
    return status;
}

void  // static
KvStoreLib::destroy()
{
    if (instance == NULL) {
        return;
    }
    delete instance;
    instance = NULL;
}

KvStoreLib *  // static
KvStoreLib::get()
{
    return instance;
}

Status
KvStoreLib::initInternal()
{
    Status status = StatusOk;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("kvstore", &kvStoreStatGroupId_, 2);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&persistSuccessCount_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(kvStoreStatGroupId_,
                                         "persist.success",
                                         persistSuccessCount_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&persistFailureCount_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(kvStoreStatGroupId_,
                                         "persist.failure",
                                         persistFailureCount_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
CommonExit:
    return status;
}

Status
KvStoreLib::isValidKeyScope(XcalarApiKeyScope scope)
{
    switch (scope) {
    case XcalarApiKeyScopeGlobal:
    case XcalarApiKeyScopeSession:
        return StatusOk;
        break;
    case XcalarApiKeyScopeUser:
        return StatusUnimpl;
        break;
    default:
        return StatusInval;
    }
    NotReached();
}

bool
KvStoreLib::requiresSession(XcalarApiKeyScope scope)
{
    if (scope == XcalarApiKeyScopeSession) {
        return true;
    } else {
        return false;
    }
}

bool
KvStoreLib::requiresUserId(XcalarApiKeyScope scope)
{
    if (scope == XcalarApiKeyScopeGlobal) {
        return false;
    } else {
        return true;
    }
}

// This is called from StringHashTable when a removeAll is done.
void
KvStoreEntry::del()
{
    memFree(this);
}

void
KvStoreLib::closeLocalInternal(KvStore *kvStore, KvStoreCloseFlags flags)
{
    kvStore->persistedClose(flags & KvStoreCloseDeletePersisted);
}

void
KvStoreLib::closeLocal(KvStoreId kvStoreId, KvStoreCloseFlags flags)
{
    KvStore *kvStore;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore != NULL) {
        KvStore *removedKv = kvStoreMap_.remove(kvStoreId);
        assert(removedKv == kvStore);
        assert(removedKv->getKvStoreId() == kvStoreId);

        assert(kvStore->getState() == KvStore::State::Ready);
        if (KvStore::State::Ready != kvStore->getState()) {
            kvStoreMapLock_.unlock();
            return;
        }

        removedKv->setState(KvStore::State::CloseInProgress);
        kvStore->incRef();
        kvStoreMapLock_.unlock();

        // Grab writer lock to quiesce the KvStore
        kvStore->lock_.lock(RwLock::Type::Writer);
        closeLocalInternal(kvStore, flags);
        kvStore->lock_.unlock(RwLock::Type::Writer);

        kvStore->decRef();
        kvStore->decRef();  // Drops the original ref from construction
    } else {
        kvStoreMapLock_.unlock();
    }
}

Status
KvStoreLib::openLocal(KvStoreId kvStoreId,
                      const char *kvStoreName,
                      KvStoreType type)
{
    Status status = StatusOk;
    KvStore *kvStore = NULL;

    if (type <= KvStoreInvalid || type >= KvStoreLast) {
        assert(0);
        return StatusInval;
    }

    kvStoreMapLock_.lock();
    if (kvStoreMap_.find(kvStoreId) != NULL) {
        kvStoreMapLock_.unlock();
        return StatusExist;
    }

    kvStore = new (std::nothrow) KvStore(kvStoreId, type);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusNoMem;
    }

    status = kvStoreMap_.insert(kvStore);
    assert(status == StatusOk);

    assert(kvStore->getState() == KvStore::State::Init);
    kvStore->setState(KvStore::State::OpenInProgress);
    kvStoreMapLock_.unlock();

    kvStore->lock_.lock(RwLock::Type::Writer);
    status = kvStore->persistedInit(kvStoreName);
    kvStore->lock_.unlock(RwLock::Type::Writer);
    BailIfFailed(status);

    // KvStore is ready now
    kvStore->setState(KvStore::State::Ready);
CommonExit:
    if (status != StatusOk) {
        if (kvStore != NULL) {
            kvStore->setState(KvStore::State::CloseInProgress);
            kvStore->lock_.lock(RwLock::Type::Writer);
            closeLocalInternal(kvStore, KvStoreCloseOnly);
            kvStore->lock_.unlock(RwLock::Type::Writer);
            // Drops to original ref from construction
            kvStore->decRef();
        }
    }

    return status;
}

size_t
KvStoreLib::kvStoreEntrySize(size_t keySize, size_t valueSize)
{
    return sizeof(KvStoreEntry) + keySize + valueSize;
}

KvStoreEntry *
KvStoreLib::kvStoreInitEntry(size_t keySize, size_t valueSize, bool persist)
{
    KvStoreEntry *kvEntry;

    kvEntry = (KvStoreEntry *) memAllocExt(kvStoreEntrySize(keySize, valueSize),
                                           "KvStoreEntry");
    if (kvEntry == NULL) {
        return NULL;
    }

    kvEntry->persist = persist;
    kvEntry->keySize = keySize;
    kvEntry->valueSize = valueSize;
    memset(&kvEntry->hook, 0, sizeof(kvEntry->hook));

    kvEntry->key = &((char *) kvEntry)[sizeof(*kvEntry)];
    kvEntry->value = kvEntry->key + keySize;

    return kvEntry;
}

//
// Inserts/replaces kvEntry in kvStore hashtable. kvEntryReplaced populated
// with replaced KvStoreEntry if replacing, NULL otherwise. Will persist and
// persist synchronously if configured on kvEntry. Caller must have already
// locked kvStore.
//
Status
KvStoreLib::addOrReplaceThenPersist(KvStore *kvStore,
                                    KvStoreEntry *kvEntry,
                                    KvStoreOptsMask optsMask,
                                    KvStoreEntry **kvEntryReplaced)
{
    KvStoreEntry *oldKvEntry = NULL;
    *kvEntryReplaced = NULL;

    if (kvStore->kvHash_.insert(kvEntry) != StatusOk) {
        // There is already an old KV entry here that needs to be kicked out.
        oldKvEntry = kvStore->kvHash_.remove(kvEntry->key);
        assert(oldKvEntry != NULL);
        verify(kvStore->kvHash_.insert(kvEntry) == StatusOk);
    }

    kvStore->bytesMemory_ +=
        kvStoreEntrySize(kvEntry->keySize, kvEntry->valueSize);

    Status status = StatusOk;
    if (kvEntry->persist && !(optsMask & KvStoreOptDisablePersist)) {
        status = kvStore->persistedPut(kvEntry);
        assert(status == StatusOk);
    }

    if (status == StatusOk) {
        if (oldKvEntry != NULL) {
            kvStore->bytesMemory_ -=
                kvStoreEntrySize(oldKvEntry->keySize, oldKvEntry->valueSize);
            *kvEntryReplaced = oldKvEntry;
        }
    } else {
        // New kv entry removed
        KvStoreEntry *kvEntryTmp = kvStore->kvHash_.remove(kvEntry->key);
        verify(kvEntryTmp == kvEntry);

        // Old kv entry replaced into kvHash_
        if (oldKvEntry != NULL) {
            verify(kvStore->kvHash_.insert(oldKvEntry) == StatusOk);
        }
    }

    return status;
}

//
// Inserts/replaces kvEntries in kvStore hashtables. kvEntryReplaced populated
// with replaced KvStoreEntries if replacing, NULL otherwise. Will persist and
// persist synchronously if configured on kvEntries. Caller must have already
// locked kvStore.
//
Status
KvStoreLib::multiAddOrReplaceThenPersist(KvStore *kvStore,
                                         KvStoreEntry **kvEntries,
                                         KvStoreOptsMask optsMask,
                                         size_t numEnts,
                                         bool persist)
{
    Status status = StatusOk;
    KvStoreEntry **oldKvEntries =
        (KvStoreEntry **) memAlloc(numEnts * sizeof(*oldKvEntries));
    BailIfNull(oldKvEntries);
    memZero(oldKvEntries, numEnts * sizeof(*oldKvEntries));
    for (size_t ii = 0; ii < numEnts; ii++) {
        oldKvEntries[ii] = NULL;
        if (kvStore->kvHash_.insert(kvEntries[ii]) != StatusOk) {
            // There is already an old KV Entry here that needs to be kicked out
            oldKvEntries[ii] = kvStore->kvHash_.remove(kvEntries[ii]->key);
            assert(oldKvEntries[ii] != NULL);
            verify(kvStore->kvHash_.insert(kvEntries[ii]) == StatusOk);
        }
        kvStore->bytesMemory_ +=
            kvStoreEntrySize(kvEntries[ii]->keySize, kvEntries[ii]->valueSize);
    }

    for (size_t ii = 0; ii < numEnts; ii++) {
        kvStore->bytesMemory_ +=
            kvStoreEntrySize(kvEntries[ii]->keySize, kvEntries[ii]->valueSize);
    }

    if (persist && !(optsMask & KvStoreOptDisablePersist)) {
        status = kvStore->persistedMultiPut(kvEntries, NULL, numEnts);
        BailIfFailed(status);
    }

CommonExit:
    if (oldKvEntries == NULL) {
        return status;
    }
    if (status == StatusOk) {
        for (size_t ii = 0; ii < numEnts; ii++) {
            if (oldKvEntries[ii] != NULL) {
                kvStore->bytesMemory_ -=
                    kvStoreEntrySize(oldKvEntries[ii]->keySize,
                                     oldKvEntries[ii]->valueSize);
            }
            memFree(oldKvEntries[ii]);
            oldKvEntries[ii] = NULL;
        }
        memFree(oldKvEntries);
        oldKvEntries = NULL;
    } else {
        for (size_t ii = 0; ii < numEnts; ii++) {
            // New kv entry removed
            KvStoreEntry *kvEntryTmp =
                kvStore->kvHash_.remove(kvEntries[ii]->key);
            verify(kvEntryTmp == kvEntries[ii]);

            // Old kv entry replaced into kvHash_
            if (oldKvEntries[ii] != NULL) {
                verify(kvStore->kvHash_.insert(oldKvEntries[ii]) == StatusOk);
            }
        }
    }
    return status;
}

Status
KvStoreLib::addOrReplaceLocal(KvStoreId kvStoreId,
                              const char *key,
                              const char *value,
                              bool persist,
                              KvStoreOptsMask optsMask)
{
    Status status;
    KvStore *kvStore;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStore->lock_.lock(RwLock::Type::Writer);
    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    status = addOrReplaceLocal(kvStore, key, value, persist, optsMask);
    BailIfFailed(status);

CommonExit:
    kvStore->lock_.unlock(RwLock::Type::Writer);
    kvStore->decRef();
    return status;
}

Status
KvStoreLib::addOrReplaceLocal(KvStore *kvStore,
                              const char *key,
                              const char *value,
                              bool persist,
                              KvStoreOptsMask optsMask)
{
    KvStoreEntry *kvEntry;
    const size_t keySize = strlen(key) + 1;
    const size_t valueSize = strlen(value) + 1;

    kvEntry = kvStoreInitEntry(keySize, valueSize, persist);
    if (kvEntry == NULL) {
        return StatusNoMem;
    }

    strlcpy((char *) kvEntry->key, key, keySize);
    strlcpy((char *) kvEntry->value, value, valueSize);

    Status status;
    KvStoreEntry *kvEntryReplaced;
    status =
        addOrReplaceThenPersist(kvStore, kvEntry, optsMask, &kvEntryReplaced);
    if (status != StatusOk) {
        memFree(kvEntry);
        kvEntry = NULL;
    } else {
        if (kvEntryReplaced != NULL) {
            memFree(kvEntryReplaced);
            kvEntryReplaced = NULL;
        }
    }

    return status;
}

Status
KvStoreLib::multiAddOrReplaceLocal(KvStoreId kvStoreId,
                                   KvStoreKeyValuePair *kvPairs,
                                   bool persist,
                                   KvStoreOptsMask optsMask,
                                   size_t numKvPairs)
{
    Status status;
    KvStore *kvStore;
    KvStoreEntry **kvEntries;
    char *kvPairsCursor = NULL;
    bool kvStoreLocked = false;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (!kvStore) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStoreLocked = true;
    kvStore->lock_.lock(RwLock::Type::Writer);

    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    kvEntries = (KvStoreEntry **) memAlloc(numKvPairs * sizeof(*kvEntries));
    BailIfNull(kvEntries);
    memZero(kvEntries, numKvPairs * sizeof(*kvEntries));
    // KvPairs is now a variable size value. Need to cursor iteratively
    kvPairsCursor = (char *) kvPairs;
    for (size_t ii = 0; ii < numKvPairs; ii++) {
        KvStoreEntry *kvEntry;
        const size_t keySize =
            strlen(((KvStoreKeyValuePair *) kvPairsCursor)->key) + 1;
        const size_t valueSize =
            strlen(((KvStoreKeyValuePair *) kvPairsCursor)->value) + 1;
        kvEntry = kvStoreInitEntry(keySize, valueSize, persist);
        BailIfNull(kvEntry);

        if (strlcpy((char *) kvEntry->key,
                    ((KvStoreKeyValuePair *) kvPairsCursor)->key,
                    keySize) >= keySize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        if (strlcpy((char *) kvEntry->value,
                    ((KvStoreKeyValuePair *) kvPairsCursor)->value,
                    valueSize) >= valueSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        kvEntries[ii] = kvEntry;
        kvPairsCursor += sizeof(KvStoreKeyValuePair) + valueSize;
    }
    status = multiAddOrReplaceThenPersist(kvStore,
                                          kvEntries,
                                          optsMask,
                                          numKvPairs,
                                          persist);
    BailIfFailed(status);

CommonExit:
    if (kvStoreLocked) {
        kvStore->lock_.unlock(RwLock::Type::Writer);
        kvStore->decRef();
    }
    if (status != StatusOk) {
        if (kvEntries != NULL) {
            for (size_t ii = 0; ii < numKvPairs; ii++) {
                memFree(kvEntries[ii]);
                kvEntries[ii] = NULL;
            }
        }
    }
    if (kvEntries != NULL) {
        memFree(kvEntries);
        kvEntries = NULL;
    }
    return status;
}

Status
KvStoreLib::appendLocal(KvStoreId kvStoreId,
                        const char *key,
                        const char *suffix,
                        KvStoreOptsMask optsMask)
{
    KvStore *kvStore;
    Status status;
    KvStoreEntry *kvEntryReplaced = NULL;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStore->lock_.lock(RwLock::Type::Writer);

    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    KvStoreEntry *oldKvEntry;
    KvStoreEntry *newKvEntry;

    oldKvEntry = kvStore->kvHash_.find(key);
    if (oldKvEntry == NULL) {
        status = StatusKvEntryNotFound;
        goto CommonExit;
    }

    size_t suffixLen;
    suffixLen = strlen(suffix);
    size_t valueSize;
    valueSize = oldKvEntry->valueSize + suffixLen;

    if (suffixLen == 0) {
        // No work to do with empty suffix
        goto CommonExit;
    }

    newKvEntry =
        kvStoreInitEntry(oldKvEntry->keySize, valueSize, oldKvEntry->persist);
    if (newKvEntry == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    strlcpy((char *) newKvEntry->key, oldKvEntry->key, oldKvEntry->keySize);

    strlcpy((char *) newKvEntry->value, oldKvEntry->value, valueSize);
    size_t catSize;
    catSize = strlcat((char *) newKvEntry->value, suffix, valueSize);
    assert(catSize == valueSize - 1);

    status = addOrReplaceThenPersist(kvStore,
                                     newKvEntry,
                                     optsMask,
                                     &kvEntryReplaced);
    if (status != StatusOk) {
        memFree(newKvEntry);
        newKvEntry = NULL;
    } else {
        assert(kvEntryReplaced == oldKvEntry);
        memFree(oldKvEntry);
        kvEntryReplaced = NULL;
        oldKvEntry = NULL;
    }
CommonExit:
    kvStore->lock_.unlock(RwLock::Type::Writer);
    kvStore->decRef();
    return status;
}

Status
KvStoreLib::setIfEqualLocal(KvStoreId kvStoreId,
                            uint32_t countSecondaryPairs,
                            const char *keyCompare,
                            const char *valueCompare,
                            const char *valueReplace,
                            const char *keySecondary,
                            const char *valueSecondary,
                            bool persist,
                            KvStoreOptsMask optsMask)
{
    Status status;
    KvStore *kvStore;

    KvStoreEntry *kvReplace = NULL;
    KvStoreEntry *kvSecondary = NULL;
    KvStoreEntry *kvPrevious = NULL;
    KvStoreEntry *kvCompareCurrent;
    size_t keyCompareSize;
    size_t valueReplaceSize;
    size_t valueSecondarySize;
    size_t keyLen = 0;
    bool kvStoreLocked = false;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }
    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStoreLocked = true;
    kvStore->lock_.lock(RwLock::Type::Writer);

    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    // Lookup existing entry.
    kvCompareCurrent = kvStore->kvHash_.find(keyCompare);
    if (kvCompareCurrent == NULL) {
        // keyCompare not currently in hash table. Value is considered to be "".
        if (strlen(valueCompare) != 0) {
            // Expected a value for keyCompare, return EntryNotFound.
            status = StatusKvEntryNotFound;
            goto CommonExit;
        }
    } else {
        // Does current KvStoreEntry match valueCompare?
        if (strcmp(kvCompareCurrent->value, valueCompare) != 0) {
            status = StatusKvEntryNotEqual;
            goto CommonExit;
        }
    }

    keyCompareSize = strlen(keyCompare) + 1;
    valueReplaceSize = strlen(valueReplace) + 1;
    valueSecondarySize = strlen(valueSecondary) + 1;

    kvReplace = kvStoreInitEntry(keyCompareSize, valueReplaceSize, persist);
    BailIfNull(kvReplace);

    if (countSecondaryPairs == 1) {
        keyLen = strlen(keySecondary) + 1;
        kvSecondary = kvStoreInitEntry(keyLen + 1, valueSecondarySize, persist);
        BailIfNull(kvSecondary);
    }

    // Replace valueCompare with valueReplace.
    strlcpy((char *) kvReplace->key, keyCompare, keyCompareSize);
    strlcpy((char *) kvReplace->value, valueReplace, valueReplaceSize);

    status = addOrReplaceThenPersist(kvStore, kvReplace, optsMask, &kvPrevious);
    BailIfFailed(status);

    if (kvPrevious != NULL) {
        memFree(kvPrevious);
        kvPrevious = NULL;
    }
    kvReplace = NULL;  // In hash table. Don't free.

    if (countSecondaryPairs == 1) {
        strlcpy((char *) kvSecondary->key, keySecondary, keyLen + 1);
        strlcpy((char *) kvSecondary->value,
                valueSecondary,
                valueSecondarySize);

        // Replace current value of keySecondary with valueSecondary.
        status = addOrReplaceThenPersist(kvStore,
                                         kvSecondary,
                                         optsMask,
                                         &kvPrevious);
        if (status != StatusOk && kvPrevious != NULL) {
            // Attempt to revert kvReplace updated.
            KvStoreEntry *kvPreviousTemp = kvPrevious;
            Status statusRevert = addOrReplaceThenPersist(kvStore,
                                                          kvPreviousTemp,
                                                          optsMask,
                                                          &kvPrevious);
            if (statusRevert != StatusOk) {
                // Cleanup code will free kvSecondary and kvCompareCurrent
                // beacuse neither is in the hash table.
                xSyslog(ModuleName,
                        XlogCrit,
                        "Couldn't restore previous KvStore state after "
                        "failure.");
            }

            goto CommonExit;
        }

        kvSecondary = NULL;
    }

CommonExit:
    if (kvStoreLocked) {
        kvStore->lock_.unlock(RwLock::Type::Writer);
        kvStore->decRef();
    }
    if (kvReplace != NULL) {
        memFree(kvReplace);
    }
    if (kvSecondary != NULL) {
        memFree(kvSecondary);
    }
    if (kvPrevious != NULL) {
        memFree(kvPrevious);
    }

    return status;
}

Status
KvStoreLib::lookupLocal(KvStoreId kvStoreId,
                        const char *key,
                        void **retOutput,
                        size_t *retOutputSize)
{
    Status status = StatusOk;
    char *value = NULL;
    bool kvStoreLocked = false;

    *retOutput = NULL;
    *retOutputSize = 0;

    kvStoreMapLock_.lock();
    KvStore *kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStoreLocked = true;
    kvStore->lock_.lock(RwLock::Type::Reader);

    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    KvStoreEntry *kvEntry;
    kvEntry = kvStore->kvHash_.find(key);
    if (kvEntry == NULL) {
        status = StatusKvEntryNotFound;
        goto CommonExit;
    }

    value = (char *) memAlloc(kvEntry->valueSize + sizeof(size_t));
    if (value == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    *((size_t *) value) = kvEntry->valueSize;
    verifyOk(strStrlcpy((char *) ((uintptr_t) value + sizeof(size_t)),
                        kvEntry->value,
                        kvEntry->valueSize));

    *retOutput = value;
    *retOutputSize = kvEntry->valueSize + sizeof(size_t);

CommonExit:
    if (kvStoreLocked) {
        kvStore->lock_.unlock(RwLock::Type::Reader);
        kvStore->decRef();
    }
    return status;
}

Status
KvStoreLib::lookupActual(KvStoreId kvStoreId,
                         const char *key,
                         char **value,
                         size_t *valueSize)
{
    Status status;

    kvStoreMapLock_.lock();
    KvStore *kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }
    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }
    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStore->lock_.lock(RwLock::Type::Reader);

    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    status = lookupActual(kvStore, key, value, valueSize);
    BailIfFailed(status);

CommonExit:
    kvStore->lock_.unlock(RwLock::Type::Reader);
    kvStore->decRef();
    return status;
}

Status
KvStoreLib::lookupActual(KvStore *kvStore,
                         const char *key,
                         char **value,
                         size_t *valueSize)
{
    const KvStoreEntry *kvEntry = kvStore->kvHash_.find(key);
    if (kvEntry == NULL) {
        *valueSize = 0;
        return StatusKvEntryNotFound;
    }

    *value = (char *) memAlloc(kvEntry->valueSize);
    if (*value == NULL) {
        return StatusNoMem;
    }

    verifyOk(strStrlcpy(*value, kvEntry->value, kvEntry->valueSize));
    *valueSize = kvEntry->valueSize;

    return StatusOk;
}

Status
KvStoreLib::listLocal(KvStoreId kvStoreId,
                      const char *keyRegex,
                      void **retOutput,
                      size_t *retOutputSize)
{
    Status status = StatusOk;
    KeyList *keyList = NULL;

    *retOutputSize = 0;
    *retOutput = NULL;

    status = listActual(kvStoreId, keyRegex, &keyList);
    BailIfFailed(status);

    *retOutputSize =
        sizeof(*keyList) + keyList->numKeys * sizeof(keyList->keys[0]);
    *retOutput = keyList;

CommonExit:
    return status;
}

Status
KvStoreLib::listActual(KvStoreId kvStoreId,
                       const char *keyRegex,
                       KeyList **keyList)
{
    Status status = StatusOk;
    int ret;
    KvStore *kvStore;
    KvStoreEntry *kv;
    regex_t reg;
    bool regCompiled = false;
    int numMatchedKeys = 0;
    int numPlacedKeys = 0;
    char errorBuf[256];
    bool kvStoreLocked = false;

    ret = regcomp(&reg, keyRegex, REG_EXTENDED | REG_NOSUB);
    if (ret != 0) {
        regerror(ret, &reg, errorBuf, sizeof(errorBuf));
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "regex compilation failed for '%s': '%s'",
                      keyRegex,
                      errorBuf);
        status = StatusFailed;
        goto CommonExit;
    }
    regCompiled = true;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStoreLocked = true;
    kvStore->lock_.lock(RwLock::Type::Reader);

    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    // XXX this is 2 pass algorithm; if we had a proper 'list' structure, this
    // could just append to it and be a single pass
    // Count our future matches so we can preallocate a buffer
    for (auto iter = kvStore->kvHash_.begin(); (kv = iter.get()) != NULL;
         iter.next()) {
        ret = regexec(&reg, kv->key, 0, NULL, 0);
        if (ret == 0) {
            // We have a regex match
            ++numMatchedKeys;
        } else if (ret != REG_NOMATCH) {
            // Handle regex errors
            regerror(ret, &reg, errorBuf, sizeof(errorBuf));
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "regex match failed for '%s': '%s'",
                          keyRegex,
                          errorBuf);
            status = StatusFailed;
            goto CommonExit;
        }
    }

    *keyList = (KeyList *) memAlloc(
        sizeof(**keyList) + numMatchedKeys * sizeof((*keyList)->keys[0]));
    BailIfNull(*keyList);

    (*keyList)->numKeys = numMatchedKeys;

    for (auto iter = kvStore->kvHash_.begin(); (kv = iter.get()) != NULL;
         iter.next()) {
        ret = regexec(&reg, kv->key, 0, NULL, 0);
        if (ret == 0) {
            // Add the matched key to the list
            strlcpy((*keyList)->keys[numPlacedKeys],
                    kv->key,
                    sizeof((*keyList)->keys[0]));
            ++numPlacedKeys;
        } else if (ret != REG_NOMATCH) {
            // Handle regex errors
            regerror(ret, &reg, errorBuf, sizeof(errorBuf));
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "regex match failed for '%s': '%s'",
                          keyRegex,
                          errorBuf);
            status = StatusFailed;
            goto CommonExit;
        }
    }
    assert(numPlacedKeys == numMatchedKeys);

CommonExit:
    if (kvStoreLocked) {
        kvStore->lock_.unlock(RwLock::Type::Reader);
        kvStore->decRef();
    }
    if (status != StatusOk) {
        if (*keyList != NULL) {
            memFree(*keyList);
            *keyList = NULL;
        }
    }
    if (regCompiled) {
        regfree(&reg);
    }
    return status;
}

Status
KvStoreLib::delLocal(KvStoreId kvStoreId,
                     const char *key,
                     KvStoreOptsMask optsMask)
{
    KvStore *kvStore;
    KvStoreEntry *kvEntry = NULL;
    Status status = StatusOk;
    bool isKeyPersisted;
    bool kvStoreLocked = false;

    kvStoreMapLock_.lock();
    kvStore = kvStoreMap_.find(kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStoreLocked = true;
    kvStore->lock_.lock(RwLock::Type::Writer);
    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    kvEntry = kvStore->kvHash_.remove(key);
    if (kvEntry == NULL) {
        status = StatusKvEntryNotFound;
        goto CommonExit;
    }
    isKeyPersisted = kvEntry->persist;

    kvStore->bytesMemory_ -=
        kvStoreEntrySize(kvEntry->keySize, kvEntry->valueSize);

    memFree(kvEntry);
    kvEntry = NULL;

    status = kvStore->persistedDel(key, &kvEntry);
    if (status == StatusKvEntryNotFound && !isKeyPersisted) {
        // We attempt a persisted delete even if kvEntry->persist is false,
        // to cover the scenario in which a user created a persisted key, and
        // then modified it to be non-persistent - this will lead to the same
        // key having two values - one in-memory and one on-disk - currently
        // this is allowed, but we may disallow it in future - please see
        // SDK-760.
        status = StatusOk;
    }
    BailIfFailed(status);

CommonExit:
    if (kvStoreLocked) {
        kvStore->lock_.unlock(RwLock::Type::Writer);
        kvStore->decRef();
    }
    if (kvEntry) {
        memFree(kvEntry);
        kvEntry = NULL;
    }
    return status;
}

unsigned
KvStoreLib::getDlmNode(KvStoreId kvStoreId)
{
    return kvStoreId % Config::get()->getActiveNodes();
}

bool
KvStoreLib::isDlmNodeLocal(KvStoreId kvStoreId)
{
    return getDlmNode(kvStoreId) == Config::get()->getMyNodeId();
}

Status
KvStoreLib::dispatchDlm(KvStoreId kvStoreId,
                        void *payload,
                        size_t payloadSize,
                        void **retPayload,
                        size_t *retPayloadSize)
{
    Status status = StatusOk;
    MsgSendRecvFlags sendRecvFlags;

    assert((retPayload == NULL && retPayloadSize == NULL) ||
           (retPayload != NULL && retPayloadSize != NULL));

    if (retPayload) {
        sendRecvFlags =
            (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrPlusPayload);
        *retPayload = NULL;
        *retPayloadSize = 0;
    } else {
        sendRecvFlags =
            (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrOnly);
    }

    KvStoreResponse response;
    memZero(&response, sizeof(KvStoreResponse));
    memcpy(&response.header, payload, sizeof(KvStoreHeader));

    MsgEphemeral eph;
    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      payload,
                                      payloadSize,
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcDlmKvStoreOp1,
                                      &response,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcDlmUpdateKvStore,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  sendRecvFlags,
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  getDlmNode(kvStoreId),
                                  TwoPcClassNonNested);
    BailIfFailed(status);

    assert(!twoPcHandle.twoPcHandle);
    status = response.status;
    BailIfFailed(status);

    if (retPayload) {
        *retPayload = response.output;
        *retPayloadSize = response.outputSize;
    }
CommonExit:
    return status;
}

void
KvStoreLib::dlmUpdateKvStoreCompletion(MsgEphemeral *eph, void *payload)
{
    KvStoreResponse *response = (KvStoreResponse *) eph->ephemeral;
    response->status = eph->status;
    if (response->status != StatusOk) {
        return;
    }

    switch (response->header.kvStoreOp) {
    case KvStoreOpen:
    case KvStoreClose:
    case KvStoreAddOrReplace:
    case KvStoreMultiAddOrReplace:
    case KvStoreAppend:
    case KvStoreSetIfEqual:
    case KvStoreDelete:
    case KvStoreUpdateImdDependency:
        // No output returned
        break;
    case KvStoreLookup: {
        response->outputSize = *((size_t *) payload);
        response->output = memAlloc(response->outputSize);
        if (response->output == NULL) {
            response->status = StatusNoMem;
            return;
        }
        memcpy(response->output,
               (void *) ((uintptr_t) payload + sizeof(size_t)),
               response->outputSize);
        break;
    }
    case KvStoreList: {
        KeyList *keyList = (KeyList *) payload;
        response->outputSize =
            sizeof(*keyList) + keyList->numKeys * sizeof(keyList->keys[0]);
        response->output = memAlloc(response->outputSize);
        if (response->output == NULL) {
            response->status = StatusNoMem;
            return;
        }
        memcpy(response->output, payload, response->outputSize);
        break;
    }
    default:
        assert(0 && "Invalid kvStoreOp");
        break;
    }
}

// Open a key/value store
// note: when finished working with a key/value store, it the caller's
//       responsibility to close it with KvStoreLib::close()
Status
KvStoreLib::open(KvStoreId kvStoreId, const char *kvStoreName, KvStoreType type)
{
    KvStoreOpenMsg openMsg;

    if (type <= KvStoreInvalid || type >= KvStoreLast) {
        assert(0);
        return StatusInval;
    }

    openMsg.header.kvStoreId = kvStoreId;
    openMsg.header.kvStoreOp = KvStoreOpen;
    openMsg.type = type;
    strlcpy(openMsg.fileName, kvStoreName, sizeof(openMsg.fileName));
    return dispatchDlm(kvStoreId, &openMsg, sizeof(openMsg), NULL, NULL);
}

void
KvStoreLib::close(KvStoreId kvStoreId, KvStoreCloseFlags flags)
{
    Status status;

    if (kvStoreId == XidMgr::XidGlobalKvStore && flags == KvStoreCloseOnly &&
        Config::get()->getMyNodeId() != 0) {
        // XXX May be pick a different node for this.
        return;
    }

    KvStoreCloseMsg closeMsg;
    closeMsg.header.kvStoreId = kvStoreId;
    closeMsg.header.kvStoreOp = KvStoreClose;
    closeMsg.flags = flags;
    status = dispatchDlm(kvStoreId, &closeMsg, sizeof(closeMsg), NULL, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "failed to close log '%lu': %s",
                kvStoreId,
                strGetFromStatus(status));
    }
}

Status
KvStoreLib::addOrReplaceInt(KvStoreId kvStoreId,
                            const char *key,
                            size_t keySize,
                            const char *value,
                            size_t valueSize,
                            bool persist,
                            KvStoreOptsMask optsMask)
{
    Status status = StatusOk;
    // keySize and valuesize include null terminator
    if (keySize == 0) {
        return StatusKvInvalidKey;
    }
    if (valueSize == 0) {
        return StatusKvInvalidValue;
    }

    const size_t msgSize = sizeof(KvStoreAddOrReplaceMsg) + valueSize;
    KvStoreAddOrReplaceMsg *addOrReplaceMsg =
        (KvStoreAddOrReplaceMsg *) memAllocExt(msgSize,
                                               "KvStoreAddOrReplaceMsg");
    if (addOrReplaceMsg == NULL) {
        return StatusNoMem;
    }

    addOrReplaceMsg->header.kvStoreId = kvStoreId;
    addOrReplaceMsg->header.kvStoreOp = KvStoreAddOrReplace;
    addOrReplaceMsg->optsMask = optsMask;
    addOrReplaceMsg->input.persist = persist;
    strlcpy(addOrReplaceMsg->input.kvPair.key, key, keySize);
    addOrReplaceMsg->input.kvPair.valueSize = valueSize;
    memcpy(addOrReplaceMsg->input.kvPair.value, value, valueSize);
    status = dispatchDlm(kvStoreId, addOrReplaceMsg, msgSize, NULL, NULL);

    if (addOrReplaceMsg != NULL) {
        memFree(addOrReplaceMsg);
        addOrReplaceMsg = NULL;
    }

    return status;
}

// Add or replace the value of a key within a key/value store
Status
KvStoreLib::addOrReplace(KvStoreId kvStoreId,
                         const char *key,
                         size_t keyLen,
                         const char *value,
                         size_t valueLen,
                         bool persist,
                         KvStoreOptsMask optsMask)
{
    return addOrReplaceInt(kvStoreId,
                           key,
                           keyLen,
                           value,
                           valueLen,
                           persist,
                           optsMask);
}

Status
KvStoreLib::multiAddOrReplace(KvStoreId kvStoreId,
                              const char **keys,
                              const char **values,
                              const size_t *keysSize,
                              const size_t *valuesSize,
                              bool persist,
                              KvStoreOptsMask optsMask,
                              size_t numKvPairs)
{
    Status status;
    size_t valuePayloadSize = 0;
    size_t kvPairsPayloadSize = 0;
    // KvStoreKeyValuePair *kvPairs = NULL;
    char *kvPairsCursor = NULL;

    for (size_t ii = 0; ii < numKvPairs; ii++) {
        valuePayloadSize += valuesSize[ii];
    }
    kvPairsPayloadSize =
        numKvPairs * sizeof(KvStoreKeyValuePair) + valuePayloadSize;

    const size_t msgSize =
        sizeof(KvStoreMultiAddOrReplaceMsg) + kvPairsPayloadSize;

    KvStoreMultiAddOrReplaceMsg *multiAddOrReplaceMsg =
        (KvStoreMultiAddOrReplaceMsg *)
            memAllocExt(msgSize, "KvStoreMultiAddOrReplaceMsg");
    BailIfNull(multiAddOrReplaceMsg);

    multiAddOrReplaceMsg->header.kvStoreId = kvStoreId;
    multiAddOrReplaceMsg->header.kvStoreOp = KvStoreMultiAddOrReplace;
    multiAddOrReplaceMsg->optsMask = optsMask;
    multiAddOrReplaceMsg->input.persist = persist;
    multiAddOrReplaceMsg->input.numKvPairs = numKvPairs;

    // KvPairs is now a variable size value. Need to cursor iteratively
    kvPairsCursor = (char *) multiAddOrReplaceMsg->input.kvPairs;
    for (size_t ii = 0; ii < numKvPairs; ii++) {
        strlcpy(((KvStoreKeyValuePair *) kvPairsCursor)->key,
                keys[ii],
                keysSize[ii]);
        ((KvStoreKeyValuePair *) kvPairsCursor)->valueSize = valuesSize[ii];
        memcpy(((KvStoreKeyValuePair *) kvPairsCursor)->value,
               values[ii],
               valuesSize[ii]);
        kvPairsCursor += sizeof(KvStoreKeyValuePair) + valuesSize[ii];
    }
    status = dispatchDlm(kvStoreId, multiAddOrReplaceMsg, msgSize, NULL, NULL);
    BailIfFailed(status);

CommonExit:
    if (multiAddOrReplaceMsg != NULL) {
        memFree(multiAddOrReplaceMsg);
        multiAddOrReplaceMsg = NULL;
    }

    return status;
}

// Append suffix to end of value corresponding to key
Status
KvStoreLib::append(KvStoreId kvStoreId,
                   const char *key,
                   const char *suffix,
                   KvStoreOptsMask optsMask)
{
    Status status = StatusOk;
    const size_t suffixSize = strlen(suffix) + 1;
    const size_t msgSize = sizeof(KvStoreAppendMsg) + suffixSize;
    KvStoreAppendMsg *appendMsg =
        (KvStoreAppendMsg *) memAllocExt(msgSize, "KvStoreAppendMsg");
    if (appendMsg == NULL) {
        return StatusNoMem;
    }
    appendMsg->header.kvStoreId = kvStoreId;
    appendMsg->header.kvStoreOp = KvStoreAppend;
    appendMsg->optsMask = optsMask;
    strlcpy(appendMsg->input.key, key, sizeof(appendMsg->input.key));
    appendMsg->input.suffixSize = suffixSize;
    memcpy(appendMsg->input.suffix, suffix, suffixSize);
    status = dispatchDlm(kvStoreId, appendMsg, msgSize, NULL, NULL);
    memFree(appendMsg);
    return status;
}

// Looks up the value corresponding to keyCompare. If that value is equal to
// valueCompare (or both valueCompare and that value are empty), atomically:
//   - Set the value for keyCompare to valueReplace.
//   - Set the value for keySecondary to valueSecondary.
//   - countSecondaryPairs gives the number of secondary pairs provided. Must
//     currently be either 0 or 1.
//     XXX Allow array of secondary k/v pairs.
Status
KvStoreLib::setIfEqual(KvStoreId kvStoreId,
                       uint32_t countSecondaryPairs,
                       const char *keyCompare,
                       const char *valueCompare,
                       const char *valueReplace,
                       const char *keySecondary,
                       const char *valueSecondary,
                       bool persist,
                       KvStoreOptsMask optsMask)
{
    Status status = StatusOk;
    const size_t valueCompareSize = strlen(valueCompare) + 1;
    const size_t valueReplaceSize = strlen(valueReplace) + 1;
    const size_t valueSecondarySize = strlen(valueSecondary) + 1;
    const size_t msgSize = sizeof(KvStoreSetIfEqualMsg) + valueCompareSize +
                           valueReplaceSize + valueSecondarySize;
    KvStoreSetIfEqualMsg *setIfEqualMsg = NULL;

    if (countSecondaryPairs == 0) {
        // Ignore passed secondary values.
        keySecondary = "";
        valueSecondary = "";
    }

    // Validation.
    if (countSecondaryPairs > 1) {
        status = StatusInval;
        goto CommonExit;
    }
    if (strlen(keyCompare) + 1 > sizeof(setIfEqualMsg->input.keyCompare)) {
        status = StatusInval;
        goto CommonExit;
    }
    if (strlen(keySecondary) + 1 > sizeof(setIfEqualMsg->input.keySecondary)) {
        status = StatusInval;
        goto CommonExit;
    }
    if (countSecondaryPairs == 1 && strcmp(keyCompare, keySecondary) == 0) {
        status = StatusInval;
        goto CommonExit;
    }

    setIfEqualMsg =
        (KvStoreSetIfEqualMsg *) memAllocExt(msgSize, "KvStoreSetIfEqualMsg");
    if (setIfEqualMsg == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    setIfEqualMsg->msgSize = msgSize;
    setIfEqualMsg->header.kvStoreId = kvStoreId;
    setIfEqualMsg->header.kvStoreOp = KvStoreSetIfEqual;
    setIfEqualMsg->optsMask = optsMask;
    setIfEqualMsg->input.persist = persist;
    setIfEqualMsg->input.countSecondaryPairs = countSecondaryPairs;
    setIfEqualMsg->input.valueCompareSize = valueCompareSize;
    setIfEqualMsg->input.valueReplaceSize = valueReplaceSize;
    setIfEqualMsg->input.valueSecondarySize = valueSecondarySize;

    strlcpy(setIfEqualMsg->input.keyCompare,
            keyCompare,
            sizeof(setIfEqualMsg->input.keyCompare));
    strlcpy(setIfEqualMsg->input.keySecondary,
            keySecondary,
            sizeof(setIfEqualMsg->input.keySecondary));
    strlcpy(setIfEqualMsg->input.values, valueCompare, valueCompareSize);
    strlcpy(setIfEqualMsg->input.values + valueCompareSize,
            valueReplace,
            valueReplaceSize);
    strlcpy(setIfEqualMsg->input.values + valueCompareSize + valueReplaceSize,
            valueSecondary,
            valueSecondarySize);
    status = dispatchDlm(kvStoreId, setIfEqualMsg, msgSize, NULL, NULL);
    BailIfFailed(status);

CommonExit:
    if (setIfEqualMsg != NULL) {
        memFree(setIfEqualMsg);
        setIfEqualMsg = NULL;
    }

    return status;
}

// Delete a key/value tuple within a key/value store
Status
KvStoreLib::del(KvStoreId kvStoreId, const char *key, KvStoreOptsMask optsMask)
{
    KvStoreDeleteMsg deleteMsg;
    deleteMsg.header.kvStoreId = kvStoreId;
    deleteMsg.header.kvStoreOp = KvStoreDelete;
    strlcpy(deleteMsg.key, key, sizeof(deleteMsg.key));
    deleteMsg.optsMask = optsMask;
    return dispatchDlm(kvStoreId, &deleteMsg, sizeof(deleteMsg), NULL, NULL);
}

Status
KvStoreLib::list(KvStoreId kvStoreId, const char *keyRegex, KeyList **keyList)
{
    Status status = StatusOk;
    size_t keyListSize;
    *keyList = NULL;

    KvStoreListMsg *listMsg =
        (KvStoreListMsg *) memAllocExt(sizeof(KvStoreListMsg),
                                       "KvStoreListMsg");
    BailIfNull(listMsg);

    listMsg->header.kvStoreId = kvStoreId;
    listMsg->header.kvStoreOp = KvStoreList;
    status = strStrlcpy(listMsg->keyRegex, keyRegex, sizeof(listMsg->keyRegex));
    BailIfFailed(status);

    status = dispatchDlm(kvStoreId,
                         listMsg,
                         sizeof(KvStoreListMsg),
                         (void **) keyList,
                         &keyListSize);
    BailIfFailed(status);

CommonExit:
    if (listMsg != NULL) {
        memFree(listMsg);
    }
    return status;
}

Status
KvStoreLib::lookup(KvStoreId kvStoreId,
                   const char *key,
                   char **value,
                   size_t *valueSize)
{
    Status status = StatusOk;
    *value = NULL;
    *valueSize = 0;

    KvStoreLookupMsg *lookupMsg =
        (KvStoreLookupMsg *) memAllocExt(sizeof(KvStoreLookupMsg),
                                         "KvStoreLookupMsg");
    BailIfNull(lookupMsg);

    lookupMsg->header.kvStoreId = kvStoreId;
    lookupMsg->header.kvStoreOp = KvStoreLookup;
    status = strStrlcpy(lookupMsg->key, key, sizeof(lookupMsg->key));
    BailIfFailed(status);

    status = dispatchDlm(kvStoreId,
                         lookupMsg,
                         sizeof(KvStoreLookupMsg),
                         (void **) value,
                         valueSize);
    BailIfFailed(status);

CommonExit:
    if (lookupMsg != NULL) {
        memFree(lookupMsg);
    }
    return status;
}

void
KvStoreLib::updateDlm(MsgEphemeral *ephIn, void *payload)
{
    Status status = StatusOk;
    KvStoreHeader *kvh = (KvStoreHeader *) payload;
    size_t numBytesUsedInPayload = 0;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    ephIn->payloadToDistribute = NULL;

    switch (kvh->kvStoreOp) {
    case KvStoreList: {
        KvStoreListMsg *listMsg = (KvStoreListMsg *) payload;
        status = listLocal(listMsg->header.kvStoreId,
                           listMsg->keyRegex,
                           &ephIn->payloadToDistribute,
                           &numBytesUsedInPayload);
        break;
    }
    case KvStoreLookup: {
        KvStoreLookupMsg *lookupMsg = (KvStoreLookupMsg *) payload;
        status = lookupLocal(lookupMsg->header.kvStoreId,
                             lookupMsg->key,
                             &ephIn->payloadToDistribute,
                             &numBytesUsedInPayload);
        break;
    }
    case KvStoreOpen: {
        KvStoreOpenMsg *openMsg = (KvStoreOpenMsg *) payload;
        status = openLocal(openMsg->header.kvStoreId,
                           openMsg->fileName,
                           openMsg->type);
        break;
    }
    case KvStoreClose: {
        KvStoreCloseMsg *closeMsg = (KvStoreCloseMsg *) payload;
        closeLocal(closeMsg->header.kvStoreId, closeMsg->flags);
        break;
    }
    case KvStoreAddOrReplace: {
        KvStoreAddOrReplaceMsg *addOrReplaceMsg =
            (KvStoreAddOrReplaceMsg *) payload;
        status = addOrReplaceLocal(addOrReplaceMsg->header.kvStoreId,
                                   addOrReplaceMsg->input.kvPair.key,
                                   addOrReplaceMsg->input.kvPair.value,
                                   addOrReplaceMsg->input.persist,
                                   addOrReplaceMsg->optsMask);
        break;
    }
    case KvStoreMultiAddOrReplace: {
        KvStoreMultiAddOrReplaceMsg *multiAddOrReplaceMsg =
            (KvStoreMultiAddOrReplaceMsg *) payload;
        status = multiAddOrReplaceLocal(multiAddOrReplaceMsg->header.kvStoreId,
                                        multiAddOrReplaceMsg->input.kvPairs,
                                        multiAddOrReplaceMsg->input.persist,
                                        multiAddOrReplaceMsg->optsMask,
                                        multiAddOrReplaceMsg->input.numKvPairs);
        break;
    }
    case KvStoreAppend: {
        KvStoreAppendMsg *appendMsg = (KvStoreAppendMsg *) payload;
        status = appendLocal(appendMsg->header.kvStoreId,
                             appendMsg->input.key,
                             appendMsg->input.suffix,
                             appendMsg->optsMask);
        break;
    }
    case KvStoreSetIfEqual: {
        KvStoreSetIfEqualMsg *setIfEqualMsg = (KvStoreSetIfEqualMsg *) payload;
        char *valueCompare;
        char *valueReplace;
        char *valueSecondary;
        status = parseSetIfEqualInput(&setIfEqualMsg->input,
                                      setIfEqualMsg->msgSize -
                                          offsetof(KvStoreSetIfEqualMsg, input),
                                      &valueCompare,
                                      &valueReplace,
                                      &valueSecondary);
        if (status != StatusOk) {
            break;
        }

        status = setIfEqualLocal(setIfEqualMsg->header.kvStoreId,
                                 setIfEqualMsg->input.countSecondaryPairs,
                                 setIfEqualMsg->input.keyCompare,
                                 valueCompare,
                                 valueReplace,
                                 setIfEqualMsg->input.keySecondary,
                                 valueSecondary,
                                 setIfEqualMsg->input.persist,
                                 setIfEqualMsg->optsMask);
        break;
    }
    case KvStoreDelete: {
        KvStoreDeleteMsg *deleteMsg = (KvStoreDeleteMsg *) payload;
        status = delLocal(deleteMsg->header.kvStoreId,
                          deleteMsg->key,
                          deleteMsg->optsMask);
        break;
    }
    case KvStoreUpdateImdDependency: {
        PublishDependencyUpdateInput *input =
            (PublishDependencyUpdateInput *) payload;

        status = updatePublishDependencyLocal(input);
        break;
    }
    default:
        assert(0);
        status = StatusInval;
    }

    ephIn->setAckInfo(status, numBytesUsedInPayload);
}

Status
KvStoreLib::parseSetIfEqualInput(KvStoreSetIfEqualInput *setIfEqualInput,
                                 size_t length,
                                 char **valueCompare,
                                 char **valueReplace,
                                 char **valueSecondary)
{
    if ((length < sizeof(*setIfEqualInput)) ||
        (length < sizeof(*setIfEqualInput) + setIfEqualInput->valueCompareSize +
                      setIfEqualInput->valueReplaceSize +
                      setIfEqualInput->valueSecondarySize)) {
        xSyslog(ModuleName, XlogErr, "XcalarApiKeySetIfEqual invalid input.");

        return StatusInval;
    }

    *valueCompare = setIfEqualInput->values;
    *valueReplace = setIfEqualInput->values + setIfEqualInput->valueCompareSize;
    *valueSecondary = setIfEqualInput->values +
                      setIfEqualInput->valueCompareSize +
                      setIfEqualInput->valueReplaceSize;
    return StatusOk;
}

// XXX: move this out of the kvstore subsystem eventually
void
KvStoreLib::removePublishedTable(json_t *dependencies,
                                 PublishDependencyUpdateInput *input)
{
    json_t *cur = json_object_get(dependencies, input->name);
    json_t *parents = json_object_get(cur, "parents");
    json_t *children = json_object_get(cur, "children");

    const char *name;
    json_t *v;
    // process parent array
    json_object_foreach (parents, name, v) {
        json_t *parent = json_object_get(dependencies, name);
        json_t *parentChildren = json_object_get(parent, "children");

        json_object_del(parentChildren, input->name);
    }

    // process children array
    json_object_foreach (children, name, v) {
        json_t *child = json_object_get(dependencies, name);
        json_t *childParents = json_object_get(child, "parents");

        json_object_del(childParents, input->name);
    }

    json_object_del(dependencies, input->name);
}

Status
KvStoreLib::addNewPublishedTable(json_t *dependencies,
                                 PublishDependencyUpdateInput *input)
{
    Status status;
    json_t *tableJson = NULL, *parentsJson = NULL, *childrenJson = NULL;
    json_t *jTrue = NULL;
    int ret;

    // setup new published table entry
    tableJson = json_object();
    BailIfNull(tableJson);

    parentsJson = json_object();
    BailIfNull(parentsJson);

    childrenJson = json_object();
    BailIfNull(childrenJson);

    {
        // process parent array
        char *parentName = input->parentNames;
        for (unsigned ii = 0; ii < input->numParents; ii++) {
            // add self to parents' children
            json_t *parent = json_object_get(dependencies, parentName);
            if (parent) {
                assert(jTrue == NULL);
                jTrue = json_true();
                BailIfNull(jTrue);
                json_t *parentChildren = json_object_get(parent, "children");
                ret = json_object_set_new(parentChildren, input->name, jTrue);
                BailIfFailedWith(ret, StatusNoMem);
                jTrue = NULL;
            }

            // add to parentName parent list
            assert(jTrue == NULL);
            jTrue = json_true();
            BailIfNull(jTrue);
            ret = json_object_set_new(parentsJson, parentName, jTrue);
            BailIfFailedWith(ret, StatusNoMem);
            jTrue = NULL;
            parentName += strlen(parentName) + 1;
        }
    }

    ret = json_object_set_new(tableJson, "parents", parentsJson);
    BailIfFailedWith(ret, StatusNoMem);
    parentsJson = NULL;

    ret = json_object_set_new(tableJson, "children", childrenJson);
    BailIfFailedWith(ret, StatusNoMem);
    childrenJson = NULL;

    ret = json_object_set_new(dependencies, input->name, tableJson);
    BailIfFailedWith(ret, StatusNoMem);
    tableJson = NULL;

CommonExit:
    if (jTrue) {
        json_decref(jTrue);
    }

    if (tableJson) {
        json_decref(tableJson);
    }

    if (parentsJson) {
        json_decref(parentsJson);
    }

    if (childrenJson) {
        json_decref(childrenJson);
    }

    return status;
}

Status
KvStoreLib::updatePublishDependencyLocal(PublishDependencyUpdateInput *input)
{
    // this function is executed underneath the kvstore lock
    char *value = NULL, *newValue = NULL;
    size_t valueSize;
    Status status;
    json_t *dependencies = NULL;
    json_error_t err;

    kvStoreMapLock_.lock();
    KvStore *kvStore = kvStoreMap_.find(input->hdr.kvStoreId);
    if (kvStore == NULL) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    if (KvStore::State::Ready != kvStore->getState()) {
        kvStoreMapLock_.unlock();
        return StatusKvStoreNotFound;
    }

    kvStore->incRef();
    kvStoreMapLock_.unlock();

    kvStore->lock_.lock(RwLock::Type::Writer);
    if (KvStore::State::Ready != kvStore->getState()) {
        status = StatusKvStoreNotFound;
        goto CommonExit;
    }

    status = lookupActual(kvStore,
                          HashTreeMgr::PublishDependencyKey,
                          &value,
                          &valueSize);
    if (unlikely(status == StatusKvEntryNotFound)) {
        // first time adding a dependency
        status = StatusOk;

        dependencies = json_object();
        BailIfNull(dependencies);
    } else if (status == StatusOk) {
        dependencies = json_loads(value, 0, &err);
        BailIfNull(dependencies);
    }
    BailIfFailed(status);

    if (input->isDelete) {
        removePublishedTable(dependencies, input);
    } else {
        status = addNewPublishedTable(dependencies, input);
        BailIfFailed(status);
    }

    newValue = json_dumps(dependencies, 0);
    BailIfNull(newValue);

    status = addOrReplaceLocal(kvStore,
                               HashTreeMgr::PublishDependencyKey,
                               newValue,
                               true,
                               KvStoreOptNone);
    BailIfFailed(status);

CommonExit:
    kvStore->lock_.unlock(RwLock::Type::Writer);
    kvStore->decRef();
    if (value) {
        memFree(value);
    }
    if (newValue) {
        memFree(newValue);
    }
    if (dependencies != NULL) {
        json_decref(dependencies);
    }
    return status;
}

Status
KvStoreLib::updatePublishDependency(PublishDependencyUpdateInput *input,
                                    size_t inputSize)
{
    Status status = StatusOk;
    input->hdr.kvStoreId = XidMgr::XidGlobalKvStore;
    input->hdr.kvStoreOp = KvStoreUpdateImdDependency;

    status =
        dispatchDlm(XidMgr::XidGlobalKvStore, input, inputSize, NULL, NULL);
    return status;
}
