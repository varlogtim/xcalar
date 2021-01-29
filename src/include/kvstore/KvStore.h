// Copyright 2015 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _KVSTORE_H
#define _KVSTORE_H

#include "primitives/Primitives.h"
#include "kvstore/KvStoreTypes.h"
#include "msg/MessageTypes.h"
#include "util/IntHashTable.h"
#include "runtime/RwLock.h"
#include "runtime/Spinlock.h"

// XXX This module needs to be re-addressed:
//      - The use of KvStoreId instead of KvStoreHandle is unecessary (except
//        in the case of API calls).

enum KvStoreCloseFlags : uint32_t {
    KvStoreCloseOnly = 0x00000000,
    // Deleted any persisted KvStore data.
    KvStoreCloseDeletePersisted = 0x00001000,
};

class KvStoreLib
{
  public:
    typedef XcalarApiKeyListOutput KeyList;
    static constexpr const char *KvGlobalStoreName = "globalKvStore";
    // Global KVStore key namespace information
    // For XCE
    static constexpr const char *KvsXCEPrefix = "/sys";
    static constexpr const char *KvsXCEDatasetMeta = "datasetMeta";
    static constexpr const char *KvsXCESessMeta = "sessionMeta";
    static constexpr const char *KvsXCEPt = "pt";
    // For XD
    static constexpr const char *KvsXDPrefix = "/gui";

    static MustCheck Status init();
    static void destroy();
    static KvStoreLib *get();

    MustCheck Status open(KvStoreId kvStoreId,
                          const char *kvStoreName,
                          KvStoreType type);
    void close(KvStoreId kvStoreId, KvStoreCloseFlags flags);
    MustCheck Status addOrReplace(KvStoreId kvStoreId,
                                  const char *key,
                                  size_t keySize,
                                  const char *value,
                                  size_t valueSize,
                                  bool persist,
                                  KvStoreOptsMask optsMask);
    MustCheck Status multiAddOrReplace(KvStoreId kvStoreId,
                                       const char **keys,
                                       const char **values,
                                       const size_t *keysSize,
                                       const size_t *valuesSize,
                                       bool persist,
                                       KvStoreOptsMask optsMask,
                                       size_t numKvPairs);
    MustCheck Status addOrReplaceThenPersist(KvStore *kvStore,
                                             KvStoreEntry *kvEntry,
                                             KvStoreOptsMask optsMask,
                                             KvStoreEntry **kvEntryReplaced);
    MustCheck Status multiAddOrReplaceThenPersist(KvStore *kvStore,
                                                  KvStoreEntry **kvEntries,
                                                  KvStoreOptsMask optsMask,
                                                  size_t numEnts,
                                                  bool persist);
    MustCheck Status append(KvStoreId kvStoreId,
                            const char *key,
                            const char *suffix,
                            KvStoreOptsMask optsMask);
    MustCheck Status setIfEqual(KvStoreId kvStoreId,
                                uint32_t countSecondaryPairs,
                                const char *keyCompare,
                                const char *valueCompare,
                                const char *valueReplace,
                                const char *keySecondary,
                                const char *valueSecondary,
                                bool persist,
                                KvStoreOptsMask optsMask);
    MustCheck Status lookup(KvStoreId kvStoreId,
                            const char *key,
                            char **value,
                            size_t *valueSize);
    MustCheck Status list(KvStoreId kvStoreId,
                          const char *keyRegex,
                          KeyList **keyList);
    MustCheck Status del(KvStoreId kvStoreId,
                         const char *key,
                         KvStoreOptsMask optsMask);

    MustCheck Status updatePublishDependency(
        PublishDependencyUpdateInput *input, size_t inputSize);

    void updateDlm(MsgEphemeral *ephIn, void *payload);
    MustCheck Status
    parseSetIfEqualInput(KvStoreSetIfEqualInput *setIfEqualInput,
                         size_t length,
                         char **valueCompare,
                         char **valueReplace,
                         char **valueSecondary);
    MustCheck Status isValidKeyScope(XcalarApiKeyScope scope);
    MustCheck bool requiresSession(XcalarApiKeyScope scope);
    MustCheck bool requiresUserId(XcalarApiKeyScope scope);
    void dlmUpdateKvStoreCompletion(MsgEphemeral *eph, void *payload);
    MustCheck KvStoreEntry *kvStoreInitEntry(size_t keySize,
                                             size_t valueSize,
                                             bool persist);

  private:
    static constexpr const char *ModuleName = "libkvstore";
    static constexpr uint64_t NumKvStoreMapHashSlots = 251;
    StatGroupId kvStoreStatGroupId_;
    StatHandle persistSuccessCount_;
    StatHandle persistFailureCount_;
    static KvStoreLib *instance;
    IntHashTable<KvStoreId,
                 KvStore,
                 &KvStore::hook_,
                 &KvStore::getKvStoreId,
                 NumKvStoreMapHashSlots,
                 hashIdentity>
        kvStoreMap_;
    Mutex kvStoreMapLock_;

    KvStoreLib();
    ~KvStoreLib();

    // Disallow.
    KvStoreLib(const KvStoreLib &) = delete;
    KvStoreLib &operator=(const KvStoreLib &) = delete;

    MustCheck Status initInternal();
    MustCheck Status addOrReplaceInt(KvStoreId kvStoreId,
                                     const char *key,
                                     size_t keySize,
                                     const char *value,
                                     size_t valueSize,
                                     bool persist,
                                     KvStoreOptsMask optsMask);
    MustCheck Status delLocal(KvStoreId kvStoreId,
                              const char *key,
                              KvStoreOptsMask optsMask);
    MustCheck Status appendLocal(KvStoreId kvStoreId,
                                 const char *key,
                                 const char *suffix,
                                 KvStoreOptsMask optsMask);
    MustCheck Status addOrReplaceLocal(KvStore *kvStore,
                                       const char *key,
                                       const char *value,
                                       bool persist,
                                       KvStoreOptsMask optsMask);
    MustCheck Status addOrReplaceLocal(KvStoreId kvStoreId,
                                       const char *key,
                                       const char *value,
                                       bool persist,
                                       KvStoreOptsMask optsMask);
    MustCheck Status multiAddOrReplaceLocal(KvStoreId kvStoreId,
                                            KvStoreKeyValuePair *kvPairs,
                                            bool persist,
                                            KvStoreOptsMask optsMask,
                                            size_t numEnts);
    MustCheck Status openLocal(KvStoreId kvStoreId,
                               const char *kvStoreName,
                               KvStoreType type);
    void closeLocal(KvStoreId kvStoreId, KvStoreCloseFlags flags);
    void closeLocalInternal(KvStore *kvStore, KvStoreCloseFlags flags);
    MustCheck Status dispatchDlm(KvStoreId kvStoreId,
                                 void *payload,
                                 size_t payloadSize,
                                 void **retPayload,
                                 size_t *retPayloadSize);
    MustCheck Status setIfEqualLocal(KvStoreId kvStoreId,
                                     uint32_t countSecondaryPairs,
                                     const char *keyCompare,
                                     const char *valueCompare,
                                     const char *valueReplace,
                                     const char *keySecondary,
                                     const char *valueSecondary,
                                     bool persist,
                                     KvStoreOptsMask optsMask);
    MustCheck size_t kvStoreEntrySize(size_t keySize, size_t valueSize);
    MustCheck Status lookupLocal(KvStoreId kvStoreId,
                                 const char *key,
                                 void **retOutput,
                                 size_t *retOutputSize);
    MustCheck Status lookupActual(KvStoreId kvStoreId,
                                  const char *key,
                                  char **value,
                                  size_t *valueSize);
    MustCheck Status lookupActual(KvStore *kvStore,
                                  const char *key,
                                  char **value,
                                  size_t *valueSize);
    MustCheck Status listLocal(KvStoreId kvStoreId,
                               const char *keyRegex,
                               void **retOutput,
                               size_t *retOutputSize);
    MustCheck Status listActual(KvStoreId kvStoreId,
                                const char *keyRegex,
                                KeyList **keyList);

    MustCheck unsigned getDlmNode(KvStoreId kvStoreId);
    MustCheck bool isDlmNodeLocal(KvStoreId kvStoreId);

    MustCheck Status addNewPublishedTable(json_t *dependencies,
                                          PublishDependencyUpdateInput *input);
    void removePublishedTable(json_t *dependencies,
                              PublishDependencyUpdateInput *input);

    MustCheck Status
    updatePublishDependencyLocal(PublishDependencyUpdateInput *input);
};

#endif
