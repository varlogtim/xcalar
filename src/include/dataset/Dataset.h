// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATASET_H_
#define _DATASET_H_

#include "dataset/DatasetTypes.h"
#include "ns/LibNs.h"
#include "ns/LibNsTypes.h"
#include "dag/DagLib.h"
#include "util/IntHashTable.h"
#include "util/StringHashTable.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "libapis/LibApisCommon.h"

extern const DfFieldValue SrcConstantKeyValue;

extern NewKeyValueMeta ResultSetJsonKvMeta;
extern NewKeyValueMeta SrcConstantKvmeta;

class Dataset final
{
    friend class LibDatasetGvm;

  public:
    static constexpr uint64_t MaxNameLen = 255;
    // Names used in libNs
    static constexpr const char *DsPrefix = "/dataset/";
    static constexpr const char *DsMetaPrefix = "/datasetMeta/";

    // Singleton APIs
    MustCheck static Status init();
    void destroy();
    MustCheck static Dataset *get();

    // Load the dataset meta at boot time
    MustCheck Status loadAllDatasetMeta();

    // No handle is acquired here. It's up to the caller to ensure that
    // this is not yanked out when being used by grabbing a handle upstream.
    MustCheck DsDataset *getDatasetFromId(DsDatasetId datasetId,
                                          Status *retStatus);

    // No handle is acquired here. It's up to the caller to ensure that
    // this is not yanked out when being used by grabbing a handle upstream.
    MustCheck DsDataset *getDatasetFromName(const char *name,
                                            Status *retStatus);

    MustCheck DsDatasetId getDatasetIdFromName(const char *name,
                                               Status *retStatus);

    // Caller is responsible for freeing retName.
    MustCheck Status getDatasetNameFromId(DsDatasetId datasetId,
                                          char **retName,
                                          size_t *retSize);

    // Caller is responsible for freeing datasetInfoOut
    MustCheck Status getDatasetInfoFromId(
        DsDatasetId datasetId, XcalarApiDatasetsInfo **datasetInfoOut);

    // Number of bytes used on this node by datasets
    MustCheck Status getDatasetTotalBytesUsed(uint64_t *bytesUsed);

    static void initXdbLoadArgs(DfLoadArgs *loadArgs,
                                TupleValueDesc *valueDesc,
                                unsigned numFields,
                                char (*fieldNames)[DfMaxFieldNameLen + 1]);

    static void updateXdbLoadArgKey(DfLoadArgs *loadArgs,
                                    const char *keyName,
                                    int keyIndex,
                                    DfFieldType keyType);

    MustCheck Status createDatasetXdb(XcalarApiBulkLoadInput *bulkLoadInput);

    MustCheck Status
    validateLoadInput(const XcalarApiBulkLoadInput *bulkLoadInput);

    // Store the dataset definition in KvStore (persistence) and in libns
    // (cluster-wide availability).
    MustCheck Status createDatasetMeta(XcalarApiBulkLoadInput *bulkLoadInput);

    // Update kvs and libNs for dataset dsName, with its metadata in queryStr
    // Return true in "dsPersisted" if the call resulted in a new creation of
    // the dataset meta in the kvs (independent of success or failure return)
    MustCheck Status persistAndPublish(const char *dsName,
                                       const char *queryStr,
                                       bool *dsPersisted);

    // Get the dataset meta and return as BulkLoad Api input
    MustCheck Status getDatasetMetaAsApiInput(
        const char *datasetName, XcalarApiBulkLoadInput **bulkLoadInputOut);

    // Get the dataset meta and return as a json string as API output
    MustCheck Status
    getDatasetMetaAsStr(const XcalarApiDatasetGetMetaInput *datasetGetMetaInput,
                        XcalarApiOutput **outputOut,
                        size_t *outputSizeOut);

    // Remove the  dataset definition from xlrroot and libns.
    MustCheck Status
    deleteDatasetMeta(const XcalarApiDatasetDeleteInput *datasetDeleteInput);

    // Caller get's a ReadWrite handle returned when a Dataset is successfully
    // loaded. This makes sure if a bulk delete comes in, the loaded Dataset
    // still exists.
    MustCheck Status loadDataset(LoadDatasetInput *loadDatasetInput,
                                 XcalarApiUserId *user,
                                 DsDataset **dataset,
                                 DatasetRefHandle *refHandle);

    // Update the dataset metadata after the load has completed data-wise.
    MustCheck Status finalizeDataset(DsDatasetId datasetId,
                                     LibNsTypes::NsHandle nsHandle);

    // Unload the dataset from memory.  The dataset definition remains in
    // xlrroot and libns.
    MustCheck Status unloadDataset(DsDataset *dataset);

    MustCheck Status unloadDatasetByName(const char *name);

    MustCheck Status unloadDatasetById(DsDatasetId datasetId);

    // Open a read only handle to a dataset by name. The caller must ensure
    // that the handle is closed.
    MustCheck Status openHandleToDatasetByName(const char *name,
                                               XcalarApiUserId *user,
                                               DsDataset **datasetOut,
                                               LibNsTypes::NsOpenFlags openFlag,
                                               DatasetRefHandle *dsRefHandle);

    // Open a read only handle to a dataset by Id. The caller must ensure that
    // the handle is closed.
    MustCheck Status openHandleToDatasetById(DsDatasetId datasetId,
                                             XcalarApiUserId *user,
                                             DsDataset **datasetOut,
                                             LibNsTypes::NsOpenFlags openFlag,
                                             DatasetRefHandle *dsRefHandleOut);

    // Close a handle to a dataset.
    MustCheck Status closeHandleToDataset(DatasetRefHandle *dsRefHandle);

    MustCheck Status unloadDatasets(const char *datasetNamePattern,
                                    XcalarApiOutput **outputOut,
                                    size_t *outputSizeOut);

    // List the datasets in a timely manner.
    MustCheck Status listDatasets(XcalarApiOutput **outputOut,
                                  size_t *outputSizeOut);

    // List the datasets and information that may be time consuming to obtain.
    MustCheck Status getDatasetsInfo(const char *namePattern,
                                     XcalarApiOutput **outputOut,
                                     size_t *outputSizeOut);

    MustCheck Status updateLoadStatus(const char *name,
                                      LibNsTypes::NsHandle nsHandle,
                                      DsLoadStatus loadStatus);

    MustCheck Status listDatasetUsers(const char *datasetName,
                                      XcalarApiOutput **outputOut,
                                      size_t *outputSizeOut);

    MustCheck Status listUserDatasets(const char *userIdName,
                                      XcalarApiOutput **outputOut,
                                      size_t *outputSizeOut);

    MustCheck Status updateLicensedValues();

    // Functions called by 2pc infrastructure
    void dlmDatasetReferenceMsg(MsgEphemeral *eph, void *payload);
    void dlmDatasetReferenceCompletion(MsgEphemeral *eph, void *payload);
    void getDatasetInfoLocal(MsgEphemeral *eph, void *payload);
    void getDatasetInfoCompletion(MsgEphemeral *eph, void *payload);

  private:
    static constexpr uint64_t NumDsHashSlots = 257;
    static constexpr uint64_t SchemaTableHashSlots = 257;
    // The largest message here is the combined output from each
    // of the individual nodes.
    static constexpr size_t InputOutputLimit =
        MaxNodes * sizeof(XcalarApiDatasetsInfo);

    struct SchemaEntry {
        static constexpr const uint64_t BigPrime = 109297270343;
        inline uint64_t hashIdentifier() const
        {
            return hashNameAndType(this->fieldName,
                                   this->fieldNameLen,
                                   this->type);
        }

        static inline uint64_t hashNameAndType(const char *fieldName,
                                               int32_t fieldNameLen,
                                               DfFieldType type)
        {
            uint64_t initial =
                hashBufFast((const uint8_t *) fieldName, fieldNameLen);
            uint64_t typeVal = (uint64_t) type * BigPrime;
            return initial ^ typeVal;
        }

        void del() { delete this; }

        SchemaEntry(const char *fieldNameArg,
                    int32_t fieldNameLenArg,
                    DfFieldType fieldTypeArg)
            : fieldName(fieldNameArg),
              fieldNameLen(fieldNameLenArg),
              type(fieldTypeArg)

        {
        }

        const char *fieldName;
        int32_t fieldNameLen;
        DfFieldType type;
        IntHashTableHook hook;
    };

    typedef IntHashTable<uint64_t,
                         SchemaEntry,
                         &SchemaEntry::hook,
                         &SchemaEntry::hashIdentifier,
                         SchemaTableHashSlots,
                         hashIdentity>
        SchemaTable;

    class DataRecord final : public NsObject
    {
      public:
        DataRecord(DsLoadStatus loadStatus)
            : NsObject(sizeof(DataRecord)),
              loadStatus_(loadStatus),
              datasetSize_(0),
              numColumns_(0)
        {
        }
        ~DataRecord() {}
        DsLoadStatus loadStatus_;
        // Save the dataset size once it's done loading.
        uint64_t datasetSize_;
        int64_t totalNumErrors_;
        bool downSampled_;
        // Save the column names once the dataset is loaded.
        uint64_t numColumns_ = 0;
        XcalarApiColumnInfo columns_[TupleMaxNumValuesPerRecord];

      private:
        DataRecord(const DataRecord &) = delete;
        DataRecord &operator=(const DataRecord &) = delete;
    };

    // Exists in libns to make dataset meta available cluster-wide and to
    // protect the data from being deleted while being used.
    class DsMetaRecord final : public NsObject
    {
      public:
        DsMetaRecord() : NsObject(sizeof(DsMetaRecord))
        {
            cachedDsMeta_[0] = '\0';
        }
        ~DsMetaRecord() {}
        // Max sized dataset meta that can be cached in libns.
        MustCheck size_t maxCachedDsMetaSize() { return MaxCachedDsMetaSize; };
        // Specifies the dataset meta to cache
        MustCheck Status setDsMeta(const char *dsMeta)
        {
            int ret;
            ret = strlcpy(cachedDsMeta_, dsMeta, sizeof(cachedDsMeta_));
            if (ret >= (int) sizeof(cachedDsMeta_)) {
                return StatusOverflow;
            }
            return StatusOk;
        }
        // Returns the size of the cached dataset meta
        MustCheck size_t getCachedDsMetaSize() { return strlen(cachedDsMeta_); }
        // Copies the dataset meta into the supplied buffer
        MustCheck Status getDsMeta(char *buf, size_t bufSize)
        {
            int ret;
            ret = strlcpy(buf, cachedDsMeta_, bufSize);
            if (ret >= (int) bufSize) {
                return StatusOverflow;
            }
            return StatusOk;
        }

      private:
        static constexpr size_t MaxCachedDsMetaSize = 1 * MB;
        char cachedDsMeta_[MaxCachedDsMetaSize + 1];
        DsMetaRecord(const DsMetaRecord &) = delete;
        DsMetaRecord &operator=(const DsMetaRecord &) = delete;
    };

    static Dataset *instance;
    Mutex datasetHTLock_;
    IntHashTable<DsDatasetId,
                 DsDataset,
                 &DsDataset::intHook_,
                 &DsDataset::getDatasetId,
                 NumDsHashSlots,
                 hashIdentity>
        datasetIdHashTable_;
    typedef StringHashTable<DsDataset,
                            &DsDataset::stringHook_,
                            &DsDataset::getDatasetName,
                            NumDsHashSlots>
        DatasetNameHashTable;
    DatasetNameHashTable datasetNameHashTable_;

    uint64_t numDatasets_ = 0;

    Dataset() {}
    ~Dataset() {}
    Dataset(const Dataset &) = delete;
    Dataset &operator=(const Dataset &) = delete;

    // Misc functions
    MustCheck Status addDatasetReference(DsDatasetId datasetId,
                                         const char *userIdName);

    MustCheck Status deleteDatasetReference(DsDatasetId datasetId,
                                            const char *userIdName);

    MustCheck Status persistDatasetMeta(const char *datasetName,
                                        const char *datasetMeta,
                                        bool *firstAdd);
    MustCheck Status readDatasetMeta(const char *datasetName,
                                     char **datasetMetaOut);
    MustCheck Status unpersistDatasetMeta(const char *datasetName);
    MustCheck Status getDatasetMeta(const char *datasetName,
                                    char **datasetMetaOut);
    MustCheck Status publishDatasetMeta(const char *datasetName,
                                        const char *datasetMeta);

    // GVM functions
    MustCheck Status createDatasetStructLocal(void *args);

    MustCheck Status unloadDatasetLocal(void *args);

    MustCheck Status finalizeDatasetLocal(void *args, size_t *outputSizeOut);

    // TwoPc functions
    MustCheck Status addDatasetReferenceLocal(void *payload);

    MustCheck Status listDatasetReferenceLocal(void *payload,
                                               void **retUsers,
                                               size_t *retUsersSize);

    MustCheck Status deleteDatasetReferenceLocal(void *payload);

    //  Other functions

    MustCheck Status populateXcalarApiDataset(DsDatasetId datasetId,
                                              XcalarApiDataset *apiDataset);

    MustCheck Status statsToInit();

    MustCheck Status addDatasetReferenceHelper(DsDatasetId datasetId,
                                               const char *userIdName);

    MustCheck Status deleteDatasetReferenceHelper(DsDatasetId datasetId,
                                                  const char *userIdName);

    MustCheck Status listDatasetUsersHelper(const char *datasetName,
                                            XcalarApiDatasetUser **user,
                                            uint64_t *userCount);

    MustCheck Status listDatasetUsersLocalHelper(DsDatasetId datasetId,
                                                 void **retUsers,
                                                 size_t *retUsersSize);

    // Returns an array of all the dataset IDs whose name matches the pattern.
    // The caller is responsible for freeing the memory.
    MustCheck Status getDatasetIds(const char *pattern,
                                   DsDatasetId **dsIdsOut,
                                   uint64_t *dsCountOut);

    MustCheck Status unloadDatasetHelper(const char *datasetName,
                                         DsDatasetId datasetId);

    // Create loaded dataset name as used by libNs
    MustCheck Status datasetNsName(const char *datasetName,
                                   char *datasetNsName,
                                   size_t maxDatasetNsName);

    // Create dataset meta name as used by libNs
    MustCheck Status datasetMetaNsName(const char *datasetName,
                                       char *datasetMetaNsName,
                                       size_t maxDatasetMetaNsName);

    // Create dataset meta name as used by KvStore
    MustCheck Status datasetMetaKvsName(const char *datasetName,
                                        char *datasetMetaKvsName,
                                        size_t maxDatasetMetaKvsName);
};

class TwoPcMsg2pcDatasetReference1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcDatasetReference1() {}
    virtual ~TwoPcMsg2pcDatasetReference1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcDatasetReference1(const TwoPcMsg2pcDatasetReference1 &) = delete;
    TwoPcMsg2pcDatasetReference1(const TwoPcMsg2pcDatasetReference1 &&) =
        delete;
    TwoPcMsg2pcDatasetReference1 &operator=(
        const TwoPcMsg2pcDatasetReference1 &) = delete;
    TwoPcMsg2pcDatasetReference1 &operator=(
        const TwoPcMsg2pcDatasetReference1 &&) = delete;
};

#endif  // _DATASET_H_
