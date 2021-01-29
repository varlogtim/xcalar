// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/stat.h>
#include <inttypes.h>

#include "StrlFunc.h"
#include "strings/String.h"
#include "primitives/Primitives.h"
#include "dataset/Dataset.h"
#include "ns/LibNs.h"
#include "df/DataFormat.h"
#include "util/WorkQueue.h"
#include "msg/Message.h"
#include "util/Base64.h"
#include "DatasetInt.h"
#include "udf/UserDefinedFunction.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "xdb/Xdb.h"
#include "runtime/Semaphore.h"
#include "constants/XcalarConfig.h"
#include "transport/TransportPage.h"
#include "util/FileUtils.h"
#include "dataset/LibDatasetGvm.h"
#include "gvm/Gvm.h"
#include "msg/Xid.h"
#include "libapis/LibApisRecv.h"
#include "libapis/OperatorHandler.h"
#include "libapis/WorkItem.h"
#include "operators/OperatorsXdbPageOps.h"
#include "datapage/DataPageIndex.h"
#include "datapage/DataPage.h"
#include "util/License.h"
#include "queryparser/QueryParser.h"
#include "log/Log.h"
#include "util/FileUtils.h"
#include "kvstore/KvStore.h"
#include "util/Vector.h"

static constexpr const char *moduleName = "libds";

Dataset *Dataset::instance = NULL;

Status  // static
Dataset::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) Dataset;
    if (instance == NULL) {
        return StatusNoMem;
    }

    Status status = StatusUnknown;
    status = instance->statsToInit();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = LibDatasetGvm::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        instance->destroy();
    }

    return status;
}

void
Dataset::destroy()
{
    if (LibDatasetGvm::get()) {
        LibDatasetGvm::get()->destroy();
    }

    datasetHTLock_.lock();
    datasetIdHashTable_.removeAll(NULL);
    datasetNameHashTable_.removeAll(&DsDataset::doDelete);
    datasetHTLock_.unlock();

    delete instance;
    instance = NULL;
}

Dataset *
Dataset::get()
{
    return instance;
}

// Assumed that callers already have a handle. Otherwise, there is no guarantee
// on the DsDataset returned.
DsDataset *
Dataset::getDatasetFromId(DsDatasetId datasetId, Status *retStatus)
{
    DsDataset *dataset;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::getDatasetFromId get dataset from Id %ld",
            datasetId);
#endif  // DATASET_DEBUG

    datasetHTLock_.lock();
    dataset = datasetIdHashTable_.find(datasetId);
    datasetHTLock_.unlock();

    if (dataset != NULL) {
        *retStatus = StatusOk;
    } else {
        *retStatus = StatusDsNotFound;
        xSyslog(moduleName,
                XlogInfo,
                "Failed to get dataset from Id %ld: %s",
                datasetId,
                strGetFromStatus(*retStatus));
    }
    return dataset;
}

// Assumed that callers already have a handle. Otherwise, there is no guarantee
// on the DsDataset returned.
DsDataset *
Dataset::getDatasetFromName(const char *name, Status *retStatus)
{
    DsDataset *dataset;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::getDatasetFromName get dataset from name %s",
            name);
#endif  // DATASET_DEBUG

    datasetHTLock_.lock();
    dataset = datasetNameHashTable_.find(name);
    datasetHTLock_.unlock();

    if (dataset != NULL) {
        *retStatus = StatusOk;
    } else {
        xSyslog(moduleName,
                XlogInfo,
                "Failed to get dataset from name %s: %s",
                name,
                strGetFromStatus(*retStatus));
        *retStatus = StatusDsNotFound;
    }
    return dataset;
}

DsDatasetId
Dataset::getDatasetIdFromName(const char *name, Status *retStatus)
{
    DsDataset *dataset;
    DsDatasetId retDatasetId = XidInvalid;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::getDatasetIdFromName name %s",
            name);
#endif  // DATASET_DEBUG

    datasetHTLock_.lock();
    dataset = datasetNameHashTable_.find(name);
    if (dataset != NULL) {
        retDatasetId = dataset->getDatasetId();
    }
    datasetHTLock_.unlock();

    if (dataset != NULL) {
        *retStatus = StatusOk;
    } else {
        *retStatus = StatusDsNotFound;
    }
    return retDatasetId;
}

// Caller is responsible for freeing retName.
Status
Dataset::getDatasetNameFromId(DsDatasetId datasetId,
                              char **retName,
                              size_t *retSize)
{
    DsDataset *dataset;
    int ret;
    Status status;
    *retName = NULL;
    *retSize = 0;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::getDatasetnameFromId Id %ld",
            datasetId);
#endif  // DATASET_DEBUG

    *retName = (char *) memAlloc(LibNsTypes::MaxPathNameLen);
    if (*retName == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset name from Id %ld: %s",
                datasetId,
                strGetFromStatus(status));
        return status;
    }

    datasetHTLock_.lock();
    dataset = datasetIdHashTable_.find(datasetId);
    if (dataset != NULL) {
        ret = snprintf(*retName,
                       LibNsTypes::MaxPathNameLen,
                       "%s",
                       dataset->name_);
        if (ret >= (int) LibNsTypes::MaxPathNameLen) {
            status = StatusNameTooLong;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get dataset name from Id %ld: %s",
                    datasetId,
                    strGetFromStatus(status));
        } else {
            status = StatusOk;
        }
    } else {
        status = StatusDsNotFound;
        xSyslog(moduleName,
                XlogInfo,
                "Failed to get dataset name from Id %ld: %s",
                datasetId,
                strGetFromStatus(status));
    }
    datasetHTLock_.unlock();

    if (status != StatusOk) {
        if (*retName != NULL) {
            memFree(*retName);
            *retName = NULL;
        }
    } else {
        *retSize = LibNsTypes::MaxPathNameLen;
    }
    return status;
}

Status
Dataset::getDatasetInfoFromId(DsDatasetId datasetId,
                              XcalarApiDatasetsInfo **datasetInfoOut)
{
    Status status = StatusOk;
    DsDataset *dataset;
    size_t datasetSize = 0;
    char datasetName[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    DataRecord *dataRecord = NULL;
    XcalarApiDatasetsInfo *datasetsInfo = NULL;
    size_t outSize;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::getDatasetInfoFromId Id %ld",
            datasetId);
#endif  // DATASET_DEBUG

    outSize = sizeof(*datasetsInfo) +
              TupleMaxNumValuesPerRecord * sizeof(datasetsInfo->columns[0]);

    datasetsInfo = (XcalarApiDatasetsInfo *) memAllocExt(outSize, moduleName);
    if (datasetsInfo == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // First look to see if we already have the info. We can tell this if we
    // have the size.  If not then we'll have to get it from libNs (and then,
    // of course, stash it away for next time).

    datasetHTLock_.lock();
    dataset = datasetIdHashTable_.find(datasetId);
    if (dataset == NULL) {
        datasetHTLock_.unlock();
        status = StatusDsNotFound;
        xSyslog(moduleName,
                XlogInfo,
                "Failed to get dataset from id %ld: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    datasetSize = dataset->datasetSize_;
    strlcpy(datasetName, dataset->name_, sizeof(datasetName));
    datasetHTLock_.unlock();

    if (datasetSize == 0) {
        // Don't have the info cached.  Get it from libNs.
        nsHandle = libNs->open(datasetId,
                               LibNsTypes::ReaderShared,
                               (NsObject **) &dataRecord,
                               &status);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to open '%s' to get dataset size: %s",
                    datasetName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        status = libNs->close(nsHandle, NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s' after getting dataset size: %s",
                    datasetName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // Stash the info away so we don't have to ask libNs the
        // next time.  No guarantee that it's still there though.
        datasetHTLock_.lock();
        dataset = datasetIdHashTable_.find(datasetId);
        if (dataset != NULL) {
            dataset->downSampled_ = dataRecord->downSampled_;
            dataset->datasetSize_ = dataRecord->datasetSize_;
            dataset->numColumns_ = dataRecord->numColumns_;
            dataset->finalTotalNumErrors_ = dataRecord->totalNumErrors_;
            for (unsigned ii = 0; ii < dataRecord->numColumns_; ii++) {
                verifyOk(strStrlcpy(dataset->columns_[ii].name,
                                    dataRecord->columns_[ii].name,
                                    sizeof(dataset->columns_[ii].name)));
                dataset->columns_[ii].type = dataRecord->columns_[ii].type;
            }
        }
        datasetHTLock_.unlock();
    }

    // If the info is available (won't be until load is complete) then
    // it'll now be cached in the local dataset.

    datasetHTLock_.lock();
    dataset = datasetIdHashTable_.find(datasetId);
    if (dataset == NULL) {
        datasetHTLock_.unlock();
        status = StatusDsNotFound;
        goto CommonExit;
    }

    datasetsInfo->datasetSize = dataset->datasetSize_;
    strlcpy(datasetsInfo->datasetName,
            dataset->name_,
            sizeof(datasetsInfo->datasetName));
    datasetsInfo->numColumns = dataset->numColumns_;
    datasetsInfo->downSampled = dataset->downSampled_;
    datasetsInfo->totalNumErrors = dataset->finalTotalNumErrors_;
    for (unsigned ii = 0; ii < dataset->numColumns_; ii++) {
        verifyOk(strStrlcpy(datasetsInfo->columns[ii].name,
                            dataset->columns_[ii].name,
                            sizeof(datasetsInfo->columns[ii].name)));
        datasetsInfo->columns[ii].type = dataset->columns_[ii].type;
    }

    datasetHTLock_.unlock();

    *datasetInfoOut = datasetsInfo;

CommonExit:

    if (status != StatusOk) {
        if (datasetsInfo != NULL) {
            memFree(datasetsInfo);
            datasetsInfo = NULL;
        }
    }

    if (dataRecord != NULL) {
        memFree(dataRecord);
        dataRecord = NULL;
    }

    return status;
}

void
Dataset::initXdbLoadArgs(DfLoadArgs *loadArgs,
                         TupleValueDesc *valueDesc,
                         unsigned numFields,
                         char (*fieldNames)[DfMaxFieldNameLen + 1])
{
    loadArgs->xdbLoadArgs.valueDesc = *valueDesc;
    loadArgs->xdbLoadArgs.fieldNamesCount = numFields;
    loadArgs->xdbLoadArgs.dstXdbId = XdbIdInvalid;

    for (unsigned ii = 0; ii < numFields; ii++) {
        strlcpy(loadArgs->xdbLoadArgs.fieldNames[ii],
                fieldNames[ii],
                sizeof(fieldNames[ii]));
    }
    loadArgs->xdbLoadArgs.keyType = DfUnknown;
    loadArgs->xdbLoadArgs.keyName[0] = '\0';
    loadArgs->xdbLoadArgs.evalString[0] = '\0';
    loadArgs->xdbLoadArgs.keyIndex = NewTupleMeta::DfInvalidIdx;
}

void
Dataset::updateXdbLoadArgKey(DfLoadArgs *loadArgs,
                             const char *keyName,
                             int keyIndex,
                             DfFieldType keyType)
{
    strlcpy(loadArgs->xdbLoadArgs.keyName,
            keyName,
            sizeof(loadArgs->xdbLoadArgs.keyName));
    loadArgs->xdbLoadArgs.keyIndex = keyIndex;
    loadArgs->xdbLoadArgs.keyType = keyType;
}

Status
Dataset::createDatasetXdb(XcalarApiBulkLoadInput *bulkLoadInput)
{
    unsigned numFields = bulkLoadInput->loadArgs.xdbLoadArgs.fieldNamesCount;
    assert(numFields > 0);
    DhtId dhtId;
    Status status;
    NewTupleMeta tupMeta;
    bool xdbCreated = false;
    XdbMgr *xdbMgr = XdbMgr::get();

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::createDatasetXdb %s",
            bulkLoadInput->datasetName);
#endif  // DATASET_DEBUG

    char **immediateNamesPtrs = NULL;
    char *cur = NULL;

    char *immediateNamesBuf =
        (char *) memAlloc(numFields * (XcalarApiMaxFieldNameLen + 1));
    BailIfNull(immediateNamesBuf);

    cur = immediateNamesBuf;

    immediateNamesPtrs = (char **) memAlloc(numFields * sizeof(char *));
    BailIfNull(immediateNamesPtrs);

    int ret;

    if (strstr(bulkLoadInput->loadArgs.xdbLoadArgs.keyName,
               DsDefaultDatasetKeyName)) {
        status = DhtMgr::get()->dhtGetDhtId(DhtMgr::DhtSystemRandomDht, &dhtId);
        BailIfFailed(status);
    } else {
        status =
            DhtMgr::get()->dhtGetDhtId(DhtMgr::DhtSystemUnorderedDht, &dhtId);
        BailIfFailed(status);
    }

    bulkLoadInput->loadArgs.xdbLoadArgs.dstXdbId = bulkLoadInput->dstXdbId;

    tupMeta.setNumFields(numFields);
    for (unsigned ii = 0; ii < numFields; ii++) {
        ret = strlcpy(cur,
                      bulkLoadInput->loadArgs.xdbLoadArgs.fieldNames[ii],
                      XcalarApiMaxFieldNameLen + 1);

        immediateNamesPtrs[ii] = cur;
        tupMeta.setFieldType(bulkLoadInput->loadArgs.xdbLoadArgs.valueDesc
                                 .valueType[ii],
                             ii);

        // + 1 for the null terminator
        cur += ret + 1;
    }

    status = xdbMgr->xdbCreate(bulkLoadInput->dstXdbId,
                               bulkLoadInput->loadArgs.xdbLoadArgs.keyName,
                               bulkLoadInput->loadArgs.xdbLoadArgs.keyType,
                               bulkLoadInput->loadArgs.xdbLoadArgs.keyIndex,
                               &tupMeta,
                               NULL,
                               0,
                               (const char **) immediateNamesPtrs,
                               numFields,
                               NULL,
                               0,
                               Unordered,
                               XdbGlobal,
                               dhtId);
    BailIfFailed(status);
    dhtId = DhtInvalidDhtId;
    xdbCreated = true;

CommonExit:
    if (status != StatusOk) {
        if (xdbCreated) {
            xdbMgr->xdbDrop(bulkLoadInput->dstXdbId);
        }
    }

    if (immediateNamesBuf != NULL) {
        memFree(immediateNamesBuf);
        immediateNamesBuf = NULL;
    }

    if (immediateNamesPtrs != NULL) {
        memFree(immediateNamesPtrs);
        immediateNamesPtrs = NULL;
    }

    return status;
}

Status
Dataset::validateLoadInput(const XcalarApiBulkLoadInput *bulkLoadInput)
{
    if (bulkLoadInput->loadArgs.maxSize < 0) {
        return StatusLoadArgsInvalid;
    }
    return StatusOk;
}

Status
Dataset::loadDataset(LoadDatasetInput *loadDatasetInput,
                     XcalarApiUserId *user,
                     DsDataset **datasetOut,
                     DatasetRefHandle *dsRefHandle)
{
    DsDataset *dataset = NULL;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    Status status = StatusOk;
    DsDatasetId datasetId = XidInvalid;
    bool datasetIdValid = false;
    bool dsHandleValid = false;
    Gvm::Payload *gPayload = NULL;
    LibDatasetGvm::CreateStructInfo *createStructInfo = NULL;
    DataRecord *dataRecord = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;

    if (datasetOut != NULL) {
        *datasetOut = NULL;
    }

    dataRecord = new (std::nothrow) DataRecord(DsLoadStatusNotStarted);
    BailIfNull(dataRecord);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    assert(user != NULL);

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::loadDataset %s",
            loadDatasetInput->bulkLoadInput.datasetName);
#endif  // DATASET_DEBUG

    status = datasetNsName(loadDatasetInput->bulkLoadInput.datasetName,
                           fullyQualName,
                           LibNsTypes::MaxPathNameLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed load dataset %s: %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    datasetId = libNs->publish(fullyQualName, dataRecord, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusInvalidDatasetName;
        } else if (status == StatusPendingRemoval || status == StatusExist) {
            status = StatusDatasetNameAlreadyExists;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed load dataset %s on NS publish: %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    datasetIdValid = true;

    dsRefHandle->nsHandle =
        libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusInvalidDatasetName;
        } else if (status == StatusAccess) {
            status = StatusDsDatasetInUse;
        } else if (status == StatusPendingRemoval ||
                   status == StatusNsNotFound) {
            status = StatusDatasetAlreadyDeleted;
        } else if (status == StatusNsNotFound) {
            status = StatusDsNotFound;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed load dataset %s on NS open: %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    dsHandleValid = true;
    dsRefHandle->datasetId = datasetId;
    strlcpy(dsRefHandle->userIdName,
            user->userIdName,
            sizeof(dsRefHandle->userIdName));

    // Create the data structure that will be used to manage the dataset.  The
    // same data structure will be created on each node of the cluster.  This
    // operation does not load the dataset.
    gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(LibDatasetGvm::CreateStructInfo));
    BailIfNull(gPayload);

    gPayload->init(LibDatasetGvm::get()->getGvmIndex(),
                   (uint32_t) LibDatasetGvm::Action::CreateStruct,
                   sizeof(LibDatasetGvm::CreateStructInfo));
    createStructInfo = (LibDatasetGvm::CreateStructInfo *) gPayload->buf;
    new (createStructInfo) LibDatasetGvm::CreateStructInfo();

    createStructInfo->input = *loadDatasetInput;
    createStructInfo->datasetId = datasetId;

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed load dataset %s on Gvm invoke: %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                strGetFromStatus(status));

        // Clean up after GVM failure
        if (gPayload != NULL) {
            memFree(gPayload);
            gPayload = NULL;
        }

        gPayload = (Gvm::Payload *) memAlloc(sizeof(DsDatasetId) +
                                             sizeof(Gvm::Payload));
        BailIfNull(gPayload);

        gPayload->init(LibDatasetGvm::get()->getGvmIndex(),
                       (uint32_t) LibDatasetGvm::Action::Unload,
                       sizeof(DsDatasetId));
        *(DsDatasetId *) gPayload->buf = datasetId;

        Status status2;
        status2 = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status2 == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status2 = nodeStatus[ii];
                    break;
                }
            }
        }
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed load dataset %s on clean out Gvm invoke: %s",
                    loadDatasetInput->bulkLoadInput.datasetName,
                    strGetFromStatus(status2));
        }
        goto CommonExit;
    }

    dataset = getDatasetFromId(datasetId, &status);
    assert(dataset != NULL && status == StatusOk && "guaranteed by contract");

    // Track the reference to the dataset.
    status = addDatasetReference(datasetId, dsRefHandle->userIdName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add dataset reference '%s', user '%s': %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                dsRefHandle->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (datasetOut != NULL) {
        *datasetOut = dataset;
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (status != StatusOk) {
        if (dsHandleValid) {
            Status status2 = libNs->close(dsRefHandle->nsHandle, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed load dataset %s on Ns close: %s",
                        loadDatasetInput->bulkLoadInput.datasetName,
                        strGetFromStatus(status2));
            }
        }
        if (datasetIdValid) {
            Status status2 = libNs->remove(datasetId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed load dataset %s on Ns remove: %s",
                        loadDatasetInput->bulkLoadInput.datasetName,
                        strGetFromStatus(status2));
            }
        }
    }
    if (dataRecord) {
        delete dataRecord;
        dataRecord = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
Dataset::unloadDatasetHelper(const char *datasetName, DsDatasetId datasetId)
{
    Status status;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    bool objDeleted = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    status =
        datasetNsName(datasetName, fullyQualName, LibNsTypes::MaxPathNameLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset %ld: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = libNs->remove(fullyQualName, &objDeleted);
    if (status != StatusOk) {
        if (status == StatusPendingRemoval) {
            status = StatusDatasetAlreadyUnloaded;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset %ld on NS remove: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (objDeleted == false) {
        goto CommonExit;
    }

    gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(DsDatasetId));
    BailIfNull(gPayload);

    gPayload->init(LibDatasetGvm::get()->getGvmIndex(),
                   (uint32_t) LibDatasetGvm::Action::Unload,
                   sizeof(DsDatasetId));
    *(DsDatasetId *) gPayload->buf = datasetId;

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset %ld on Gvm invoke: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
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
    return status;
}

Status
Dataset::unloadDataset(DsDataset *dataset)
{
    Status status;

#ifdef DATASET_DEBUG
    xSyslog(moduleName, XlogInfo, "Dataset::unloadDataset %s", dataset->name_);
#endif  // DATASET_DEBUG

    DsDatasetId id = dataset->datasetId_;
    status = unloadDatasetHelper(dataset->name_, dataset->datasetId_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset %ld: %s",
                id,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
Dataset::unloadDatasetByName(const char *name)
{
    Status status;
    DsDatasetId datasetId = XidInvalid;

#ifdef DATASET_DEBUG
    xSyslog(moduleName, XlogInfo, "Dataset::unloadDatasetByName %s", name);
#endif  // DATASET_DEBUG

    datasetId = getDatasetIdFromName(name, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset Id for '%s': %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = unloadDatasetHelper(name, datasetId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
Dataset::unloadDatasetById(DsDatasetId datasetId)
{
    Status status;
    char *datasetName = NULL;
    size_t datasetNameLen = 0;

#ifdef DATASET_DEBUG
    xSyslog(moduleName, XlogInfo, "Dataset::unloadDatasetById %ld", datasetId);
#endif  // DATASET_DEBUG

    status = getDatasetNameFromId(datasetId, &datasetName, &datasetNameLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset name for Id %ld: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = unloadDatasetHelper(datasetName, datasetId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset '%s' (Id %ld): %s",
                datasetName,
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (datasetName != NULL) {
        memFree(datasetName);
        datasetName = NULL;
    }

    return status;
}

Status
Dataset::finalizeDataset(DsDatasetId datasetId, LibNsTypes::NsHandle nsHandle)
{
    Status status = StatusOk;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    uint64_t *outputPerNode[MaxNodes];
    uint64_t sizePerNode[MaxNodes];
    uint64_t totalSize = 0;
    int64_t totalNumErrors = 0;
    LibNs *libNs = LibNs::get();
    DataRecord *dataRecord = NULL;
    bool datasetDownsampled = false;
    uint64_t numColumns = 0;

    SchemaTable schemaTable;
    SchemaEntry *newEntry = NULL;
    Vector<SchemaEntry *> schemaOrder;

#ifdef DATASET_DEBUG
    xSyslog(moduleName, XlogInfo, "Dataset::finalizeDataset %ld", datasetId);
#endif  // DATASET_DEBUG

    memZero(outputPerNode, sizeof(outputPerNode));
    memZero(sizePerNode, sizeof(sizePerNode));

    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(DsDatasetId));
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload->init(LibDatasetGvm::get()->getGvmIndex(),
                   (uint32_t) LibDatasetGvm::Action::Finalize,
                   sizeof(DsDatasetId));
    *(DsDatasetId *) gPayload->buf = datasetId;

    status = Gvm::get()->invokeWithOutput(gPayload,
                                          sizeof(XcalarApiDatasetsInfo),
                                          (void **) &outputPerNode,
                                          sizePerNode,
                                          nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed finalize dataset %ld on Gvm invoke: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        XcalarApiDatasetsInfo *datasetInfo = NULL;
        assert(sizePerNode[ii] >= sizeof(XcalarApiDatasetsInfo));
        assert(outputPerNode[ii] != NULL);

        datasetInfo = (XcalarApiDatasetsInfo *) outputPerNode[ii];
        totalSize += datasetInfo->datasetSize;
        totalNumErrors += datasetInfo->totalNumErrors;

        if (datasetInfo->downSampled) {
            datasetDownsampled = true;
        }

        // Combine all of the fields in our schema so we have a cohesive,
        // dataset-wide schema
        for (int jj = 0; jj < (int) datasetInfo->numColumns; jj++) {
            auto column = &datasetInfo->columns[jj];
            int32_t colLen = strlen(column->name);
            uint64_t hashedIdentity =
                SchemaEntry::hashNameAndType(column->name,
                                             colLen,
                                             column->type);
            SchemaEntry *entry = schemaTable.find(hashedIdentity);
            if (entry == NULL) {
                // This combo of field name and type have not been set yet;
                // let's keep track of them. We don't need to copy the string,
                // since our datasetInfo will be sticking around for now.
                newEntry = new (std::nothrow)
                    SchemaEntry(column->name, colLen, column->type);
                BailIfNull(newEntry);

                // We need to keep track of the schema order; the hash table
                // will mix this up for us. For now, we can just take fields in
                // the order that we observe them.
                // We also make sure to do this before inserting into the hash
                // table, since this is just borrowing the data, it doesn't own
                // it.
                status = schemaOrder.append(newEntry);
                BailIfFailed(status);

                status = schemaTable.insert(newEntry);
                BailIfFailed(status);
                newEntry = NULL;
            }
        }
    }

    // Update the libNs record with the size so it's globally available
    dataRecord = (DataRecord *) libNs->getNsObject(nsHandle);
    if (dataRecord == NULL) {
        status = StatusDsNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset record %ld size: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(dataRecord->loadStatus_ == DsLoadStatusComplete);
    assert(dataRecord->datasetSize_ == 0);

    dataRecord->datasetSize_ = totalSize;
    dataRecord->downSampled_ = datasetDownsampled;
    dataRecord->totalNumErrors_ = totalNumErrors;

    numColumns = schemaOrder.size();
    if (numColumns > TupleMaxNumValuesPerRecord) {
        numColumns = TupleMaxNumValuesPerRecord;
        xSyslog(moduleName,
                XlogWarn,
                "Truncating the number of columns in dataset "
                "record %ld from %d to %ld",
                datasetId,
                schemaOrder.size(),
                numColumns);
    }

    for (unsigned ii = 0; ii < numColumns; ii++) {
        const SchemaEntry *entry = schemaOrder.get(ii);
        strlcpy(dataRecord->columns_[ii].name,
                entry->fieldName,
                sizeof(dataRecord->columns_[ii].name));
        dataRecord->columns_[ii].type = entry->type;
    }
    dataRecord->numColumns_ = numColumns;

    nsHandle = libNs->updateNsObject(nsHandle, dataRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to update dataset record %ld size: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        if (outputPerNode[ii] != NULL) {
            memFree(outputPerNode[ii]);
        }
    }

    if (dataRecord != NULL) {
        memFree(dataRecord);
        dataRecord = NULL;
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (newEntry != NULL) {
        delete newEntry;
        newEntry = NULL;
    }

    schemaTable.removeAll(&SchemaEntry::del);

    return status;
}

Status
Dataset::addDatasetReferenceHelper(DsDatasetId datasetId,
                                   const char *userIdName)
{
    Status status = StatusOk;
    DsDataset *dataset;

    datasetHTLock_.lock();

    dataset = datasetIdHashTable_.find(datasetId);
    assert(dataset != NULL);

    // Keep track of users and number of references for the user

    DsDataset::DsReferenceUserEntry *userEntry =
        dataset->referenceUserTable_.find(userIdName);

    if (userEntry != NULL) {
        // User has an entry in the table.  Means they have multiple
        // references to the dataset.
        userEntry->referenceCount++;
    } else {
        // First reference for this user...allocate an entry.
        userEntry = new (std::nothrow) DsDataset::DsReferenceUserEntry();
        BailIfNull(userEntry);

        strlcpy(userEntry->userName, userIdName, sizeof(userEntry->userName));
        userEntry->referenceCount = 1;

        status = dataset->referenceUserTable_.insert(userEntry);
        assert(status == StatusOk);  // Should never fail

        dataset->userCount_++;
    }

CommonExit:

    datasetHTLock_.unlock();

    return status;
}

Status
Dataset::deleteDatasetReferenceHelper(DsDatasetId datasetId,
                                      const char *userIdName)
{
    Status status = StatusOk;
    DsDataset *dataset;
    DsDataset::DsReferenceUserEntry *userEntry = NULL;

    datasetHTLock_.lock();

    dataset = datasetIdHashTable_.find(datasetId);
    assert(dataset != NULL);

    userEntry = dataset->referenceUserTable_.find(userIdName);
    if (userEntry == NULL) {
        // It's possible that the user reference may have already been removed
        // if errors occurred during cleanout of an operation where some steps
        // had completed and others hadn't.
        goto CommonExit;
    }

    assert(userEntry->referenceCount > 0);
    userEntry->referenceCount--;

    if (userEntry->referenceCount == 0) {
        // Remove the user from the table and delete
        userEntry = dataset->referenceUserTable_.remove(userIdName);
        assert(userEntry != NULL);
        delete userEntry;

        dataset->userCount_--;
    }

CommonExit:

    datasetHTLock_.unlock();

    return status;
}

Status
Dataset::addDatasetReference(DsDatasetId datasetId, const char *userIdName)
{
    Status status = StatusOk;
    Config *config = Config::get();
    NodeId dlmNode = datasetId % config->getActiveNodes();
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();

    DsReferenceMsg referenceMsg;
    referenceMsg.op = AddReference;
    referenceMsg.datasetId = datasetId;
    verifyOk(strStrlcpy(referenceMsg.userIdName,
                        userIdName,
                        sizeof(referenceMsg.userIdName)));

    DsReferenceResponse referenceResponse;
    referenceResponse.op = AddReference;

    msgMgr->twoPcEphemeralInit(&eph,
                               &referenceMsg,
                               sizeof(referenceMsg),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDatasetReference1,
                               &referenceResponse,
                               (TwoPcBufLife)(TwoPcMemCopyInput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDatasetReference,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrOnly),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);
    BailIfFailed(status);

    assert(!twoPcHandle.twoPcHandle);
    status = referenceResponse.status;
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
Dataset::deleteDatasetReference(DsDatasetId datasetId, const char *userIdName)
{
    Status status = StatusOk;
    Config *config = Config::get();
    NodeId dlmNode = datasetId % config->getActiveNodes();

    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();

    DsReferenceMsg referenceMsg;
    referenceMsg.op = DeleteReference;
    referenceMsg.datasetId = datasetId;
    verifyOk(strStrlcpy(referenceMsg.userIdName,
                        userIdName,
                        sizeof(referenceMsg.userIdName)));

    DsReferenceResponse referenceResponse;
    referenceResponse.op = DeleteReference;

    msgMgr->twoPcEphemeralInit(&eph,
                               &referenceMsg,
                               sizeof(referenceMsg),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDatasetReference1,
                               &referenceResponse,
                               (TwoPcBufLife)(TwoPcMemCopyInput));

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcDatasetReference,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrOnly),
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  dlmNode,
                                  TwoPcClassNonNested);
    BailIfFailed(status);

    assert(!twoPcHandle.twoPcHandle);
    status = referenceResponse.status;
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
Dataset::listDatasetUsersHelper(const char *datasetName,
                                XcalarApiDatasetUser **user,
                                uint64_t *userCount)
{
    DsReferenceResponse referenceResponse;
    Status status = StatusOk;
    Config *config = Config::get();
    MsgMgr *msgMgr = MsgMgr::get();
    MsgEphemeral eph;
    *user = NULL;
    *userCount = 0;

    DsDatasetId datasetId = getDatasetIdFromName(datasetName, &status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    NodeId dlmNode;
    dlmNode = datasetId % config->getActiveNodes();
    TwoPcHandle twoPcHandle;

    DsReferenceMsg referenceMsg;
    memZero(&referenceMsg, sizeof(DsReferenceMsg));
    referenceMsg.op = ListReferences;
    referenceMsg.datasetId = datasetId;

    referenceResponse.op = ListReferences;

    msgMgr->twoPcEphemeralInit(&eph,
                               &referenceMsg,
                               sizeof(referenceMsg),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDatasetReference1,
                               &referenceResponse,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDatasetReference,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);
    BailIfFailed(status);

    assert(!twoPcHandle.twoPcHandle);
    status = referenceResponse.status;
    BailIfFailed(status);

    assert(referenceResponse.outputSize % sizeof(XcalarApiDatasetUser) == 0);
    *userCount = referenceResponse.outputSize / sizeof(XcalarApiDatasetUser);
    *user = (XcalarApiDatasetUser *) referenceResponse.output;

CommonExit:
    if (status == StatusNoDsUsers) {
        // This error occurs when the dataset has no users and the information
        // is located on a remote node (required twoPc).  Rather than force the
        // caller to handle the error we'll convert it to success and return
        // zero users.
        status = StatusOk;
    }
    return status;
}

Status
Dataset::listDatasetUsers(const char *datasetName,
                          XcalarApiOutput **outputOut,
                          size_t *outputSizeOut)
{
    Status status = StatusOk;
    XcalarApiDatasetUser *user = NULL;
    uint64_t userCount = 0;
    XcalarApiListDatasetUsersOutput *datasetUsersOutput = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;

    status = listDatasetUsersHelper(datasetName, &user, &userCount);
    if (status != StatusOk) {
        goto CommonExit;
    }

    outputSize = XcalarApiSizeOfOutput(*datasetUsersOutput) +
                 (userCount * sizeof(datasetUsersOutput->user[0]));
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    datasetUsersOutput = &output->outputResult.listDatasetUsersOutput;
    datasetUsersOutput->usersCount = userCount;

    for (unsigned ii = 0; ii < userCount; ii++) {
        strlcpy(datasetUsersOutput->user[ii].userId.userIdName,
                user[ii].userId.userIdName,
                sizeof(datasetUsersOutput->user[ii].userId.userIdName));
        datasetUsersOutput->user[ii].referenceCount = user[ii].referenceCount;
    }

CommonExit:

    *outputOut = output;
    *outputSizeOut = outputSize;

    if (user != NULL) {
        memFree(user);
        user = NULL;
    }

    return status;
}

Status
Dataset::listUserDatasets(const char *userIdName,
                          XcalarApiOutput **outputOut,
                          size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiListUserDatasetsOutput *userDatasetsOutput = NULL;
    size_t outputSize = 0;
    DsDatasetId *dsIds = NULL;
    uint64_t dsCount = 0;
    XcalarApiDatasetUser *user = NULL;
    uint64_t userCount = 0;
    unsigned outputCount = 0;

    // Get a list of all the datasets in the cluster at this moment.
    status = getDatasetIds("*", &dsIds, &dsCount);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset IDs: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (dsCount == 0) {
        status = StatusDsNotFound;
        goto CommonExit;
    }

    // We have a list of the datasets in the cluster but no guarantees
    // wrt additions or deletions.

    outputSize = XcalarApiSizeOfOutput(*userDatasetsOutput) +
                 (dsCount * sizeof(userDatasetsOutput->datasets[0]));
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    userDatasetsOutput = &output->outputResult.listUserDatasetsOutput;

    for (unsigned ii = 0; ii < dsCount; ii++) {
        char *datasetName = NULL;
        size_t datasetNameLen = 0;

        // Use the dataset name rather than the fully qualified pathname.
        // Remember there's no guarantee the dataset still is around.
        status = getDatasetNameFromId(dsIds[ii], &datasetName, &datasetNameLen);
        if (status != StatusOk) {
            continue;
        }
        status = listDatasetUsersHelper(datasetName, &user, &userCount);
        if (status != StatusOk) {
            memFree(datasetName);
            datasetName = NULL;
            continue;
        }
        strlcpy(userDatasetsOutput->datasets[outputCount].datasetName,
                datasetName,
                sizeof(userDatasetsOutput->datasets[outputCount].datasetName));
        userDatasetsOutput->datasets[outputCount].isLocked = false;

        for (unsigned jj = 0; jj < userCount; jj++) {
            if (strcmp(user[jj].userId.userIdName, userIdName) == 0) {
                // The user has a reference to the dataset
                userDatasetsOutput->datasets[outputCount].isLocked = true;
                break;
            }
        }
        outputCount++;
        memFree(datasetName);
        datasetName = NULL;
        memFree(user);
        user = NULL;
    }

    // Return the actual number of datasets that we found
    userDatasetsOutput->numDatasets = outputCount;

CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;

    if (dsIds != NULL) {
        memFree(dsIds);
        dsIds = NULL;
    }

    return status;
}

Status
Dataset::openHandleToDatasetByName(const char *name,
                                   XcalarApiUserId *user,
                                   DsDataset **datasetOut,
                                   LibNsTypes::NsOpenFlags openFlag,
                                   DatasetRefHandle *dsRefHandleOut)
{
    Status status = StatusUnknown;
    DsDataset *dataset = NULL;
    LibNsTypes::NsHandle nsHandle;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    bool datasetOpened = false;

    assert(user != NULL);

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::openHandleToDatasetByName %s",
            name);
#endif  // DATASET_DEBUG

    if (datasetOut != NULL) {
        *datasetOut = NULL;
    }

    status = datasetNsName(name, fullyQualName, LibNsTypes::MaxPathNameLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open handle to dataset %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsHandle = libNs->open(fullyQualName, openFlag, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusInvalidDatasetName;
        } else if (status == StatusAccess) {
            status = StatusDsDatasetInUse;
        } else if (status == StatusPendingRemoval) {
            status = StatusDatasetAlreadyDeleted;
        } else if (status == StatusNsNotFound) {
            status = StatusDsNotFound;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open handle to dataset %s on Ns open: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    datasetOpened = true;

    // Safe to get dataset, since we already have a handle now.
    dataset = getDatasetFromName(name, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset ID for %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Track this reference
    status = addDatasetReference(dataset->datasetId_, user->userIdName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add dataset reference '%s', user '%s': %s",
                name,
                user->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    dsRefHandleOut->nsHandle = nsHandle;
    dsRefHandleOut->datasetId = dataset->datasetId_;
    strlcpy(dsRefHandleOut->userIdName,
            user->userIdName,
            sizeof(dsRefHandleOut->userIdName));

    if (datasetOut != NULL) {
        *datasetOut = dataset;
    }

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (datasetOpened) {
            Status status2 = libNs->close(nsHandle, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to close dataset '%s', user '%s': %s",
                        name,
                        user->userIdName,
                        strGetFromStatus(status2));
            }
            datasetOpened = false;
        }
    }

    return status;
}

Status
Dataset::openHandleToDatasetById(DsDatasetId datasetId,
                                 XcalarApiUserId *user,
                                 DsDataset **datasetOut,
                                 LibNsTypes::NsOpenFlags openFlag,
                                 DatasetRefHandle *dsRefHandleOut)
{
    Status status = StatusUnknown;
    char *datasetName = NULL;
    size_t datasetNameLen = 0;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::openHandleToDatasetById %ld",
            datasetId);
#endif  // DATASET_DEBUG

    status = getDatasetNameFromId(datasetId, &datasetName, &datasetNameLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open handle to dataset %ld: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = openHandleToDatasetByName(datasetName,
                                       user,
                                       datasetOut,
                                       openFlag,
                                       dsRefHandleOut);
    if (status != StatusOk) {
        // Error already syslogged
        goto CommonExit;
    }

CommonExit:

    if (datasetName != NULL) {
        memFree(datasetName);
    }

    return status;
}

Status
Dataset::closeHandleToDataset(DatasetRefHandle *dsRefHandle)
{
    Status status;
    LibNs *libNs = LibNs::get();
    DsDataset *dataset = NULL;
    bool isObjDeleted = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::closeHandleToDataset %ld",
            dsRefHandle->nsHandle.nsId);
#endif  // DATASET_DEBUG

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    // Since we have a handle, by contract DsDataset lookup is fine.
    dataset = getDatasetFromId(dsRefHandle->nsHandle.nsId, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close handle to dataset %ld: %s",
                dsRefHandle->nsHandle.nsId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(dataset != NULL);

    // Remove the user's reference to the dataset
    status =
        deleteDatasetReference(dsRefHandle->datasetId, dsRefHandle->userIdName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close handle to dataset %ld on delete "
                "dataset reference: %s",
                dsRefHandle->nsHandle.nsId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = libNs->close(dsRefHandle->nsHandle, &isObjDeleted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close handle to dataset %ld on Ns close: %s",
                dsRefHandle->nsHandle.nsId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (isObjDeleted == true) {
        gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                             sizeof(DsDatasetId));
        BailIfNull(gPayload);

        gPayload->init(LibDatasetGvm::get()->getGvmIndex(),
                       (uint32_t) LibDatasetGvm::Action::Unload,
                       sizeof(DsDatasetId));
        *(DsDatasetId *) gPayload->buf = dsRefHandle->datasetId;

        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close handle to dataset %ld on Gvm invoke: %s",
                    dsRefHandle->nsHandle.nsId,
                    strGetFromStatus(status));
            goto CommonExit;
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
    return status;
}

Status
Dataset::unloadDatasets(const char *datasetNamePattern,
                        XcalarApiOutput **outputOut,
                        size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    size_t ii;
    size_t numDatasetsToUnload = 0;
    XcalarApiOutput *output = NULL;
    XcalarApiDatasetUnloadOutput *datasetUnloadOutput = NULL;
    size_t outputSize = 0;
    XcalarApiDataset *datasets = NULL;
    uint64_t dsCount = 0;
    DsDatasetId *dsIds = NULL;
    char datasetNamePatternWithPrefix[XcalarApiDsDatasetNameLen + 1];
    bool addNamePrefix = false;
    int ret;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::unloadDatasets %s",
            datasetNamePattern);
#endif  // DATASET_DEBUG

    if ((strncmp(datasetNamePattern,
                 XcalarApiDatasetPrefix,
                 XcalarApiDatasetPrefixLen) != 0) &&
        (strncmp(datasetNamePattern,
                 XcalarApiLrqPrefix,
                 XcalarApiLrqPrefixLen) != 0)) {
        // Need to add the xcalar dataset prefix
        addNamePrefix = true;
    }
    ret = snprintf(datasetNamePatternWithPrefix,
                   XcalarApiDsDatasetNameLen,
                   "%s%s",
                   (addNamePrefix ? XcalarApiDatasetPrefix : ""),
                   datasetNamePattern);
    if (ret >= (int) sizeof(datasetNamePatternWithPrefix)) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

    // Get a list of all the datasets in the cluster at this moment.
    status = getDatasetIds(datasetNamePatternWithPrefix, &dsIds, &dsCount);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset IDs: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (dsCount == 0) {
        status = StatusDsNotFound;
        goto CommonExit;
    }

    // We have a list of the datasets in the cluster but no guarantees
    // wrt additions or deletions.

    datasets =
        (XcalarApiDataset *) memAlloc(sizeof(XcalarApiDataset) * dsCount);
    if (datasets == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload datasets %s: %s",
                datasetNamePattern,
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(datasets, sizeof(XcalarApiDataset) * dsCount);

    for (ii = 0, numDatasetsToUnload = 0; ii < dsCount; ii++) {
        status =
            populateXcalarApiDataset(dsIds[ii], &datasets[numDatasetsToUnload]);
        if (status != StatusOk) {
            if (status != StatusDsNotFound) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to unload dataset %ld: %s",
                        dsIds[ii],
                        strGetFromStatus(status));
            }
            continue;
        }
        numDatasetsToUnload++;
    }

    outputSize =
        XcalarApiSizeOfOutput(*datasetUnloadOutput) +
        (sizeof(datasetUnloadOutput->statuses[0]) * numDatasetsToUnload);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload datasets %s: %s",
                datasetNamePattern,
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(output, outputSize);

    datasetUnloadOutput = &output->outputResult.datasetUnloadOutput;
    datasetUnloadOutput->numDatasets = numDatasetsToUnload;

    // If a dataset is pending removal (in libNs) we'll not be able to
    // remove it again in unloadDatasetById.  We include it in the results
    // but the statuses[ii].status will be StatusDatasetAlreadyUnloaded
    // (which is StatusPendingRemoval translated to a dataset error).
    for (ii = 0; ii < numDatasetsToUnload; ii++) {
        memcpy(&datasetUnloadOutput->statuses[ii].dataset,
               &datasets[ii],
               sizeof(XcalarApiDataset));
        status = unloadDatasetById(datasets[ii].datasetId);
        if (status == StatusOk) {
            datasetUnloadOutput->statuses[ii].dataset.isListable = false;
        } else if (status != StatusOk &&
                   datasetUnloadOutput->statuses[ii].status ==
                       StatusOk.code()) {
            datasetUnloadOutput->statuses[ii].status = status.code();
        }
    }

    status = StatusOk;

CommonExit:
    if (datasets != NULL) {
        memFree(datasets);
        datasets = NULL;
    }
    if (dsIds != NULL) {
        memFree(dsIds);
        dsIds = NULL;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
Dataset::listDatasets(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status;
    XcalarApiOutput *output = NULL;
    XcalarApiListDatasetsOutput *listDatasets = NULL;
    size_t outputSize = 0;
    size_t curDatasetIndex;
    DsDatasetId *dsIds = NULL;
    uint64_t dsCount = 0;
    LibNs *libNs = LibNs::get();
    int ret;
    char datasetMetaPattern[LibNsTypes::MaxPathNameLen];
    uint64_t numLoadedDatasets;
    uint64_t numDatasetMetas;
    LibNsTypes::PathInfoOut *pathInfo = NULL;

#ifdef DATASET_DEBUG
    xSyslog(moduleName, XlogInfo, "Dataset::listDatasets");
#endif  // DATASET_DEBUG

    // Get a list of all the datasets that are currently loaded in the cluster
    // at this moment.
    status = getDatasetIds("*", &dsIds, &dsCount);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset IDs: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    numLoadedDatasets = dsCount;

    // Get a list of all the datasets defined in the cluster (includes those
    // that are not currently loaded).
    ret = snprintf(datasetMetaPattern,
                   sizeof(datasetMetaPattern),
                   "%s*",
                   Dataset::DsMetaPrefix);
    assert(ret < (int) sizeof(datasetMetaPattern));
    numDatasetMetas = libNs->getPathInfo(datasetMetaPattern, &pathInfo);
    if (numDatasetMetas > 0) {
        char fullyQualName[LibNsTypes::MaxPathNameLen];
        LibNsTypes::NsId nsId;
        // Only want to add those that weren't included in the above
        // getDatasetIds() call.  We can tell this by trying to get the libNs Id
        // for the dataset associated with the dataset meta.  If the libNs Id is
        // available then the datset is loaded.  e.g. if the datasetMeta is:
        //      /datasetMeta/datasetNameAbc
        // then we'll try to get the libNs Id for
        //      /dataset/datasetNameAbc
        for (unsigned ii = 0; ii < numDatasetMetas; ii++) {
            // Skip past the prefix to the dataset name.  The -1 is for "*".
            char *startDsName =
                pathInfo[ii].pathName + strlen(datasetMetaPattern) - 1;
            status = datasetNsName(startDsName,
                                   fullyQualName,
                                   sizeof(fullyQualName));
            assert(status == StatusOk);
            BailIfFailed(status);
            nsId = libNs->getNsId(fullyQualName);
            if (nsId == LibNsTypes::NsInvalidId) {
                // Dataset isn't in the namespace so it can't be loaded.
                dsCount++;
            } else {
                // Dataset has a valid NsId so it's loaded.  Mark the pathInfo
                // entry invalid so we skip it in the returned results.
                pathInfo[ii].nsId = LibNsTypes::NsInvalidId;
            }
        }
    }

    // We have a list of the datasets in the cluster but no guarantees
    // wrt additions or deletions.

    outputSize = XcalarApiSizeOfOutput(*listDatasets) +
                 (dsCount * sizeof(listDatasets->datasets[0]));
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to list datasets: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Add the information for the loaded datasets

    listDatasets = &output->outputResult.listDatasetsOutput;
    listDatasets->numDatasets = dsCount;
    curDatasetIndex = 0;
    for (unsigned ii = 0; ii < numLoadedDatasets; ii++) {
        status =
            populateXcalarApiDataset(dsIds[ii],
                                     &listDatasets->datasets[curDatasetIndex]);
        if (status != StatusOk) {
            if (status != StatusDsNotFound) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Failed to list dataset %ld: %s",
                        dsIds[ii],
                        strGetFromStatus(status));
            }
            listDatasets->numDatasets--;
            continue;
        } else {
            curDatasetIndex++;
        }
    }

    // Addition for the unloaded datasets.
    for (unsigned ii = 0; ii < numDatasetMetas; ii++) {
        if (pathInfo[ii].nsId != LibNsTypes::NsInvalidId) {
            // Fill in minimal info for unloaded datasets.
            memset(&listDatasets->datasets[curDatasetIndex],
                   0,
                   sizeof(listDatasets->datasets[curDatasetIndex]));
            // Skip past the prefix to the dataset name.  The -1 is for "*".
            char *startDsName =
                pathInfo[ii].pathName + strlen(datasetMetaPattern) - 1;

            listDatasets->datasets[curDatasetIndex].datasetId =
                LibNsTypes::NsInvalidId;
            listDatasets->datasets[curDatasetIndex].loadIsComplete = false;
            listDatasets->datasets[curDatasetIndex].isListable = true;
            strlcpy(listDatasets->datasets[curDatasetIndex].name,
                    startDsName,
                    sizeof(listDatasets->datasets[curDatasetIndex].name));
            curDatasetIndex++;
        }
    }
    assert(curDatasetIndex == listDatasets->numDatasets);

    status = StatusOk;

CommonExit:

#ifdef DEBUG
    if (status == StatusOk) {
        assert(output != NULL && outputSize > 0);
    } else {
        assert(output == NULL);
    }
#endif
    *outputOut = output;
    *outputSizeOut = outputSize;

    if (dsIds != NULL) {
        memFree(dsIds);
        dsIds = NULL;
    }
    if (pathInfo != NULL) {
        memFree(pathInfo);
        pathInfo = NULL;
    }

    return status;
}

Status
Dataset::getDatasetsInfo(const char *datasetsNamePattern,
                         XcalarApiOutput **outputOut,
                         size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiGetDatasetsInfoOutput *getDatasetsInfoOutput = NULL;
    XcalarApiDatasetsInfo *datasetInfo = NULL;
    size_t outputSize = 0;
    uint64_t outputCount = 0;
    DsDatasetId *dsIds = NULL;
    uint64_t dsCount = 0;

    // Get a list of all the datasets in the cluster at this moment.
    status = getDatasetIds(datasetsNamePattern, &dsIds, &dsCount);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset IDs: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (dsCount == 0) {
        status = StatusDsNotFound;
        goto CommonExit;
    }

    // We have a list of the datasets in the cluster but no guarantees
    // wrt additions or deletions that occur while processing this
    // request.

    outputSize = XcalarApiSizeOfOutput(*getDatasetsInfoOutput) +
                 dsCount * sizeof(*datasetInfo);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    getDatasetsInfoOutput = &output->outputResult.getDatasetsInfoOutput;

    for (unsigned ii = 0; ii < dsCount; ii++) {
        char *datasetName = NULL;
        size_t datasetNameLen = 0;

        // If we get an error for a dataset we'll plow on and process
        // the rest.

        // Get the dataset name separately from the rest of the info as
        // we can always get the name locally.
        status = getDatasetNameFromId(dsIds[ii], &datasetName, &datasetNameLen);
        if (status != StatusOk) {
            continue;
        }

        // Get the rest of the dataset info.  The info will not be
        // available if the dataset is still loading.
        status = getDatasetInfoFromId(dsIds[ii], &datasetInfo);
        if (status != StatusOk) {
            if (status == StatusAccess) {
                // It's likely the dataset is being loaded.  No
                // datasetInfo was returned but we want to include this
                // dataset in the results.
                status = StatusOk;
            } else {
                // No info to report, skip it.
                memFree(datasetName);
                datasetName = NULL;
                continue;
            }
        }
        memZero(&getDatasetsInfoOutput->datasets[outputCount],
                sizeof(getDatasetsInfoOutput->datasets[0]));
        strlcpy(getDatasetsInfoOutput->datasets[outputCount].datasetName,
                datasetName,
                sizeof(
                    getDatasetsInfoOutput->datasets[outputCount].datasetName));
        if (datasetInfo) {
            getDatasetsInfoOutput->datasets[outputCount].datasetSize =
                datasetInfo->datasetSize;
            getDatasetsInfoOutput->datasets[outputCount].downSampled =
                datasetInfo->downSampled;
            getDatasetsInfoOutput->datasets[outputCount].numColumns =
                datasetInfo->numColumns;
            getDatasetsInfoOutput->datasets[outputCount].totalNumErrors =
                datasetInfo->totalNumErrors;
            for (unsigned kk = 0; kk < datasetInfo->numColumns; kk++) {
                strlcpy(getDatasetsInfoOutput->datasets[outputCount]
                            .columns[kk]
                            .name,
                        datasetInfo->columns[kk].name,
                        sizeof(getDatasetsInfoOutput->datasets[outputCount]
                                   .columns[kk]
                                   .name));
                getDatasetsInfoOutput->datasets[outputCount].columns[kk].type =
                    datasetInfo->columns[kk].type;
            }
        }

        outputCount++;
        memFree(datasetName);
        datasetName = NULL;
        if (datasetInfo != NULL) {
            memFree(datasetInfo);
            datasetInfo = NULL;
        }
    }

    // Return the actual number of datasets that we found
    getDatasetsInfoOutput->numDatasets = outputCount;

CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;

    if (dsIds != NULL) {
        memFree(dsIds);
        dsIds = NULL;
    }

    return status;
}

Status
Dataset::updateLoadStatus(const char *name,
                          LibNsTypes::NsHandle nsHandle,
                          DsLoadStatus loadStatus)
{
    DataRecord *dataRecord = NULL;
    LibNs *libNs = LibNs::get();
    Status status;

#ifdef DATASET_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "Dataset::updateLoadStatus name %s nsHandle %ld loadStatus "
            "%u",
            name,
            nsHandle.nsId,
            loadStatus);
#endif  // DATASET_DEBUG

    dataRecord = (DataRecord *) libNs->getNsObject(nsHandle);
    if (dataRecord == NULL) {
        status = StatusDsNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed to update load status %u for dataset %s on"
                " get Ns object: %s",
                loadStatus,
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (loadStatus == DsLoadStatusInProgress &&
        dataRecord->loadStatus_ == DsLoadStatusInProgress) {
        status = StatusDsLoadAlreadyStarted;
        xSyslog(moduleName,
                XlogInfo,
                "Update load status for dataset %s to %u failed: %s",
                name,
                loadStatus,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Update loadStatus now in the LibNs object which in turn provides
    // global visibility.
    dataRecord->loadStatus_ = loadStatus;

    // Given we have exclusive Read Write access, this is not going to
    // change behind our back. Even though the nsHandle version changes,
    // it does not need to be bubbled up back to the caller.
    nsHandle = libNs->updateNsObject(nsHandle, dataRecord, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusInvalidDatasetName;
        } else if (status == StatusAccess) {
            status = StatusDsDatasetInUse;
        } else if (status == StatusPendingRemoval) {
            status = StatusDatasetAlreadyDeleted;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to update load status %u for dataset %s on"
                " update Ns object: %s",
                loadStatus,
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (dataRecord != NULL) {
        memFree(dataRecord);
    }

    return status;
}

// This function creates the dataset structure needed to manage the
// dataset information on this node.
Status
Dataset::createDatasetStructLocal(void *arg)
{
    DsDataset *dataset = NULL;
    LibDatasetGvm::CreateStructInfo *createInfo =
        (LibDatasetGvm::CreateStructInfo *) arg;
    LoadDatasetInput *loadDatasetInput = (LoadDatasetInput *) arg;
    Status status = StatusOk;
    bool htLocked = false;

    dataset = DsDataset::allocDsDataset();
    if (dataset == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed create dataset %s locally: %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = dataset->init(loadDatasetInput, createInfo->datasetId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed create dataset %s locally: %s",
                loadDatasetInput->bulkLoadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    datasetHTLock_.lock();
    htLocked = true;
    if (datasetNameHashTable_.find(dataset->name_) != NULL) {
        status = StatusDatasetNameAlreadyExists;
        goto CommonExit;
    }
    status = datasetIdHashTable_.insert(dataset);
    assert(status == StatusOk);  // Should never fail.
    status = datasetNameHashTable_.insert(dataset);
    assert(status == StatusOk);  // Should never fail.
    ++numDatasets_;

CommonExit:
    if (htLocked) {
        datasetHTLock_.unlock();
        htLocked = false;
    }
    if (status != StatusOk) {
        if (dataset != NULL) {
            DsDataset::freeDsDataset(dataset);
            dataset = NULL;
        }
    }

    return status;
}

Status
Dataset::unloadDatasetLocal(void *arg)
{
    DsDatasetId datasetId;
    DsDataset *dataset;
    Status status;

    datasetId = *((DsDatasetId *) arg);

    datasetHTLock_.lock();
    {
        DsDataset *datasetInt, *datasetStr;
        datasetInt = datasetIdHashTable_.remove(datasetId);
        if (datasetInt != NULL) {
            datasetStr = datasetNameHashTable_.remove(datasetInt->name_);
            assert(datasetInt == datasetStr);
            dataset = datasetInt;
            --numDatasets_;
        } else {
            dataset = NULL;
        }
    }
    datasetHTLock_.unlock();

    if (dataset == NULL) {
        status = StatusDsNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed to unload dataset %ld locally: %s",
                datasetId,
                strGetFromStatus(status));
        return status;
    }
    dataset->doDelete();

    return StatusOk;
}

Status
Dataset::finalizeDatasetLocal(void *arg, size_t *outputSizeOut)
{
    DsDatasetId datasetId;
    DsDataset *dataset;
    Status status = StatusOk;
    Config *config = Config::get();
    TransportPageMgr *tpMgr = TransportPageMgr::get();
    XcalarApiDatasetsInfo *outputDatasetInfo = NULL;
    DfRecordId ignored;

    SchemaTable schemaTable;
    SchemaEntry *newEntry = NULL;

    datasetId = *((DsDatasetId *) arg);

    datasetHTLock_.lock();
    dataset = datasetIdHashTable_.find(datasetId);
    datasetHTLock_.unlock();
    if (dataset == NULL) {
        status = StatusDsNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed finalize dataset %ld locally: %s",
                datasetId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dataset->pageIndex_->getNumRecords() > 0) {
        status = dataset->pageIndex_->finalize();
        BailIfFailed(status);

        status = DataFormat::get()->addPageIndex(dataset->pageIndex_,
                                                 &dataset->startRecordId_);
        BailIfFailed(status);
    }

    if (dataset->errorPageIndex_->getNumRecords() > 0) {
        status = dataset->errorPageIndex_->finalize();
        BailIfFailed(status);

        status =
            DataFormat::get()->addPageIndex(dataset->errorPageIndex_, &ignored);
        BailIfFailed(status);
    }

    if (strlen(dataset->loadArgs_.xdbLoadArgs.keyName) > 0) {
        // need to wait for all indexes to complete
        OpLoadInfo *loadInfo = &dataset->dstMeta_->loadInfo;
        OpPagePoolNode *node = loadInfo->allNodesList;

        // ship all of the partial pages
        while (node != NULL) {
            assert(node->transPages != NULL);

            for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
                if (node->transPages[ii] == NULL) {
                    continue;
                }
                assert(0);
                if (status == StatusOk) {
                    status = loadInfo->transPageHandle->enqueuePage(
                        (TransportPage *) node->transPages[ii]);
                    if (status != StatusOk) {
                        tpMgr->freeTransportPage(node->transPages[ii]);
                    }
                } else {
                    tpMgr->freeTransportPage(node->transPages[ii]);
                }
                node->transPages[ii] = NULL;
            }
            memFree(node->transPages);
            node->transPages = NULL;

            node = node->listNext;
        }
        BailIfFailed(status);

        status = loadInfo->transPageHandle->waitForCompletions();
        BailIfFailed(status);
    }

    // Return our portion of the dataset size and the column names that
    // we see.
    outputDatasetInfo = (XcalarApiDatasetsInfo *) arg;

    outputDatasetInfo->totalNumErrors = atomicRead64(&dataset->numErrors_);
    outputDatasetInfo->downSampled = dataset->downSampled_;
    outputDatasetInfo->datasetSize = dataset->pageIndex_->getPageMemUsed();
    {
        // Gather the schema info from all of the fields in this dataset by
        // looking at the metadata in each page

        DataPageReader dpr;
        DataPageIndex::PageIterator pageIter(dataset->pageIndex_);
        pageIter.seek(0);
        uint8_t *page;
        int numFields = 0;
        // We limit the number of columns we return.
        while (pageIter.getNext(&page) &&
               numFields < TupleMaxNumValuesPerRecord) {
            dpr.init(page, XdbMgr::bcSize());
            DataPageReader::FieldMetaIterator fieldIter(&dpr);
            const char *fieldName = NULL;
            int32_t fieldNameLen = -1;
            ValueType type;
            while (fieldIter.getNext(&fieldName, &fieldNameLen, &type) &&
                   numFields < TupleMaxNumValuesPerRecord) {
                // We have a field meta; lets see if we are already keeping
                // track of this one
                DfFieldType dfType = valueTypeToDfType(type);
                uint64_t hashedIdentity =
                    SchemaEntry::hashNameAndType(fieldName,
                                                 fieldNameLen,
                                                 dfType);
                SchemaEntry *entry = schemaTable.find(hashedIdentity);
                if (entry == NULL) {
                    // We haven't seen this entry before, let's copy it into
                    // our output and add an entry to the schema table so we
                    // can easily check to make sure we've taken care of it.
                    // We could actually use
                    newEntry = new (std::nothrow)
                        SchemaEntry(fieldName, fieldNameLen, dfType);
                    BailIfNull(newEntry);

                    status = schemaTable.insert(newEntry);
                    BailIfFailed(status);
                    newEntry = NULL;

                    // It's in the schema table, now let's copy it into the
                    // output
                    int32_t len =
                        xcMin(fieldNameLen, (int32_t) DfMaxFieldNameLen);
                    strlcpy(outputDatasetInfo->columns[numFields].name,
                            fieldName,
                            len + 1);
                    outputDatasetInfo->columns[numFields].type = dfType;
                    numFields++;
                }
            }
        }
        outputDatasetInfo->numColumns = numFields;
    }

    *outputSizeOut = sizeof(*outputDatasetInfo);
CommonExit:
    if (newEntry != NULL) {
        delete newEntry;
        newEntry = NULL;
    }

    schemaTable.removeAll(&SchemaEntry::del);
    return status;
}

Status
Dataset::getDatasetTotalBytesUsed(uint64_t *bytesUsed)
{
    Status status = StatusOk;
    DsDataset *dataset = NULL;
    uint64_t totalIndexOverhead = 0;
    uint64_t totalPageMemUsed = 0;

    datasetHTLock_.lock();

    auto datasetIter = datasetIdHashTable_.begin();
    while ((dataset = datasetIter.get()) != NULL) {
        totalIndexOverhead += dataset->pageIndex_->getIndexOverhead();
        totalPageMemUsed += dataset->pageIndex_->getPageMemUsed();

        totalIndexOverhead += dataset->errorPageIndex_->getIndexOverhead();
        totalPageMemUsed += dataset->errorPageIndex_->getPageMemUsed();

        datasetIter.next();
    }

    datasetHTLock_.unlock();

    *bytesUsed = totalIndexOverhead + totalPageMemUsed;

    return status;
}

Status
Dataset::populateXcalarApiDataset(DsDatasetId datasetId,
                                  XcalarApiDataset *apiDataset)
{
    DsDataset *dataset = NULL;
    Status status;
    bool isObjDeleted = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    datasetHTLock_.lock();
    dataset = datasetIdHashTable_.find(datasetId);
    if (dataset == NULL) {
        datasetHTLock_.unlock();
        xSyslog(moduleName,
                XlogErr,
                "Failed populate XcalarApiDataset for dataset %ld: %s",
                datasetId,
                strGetFromStatus(StatusDsNotFound));
        return StatusDsNotFound;
    }
    apiDataset->loadArgs = dataset->loadArgs_;
    apiDataset->datasetId = dataset->datasetId_;
    strlcpy(apiDataset->name, dataset->name_, sizeof(apiDataset->name));
    datasetHTLock_.unlock();
    dataset = NULL;  // Not safe to access anymore

    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle nsHandle;
    DataRecord *dataRecord = NULL;
    LibNs *libNs = LibNs::get();

    status = datasetNsName(apiDataset->name,
                           fullyQualName,
                           LibNsTypes::MaxPathNameLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed populate XcalarApiDataset for dataset %s: %s",
                apiDataset->name,
                strGetFromStatus(status));
        return status;
    }

    nsHandle = libNs->open(fullyQualName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &dataRecord,
                           &status);
    if (status != StatusOk) {
        if (status == StatusPendingRemoval) {
            // Already marked for removal, so open has failed here.
            // - Dataset is not listable anymore.
            // - Most likely the load is complete, but we are not sure
            // here. So declare load is not yet complete.
            apiDataset->isListable = false;
            apiDataset->loadIsComplete = false;
            status = StatusOk;
        } else if (status == StatusAccess) {
            // Already open in exclusive mode, so open has failed here.
            // - Dataset is listable, since it is not marked for
            // removal.
            // - Most likely load is in progress, but we are not sure
            // here. So declare load is not yet complete.
            apiDataset->isListable = true;
            apiDataset->loadIsComplete = false;
            status = StatusOk;
        } else {
            if (status == StatusNsInvalidObjName) {
                status = StatusInvalidDatasetName;
            } else if (status == StatusNsNotFound) {
                status = StatusDsNotFound;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "Failed populate XcalarApiDataset for dataset %s on"
                    " Ns open: %s",
                    apiDataset->name,
                    strGetFromStatus(status));
        }
        goto CommonExit;
    }

    status = libNs->close(nsHandle, &isObjDeleted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed populate XcalarApiDataset for dataset %s on"
                " NS close: %s",
                apiDataset->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (isObjDeleted == true) {
        gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                             sizeof(DsDatasetId));
        BailIfNull(gPayload);

        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);

        gPayload->init(LibDatasetGvm::get()->getGvmIndex(),
                       (uint32_t) LibDatasetGvm::Action::Unload,
                       sizeof(DsDatasetId));
        *(DsDatasetId *) gPayload->buf = datasetId;

        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to populate XcalarApiDataset for dataset "
                    "%s on"
                    " Gvm invoke: %s",
                    apiDataset->name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    apiDataset->isListable = true;  // Listable since open succeeded.
    apiDataset->loadIsComplete =
        (dataRecord->loadStatus_ == DsLoadStatusComplete ||
         dataRecord->loadStatus_ == DsLoadStatusFailed);

CommonExit:
    if (dataRecord != NULL) {
        memFree(dataRecord);
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

// Stats init entry point
Status
Dataset::statsToInit()
{
    Status status;
    StatGroupId statsGrpId;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(moduleName, &statsGrpId, 2);
    if (status != StatusOk) {
        return status;
    }

    status = statsLib->initStatHandle(&dfGetFieldValuesFatptrCompletionCount);
    if (status != StatusOk) {
        return status;
    }
    status =
        statsLib->initAndMakeGlobal(statsGrpId,
                                    "dfGetFieldValuesFatptrCompletionCount",
                                    dfGetFieldValuesFatptrCompletionCount,
                                    StatUint64,
                                    StatCumulative,
                                    StatRefValueNotApplicable);
    if (status != StatusOk) {
        return status;
    }

    status = statsLib->initStatHandle(&dfGetFieldValuesFatptrCount);
    if (status != StatusOk) {
        return status;
    }
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "dfGetFieldValuesFatptrCount",
                                         dfGetFieldValuesFatptrCount,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);

    return status;
}

DsDataset *
DsDataset::allocDsDataset()
{
    DsDataset *dataset = NULL;
    void *ptr = mmap(NULL,
                     sizeof(DsDataset),
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS,
                     -1,
                     0);
    if (ptr != MAP_FAILED) {
        dataset = new (ptr) DsDataset;
    }
    return dataset;
}

void
DsDataset::freeDsDataset(DsDataset *dataset)
{
    dataset->~DsDataset();
    int ret = munmap(dataset, sizeof(DsDataset));
    if (ret) {
        // In theory munmap can fail, almost always due to an invalid
        // address.  In malloc/free land this would be heap corruption, so
        // we treat this just like free and keep going (and log an error
        // since we can).
        xSyslog(moduleName,
                XlogErr,
                "munmap failed with %s",
                strGetFromStatus(sysErrnoToStatus(errno)));
        assert(false && "Failed to unmap DsDataset memory");
    }
}

Status
DsDataset::init(const LoadDatasetInput *input, DsDatasetId datasetId)
{
    Status status = StatusOk;
    unsigned ret;
    const XcalarApiBulkLoadInput *bulkLoadInput = &input->bulkLoadInput;

    datasetId_ = datasetId;
    dagId_ = input->dagId;
    nodeId_ = bulkLoadInput->dagNodeId;
    ret = strlcpy(name_, bulkLoadInput->datasetName, sizeof(name_));
    if (ret >= sizeof(name_)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Failed load dataset %s locally on name copy: %s",
                input->bulkLoadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    verifyOk(::sparseMemCpy(&loadArgs_,
                            &bulkLoadInput->loadArgs,
                            sizeof(DfLoadArgs),
                            PageSize,
                            NULL));

    atomicWrite64(&numErrors_, 0);

    pageIndex_ = new (std::nothrow) DataPageIndex();
    BailIfNull(pageIndex_);

    status = pageIndex_->init(XdbMgr::bcSize(), DataFormat::freeXdbPage);
    BailIfFailed(status);

    errorPageIndex_ = new (std::nothrow) DataPageIndex();
    BailIfNull(errorPageIndex_);

    status = errorPageIndex_->init(XdbMgr::bcSize(), DataFormat::freeXdbPage);
    BailIfFailed(status);

    if (strlen(loadArgs_.xdbLoadArgs.keyName) > 0) {
        status = XdbMgr::get()->xdbGet(loadArgs_.xdbLoadArgs.dstXdbId,
                                       &dstXdb_,
                                       &dstMeta_);
        assert(status == StatusOk);
    }

CommonExit:
    return status;
}

DsDatasetId
DsDataset::getDatasetId() const
{
    return datasetId_;
}

void
DsDataset::doDelete()
{
    if (pageIndex_) {
        delete pageIndex_;
        pageIndex_ = NULL;
    }

    if (errorPageIndex_) {
        delete errorPageIndex_;
        errorPageIndex_ = NULL;
    }

    referenceUserTable_.removeAll(&DsReferenceUserEntry::del);

    DsDataset::freeDsDataset(this);
}

const char *
DsDataset::getDatasetName() const
{
    return name_;
}

void
Dataset::dlmDatasetReferenceMsg(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    DsReferenceMsg *referenceMsg = (DsReferenceMsg *) payload;
    size_t numBytesUsedInPayload = 0;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    eph->payloadToDistribute = NULL;

    switch (referenceMsg->op) {
    case AddReference:
        status = addDatasetReferenceLocal(payload);
        break;
    case ListReferences:
        status = listDatasetReferenceLocal(payload,
                                           &eph->payloadToDistribute,
                                           &numBytesUsedInPayload);
        break;
    case DeleteReference:
        status = deleteDatasetReferenceLocal(payload);
        break;
    default:
        assert(0);
        status = StatusInval;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to process dataset reference msg %d: %s",
                referenceMsg->op,
                strGetFromStatus(status));
    }

    assert(numBytesUsedInPayload <= MsgMgr::getMsgMaxPayloadSize());
    eph->setAckInfo(status, numBytesUsedInPayload);
}

void
Dataset::dlmDatasetReferenceCompletion(MsgEphemeral *eph, void *payload)
{
    DsReferenceResponse *response = (DsReferenceResponse *) eph->ephemeral;
    response->status = eph->status;
    if (response->status != StatusOk) {
        return;
    }

    switch (response->op) {
    case AddReference:
    case DeleteReference:
        // No output required
        break;
    case ListReferences: {
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
    default:
        assert(0 && "Invalid op");
        break;
    }
}

Status
Dataset::addDatasetReferenceLocal(void *payload)
{
    Status status = StatusOk;
    DsDatasetId datasetId;
    Config *config = Config::get();

    DsReferenceMsg *referenceMsg = (DsReferenceMsg *) payload;

    datasetId = referenceMsg->datasetId;

    assert(datasetId % config->getActiveNodes() == config->getMyNodeId());

    status = addDatasetReferenceHelper(datasetId, referenceMsg->userIdName);

    return status;
}

Status
Dataset::deleteDatasetReferenceLocal(void *payload)
{
    Status status = StatusOk;
    DsDatasetId datasetId;
    Config *config = Config::get();

    DsReferenceMsg *referenceMsg = (DsReferenceMsg *) payload;

    datasetId = referenceMsg->datasetId;

    assert(datasetId % config->getActiveNodes() == config->getMyNodeId());

    status = deleteDatasetReferenceHelper(datasetId, referenceMsg->userIdName);

    return status;
}

Status
Dataset::listDatasetReferenceLocal(void *payload,
                                   void **retUsers,
                                   size_t *retUsersSize)
{
    Status status = StatusOk;
    Config *config = Config::get();
    DsReferenceMsg *referenceMsg = (DsReferenceMsg *) payload;
    DsDatasetId datasetId = referenceMsg->datasetId;

    assert(datasetId % config->getActiveNodes() == config->getMyNodeId());

    *retUsersSize = 0;
    *retUsers = NULL;

    status = listDatasetUsersLocalHelper(datasetId, retUsers, retUsersSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (*retUsersSize == 0) {
        // There aren't any users of the dataset...sendBuffer doesn't
        // support sending a zero length payload so convey this via a
        // status.
        status = StatusNoDsUsers;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
Dataset::listDatasetUsersLocalHelper(DsDatasetId datasetId,
                                     void **retUsers,
                                     size_t *retUsersSize)
{
    Status status = StatusOk;
    DsDataset::DsReferenceUserEntry *userEntry = NULL;
    uint64_t cnt = 0;
    XcalarApiDatasetUser *users = NULL;
    DsDataset *dataset;

    *retUsers = NULL;
    *retUsersSize = 0;

    datasetHTLock_.lock();

    dataset = datasetIdHashTable_.find(datasetId);
    assert(dataset != NULL);
    if (dataset->userCount_ == 0) {
        goto CommonExit;
    }

    *retUsersSize =
        (sizeof(XcalarApiDatasetUser) * dataset->userCount_) + sizeof(size_t);
    *retUsers = memAllocExt(*retUsersSize, moduleName);
    BailIfNull(*retUsers);

    *((size_t *) (*retUsers)) =
        sizeof(XcalarApiDatasetUser) * dataset->userCount_;
    users = (XcalarApiDatasetUser *) ((uintptr_t)(*retUsers) + sizeof(size_t));
    for (DsDataset::ReferenceUserTable::iterator it =
             dataset->referenceUserTable_.begin();
         (userEntry = it.get()) != NULL;
         it.next()) {
        assert(cnt < dataset->userCount_);
        verifyOk(strStrlcpy(users[cnt].userId.userIdName,
                            userEntry->userName,
                            sizeof(users[cnt].userId.userIdName)));
        users[cnt].referenceCount = userEntry->referenceCount;
        cnt++;
    }

CommonExit:
    datasetHTLock_.unlock();
    return status;
}

Status
Dataset::updateLicensedValues()
{
    XcalarConfig *xcalarConfig = XcalarConfig::get();
    LicenseMgr *licenseMgr = LicenseMgr::get();
    uint64_t currentMax;
    uint64_t licensedMax;

    currentMax = xcalarConfig->getMaxInteractiveDataSize();
    licensedMax = licenseMgr->getLicenseData()->getMaxInteractiveDataSize();

    if (currentMax > licensedMax) {
        // The current setting is higher than the maximum licensed
        // value.  Set it to the licensed maximum.
        xSyslog(moduleName,
                XlogErr,
                "Configured MaxInteractiveDataSize '%lu' exceeds the "
                "licensed maximum '%lu'.  The licensed maximum will be "
                "enforced.",
                currentMax,
                licensedMax);
        xcalarConfig->setMaxInteractiveDataSize(licensedMax);
    }

    return StatusOk;
}

Status
Dataset::getDatasetIds(const char *pattern,
                       DsDatasetId **dsIdsOut,
                       uint64_t *dsCountOut)
{
    Status status = StatusOk;
    DsDatasetId *dsIds = NULL;
    bool htLocked = false;
    uint64_t dsCount = 0;
    DsDataset *dataset;

    if (strlen(pattern) > XcalarApiDsDatasetNameLen) {
        status = StatusInvalidDatasetName;
        goto CommonExit;
    }

    datasetHTLock_.lock();
    htLocked = true;

    // Allocate memory for all the datasets even though we're only going
    // to return those that match the pattern.
    dsIds = (DsDatasetId *) memAlloc(numDatasets_ * sizeof(*dsIds));
    if (dsIds == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    for (DatasetNameHashTable::iterator it = datasetNameHashTable_.begin();
         (dataset = it.get()) != NULL;
         it.next()) {
        if (strMatch(pattern, dataset->name_)) {
            assert(dsCount < numDatasets_);
            dsIds[dsCount] = dataset->datasetId_;
            dsCount++;
        }
    }

    *dsIdsOut = dsIds;
    *dsCountOut = dsCount;

CommonExit:
    if (htLocked) {
        datasetHTLock_.unlock();
        htLocked = false;
    }
    if (status != StatusOk) {
        if (dsIds != NULL) {
            memFree(dsIds);
            dsIds = NULL;
        }
    }

    return status;
}

// Persist dataset metadata supplied in queryStr, in global kvs for the
// dataset "dsName", and publish the datasetMeta in libNs.
//
// Return a boolean indicating if the call results in the first time creation
// of a global KVS entry for the dataset meta. The caller can use this state to
// decide whether to remove it during cleanup to avoid removing an already
// existing key (in which case dsPersisted will be false).
//
// NOTE about relationship between status return and dsPersisted:
//
// Can't assert about relationship b/w status and dsPersisted given that
// persistAndPublish performs two separate non-atomic actions:
//     persist and libNs publish
//
// if success => dsPersisted may be false (if metadata == existing entry)
//
// Both of the following can't be asserted since after a successful
// persist, libNs publication may fail and so could the unpersist during
// failure processing!
//
// if failure => dsPersisted must be false (can't be asserted)
// if dsPersisted == true then success must've been returned (can't be asserted)
//
// The only thing you can rely on is that if dsPersisted is true, the
// ds was persisted for the first time by this routine - and so removing it is
// ok if we choose to do so (i.e. it will not be removing a previously
// persisted entry)
//

Status
Dataset::persistAndPublish(const char *dsName,
                           const char *queryStr,
                           bool *dsPersisted)
{
    Status status;

    *dsPersisted = false;
    // Write queryStr to KvStore
    status = persistDatasetMeta(dsName, queryStr, dsPersisted);

    assert(status == StatusOk || *dsPersisted == false);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to persist dataset metadata for '%s': %s",
                dsName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = publishDatasetMeta(dsName, queryStr);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to publish dataset meta in namespace for '%s': "
                "%s",
                dsName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (*dsPersisted) {
            // if this was the first add, remove it; otherwise, would be
            // removing a pre-existing ds meta entry
            Status status2 = unpersistDatasetMeta(dsName);
            if (status2 != StatusOk) {
                // If this error arises we may consider using a nodeid-0 libNs
                // object.
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to unpersist dataset meta for '%s': %s",
                        dsName,
                        strGetFromStatus(status2));
                // Continue...
            } else {
                *dsPersisted = false;
            }
        }
    }
    return status;
}

Status
Dataset::createDatasetMeta(XcalarApiBulkLoadInput *bulkLoadInput)
{
    Status status = StatusOk;
    QueryCmdParser *cmdParser = NULL;
    char *queryStr = NULL;
    json_t *query = NULL;
    json_t *queryObj = NULL;
    json_error_t err;
    char *parserFnName = NULL;
    bool dsPersisted = false;
    char *relativeName = NULL;

    query = json_array();
    BailIfNull(query);

    cmdParser = QueryParser::get()->getCmdParser(XcalarApiBulkLoad);
    BailIfNull(cmdParser);

    // Copy the parser UDF module, if any, to shared UDF space
    parserFnName = (char *) bulkLoadInput->loadArgs.parseArgs.parserFnName;
    status =
        UserDefinedFunction::get()->copyUdfParserToSharedSpace(parserFnName,
                                                               &relativeName);
    if (status != StatusOk) {
        if (status == StatusUdfModuleFullNameRequired) {
            // All we have is a relative name which occurs for legacy retina
            // files.  We'll not error out with the assumption that the UDF
            // module is also part of the retina and "late binding" of the
            // relative UDF module will succeed.
            xSyslog(moduleName,
                    XlogWarn,
                    "Failed to copy parser '%s' to shared UDF space.  Will "
                    "attempt UDF module resolution at load time",
                    parserFnName);
            status = StatusOk;
            // continue on...
        } else {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to copy parser '%s' to shared UDF space: %s",
                          parserFnName,
                          strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Copy the relative name into the load args.  It's possible there's no
    // relative name returned (e.g. default module or already in shared udf
    // space).
    if (relativeName != NULL) {
        assert(strlen(relativeName) <
               sizeof(bulkLoadInput->loadArgs.parseArgs.parserFnName));
        strncpy(bulkLoadInput->loadArgs.parseArgs.parserFnName,
                relativeName,
                sizeof(bulkLoadInput->loadArgs.parseArgs.parserFnName));
        memFree(relativeName);
        relativeName = NULL;
    }

    // Convert input to json
    status = cmdParser->reverseParse(XcalarApiBulkLoad,
                                     "",
                                     "",
                                     DgDagStateUnknown,
                                     (const XcalarApiInput *) bulkLoadInput,
                                     NULL,
                                     &err,
                                     query);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to parse dataset creation input for dataset "
                      "'%s'): %s, "
                      "source '%s', line %d, column %d, position %d: '%s'",
                      bulkLoadInput->datasetName,
                      strGetFromStatus(status),
                      err.source,
                      err.line,
                      err.column,
                      err.position,
                      err.text);
        goto CommonExit;
    }

    assert(json_typeof(query) == JSON_ARRAY);

    queryObj = json_array_get(query, 0);

    // Convert json to a string
    queryStr = json_dumps(queryObj, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    BailIfNull(queryStr);

    status =
        persistAndPublish(bulkLoadInput->datasetName, queryStr, &dsPersisted);

    BailIfFailed(status);

CommonExit:

    if (query) {
        json_decref(query);
        query = NULL;
    }
    if (queryStr) {
        memFree(queryStr);
        queryStr = NULL;
    }

    return status;
}

// Given a dataset name, generate the name to use when storing the meta data in
// KvStore.
Status
Dataset::datasetMetaKvsName(const char *datasetName,
                            char *datasetMetaKvsName,
                            size_t maxDatasetMetaKvsName)
{
    Status status = StatusOk;
    bool addNamePrefix = false;
    int ret;

    if (strncmp(datasetName,
                XcalarApiDatasetPrefix,
                XcalarApiDatasetPrefixLen) != 0) {
        addNamePrefix = true;
    }
    ret = snprintf(datasetMetaKvsName,
                   maxDatasetMetaKvsName,
                   "%s/%s/%s%s",
                   KvStoreLib::KvsXCEPrefix,
                   KvStoreLib::KvsXCEDatasetMeta,
                   (addNamePrefix ? XcalarApiDatasetPrefix : ""),
                   datasetName);
    if (ret >= (int) maxDatasetMetaKvsName) {
        status = StatusOverflow;
        goto CommonExit;
    }

CommonExit:

    return status;
}

// Given a dataset name, generate the name to use when storing the dataset in
// the global namespace.
Status
Dataset::datasetNsName(const char *datasetName,
                       char *datasetNsName,
                       size_t maxDatasetNsName)
{
    Status status = StatusOk;
    bool addNamePrefix = false;
    int ret;

    if (strncmp(datasetName,
                XcalarApiDatasetPrefix,
                XcalarApiDatasetPrefixLen) != 0) {
        addNamePrefix = true;
    }
    ret = snprintf(datasetNsName,
                   maxDatasetNsName,
                   "%s%s%s",
                   Dataset::DsPrefix,
                   (addNamePrefix ? XcalarApiDatasetPrefix : ""),
                   datasetName);
    if (ret >= (int) maxDatasetNsName) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

CommonExit:

    return status;
}

// Given a dataset name, generate the name to use when storing the meta data in
// the global namespace.
Status
Dataset::datasetMetaNsName(const char *datasetName,
                           char *datasetMetaNsName,
                           size_t maxDatasetMetaNsName)
{
    Status status = StatusOk;
    bool addNamePrefix = false;
    int ret;

    if (strncmp(datasetName,
                XcalarApiDatasetPrefix,
                XcalarApiDatasetPrefixLen) != 0) {
        addNamePrefix = true;
    }
    ret = snprintf(datasetMetaNsName,
                   maxDatasetMetaNsName,
                   "%s%s%s",
                   Dataset::DsMetaPrefix,
                   (addNamePrefix ? XcalarApiDatasetPrefix : ""),
                   datasetName);
    if (ret >= (int) maxDatasetMetaNsName) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

CommonExit:

    return status;
}

// Write the dataset meta to the Global KvStore
Status
Dataset::persistDatasetMeta(const char *datasetName,
                            const char *datasetMeta,
                            bool *firstAdd)
{
    Status status = StatusOk;
    char keyName[XcalarApiMaxKeyLen + 1];
    KvStoreLib *kvs = KvStoreLib::get();
    char *value = NULL;
    size_t valueSize = 0;

    *firstAdd = false;

    status = datasetMetaKvsName(datasetName, keyName, sizeof(keyName));
    BailIfFailed(status);

    status = kvs->lookup(XidMgr::XidGlobalKvStore, keyName, &value, &valueSize);
    if (status == StatusOk) {
        // already in global kvs; value most likely matches so assert this
        // and return success. This also means it must be in libNs...since
        // persistDatasetMeta is called together with publish in
        // persistAndPublish. If the key exists but value doesn't match, don't
        // want to over-write the value so return failure in this case
        if (strncmp(datasetMeta, value, valueSize) != 0) {
            status = StatusExist;
            xSyslog(moduleName,
                    XlogErr,
                    "persistDatasetMeta failed to write '%s' "
                    "to KvStore key '%s' which has value '%s': %s",
                    datasetMeta,
                    keyName,
                    value,
                    strGetFromStatus(status));
            assert(0);
        }
        goto CommonExit;  // success or failure, get out of here
    }

    status = kvs->addOrReplace(XidMgr::XidGlobalKvStore,
                               keyName,
                               strlen(keyName) + 1,
                               datasetMeta,
                               strlen(datasetMeta) + 1,
                               true,
                               KvStoreOptSync);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to write '%s' to KvStore key '%s': %s",
                datasetMeta,
                keyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    *firstAdd = true;

#ifdef DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "Successfully wrote key '%s' to global KvStore",
            keyName);
#endif

CommonExit:

    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

// Read the dataset meta from KvStore
Status
Dataset::readDatasetMeta(const char *datasetName, char **datasetMetaOut)
{
    Status status = StatusOk;
    char keyName[XcalarApiMaxKeyLen + 1];
    KvStoreLib *kvs = KvStoreLib::get();
    char *value = NULL;
    size_t valueSize = 0;

    status = datasetMetaKvsName(datasetName, keyName, sizeof(keyName));
    BailIfFailed(status);

    status = kvs->lookup(XidMgr::XidGlobalKvStore, keyName, &value, &valueSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to lookup key '%s' from global KvStore: %s",
                keyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

#ifdef DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "Successfully read key '%s' from global KvStore",
            keyName);
#endif

    *datasetMetaOut = value;
    value = NULL;

CommonExit:
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }

    return status;
}

// Get the dataset meta from libNs (if cached) otherwise read from
// KvStore
Status
Dataset::getDatasetMeta(const char *datasetName, char **datasetMetaOut)
{
    Status status = StatusOk;
    char datasetMetaName[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    DsMetaRecord *dsMetaRecord = NULL;
    bool nsOpened = false;
    size_t cachedSize = 0;

    status = datasetMetaNsName(datasetName,
                               datasetMetaName,
                               sizeof(datasetMetaName));
    BailIfFailed(status);

    nsHandle = libNs->open(datasetMetaName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &dsMetaRecord,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound) {
            // Convert to meta data error
            status = StatusDsMetaDataNotFound;
        }
        goto CommonExit;
    }
    nsOpened = true;

    // One extra for terminating null
    cachedSize = dsMetaRecord->getCachedDsMetaSize() + 1;
    if (cachedSize > 1) {
        // Copy from ns object
        char *dsMeta = (char *) memAllocExt(cachedSize, moduleName);
        BailIfNull(dsMeta);
        status = dsMetaRecord->getDsMeta(dsMeta, cachedSize);
        BailIfFailed(status);
        *datasetMetaOut = dsMeta;
    } else {
        // It was too large to be cached so read from KvStore
        status = readDatasetMeta(datasetName, datasetMetaOut);
        BailIfFailed(status);
    }

CommonExit:

    if (dsMetaRecord != NULL) {
        memFree(dsMetaRecord);
        dsMetaRecord = NULL;
    }
    if (nsOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            // Syslog as this should never happen
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close namespace handle for '%s': %s",
                    datasetName,
                    strGetFromStatus(status2));
            // Continue...
        }
    }

    return status;
}

Status
Dataset::getDatasetMetaAsApiInput(const char *datasetName,
                                  XcalarApiBulkLoadInput **bulkLoadInputOut)
{
    Status status = StatusOk;
    QueryCmdParser *cmdParser = NULL;
    char *metaStr = NULL;
    json_error_t err;
    json_t *query = NULL;
    json_t *args = NULL;
    XcalarWorkItem *workItem = NULL;

    status = getDatasetMeta(datasetName, &metaStr);
    BailIfFailed(status);

    // Load the json string into the query
    query = json_loads(metaStr, 0, &err);
    if (query == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to load json query, source %s, line %d, column "
                "%d, "
                "position %d: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text);
        assert(0 && "Json load error on data we control");
        goto CommonExit;
    }
    assert(json_typeof(query) == JSON_OBJECT);
    args = json_object_get(query, QueryCmdParser::ArgsKey);
    if (args == NULL) {
        status = StatusJsonQueryParseError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to get '%s' from json load operation",
                QueryCmdParser::ArgsKey);
        goto CommonExit;
    }

    cmdParser = QueryParser::get()->getCmdParser(XcalarApiBulkLoad);
    BailIfNull(cmdParser);

    status = cmdParser->parseJson(args, &err, &workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse json query, source %s, line %d, "
                "column "
                "%d, "
                "position %d: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text);
        assert(0 && "Json parse error on data we control");
        goto CommonExit;
    }
    assert(workItem->input != NULL);

    // Return the api input to the caller who is responsible for freeing
    // it
    *bulkLoadInputOut = (XcalarApiBulkLoadInput *) workItem->input;
    workItem->input = NULL;  // So freeing workitem doesn't free input

CommonExit:

    if (metaStr != NULL) {
        memFree(metaStr);
        metaStr = NULL;
    }
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    if (query) {
        json_decref(query);
        query = NULL;
    }

    return status;
}

Status
Dataset::getDatasetMetaAsStr(
    const XcalarApiDatasetGetMetaInput *datasetGetMetaInput,
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut)
{
    Status status = StatusOk;
    char *metaStr = NULL;
    size_t metaStrLen;
    XcalarApiOutput *output = NULL;
    XcalarApiDatasetGetMetaOutput *datasetGetMetaOutput = NULL;
    size_t outputSize = 0;

    status = getDatasetMeta(datasetGetMetaInput->datasetName, &metaStr);
    BailIfFailed(status);

    metaStrLen = strlen(metaStr);

    // Include the null terminator
    outputSize = XcalarApiSizeOfOutput(*datasetGetMetaOutput) + metaStrLen + 1;
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    BailIfNull(output);

    datasetGetMetaOutput = &output->outputResult.datasetGetMetaOutput;
    memcpy(datasetGetMetaOutput->datasetMeta, metaStr, metaStrLen + 1);
    datasetGetMetaOutput->datasetMetaSize = metaStrLen + 1;

CommonExit:

    if (metaStr != NULL) {
        memFree(metaStr);
        metaStr = NULL;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
Dataset::unpersistDatasetMeta(const char *datasetName)
{
    Status status = StatusOk;
    char keyName[XcalarApiMaxKeyLen + 1];
    KvStoreLib *kvs = KvStoreLib::get();

    status = datasetMetaKvsName(datasetName, keyName, sizeof(keyName));
    BailIfFailed(status);

    status = kvs->del(XidMgr::XidGlobalKvStore, keyName, KvStoreOptSync);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete KvStore key '%s': %s",
                keyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

#ifdef DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "Removed key '%s' from global KvStore",
            keyName);
#endif

CommonExit:

    return status;
}

Status
Dataset::deleteDatasetMeta(
    const XcalarApiDatasetDeleteInput *deleteDatasetInput)
{
    Status status = StatusOk;
    char datasetNsFQN[LibNsTypes::MaxPathNameLen];
    char datasetMetaName[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    bool objDeleted = false;
    LibNsTypes::NsHandle nsHandle;
    uint64_t numDatasets;
    LibNsTypes::PathInfoOut *pathInfo = NULL;
    bool pathOpened = false;
    bool objRemoved = false;

    status = datasetMetaNsName(deleteDatasetInput->datasetName,
                               datasetMetaName,
                               sizeof(datasetMetaName));
    BailIfFailed(status);

    // In order to delete the dataset meta we must have exclusive access.  This
    // avoids races with others but means it cannot be deleted until there's
    // noone using it.

    nsHandle = libNs->open(datasetMetaName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' in namespace in order to delete: %s",
                datasetMetaName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    // We now have exclusive access to the dataset meta.  We have to see if
    // there's a dataset loaded from this meta.  We can tell this by the dataset
    // being in the namespace.

    status = datasetNsName(deleteDatasetInput->datasetName,
                           datasetNsFQN,
                           sizeof(datasetNsFQN));
    BailIfFailed(status);

    numDatasets = libNs->getPathInfo(datasetNsFQN, &pathInfo);
    if (numDatasets) {
        status = StatusDsDatasetInUse;
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete dataset meta for '%s': %s",
                deleteDatasetInput->datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // There's no datasets loaded.  We can remove the dataset meta.

    status = libNs->remove(datasetMetaName, &objDeleted);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to remove dataset meta for '%s' from "
                      "namespace: %s",
                      datasetMetaName,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    // We have the object open so it can't have been deleted.  It will be
    // deleted when the libNs handle is closed at function exit.
    assert(objDeleted == false);
    objRemoved = true;

    status = unpersistDatasetMeta(deleteDatasetInput->datasetName);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to unpersist '%s': %s",
                      deleteDatasetInput->datasetName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (pathInfo != NULL) {
        memFree(pathInfo);
        pathInfo = NULL;
    }
    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, &objDeleted);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s' in namespace: %s",
                    datasetMetaName,
                    strGetFromStatus(status));
            // Continue...
        }
        if (objRemoved) {
            assert(objDeleted = true);
        }
    }

    return status;
}

Status
Dataset::loadAllDatasetMeta()
{
    Status status = StatusOk;
    KvStoreLib *kvs = KvStoreLib::get();
    KvStoreLib::KeyList *keyList = NULL;
    char keyPrefix[XcalarApiMaxKeyLen + 1];
    int ret;
    char *value = NULL;
    size_t valueSize;
    char *datasetNameStart;

    // Build the search regex to find all our dataset meta keys
    ret = snprintf(keyPrefix,
                   sizeof(keyPrefix),
                   "%s/%s/*",
                   KvStoreLib::KvsXCEPrefix,
                   KvStoreLib::KvsXCEDatasetMeta);
    assert(ret < (int) sizeof(keyPrefix));

    status = kvs->list(XidMgr::XidGlobalKvStore, keyPrefix, &keyList);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset meta list from global KvStore: "
                "%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (int ii = 0; ii < keyList->numKeys; ii++) {
        status = kvs->lookup(XidMgr::XidGlobalKvStore,
                             keyList->keys[ii],
                             &value,
                             &valueSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to lookup KvStore key '%s' for dataset "
                    "meta: %s",
                    keyList->keys[ii],
                    strGetFromStatus(status));
            // Continue with rest...
            continue;
        }
        // Point past the keyPrefix to the dataset name.  The -1 is to
        // skip the "*".
        datasetNameStart = keyList->keys[ii] + strlen(keyPrefix) - 1;
        status = publishDatasetMeta(datasetNameStart, value);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to publish dataset meta for '%s': %s",
                    datasetNameStart,
                    strGetFromStatus(status));
            // Continue with rest...
            continue;
        }

        memFree(value);
        value = NULL;
    }

CommonExit:

    if (keyList != NULL) {
        memFree(keyList);
        keyList = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }

    return status;
}

// Publish dataset meta into namespace so it is available cluster wide
// and can be access protected.
Status
Dataset::publishDatasetMeta(const char *datasetName, const char *datasetMeta)
{
    Status status = StatusOk;
    size_t datasetMetaLen = 0;
    char datasetMetaName[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    DsMetaRecord *dsMetaRecord = NULL;
    LibNsTypes::NsId nsId;

    status = datasetMetaNsName(datasetName,
                               datasetMetaName,
                               sizeof(datasetMetaName));
    BailIfFailed(status);

    dsMetaRecord = new (std::nothrow) DsMetaRecord();
    BailIfNull(dsMetaRecord);

    datasetMetaLen = strlen(datasetMeta);
    if (datasetMetaLen <= dsMetaRecord->maxCachedDsMetaSize()) {
        status = dsMetaRecord->setDsMeta(datasetMeta);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to cache dataset meta for '%s': %s",
                    datasetName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        xSyslog(moduleName,
                XlogInfo,
                "Failed to cache dataset meta for '%s' as size %lu "
                "exceeds "
                "maximum cache size %lu",
                datasetName,
                datasetMetaLen,
                dsMetaRecord->maxCachedDsMetaSize());
        assert(0 && "Dataset meta size exceeds maximum cache size");
    }

    nsId = libNs->publish(datasetMetaName, dsMetaRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to publish '%s' to global namespace: %s",
                datasetMetaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (dsMetaRecord != NULL) {
        delete dsMetaRecord;
        dsMetaRecord = NULL;
    }

    return status;
}
