// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/KvStoreService.h"
#include "usr/Users.h"
#include "kvstore/KvStore.h"
#include "msg/Xid.h"

using namespace xcalar::compute::localtypes::KvStore;
using namespace xcalar::compute::localtypes::Workbook;

KvStoreService::KvStoreService()
{
    userMgr_ = UserMgr::get();
    assert(userMgr_);
    kvsLib_ = KvStoreLib::get();
    assert(kvsLib_);
}

ServiceAttributes
KvStoreService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    if (!strcmp(methodName, "Lookup")) {
        // XXX TODO There is a Kvstore lookup syslog diarrhea bc of stats
        // collection. Re-enable once that's fixed.
        sattr.syslogRequest = false;
        sattr.syslogResponse = false;
    }
    return sattr;
}

Status
KvStoreService::lookup(const LookupRequest *lookupRequest,
                       LookupResponse *lookupResponse)
{
    Status status;
    KvStoreId kvStoreId;
    char *value = NULL;
    size_t valueSize = 0;
    bool trackOpsToSession = false;
    bool getKvIdPass = false;

    status = getKvStoreId(&lookupRequest->key().scope(),
                          &kvStoreId,
                          &trackOpsToSession);
    BailIfFailed(status);
    getKvIdPass = true;
    if (lookupRequest->key().scope().specifier_case() ==
        WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : lookup possible refCount Leak: flag is corrupted. "
                    "API status : %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
            goto CommonExit;
        }
    }

    status = kvsLib_->lookup(kvStoreId,
                             lookupRequest->key().name().c_str(),
                             &value,
                             &valueSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // The value size sometimes (always?) includes the null terminator; fix that
    if (valueSize > 0 && value[valueSize - 1] == '\0') {
        --valueSize;
    }

    try {
        lookupResponse->mutable_value()->set_text(value, valueSize);
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (getKvIdPass && lookupRequest->key().scope().specifier_case() ==
                           WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : lookup possible refCount Leak "
                    "flag is corrupted right "
                    "before decrement. API status : %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
        }
    }
    if (trackOpsToSession) {
        Status status2 = unTrackOpsToSession(&lookupRequest->key().scope());
        assert(status2 == StatusOk);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : lookup failed to decrement. API Status : %s",
                    strGetFromStatus(status));
            status = status2;
        }
        trackOpsToSession = false;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

Status
KvStoreService::addOrReplace(const AddOrReplaceRequest *addOrReplaceRequest,
                             google::protobuf::Empty *empty)
{
    Status status;
    KvStoreId kvStoreId;
    bool trackOpsToSession = false;
    bool getKvIdPass = false;

    status = getKvStoreId(&addOrReplaceRequest->key().scope(),
                          &kvStoreId,
                          &trackOpsToSession);
    BailIfFailed(status);
    getKvIdPass = true;
    if (addOrReplaceRequest->key().scope().specifier_case() ==
        WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : AddOrReplace possible refCount Leak: flag is "
                    "corrupted. API Status: %s ",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
            goto CommonExit;
        }
    }
    status =
        kvsLib_->addOrReplace(kvStoreId,
                              addOrReplaceRequest->key().name().c_str(),
                              addOrReplaceRequest->key().name().length() + 1,
                              addOrReplaceRequest->value().text().c_str(),
                              addOrReplaceRequest->value().text().length() + 1,
                              addOrReplaceRequest->persist(),
                              KvStoreOptSync);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (getKvIdPass && addOrReplaceRequest->key().scope().specifier_case() ==
                           WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : AddOrReplace possible refCount Leak: flag is "
                    "corrupted right "
                    "before decrement. API Status: %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
        }
    }
    if (trackOpsToSession) {
        Status status2 =
            unTrackOpsToSession(&addOrReplaceRequest->key().scope());
        assert(status2 == StatusOk);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : addOrReplace failed to  decrement. API Status: %s",
                    strGetFromStatus(status));
            status = status2;
        }
        trackOpsToSession = false;
    }
    return status;
}

Status
KvStoreService::multiAddOrReplace(
    const MultiAddOrReplaceRequest *multiAddOrReplaceRequest,
    google::protobuf::Empty *empty)
{
    Status status;
    KvStoreId kvStoreId;
    bool trackOpsToSession = false;
    char **keys = NULL;
    char **values = NULL;
    size_t *keysSize = NULL;
    size_t *valuesSize = NULL;
    size_t numKvPairs = 0;

    // TODO: support global-scope updates
    if (multiAddOrReplaceRequest->scope().specifier_case() ==
        WorkbookScope::kGlobl) {
        status = StatusInval;
        goto CommonExit;
    }
    status = getKvStoreId(&multiAddOrReplaceRequest->scope(),
                          &kvStoreId,
                          &trackOpsToSession);
    BailIfFailed(status);

    // the number of keys should be the same as values
    numKvPairs = multiAddOrReplaceRequest->keys_size();
    keys = (char **) memAlloc(numKvPairs * sizeof(*keys));
    BailIfNull(keys);
    memZero(keys, numKvPairs * sizeof(*keys));
    keysSize = (size_t *) memAlloc(numKvPairs * sizeof(*keysSize));
    BailIfNull(keysSize);
    memZero(keysSize, numKvPairs * sizeof(*keysSize));
    values = (char **) memAlloc(numKvPairs * sizeof(*values));
    BailIfNull(values);
    memZero(values, numKvPairs * sizeof(*values));
    valuesSize = (size_t *) memAlloc(numKvPairs * sizeof(*valuesSize));
    BailIfNull(valuesSize);
    memZero(valuesSize, numKvPairs * sizeof(*valuesSize));
    for (size_t ii = 0; ii < numKvPairs; ii++) {
        keys[ii] = strdup(multiAddOrReplaceRequest->keys(ii).c_str());
        BailIfNull(keys[ii]);
        keysSize[ii] = multiAddOrReplaceRequest->keys(ii).length() + 1;
        values[ii] =
            strdup(multiAddOrReplaceRequest->values(ii).text().c_str());
        BailIfNull(values[ii]);
        valuesSize[ii] =
            multiAddOrReplaceRequest->values(ii).text().length() + 1;
    }

    status = kvsLib_->multiAddOrReplace(kvStoreId,
                                        (const char **) keys,
                                        (const char **) values,
                                        (const size_t *) keysSize,
                                        (const size_t *) valuesSize,
                                        multiAddOrReplaceRequest->persist(),
                                        KvStoreOptSync,
                                        numKvPairs);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (keys != NULL) {
        for (size_t ii = 0; ii < numKvPairs; ii++) {
            memFree(keys[ii]);
            keys[ii] = NULL;
        }
        memFree(keys);
        keys = NULL;
    }
    if (keysSize != NULL) {
        memFree(keysSize);
        keysSize = NULL;
    }
    if (values != NULL) {
        for (size_t ii = 0; ii < numKvPairs; ii++) {
            memFree(values[ii]);
            values[ii] = NULL;
        }
        memFree(values);
        values = NULL;
    }
    if (valuesSize != NULL) {
        memFree(keysSize);
        valuesSize = NULL;
    }
    if (trackOpsToSession) {
        unTrackOpsToSession(&multiAddOrReplaceRequest->scope());
        trackOpsToSession = false;
    }
    return status;
}

Status
KvStoreService::deleteKey(const DeleteKeyRequest *deleteKeyRequest,
                          google::protobuf::Empty *empty)
{
    Status status;
    KvStoreId kvStoreId;
    bool trackOpsToSession = false;
    bool getKvIdPass = false;

    status = getKvStoreId(&deleteKeyRequest->key().scope(),
                          &kvStoreId,
                          &trackOpsToSession);
    BailIfFailed(status);
    getKvIdPass = true;
    if (deleteKeyRequest->key().scope().specifier_case() ==
        WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : deleteKey possible refCount Leak: flag is corrupted. "
                    "API Status : %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
            goto CommonExit;
        }
    }
    status = kvsLib_->del(kvStoreId,
                          deleteKeyRequest->key().name().c_str(),
                          KvStoreOptSync);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (getKvIdPass && deleteKeyRequest->key().scope().specifier_case() ==
                           WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : deleteKey possible refCount Leak: flag is corrupted "
                    "right "
                    "before decrement. API Status: %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
        }
    }
    if (trackOpsToSession) {
        Status status2 = unTrackOpsToSession(&deleteKeyRequest->key().scope());
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : deleteKey failed decrement. API Status : %s",
                    strGetFromStatus(status));
            status = status2;
        }
        trackOpsToSession = false;
    }
    return status;
}

Status
KvStoreService::append(const AppendRequest *appendRequest,
                       google::protobuf::Empty *empty)
{
    Status status;
    KvStoreId kvStoreId;
    bool trackOpsToSession = false;
    bool getKvIdPass = false;
    status = getKvStoreId(&appendRequest->key().scope(),
                          &kvStoreId,
                          &trackOpsToSession);
    BailIfFailed(status);
    getKvIdPass = true;
    if (appendRequest->key().scope().specifier_case() ==
        WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : append possible refCount Leak: flag is corrupted. "
                    "API Status %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
            goto CommonExit;
        }
    }
    status = kvsLib_->append(kvStoreId,
                             appendRequest->key().name().c_str(),
                             appendRequest->suffix().c_str(),
                             KvStoreOptSync);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (getKvIdPass && appendRequest->key().scope().specifier_case() ==
                           WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : append possible refCount Leak: flag is corrupted "
                    "right "
                    "before decrement. API Status: %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
        }
    }
    if (trackOpsToSession) {
        Status status2 = unTrackOpsToSession(&appendRequest->key().scope());
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : append failed decrement. API Status: %s",
                    strGetFromStatus(status));
            status = status2;
        }
        trackOpsToSession = false;
    }
    return status;
}

Status
KvStoreService::setIfEqual(const SetIfEqualRequest *setIfEqualRequest,
                           google::protobuf::Empty *empty)
{
    Status status;
    KvStoreId kvStoreId;
    bool trackOpsToSession = false;
    bool getKvIdPass = false;

    status = getKvStoreId(&setIfEqualRequest->scope(),
                          &kvStoreId,
                          &trackOpsToSession);
    BailIfFailed(status);
    getKvIdPass = true;
    if (setIfEqualRequest->scope().specifier_case() ==
        WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : setIfEqual possible refCount Leak: flag is corrupted "
                    "right "
                    "before decrement. API Status : %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
            goto CommonExit;
        }
    }

    status = kvsLib_->setIfEqual(kvStoreId,
                                 setIfEqualRequest->countsecondarypairs(),
                                 setIfEqualRequest->keycompare().c_str(),
                                 setIfEqualRequest->valuecompare().c_str(),
                                 setIfEqualRequest->valuereplace().c_str(),
                                 setIfEqualRequest->keysecondary().c_str(),
                                 setIfEqualRequest->valuesecondary().c_str(),
                                 setIfEqualRequest->persist(),
                                 KvStoreOptSync);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (getKvIdPass && setIfEqualRequest->scope().specifier_case() ==
                           WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : setIfEqual possible refCount Leak: flag is corrupted "
                    "right "
                    "before decrement. API Status %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
        }
    }
    if (trackOpsToSession) {
        Status status2 = unTrackOpsToSession(&setIfEqualRequest->scope());
        assert(status2 == StatusOk);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : setIfEqual failed decrement. API Status : %s",
                    strGetFromStatus(status));
            status = status2;
        }
        trackOpsToSession = false;
    }
    return status;
}

Status
KvStoreService::list(const ListRequest *listRequest, ListResponse *listResponse)
{
    Status status;
    KvStoreId kvStoreId;
    KvStoreLib::KeyList *keyList = NULL;
    bool trackOpsToSession = false;
    bool getKvIdPass = false;

    status =
        getKvStoreId(&listRequest->scope(), &kvStoreId, &trackOpsToSession);
    BailIfFailed(status);
    getKvIdPass = true;
    if (listRequest->scope().specifier_case() == WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : list possible refCount Leak: flag is corrupted. API "
                    "Status %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
            goto CommonExit;
        }
    }

    status =
        kvsLib_->list(kvStoreId, listRequest->keyregex().c_str(), &keyList);
    if (status != StatusOk) {
        goto CommonExit;
    }

    try {
        for (int ii = 0; ii < keyList->numKeys; ii++) {
            auto newKey = listResponse->add_keys();
            *newKey = keyList->keys[ii];
        }
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (keyList != NULL) {
        memFree(keyList);
        keyList = NULL;
    }
    if (getKvIdPass &&
        listRequest->scope().specifier_case() == WorkbookScope::kWorkbook) {
        assert(trackOpsToSession);
        if (trackOpsToSession == false) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : list possible refCount Leak: flag is corrupted right "
                    "before decrement. API Status:  %s",
                    strGetFromStatus(status));
            status = StatusKVSRefCountLeak;
        }
    }
    if (trackOpsToSession) {
        Status status2 = unTrackOpsToSession(&listRequest->scope());
        assert(status2 == StatusOk);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "kv : list failed decrement. API Status : %s",
                    strGetFromStatus(status));
            status = status2;
        }
        trackOpsToSession = false;
    }
    return status;
}

Status
KvStoreService::unTrackOpsToSession(const WorkbookScope *scope)
{
    Status status = StatusOk;

    switch (scope->specifier_case()) {
    case WorkbookScope::kGlobl:
        // NOOP, since there is no workbook associated here
        break;
    case WorkbookScope::kWorkbook: {
        auto workbookSpec = scope->workbook();
        status = userMgr_->trackOutstandOps(&workbookSpec,
                                            UserMgr::OutstandOps::Dec);
        BailIfFailed(status);
        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    assert(status == StatusOk);
    return status;
}

Status
KvStoreService::getKvStoreId(const WorkbookScope *scope,
                             KvStoreId *id,
                             bool *retTrackOpsToSession)
{
    Status status = StatusOk;

    switch (scope->specifier_case()) {
    case WorkbookScope::kGlobl:
        *id = XidMgr::XidGlobalKvStore;
        break;
    case WorkbookScope::kWorkbook: {
        auto workbookSpec = scope->workbook();
        status = userMgr_->trackOutstandOps(&workbookSpec,
                                            UserMgr::OutstandOps::Inc);
        BailIfFailed(status);

        *retTrackOpsToSession = true;

        status = userMgr_->getKvStoreId(&workbookSpec, id);
        BailIfFailed(status);
        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}
