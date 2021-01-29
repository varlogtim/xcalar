// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>
#include <jansson.h>

#include "primitives/Primitives.h"
#include "util/Uuid.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "kvstore/KvStore.h"
#include "msg/Xid.h"

static constexpr const char *ClusterGenKey = "/sys/clusterGeneration";
static constexpr const char *ClusterUuid = "UUID";
static constexpr const char *ClusterGen = "Generation";
static uint64_t clusterGeneration;
static constexpr const size_t ClusterUuidStrSize = 128;
static char clusterUuidStr[ClusterUuidStrSize];

static const char *moduleName = "ClusterGen";

const char *
getClusterUuidStr()
{
    return (const char *) clusterUuidStr;
}

uint64_t
getClusterGeneration()
{
    return clusterGeneration;
}

Status
addClusterGen()
{
    Status status = StatusOk;
    Uuid clusterUuid;
    std::string uuidStr;
    json_t *clusterGenInfo = NULL;
    json_t *gen = NULL;
    json_t *uuid = NULL;
    int ret;
    char *value = NULL;

    status = uuidGenerate(&clusterUuid);
    BailIfFailedMsg(moduleName, status, "Failed uuidGenerate");

    try {
        uuidStr.append(std::to_string(clusterUuid.buf[0]));
        uuidStr.append(std::to_string(clusterUuid.buf[1]));
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    };

    clusterGenInfo = json_object();
    BailIfNullMsg(clusterGenInfo,
                  StatusNoMem,
                  moduleName,
                  "Failed to add cluster generation: %s",
                  strGetFromStatus(status));

    // cluster generation starts at 1
    clusterGeneration = 1;

    gen = json_string(std::to_string(clusterGeneration).c_str());
    BailIfNullMsg(gen,
                  StatusNoMem,
                  moduleName,
                  "Failed to add cluster generation: %s",
                  strGetFromStatus(status));

    ret = json_object_set_new(clusterGenInfo, ClusterGen, gen);
    if (ret != 0) {
        status = StatusJsonError;
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to add cluster generation: %s",
                        strGetFromStatus(status));
    }
    gen = NULL;

    status = strStrlcpy(clusterUuidStr, uuidStr.c_str(), ClusterUuidStrSize);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to add cluster generation: %s",
                    strGetFromStatus(status));

    uuid = json_string(uuidStr.c_str());
    BailIfNullMsg(uuid,
                  StatusNoMem,
                  moduleName,
                  "Failed to add cluster generation: %s",
                  strGetFromStatus(status));

    ret = json_object_set_new(clusterGenInfo, ClusterUuid, uuid);
    if (ret != 0) {
        status = StatusJsonError;
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to add cluster generation: %s",
                        strGetFromStatus(status));
    }
    uuid = NULL;

    value = json_dumps(clusterGenInfo, 0);
    BailIfNullMsg(value,
                  StatusNoMem,
                  moduleName,
                  "Failed to add cluster generation: %s",
                  strGetFromStatus(status));

    status = KvStoreLib::get()->addOrReplace(XidMgr::XidGlobalKvStore,
                                             ClusterGenKey,
                                             strlen(ClusterGenKey) + 1,
                                             value,
                                             strlen(value) + 1,
                                             true,
                                             KvStoreOptSync);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to add cluster generation: %s",
                    strGetFromStatus(status));

    xSyslog(moduleName,
            XlogInfo,
            "Cluster Uuid '%s' generation %ld",
            clusterUuidStr,
            clusterGeneration);

CommonExit:
    if (gen != NULL) {
        json_decref(gen);
        gen = NULL;
    }
    if (uuid != NULL) {
        json_decref(uuid);
        uuid = NULL;
    }
    if (clusterGenInfo != NULL) {
        json_decref(clusterGenInfo);
        clusterGenInfo = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

Status
lookupClusterGen()
{
    Status status = StatusOk;
    char *value = NULL;
    size_t valueSize = 0;
    json_t *clusterGenInfo = NULL;
    json_t *gen = NULL;
    json_t *uuid = NULL;
    json_error_t err;

    status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                       ClusterGenKey,
                                       &value,
                                       &valueSize);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to lookup cluster generation: %s",
                    strGetFromStatus(status));

    clusterGenInfo = json_loads(value, 0, &err);
    BailIfNullMsg(clusterGenInfo,
                  StatusNoMem,
                  moduleName,
                  "Failed to lookup cluster generation: %s",
                  strGetFromStatus(status));

    gen = json_object_get(clusterGenInfo, ClusterGen);
    BailIfNullMsg(gen,
                  StatusNoMem,
                  moduleName,
                  "Failed to lookup cluster generation: %s",
                  strGetFromStatus(status));

    uuid = json_object_get(clusterGenInfo, ClusterUuid);
    BailIfNullMsg(uuid,
                  StatusNoMem,
                  moduleName,
                  "Failed to lookup cluster generation: %s",
                  strGetFromStatus(status));

    clusterGeneration = (uint64_t) strtoull(json_string_value(gen), NULL, 10);
    if (clusterGeneration == 0 || clusterGeneration == ULLONG_MAX) {
        // XXX TODO May be just delete the key instead of failing cluster
        // boot.
        status = StatusFailed;
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to lookup cluster generation: %s",
                        strGetFromStatus(status));
    }

    status =
        strStrlcpy(clusterUuidStr, json_string_value(uuid), ClusterUuidStrSize);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to lookup cluster generation: %s",
                    strGetFromStatus(status));

    xSyslog(moduleName,
            XlogInfo,
            "Cluster Uuid '%s' generation %ld",
            clusterUuidStr,
            clusterGeneration);

CommonExit:
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

Status
updateClusterGen()
{
    Status status = StatusOk;
    char *value = NULL;
    size_t valueSize = 0;
    json_t *clusterGenInfo = NULL;
    json_t *genNew = NULL;
    int ret;

    status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                       ClusterGenKey,
                                       &value,
                                       &valueSize);
    if (status == StatusKvEntryNotFound) {
        status = addClusterGen();
        goto CommonExit;
    } else if (status == StatusOk) {
        json_t *gen = NULL;
        json_t *uuid = NULL;
        json_error_t err;

        clusterGenInfo = json_loads(value, 0, &err);
        BailIfNullMsg(clusterGenInfo,
                      StatusNoMem,
                      moduleName,
                      "Failed update cluster generation: %s",
                      strGetFromStatus(status));

        gen = json_object_get(clusterGenInfo, ClusterGen);
        BailIfNullMsg(gen,
                      StatusNoMem,
                      moduleName,
                      "Failed update cluster generation: %s",
                      strGetFromStatus(status));

        uuid = json_object_get(clusterGenInfo, ClusterUuid);
        BailIfNullMsg(uuid,
                      StatusNoMem,
                      moduleName,
                      "Failed update cluster generation: %s",
                      strGetFromStatus(status));

        status = strStrlcpy(clusterUuidStr,
                            json_string_value(uuid),
                            ClusterUuidStrSize);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to update cluster generation: %s",
                        strGetFromStatus(status));

        clusterGeneration =
            (uint64_t) strtoull(json_string_value(gen), NULL, 10);
        if (clusterGeneration == 0 || clusterGeneration == ULLONG_MAX) {
            // XXX TODO May be just delete the key instead of failing cluster
            // boot.
            status = StatusFailed;
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed update cluster generation: %s",
                            strGetFromStatus(status));
        }

        ret = json_object_del(clusterGenInfo, ClusterGen);
        if (ret != 0) {
            status = StatusJsonError;
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed to update cluster generation: %s",
                            strGetFromStatus(status));
        }

        clusterGeneration++;
        genNew = json_string(std::to_string(clusterGeneration).c_str());
        BailIfNullMsg(genNew,
                      StatusNoMem,
                      moduleName,
                      "Failed to update cluster generation: %s",
                      strGetFromStatus(status));

        ret = json_object_set_new(clusterGenInfo, ClusterGen, genNew);
        if (ret != 0) {
            status = StatusJsonError;
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed to update cluster generation: %s",
                            strGetFromStatus(status));
        }
        genNew = NULL;

        memFree(value);
        value = json_dumps(clusterGenInfo, 0);
        BailIfNullMsg(value,
                      StatusNoMem,
                      moduleName,
                      "Failed to update cluster generation: %s",
                      strGetFromStatus(status));

        // XXX TODO may be add compare and swap here instead of just update.
        status = KvStoreLib::get()->addOrReplace(XidMgr::XidGlobalKvStore,
                                                 ClusterGenKey,
                                                 strlen(ClusterGenKey) + 1,
                                                 value,
                                                 strlen(value) + 1,
                                                 true,
                                                 KvStoreOptSync);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to update cluster generation: %s",
                        strGetFromStatus(status));

        xSyslog(moduleName,
                XlogInfo,
                "Cluster Uuid '%s' after update generation %ld",
                clusterUuidStr,
                clusterGeneration);
    } else {
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to lookup %s",
                        strGetFromStatus(status));
    }

CommonExit:
    if (clusterGenInfo != NULL) {
        json_decref(clusterGenInfo);
        clusterGenInfo = NULL;
    }
    if (genNew != NULL) {
        json_decref(genNew);
        genNew = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}
