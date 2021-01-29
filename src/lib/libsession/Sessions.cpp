// Copyright 2015 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//

#include <new>
#include <jansson.h>
#include <cstdlib>
#include <stdio.h>
#include <pthread.h>
#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <dirent.h>
#include <google/protobuf/util/json_util.h>

#include "StrlFunc.h"
#include "limits.h"
#include "primitives/Primitives.h"
#include "log/Log.h"
#include "libapis/LibApisCommon.h"
#include "util/MemTrack.h"
#include "msg/Message.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "dag/DagTypes.h"
#include "libapis/LibApisRecv.h"
#include "sys/XLog.h"
#include "kvstore/KvStore.h"
#include "session/Sessions.h"
#include "strings/String.h"
#include "stat/Statistics.h"
#include "queryeval/QueryEvaluate.h"
#include "queryparser/QueryParser.h"
#include "msg/Xid.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "SessionTypes.h"
#include "usr/Users.h"
#include "udf/UserDefinedFunction.h"
#include "app/AppMgr.h"
#include "common/Version.h"
#include "dataset/Dataset.h"

static constexpr const char *moduleName = "Sessions";

SessionMgr *SessionMgr::instance = NULL;

SessionMgr *
SessionMgr::get()
{
    return instance;
}

Status
SessionMgr::init()
{
    Status status = StatusOk;
    StatsLib *statsLib = StatsLib::get();

    instance = (SessionMgr *) memAllocExt(sizeof(SessionMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) SessionMgr();

    // Generate the name for the directory for the saved sessions to be used by
    // the readSessionsProtobuf function. This directory path is now needed
    // only for upgrade to Dionysus (pre-Dio systems had this directory).
    // XXX: This dir path can be deleted in Eos

    snprintf(instance->sessionPath_,
             sizeof(instance->sessionPath_),
             "%s/%s",
             XcalarConfig::get()->xcalarRootCompletePath_,
             LogLib::get()->getSessionDirName());
    status = statsLib->initNewStatGroup(moduleName,
                                        &instance->sessionStatGroupId_,
                                        StatCount);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&instance->stats.writtenCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->sessionStatGroupId_,
                                         "written.count",
                                         instance->stats.writtenCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&instance->stats.writtenFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->sessionStatGroupId_,
                                         "written.failures",
                                         instance->stats.writtenFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
CommonExit:
    if (status != StatusOk) {
        if (instance != NULL) {
            instance->destroy();
        }
    }
    return status;
}

// Utility function called by UserMgr::shutdown to persist sessions
// during shutdown
void
SessionMgr::Session::shutdownSessUtil(char *userName, ShutDown *sdType)
{
    Status status = StatusUnknown;
    DagLib *dagLib = DagLib::get();
    Dag *dagGone = (Dag *) DagTypes::DagIdInvalid;

    // If the session's DAG is in the rebuilt state, call cleanup to force all
    // the objects the session owns to be dropped.
    if (sessionActive_ == false) {
        *sdType = ShutDown::Inactive;
        if (sessionDag_ != NULL) {  // paused session
            untrackResultsets();
            status = dagLib->destroyDag(sessionDag_,
                                        DagTypes::DestroyDeleteAndCleanNodes);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Session shutdown: Destroy DAG Usr %s"
                        " Session %lX %s failed: %s",
                        userName,
                        sessionPersistedData_.sessionId,
                        sessionPersistedData_.sessionName,
                        strGetFromStatus(status));
            }
            sessionDag_ = dagGone;
            *sdType = ShutDown::InactiveWithDagClean;
        }
    } else {
        *sdType = ShutDown::Active;
        clock_gettime(CLOCK_REALTIME,
                      &sessionPersistedData_.sessionLastChangedTime);

        status = writeSession(ExistSession);
        if (status != StatusOk) {
            assert(0 && "Failed to make sessions durable");
            xSyslog(moduleName,
                    XlogErr,
                    "Session shutdown: Write Session Usr %s"
                    " Session %lX %s failed: %s",
                    userName,
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
        }
        untrackResultsets();
        status = dagLib->destroyDag(sessionDag_,
                                    DagTypes::DestroyDeleteAndCleanNodes);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Session shutdown: Destroy DAG Usr %s"
                    " Session %lX %s failed: %s",
                    userName,
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
        }
        sessionDag_ = dagGone;
        KvStoreLib::get()->close(sessionKvStoreId_, KvStoreCloseOnly);
    }
}

void
SessionMgr::Session::destroySessUtil()
{
    // Turns out we need to close the log files here
    // because the log shutdown logic depends on the logHandles
    // being not freed, which we are freeing here
    if (sessionLogHandle_ != NULL) {
        LogLib::get()->close(sessionLogHandle_);
        memFree(sessionLogHandle_);
        sessionLogHandle_ = NULL;
    }
    if (sessionLogFilePrefix_ != NULL) {
        memFree(sessionLogFilePrefix_);
    }
}

void
SessionMgr::destroy()
{
    this->~SessionMgr();
    memFree(instance);
    instance = NULL;
}

//
// On opening a workbook's existing KVS, the value of its version key is
// supplied to this routine - which validates the version info and triggers
// upgrade (of the workbook KVS contents) if needed.
//
Status
SessionMgr::Session::validateKvsVerAndUpgrade(char *wbVerKeyValue)
{
    Status status = StatusOk;
    json_error_t err;
    json_t *versionInfoJson = NULL;
    json_t *wbVerJson = NULL;
    unsigned wkbookVer = 0;

    versionInfoJson = json_loads(wbVerKeyValue, 0, &err);
    if (versionInfoJson == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "validateKvsVerAndUpgrade failed for session ID %lX, "
                "name '%s', on key value '%s': "
                "source %s line %d, column %d, position %d, '%s': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                wbVerKeyValue,
                err.source,
                err.line,
                err.column,
                err.position,
                err.text,
                strGetFromStatus(status));
        goto CommonExit;
    }
    wbVerJson = json_object_get(versionInfoJson, WkbookVersionKey);
    assert(json_is_string(wbVerJson));
    if (wbVerJson == NULL || !json_is_string(wbVerJson)) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "sessionKvsCreateOrOpen for session ID %lX, name %s failed "
                "due to bad workbook version from key '%s': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                WkbookVersionKey,
                strGetFromStatus(status));
        goto CommonExit;
    }

    wkbookVer = strtoul(json_string_value(wbVerJson), NULL, 0);

    // So far, no upgrade scenarios in this code path. In the future,
    // the WorkbookVersionCur may have been bumped up to be larger than
    // wkbookVer, in which case the bump should've added upgrade code
    // here if needed

    assert(wkbookVer == WorkbookVersionCur);

    if (wkbookVer == 0 || wkbookVer != WorkbookVersionCur) {
        status = StatusWorkbookInvalidVersion;
        xSyslog(moduleName,
                XlogErr,
                "sessionKvsCreateOrOpen for session ID %lX, name %s failed "
                "due to bad workbook version '%d': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                wkbookVer,
                strGetFromStatus(status));
        goto CommonExit;
    }
    // FUTURE UPGRADE CODE to UPGRADE IF NEEDED, CAN GO HERE
    // if (wkbookVer < WorkbookVersionCur) {
    //     0 get list of all keys in this workbook's KVS
    //     1 for (each key) {
    //         - lookup key in this workbook's KVS to get its value
    //         - add key,value strings to a JSON array object
    //       }
    //     2 json_dumps() on the json object -> string_of_kv_pairs
    //     3 send "string_of_kv_pairs" to expServer
    //       (done using python app similar to
    //       scripts/queryToDF2Upgrade.py)
    //     4 expServer returns "upgraded_string_of_kv_pairs". It does
    //       upgrade by first inspecting the versionInfo key in the
    //       list of kv pairs it receives, and uses that to decide what
    //       and how to upgrade
    //     5 for (each key in "upgraded_string_of_kv_pairs") {
    //         - update key's value in this workbook's KVS
    //       }
    //
    //  NOTE: steps 3,4,5 can be done by a scheme similar to that in
    //  queryToDF2Upgrade(). Instead, a "dataflowUpgrade()" routine
    //  can be supplied the "string_of_kv_pairs" from step 2, which
    //  would then invoke a new app (say scripts/dataflowUpgrade.py)
    //  which, similarly to scripts/queryToDF2Upgrade.py, would send
    //  the supplied string to a new expServer REST end-point (say,
    //  "service/upgradeDataflow" instead of "service/upgradeQuery").
    //  This new endpoint would look for the special versionInfo key
    //  in the list of kv-pairs it receives, and uses that to decide
    //  what/how to upgrade, and returns the
    //  "upgraded_string_of_kv_pairs" (step 4 above)
    // }

CommonExit:
    if (versionInfoJson) {
        json_decref(versionInfoJson);
        versionInfoJson = NULL;
    }
    return status;
}

// When creating a workbook's KVS store, it must be stamped with the workbook's
// version by adding the version information as value to the supplied key.
Status
SessionMgr::Session::addVersionKeyToKvs(char *wbVerKeyName)
{
    Status status = StatusOk;
    json_t *versionInfoJson = NULL;
    json_t *xcVerJson = NULL;
    char wbVerStr[SessionMgr::MaxTextHexIdSize];
    json_t *wbVerJson = NULL;
    char *versionInfoStr = NULL;
    KvStoreLib *kvs = KvStoreLib::get();
    int ret;

    versionInfoJson = json_object();
    BailIfNull(versionInfoJson);

    xcVerJson = json_string(versionGetStr());
    BailIfNull(xcVerJson);

    ret = json_object_set_new(versionInfoJson, XcVersionKey, xcVerJson);
    BailIfFailedWith(ret, StatusJsonError);
    xcVerJson = NULL;  // ref passed to versionInfoJson

    snprintf(wbVerStr, sizeof(wbVerStr), "%d", WorkbookVersionCur);
    wbVerJson = json_string(wbVerStr);
    BailIfNull(wbVerJson);

    ret = json_object_set_new(versionInfoJson, WkbookVersionKey, wbVerJson);
    BailIfFailedWith(ret, StatusJsonError);
    wbVerJson = NULL;  // ref passed to versionInfoJson

    versionInfoStr =
        json_dumps(versionInfoJson, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    BailIfNull(versionInfoStr);

    if (versionInfoStr == NULL) {
        status = StatusWorkbookInvalidVersion;
        goto CommonExit;
    }
    status = kvs->addOrReplace(sessionKvStoreId_,
                               wbVerKeyName,
                               strlen(wbVerKeyName) + 1,
                               versionInfoStr,
                               strlen(versionInfoStr) + 1,
                               true,
                               KvStoreOptSync);
    BailIfFailed(status);

CommonExit:
    if (versionInfoJson) {
        json_decref(versionInfoJson);
        versionInfoJson = NULL;
    }
    if (xcVerJson) {
        json_decref(xcVerJson);
        xcVerJson = NULL;
    }
    if (wbVerJson) {
        json_decref(wbVerJson);
        wbVerJson = NULL;
    }
    if (versionInfoStr) {
        memFree(versionInfoStr);
        versionInfoStr = NULL;
    }
    return status;
}

// Each session has an associated KvStore to store session-specific values. Open
// or create the KvStore for this session. This assumes the session is only
// active on a single node at any given time, which is currently enforced by
// associating the node number with the session when it is active. If the
// session's KvStore is being created in this call, it must be stamped with the
// workbook version; if the call is merely opening it, the version key in it
// must be validated and upgrade of the KVS contents triggered if needed.

Status
SessionMgr::Session::sessionKvsCreateOrOpen()
{
    Status status;
    Xid xid;
    char kvStoreName[SessionMgr::MaxSessionKvStoreNameLen + 1];
    KvStoreLib *kvs = KvStoreLib::get();
    char wbVerKeyName[XcalarApiMaxKeyLen + 1];
    int ret;
    char *wbVerKeyValue = NULL;
    size_t valueSize = 0;

    // XXX Remove dependence on UsrId here? UUID is a great way to make the name
    // unique.
    // Session name is based off session ID so that persisted data for session
    // is restored once this session is opened at a later time on another node.
    snprintf(kvStoreName,
             sizeof(kvStoreName),
             "Xcalar.kvstore.%s.%lX",
             sessionPersistedData_.sessionOwningUserId,
             sessionPersistedData_.sessionId);

    xid = XidMgr::get()->xidGetNext();
    // Following call creates the KVS if it doesn't exist.
    status = KvStoreLib::get()->open(xid, kvStoreName, KvStoreSession);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "sessionKvsCreateOrOpen for session ID %lX, name %s failed: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    sessionKvStoreId_ = xid;
    // Add version stamp to the session's KVS, if this is newly created
    ret = snprintf(wbVerKeyName,
                   sizeof(wbVerKeyName),
                   "%s/%s/%s-1",
                   KvStoreLib::KvsXCEPrefix,
                   KvStoreLib::KvsXCESessMeta,
                   WorkbookVersionInfoKey);
    if (ret >= (int) sizeof(wbVerKeyName)) {
        status = StatusOverflow;
        xSyslog(moduleName,
                XlogErr,
                "sessionKvsCreateOrOpen for session ID %lX, name %s failed "
                "to create KVS key '%s' for version info: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                wbVerKeyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = kvs->lookup(sessionKvStoreId_,
                         wbVerKeyName,
                         &wbVerKeyValue,
                         &valueSize);

    if (status == StatusKvEntryNotFound) {
        // must be a newly created wkbook KVS so add a version stamp
        status = addVersionKeyToKvs(wbVerKeyName);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "sessionKvsCreateOrOpen for session ID %lX, name %s failed "
                    "to add version info to key '%s' in workbook KVS: %s",
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    wbVerKeyName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (status == StatusOk) {
        // version key present - so validate and upgrade if needed.
        status = validateKvsVerAndUpgrade(wbVerKeyValue);
        BailIfFailed(status);
    } else {
        // error other than key-not-found, in KVS lookup
        xSyslog(moduleName,
                XlogErr,
                "sessionKvsCreateOrOpen for session ID %lX, name %s failed "
                "to lookup version info key '%s' in workbook KVS: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                wbVerKeyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (wbVerKeyValue) {
        memFree(wbVerKeyValue);
        wbVerKeyValue = NULL;
    }
    return status;
}

void
SessionMgr::parseSessionIdFromPrefix(const char *sessionLogPrefix,
                                     uint64_t *sessionId)
{
    char *sessionIdStrStart;
    char *sessionIdStrEnd;

    // XXX No need for UsrName in the sessions file name. UUID is a prefectly
    // reasonable way to make these unique?
    // Format of sessionLogPrefix Xcalar.session.<uname>.<sessId>-
    sessionIdStrStart = (char *) rindex(sessionLogPrefix, '.');
    if (sessionIdStrStart == NULL) {
        assert(sessionIdStrStart != NULL);
        *sessionId = 0;
        return;
    }
    sessionIdStrStart++;  // Advance to start of <sessId>.
    assert(*sessionIdStrStart != '\0');

    *sessionId = strtoull(sessionIdStrStart, &sessionIdStrEnd, 0x10);
}

// Deserialize session metadata into C (into SessionPersistedData_) or into
// JSON (returned in jsonStrOut), with the KVS key for the session specified
// via first param (gKvsKey), or constructed using session ID/user-name.
// The 'userLen' param is used if gKvsKey is supplied, to extract the session
// ID from the key, for validation purposes.
Status
SessionMgr::Session::deserialize(size_t userLen,
                                 char *gKvsKey,
                                 char **jsonStrOut)
{
    Status status = StatusOk;
    char keyName[XcalarApiMaxKeyLen + 1];
    KvStoreLib *kvs = KvStoreLib::get();
    char *value = NULL;
    size_t valueSize = 0;
    json_error_t err;
    json_t *sessMetaJson = NULL;
    json_t *verJson = NULL;
    json_t *magicJson = NULL;
    json_t *sessIdJson = NULL;
    json_t *sessUserJson = NULL;
    json_t *sessCrTimeJson = NULL;
    json_t *sessModTimeJson = NULL;
    json_t *sessRefTimeJson = NULL;
    json_t *sessNameJson = NULL;
    json_t *tvSecJson = NULL;
    json_t *tvNsecJson = NULL;
    unsigned wkbookVer = 0;
    uint64_t magic = 0;
    uint64_t sessIdFromKey = 0;
    uint64_t sessIdFromDisk = 0;
    char sessIdStr[SessionMgr::MaxTextHexIdSize];

    if (gKvsKey == NULL) {
        // For download use case; the session metadata is deserialized from disk
        // into the jsonStrOut. Note that the in-core C-struct should have the
        // same info, but the on-disk state is already in json so this is
        // convenient.
        //
        // The sessionOwningUserId/sessionId are of course valid, having been
        // populated earlier via the deserialize call with the supplied key.

        status = sessionMetaKvsName(sessionPersistedData_.sessionOwningUserId,
                                    sessionPersistedData_.sessionId,
                                    keyName,
                                    sizeof(keyName));
        BailIfFailed(status);
        status =
            kvs->lookup(XidMgr::XidGlobalKvStore, keyName, &value, &valueSize);
        BailIfFailed(status);
    } else {
        // For first instance of deserialization of session metadata from disk
        // after re-boot - the only information that's available is the gKvsKey
        // so sessIdFromKey is extracted for validation of the on-disk session
        // ID
        status =
            kvs->lookup(XidMgr::XidGlobalKvStore, gKvsKey, &value, &valueSize);
        BailIfFailed(status);
        // see SessionMgr::Session::sessionMetaKvsName() - it uses the format:
        // /<KvStoreLib::KvsXCEPrefix>/<KvStoreLib::KvsXCESessMeta>/ \
        //     <userName>-<sessionID>-1
        // In the following the first 2 1's are for slashes, and the last for
        // the hyphen separating <userName> and <sessionID> in the name
        // Assumes length of sessionID in the gKvsKey is
        // (SessionMgr::MaxTextHexIdSize - 1)
        assert(userLen != 0);
        strlcpy(sessIdStr,
                gKvsKey + strlen(KvStoreLib::KvsXCEPrefix) + 1 +
                    strlen(KvStoreLib::KvsXCESessMeta) + 1 + userLen + 1,
                sizeof(sessIdStr));
        sessIdFromKey = strtoull(sessIdStr, NULL, 16);
    }
    if (jsonStrOut) {
        // De-serialize into JSON (typically for download)
        assert(*jsonStrOut == NULL);
        *jsonStrOut = value;
        value = NULL;
    } else {
        sessMetaJson = json_loads(value, 0, &err);
        if (sessMetaJson == NULL) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from %s global KVS key failed, "
                    "source %s line %d, column %d, position %d, '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    err.source,
                    err.line,
                    err.column,
                    err.position,
                    err.text,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        verJson = json_object_get(sessMetaJson, WkbookVersionKey);
        assert(json_is_string(verJson));
        if (verJson == NULL || !json_is_string(verJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "bad workbook version from key '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    WkbookVersionKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        wkbookVer = strtol(json_string_value(verJson), NULL, 0);

        // So far, no upgrade scenarios in this code path. In the future, the
        // WorkbookVersionCur may have been bumped up to be larger than
        // wkbookVer, in which case the bump should've added upgrade code here
        // if needed
        if (wkbookVer == 0 || wkbookVer != WorkbookVersionCur) {
            status = StatusWorkbookInvalidVersion;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "bad workbook version %d: %s",
                    gKvsKey ? gKvsKey : keyName,
                    wkbookVer,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        assert(wkbookVer == WorkbookVersionCur);

        // FUTURE UPGRADE CODE to UPGRADE SESS METADATA, IF NEEDED, CAN GO HERE
        //
        // if (wkbookVer < WorkbookVersionCur) {
        //     json_t *newMetaJson = NULL;
        //
        //     doSessMdataUpgrade(wkbookVer, sessMetaJson, &newMetaJson);
        //     json_decref(sessMetaJson);
        //     sessMetaJson = newMetaJson;
        //     newMetaJson = NULL;
        // }

        // magic
        magicJson = json_object_get(sessMetaJson, MagicKey);
        assert(json_is_string(magicJson));
        if (magicJson == 0 || !json_is_string(magicJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad magic '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    MagicKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        magic = strtoull(json_string_value(magicJson), NULL, 16);
        assert(magic == SessionMagicV2);
        if (magic != SessionMagicV2) {
            status = StatusSessMdataInconsistent;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "inconsistent magic '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    MagicKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // sessId
        sessIdJson = json_object_get(sessMetaJson, SessionIdKey);
        assert(json_is_string(sessIdJson));
        if (sessIdJson == 0 || !json_is_string(sessIdJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad session ID '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    SessionIdKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessIdFromDisk = strtoull(json_string_value(sessIdJson), NULL, 16);
        assert(gKvsKey == NULL || sessIdFromKey == sessIdFromDisk);
        if (gKvsKey && sessIdFromKey != sessIdFromDisk) {
            status = StatusSessMdataInconsistent;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "inconsistent session ID - expected 0x%lX, is 0x%lX: %s",
                    gKvsKey,
                    sessIdFromKey,
                    sessIdFromDisk,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionId = sessIdFromDisk;

        // session name
        sessNameJson = json_object_get(sessMetaJson, SessionNameKey);
        assert(sessNameJson && json_is_string(sessNameJson));
        if (sessNameJson == 0 || !json_is_string(sessNameJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad session name '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    SessionNameKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        const char *nameOut = json_string_value(sessNameJson);
        verify(strlcpy(sessionPersistedData_.sessionName,
                       nameOut,
                       sizeof(sessionPersistedData_.sessionName)) ==
               strlen(nameOut));

        // session user name
        sessUserJson = json_object_get(sessMetaJson, SessionUserKey);
        assert(sessUserJson && json_is_string(sessUserJson));
        if (sessUserJson == 0 || !json_is_string(sessUserJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad user name '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    SessionUserKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        const char *userNameOut = json_string_value(sessUserJson);
        verify(strlcpy(sessionPersistedData_.sessionOwningUserId,
                       userNameOut,
                       sizeof(sessionPersistedData_.sessionOwningUserId)) ==
               strlen(userNameOut));

        // time stamps
        //  cr time
        sessCrTimeJson = json_object_get(sessMetaJson, SessionCrTimeKey);
        assert(json_is_object(sessCrTimeJson));
        if (sessCrTimeJson == 0 || !json_is_object(sessCrTimeJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad create time stamp '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    SessionCrTimeKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        tvSecJson = json_object_get(sessCrTimeJson, TvSecKey);
        assert(json_is_string(tvSecJson));
        if (tvSecJson == 0 || !json_is_string(tvSecJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad create time seconds '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    TvSecKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionCreateTime.tv_sec =
            strtol(json_string_value(tvSecJson), NULL, 0);

        tvNsecJson = json_object_get(sessCrTimeJson, TvNsecKey);
        assert(json_is_string(tvNsecJson));
        if (tvNsecJson == 0 || !json_is_string(tvNsecJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad create time nsecs '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    TvNsecKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionCreateTime.tv_nsec =
            strtol(json_string_value(tvNsecJson), NULL, 0);

        // mod time
        sessModTimeJson = json_object_get(sessMetaJson, SessionModTimeKey);
        assert(json_is_object(sessModTimeJson));
        if (sessModTimeJson == 0 || !json_is_object(sessModTimeJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad mod time stamp '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    SessionModTimeKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        tvSecJson = json_object_get(sessModTimeJson, TvSecKey);
        assert(json_is_string(tvSecJson));
        if (tvSecJson == 0 || !json_is_string(tvSecJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad mod time seconds '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    TvSecKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionLastChangedTime.tv_sec =
            strtol(json_string_value(tvSecJson), NULL, 0);

        tvNsecJson = json_object_get(sessModTimeJson, TvNsecKey);
        assert(json_is_string(tvNsecJson));
        if (tvNsecJson == 0 || !json_is_string(tvNsecJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad mod time nsecs '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    TvNsecKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionLastChangedTime.tv_nsec =
            strtol(json_string_value(tvNsecJson), NULL, 0);

        // ref time
        sessRefTimeJson = json_object_get(sessMetaJson, SessionRefTimeKey);
        assert(json_is_object(sessRefTimeJson));
        if (sessRefTimeJson == 0 || !json_is_object(sessRefTimeJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad ref time stamp '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    SessionRefTimeKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        tvSecJson = json_object_get(sessRefTimeJson, TvSecKey);
        assert(json_is_string(tvSecJson));
        if (tvSecJson == 0 || !json_is_string(tvSecJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad ref time seconds '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    TvSecKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionLastUsedTime.tv_sec =
            strtol(json_string_value(tvSecJson), NULL, 0);

        tvNsecJson = json_object_get(sessRefTimeJson, TvNsecKey);
        assert(json_is_string(tvNsecJson));
        if (tvNsecJson == 0 || !json_is_string(tvNsecJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize from '%s' global KVS key failed due to "
                    "missing or bad ref time nsecs '%s': %s",
                    gKvsKey ? gKvsKey : keyName,
                    TvNsecKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionPersistedData_.sessionLastUsedTime.tv_nsec =
            strtol(json_string_value(tvNsecJson), NULL, 0);

        // now that session should have the username and sessionName, init the
        // udf container which needs these names
        status = initUdfContainer();
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed deserialize for session Usr %s name %s on creating "
                    "UDF container: %s",
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
        }
    }

CommonExit:
    if (sessMetaJson) {
        json_decref(sessMetaJson);
        sessMetaJson = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

// Construct the key in global KVS for a session (given its ID and user-name)
Status
SessionMgr::Session::sessionMetaKvsName(const char *userName,
                                        uint64_t sessionId,
                                        char *sessMetaKvsName,
                                        size_t maxSessMetaKvsName)
{
    Status status = StatusOk;
    int ret;

    ret = snprintf(sessMetaKvsName,
                   maxSessMetaKvsName,
                   "%s/%s/%s-%lX-1",
                   KvStoreLib::KvsXCEPrefix,
                   KvStoreLib::KvsXCESessMeta,
                   userName,
                   sessionId);
    if (ret >= (int) maxSessMetaKvsName) {
        status = StatusOverflow;
        goto CommonExit;
    }

CommonExit:

    return status;
}

// Serialize session metadata into global KVS. The uploadStr param is supplied
// only during workbook-upload, so that the session metadata from the workbook
// can be upgraded before being serialized to the KVS.
Status
SessionMgr::Session::serialize(char *uploadStr)
{
    Status status = StatusOk;
    int ret;
    char keyName[XcalarApiMaxKeyLen + 1];
    KvStoreLib *kvs = KvStoreLib::get();
    char *value = NULL;
    size_t valueSize = 0;
    char *sessMeta = NULL;
    json_t *json_packed = NULL;
    json_t *sessMetaJson = NULL;
    json_t *crTimeJson = NULL, *modTimeJson = NULL, *refTimeJson = NULL;
    json_t *tvSecJson = NULL, *tvNsecJson = NULL;
    json_t *magicJson = NULL, *sessIdJson = NULL, *sessUserJson = NULL;
    json_t *sessNameJson = NULL;
    json_t *uploadJson = NULL;
    json_error_t err;
    json_t *verJson = NULL;
    json_t *xcVerJson = NULL;
    json_t *wkbkVerJson = NULL;
    char timeStr[SessionMgr::MaxTextHexIdSize];
    char wbVerStr[SessionMgr::MaxTextHexIdSize];
    char magicStr[SessionMgr::MaxTextHexIdSize];
    char sessIdStr[SessionMgr::MaxTextHexIdSize];
    unsigned wbVer = 0;
    uint64_t sessIdInKvs;
    uint64_t magicInKvs;

    xSyslog(moduleName,
            XlogInfo,
            "Serializing session metadata for '%s'",
            sessionPersistedData_.sessionName);
    sessionPersistedData_.magic = SessionMagicV2;
    status = sessionMetaKvsName(sessionPersistedData_.sessionOwningUserId,
                                sessionPersistedData_.sessionId,
                                keyName,
                                sizeof(keyName));
    BailIfFailed(status);
    status = kvs->lookup(XidMgr::XidGlobalKvStore, keyName, &value, &valueSize);
    if (status == StatusOk) {
        // validate/sanity-check the session metadata key if it exists:
        // (sessionId, userId must be the same; version and magic must have
        // been upgraded already via deserialize() invoked during UserMgr::list,
        // so they must be equal to the current version/magic - otherwise there
        // is something very wrong. Basically, if the version/magic aren't as
        // expected, we need to fail the attempt to serialize, which will
        // over-write the old contents (instead of upgrading it, potentially
        // causing loss of metadata and therefore availability to data).
        sessMetaJson = json_loads(value, 0, &err);
        if (sessMetaJson == NULL) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s'; source %s line %d, column %d, position %d, '%s': %s",
                    keyName,
                    err.source,
                    err.line,
                    err.column,
                    err.position,
                    err.text,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // wb version
        verJson = json_object_get(sessMetaJson, WkbookVersionKey);
        assert(json_is_string(verJson));
        if (verJson == NULL || !json_is_string(verJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' due to bad workbook version from key '%s': %s",
                    keyName,
                    WkbookVersionKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        wbVer = strtol(json_string_value(verJson), NULL, 0);
        assert(wbVer == WorkbookVersionCur);
        if (wbVer == 0 || wbVer != WorkbookVersionCur) {
            status = StatusWorkbookInvalidVersion;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' due to bad existing workbook version %d: %s",
                    keyName,
                    wbVer,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // magic
        magicJson = json_object_get(sessMetaJson, MagicKey);
        assert(json_is_string(magicJson));
        if (magicJson == NULL || !json_is_string(magicJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' due to bad magic from key '%s': %s",
                    keyName,
                    MagicKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        magicInKvs = strtoull(json_string_value(magicJson), NULL, 16);
        assert(magicInKvs == SessionMagicV2);
        if (magicInKvs != SessionMagicV2) {
            status = StatusSessMdataInconsistent;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' expected magic 0x%lX but is 0x%lX: %s",
                    keyName,
                    SessionMagicV2,
                    magicInKvs,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // session ID
        sessIdJson = json_object_get(sessMetaJson, SessionIdKey);
        assert(json_is_string(sessIdJson));
        if (sessIdJson == 0 || !json_is_string(sessIdJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' due to missing or bad session ID '%s': %s",
                    keyName,
                    SessionIdKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessIdInKvs = strtoll(json_string_value(sessIdJson), NULL, 16);
        if (sessIdInKvs != sessionPersistedData_.sessionId) {
            status = StatusSessMdataInconsistent;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' expected session ID 0x%lX but is 0x%lX: %s",
                    keyName,
                    sessionPersistedData_.sessionId,
                    sessIdInKvs,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // user
        sessUserJson = json_object_get(sessMetaJson, SessionUserKey);
        assert(sessUserJson && json_is_string(sessUserJson));
        if (sessUserJson == 0 || !json_is_string(sessUserJson)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' due to missing or bad user name '%s': %s",
                    keyName,
                    SessionUserKey,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        const char *userNameOut = json_string_value(sessUserJson);
        if (strncmp(userNameOut,
                    sessionPersistedData_.sessionOwningUserId,
                    sizeof(sessionPersistedData_.sessionOwningUserId)) != 0) {
            status = StatusSessMdataInconsistent;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize failed sanity check on global KVS key: "
                    "'%s' expected user name '%s' but is '%s': %s",
                    keyName,
                    sessionPersistedData_.sessionOwningUserId,
                    userNameOut,
                    strGetFromStatus(status));
            goto CommonExit;
        }

    } else if (status != StatusKvEntryNotFound) {
        // lookup failure can't be other than StatusKvEntryNotFound
        xSyslog(moduleName,
                XlogErr,
                "Session serialize unexpected error on KVS key '%s' lookup: %s",
                keyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;  // clear KVS key not found error

    if (uploadStr) {
        uploadJson = json_loads(uploadStr, 0, &err);
        if (uploadJson == NULL) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Session serialize KVS key '%s' failed during upload: %s",
                    keyName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        verJson = json_object_get(uploadJson, WkbookVersionKey);

        // either version is absent (pre-Dionysus), or it must be current
        // version. In the future, upgrade code may need to be added if version
        // is < WorkbookVersionCur, and there's need to upgrade from the old
        // version. Currently, this isn't needed - so just assert that version,
        // if it exists in uploadStr, must match current workbook version!

        if (verJson) {
            wbVer = strtol(json_string_value(verJson), NULL, 0);
            // *************************************************************
            // NOTE: if following assert fails, you may need to add upgrade
            // code here to upgrade the contents of "uploadStr" which are at
            // version 'wbVer' (which must be less than WorkbookVersionCur if
            // the assert failed).
            // *************************************************************
            assert(wbVer == WorkbookVersionCur);
            if (wbVer != WorkbookVersionCur) {
                status = StatusWorkbookInvalidVersion;
                xSyslog(moduleName,
                        XlogErr,
                        "Session serialize KVS key '%s' failed during upload: "
                        "%s",
                        keyName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            // Uploading session metadata without a version; so add one.
            // This is basically akin to upgrading 'uploadStr' to get a version
            xcVerJson = json_string(versionGetStr());
            BailIfNull(xcVerJson);

            ret = json_object_set_new(uploadJson, XcVersionKey, xcVerJson);
            if (ret != 0) {
                status = StatusNoMem;
                xSyslog(moduleName,
                        XlogErr,
                        "Session serialize KVS key '%s' failed to add Xcalar "
                        "version during upload: %s",
                        keyName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            xcVerJson = NULL;

            snprintf(wbVerStr, sizeof(wbVerStr), "%d", WorkbookVersionCur);
            wkbkVerJson = json_string(wbVerStr);
            BailIfNull(wkbkVerJson);

            ret =
                json_object_set_new(uploadJson, WkbookVersionKey, wkbkVerJson);
            if (ret != 0) {
                status = StatusNoMem;
                xSyslog(moduleName,
                        XlogErr,
                        "Session serialize KVS key '%s' failed to add workbook "
                        "version during upload: %s",
                        keyName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            wkbkVerJson = NULL;
        }

        snprintf(magicStr, sizeof(magicStr), "%lx", SessionMagicV2);

        magicJson = json_string(magicStr);
        BailIfNull(magicJson);

        snprintf(sessIdStr,
                 sizeof(sessIdStr),
                 "%lx",
                 sessionPersistedData_.sessionId);

        sessIdJson = json_string(sessIdStr);
        BailIfNull(sessIdJson);

        sessUserJson = json_string(sessionPersistedData_.sessionOwningUserId);
        BailIfNull(sessUserJson);

        sessNameJson = json_string(sessionPersistedData_.sessionName);
        BailIfNull(sessNameJson);

        ret = json_object_set_new(uploadJson, MagicKey, magicJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        magicJson = NULL;

        ret = json_object_set_new(uploadJson, SessionIdKey, sessIdJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        sessIdJson = NULL;

        ret = json_object_set_new(uploadJson, SessionUserKey, sessUserJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        sessUserJson = NULL;

        ret = json_object_set_new(uploadJson, SessionNameKey, sessNameJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        sessNameJson = NULL;

        sessMeta = json_dumps(uploadJson, JSON_INDENT(4) | JSON_ENSURE_ASCII);
        BailIfNull(sessMeta);
    } else {
        // create time
        crTimeJson = json_object();
        BailIfNull(crTimeJson);

        snprintf(timeStr,
                 sizeof(timeStr),
                 "%ld",
                 sessionPersistedData_.sessionCreateTime.tv_sec);
        tvSecJson = json_string(timeStr);
        BailIfNull(tvSecJson);

        snprintf(timeStr,
                 sizeof(timeStr),
                 "%ld",
                 sessionPersistedData_.sessionCreateTime.tv_nsec);
        tvNsecJson = json_string(timeStr);
        BailIfNull(tvNsecJson);

        ret = json_object_set_new(crTimeJson, TvSecKey, tvSecJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        tvSecJson = NULL;

        ret = json_object_set_new(crTimeJson, TvNsecKey, tvNsecJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        tvNsecJson = NULL;

        // mod Time (aka LastChanged)
        modTimeJson = json_object();
        BailIfNull(modTimeJson);

        snprintf(timeStr,
                 sizeof(timeStr),
                 "%ld",
                 sessionPersistedData_.sessionLastChangedTime.tv_sec);
        tvSecJson = json_string(timeStr);

        snprintf(timeStr,
                 sizeof(timeStr),
                 "%ld",
                 sessionPersistedData_.sessionLastChangedTime.tv_nsec);
        tvNsecJson = json_string(timeStr);

        ret = json_object_set_new(modTimeJson, TvSecKey, tvSecJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        tvSecJson = NULL;

        ret = json_object_set_new(modTimeJson, TvNsecKey, tvNsecJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        tvNsecJson = NULL;

        // refTime (aka LastUsed)
        refTimeJson = json_object();
        BailIfNull(refTimeJson);

        snprintf(timeStr,
                 sizeof(timeStr),
                 "%ld",
                 sessionPersistedData_.sessionLastUsedTime.tv_sec);
        tvSecJson = json_string(timeStr);
        BailIfNull(tvSecJson);

        snprintf(timeStr,
                 sizeof(timeStr),
                 "%ld",
                 sessionPersistedData_.sessionLastUsedTime.tv_nsec);
        tvNsecJson = json_string(timeStr);
        BailIfNull(tvNsecJson);

        ret = json_object_set_new(refTimeJson, TvSecKey, tvSecJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        tvSecJson = NULL;

        ret = json_object_set_new(refTimeJson, TvNsecKey, tvNsecJson);
        BailIfFailedWith(ret, StatusJsonSessSerializeError);
        tvNsecJson = NULL;

        snprintf(wbVerStr, sizeof(wbVerStr), "%d", WorkbookVersionCur);
        snprintf(magicStr,
                 sizeof(magicStr),
                 "%lx",
                 sessionPersistedData_.magic);
        snprintf(sessIdStr,
                 sizeof(sessIdStr),
                 "%lx",
                 sessionPersistedData_.sessionId);

        // pack all together
        json_packed = json_pack_ex(&err,
                                   0,
                                   JsonPackFormatString,
                                   XcVersionKey,
                                   versionGetStr(),
                                   WkbookVersionKey,
                                   wbVerStr,
                                   MagicKey,
                                   magicStr,
                                   SessionIdKey,
                                   sessIdStr,
                                   SessionUserKey,
                                   sessionPersistedData_.sessionOwningUserId,
                                   SessionCrTimeKey,
                                   crTimeJson,
                                   SessionModTimeKey,
                                   modTimeJson,
                                   SessionRefTimeKey,
                                   refTimeJson,
                                   SessionNameKey,
                                   sessionPersistedData_.sessionName);

        crTimeJson = modTimeJson = refTimeJson = NULL;
        BailIfNullWith(json_packed, StatusJsonSessSerializeError);

        sessMeta = json_dumps(json_packed, JSON_INDENT(4) | JSON_ENSURE_ASCII);
        BailIfNull(sessMeta);
    }
    status = kvs->addOrReplace(XidMgr::XidGlobalKvStore,
                               keyName,
                               strlen(keyName) + 1,
                               sessMeta,
                               strlen(sessMeta) + 1,
                               true,
                               KvStoreOptSync);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Session serialize addOrReplace failed (global KVS key '%s'): "
                "%s",
                keyName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (xcVerJson) {
        json_decref(xcVerJson);
        xcVerJson = NULL;
    }
    if (wkbkVerJson) {
        json_decref(wkbkVerJson);
        wkbkVerJson = NULL;
    }
    if (magicJson) {
        json_decref(magicJson);
        magicJson = NULL;
    }
    if (sessIdJson) {
        json_decref(sessIdJson);
        sessIdJson = NULL;
    }
    if (sessUserJson) {
        json_decref(sessUserJson);
        sessUserJson = NULL;
    }
    if (sessNameJson) {
        json_decref(sessNameJson);
        sessNameJson = NULL;
    }

    if (crTimeJson) {
        json_decref(crTimeJson);
        crTimeJson = NULL;
    }
    if (modTimeJson) {
        json_decref(modTimeJson);
        modTimeJson = NULL;
    }
    if (refTimeJson) {
        json_decref(refTimeJson);
        refTimeJson = NULL;
    }
    if (tvSecJson) {
        json_decref(tvSecJson);
        tvSecJson = NULL;
    }
    if (tvNsecJson) {
        json_decref(tvNsecJson);
        tvNsecJson = NULL;
    }
    if (uploadJson) {
        json_decref(uploadJson);
        uploadJson = NULL;
    }
    if (json_packed) {
        json_decref(json_packed);
        json_packed = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    if (sessMeta != NULL) {
        memFree(sessMeta);
        sessMeta = NULL;
    }
    return status;
}

static size_t
getUserNameLength(const char *key)
{
    size_t prefixLength = strlen(KvStoreLib::KvsXCEPrefix) + 1 +
                          strlen(KvStoreLib::KvsXCESessMeta) + 1;
    int counter = 0;
    size_t cursorIdx = strlen(key) - 1;
    while (cursorIdx > 0 && counter < 2) {
        if (key[cursorIdx] == '-') {
            counter++;
        }
        cursorIdx--;
    }
    assert(counter == 2 && cursorIdx >= prefixLength);
    return cursorIdx - prefixLength + 1;
}

// Read the session metadata for all sessions belonging to the specified user
// (in userName), from global KVS. Do this by getting all keys with the same
// prefix (with the specified username), and then invoking deserialize() for
// each key, which will update the session with the key's value (i.e. session
// metadata).
Status
SessionMgr::Session::readSessionsKvs(char *userName,
                                     SessionListElt **sessListRet)
{
    Status status = StatusSessionNotFound;
    KvStoreLib *kvs = KvStoreLib::get();
    KvStoreLib::KeyList *keyList = NULL;
    char keyPrefix[XcalarApiMaxKeyLen + 1];
    int ret;
    SessionMgr::Session *thisSession = NULL;
    SessionListElt *sessList = NULL;
    SessionListElt *sessListCurr = NULL;
    bool sessListIncomp = false;

    ret = snprintf(keyPrefix,
                   sizeof(keyPrefix),
                   "%s/%s/%s-",
                   KvStoreLib::KvsXCEPrefix,
                   KvStoreLib::KvsXCESessMeta,
                   userName);
    assert(ret < (int) sizeof(keyPrefix));

    status = kvs->list(XidMgr::XidGlobalKvStore, keyPrefix, &keyList);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session meta list from global KvStore: "
                "%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (int ii = 0; ii < keyList->numKeys; ii++) {
        thisSession = new (std::nothrow) SessionMgr::Session(NULL);
        if (thisSession == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "readSessionsKvs: failure for key '%s': %s",
                    keyList->keys[ii],
                    strGetFromStatus(status));
            sessListIncomp = true;
            continue;
        }

        // SYS-245, in case of the username contains hyphen, the keylist may
        // have wrong match, then we should check the real user name match

        size_t usrNameLen = getUserNameLength(keyList->keys[ii]);
        if (usrNameLen != strlen(userName)) {
            continue;
        }

        status =
            thisSession->deserialize(strlen(userName), keyList->keys[ii], NULL);
        if (status != StatusOk) {
            thisSession->decRef();
            xSyslog(moduleName,
                    XlogErr,
                    "readSessionsKvs: Failed to deserialize key '%s': %s",
                    keyList->keys[ii],
                    strGetFromStatus(status));
            sessListIncomp = true;
            continue;
        }
        // formulate the list of sessions to be returned
        assert(thisSession != NULL);
        sessListCurr = new (std::nothrow) SessionListElt();
        if (sessListCurr == NULL) {
            thisSession->decRef();
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "readSessionsKvs: failed after deserializing key '%s' "
                    ": %s",
                    keyList->keys[ii],
                    strGetFromStatus(status));
            sessListIncomp = true;
            // no issues in reading/deserializing the session; just that
            // the entire list of sessions isn't being returned so mark
            // the session list incomplete
            continue;
        }
        sessListCurr->session = thisSession;
        sessListCurr->next = sessList;
        sessList = sessListCurr;
        // Continue trying to load sessions even if reading fails, or memory
        // allocation fails
    }

    status = StatusOk;
    // If we are going to automatically determine the current active session,
    // this is the place to do it. The chain could be read and the session with
    // the newest changed or used time could be active for this user.

    *sessListRet = sessList;

CommonExit:
    if (keyList != NULL) {
        memFree(keyList);
        keyList = NULL;
    }
    if (sessListIncomp) {
        return StatusSessListIncomplete;
    } else {
        return status;
    }
}

// read all sessions from disk
Status
SessionMgr::Session::readSessions(char *userName, SessionListElt **sessListRet)
{
    Status status = StatusUnknown;

    status = readSessionsKvs(userName, sessListRet);
    return (status);
}

// Write out the session metadata to KVS
Status
SessionMgr::Session::writeSession(bool newSession)
{
    Status status = StatusUnknown;

    if (!newSession &&
        strcmp(sessionPersistedData_.sessionName, "sql-workbook") == 0 &&
        strcmp(sessionPersistedData_.sessionOwningUserId,
               "xcalar-internal-sql") == 0) {
        // Bug 13465: We only persist creation of the internal SQL session to
        // serve as a query execution context for a given XCE instance.
        status = StatusOk;
        goto WriteSessionExit;
    }

    // If any other session related data needs to be saved, do it here and
    // record the size in the sesion block.
    status = serialize(NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Session write: Session %lX %s failed on serialize: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto WriteSessionExit;
    }

    StatsLib::statAtomicIncr64(SessionMgr::get()->stats.writtenCount);

WriteSessionExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed Session write Id: %lX %s, Usr: %s: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionOwningUserId,
                strGetFromStatus(status));
        StatsLib::statAtomicIncr64(SessionMgr::get()->stats.writtenFailures);
    }

    return status;
}

// Drop all the tables and datasets associated with a query graph (DAG). Since
// there is no dgXXXX operation to do this, just destroy the existing.
//
// XXX: Review - if this routine fails, it seems as if
// there's still a change - since caller has already marked the session
// inactive for one. Instead this routine should return a status and caller
// should ensure no change occurs in case of failure...
// XXX: Review following in light of new model in which there's no persisted
// query graph, and there's a failure to destroy the session query graph: is
// it necessary to return a status here?

void
SessionMgr::Session::cleanQgraph()
{
    Status status = StatusUnknown;
    Dag *oldDag = sessionDag_;
    DagLib *dagLib = DagLib::get();

    assert(sessionDag_->inSession() == true);

    sessionDag_ = NULL;

    untrackResultsets();

    // XXX In the future, this should be queued to execute independently
    // Neither of the dags are needed now; destroy them both
    status = dagLib->destroyDag(oldDag, DagTypes::DestroyDeleteAndCleanNodes);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Session cleanQgraph: Destroy DAG Session %lX %s failed: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        // failure to delete oldDag shouldn't fail operation - can be viewed as
        // a leaked resource - the occurrence of which shouldn't be fatal
        status = StatusOk;
    }
}

// Create a new session. A session may be:
// - completely new
// - or it may be forked from an existing session, in which case the old
//   session's UDFs/KVS are cloned and reference counts are increased
// - or it may be born with a workbook file that's being uploaded
Status
SessionMgr::Session::create(XcalarApiUserId *sessionUser,
                            XcalarApiSessionNewInput *sessionInput,
                            SessionUploadPayload *sessionUploadPayload,
                            XcalarApiSessionNewOutput *sessionOutput,
                            SessionMgr::Session *forkedSession)
{
    const char *errorLog = "Unable to save session data";
    const char *errorKvs = "Could not create KVS";
    const char *errorUpload = "Could not upload session";
    Status status = StatusUnknown;
    UserDefinedFunction *udfLib = UserDefinedFunction::get();
    KvStoreLib *kvs = KvStoreLib::get();
    EvalUdfModuleSet udfModules;
    XcalarApiOutput *getFnOutput = NULL;
    size_t getFnOutputSize = 0;
    XcalarApiListXdfsOutput *listXdfsOutput;
    XcalarEval *xcalarEval = XcalarEval::get();
    XcalarApiUdfGetInput udfGetInput;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize = 0;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize = 0;
    bool udfsAdded = false;
    EvalUdfModule *module = NULL;
    UdfModuleSrc *moduleSrc = NULL;
    KvStoreLib::KeyList *keyList = NULL;
    char *value = NULL;
    size_t valueSize;

    assert(sessionUser != NULL);
    assert(sessionInput != NULL);
    assert(sessionOutput != NULL);

    xSyslog(moduleName,
            XlogDebug,
            "Session create for session Usr %s name %s",
            sessionUser->userIdName,
            sessionInput->sessionName);

    if (strlen(sessionInput->sessionName) == 0) {
        status = StatusSessionUsrNameInvalid;  // XXX: Add a new error code
        xSyslog(moduleName,
                XlogErr,
                "Failed session create for Usr %s. No session name! (%s)",
                sessionUser->userIdName,
                strGetFromStatus(status));
        goto SessionCreateExit;
    }

    // The timestamp fields and the session ID derived from the time
    clock_gettime(CLOCK_REALTIME, &(sessionPersistedData_.sessionCreateTime));
    sessionPersistedData_.sessionId =
        (sessionPersistedData_.sessionCreateTime.tv_sec << 32) +
        sessionPersistedData_.sessionCreateTime.tv_nsec;
    // session name must already be present in "thisSession"
    assert(strncmp(sessionPersistedData_.sessionName,
                   sessionInput->sessionName,
                   sizeof(sessionPersistedData_.sessionName)) == 0);

    strlcpy(sessionPersistedData_.sessionOwningUserId,
            sessionUser->userIdName,
            LOGIN_NAME_MAX + 1);

    status = initUdfContainer();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed session create for session Usr %s name %s on creating "
                "UDF container: %s",
                sessionUser->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(status));
        goto SessionCreateExit;
    }

    // create and open kvs for this new workbook after session ID and username
    // are initialized above (needed for the KVS filename)

    status = sessionKvsCreateOrOpen();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed session create for session Usr %s name %s on creating "
                "KVS: %s",
                sessionUser->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(status));
        strlcpy(sessionOutput->error, errorKvs, sizeof(sessionOutput->error));
        goto SessionCreateExit;
    }

    if (sessionUploadPayload != NULL) {
        // workbook file is being uploaded. Extract all the necessary sections
        // from the upload payload - this will create the dag too.
        status = upload(sessionUser, sessionUploadPayload);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed session upload Usr %s name %s: %s",
                    sessionUser->userIdName,
                    sessionInput->sessionName,
                    strGetFromStatus(status));
            strlcpy(sessionOutput->error,
                    errorUpload,
                    sizeof(sessionOutput->error));
            goto SessionCreateExit;
        }
    } else {
        sessionPersistedData_.sessionLastUsedTime =
            sessionPersistedData_.sessionCreateTime;
        sessionPersistedData_.sessionLastChangedTime =
            sessionPersistedData_.sessionCreateTime;

        //
        // The new session is born inactive, and so it has a NULL SQG. This is
        // true whether or not the sesssion's being forked or not. If forking an
        // existing session, the key requirement the higher layer has to meet is
        // that the PQG and session Dag of the session being forked has been
        // merged, and the new session's PQG reflects the merge. The C-code here
        // doesn't need to be concerned about this, or whether the forkedSession
        // is active or not, etc. All this code would do is copy the UDFs and
        // KVS (this should NOT be done by the higher layer since SDK API for
        // session-fork needs to clone the UDFs/KVS).  The new workbook's KVS,
        // if it's a forked workbook, will have the new PQG, which can be
        // displayed and selectively replayed...on demand, by the user.
        //

        assert(sessionDag_ == NULL);
        if (forkedSession != NULL) {
            // Copy all the UDF modules from the source workbook to the forked
            // workbook.

            // Get the list of UDF functions in the source workbook.
            status = xcalarEval->getFnList("*",
                                           strGetFromFunctionCategory(
                                               FunctionCategoryUdf),
                                           forkedSession->udfContainer_,
                                           &getFnOutput,
                                           &getFnOutputSize);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to get UDFs from session '%s' for Usr '%s' "
                        "session '%s': %s",
                        forkedSession->getName(),
                        sessionUser->userIdName,
                        sessionInput->sessionName,
                        strGetFromStatus(status));
                goto SessionCreateExit;
            }
            // Get the modules for each function
            listXdfsOutput = &getFnOutput->outputResult.listXdfsOutput;
            for (unsigned ii = 0; ii < listXdfsOutput->numXdfs; ii++) {
                char *fnName = listXdfsOutput->fnDescs[ii].fnName;
                status =
                    xcalarEval->getUdfModuleFromLoad(fnName, &udfModules, NULL);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to get UDF module for function '%s' from "
                            "session '%s' for Usr '%s' session '%s': %s",
                            fnName,
                            forkedSession->getName(),
                            sessionUser->userIdName,
                            sessionInput->sessionName,
                            strGetFromStatus(status));
                    goto SessionCreateExit;
                }
            }
            // Copy each UDF module from the source to forked session
            for (EvalUdfModuleSet::iterator it = udfModules.begin();
                 (module = it.get()) != NULL;
                 it.next()) {
                verify(strlcpy(udfGetInput.moduleName,
                               it.get()->getName(),
                               sizeof(udfGetInput.moduleName)) <
                       sizeof(udfGetInput.moduleName));
                status = udfLib->getUdf(&udfGetInput,
                                        forkedSession->udfContainer_,
                                        &getUdfOutput,
                                        &getUdfOutputSize);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to get UDF module '%s' from session '%s' "
                            "for Usr '%s' session '%s': %s",
                            forkedSession->getName(),
                            udfGetInput.moduleName,
                            sessionUser->userIdName,
                            sessionInput->sessionName,
                            strGetFromStatus(status));
                    goto SessionCreateExit;
                }
                moduleSrc = &getUdfOutput->outputResult.udfGetOutput;
                status = udfLib->addUdf(moduleSrc,
                                        udfContainer_,
                                        &addUdfOutput,
                                        &addUdfOutputSize);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to add UDF module '%s' from session '%s' "
                            "for Usr '%s' session '%s': %s",
                            forkedSession->getName(),
                            udfGetInput.moduleName,
                            sessionUser->userIdName,
                            sessionInput->sessionName,
                            strGetFromStatus(status));
                    goto SessionCreateExit;
                }
                udfsAdded = true;
                memFree(getUdfOutput);
                getUdfOutput = NULL;
                memFree(addUdfOutput);
                addUdfOutput = NULL;
            }

            // Copy the session kvs from the source session to the forked
            // session.

            status = kvs->list(forkedSession->getKvStoreId(), ".*", &keyList);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to list KvStore keys for session '%s' to "
                        "clone to Usr '%s' session '%s': %s",
                        forkedSession->getName(),
                        sessionUser->userIdName,
                        sessionInput->sessionName,
                        strGetFromStatus(status));
                goto SessionCreateExit;
            }
            for (int ii = 0; ii < keyList->numKeys; ii++) {
                status = kvs->lookup(forkedSession->getKvStoreId(),
                                     keyList->keys[ii],
                                     &value,
                                     &valueSize);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to lookup KvStore key '%s' for session "
                            " '%s' to clone to Usr '%s' session '%s': %s",
                            forkedSession->getName(),
                            keyList->keys[ii],
                            sessionUser->userIdName,
                            sessionInput->sessionName,
                            strGetFromStatus(status));
                    goto SessionCreateExit;
                }
                status = kvs->addOrReplace(sessionKvStoreId_,
                                           keyList->keys[ii],
                                           strlen(keyList->keys[ii]) + 1,
                                           value,
                                           valueSize,
                                           true,
                                           KvStoreOptSync);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to add KvStore key '%s' for session "
                            " '%s' to clone to Usr '%s' session '%s': %s",
                            forkedSession->getName(),
                            keyList->keys[ii],
                            sessionUser->userIdName,
                            sessionInput->sessionName,
                            strGetFromStatus(status));
                    goto SessionCreateExit;
                }
                memFree(value);
                value = NULL;
            }
        }
    }

    assert(status == StatusOk);

    // NOTE: the session is born inactive - so the sessionDag_ is NULL

    // Write the session and the DAG
    status = writeSession(CreateSession);
    if (status != StatusOk) {
        strcpy(sessionOutput->error, errorLog);
        xSyslog(moduleName,
                XlogErr,
                "Failed session create for session Usr %s name %s on"
                " write session: %s",
                sessionUser->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(status));
        goto SessionCreateExit;
    }

    sessionOutput->sessionId = sessionPersistedData_.sessionId;

SessionCreateExit:

    udfModules.removeAll(&EvalUdfModule::del);

    if (getFnOutput != NULL) {
        memFree(getFnOutput);
        getFnOutput = NULL;
    }
    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    if (addUdfOutput != NULL) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    if (keyList != NULL) {
        memFree(keyList);
        keyList = NULL;
    }

    if (status != StatusOk) {
        Status status2;
        char keyName[XcalarApiMaxKeyLen + 1];
        KvStoreLib *kvs = KvStoreLib::get();

        if (udfsAdded) {
            assert(forkedSession != NULL);
            // Clean up the UDFs for the failed-to-clone session
            status2 = deleteWorkbookUdfs(udfContainer_);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Clone of session '%s' failed to delete workbook UDFs "
                        "for Usr '%s' (session '%s' ID '%lX'): %s",
                        forkedSession->getName(),
                        sessionUser->userIdName,
                        sessionPersistedData_.sessionName,
                        sessionPersistedData_.sessionId,
                        strGetFromStatus(status2));
            }
        }

        status2 = sessionMetaKvsName(sessionPersistedData_.sessionOwningUserId,
                                     sessionPersistedData_.sessionId,
                                     keyName,
                                     sizeof(keyName));

        if (status2 == StatusOk) {
            status2 =
                kvs->del(XidMgr::XidGlobalKvStore, keyName, KvStoreOptSync);
            // this is an attempt ; it's ok if it fails - key may not be
            // present - the main thing is to remove a key, if any from
            // global KVS, if the session create has failed
        }

        if (sessionKvStoreId_ != 0) {
            KvStoreLib::get()->close(sessionKvStoreId_,
                                     KvStoreCloseDeletePersisted);
        }
        if (udfContainer_ != NULL) {
            memFree(udfContainer_);
            udfContainer_ = NULL;
        }
    } else {
        strcpy(sessionOutput->error, "Session created successfully");
    }

    return status;
}

// List a user's session, filtered by the input pattern. The default pattern is
// "*" for all sessions.
Status
SessionMgr::Session::list(XcalarApiUserId *sessionUser,
                          SessionListElt *sessList,
                          const char *sessionListPattern,
                          XcalarApiOutput **outputOut,
                          size_t *sessionListOutputSize)
{
    Status status = StatusUnknown;
    size_t sessionOutputSize;
    XcalarApiOutput *output = NULL;
    int sessionCount = 0;
    int ii;
    bool listFilter;
    XcalarApiSessionListOutput *sessionListOutput;
    SessionMgr::Session *thisSession = NULL;
    SessionListElt *sessListLocal = sessList;
    SessionListElt *sessListOld = NULL;
    KvStoreLib *kvs = KvStoreLib::get();
    char *wbDescValue = NULL;  // value of special key XD uses
    size_t wbDescValueSize = 0;

    xSyslog(moduleName,
            XlogDebug,
            "Session list for session Usr %s",
            sessionUser->userIdName);

    // If the user asked for all sessions either exlicitly or by default,
    // turn off session name filtering
    // TODO: the session filter could be moved to User module - to avoid
    // three iterations through the list of sessions - one in User module to
    // create the list of all sessions, second here, to filter, and the
    // third, through the filtered list. Instead, user module can apply the
    // filter once iterating through all sessions, and only if sessions count
    // is non-zero call in here, with the list of sessions which were caught
    // in the filter, with this routine just filling out the output.
    if (strlen(sessionListPattern) == 1 &&
        strcmp(sessionListPattern, "*") == 0) {
        listFilter = false;
    } else {
        listFilter = true;
    }

    // Count the number of sessions, subject to the name filter if
    // there was one supplied.

    while (sessListLocal != NULL) {
        thisSession = sessListLocal->session;
        if ((listFilter &&
             strMatch(sessionListPattern,
                      thisSession->sessionPersistedData_.sessionName)) ||
            !listFilter) {
            sessionCount++;
        } else {
            // drop reference here, since this session will not be needed,
            // and then NULLify the pointer from list so the session can't
            // be reached/accessed from then on. Doing this here, prevents
            // the code from having to invoke strMatch() yet again in the
            // second iteration below.
            thisSession->decRef();
            sessListLocal->session = NULL;
        }
        sessListLocal = sessListLocal->next;
    }

    if (listFilter == false) {
        assert(sessionCount > 0);
    }

    // Allocate a return area sized for the number of qualifying sessions
    sessionOutputSize = XcalarApiSizeOfOutput(*sessionListOutput) +
                        sessionCount * (sizeof(sessionListOutput->sessions[0]));
    output = (XcalarApiOutput *) memAllocExt(sessionOutputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed session list for session Usr %s: %s",
                sessionUser->userIdName,
                strGetFromStatus(status));
        goto SessionListExit;
    }

    // Ensure no residual data is sent back
    memZero(output, sessionOutputSize);
    sessionListOutput = &output->outputResult.sessionListOutput;

    *sessionListOutputSize = sessionOutputSize;
    sessionListOutput->numSessions = sessionCount;
    xSyslog(moduleName,
            XlogDebug,
            "Session Usr %s, %d session(s) found",
            sessionUser->userIdName,
            sessionCount);
    if (sessionCount == 0) {
        xSyslog(moduleName,
                XlogDebug,
                "Session Usr %s, No sessions passed filtering",
                sessionUser->userIdName);
        status = StatusSessionNotFound;
        *outputOut = output;
        goto SessionListExit;
    }

    // Generate the output filtered list of sessions
    ii = 0;
    sessListLocal = sessList;  // reset to head of sessions list
    while (sessListLocal != NULL && ii < sessionCount) {
        thisSession = sessListLocal->session;
        if (thisSession != NULL) {
            //
            // Previous loop NULL'ified any sessions which weren't caught in
            // the filter. So no need to invoke strMatch() again - but assert
            // that the strMatch() is true.
            //
            assert(strMatch(sessionListPattern,
                            thisSession->sessionPersistedData_.sessionName));
            sessionListOutput->sessions[ii].sessionId =
                thisSession->sessionPersistedData_.sessionId;
            if (thisSession->sessionActive_ == true) {
                strcpy(sessionListOutput->sessions[ii].state, "Active");
                sessionListOutput->sessions[ii].activeNode =
                    Config::get()->getMyNodeId();
                strcpy(sessionListOutput->sessions[ii].info, "Has resources");
            } else {
                strcpy(sessionListOutput->sessions[ii].state, "Inactive");
                sessionListOutput->sessions[ii].activeNode = NodeIdInvalid;
                if (thisSession->sessionDag_ != NULL) {
                    strcpy(sessionListOutput->sessions[ii].info,
                           "Has resources");
                } else {
                    strcpy(sessionListOutput->sessions[ii].info,
                           "No resources");
                }
            }
            strlcpy(sessionListOutput->sessions[ii].name,
                    thisSession->sessionPersistedData_.sessionName,
                    sizeof(thisSession->sessionPersistedData_.sessionName));
            // fill-in workbook description if it exists
            //
            // XXX: see large comment in Sessions.h where the WorkbookDescrKey
            // is defined about the rationale and future for this key

            status = kvs->lookup(thisSession->sessionKvStoreId_,
                                 WorkbookDescrKey,
                                 &wbDescValue,
                                 &wbDescValueSize);
            if (status == StatusOk) {
                strlcpy(sessionListOutput->sessions[ii].description,
                        wbDescValue,
                        sizeof(sessionListOutput->sessions[ii].description));
                memFree(wbDescValue);
                wbDescValue = NULL;
            } else {
                // ignore failure to read workbook description key. The
                // description may not have been stored. The description will
                // be empty. Log if there's any other error
                if (status != StatusKvEntryNotFound) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Session Usr '%s' failed to get description for "
                            "workbook '%s': %s",
                            sessionUser->userIdName,
                            thisSession->sessionPersistedData_.sessionName,
                            strGetFromStatus(status));
                }
                status = StatusOk;
            }
            ii++;
            thisSession->decRef();
            sessListLocal->session = NULL;
        }
        sessListLocal = sessListLocal->next;
    }

    // Verify that we output the expected number of sessions. If this fails,
    // it implies there is a bug in the session list locking logic.
    if (ii != sessionCount || (listFilter == false && sessListLocal != NULL)) {
        xSyslog(moduleName,
                XlogErr,
                "Session Usr %s list error, output may be incomplete",
                sessionUser->userIdName);
    }

    *outputOut = output;
    status = StatusOk;

SessionListExit:
    sessListLocal = sessList;
    while (sessListLocal != NULL) {
        thisSession = sessListLocal->session;
        // all sessions must've been processed by now so session must be NULL
        // unless the output list allocation failed
        assert(status == StatusNoMem || thisSession == NULL);
        if (thisSession != NULL) {
            thisSession->decRef();
            sessListLocal->session = NULL;
            thisSession = NULL;
        }
        sessListOld = sessListLocal;
        sessListLocal = sessListLocal->next;
        delete sessListOld;
    }
    return status;
}

Status
SessionMgr::Session::doDelete(XcalarApiUserId *sessionUser,
                              SessionListElt *sessList,
                              int sessCount)
{
    Status status = StatusUnknown;
    int sessionDeleteCount = 0;
    SessionMgr::Session *thisSession = NULL;
    SessionListElt *sessListLocal = sessList;
    char queryName[LibNsTypes::MaxPathNameLen + 1];
    int ret;
    KvStoreLib *kvs = KvStoreLib::get();

    xSyslog(moduleName,
            XlogDebug,
            "Session delete for Usr %s",
            sessionUser->userIdName);

    // TODO: Put iteration in UserMgr and do only instance delete here
    while (sessListLocal != NULL) {
        char keyName[XcalarApiMaxKeyLen + 1];
        thisSession = sessListLocal->session;
        xSyslog(moduleName,
                XlogDebug,
                "Session delete for Usr %s, session %s",
                sessionUser->userIdName,
                thisSession->getName());

        assert(thisSession->getCrState() ==
               SessionMgr::Session::CrState::DelInProg);
        // start cleaning out the session in thisSession
        sessionDeleteCount++;
        // remove KVS entry for session metadata from global KVS
        status =
            thisSession->sessionMetaKvsName(thisSession->sessionPersistedData_
                                                .sessionOwningUserId,
                                            thisSession->sessionPersistedData_
                                                .sessionId,
                                            keyName,
                                            sizeof(keyName));
        if (status != StatusOk) {
            // key must've been successfully generated when session was created
            // so how can it fail now?
            assert(0);
            xSyslog(moduleName,
                    XlogDebug,
                    "Session delete failed to generate its global KVS key "
                    "for Usr %s, session %s",
                    thisSession->sessionPersistedData_.sessionOwningUserId,
                    thisSession->getName());
            // keep going...to delete this session and others
        } else {
            status =
                kvs->del(XidMgr::XidGlobalKvStore, keyName, KvStoreOptSync);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "Session delete failed to delete its global KVS key "
                        "'%s' for Usr %s, session %s",
                        keyName,
                        thisSession->sessionPersistedData_.sessionOwningUserId,
                        thisSession->getName());
                // keep going...
            }
        }

        // This session must've been inactivated before delete can be called
        // on it. And so sessionDag_ must be NULL.
        assert(thisSession->sessionDag_ == NULL);

        // Delete the query for this session if it exists
        ret = snprintf(queryName,
                       sizeof(queryName),
                       "%s:%s",
                       sessionUser->userIdName,
                       thisSession->sessionPersistedData_.sessionName);
        assert(ret < (int) sizeof(queryName));
        status = QueryManager::get()->requestQueryDelete(queryName);
        if (status != StatusOk && status != StatusQrQueryNotExist) {
            xSyslog(moduleName,
                    XlogErr,
                    "Session delete: failed to delete query '%s': %s",
                    queryName,
                    strGetFromStatus(status));
        }

        if (thisSession->sessionKvStoreId_ != 0) {
            //
            // If a session that was just created, couldn't get a KVS (due
            // to a failure when opening its KVS), it'd need to be deleted,
            // which is why its sessionKvStoreId_ may be 0.
            //
            KvStoreLib::get()->close(thisSession->sessionKvStoreId_,
                                     KvStoreCloseDeletePersisted);
        }

        // Delete all UDFS in the workbook
        status = thisSession->deleteWorkbookUdfs(thisSession->udfContainer_);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Session delete: Failed to delete workbook UDFs for "
                    "user '%s' (session '%s' ID '%lX': %s",
                    sessionUser->userIdName,
                    thisSession->sessionPersistedData_.sessionName,
                    thisSession->sessionPersistedData_.sessionId,
                    strGetFromStatus(status));
            // Keep going...
        }

        // free the rest of the things hanging off the session block
        if (thisSession->udfContainer_ != NULL) {
            memFree(thisSession->udfContainer_);
            thisSession->udfContainer_ = NULL;
        }
        sessListLocal = sessListLocal->next;
    }
    assert(sessionDeleteCount == sessCount);
    status = StatusOk;  // since sessionDeleteCount sessions were deleted

    return status;
}

Status
SessionMgr::Session::activate(XcalarApiUserId *sessionUser,
                              XcalarApiSessionGenericOutput *sessionOutput)
{
    Status status;
    int returnCode;
    DagLib *dagLib = DagLib::get();

    // Mark the new session active and update the owning user ID since
    // at some time in the future it may not be the originator
    xSyslog(moduleName,
            XlogDebug,
            "Session activate for Usr %s, session %s",
            sessionUser->userIdName,
            sessionPersistedData_.sessionName);

    strlcpy(sessionPersistedData_.sessionOwningUserId,
            sessionUser->userIdName,
            sizeof(sessionPersistedData_.sessionOwningUserId));
    sessionUserIdUnique_ = sessionUser->userIdUnique;
    sessionActive_ = true;

    clock_gettime(CLOCK_REALTIME, &(sessionPersistedData_.sessionLastUsedTime));
    sessionPersistedData_.sessionLastChangedTime =
        sessionPersistedData_.sessionLastUsedTime;

    if (sessionDag_ == NULL) {
        status = dagLib->createNewDag(SessionMgr::NumSlotsForCreateNewDag,
                                      DagTypes::WorkspaceGraph,
                                      udfContainer_,
                                      &sessionDag_);

        if (status != StatusOk) {
            sessionUserIdUnique_ = 0;
            sessionActive_ = false;
            returnCode = snprintf(sessionOutput->errorMessage,
                                  sizeof(sessionOutput->errorMessage),
                                  "Session activate failed - failed to create"
                                  "session dag");
            assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
            assert(sessionDag_ == NULL);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed session activate Usr %s name %s Id %ld on"
                    " replay query graph: %s",
                    sessionUser->userIdName,
                    sessionPersistedData_.sessionName,
                    sessionPersistedData_.sessionId,
                    strGetFromStatus(status));
            goto SessionActExit;
        }

        // The createNewDag above should've set the sessionDag_ to be in
        // the current session (udfContainer). Assert this.
        assert(sessionDag_->inSession() == true);
        if (sessionDag_->inSession() == false) {
            sessionDag_->initContainer(udfContainer_);
        }
    }
    assert(sessionDag_ != NULL);

    status = writeSession(ExistSession);
    if (status != StatusOk) {
        sessionUserIdUnique_ = 0;
        sessionActive_ = false;
        returnCode = snprintf(sessionOutput->errorMessage,
                              sizeof(sessionOutput->errorMessage),
                              "Session activate failed - log write error");
        assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
        xSyslog(moduleName,
                XlogErr,
                "Failed session activate for Usr %s name %s Id %ld on"
                " write session: %s",
                sessionUser->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        goto SessionActExit;
    }

    returnCode = snprintf(sessionOutput->errorMessage,
                          sizeof(sessionOutput->errorMessage),
                          "Session activate completed successfully");
    assert(returnCode < (int) sizeof(sessionOutput->errorMessage));

SessionActExit:
    return status;
}

void
SessionMgr::Session::updateNameUtil(char *sessionName)
{
    strlcpy(sessionPersistedData_.sessionName,
            sessionName,
            sizeof(sessionPersistedData_.sessionName));

    // update udfContainer's name in session and in session's dag

    strlcpy(udfContainer_->sessionInfo.sessionName,
            sessionName,
            sizeof(udfContainer_->sessionInfo.sessionName));
    udfContainer_->sessionInfo.sessionNameLength =
        strlen(udfContainer_->sessionInfo.sessionName);
    if (sessionDag_ != NULL) {
        sessionDag_->initContainer(udfContainer_);
    }
}

// Update session name. The session may be inactive. This is a utility function
// in session layer for UserMgr::rename API.
Status
SessionMgr::Session::updateName(XcalarApiUserId *sessionUser,
                                XcalarApiSessionRenameInput *sessionInput)
{
    Status status = StatusUnknown;

    xSyslog(moduleName,
            XlogDebug,
            "Session updateName for session Usr %s old %s, new name %s",
            sessionUser->userIdName,
            sessionInput->origSessionName,
            sessionInput->sessionName);

    clock_gettime(CLOCK_REALTIME, &(sessionPersistedData_.sessionLastUsedTime));
    sessionPersistedData_.sessionLastChangedTime =
        sessionPersistedData_.sessionLastUsedTime;

    updateNameUtil(sessionInput->sessionName);

    // Write session block.
    status = writeSession(ExistSession);
    if (status != StatusOk) {
        assert(0 && "Failed to make sessions durable");
        xSyslog(moduleName,
                XlogErr,
                "Session updateName: Write Session %lX %s Usr %s"
                " failed: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                sessionUser->userIdName,
                strGetFromStatus(status));
        //
        // Revert thisSession to oldName. This should be rolled back to old
        // name in-memory so that the in-memory name matches on-disk name. On a
        // retry this may succeed.
        //
        updateNameUtil(sessionInput->origSessionName);
    }
    return status;
}

// Force a list of sessions to be inactive.
// The intent of this function is to allow the
// UI to recover from a browser crash. Without this function, if a browser
// crashes a session will remain active forever and the user would never be
// allowed to switch to or delete it.
//
Status
SessionMgr::Session::inactivate(XcalarApiUserId *sessionUser,
                                SessionListElt *sessList,
                                int sessCount)
{
    return inactivateInternal(sessionUser, true, sessList, sessCount);
}

Status
SessionMgr::Session::inactivateNoClean(XcalarApiUserId *sessionUser,
                                       SessionListElt *sessList,
                                       int sessCount)
{
    return inactivateInternal(sessionUser, false, sessList, sessCount);
}

// By default, inactivating a session will free the datasets and tables
// it owns. The "cleanup" flag being set to false doesn't do the freeing.
// This is only used by the session download functionality.
Status
SessionMgr::Session::inactivateInternal(XcalarApiUserId *sessionUser,
                                        bool cleanup,
                                        SessionListElt *sessList,
                                        int sessCount)
{
    Status status = StatusUnknown;
    SessionListElt *sessListLocal = sessList;
    SessionMgr::Session *thisSession = NULL;
    int countInactivated = 0;

    xSyslog(moduleName,
            XlogDebug,
            "Session inactivate for Usr %s",
            sessionUser->userIdName);

    while (sessListLocal != NULL) {
        countInactivated++;
        thisSession = sessListLocal->session;
        xSyslog(moduleName,
                XlogDebug,
                "Session inactivate for Usr %s, session %s %s",
                sessionUser->userIdName,
                thisSession->sessionPersistedData_.sessionName,
                cleanup ? "cleanup" : "nocleanup");

        // User layer should send down only those sessions in sessList, for
        // which there is *something* to do - either the session's active, or
        // it's inactive but its resources need to be cleaned up. Assert this.
        // Otherwise the userMgr should've returned AlreadyInact status failure
        // So, here the routine can't really fail except for
        // StatusOperationOutstanding (checked below)
        assert(thisSession->sessionActive_ == true ||
               (cleanup == true && thisSession->sessionDag_ != NULL));
        if (cleanup == false) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Resources for session %lX %s Usr %s not freed due to user"
                    " request",
                    thisSession->sessionPersistedData_.sessionId,
                    thisSession->sessionPersistedData_.sessionName,
                    sessionUser->userIdName);
        }

        // check for outstanding operations
        if (thisSession->getOutstandOps()) {
            status = StatusOperationOutstanding;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed session inactivate session Usr %s name %s Id %ld: "
                    "%s",
                    sessionUser->userIdName,
                    thisSession->sessionPersistedData_.sessionName,
                    thisSession->sessionPersistedData_.sessionId,
                    strGetFromStatus(status));
            goto SessionInactExit;
        }

        if (thisSession->sessionActive_ == true) {
            // Mark the session inactive
            // XXX: review the logic: if there's a failure in writeSession() or
            // cleanQGraph() below, the session probably shouldn't be marked
            // inactive - so the marking of the session as inactive should
            // occur only after these calls have been successfully made. And
            // in case of failure, a continue should occur to the next session
            // in the list.

            // XXX Deleting the non-persisted prefixed IMD related keys here,
            // this is a workaround for non-persisted keys to get deleted upon
            // inactivation will be handled properly in this
            // https://xcalar.atlassian.net/browse/XD-1986
            status = deleteNPPrefixedImdKeys(thisSession->getKvStoreId());
            if (status != StatusOk) {
                goto SessionInactExit;
            }
            thisSession->sessionActive_ = false;
            thisSession->sessionUserIdUnique_ = 0;
            clock_gettime(CLOCK_REALTIME,
                          &(thisSession->sessionPersistedData_
                                .sessionLastUsedTime));
            // 2 possible cases:
            // 1) User wants to drop resource -- we destroy the sessionGraph
            // 2) USer doesn't want to drop resource -- we keep sessionGraph
            //
            // Write the session block
            status = thisSession->writeSession(ExistSession);
            if (status != StatusOk) {
                assert(0 && "Failed to make sessions durable");
                xSyslog(moduleName,
                        XlogErr,
                        "Session inactive: Write Session %lX %s Usr %s"
                        " failed: %s",
                        thisSession->sessionPersistedData_.sessionId,
                        thisSession->sessionPersistedData_.sessionName,
                        sessionUser->userIdName,
                        strGetFromStatus(status));
            }
            // kvs closed during session unload (i.e. destroy)
        }

        // Release any resources associated with the session graph unless the
        // user asked for them to be retained.
        if (cleanup && thisSession->sessionDag_ != NULL) {
            thisSession->cleanQgraph();
        }
        sessListLocal = sessListLocal->next;
    }
    assert(countInactivated == sessCount);
    status = StatusOk;  // since countInactivated sessions were inactivated
SessionInactExit:
    return status;
}

Status
SessionMgr::Session::getDag(Dag **sessionGraphHandleOut)
{
    Status status = StatusUnknown;
    // If a session is active, sessionDag can't be NULL since it represents the
    // mutable, distributed, active Dag with XDB backing it, for the session.
    // If a session is inactive, there are two scenarios:
    //
    // a) Pause: workbook is paused and so its DAG is still in-memory and not
    //    destroyed - this includes all the XDB resources backing the DAG. So
    //    sessionDag is valid/non-NULL even though sessionActive is false.
    //
    // b) Inactive: workbook is inactive and so the XDB resources backing the
    //    DAG have been destroyed, and the sessionDag_ is NULL.  sessionActive
    //    is of course false even in this case.
    //
    assert(sessionActive_ == false || sessionDag_ != NULL);
    if (sessionDag_ == NULL) {
        status = StatusSessionInact;
        goto CommonExit;
    }
    if (sessionGraphHandleOut != NULL) {
        *sessionGraphHandleOut = sessionDag_;
        status = StatusOk;
    }

CommonExit:
    return status;
}

// Persist the session (write the session to disk).
// The API currently assumes sessionCount could be > 1; this is deprecated
// but for now, we'll need to support the API so as to not break XD
// TODO: fix API eventually and modify XD to match
Status
SessionMgr::Session::persist(XcalarApiUserId *userId,
                             XcalarApiOutput **outputOut,
                             size_t *sessionListOutputSize)
{
    Status status = StatusUnknown;
    size_t sessionOutputSize;
    int sessionCount = 0;
    XcalarApiOutput *output = NULL;
    XcalarApiSessionListOutput *sessionListOutput;

    xSyslog(moduleName,
            XlogDebug,
            "Session persist for session Usr %s name %s Id %ld",
            userId->userIdName,
            sessionPersistedData_.sessionName,
            sessionPersistedData_.sessionId);

    sessionCount = 1;

    // Allocate the return area using the count determined above
    sessionOutputSize = XcalarApiSizeOfOutput(*sessionListOutput) +
                        sessionCount * (sizeof(sessionListOutput->sessions[0]));
    output = (XcalarApiOutput *) memAllocExt(sessionOutputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed session persist for session Usr %s name %s Id %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        goto SessionPersistExit;
    }
    memZero(output, sessionOutputSize);

    sessionListOutput = &output->outputResult.sessionListOutput;
    *sessionListOutputSize = sessionOutputSize;
    *outputOut = output;

    sessionListOutput->numSessions = sessionCount;

    // Persist the requested session and generate result output
    status = writeSession(ExistSession);
    sessionListOutput->sessions[0].sessionId = sessionPersistedData_.sessionId;
    if (status == StatusOk) {
        strcpy(sessionListOutput->sessions[0].state, "Successful");
    } else {
        strcpy(sessionListOutput->sessions[0].state, "Failed");
    }
    strlcpy(sessionListOutput->sessions[0].name,
            sessionPersistedData_.sessionName,
            sizeof(sessionPersistedData_.sessionName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed session persist for session Usr %s name %s Id %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
    }
SessionPersistExit:
    return status;
}

// Deserialize session metadata from workbook into JSON and add to supplied
// manifest for workbook download
Status
SessionMgr::Session::addSessionToManifest(json_t *archiveChecksum,
                                          ArchiveManifest *archiveManifest,
                                          char *userName)
{
    Status status;
    char *sess_json = NULL;
    int ret;
    char filePath[XcalarApiMaxPathLen + 1];

    status = deserialize(0, NULL, &sess_json);
    BailIfFailed(status);

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   SessionFileName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveAddFileToManifest(filePath,
                                      sess_json,
                                      strlen(sess_json),
                                      archiveManifest,
                                      archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add file '%s' to archive: %s",
                SessionFileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (sess_json != NULL) {
        memFree(sess_json);
        sess_json = NULL;
    }
    return status;
}

Status
SessionMgr::Session::initArchiveHeader(json_t *archiveHeader)
{
    Status status = StatusOk;
    json_t *jsonXcalarVersion = NULL;
    json_t *jsonWorkbookVersion = NULL;
    int ret;

    jsonXcalarVersion = json_string(versionGetStr());
    if (jsonXcalarVersion == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert version string '%s' to json",
                versionGetStr());
        goto CommonExit;
    }

    ret = json_object_set(archiveHeader, "xcalarVersion", jsonXcalarVersion);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to set version string '%s'",
                versionGetStr());
        goto CommonExit;
    }

    jsonWorkbookVersion = json_integer(WorkbookVersionCur);
    if (jsonWorkbookVersion == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert workbook version '%u' to json",
                WorkbookVersionCur);
        goto CommonExit;
    }

    ret =
        json_object_set(archiveHeader, "workbookVersion", jsonWorkbookVersion);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to set workbook version '%u'",
                WorkbookVersionCur);
        goto CommonExit;
    }

    status = updateWbHealth(archiveHeader, workbookHealth, workbookHealthGood);
    BailIfFailed(status);
CommonExit:
    if (jsonXcalarVersion != NULL) {
        json_decref(jsonXcalarVersion);
        jsonXcalarVersion = NULL;
    }
    if (jsonWorkbookVersion != NULL) {
        json_decref(jsonWorkbookVersion);
        jsonWorkbookVersion = NULL;
    }
    return status;
}

Status
SessionMgr::Session::addArchiveHeaderToManifest(
    json_t *archiveHeader,
    json_t *archiveChecksum,
    ArchiveManifest *archiveManifest,
    size_t *archiveHeaderSizeOut)
{
    Status status = StatusOk;
    char *jsonStr = NULL;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;

    jsonStr = json_dumps(archiveHeader, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    if (jsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert json archive header to string");
        goto CommonExit;
    }
    *archiveHeaderSizeOut = strlen(jsonStr);

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   WorkbookHeaderName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveAddFileToManifest(filePath,
                                      jsonStr,
                                      strlen(jsonStr),
                                      archiveManifest,
                                      archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add archive header to archive: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (jsonStr != NULL) {
        memFree(jsonStr);
        jsonStr = NULL;
    }

    return status;
}

Status
SessionMgr::Session::removeArchiveHeaderFromManifest(
    size_t archiveHeaderSize, ArchiveManifest *archiveManifest)
{
    Status status = StatusOk;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   WorkbookHeaderName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveRemoveFileFromManifest(filePath,
                                           archiveHeaderSize,
                                           archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to remove archive header from archive: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
SessionMgr::Session::getArchiveChecksumFromManifest(
    json_t **archiveChecksum, ArchiveManifest *archiveManifest)
{
    Status status = StatusOk;
    void *jsonRaw = NULL;
    size_t jsonRawSize = 0;
    char *jsonStr = NULL;
    json_error_t err;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   WorkbookChecksumName);

    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveGetFileData(archiveManifest,
                                filePath,
                                &jsonRaw,
                                &jsonRawSize,
                                NULL);
    // If there is no checksum file.
    if (status != StatusOk) {
        status = StatusOk;
        goto CommonExit;
    }
    // Change to null terminated string.
    jsonStr = (char *) memAllocExt(jsonRawSize + 1, moduleName);
    if (!jsonStr) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate '%lu' bytes for archive checksum",
                jsonRawSize + 1);
        goto CommonExit;
    }
    memcpy(jsonStr, jsonRaw, jsonRawSize);
    jsonStr[jsonRawSize] = '\0';

    *archiveChecksum = json_loads((const char *) jsonStr, 0, &err);
    if (!*archiveChecksum) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse workbook archive checksum, source '%s', "
                "line %d, column %d, position %d: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text);
        goto CommonExit;
    }
    assert(json_typeof(*archiveChecksum) == JSON_OBJECT);

CommonExit:

    if (jsonStr) {
        memFree(jsonStr);
        jsonStr = NULL;
    }

    return status;
}

Status
SessionMgr::Session::getArchiveHeaderFromManifest(
    json_t **archiveHeaderOut,
    ArchiveManifest *archiveManifest,
    const json_t *archiveChecksum)
{
    Status status = StatusOk;
    void *jsonRaw = NULL;
    size_t jsonRawSize = 0;
    char *jsonStr = NULL;
    json_t *jsonArchiveHeader = NULL;
    json_error_t err;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   WorkbookHeaderName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveGetFileData(archiveManifest,
                                filePath,
                                &jsonRaw,
                                &jsonRawSize,
                                archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get workbook archive header from archive: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    // Change to null terminated string.
    jsonStr = (char *) memAllocExt(jsonRawSize + 1, moduleName);
    if (jsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate '%lu' bytes for archive header",
                jsonRawSize + 1);
        goto CommonExit;
    }
    memcpy(jsonStr, jsonRaw, jsonRawSize);
    jsonStr[jsonRawSize] = '\0';

    jsonArchiveHeader = json_loads((const char *) jsonStr, 0, &err);
    if (jsonArchiveHeader == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse workbook archive header, source '%s', "
                "line %d, column %d, position %d: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text);
        goto CommonExit;
    }
    assert(json_typeof(jsonArchiveHeader) == JSON_OBJECT);

    // XXX Do some valildation of the header

    *archiveHeaderOut = jsonArchiveHeader;
    jsonArchiveHeader = NULL;

CommonExit:
    if (jsonArchiveHeader != NULL) {
        json_decref(jsonArchiveHeader);
        jsonArchiveHeader = NULL;
    }
    if (jsonStr != NULL) {
        memFree(jsonStr);
        jsonStr = NULL;
    }

    return status;
}

Status
SessionMgr::Session::updateWbHealth(json_t *archiveHeader,
                                    const char *key,
                                    const char *value)
{
    Status status = StatusOk;
    json_t *jsonWbHealthVal = NULL;
    int ret;

    jsonWbHealthVal = json_string(value);
    ret = json_object_set(archiveHeader, key, jsonWbHealthVal);
    if (ret != 0) {
        status = StatusJsonError;  // hard failure; can't fix later so fail
        xSyslog(moduleName,
                XlogErr,
                "Failed to update workbook health '%s, %s' for user '%s', "
                "session '%s': %s",
                key,
                value,
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (jsonWbHealthVal != NULL) {
        json_decref(jsonWbHealthVal);
        jsonWbHealthVal = NULL;
    }
    return status;
}

// Ideally, this function should be called every time when the error is possibly
// fixable, so that only fatal errors will stop a function. But using this
// everywhere in the call-graph under "download" makes the code unnecessarily
// complex given that most fatal errors are highly unlikely and there aren't so
// many fixable errors.
//
// The current strategy is to only call this function inside a specific download
// component where fixable errors are deemed common - these errors should be
// included in the enumerated list below so the component continues to execute
// its portion of the download despite such errors. Other failure sites where
// this function isn't used would stop early even if the error is fixable.
Status
SessionMgr::Session::isErrorFixable(const Status &status, Status *badStatus)
{
    bool isFixable = (status == StatusUdfModuleNotFound ||
                      status == StatusUdfModuleLoadFailed ||
                      status == StatusAstNoSuchFunction);
    if (!isFixable) {
        return status;
    } else {
        if (badStatus) {
            *badStatus = status;
        }
        return StatusOk;
    }
}

// Download should proceed despite errors. Even if one section (sessionInfo,
// KVS, qgraph...) fails, download will continue by moving to the next section.
// This function is called every time after adding a section during download. It
// updates the header and adds any failure to the missing files list. It changes
// status back to StatusOk to continue downloading.
Status
SessionMgr::Session::ignoreDownloadError(const Status &statusIn,
                                         json_t *archiveHeader,
                                         json_t *missingFilesArray,
                                         const char *missingFile,
                                         bool *badWorkbook)
{
    Status status = StatusUnknown;
    json_t *jsonMissingFile = NULL;
    int ret = 0;

    if (statusIn != StatusOk) {
        *badWorkbook = true;

        status =
            updateWbHealth(archiveHeader, workbookHealth, workbookHealthBad);

        jsonMissingFile = json_string(missingFile);
        ret = json_array_append_new(missingFilesArray, jsonMissingFile);
        if (ret) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error adding \"%s\" to missing files array",
                    missingFile);
        }
    }

    status = StatusOk;

    return status;
}

Status
SessionMgr::Session::addArchiveChecksumToManifest(
    json_t *archiveChecksum, ArchiveManifest *archiveManifest)
{
    Status status = StatusOk;
    char *jsonStr = NULL;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;

    jsonStr = json_dumps(archiveChecksum, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    if (!jsonStr) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert json archive checksum to string");
        goto CommonExit;
    }
    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   WorkbookChecksumName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveAddFileToManifest(filePath,
                                      jsonStr,
                                      strlen(jsonStr),
                                      archiveManifest,
                                      NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add archive checksum to archive: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (jsonStr) {
        memFree(jsonStr);
        jsonStr = NULL;
    }

    return status;
}

// Create the tar.gz file content for the specified session.
Status
SessionMgr::Session::download(
    XcalarApiUserId *userId,
    XcalarApiSessionDownloadInput *sessionDownloadInput,
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    ArchiveManifest *archiveManifest = NULL;
    void *sessionBuf = NULL;
    size_t sessionBufSize = 0;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiSessionDownloadOutput *sessionDownloadOutput = NULL;
    json_t *archiveHeader = NULL;
    json_t *archiveChecksum = NULL;
    json_t *missingFilesArray = NULL;
    int ret = 0;
    bool badWorkbook = false;
    bool badWorkbookTmp = false;
    size_t archiveHeaderSize = 0;

    xSyslog(moduleName,
            XlogDebug,
            "Session download for session Usr '%s' name '%s' ID %ld",
            userId->userIdName,
            sessionPersistedData_.sessionName,
            sessionPersistedData_.sessionId);

    archiveHeader = json_object();
    if (archiveHeader == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate session archive header json object for "
                "Usr '%s' name '%s' ID %ld",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId);
        goto CommonExit;
    }

    status = initArchiveHeader(archiveHeader);
    BailIfFailed(status);

    archiveChecksum = json_object();
    if (archiveChecksum == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate session archive checksum json object for "
                "Usr '%s' name '%s' ID %ld",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId);
        status = ignoreDownloadError(status,
                                     archiveHeader,
                                     missingFilesArray,
                                     WorkbookChecksumName,
                                     &badWorkbook);
        BailIfFailed(status);
    }

    missingFilesArray = json_array();
    if (missingFilesArray == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate session archive missing files json array "
                "for Usr '%s' name '%s' ID %ld",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId);
    }

    archiveManifest = archiveNewEmptyManifest();
    if (archiveManifest == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed archive creation for Usr '%s' name '%s' ID %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Add the top-most directory to the archive.  All content (files and/or
    // subdirectories will be within that directory.
    status = archiveAddDirToManifest(WorkbookDirName, archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add directory '%s' to archive for Usr '%s' "
                "name '%s' ID %ld: %s",
                WorkbookDirName,
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = addSessionKvStoreToManifest(archiveChecksum, archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add session KV to archive for Usr '%s' name '%s' "
                "ID %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        status = ignoreDownloadError(status,
                                     archiveHeader,
                                     missingFilesArray,
                                     KvStoreFileName,
                                     &badWorkbook);
        BailIfFailed(status);
    }

    // Create a manifest with the content
    // addVersionInfo

    status = addSessionToManifest(archiveChecksum,
                                  archiveManifest,
                                  userId->userIdName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add session metadata to archive for Usr '%s' "
                "name '%s' ID %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        status = ignoreDownloadError(status,
                                     archiveHeader,
                                     missingFilesArray,
                                     SessionFileName,
                                     &badWorkbook);
        BailIfFailed(status);
    }

    // Adds UDF info to the archive header
    status = addSessionUDFsToManifest(archiveHeader,
                                      archiveChecksum,
                                      archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add session UDFs to archive for Usr '%s' name '%s' "
                "ID %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        status = ignoreDownloadError(status,
                                     archiveHeader,
                                     missingFilesArray,
                                     UDFCommonFileName,
                                     &badWorkbook);
        BailIfFailed(status);
    }

    status = addSessionJupyterNBsToManifest(archiveChecksum,
                                            sessionDownloadInput
                                                ->pathToAdditionalFiles,
                                            archiveManifest);
    if (status != StatusOk && status != StatusNoEnt) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add Jupyter notebooks from '%s' to archive for "
                "Usr '%s' name '%s' ID %ld: %s",
                sessionDownloadInput->pathToAdditionalFiles,
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        status = ignoreDownloadError(status,
                                     archiveHeader,
                                     missingFilesArray,
                                     JupyterNBDirName,
                                     &badWorkbook);
        BailIfFailed(status);
    }

    if (badWorkbook) {
        ret = json_object_set(archiveHeader, missingFiles, missingFilesArray);
        if (ret) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to add missing files array to header for Usr '%s' "
                    "name "
                    "'%s' ID %ld",
                    userId->userIdName,
                    sessionPersistedData_.sessionName,
                    sessionPersistedData_.sessionId);
        }
    }

    status = addArchiveHeaderToManifest(archiveHeader,
                                        archiveChecksum,
                                        archiveManifest,
                                        &archiveHeaderSize);
    status = ignoreDownloadError(status,
                                 archiveHeader,
                                 missingFilesArray,
                                 WorkbookHeaderName,
                                 &badWorkbook);
    BailIfFailed(status);

    status = addArchiveChecksumToManifest(archiveChecksum, archiveManifest);
    status = ignoreDownloadError(status,
                                 archiveHeader,
                                 missingFilesArray,
                                 WorkbookChecksumName,
                                 &badWorkbookTmp);
    badWorkbook = badWorkbook || badWorkbookTmp;
    BailIfFailed(status);

    // Error adding checksum file.
    // Need to update the header without a checksum now.
    if (badWorkbookTmp) {
        status =
            removeArchiveHeaderFromManifest(archiveHeaderSize, archiveManifest);
        BailIfFailed(status);

        ret = json_object_set(archiveHeader, missingFiles, missingFilesArray);
        if (ret) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to add missing files array to header for Usr '%s' "
                    "name '%s' ID %ld",
                    userId->userIdName,
                    sessionPersistedData_.sessionName,
                    sessionPersistedData_.sessionId);
        }

        status = addArchiveHeaderToManifest(archiveHeader,
                                            NULL,  // No checksum file
                                            archiveManifest,
                                            &archiveHeaderSize);
        status = ignoreDownloadError(status,
                                     archiveHeader,
                                     missingFilesArray,
                                     WorkbookHeaderName,
                                     &badWorkbook);
        BailIfFailed(status);
    }

    status = archivePack(archiveManifest, &sessionBuf, &sessionBufSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to pack archive for Usr '%s' name '%s' ID %ld: %s",
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    outputSize = XcalarApiSizeOfOutput(*sessionDownloadOutput) + sessionBufSize;
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate '%lu' bytes for Usr '%s' name '%s' "
                "ID %ld: %s",
                outputSize,
                userId->userIdName,
                sessionPersistedData_.sessionName,
                sessionPersistedData_.sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    sessionDownloadOutput = &output->outputResult.sessionDownloadOutput;
    sessionDownloadOutput->sessionContentCount = sessionBufSize;
    memcpy(sessionDownloadOutput->sessionContent, sessionBuf, sessionBufSize);

CommonExit:
    if (sessionBuf != NULL) {
        memFree(sessionBuf);
        sessionBuf = NULL;
    }
    if (archiveManifest != NULL) {
        archiveFreeManifest(archiveManifest);
        archiveManifest = NULL;
    }
    if (missingFilesArray) {
        json_decref(missingFilesArray);
        missingFilesArray = NULL;
    }
    if (archiveHeader) {
        json_decref(archiveHeader);
        archiveHeader = NULL;
    }
    if (archiveChecksum) {
        json_decref(archiveChecksum);
        archiveChecksum = NULL;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    if (status == StatusOk && badWorkbook == true) {
        // Report download failure to XD.
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to add some of the files to archive for user "
                      "'%s' workbook name '%s'. Workbook is damaged.",
                      userId->userIdName,
                      sessionPersistedData_.sessionName);

        // in future, API caller can use this special error code to handle the
        // fact that although API is downloading a workbook, it's a bad workbook
        // which must be repaired. e.g. XD can pop-up a warning for the user
        // before downloading the workbook. Currently, StatusOk is returned
        // due to release constraints (XD doesn't have code to handle this
        // non-fatal error code) - so a workbook download will succeed - the
        // fact that its health is marked in the workbook makes this acceptable.
        //
        // return StatusDownloadBadWorkbook;
        return StatusOk;
    } else {
        return status;  // fatal failure or everything's good
    }
}

// Extract and update session metadata from workbook file being uploaded
Status
SessionMgr::Session::getSessionFromManifest(ArchiveManifest *archiveManifest,
                                            const json_t *archiveChecksum)
{
    Status status;
    void *sectionInfo = NULL;
    size_t sectionInfoSize;
    char *sectionStr = NULL;
    int ret;
    char filePath[XcalarApiMaxPathLen + 1];

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   SessionFileName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveGetFileData(archiveManifest,
                                filePath,
                                &sectionInfo,
                                &sectionInfoSize,
                                archiveChecksum);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to extract '%s' from archive: %s",
                SessionFileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    sectionStr = (char *) memAllocExt(sectionInfoSize + 1, moduleName);
    memcpy(sectionStr, sectionInfo, sectionInfoSize);
    sectionStr[sectionInfoSize] = '\0';
    assert(sectionInfoSize == strlen(sectionStr));

    // persist the json to KVS
    status = serialize(sectionStr);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getSessionFromManifest: session %lX %s failed json to "
                "protobuf conversion: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // serialized KVS to C (mainly to update time stamps in C from upload strr)
    // the serialize call above, serialized the upload metadata and the call to
    // deserialize below just updates C structs
    status = deserialize(0, NULL, NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getSessionFromManifest: session %lX %s fails protobuf Des: %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (sectionStr != NULL) {
        memFree(sectionStr);
        sectionStr = NULL;
    }
    return status;
}

// Fix passed in query graph in "queryStr" to replace the username component
// in any dataset names found in the query graph, with the username of the
// username in udfContainer_
//
// NOTE: an important side-effect of this routine is to trigger any per-node
// query upgrade code during the qp->parse() invocation (e.g. parseJson in
// QpExport.cpp upgrades export node).
//
// XXX: This needs to be evolved in the future to pass in version information
// to qp->parse, so each node in query can invoke version specific upgrade logic
Status
SessionMgr::Session::fixQueryGraphForUpgrade(char *queryStr,
                                             Dag *queryGraphIn,
                                             bool workBook,
                                             json_t **fixedQueryJsonOut)
{
    Status status;
    Dag *queryGraph = NULL;
    uint64_t numQueryGraphNodes;
    DagLib *dagLib = DagLib::get();
    QueryParser *qp = QueryParser::get();
    json_t *fixedQueryJson = NULL;

    // Dag's NULL if queryStr isn't
    assert(queryStr == NULL || queryGraphIn == NULL);
    // Dag must be supplied if queryStr is NULL
    assert(queryStr != NULL || queryGraphIn != NULL);

    queryGraph = queryGraphIn;

    if (queryGraph == NULL) {
        status = qp->parse(queryStr,
                           udfContainer_,
                           &queryGraph,
                           &numQueryGraphNodes);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "fixupQueryGraphForWorkbookUpload: Session %lX %s failed "
                    "qgraph json parse: %s",
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (workBook) {
        // Fix up any bulk loads and indexes in the dag with dataset names using
        // the current user.  Point at any existing dataset with the exact name
        // otherwise it'll be loaded.  As an alternative, this routine could be
        // ported to a different routine invoked in higher-layer of the API
        // (by porting this routine into a higher-layer version written in
        // python or JS to do the fixup).
        status = dagLib->fixupQueryGraphForWorkbookUpload(queryGraph);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "fixupQueryGraphForWorkbookUpload: Session %lX %s failed "
                    "to fix up load nodes: %s",
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Now we need to place the query graph into this workbook's KVS. So
    // first convert the queryGraph to a json string. The caller will invoke
    // an app to convert the json string to a list of KV pairs which will be
    // added in SessionMgr::Session::queryToDF2Upgrade

    status = qp->reverseParse(queryGraph, &fixedQueryJson);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed reverseParse for Dag Id %lu",
                queryGraph->getId());
        goto CommonExit;
    }

    *fixedQueryJsonOut = fixedQueryJson;
    fixedQueryJson = NULL;  // ref passed to return param
CommonExit:
    // query graph has been converted to json and the caller will store this
    // in kvs using the fixedQueryJsonOut - so now the dag can be destroyed
    if (queryGraph != NULL) {
        Status status2;

        status2 = dagLib->destroyDag(queryGraph, DagTypes::DestroyDeleteNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy Dag for Usr %s, session %s: %s",
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status2));
        }
        queryGraph = NULL;
    }

    if (fixedQueryJson) {
        json_decref(fixedQueryJson);
        fixedQueryJson = NULL;
    }
    return status;
}

// Extract and update qgraph metadata from workbook file being uploaded. This
// is needed only for pre-Dionysus workbooks since Dionysus workbooks will NOT
// have a qgraph metadata section (entire graph will be in the KVS section).
//
// This routine will extract the q-graph from the old workbook, and return a
// C string back to caller via "queryStr".
//
// The caller will convert the query string to a series of K-V pairs and enter
// these K-V pairs into the newly created/uploaded workbook which represent the
// new DF2.0 dataflows in the (upgraded, uploaded) workbook.
//
Status
SessionMgr::Session::getQGraphFromManifest(ArchiveManifest *archiveManifest,
                                           const json_t *archiveChecksum,
                                           char **queryStrRet)
{
    Status status;
    void *sectionInfo = NULL;
    size_t sectionInfoSize;
    char *sectionStr = NULL;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;

    assert(queryStrRet != NULL);
    *queryStrRet = NULL;

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   QGraphFileName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveGetFileData(archiveManifest,
                                filePath,
                                &sectionInfo,
                                &sectionInfoSize,
                                archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to extract '%s' from archive: %s",
                QGraphFileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    sectionStr = (char *) memAllocExt(sectionInfoSize + 1, moduleName);
    memcpy(sectionStr, sectionInfo, sectionInfoSize);
    sectionStr[sectionInfoSize] = '\0';
    assert(sectionInfoSize == strlen(sectionStr));

    *queryStrRet = sectionStr;  // caller must free

CommonExit:
    return status;
}

MustCheck Status
SessionMgr::Session::addKeyNameForCleanup(const char *keyName,
                                          SessionMgr::Session *targetSess)
{
    Status status = StatusOk;
    KeyNameHtEntry *keyTrack;

    keyTrack = targetSess->keyTrackHt_.find(keyName);
    if (keyTrack == NULL) {
        keyTrack = (KeyNameHtEntry *) memAlloc(sizeof(*keyTrack));
        BailIfNull(keyTrack);
        memset(keyTrack, 0, sizeof(*keyTrack));
        strlcpy(keyTrack->keyName, keyName, sizeof(keyTrack->keyName));
        status = targetSess->keyTrackHt_.insert(keyTrack);
        assert(status == StatusOk);
    }

CommonExit:

    return status;
}

void
KeyNameHtEntry::del()
{
    memFree(this);
}

void
SessionMgr::Session::doKeyCleanup(Status status,
                                  SessionMgr::Session *targetSess)
{
    KvStoreLib *kvs = KvStoreLib::get();
    KeyNameHtEntry *keyTrack = NULL;
    char *keyNameDup = NULL;
    Status status2;

    if (status == StatusOk) {
        // no need to cleanup/remove keys; just cleanup the hash table
        goto CommonExit;
    }

    // failure! Cleanup any keys added to the kvs (global or workbook)
    for (KeyNameHashTable::iterator it = targetSess->keyTrackHt_.begin();
         (keyTrack = it.get()) != NULL;
         it.next()) {
        if (keyNameDup != NULL) {
            memFree(keyNameDup);
            keyNameDup = NULL;
        }
        keyNameDup = strAllocAndCopy(keyTrack->getKeyName());

        if (strncmp(keyNameDup, GlobalKeyDsPrefix, strlen(GlobalKeyDsPrefix)) ==
            0) {
            XcalarApiDatasetDeleteInput dsDelInput;
            memset(&dsDelInput, 0, sizeof(dsDelInput));

            strlcpy(dsDelInput.datasetName,
                    basename(keyNameDup),
                    sizeof(dsDelInput.datasetName));

            // unpublish and unpersist the dataset; note that the fact that
            // this dataset appears in the hash table means it must've been
            // persisted by the caller (i.e. it didn't already exist) to make
            // sure only dataset metas which were created by the caller are
            // being removed, and not those which may have been created by
            // a different, prior caller
            status2 = Dataset::get()->deleteDatasetMeta(&dsDelInput);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed doKeyCleanup for session '%s', "
                        "failed to delete ds meta for dataset '%s'",
                        targetSess->getName(),
                        dsDelInput.datasetName);
                continue;
            }
        } else if (strncmp(keyNameDup,
                           GlobalKeyPrefix,
                           strlen(GlobalKeyPrefix)) == 0) {
            Status status2;

            status2 = kvs->del(XidMgr::XidGlobalKvStore,
                               keyNameDup + strlen(GlobalKeyPrefix),
                               KvStoreOptSync);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed doKeyCleanup for session '%s', "
                        "failed to delete key '%s'",
                        targetSess->getName(),
                        keyNameDup);
                continue;
            }
        } else if (strncmp(keyNameDup,
                           WkbookKeyPrefix,
                           strlen(WkbookKeyPrefix)) == 0) {
            Status status2;

            status2 = kvs->del(targetSess->getKvStoreId(),
                               keyNameDup + strlen(WkbookKeyPrefix),
                               KvStoreOptSync);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed doKeyCleanup for session '%s', "
                        "failed to delete key '%s'",
                        targetSess->getName(),
                        keyNameDup);
                continue;
            }
        }
    }
CommonExit:

    if (keyNameDup != NULL) {
        memFree(keyNameDup);
        keyNameDup = NULL;
    }
    targetSess->keyTrackHt_.removeAll(&KeyNameHtEntry::del);
}

// This is for upgrade of pre-Dionysus queries:
//
// Converts an old-style (pre-Dionysus) query into a Dionysus style dataflow,
// and enters the corresponding Key Value entries (corresponding to the workbook
// dataflow key-value pairs needed by XD) into the workbook.
//
Status
SessionMgr::Session::queryToDF2Upgrade(char *queryStr)
{
    Status status;
    App *queryToDF2UpgradeApp = NULL;
    char *outStr = NULL;
    char *errStr = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;
    KvStoreLib *kvs = KvStoreLib::get();
    const char *keyName;
    json_t *kvJsonValue = NULL;
    json_error_t err;
    char *valueBuf = NULL;
    size_t valueBufSize = 0;
    int numNodeOuts;
    int numOuts;
    json_t *clusterJson;
    json_t *nodeJson;
    json_t *kvJsonStr;
    json_t *kvJsonObj;
    const char *kvStr;
    char *keyNameDup = NULL;  // for keyName manipulation, if needed
    bool dsPersisted = false;
    bool dsKey = false;

    queryToDF2UpgradeApp =
        AppMgr::get()->openAppHandle(AppMgr::QueryToDF2UpgradeAppName, &handle);
    if (queryToDF2UpgradeApp == NULL) {
        status = StatusAppDoesNotExist;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2Upgrade for session '%s', app %s "
                      "does not exist",
                      sessionPersistedData_.sessionName,
                      AppMgr::QueryToDF2UpgradeAppName);
        goto CommonExit;
    }

    // Run the app locally
    status = AppMgr::get()->runMyApp(queryToDF2UpgradeApp,
                                     AppGroup::Scope::Local,
                                     "",
                                     0,
                                     queryStr,
                                     0,
                                     &outStr,
                                     &errStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2Upgrade for session '%s', "
                      "app \"%s\" failed with status:\"%s\", "
                      "error:\"%s\", output\"%s\"",
                      sessionPersistedData_.sessionName,
                      queryToDF2UpgradeApp->getName(),
                      strGetFromStatus(status),
                      errStr ? errStr : "",
                      outStr ? outStr : "");
        goto CommonExit;
    }

    clusterJson = json_loads(outStr, 0, &err);
    if (clusterJson == NULL) {
        status = StatusJsonError;
        // Emit short form and long form logs (outStr may be large)
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2Upgrade: KVs returned by convertor "
                      "couldn't be loaded as json; session '%s', status '%s'",
                      sessionPersistedData_.sessionName,
                      strGetFromStatus(status));
        xSyslog(moduleName,
                XlogErr,
                "Failed queryToDF2Upgrade: KVs returned by convertor: "
                "'%s' couldn't be loaded as json; session '%s', "
                "status '%s'",
                outStr,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // An app returns output as an array of arrays. The outer array is for
    // nodes in the cluster (size is 1 if run locally otherwise N for N nodes),
    // and inner array for a node's app output which might append output to
    // the inner array at different times during the app's execution.
    assert(json_typeof(clusterJson) == JSON_ARRAY);
    if (json_typeof(clusterJson) != JSON_ARRAY) {
        status = StatusAppFailedToGetOutput;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2 upgrade: upgrade app returned "
                      "non-array clusterJson for session '%s' ID %lx: %s",
                      sessionPersistedData_.sessionName,
                      sessionPersistedData_.sessionId,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    numNodeOuts = json_array_size(clusterJson);
    assert(numNodeOuts == 1);  // since it's run locally
    nodeJson = json_array_get(clusterJson, 0);

    assert(json_typeof(nodeJson) == JSON_ARRAY);
    if (json_typeof(nodeJson) != JSON_ARRAY) {
        status = StatusAppFailedToGetOutput;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2 upgrade: upgrade app returned "
                      "non-array nodeJson for session '%s' ID %lx: %s",
                      sessionPersistedData_.sessionName,
                      sessionPersistedData_.sessionId,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    numOuts = json_array_size(nodeJson);
    assert(numOuts == 1);  // one out since this app uses singleton array
    kvJsonStr = json_array_get(nodeJson, 0);

    assert(json_typeof(kvJsonStr) == JSON_STRING);
    if (json_typeof(kvJsonStr) != JSON_STRING) {
        status = StatusAppFailedToGetOutput;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2 upgrade: upgrade app returned "
                      "non-string result for session '%s' ID %lx: %s",
                      sessionPersistedData_.sessionName,
                      sessionPersistedData_.sessionId,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    kvStr = json_string_value(kvJsonStr);
    if (kvStr == NULL) {
        status = StatusAppFailedToGetOutput;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2 upgrade: upgrade app returned "
                      "invalid KV string for session '%s' ID %lx: %s",
                      sessionPersistedData_.sessionName,
                      sessionPersistedData_.sessionId,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    kvJsonObj = json_loads(kvStr, 0, &err);
    assert(kvJsonObj && json_typeof(kvJsonObj) == JSON_OBJECT);
    if (kvJsonObj == NULL || json_typeof(kvJsonObj) != JSON_OBJECT) {
        status = StatusJsonError;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed queryToDF2 upgrade: json parse of kvStr failed "
                      "source %s, line %d, column %d, position %d, '%s' "
                      "for session '%s' ID %lx: %s",
                      err.source,
                      err.line,
                      err.column,
                      err.position,
                      err.text,
                      sessionPersistedData_.sessionName,
                      sessionPersistedData_.sessionId,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    json_object_foreach (kvJsonObj, keyName, kvJsonValue) {
        const char *kvKey;
        const char *kvValue;
        dsPersisted = false;
        dsKey = false;
        keyNameDup = strAllocAndCopy(keyName);
        if (strncmp(keyName, GlobalKeyDsPrefix, strlen(GlobalKeyDsPrefix)) ==
            0) {
            // global key for dataset meta-data
            //
            // This is used to support auto-dataset creation during upgrade
            // of legacy workbooks. It was decided to NOT support this for
            // legacy batch dataflow upgrade (due to usernames in datasets
            // not being fixed up for legacy dataflows given the API to do
            // the fixup is very onerous and DS separation in DF2 already
            // prepares the user to accept dataset configuration being their
            // responsibility).
            //
            // For upgrade of existing workbooks, there's no choice -
            // the auto-creation is a must since the upgrade should look
            // like a re-boot, and post-upgrade Dionysus would look for
            // datasets in the global KVS. However, for existing BDFs, the
            // post-upgrade scene is different, and the user will need to
            // create the necessary datasets after upgrade for the new
            // "shared" dataflows.

            dsKey = true;
            char *dsName;

            dsName = basename(keyNameDup);
            // add key to global KVS to simulate dataset creation
            // and publish dataset meta in libNs - latter may fail with
            // StatusExist if the dataset has already been published - which
            // is ok
            status = Dataset::get()->persistAndPublish(dsName,
                                                       json_string_value(
                                                           kvJsonValue),
                                                       &dsPersisted);
            if (status != StatusOk && status != StatusExist) {
                // Emit short form and long form logs (kvJsonValue may be large)
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Failed queryToDF2 upgrade: persistAndPublish "
                              "failed for dataset '%s' "
                              "for session '%s', ID %lX: '%s'",
                              dsName,
                              sessionPersistedData_.sessionName,
                              sessionPersistedData_.sessionId,
                              strGetFromStatus(status));
                xSyslog(moduleName,
                        XlogErr,
                        "Failed queryToDF2 upgrade: persistAndPublish "
                        "failed for dataset '%s', with meta-data '%s' "
                        "for session '%s', ID %lX: '%s'",
                        dsName,
                        json_string_value(kvJsonValue),
                        sessionPersistedData_.sessionName,
                        sessionPersistedData_.sessionId,
                        strGetFromStatus(status));
                goto CommonExit;
            } else {
                // Dataset may already exist due to multiple references to it in
                // the query graph - so overlook such errors
                status = StatusOk;
            }
        } else if (strncmp(keyName, GlobalKeyPrefix, strlen(GlobalKeyPrefix)) ==
                   0) {
            // non-dataset global key
            // XXX: add lookup to check for StatusExist case; currently there
            // are no non-dataset global keys encountered during upgrade
            kvKey = keyNameDup + strlen(GlobalKeyPrefix);
            kvValue = json_string_value(kvJsonValue);
            if (kvValue == NULL) {
                status = StatusJsonError;
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to convert json value to string for key "
                        " '%s' for session '%s', ID %lX: '%s'",
                        keyNameDup,
                        sessionPersistedData_.sessionName,
                        sessionPersistedData_.sessionId,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            status = kvs->addOrReplace(XidMgr::XidGlobalKvStore,
                                       kvKey,
                                       strlen(kvKey) + 1,
                                       kvValue,
                                       strlen(kvValue) + 1,
                                       true,
                                       KvStoreOptSync);
        } else if (strncmp(keyName, WkbookKeyPrefix, strlen(WkbookKeyPrefix)) ==
                   0) {
            // workbook key
            status = kvs->lookup(sessionKvStoreId_,
                                 keyNameDup + strlen(WkbookKeyPrefix),
                                 &valueBuf,
                                 &valueBufSize);
            // The key should not already exist
            if (status != StatusKvEntryNotFound) {
                if (status == StatusOk) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "Failed queryToDF2 upgrade: Key '%s' aready "
                                  "exists session %s ID %lx: %s",
                                  keyName,
                                  sessionPersistedData_.sessionName,
                                  sessionPersistedData_.sessionId,
                                  strGetFromStatus(status));
                    status = StatusExist;
                    assert(0);
                    goto CommonExit;

                    // XXX: continue despite failure? since we should probably
                    // try and enter as many kv pairs as possible before
                    // failing? But there's no convenient UX to report such
                    // events but still succeed. So just fail for now. This
                    // applies to all "goto CommonExit" statements in the loop

                } else {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "Failed queryToDF2 upgrade: key '%s' lookup "
                                  "failed for session '%s' ID %lx: %s",
                                  keyName,
                                  sessionPersistedData_.sessionName,
                                  sessionPersistedData_.sessionId,
                                  strGetFromStatus(status));
                    goto CommonExit;
                }
            }
            kvKey = keyNameDup + strlen(WkbookKeyPrefix);
            kvValue = json_string_value(kvJsonValue);
            if (kvValue == NULL) {
                status = StatusJsonError;
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to convert value to json string for key "
                        " '%s' for session '%s', ID %lX: '%s'",
                        keyNameDup,
                        sessionPersistedData_.sessionName,
                        sessionPersistedData_.sessionId,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            status = kvs->addOrReplace(sessionKvStoreId_,
                                       kvKey,
                                       strlen(kvKey) + 1,
                                       kvValue,
                                       strlen(kvValue) + 1,
                                       true,
                                       KvStoreOptSync);
        }
        // If   processing dsKey and one was persisted in global KVS
        //   OR status is StatusOk (sufficient condition for all other keys)
        // then
        //    record the fact that a key was added
        if ((dsKey && dsPersisted) || (!dsKey && status == StatusOk)) {
            Status status2;
            status2 = addKeyNameForCleanup(keyNameDup, this);
            if (status2 != StatusOk) {
                // emit syslog to log any keys which were added to kvs but
                // couldn't be tracked for cleanup
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "key '%s' added but couldn't track for cleanup "
                              "for session '%s' ID %lx: '%s' (not fatal)",
                              keyNameDup,
                              sessionPersistedData_.sessionName,
                              sessionPersistedData_.sessionId,
                              strGetFromStatus(status));
            }
        }

        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed queryToDF2 upgrade: key '%s' add failed for "
                          "session '%s' ID %lx: '%s' ",
                          keyName,
                          sessionPersistedData_.sessionName,
                          sessionPersistedData_.sessionId,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        // Before looping around or exiting loop, free the dup key name memory
        // Need this here and in CommonExit block if a goto occurs in loop due
        // to failure
        if (keyNameDup != NULL) {
            memFree(keyNameDup);
            keyNameDup = NULL;
        }
    }
CommonExit:
    doKeyCleanup(status, this);

    if (keyNameDup != NULL) {
        memFree(keyNameDup);
        keyNameDup = NULL;
    }
    if (queryToDF2UpgradeApp) {
        AppMgr::get()->closeAppHandle(queryToDF2UpgradeApp, handle);
        queryToDF2UpgradeApp = NULL;
    }
    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }
    if (errStr) {
        memFree(errStr);
        errStr = NULL;
    }
    return status;
}

// Init the header for the upgrade payload to be sent to the expserver.
// The header contains two elements:
// - XcalarVersion
// - Version of workbook or batch dataflow
//
// The upgrade routine can use the version string to decide whether it's a
// workbook or batch dataflow whose query string needs to be upgraded. The
// k-v pairs generated by the upgrade routine depends on whether it's a
// workbook or batch dataflow.
//
// Since batch dataflows didn't have versions, the routine below just takes
// a workbookVersion - if it's 0, it must be a batch dataflow.
//
Status
SessionMgr::Session::initUpgrHeader(json_t *upgrHeader, int workbookVersion)
{
    Status status = StatusOk;
    json_t *jsonXcalarVersion = NULL;
    json_t *jsonWorkbookVersion = NULL;
    json_t *jsonBdfVersion = NULL;
    int ret;

    jsonXcalarVersion = json_string(versionGetStr());
    BailIfNull(jsonXcalarVersion);

    ret = json_object_set(upgrHeader, "xcalarVersion", jsonXcalarVersion);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to set version string '%s'",
                versionGetStr());
        goto CommonExit;
    }

    if (workbookVersion) {
        jsonWorkbookVersion = json_integer(workbookVersion);
        if (jsonWorkbookVersion == NULL) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to convert workbook version '%u' to json",
                    workbookVersion);
            goto CommonExit;
        }
        ret =
            json_object_set(upgrHeader, "workbookVersion", jsonWorkbookVersion);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to set workbook version '%u'",
                    workbookVersion);
            goto CommonExit;
        }
    } else {
        // No workbook version implies this is a batch dataflow. BDFs had the
        // following versions (not very well done but this is history):
        //
        // retinaVersion 1 (very old retina with retinaInfo.json file)
        // dataflowVersion 1 (Chronos retinas with dataflowInfo.json file)
        //
        // By now the legacy BDF (of either version) has been converted (in
        // fixQueryGraphForUpgrade() - using parse followed by reverseParse) to
        // a DF2 query before it's sent to expServer for upgrade into the final
        // DF2 layout appropriate for KVS storage (i.e. list of KV pairs).
        //
        // So the query version is really now the first version of the DF2
        // query - hence we use the new DataflowVersion1. This may be the first
        // and last version for it, since future upgrades would be done using
        // the workbook version (given that dataflows are unified and their
        // version could for now be tied to the workbook version in which
        // they're contained).

        jsonBdfVersion = json_integer(DataflowVersion1);
        if (jsonBdfVersion == NULL) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to convert batch dataflow version '%u' to json",
                    DataflowVersion1);
            goto CommonExit;
        }
        ret =
            json_object_set(upgrHeader, "batchDataflowVersion", jsonBdfVersion);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to set batch dataflow version '%u'",
                    DataflowVersion1);
            goto CommonExit;
        }
    }

CommonExit:

    if (jsonXcalarVersion != NULL) {
        json_decref(jsonXcalarVersion);
        jsonXcalarVersion = NULL;
    }
    if (jsonWorkbookVersion != NULL) {
        json_decref(jsonWorkbookVersion);
        jsonWorkbookVersion = NULL;
    }
    if (jsonBdfVersion != NULL) {
        json_decref(jsonBdfVersion);
        jsonBdfVersion = NULL;
    }
    return status;
}

// See block comment in header file for interface/description of this routine
Status
SessionMgr::Session::prepQueryForUpgrade(char *queryStr,
                                         Dag *queryGraph,
                                         int wbVersion,
                                         json_t *retQueryJsonObj,
                                         char **hdrWqueryOut)
{
    Status status;
    json_t *queryJson = NULL;
    json_t *queryJsonObj = NULL;
    json_t *upgrHeader = NULL;
    json_t *upgrQueryJsonArr = NULL;
    char *queryJsonAndHdrStr = NULL;
    char *queryJsonStr = NULL;
    int ret = 0;
    KvStoreLib *kvs = KvStoreLib::get();
    char *gInfoValue = NULL;  // value of special key XD uses
    size_t gInfoValueSize = 0;
    json_t *gInfoJsonObj = NULL;

    if (wbVersion == 0) {
        // no workbook version => this is a query for a retina upgrade so
        // the json object must already be supplied in retQueryJsonObj
        assert(retQueryJsonObj != NULL);
        queryJsonObj = retQueryJsonObj;
        status = fixQueryGraphForUpgrade(queryStr,
                                         queryGraph,
                                         false,
                                         &upgrQueryJsonArr);
        BailIfFailed(status);
    } else {
        json_error_t err;
        // workbook whose version == wbVersion
        status = fixQueryGraphForUpgrade(queryStr,
                                         queryGraph,
                                         true,
                                         &upgrQueryJsonArr);
        BailIfFailed(status);
        queryJsonObj = json_object();
        BailIfNull(queryJsonObj);
        status = kvs->lookup(sessionKvStoreId_,
                             TableDescKeyForXD,
                             &gInfoValue,
                             &gInfoValueSize);
        if (status != StatusOk) {
            // non-fatal; the special key must always exist but maybe someone
            // modified the workbook and removed it ...keep going - don't
            // want to fail upgrade if the special key doesn't exist
            xSyslog(moduleName,
                    XlogErr,
                    "Upgrade workbook '%s' lookup of '%s' key failed: %s",
                    sessionPersistedData_.sessionName,
                    TableDescKeyForXD,
                    strGetFromStatus(status));
            status = StatusOk;
        } else {
            gInfoJsonObj = json_loads((const char *) gInfoValue, 0, &err);
            if (gInfoJsonObj == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Upgrade workbook '%s' failed parse json in key '%s': "
                        "source %s line %d, column %d, position %d, '%s': %s",
                        sessionPersistedData_.sessionName,
                        TableDescKeyForXD,
                        err.source,
                        err.line,
                        err.column,
                        err.position,
                        err.text,
                        strGetFromStatus(status));
            } else {
                ret = json_object_set_new(queryJsonObj,
                                          TableDescKeyForXD,
                                          gInfoJsonObj);
                if (ret != 0) {
                    status = StatusJsonError;
                    xSyslog(moduleName,
                            XlogErr,
                            "Upgrade workbook '%s' json failed to set '%s' "
                            "key: %s",
                            sessionPersistedData_.sessionName,
                            TableDescKeyForXD,
                            strGetFromStatus(status));
                    goto CommonExit;
                } else {
                    gInfoJsonObj = NULL;  // ref stolen by queryJsonObj
                }
            }
        }
    }

    ret = json_object_set_new(queryJsonObj, "query", upgrQueryJsonArr);
    if (ret != 0) {
        status = StatusJsonError;
        goto CommonExit;
    }
    upgrQueryJsonArr = NULL;  // reference stolen by queryJsonObj

    queryJsonStr = json_dumps(queryJsonObj, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    if (queryJsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert json to string for session '%s': '%s'",
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    upgrHeader = json_object();
    if (upgrHeader == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate json object header for upgrade payload"
                "Usr '%s' name '%s'",
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName);
        goto CommonExit;
    }
    status = initUpgrHeader(upgrHeader, wbVersion);
    BailIfFailed(status);

    json_error_t err;
    queryJson = json_loads(queryJsonStr, 0, &err);
    if (queryJson == NULL) {
        status = StatusJsonError;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to parse query string, "
                      "source %s line %d, column %d, position %d, '%s': %s",
                      err.source,
                      err.line,
                      err.column,
                      err.position,
                      err.text,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    ret = json_object_set_new(queryJson, "header", upgrHeader);
    if (ret != 0) {
        status = StatusNoMem;
        goto CommonExit;
    }
    upgrHeader = NULL;  // passed ref to queryJson

    // on success, upgrHeader's reference doesn't need to be dec-ref'ed since
    // the call to json_array_insert_new() steals the reference to it
    queryJsonAndHdrStr =
        json_dumps(queryJson, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    BailIfNull(queryJsonAndHdrStr);

    assert(hdrWqueryOut != NULL);

    *hdrWqueryOut = queryJsonAndHdrStr;

CommonExit:
    if (gInfoValue) {
        memFree(gInfoValue);
        gInfoValue = NULL;
    }
    if (gInfoJsonObj) {
        json_decref(gInfoJsonObj);
        gInfoJsonObj = NULL;
    }
    if (upgrHeader) {
        json_decref(upgrHeader);
        upgrHeader = NULL;
    }
    if (queryJsonStr != NULL) {
        memFree(queryJsonStr);
        queryJsonStr = NULL;
    }
    if (queryJson != NULL) {
        json_decref(queryJson);
        queryJson = NULL;
    }
    if (wbVersion) {
        // decref only if this object was allocated here and not passed down
        json_decref(queryJsonObj);
        queryJsonObj = NULL;
    }
    return status;
}

// All upgrade code, to upgrade a workbook during upload, should go here.
Status
SessionMgr::Session::wbUploadUpgrade(unsigned workbookVersion,
                                     ArchiveManifest *archiveManifest,
                                     const json_t *archiveChecksum)
{
    Status status;
    char *inObjStr = NULL;
    char *inObjwHdrStr = NULL;

    if (workbookVersion <= WorkbookVersion2) {
        // unpack QGraph section only if it is supposed to have one (<= V2)
        status =
            getQGraphFromManifest(archiveManifest, archiveChecksum, &inObjStr);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get QGraph from archive '%s' for Usr %s: %s",
                    sessionPersistedData_.sessionName,
                    sessionPersistedData_.sessionOwningUserId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        status = prepQueryForUpgrade(inObjStr,
                                     NULL,
                                     workbookVersion,
                                     NULL,
                                     &inObjwHdrStr);
        BailIfFailed(status);

        status = this->queryToDF2Upgrade(inObjwHdrStr);
        BailIfFailed(status);
    }  // now session is upgraded to V3

    if (workbookVersion < WorkbookVersion4) {  // upgrade for v3 to v4 change
        // no-op since the only change from V3 to V4 is the addition of version
        // to session metadata which is taken care of in serialize() (called
        // by getSessionFromManifest())
    }
    //
    // The following assert serves as a ratchet to think about upgrade. If this
    // assert fails, WorkbookVersionCur has probably been bumped up beyond the
    // version on the right of the '==' (say vX)...if this version change needs
    // upgrade code to be written (to upgrade from vX to WorkbookVersionCur),
    // then it should be added here above the assert and then vX bumped up to
    // equal the value of WorkbookVersionCur. If no upgrade code needs to be
    // written, just fix the assert by bumping up vX to the highest version
    // in force currently (to which WorkbookVersionCur is currently set).
    //
    assertStatic(WorkbookVersionCur == WorkbookVersion4);
    status = StatusOk;

CommonExit:

    if (inObjStr) {
        memFree(inObjStr);
        inObjStr = NULL;
    }

    if (inObjwHdrStr) {
        memFree(inObjwHdrStr);
        inObjwHdrStr = NULL;
    }
    return status;
}

// Assume the supplied buffer is the contents of a retina tar.gz file. And try
// upgrading it to DF2. This is if a legacy retina tar.gz is supplied to the
// postDF2 dataflow upload path ...then the following will do the needful
// to upgrade it
Status
SessionMgr::Session::tryUpgradeRetinaToDF2(void *buf, size_t bufSize)
{
    Status status;
    XcalarApiOutput **uploadUdfOutputArray = NULL;
    size_t *outputSizeArray = NULL;
    int numUdfModules;
    RetinaInfo *retinaInfo = NULL;
    char *retinaQueryAndHdrStr = NULL;
    int ii;
    json_t *retinaJson = NULL;
    json_t *queryJsonObj = NULL;
    char *queryStr = NULL;
    char *queryStrToFree = NULL;

    status = DagLib::get()->parseRetinaFile(buf,
                                            bufSize,
                                            sessionPersistedData_.sessionName,
                                            &retinaInfo);
    BailIfFailed(status);

    numUdfModules = retinaInfo->numUdfModules;

    if (numUdfModules) {
        status =
            UserDefinedFunction::get()->bulkAddUdf(retinaInfo->udfModulesArray,
                                                   numUdfModules,
                                                   udfContainer_,
                                                   &uploadUdfOutputArray,
                                                   &outputSizeArray);
        BailIfFailed(status);
    }

    status =
        DagLib::get()->parseRetinaFileToJson(buf,
                                             bufSize,
                                             sessionPersistedData_.sessionName,
                                             &retinaJson);
    BailIfFailed(status);

    //
    // To run through per-node upgrade for the query in retinaJson, do the
    // following (essentially leveraging parse/reverseparse to do the upgrade):
    //
    // 0. get "query" key's value from retinaJson (in "queryJsonObj")
    // 1. then extract C string from queryJsonObj -> queryStr
    // 2. qp->parse(queryStr) -> queryGraph
    // 3. qp->reverseParse(queryGraph) -> upgrQueryJsonArr json array obj
    //    NOTE: parse/reverseparse done inside fixQueryGraphForUpgrade
    // 4. update 'query' key's value in retinaJson with upgrQueryArr
    //

    queryJsonObj = json_object_get(retinaJson, "query");
    BailIfNull(queryJsonObj);

    // Note: json_object_get returns a 'borrowed reference' so do NOT decref
    // queryJsonObj ...retinaJson holds the reference which is decref'ed below

    if (json_is_string(queryJsonObj)) {
        // legacy layout (retinaInfo.json used string (first char ") for query)
        queryStr = (char *) json_string_value(queryJsonObj);
    } else {
        queryStr = json_dumps(queryJsonObj, JSON_INDENT(4) | JSON_ENSURE_ASCII);
        queryStrToFree = queryStr;  // must be freed unlike value of JSON_STRING
    }
    if (queryStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert query from json to string for retina '%s': "
                "'%s'",
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // NOTE: For retina upgrade, the dataset names aren't fixed to reflect the
    // upgrading/uploading userName (workbook upgrade does). This is because
    // the ds user name update isn't a must have for DF2.0 but it already works
    // for workbook upgrade so we support it to make upgrade smoother. Also,
    // upgrade of existing legacy dataflows doesn't need the user-name fixup
    // since they're global. So the only upgrade case being left out is the
    // on-the-fly upgrade of a legacy batch dataflow being uploaded into DF2.
    // The separation of datasets from dataflows makes this acceptable if not
    // recommended (so auto-creation of datasets when uploading dataflows is
    // avoided).
    //
    // Avoid the additional dev cost for legacy batch dataflows - the cost is a
    // very clunky API change (to pass two usernames around - the fake username
    // needed for dataflow upload that XD needs, and the uploading user's
    // username) - this seems too hacky and if it's not needed, don't do it.

    status = prepQueryForUpgrade(queryStr,
                                 NULL,
                                 0,
                                 retinaJson,
                                 &retinaQueryAndHdrStr);
    BailIfFailed(status);

    status = this->queryToDF2Upgrade(retinaQueryAndHdrStr);
    BailIfFailed(status);

CommonExit:
    if (uploadUdfOutputArray != NULL) {
        for (ii = 0; ii < numUdfModules; ii++) {
            assert(uploadUdfOutputArray[ii] != NULL);
            memFree(uploadUdfOutputArray[ii]);
            uploadUdfOutputArray[ii] = NULL;
        }
        memFree(uploadUdfOutputArray);
        uploadUdfOutputArray = NULL;
    }
    if (outputSizeArray != NULL) {
        memFree(outputSizeArray);
    }
    if (retinaQueryAndHdrStr) {
        memFree(retinaQueryAndHdrStr);
        retinaQueryAndHdrStr = NULL;
    }
    if (queryStrToFree) {
        memFree(queryStrToFree);
        queryStrToFree = NULL;
    }
    if (retinaInfo != NULL) {
        DagLib::get()->destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }
    if (retinaJson != NULL) {
        json_decref(retinaJson);
        retinaJson = NULL;
    }
    return status;
}

// Upload a workbook file into a new session (called from ::create).
// This routine extracts the various sections from the workbook file, needed
// to hydrate the session, including creating the dag when extracting the
// QGraph section. It also results in creating any on-disk files needed to
// house the various sections.
Status
SessionMgr::Session::upload(XcalarApiUserId *userId,
                            SessionUploadPayload *sessionUploadPayload)
{
    Status status = StatusOk;
    void *sessionContent;
    size_t sessionContentCount;
    ArchiveManifest *archiveManifest = NULL;
    json_t *archiveHeader = NULL;
    json_t *archiveChecksum = NULL;
    json_t *jsonWorkbookVersion = NULL;
    unsigned version;

    sessionContent = sessionUploadPayload->sessionContentP;
    sessionContentCount = sessionUploadPayload->sessionContentCount;

    status =
        archiveUnpack(sessionContent, sessionContentCount, &archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unpack session archive '%s' for Usr %s: %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // getArchiveChecksumFromManifest must be called even for V1 workbooks,
    // since the version isn't known yet. And that this routine returns StatusOk
    // even if checksum file isn't in manifest. The
    // getArchiveHeaderFromManifest() will either do checksum validation or not,
    // depending on whether archiveChecksum is present or not. The error
    // checking to ensure checksum is present for workbook versions >= V2,
    // occurs later, when version is known.
    status = getArchiveChecksumFromManifest(&archiveChecksum, archiveManifest);
    // Should not fail here because of missing checksum. Could be version 1
    // workbook.
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session archive checksum from archive '%s' for "
                "Usr '%s': %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Will only verify checksum if archiveChecksum is not NULL.
    status = getArchiveHeaderFromManifest(&archiveHeader,
                                          archiveManifest,
                                          archiveChecksum);
    if (status == StatusNoEnt) {
        // workbook header archive missing; this may be a retina tar.gz file
        // try upgrading this file
        Status status2;

        status2 = tryUpgradeRetinaToDF2(sessionContent, sessionContentCount);
        if (status2 == StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Upgraded legacy batch dataflow to DF2 dataflow'%s' for "
                    "Usr '%s'",
                    sessionPersistedData_.sessionName,
                    userId->userIdName);
            status = StatusOk;
            goto CommonExit;
        } else {
            status = status2;
            xSyslog(moduleName,
                    XlogErr,
                    "Session archive header missing in archive '%s' ; "
                    "also failed in upgrading as retina for "
                    "Usr '%s': %s",
                    sessionPersistedData_.sessionName,
                    userId->userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session archive header from archive '%s' for "
                "Usr '%s': %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    //
    // rest of code is about uploading a workbook (workbook header is valid)
    //
    jsonWorkbookVersion = json_object_get(archiveHeader, "workbookVersion");
    if (jsonWorkbookVersion == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get workbook version from archive '%s' for "
                "Usr '%s': %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    version = json_integer_value(jsonWorkbookVersion);
    if (version == 0 || version > WorkbookVersionCur) {
        status = StatusWorkbookInvalidVersion;
        xSyslog(moduleName,
                XlogErr,
                "Invalid workbook version '%d' from archive '%s' for "
                "Usr '%s': %s",
                version,
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Checksum file not found.
    if (version >= WorkbookVersion2 && !archiveChecksum) {
        xSyslogTxnBuf(moduleName,
                      XlogWarn,
                      "Failed to get workbook checksum from archive: %s",
                      strGetFromStatus(status));
        status = StatusChecksumNotFound;
        goto CommonExit;
    }

    // Extract each of the sections and process
    status = getSessionKvStoreFromManifest(archiveManifest, archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session KV from archive '%s' for Usr %s: %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = getSessionUDFsFromManifest(archiveHeader,
                                        archiveManifest,
                                        archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session Udfs from archive '%s' for Usr '%s': %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // unpack session metadata
    status = getSessionFromManifest(archiveManifest, archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session from archive '%s' for Usr %s: %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // upgrade the workbook if necessary
    status = wbUploadUpgrade(version, archiveManifest, archiveChecksum);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to upgrade version %d workbook from archive '%s' "
                      "for Usr '%s' (pls delete any stale datasets): %s",
                      version,
                      sessionPersistedData_.sessionName,
                      userId->userIdName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    status = getSessionJupyterNBsFromManifest(sessionUploadPayload
                                                  ->pathToAdditionalFiles,
                                              archiveManifest,
                                              archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get Jupyter notebooks from archive '%s' for "
                "Usr '%s': %s",
                sessionPersistedData_.sessionName,
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (archiveManifest != NULL) {
        archiveFreeManifest(archiveManifest);
        archiveManifest = NULL;
    }
    if (archiveHeader != NULL) {
        json_decref(archiveHeader);
        archiveHeader = NULL;
    }
    if (archiveChecksum) {
        json_decref(archiveChecksum);
        archiveChecksum = NULL;
    }
    return status;
}

void
SessionMgr::Session::setName(char *sessName)
{
    if (sessName != NULL) {
        strlcpy(sessionPersistedData_.sessionName,
                sessName,
                sizeof(sessionPersistedData_.sessionName));
    } else {
        sessionPersistedData_.sessionName[0] = '\0';
    }
}

// Extract session kvstore content from workbook, convert it to JSON, and add
// to supplied manifest for workbook download
Status
SessionMgr::Session::addSessionKvStoreToManifest(
    json_t *archiveChecksum, ArchiveManifest *archiveManifest)
{
    Status status = StatusOk;
    KvStoreLib *kvs = KvStoreLib::get();
    char *value = NULL;
    size_t valueSize = 0;
    char *jsonStr = NULL;
    KvStoreLib::KeyList *keyList = NULL;
    json_t *keyValues = NULL;
    json_t *jsonValue = NULL;
    int ret;
    char filePath[XcalarApiMaxPathLen + 1];

    status = kvs->list(sessionKvStoreId_, ".*", &keyList);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get key names for session ID %lX, name '%s': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    keyValues = json_object();
    BailIfNull(keyValues);

    for (int ii = 0; ii < keyList->numKeys; ii++) {
        status = kvs->lookup(sessionKvStoreId_,
                             keyList->keys[ii],
                             &value,
                             &valueSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get value of '%s' for session ID %lx, "
                    "name '%s': %s",
                    keyList->keys[ii],
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        assert(jsonValue == NULL);
        jsonValue = json_string(value);
        if (jsonValue == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to convert '%s' to json string",
                    value);
            goto CommonExit;
        }
        ret = json_object_set_new(keyValues, keyList->keys[ii], jsonValue);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to set '%s' in json object for key '%s'",
                    value,
                    keyList->keys[ii]);
            goto CommonExit;
        }
        jsonValue = NULL;  // reference passed to keyValues
        memFree(value);
        value = NULL;
    }

    jsonStr = json_dumps(keyValues, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    if (jsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert json to string for session KV for session "
                "ID %lx, name '%s': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   KvStoreFileName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveAddFileToManifest(filePath,
                                      jsonStr,
                                      strlen(jsonStr),
                                      archiveManifest,
                                      archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add session KV to archive for session ID %lx, "
                "name '%s': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
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
    if (jsonStr != NULL) {
        memFree(jsonStr);
        jsonStr = NULL;
    }
    if (keyValues != NULL) {
        json_decref(keyValues);
        keyValues = NULL;
    }
    if (jsonValue != NULL) {
        json_decref(jsonValue);
        jsonValue = NULL;
    }

    return status;
}

// Extract and update workbook's kvstore from workbook file being uploaded
Status
SessionMgr::Session::getSessionKvStoreFromManifest(
    ArchiveManifest *archiveManifest, const json_t *archiveChecksum)
{
    Status status = StatusOk;
    KvStoreLib *kvs = KvStoreLib::get();
    void *jsonRaw = NULL;
    size_t jsonRawSize = 0;
    char *jsonStr = NULL;
    json_t *keyValues = NULL;
    const char *keyName;
    json_t *jsonValue = NULL;
    json_error_t err;
    char *valueBuf = NULL;
    size_t valueBufSize = 0;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;
    char *keyNameDup = NULL;

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   KvStoreFileName);
    assert(ret > 0 && ret < (int) sizeof(filePath));

    status = archiveGetFileData(archiveManifest,
                                filePath,
                                &jsonRaw,
                                &jsonRawSize,
                                archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get session from archive for session ID %lx, "
                "name '%s': %s",
                sessionPersistedData_.sessionId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    // Change to null terminated string
    jsonStr = (char *) memAllocExt(jsonRawSize + 1, moduleName);

    if (jsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate '%lu' bytes for json string",
                jsonRawSize + 1);
        goto CommonExit;
    }
    memcpy(jsonStr, jsonRaw, jsonRawSize);
    jsonStr[jsonRawSize] = '\0';

    keyValues = json_loads(jsonStr, 0, &err);
    if (keyValues == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse session KV, source '%s', line %d, "
                "column %d, position %d: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text);
        goto CommonExit;
    }
    assert(json_typeof(keyValues) == JSON_OBJECT);
    json_object_foreach (keyValues, keyName, jsonValue) {
        const char *kvValue;
        // Clean up any values from prior iterations of this loop
        if (keyNameDup != NULL) {
            memFree(keyNameDup);
            keyNameDup = NULL;
        }
        if (valueBuf != NULL) {
            memFree(valueBuf);
            valueBuf = NULL;
        }
        status =
            kvs->lookup(sessionKvStoreId_, keyName, &valueBuf, &valueBufSize);
        // The key should not already exist
        if (status != StatusKvEntryNotFound) {
            if (status == StatusOk) {
                keyNameDup = strAllocAndCopy(keyName);
                BailIfNull(keyNameDup);
                if (strncmp(basename(keyNameDup),
                            WorkbookVersionInfoKey,
                            strlen(WorkbookVersionInfoKey)) == 0) {
                    // if this is the workbook version key, this would already
                    // exist so this isn't an error; continue to the next key
                    continue;
                }
                xSyslog(moduleName,
                        XlogErr,
                        "Key '%s' aready exists for session KV for session "
                        "ID %lx, name '%s': %s",
                        keyName,
                        sessionPersistedData_.sessionId,
                        sessionPersistedData_.sessionName,
                        strGetFromStatus(status));
                status = StatusExist;
                assert(0);
                goto CommonExit;
            } else {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to lookup key '%s' for session KV for session "
                        "ID %lx name '%s': %s",
                        keyName,
                        sessionPersistedData_.sessionId,
                        sessionPersistedData_.sessionName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
        kvValue = json_string_value(jsonValue);
        if (kvValue == NULL) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to convert json value to string for key "
                    " '%s' for session '%s', ID %lX: '%s'",
                    keyName,
                    sessionPersistedData_.sessionName,
                    sessionPersistedData_.sessionId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        status = kvs->addOrReplace(sessionKvStoreId_,
                                   keyName,
                                   strlen(keyName) + 1,
                                   kvValue,
                                   strlen(kvValue) + 1,
                                   true,
                                   KvStoreOptSync);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to add key '%s' to session KV for session ID %lx "
                    "name '%s': %s",
                    keyName,
                    sessionPersistedData_.sessionId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (keyValues != NULL) {
        json_decref(keyValues);
        keyValues = NULL;
    }
    if (jsonStr != NULL) {
        memFree(jsonStr);
        jsonStr = NULL;
    }
    if (keyNameDup != NULL) {
        memFree(keyNameDup);
        keyNameDup = NULL;
    }
    if (valueBuf != NULL) {
        memFree(valueBuf);
        valueBuf = NULL;
    }
    return status;
}

// store
Status
SessionMgr::Session::addSessionUDFsToManifest(json_t *archiveHeader,
                                              json_t *archiveChecksum,
                                              ArchiveManifest *archiveManifest)
{
    // badStatus is only ever set to bad status, so if it's never set,
    // it means we didn't encounter any errors
    Status status = StatusOk;
    Status badStatus = StatusOk;
    Status status2;
    DagLib *dagLib = DagLib::get();
    EvalUdfModuleSet udfModules;
    char filePath[XcalarApiMaxPathLen + 1];
    int ret;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    XcalarApiListXdfsOutput *listXdfsOutput;
    XcalarEval *xcalarEval = XcalarEval::get();
    Dag *tempQgraph = NULL;

    // The session is inactive so the Dags will be "stable".  This gets all
    // the UDF modules being used in the dataflow which may include ones
    // not specific to the workbook (currently only "default").
    //
    //
    // The udfModules will be populated by extracting UDF references from
    // the graph stored in the KVS. So getUdfModulesFromDag() isn't correct
    // here. Now, in the current model where the PQG in the session can
    // reference only UDFs in the workbook, downloading all UDFs in the workbook
    // is sufficient. So no scan needed of the PQG to decide which UDFs to
    // include in the download.
    //
    // XXX: However, in the future, UDFs may come from a different space (e.g.
    // global) and the PQG may refer to them - such UDFs will need to be
    // identified from PQG in KVS, and then copied from the source space, into
    // the download content here. This needs a 'getUdfsFromQgraph(kvs,...)'
    // which would get the PQG from the KVS and scan it for the UDFs.
    //

    // Get all UDFS in the workbook including those not currently being used.

    status =
        xcalarEval->getFnList("*",
                              strGetFromFunctionCategory(FunctionCategoryUdf),
                              udfContainer_,
                              &output,
                              &outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Download failed to get UDFs for Usr '%s' session '%s': %s",
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));

        status = isErrorFixable(status, &badStatus);
        BailIfFailed(status);
    } else {
        // Add any unused ones
        listXdfsOutput = &output->outputResult.listXdfsOutput;
        for (unsigned ii = 0; ii < listXdfsOutput->numXdfs; ii++) {
            char *fnName = listXdfsOutput->fnDescs[ii].fnName;
            // XXX: eventually, could we get rid of this and just
            // get get all UDFs in the workbook without bothering
            // to check in-use or not since the result is the same?
            // shared UDFs though may be in udfModules from a prior
            // call to get UDF refs from the dag - but with the
            // DF2.0 dag in kvs, this may not be done - in which
            // case, we could just get all UDFs in workbook and
            // not bother about searching dag. This would simplify
            // the logic which forces us to call
            // getUdfModulesFromLoad()...
            //
            // status = UserDefinedFunction::get()->getUdfName(fqFnName,
            // sizeof(fqFnName),
            // listXdfsOutput->fnDescs[ii].fnName,
            // udfContainer_,
            // false);
            // BailIfFailed(status);

            status =
                xcalarEval->getUdfModuleFromLoad(fnName, &udfModules, NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Download failed to get UDF module for Usr '%s', "
                        "session '%s': %s",
                        sessionPersistedData_.sessionOwningUserId,
                        sessionPersistedData_.sessionName,
                        strGetFromStatus(status));

                status = isErrorFixable(status, &badStatus);
                BailIfFailed(status);
            }
        }
    }

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   UdfWkbkDirName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    status = archiveAddDirToManifest(filePath, archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add directory '%s' to archive for session Usr '%s' "
                "name '%s': %s",
                filePath,
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Add each of the UDF modules to the archive and the info about each
    // module to the session archive header.
    status = dagLib->populateJsonUdfModulesArray(WorkbookDirName,
                                                 filePath,
                                                 archiveHeader,
                                                 archiveManifest,
                                                 udfContainer_,
                                                 &udfModules,
                                                 archiveChecksum);
    status = isErrorFixable(status, &badStatus);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Download failed to create json array for Usr '%s' "
                "session '%s': %s",
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status == StatusOk) {
        status = badStatus;
    }

    udfModules.removeAll(&EvalUdfModule::del);

    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
    if (tempQgraph != NULL) {
        status2 = dagLib->destroyDag(tempQgraph, DagTypes::DestroyDeleteNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy Dag for Usr %s, session %s: %s",
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status2));
        }
        tempQgraph = NULL;
    }
    return status;
}

Status
SessionMgr::Session::getSessionUDFsFromManifest(
    json_t *archiveHeader,
    ArchiveManifest *archiveManifest,
    const json_t *archiveChecksum)
{
    Status status = StatusOk;
    DagLib *dagLib = DagLib::get();
    RetinaInfo *retinaInfo = NULL;
    uint64_t numUdfModules = 0;
    XcalarApiOutput **uploadUdfOutputArray = NULL;
    size_t *outputSizeArray = NULL;
    char filePath[XcalarApiMaxPathLen + 1];
    XcalarApiUdfContainer *udfContainer = NULL;
    int ret;

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s%s",
                   WorkbookDirName,
                   UdfWkbkDirName);
    assert(ret >= 0 && ret < (int) sizeof(filePath));

    // This is only used to get the UDFs
    retinaInfo = (RetinaInfo *) memAllocExt(sizeof(*retinaInfo), moduleName);
    BailIfNull(retinaInfo);
    memZero(retinaInfo, sizeof(*retinaInfo));

    status = dagLib->populateUdfModulesArray(archiveHeader,
                                             filePath,
                                             retinaInfo,
                                             archiveManifest,
                                             archiveChecksum);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to extract Udf modules for session Usr '%s' "
                "name '%s': %s",
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    numUdfModules = retinaInfo->numUdfModules;
    if (numUdfModules > 0) {
        status =
            UserDefinedFunction::get()->bulkAddUdf(retinaInfo->udfModulesArray,
                                                   numUdfModules,
                                                   udfContainer_,
                                                   &uploadUdfOutputArray,
                                                   &outputSizeArray);
        assert(status != StatusUdfModuleAlreadyExists);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to add Udf modules for session Usr '%s' "
                    "name '%s': %s",
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (retinaInfo != NULL) {
        dagLib->destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }
    if (udfContainer != NULL) {
        memFree(udfContainer);
        udfContainer = NULL;
    }
    if (uploadUdfOutputArray != NULL) {
        for (unsigned ii = 0; ii < numUdfModules; ii++) {
            assert(uploadUdfOutputArray[ii] != NULL);
            memFree(uploadUdfOutputArray[ii]);
            uploadUdfOutputArray[ii] = NULL;
        }
        memFree(uploadUdfOutputArray);
        uploadUdfOutputArray = NULL;
    }
    if (outputSizeArray != NULL) {
        memFree(outputSizeArray);
        outputSizeArray = NULL;
    }

    return status;
}

Status
SessionMgr::Session::addSessionJupyterNBsToManifest(
    json_t *archiveChecksum,
    const char *pathToAdditionalFiles,
    ArchiveManifest *archiveManifest)
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    FILE *fp = NULL;
    char jupyterPathInArchive[XcalarApiMaxPathLen + 1];
    char filePath[XcalarApiMaxPathLen + 1];
    char notebookPathInArchive[XcalarApiMaxPathLen + 1];
    int ret;
    int retVal;
    struct stat statBuf;
    void *fileContents = NULL;
    size_t fileSize;
    char fullPathToAdditionalFiles[XcalarApiMaxPathLen + 1];

    if (strlen(pathToAdditionalFiles) == 0) {
        // Nothing to add
        goto CommonExit;
    }
    if (pathToAdditionalFiles[0] == '/') {
        // Already fully qualified path name
        if (strlcpy(fullPathToAdditionalFiles,
                    pathToAdditionalFiles,
                    sizeof(fullPathToAdditionalFiles)) >=
            sizeof(fullPathToAdditionalFiles)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
    } else {
        // Relative name to XLRROOT
        ret = snprintf(fullPathToAdditionalFiles,
                       sizeof(fullPathToAdditionalFiles),
                       "%s/%s",
                       XcalarConfig::get()->xcalarRootCompletePath_,
                       pathToAdditionalFiles);
        if (ret < 0 || ret >= (int) sizeof(fullPathToAdditionalFiles)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
    }

    ret = snprintf(jupyterPathInArchive,
                   sizeof(jupyterPathInArchive),
                   "%s%s",
                   WorkbookDirName,
                   JupyterNBDirName);
    if (ret < 0 || ret >= (int) sizeof(jupyterPathInArchive)) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

    status = archiveAddDirToManifest(jupyterPathInArchive, archiveManifest);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add directory '%s' to archive for Usr '%s' "
                "name '%s': %s",
                jupyterPathInArchive,
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    dirIter = opendir(fullPathToAdditionalFiles);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to open Jupyter notebook directory '%s' for session "
                "Usr '%s' name '%s': %s",
                fullPathToAdditionalFiles,
                sessionPersistedData_.sessionOwningUserId,
                sessionPersistedData_.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    while (true) {
        // glibc docs for correct use of readdir/readdir_r/errno and threads
        errno = 0;
        struct dirent *currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            // The only error is EBAD, which should exclusively be a program
            // error.
            assert(errno == 0);
            // End of directory.
            break;
        }
        if (strcmp(currentFile->d_name, ".") == 0 ||
            strcmp(currentFile->d_name, "..") == 0) {
            continue;
        }
        // All files in the directory are included as they may be ones
        // associated with the notebook.

        ret = snprintf(filePath,
                       sizeof(filePath),
                       "%s%s",
                       fullPathToAdditionalFiles,
                       currentFile->d_name);
        if (ret < 0 || ret >= (int) sizeof(filePath)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
        retVal = stat(filePath, &statBuf);
        if (retVal != 0) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }
        if (S_ISDIR(statBuf.st_mode)) {
            // No subdirectories
            continue;
        }
        // @SymbolCheckIgnore
        fp = fopen(filePath, "r");
        if (fp == NULL) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to open '%s' for session Usr '%s' name '%s': %s",
                    filePath,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        fileSize = statBuf.st_size;
        fileContents = (void *) memAllocExt(fileSize, moduleName);
        if (fileContents == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to allocate %lu bytes for '%s' for session "
                    "Usr '%s' name '%s'",
                    fileSize,
                    currentFile->d_name,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName);
            goto CommonExit;
        }
        // @SymbolCheckIgnore
        if (fread(fileContents, 1, fileSize, fp) != fileSize) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to read '%s' for session Usr '%s' name '%s': %s",
                    filePath,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        ret = snprintf(notebookPathInArchive,
                       sizeof(notebookPathInArchive),
                       "%s%s",
                       jupyterPathInArchive,
                       currentFile->d_name);
        if (ret < 0 || ret >= (int) sizeof(notebookPathInArchive)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }

        status = archiveAddFileToManifest(notebookPathInArchive,
                                          fileContents,
                                          fileSize,
                                          archiveManifest,
                                          archiveChecksum);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to add '%s' to archive for session Usr '%s' "
                    "name '%s': %s",
                    filePath,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;

        assert(fileContents != NULL);
        memFree(fileContents);
        fileContents = NULL;
    }

CommonExit:
    if (fileContents != NULL) {
        assert(status != StatusOk);
        memFree(fileContents);
        fileContents = NULL;
    }
    if (fp != NULL) {
        assert(status != StatusOk);
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }
    if (dirIter != NULL) {
        closedir(dirIter);
        dirIter = NULL;
    }

    return status;
}

Status
SessionMgr::Session::getSessionJupyterNBsFromManifest(
    const char *pathToAdditionalFiles,
    ArchiveManifest *archiveManifest,
    const json_t *archiveChecksum)
{
    Status status = StatusOk;
    ArchiveFile *archiveFile;
    void *fileContents = NULL;
    size_t fileSize = 0;
    char *notebookName;
    char filePath[XcalarApiMaxPathLen + 1];
    FILE *fp = NULL;
    size_t sizeWritten;
    int ret;
    char fullPathToAdditionalFiles[XcalarApiMaxPathLen + 1];

    if (strlen(pathToAdditionalFiles) == 0) {
        // Caller doesn't want anything
        goto CommonExit;
    }
    if (pathToAdditionalFiles[0] == '/') {
        // Already fully qualified path name
        if (strlcpy(fullPathToAdditionalFiles,
                    pathToAdditionalFiles,
                    sizeof(fullPathToAdditionalFiles)) >
            sizeof(fullPathToAdditionalFiles)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
    } else {
        // Relative name to XLRROOT
        ret = snprintf(fullPathToAdditionalFiles,
                       sizeof(fullPathToAdditionalFiles),
                       "%s/%s",
                       XcalarConfig::get()->xcalarRootCompletePath_,
                       pathToAdditionalFiles);
        if (ret < 0 || ret >= (int) sizeof(fullPathToAdditionalFiles)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
    }

    for (ArchiveHashTable::iterator it = archiveManifest->fileContents->begin();
         (archiveFile = it.get()) != NULL;
         it.next()) {
        if (!archiveFile->isFile) {
            // Skip directories
            continue;
        }
        if (!strstr(archiveFile->fileName, JupyterNBDirName)) {
            // Not a file in the jupyter notebook directory
            continue;
        }

        status = archiveGetFileData(archiveManifest,
                                    archiveFile->fileName,
                                    &fileContents,
                                    &fileSize,
                                    archiveChecksum);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get Jupyter notebook '%s' from archive for "
                    "session Usr '%s' name '%s': %s",
                    archiveFile->fileName,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        // Strip off the section name to get the original file name
        notebookName = (char *) rindex(archiveFile->fileName, '/');
        if (notebookName == NULL) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid Jupyter noteboook '%s' found in archive for "
                    "session Usr '%s' name '%s'",
                    archiveFile->fileName,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName);
            assert(0);
            goto CommonExit;
        }
        notebookName++;  // Advance past "/"
        assert(*notebookName != '\0');
        ret = snprintf(filePath,
                       sizeof(filePath),
                       "%s%s",
                       fullPathToAdditionalFiles,
                       notebookName);
        if (ret < 0 || ret >= (int) sizeof(filePath)) {
            status = StatusNameTooLong;
            goto CommonExit;
        }
        // Create the directory
        status = FileUtils::recursiveMkdir(fullPathToAdditionalFiles, 0750);
        if (status != StatusOk && status != StatusExist) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create directory '%s' for session Usr '%s' "
                    "name '%s': %s",
                    fullPathToAdditionalFiles,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // @SymbolCheckIgnore
        fp = fopen(filePath, "w");
        if (fp == NULL) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to open '%s' for session Usr '%s' name '%s': %s",
                    filePath,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        // @SymbolCheckIgnore
        sizeWritten = fwrite(fileContents, 1, fileSize, fp);
        if (sizeWritten != fileSize) {
            status = StatusFailed;
            xSyslog(moduleName,
                    XlogErr,
                    "Incomplete write (%lu of %lu bytes) to file '%s' for "
                    "session Usr '%s' name '%s'",
                    sizeWritten,
                    fileSize,
                    filePath,
                    sessionPersistedData_.sessionOwningUserId,
                    sessionPersistedData_.sessionName);
            goto CommonExit;
        }
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

CommonExit:
    if (fp != NULL) {
        assert(status != StatusOk);
        fclose(fp);
        fp = NULL;
    }

    return status;
}

//
// Each session has a udfContainer_ ; initialize it with user and session info
// Even though this seems redundant (since sessionPersistedData_ already has
// this info), there are two reasons: main one is that in the future, a
// retinaName will be added to the container for retinas in workbooks. Secondary
// one is that of convenience since wherever a session needs the udfContainer
// info in that layout, it's available
//
Status
SessionMgr::Session::initUdfContainer()
{
    Status status = StatusOk;
    XcalarApiUserId userId;
    XcalarApiSessionInfoInput sessInfo;

    udfContainer_ =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(*udfContainer_),
                                              moduleName);
    BailIfNull(udfContainer_);

    strlcpy(userId.userIdName,
            sessionPersistedData_.sessionOwningUserId,
            sizeof(userId.userIdName));
    userId.userIdUnique = 0;  // noop
    strlcpy(sessInfo.sessionName,
            sessionPersistedData_.sessionName,
            sizeof(sessInfo.sessionName));
    sessInfo.sessionNameLength = strlen(sessInfo.sessionName);
    sessInfo.sessionId = sessionPersistedData_.sessionId;

    status = UserDefinedFunction::initUdfContainer(udfContainer_,
                                                   &userId,
                                                   &sessInfo,
                                                   NULL);
CommonExit:
    return status;
}

Status
SessionMgr::Session::deleteWorkbookUdfs(XcalarApiUdfContainer *udfContainer)
{
    Status status = StatusOk;
    XcalarEval *xcalarEval = XcalarEval::get();
    UserDefinedFunction *udfLib = UserDefinedFunction::get();
    XcalarApiOutput *getFnOutput = NULL;
    size_t getFnOutputSize = 0;
    XcalarApiListXdfsOutput *listXdfsOutput;
    XcalarApiUdfDeleteInput deleteUdfInput;
    const char *sessionName = udfContainer->sessionInfo.sessionName;
    const char *userName = udfContainer->userId.userIdName;
    const uint64_t sessionId = udfContainer->sessionInfo.sessionId;
    EvalUdfModuleSet udfModules;
    EvalUdfModule *module;

    // Get the list of UDF functions used by the workbook.

    status =
        xcalarEval->getFnList("*",
                              strGetFromFunctionCategory(FunctionCategoryUdf),
                              udfContainer,
                              &getFnOutput,
                              &getFnOutputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get UDFs for user '%s' (session '%s' ID '%lx'): %s",
                userName,
                sessionName,
                sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // We have a list of functions but want to do the deletion on a per-module
    // basis.  So first, build a list of the modules
    listXdfsOutput = &getFnOutput->outputResult.listXdfsOutput;
    for (unsigned ii = 0; ii < listXdfsOutput->numXdfs; ii++) {
        char *fqFnName = listXdfsOutput->fnDescs[ii].fnName;
        status = xcalarEval->getUdfModuleFromLoad(fqFnName, &udfModules, NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get UDF module for '%s' for user '%s' "
                    "(session '%s' ID '%lX'): %s",
                    fqFnName,
                    userName,
                    sessionName,
                    sessionId,
                    strGetFromStatus(status));
            // Keep going...
        }
    }
    // Now delete each module
    for (EvalUdfModuleSet::iterator it = udfModules.begin();
         (module = it.get()) != NULL;
         it.next()) {
        strlcpy(deleteUdfInput.moduleName,
                module->getName(),
                sizeof(deleteUdfInput.moduleName));
        status = udfLib->deleteUdf(&deleteUdfInput, udfContainer);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete UDF module '%s' for user '%s' (session "
                    "'%s' ID '%lX'): %s",
                    module->getName(),
                    userName,
                    sessionName,
                    sessionId,
                    strGetFromStatus(status));
            // Keep going...
        }
    }

    // Delete the persisted workbook directory for this session.

    status = udfLib->deleteWorkbookDirectory(userName, sessionId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to remove workbook directory for user '%s' "
                "(session ID %lX): %s",
                userName,
                sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (getFnOutput != NULL) {
        memFree(getFnOutput);
        getFnOutput = NULL;
    }

    udfModules.removeAll(&EvalUdfModule::del);

    return status;
}

// Process the entire on-disk file system tree of a workbook's UDFs or
// Dataflows. The code is recursive, and the "treeDepth" reflects the depth of
// recursion. The code assumes that the file path to a workbook's UDFs dir is:
//
//   <sharedXcalarRoot>/workbooks/<userName>/<workbookID>/udfs
//
// or the path to a workbook's Dataflows dir is:
//
//   <sharedXcalarRoot>/workbooks/<userName>/<workbookID>/dataflows
//
// Shared UDFs are under <sharedXcalarRoot>/sharedUdfs - they're loaded via
// a different routine
//
// NOTE: if upgradeToDionysus is true, the logic is different but shares common
// code with the non-upgrade logic. Upgrade expects UDF metafiles, which are
// used to identify and validate the UDF files...after the loading is
// successful, all metafiles and any old versioned UDFs are deleted, and the
// UDF file with the largest version is retained after stripping its version
// suffix since the new design in Dionysus and beyond doesn't need to retain a
// module's version number persistently.
//
// XXX: Note that the decision on whether to have a upgradeToDionysus flag
// here, or whether to let processWorkbookDirTree always execute the upgrade
// code unconditionally (a no-op if the legacy UDF meta files are absent), is
// still TBD (until upgrade milestone)

Status
SessionMgr::processWorkbookDirTree(WorkbookDirType dirType,
                                   DIR *dirIter,
                                   char *dirPath,
                                   size_t maxDirNameLen,
                                   XcalarApiUdfContainer *udfContainer,
                                   bool upgradeToDionysus,
                                   int *treeDepth)
{
    Status status = StatusUnknown;
    DIR *cdirIter = NULL;
    char *cdirName = NULL;
    char *cdirPath = NULL;
    LogLib *logLib = LogLib::get();
    char *dirPathDup = NULL;

    xSyslog(moduleName,
            XlogDebug,
            "processWorkbookDirTree processing directory %s",
            dirPath);

    if (*treeDepth == 3) {
        dirPathDup = strAllocAndCopy(dirPath);
        BailIfNull(dirPathDup);
        if (dirType == WorkbookDirType::Udfs &&
            strcmp(basename(dirPathDup), LogLib::UdfWkBookDirName) == 0) {
            // reached a leaf dir where UDFs reside - process them
            UserDefinedFunction::get()->processUdfsDir(dirIter,
                                                       dirPath,
                                                       maxDirNameLen,
                                                       udfContainer,
                                                       upgradeToDionysus);
        } else if (dirType == WorkbookDirType::Dataflows &&
                   strcmp(basename(dirPathDup),
                          LogLib::DataflowWkBookDirName) == 0) {
            // XXX: fill in with dataflow sandboxing
        }
    } else {
        cdirName = (char *) memAllocExt(maxDirNameLen, moduleName);
        BailIfNull(cdirName);
        cdirPath =
            (char *) memAllocExt(SessionMgr::MaxDataflowPathLen, moduleName);
        BailIfNull(cdirPath);
        // traverse each dir in this dir
        while ((cdirIter = getNextDirAndName(dirIter,
                                             dirPath,
                                             cdirName,
                                             maxDirNameLen)) != NULL) {
            if (*treeDepth == 0) {
                assert(strcmp(dirPath, logLib->wkbkPath_) == 0);
                // very custom to current layout; a dir in a level 0 dir
                // is found - this must be a user dir - update udfContainer
                // XXX: cdirName could be the special 'udfs' dir if we allow
                // /workbooks/udfs to appear as a container for 'global' UDFs
                // This is still TBD.
                strlcpy(udfContainer->userId.userIdName,
                        cdirName,
                        sizeof(udfContainer->userId.userIdName));
                udfContainer->userId.userIdUnique = 0;
            } else if (*treeDepth == 1) {
                dirPathDup = strAllocAndCopy(dirPath);
                BailIfNull(dirPathDup);
                assert(strcmp(basename(dirPathDup),
                              udfContainer->userId.userIdName) == 0);
                // traversing a user dir so this dir must be a workbook ID
                // XXX: again, this is false if we allow global UDFs
                udfContainer->sessionInfo.sessionId =
                    strtoll(cdirName, NULL, 16);
                udfContainer->sessionInfo.sessionNameLength = 0;
            } else {
                // assert the current layout.
                // XXX: will need to extend if a wkbook dir can have other dirs,
                // or we need to go deeper to find a leaf udfs dir (e.g. if
                // dataflows dir is added as a subdir inside a workbook dir)
                assert(*treeDepth == 2);
                assert(strcmp(cdirName, LogLib::UdfWkBookDirName) == 0);
            }
            snprintf(cdirPath, MaxDataflowPathLen, "%s/%s", dirPath, cdirName);
            *treeDepth += 1;
            status = processWorkbookDirTree(dirType,
                                            cdirIter,
                                            cdirPath,
                                            maxDirNameLen,
                                            udfContainer,
                                            upgradeToDionysus,
                                            treeDepth);
            BailIfFailed(status);
            *treeDepth -= 1;

            if (cdirIter != NULL) {
                verify(closedir(cdirIter) == 0);
                cdirIter = NULL;
            }
        }
    }
    status = StatusOk;
CommonExit:
    if (cdirName != NULL) {
        memFree(cdirName);
        cdirName = NULL;
    }
    if (cdirPath != NULL) {
        memFree(cdirPath);
        cdirPath = NULL;
    }
    if (dirPathDup != NULL) {
        memFree(dirPathDup);
        dirPathDup = NULL;
    }
    if (cdirIter != NULL) {
        verify(closedir(cdirIter) == 0);
        cdirIter = NULL;
    }
    return status;
}

//
// Get the next sub-directory in the directory supplied via "dirIter", whose
// path is "dirPath".
//
// Return the name of this sub-directory in "nextDirName", and its DIR pointer
// via the function return.
//
DIR *
SessionMgr::getNextDirAndName(DIR *dirIter,
                              char *dirPath,
                              char *nextDirName,
                              size_t maxDirNameLen)
{
    DIR *cdirIter = NULL;
    Status status;
    char dirAbsPath[MaxDataflowPathLen + 1];
    int ret;
    struct stat fileStat;

    while (true) {
        errno = 0;
        struct dirent *currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error
            assert(errno == 0);
            // End of directory
            break;
        }

        // Skip entries for the current and higher level directory as well
        // as the unlikely case of "dot-files"
        if (currentFile->d_name[0] == '.') {
            continue;
        }

        verify(snprintf(dirAbsPath,
                        sizeof(dirAbsPath),
                        "%s/%s",
                        dirPath,
                        currentFile->d_name) < (int) sizeof(dirAbsPath));

        ret = stat(dirAbsPath, &fileStat);
        if (ret == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "getNextDirAndName stat of '%s' failed: %s",
                    dirAbsPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // Skip non-dirs since we're looking for dirs to descend into
        if (!S_ISDIR(fileStat.st_mode)) {
            continue;
        }

        // found dir; open it

        cdirIter = opendir(dirAbsPath);
        if (cdirIter == NULL) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "getNextDirAndName opendir of %s failed: %s",
                    dirAbsPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        strlcpy(nextDirName, currentFile->d_name, maxDirNameLen);
        break;  // found a dir so get out
    }
CommonExit:
    return cdirIter;
}

Status
SessionMgr::Session::getResultsetIdsByTableId(TableNsMgr::TableId tableId,
                                              ResultSetId **rsIdsOut,
                                              unsigned *numRsIdsOut)
{
    TableRsIdTrack *tableRsidTrack = NULL;
    RsIdTrack *rsIdTrack = NULL;
    ResultSetId *rsIds = NULL;
    unsigned numRsIds = 0;
    unsigned ii = 0;

    *numRsIdsOut = 0;
    auto guard = tableRsLock_.take();

    tableRsidTrack = tableRsIdHt_.find(tableId);
    if (tableRsidTrack == NULL) {
        return StatusOk;
    }
    numRsIds = tableRsidTrack->rsIdHt_.getSize();
    if (numRsIds == 0) {
        return StatusOk;
    }
    rsIds = new (std::nothrow) ResultSetId[numRsIds];
    if (rsIds == NULL) {
        return StatusNoMem;
    }
    for (SessionMgr::Session::RsIdHashTable::iterator it =
             tableRsidTrack->rsIdHt_.begin();
         (rsIdTrack = it.get()) != NULL;
         it.next()) {
        rsIds[ii++] = rsIdTrack->getRsId();
    }

    *numRsIdsOut = numRsIds;
    *rsIdsOut = rsIds;

    return StatusOk;
}

Status
SessionMgr::Session::trackResultset(ResultSetId rsId,
                                    TableNsMgr::TableId tableId)
{
    Status status;
    TableRsIdTrack *tableRsidTrack = NULL;
    bool locked = false;
    RsIdTrack *rsIdTrack = NULL;
#if 0
    xSyslog(moduleName,
            XlogDebug,
            "Session::trackResultsets session %ld '%s', rsId %ld tableId %ld",
            getSessId(),
            getName(),
            rsId,
            tableId);
#endif
    locked = true;
    tableRsLock_.lock();

    tableRsidTrack = tableRsIdHt_.find(tableId);
    if (tableRsidTrack == NULL) {
        tableRsidTrack = new (std::nothrow) TableRsIdTrack(tableId);
        BailIfNullMsg(tableRsidTrack,
                      status,
                      moduleName,
                      "Failed trackResultset session %ld '%s', rsId %ld, "
                      "tableId %ld: %s",
                      getSessId(),
                      getName(),
                      rsId,
                      tableId,
                      strGetFromStatus(status));

        status = tableRsIdHt_.insert(tableRsidTrack);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed trackResultset session %ld '%s', rsId %ld "
                        "tableId %ld: %s",
                        getSessId(),
                        getName(),
                        rsId,
                        tableId,
                        strGetFromStatus(status));
    }

    rsIdTrack = new (std::nothrow) RsIdTrack(rsId);
    BailIfNullMsg(rsIdTrack,
                  status,
                  moduleName,
                  "Failed trackResultset session %ld '%s', rsId %ld tableId "
                  "%ld: %s",
                  getSessId(),
                  getName(),
                  rsId,
                  tableId,
                  strGetFromStatus(status));

    status = tableRsidTrack->rsIdHt_.insert(rsIdTrack);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed trackResultset session %ld '%s', rsId %ld tableId "
                    "%ld: %s",
                    getSessId(),
                    getName(),
                    rsId,
                    tableId,
                    strGetFromStatus(status));
    rsIdTrack = NULL;

CommonExit:
    if (rsIdTrack) {
        delete rsIdTrack;
        rsIdTrack = NULL;
    }
    if (locked) {
        tableRsLock_.unlock();
    }
    return status;
}

void
SessionMgr::Session::untrackResultset(ResultSetId rsId,
                                      TableNsMgr::TableId tableId)
{
    Status status;
    TableRsIdTrack *tableRsidTrack = NULL;
    bool locked = false;
    RsIdTrack *rsIdTrack = NULL;
#if 0
    xSyslog(moduleName,
            XlogDebug,
            "Session::untrackResultsets session %ld '%s', rsId %ld tableId %ld",
            getSessId(),
            getName(),
            rsId,
            tableId);
#endif
    locked = true;
    tableRsLock_.lock();

    tableRsidTrack = tableRsIdHt_.find(tableId);
    if (tableRsidTrack == NULL) {
        status = StatusNoEnt;
        xSyslog(moduleName,
                XlogErr,
                "Failed untrackResultset session %ld '%s', rsId %ld tableId "
                "%ld: %s",
                getSessId(),
                getName(),
                rsId,
                tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    rsIdTrack = tableRsidTrack->rsIdHt_.remove(rsId);
    if (rsIdTrack == NULL) {
        status = StatusNoEnt;
        xSyslog(moduleName,
                XlogErr,
                "Failed untrackResultset session %ld '%s', rsId %ld tableId "
                "%ld: %s",
                getSessId(),
                getName(),
                rsId,
                tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    rsIdTrack->destroy();
    rsIdTrack = NULL;

    if (tableRsidTrack->rsIdHt_.getSize() == 0) {
        verify(tableRsIdHt_.remove(tableId) == tableRsidTrack);
        tableRsidTrack->destroy();
        tableRsidTrack = NULL;
    }

CommonExit:
    if (locked) {
        tableRsLock_.unlock();
    }
}

void
SessionMgr::Session::untrackResultsets(TableNsMgr::TableId tableId)
{
    xSyslog(moduleName,
            XlogInfo,
            "Session::untrackResultsets session %ld '%s', tableId %ld",
            getSessId(),
            getName(),
            tableId);

    TableRsIdTrack *tableRsidTrack = tableRsIdHt_.remove(tableId);
    if (tableRsidTrack == NULL) {
        return;
    }

    tableRsidTrack->destroy();
    tableRsidTrack = NULL;
}

void
SessionMgr::Session::untrackResultsets()
{
    xSyslog(moduleName,
            XlogInfo,
            "Session::untrackResultsets %ld %s",
            getSessId(),
            getName());
    tableRsIdHt_.removeAll(&TableRsIdTrack::destroy);
}

// This method deletes all the keys in session prefixed with
// IMD_ONLY_NP_PREFIX_
Status
SessionMgr::Session::deleteNPPrefixedImdKeys(KvStoreId id)
{
    Status status = StatusOk;
    Status tmpStatus = StatusOk;
    KvStoreLib::KeyList *keyList = NULL;
    KvStoreLib *kvs = KvStoreLib::get();
    status = kvs->list(id, NonPersistIMDKeyALL, &keyList);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get imd only non-persist list with prefix %s: "
                "%s",
                NonPersistIMDKeyALL,
                strGetFromStatus(status));
        goto CommonExit;
    }
    for (int ii = 0; ii < keyList->numKeys; ii++) {
        // will attempt to delete all the keys and reports last failure
        tmpStatus = kvs->del(id, keyList->keys[ii], KvStoreOptSync);
        if (tmpStatus != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete imd non-persist key %s: "
                    "%s",
                    keyList->keys[ii],
                    strGetFromStatus(tmpStatus));
            status = tmpStatus;
        }
    }
CommonExit:
    if (keyList != NULL) {
        memFree(keyList);
        keyList = NULL;
    }
    return status;
}

#ifdef DEBUG_SESSION_REFS
void
SessionMgr::Session::printBtsForOutstandOps(const char *tag)
{
    static constexpr const uint32_t BtBuffer = 4096;
    static constexpr const uint32_t TraceSize = 8;
    char btBuffer[BtBuffer];
    void *trace[TraceSize];
    int traceSize = getBacktrace(trace, TraceSize);
    printBackTrace(btBuffer, sizeof(btBuffer), traceSize, trace);

    xSyslog(moduleName,
            XlogInfo,
            "Session %s ID %lX REF %ld '%s'\n%s",
            getName(),
            getSessId(),
            atomicRead64(&outstandingOps_),
            tag,
            btBuffer);
}
#endif  // DEBUG_SESSION_REFS

uint64_t
SessionMgr::Session::incOutstandOps()
{
#ifdef DEBUG_SESSION_REFS
    printBtsForOutstandOps("INC");
#endif  // DEBUG_SESSION_REFS
    return atomicInc64(&outstandingOps_);
}

uint64_t
SessionMgr::Session::decOutstandOps()
{
#ifdef DEBUG_SESSION_REFS
    printBtsForOutstandOps("DEC");
#endif  // DEBUG_SESSION_REFS
    assert(atomicRead64(&outstandingOps_) > 0);
    return atomicDec64(&outstandingOps_);
}
