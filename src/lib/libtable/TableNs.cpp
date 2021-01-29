// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <math.h>

#include "primitives/Primitives.h"
#include "sys/XLog.h"
#include "ns/LibNs.h"
#include "table/TableNs.h"
#include "table/Table.h"
#include "TableNsInternal.h"
#include "msg/Xid.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "operators/Operators.h"

TableNsMgr *TableNsMgr::instance = NULL;

TableNsMgr *
TableNsMgr::get()
{
    return instance;
}

Status
TableNsMgr::init()
{
    instance = new (std::nothrow) TableNsMgr();
    if (instance == NULL) {
        return StatusNoMem;
    }
    return instance->initInternal();
}

Status
TableNsMgr::initInternal()
{
    Status status = StatusOk;
    return status;
}

void
TableNsMgr::destroy()
{
    delete instance;
    instance = NULL;
}

TableNsMgr::TableId
TableNsMgr::genTableId()
{
    return XidMgr::get()->xidGetNext();
}

Status
TableNsMgr::listGlobalTables(const char *pattern,
                             char **&retTableNames,
                             size_t &retNumTables)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    LibNsTypes::PathInfoOut *pathInfo = NULL;
    char regexBuf[LibNsTypes::MaxPathNameLen];
    TableMgr *tmgr = TableMgr::get();
    TableIdRecord *idRecord = NULL;
    size_t curIdx = 0;

    retTableNames = NULL;
    retNumTables = 0;

    int ret = snprintf(regexBuf,
                       LibNsTypes::MaxPathNameLen,
                       "%s/*/%s",
                       TableNamesNsPrefix,
                       pattern);
    if (ret < 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed listGlobalTables: %s",
                strGetFromStatus(status));
        return status;
    }

    if ((size_t) ret >= LibNsTypes::MaxPathNameLen) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Failed listGlobalTables: %s",
                strGetFromStatus(status));
        return status;
    }

    size_t numPaths = libNs->getPathInfo(regexBuf, &pathInfo);
    if (numPaths == 0) {
        goto CommonExit;
    }

    char **tableNames;
    tableNames = new (std::nothrow) char *[numPaths];
    BailIfNullMsg(tableNames,
                  StatusNoMem,
                  ModuleName,
                  "Failed listGlobalTables: %s",
                  strGetFromStatus(status));

    for (size_t ii = 0; ii < numPaths; ++ii) {
        TableNsMgr::TableId curTableId =
            tmgr->getTableIdFromName(pathInfo[ii].pathName);
        if (curTableId == TableNsMgr::InvalidTableId) {
            // List Tables could race with Table deletes
            status = StatusOk;
            continue;
        }

        if (idRecord) {
            memFree(idRecord);
            idRecord = NULL;
        }

        status = getIdRecord(curTableId, &idRecord);
        if (status != StatusOk) {
            if (status == StatusTableNameNotFound) {
                // List Tables could race with Table deletes
                status = StatusOk;
                continue;
            } else {
                goto CommonExit;
            }
        }

        if (idRecord->isGlobal_) {
            size_t curPathLen = strlen(pathInfo[ii].pathName);
            tableNames[curIdx] = new (std::nothrow) char[curPathLen + 1];
            BailIfNullMsg(tableNames[curIdx],
                          StatusNoMem,
                          ModuleName,
                          "Failed listGlobalTables: %s",
                          strGetFromStatus(status));

            verifyOk(strStrlcpy(tableNames[curIdx],
                                pathInfo[ii].pathName,
                                curPathLen + 1));
            curIdx++;
        }
    }
    retTableNames = tableNames;
    retNumTables = curIdx;

CommonExit:
    if (pathInfo) {
        memFree(pathInfo);
    }
    if (idRecord) {
        memFree(idRecord);
    }

    if (status != StatusOk && tableNames != NULL) {
        for (size_t ii = 0; ii < curIdx; ii++) {
            delete[] tableNames[ii];
        }
        assert(retTableNames == NULL);
        delete[] tableNames;
    }

    return status;
}

Status
TableNsMgr::listSessionTables(const char *pattern,
                              char **&retTableNames,
                              size_t &retNumTables,
                              const XcalarApiUdfContainer *sessionContainer)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    LibNsTypes::PathInfoOut *pathInfo = NULL;
    char regexBuf[LibNsTypes::MaxPathNameLen];
    size_t curIdx = 0;

    retTableNames = NULL;
    retNumTables = 0;

    status =
        getFQN(regexBuf, LibNsTypes::MaxPathNameLen, sessionContainer, pattern);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed listSessionTables userName '%s', session '%s', Id "
                    "%lX, retina '%s': %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    strGetFromStatus(status));

    size_t numPaths;
    numPaths = libNs->getPathInfo(regexBuf, &pathInfo);
    if (numPaths == 0) {
        goto CommonExit;
    }

    char **tableNames;
    tableNames = new (std::nothrow) char *[numPaths];
    BailIfNullMsg(tableNames,
                  StatusNoMem,
                  ModuleName,
                  "Failed listSessionTables userName '%s', session '%s', Id "
                  "%lX, retina '%s': %s",
                  sessionContainer->userId.userIdName,
                  sessionContainer->sessionInfo.sessionName,
                  sessionContainer->sessionInfo.sessionId,
                  sessionContainer->retinaName,
                  strGetFromStatus(status));

    for (size_t ii = 0; ii < numPaths; ++ii) {
        status =
            getNameFromFqTableName(pathInfo[ii].pathName, tableNames[curIdx]);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed listSessionTables userName '%s', session '%s', "
                        "Id %lX, retina '%s': %s",
                        sessionContainer->userId.userIdName,
                        sessionContainer->sessionInfo.sessionName,
                        sessionContainer->sessionInfo.sessionId,
                        sessionContainer->retinaName,
                        strGetFromStatus(status));
        curIdx++;
    }
    retTableNames = tableNames;
    retNumTables = curIdx;

CommonExit:
    if (pathInfo) {
        memFree(pathInfo);
    }
    if (status != StatusOk && tableNames != NULL) {
        for (size_t ii = 0; ii < curIdx; ii++) {
            delete tableNames[ii];
        }
        assert(retTableNames == NULL);
        delete[] tableNames;
    }

    return status;
}

Status
TableNsMgr::getNameFromFqTableName(const char *fullyQualName,
                                   char *&retTableName)
{
    Status status;
    char *token, *strState;
    char *tableName = NULL;
    char tmpBuf[LibNsTypes::MaxPathNameLen];

    status = strStrlcpy(tmpBuf, fullyQualName, LibNsTypes::MaxPathNameLen);
    BailIfFailed(status);

    token = strtok_r(tmpBuf, "/", &strState);  // parse TableNamesNsPrefix
    token = strtok_r(NULL, "/", &strState);    // parse <UserName>
    token = strtok_r(NULL, "/", &strState);    // parse <SessionId>

    size_t curPathLen;
    curPathLen = strlen(token);
    tableName = new (std::nothrow) char[curPathLen + 1];
    BailIfNull(tableName);

    verifyOk(strStrlcpy(tableName, token, curPathLen + 1));

    retTableName = tableName;
    tableName = NULL;

CommonExit:
    delete[] tableName;
    return (status);
}

Status
TableNsMgr::publishOrUnpublishTable(
    const XcalarApiUdfContainer *sessionContainer,
    const char *tableName,
    bool isGlobal)
{
    Status status = StatusOk;
    IdHandle idHandle;
    bool validHandle = false;

    status = openHandleToNsWithName(sessionContainer,
                                    tableName,
                                    LibNsTypes::NsOpenFlags::WriterExcl,
                                    &idHandle,
                                    OpenSleepInUsecs);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed openHandleToNsWithName userName '%s', session '%s', Id "
                "%lX, retina '%s', table '%s': %s",
                sessionContainer->userId.userIdName,
                sessionContainer->sessionInfo.sessionName,
                sessionContainer->sessionInfo.sessionId,
                sessionContainer->retinaName,
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    validHandle = true;

    idHandle.isGlobal = isGlobal;

    idHandle = updateNsObject(&idHandle, status);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed openHandleToNsWithName userName '%s', session '%s', Id "
                "%lX, retina '%s', table '%s': %s",
                sessionContainer->userId.userIdName,
                sessionContainer->sessionInfo.sessionName,
                sessionContainer->sessionInfo.sessionId,
                sessionContainer->retinaName,
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (validHandle) {
        closeHandleToNs(&idHandle);
    }

    return status;
}

Status
TableNsMgr::addToNs(const XcalarApiUdfContainer *sessionContainer,
                    TableId tableId,
                    const char *tableName,
                    DagTypes::DagId dagId,
                    DagTypes::NodeId dagNodeId)
{
    bool addedToNs = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    Status status;

    status = getFQN(fullyQualName,
                    sizeof(fullyQualName),
                    sessionContainer,
                    tableName);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::addToNs userName '%s', session '%s', "
                    "Id %lX, retina '%s', table '%s', Id %ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableName,
                    tableId,
                    strGetFromStatus(status));

    status = addToNsInternal(sessionContainer,
                             tableId,
                             tableName,
                             fullyQualName,
                             false);
    BailIfFailed(status);
    addedToNs = true;

    status = TableMgr::addTableObj(tableId,
                                   fullyQualName,
                                   dagId,
                                   dagNodeId,
                                   sessionContainer);
    BailIfFailed(status);

CommonExit:
    if (addedToNs &&
        (status != StatusOk && status != StatusTableAlreadyExists)) {
        removeFromNs(sessionContainer, tableId, tableName);
    }
    return status;
}

Status
TableNsMgr::addToNsInternal(const XcalarApiUdfContainer *sessionContainer,
                            TableId tableId,
                            const char *tableName,
                            const char *fullyQualName,
                            bool tableNameOnly)
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    LibNsTypes::NsId nsName = LibNsTypes::NsInvalidId;
    LibNsTypes::NsHandle nsHandle;
    char fullyQualId[LibNsTypes::MaxPathNameLen];
    TableIdRecord idRecord(tableId,
                           sessionContainer,
                           StartVersion,
                           StartVersion + 1,
                           false,
                           false);
    TableNameRecord nameRecord(tableId);

    assert(isTableIdValid(tableId));

    xSyslog(ModuleName,
            XlogInfo,
            "TableNsMgr::addToNsInternal userName '%s', session '%s', Id %lX, "
            "retina '%s', table '%s', Id %ld",
            sessionContainer->userId.userIdName,
            sessionContainer->sessionInfo.sessionName,
            sessionContainer->sessionInfo.sessionId,
            sessionContainer->retinaName,
            tableName,
            tableId);

    // Publish table name into the NS
    nsId = libNs->publish(fullyQualName, &nameRecord, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusInvTableName;
        } else if (status == StatusPendingRemoval || status == StatusExist) {
            status = StatusTableAlreadyExists;
        }
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed TableNsMgr::addToNsInternal userName '%s', "
                        "session '%s', Id %lX, retina '%s', table '%s', Id "
                        "%ld: %s",
                        sessionContainer->userId.userIdName,
                        sessionContainer->sessionInfo.sessionName,
                        sessionContainer->sessionInfo.sessionId,
                        sessionContainer->retinaName,
                        tableName,
                        tableId,
                        strGetFromStatus(status));
    }

    // Publish table ID into the NS
    if (!tableNameOnly) {
        status = getFQN(fullyQualId, sizeof(fullyQualId), tableId);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed TableNsMgr::addToNsInternal userName '%s', "
                        "session '%s', Id %lX, retina '%s', table '%s', Id "
                        "%ld: %s",
                        sessionContainer->userId.userIdName,
                        sessionContainer->sessionInfo.sessionName,
                        sessionContainer->sessionInfo.sessionId,
                        sessionContainer->retinaName,
                        tableName,
                        tableId,
                        strGetFromStatus(status));

        nsName = libNs->publish(fullyQualId, &idRecord, &status);
        if (status != StatusOk) {
            if (status == StatusNsInvalidObjName) {
                status = StatusInvTableName;
            } else if (status == StatusPendingRemoval ||
                       status == StatusExist) {
                status = StatusTableAlreadyExists;
            }
            BailIfFailedMsg(ModuleName,
                            status,
                            "Failed TableNsMgr::addToNsInternal userName '%s', "
                            "session '%s' Id %lX, retina '%s', table '%s', Id "
                            "%ld: %s",
                            sessionContainer->userId.userIdName,
                            sessionContainer->sessionInfo.sessionName,
                            sessionContainer->sessionInfo.sessionId,
                            sessionContainer->retinaName,
                            tableName,
                            tableId,
                            strGetFromStatus(status));
        }
    }

CommonExit:
    if (status != StatusOk && status != StatusTableAlreadyExists) {
        removeFromNs(sessionContainer, tableId, tableName);
    }
    return status;
}

void
TableNsMgr::removeFromNs(const XcalarApiUdfContainer *sessionContainer,
                         TableId tableId,
                         const char *tableName)
{
    removeFromNsInternal(sessionContainer, tableId, tableName, false);
}

void
TableNsMgr::removeFromNsInternal(const XcalarApiUdfContainer *sessionContainer,
                                 TableId tableId,
                                 const char *tableName,
                                 bool tableNameOnly)
{
    Status status;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    char fullyQualId[LibNsTypes::MaxPathNameLen];

    assert(isTableIdValid(tableId));

    xSyslog(ModuleName,
            XlogInfo,
            "TableNsMgr::removeFromNsInternal userName '%s', session '%s', Id "
            "%lX, retina '%s', table '%s', Id %ld, '%s'",
            sessionContainer->userId.userIdName,
            sessionContainer->sessionInfo.sessionName,
            sessionContainer->sessionInfo.sessionId,
            sessionContainer->retinaName,
            tableName,
            tableId,
            tableNameOnly ? "NameOnly" : "NameAndId");

    // Remove table name from the NS. Tolerate errors and fall through, since
    // this is best effort anyway.
    status = getFQN(fullyQualName,
                    sizeof(fullyQualName),
                    sessionContainer,
                    tableName);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed TableNsMgr::removeFromNsInternal userName '%s', "
                "session '%s', Id %lX, retina '%s', table '%s', Id %ld: %s",
                sessionContainer->userId.userIdName,
                sessionContainer->sessionInfo.sessionName,
                sessionContainer->sessionInfo.sessionId,
                sessionContainer->retinaName,
                tableName,
                tableId,
                strGetFromStatus(status));
    } else {
        status = libNs->remove(fullyQualName, NULL);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed TableNsMgr::removeFromNsInternal userName '%s', "
                    "session '%s', Id %lX, retina '%s', table '%s', Id %ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableName,
                    tableId,
                    strGetFromStatus(status));
        }
    }

    // Remove table ID from the NS. Tolerate errors and fall through, since
    // this is best effort anyway.
    if (!tableNameOnly) {
        status = getFQN(fullyQualId, sizeof(fullyQualId), tableId);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed TableNsMgr::removeFromNsInternal userName '%s', "
                    "session '%s', Id %lX, retina '%s', table '%s', Id %ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableName,
                    tableId,
                    strGetFromStatus(status));
        } else {
            bool objDeleted = false;
            status = libNs->remove(fullyQualId, &objDeleted);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed TableNsMgr::removeFromNsInternal userName "
                        "'%s', session '%s', Id %lX, retina '%s', table '%s', "
                        "Id %ld: %s",
                        sessionContainer->userId.userIdName,
                        sessionContainer->sessionInfo.sessionName,
                        sessionContainer->sessionInfo.sessionId,
                        sessionContainer->retinaName,
                        tableName,
                        tableId,
                        strGetFromStatus(status));
            }
            if (objDeleted) {
                TableMgr::removeTableObj(tableId);
            }
        }
    }
}

Status
TableNsMgr::renameNs(const XcalarApiUdfContainer *sessionContainer,
                     TableId tableId,
                     const char *tableNameOld,
                     const char *tableNameNew)
{
    Status status;
    bool openedNs = false;
    IdHandle handle;
    char fullyQualName[LibNsTypes::MaxPathNameLen];

    assert(isTableIdValid(tableId));

    xSyslog(ModuleName,
            XlogInfo,
            "TableNsMgr::renameNs userName '%s', session '%s', Id %lX, retina "
            "'%s', table old '%s' new '%s', Id %ld",
            sessionContainer->userId.userIdName,
            sessionContainer->sessionInfo.sessionName,
            sessionContainer->sessionInfo.sessionId,
            sessionContainer->retinaName,
            tableNameOld,
            tableNameNew,
            tableId);

    status = getFQN(fullyQualName,
                    sizeof(fullyQualName),
                    sessionContainer,
                    tableNameNew);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::renameNs userName '%s', session '%s', "
                    "Id %lX, retina '%s', table old '%s' new '%s', Id %ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableNameOld,
                    tableNameNew,
                    tableId,
                    strGetFromStatus(status));

    status = openHandleToNs(sessionContainer,
                            tableId,
                            LibNsTypes::ReaderShared,
                            &handle,
                            OpenNoSleep);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::renameNs userName '%s', session '%s', "
                    "Id %lX, retina '%s', tableOld '%s', tableNew '%s', Id "
                    "%ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableNameOld,
                    tableNameNew,
                    tableId,
                    strGetFromStatus(status));
    openedNs = true;

    // Add the new table name to NS
    status = addToNsInternal(sessionContainer,
                             tableId,
                             tableNameNew,
                             fullyQualName,
                             true);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::renameNs userName '%s', session '%s', "
                    "Id %lX, retina '%s', tableOld '%s', tableNew '%s', Id "
                    "%ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableNameOld,
                    tableNameNew,
                    tableId,
                    strGetFromStatus(status));

    // Remove the old table name from NS. Note that in certain cases, old
    // table name may get leaked, but that's ok, since adding new table name
    // to NS is considered Txn commit.
    removeFromNsInternal(sessionContainer, tableId, tableNameOld, true);

CommonExit:
    if (openedNs) {
        closeHandleToNs(&handle);
    }
    return status;
}

Status
TableNsMgr::openHandleToNsWithName(
    const XcalarApiUdfContainer *sessionContainer,
    const char *tableName,
    LibNsTypes::NsOpenFlags openFlag,
    IdHandle *retHandle,
    uint64_t sleepInUsecs)
{
    Status status;
    TableId tableId = InvalidTableId;

    status = getTableIdFromName(sessionContainer, tableName, &tableId);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::openHandleToNsWithName userName "
                    "'%s', session '%s', Id %lX, retina '%s', table '%s': %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableName,
                    strGetFromStatus(status));

    status = openHandleToNs(sessionContainer,
                            tableId,
                            openFlag,
                            retHandle,
                            sleepInUsecs);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::openHandleToNsWithName userName '%s', "
                    "session '%s', Id %lX, retina '%s', table '%s': %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableName,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
TableNsMgr::openHandleToNs(const XcalarApiUdfContainer *sessionContainer,
                           TableId tableId,
                           LibNsTypes::NsOpenFlags openFlag,
                           IdHandle *retHandle,
                           uint64_t sleepInUsecs)
{
    Status status;
    LibNsTypes::NsHandle nsHandle;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    NsObject *obj = NULL;
    TableIdRecord *record = NULL;
    LibNs *libNs = LibNs::get();
    uint64_t retryCount = 0;
    Semaphore tSem(0);
    Stopwatch stopwatch;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;

    assert(isTableIdValid(tableId));

    status = getFQN(fullyQualName, sizeof(fullyQualName), tableId);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::openHandleToNs userName '%s', session "
                    "'%s', Id %lX, retina '%s', table Id %ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableId,
                    strGetFromStatus(status));

    retHandle->tableId = tableId;
    UserDefinedFunction::copyContainers(&retHandle->sessionContainer,
                                        sessionContainer);
    do {
        nsHandle = libNs->open(fullyQualName, openFlag, &obj, &status);
        if (status != StatusOk && status != StatusAccess) {
            if (status == StatusNsNotFound) {
                status = StatusTableNameNotFound;
            }
            BailIfFailedMsg(ModuleName,
                            status,
                            "Failed TableNsMgr::openHandleToNs userName '%s', "
                            "session '%s', Id %lX, retina '%s', table Id %ld: "
                            "%s",
                            sessionContainer->userId.userIdName,
                            sessionContainer->sessionInfo.sessionName,
                            sessionContainer->sessionInfo.sessionId,
                            sessionContainer->retinaName,
                            tableId,
                            strGetFromStatus(status));
        }
        if (status == StatusAccess) {
            tSem.timedWait(sleepInUsecs);
            retryCount++;
        }
    } while (status == StatusAccess);

    record = static_cast<TableIdRecord *>(obj);
    retHandle->nsHandle = nsHandle;
    retHandle->consistentVersion = record->consistentVersion_;
    retHandle->nextVersion = record->nextVersion_;
    retHandle->isGlobal = record->isGlobal_;
    retHandle->mergePending = record->mergePending_;

    if (!retHandle->mergePending) {
        // Merge is not pending.
        goto CommonExit;
    }

    // Kick IMD merge on this table
    status =
        Operators::get()->mergePostCommit(retHandle,
                                          Operators::PostCommitType::Deferred);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::openHandleToNs userName '%s', session "
                    "'%s', Id %lX, retina '%s', table Id %ld: %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableId,
                    strGetFromStatus(status));

CommonExit:
    if (obj != NULL) {
        memFree(obj);
        obj = NULL;
    }

    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);

    xSyslog(ModuleName,
            XlogInfo,
            "TableNsMgr::openHandleToNs userName '%s', session '%s', Id %lX, "
            "retina '%s', table Id %ld: %s: finished in %lu:%02lu:%02lu.%03lu",
            sessionContainer->userId.userIdName,
            sessionContainer->sessionInfo.sessionName,
            sessionContainer->sessionInfo.sessionId,
            sessionContainer->retinaName,
            tableId,
            strGetFromStatus(status),
            hours,
            minutesLeftOver,
            secondsLeftOver,
            millisecondsLeftOver);
    return status;
}

void
TableNsMgr::closeHandleToNs(IdHandle *handleIn)
{
    Status status;
    LibNs *libNs = LibNs::get();
    bool objDeleted = false;

    status = libNs->close(handleIn->nsHandle, &objDeleted);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::closeHandleToNs userName '%s', session "
                    "'%s', Id %lX, retina '%s', tableId %ld: %s",
                    handleIn->sessionContainer.userId.userIdName,
                    handleIn->sessionContainer.sessionInfo.sessionName,
                    handleIn->sessionContainer.sessionInfo.sessionId,
                    handleIn->sessionContainer.retinaName,
                    handleIn->tableId,
                    strGetFromStatus(status));

    if (objDeleted) {
        TableMgr::removeTableObj(handleIn->tableId);
    }

CommonExit:
    return;
}

Status
TableNsMgr::getFQN(char *retFqn,
                   size_t fqnLen,
                   const XcalarApiUdfContainer *sessionContainer,
                   const char *tableName)
{
    Status status = StatusOk;
    int ret = 0;

    if (UserDefinedFunction::containerWithWbScope(sessionContainer)) {
        ret = snprintf(retFqn,
                       fqnLen,
                       "%s/%lX/%s",
                       TableNamesNsPrefix,
                       sessionContainer->sessionInfo.sessionId,
                       tableName);
    } else {
        assert(UserDefinedFunction::containerForDataflows(sessionContainer));
        ret = snprintf(retFqn,
                       fqnLen,
                       "%s/%s/%s/%s",
                       TableNamesNsPrefix,
                       LegacyRetina,
                       sessionContainer->retinaName,
                       tableName);
    }

    if (ret < 0 || ret >= (int) fqnLen) {
        status = StatusNameTooLong;
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed TableNsMgr::getFQN userName '%s', session "
                        "'%s', Id %lX, retina '%s', tableName '%s': %s",
                        sessionContainer->userId.userIdName,
                        sessionContainer->sessionInfo.sessionName,
                        sessionContainer->sessionInfo.sessionId,
                        sessionContainer->retinaName,
                        tableName,
                        strGetFromStatus(status));
    }

    xSyslog(ModuleName,
            XlogDebug,
            "TableNsMgr::getFQN userName '%s', session "
            "'%s', Id %lX, retina '%s', tableName '%s', '%s'",
            sessionContainer->userId.userIdName,
            sessionContainer->sessionInfo.sessionName,
            sessionContainer->sessionInfo.sessionId,
            sessionContainer->retinaName,
            tableName,
            retFqn);

CommonExit:
    return status;
}

Status
TableNsMgr::getFQN(char *retFqn, size_t fqnLen, TableId tableId) const
{
    Status status = StatusOk;
    int ret = snprintf(retFqn, fqnLen, "%s/%ld", TableIdsNsPrefix, tableId);

    if (ret < 0 || ret >= (int) fqnLen) {
        status = StatusNameTooLong;
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed TableNsMgr::getFQN tableId '%ld': %s",
                        tableId,
                        strGetFromStatus(status));
    }

    xSyslog(ModuleName,
            XlogDebug,
            "TableNsMgr::getFQN tableId '%ld' '%s'",
            tableId,
            retFqn);

CommonExit:
    return status;
}

bool
TableNsMgr::isGlobal(const char *fullyQualName, Status &retStatus)
{
    Status status;
    TableMgr *tmgr = TableMgr::get();
    bool global = false;
    TableIdRecord *idRecord = NULL;

    TableId tableId = tmgr->getTableIdFromName(fullyQualName);
    if (!isTableIdValid(tableId)) {
        status = StatusTableNameNotFound;
        goto CommonExit;
    }

    status = getIdRecord(tableId, &idRecord);
    BailIfFailed(status);

    global = idRecord->isGlobal_;

CommonExit:
    if (idRecord) {
        memFree(idRecord);
    }
    retStatus = status;
    return global;
}

Status
TableNsMgr::getTableIdFromName(const XcalarApiUdfContainer *sessionContainer,
                               const char *tableName,
                               TableId *retTableId) const
{
    Status status;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    TableMgr *tmgr = TableMgr::get();

    *retTableId = InvalidTableId;

    status = getFQN(fullyQualName,
                    sizeof(fullyQualName),
                    sessionContainer,
                    tableName);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed TableNsMgr::getTableIdFromName userName '%s', "
                    "session '%s', Id %lX, retina '%s', table '%s': %s",
                    sessionContainer->userId.userIdName,
                    sessionContainer->sessionInfo.sessionName,
                    sessionContainer->sessionInfo.sessionId,
                    sessionContainer->retinaName,
                    tableName,
                    strGetFromStatus(status));

    *retTableId = tmgr->getTableIdFromName(fullyQualName);
    if (!isTableIdValid(*retTableId)) {
        status = StatusTableNameNotFound;
        goto CommonExit;
    }

CommonExit:
    return status;
}

TableNsMgr::IdHandle
TableNsMgr::updateNsObject(IdHandle *handle, Status &retStatus)
{
    Status status;
    IdHandle retHandle;
    LibNsTypes::NsHandle updateHandle;
    LibNs *libNs = LibNs::get();
    TableIdRecord *record =
        new (std::nothrow) TableIdRecord(handle->tableId,
                                         &handle->sessionContainer,
                                         handle->consistentVersion,
                                         handle->nextVersion,
                                         handle->isGlobal,
                                         handle->mergePending);
    BailIfNullMsg(record,
                  StatusNoMem,
                  ModuleName,
                  "Failed updateNsObject userName '%s', session '%s', Id %lX, "
                  "retina '%s', table %ld: %s",
                  handle->sessionContainer.userId.userIdName,
                  handle->sessionContainer.sessionInfo.sessionName,
                  handle->sessionContainer.sessionInfo.sessionId,
                  handle->sessionContainer.retinaName,
                  handle->tableId,
                  strGetFromStatus(status));

    updateHandle = libNs->updateNsObject(handle->nsHandle,
                                         static_cast<NsObject *>(record),
                                         &status);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed updateNsObject userName '%s', session '%s', Id "
                    "%lX, retina '%s', table %ld: %s",
                    handle->sessionContainer.userId.userIdName,
                    handle->sessionContainer.sessionInfo.sessionName,
                    handle->sessionContainer.sessionInfo.sessionId,
                    handle->sessionContainer.retinaName,
                    handle->tableId,
                    strGetFromStatus(status));

    retHandle.tableId = handle->tableId;
    retHandle.consistentVersion = handle->consistentVersion;
    retHandle.nextVersion = handle->nextVersion;
    retHandle.isGlobal = handle->isGlobal;
    retHandle.mergePending = handle->mergePending;
    retHandle.nsHandle = updateHandle;
    UserDefinedFunction::copyContainers(&retHandle.sessionContainer,
                                        &handle->sessionContainer);

CommonExit:
    if (record) {
        delete record;
        record = NULL;
    }
    retStatus = status;
    return retHandle;
}

Status
TableNsMgr::getIdRecord(const char *fullyQualTableId,
                        TableIdRecord **retIdRecord) const
{
    Status status = StatusOk;
    NsObject *obj = NULL;
    LibNs *libNs = LibNs::get();

    obj = libNs->getNsObject(fullyQualTableId);
    if (!obj) {
        status = StatusTableNameNotFound;
        goto CommonExit;
    }

    *retIdRecord = static_cast<TableIdRecord *>(obj);
CommonExit:
    if (status != StatusOk) {
        memFree(obj);
    }
    return status;
}

Status
TableNsMgr::getIdRecord(const TableId tableId,
                        TableIdRecord **retIdRecord) const
{
    char fullyQualifiedId[LibNsTypes::MaxPathNameLen];
    Status status = StatusOk;

    status = getFQN(fullyQualifiedId, LibNsTypes::MaxPathNameLen, tableId);
    if (!status.ok()) {
        return status;
    }

    return getIdRecord(fullyQualifiedId, retIdRecord);
}
