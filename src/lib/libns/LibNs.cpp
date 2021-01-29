// Copyright 2017 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <string.h>

#include "StrlFunc.h"
#include "ns/LibNs.h"
#include "util/MemTrack.h"
#include "msg/Xid.h"
#include "msg/Message.h"
#include "sys/XLog.h"
#include "strings/String.h"

static constexpr char const *moduleName = "libns";
LibNs *LibNs::instance = NULL;

// Uncomment this out to get verbose LibNs logging.  This is
// useful when developing new features and/or fixing bugs.
// #define LIBNS_DEBUG

Status
LibNs::init()
{
    Status status = StatusOk;
    StatsLib *statsLib = StatsLib::get();

    assert(instance == NULL);
    instance = new (std::nothrow) LibNs;
    if (instance == NULL) {
        return StatusNoMem;
    }

    // Initialize stats

    status = statsLib->initNewStatGroup("namespace",
                                        &instance->namespaceStatGroupId_,
                                        StatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.published);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "objects.published",
                                         instance->stats_.published,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.removed);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "objects.removed",
                                         instance->stats_.removed,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.updated);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "objects.updated",
                                         instance->stats_.updated,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.lookedup);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "objects.lookedup",
                                         instance->stats_.lookedup,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.opened);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "objects.opened",
                                         instance->stats_.opened,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.closed);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "objects.closed",
                                         instance->stats_.closed,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.streamedCnt);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "stream.count",
                                         instance->stats_.streamedCnt,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats_.streamedBytes);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->namespaceStatGroupId_,
                                         "stream.bytes",
                                         instance->stats_.streamedBytes,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:

    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }

    return status;
}

void
LibNs::destroy()
{
    if (instance == NULL) {
        return;
    }
#ifdef DEBUG
    {
        PathNameToNsIdEntry *pathNameToNsId;
        NsDlmObject *dlmObj;
        NsIdToNsDlmObjectEntry *nsIdToNsDlmObj;

        // Print out all the leaked NS objects
        for (PathNameToNsIdHashTable::iterator it =
                 pathNameToNsIdHashTable_.begin();
             (pathNameToNsId = it.get()) != NULL;
             it.next()) {
            nsIdToNsDlmObj =
                nsIdToNsDlmObjectHashTable_.find(pathNameToNsId->nsId);
            assert(nsIdToNsDlmObj != NULL);

            dlmObj = (NsDlmObject *) nsIdToNsDlmObj->nsDlmObject;

            if (dlmObj->openedReaderWriterExcl) {
                xSyslog(moduleName,
                        XlogDebug,
                        "NS Leak! Id: %lu, name: %s, refCount: %u, "
                        "Reader Writer Exclusive, readers %u writers %u",
                        dlmObj->nsId,
                        dlmObj->pathName,
                        refRead(&dlmObj->refCount),
                        dlmObj->numReaderOpens,
                        dlmObj->numWriterOpens);
            } else if (dlmObj->openedReaderShared) {
                xSyslog(moduleName,
                        XlogDebug,
                        "NS Leak! Id: %lu, name: %s, refCount: %u, "
                        "Readers Shared, readers %u writers %u",
                        dlmObj->nsId,
                        dlmObj->pathName,
                        refRead(&dlmObj->refCount),
                        dlmObj->numReaderOpens,
                        dlmObj->numWriterOpens);
            } else if (dlmObj->openedReaderSharedWriterExcl) {
                xSyslog(moduleName,
                        XlogDebug,
                        "NS Leak! Id: %lu, name: %s, refCount: %u, "
                        "Readers Shared Writer Exclusive, readers %u "
                        "writers %u",
                        dlmObj->nsId,
                        dlmObj->pathName,
                        refRead(&dlmObj->refCount),
                        dlmObj->numReaderOpens,
                        dlmObj->numWriterOpens);
            } else {
                // Objects which have no opens but weren't removed
                xSyslog(moduleName,
                        XlogDebug,
                        "NS Leak! Id: %lu, name: %s, refCount: %u",
                        dlmObj->nsId,
                        dlmObj->pathName,
                        refRead(&dlmObj->refCount));
            }
        }
    }
#endif  // DEBUG
    delete instance;
    instance = NULL;
}

LibNs *
LibNs::get()
{
    return instance;
}

//
// Valid path names consist of:
//      <path name portion>[<path name portion]...
// Below constrains hold true:
// - Must start with "/"
// - Character following "/" must not be followed by "/"
// - Cannot end with trailing "/"
//
bool
LibNs::isValidPathName(const char *pathName, NodeId *retNodeId)
{
    *retNodeId = MaxNodes;

    if (pathName == NULL) {
        return false;
    }

    size_t len = strlen(pathName);
    if (len < 2 || len >= LibNsTypes::MaxPathNameLen) {
        return false;
    }

    // Must start with a "/"
    if (*pathName != '/') {
        return false;
    }

    // Cannot end with a trailing slash
    if (pathName[len - 1] == '/') {
        return false;
    }

    char curPath[LibNsTypes::MaxPathNameLen];
    snprintf(curPath, LibNsTypes::MaxPathNameLen, "%s", pathName);
    char *saveptr = NULL;
    char *cur = strtok_r((char *) curPath, "/", &saveptr);
    while (cur != NULL) {
        size_t prevCharOff = (uintptr_t) cur - (uintptr_t) curPath - 1;
        char prevChar = pathName[prevCharOff];
        if (prevChar == '/') {
            if (prevCharOff != 0 && pathName[prevCharOff - 1] == '/') {
                // Character following "/" must be not be "/"
                return false;
            }
        }
        cur = strtok_r(NULL, "/", &saveptr);
    }

    NodeId tmpNodeId;
    static constexpr const ssize_t NumMatches = 2;
    if (sscanf(pathName, "/nodeId-%u%s", &tmpNodeId, curPath) == NumMatches) {
        if (tmpNodeId >= Config::get()->getActiveNodes()) {
            return false;
        } else {
            *retNodeId = tmpNodeId;
        }
    } else {
        *retNodeId = hashStringFast(pathName) % Config::get()->getActiveNodes();
    }

    return true;
}

NodeId
LibNs::getNodeId(const char *pathName, Status *retStatus)
{
    NodeId objNodeId = MaxNodes;
    Status status = StatusOk;
    if (!isValidPathName(pathName, &objNodeId)) {
        status = StatusNsInvalidObjName;
        goto CommonExit;
    }

CommonExit:
    *retStatus = status;
    return objNodeId;
}

// return true if fqn has the libNs node-id prefix
bool
LibNs::fqnHasNodeIdPrefix(char *fqn)
{
    return strncmp(fqn, NodeIdPrefix, strlen(NodeIdPrefix)) == 0;
}

// Note that the NodeId prefixed to the FQN affects the location of the LibNs
// object. However for the purpose of namespace scoping, the nodeId is part of
// the FQN.
// For instance you can have,
// fqn = /path/foo and
// fqn = /nodeId-<NodeId>/path/foo.
// Both are equally valid FQNs and can coexist in the namespace.
Status
LibNs::setFQNForNodeId(NodeId nodeId, char *fqn, size_t fqnSize)
{
    char fqnTmp[LibNsTypes::MaxPathNameLen];
    Status status;

    if (fqnHasNodeIdPrefix(fqn) == false) {
        if (snprintf(fqnTmp, LibNsTypes::MaxPathNameLen, "%s", fqn) >=
            (ssize_t) LibNsTypes::MaxPathNameLen) {
            return StatusNameTooLong;
        }

        if (snprintf(fqn, fqnSize, "%s%u%s", NodeIdPrefix, nodeId, fqnTmp) >=
            (ssize_t) fqnSize) {
            return StatusNameTooLong;
        } else {
            return StatusOk;
        }
    } else {
        return StatusOk;  // the nodeId prefix already present
    }
}

// Strip away the nodeId prefix, if present in 'fqn'
Status
LibNs::setFQNStripNodeId(char *fqn, size_t fqnSize)
{
    if (fqnHasNodeIdPrefix(fqn) == true) {
        char fqnTmp[LibNsTypes::MaxPathNameLen];
        char *strcursor = NULL;

        if (snprintf(fqnTmp, LibNsTypes::MaxPathNameLen, "%s", fqn) >=
            (ssize_t) LibNsTypes::MaxPathNameLen) {
            return StatusNameTooLong;
        }
        strcursor = &fqnTmp[1];  // position cursor after the first '/'
        strcursor = strchr(strcursor, '/');  // position at the second '/'
        if (strcursor == NULL) {
            return StatusOk;  // no '/': return what was passed in
        } else if (snprintf(fqn, fqnSize, "%s", strcursor) >=
                   (ssize_t) fqnSize) {
            return StatusNameTooLong;
        } else {
            return StatusOk;
        }
    } else {
        return StatusOk;
    }
}

void
LibNs::setHandleInvalid(LibNsTypes::NsHandle *nsHandle)
{
    nsHandle->nsId = LibNsTypes::NsInvalidId;
}

void
LibNs::refInit(Atomic32 *refCount)
{
    atomicWrite32(refCount, 1);
#ifdef LIBNS_DEBUG
    NsDlmObject *dlmObj = ContainerOf(refCount, NsDlmObject, refCount);
    xSyslog(moduleName,
            XlogDebug,
            "refInit %d for dlm object %s",
            1,
            dlmObj->pathName);
#endif
}

void
LibNs::refInc(Atomic32 *refCount)
{
    int32_t newCount;
    newCount = atomicInc32(refCount);
    assert(newCount > 1);
#ifdef LIBNS_DEBUG
    NsDlmObject *dlmObj = ContainerOf(refCount, NsDlmObject, refCount);
    xSyslog(moduleName,
            XlogDebug,
            "refInc %d for dlm object %s",
            newCount,
            dlmObj->pathName);
#endif
}

void
LibNs::refDec(Atomic32 *refCount)
{
    int32_t newCount;
    NsDlmObject *dlmObj = ContainerOf(refCount, NsDlmObject, refCount);

    newCount = atomicDec32(refCount);
    assert(newCount >= 0);
#ifdef LIBNS_DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "refDec %d for dlm object %s",
            newCount,
            dlmObj->pathName);
#endif

    if (newCount == 0) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogDebug,
                "Freeing memory for dlm object %s",
                dlmObj->pathName);
#endif
        statsIncRemoved();

        // Remove the dlm object from the name-to-nsid and nsid-to-dlmobj
        // hash tables.  And then free the memory.
        deleteDlmObject(dlmObj);
    }
}

uint32_t
LibNs::refRead(const Atomic32 *refCount)
{
    return atomicRead32(refCount);
}

LibNsTypes::NsId
LibNs::publish(const char *pathName, Status *status)
{
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    NsObject *nsObj = NULL;
    NodeId objNodeId = MaxNodes;

    assert(status != NULL);
    *status = StatusOk;

    if (!isValidPathName(pathName, &objNodeId)) {
        *status = StatusNsInvalidObjName;
        goto CommonExit;
    }

    // Caller didn't provide an object to associate with the pathname so
    // create a minimal one for bookkeeping purposes.

    nsObj = (NsObject *) memAllocExt(sizeof(*nsObj), moduleName);
    if (nsObj == NULL) {
        *status = StatusNoMem;
        goto CommonExit;
    }

    new (nsObj) NsObject(sizeof(*nsObj));

    nsId = publishInternal(pathName,
                           nsObj,
                           status,
                           LibNsTypes::DoNotAutoRemove,
                           objNodeId);

#ifdef LIBNS_DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "publish (object not provided) for '%s' returned ID %lu: %s",
            pathName,
            nsId,
            strGetFromStatus(*status));
#endif

CommonExit:
    if (nsObj != NULL) {
        // Free the mimimal object that we created.  If the publish was
        // successful the object is now copied over to the dlm node.
        memFree(nsObj);
        nsObj = NULL;
    }

    return nsId;
}

LibNsTypes::NsId
LibNs::publish(const char *pathName, NsObject *nsObj, Status *status)
{
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    NodeId objNodeId = MaxNodes;

    assert(nsObj != NULL);
    assert(status != NULL);

    if (!isValidPathName(pathName, &objNodeId)) {
        *status = StatusNsInvalidObjName;
        goto CommonExit;
    }

    nsId = publishInternal(pathName,
                           nsObj,
                           status,
                           LibNsTypes::DoNotAutoRemove,
                           objNodeId);

#ifdef LIBNS_DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "publish of object for '%s' returned ID %lu: %s",
            pathName,
            nsId,
            strGetFromStatus(*status));
#endif

CommonExit:

    return nsId;
}

LibNsTypes::NsId
LibNs::publishWithAutoRemove(const char *pathName,
                             NsObject *nsObj,
                             Status *status)
{
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    NodeId objNodeId = MaxNodes;

    assert(nsObj != NULL);
    assert(status != NULL);

    if (!isValidPathName(pathName, &objNodeId)) {
        *status = StatusNsInvalidObjName;
        goto CommonExit;
    }

    nsId = publishInternal(pathName,
                           nsObj,
                           status,
                           LibNsTypes::DoAutoRemove,
                           objNodeId);

#ifdef LIBNS_DEBUG
    xSyslog(moduleName,
            XlogDebug,
            "publish of object for '%s' returned ID %lu: %s",
            pathName,
            nsId,
            strGetFromStatus(*status));
#endif

CommonExit:

    return nsId;
}

LibNsTypes::NsId
LibNs::publishInternal(const char *pathName,
                       NsObject *nsObj,
                       Status *status,
                       LibNsTypes::NsAutoRemove autoRemove,
                       NodeId objNodeId)
{
    Status myStatus;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsPublish;
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    NsPublishMsg *publishMsg = NULL;
    size_t objSize = nsObj->getSize();
    NodeId dlmNode = objNodeId;

    // The payload for the twoPc
    size_t msgSize = sizeof(NsPublishMsg) + objSize;
    publishMsg = (NsPublishMsg *) memAllocExt(msgSize, moduleName);
    if (publishMsg == NULL) {
        myStatus = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "publishInternal: failed to allocate %zu bytes for object",
                msgSize);
        goto CommonExit;
    }

    publishMsg->header.nsOp = NsPublish;
    verifyOk(strStrlcpy(publishMsg->header.pathName,
                        pathName,
                        LibNsTypes::MaxPathNameLen));
    publishMsg->header.viaId = false;
    publishMsg->autoRemove = autoRemove;
    // Copy the associated object into the message.
    memcpy(publishMsg->nsObject, nsObj, objSize);
    publishMsg->nsObjSize = objSize;

    myStatus = dispatchDlm(dlmNode, publishMsg, msgSize, &msgOutput);
    if (myStatus != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "publishInternal: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(myStatus));
        goto CommonExit;
    }

#ifdef LIBNS_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "publishInternal: %s succeeded on dlm node %u",
            pathName,
            dlmNode);
#endif

    myStatus = msgOutput.status;
    if (myStatus != StatusOk) {
#ifdef LIBNS_DEBUG
        // The caller can syslog if they desire.
        xSyslog(moduleName,
                XlogErr,
                "publishInternal: dlm node %u failed request: %s",
                dlmNode,
                strGetFromStatus(myStatus));
#endif
        goto CommonExit;
    }
    nsId = msgOutput.opOutput.nsId;

CommonExit:
    if (publishMsg != NULL) {
        memFree(publishMsg);
        publishMsg = NULL;
    }
    *status = myStatus;
    return nsId;
}

LibNsTypes::NsHandle
LibNs::open(const char *pathName,
            LibNsTypes::NsOpenFlags openFlags,
            Status *status)
{
    LibNsTypes::NsHandle nsHandle;
    NsOpenMsg nsOpenMsg;
    NodeId objNodeId = MaxNodes;

    if (!isValidPathName(pathName, &objNodeId)) {
        *status = StatusNsInvalidObjName;
        goto CommonExit;
    }

    nsOpenMsg.header.nsOp = NsOpen;
    verifyOk(strStrlcpy(nsOpenMsg.header.pathName,
                        pathName,
                        LibNsTypes::MaxPathNameLen));
    nsOpenMsg.header.viaId = false;
    nsOpenMsg.openFlags = openFlags;
    nsOpenMsg.includeObject = false;

    nsHandle = openCommon(nsOpenMsg, NULL, status, objNodeId);

CommonExit:

    return nsHandle;
}

LibNsTypes::NsHandle
LibNs::open(LibNsTypes::NsId nsId,
            LibNsTypes::NsOpenFlags openFlags,
            Status *status)
{
    LibNsTypes::NsHandle nsHandle;
    NsOpenMsg nsOpenMsg;
    NodeId objNodeId = XidMgr::get()->xidGetNodeId(nsId);

    nsOpenMsg.header.nsOp = NsOpen;
    nsOpenMsg.header.nsId = nsId;
    nsOpenMsg.header.viaId = true;
    nsOpenMsg.openFlags = openFlags;
    nsOpenMsg.includeObject = false;

    nsHandle = openCommon(nsOpenMsg, NULL, status, objNodeId);

    return nsHandle;
}

LibNsTypes::NsHandle
LibNs::open(const char *pathName,
            LibNsTypes::NsOpenFlags openFlags,
            NsObject **nsObject,
            Status *status)
{
    LibNsTypes::NsHandle nsHandle;
    NsOpenMsg nsOpenMsg;
    NodeId objNodeId = MaxNodes;

    setHandleInvalid(&nsHandle);

    if (!isValidPathName(pathName, &objNodeId)) {
        *status = StatusNsInvalidObjName;
        goto CommonExit;
    }

    nsOpenMsg.header.nsOp = NsOpen;
    verifyOk(strStrlcpy(nsOpenMsg.header.pathName,
                        pathName,
                        LibNsTypes::MaxPathNameLen));
    nsOpenMsg.header.viaId = false;
    nsOpenMsg.openFlags = openFlags;
    nsOpenMsg.includeObject = true;

    nsHandle = openCommon(nsOpenMsg, nsObject, status, objNodeId);

CommonExit:

    return nsHandle;
}

LibNsTypes::NsHandle
LibNs::open(LibNsTypes::NsId nsId,
            LibNsTypes::NsOpenFlags openFlags,
            NsObject **nsObject,
            Status *status)
{
    LibNsTypes::NsHandle nsHandle;
    NsOpenMsg nsOpenMsg;
    NodeId objNodeId = XidMgr::get()->xidGetNodeId(nsId);

    nsOpenMsg.header.nsOp = NsOpen;
    nsOpenMsg.header.nsId = nsId;
    nsOpenMsg.header.viaId = true;
    nsOpenMsg.openFlags = openFlags;
    nsOpenMsg.includeObject = true;

    nsHandle = openCommon(nsOpenMsg, nsObject, status, objNodeId);

    return nsHandle;
}

LibNsTypes::NsHandle
LibNs::openCommon(NsOpenMsg nsOpenMsg,
                  NsObject **nsObject,
                  Status *status,
                  NodeId objNodeId)
{
    MsgNsOutput msgOutput;
    msgOutput.ops = NsOpen;
    Status myStatus;
    LibNsTypes::NsHandle nsHandle;
    NodeId dlmNode = objNodeId;

    setHandleInvalid(&nsHandle);

    *status = StatusOk;
    if (nsObject != NULL) {
        *nsObject = NULL;
    }

    if (!LibNsTypes::isValidNsOpenFlags(nsOpenMsg.openFlags)) {
        myStatus = StatusInval;
        goto CommonExit;
    }

    myStatus = dispatchDlm(dlmNode, &nsOpenMsg, sizeof(nsOpenMsg), &msgOutput);
    if (myStatus != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "openCommon: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(myStatus));
        goto CommonExit;
    }

    myStatus = msgOutput.status;
    if (myStatus != StatusOk) {
#ifdef LIBNS_DEBUG
        // The caller can syslog if they desire
        xSyslog(moduleName,
                XlogErr,
                "openCommon: dlm node %u failed request: %s",
                dlmNode,
                strGetFromStatus(myStatus));
#endif
        goto CommonExit;
    }

    nsHandle = msgOutput.opOutput.nsHandle;
    if (nsOpenMsg.includeObject) {
        *nsObject = msgOutput.nsObject;
    }

CommonExit:
    *status = myStatus;
    return nsHandle;
}

Status
LibNs::close(LibNsTypes::NsHandle nsHandle, bool *objDeleted)
{
    NsCloseMsg closeMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsClose;
    Status status;
    NodeId dlmNode = XidMgr::get()->xidGetNodeId(nsHandle.nsId);

    // Passed as the payload to the twoPc request.
    closeMsg.header.nsOp = NsClose;
    closeMsg.header.nsId = nsHandle.nsId;
    closeMsg.header.viaId = true;
    closeMsg.nsHandle = nsHandle;

    status = dispatchDlm(dlmNode, &closeMsg, sizeof(closeMsg), &msgOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "close: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = msgOutput.status;
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        // Caller can syslog if they choose to.
        xSyslog(moduleName,
                XlogErr,
                "close: dlm node %u failed request: %s",
                dlmNode,
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    if (objDeleted != NULL) {
        *objDeleted = (msgOutput.opOutput.refCount == 0);
    }
CommonExit:
    return status;
}

LibNsTypes::NsId
LibNs::getNsId(const char *pathName)
{
    Status status;
    NsGetInfoMsg getInfoMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsGetInfo;
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    NodeId dlmNode = MaxNodes;

    if (!isValidPathName(pathName, &dlmNode)) {
        status = StatusInval;
        goto CommonExit;
    }

    getInfoMsg.header.nsOp = NsGetInfo;
    verifyOk(strStrlcpy(getInfoMsg.header.pathName,
                        pathName,
                        LibNsTypes::MaxPathNameLen));
    getInfoMsg.header.viaId = false;
    getInfoMsg.includeObject = false;

    status = dispatchDlm(dlmNode, &getInfoMsg, sizeof(getInfoMsg), &msgOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getNsid: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = msgOutput.status;
    BailIfFailed(status);

    nsId = msgOutput.opOutput.nsId;

CommonExit:
    return nsId;
}

NsObject *
LibNs::getNsObject(const char *pathName)
{
    NsObject *nsObject = NULL;

    // create a dummy handle to leverage existing apis
    LibNsTypes::NsHandle dummyHandle;
    dummyHandle.openNsId = XidInvalid;
    dummyHandle.nsId = getNsId(pathName);

    if (dummyHandle.nsId != LibNsTypes::NsInvalidId) {
        nsObject = getNsObject(dummyHandle);
    }

    return nsObject;
}

NsObject *
LibNs::getNsObject(LibNsTypes::NsHandle nsHandle)
{
    MsgNsOutput msgOutput;
    msgOutput.ops = NsGetInfo;
    Status status;
    NodeId dlmNode = XidMgr::get()->xidGetNodeId(nsHandle.nsId);
    NsObject *returnedObj = NULL;
    NsGetInfoMsg getInfoMsg;
    getInfoMsg.header.nsOp = NsGetInfo;
    getInfoMsg.header.nsId = nsHandle.nsId;
    getInfoMsg.nsHandle = nsHandle;
    getInfoMsg.header.viaId = true;
    getInfoMsg.includeObject = true;

    status = dispatchDlm(dlmNode, &getInfoMsg, sizeof(getInfoMsg), &msgOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getNsObject: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = msgOutput.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getNsObject: dlm node %u failed request: %s",
                dlmNode,
                strGetFromStatus(status));
        goto CommonExit;
    }

    returnedObj = msgOutput.nsObject;
CommonExit:
    return returnedObj;
}

LibNsTypes::NsHandle
LibNs::updateNsObject(LibNsTypes::NsHandle nsHandle,
                      NsObject *nsObj,
                      Status *status)
{
    Status myStatus;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsUpdate;
    LibNsTypes::NsHandle newHandle;
    NsUpdateMsg *updateMsg = NULL;
    size_t objSize = nsObj->getSize();
    size_t msgSize = sizeof(NsUpdateMsg) + objSize;
    NodeId dlmNode = XidMgr::get()->xidGetNodeId(nsHandle.nsId);

    *status = StatusOk;
    setHandleInvalid(&newHandle);

    if (!(nsHandle.openFlags == LibNsTypes::WriterExcl ||
          nsHandle.openFlags == LibNsTypes::ReadSharedWriteExclWriter)) {
        myStatus = StatusAccess;
        goto CommonExit;
    }

    // The payload for the twoPc
    updateMsg = (NsUpdateMsg *) memAllocExt(msgSize, moduleName);
    if (updateMsg == NULL) {
        myStatus = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "updateNsObject: failed to allocate %lu bytes for object",
                msgSize);
        goto CommonExit;
    }

    updateMsg->header.nsOp = NsUpdate;
    updateMsg->header.nsId = nsHandle.nsId;
    updateMsg->header.viaId = true;
    updateMsg->nsHandle = nsHandle;
    // Copy the updated object into the message
    memcpy(updateMsg->nsObject, nsObj, objSize);
    updateMsg->nsObjSize = objSize;

    myStatus = dispatchDlm(dlmNode, updateMsg, msgSize, &msgOutput);
    if (myStatus != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "updateNsObject: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(myStatus));
        goto CommonExit;
    }

    myStatus = msgOutput.status;
    if (myStatus != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "updateNsObject: dlm node %u failed request: %s",
                dlmNode,
                strGetFromStatus(myStatus));
#endif
        goto CommonExit;
    }
    newHandle = msgOutput.opOutput.nsHandle;

CommonExit:
    *status = myStatus;
    if (updateMsg != NULL) {
        memFree(updateMsg);
        updateMsg = NULL;
    }
    return newHandle;
}

char *
LibNs::getPathName(LibNsTypes::NsId nsId)
{
    Status status;
    NsGetInfoMsg getInfoMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsGetInfo;
    char *returnedPathName = NULL;
    NodeId dlmNode = XidMgr::get()->xidGetNodeId(nsId);

    // The payload for the twoPc
    getInfoMsg.header.nsOp = NsGetInfo;
    getInfoMsg.header.nsId = nsId;
    getInfoMsg.header.viaId = true;
    getInfoMsg.includeObject = false;

    status = dispatchDlm(dlmNode, &getInfoMsg, sizeof(getInfoMsg), &msgOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getPathName: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = msgOutput.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getPathName: dlm node %u failed request (ID %lu): %s",
                dlmNode,
                nsId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Caller is responsible for freeing memory
    returnedPathName =
        (char *) memAllocExt(LibNsTypes::MaxPathNameLen, moduleName);
    if (returnedPathName == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate memory (%zu bytes) for returned "
                "path name",
                LibNsTypes::MaxPathNameLen);
        goto CommonExit;
    }
    verifyOk(strStrlcpy(returnedPathName,
                        msgOutput.opOutput.pathName,
                        LibNsTypes::MaxPathNameLen));

CommonExit:
    return returnedPathName;
}

size_t
LibNs::getPathInfo(const char *pattern, LibNsTypes::PathInfoOut **pathInfo)
{
    Status status;
    NsGetPathInfoMsg getPathInfoMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsGetPathInfo;
    NsStreamMgmt streamMgmt;
    size_t matchCount = 0;
    Config *config = Config::get();

    *pathInfo = NULL;

    if (strlen(pattern) >= LibNsTypes::MaxPathNameLen) {
        xSyslog(moduleName,
                XlogErr,
                "Specified pattern '%s' is too long (max is %zu bytes)",
                pattern,
                LibNsTypes::MaxPathNameLen);
        status = StatusNsObjNameTooLong;
        goto CommonExit;
    }

#ifdef LIBNS_DEBUG
    if (strstr(pattern, "*") == NULL) {
        // No wildcard in the pattern.  Provide a warning but plow on.
        xSyslog(moduleName,
                XlogInfo,
                "Specified pattern '%s' does not contain a wildcard",
                pattern);
    }
#endif

    getPathInfoMsg.header.nsOp = NsGetPathInfo;
    verifyOk(strStrlcpy(getPathInfoMsg.header.pathName,
                        pattern,
                        LibNsTypes::MaxPathNameLen));
    getPathInfoMsg.header.viaId = false;
    getPathInfoMsg.senderId = config->getMyNodeId();

    // Used to manage the message stream used by the receiver of this twoPc
    // to send back (out of band wrt this function) the path info.
    streamMgmt.head = NULL;
    new (&streamMgmt.chunkLock) Mutex;

    msgOutput.streamMgmt = &streamMgmt;

    // As each node of the cluster hosts a subset of the namespace the request
    // has to be sent to each node.  The receiving node will initiate a
    // msgStream back to this node with the information for matching path names.
    // Once that has completed the node will respond to the twoPc request.  No
    // guarantees are made wrt races with new names being published or removed.

    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        status = dispatchDlm(ii,
                             &getPathInfoMsg,
                             sizeof(getPathInfoMsg),
                             &msgOutput);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "getPathInfo: failed to dispatch to dlm node %u: %s",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = msgOutput.status;
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "getPathInfo: dlm node %u failed request: %s",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Have received the information from each of the nodes.  Now combine it
    // into a single result.
    if (streamMgmt.head != NULL) {
        NsStreamChunk *chunk = streamMgmt.head;
        size_t resultSize = 0;

        // Figure out the required size and allocate the memory.
        while (chunk != NULL) {
            resultSize += chunk->size;
            chunk = chunk->next;
        }

        *pathInfo =
            (LibNsTypes::PathInfoOut *) memAllocExt(resultSize, moduleName);
        if (*pathInfo == NULL) {
            // XXX syslog
            status = StatusNoMem;
            goto CommonExit;
        }

        // Now combine the results into one.
        chunk = streamMgmt.head;
        size_t offset = 0;
        while (chunk != NULL) {
            void *dest = (void *) ((uintptr_t) *pathInfo + offset);
            memcpy(dest, &chunk->dataChunk, chunk->size);
            offset += chunk->size;

            NsStreamChunk *tmp = chunk;
            chunk = chunk->next;
            memFree(tmp);
        }
        assert(offset == resultSize);
        assert((resultSize % sizeof(LibNsTypes::PathInfoOut)) == 0);

        matchCount = resultSize / sizeof(LibNsTypes::PathInfoOut);

        streamMgmt.head = NULL;
    }

CommonExit:
    if (streamMgmt.head != NULL) {
        // Clean up partial results
        NsStreamChunk *chunk = streamMgmt.head;
        while (chunk != NULL) {
            NsStreamChunk *tmp = chunk;
            chunk = chunk->next;
            memFree(tmp);
        }
        streamMgmt.head = NULL;
    }

    return matchCount;
}

Status
LibNs::remove(const char *pathName, bool *objDeleted)
{
    NodeId objNodeId = MaxNodes;
    if (!isValidPathName(pathName, &objNodeId)) {
        return StatusNsInvalidObjName;
    }

    return markForRemovalCommon(pathName, objDeleted, objNodeId);
}

Status
LibNs::markForRemovalCommon(const char *pathName,
                            bool *objDeleted,
                            NodeId objNodeId)
{
    Status status;
    NsRemoveMsg removeMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsRemove;
    NodeId dlmNode = objNodeId;

    removeMsg.header.nsOp = NsRemove;
    verifyOk(strStrlcpy(removeMsg.header.pathName,
                        pathName,
                        LibNsTypes::MaxPathNameLen));
    removeMsg.header.viaId = false;
    removeMsg.pathNameIsPattern = false;

    status = dispatchDlm(dlmNode, &removeMsg, sizeof(removeMsg), &msgOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "markForRemovalCommon: failed to dispatch to dlm node %u for "
                "'%s': %s",
                dlmNode,
                pathName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = msgOutput.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "markForRemovalCommon: dlm node %u failed request for '%s': %s",
                dlmNode,
                pathName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (objDeleted != NULL) {
        *objDeleted = (msgOutput.opOutput.refCount == 0);
    }

CommonExit:
    return status;
}

Status
LibNs::remove(LibNsTypes::NsId nsId, bool *objDeleted)
{
    return markForRemovalCommon(nsId, objDeleted);
}

Status
LibNs::markForRemovalCommon(LibNsTypes::NsId nsId, bool *objDeleted)
{
    Status status;
    NsRemoveMsg removeMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsRemove;
    NodeId dlmNode = XidMgr::get()->xidGetNodeId(nsId);

    removeMsg.header.nsOp = NsRemove;
    removeMsg.header.nsId = nsId;
    removeMsg.header.viaId = true;
    removeMsg.pathNameIsPattern = false;

    status = dispatchDlm(dlmNode, &removeMsg, sizeof(removeMsg), &msgOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "markForRemovalCommon: failed to dispatch to dlm node %u: %s",
                dlmNode,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = msgOutput.status;
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "markForRemovalCommon: dlm node %u failed request: %s",
                dlmNode,
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    if (objDeleted != NULL) {
        *objDeleted = (msgOutput.opOutput.refCount == 0);
    }

CommonExit:
    return status;
}

Status
LibNs::removeMatching(const char *pattern)
{
    Status status;
    NsRemoveMsg removeMsg;
    MsgNsOutput msgOutput;
    msgOutput.ops = NsRemove;

    if (strlen(pattern) >= LibNsTypes::MaxPathNameLen) {
        xSyslog(moduleName,
                XlogErr,
                "Specified pattern '%s' is too long (max is %zu bytes)",
                pattern,
                LibNsTypes::MaxPathNameLen);
        status = StatusNsObjNameTooLong;
        goto CommonExit;
    }

#ifdef LIBNS_DEBUG
    if (strstr(pattern, "*") == NULL) {
        // No wildcard in the pattern.  Provide a warning but plow on.
        xSyslog(moduleName,
                XlogInfo,
                "Specified pattern '%s' does not contain a wildcard",
                pattern);
    }
#endif

    removeMsg.header.nsOp = NsRemove;
    verifyOk(strStrlcpy(removeMsg.header.pathName,
                        pattern,
                        LibNsTypes::MaxPathNameLen));
    removeMsg.header.viaId = false;
    removeMsg.pathNameIsPattern = true;

    // As each node of the cluster hosts a subset of the namespace the request
    // has to be sent to each node.  No guarantees are made wrt races with
    // new names being published...especially as this is done serially through
    // the nodes.
    for (unsigned ii = 0; ii < Config::get()->getActiveNodes(); ii++) {
        status = dispatchDlm(ii, &removeMsg, sizeof(removeMsg), &msgOutput);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "removeMatching: failed to dispatch to dlm node %u: %s",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = msgOutput.status;
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "removeMatching: dlm node %u failed request: %s",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

Status
LibNs::addStreamData(void *ephemeral, void *data, size_t dataSize)
{
    Status status = StatusOk;
    MsgNsOutput *msgOutput = (MsgNsOutput *) ephemeral;

    assert((dataSize % sizeof(LibNsTypes::PathInfoOut)) == 0);

    // Allocate memory for the chunk.
    NsStreamChunk *streamChunk =
        (NsStreamChunk *) memAllocExt(dataSize + sizeof(NsStreamChunk),
                                      moduleName);
    if (streamChunk == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    streamChunk->next = NULL;
    streamChunk->size = dataSize;
    memcpy(&streamChunk->dataChunk, data, dataSize);

    // Put the chunk onto the list
    msgOutput->streamMgmt->chunkLock.lock();
    if (msgOutput->streamMgmt->head == NULL) {
        msgOutput->streamMgmt->head = streamChunk;
    } else {
        NsStreamChunk *tmp = msgOutput->streamMgmt->head;
        msgOutput->streamMgmt->head = streamChunk;
        streamChunk->next = tmp;
    }
    msgOutput->streamMgmt->chunkLock.unlock();

CommonExit:

    return status;
}

// Removes the dlm object from the hash tables and then gets rid of
// the object.
void
LibNs::deleteDlmObject(NsDlmObject *nsDlmObject)
{
    PathNameToNsIdEntry *pathNameToNsId;
    NsIdToNsDlmObjectEntry *nsIdToNsDlmObj;

    pathNameToNsId = pathNameToNsIdHashTable_.remove(nsDlmObject->pathName);
    assert(pathNameToNsId != NULL);
    assert(pathNameToNsId->nsId == nsDlmObject->nsId);

    nsIdToNsDlmObj = nsIdToNsDlmObjectHashTable_.remove(nsDlmObject->nsId);
    assert(nsIdToNsDlmObj != NULL);
    assert(nsIdToNsDlmObj->nsDlmObject == nsDlmObject);

    nsDlmObject->openTable->~OpenTableHashTable();
    memFree(nsDlmObject->openTable);
    memFree(nsDlmObject);

    // Free the hash table entriesa
    memFree(pathNameToNsId);
    memFree(nsIdToNsDlmObj);
}

Status
LibNs::dispatchDlm(NodeId dlmNode,
                   void *payload,
                   size_t payloadSize,
                   void *response)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();

    msgMgr->twoPcEphemeralInit(&eph,
                               payload,
                               payloadSize,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDlmUpdateNs1,
                               response,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    // Lib NS uses could be already in the context of a 2PC when they access
    // Lib NS. So to be conservative, we will always assume this 2PC to be of
    // nested type.
    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDlmUpdateNs,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNested);
    if (status == StatusOk) {
        assert(!twoPcHandle.twoPcHandle);
    }

    return status;
}

void
LibNs::dlmUpdateNsCompletion(MsgEphemeral *eph, void *payload)
{
    MsgNsOutput *msgOutput = (MsgNsOutput *) eph->ephemeral;
    msgOutput->status = eph->status;
    if (msgOutput->status != StatusOk) {
        return;
    }

    NsOpResponse *response = (NsOpResponse *) payload;
    memcpy(&msgOutput->opOutput, &response->opOutput, sizeof(NsOpOutput));

    if (response->nsObjectSize != 0) {
        msgOutput->nsObject = (NsObject *) memAlloc(response->nsObjectSize);
        if (msgOutput->nsObject == NULL) {
            msgOutput->status = StatusNoMem;
            return;
        }
        memcpy(msgOutput->nsObject, response->nsObject, response->nsObjectSize);
    }
}

void
LibNs::updateDlm(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    NsHeader *nsHdr = (NsHeader *) payload;
    size_t numBytesUsedInPayload = 0;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    eph->payloadToDistribute = NULL;

    dlmLock_.lock();

    switch (nsHdr->nsOp) {
    case NsPublish:
        status = publishLocal(payload,
                              &eph->payloadToDistribute,
                              &numBytesUsedInPayload);
        break;
    case NsOpen:
        status = openLocal(payload,
                           &eph->payloadToDistribute,
                           &numBytesUsedInPayload);
        break;
    case NsClose:
        status = closeLocal(payload,
                            &eph->payloadToDistribute,
                            &numBytesUsedInPayload);
        break;
    case NsGetInfo:
        status = getInfoLocal(payload,
                              &eph->payloadToDistribute,
                              &numBytesUsedInPayload);
        break;
    case NsUpdate:
        status = updateLocal(payload,
                             &eph->payloadToDistribute,
                             &numBytesUsedInPayload);
        break;
    case NsGetPathInfo:
        status = getPathInfoLocal(payload,
                                  &eph->payloadToDistribute,
                                  &numBytesUsedInPayload,
                                  eph);
        break;
    case NsRemove:
        status = removeLocal(payload,
                             &eph->payloadToDistribute,
                             &numBytesUsedInPayload);
        break;
    default:
        assert(0);
        status = StatusInval;
    }

    dlmLock_.unlock();

    xcAssertIf((status != StatusOk), (numBytesUsedInPayload == 0));
    eph->setAckInfo(status, numBytesUsedInPayload);
}

// ----------------------------------------------------------------------------
// These *Local functions are the "handlers" which run on the dlm node.
// The functions are serialized though the dlmLock and so there shouldn't
// be need for locking of items accessed only under the dlmLock.
// ----------------------------------------------------------------------------

// Publish locally means the pathname to NsId hashtable and
// NsId to NsObject hashtable is populated.

Status
LibNs::publishLocal(void *payload, void **retOutput, size_t *retOutputSize)
{
    Status status = StatusOk;
    PathNameToNsIdEntry *pathNameToNsId = NULL;
    NsDlmObject *dlmObj = NULL;
    NsIdToNsDlmObjectEntry *nsIdToNsDlmObjectEntry = NULL;
    size_t dlmObjSize;
    OpenTableHashTable *openTableHashTable = NULL;

    // Payload is used both IN and Out
    NsPublishMsg *publishMsg = (NsPublishMsg *) payload;
    NsOpResponse *opResponse = NULL;
    NsOpOutput *opOutput = NULL;
    char *pathName = publishMsg->header.pathName;
    NsObject *nsObj = (NsObject *) publishMsg->nsObject;

    pathNameToNsId = pathNameToNsIdHashTable_.find(pathName);

    *retOutput = NULL;
    *retOutputSize = 0;

    if (pathNameToNsId != NULL) {
        // Path name exists...return appropriate status
        status = getDlmObj((NsHeader *) publishMsg, &dlmObj);
        if (status != StatusOk) {
            assert(0 && "Unable to find namespace object");
            // On non-debug builds, syslog and bubble up an error
            xSyslog(moduleName,
                    XlogErr,
                    "publishLocal: '%s' inconsistency in namespace tables",
                    pathName);
            status = StatusNsInternalTableError;
        } else {
            if (dlmObj->pendingRemove) {
                status = StatusPendingRemoval;
            } else {
                status = StatusExist;
            }
        }

        // Return status here.  We cannot go through the CommonExit
        // clean up as we don't want to affect the existing object.
        return status;
    }

    // Allocate the entries that will be added to the name-to-nsid
    // and nsid-to-dlmObj hash tables.
    pathNameToNsId =
        (PathNameToNsIdEntry *) memAllocExt(sizeof(*pathNameToNsId),
                                            moduleName);
    BailIfNull(pathNameToNsId);
    nsIdToNsDlmObjectEntry =
        (NsIdToNsDlmObjectEntry *) memAllocExt(sizeof(*nsIdToNsDlmObjectEntry),
                                               moduleName);
    BailIfNull(nsIdToNsDlmObjectEntry);

    // Allocate a hash table to keep track of the NsId(s) used to open
    // the object.  A close must use the same NsId(s).
    openTableHashTable =
        (OpenTableHashTable *) memAllocExt(sizeof(*openTableHashTable),
                                           moduleName);
    BailIfNull(openTableHashTable);

    // Allocate an object which LibNs manages and contains the user
    // specified object.
    dlmObjSize = sizeof(NsDlmObject) + nsObj->getSize();
    dlmObj = (NsDlmObject *) memAllocExt(dlmObjSize, moduleName);
    BailIfNull(dlmObj);

    new (dlmObj) NsDlmObject();

    // Fill in the entry for the name-to-nsid hash table and insert
    // it into the table.
    verifyOk(strStrlcpy(pathNameToNsId->pathName,
                        pathName,
                        LibNsTypes::MaxPathNameLen));
    pathNameToNsId->nsId = XidMgr::get()->xidGetNext();

    status = pathNameToNsIdHashTable_.insert(pathNameToNsId);
    assert(status == StatusOk);

    // Populate the LibNs managed object.
    verifyOk(
        strStrlcpy(dlmObj->pathName, pathName, LibNsTypes::MaxPathNameLen));
    dlmObj->nsId = pathNameToNsId->nsId;
    refInit(&dlmObj->refCount);
    dlmObj->version = 1;
    dlmObj->openedReaderWriterExcl = false;
    dlmObj->openedReaderShared = false;
    dlmObj->openedReaderSharedWriterExcl = false;

    dlmObj->numReaderOpens = 0;
    dlmObj->numWriterOpens = 0;
    dlmObj->openTable = new (openTableHashTable) OpenTableHashTable();
    dlmObj->pendingRemove = false;
    dlmObj->autoRemove = publishMsg->autoRemove;
    memcpy(dlmObj->nsObject, nsObj, nsObj->getSize());

    // Fill in the entry for the xid-to-nsobj hash table and insert
    // it into the table.
    nsIdToNsDlmObjectEntry->nsId = pathNameToNsId->nsId;
    nsIdToNsDlmObjectEntry->nsDlmObject = dlmObj;

    status = nsIdToNsDlmObjectHashTable_.insert(nsIdToNsDlmObjectEntry);
    assert(status != StatusExist);

    // Cannot access publishMsg once we start writing the output
    // as it reuses the payload.

    // Package up the return info
    opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
    BailIfNull(opResponse);

    opResponse->nsObjectSize = 0;
    opOutput = &opResponse->opOutput;
    opOutput->nsId = pathNameToNsId->nsId;
    opOutput->refCount = 1;

    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse);

    statsIncPublished();

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failure in publishLocal '%s': %s",
                pathName,
                strGetFromStatus(status));

        if (pathNameToNsId != NULL) {
            memFree(pathNameToNsId);
            pathNameToNsId = NULL;
        }
        if (nsIdToNsDlmObjectEntry != NULL) {
            memFree(nsIdToNsDlmObjectEntry);
            nsIdToNsDlmObjectEntry = NULL;
        }
        if (dlmObj != NULL) {
            memFree(dlmObj);
            dlmObj = NULL;
        }
        if (openTableHashTable != NULL) {
            memFree(openTableHashTable);
            openTableHashTable = NULL;
        }
    }

    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

Status
LibNs::openHelper(LibNsTypes::NsOpenFlags openFlags, NsDlmObject *dlmObj)
{
    Status status = StatusOk;

    if (openFlags == LibNsTypes::WriterExcl) {
        if (dlmObj->openedReaderWriterExcl || dlmObj->openedReaderShared ||
            dlmObj->openedReaderSharedWriterExcl) {
            status = StatusAccess;
            goto CommonExit;
        }
        assert(dlmObj->numReaderOpens == 0 && dlmObj->numWriterOpens == 0);
        dlmObj->openedReaderWriterExcl = true;
    } else if (openFlags == LibNsTypes::ReaderShared) {
        if (dlmObj->openedReaderWriterExcl ||
            dlmObj->openedReaderSharedWriterExcl) {
            status = StatusAccess;
            goto CommonExit;
        }
        if (dlmObj->numReaderOpens == 0) {
            assert(dlmObj->openedReaderShared == false);
            dlmObj->openedReaderShared = true;
        }
        assert(dlmObj->openedReaderShared == true);
        assert(dlmObj->numWriterOpens == 0);
        dlmObj->numReaderOpens++;
    } else if (openFlags ==
               (LibNsTypes::ReaderSharedWriterExcl | LibNsTypes::Reader)) {
        if (dlmObj->openedReaderWriterExcl || dlmObj->openedReaderShared) {
            status = StatusAccess;
            goto CommonExit;
        }
        if (dlmObj->numReaderOpens == 0 && dlmObj->numWriterOpens == 0) {
            assert(dlmObj->openedReaderSharedWriterExcl == false);
            dlmObj->openedReaderSharedWriterExcl = true;
        }
        assert(dlmObj->numReaderOpens >= 0 && dlmObj->numWriterOpens <= 1);
        dlmObj->numReaderOpens++;
    } else if (openFlags ==
               (LibNsTypes::ReaderSharedWriterExcl | LibNsTypes::Writer)) {
        if (dlmObj->openedReaderWriterExcl || dlmObj->openedReaderShared ||
            (dlmObj->openedReaderSharedWriterExcl &&
             dlmObj->numWriterOpens != 0)) {
            status = StatusAccess;
            goto CommonExit;
        }
        if (dlmObj->numReaderOpens == 0 && dlmObj->numWriterOpens == 0) {
            assert(dlmObj->openedReaderSharedWriterExcl == false);
            dlmObj->openedReaderSharedWriterExcl = true;
        }
        dlmObj->numWriterOpens++;
        assert(dlmObj->numReaderOpens >= 0 && dlmObj->numWriterOpens <= 1);
    } else {
        assert(0 && "Passed in invalid open flags");
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
LibNs::openLocal(void *payload, void **retOutput, size_t *retOutputSize)
{
    Status status = StatusOk;
    LibNsTypes::NsId openNsId;
    OpenTableHTEntry *openTableHTEntry = NULL;
    bool addedToHT = false;
    size_t numBytesUsedInPayload = 0;

    // Payload is used both IN and OUT
    NsOpenMsg *openMsg = (NsOpenMsg *) payload;
    NsOpResponse *opResponse = NULL;
    NsOpOutput *opOutput = NULL;

    NsDlmObject *dlmObj;
    LibNsTypes::NsOpenFlags openFlags;
    bool includeObject;

    *retOutput = NULL;
    *retOutputSize = 0;

    status = getDlmObj((NsHeader *) openMsg, &dlmObj);
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "openLocal: failed to get dlm object: %s",
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    if (dlmObj->pendingRemove) {
        status = StatusPendingRemoval;
        goto CommonExit;
    }

    status = openHelper(openMsg->openFlags, dlmObj);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Protect the open.
    refInc(&dlmObj->refCount);

    // Get a NsId to associate with this open.  It must match when the
    // close is done.
    openTableHTEntry =
        (OpenTableHTEntry *) memAllocExt(sizeof(*openTableHTEntry), moduleName);
    BailIfNull(openTableHTEntry);

    openNsId = XidMgr::get()->xidGetNext();
    openTableHTEntry->openNsId = openNsId;
    status = dlmObj->openTable->insert(openTableHTEntry);
    assert(status == StatusOk);
    addedToHT = true;

    // Cannot access openMsg once we start writing the output
    // as it reuses the payload.  So save anything needed.
    openFlags = openMsg->openFlags;
    includeObject = openMsg->includeObject;

    // Package up the return info
    if (includeObject) {
        NsObject *nsObj = (NsObject *) dlmObj->nsObject;
        numBytesUsedInPayload = sizeof(NsOpResponse) + nsObj->getSize();
        opResponse = (NsOpResponse *) memAlloc(numBytesUsedInPayload);
        BailIfNull(opResponse);
        opResponse->nsObjectSize = nsObj->getSize();
        memcpy(opResponse->nsObject, nsObj, nsObj->getSize());
    } else {
        numBytesUsedInPayload = sizeof(NsOpResponse);
        opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
        BailIfNull(opResponse);
        opResponse->nsObjectSize = 0;
    }
    opOutput = &opResponse->opOutput;
    opOutput->nsHandle.nsId = dlmObj->nsId;
    opOutput->nsHandle.version = dlmObj->version;
    opOutput->nsHandle.openFlags = openFlags;
    opOutput->nsHandle.openNsId = openNsId;

    *retOutput = opResponse;
    *retOutputSize = numBytesUsedInPayload;
    statsIncOpened();

CommonExit:

    if (status != StatusOk) {
        if (addedToHT) {
            OpenTableHTEntry *htEntry = dlmObj->openTable->remove(openNsId);
            assert(htEntry == openTableHTEntry);
            memFree(openTableHTEntry);
            openTableHTEntry = NULL;
        }
    }

    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

void
LibNs::closeHelper(LibNsTypes::NsOpenFlags openFlags, NsDlmObject *dlmObj)
{
    if (openFlags == LibNsTypes::WriterExcl) {
        assert(dlmObj->openedReaderWriterExcl == true);
        dlmObj->openedReaderWriterExcl = false;
        assert(dlmObj->numReaderOpens == 0 && dlmObj->numWriterOpens == 0);
    } else if (openFlags == LibNsTypes::ReaderShared) {
        assert(dlmObj->numReaderOpens > 0);
        dlmObj->numReaderOpens--;
        assert(dlmObj->numWriterOpens == 0);
        assert(dlmObj->openedReaderShared == true);
        if (dlmObj->numReaderOpens == 0) {
            dlmObj->openedReaderShared = false;
        }
    } else if (openFlags == LibNsTypes::ReadSharedWriteExclReader) {
        assert(dlmObj->numReaderOpens > 0);
        dlmObj->numReaderOpens--;
        assert(dlmObj->numWriterOpens <= 1);
        assert(dlmObj->openedReaderSharedWriterExcl == true);
        if (dlmObj->numReaderOpens == 0 && dlmObj->numWriterOpens == 0) {
            dlmObj->openedReaderSharedWriterExcl = false;
        }
    } else {
        assert(openFlags == LibNsTypes::ReadSharedWriteExclWriter);
        assert(dlmObj->numWriterOpens == 1);
        dlmObj->numWriterOpens--;
        assert(dlmObj->numReaderOpens >= 0);
        assert(dlmObj->openedReaderSharedWriterExcl == true);
        if (dlmObj->numReaderOpens == 0 && dlmObj->numWriterOpens == 0) {
            dlmObj->openedReaderSharedWriterExcl = false;
        }
    }
}

Status
LibNs::closeLocal(void *payload, void **retOutput, size_t *retOutputSize)
{
    Status status = StatusOk;
    OpenTableHTEntry *openTableHTEntry = NULL;

    // Payload is used both IN and OUT
    NsCloseMsg *closeMsg = (NsCloseMsg *) payload;
    NsOpResponse *opResponse = NULL;
    NsOpOutput *opOutput = NULL;
    NsDlmObject *dlmObj;

    *retOutput = NULL;
    *retOutputSize = 0;

    status = getDlmObj((NsHeader *) closeMsg, &dlmObj);
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "closeLocal: failed to get dlm object: %s",
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    // Ensure the handle is still valid (hasn't already been closed).
    openTableHTEntry = dlmObj->openTable->remove(closeMsg->nsHandle.openNsId);
    if (openTableHTEntry == NULL) {
        status = StatusNsStale;
        goto CommonExit;
    }
    memFree(openTableHTEntry);
    openTableHTEntry = NULL;

    closeHelper(closeMsg->nsHandle.openFlags, dlmObj);

    // Cannot access closeMsg once we start writing the output
    // as it reuses the payload.

    // Return the ref count value reflecting the decrement
    opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
    BailIfNull(opResponse);

    opResponse->nsObjectSize = 0;
    opOutput = &opResponse->opOutput;
    opOutput->refCount = refRead(&dlmObj->refCount) - 1;

    if (dlmObj->autoRemove == LibNsTypes::DoAutoRemove &&
        opOutput->refCount == 1) {
        // Automatically remove the object when the refcount transitions
        // down from 2 to 1.
        opOutput->refCount = 0;
        // This decrements to 1.  The below refDec will transition to 0.
        refDec(&dlmObj->refCount);
    }

    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse);

    // Caution, the dlm object may be gone by the time this function
    // returns.
    refDec(&dlmObj->refCount);

    statsIncClosed();

CommonExit:
    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

Status
LibNs::updateLocal(void *payload, void **retOutput, size_t *retOutputSize)
{
    Status status = StatusOk;

    // Payload is used both IN and OUt
    NsUpdateMsg *updateMsg = (NsUpdateMsg *) payload;
    NsOpResponse *opResponse = NULL;
    NsOpOutput *opOutput = NULL;

    OpenTableHTEntry *openTableHTEntry = NULL;
    NsObject *newNsObj;
    NsDlmObject *curDlmObj = NULL;
    NsDlmObject *newDlmObj = NULL;
    size_t newDlmObjSize;
    LibNsTypes::NsOpenFlags openFlags;
    NsIdToNsDlmObjectEntry *nsIdToNsDlmObjectEntry = NULL;
    NsIdToNsDlmObjectEntry *oldnsIdToNsDlmObjectEntry = NULL;

    *retOutput = NULL;
    *retOutputSize = 0;

    // Get the current dlm object.
    status = getDlmObj((NsHeader *) updateMsg, &curDlmObj);
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "updateLocal: failed to get dlm object: %s",
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    // The open flags were also checked for readwrite on the source side.
    assert(updateMsg->nsHandle.openFlags == LibNsTypes::WriterExcl ||
           updateMsg->nsHandle.openFlags ==
               LibNsTypes::ReadSharedWriteExclWriter);
    assert(updateMsg->header.nsId == updateMsg->nsHandle.nsId);

#ifdef DEBUG
    if (curDlmObj->openedReaderWriterExcl) {
        assert(curDlmObj->numWriterOpens == 0 &&
               curDlmObj->numReaderOpens == 0);
    } else {
        assert(curDlmObj->openedReaderSharedWriterExcl);
        assert(curDlmObj->numWriterOpens == 1 &&
               curDlmObj->numReaderOpens >= 0);
    }
#endif  // DEBUG

    // Ensure the handle is still valid (hasn't already been closed).
    openTableHTEntry = curDlmObj->openTable->find(updateMsg->nsHandle.openNsId);
    if (openTableHTEntry == NULL) {
        status = StatusNsStale;
        goto CommonExit;
    }
    openTableHTEntry = NULL;

    // The new client-supplied object to use in the update
    newNsObj = (NsObject *) updateMsg->nsObject;

    // Allocate a new dlm object.
    newDlmObjSize = sizeof(NsDlmObject) + newNsObj->getSize();
    newDlmObj = (NsDlmObject *) memAllocExt(newDlmObjSize, moduleName);
    BailIfNull(newDlmObj);

    new (newDlmObj) NsDlmObject();

    // Populate the new dlm object.
    verifyOk(strStrlcpy(newDlmObj->pathName,
                        curDlmObj->pathName,
                        LibNsTypes::MaxPathNameLen));
    newDlmObj->nsId = curDlmObj->nsId;
    // Transfer the ref count info.
    newDlmObj->refCount = curDlmObj->refCount;
    newDlmObj->version = ++curDlmObj->version;
    // Transfer open hash table
    newDlmObj->openTable = curDlmObj->openTable;
    newDlmObj->openedReaderWriterExcl = curDlmObj->openedReaderWriterExcl;
    newDlmObj->openedReaderShared = curDlmObj->openedReaderShared;
    newDlmObj->openedReaderSharedWriterExcl =
        curDlmObj->openedReaderSharedWriterExcl;
    newDlmObj->numReaderOpens = curDlmObj->numReaderOpens;
    newDlmObj->numWriterOpens = curDlmObj->numWriterOpens;
    newDlmObj->pendingRemove = curDlmObj->pendingRemove;
    memcpy(newDlmObj->nsObject, newNsObj, newNsObj->getSize());

    // Update the xid-to-obj hash table to point at the new dlm object.  This
    // requires removing the current entry and replacing it with a new one.
    nsIdToNsDlmObjectEntry =
        (NsIdToNsDlmObjectEntry *) memAllocExt(sizeof(*nsIdToNsDlmObjectEntry),
                                               moduleName);
    BailIfNull(nsIdToNsDlmObjectEntry);

    // Fill in the entry for the nsId-to-dlmObj hash table.
    nsIdToNsDlmObjectEntry->nsId = newDlmObj->nsId;
    nsIdToNsDlmObjectEntry->nsDlmObject = newDlmObj;

    // Now do the hash table remove/insert
    oldnsIdToNsDlmObjectEntry =
        nsIdToNsDlmObjectHashTable_.remove(curDlmObj->nsId);
    assert(oldnsIdToNsDlmObjectEntry != NULL);
    status = nsIdToNsDlmObjectHashTable_.insert(nsIdToNsDlmObjectEntry);
    assert(status == StatusOk);

    // Cannot access updateMsg once we start writing the output
    // as it reuses the payload.  So save anthing needed.
    openFlags = updateMsg->nsHandle.openFlags;

    // Package up the return info
    opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
    BailIfNull(opResponse);

    opResponse->nsObjectSize = 0;
    opOutput = &opResponse->opOutput;
    opOutput->nsHandle.nsId = newDlmObj->nsId;
    opOutput->nsHandle.openNsId = updateMsg->nsHandle.openNsId;
    opOutput->nsHandle.version = newDlmObj->version;
    opOutput->nsHandle.openFlags = openFlags;

    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse);

    statsIncUpdated();

CommonExit:

    if (status == StatusOk) {
        // Successfully replaced the old object with the new one.
        if (curDlmObj != NULL) {
            // Don't need the old object...it's no longer considered
            // an object (e.g. not in hash table, ref count not used, etc)
            // so just get rid of it.
            memFree(curDlmObj);
            curDlmObj = NULL;
        }
    }

    if (oldnsIdToNsDlmObjectEntry) {
        memFree(oldnsIdToNsDlmObjectEntry);
        oldnsIdToNsDlmObjectEntry = NULL;
    }

    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

Status
LibNs::getDlmObj(NsHeader *nsHdr, NsDlmObject **dlmObj)
{
    Status status = StatusOk;
    LibNsTypes::NsId nsId;
    bool getViaName = true;

    if (nsHdr->viaId) {
        getViaName = false;
    }

    NsIdToNsDlmObjectEntry *nsIdToNsDlmObj;

    *dlmObj = NULL;

    if (getViaName) {
        // Use the name to get the associated NsId from the hash table.
        char *pathName = nsHdr->pathName;
        PathNameToNsIdEntry *pathNameToNsId;

        pathNameToNsId = pathNameToNsIdHashTable_.find(pathName);

        if (pathNameToNsId == NULL) {
            status = StatusNsNotFound;
            goto CommonExit;
        }

        nsId = pathNameToNsId->nsId;
    } else {
        // Use the specified NsId.
        nsId = nsHdr->nsId;
    }

    nsIdToNsDlmObj = nsIdToNsDlmObjectHashTable_.find(nsId);

    if (nsIdToNsDlmObj == NULL) {
        assert(!getViaName && "path name in Hash Table but not NsId");
        status = StatusNsNotFound;
        goto CommonExit;
    }

    *dlmObj = (NsDlmObject *) nsIdToNsDlmObj->nsDlmObject;

CommonExit:

    return status;
}

// This function is called when the requested object is hosted (via Dlm)
// on the local node.
NsObject *
LibNs::getNsObjectDlmLocal(LibNsTypes::NsHandle nsHandle)
{
    Status status = StatusOk;
    NsHeader nsHeader;
    NsObject *returnedObj = NULL;
    NsObject *nsObj = NULL;
    bool lockHeld = false;
    NsDlmObject *dlmObj;
    OpenTableHTEntry *openTableHTEntry = NULL;

    assert(XidMgr::get()->xidGetNodeId(nsHandle.nsId) ==
           Config::get()->getMyNodeId());

    nsHeader.nsOp = NsGetInfo;
    nsHeader.nsId = nsHandle.nsId;
    nsHeader.viaId = true;

    dlmLock_.lock();
    lockHeld = true;

    status = getDlmObj(&nsHeader, &dlmObj);
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "getNsObjectDlmLocal: failed to get dlm object: %s",
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    if (nsHandle.openNsId != XidInvalid) {
        openTableHTEntry = dlmObj->openTable->find(nsHandle.openNsId);
        if (openTableHTEntry == NULL) {
            // No open associated with handle
            goto CommonExit;
        }
    }

    if (dlmObj->pendingRemove) {
        // Object has been removed
        goto CommonExit;
    }

    nsObj = (NsObject *) dlmObj->nsObject;

    returnedObj = (NsObject *) memAllocExt(nsObj->getSize(), moduleName);
    if (returnedObj == NULL) {
        goto CommonExit;
    }

    memcpy(returnedObj, nsObj, nsObj->getSize());

    statsIncLookedup();

CommonExit:
    if (lockHeld) {
        dlmLock_.unlock();
        lockHeld = false;
    }

    return returnedObj;
}

Status
LibNs::getInfoLocal(void *payload, void **retOutput, size_t *retOutputSize)
{
    Status status = StatusOk;

    // Payload is used both IN and OUT
    NsGetInfoMsg *getInfoMsg = (NsGetInfoMsg *) payload;
    NsOpResponse *opResponse = NULL;
    NsOpOutput *opOutput = NULL;

    NsDlmObject *dlmObj;
    bool includeObject;
    OpenTableHTEntry *openTableHTEntry = NULL;

    if (getInfoMsg->header.viaId) {
        assert(XidMgr::get()->xidGetNodeId(getInfoMsg->header.nsId) ==
               Config::get()->getMyNodeId());
    }

    *retOutput = NULL;
    *retOutputSize = 0;

    status = getDlmObj((NsHeader *) getInfoMsg, &dlmObj);
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "getInfoLocal: failed to get dlm object: %s",
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    // Cannot access the getInfoMsg once we start writing the output
    // as it reuses the payload.  So save anything needed.
    includeObject = getInfoMsg->includeObject;

    // Ensure the handle is still valid (hasn't already been closed).
    if (includeObject) {
        if (getInfoMsg->nsHandle.openNsId != XidInvalid) {
            openTableHTEntry =
                dlmObj->openTable->find(getInfoMsg->nsHandle.openNsId);
            if (openTableHTEntry == NULL) {
                status = StatusNsStale;
                goto CommonExit;
            }
            openTableHTEntry = NULL;
        }
    }

    if (dlmObj->pendingRemove) {
        status = StatusPendingRemoval;
        goto CommonExit;
    }

    // Package up the response payload
    if (includeObject) {
        NsObject *nsObj = (NsObject *) dlmObj->nsObject;
        opResponse =
            (NsOpResponse *) memAlloc(sizeof(NsOpResponse) + nsObj->getSize());
        BailIfNull(opResponse);
        opResponse->nsObjectSize = nsObj->getSize();
        memcpy(opResponse->nsObject, nsObj, nsObj->getSize());
    } else {
        opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
        BailIfNull(opResponse);
        opResponse->nsObjectSize = 0;
    }
    opOutput = &opResponse->opOutput;
    opOutput->nsId = dlmObj->nsId;
    verifyOk(strStrlcpy(opOutput->pathName,
                        dlmObj->pathName,
                        LibNsTypes::MaxPathNameLen));
    opOutput->refCount = refRead(&dlmObj->refCount);

    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse) + opResponse->nsObjectSize;

    statsIncLookedup();

CommonExit:
    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

Status
LibNs::removeLocal(void *payload, void **retOutput, size_t *retOutputSize)
{
    Status status = StatusOk;
    // Payload is used for IN and OUT
    NsRemoveMsg *removeMsg = (NsRemoveMsg *) payload;
    NsOpResponse *opResponse = NULL;
    NsOpOutput *opOutput = NULL;

    NsDlmObject *dlmObj;

    *retOutput = NULL;
    *retOutputSize = 0;

    if (removeMsg->pathNameIsPattern) {
        // Delete the matching objects
        return removeMatchingLocal(payload, retOutput, retOutputSize);
    }

    status = getDlmObj((NsHeader *) removeMsg, &dlmObj);
    if (status != StatusOk) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "removeLocal: failed to get dlm object: %s",
                strGetFromStatus(status));
#endif
        goto CommonExit;
    }

    if (dlmObj->pendingRemove) {
#ifdef LIBNS_DEBUG
        xSyslog(moduleName,
                XlogErr,
                "removeLocal: '%s' already marked for removal",
                dlmObj->pathName);
#endif
        status = StatusPendingRemoval;
        goto CommonExit;
    }

    // Mark as pending removal which disallows some (see .h file) subsequent
    // operations on the object.
    dlmObj->pendingRemove = true;

#ifdef LIBNS_DEBUG
    xSyslog(moduleName,
            XlogInfo,
            "removeLocal: marking %s for removal",
            dlmObj->pathName);
#endif

    // Cannot access the removeMsg once we start writing the output
    // as it reuses the payload.  So save anything needed.

    // Package up the response payload.
    opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
    BailIfNull(opResponse);

    opResponse->nsObjectSize = 0;
    opOutput = &opResponse->opOutput;
    opOutput->nsId = dlmObj->nsId;
    opOutput->refCount = refRead(&dlmObj->refCount);
    opOutput->refCount--;

    // Caution, the dlm object may be gone when the refDec returns.
    refDec(&dlmObj->refCount);

    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse);

CommonExit:
    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

Status
LibNs::removeMatchingLocal(void *payload,
                           void **retOutput,
                           size_t *retOutputSize)
{
    Status status = StatusOk;
    // Payload is used for IN and OUT
    NsRemoveMsg *removeMsg = (NsRemoveMsg *) payload;
    NsOpResponse *opResponse = NULL;

    NsDlmObject *dlmObj;
    NsDlmObject *putObjList = NULL;
    PathNameToNsIdEntry *pathNameToNsId;
    char *pattern = removeMsg->header.pathName;
    NsIdToNsDlmObjectEntry *nsIdToNsDlmObj;

    assert(removeMsg->pathNameIsPattern);

    *retOutput = NULL;
    *retOutputSize = 0;

    // Iterate through all the items in the name-to-nsid hash table.
    for (PathNameToNsIdHashTable::iterator it =
             pathNameToNsIdHashTable_.begin();
         (pathNameToNsId = it.get()) != NULL;
         it.next()) {
        if (strMatch(pattern, pathNameToNsId->getPathName())) {
            // Path name matches the pattern.  Get the entry from the
            // nsid-to-dlmobj hash table.
            nsIdToNsDlmObj =
                nsIdToNsDlmObjectHashTable_.find(pathNameToNsId->nsId);
            if (nsIdToNsDlmObj == NULL) {
                assert(0 && "path name in Hash Table but not NsId");
                // On non-debug builds, syslog and bubble up an error
                xSyslog(moduleName,
                        XlogErr,
                        "removeMatchingLocal: '%s' in hash table but "
                        "NsId '%lu' was not found",
                        pathNameToNsId->getPathName(),
                        pathNameToNsId->nsId);
                status = StatusNsInternalTableError;
                goto CommonExit;
            }

            // Mark as pending removal which disallows some (see .h file)
            // operations on the object.
            dlmObj = (NsDlmObject *) nsIdToNsDlmObj->nsDlmObject;
            if (dlmObj->pendingRemove) {
#ifdef LIBNS_DEBUG
                xSyslog(moduleName,
                        XlogInfo,
                        "removeMatchingLocal: '%s' already marked for removal",
                        dlmObj->pathName);
#endif
                continue;
            }

            dlmObj->pendingRemove = true;
#ifdef LIBNS_DEBUG
            xSyslog(moduleName,
                    XlogInfo,
                    "removeMatchingLocal: marking %s for removal",
                    dlmObj->pathName);
#endif

            // Keep a list of the objects that are being marked for removal.
            // We can't do a refDec here as the ref count could go to zero.
            // This would lead to the hash table entries getting removed
            // which would mess up the iteration we're doing here.
            if (putObjList == NULL) {
                putObjList = dlmObj;
                dlmObj->next = NULL;
            } else {
                // Put it at the front of the list.
                NsDlmObject *tmpDlmObj = putObjList;
                putObjList = dlmObj;
                dlmObj->next = tmpDlmObj;
            }
        }
    }

    // If there were matching objects found process them here.
    while (putObjList != NULL) {
        dlmObj = putObjList;
        putObjList = putObjList->next;

        // Caution, the dlm object may be gone when the refDec returns.
        refDec(&dlmObj->refCount);
    }

    // Cannot access the removeMsg once we start writing the output
    // as it reuses the payload.  So save anything needed.

    // Package up the response payload.
    // No info currently returned.
    opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
    BailIfNull(opResponse);

    opResponse->nsObjectSize = 0;
    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse);

CommonExit:
    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

Status
LibNs::getPathInfoLocal(void *payload,
                        void **retOutput,
                        size_t *retOutputSize,
                        MsgEphemeral *ephIn)
{
    Status status = StatusOk;
    // Payload is used for IN and OUT
    NsGetPathInfoMsg *getPathInfoMsg = (NsGetPathInfoMsg *) payload;
    NsOpResponse *opResponse = NULL;
    LibNsTypes::PathInfoOut *pathInfoOut = NULL;

    NsDlmObject *dlmObj;
    NsDlmObject *matchingList = NULL;
    unsigned matchingCount = 0;
    PathNameToNsIdEntry *pathNameToNsId;
    char *pattern = getPathInfoMsg->header.pathName;
    NsIdToNsDlmObjectEntry *nsIdToNsDlmObj;

    *retOutput = NULL;
    *retOutputSize = 0;

    // Iterate through all the items in the name-to-nsid hash table.
    for (PathNameToNsIdHashTable::iterator it =
             pathNameToNsIdHashTable_.begin();
         (pathNameToNsId = it.get()) != NULL;
         it.next()) {
        if (strMatch(pattern, pathNameToNsId->getPathName())) {
            // Path name matches the pattern.  Get the entry from the
            // nsid-to-dlmobj hash table.
            nsIdToNsDlmObj =
                nsIdToNsDlmObjectHashTable_.find(pathNameToNsId->nsId);
            if (nsIdToNsDlmObj == NULL) {
                assert(0 && "path name in Hash Table but not NsId");
                // On non-debug builds, syslog and bubble up an error.
                xSyslog(moduleName,
                        XlogErr,
                        "getPathInfoLocal: '%s' in hash table but "
                        "NsId '%lu' was not found",
                        pathNameToNsId->getPathName(),
                        pathNameToNsId->nsId);
                status = StatusNsInternalTableError;
                goto CommonExit;
            }

            // Add the object to the matching list.  Once all the matching
            // path names are found we'll know how much memory to allocate
            // for the response.

            dlmObj = nsIdToNsDlmObj->nsDlmObject;
            if (matchingList == NULL) {
                matchingList = dlmObj;
                dlmObj->next = NULL;
            } else {
                // Put it at the front of the list
                NsDlmObject *tmpDlmObj = matchingList;
                matchingList = dlmObj;
                dlmObj->next = tmpDlmObj;
            }
            matchingCount++;
        }
    }

    if (matchingCount) {
        size_t infoSize = matchingCount * sizeof(LibNsTypes::PathInfoOut);
        unsigned ndx = 0;
        pathInfoOut =
            (LibNsTypes::PathInfoOut *) memAllocExt(infoSize, moduleName);
        if (pathInfoOut == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Unable to allocate %lu bytes to return pathInfo",
                    infoSize);
            goto CommonExit;
        }

        dlmObj = matchingList;
        while (dlmObj != NULL) {
            pathInfoOut[ndx].nsId = dlmObj->nsId;
            pathInfoOut[ndx].pendingRemoval = dlmObj->pendingRemove;
            pathInfoOut[ndx].millisecondsSinceCreation =
                dlmObj->startTimer.getCurElapsedMSecs();

            verifyOk(strStrlcpy((char *) &pathInfoOut[ndx].pathName,
                                dlmObj->pathName,
                                LibNsTypes::MaxPathNameLen));
            dlmObj = dlmObj->next;
            ndx++;
        }
        assert(ndx == matchingCount);

        status = sendViaStream(getPathInfoMsg->senderId,
                               ephIn,
                               pathInfoOut,
                               infoSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Unable to return path info to node %u: %s",
                    getPathInfoMsg->senderId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    opResponse = (NsOpResponse *) memAlloc(sizeof(NsOpResponse));
    BailIfNull(opResponse);

    opResponse->nsObjectSize = 0;
    *retOutput = opResponse;
    *retOutputSize = sizeof(NsOpResponse);

CommonExit:
    if (pathInfoOut != NULL) {
        memFree(pathInfoOut);
        pathInfoOut = NULL;
    }

    xcAssertIf((status != StatusOk), (*retOutputSize == 0));

    return status;
}

// Returns pointer to the underlying libNs object.
NsObject *
LibNs::getRefToObject(LibNsTypes::NsHandle nsHandle, Status *retStatus)
{
    Status status = StatusOk;
    NodeId dlmNode = XidMgr::get()->xidGetNodeId(nsHandle.nsId);
    NsObject *nsObj = NULL;
    Config *config = Config::get();
    bool lockHeld = false;
    NsDlmObject *dlmObj = NULL;
    OpenTableHTEntry *openTableHTEntry = NULL;
    NsHeader nsHeader;
    nsHeader.nsOp = NsGetInfo;
    nsHeader.nsId = nsHandle.nsId;
    nsHeader.viaId = true;

    if (dlmNode != config->getMyNodeId()) {
        // Needs to be node local
        status = StatusNsRefToObjectDenied;
        goto CommonExit;
    }

    if (nsHandle.openFlags != LibNsTypes::WriterExcl) {
        // Needs to have exclusive read and write access
        status = StatusNsRefToObjectDenied;
        goto CommonExit;
    }

    dlmLock_.lock();
    lockHeld = true;

    status = getDlmObj(&nsHeader, &dlmObj);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (nsHandle.openNsId != XidInvalid) {
        openTableHTEntry = dlmObj->openTable->find(nsHandle.openNsId);
        if (openTableHTEntry == NULL) {
            // No open associated with handle
            status = StatusNsStale;
            goto CommonExit;
        }
    }

    if (dlmObj->pendingRemove) {
        status = StatusPendingRemoval;
        goto CommonExit;
    }

    nsObj = (NsObject *) dlmObj->nsObject;

CommonExit:
    if (lockHeld) {
        dlmLock_.unlock();
        lockHeld = false;
    }

    *retStatus = status;
    return nsObj;
}
