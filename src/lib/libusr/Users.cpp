// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "usr/Users.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "dag/Dag.h"

using namespace xcalar::compute::localtypes::KvStore;
using namespace xcalar::compute::localtypes::Workbook;

UserMgr *UserMgr::instance = NULL;

Status
UserMgr::init()
{
    Status status = StatusOk;
    instance = (UserMgr *) memAllocExt(sizeof(UserMgr), ModuleName);
    if (instance == NULL) {
        return StatusNoMem;
    }

    new (instance) UserMgr();

    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(ModuleName,
                                        &instance->usrStatGroupId_,
                                        StatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.createdCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "created.count",
                                         instance->stats.createdCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.createdFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "created.failures",
                                         instance->stats.createdFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.deletedCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "deleted.count",
                                         instance->stats.deletedCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.deletedFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "deleted.failures",
                                         instance->stats.deletedFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.inactedCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "inacted.count",
                                         instance->stats.inactedCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.inactedFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "inacted.failures",
                                         instance->stats.inactedFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.actedCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "acted.count",
                                         instance->stats.actedCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.actedFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "acted.failures",
                                         instance->stats.actedFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.persistCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "persist.count",
                                         instance->stats.persistCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.persistFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "persist.failures",
                                         instance->stats.persistFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&instance->stats.renameCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "rename.count",
                                         instance->stats.renameCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&instance->stats.renameFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "rename.failures",
                                         instance->stats.renameFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.downloadCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "download.count",
                                         instance->stats.downloadCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.downloadFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "download.failures",
                                         instance->stats.downloadFailures,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.uploadCount);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "upload.count",
                                         instance->stats.uploadCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.uploadFailures);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->usrStatGroupId_,
                                         "upload.failures",
                                         instance->stats.uploadFailures,
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

void
UserMgr::shutdown()
{
    Status status;
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *usrPerSessInfo = NULL;
    SessionMgr::Session::ShutDown sdType;
    uint64_t inactiveSessionsDagCleanup = 0;
    uint64_t inactiveSessions = 0;
    uint64_t activeSessions = 0;
    uint64_t totalUsers = 0;
    uint64_t totalSessions = 0;
    char usrPath[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    bool objRemoved;

    usrHtLock_.lock();
    UsrsHt::iterator uit = usrsHt_.begin();
    while ((perUsrInfo = uit.get()) != NULL) {
        totalUsers++;
        perUsrInfo->sessionHtLock_.lock();
        SessionsHt::iterator sit = perUsrInfo->sessionsHt_.begin();
        while ((usrPerSessInfo = sit.get()) != NULL) {
            totalSessions++;
            usrPerSessInfo->session_->shutdownSessUtil(perUsrInfo->usrName_,
                                                       &sdType);
            if (sdType == SessionMgr::Session::ShutDown::Active) {
                activeSessions++;
            } else if (sdType == SessionMgr::Session::ShutDown::Inactive) {
                inactiveSessions++;
            } else {
                assert(sdType ==
                       SessionMgr::Session::ShutDown::InactiveWithDagClean);
                inactiveSessions++;
                inactiveSessionsDagCleanup++;
            }
            sit.next();
        }
        perUsrInfo->sessionHtLock_.unlock();
        // Remove from libNs.  Currently we only publish/remove users so
        // there's nothing to close.
        status = strSnprintf(usrPath,
                             LibNsTypes::MaxPathNameLen,
                             "%s%s",
                             PathPrefix,
                             perUsrInfo->usrName_);
        assert(status == StatusOk);
        status = libNs->remove(usrPath, &objRemoved);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to remove '%s' from name space: %s",
                    usrPath,
                    strGetFromStatus(status));
            // continue...
        } else {
            assert(objRemoved);
        }
        uit.next();
    }
    usrHtLock_.unlock();

    xSyslog(ModuleName,
            XlogInfo,
            "UserMgr shutdown: Total Users %lu, Total Sessions %lu,"
            " Sessions Active %lu, Inactive %lu,"
            " Inactive with resources %lu",
            totalUsers,
            totalSessions,
            activeSessions,
            inactiveSessions,
            inactiveSessionsDagCleanup);
}

void
UserMgr::destroy()
{
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *perSessInfo = NULL;
    uint64_t totalUsers = 0;
    uint64_t totalSessions = 0;

    usrHtLock_.lock();
    while ((perUsrInfo = usrsHt_.begin().get()) != NULL) {
        usrsHt_.remove(perUsrInfo->getName());
        totalUsers++;
        perUsrInfo->sessionHtLock_.lock();
        while ((perSessInfo = perUsrInfo->sessionsHt_.begin().get()) != NULL) {
            perUsrInfo->sessionsHt_.remove(perSessInfo->getName());
            totalSessions++;
            perSessInfo->session_->destroySessUtil();
            perSessInfo->session_->decRef();
            delete perSessInfo;
        }
        perUsrInfo->sessionHtLock_.unlock();
        perUsrInfo->decRef();
    }
    usrHtLock_.unlock();

    xSyslog(ModuleName,
            XlogDebug,
            "UserMgr destroy: Total Users %lu Sessions total %lu",
            totalUsers,
            totalSessions);

    if (SessionMgr::get() != NULL) {
        SessionMgr::get()->destroy();
    }

    this->~UserMgr();
    memFree(instance);
    instance = NULL;
}

UserMgr *
UserMgr::get()
{
    return instance;
}

Status
UserMgr::getOwnerNodeId(const char *userName, NodeId *nodeId)
{
    Status status = StatusOk;
    LibNsTypes::NsHandle nsHandle;
    UsrNsObject *usrNsObject = NULL;
    char usrPath[LibNsTypes::MaxPathNameLen] = {'\0'};

    status = strSnprintf(usrPath,
                         LibNsTypes::MaxPathNameLen,
                         "%s%s",
                         PathPrefix,
                         userName);
    if (status != StatusOk) {
        status = StatusInvalidUserNameLen;
        goto CommonExit;
    }

    nsHandle = LibNs::get()->open(usrPath,
                                  LibNsTypes::ReaderShared,
                                  (NsObject **) &usrNsObject,
                                  &status);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed User open '%s': %s",
                usrPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    *nodeId = usrNsObject->getNodeId();

    status = LibNs::get()->close(nsHandle, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed Session close: %s",
                strGetFromStatus(status));
        // caller cares about NodeId which we have, so dont propagate failure
        status = StatusOk;
    }

CommonExit:
    if (usrNsObject != NULL) {
        memFree(usrNsObject);
        usrNsObject = NULL;
    }

    return status;
}

Status
UserMgr::updateUsrOwnerNodeInfo(
    char *userName,
    NodeId ownerNodeId,
    XcalarApiSessionGenericOutput *sessionGenericOutput)
{
    Status status = StatusOk;
    size_t numBytesCopied = 0;

    sessionGenericOutput->nodeId = ownerNodeId;
    numBytesCopied = strlcpy(sessionGenericOutput->ipAddr,
                             Config::get()->getIpAddr(ownerNodeId),
                             sizeof(sessionGenericOutput->ipAddr));
    if (numBytesCopied < sizeof(sessionGenericOutput->ipAddr)) {
        sessionGenericOutput->outputAdded = true;
    } else {
        status = StatusIpAddrTooLong;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
UserMgr::getOwnerNodeIdAndFillIn(
    char *userName, XcalarApiSessionGenericOutput *sessionGenericOutput)
{
    Status status = StatusOk;
    NodeId nodeId;

    assert(userName != NULL);
    assert(sessionGenericOutput != NULL);

    status = getOwnerNodeId(userName, &nodeId);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = updateUsrOwnerNodeInfo(userName, nodeId, sessionGenericOutput);
CommonExit:
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failure to propagate node info to XD: %s",
                strGetFromStatus(status));
    }
    return status;
}
// this function will be invoked when we have encountered an error
// if this function also encounters an error, there is nothing more
// to do, but do not manipulate the status with this new error
void
UserMgr::allocListOutputAndFillInOwnerNodeInfo(XcalarApiUserId *user,
                                               NodeId nodeId,
                                               XcalarApiOutput **outputOut,
                                               size_t *outputSizeOut)
{
    Status status = StatusOk;
    XcalarApiOutput *output = NULL;
    size_t sessionOutputSize = 0;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    *outputSizeOut = 0;

    sessionOutputSize = XcalarApiSizeOfOutput(XcalarApiSessionListOutput);
    output = (XcalarApiOutput *) memAllocExt(sessionOutputSize, ModuleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    sessionGenericOutput =
        &(output->outputResult.sessionListOutput.sessionGenericOutput);
    memZero(sessionGenericOutput, sizeof(*sessionGenericOutput));

    status =
        updateUsrOwnerNodeInfo(user->userIdName, nodeId, sessionGenericOutput);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
        sessionOutputSize = 0;

        xSyslog(ModuleName,
                XlogErr,
                "Failure to propagate node info to XD: %s",
                strGetFromStatus(status));
    }

    *outputOut = output;
    *outputSizeOut = sessionOutputSize;
}

UserMgr::PerUsrInfo *
UserMgr::getUsrInfo(char *usrName,
                    size_t usrNameLen,
                    Status *retStatus,
                    NodeId *retUsrNodeId)
{
    return getOrAllocUsrInfo(usrName, usrNameLen, retStatus, retUsrNodeId, Get);
}

// This routine assumes perUsrInfo is non-NULL, the usrHtLock_ is held, and it
// removes the user from the usrHt_ (the caller must've ensured that the user
// didn't have any sessions - either because this is called during cleanout of a
// user allocation in getOrAllocUsrInfo(), when no sessions have yet been
// loaded, or because all sessions have been unloaded from the user in
// destroyUsrAndUnloadSessions()).
//
// This routine drops the refcount on perUsrInfo after removing the user from
// libNs.

void
UserMgr::destroyUser(PerUsrInfo *perUsrInfo, bool usrPublished)
{
    char usrPath[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    bool objRemoved;
    Status status;

    // must be called with usrHtLock_ held and a non-NULL perUsrInfo
    assert(perUsrInfo != NULL);
#ifndef XC6593_FIXED
    assert(usrHtLock_.tryLock() == false);  // XXX: doesn't guarantee I own it
#else
    assert(usrHtLock_.mine() == true);
#endif
    // this condition is associated with the publishCondVar_ below
    perUsrInfo->setUsrState(PerUsrInfo::UsrState::UnpublishedFromNs);

    PerUsrInfo *tmp = usrsHt_.remove(perUsrInfo->getName());
    assert(tmp == perUsrInfo);
    // Poke everyone who may be waiting for a user publish to finish
    // successfully or to fail (since this routine may be called during cleanout
    // of a user publish).
    perUsrInfo->publishCondVar_.broadcast();
    if (usrPublished == true) {
        // Remove from libNs.  Currently we only publish/remove users so
        // there's nothing to close.
        status = strSnprintf(usrPath,
                             LibNsTypes::MaxPathNameLen,
                             "%s%s",
                             PathPrefix,
                             perUsrInfo->usrName_);
        assert(status == StatusOk);
        status = libNs->remove(usrPath, &objRemoved);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to remove '%s' from name space: %s",
                    usrPath,
                    strGetFromStatus(status));
            // Not considered a hard error, so don't return failed status
        } else {
            assert(objRemoved);
        }
    }
    perUsrInfo->decRef();
    perUsrInfo = NULL;
}

UserMgr::PerUsrInfo *
UserMgr::getOrAllocUsrInfo(char *usrName,
                           size_t usrNameLen,
                           Status *retStatus,
                           NodeId *retUsrNodeId,
                           GetMode getMode)
{
    Status status = StatusOk;
    PerUsrInfo *perUsrInfo = NULL;
    size_t usrIdSize = 0;
    char usrPath[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    NodeId nodeId = NodeIdInvalid;
    bool usrHtLockHeld = false;
    LibNsTypes::NsHandle nsHandle;
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    bool pokePublishCondVar = false;
    bool usrPublished = false;

    usrIdSize = strnlen(usrName, usrNameLen);
    if (usrIdSize < 1 || usrIdSize >= usrNameLen) {
        status = StatusInvalidUserNameLen;
        xSyslog(ModuleName,
                XlogErr,
                "Get or Alloc user '%s' failed: %s",
                usrName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    usrHtLockHeld = true;
    usrHtLock_.lock();

    perUsrInfo = usrsHt_.find(usrName);
    if (perUsrInfo == NULL) {
        // user not found in HT
        if (getMode == Get) {
            // In get mode, if usrInfo not found in HT, check libNs -
            // maybe user has been added on some other node. First, drop the HT
            // lock since a libNs lookup involves a 2pc.
            usrHtLock_.unlock();
            usrHtLockHeld = false;

            status = getOwnerNodeId(usrName, &nodeId);
            if (status != StatusOk) {
                if (status == StatusNsInvalidObjName) {
                    status = StatusSessionUsrNameInvalid;
                } else if (status == StatusAccess) {
                    status = StatusSessionUsrInUse;
                } else if (status == StatusPendingRemoval) {
                    status = StatusSessionUsrAlreadyDeleted;
                } else if (status == StatusNsNotFound) {
                    status = StatusSessionUsrNotExist;
                }
                xSyslog(ModuleName,
                        XlogErr,
                        "Get user '%s' failed: %s",
                        usrName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            // Alloc mode
            Status status2;
            UsrNsObject curUsrNsObject(sizeof(UsrNsObject),
                                       Config::get()->getMyNodeId());

            // There's no entry for this user, and the mode is Alloc.
            // So, start tracking the new user by allocating and registering the
            // new user in the HT.
            perUsrInfo = new (std::nothrow) PerUsrInfo(usrName);
            if (perUsrInfo == NULL) {
                status = StatusNoMem;
                xSyslog(ModuleName,
                        XlogErr,
                        "Get or Alloc user '%s' failed: %s",
                        usrName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            status = usrsHt_.insert(perUsrInfo);
            verify(status == StatusOk);  // this should not fail
            perUsrInfo
                ->incRef();  // Grab a ref before dropping usrHtLock_ lock.
            pokePublishCondVar = true;  // inserted in HT, so set poke CV
            // Going to do NS lookups which could involve 2PC to remove node. So
            // have to give up the Spinlock.
            usrHtLock_.unlock();
            usrHtLockHeld = false;

            status = strSnprintf(usrPath,
                                 LibNsTypes::MaxPathNameLen,
                                 "%s%s",
                                 PathPrefix,
                                 usrName);
            if (status != StatusOk) {
                status = StatusInvalidUserNameLen;
                xSyslog(ModuleName,
                        XlogErr,
                        "Get or Alloc user '%s' failed: %s",
                        usrName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            // Attempt to publish the user ownership to the local Node. Note
            // that this may fail due to a variety of reasons like:
            // * User may have logged in on another node in the cluster.
            // * Other 2PC related failures. etc.
            nsId = libNs->publish(usrPath, &curUsrNsObject, &status);
            if (status != StatusOk) {
                if (status == StatusNsInvalidObjName) {
                    status = StatusSessionUsrNameInvalid;
                } else if (status == StatusPendingRemoval ||
                           status == StatusExist) {
                    status = StatusSessionUsrAlreadyExists;
                    status2 = getOwnerNodeId(usrName, &nodeId);
                    if (status2 != StatusOk) {
                        xSyslog(ModuleName,
                                XlogErr,
                                "Get or Alloc user '%s' failed: %s",
                                usrName,
                                strGetFromStatus(status2));
                        //
                        // isn't a fatal error; log it but keep going to report
                        // StatusSessionUsrAlreadyExists, though without the
                        // owner nodeId
                    }
                }
                xSyslog(ModuleName,
                        XlogErr,
                        "Get or Alloc user '%s' failed: %s",
                        usrName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            usrPublished = true;
            nodeId = curUsrNsObject.getNodeId();
        }
    } else {
        // user found in HT
        if (perUsrInfo->getUsrState() == PerUsrInfo::UsrState::PublishedToNs) {
            // and the user has been published globally too; if so, return with
            // the perUsrInfo after bumping refcount, and get current nodeId
            // since the user HT is located on the node to which the user is
            // bound - and since the user's found in *this* node's HT, return
            // *this* node. In DEBUG, maybe we should add an assert that a libNs
            // lookup must return the same nodeId here.
            perUsrInfo->incRef();
            nodeId = Config::get()->getMyNodeId();
        } else {
            // user in HT, but state isn't published yet
            if (getMode == Get) {
                // fast path get mode. If the usr entry in HT hasn't been
                // published yet, get out: no point in waiting on the CV for pub
                // to finish.
                usrHtLock_.unlock();  // release lock ASAP since Get is hot path
                usrHtLockHeld = false;
                perUsrInfo = NULL;  // not returning a UsrInfo
                status = StatusUsrAddInProg;
                goto CommonExit;
            }
            //
            // In alloc mode, user found in HT, but state isn't published yet.
            // So, wait until state leaves Initializing state.
            //
            perUsrInfo
                ->incRef();  // Grab a ref before dropping usrHtLock_ lock.

            // Note that the global usrHtLock_ is being used here to
            // synchronize around PerUsrInfo state such as waiting for an
            // in-progress init, and similarly when the initer is done, it
            // grabs the usrHtLock_ to set the state to published/failed, etc.
            // If the global lock becomes hot, in case of many users adding,
            // deleting sessions, the locking can be made to be per-PerUsrInfo,
            // by using a lock embedded inside PerUsrInfo. This will complicate
            // the locking though, between usrHtLock_ and the lock embedded
            // inside PerUsrInfo. The ref on the perUsrInfo is key. It'll be
            // acquired as above. Following other steps will be needed:
            //
            //     1. Drop the global HT lock here, and then get the perUsrInfo
            //        lock before checking its state.  Of course, the
            //        publishCondVar_ would be protected with this lock,
            //        instead of the global usrHtLock_.
            //     2. In case state indicates failure, the global lock must be
            //        acquired across the assert's call to usrsHt_.find() below.
            //        Note this must be done with the perUsrInfo lock held so
            //        that the check on the state and the HT find is done
            //        atomically.
            //     3. The init'er, responsible for issuing the broadcast, must
            //        first get the perUsrInfo lock, and if the state is being
            //        set to failure, it must first acquire the global HT lock
            //        before setting the state to failure and removing the
            //        perUsrInfo from the HT - to honor the assert in item 2
            //        above.
            //
            //  Needless to say, this is complex, with nested locking, etc.,
            //  and so this shouldn't be done unless there's a reason (such as
            //  the global HT lock becoming a hotspot).

            while (perUsrInfo->getUsrState() ==
                   PerUsrInfo::UsrState::Initializing) {
                // So someone is already in the middle of Publishing. Just wait
                // for publish to finish.
                perUsrInfo->publishCondVar_.wait(&usrHtLock_);
            }
            if (perUsrInfo->getUsrState() ==
                PerUsrInfo::UsrState::PublishedToNs) {
                nodeId = Config::get()->getMyNodeId();
            } else {
                assert(perUsrInfo->getUsrState() ==
                       PerUsrInfo::UsrState::UnpublishedFromNs);
                // perUsrInfo should not be tracked anymore in the UsrsHt
                assert(usrsHt_.find(usrName) == NULL);
                // Don't touch perUsrInfo after dropping ref
                perUsrInfo->decRef();
                perUsrInfo = NULL;
                status = StatusSessionUsrNotExist;
                xSyslog(ModuleName,
                        XlogErr,
                        "Get user '%s' failed: %s",
                        usrName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
    }
    assert(status == StatusOk);
CommonExit:
    if (getMode == GetAlloc) {
        if (pokePublishCondVar == true) {
            assert(usrHtLockHeld == false);
            assert(perUsrInfo->getUsrState() ==
                   PerUsrInfo::UsrState::Initializing);
            assert(perUsrInfo != NULL);

            // Need to grab lock which wouldn't be held here to ensure a
            // potential waiter can't miss this event update and broadcast. Note
            // that the broadcast itself doesn't need to be issued under the
            // lock - this is a little better since a woken waiter will
            // otherwise sleep immediately for the lock held by the broadcaster.
            // The main change which needs to be atomic with the waiter's state
            // check, is the change in state below, and hence at least this must
            // be done with the lock held.

            usrHtLock_.lock();
            if (status == StatusOk) {
                assert(usrPublished == true);
                perUsrInfo->setUsrState(PerUsrInfo::UsrState::PublishedToNs);
                // Poke everyone who is waiting for the publish to finish
                // successfully or to fail.
                perUsrInfo->publishCondVar_.broadcast();
            } else {
                // all failure processing during Alloc is here
                UserMgr::destroyUser(perUsrInfo, usrPublished);
                perUsrInfo = NULL;
            }
            usrHtLock_.unlock();
        }
        // in case of failure, perUsrInfo better have been cleared by now
        assert(status == StatusOk || perUsrInfo == NULL);
    }
    if (usrHtLockHeld == true) {
        usrHtLock_.unlock();
    }
    *retStatus = status;
    *retUsrNodeId = nodeId;
    // If perUsrInfo is valid, the caller needs to decRef() it after using.
    return perUsrInfo;
}

// Abort reading of sessions by freeing up the lists and sessions which have
// been allocated so far while building out the usrPerSessList. Once the
// usrPerSessList has been fully built (i.e. contains all loaded sessions), no
// more memory allocations will occur before entering each usrPerSessInfo from
// the usrPerSessList into the PerUsrInfo's sessionsHt_, and the latter can't
// fail - so this abort routine is sufficient to cleanup a failure during
// sessions load.

void
UserMgr::abortLoadSessions(SessionListElt *sessList,
                           UsrPerSessInfoListElt *usrPerSessList,
                           PerUsrInfo *perUsrInfo)
{
    SessionListElt *sessListPrev;
    UsrPerSessInfoListElt *usrPerSessListPrev;

    while (sessList != NULL) {
        sessList->session->decRef();
        sessList->session = NULL;
        sessListPrev = sessList;
        sessList = sessList->next;
        delete sessListPrev;
    }
    while (usrPerSessList != NULL) {
        usrPerSessList->usrPerSessInfo->session_->decRef();
        usrPerSessList->usrPerSessInfo->session_ = NULL;
        delete usrPerSessList->usrPerSessInfo;
        usrPerSessList->usrPerSessInfo = NULL;
        usrPerSessListPrev = usrPerSessList;
        usrPerSessList = usrPerSessList->next;
        delete usrPerSessListPrev;
    }
    perUsrInfo->sessionHtLock_.lock();
    perUsrInfo->setSessState(PerUsrInfo::SessState::Failed);
    perUsrInfo->usrSessLoadCondVar_.broadcast();
    perUsrInfo->sessionHtLock_.unlock();
}

Status
UserMgr::openWorkBookKvStore(UsrPerSessInfoListElt *usrPerSessList)
{
    Status status = StatusOk;
    UsrPerSessInfoListElt *uPi = usrPerSessList;

    while (uPi != NULL) {
        status = uPi->usrPerSessInfo->session_->sessionKvsCreateOrOpen();
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "openWorkBookKvStore failed kvs open for Session '%s': %s",
                    uPi->usrPerSessInfo->getName(),
                    strGetFromStatus(status));
            break;
        }
        uPi = uPi->next;
    }
    return status;
}

// This routine finds the user in the node's user hash table, unloads all its
// sessions, and then destroys the user (which removes it from the user hash
// table and drops the ref on the PerUsrInfo object instance).
Status
UserMgr::destroyUsrAndUnloadSessions(XcalarApiUserId *user)
{
    Status status;
    bool usrHtLockHeld = false;
    bool perUsrLockHeld = false;
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *perSessInfo = NULL;
    SessionListElt sessList;

    usrHtLockHeld = true;
    usrHtLock_.lock();
    // Don't remove the user from the hash table in case we're not able to
    // clean up all the sessions to avoid dangling sessions.
    perUsrInfo = usrsHt_.find(user->userIdName);
    if (perUsrInfo == NULL) {
        // User no longer on this node
        status = StatusSessionUsrNotExist;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to find '%s' in users hash table",
                user->userIdName);
        goto CommonExit;
    }

    perUsrLockHeld = true;
    perUsrInfo->sessionHtLock_.lock();
    while ((perSessInfo = perUsrInfo->sessionsHt_.begin().get()) != NULL) {
        // Inactivate the session before removal if the session is active
        // or it's inactive but needs to be persisted.
        if (perSessInfo->session_->isActive() ||
            !perSessInfo->session_->isPersisted()) {
            sessList.session = perSessInfo->session_;
            sessList.next = NULL;
            status = SessionMgr::Session::inactivate(user, &sessList, 1);
            if (status != StatusOk) {
                assert(0 && "Failed to inactivate session");
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to inactivate session '%s' for Usr '%s': %s",
                        perSessInfo->getName(),
                        user->userIdName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
        perUsrInfo->sessionsHt_.remove(perSessInfo->getName());
        perSessInfo->session_->destroySessUtil();
        perSessInfo->session_->decRef();
        delete perSessInfo;
    }
    perUsrInfo->sessionHtLock_.unlock();
    perUsrLockHeld = false;

    UserMgr::destroyUser(perUsrInfo, true);

CommonExit:
    if (perUsrLockHeld) {
        perUsrInfo->sessionHtLock_.unlock();
        perUsrLockHeld = false;
    }
    if (usrHtLockHeld) {
        usrHtLock_.unlock();
        usrHtLockHeld = false;
    }

    return status;
}

Status
UserMgr::validateUsrAndLoadSessions(XcalarApiUserId *user,
                                    PerUsrInfo **perUsrInfoRet,
                                    NodeId *ownerNodeId)
{
    Status status;
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *usrPerSessInfo = NULL;
    UsrPerSessInfoListElt *usrPerSessListCurr = NULL;
    UsrPerSessInfoListElt *usrPerSessList = NULL;
    SessionListElt *sessList = NULL;
    SessionListElt *sessListPrev = NULL;
    bool lockHeld = true;
    bool sessListIncomp = false;

    assert(ownerNodeId != NULL);
    assert(user != NULL);
    assert(perUsrInfoRet != NULL);

    perUsrInfo = getOrAllocUsrInfo(user->userIdName,
                                   sizeof(user->userIdName),
                                   &status,
                                   ownerNodeId,
                                   GetAlloc);

    // If call succeeds, perUsrInfo must be non-NULL, and ownerNodeId valid
    assert(
        status != StatusOk ||
        (perUsrInfo != NULL && *ownerNodeId == Config::get()->getMyNodeId()));
    if (status == StatusSessionUsrAlreadyExists) {
        assert(*ownerNodeId != Config::get()->getMyNodeId());
        xSyslog(ModuleName,
                XlogErr,
                "validateUsrAndLoadSessions fails: user exists on node %d; "
                "this is node %d",
                *ownerNodeId,
                Config::get()->getMyNodeId());
        goto CommonExit;
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "validateUsrAndLoadSessions fails getOrAllocUsrInfo Usr '%s': "
                "%s",
                user->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(*ownerNodeId == Config::get()->getMyNodeId());
    assert(perUsrInfo != NULL);
    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();
    if (perUsrInfo->getSessState() != PerUsrInfo::SessState::Loaded &&
        perUsrInfo->getSessState() != PerUsrInfo::SessState::LoadedPartial) {
        if (perUsrInfo->getSessState() == PerUsrInfo::SessState::Init) {
            // unlock across disk I/O, setting state for synchronization
            perUsrInfo->setSessState(PerUsrInfo::SessState::Loading);
            perUsrInfo->sessionHtLock_.unlock();
            lockHeld = false;

            status = SessionMgr::Session::readSessions(perUsrInfo->usrName_,
                                                       &sessList);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "validateUsrAndLoadSessions fails in "
                        "readSessionsProtobuf, Usr '%s': %s",
                        user->userIdName,
                        strGetFromStatus(status));
                if (status == StatusSessListIncomplete) {
                    sessListIncomp = true;
                    status = StatusOk;
                } else {
                    goto CommonExit;
                }
            }
            // for each session in list, allocate a UsrPerSessInfo, init it with
            // the session, and add it to a local list, to avoid doing
            // allocs of UsrPerSessInfo under the lock.
            //
            // Then iterate through the UsrPerSessInfo list to add each item to
            // the hash table, under the lock.
            //
            // If a failure occurs while building the UsrPerSessInfo list, all
            // memory allocated so far must be freed before returning a failure.
            // This is done via abortLoadSessions().
            while (sessList != NULL) {
                usrPerSessInfo = new (std::nothrow) UsrPerSessInfo();
                if (usrPerSessInfo == NULL) {
                    status = StatusNoMem;
                    break;
                }
                usrPerSessInfo->session_ = sessList->session;
                usrPerSessListCurr = new (std::nothrow) UsrPerSessInfoListElt();
                if (usrPerSessListCurr == NULL) {
                    usrPerSessInfo->session_ = NULL;
                    delete usrPerSessInfo;
                    status = StatusNoMem;
                    break;
                }
                usrPerSessListCurr->usrPerSessInfo = usrPerSessInfo;
                usrPerSessListCurr->next = usrPerSessList;
                usrPerSessList = usrPerSessListCurr;
                sessListPrev = sessList;
                sessList = sessList->next;
                delete sessListPrev;
            }

            if (status != StatusOk) {
                // building of usrPerSessList of usrPerSessInfo's failed
                abortLoadSessions(sessList, usrPerSessList, perUsrInfo);
                xSyslog(ModuleName,
                        XlogErr,
                        "validateUsrAndLoadSessions failed- Usr '%s': %s",
                        user->userIdName,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            assert(sessList == NULL);  // now all are on usrPerSessList

            status = UserMgr::openWorkBookKvStore(usrPerSessList);

            if (status != StatusOk) {
                // Attempt to open KVS for a workbook in usrPerSessList failed
                abortLoadSessions(sessList, usrPerSessList, perUsrInfo);
                xSyslog(ModuleName,
                        XlogErr,
                        "validateUsrAndLoadSessions failed kvs open Usr '%s': "
                        "%s",
                        user->userIdName,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            lockHeld = true;
            perUsrInfo->sessionHtLock_.lock();
            while (usrPerSessList != NULL) {
                status = perUsrInfo->sessionsHt_.insert(
                    usrPerSessList->usrPerSessInfo);
                //
                // once the session is inserted into the user's sessionsHt_
                // table above, the session is considered fully created so
                // mark it as such, and assert that its op state is already
                // idle.
                //
                if (status == StatusOk) {
                    usrPerSessList->usrPerSessInfo->session_->setCrState(
                        SessionMgr::Session::CrState::Created);
                } else {
                    xSyslog(ModuleName,
                            XlogErr,
                            "validateUsrAndLoadSessions- Usr '%s' failed to "
                            "insert '%s': %s",
                            user->userIdName,
                            usrPerSessList->usrPerSessInfo->session_->getName(),
                            strGetFromStatus(status));
                    usrPerSessList->usrPerSessInfo->session_->setCrState(
                        SessionMgr::Session::CrState::Failed);
                    usrPerSessList->usrPerSessInfo->session_->setCrFailedStatus(
                        status);
                    sessListIncomp = true;
                }
                assert(usrPerSessList->usrPerSessInfo->session_->getOpState() ==
                       SessionMgr::Session::OpState::Idle);
                usrPerSessListCurr = usrPerSessList;
                usrPerSessList = usrPerSessList->next;
                delete usrPerSessListCurr;
            }
            if (sessListIncomp == true) {
                perUsrInfo->setSessState(PerUsrInfo::SessState::LoadedPartial);
            } else {
                perUsrInfo->setSessState(PerUsrInfo::SessState::Loaded);
            }
            perUsrInfo->usrSessLoadCondVar_.broadcast();
        }
        while (perUsrInfo->getSessState() == PerUsrInfo::SessState::Loading) {
            perUsrInfo->usrSessLoadCondVar_.wait(&perUsrInfo->sessionHtLock_);
        }
        if (perUsrInfo->getSessState() == PerUsrInfo::SessState::Failed) {
            lockHeld = false;
            perUsrInfo->sessionHtLock_.unlock();
            status = StatusUsrSessLoadFailed;
            xSyslog(ModuleName,
                    XlogErr,
                    "validateUsrAndLoadSessions- loader fails- Usr '%s': %s",
                    user->userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        assert(perUsrInfo->getSessState() == PerUsrInfo::SessState::Loaded ||
               perUsrInfo->getSessState() ==
                   PerUsrInfo::SessState::LoadedPartial);
    }
    if (perUsrInfo->getSessState() == PerUsrInfo::SessState::LoadedPartial) {
        // readSessionsProtobuf() is done only during Init; on subsequent
        // invocations of this routine, the sessListIncomp must be set to true
        // based on state
        xSyslog(ModuleName,
                XlogErr,
                "validateUsrAndLoadSessions failed for Usr '%s': %s",
                user->userIdName,
                strGetFromStatus(StatusSessListIncomplete));
        sessListIncomp = true;
    }
CommonExit:
    if (status == StatusOk) {
        assert(perUsrInfo != NULL);
        *perUsrInfoRet = perUsrInfo;
        // perUsrInfo ref-count was incremented in getOrAllocUsrInfo()
    }
    if (perUsrInfo != NULL && lockHeld == true) {
        perUsrInfo->sessionHtLock_.unlock();
    }
    if (status != StatusOk && perUsrInfo != NULL) {
        Status userDestroyStatus;

        *perUsrInfoRet = NULL;
        perUsrInfo->decRef();  // caller will not decRef now, so do it here
        perUsrInfo = NULL;
        userDestroyStatus = destroyUsrAndUnloadSessions(user);
        if (userDestroyStatus != StatusOk) {
            assert(0 && "Failed to destroy user");
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to destroy user '%s': %s",
                    user->userIdName,
                    strGetFromStatus(userDestroyStatus));
        }
    }
    if (status == StatusOk && sessListIncomp) {
        return StatusSessListIncomplete;
    } else {
        return status;
    }
}

// Common routine between ::create and ::upload APIs. The code in this routine
// is absolutely identical for the two callers (except that ::upload supplies
// a non-NULL sessionUploadPayload which is passed down to
// SessionMgr::Session::create() which does the heavy lifting).
//
Status
UserMgr::createInternal(XcalarApiUserId *user,
                        XcalarApiSessionNewInput *sessionInput,
                        SessionUploadPayload *sessionUploadPayload,
                        XcalarApiSessionNewOutput *sessionOutput)
{
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *perSessInfo = NULL;
    UsrPerSessInfo *forkedSession = NULL;
    SessionMgr::Session *thisSessInfo = NULL;
    NodeId ownerNodeId = 0;
    Status status;
    Status status2;
    bool lockHeld = false;

    // Init Logic
    // ----------
    // 0. Call getOrAllocUsrInfo() to get pointer to PerUsrInfo class
    //    with the getMode param set to GetAlloc (i.e. alloc class if absent)
    // 1. perUsrInfo is returned with a ref on it
    // 2. check if the Usr's node affinity matches caller's node
    //     - if not, fail
    // 3. does perUsrInfo's state indicate sessions are loaded now?
    //    if not, call readSessions()
    //     -- readSessions() will return all sessions in a list
    //         -- it calls readSession() to do the actual de-serialization
    //         -- readSession() will call deserialize as normal
    //
    // Above logic is common for ::create and ::list so a common routine
    // UserMgr::validateUsrAndLoadSessions() is used for both.
    //
    // Creation logic
    // --------------
    // Search all existing sessions in sessionHt_, to check if a session with
    // same name already exists, OR if fork is requested, does the source of
    // fork exist? If there's a conflict (session already exists, or there's
    // no fork source but fork's requested), fail.
    //
    // Now, add an entry for the new session name to sessionsHt_ with state
    // Creating. This is done by the operation (say A), so
    // that a concurrent operation (say, B), to create a session with the same
    // name for this user, would fail in the check for a name conflict above.
    // The failure path can report the race by checking if the conflicting
    // session's Creating state is true or false, and emit an appropriate
    // message indicating the bug in the application (of attempting to create
    // duplicate sessions concurrently if Creating is true when the conflict
    // in name is detected above).
    //
    // Note that an alternative is to add one more state and for B to wait for A
    // to either be fully created, or to fail - and then proceed accordingly-
    // return failure if A is successfully created, or to continue its creation
    // attempt if A fails to be created. Note that this doesn't seem worth doing
    // to support a bug in the caller (of attempting to create dup sessions).
    // However, at least the Creating state is needed for operations like
    // ::list, for reporting purposes - a session in the sessionsHt_ may not be
    // a fully created session, and that's important to know.
    //
    // Alloc a new session, and then call SessionMgr::Session::create() to init
    // the new session:
    //     - create a new dag or fork from an existing dag
    //     - create kvstore, set state
    //     - invoke writeSession()
    //     - new session will be inactive on creation
    //     - close out the log handles (opened for write) since it'd be inactive
    //
    // Now, get the sessionsHt_ lock, and if the create succeeded, mark the
    // session as created. If the create fails, remove the session from the
    // sessionsHt_, and delete/free the PerSessionInfo/SessionMgr::Session
    // instances.
    //
    // Finally, drop the ref to perUsrInfo in case of a failure return.

    status = validateUsrAndLoadSessions(user, &perUsrInfo, &ownerNodeId);
    if (status != StatusOk) {
        strlcpy(sessionOutput->error,
                strGetFromStatus(status),
                sizeof(sessionOutput->error));
        xSyslog(ModuleName,
                XlogErr,
                "Session create fails in validateUsrAndLoadSessions() - Usr "
                "'%s', session name '%s': %s",
                user->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(status));
        // For create, ownerNodeId needed to be updated only if failure status
        // is StatusSessionUsrAlreadyExists
        // Possible for ownerNodeId to be Invalid when getOwnerNodeId fails
        // in validateUsrAndLoadSessions
        if (status == StatusSessionUsrAlreadyExists &&
            ownerNodeId != NodeIdInvalid) {
            status2 =
                updateUsrOwnerNodeInfo(user->userIdName,
                                       ownerNodeId,
                                       &sessionOutput->sessionGenericOutput);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Session create failure; updateUsrOwnerNodeInfo failed"
                        " to update output: %s",
                        strGetFromStatus(status2));
            }
        }
        goto CommonExit;
    }

    // sessions (if there are any), have now been loaded

    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();
    // Do Validation
    // - Fail if new session name already exists
    perSessInfo = perUsrInfo->sessionsHt_.find(sessionInput->sessionName);
    if (perSessInfo != NULL) {
        status = StatusSessionExists;
        strlcpy(sessionOutput->error,
                strGetFromStatus(StatusSessionExists),
                sizeof(sessionOutput->error));
        xSyslog(ModuleName,
                XlogErr,
                "Session create fails - Usr '%s', session name '%s': %s",
                user->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // - Fail if fork's requested, but session to be forked doesn't exist
    if (sessionInput->fork) {
        // TODO: CLI semantics require that even if fork's true,
        // forkedSessionNameLength could be 0 - i.e. a request to fork, without
        // a session name to be forked. XD doesn't need this semantic. The
        // session to be forked is the ONE active session.
        //
        // Fix the CLI (e.g. SessionCli.cpp) and tests which use this semantic,
        // and then enforce that forkedSessionName must be supplied here.
        forkedSession = findSess(perUsrInfo,
                                 sessionInput->forkedSessionNameLength
                                     ? sessionInput->forkedSessionName
                                     : NULL);
        if (forkedSession == NULL) {
            status = StatusSessionNotFound;
            strlcpy(sessionOutput->error,
                    strGetFromStatus(StatusSessionNotFound),
                    sizeof(sessionOutput->error));
            xSyslog(ModuleName,
                    XlogErr,
                    "Session fork fails - Usr '%s', new session '%s'"
                    ", forked session '%s': status %s ",
                    user->userIdName,
                    sessionInput->sessionName,
                    sessionInput->forkedSessionName,
                    strGetFromStatus(StatusSessionNotFound));
            goto CommonExit;
        }
    }

    thisSessInfo =
        new (std::nothrow) SessionMgr::Session(sessionInput->sessionName);

    if (thisSessInfo == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Session create fails - Usr '%s', session '%s': %s",
                user->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(StatusSessionNotFound));
        goto CommonExit;
    }
    perSessInfo = new (std::nothrow) UserMgr::UsrPerSessInfo();
    if (perSessInfo == NULL) {
        thisSessInfo->decRef();
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Session create fails - Usr '%s', session '%s': %s",
                user->userIdName,
                sessionInput->sessionName,
                strGetFromStatus(StatusSessionNotFound));
        goto CommonExit;
    }
    perSessInfo->session_ = thisSessInfo;
    status = perUsrInfo->sessionsHt_.insert(perSessInfo);
    assert(status == StatusOk);
    lockHeld = false;
    perUsrInfo->sessionHtLock_.unlock();

    //
    // all state needed to create the new session now ready
    //
    status =
        thisSessInfo->create(user,
                             sessionInput,
                             sessionUploadPayload,
                             sessionOutput,
                             forkedSession ? forkedSession->session_ : NULL);
    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();
    if (status == StatusOk) {
        assert(thisSessInfo != NULL);
        // although the SessionMgr::Session state is in the session itself, the
        // locking must be in UsrMgr under the sessionHtLock_ since the APIs
        // land through UsrMgr, and may need to read/write this state while
        // holding the user's sessionHtLock_
        thisSessInfo->setCrState(SessionMgr::Session::CrState::Created);
        thisSessInfo->setOpState(SessionMgr::Session::OpState::Idle);
    } else {
        UsrPerSessInfo *removedPerSI;
        removedPerSI = perUsrInfo->sessionsHt_.remove(perSessInfo->getName());
        assert(removedPerSI == perSessInfo);
        perSessInfo->session_ = NULL;
        thisSessInfo->decRef();
        delete perSessInfo;
    }
    // broadcast for anyone waiting for session to finish (or fail) creation
    perUsrInfo->usrSessBusyCondVar_.broadcast();
    lockHeld = false;
    perUsrInfo->sessionHtLock_.unlock();

CommonExit:
    if (perUsrInfo != NULL && lockHeld == true) {
        perUsrInfo->sessionHtLock_.unlock();
    }
    if (status != StatusOk) {
        StatsLib::statAtomicIncr64(stats.createdFailures);
    } else {
        StatsLib::statAtomicIncr64(stats.createdCount);
    }
    if (perUsrInfo != NULL) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// This routine essentially creates a session. It also creates user metadata
// when the first session for a user is created. Note that a session is born
// in the Inactive state. It must be explicitly activated before being used.
// It uses a common routine, shared with the ::upload API. It supplies NULL
// as the upload payload of course.
Status
UserMgr::create(XcalarApiUserId *user,
                XcalarApiSessionNewInput *sessionInput,
                XcalarApiSessionNewOutput *sessionOutput)
{
    Status status;

    status = createInternal(user, sessionInput, NULL, sessionOutput);
    return status;
}

Status
UserMgr::list(XcalarApiUserId *user,
              const char *sessionListPattern,
              XcalarApiOutput **outputOut,
              size_t *sessionListOutputSize)
{
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *perSessInfo = NULL;
    NodeId ownerNodeId;
    int sessCount = 0;
    SessionListElt *sessList = NULL;
    SessionListElt *sessListCurr = NULL;
    size_t sessionOutputSize;
    XcalarApiOutput *output = NULL;
    XcalarApiSessionListOutput *sessionListOutput;
    Status status;
    bool sessListIncomp = false;

    assert(user != NULL);
    assert(sessionListOutputSize != NULL);

    status = validateUsrAndLoadSessions(user, &perUsrInfo, &ownerNodeId);
    if (status == StatusSessListIncomplete) {
        sessListIncomp = true;
        status = StatusOk;
    }
    if (status != StatusOk) {
        // some failure other than StatusSessListIncomplete
        xSyslog(ModuleName,
                XlogErr,
                "Session list fails in validateUsrAndLoadSessions() -"
                " Usr '%s': %s",
                user->userIdName,
                strGetFromStatus(status));
        if (status == StatusSessionUsrAlreadyExists) {
            allocListOutputAndFillInOwnerNodeInfo(user,
                                                  ownerNodeId,
                                                  outputOut,
                                                  sessionListOutputSize);
            if (sessionListOutputSize != 0) {
                output = *outputOut;
                XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

                sessionGenericOutput = &(output->outputResult.sessionListOutput
                                             .sessionGenericOutput);
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Error: %s. The user '%s' has affinity to "
                              "nodeId %d, with IP address '%s'",
                              strGetFromStatus(status),
                              user->userIdName,
                              sessionGenericOutput->nodeId,
                              sessionGenericOutput->ipAddr);
            }
        }
        if (perUsrInfo) {
            perUsrInfo->decRef();
            perUsrInfo = NULL;
        }
        return status;
    }

    // sessions (if there are any), have now been loaded

    // Now, iterate through the sessionsHt_ to create the sessions list
    perUsrInfo->sessionHtLock_.lock();
    SessionsHt::iterator it = perUsrInfo->sessionsHt_.begin();

    while ((perSessInfo = it.get()) != NULL) {
        sessListCurr = new (std::nothrow) SessionListElt();
        if (sessListCurr == NULL) {
            // keep going to other sessions even if memory allocation failed
            // here so as many sessions as possible can be listed, but log
            // the failure and report the session for which the new failed
            xSyslog(ModuleName,
                    XlogErr,
                    "SessionMgr::Session::list Usr '%s' with filter '%s' "
                    "failed to alloc memory for session '%s'",
                    user->userIdName,
                    sessionListPattern,
                    perSessInfo->session_->getName());
        } else {
            sessListCurr->session = perSessInfo->session_;
            sessListCurr->next = sessList;
            sessList = sessListCurr;
            // SessionMgr::Session::list will be supplied this list of
            // sessions, after dropping the sessionsHtLock_ - so a request to
            // delete a session may come in and be executed while
            // SessionMgr::Session::list is still in the middle of processing
            // the list. So bump up the ref count for the session. It will be
            // dropped by SessionMgr::Session::list after it's done with a
            // session.
            sessListCurr->session->incRef();
            sessCount++;
        }
        it.next();
    }
    perUsrInfo->sessionHtLock_.unlock();

    if (sessCount == 0) {
        // No sessions at all
        if (sessListIncomp) {
            // there may not be any sessions due to failure to load any; in this
            // case, preserve the status to be returned back to caller
            status = StatusSessListIncomplete;
        } else {
            status = StatusSessionNotFound;
        }
        sessionOutputSize = XcalarApiSizeOfOutput(*sessionListOutput);

        output = (XcalarApiOutput *) memAllocExt(sessionOutputSize, ModuleName);
        if (output == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed session list for session Usr '%s': %s",
                    user->userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        sessionListOutput = &output->outputResult.sessionListOutput;
        sessionListOutput->numSessions = 0;
        *sessionListOutputSize = sessionOutputSize;
        *outputOut = output;
    } else {
        // Now call SessionMgr's list function which will fill out the outputOut
        // and sessionListOutputSize with the results
        status = SessionMgr::Session::list(user,
                                           sessList,
                                           sessionListPattern,
                                           outputOut,
                                           sessionListOutputSize);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "SessionMgr::Session::list for Usr '%s' with filter '%s' "
                    "failed: %s, %s",
                    user->userIdName,
                    sessionListPattern,
                    sessListIncomp ? strGetFromStatus(StatusSessListIncomplete)
                                   : "",
                    strGetFromStatus(status));
        }
    }
CommonExit:
    if (perUsrInfo != NULL) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    if (status == StatusOk && sessListIncomp) {
        return StatusSessListIncomplete;
    } else {
        return status;
    }
}

// Utility function to choose a session for an operation given some parameters.
// An op can be performed on a session if it's Created and Idle, and some per-op
// state is true.
//
// NOTE: KEEP THE FAILURE CONDITIONS PER-OP IN SYNC WITH THE CALLER's USE OF
// FAILURE STATUS CODES IF chooseSess() fails for the sessOp. e.g. Persist's
// failure condition is that the session wasn't active - so the caller must use
// StatusSessionNotActive as a return status to its caller. Eventually, the
// status could be returned back from chooseSess() itself.
//
bool
UserMgr::chooseSess(SessOp sessOp, SessionMgr::Session *thisSession)
{
    bool stateOk = false;
    bool opOk = false;

    if (thisSession->getCrState() == SessionMgr::Session::CrState::Created &&
        thisSession->getOpState() == SessionMgr::Session::OpState::Idle) {
        stateOk = true;
    }
    switch (sessOp) {
    case SessOp::Activate:
    // fall through to Delete
    case SessOp::Delete:
        opOk = !thisSession->isActive();
        break;
    case SessOp::Inactivate:
        // for inactivate, choose the session only if session's active
        if (thisSession->isActive()) {
            opOk = true;
        }
        break;
    case SessOp::Persist:
        opOk = true;
        break;
    case SessOp::Rename:
        opOk = true;
        break;
    case SessOp::Upgrade:
        opOk = true;
        break;
    case SessOp::ReadState:
        opOk = true;
        break;
    default:
        assert(0);
        break;
    }
    return stateOk && opOk;
}

//
// findSess looks up perUsrInfo's sessions HT to find a session with a given
// name OR if there's no name given, return the first session it finds which is
// active. The latter is needed for compatibility with the model in which
// there's only one session active for a user, at any given time. When this
// model is retired, the search for the ONE active session should be removed.
//
// NOTE: if session name isn't supplied AND there's more than one active session
// found, this is a violation of the interface requirements. In this case,
// returning whichever session is found first will result in non-deterministic
// behavior and so the function returns NULL in such a case: this will
// eventually trigger a failure in the higher layers which violated the
// requirement.
//

UserMgr::UsrPerSessInfo *
UserMgr::findSess(PerUsrInfo *perUsrInfo, char *name)
{
    UsrPerSessInfo *perSessInfo = NULL;
    UsrPerSessInfo *foundSess = NULL;
    // must be called with perUsrInfo->sessionHtLock_ held
    if (name != NULL) {
        foundSess = perUsrInfo->sessionsHt_.find(name);
    } else {
        // for b/w compatibility, if no name given, find the first (only)
        // active session. If more than one active session found, fail and
        // return NULL.
        SessionsHt::iterator sit = perUsrInfo->sessionsHt_.begin();
        while ((perSessInfo = sit.get()) != NULL) {
            if (perSessInfo->session_->isActive()) {
                if (foundSess == NULL) {
                    foundSess = perSessInfo;
                } else {
                    xSyslog(ModuleName,
                            XlogErr,
                            "findSess failed: multiple sessions active but no "
                            "session name! Usr '%s', firstActive '%s', "
                            "secondActive '%s'",
                            perUsrInfo->getName(),
                            foundSess->getName(),
                            perSessInfo->getName());
                    foundSess = NULL;
                    break;
                }
            }
            sit.next();
        }
    }
    return foundSess;
}

// Utility function for getSessList() to add thisSession to the supplied session
// list and to bump up the session counter. It also sets the session's op state
// depending on the sessOp. If there's a memory failure, the entire list is
// freed up, and each session's state set back to idle, with the session count
// reset to 0.
Status
UserMgr::addSessToList(SessionMgr::Session *thisSession,
                       SessionListElt **sessListp,
                       int *sCount,
                       SessOp sessOp)
{
    SessionListElt *sessListCurr = NULL;
    SessionListElt *sessListPrev = NULL;
    SessionListElt *sessList = *sessListp;

    sessListCurr = new (std::nothrow) SessionListElt();
    if (sessListCurr == NULL) {
        while (sessList != NULL) {
            sessList->session->setOpState(SessionMgr::Session::OpState::Idle);
            sessList->session = NULL;
            sessListPrev = sessList;
            sessList = sessList->next;
            delete sessListPrev;
        }
        *sessListp = NULL;
        *sCount = 0;
        return StatusNoMem;
    }
    sessListCurr->session = thisSession;
    if (sessOp == SessOp::Delete) {
        thisSession->setCrState(SessionMgr::Session::CrState::DelInProg);
    } else {
        assert(sessOp == SessOp::Inactivate);
        thisSession->setOpState(SessionMgr::Session::OpState::Inactivating);
    }
    sessListCurr->next = *sessListp;
    *sessListp = sessListCurr;
    *sCount += 1;
    return StatusOk;
}

bool
UserMgr::foundSessListBusy(XcalarApiSessionDeleteInput *sessionInput,
                           PerUsrInfo *perUsrInfo,
                           UserMgr::SessOp sessOp,
                           SessionListElt **sessListp,
                           int *sCount,
                           Status *statusp)
{
    Status status = StatusUnknown;
    bool sessNameExactMatch = false;
    char *checkWildCard = NULL;
    UsrPerSessInfo *perSessInfo;
    SessionMgr::Session *thisSession = NULL;
    bool cleanup = !sessionInput->noCleanup;

    assert(cleanup);

    if (sessionInput->sessionId == 0) {
        checkWildCard = strchr(sessionInput->sessionName, '*');
        if (checkWildCard == NULL) {
            sessNameExactMatch = true;
        }
    }
    if (sessNameExactMatch == true) {
        perSessInfo =
            findSess(perUsrInfo,
                     sessionInput->sessionNameLength ? sessionInput->sessionName
                                                     : NULL);
        if (perSessInfo != NULL) {
            thisSession = perSessInfo->session_;
        }
    } else {
        // search for session with specified session ID or name with wildcard
        SessionsHt::iterator it = perUsrInfo->sessionsHt_.begin();
        while ((perSessInfo = it.get()) != NULL) {
            if (sessionInput->sessionId) {
                if (perSessInfo->session_->getSessId() ==
                    sessionInput->sessionId) {
                    thisSession = perSessInfo->session_;
                    break;  // explicit session ID matched so exit search loop
                }
            } else {
                // wild card pattern match
                // session matches, and is created, idle and active state is
                // as expected
                status = StatusOk;  // bulk call succeeds even if no sess found
                thisSession = perSessInfo->session_;
                if (strMatch(sessionInput->sessionName,
                             thisSession->getName())) {
                    if (chooseSess(sessOp, thisSession)) {
                        // found a created, idle session matching pattern
                        // and which meets selection rules in chooseSess().
                        status = addSessToList(thisSession,
                                               sessListp,
                                               sCount,
                                               sessOp);
                        if (status != StatusOk) {
                            xSyslog(ModuleName,
                                    XlogDebug,
                                    "foundSessListBusy failed in addSessToList "
                                    "Usr '%s', session '%s': %s",
                                    perUsrInfo->getName(),
                                    thisSession->getName(),
                                    strGetFromStatus(status));
                            break;
                        }
                    }
                }
            }
            it.next();
        }
    }
    if (sessNameExactMatch == true || sessionInput->sessionId) {
        if (thisSession != NULL) {
            // not a bulk call, and found the one session being searched for
            // now apply the chooseSess() filter on this session
            if (chooseSess(sessOp, thisSession)) {
                status = addSessToList(thisSession, sessListp, sCount, sessOp);
                if (status != StatusOk) {
                    xSyslog(ModuleName,
                            XlogDebug,
                            "foundSessListBusy failed in addSessToList "
                            "Usr '%s', session '%s': %s",
                            perUsrInfo->getName(),
                            thisSession->getName(),
                            strGetFromStatus(status));
                    goto CommonExit;
                }
            } else {
                if (thisSession->getCrState() !=
                    SessionMgr::Session::CrState::Created) {
                    status = StatusBusy;
                } else if (thisSession->getOpState() !=
                           SessionMgr::Session::OpState::Idle) {
                    status = StatusBusy;
                } else if (sessOp == SessOp::Delete) {
                    status = StatusSessionNotInact;
                } else if (sessOp == SessOp::Inactivate) {
                    status = StatusSessionAlreadyInact;
                }
            }
        } else {
            status = StatusSessionNotFound;
        }
    } else if (*sCount == 0) {
        status = StatusSessionNotFound;
    }

CommonExit:
    *statusp = status;
    if (status == StatusBusy) {
        return true;
    } else {
        return false;
    }
}

//
// Utility function for bulk inactivate and bulk delete - gets a session list
// given input params which may have a session name with wild-card chars. Each
// session in list has its opState updated to reflect the supplied op in
// sessOp. It is caller's responsibility to revert the status back to Idle,
// after it's done with the session, to free the session up for other
// operations in future.
Status
UserMgr::getSessList(XcalarApiUserId *user,
                     XcalarApiSessionDeleteInput *sessionInput,
                     SessOp sessOp,
                     PerUsrInfo **perUsrInfoRet,
                     int *sessCountRet,
                     SessionListElt **sessListRet)
{
    Status status;
    NodeId nodeId;
    PerUsrInfo *perUsrInfo;
    bool lockHeld = false;
    SessionListElt *sessList = NULL;
    int sessCount = 0;

    assert(perUsrInfoRet != NULL);
    *perUsrInfoRet = NULL;
    perUsrInfo = getUsrInfo(user->userIdName,
                            sizeof(user->userIdName),
                            &status,
                            &nodeId);
    if (status != StatusOk || nodeId != Config::get()->getMyNodeId()) {
        // session can't be found if user can't - so must return
        // StatusSessionNotFound here. However log can report more info.
        // See comment in getSess() regarding the same issue
        status = StatusSessionNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "getSessList couldn't get Usr info "
                "Usr '%s': %s",
                user->userIdName,
                status != StatusOk
                    ? strGetFromStatus(status)
                    : strGetFromStatus(StatusSessionUsrAlreadyExists));
        goto CommonExit;
    }
    *perUsrInfoRet = perUsrInfo;
    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();

    // for bulk ops, busy sessions are skipped, and not reported, so the
    // while loop body is executed only if a single session is found busy
    //
    while (foundSessListBusy(sessionInput,
                             perUsrInfo,
                             sessOp,
                             &sessList,
                             &sessCount,
                             &status)) {
        assert(status == StatusBusy);
        if (sessionInput->sessionId) {
            xSyslog(ModuleName,
                    XlogDebug,
                    "getSessList must wait Usr '%s', session 0x%lX: %s",
                    user->userIdName,
                    sessionInput->sessionId,
                    "not in created state or not idle");
        } else {
            xSyslog(ModuleName,
                    XlogDebug,
                    "getSessList must wait Usr '%s', session '%s': %s",
                    user->userIdName,
                    sessionInput->sessionNameLength > 0
                        ? sessionInput->sessionName
                        : "",
                    "not in created state or not idle");
        }
        perUsrInfo->usrSessBusyCondVar_.wait(&perUsrInfo->sessionHtLock_);
    }

    if (status == StatusSessionNotFound) {
        assert(sessCount == 0);
        if (sessionInput->sessionId == 0) {
            xSyslog(ModuleName,
                    XlogErr,
                    "getSessList failed "
                    "Usr '%s', session '%s': %s",
                    user->userIdName,
                    sessionInput->sessionNameLength ? sessionInput->sessionName
                                                    : "<Empty Session Name>",
                    strGetFromStatus(status));
        } else {
            xSyslog(ModuleName,
                    XlogErr,
                    "getSessList failed "
                    "Usr '%s', sessionID 0x%lX: %s",
                    user->userIdName,
                    sessionInput->sessionId,
                    strGetFromStatus(status));
        }
    } else if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "getSessList failed Usr '%s', Session(s) '%s': %s",
                user->userIdName,
                sessionInput->sessionNameLength ? sessionInput->sessionName
                                                : "<Empty Session Name>",
                strGetFromStatus(status));
    }
    perUsrInfo->sessionHtLock_.unlock();
    lockHeld = false;

CommonExit:
    if (lockHeld == true) {
        perUsrInfo->sessionHtLock_.unlock();
    }
    *sessCountRet = sessCount;
    *sessListRet = sessList;
    return status;
}

Status
UserMgr::getSessionId(XcalarApiUserId *user,
                      XcalarApiSessionInfoInput *sessionInput)
{
    Status status = StatusUnknown;
    SessionMgr::Session *thisSess = NULL;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiSessionInfoInput sessInputInternal;

    if (sessionInput == NULL) {
        status = StatusInval;
        goto CommonExit;
    }

    sessInputInternal.sessionNameLength = sessionInput->sessionNameLength;
    strlcpy(sessInputInternal.sessionName,
            sessionInput->sessionName,
            sizeof(sessInputInternal.sessionName));
    sessInputInternal.sessionId = sessionInput->sessionId;

    status = getSess(user,
                     &sessInputInternal,
                     SessOp::ReadState,
                     &thisSess,
                     &perUsrInfo);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::getSessionId failed Usr '%s': %s",
                user->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    sessionInput->sessionId = thisSess->getSessId();  // maybe add return val
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        // revert state back to Idle for this session - the API's done
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::doDelete(XcalarApiUserId *user,
                  XcalarApiSessionDeleteInput *sessionInput,
                  XcalarApiSessionGenericOutput *sessionOutput)
{
    Status status;
    PerUsrInfo *perUsrInfo = NULL;
    SessionListElt *sessList = NULL;
    SessionListElt *sessListOld = NULL;
    int sessCount = 0;
    SessionMgr::Session *thisSession;
    UsrPerSessInfo *perSessInfo;
    int returnCode;

    status = getSessList(user,
                         sessionInput,
                         SessOp::Delete,
                         &perUsrInfo,
                         &sessCount,
                         &sessList);
    if (status != StatusOk) {
        goto CommonExit;
    }
    if (sessCount != 0) {
        // TODO: Iterate here rather than in sessions layer so can call
        // method via the session instance
        status = SessionMgr::Session::doDelete(user, sessList, sessCount);
        assert(status == StatusOk);
        perUsrInfo->sessionHtLock_.lock();
        while (sessList != NULL) {
            thisSession = sessList->session;
            perSessInfo =
                perUsrInfo->sessionsHt_.remove(thisSession->getName());
            assert(perSessInfo->session_ == thisSession);
            perSessInfo->session_ = NULL;  // cleaner to clear before delete
            sessListOld = sessList;
            sessList = sessList->next;
            sessListOld->session = NULL;
            // decrement ref on session now that UserMgr isn't tracking it
            thisSession->decRef();
            delete sessListOld;
            delete perSessInfo;
        }
        // Wake up any waiters waiting for a session to not be busy: in this
        // case, they'll find the session being waited on, gone (i.e. deleted)
        perUsrInfo->usrSessBusyCondVar_.broadcast();
        perUsrInfo->sessionHtLock_.unlock();
    } else {
        status = StatusSessionNotFound;
    }
CommonExit:
    if (sessCount != 0) {
        returnCode = snprintf(sessionOutput->errorMessage,
                              sizeof(sessionOutput->errorMessage),
                              "%d session(s) deleted",
                              sessCount);
        assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
        StatsLib::statAtomicAdd64(stats.deletedCount, sessCount);
    } else {
        returnCode = snprintf(sessionOutput->errorMessage,
                              sizeof(sessionOutput->errorMessage),
                              "No sessions deleted.");
        assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed session delete for session Usr '%s': %s",
                user->userIdName,
                strGetFromStatus(status));
        StatsLib::statAtomicIncr64(stats.deletedFailures);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::inactivate(XcalarApiUserId *user,
                    XcalarApiSessionDeleteInput *sessionInput,
                    XcalarApiSessionGenericOutput *sessionOutput)
{
    Status status;
    PerUsrInfo *perUsrInfo = NULL;
    SessionListElt *sessList = NULL;
    SessionListElt *sessListOld = NULL;
    int sessCount = 0;
    SessionMgr::Session *thisSession;
    bool cleanup = !sessionInput->noCleanup;
    int returnCode;

    assert(cleanup);

    status = getSessList(user,
                         sessionInput,
                         SessOp::Inactivate,
                         &perUsrInfo,
                         &sessCount,
                         &sessList);
    if (status != StatusOk) {
        if (status == StatusSessionAlreadyInact) {
            // Inactivating a session that is already inactive is considered
            // a success.
            status = StatusOk;
        }
        goto CommonExit;
    }

    if (sessCount != 0) {
        // TODO: Do iteration in UserMgr, here, so the call to sessions layer
        // can be on the session instance, cleaning up the interface (similar
        // to bulk deletion)
        status = SessionMgr::Session::inactivate(user, sessList, sessCount);

        // After call to session layer is done, mark each session as idle
        // (whether or not status is StatusOk - the attempt to inactivate is
        // done - so each session's OpState must be reset to Idle so it's free
        // to accept other Ops).
        //
        // This could be done in the sessions layer, but it's safer to do this
        // under the sessionHtLock_ under which this state is being read /
        // written. Since bulk inactivate isn't yet supported, this is more of a
        // perf issue (second iter here) rather than correctness.  Eventually,
        // if the second iter is an issue, the state update could be done inside
        // the sessions layer.  As you iterate through the list, start freeing
        // list elements since list isn't needed any more.

        perUsrInfo->sessionHtLock_.lock();
        while (sessList != NULL) {
            thisSession = sessList->session;
            thisSession->setOpState(SessionMgr::Session::OpState::Idle);
            sessListOld = sessList;
            sessList = sessList->next;
            sessListOld->session = NULL;
            delete sessListOld;
        }
        // Wake up any waiters waiting for a session to not be busy
        perUsrInfo->usrSessBusyCondVar_.broadcast();
        perUsrInfo->sessionHtLock_.unlock();

        BailIfFailed(status);
    } else {
        status = StatusSessionNotFound;
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed inactivate for session Usr '%s', Session(s) '%s': %s",
                user->userIdName,
                sessionInput->sessionNameLength ? sessionInput->sessionName
                                                : "<Empty Session Name>",
                strGetFromStatus(status));
    }
    if (sessCount != 0) {
        if (status == StatusOk) {
            returnCode = snprintf(sessionOutput->errorMessage,
                                  sizeof(sessionOutput->errorMessage),
                                  "%d session(s) inactivated",
                                  sessCount);
            assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
            StatsLib::statAtomicAdd64(stats.inactedCount, sessCount);
        } else {
            returnCode = snprintf(sessionOutput->errorMessage,
                                  sizeof(sessionOutput->errorMessage),
                                  "some of %d session(s) failed to inactivate",
                                  sessCount);
            assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
            StatsLib::statAtomicIncr64(stats.inactedFailures);
        }
    } else {
        returnCode = snprintf(sessionOutput->errorMessage,
                              sizeof(sessionOutput->errorMessage),
                              "No sessions inactivated.");
        assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
        if (status != StatusOk) {
            StatsLib::statAtomicIncr64(stats.inactedFailures);
        }
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// Rename is complicated by the fact that name is the hook via which UserMgr
// tracks sessions in the sessionHt_, and the session name is changed outside
// the sessionHtLock_ by the sessions layer, which must do a disk write to
// update the durable structure for the name change. And the latter may fail,
// so the UserMgr has to hold down both the old and new names via dummy
// sessions in the sessions hash table, while the name update occurs outside the
// lock in the sessions layer. After the sessions layer is done, the UserMgr
// routine here must either cleanout the old hash table entry or the new one,
// depending on success or failure, respectively, of the name update in the
// sessions layer. Details high-level logic is below:
//
// 0. Check if old name is present: if absent, fail; use getSess() to get the
//    pointer to the session whose name will be changed, so that its state is
//    updated to Renaming, and the perUsrInfo pointer is obtained for future
//    operations.
// 1. Grab perUsrInfo->sessionHtLock_ to enter dummy sessions for the old and
//    new names (given that the real session's name is changing, can't rely
//    on it being pointed to by the UserMgr hash table)
// 2. Check if new name is available: if EEXIST, release lock and fail
// 3. If new name's available, create a dummy session with new name, with the
//    right Cr state (Creating) so concurrent operations (such as create, or
//    say persist, etc.), can't grab the name, or operate on the dummy session.
//    Also, create a dummy session for old name, so real session's name can
//    change without it becoming inconsistent with the hash table
// 4. Now, release sessionHtLock_ - all 3 sessions (existing one with old name
//    and the two dummy ones) are protected with the respective states:
//    existing one with OpState set to Renaming, and dummy ones with the
//    CrState set to Creating
// 5. Now, call sessions layer with existing session Info pointer - this call
//    may fail...
// 6. On return from sessions layer, either the real session's name changed
//    or remained unchanged. If it succeeded, update the HT entry for new name
//    with the real session, and remove/free the old HT entry. Do the reverse,
//    if the name change failed in the sessions layer.
// 7. Detailed steps for step 6 are: grab sessionHtLock_ and do the following:
//    - get newPerSessInfo from sessionsHt_, which points to dummy session
//      using new name
//    - get oldPerSessInfo from sessionsHt_, which points to dummy session
//      (which has old name)
//    - if real session's name changed, then:
//        - update newPerSessInfo with real session
//        - remove old-name from HT, and delete oldPerSessInfo, and the two
//          dummy sessions
//    - if real session's name unchanged, then:
//        - update oldPerSessInfo with real session
//        - remove new-name from HT, and delete newPerSessInfo, and the two
//          dummy sessions
// 8. Fee sessionHtLock_
//
Status
UserMgr::rename(XcalarApiUserId *user,
                XcalarApiSessionRenameInput *sessionInput,
                XcalarApiSessionGenericOutput *sessionOutput)
{
    Status status;
    XcalarApiSessionInfoInput sessInputInternal;
    SessionMgr::Session *oldSess = NULL;
    SessionMgr::Session *dummyOldSess = NULL;
    SessionMgr::Session *dummyNewSess = NULL;
    PerUsrInfo *perUsrInfo = NULL;
    UsrPerSessInfo *perSessInfo = NULL;
    UsrPerSessInfo *oldPerSessInfo = NULL;
    UsrPerSessInfo *newPerSessInfo = NULL;
    bool lockHeld = false;
    int returnCode;

    // Check if there's a session with the old name. If so, get the session
    // with the old name (whose state will be updated as Renaming on successful
    // return from getSess()) to lock out other ops on it.
    sessInputInternal.sessionNameLength = sessionInput->origSessionNameLength;
    strlcpy(sessInputInternal.sessionName,
            sessionInput->origSessionName,
            sizeof(sessInputInternal.sessionName));
    sessInputInternal.sessionId = 0;  // unused

    status = getSess(user,
                     &sessInputInternal,
                     SessOp::Rename,
                     &oldSess,
                     &perUsrInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(perUsrInfo != NULL && oldSess != NULL);
    // oldSess is marked as Renaming at this point - it'll be reset to Idle
    // after we're done
    assert(perUsrInfo != NULL);

    if (perUsrInfo->getSessState() == PerUsrInfo::SessState::LoadedPartial) {
        status = StatusSessListIncomplete;
        goto CommonExit;
    }

    // Now check new name for availability: is it free? Or some other session
    // has this name?
    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();
    perSessInfo = perUsrInfo->sessionsHt_.find(sessionInput->sessionName);
    if (perSessInfo != NULL) {
        status = StatusSessionExists;
        goto CommonExit;
    }
    dummyNewSess =
        new (std::nothrow) SessionMgr::Session(sessionInput->sessionName);
    if (dummyNewSess == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    dummyOldSess =
        new (std::nothrow) SessionMgr::Session(sessionInput->origSessionName);
    if (dummyOldSess == NULL) {
        status = StatusNoMem;
        dummyNewSess->decRef();
        goto CommonExit;
    }
    // Note that CrState for dummyOldSess, dummNewSess is Creating here
    newPerSessInfo = new (std::nothrow) UserMgr::UsrPerSessInfo();

    if (newPerSessInfo == NULL) {
        dummyNewSess->decRef();
        dummyOldSess->decRef();
        status = StatusNoMem;
        goto CommonExit;
    }
    newPerSessInfo->session_ = dummyNewSess;
    status = perUsrInfo->sessionsHt_.insert(newPerSessInfo);
    assert(status == StatusOk);

    // get the old PerSessInfo so it can be updated to point to a dummy sess
    // while the real session's name is changing - otherwise we'd not be able
    // to remove it from the hash table after session layer's done updating the
    // real session's name (since name is the hash table key)
    oldPerSessInfo =
        perUsrInfo->sessionsHt_.find(sessionInput->origSessionName);
    assert(oldPerSessInfo->session_ == oldSess);

    // to let oldPerSessInfo be locatable with the old name after the call to
    // SessionMgr::Session::updateName finishes successfully, it should point
    // to a dummy session which still has old name

    oldPerSessInfo->session_ = dummyOldSess;

    // now, it's safe to release sessionHtLock_ here;
    // all three sessions: oldSess, dummyNewSess, and dummyOldSess are
    // protected via their states
    perUsrInfo->sessionHtLock_.unlock();
    lockHeld = false;

    status = oldSess->updateName(user, sessionInput);

    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();

    // already have newPerSessInfo; assert that this still matches new name
    assert(perUsrInfo->sessionsHt_.find(sessionInput->sessionName) ==
           newPerSessInfo);
    // already have oldPerSessInfo; assert that this still matches old name
    assert(perUsrInfo->sessionsHt_.find(sessionInput->origSessionName) ==
           oldPerSessInfo);
    assert(dummyNewSess == newPerSessInfo->session_);
    assert(dummyOldSess == oldPerSessInfo->session_);

    if (status == StatusOk) {
        assert(strcmp(oldSess->getName(), dummyNewSess->getName()) == 0);
        newPerSessInfo->session_ = oldSess;  // oldSess's name was updated
        assert(perUsrInfo->sessionsHt_.find(oldSess->getName()) ==
               newPerSessInfo);
        verify(perUsrInfo->sessionsHt_.remove(sessionInput->origSessionName) ==
               oldPerSessInfo);
        oldPerSessInfo->session_ = NULL;
        dummyNewSess->decRef();
        dummyOldSess->decRef();
        delete oldPerSessInfo;
    } else {
        assert(strcmp(oldSess->getName(), dummyOldSess->getName()) == 0);
        oldPerSessInfo->session_ = oldSess;  // oldSess's name was unchanged
        assert(perUsrInfo->sessionsHt_.find(oldSess->getName()) ==
               oldPerSessInfo);
        verify(perUsrInfo->sessionsHt_.remove(sessionInput->sessionName) ==
               newPerSessInfo);
        newPerSessInfo->session_ = NULL;
        dummyNewSess->decRef();
        dummyOldSess->decRef();
        delete newPerSessInfo;
    }
    lockHeld = false;
    perUsrInfo->sessionHtLock_.unlock();

CommonExit:
    if (lockHeld) {
        perUsrInfo->sessionHtLock_.unlock();
    }
    if (perUsrInfo != NULL && oldSess != NULL) {
        // revert state back to Idle for this session - the API's done
        perUsrInfo->setSessIdle(oldSess);
    }
    if (status == StatusOk) {
        returnCode = snprintf(sessionOutput->errorMessage,
                              sizeof(sessionOutput->errorMessage),
                              "Session renamed successfully");
        assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
        StatsLib::statAtomicIncr64(stats.renameCount);
    } else {
        xSyslog(ModuleName,
                XlogErr,
                "Failed session rename Usr '%s', from '%s' to '%s: %s",
                user->userIdName,
                sessionInput->origSessionName,
                sessionInput->sessionName,
                strGetFromStatus(status));
        returnCode = snprintf(sessionOutput->errorMessage,
                              sizeof(sessionOutput->errorMessage),
                              "Session rename failed");
        assert(returnCode < (int) sizeof(sessionOutput->errorMessage));
        StatsLib::statAtomicIncr64(stats.renameFailures);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

bool
UserMgr::foundSessBusy(XcalarApiSessionInfoInput *sessionInput,
                       PerUsrInfo *perUsrInfo,
                       UserMgr::SessOp sessOp,
                       SessionMgr::Session **thisSessRet,
                       Status *statusp)
{
    Status status = StatusUnknown;
    UsrPerSessInfo *foundSess = NULL;
    SessionMgr::Session *thisSess = NULL;

    *thisSessRet = NULL;

    if (sessionInput->sessionNameLength > 0) {
        foundSess = perUsrInfo->sessionsHt_.find(sessionInput->sessionName);
    } else {
        //
        // For now, if neither name, nor sessionId is given, find the only
        // session which can be active (currently, only one session can be
        // active). In future, only sessionName will be allowed and must be
        // specified - otherwise it'll fail.
        //
        UsrPerSessInfo *perSessInfo = NULL;

        SessionsHt::iterator sit = perUsrInfo->sessionsHt_.begin();
        while ((perSessInfo = sit.get()) != NULL) {
            if (sessionInput->sessionId != 0) {
                if (perSessInfo->session_->getSessId() ==
                    sessionInput->sessionId) {
                    foundSess = perSessInfo;
                    break;
                }
            } else if (perSessInfo->session_->isActive()) {
                if (foundSess == NULL) {
                    foundSess = perSessInfo;
                } else {
                    xSyslog(ModuleName,
                            XlogErr,
                            "foundSessBusy failed: multiple sessions active "
                            "but no session name! Usr '%s', firstActive '%s', "
                            "secondActive '%s'",
                            perUsrInfo->getName(),
                            foundSess->getName(),
                            perSessInfo->getName());
                    foundSess = NULL;
                    break;
                }
            }
            sit.next();
        }
    }
    if (foundSess == NULL) {
        status = StatusSessionNotFound;
        goto CommonExit;
    }
    thisSess = foundSess->session_;
    // activate -> state must be Created (i.e. not Creating, and not DelInProg)
    // and the OpState must be Idle, and session must be inactive
    // persist -> state must be Created (i.e. not Creating, and not DelInProg)
    // and the OpState must be Idle, and session must be active
    if (chooseSess(sessOp, thisSess)) {
        if (sessOp == SessOp::Activate) {
            thisSess->setOpState(SessionMgr::Session::OpState::Activating);
        } else if (sessOp == SessOp::Persist) {
            thisSess->setOpState(SessionMgr::Session::OpState::Persisting);
        } else if (sessOp == SessOp::Rename) {
            thisSess->setOpState(SessionMgr::Session::OpState::Renaming);
        } else if (sessOp == SessOp::Upgrade) {
            thisSess->setOpState(SessionMgr::Session::OpState::Upgrading);
        } else {
            assert(sessOp == SessOp::ReadState);
            thisSess->setOpState(SessionMgr::Session::OpState::Reading);
        }
        status = StatusOk;
    } else {
        if (thisSess->getCrState() != SessionMgr::Session::CrState::Created) {
            status = StatusBusy;
        } else if (thisSess->getOpState() !=
                   SessionMgr::Session::OpState::Idle) {
            status = StatusBusy;
        } else if (sessOp == SessOp::Activate) {
            status = StatusSessionNotInact;
        } else if (sessOp == SessOp::Persist) {
            status = StatusSessionNotActive;
        } else if (sessOp == SessOp::Rename) {
            assert(0);
        }
    }
CommonExit:
    *thisSessRet = thisSess;
    *statusp = status;
    if (status == StatusBusy) {
        return true;  // found a session but it's busy
    } else {
        return false;  // session doesn't exist, OR its update was a success
    }
}

// Used by both activate and persist to return a session, given either a
// session name or a session ID. The session's ref count is not incremented on
// return, since an attempt to delete the session will fail given that the
// operation (activate or persist) is in progress.
//
// The session returned has its OpState updated to reflect the op supplied
// in sessOp. It is caller's responsibility to revert the OpState back to Idle,
// after it's done with the session, to free the session up for other
// operations in future.
Status
UserMgr::getSess(XcalarApiUserId *user,
                 XcalarApiSessionInfoInput *sessionInput,
                 UserMgr::SessOp sessOp,
                 SessionMgr::Session **thisSessRet,
                 PerUsrInfo **perUsrInfoRet)
{
    Status status;
    NodeId nodeId;
    PerUsrInfo *perUsrInfo;
    SessionMgr::Session *session = NULL;
    bool lockHeld = false;

    // init returns
    *thisSessRet = NULL;
    if (perUsrInfoRet != NULL) {
        *perUsrInfoRet = NULL;
    }
    perUsrInfo = getUsrInfo(user->userIdName,
                            sizeof(user->userIdName),
                            &status,
                            &nodeId);
    if (status != StatusOk || nodeId != Config::get()->getMyNodeId()) {
        status = StatusSessionNotFound;
        //
        // in future, StatusSessionUsrAlreadyExists or
        // StatusSessionUsrNotExist from getUsrInfo() can be passed back up
        // to the session APIs but today, there's not a strong concept of
        // creating a user - the user gets created as a side-effect of
        // creating a session - so the APIs and callers aren't ready for
        // other failure statuses which spell out why a session couldn't be
        // found (b/c the user isn't accessible). The syslog can provide that
        // detail here though.
        //
        xSyslog(ModuleName,
                XlogErr,
                "getSess couldn't get Usr info "
                "Usr '%s': %s",
                user->userIdName,
                status != StatusOk
                    ? strGetFromStatus(status)
                    : strGetFromStatus(StatusSessionUsrAlreadyExists));
        goto CommonExit;
    }
    if (perUsrInfoRet != NULL) {
        *perUsrInfoRet = perUsrInfo;  // return perUsrInfo for those who want it
    }
    lockHeld = true;
    perUsrInfo->sessionHtLock_.lock();

    while (foundSessBusy(sessionInput, perUsrInfo, sessOp, &session, &status)) {
        assert(status == StatusBusy);
        assert(session != NULL);
        xSyslog(ModuleName,
                XlogDebug,
                "getSess must wait Usr '%s', session '%s: %s",
                user->userIdName,
                session->getName(),
                "not in created state or not idle");
        perUsrInfo->usrSessBusyCondVar_.wait(&perUsrInfo->sessionHtLock_);
    }
    if (status == StatusSessionNotFound) {
        assert(session == NULL);
        xSyslog(ModuleName,
                XlogErr,
                "getSess failed Usr '%s' session '%s': %s",
                user->userIdName,
                sessionInput->sessionNameLength ? sessionInput->sessionName
                                                : "",
                strGetFromStatus(status));
    } else if (status != StatusSessionNotInact && status != StatusOk) {
        assert(session != NULL);
        xSyslog(ModuleName,
                XlogErr,
                "getSess failed Usr '%s', session '%s': %s",
                user->userIdName,
                session->getName(),
                strGetFromStatus(status));
    }
    lockHeld = false;
    perUsrInfo->sessionHtLock_.unlock();
    *thisSessRet = session;
CommonExit:
    if (lockHeld) {
        perUsrInfo->sessionHtLock_.unlock();
    }
    return status;
}

Status
UserMgr::activate(XcalarApiUserId *user,
                  XcalarApiSessionActivateInput *sessionInput,
                  XcalarApiSessionGenericOutput *sessionOutput)
{
    Status status = StatusUnknown;
    SessionMgr::Session *thisSess = NULL;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiSessionInfoInput sessInputInternal;

    sessInputInternal.sessionId = sessionInput->sessionId;
    strlcpy(sessInputInternal.sessionName,
            sessionInput->sessionName,
            sizeof(sessInputInternal.sessionName));
    sessInputInternal.sessionNameLength = strlen(sessionInput->sessionName);

    status = getSess(user,
                     &sessInputInternal,
                     SessOp::Activate,
                     &thisSess,
                     &perUsrInfo);
    if (status != StatusOk) {
        if (status == StatusSessionNotInact) {
            // Activating an already active session is considered a success.
            status = StatusOk;
        } else {
            xSyslog(ModuleName,
                    XlogErr,
                    "UserMgr::activate failed Usr '%s': %s",
                    user->userIdName,
                    strGetFromStatus(status));
        }
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    status = thisSess->activate(user, sessionOutput);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr activate failed Usr '%s', session '%s': %s",
                user->userIdName,
                thisSess->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (status == StatusOk) {
        StatsLib::statAtomicIncr64(stats.actedCount);
    } else {
        StatsLib::statAtomicIncr64(stats.actedFailures);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// TODO: Persist API reflects the use of wildcard and bulk persist. This is
// being deprecated, and eventually the persist API should have a single
// session interface similar to activate.
Status
UserMgr::persist(XcalarApiUserId *user,
                 XcalarApiSessionDeleteInput *sessionInput,
                 XcalarApiOutput **outputOut,
                 size_t *sessionListOutputSize)
{
    Status status = StatusUnknown;
    SessionMgr::Session *thisSess = NULL;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiSessionInfoInput sessionInfoInput;

    sessionInfoInput.sessionId = sessionInput->sessionId;
    strlcpy(sessionInfoInput.sessionName,
            sessionInput->sessionName,
            sizeof(sessionInfoInput.sessionName));
    sessionInfoInput.sessionNameLength = sessionInput->sessionNameLength;

    status = getSess(user,
                     &sessionInfoInput,
                     SessOp::Persist,
                     &thisSess,
                     &perUsrInfo);
    if (status != StatusOk) {
        if (status == StatusSessionNotActive) {
            // Session has already been persisted and is inactive.  Consider
            // this as a success.
            status = StatusOk;
        } else {
            xSyslog(ModuleName,
                    XlogErr,
                    "UserMgr::persist failed Usr '%s': %s",
                    user->userIdName,
                    strGetFromStatus(status));
        }
        goto CommonExit;
    }
    assert(perUsrInfo != NULL);  // needed to update SessState at the end
    assert(thisSess != NULL);
    status = thisSess->persist(user, outputOut, sessionListOutputSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::persist failed Usr '%s', session '%s': %s",
                user->userIdName,
                thisSess->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (status == StatusOk) {
        StatsLib::statAtomicIncr64(stats.persistCount);
    } else {
        StatsLib::statAtomicIncr64(stats.persistFailures);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::download(XcalarApiUserId *userId,
                  XcalarApiSessionDownloadInput *sessionDownloadInput,
                  XcalarApiOutput **outputOut,
                  size_t *outputSizeOut)
{
    Status status = StatusOk;
    SessionMgr::Session *thisSess = NULL;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiSessionInfoInput sessInputInternal;
    bool madeSessInactive = false;

    sessInputInternal.sessionNameLength =
        sessionDownloadInput->sessionNameLength;
    strlcpy(sessInputInternal.sessionName,
            sessionDownloadInput->sessionName,
            sizeof(sessInputInternal.sessionName));
    sessInputInternal.sessionId = sessionDownloadInput->sessionId;

    status = getSess(userId,
                     &sessInputInternal,
                     SessOp::ReadState,
                     &thisSess,
                     &perUsrInfo);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to get session for Usr '%s': %s",
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    // commit to reverting state to Idle since the attempt to inactivate below
    // could fail
    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    if (thisSess->isActive()) {
        // Must inactivate the session first.
        SessionListElt sessList;
        sessList.session = thisSess;
        sessList.next = NULL;
        status = SessionMgr::Session::inactivateNoClean(userId, &sessList, 1);
        if (status != StatusOk) {
            // if the session fails to be inactivated, fail the download. This
            // is a failure to pause - the user can always cancel whatever
            // operation is preventing the pause, and re-issue the download.
            // Letting the download proceed despite a failure to pause may
            // result in new unforeseen issues - which need to be tested!
            //
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to inactivate session for Usr '%s': %s",
                    userId->userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        madeSessInactive = true;
    }
    status = thisSess->download(userId,
                                sessionDownloadInput,
                                outputOut,
                                outputSizeOut);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to download session for Usr '%s', session '%s': %s",
                userId->userIdName,
                thisSess->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (madeSessInactive) {
        Status status2;
        XcalarApiSessionGenericOutput sessionOutput;
        status2 = thisSess->activate(userId, &sessionOutput);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to re-activate session for Usr '%s', sesion '%s': "
                    "%s",
                    userId->userIdName,
                    thisSess->getName(),
                    strGetFromStatus(status2));
            // continue...
        }
    }
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (status == StatusOk) {
        StatsLib::statAtomicIncr64(stats.downloadCount);
    } else {
        StatsLib::statAtomicIncr64(stats.downloadFailures);
    }

    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// Upload API creates a new workbook but with the contents of the workbook file
// supplied in the input payload to the API. The new workbook is born inactive,
// but with all the relevant sections of a workbook initialized (e.g. dag is
// created and hydrated, kvs is updated, etc) from the workbook file.
//
// This API shares a common routine (createInternal()), with the ::create API.
Status
UserMgr::upload(XcalarApiUserId *user,
                XcalarApiSessionUploadInput *sessionInput,
                XcalarApiSessionNewOutput *sessionOutput)
{
    Status status = StatusOk;
    XcalarApiSessionNewInput sessInputInternal;
    SessionUploadPayload sessUpPayload;

    sessInputInternal.sessionNameLength = sessionInput->sessionNameLength;
    strlcpy(sessInputInternal.sessionName,
            sessionInput->sessionName,
            sizeof(sessInputInternal.sessionName));
    sessInputInternal.fork = false;  // not a fork obviously
    sessInputInternal.forkedSessionNameLength = 0;

    sessUpPayload.sessionContentCount = sessionInput->sessionContentCount;
    sessUpPayload.sessionContentP = sessionInput->sessionContent;
    strlcpy(sessUpPayload.pathToAdditionalFiles,
            sessionInput->pathToAdditionalFiles,
            sizeof(sessUpPayload.pathToAdditionalFiles));

    status =
        createInternal(user, &sessInputInternal, &sessUpPayload, sessionOutput);

    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to upload session for Usr '%s', session '%s': %s",
                      user->userIdName,
                      sessionInput->sessionName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status == StatusOk) {
        StatsLib::statAtomicIncr64(stats.uploadCount);
    } else {
        StatsLib::statAtomicIncr64(stats.uploadFailures);
    }

    return status;
}

Status
UserMgr::getResultSetIdsByTableId(TableNsMgr::TableId tableId,
                                  XcalarApiUserId *user,
                                  XcalarApiSessionInfoInput *sessInput,
                                  ResultSetId **rsIdsOut,
                                  unsigned *numRsIdsOut)
{
    Status status;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;

    status =
        getSess(user, sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed get resultset ids for tableId %ld: %s",
                tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    status = thisSess->getResultsetIdsByTableId(tableId, rsIdsOut, numRsIdsOut);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed get resultset ids, session %ld '%s', tableId "
                    "%ld: %s",
                    thisSess->getSessId(),
                    thisSess->getName(),
                    tableId,
                    strGetFromStatus(status));

CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::trackResultset(
    ResultSetId rsId,
    TableNsMgr::TableId tableId,
    const xcalar::compute::localtypes::Workbook::WorkbookSpecifier *specifier)
{
    Status status = StatusOk;
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;

    status = getUserAndSessInput(specifier, &user, &sessInput);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed trackResultset rsId %ld tableId %ld: %s",
                    rsId,
                    tableId,
                    strGetFromStatus(status));

    status = trackResultset(rsId, tableId, &user, &sessInput);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed trackResultset rsId %ld tableId %ld: %s",
                    rsId,
                    tableId,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
UserMgr::trackResultset(ResultSetId rsId,
                        TableNsMgr::TableId tableId,
                        XcalarApiUserId *user,
                        XcalarApiSessionInfoInput *sessInput)
{
    Status status = StatusOk;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;

    status =
        getSess(user, sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed trackResultset rsId %ld tableId %ld: %s",
                rsId,
                tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    status = thisSess->trackResultset(rsId, tableId);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed trackResultset, session %ld '%s', rsId %ld tableId "
                    "%ld: %s",
                    thisSess->getSessId(),
                    thisSess->getName(),
                    rsId,
                    tableId,
                    strGetFromStatus(status));

CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

void
UserMgr::untrackResultsets(TableNsMgr::TableId tableId,
                           XcalarApiUserId *user,
                           XcalarApiSessionInfoInput *sessInput)
{
    Status status = StatusOk;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;

    status =
        getSess(user, sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed untrackResultset tableId %ld: %s",
                tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    thisSess->untrackResultsets(tableId);
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
}

void
UserMgr::untrackResultset(
    ResultSetId rsId,
    TableNsMgr::TableId tableId,
    const xcalar::compute::localtypes::Workbook::WorkbookSpecifier *specifier)
{
    Status status = StatusOk;
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;

    status = getUserAndSessInput(specifier, &user, &sessInput);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed untrackResultset rsId %ld tableId %ld: %s",
                rsId,
                tableId,
                strGetFromStatus(status));
    } else {
        untrackResultset(rsId, tableId, &user, &sessInput);
    }
}

void
UserMgr::untrackResultset(ResultSetId rsId,
                          TableNsMgr::TableId tableId,
                          XcalarApiUserId *user,
                          XcalarApiSessionInfoInput *sessInput)
{
    Status status = StatusOk;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;

    status =
        getSess(user, sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed untrackResultset rsId %ld tableId %ld: %s",
                rsId,
                tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    thisSess->untrackResultset(rsId, tableId);

CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
}

Status
UserMgr::trackOutstandOps(
    const xcalar::compute::localtypes::Workbook::WorkbookSpecifier *specifier,
    OutstandOps oops)
{
    Status status = StatusOk;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;

    status = getUserAndSessInput(specifier, &user, &sessInput);
    BailIfFailed(status);

    status =
        getSess(&user, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    if (oops == UserMgr::OutstandOps::Inc) {
        thisSess->incOutstandOps();
    } else {
        assert(oops == UserMgr::OutstandOps::Dec);
        thisSess->decOutstandOps();
    }

CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::trackOutstandOps(XcalarApiUdfContainer *udfContainer,
                          UserMgr::OutstandOps oops)
{
    return trackOutstandOps(&udfContainer->userId,
                            udfContainer->sessionInfo.sessionName,
                            oops);
}

Status
UserMgr::trackOutstandOps(XcalarApiUserId *userId,
                          char *sessionName,
                          UserMgr::OutstandOps oops)
{
    Status status = StatusOk;
    SessionMgr::Session *thisSess = NULL;
    XcalarApiSessionInfoInput sessInput;
    PerUsrInfo *perUsrInfo = NULL;

    sessInput.sessionId = 0;
    if (sessionName != NULL) {
        sessInput.sessionNameLength =
            strnlen(sessionName, XcalarApiSessionNameLen + 1);
        strlcpy(sessInput.sessionName,
                sessionName,
                sizeof(sessInput.sessionName));
    } else {
        sessInput.sessionNameLength = 0;
        sessInput.sessionName[0] = '\0';
    }
    status =
        getSess(userId, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::trackOutstandOps failed Usr '%s', session '%s': %s",
                userId->userIdName,
                sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    if (oops == UserMgr::OutstandOps::Inc) {
        thisSess->incOutstandOps();
    } else {
        assert(oops == UserMgr::OutstandOps::Dec);
        thisSess->decOutstandOps();
    }

CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// XXX This needs to handle the case when the Session is not node local.
Status
UserMgr::getSessionContainer(const char *udfUserName,
                             const char *udfSessionName,
                             XcalarApiUdfContainer **retUdfContainer)
{
    XcalarApiUserId userId;
    Status status = StatusUnknown;
    Dag *udfDag;
    *retUdfContainer = NULL;
    verifyOk(
        strStrlcpy(userId.userIdName, udfUserName, sizeof(userId.userIdName)));

    status = UserMgr::get()->getDag(&userId, udfSessionName, &udfDag);
    BailIfFailed(status);
    *retUdfContainer = udfDag->getSessionContainer();

CommonExit:
    return status;
}

// XXX This needs to handle the case when the Session is not node local.
Status
UserMgr::getUdfContainer(const char *udfUserName,
                         const char *udfSessionName,
                         XcalarApiUdfContainer **retUdfContainer)
{
    XcalarApiUserId userId;
    Status status = StatusUnknown;
    Dag *udfDag;
    *retUdfContainer = NULL;
    verifyOk(
        strStrlcpy(userId.userIdName, udfUserName, sizeof(userId.userIdName)));

    status = UserMgr::get()->getDag(&userId, udfSessionName, &udfDag);
    BailIfFailed(status);
    *retUdfContainer = udfDag->getUdfContainer();

CommonExit:
    return status;
}

Status
UserMgr::getDag(const WorkbookSpecifier *specifier, Dag **returnHandle)
{
    Status status;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;
    Dag *sessGraph = NULL;

    status = getUserAndSessInput(specifier, &user, &sessInput);
    BailIfFailed(status);

    status =
        getSess(&user, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    status = thisSess->getDag(&sessGraph);
    *returnHandle = sessGraph;
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::getDag(XcalarApiUserId *userId,
                const char *sessionName,
                Dag **returnHandle)
{
    Status status = StatusUnknown;
    SessionMgr::Session *thisSess;
    XcalarApiSessionInfoInput sessInput;
    PerUsrInfo *perUsrInfo = NULL;
    Dag *sessGraph = NULL;

    sessInput.sessionId = 0;
    if (sessionName != NULL) {
        sessInput.sessionNameLength =
            strnlen(sessionName, XcalarApiSessionNameLen + 1);
        strlcpy(sessInput.sessionName,
                sessionName,
                sizeof(sessInput.sessionName));
    } else {
        sessInput.sessionNameLength = 0;
        sessInput.sessionName[0] = '\0';
    }
    status =
        getSess(userId, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::getDag failed Usr '%s', session '%s': %s",
                userId->userIdName,
                sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    status = thisSess->getDag(&sessGraph);
    *returnHandle = sessGraph;
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// Interface for stand-alone tool - to do workbook upgrade.  A Dag
// (deserialized from protobuf to C) is passed in to this routine. The routine
// fixes-up the Dag, extracts UDFs embedded in any exec-retina nodes in the Dag
// (with UDF names prefixed by retina name), and then reverse-parses the Dag
// into JSON which is passed to queryToDF2Upgrade, which does the upgrade via
// an app.

Status
UserMgr::queryToDF2Upgrade(Dag *queryGraph,
                           XcalarApiUserId *user,
                           XcalarApiSessionInfoInput *sessionInput)
{
    Status status;
    SessionMgr::Session *thisSess = NULL;
    PerUsrInfo *perUsrInfo = NULL;
    char *queryStrOut = NULL;

    status =
        getSess(user, sessionInput, SessOp::Upgrade, &thisSess, &perUsrInfo);

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::queryToDF2Upgrade failed Usr '%s': %s",
                user->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);

    status =
        thisSess->prepQueryForUpgrade(NULL,
                                      queryGraph,
                                      SessionMgr::Session::WorkbookVersion2,
                                      NULL,
                                      &queryStrOut);
    BailIfFailed(status);

    status = thisSess->queryToDF2Upgrade(queryStrOut);
    BailIfFailed(status);

CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    if (queryStrOut) {
        memFree(queryStrOut);
        queryStrOut = NULL;
    }
    return status;
}

// Return the DAG handle given an input user id and session ID. This is intended
// for use by unit tests, though it will evenutally be useful for admin user
// actions. The session may be inactive or active on another node. The requester
// must use the DAG handle properly.

Status
UserMgr::sessionGetDagById(XcalarApiUserId *userId,
                           Dag **sessionGraphHandleOut,
                           uint64_t sessionId)
{
    Status status = StatusUnknown;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiSessionInfoInput sessInput;

    sessInput.sessionId = sessionId;
    sessInput.sessionNameLength = 0;  // no session name, just ID
    sessInput.sessionName[0] = '\0';

    status =
        getSess(userId, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::sessionGetDagsById failed Usr '%s', "
                "sessionID 0x%lX: %s",
                userId->userIdName,
                sessionId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    status = thisSess->getDag(sessionGraphHandleOut);
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

Status
UserMgr::getKvStoreId(XcalarApiUserId *userId,
                      XcalarApiSessionInfoInput *sessionInfo,
                      KvStoreId *kvStoreId)
{
    Status status = StatusUnknown;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiSessionInfoInput sessInput;

    sessInput.sessionId = sessionInfo->sessionId;
    strlcpy(sessInput.sessionName,
            sessionInfo->sessionName,
            sizeof(sessInput.sessionName));
    sessInput.sessionNameLength = sessionInfo->sessionNameLength;

    status =
        getSess(userId, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "UserMgr::getKvStoreId failed Usr '%s': %s",
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    *kvStoreId = thisSess->getKvStoreId();
    status = StatusOk;
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}

// getUserAndSessInput() is a general utility function which translates a
// protobuf workbook specifier to its C equivalents (username and session info).
//
// For WorkbookSpecifier's protobuf definition, see
//     src/include/pb/xcalar/compute/localtypes/Workbook.proto
Status
UserMgr::getUserAndSessInput(const WorkbookSpecifier *specifier,
                             XcalarApiUserId *user,
                             XcalarApiSessionInfoInput *sessInput)
{
    Status status;
    assert(user != NULL);
    assert(sessInput != NULL);

    auto nameSpec = specifier->name();

    if (strlen(nameSpec.workbookname().c_str())) {
        status = strStrlcpy(sessInput->sessionName,
                            nameSpec.workbookname().c_str(),
                            sizeof(sessInput->sessionName));
        BailIfFailed(status);
        sessInput->sessionNameLength = nameSpec.workbookname().size();
    } else {
        memZero(sessInput, sizeof(XcalarApiSessionInfoInput));
    }

    status = strStrlcpy(user->userIdName,
                        nameSpec.username().c_str(),
                        sizeof(user->userIdName));
    if (!status.ok()) {
        xSyslog(ModuleName,
                XlogErr,
                "username '%s'(%lu bytes) is longer than the max (%lu bytes): "
                "%s",
                nameSpec.username().c_str(),
                nameSpec.username().size(),
                sizeof(user->userIdName),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
UserMgr::getKvStoreId(const WorkbookSpecifier *specifier, KvStoreId *kvStoreId)
{
    Status status;
    SessionMgr::Session *thisSess;
    PerUsrInfo *perUsrInfo = NULL;
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;

    status = getUserAndSessInput(specifier, &user, &sessInput);
    BailIfFailed(status);

    status =
        getSess(&user, &sessInput, SessOp::ReadState, &thisSess, &perUsrInfo);
    if (!status.ok()) {
        goto CommonExit;
    }

    assert(perUsrInfo != NULL);
    assert(thisSess != NULL);
    *kvStoreId = thisSess->getKvStoreId();
CommonExit:
    if (perUsrInfo != NULL && thisSess != NULL) {
        perUsrInfo->setSessIdle(thisSess);
    }
    if (perUsrInfo) {
        perUsrInfo->decRef();
        perUsrInfo = NULL;
    }
    return status;
}
