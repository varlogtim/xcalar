// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <spawn.h>
#include <unistd.h>
#include <signal.h>

#include "ParentChild.h"
#include "parent/Parent.h"
#include "config/Config.h"
#include "util/Atomics.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "app/AppMgr.h"
#include "table/Table.h"
#include "StrlFunc.h"
#include "util/CgroupMgr.h"
#include "libapis/LibApisRecv.h"
#include <iostream>
#include <fstream>
#include <string>

void
ParentChild::setDebugOn()
{
}

//
// Creation of ParentChild objects and their associated processes.
//

// Entry point to ParentChild initialization. Allocates backing struct and calls
// private initialization routines.
Status
ParentChild::create(Id id,
                    Level level,
                    ParentChild::SchedId schedId,
                    const char *userName,
                    const char *childBinPath,
                    ParentChild **childOut)
{
    Status status = StatusOk;
    Status status2 = StatusOk;

    Config *config = Config::get();
    size_t childBinPathSize = strlen(childBinPath) + 1;
    char childBinPathCopy[childBinPathSize];
    verify(strlcpy(childBinPathCopy, childBinPath, childBinPathSize) <
           childBinPathSize);

    // Construct args to exec.
    char argChildNodeId[UInt64MaxStrLen + 1];
    verify(
        (size_t) snprintf(argChildNodeId, sizeof(argChildNodeId), "%lu", id) <=
        sizeof(argChildNodeId));

    char argUsrNodeCount[UInt64MaxStrLen + 1];
    verify((size_t) snprintf(argUsrNodeCount,
                             sizeof(argUsrNodeCount),
                             "%u",
                             config->getActiveNodes()) <=
           sizeof(argUsrNodeCount));

    char argUsrNodeId[UInt64MaxStrLen + 1];
    verify((size_t) snprintf(argUsrNodeId,
                             sizeof(argUsrNodeId),
                             "%u",
                             config->getMyNodeId()) <= sizeof(argUsrNodeId));

    const char *udsPath = LocalMsg::get()->getSockPath();
    struct sockaddr_un argParentUdsPath;
    assert(strlen(udsPath) < sizeof(argParentUdsPath.sun_path));
    verify(strlcpy(argParentUdsPath.sun_path,
                   udsPath,
                   sizeof(argParentUdsPath.sun_path)) <
           sizeof(argParentUdsPath.sun_path));

    std::string cgroup;
    char *argv[12];
    argv[0] = childBinPathCopy;
    argv[1] = (char *) "-i";
    argv[2] = argChildNodeId;
    argv[3] = (char *) "-f";
    argv[4] = config->configFilePath_;
    argv[5] = (char *) "-n";
    argv[6] = argUsrNodeCount;
    argv[7] = (char *) "-u";
    argv[8] = argUsrNodeId;
    argv[9] = (char *) "-s";
    argv[10] = argParentUdsPath.sun_path;
    argv[11] = NULL;

    ParentChild *child = new (std::nothrow) ParentChild(id, level, schedId);
    if (child == NULL) {
        return StatusNoMem;
    }
    status = child->startChildProcess(argv, childBinPathCopy);
    if (status != StatusOk) {
        child->refPut();
    } else {
        *childOut = child;
        xSyslog(ModuleName,
                XlogInfo,
                "Created XPU pid %d, level %u schedId %u: %s",
                child->getPid(),
                static_cast<unsigned>(level),
                static_cast<unsigned>(schedId),
                strGetFromStatus(status));

        if (CgroupMgr::enabled() && CgroupMgr::CgroupPathCount > 0) {
            char *cgroupLvl = NULL;
            char *cgroupSched = NULL;

            if (level == Level::Sys) {
                cgroupLvl = (char *) CgroupMgr::CgroupNameSysClass;
            } else {
                cgroupLvl = (char *) CgroupMgr::CgroupNameUsrClass;
            }

            if (schedId == ParentChild::SchedId::Sched0) {
                cgroupSched = (char *) CgroupMgr::CgroupNameSched0;
            } else if (schedId == ParentChild::SchedId::Sched1) {
                cgroupSched = (char *) CgroupMgr::CgroupNameSched1;
            } else {
                assert(schedId == ParentChild::SchedId::Sched2);
                cgroupSched = (char *) CgroupMgr::CgroupNameSched2;
            }

            status = CgroupMgr::childNodeClassify(child->getPid(),
                                                  cgroupLvl,
                                                  cgroupSched);

            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to move XPUs to cgroup controllers: %s",
                        strGetFromStatus(status));
            }
        }
    }
    return status;
}

// Private, first step of initialization. Just assign defaults to members.
ParentChild::ParentChild(Id id, Level level, ParentChild::SchedId schedId)
    : id_(id),
      state_(State::Init),
      level_(level),
      schedId_(schedId),
      ready_(0),
      readyStatus_(StatusUnknown),
      connection_(NULL),
      currentParent_(NULL)
{
    memZero(&ref_, sizeof(ref_));
    atomicWrite32(&ref_, 1);
}

Status
ParentChild::rmEnvVar(const char *var, char ***envVars)
{
    Status status = StatusOk;
    int currEnvVar;
    for (currEnvVar = 0; environ[currEnvVar] != NULL; currEnvVar++) {
        // Empty loop body intentional
    }
    assert(currEnvVar > 0);
    const size_t numEnvVars = currEnvVar + 1;
    size_t currOutEnvVar = 0;

    *envVars = (char **) memAlloc(numEnvVars * sizeof(**envVars));
    BailIfNull(envVars);

    memset(*envVars, 0, numEnvVars * sizeof(**envVars));
    for (currEnvVar = 0; environ[currEnvVar] != NULL; currEnvVar++) {
        if (strspn(environ[currEnvVar], var) == strlen(var)) {
            xSyslog(ModuleName,
                    XlogWarn,
                    "Removing child's environment variable %s",
                    environ[currEnvVar]);
        } else {
            (*envVars)[currOutEnvVar] = environ[currEnvVar];
            currOutEnvVar++;
        }
    }

CommonExit:
    return status;
}

// Actually starts a child process.
Status
ParentChild::startChildProcess(char **argv, char *childBinPath)
{
    Status status = StatusOk;
    posix_spawnattr_t attr;
    verify(posix_spawnattr_init(&attr) == 0);
    verify(posix_spawnattr_setflags(&attr, POSIX_SPAWN_USEVFORK) == 0);
    char **envVars = NULL;
    if (XcalarConfig::get()->noChildLdPreload_) {
        status = rmEnvVar("LD_PRELOAD=", &envVars);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed startChildProcess removing Env: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        envVars = environ;
    }

    // Block signals in the parent before calling posix_spawn.
    sigset_t signalMask, oldSignalMask;
    sigemptyset(&signalMask);
    sigaddset(&signalMask, SIGINT);

    pthread_sigmask(SIG_BLOCK, &signalMask, &oldSignalMask);

    // Pass through usrnode's environment to child.
    if (posix_spawnp(&this->pid_, childBinPath, NULL, &attr, argv, envVars) !=
        0) {
        status = sysErrnoToStatus(errno);
    } else {
        status = StatusOk;
    }

    // Restore the signal masks in the parent as quickly as possible to
    // reduce signal handling latency. Note that childnode during
    // bootstrapping will set up it's appropriate sigmask.
    pthread_sigmask(SIG_SETMASK, &oldSignalMask, NULL);

    verify(posix_spawnattr_destroy(&attr) == 0);

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed startChildProcess in posix spawn: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (XcalarConfig::get()->noChildLdPreload_) {
        if (envVars != NULL) {
            memFree(envVars);
            envVars = NULL;
        }
    }

    return status;
}

void
ParentChild::refGet()
{
    assert(atomicRead32(&ref_) > 0);
    atomicInc32(&ref_);
}

void
ParentChild::refPut()
{
    assert(atomicRead32(&ref_) > 0);
    if (atomicDec32(&ref_) == 0) {
        assert(connection_ == NULL);
        delete this;
    }
}

unsigned
ParentChild::refPeek()
{
    return atomicRead32(&ref_);
}

//
// Handling the death of a child.
//

ParentChild::~ParentChild()
{
    assert(connection_ == NULL);
    assert(atomicRead32(&ref_) == 0);
    assert(state_ != State::Ready);
}

// Only way to delete a ParentChild object. Terminate the child process if
// it still exists. Free all associated data structures and delete
// ParentChild object. Must only be invoked by Parent.
void
ParentChild::shootdown()
{
    stateLock_.lock();

    State prevState = this->state_;
    this->state_ = State::Terminated;

    if (prevState < State::UnexpectedDeath) {
        // There is no point shutting down a child. Just shoot it down.
        kill();
    }

    connectionLock_.lock();
    if (connection_ != NULL) {
        connection_->setHandler(NULL);
        connection_->refPut();
        connection_ = NULL;
    }
    connectionLock_.unlock();
    stateLock_.unlock();
}

// Forcefully terminate child process. Must be holding lock.
void
ParentChild::kill()
{
    if (this->pid_ != 0) {
        // Don't really care if this fails. If we can't kill this child,
        // we'll just orphan him.
        ::kill(this->pid_, SIGKILL);
    }
}

// Child process has died prematurely.
void
ParentChild::onUnexpectedDeath()
{
    stateLock_.lock();

    if (this->state_ >= State::UnexpectedDeath) {
        stateLock_.unlock();
        return;
    }

    // Somebody might be waiting for this child. Pass on the bad news.
    this->readyStatus_ = StatusChildTerminated;

    this->state_ = State::UnexpectedDeath;
    this->ready_.post();
    this->pid_ = 0;  // Prevents later killing of this PID.

    connectionLock_.lock();
    if (connection_ != NULL) {
        // Not gonna need this anymore.
        connection_->setHandler(NULL);
        connection_->refPut();
        connection_ = NULL;
    }
    connectionLock_.unlock();

    IParentNotify *currentParent = currentParent_;
    if (currentParent != NULL) {
        currentParent->refGet();
    }
    stateLock_.unlock();

    if (currentParent != NULL) {
        currentParent->onDeath();
        currentParent->refPut();
    }
}

void
ParentChild::abortConnection(Status status)
{
    stateLock_.lock();

    if (this->state_ >= State::UnexpectedDeath) {
        stateLock_.unlock();
        return;
    }

    this->state_ = State::UnexpectedDeath;

    connectionLock_.lock();
    if (connection_ != NULL) {
        connection_->setHandler(NULL);
        connection_->refPut();
        connection_ = NULL;
    }
    connectionLock_.unlock();

    this->readyStatus_ = status;
    this->ready_.post();

    kill();
    this->pid_ = 0;  // Prevents later killing of this PID.

    IParentNotify *currentParent = currentParent_;
    if (currentParent != NULL) {
        currentParent->refGet();
    }
    stateLock_.unlock();

    if (currentParent != NULL) {
        currentParent->onDeath();
        currentParent->refPut();
    }
}

void
ParentChild::registerParent(IParentNotify *parent)
{
    stateLock_.lock();
    currentParent_ = parent;
    stateLock_.unlock();
}

void
ParentChild::deregisterParent(IParentNotify *parent)
{
    stateLock_.lock();
    assert(currentParent_ == parent);
    currentParent_ = NULL;
    stateLock_.unlock();
}

Status
ParentChild::waitUntilReady(uint64_t timeoutUSecs)
{
    if (this->readyStatus_ != StatusUnknown) {
        return this->readyStatus_;
    }

    Status status = this->ready_.timedWait(timeoutUSecs);
    if (status == StatusOk) {
        status = this->readyStatus_;
    }
    return status;
}

//
// Methods for parent <-> child communication.
//

// Called on receipt of request message *from child*.
void
ParentChild::onRequestMsg(LocalMsgRequestHandler *reqHandler,
                          LocalConnection *connection,
                          const ProtoRequestMsg *request,
                          ProtoResponseMsg *response)
{
    Status status;
    assert(request->target() == ProtoMsgTargetParent);

    switch (request->parent().func()) {
    case ParentFuncConnect:
        stateLock_.lock();
        if (this->state_ != State::Init) {
            // Our internal state about this particular child says it's
            // either already connected, terminated, or not initialized.
            // This is an invalid connect.
            this->readyStatus_ = StatusBusy;
            kill();
        } else {
            this->state_ = State::Ready;

            connectionLock_.lock();
            connection_ = connection;
            connection_->refGet();
            connectionLock_.unlock();

            this->readyStatus_ = StatusOk;
        }

        this->ready_.post();  // We're ready on failure or success.
        stateLock_.unlock();

        response->set_status(this->readyStatus_.code());
        break;

    case ParentFuncAppDone:
    case ParentFuncAppGetGroupId:
    case ParentFuncAppReportNumFiles:
    case ParentFuncAppReportFileError:
    case ParentFuncAppGetOutputBuffer:
    case ParentFuncAppLoadBuffer:
    case ParentFuncXpuSendListToDsts:
        AppMgr::get()->onRequestMsg(reqHandler, connection, request, response);
        return;

    case ParentFuncXdbGetLocalRows:
    case ParentFuncXdbGetMeta:
    case ParentFuncGetRuntimeHistograms:
        if (currentParent_ != NULL) {
            AppMgr::get()->onRequestMsg(reqHandler,
                                        connection,
                                        request,
                                        response);
        } else {
            // XXX TODO
            // This is a hack to support Table cursoring for Op Maps. Need to
            // provide a common infrastructure to manage XPUs.
            status = TableMgr::get()->onRequestMsg(reqHandler,
                                                   connection,
                                                   request,
                                                   response);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogDebug,
                        "Failed XPU SDK API %u: %s",
                        request->parent().func(),
                        strGetFromStatus(status));
            }
        }
        return;

    default:
        DCHECK(false) << "Unexpected function: "
                      << static_cast<int>(request->parent().func());
        response->set_status(StatusCodeUnimpl);
        break;
    }
}

// Treat connection errors as fatal (they're likely to be conn resets). Very
// similar to onUnexpectedDeath except the child isn't confirmed dead (we
// can take care of that!). Only the management connection is fatal on
// closing; all other connections are user level connections and may be
// opened and closed as needed
void
ParentChild::onConnectionClose(LocalConnection *connection, Status status)
{
    connectionLock_.lock();
    bool shouldAbort = connection == connection_;
    connectionLock_.unlock();
    if (shouldAbort) {
        abortConnection(status);
    }
}

// Send message to child and wait for response. Caller must dec ref on
// responseOut.
Status
ParentChild::sendSync(ProtoChildRequest *childRequest,
                      uint64_t timeoutUSecs,
                      LocalConnection::Response **responseOut)
{
    if (responseOut != NULL) {
        *responseOut = NULL;
    }
    LocalConnection::Response *response;

    Status status = sendAsync(childRequest, &response);
    if (status != StatusOk) {
        return status;
    }

    status = response->wait(timeoutUSecs);
    if (status != StatusOk) {
        response->refPut();
        if (status == StatusTimedOut) {
            xSyslog(ModuleName,
                    XlogErr,
                    "%s timed out on child pid %d request %u (timeout %llu "
                    "usecs)",
                    __PRETTY_FUNCTION__,
                    getPid(),
                    childRequest->func(),
                    (unsigned long long) timeoutUSecs);
        }
        return status;
    }

    if (responseOut != NULL) {
        *responseOut = response;
    } else {
        status.fromStatusCode((StatusCode) response->get()->status());
        response->refPut();
    }

    return status;
}

// Pass transaction ID to child nodes, so it can be used to tie operations
// in child nodes with those in the parent, using the same transaction ID to
// tie them together
Status
ParentChild::setTxnContext(ProtoChildRequest *childRequest)
{
    Txn currTxn = Txn::currentTxn();
    assert(currTxn.valid());
    Status status = StatusOk;
    ProtoTxn *protoTxn;

    try {
        protoTxn = new ProtoTxn();
        childRequest->set_allocated_txn(protoTxn);
        protoTxn->set_id(currTxn.id_);
        // XXX need to replace existing Enums with proto enums
        // to avoid this conversion, will be handled in SDK-732
        switch (currTxn.mode_) {
        case Txn::Mode::LRQ:
            protoTxn->set_mode(ProtoTxn_ModeType_LRQ);
            break;
        case Txn::Mode::NonLRQ:
            protoTxn->set_mode(ProtoTxn_ModeType_NonLRQ);
            break;
        case Txn::Mode::Invalid:
            protoTxn->set_mode(ProtoTxn_ModeType_Invalid);
            break;
        default:
            assert(0 && "Invalid Mode while creating protoTxn request");
        }

        protoTxn->set_runtimetype(
            (xcalar::compute::localtypes::XcalarEnumType::RuntimeType)
                currTxn.rtType_);

        switch (currTxn.rtSchedId_) {
        case Runtime::SchedId::Sched0:
            protoTxn->set_schedid(ProtoTxn_SchedType_Sched0);
            break;
        case Runtime::SchedId::Sched1:
            protoTxn->set_schedid(ProtoTxn_SchedType_Sched1);
            break;
        case Runtime::SchedId::Sched2:
            protoTxn->set_schedid(ProtoTxn_SchedType_Sched2);
            break;
        case Runtime::SchedId::Immediate:
            protoTxn->set_schedid(ProtoTxn_SchedType_Immediate);
            break;
        case Runtime::SchedId::MaxSched:
            protoTxn->set_schedid(ProtoTxn_SchedType_MaxSched);
            break;
        default:
            assert(0 && "Invalid SchedId while creating protoTxn request");
        }
    } catch (std::exception &e) {
        return StatusNoMem;
    }
    return status;
}

// Send message to child. Allow caller to deal with response. Caller must dec
// ref on responseOut.
Status
ParentChild::sendAsync(ProtoChildRequest *childRequest,
                       LocalConnection::Response **responseOut)
{
    Status status = StatusOk;
    *responseOut = NULL;
    connectionLock_.lock();
    LocalConnection *connection = connection_;
    if (connection != NULL) {
        connection->refGet();
    }
    connectionLock_.unlock();

    if (connection == NULL) {
        return StatusChildTerminated;
    }

    status = setTxnContext(childRequest);
    if (status != StatusOk) {
        connection->refPut();
        return status;
    }

    ProtoRequestMsg requestMsg;
    requestMsg.set_target(ProtoMsgTargetChild);
    requestMsg.set_allocated_child(childRequest);

    status = connection->sendRequest(&requestMsg, responseOut);
    requestMsg.release_child();
    childRequest->clear_txn();

    connection->refPut();

    return status;
}

// Fetch path to child. Guaranteed non-NULL if parentChildBinPathLoad was
// successfully called.
const char *
ParentChild::childnodeBinGet()
{
    return ChildnodeName;
}
