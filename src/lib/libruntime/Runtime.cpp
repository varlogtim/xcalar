// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <unistd.h>
#include <new>
#include <limits>

#include "runtime/Runtime.h"
#include "RuntimeStats.h"
#include "Scheduler.h"
#include "runtime/Schedulable.h"
#include "FiberSchedThread.h"
#include "FiberCache.h"
#include "DedicatedThread.h"
#include "Scheduler.h"
#include "Timer.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "runtime/Tls.h"
#include "LibRuntimeGvm.h"
#include "gvm/Gvm.h"
#include "libapis/LibApisCommon.h"
#include "strings/String.h"
#include "kvstore/KvStore.h"
#include "util/CgroupMgr.h"
#include "ns/LibNs.h"
#include "runtime/Stats.h"
#include "transport/TransportPage.h"

Runtime *Runtime::instance = NULL;

Runtime::Runtime() {}

Runtime::~Runtime()
{
    assert(active_ == false);
    assert(fiberCache_ == NULL);
    assert(stats_ == NULL);
}

void
Runtime::drainRunnables(SchedId schedId)
{
    SchedInstance *inst = &schedInstance_[static_cast<uint8_t>(schedId)];

    if (!inst->threadsCount) {
        return;
    }

    // wait for queue to be empty
    while (!inst->scheduler->isEmpty()) {
        sysUSleep(100);
    }

    bool threadRunning;
    do {
        threadRunning = false;
        PerThreadInfo *ptinfo = inst->fSchedThreads;
        while (ptinfo) {
            if (ptinfo->fSchedThread->getState() == Thread::State::Running) {
                threadRunning = true;
                break;
            }
            ptinfo = ptinfo->next;
        }
    } while (threadRunning);
}

// this is only called on shutdown when no new runnables should be added
// to the queue
void
Runtime::drainAllRunnables()
{
    for (uint8_t ii = 0; ii < TotalScheds; ii++) {
        drainRunnables(static_cast<SchedId>(ii));
    }
}

void
Runtime::destroy()
{
    active_ = false;
    for (uint8_t ii = 0; ii < TotalScheds; ii++) {
        SchedInstance *inst = &schedInstance_[ii];
        if (inst->fSchedThreads != NULL) {
            removeThreads(inst, 0);
            inst->fSchedThreads = NULL;
        }
        if (inst->scheduler != NULL) {
            delete inst->scheduler;
            inst->scheduler = NULL;
        }
        if (inst->timer != NULL) {
            delete inst->timer;
            inst->timer = NULL;
        }
    }
    if (LibRuntimeGvm::get()) {
        LibRuntimeGvm::get()->destroy();
    }
    if (fiberCache_ != NULL) {
        fiberCache_->destroy();
        delete fiberCache_;
        fiberCache_ = NULL;
    }
    if (stats_ != NULL) {
        delete stats_;
        stats_ = NULL;
    }
    if (rtSetParamInput_ != NULL) {
        delete rtSetParamInput_;
        rtSetParamInput_ = NULL;
    }
    delete this;
    instance = NULL;

    StatsCollector::shutdown();
}

Status  // static
Runtime::init(InitLevel initLevel)
{
    StatsCollector::construct();

    assert(instance == NULL);
    instance = new (std::nothrow) Runtime;
    if (instance == NULL) {
        return StatusNoMem;
    }
    return instance->initInternal(initLevel);
}

Status
Runtime::initInternal(InitLevel initLevel)
{
    Status status = StatusUnknown;
    int ret;
    pthread_attr_t attr;
    void *ignoredAddr;
    XcalarConfig *xcalarConfig = XcalarConfig::get();
    unsigned coreCount = (unsigned) XcSysHelper::get()->getNumOnlineCores();

    active_ = true;

    rtSetParamInput_ = new (std::nothrow) XcalarApiRuntimeSetParamInput();
    if (rtSetParamInput_ == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Insufficient memory to allocate XcalarApiRuntimeSetParamInput "
                "object");
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint8_t ii = 0; ii < TotalSdkScheds; ii++) {
        switch (static_cast<Runtime::SchedId>(ii)) {
        case Runtime::SchedId::Sched0:
            strlcpy(rtSetParamInput_->schedParams[ii].schedName,
                    Runtime::NameSchedId0,
                    sizeof(rtSetParamInput_->schedParams[ii].schedName));
            rtSetParamInput_->schedParams[ii].runtimeType =
                RuntimeType::RuntimeTypeThroughput;
            rtSetParamInput_->schedParams[ii].cpusReservedInPct =
                MaxCpusReservedInPct;
            break;
        case Runtime::SchedId::Sched1:
            strlcpy(rtSetParamInput_->schedParams[ii].schedName,
                    Runtime::NameSchedId1,
                    sizeof(rtSetParamInput_->schedParams[ii].schedName));
            rtSetParamInput_->schedParams[ii].runtimeType =
                RuntimeType::RuntimeTypeLatency;
            rtSetParamInput_->schedParams[ii].cpusReservedInPct =
                MaxCpusReservedInPct;
            break;
        case Runtime::SchedId::Sched2:
            strlcpy(rtSetParamInput_->schedParams[ii].schedName,
                    Runtime::NameSchedId2,
                    sizeof(rtSetParamInput_->schedParams[ii].schedName));
            rtSetParamInput_->schedParams[ii].runtimeType =
                RuntimeType::RuntimeTypeLatency;
            rtSetParamInput_->schedParams[ii].cpusReservedInPct =
                MaxCpusReservedInPct;
            break;
        case Runtime::SchedId::Immediate:
            strlcpy(rtSetParamInput_->schedParams[ii].schedName,
                    Runtime::NameImmediate,
                    sizeof(rtSetParamInput_->schedParams[ii].schedName));
            rtSetParamInput_->schedParams[ii].runtimeType =
                RuntimeType::RuntimeTypeImmediate;
            rtSetParamInput_->schedParams[ii].cpusReservedInPct =
                MaxImmediateCpusReservedInPct;
            break;
        default:
            assert(0 && "Invalid Runtime::Type");
            break;
        }
    }

    stats_ = new (std::nothrow) RuntimeStats;
    if (stats_ == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Insufficient memory to allocate RuntimeStats object");
        status = StatusNoMem;
        goto CommonExit;
    }

    fiberCache_ = new (std::nothrow) FiberCache;
    if (fiberCache_ == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Insufficient memory to allocate FiberCache object");
        status = StatusNoMem;
        goto CommonExit;
    }

    if (initLevel >= InitLevel::UsrNode) {
        status = LibRuntimeGvm::init();
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed LibRuntimeGvm init:%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    //
    // Load stack sizes from pthread.
    //

    stackBytes_ = xcalarConfig->thrStackSize_;
    stackGuardBytes_ = DefaultStackGuardBytes;

    ret = pthread_attr_init(&attr);
    if (ret != 0) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to get pthread attributes: %s",
                strGetFromStatus(sysErrnoToStatus(ret)));
    } else {
        ret = pthread_attr_getstack(&attr, &ignoredAddr, &stackBytes_);
        if (ret != 0) {
            stackBytes_ = xcalarConfig->thrStackSize_;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to get pthread stack size: %s. Using default",
                    strGetFromStatus(sysErrnoToStatus(ret)));
        } else {
            ret = pthread_attr_getguardsize(&attr, &stackGuardBytes_);
            if (ret != 0) {
                stackGuardBytes_ = DefaultStackGuardBytes;
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to get pthread stack guard size: %s. "
                        "Using default",
                        strGetFromStatus(sysErrnoToStatus(ret)));
            }
        }
        verify(pthread_attr_destroy(&attr) == 0);
    }

    if (stackBytes_ == 0) {
        stackBytes_ = xcalarConfig->thrStackSize_;
    }
    if (stackGuardBytes_ == 0) {
        stackGuardBytes_ = DefaultStackGuardBytes;
    }

    xSyslog(ModuleName,
            XlogInfo,
            "Fiber stack size: 0x%llx bytes, "
            "stack guard size: 0x%llx bytes",
            (unsigned long long) stackBytes_,
            (unsigned long long) stackGuardBytes_);

    // Init members that need stack inst.
    status = stats_->init();
    BailIfFailed(status);
    StatsLib::statNonAtomicSet64(stats_->fiberBytes_, stackBytes_);

    status = fiberCache_->init(initLevel);
    BailIfFailed(status);

    for (uint8_t ii = 0; ii < TotalScheds; ii++) {
        SchedInstance *inst = &schedInstance_[ii];
        unsigned threadsCount = 0;
        const char *name;
        RuntimeType type = RuntimeTypeInvalid;

        assert(inst->fSchedThreads == NULL);

        switch (static_cast<SchedId>(ii)) {
        case SchedId::Sched0: {
            if (initLevel <= InitLevel::ChildNode) {
                threadsCount = XpuRuntimeThreadCount;
            } else {
                threadsCount = coreCount;
            }
            name = NameSchedId0;
            type = RuntimeType::RuntimeTypeThroughput;
            break;
        }
        case SchedId::Sched1: {
            if (initLevel <= InitLevel::ChildNode) {
                continue;
            } else {
                threadsCount = coreCount;
            }
            name = NameSchedId1;
            type = RuntimeType::RuntimeTypeLatency;
            break;
        }
        case SchedId::Sched2: {
            if (initLevel <= InitLevel::ChildNode) {
                continue;
            } else {
                threadsCount = coreCount;
            }
            name = NameSchedId2;
            type = RuntimeType::RuntimeTypeLatency;
            break;
        }
        case SchedId::Immediate: {
            if (initLevel <= InitLevel::ChildNode) {
                continue;
            } else {
                threadsCount =
                    coreCount * (MaxImmediateCpusReservedInPct / 100);
            }
            name = NameImmediate;
            type = RuntimeType::RuntimeTypeImmediate;
            break;
        }
        case SchedId::SendMsgNw: {
            if (initLevel <= InitLevel::ChildNode) {
                continue;
            } else {
                threadsCount = XcalarConfig::get()->tpShippersCount_;
            }
            name = NameSendMsgNw;
            type = RuntimeType::RuntimeTypeThroughput;
            break;
        }
        case SchedId::AsyncPager: {
            if (initLevel <= InitLevel::ChildNode) {
                continue;
            } else {
                threadsCount = XcalarConfig::get()->maxAsyncPagingIOs_;
            }
            name = NameAsyncPager;
            type = RuntimeType::RuntimeTypeThroughput;
            break;
        }
        default:
            assert(0 && "Unknown SchedId");
            break;
        }

        inst->type = type;
        inst->schedId = static_cast<SchedId>(ii);
        inst->timer = new (std::nothrow) Timer;
        if (inst->timer == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Insufficient memory to allocate Timer object");
            status = StatusNoMem;
            goto CommonExit;
        }

        inst->scheduler = new (std::nothrow) Scheduler(name, inst->timer);
        if (inst->scheduler == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Insufficient memory to allocate Scheduler %u",
                    ii);
            status = StatusNoMem;
            goto CommonExit;
        }

        status = inst->scheduler->init();
        BailIfFailed(status);

        status = addThreads(inst, threadsCount);
        BailIfFailed(status);

        if (threadsCount != 0) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Runtime scheduler Id %u name %s Type %u created %u "
                    "threads",
                    ii,
                    name,
                    static_cast<unsigned>(inst->type),
                    inst->threadsCount);
        }
    }

    assert(status == StatusOk);
CommonExit:
    if (status != StatusOk) {
        active_ = false;
        for (uint8_t ii = 0; ii < static_cast<uint8_t>(TotalScheds); ii++) {
            SchedInstance *inst = &schedInstance_[ii];
            if (inst->threadsCount) {
                removeThreads(inst, 0);
                inst->fSchedThreads = NULL;
            }
        }
        destroy();
    }
    return status;
}

Status
Runtime::createBlockableThread(pthread_t *thread,
                               const pthread_attr_t *attr,
                               void *(*entryPoint)(void *),
                               void *arg)
{
    DedicatedThread *dedThread =
        new (std::nothrow) DedicatedThread(entryPoint, arg);
    if (dedThread == NULL) {
        return StatusNoMem;
    }

    Status status = dedThread->create(thread, attr);
    if (status != StatusOk) {
        delete dedThread;
    }

    // The thread will delete itself before it exits.
    return status;
}

Status
Runtime::schedule(Schedulable *schedulable, Runtime::SchedId rtSchedId)
{
    Txn txn = Txn::currentTxn();
    schedulable->setTxn(txn);
    Scheduler *scheduler = getSched(rtSchedId);
    SchedObject *schedObj =
        new (std::nothrow) SchedObject(schedulable, scheduler);
    if (schedObj == NULL) {
        return StatusNoMem;
    }

    Status status = schedObj->makeRunnable();
    if (status != StatusOk) {
        delete schedObj;
    }
    return status;
}

// Wrap Schedulable in SchedObject and enqueue into Scheduler.
Status
Runtime::schedule(Schedulable *schedulable)
{
    return schedule(schedulable, Txn::currentTxn().rtSchedId_);
}

Schedulable *
Runtime::getRunningSchedulable()
{
    FiberSchedThread *thr = FiberSchedThread::getRunningThread();
    if (thr == NULL) {
        return NULL;
    }
    SchedObject *schedObj = thr->getRunningSchedObj();
    if (schedObj == NULL) {
        return NULL;
    }
    return schedObj->getSchedulable();
}

void
Runtime::assertThreadBlockable()
{
    assert(FiberSchedThread::getRunningThread() == NULL);
}

size_t
Runtime::getFiberCacheSize() const
{
    return fiberCache_->getSize();
}

Status
Runtime::addThreads(Runtime::SchedInstance *schedInstance,
                    unsigned newThreadsCount)
{
    Status status = StatusOk;
    unsigned jj = 0;
    unsigned curThreadsCount = schedInstance->threadsCount;

    assert(!Runtime::get()->getRunningSchedulable() ||
           (Runtime::get()->getRunningSchedulable() &&
            Txn::currentTxnImmediate()));

    if (curThreadsCount == newThreadsCount) {
        goto CommonExit;
    }

    StatsLib::statAtomicAdd64(stats_->threadsAdded_, newThreadsCount);

    assert(newThreadsCount > curThreadsCount);

    for (jj = 0; jj < newThreadsCount - curThreadsCount; jj++) {
        PerThreadInfo *ptinfo = new (std::nothrow) PerThreadInfo();
        if (!ptinfo) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed addThreads, insufficient memory to allocate "
                    "PerThreadInfo for Scheduler Id %u, name %s, type %u",
                    jj,
                    schedInstance->scheduler->getName(),
                    static_cast<uint8_t>(schedInstance->type));
            status = StatusNoMem;
            goto CommonExit;
        }

        ptinfo->fSchedThread = new (std::nothrow) FiberSchedThread();
        if (!ptinfo->fSchedThread) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed addThreads, insufficient memory to allocate "
                    "PerThreadInfo for Scheduler Id %u, name %s, type %u",
                    jj,
                    schedInstance->scheduler->getName(),
                    static_cast<uint8_t>(schedInstance->type));
            delete ptinfo;
            ptinfo = NULL;
            status = StatusNoMem;
            goto CommonExit;
        }

        status = ptinfo->fSchedThread->create(schedInstance->type,
                                              schedInstance->schedId);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed addThreads for Scheduler Id %u, name %s, type %u: "
                    "%s",
                    jj,
                    schedInstance->scheduler->getName(),
                    static_cast<uint8_t>(schedInstance->type),
                    strGetFromStatus(status));
            delete ptinfo->fSchedThread;
            ptinfo->fSchedThread = NULL;
            delete ptinfo;
            ptinfo = NULL;
            goto CommonExit;
        }

        if (schedInstance->fSchedThreads == NULL) {
            schedInstance->fSchedThreads = ptinfo;
        } else {
            ptinfo->next = schedInstance->fSchedThreads;
            schedInstance->fSchedThreads = ptinfo;
        }
        schedInstance->threadsCount++;
    }

CommonExit:
    if (status != StatusOk) {
        removeThreads(schedInstance, curThreadsCount);
    }
    return status;
}

void
Runtime::removeThreads(Runtime::SchedInstance *schedInstance,
                       unsigned newThreadsCount)
{
    assert(!Runtime::get()->getRunningSchedulable() ||
           (Runtime::get()->getRunningSchedulable() &&
            Txn::currentTxnImmediate()));

    if (schedInstance->threadsCount == newThreadsCount) {
        return;
    }

    StatsLib::statAtomicAdd64(stats_->threadsRemoved_, newThreadsCount);

    assert(schedInstance->threadsCount > newThreadsCount);
    unsigned diff = schedInstance->threadsCount - newThreadsCount;
    PerThreadInfo *ptinfo = NULL;

    ptinfo = schedInstance->fSchedThreads;
    for (unsigned ii = 0; ii < diff; ii++, ptinfo = ptinfo->next) {
        assert(ptinfo != NULL);
        ptinfo->fSchedThread->markToExit();
    }

    if (schedInstance->scheduler != NULL) {
        for (unsigned ii = 0; ii < schedInstance->threadsCount; ii++) {
            schedInstance->scheduler->wakeThread();
        }
    }

    ptinfo = schedInstance->fSchedThreads;
    for (unsigned ii = 0; ii < diff; ii++, ptinfo = ptinfo->next) {
        assert(ptinfo != NULL);
        ptinfo->fSchedThread->destroy();
    }

    ptinfo = schedInstance->fSchedThreads;
    for (unsigned ii = 0; ii < diff; ii++) {
        assert(ptinfo != NULL);
        schedInstance->fSchedThreads = ptinfo->next;
        delete ptinfo->fSchedThread;
        ptinfo->fSchedThread = NULL;
        ptinfo->next = NULL;
        delete ptinfo;
        ptinfo = schedInstance->fSchedThreads;
    }
    schedInstance->threadsCount = newThreadsCount;
}

Status
Runtime::saveParamsToKvstore(XcalarApiRuntimeSetParamInput *inputParam)
{
    char key[XcalarApiMaxKeyLen + 1];
    json_t *runtimeInfo = NULL, *schedArrInfo = NULL, *jsonSched = NULL,
           *jsonSchedName = NULL, *jsonCpuReservedPct = NULL,
           *jsonRtType = NULL;
    int ret;
    char *value = NULL;
    Status status = StatusOk;

    status = strSnprintf(key,
                         sizeof(key),
                         "%s/%s",
                         RuntimeKeyPrefix,
                         SchedParamsSuffix);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to save changes to Kvstore: %s",
                    strGetFromStatus(status));

    runtimeInfo = json_object();
    BailIfNullMsg(runtimeInfo,
                  StatusNoMem,
                  ModuleName,
                  "Failed to save changes to Kvstore: %s",
                  strGetFromStatus(status));

    schedArrInfo = json_array();
    BailIfNullMsg(schedArrInfo,
                  StatusNoMem,
                  ModuleName,
                  "Failed to save changes to Kvstore: %s",
                  strGetFromStatus(status));

    for (uint8_t ii = 0; ii < TotalSdkScheds; ii++) {
        jsonSched = json_object();
        BailIfNullMsg(jsonSched,
                      StatusNoMem,
                      ModuleName,
                      "Failed to save changes to Kvstore: %s",
                      strGetFromStatus(status));

        // sched name
        jsonSchedName =
            json_string(getSchedNameFromId(static_cast<SchedId>(ii)));
        BailIfNullMsg(jsonSchedName,
                      StatusNoMem,
                      ModuleName,
                      "Failed to save changes to Kvstore: %s",
                      strGetFromStatus(status));

        ret = json_object_set_new(jsonSched, SchedName, jsonSchedName);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to save changes to Kvstore: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonSchedName = NULL;

        // runtime type
        jsonRtType = json_string(
            std::to_string(inputParam->schedParams[ii].runtimeType).c_str());
        BailIfNullMsg(jsonRtType,
                      StatusNoMem,
                      ModuleName,
                      "Failed to save changes to Kvstore: %s",
                      strGetFromStatus(status));

        ret = json_object_set_new(jsonSched, SchedRuntimeType, jsonRtType);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to save changes to Kvstore: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonRtType = NULL;

        // CPU reserved percent
        jsonCpuReservedPct = json_string(
            std::to_string(inputParam->schedParams[ii].cpusReservedInPct)
                .c_str());
        BailIfNullMsg(jsonCpuReservedPct,
                      StatusNoMem,
                      ModuleName,
                      "Failed to save changes to Kvstore: %s",
                      strGetFromStatus(status));

        ret =
            json_object_set_new(jsonSched, CpuReservedPct, jsonCpuReservedPct);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to save changes to Kvstore: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonCpuReservedPct = NULL;

        ret = json_array_append_new(schedArrInfo, jsonSched);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to save changes to Kvstore: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonSched = NULL;
    }

    ret = json_object_set_new(runtimeInfo, SchedInfo, schedArrInfo);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to save changes to Kvstore: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    schedArrInfo = NULL;

    value = json_dumps(runtimeInfo, 0);
    BailIfNullMsg(value,
                  StatusNoMem,
                  ModuleName,
                  "Failed to save changes to Kvstore: %s",
                  strGetFromStatus(status));

    status = KvStoreLib::get()->addOrReplace(XidMgr::XidGlobalKvStore,
                                             key,
                                             strlen(key) + 1,
                                             value,
                                             strlen(value) + 1,
                                             true,
                                             KvStoreOptSync);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to save changes to Kvstore: %s",
                    strGetFromStatus(status));

CommonExit:
    if (runtimeInfo != NULL) {
        json_decref(runtimeInfo);
        runtimeInfo = NULL;
    }
    if (schedArrInfo != NULL) {
        assert(status != StatusOk);
        json_decref(schedArrInfo);
        schedArrInfo = NULL;
    }
    if (jsonSched != NULL) {
        assert(status != StatusOk);
        json_decref(jsonSched);
        jsonSched = NULL;
    }
    if (jsonSchedName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonSchedName);
        jsonSchedName = NULL;
    }
    if (jsonCpuReservedPct != NULL) {
        assert(status != StatusOk);
        json_decref(jsonCpuReservedPct);
        jsonCpuReservedPct = NULL;
    }
    if (jsonRtType != NULL) {
        assert(status != StatusOk);
        json_decref(jsonRtType);
        jsonRtType = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

Status
Runtime::issueCgroupChanges(XcalarApiRuntimeSetParamInput *inputParam)
{
    Status status = StatusOk;
    json_t *jsonCgChanges = NULL, *jsonFunc = NULL;
    int ret;
    char *jsonInput = NULL, *retJsonOutput = NULL;
    CgroupMgr *cgMgr = CgroupMgr::get();

    jsonCgChanges = json_object();
    BailIfNullMsg(jsonCgChanges,
                  StatusNoMem,
                  ModuleName,
                  "Failed issueCgroupChanges: %s",
                  strGetFromStatus(status));

    jsonFunc = json_string(CgroupMgr::CgroupFuncRefreshMixedMode);
    BailIfNullMsg(jsonFunc,
                  StatusNoMem,
                  ModuleName,
                  "Failed issueCgroupChanges: %s",
                  strGetFromStatus(status));

    ret = json_object_set_new(jsonCgChanges,
                              CgroupMgr::CgroupFuncNameKey,
                              jsonFunc);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed issueCgroupChanges: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    jsonFunc = NULL;

    jsonInput = json_dumps(jsonCgChanges, 0);
    BailIfNullMsg(jsonInput,
                  StatusNoMem,
                  ModuleName,
                  "Failed issueCgroupChanges: %s",
                  strGetFromStatus(status));

    status =
        cgMgr->process(jsonInput, &retJsonOutput, CgroupMgr::Type::External);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed Cgroups set up: %s",
                    strGetFromStatus(status));

    status = cgMgr->parseOutput(retJsonOutput);
    BailIfFailed(status);

CommonExit:
    if (jsonFunc != NULL) {
        assert(status != StatusOk);
        json_decref(jsonFunc);
        jsonFunc = NULL;
    }
    if (jsonCgChanges != NULL) {
        json_decref(jsonCgChanges);
        jsonCgChanges = NULL;
    }
    if (retJsonOutput != NULL) {
        memFree(retJsonOutput);
        retJsonOutput = NULL;
    }
    if (jsonInput != NULL) {
        memFree(jsonInput);
        jsonInput = NULL;
    }
    return status;
}

Status
Runtime::setupClusterState()
{
    Status status = StatusOk;
    XcalarApiRuntimeSetParamInput paramInput;
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;

    nsId = LibNs::get()->publish(RuntimeMixedModeNs, &status);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed setupClusterState: %s",
                    strGetFromStatus(status));

    status = getParamsFromKvstore(&paramInput);
    if (status != StatusOk) {
        status = saveParamsToKvstore(rtSetParamInput_);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed setupClusterState: %s",
                        strGetFromStatus(status));
    } else if (status == StatusOk) {
        memcpy(rtSetParamInput_,
               &paramInput,
               sizeof(XcalarApiRuntimeSetParamInput));
    }

    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed setupClusterState: %s",
                    strGetFromStatus(status));

CommonExit:
    return status;
}

void
Runtime::teardownClusterState()
{
    Status status = LibNs::get()->removeMatching(RuntimeMixedModeNs);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed teardownClusterState: %s",
                strGetFromStatus(status));
    }
}

Status
Runtime::tuneWithParams()
{
    Status status = StatusOk;

    status = changeThreadCountLocalWrap(rtSetParamInput_);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed tuneWithParams: %s",
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
Runtime::changeThreadCountLocalHandler(
    XcalarApiRuntimeSetParamInput *inputParam)
{
    Status status = StatusOk;

    status = changeThreadCountLocalWrap(rtSetParamInput_);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed tuneWithParams: %s",
                    strGetFromStatus(status));

    // Keep the input params around
    memcpy(rtSetParamInput_, inputParam, sizeof(XcalarApiRuntimeSetParamInput));

CommonExit:
    return status;
}

Status
Runtime::changeThreadCountLocalWrap(XcalarApiRuntimeSetParamInput *inputParam)
{
    Status status = StatusOk;
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned coreCount = MsgMgr::get()->numCoresOnNode(myNodeId);
    Runtime::SchedId schedId[Runtime::TotalSdkScheds];

    for (uint8_t ii = 0; ii < Runtime::TotalSdkScheds; ii++) {
        schedId[ii] = Runtime::SchedId::MaxSched;
    }

    for (uint8_t ii = 0; ii < Runtime::TotalSdkScheds; ii++) {
        XcalarApiSchedParam *schedParam =
            (XcalarApiSchedParam *) ((uintptr_t) inputParam->schedParams +
                                     sizeof(XcalarApiSchedParam) * ii);

        unsigned numThreads = (schedParam->cpusReservedInPct * coreCount) / 100;
        Runtime::SchedId curSchedId = Runtime::SchedId::MaxSched;
        uint8_t jj = 0;

        curSchedId = Runtime::getSchedIdFromName(schedParam->schedName,
                                                 sizeof(schedParam->schedName));
        if (curSchedId == Runtime::SchedId::MaxSched) {
            status = StatusRuntimeSetParamInvalid;
            goto CommonExit;
        }

        for (jj = 0; jj < Runtime::TotalSdkScheds; jj++) {
            if (schedId[jj] == curSchedId) {
                break;
            }
        }

        if (jj == Runtime::TotalSdkScheds) {
            schedId[ii] = curSchedId;
        } else {
            status = StatusRuntimeSetParamInvalid;
            goto CommonExit;
        }

        switch (schedId[ii]) {
        case Runtime::SchedId::Sched0:
            if (schedParam->runtimeType != RuntimeTypeThroughput) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        case Runtime::SchedId::Sched1:
            if (schedParam->runtimeType != RuntimeTypeThroughput &&
                schedParam->runtimeType != RuntimeTypeLatency) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        case Runtime::SchedId::Sched2:
            if (schedParam->runtimeType != RuntimeTypeLatency) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        case Runtime::SchedId::Immediate:
            if (schedParam->runtimeType != RuntimeTypeImmediate) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        default:
            status = StatusRuntimeSetParamInvalid;
            break;
        }

        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to change runtime thread count to %u, "
                    "Scheduler name %s type %u: %s",
                    numThreads,
                    schedParam->schedName,
                    schedParam->runtimeType,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = changeThreadsCountLocal(schedId[ii],
                                         schedParam->runtimeType,
                                         numThreads);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to change runtime thread count to %u, "
                    "Scheduler name %s type %u: %s",
                    numThreads,
                    schedParam->schedName,
                    schedParam->runtimeType,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

Status
Runtime::getParamsFromKvstore(XcalarApiRuntimeSetParamInput *retRtSetParamInput)
{
    Status status = StatusOk;
    char kvKey[XcalarApiMaxKeyLen + 1];
    char *kvValue = NULL;
    size_t kvValueSize = 0;
    json_error_t err;
    json_t *runtimeInfo = NULL, *schedArrInfo = NULL, *jsonSched = NULL,
           *jsonSchedName = NULL, *jsonCpuReservedPct = NULL,
           *jsonRtType = NULL;

    status = strSnprintf(kvKey,
                         sizeof(kvKey),
                         "%s/%s",
                         RuntimeKeyPrefix,
                         SchedParamsSuffix);
    if (status == StatusKvEntryNotFound) {
        goto CommonExit;  // Not necessarily an error scenario
    }
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to get params from Kvstore: %s",
                    strGetFromStatus(status));

    status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                       kvKey,
                                       &kvValue,
                                       &kvValueSize);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to get params from Kvstore: %s",
                    strGetFromStatus(status));

    runtimeInfo = json_loads(kvValue, 0, &err);
    if (runtimeInfo == NULL) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse runtime info json, "
                "source %s line %d, column %d, position %d, '%s': %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text,
                strGetFromStatus(status));
        goto CommonExit;
    }

    schedArrInfo = json_object_get(runtimeInfo, SchedInfo);
    BailIfNullMsg(schedArrInfo,
                  StatusNoMem,
                  ModuleName,
                  "Failed to get params from Kvstore: %s",
                  strGetFromStatus(status));

    if (!json_is_array(schedArrInfo)) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to get params from Kvstore: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (json_array_size(schedArrInfo) != TotalSdkScheds) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to get params from Kvstore: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (uint8_t ii = 0; ii < TotalSdkScheds; ii++) {
        jsonSched = json_array_get(schedArrInfo, ii);
        BailIfNullMsg(jsonSched,
                      StatusNoMem,
                      ModuleName,
                      "Failed to get params from Kvstore: %s",
                      strGetFromStatus(status));

        jsonSchedName = json_object_get(jsonSched, SchedName);
        BailIfNullMsg(jsonSchedName,
                      StatusNoMem,
                      ModuleName,
                      "Failed to get params from Kvstore: %s",
                      strGetFromStatus(status));

        SchedId schedId;
        const char *schedName;
        XcalarApiSchedParam *schedParam;

        schedName = json_string_value(jsonSchedName);
        schedId = getSchedIdFromName(schedName, strlen(schedName));
        schedParam =
            &retRtSetParamInput->schedParams[static_cast<uint8_t>(schedId)];

        if (strlcpy(schedParam->schedName,
                    schedName,
                    sizeof(schedParam->schedName)) >=
            sizeof(schedParam->schedName)) {
            status = StatusOverflow;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to get params from Kvstore: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        jsonCpuReservedPct = json_object_get(jsonSched, CpuReservedPct);
        BailIfNullMsg(jsonCpuReservedPct,
                      StatusNoMem,
                      ModuleName,
                      "Failed to get params from Kvstore: %s",
                      strGetFromStatus(status));

        schedParam->cpusReservedInPct = (uint32_t) std::stoi(
            std::string(json_string_value(jsonCpuReservedPct)));

        jsonRtType = json_object_get(jsonSched, SchedRuntimeType);
        BailIfNullMsg(jsonRtType,
                      StatusNoMem,
                      ModuleName,
                      "Failed to get params from Kvstore: %s",
                      strGetFromStatus(status));

        schedParam->runtimeType =
            (RuntimeType) std::stoi(std::string(json_string_value(jsonRtType)));
    }

CommonExit:
    if (runtimeInfo) {
        json_decref(runtimeInfo);
        runtimeInfo = NULL;
    }
    return status;
}

Status
Runtime::changeThreadsCount(XcalarApiRuntimeSetParamInput *inputParam)
{
    Status status = StatusOk;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    LibNsTypes::NsHandle nsHandle;
    NsObject *obj = NULL;
    LibNs *libNs = LibNs::get();
    bool validHandle = false;

    nsHandle =
        libNs->open(RuntimeMixedModeNs, LibNsTypes::WriterExcl, &obj, &status);
    if (status == StatusAccess) {
        // Runtime Mixed mode change already active.
        status = StatusRuntimeChangeInProgress;
    }
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to open handle to runtime namespace: %s",
                      strGetFromStatus(status));
        goto CommonExit;
    }
    validHandle = true;

    status = saveParamsToKvstore(inputParam);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to set runtime params: %s",
                    strGetFromStatus(status));

    gPayload = (Gvm::Payload *) memAlloc(sizeof(XcalarApiRuntimeSetParamInput) +
                                         sizeof(Gvm::Payload));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  ModuleName,
                  "Failed to set runtime params: %s",
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  ModuleName,
                  "Failed to set runtime params: %s",
                  strGetFromStatus(status));

    memcpy(gPayload->buf, inputParam, sizeof(XcalarApiRuntimeSetParamInput));
    gPayload->init(LibRuntimeGvm::get()->getGvmIndex(),
                   (uint32_t) LibRuntimeGvm::Action::ChangeThreads,
                   sizeof(XcalarApiRuntimeSetParamInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    // No point attempting cleanout, since that may fail too.
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed set runtime params: %s",
                    strGetFromStatus(status));

    if (CgroupMgr::enabled()) {
        status = issueCgroupChanges(inputParam);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed set runtime params: %s",
                        strGetFromStatus(status));
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
    if (validHandle) {
        status = libNs->close(nsHandle, NULL);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to close handle to runtime: %s",
                    strGetFromStatus(status));
        }
        validHandle = false;
    }
    if (obj) {
        memFree(obj);
        obj = NULL;
    }
    return status;
}

Status
Runtime::changeThreadsCountLocal(SchedId schedId,
                                 RuntimeType rtType,
                                 uint32_t newThreadsCount)
{
    Status status = StatusOk;
    bool locked = false;
    StatsLib::statAtomicIncr64(stats_->changeThreadsCount_);

    assert(static_cast<int8_t>(schedId) <
           static_cast<uint8_t>(SchedId::MaxSched));
    SchedInstance *inst = &schedInstance_[static_cast<int8_t>(schedId)];

    uint32_t oldThreadCount = inst->threadsCount;

    if (Runtime::get()->getRunningSchedulable() &&
        !Txn::currentTxnImmediate()) {
        assert(0 &&
               "Cannot be on scheduled on Runtime to change runtime threads. "
               "Exception to this is the immediate scheduler, since we don't "
               "allow changing of threads in immediate scheduler.");
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "Failed changeThreadsCount for Scheduler Id %u, name %s, "
                "type %u from %u to %u: %s",
                static_cast<uint8_t>(schedId),
                inst->scheduler->getName(),
                static_cast<int8_t>(inst->type),
                inst->threadsCount,
                newThreadsCount,
                strGetFromStatus(status));
        goto CommonExit;
    }

    verify(pthread_mutex_lock(&inst->changeLock_) == 0);
    locked = true;

    if (newThreadsCount > inst->threadsCount) {
        status = addThreads(inst, newThreadsCount);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed changeThreadsCount for Scheduler Id %u, name %s, "
                    "type %u from %u to %u: %s",
                    static_cast<uint8_t>(schedId),
                    inst->scheduler->getName(),
                    static_cast<int8_t>(inst->type),
                    inst->threadsCount,
                    newThreadsCount,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (newThreadsCount < inst->threadsCount) {
        // Disallow reducing the number of threads in immediate Runtime,
        // since Fibers changing the Runtime threads ride on immediate
        // Runtime.
        if (inst->schedId == SchedId::Immediate) {
            status = StatusRuntimeSetParamInvalid;
            xSyslog(ModuleName,
                    XlogErr,
                    "Disallow changeThreadsCount for Scheduler Id %u, name %s, "
                    "type %u from %u to %u: %s",
                    static_cast<uint8_t>(schedId),
                    inst->scheduler->getName(),
                    static_cast<int8_t>(inst->type),
                    inst->threadsCount,
                    newThreadsCount,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        removeThreads(inst, newThreadsCount);
    } else {
        // NOOP
        goto CommonExit;
    }
    xSyslog(ModuleName,
            XlogInfo,
            "Runtime Scheduler Id %u, name %s, type %u thread count changed "
            "from %u to %u",
            static_cast<uint8_t>(schedId),
            inst->scheduler->getName(),
            static_cast<int8_t>(inst->type),
            oldThreadCount,
            newThreadsCount);
CommonExit:
    if (locked) {
        verify(pthread_mutex_unlock(&inst->changeLock_) == 0);
    }
    return status;
}

void
Runtime::kickSchedsTxnAbort()
{
    StatsLib::statAtomicIncr64(stats_->kickSchedsTxnAbort_);
    for (uint8_t ii = 0; ii < TotalScheds; ii++) {
        SchedInstance *inst = &schedInstance_[ii];
        if (inst->scheduler != NULL) {
            inst->scheduler->markToAbortSchedObjs();
        }
    }
}

Scheduler *
Runtime::getSched(SchedId schedId)
{
    if (schedId == SchedId::MaxSched) {
        // For now, use SchedId::Sched0.
        schedId = SchedId::Sched0;
    }
    assert(schedId < SchedId::MaxSched);
    return schedInstance_[static_cast<uint8_t>(schedId)].scheduler;
}

Timer *
Runtime::getTimer(SchedId schedId)
{
    if (schedId == SchedId::MaxSched) {
        // For now, use SchedId::Sched0.
        schedId = SchedId::Sched0;
    }
    assert(schedId < SchedId::MaxSched);
    return schedInstance_[static_cast<uint8_t>(schedId)].timer;
}

RuntimeType
Runtime::getType(SchedId schedId)
{
    if (schedId == SchedId::MaxSched) {
        // For now, use SchedId::Sched0.
        schedId = SchedId::Sched0;
    }
    assert(schedId < SchedId::MaxSched);
    return schedInstance_[static_cast<uint8_t>(schedId)].type;
}

unsigned
Runtime::getThreadsCount(SchedId schedId)
{
    if (schedId == SchedId::MaxSched) {
        // For now, use SchedId::Sched0.
        schedId = SchedId::Sched0;
    }
    assert(schedId < SchedId::MaxSched);
    return schedInstance_[static_cast<uint8_t>(schedId)].threadsCount;
}

Runtime::SchedId
Runtime::getSchedIdFromName(const char *schedName, size_t schedNameLen)
{
    Runtime::SchedId schedId = SchedId::MaxSched;
    if (!schedName) {
        schedId = SchedId::MaxSched;
    } else if (!strncmp(schedName, Runtime::NameSchedId0, schedNameLen)) {
        schedId = Runtime::SchedId::Sched0;
    } else if (!strncmp(schedName, Runtime::NameSchedId1, schedNameLen)) {
        schedId = Runtime::SchedId::Sched1;
    } else if (!strncmp(schedName, Runtime::NameSchedId2, schedNameLen)) {
        schedId = Runtime::SchedId::Sched2;
    } else if (!strncmp(schedName, Runtime::NameImmediate, schedNameLen)) {
        schedId = Runtime::SchedId::Immediate;
    } else if (!strncmp(schedName, Runtime::NameSendMsgNw, schedNameLen)) {
        schedId = Runtime::SchedId::SendMsgNw;
    } else if (!strncmp(schedName, Runtime::NameAsyncPager, schedNameLen)) {
        schedId = Runtime::SchedId::AsyncPager;
    } else {
        assert(0 && "Unknown schedName");
    }
    return schedId;
}

const char *
Runtime::getSchedNameFromId(SchedId schedId)
{
    if (schedId == SchedId::Sched0) {
        return Runtime::NameSchedId0;
    } else if (schedId == SchedId::Sched1) {
        return Runtime::NameSchedId1;
    } else if (schedId == SchedId::Sched2) {
        return Runtime::NameSchedId2;
    } else if (schedId == SchedId::Immediate) {
        return Runtime::NameImmediate;
    } else if (schedId == SchedId::SendMsgNw) {
        return Runtime::NameSendMsgNw;
    } else if (schedId == SchedId::AsyncPager) {
        return Runtime::NameAsyncPager;
    } else {
        assert(0 && "Unknown schedId");
        return NULL;
    }
}

void
Runtime::extractRuntimeStats(std::map<std::string, stats::Digest> *digest)
{
    DCHECK(digest != nullptr);

    for (uint8_t idx = 0; idx < TotalScheds; ++idx) {
        auto ptinfo = schedInstance_[idx].fSchedThreads;
        while (ptinfo != nullptr) {
            ptinfo->fSchedThread->extractRuntimeStats(digest);
            ptinfo = ptinfo->next;
        }
    }
}
