// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef RUNTIME_H
#define RUNTIME_H

#include <deque>
#include <map>
#include <new>

#include <assert.h>
#include <pthread.h>
#include "primitives/Primitives.h"
#include "common/InitLevel.h"
#include "operators/GenericTypes.h"
#include "RuntimeEnums.h"
#include "runtime/Mutex.h"
#include "ns/LibNsTypes.h"

//
// Process-wide singleton that initializes all components of Xcalar runtime.
// Allows scheduling of Schedulables (work) on the runtime and creation of
// blockable threads.
//

class FiberSchedThread;
class RuntimeStats;
class FiberCache;
class Scheduler;
class Schedulable;
class Timer;
class SchedObject;
struct XcalarApiRuntimeSetParamInput;

namespace stats
{
// Represents a partial digest of the runtime perf stats. The threads keep track
// of the Schedulable objects (i.e. fibers) that they execute and can furnish
// the last N seconds of samples.
struct Digest {
    // The number of suspensions that the given fiber had suffered.
    std::deque<int64_t> suspensions;

    // These numbers represent various timings in us.
    std::deque<int64_t> duration, suspendedTime, infraTime;
};
}  // namespace stats

// Runtime by default will have 3 schedulers enabled.
// -----------------------------------------------------------------------------
// SchedId      Type            CpusReservedInPct
// -----------------------------------------------------------------------------
// Sched0       Throughput                  100
// Sched1       Latency/Throughput          100
//              (default Latency)
// Sched2       Latency                     100
// Immediate    Immediate                   100
// -----------------------------------------------------------------------------

// So whether the deployment is for XDP or VDW use case, the pre-condition for
// changing the Scheduler configuration are,
// * Sufficient CPU resources on the host server.
// * Symmetric/same core count on all nodes in the cluster.
//
// Note that the sum of cpusReservedInPercent for the schedulers can exceed 100
// percent. It's up to the user to compute the resource partitioning for
// at least or at most X% of CPU resource per scheduler.

class Runtime final
{
    friend class LibRuntimeGvm;

  public:
    static constexpr const char *NameSchedId0 = "Scheduler-0";
    static constexpr const char *NameSchedId1 = "Scheduler-1";
    static constexpr const char *NameSchedId2 = "Scheduler-2";
    static constexpr const char *NameImmediate = "Immediate";
    static constexpr const char *NameSendMsgNw = "SendMsgNw";
    static constexpr const char *NameAsyncPager = "AsyncPager";
    static constexpr const uint32_t MaxCpusReservedInPct = 100;
    static constexpr const uint32_t MaxImmediateCpusReservedInPct = 400;

    enum class SchedId {
        Sched0 = 0,  // Sched0 is always of Type Throughput for now
        Sched1,      // Throughput or Latency type
        Sched2,      // Throughput or Latency type
        Immediate,
        SendMsgNw,
        AsyncPager,
        MaxSched,
    };

    static constexpr const uint8_t TotalFastPathScheds =
        static_cast<uint8_t>(SchedId::Immediate);

    // Total schedulers exposed to SDK.
    static constexpr const uint8_t TotalSdkScheds =
        static_cast<uint8_t>(SchedId::Immediate) + 1;

    static constexpr const uint8_t TotalScheds =
        static_cast<uint8_t>(SchedId::MaxSched);

    // Accessors.
    static MustCheck Runtime *get() { return Runtime::instance; }

    // Composite objects.
    MustCheck Scheduler *getSched(SchedId schedId);

    MustCheck FiberCache *getFiberCache() { return fiberCache_; }

    MustCheck RuntimeStats *getStats() { return stats_; }

    static MustCheck SchedId getSchedIdFromName(const char *schedName,
                                                size_t schedNameLen);

    static MustCheck const char *getSchedNameFromId(SchedId schedId);

    MustCheck Timer *getTimer(SchedId schedId);

    MustCheck size_t getFiberCacheSize() const;

    // Constants.
    MustCheck size_t getStackBytes() const { return stackBytes_; }

    MustCheck size_t getStackGuardBytes() const { return stackGuardBytes_; }

    // System state info.
    MustCheck bool isActive() const { return active_; }

    MustCheck RuntimeType getType(SchedId schedId);

    MustCheck unsigned getThreadsCount(SchedId schedId);

    void kickSchedsTxnAbort();

    // Lifecycle
    MustCheck static Status init(InitLevel initLevel);
    void destroy();
    void drainAllRunnables();
    void drainRunnables(SchedId schedId);

    // Scheduling
    MustCheck Status createBlockableThread(pthread_t *thread,
                                           const pthread_attr_t *attr,
                                           void *(*entryPoint)(void *),
                                           void *arg);
    MustCheck Status schedule(Schedulable *schedulable);
    MustCheck Status schedule(Schedulable *schedulable,
                              Runtime::SchedId rtSchedId);

    // Introspection
    MustCheck Schedulable *getRunningSchedulable();
    void assertThreadBlockable();

    // Configuration
    MustCheck Status
    changeThreadsCount(XcalarApiRuntimeSetParamInput *inputParam);
    MustCheck Status
    changeThreadCountLocalHandler(XcalarApiRuntimeSetParamInput *inputParam);

    MustCheck Status changeThreadsCountLocal(SchedId schedId,
                                             RuntimeType rtType,
                                             uint32_t newThreadsCount);
    MustCheck Status setupClusterState();
    void teardownClusterState();
    MustCheck Status tuneWithParams();

    // Extracts runtime perf stats from every fiber-running thread in the system
    // and them into the given map.
    void extractRuntimeStats(std::map<std::string, stats::Digest> *digest);

  private:
    static constexpr const char *ModuleName = "libruntime";
    static constexpr size_t DefaultStackGuardBytes = PageSize;
    static constexpr const unsigned XpuRuntimeThreadCount = 2;

    static constexpr const char *RuntimeMixedModeNs = "/runtimeMixedMode";
    static constexpr const char *RuntimeKeyPrefix = "/sys/runtime";
    static constexpr const char *SchedParamsSuffix = "schedParams";
    static constexpr const char *SchedInfo = "schedInfo";
    static constexpr const char *SchedName = "schedName";
    static constexpr const char *CpuReservedPct = "cpuReservedPct";
    static constexpr const char *SchedRuntimeType = "runtimeType";

    template <typename T>
    struct BlockableThreadArg {
        T *self;
        void *(T::*memberFn)();
    };

    template <typename T>
    static void *blockableThreadWrapper(void *arg)
    {
        BlockableThreadArg<T> *thrArg =
            static_cast<BlockableThreadArg<T> *>(arg);

        T *self = thrArg->self;
        void *(T::*memberFn)() = thrArg->memberFn;

        delete thrArg;

        return (self->*(memberFn))();
    }

    struct PerThreadInfo {
        FiberSchedThread *fSchedThread = NULL;
        PerThreadInfo *next = NULL;
        PerThreadInfo() = default;
        ~PerThreadInfo()
        {
            assert(fSchedThread == NULL);
            assert(next == NULL);
        }
    };

    struct SchedInstance {
        SchedInstance() { verify(pthread_mutex_init(&changeLock_, NULL) == 0); }
        ~SchedInstance()
        {
            assert(scheduler == NULL);
            assert(timer == NULL);
            assert(fSchedThreads == NULL);
            assert(threadsCount == 0);
            verify(pthread_mutex_destroy(&changeLock_) == 0);
        }
        RuntimeType type;
        SchedId schedId;
        Scheduler *scheduler = NULL;
        Timer *timer = NULL;
        PerThreadInfo *fSchedThreads = NULL;
        unsigned threadsCount = 0;
        pthread_mutex_t changeLock_;
    };

    Runtime();
    ~Runtime();
    MustCheck Status initInternal(InitLevel initLevel);
    void removeThreads(SchedInstance *schedInstance, unsigned newThreadsCount);
    MustCheck Status addThreads(SchedInstance *schedInstance,
                                unsigned newThreadsCount);
    MustCheck Status
    saveParamsToKvstore(XcalarApiRuntimeSetParamInput *rtSetParamInput);
    MustCheck Status
    getParamsFromKvstore(XcalarApiRuntimeSetParamInput *retRtSetParamInput);
    MustCheck Status
    issueCgroupChanges(XcalarApiRuntimeSetParamInput *rtSetParamInput);
    MustCheck Status
    changeThreadCountLocalWrap(XcalarApiRuntimeSetParamInput *inputParam);

    // Disallow.
    Runtime(const Runtime &) = delete;
    Runtime &operator=(const Runtime &) = delete;

    // Composite objects.
    FiberCache *fiberCache_ = NULL;
    RuntimeStats *stats_ = NULL;
    SchedInstance schedInstance_[TotalScheds];

    size_t stackBytes_;
    size_t stackGuardBytes_;
    bool active_ = false;
    XcalarApiRuntimeSetParamInput *rtSetParamInput_ = NULL;
    static Runtime *instance;

  public:
    // Function allowing creation of threads that call member functions.
    template <typename T>
    Status createBlockableThread(pthread_t *thread,
                                 pthread_attr_t *attr,
                                 T *self,
                                 void *(T::*memberFn)())
    {
        BlockableThreadArg<T> *thrArg =
            new (std::nothrow) BlockableThreadArg<T>;
        if (thrArg == NULL) {
            return StatusNoMem;
        }

        thrArg->self = self;
        thrArg->memberFn = memberFn;

        return createBlockableThread(thread,
                                     attr,
                                     blockableThreadWrapper<T>,
                                     thrArg);
    }

    template <typename T>
    Status createBlockableThread(pthread_t *thread,
                                 T *self,
                                 void *(T::*memberFn)())
    {
        return createBlockableThread(thread, NULL, self, memberFn);
    }
};

#endif  // RUNTIME_H
