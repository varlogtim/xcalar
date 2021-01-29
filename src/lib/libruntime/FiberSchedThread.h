// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef FIBERSCHEDTHREAD_H
#define FIBERSCHEDTHREAD_H
#include <chrono>
#include <deque>
#include <map>
#include <pthread.h>

#include "primitives/Primitives.h"
#include "Thread.h"
#include "runtime/Runtime.h"
#include "std_chrono_compat.h"

class Fiber;
class SchedObject;
class Stats;

//
// A Thread onto which Fibers are scheduled to run. A fixed number of these are
// created on runtime init. They should never block in the kernel and, instead,
// use the runtime synchronization primitives.
//

class FiberSchedThread final : public Thread
{
  public:
    FiberSchedThread() = default;
    ~FiberSchedThread() override;

    Status create(RuntimeType runtimeType, Runtime::SchedId schedId);
    void destroy();

    virtual SchedObject *getRunningSchedObj() override;
    virtual void suspendRunningSchedObj() override;

    void cleanupFiber(Fiber *fiber);

    unsigned getId() const;
    Fiber *getRunningFiber();

    static void main();
    static FiberSchedThread *getRunningThread() { return runningThread_; }

    void markToExit();

    // Runtime performance stats. We aggregate the "finish" samples across our
    // thread pool in order to reduce locking. The runtime infra fetches these
    // stats from each thread and then performs the final aggregation to compose
    // the RPC response.
    void logFinish(Stats *stats);
    void extractRuntimeStats(std::map<std::string, stats::Digest> *map);

  private:
    static constexpr const char *ModuleName = "libruntime";
    static constexpr unsigned AllocFailureSleepUSecs = 100;

    virtual void threadEntryPoint() override;
    void resumeFiber(Fiber *fiber, bool recycleRunningFiber);

    // Disallow.
    FiberSchedThread(const FiberSchedThread &) = delete;
    FiberSchedThread &operator=(const FiberSchedThread &) = delete;

    RuntimeType runtimeType_ = RuntimeTypeInvalid;
    Runtime::SchedId schedId_ = Runtime::SchedId::MaxSched;
    pthread_t thread_;  // Backing pthread.
    bool markedToExit_ = false;

    Fiber *runningFiber_ = nullptr;

    // Preallocated Fiber to switch to once necessary. This is allocated at a
    // convenient time to avoid having to allocate in the middle of a context
    // switch.
    Fiber *preallocedFiber_ = nullptr;

    // This object captures runtime performance statistics that are being
    // aggregated in this thread.
    struct PerfStats {
        struct Sample {
            // TODO(Oleg): remove this hand-written constructor when we've
            //             enabled C++14, as this is an aggregate.
            Sample(std::chrono::steady_clock::time_point tstamp,
                   std::chrono::microseconds duration,
                   std::chrono::microseconds suspendedTime,
                   int64_t suspensions,
                   std::chrono::microseconds lockingTime)
                : tstamp(tstamp),
                  duration(duration),
                  suspendedTime(suspendedTime),
                  suspensions(suspensions),
                  lockingTime(lockingTime)
            {
            }

            std::chrono::steady_clock::time_point tstamp;
            std::chrono::microseconds duration;
            std::chrono::microseconds suspendedTime;
            int64_t suspensions = 0;
            std::chrono::microseconds lockingTime;
        };

        // Per-op stats represented by a sequence of 1s periods.
        struct OpStats {
            // Each period represents 1s of samples.
            struct Period {
                std::deque<Sample> samples;
            };

            std::deque<Period> periods_;

            // Pushes a new data point while pruning everything that is older
            // than 60s.
            void push(const Stats &stats);
        };

        // Lock to protect the rest of the member variables. We need it to
        // synchronize access with the external caller that fetches data from
        // each thread.
        Spinlock lock_;

        // Stats data for the last 60s keyed off the operation's name. We use
        // std::map here because there are very few unique op names in the
        // system and we need iterator stability for the fine-grained locking
        // scheme.
        std::map<std::string, OpStats> opStats_;
    } pstats_;

    static thread_local FiberSchedThread *runningThread_;
};

#endif  // FIBERSCHEDTHREAD_H
