// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <atomic>
#include <immintrin.h>  // _mm_pause
#include <jemalloc/jemalloc.h>
#include <unistd.h>       // __NR_gettid
#include <sys/syscall.h>  // syscall

#include "Thread.h"
#include "FiberSchedThread.h"
#include "DedicatedThread.h"
#include "runtime/Tls.h"
#include "sys/XLog.h"

extern "C" {
// This is a piece of jemalloc's API. The point here is to make the code below
// do the right thing without extra config options or macros.
//
// http://jemalloc.net/jemalloc.3.html
int mallctl(const char *name,
            void *oldp,
            size_t *oldlenp,
            void *newp,
            size_t newlen) __attribute__((__nothrow__, __weak__));
}

namespace
{
// The allocation limit that we will impose on this process' heap.
std::atomic<size_t> _allocationLimit;

// A lock to guard the global variables defined below. We need it for two
// reasons: serialization of arena patching and visibility of the global data
// after the given arena is patched.
std::atomic_flag _lock = ATOMIC_FLAG_INIT;

// Hooks for JE-Malloc - it must remain resident.
extent_hooks_t _jemHooks;

// Original alloc/dealloc functions to which we dispatch.
extent_alloc_t *_originalAlloc = nullptr;
extent_dalloc_t *_originalDalloc = nullptr;

void *wrappingAlloc(extent_hooks_t *extent,
                    void *new_addr,
                    size_t size,
                    size_t alignment,
                    bool *zero,
                    bool *commit,
                    unsigned arena_ind);

bool wrappingDalloc(extent_hooks_t *extent_hooks,
                    void *addr,
                    size_t size,
                    bool committed,
                    unsigned arena_ind);

constexpr const char *ModuleName = "libruntime";

// Configures hooks for the JE-Malloc arena associated with the calling thread
// so that we are able to monitor extent allocations.
//
// Returns 'true' on success.
bool
configureArenaHooks()
{
    // Check whether the executable is linked against je-malloc.
    if (mallctl == nullptr) {
        xSyslog(ModuleName,
                XlogInfo,
                "The application was not linked with je-malloc. There will be "
                "no heap limit checks...");
        return false;
    }

    // Check whether JE-Malloc was compiled with stats.
    {
        bool haveStats;
        size_t len = sizeof(haveStats);
        verify(mallctl("config.stats", &haveStats, &len, nullptr, 0) == 0);
        if (!haveStats) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "The je-malloc version linked to this process does not "
                    "have stats. There will be no heap limit checks...");
            return false;
        }
    }

    // Initialize the arena assigned to this thread and get its index.
    unsigned arenaIdx;
    size_t len = sizeof(arenaIdx);
    verify(mallctl("thread.arena", &arenaIdx, &len, nullptr, 0) == 0);
    char arenaHooksName[128];

    {
        int slen = snprintf(arenaHooksName,
                            sizeof(arenaHooksName),
                            "arena.%u.extent_hooks",
                            arenaIdx);
        assert(slen <= static_cast<int>(sizeof(arenaHooksName)));
        if (slen < 0) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Failed to format the arena's hook name. There will be no "
                    "heap limit checks...");
            return false;
        }
    }

    // Get the hooks for the arena.
    extent_hooks_t *hooks;
    len = sizeof(hooks);
    verify(mallctl(arenaHooksName, &hooks, &len, nullptr, 0) == 0);
    assert(hooks != nullptr);

    while (_lock.test_and_set(std::memory_order_acquire)) _mm_pause();

    // Save the default alloc/dalloc functions on the first call.
    if (_jemHooks.alloc == nullptr) {
        // TODO(Oleg): expose an API to set this from our applications.
        _allocationLimit = 0;

        _jemHooks = *hooks;

        // Backup and patch the alloc/dalloc functions - we will use the hooks
        // object for each arena.
        _originalAlloc = _jemHooks.alloc;
        _originalDalloc = _jemHooks.dalloc;
        _jemHooks.alloc = wrappingAlloc;
        _jemHooks.dalloc = wrappingDalloc;
    }

    // Set the custom arena hook once for each arena - we want to intercept
    // alloc/dealloc calls.
    if (hooks->alloc != wrappingAlloc) {
        auto patched_hooks = &_jemHooks;
        verify(mallctl(arenaHooksName,
                       nullptr,
                       nullptr,
                       &patched_hooks,
                       sizeof(patched_hooks)) == 0);
    }

    _lock.clear();

    return true;
}

void *
wrappingAlloc(extent_hooks_t *extent,
              void *new_addr,
              size_t size,
              size_t alignment,
              bool *zero,
              bool *commit,
              unsigned arena_ind)
{
    // Enforce the allocation limit if it is set.
    size_t allocated;
    size_t len = sizeof(allocated);
    verify(mallctl("stats.allocated", &allocated, &len, nullptr, 0) == 0);
    if (_allocationLimit > 0 && allocated > _allocationLimit) return nullptr;

    return _originalAlloc(extent,
                          new_addr,
                          size,
                          alignment,
                          zero,
                          commit,
                          arena_ind);
}

bool
wrappingDalloc(extent_hooks_t *extent_hooks,
               void *addr,
               size_t size,
               bool committed,
               unsigned arena_ind)
{
    // TODO(Oleg): explore manual accounting - perhaps it will be faster to
    //             count the extents ourselves and compile JE-Malloc without
    //             stats...
    return _originalDalloc(extent_hooks, addr, size, committed, arena_ind);
}
}

Thread::Thread() : state(Thread::State::Invalid), tid_(0) {}

Thread::~Thread()
{
    assert(state == State::NotRunning || state == State::Invalid);
    state = State::Invalid;
}

Thread *  // static
Thread::getRunningThread()
{
    // These are implemented by FiberSchedThread and DedicatedThread so that
    // they can do the lookup quickly when they need to.
    Thread *runningThread = FiberSchedThread::getRunningThread();
    if (runningThread == NULL) {
        runningThread = DedicatedThread::getRunningThread();
    } else {
        assert(DedicatedThread::getRunningThread() == NULL);
    }
    return runningThread;
}

// Interprets arg as Thread * and dispatches to virtual threadEntryPoint.
void *  // static
Thread::threadEntryPointWrapper(void *arg)
{
    configureArenaHooks();

    auto thr = static_cast<Thread *>(arg);
    assert(thr != nullptr);

    __txn = Txn();

    thr->tid_ = syscall(__NR_gettid);

    // Dispatch to the derived class' virtual function.
    thr->threadEntryPoint();

    // main should exit and terminate thread's lifetime
    NotReached();

    return NULL;
}
