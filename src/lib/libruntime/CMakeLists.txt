add_xce_library(libruntime STATIC
    Runtime.cpp
    Scheduler.cpp
    Fiber.cpp
    Thread.cpp
    FiberSchedThread.cpp
    DedicatedThread.cpp
    Spinlock.cpp
    Semaphore.cpp
    Mutex.cpp
    RwLock.cpp
    RuntimeStats.cpp
    SchedObject.cpp
    FiberCache.cpp
    Timer.cpp
    Waitable.cpp
    CondVar.cpp
    PthreadWrap.cpp
    LibRuntimeGvm.cpp
    Txn.cpp
    Schedulable.cpp
    Stats.cpp)

target_link_libraries(libruntime
    # Xcalar libs
    libsys

    # 3rd-party libs
    stdc++
)
