// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <errno.h>
#include "DedicatedThread.h"
#include "util/System.h"
#include "operators/GenericTypes.h"
#include "runtime/Tls.h"
#include "runtime/Schedulable.h"
#include "runtime/Runtime.h"
#include "Timer.h"
#include "msg/Message.h"

__thread Txn __txn;

thread_local DedicatedThread *DedicatedThread::runningThread = NULL;

DedicatedThread::DedicatedThread(void *(*entryPoint)(void *), void *arg)
    : Thread(),
      schedObj(this),
      clientEntryPoint(entryPoint),
      clientArg(arg),
      txn_()
{
    verify(sem_init(&semSuspend, 0, 0) == 0);
}

DedicatedThread::~DedicatedThread()
{
    assert(this->schedObj.getState() == SchedObject::State::Created ||
           this->schedObj.getState() == SchedObject::State::Done);
    verify(sem_destroy(&semSuspend) == 0);
}

Status
DedicatedThread::create(pthread_t *thread, const pthread_attr_t *attr)
{
    // Pass our TXN ID to the descendant thread
    this->txn_ = Txn::currentTxn();

    int ret = sysThreadCreate(thread,
                              attr,
                              &Thread::threadEntryPointWrapper,
                              static_cast<Thread *>(this));
    if (ret != 0) {
        return sysErrnoToStatus(ret);
    }

    return StatusOk;
}

void  // virtual
DedicatedThread::threadEntryPoint()
{
    runningThread = this;

    state = Thread::State::Running;
    schedObj.setState(SchedObject::State::Running);

    // Inherit txnId from creator
    __txn = txn_;

    void *retVal = clientEntryPoint(clientArg);

    assert(schedObj.getState() == SchedObject::State::Running);
    schedObj.setState(SchedObject::State::Done);
    state = Thread::State::NotRunning;

    // It's difficult to see why the below is safe. This class is never exposed
    // outside of the runtime. There's nobody else to delete it. So we delete
    // right before exitting.
    runningThread = NULL;
    delete this;

    pthread_exit(retVal);

    // pthread_exit should have terminated thread.
    NotReached();
}

////////////////////////////////////////////////////////////////////////////////

//
// Overrides necessary to implement suspend.
//

SchedObject *  // virtual
DedicatedThread::getRunningSchedObj()
{
    return &schedObj;
}

void  // virtual
DedicatedThread::suspendRunningSchedObj()
{
    assert(this == DedicatedThread::getRunningThread());
    assert(state == Thread::State::Running);

    state = Thread::State::NotRunning;

    // Timer wake implementation for DedicatedThreads. We cannot rely on
    // FiberSchedThread waking these up. They must unblock themselves.

    int ret;
    while (true) {
        if (schedObj.usecsAbsWake_ == 0) {
            do {
                ret = sem_wait(&semSuspend);
            } while (ret == -1 && errno == EINTR);
            break;
        } else {
            struct timespec absTimeout;
            absTimeout.tv_sec = schedObj.usecsAbsWake_ / USecsPerSec;
            absTimeout.tv_nsec =
                (schedObj.usecsAbsWake_ % USecsPerSec) * NSecsPerUSec;
            do {
                // @SymbolCheckIgnore
                ret = sem_timedwait(&semSuspend, &absTimeout);
            } while (ret == -1 && errno == EINTR);

            if (ret == -1 && errno == ETIMEDOUT) {
                for (uint8_t ii = 0; ii < Runtime::TotalScheds; ii++) {
                    Timer *timer = Runtime::get()->getTimer(
                        static_cast<Runtime::SchedId>(ii));
                    if (timer) {
                        timer->wakeExpired();
                    }
                }
            } else {
                assert(ret == 0);
                break;  // Resume. semSuspend was posted by call to
                        // makeRunnable.
            }
        }
    }

    state = Thread::State::Running;

    assert(schedObj.getState() == SchedObject::State::Runnable);
    schedObj.setState(SchedObject::State::Running);
}

////////////////////////////////////////////////////////////////////////////////

//
// Implementation of dummy SchedObject to deal with all the scheduling stuff.
//

DedicatedThread::DedicatedThreadSchedObject::DedicatedThreadSchedObject(
    DedicatedThread *thread)
    : SchedObject(NULL, NULL), thr(thread)
{
}

DedicatedThread::DedicatedThreadSchedObject::~DedicatedThreadSchedObject() {}

void  // virtual
DedicatedThread::DedicatedThreadSchedObject::makeNotRunnable()
{
// No-op. Because this uses sem wait/post, there's no synchronization
// necessary.
#ifdef DEBUG
    notRunnableCount++;
#endif

    assert(getState() == SchedObject::State::Running);
    setState(SchedObject::State::NotRunnable);
}

void  // virtual
DedicatedThread::DedicatedThreadSchedObject::rollbackNotRunnable()
{
    assert(getState() == SchedObject::State::NotRunnable);
    setState(SchedObject::State::Running);
}

Status  // virtual
DedicatedThread::DedicatedThreadSchedObject::makeRunnable()
{
    assert(getState() == SchedObject::State::NotRunnable);
    setState(SchedObject::State::Runnable);

    verify(sem_post(&thr->semSuspend) == 0);
    return StatusOk;
}
