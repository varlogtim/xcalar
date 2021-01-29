// Copyright 2017-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "childpool/ChildPool.h"
#include "parent/Parent.h"
#include "udf/UserDefinedFunction.h"
#include "runtime/Runtime.h"
#include "util/MemTrack.h"

ChildPool::ChildPool() : sem_(NULL), children_(NULL) {}

Status
ChildPool::init(size_t numChildren,
                EvalUdfModuleSet *modules,
                const char *userIdName,
                uint64_t sessionId)
{
    Status status = StatusOk;
    ParentChild **pchildren = NULL;

    numChildren_ = numChildren;

    parent_ = Parent::get();
    assert(parent_ != NULL);

    userDefinedFunction_ = UserDefinedFunction::get();
    assert(userDefinedFunction_ != NULL);

    sem_ = new (std::nothrow) Semaphore(numChildren_);
    BailIfNull(sem_);

    children_ = new (std::nothrow) PooledChild[numChildren_];
    BailIfNull(children_);

    pchildren = (ParentChild **) memAlloc(sizeof(ParentChild *) * numChildren_);
    BailIfNull(pchildren);

    memZero(pchildren, sizeof(ParentChild *) * numChildren_);

    status = parent_->getChildren(pchildren,
                                  numChildren,
                                  ParentChild::Level::User,
                                  ParentChild::getXpuSchedId(
                                      Txn::currentTxn().rtSchedId_));
    BailIfFailed(status);

    status = transferMod(pchildren, modules, userIdName, sessionId);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        destroy();
    }

    if (pchildren != NULL) {
        for (size_t ii = 0; ii < numChildren_; ii++) {
            ParentChild *newChild = pchildren[ii];
            if (newChild != NULL) {
                assert(status != StatusOk);
                newChild->abortConnection(status);
            }
        }
        memFree(pchildren);
    }

    return status;
}

Status
ChildPool::transferMod(ParentChild **pchildren,
                       EvalUdfModuleSet *modules,
                       const char *userIdName,
                       uint64_t sessionId)
{
    Status status = StatusOk;
    Mutex lock;
    CondVar doneCondVar;
    size_t sendCount = 0;
    size_t doneCount = 0;
    bool onRuntime = false;
    TransModule **tmod = NULL;

    if (!modules) {
        for (size_t ii = 0; ii < numChildren_; ii++) {
            PooledChild *thisChild = &children_[ii];
            ParentChild *newChild = pchildren[ii];
            pchildren[ii] = NULL;
            thisChild->child = newChild;
            thisChild->alive = true;
        }
        goto CommonExit;
    }

    tmod = (TransModule **) memAlloc(sizeof(TransModule *) * numChildren_);
    BailIfNull(tmod);
    memZero(tmod, sizeof(TransModule *) * numChildren_);

    for (size_t ii = 0; ii < numChildren_; ii++) {
        tmod[ii] = new (std::nothrow) TransModule(pchildren[ii],
                                                  &children_[ii],
                                                  modules,
                                                  userIdName,
                                                  sessionId,
                                                  &lock,
                                                  &doneCondVar,
                                                  ii,
                                                  numChildren_,
                                                  &doneCount);
        pchildren[ii] = NULL;
        BailIfNull(tmod[ii]);
    }

    if (numChildren_ > 1) {
        onRuntime = true;
    }

    for (size_t ii = 0; ii < numChildren_; ii++) {
        TransModule *curTmod = tmod[ii];
        sendCount++;
        if (onRuntime == true) {
            status = Runtime::get()->schedule(curTmod);
            if (status == StatusOk) {
                continue;
            }
        }
        curTmod->run();
        curTmod->done();
    }

    assert(sendCount == numChildren_);
    lock.lock();
    while (sendCount != doneCount) {
        doneCondVar.wait(&lock);
    }
    lock.unlock();
    assert(sendCount == doneCount);

    for (size_t ii = 0; ii < numChildren_; ii++) {
        TransModule *curTmod = tmod[ii];
        if (curTmod->getStatus() != StatusOk) {
            status = curTmod->getStatus();
            goto CommonExit;
        }
    }

CommonExit:
    if (tmod != NULL) {
        for (size_t ii = 0; ii < numChildren_; ii++) {
            if (tmod[ii] != NULL) {
                delete tmod[ii];
                tmod[ii] = NULL;
            }
        }
        memFree(tmod);
        tmod = NULL;
    }

    return status;
}

ChildPool::TransModule::TransModule(ParentChild *pchild,
                                    PooledChild *pooledChild,
                                    EvalUdfModuleSet *modules,
                                    const char *userIdName,
                                    uint64_t sessionId,
                                    Mutex *lock,
                                    CondVar *doneCondVar,
                                    size_t seqNum,
                                    size_t sendCount,
                                    size_t *doneCount)
    : Schedulable("TransModule"),
      pchild_(pchild),
      pooledChild_(pooledChild),
      modules_(modules),
      userIdName_(userIdName),
      sessionId_(sessionId),
      lock_(lock),
      doneCondVar_(doneCondVar),
      seqNum_(seqNum),
      sendCount_(sendCount),
      doneCount_(doneCount)
{
}

void
ChildPool::TransModule::run()
{
    Status status = StatusOk;
    UserDefinedFunction *udf = UserDefinedFunction::get();
    status = udf->transferModules(pchild_, modules_, userIdName_, sessionId_);
    if (status != StatusOk) {
        pchild_->abortConnection(status);
        goto CommonExit;
    }
    pooledChild_->child = pchild_;
    pooledChild_->alive = true;

CommonExit:
    retStatus_ = status;
}

void
ChildPool::TransModule::done()
{
    lock_->lock();
    assert(*doneCount_ < sendCount_);
    (*doneCount_)++;
    doneCondVar_->signal();
    lock_->unlock();
}

void
ChildPool::destroy()
{
    if (children_ != NULL) {
        for (size_t ii = 0; ii < numChildren_; ii++) {
            PooledChild *thisChild = &children_[ii];
            if (thisChild->child) {
                parent_->putChild(thisChild->child);
            }
        }
        delete[] children_;
    }
    if (sem_ != NULL) {
        delete sem_;
    }
}

ChildPool::PooledChild *
ChildPool::getChild()
{
    sem_->semWait();
    // If this semaphore returns, it means there must be a child available

    listLock_.lock();
    PooledChild *pooledChild = NULL;
    for (size_t ii = 0; ii < numChildren_; ii++) {
        if (children_[ii].alive && !children_[ii].inUse) {
            children_[ii].inUse = true;
            pooledChild = &children_[ii];
            break;
        }
    }
    listLock_.unlock();

    return pooledChild;
}

void
ChildPool::putChild(ChildPool::PooledChild *child, bool killChild)
{
    // This lock is superfluous, since it is a single byte read, but it enforces
    // the notion that the list should be locked around. The alternative
    // would be a memBarrier after setting available
    listLock_.lock();
    child->inUse = false;
    if (killChild) {
        child->child->abortConnection(StatusCanceled);
    }
    listLock_.unlock();

    // This semaphore post must be after the available set.
    // Contract: SemCount <= NumAvailableChildren
    // If we post first, then it's possible for a getChild to wake up, but
    // find no children
    sem_->post();
}
