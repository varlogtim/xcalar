// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// This is a simple pooled allocator, specifically for ParentChilds. When the
// pool is empty, the get call will wait on a runtime semaphore

#ifndef _CHILDPOOL_H_
#define _CHILDPOOL_H_

#include "primitives/Primitives.h"
#include "runtime/Semaphore.h"
#include "runtime/Spinlock.h"
#include "operators/XcalarEvalTypes.h"
#include "runtime/Schedulable.h"
#include "runtime/CondVar.h"

class ParentChild;
class Parent;
class UserDefinedFunction;

// XXX - this is basically the same thing as a SLOB allocator, also known
// internally as a BufferCache
class ChildPool
{
  public:
    struct PooledChild {
        ParentChild *child = NULL;
        bool alive = false;
        bool inUse = false;
    };

    ChildPool();
    MustCheck Status init(size_t numChildren,
                          EvalUdfModuleSet *modules,
                          const char *userIdName,
                          uint64_t sessionId);
    ~ChildPool(){};

    void destroy();

    MustCheck PooledChild *getChild();

    // killChild should be specified if the user of this child was unable to
    // successfully conclude their operations
    void putChild(PooledChild *child, bool killChild);

  private:
    // Singleton dependencies
    Parent *parent_;
    UserDefinedFunction *userDefinedFunction_;

    // Member variables
    Semaphore *sem_;
    Mutex listLock_;
    size_t numChildren_;
    PooledChild *children_;

    class TransModule : public Schedulable
    {
      public:
        TransModule(ParentChild *pchild,
                    PooledChild *pooledChild,
                    EvalUdfModuleSet *modules,
                    const char *userIdName,
                    uint64_t sessionId,
                    Mutex *lock,
                    CondVar *doneCondVar,
                    size_t seqNum,
                    size_t sendCount,
                    size_t *doneCount);

        void run() override;
        void done() override;

        MustCheck Status getStatus() { return retStatus_; }

      private:
        ParentChild *pchild_ = NULL;
        PooledChild *pooledChild_ = NULL;
        EvalUdfModuleSet *modules_;
        const char *userIdName_;
        const uint64_t sessionId_;
        Status retStatus_ = StatusOk;
        Mutex *lock_ = NULL;
        CondVar *doneCondVar_ = NULL;
        size_t seqNum_ = (size_t)(-2);
        size_t sendCount_ = 0;
        size_t *doneCount_ = NULL;
    };

    MustCheck Status transferMod(ParentChild **pchildren,
                                 EvalUdfModuleSet *modules,
                                 const char *userIdName,
                                 uint64_t sessionId);

    ChildPool(const ChildPool &) = delete;
    ChildPool &operator=(const ChildPool &) = delete;
};

#endif  // _CHILDPOOL_H_
