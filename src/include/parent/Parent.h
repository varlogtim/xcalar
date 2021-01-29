// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PARENT_H_
#define _PARENT_H_

#include "primitives/Primitives.h"
#include "shmsg/RingBuffer.h"
#include "runtime/Mutex.h"
#include "runtime/Runtime.h"
#include "parent/ParentChild.h"
#include "ns/LibNs.h"

class ParentStats;

class Parent final
{
  public:
    MustCheck static Parent *get() { return instance; }

    static MustCheck Status init();
    MustCheck Status initXpuCache();
    void destroy();
    MustCheck Status getChildren(ParentChild **childOut,
                                 size_t numChildren,
                                 ParentChild::Level level,
                                 ParentChild::SchedId schedId);
    void putChild(ParentChild *child);
    void onChildTerminate(ParentChild *child);
    MustCheck ILocalMsgHandler *lookupHandler(ParentChild::Id childId,
                                              pid_t pid);

    MustCheck Status getListOfChildren(pid_t **arrayOfChildPids,
                                       size_t *numChildren);

    void shutdownAllChildren();

    // Used by test code
    void killAllChildren();

  private:
    // Calls on dedicated threads timeout to allow for teardown.
    static constexpr uint64_t TimeoutUSecsShutdown = USecsPerSec * 3;
    static constexpr size_t ReaperQueueLength = 1024;
    static constexpr const char *Childnode = "childnode";
    static constexpr const char *ModuleName = "libparent";

    class ProcessUnexpectedChildDeath : public Schedulable
    {
      public:
        ProcessUnexpectedChildDeath(ParentChild *childBody);

        void run() override;
        void done() override;

      private:
        ParentChild *childBody_;
    };

    class GetOneChild : public Schedulable
    {
      public:
        GetOneChild(ParentChild::Level level,
                    ParentChild::SchedId schedId,
                    Mutex *lock,
                    CondVar *doneCondVar,
                    size_t seqNum,
                    size_t sendCount,
                    size_t *doneCount);

        void run() override;
        void done() override;

        MustCheck ParentChild *getChildOut() { return childOut_; }

        MustCheck Status getChildStatus() { return retStatus_; }

      private:
        ParentChild::Level level_ = ParentChild::Level::Max;
        ParentChild::SchedId schedId_ = ParentChild::SchedId::MaxSched;
        ParentChild *childOut_ = NULL;
        Status retStatus_ = StatusOk;
        Mutex *lock_ = NULL;
        CondVar *doneCondVar_ = NULL;
        size_t seqNum_ = (size_t)(-2);
        size_t sendCount_ = 0;
        size_t *doneCount_ = NULL;
    };

    static Parent *instance;

    // Collection of all children on this node.
    ParentChild::HashTable children_;
    size_t totalChildrenCount_;
    size_t childrenCount_[static_cast<unsigned>(ParentChild::Level::Max)]
                         [static_cast<unsigned>(
                             ParentChild::SchedId::MaxSched)] = {{0}};
    Mutex childrenLock_;
    ParentStats *parentStats_;
    RingBuffer *reaperQueue_;
    pthread_t waitpidThread_;
    pthread_t reaperThread_;
    sem_t semWaitpid_;

    // Should we continue to reap children or are we tearing down?
    bool isAlive_;
    bool debugChild_;
    char userName_[LOGIN_NAME_MAX + 1];

    MustCheck ParentChild *checkForChildCorpse(pid_t pid);
    static void sigHandler(int sig);
    MustCheck void *waitpidLoop();
    MustCheck void *reaperLoop();

    Parent();
    ~Parent();

    Parent(const Parent &) = delete;
    Parent &operator=(const Parent &) = delete;
};

#endif  // _PARENT_H_
