// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "parent/Parent.h"
#include "ParentChild.h"
#include "ParentStats.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "util/Stopwatch.h"
#include "runtime/Runtime.h"
#include "msg/Xid.h"
#include "ns/LibNs.h"
#include "gvm/Gvm.h"

Parent *Parent::instance = NULL;

Parent::Parent()
    : totalChildrenCount_(0),
      parentStats_(NULL),
      reaperQueue_(NULL),
      isAlive_(false),
      debugChild_(false)
{
}

Parent::~Parent()
{
#ifdef DEBUG
    assert(totalChildrenCount_ == 0);
    for (unsigned ii = static_cast<unsigned>(ParentChild::Level::Sys);
         ii < static_cast<unsigned>(ParentChild::Level::Max);
         ii++) {
        for (unsigned jj = static_cast<unsigned>(ParentChild::SchedId::Sched0);
             jj < static_cast<unsigned>(ParentChild::SchedId::MaxSched);
             jj++) {
            assert(childrenCount_[ii][jj] == 0);
        }
    }
#endif
}

Status
Parent::init()
{
    Status status;
    bool sigHandlerInstalled = false;

    instance = (Parent *) memAllocExt(sizeof(*instance), ModuleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) Parent();

    //
    // Initialize members to default values.
    //
#ifdef DEBUG
    {
        const char *debug = getenv(ParentChild::EnvDebug);
        const char *debugBreak = getenv(ParentChild::EnvChildBreak);
        if ((debugBreak != NULL && strcmp(debugBreak, "1") == 0) ||
            (debug != NULL && strcmp(debug, "1") == 0)) {
            instance->debugChild_ = true;
            ParentChild::setDebugOn();
        }
    }
#endif  // DEBUG

    status = ParentStats::create(&instance->parentStats_);
    BailIfFailed(status);

    struct passwd *pwd;
    pwd = getpwuid(geteuid());
    if (!pwd) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if (strlcpy(instance->userName_,
                pwd->pw_name,
                sizeof(instance->userName_)) >= sizeof(instance->userName_)) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    //
    // Init child reap stuff.
    //
    status = ringBufCreate(&instance->reaperQueue_,
                           "Parent::reaperQueue_",
                           sizeof(pid_t),
                           ReaperQueueLength);
    BailIfFailed(status);

    verify(sem_init(&instance->semWaitpid_, 0, 0) == 0);

    instance->isAlive_ = true;
    memBarrier();

    status = Runtime::get()->createBlockableThread(&instance->waitpidThread_,
                                                   instance,
                                                   &Parent::waitpidLoop);
    if (status != StatusOk) {
        instance->isAlive_ = false;
        goto CommonExit;
    }

    status = Runtime::get()->createBlockableThread(&instance->reaperThread_,
                                                   instance,
                                                   &Parent::reaperLoop);
    if (status != StatusOk) {
        instance->isAlive_ = false;
        verify(sysThreadJoin(instance->waitpidThread_, NULL) == 0);
        goto CommonExit;
    }

    // Install signal handler for child death.
    struct sigaction action;
    struct sigaction oldAction;

    memZero(&oldAction, sizeof(oldAction));
    memZero(&action, sizeof(action));
    action.sa_handler = Parent::sigHandler;
    action.sa_flags = SA_RESTART;  // Restart interrupted syscalls.

    if (sigaction(SIGCHLD, &action, &oldAction) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    sigHandlerInstalled = true;

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        instance->destroy();
        if (sigHandlerInstalled) {
            verify(sigaction(SIGCHLD, &oldAction, NULL) == 0);
        }
    }
    return status;
}

Status
Parent::initXpuCache()
{
    Status status = StatusOk;
    XcalarConfig *xConf = XcalarConfig::get();
    uint64_t numBootstrapChildren = xConf->xpuBootstrapCachedCount_;

    if (numBootstrapChildren) {
        for (unsigned ii = static_cast<unsigned>(ParentChild::Level::Sys);
             ii < static_cast<unsigned>(ParentChild::Level::Max);
             ii++) {
            for (unsigned jj =
                     static_cast<unsigned>(ParentChild::SchedId::Sched0);
                 jj < static_cast<unsigned>(ParentChild::SchedId::MaxSched);
                 jj++) {
                // prime the XPU cache
                ParentChild *pChild[numBootstrapChildren];
                status =
                    instance->getChildren(pChild,
                                          numBootstrapChildren,
                                          static_cast<ParentChild::Level>(ii),
                                          static_cast<ParentChild::SchedId>(
                                              jj));
                BailIfFailed(status);
                for (uint64_t ii = 0; ii < numBootstrapChildren; ii++) {
                    instance->putChild(pChild[ii]);
                }
            }
        }
    }

CommonExit:
    return status;
}

void
Parent::destroy()
{
    childrenLock_.lock();

    // Forcefully shootdown all children. If shutdown initiated, they shouldn't
    // be in use.
    ParentChild *child;
    while ((child = children_.begin().get()) != NULL) {
        verify(children_.remove(child->getId()) == child);
        assert(totalChildrenCount_ > 0);
        totalChildrenCount_--;
        assert(static_cast<unsigned>(child->getLevel()) <
               static_cast<unsigned>(ParentChild::Level::Max));
        assert(static_cast<unsigned>(child->getSchedId()) <
               static_cast<unsigned>(ParentChild::SchedId::MaxSched));
        assert(childrenCount_[static_cast<unsigned>(child->getLevel())]
                             [static_cast<unsigned>(child->getSchedId())] > 0);
        childrenCount_[static_cast<unsigned>(child->getLevel())]
                      [static_cast<unsigned>(child->getSchedId())]--;
        childrenLock_.unlock();

        child->shootdown();
        child->refPut();
        parentStats_->onChildDeath();

        childrenLock_.lock();
    }

    childrenLock_.unlock();

    if (isAlive_) {
        struct sigaction action;
        memZero(&action, sizeof(action));
        action.sa_handler = SIG_DFL;
        verify(sigaction(SIGCHLD, &action, NULL) == 0);

        isAlive_ = false;
        verify(sysThreadJoin(waitpidThread_, NULL) == 0);
        verify(sysThreadJoin(reaperThread_, NULL) == 0);

        ringBufDelete(&reaperQueue_);
    }

    if (parentStats_ != NULL) {
        parentStats_->del();
    }

    instance->~Parent();
    memFree(instance);
    instance = NULL;

    // XXX Must leak parentStats_ because there is no way to teardown stats.
}

Parent::GetOneChild::GetOneChild(ParentChild::Level level,
                                 ParentChild::SchedId schedId,
                                 Mutex *lock,
                                 CondVar *doneCondVar,
                                 size_t seqNum,
                                 size_t sendCount,
                                 size_t *doneCount)
    : Schedulable("GetOneChild"),
      level_(level),
      schedId_(schedId),
      lock_(lock),
      doneCondVar_(doneCondVar),
      seqNum_(seqNum),
      sendCount_(sendCount),
      doneCount_(doneCount)
{
}

void
Parent::GetOneChild::run()
{
    Status status = StatusOk;
    Parent *parent = Parent::get();
    ParentChild *childBody = NULL;
    ParentChild *child = NULL;

    parent->childrenLock_.lock();

    // Look for an already-created child that isn't in use.
    for (ParentChild::HashTable::iterator it(parent->children_);
         it.get() != NULL;
         it.next()) {
        if (!it.get()->getInUse()) {
            // Parent is the only ref.
            child = it.get();
            child->refGet();

            // Ah shoot, a dead child
            if (child->getState() >= ParentChild::State::UnexpectedDeath ||
                child->getLevel() != level_ ||
                child->getSchedId() != schedId_) {
                child->refPut();
                child = NULL;
                continue;
            }

            child->setInUse(true);
            parent->childrenLock_.unlock();
            goto CommonExit;
        }
    }

    status = ParentChild::create(XidMgr::get()->xidGetNext(),
                                 level_,
                                 schedId_,
                                 parent->userName_,
                                 ParentChild::childnodeBinGet(),
                                 &child);
    if (status != StatusOk) {
        parent->childrenLock_.unlock();
        xSyslog(ModuleName,
                XlogErr,
                "Get one child create failed, level %u schedId %u: %s",
                static_cast<unsigned>(level_),
                static_cast<unsigned>(schedId_),
                strGetFromStatus(status));
        child = NULL;
        goto CommonExit;
    }
    parent->parentStats_->onChildCreate();

    // Get rid of any stale children
    childBody = parent->checkForChildCorpse(child->getPid());

    parent->children_.insert(child);  // Initial ref owned by children_.
    parent->totalChildrenCount_++;
    assert(static_cast<unsigned>(child->getLevel()) <
           static_cast<unsigned>(ParentChild::Level::Max));
    assert(static_cast<unsigned>(child->getSchedId()) <
           static_cast<unsigned>(ParentChild::SchedId::MaxSched));
    parent->childrenCount_[static_cast<unsigned>(child->getLevel())]
                          [static_cast<unsigned>(child->getSchedId())]++;

    // Grab another ref for this method. Will be passed to caller on success.
    child->refGet();
    child->setInUse(true);

    // Must hold lock until child process is registered in children_ to allow
    // early reaping.
    parent->childrenLock_.unlock();

    // Now that the childrenLock has been released, we can safely process our
    // corpse. Note that creating a child and inserting it into children_ must
    // be atomic with respect to childrenLock_. This is why we needed to hold
    // off processing our corpse until now
    if (childBody != NULL) {
        childBody->onUnexpectedDeath();
        childBody->refPut();
        childBody = NULL;
    }

    status = child->waitUntilReady(LocalMsg::timeoutUSecsConfig);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Get one child pid %d waitUntilReady failed, level %u schedId "
                "%u: %s",
                child->getPid(),
                static_cast<unsigned>(level_),
                static_cast<unsigned>(schedId_),
                strGetFromStatus(status));
        child->shootdown();
        parent->parentStats_->onChildDeath();

        parent->childrenLock_.lock();
        if (parent->children_.remove(child->getId()) != NULL) {
            child->refPut();
            assert(parent->totalChildrenCount_ > 0);
            parent->totalChildrenCount_--;
            assert(static_cast<unsigned>(child->getLevel()) <
                   static_cast<unsigned>(ParentChild::Level::Max));
            assert(static_cast<unsigned>(child->getSchedId()) <
                   static_cast<unsigned>(ParentChild::SchedId::MaxSched));
            assert(
                parent->childrenCount_[static_cast<unsigned>(child->getLevel())]
                                      [static_cast<unsigned>(
                                          child->getSchedId())] > 0);
            parent
                ->childrenCount_[static_cast<unsigned>(child->getLevel())]
                                [static_cast<unsigned>(child->getSchedId())]--;
        }
        parent->childrenLock_.unlock();

        child->refPut();
        child = NULL;
        goto CommonExit;
    }

CommonExit:
    childOut_ = child;
    retStatus_ = status;
}

void
Parent::GetOneChild::done()
{
    lock_->lock();
    assert(*doneCount_ < sendCount_);
    (*doneCount_)++;
    doneCondVar_->signal();
    lock_->unlock();
}

Status
Parent::getChildren(ParentChild **childOut,
                    size_t numChildren,
                    ParentChild::Level level,
                    ParentChild::SchedId schedId)
{
    Status status = StatusOk;
    Mutex lock;
    CondVar doneCondVar;
    size_t sendCount = 0;
    size_t doneCount = 0;
    bool onRuntime = false;
    memZero(childOut, sizeof(ParentChild *) * numChildren);
    GetOneChild **goCh = NULL;

    if (static_cast<uint8_t>(schedId) >= ParentChild::MixedModeXpuScheds) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "Get one child create failed, level %u, schedId %u: %s",
                static_cast<unsigned>(level),
                static_cast<unsigned>(schedId),
                strGetFromStatus(status));
        goto CommonExit;
    }

    goCh = (GetOneChild **) memAlloc(sizeof(GetOneChild *) * numChildren);
    if (goCh == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Get one child create failed, level %u, schedId %u: %s",
                static_cast<unsigned>(level),
                static_cast<unsigned>(schedId),
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(goCh, sizeof(GetOneChild *) * numChildren);

    for (size_t ii = 0; ii < numChildren; ii++) {
        goCh[ii] = new (std::nothrow) GetOneChild(level,
                                                  schedId,
                                                  &lock,
                                                  &doneCondVar,
                                                  ii,
                                                  numChildren,
                                                  &doneCount);
        if (goCh[ii] == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Get one child create failed, level %u, schedId %u: %s",
                    static_cast<unsigned>(level),
                    static_cast<unsigned>(schedId),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (numChildren > 1) {
        onRuntime = true;
    }

    for (size_t ii = 0; ii < numChildren; ii++) {
        GetOneChild *curGoch = goCh[ii];
        sendCount++;
        if (onRuntime == true) {
            status = Runtime::get()->schedule(curGoch);
            if (status == StatusOk) {
                continue;
            }
        }
        curGoch->run();
        curGoch->done();
    }

    assert(sendCount == numChildren);
    lock.lock();
    while (sendCount != doneCount) {
        doneCondVar.wait(&lock);
    }
    lock.unlock();
    assert(sendCount == doneCount);

    for (size_t ii = 0; ii < numChildren; ii++) {
        GetOneChild *curGoch = goCh[ii];
        if (curGoch->getChildStatus() != StatusOk) {
            status = curGoch->getChildStatus();
            goto CommonExit;
        } else {
            childOut[ii] = curGoch->getChildOut();
        }
    }

CommonExit:
    if (status != StatusOk) {
        memZero(childOut, sizeof(ParentChild *) * numChildren);
    }

    if (goCh != NULL) {
        for (size_t ii = 0; ii < numChildren; ii++) {
            if (goCh[ii] != NULL) {
                ParentChild *pc = goCh[ii]->getChildOut();
                if (pc != NULL && status != StatusOk) {
                    pc->abortConnection(StatusCanceled);
                }
                delete goCh[ii];
                goCh[ii] = NULL;
            }
        }
        memFree(goCh);
        goCh = NULL;
    }

    return status;
}

void
Parent::putChild(ParentChild *child)
{
    struct rusage rUsage;
    bool resourceLimit = false;
    unsigned childrenCount = 0;

    if (!getrusage(RUSAGE_CHILDREN, &rUsage)) {
        // RUSAGE_CHILDREN
        // Return resource usage statistics for all children of the
        // calling process that have terminated and been waited for.
        // These statistics will include the resources used by
        // grandchildren, and further removed descendants, if all of the
        // intervening descendants waited on their terminated children.
        //
        // ru_maxrss (since Linux 2.6.32)
        // This is the maximum resident set size used (in kilobytes).
        // For RUSAGE_CHILDREN, this is the resident set size of the
        // largest child, not the maximum resident set size of the
        // process tree.
        if (rUsage.ru_maxrss > (long) XcalarConfig::get()->xpuMaxRssInKBs_) {
            resourceLimit = true;
        }
    }

    childrenLock_.lock();

    // Make sure this child was created by Parent (all should be).
    child->setInUse(false);
    assert(static_cast<unsigned>(child->getLevel()) <
           static_cast<unsigned>(ParentChild::Level::Max));
    assert(static_cast<unsigned>(child->getSchedId()) <
           static_cast<unsigned>(ParentChild::SchedId::MaxSched));

    childrenCount = childrenCount_[static_cast<unsigned>(child->getLevel())]
                                  [static_cast<unsigned>(child->getSchedId())];

    if (resourceLimit ||
        childrenCount > XcalarConfig::get()->xpuMaxCachedCount_ ||
        child->getState() >= ParentChild::State::UnexpectedDeath) {
        if (children_.remove(child->getId()) != NULL) {
            // We have to remove it from the hashTable before we can release the
            // lock. Otherwise, since we already marked it as not in use,
            // some other guy can grab this kid we're about to murder
            childrenLock_.unlock();

            xSyslog(ModuleName,
                    XlogInfo,
                    "Reaped child process Pid %u resourceLimit %u "
                    "childrenCount %u state %u",
                    child->getPid(),
                    resourceLimit,
                    childrenCount,
                    child->getState());

            child->shootdown();
            childrenLock_.lock();

            child->refPut();  // Dec ref because we removed from hashTable
            assert(totalChildrenCount_ > 0);
            totalChildrenCount_--;
            assert(childrenCount_[static_cast<unsigned>(child->getLevel())]
                                 [static_cast<unsigned>(child->getSchedId())] >
                   0);
            childrenCount_[static_cast<unsigned>(child->getLevel())]
                          [static_cast<unsigned>(child->getSchedId())]--;
            parentStats_->onChildDeath();
        }
    }

    childrenLock_.unlock();
    child->refPut();  // Dec ref incremented by getChild.
}

ILocalMsgHandler *
Parent::lookupHandler(ParentChild::Id childId, pid_t pid)
{
    ParentChild *child = NULL;
    childrenLock_.lock();
    child = children_.find(childId);
    if (child == NULL || child->getPid() != pid) {
        child = NULL;
        goto CommonExit;
    }

    child->refGet();
CommonExit:
    childrenLock_.unlock();
    return child;
}

//
// Dealing with a dead/stopped child.
//

// Reaps stopped/terminated child processes via waitpid. Places them in
// reaperQueue_ to be processed by reaperLoop.
void *
Parent::waitpidLoop()
{
    while (isAlive_) {
        Status status = sysSemTimedWait(&semWaitpid_, TimeoutUSecsShutdown);
        if (status != StatusOk) {
            assert(status == StatusTimedOut);
            continue;
        }

        // waitpid must be done in a loop because there's no guarantee that one
        // SIGCHLD will be sent per terminated child. When several SIGCHLDs are
        // received "at the same time", they may not each be individually
        // delivered.
        pid_t pid;
        while (true) {
            // Reap a stopped/terminated process.
            int stat;

            // WNOHANG returns immediately if no child has exited.
            // WUNTRACED also returns if a child has stopped (but not traced via
            // ptrace(2)).Status for traced children which have stopped is
            // provided even if this option is not specified.
            pid = waitpid(-1, &stat, WNOHANG | WUNTRACED);
            if (pid == -1 ||  // Error.
                pid == 0) {   // Timeout.
                // Break out of loop once there's no more stopped/terminated
                // processes to reap.
                break;
            }

            // Log what we know about this child stopped/terminated.
            int exitStatus = StatusCodeUnknown;
            if (WIFEXITED(stat)) {
                // WIFEXITED returns true if the child terminated normally, that
                // is, by calling exit(3) or _exit(2), or by returning from
                // main().
                //
                // WEXITSTATUS returns the exit status of the child.
                // This consists of the least  significant 8 bits of the status
                // argument that the child specified in a call to exit(3) or
                // _exit(2) or as the argument for a return statement in main().
                // This macro should be employed only if WIFEXITED returned
                // true.
                exitStatus = WEXITSTATUS(stat);
                xSyslog(ModuleName,
                        XlogInfo,
                        "Reaped child process (PID=%u). Exited with status %d",
                        pid,
                        exitStatus);
            } else if (WIFSIGNALED(stat)) {
                // WIFSIGNALED returns true if the child process was terminated
                // by a signal.
                if (WTERMSIG(stat) != 9) {
                    // WTERMSIG returns the number of the signal that caused
                    // the child process to terminate. This macro should be
                    // employed only if WIFSIGNALED returned true. All Child
                    // processes are sent "kill -9" by default instead of
                    // shutdown now.
                    xSyslog(ModuleName,
                            XlogInfo,
                            "Reaped child process (PID=%u)."
                            " Died due to signal %d",
                            pid,
                            WTERMSIG(stat));
                }
#ifdef WCOREDUMP
                if (WCOREDUMP(stat)) {
                    // WCOREDUMP returns true if the child produced a  core
                    // dump.   This  macro  should  be employed only if
                    // WIFSIGNALED returned true.
                    xSyslog(ModuleName,
                            XlogInfo,
                            "Reaped child process (PID=%u). Core dump produced",
                            pid);
                }
#endif  // WCOREDUMP
            } else if (WIFSTOPPED(stat)) {
                // WIFSTOPPED returns true if the child process was stopped by
                // delivery of a signal; this is possible only if the call was
                // done using WUNTRACED or when the child  is being traced (see
                // ptrace(2)).
                //
                // XPU have been stopped here. Just SIGKILL it. Note that with
                // Cgroups, it's now possible for a task to be stopped when it
                // hits resource limits.
                kill(pid, SIGKILL);
                xSyslog(ModuleName,
                        XlogInfo,
                        "Reaped child process (PID=%u) stopped with signal %d, "
                        "just SIGKILL it",
                        pid,
                        WSTOPSIG(stat));
            } else {
                xSyslog(ModuleName,
                        XlogInfo,
                        "Reaped child process (PID=%u). Cause of death unknown",
                        pid);
            }

            do {
                status = ringBufEnqueue(reaperQueue_,
                                        &pid,
                                        TimeoutUSecsShutdown / USecsPerSec);
                assert(status == StatusOk || status == StatusTimedOut);
            } while (status != StatusOk && isAlive_);
        }
    }

    return NULL;
}

// On (StatusOk && *numChildren > 0), it is caller's responsibility
// to release *arrayOfChildPids
Status
Parent::getListOfChildren(pid_t **arrayOfChildPids, size_t *numChildren)
{
    Status status = StatusOk;
    pid_t *listOfChildren = NULL;

    size_t childCounter = 0;
    size_t numPidsAvailable = 0;
    bool lockHeld = false;

    assert(arrayOfChildPids != NULL);
    assert(*arrayOfChildPids == NULL);
    assert(numChildren != NULL);
    *numChildren = 0;

    childrenLock_.lock();
    lockHeld = true;

    if (totalChildrenCount_ == 0) {
        assert(status == StatusOk);
        assert(*numChildren == 0);
        assert(*arrayOfChildPids == NULL);
        goto CommonExit;
    }

    numPidsAvailable = totalChildrenCount_;

    childrenLock_.unlock();
    lockHeld = false;

    listOfChildren = (pid_t *) memAlloc(sizeof(pid_t) * numPidsAvailable);
    if (listOfChildren == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    childrenLock_.lock();
    lockHeld = true;

    // checking if state change between unlock and lock
    if (totalChildrenCount_ == 0) {
        assert(listOfChildren != NULL);
        memFree(listOfChildren);

        assert(status == StatusOk);
        assert(*numChildren == 0);
        assert(*arrayOfChildPids == NULL);
        goto CommonExit;
    }

    childCounter = 0;
    for (ParentChild::HashTable::iterator it(children_); it.get() != NULL;
         it.next()) {
        ParentChild *child = it.get();
        listOfChildren[childCounter] = child->getPid();

        childCounter++;
        if (childCounter == numPidsAvailable) {
            // children were added between unlock and lock
            // just return first numPidsAvailable children
            break;
        }
    }

    *arrayOfChildPids = listOfChildren;

    // children could have disappeared between lock release for malloc
    assert(childCounter <= numPidsAvailable);
    *numChildren = childCounter;

CommonExit:
    if (lockHeld == true) {
        childrenLock_.unlock();
    }

#ifdef DEBUG
    if (status != StatusOk) {
        assert(*arrayOfChildPids == NULL);
        assert(*numChildren == 0);
    }
#endif  // DEBUG

    return status;
}

void
Parent::shutdownAllChildren()
{
    ProtoChildRequest childShutdownRequest;
    childShutdownRequest.set_func(ChildFuncShutdown);

    childrenLock_.lock();
    for (ParentChild::HashTable::iterator it(children_); it.get() != NULL;
         it.next()) {
        ParentChild *child = it.get();
        Status status = child->sendSync(&childShutdownRequest,
                                        LocalMsg::timeoutUSecsConfig,
                                        NULL);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Failed to shutdown childnode: %s",
                    strGetFromStatus(status));
        }
    }
    childrenLock_.unlock();
}

void
Parent::killAllChildren()
{
    childrenLock_.lock();
    for (ParentChild::HashTable::iterator it(children_); it.get() != NULL;
         it.next()) {
        ParentChild *child = it.get();
        child->kill();
    }
    childrenLock_.unlock();
}

// Check if there there is a child of given pid just lying dead by the sidewalk
// If we find a dead body, remove it from the children_ hashTable and give it to
// the parent for processing
ParentChild *
Parent::checkForChildCorpse(pid_t pid)
{
    ParentChild *childBody = NULL;

    // We need children_ lock
    assert(!childrenLock_.tryLock());

    for (ParentChild::HashTable::iterator it(children_); it.get() != NULL;
         it.next()) {
        ParentChild *child = it.get();
        if (child->getPid() == pid) {
            child->refGet();
            childBody = child;
            verify(children_.remove(child->getId()) != NULL);
            child->refPut();
            assert(totalChildrenCount_ > 0);
            totalChildrenCount_--;
            assert(static_cast<unsigned>(child->getLevel()) <
                   static_cast<unsigned>(ParentChild::Level::Max));
            assert(static_cast<unsigned>(child->getSchedId()) <
                   static_cast<unsigned>(ParentChild::SchedId::MaxSched));
            assert(childrenCount_[static_cast<unsigned>(child->getLevel())]
                                 [static_cast<unsigned>(child->getSchedId())] >
                   0);
            childrenCount_[static_cast<unsigned>(child->getLevel())]
                          [static_cast<unsigned>(child->getSchedId())]--;
            parentStats_->onChildDeath();
            break;
        }
    }

    return childBody;
}

// Deals with clearing dead children from metadata and sending onDeath
// notifications.
void *
Parent::reaperLoop()
{
    ParentChild *childBody = NULL;
    pid_t pid;

    while (isAlive_) {
    Begin:
        Status status = ringBufDequeue(reaperQueue_,
                                       &pid,
                                       sizeof(pid),
                                       TimeoutUSecsShutdown / USecsPerSec);
        if (status != StatusOk) {
            assert(status == StatusTimedOut);
            continue;
        }

        childrenLock_.lock();
        childBody = checkForChildCorpse(pid);
        childrenLock_.unlock();

        if (childBody == NULL) {
            continue;
        }

        static constexpr const size_t TxnAbortMaxRetryCount = 1000;
        static constexpr uint64_t TxnAbortSleepUSec = 1000;
        size_t ii = 0;
        Runtime *rt = Runtime::get();
        ProcessUnexpectedChildDeath *procChild = NULL;
        do {
            if (procChild == NULL) {
                procChild =
                    new (std::nothrow) ProcessUnexpectedChildDeath(childBody);
            }
            if (procChild != NULL) {
                status = Runtime::get()->schedule(procChild);
                if (status == StatusOk) {
                    goto Begin;
                }
            }
            // Kick Txn abort on Schedulables in the hope that some resources
            // will be released.
            rt->kickSchedsTxnAbort();
            ii++;
            sysUSleep(TxnAbortSleepUSec);
        } while (ii < TxnAbortMaxRetryCount);
        buggyPanic("Failed to schedule ProcessUnexpectedChildDeath");
    }

    return NULL;
}

// Handler for SIGCHLD (child process died) signals. Simply notifies waitpid
// thread (can't really do much in signal handlers).
void  // static
Parent::sigHandler(int sig)
{
    assert(Parent::get() != NULL &&
           "Deregister this handler before destroying Parent");

    verify(sem_post(&Parent::get()->semWaitpid_) == 0);
}

Parent::ProcessUnexpectedChildDeath::ProcessUnexpectedChildDeath(
    ParentChild *childBody)
    : Schedulable("ProcessUnexpectedChildDeath"), childBody_(childBody)
{
}

void
Parent::ProcessUnexpectedChildDeath::run()
{
    childBody_->onUnexpectedDeath();
    childBody_->refPut();
    childBody_ = NULL;
}

void
Parent::ProcessUnexpectedChildDeath::done()
{
    delete this;
}
