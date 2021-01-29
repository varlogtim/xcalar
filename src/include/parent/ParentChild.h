// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef PARENTCHILD_H_
#define PARENTCHILD_H_

#include <pthread.h>

#include "primitives/Primitives.h"
#include "IParentNotify.h"
#include "util/AtomicTypes.h"
#include "runtime/Spinlock.h"
#include "runtime/Mutex.h"
#include "runtime/Runtime.h"
#include "localmsg/LocalConnection.h"
#include "localmsg/LocalMsg.h"
#include "localmsg/ILocalMsgHandler.h"

// Parent-side state concerning a child process.
class ParentChild final : public ILocalMsgHandler
{
    friend class Parent;

  public:
    static constexpr const char *EnvSilence = "XLRCHILD_SILENCE";
    static constexpr const char *EnvChildBreak = "XLRCHILD_DEBUG_BREAK";
    static constexpr const char *EnvDebug = "XLRCHILD_DEBUG";

    typedef Xid Id;

    // Various states describing a child process's lifetime.
    enum class State {
        // State should only be undefined on initial creation.
        Undefined,
        // State is init if a child has been spawned but hasn't yet attached.
        Init,
        // Child is ready for use. This is the normal mode of operation.
        Ready,
        // Child process has died unexpectedly.
        UnexpectedDeath,
        // Consumer has finished with the child. Child is being destroyed.
        Terminated
    };

    enum class Level {
        Sys,
        User,
        Max,
    };

    enum class SchedId : uint8_t {
        Sched0 = static_cast<uint8_t>(Runtime::SchedId::Sched0),
        Sched1 = static_cast<uint8_t>(Runtime::SchedId::Sched1),
        Sched2 = static_cast<uint8_t>(Runtime::SchedId::Sched2),
        MaxSched,
    };

    static constexpr const uint8_t MixedModeXpuScheds =
        static_cast<uint8_t>(SchedId::MaxSched);

    static SchedId getXpuSchedId(Runtime::SchedId schedId)
    {
        SchedId xpuSchedId = SchedId::MaxSched;
        switch (schedId) {
        case Runtime::SchedId::Sched0:
            xpuSchedId = SchedId::Sched0;
            break;
        case Runtime::SchedId::Sched1:
            xpuSchedId = SchedId::Sched1;
            break;
        case Runtime::SchedId::Sched2:
            xpuSchedId = SchedId::Sched2;
            break;
        case Runtime::SchedId::Immediate:
            // For Immediates will use Sched2, since XPUs need to map to either
            // Sched0/1/2.
            xpuSchedId = SchedId::Sched2;
            break;
        default:
            assert(0 && "Invalid schedId");
            break;
        }
        return xpuSchedId;
    }

    //
    // Accessors.
    //
    MustCheck Id getId() const { return this->id_; }

    MustCheck State getState() const { return this->state_; }

    MustCheck pid_t getPid() const { return this->pid_; }

    static MustCheck Status create(Id id,
                                   Level level,
                                   SchedId schedId,
                                   const char *userName,
                                   const char *childBinPath,
                                   ParentChild **childOut);
    void abortConnection(Status status);
    void refGet() override;
    void refPut() override;
    unsigned refPeek();

    //
    // Event handlers.
    //
    void onUnexpectedDeath();
    void registerParent(IParentNotify *parent);
    void deregisterParent(IParentNotify *parent);
    void onRequestMsg(LocalMsgRequestHandler *reqHandler,
                      LocalConnection *connection,
                      const ProtoRequestMsg *request,
                      ProtoResponseMsg *response) override;
    void onConnectionClose(LocalConnection *connection, Status status) override;

    //
    // Things parents do with their kids.
    //
    void shootdown();
    MustCheck Status waitUntilReady(uint64_t timeoutUSecs);
    static void setDebugOn();
    MustCheck Status sendSync(ProtoChildRequest *childRequest,
                              uint64_t timeoutUSecs,
                              LocalConnection::Response **responseOut);
    MustCheck Status sendAsync(ProtoChildRequest *childRequest,
                               LocalConnection::Response **responseOut);
    static const char *childnodeBinGet();
    MustCheck Status setTxnContext(ProtoChildRequest *childRequest);

    void setInUse(bool val) { inUse_ = val; }

    MustCheck bool getInUse() const { return inUse_; }

    MustCheck Level getLevel() const { return level_; }

    MustCheck SchedId getSchedId() const { return schedId_; }

  private:
    static constexpr const char *ModuleName = "libparent";
    // Rough estimate on expected number of children in system. Okay to
    // over-estimate.
    static constexpr size_t SlotsParentChild = 61;

    static constexpr const char *ChildnodeName = "childnode";

    Id id_;
    State state_;
    Level level_;
    SchedId schedId_;
    pid_t pid_;
    Semaphore ready_;     // Posted once child "ready" for work.
    Status readyStatus_;  // ready may wake up with a failure.
    LocalConnection *connection_;
    Spinlock connectionLock_;  // Acquire AFTER stateLock_.

    // Pointer to whoever is currently using this child.
    IParentNotify *currentParent_;

    // Gives methods a consistend snapshot of child state. Due to the
    // indeterminate nature of child processes, state should not be considered
    // a reflection of the process's "real" state, just our most up-to-date
    // accounting of it.
    // *** Be careful when holding this lock! Calls into Parent made while
    // *** holding lock should never acquire another lock.
    Spinlock stateLock_;

    Atomic32 ref_;
    IntHashTableHook hook_;

    // Currently, Parent will give this child to only one consumer at a time.
    // Protected by Parent::childrenLock_.
    bool inUse_;

    MustCheck Status startChildProcess(char **argv, char *childBinPath);
    MustCheck Status rmEnvVar(const char *var, char ***envVars);
    void kill();
    MustCheck Status doCgclassify(const char *userName);

    // Call shutdown to teardown and free ParentChild.
    ParentChild(Id id, Level level, SchedId schedId);
    ~ParentChild();

    // Disallow.
    ParentChild(const ParentChild &) = delete;
    ParentChild &operator=(const ParentChild &) = delete;

  public:
    typedef IntHashTable<Id,
                         ParentChild,
                         &ParentChild::hook_,
                         &ParentChild::getId,
                         SlotsParentChild,
                         hashIdentity>
        HashTable;
};

#endif  // PARENTCHILD_H_
