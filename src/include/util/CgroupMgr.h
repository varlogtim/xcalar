// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CGROUP_MGR_H_
#define _CGROUP_MGR_H_

#include "primitives/Primitives.h"
#include "util/SchedulableFsm.h"
#include "util/Atomics.h"

class CgroupMgr final
{
  public:
    static constexpr const size_t PathLength = 128;
    static constexpr const size_t CgConfigPathLength = 2 * PathLength;
    static constexpr const size_t CgroupControllerCount = 16;

    static constexpr const char *CgroupFuncNameKey = "func";
    static constexpr const char *CgroupFuncSet = "setCgroup";
    static constexpr const char *CgroupFuncGet = "getCgroup";
    static constexpr const char *CgroupFuncInit = "initCgroups";
    static constexpr const char *CgroupFuncList = "listCgroups";
    static constexpr const char *CgroupFuncRefreshMixedMode =
        "refreshMixedModeCgroups";

    static constexpr const char *CgroupNameXce = "xcalar_xce";
    static constexpr const char *CgroupNameSysXpu = "xcalar_sys_xpus";
    static constexpr const char *CgroupNameUsrXpu = "xcalar_usr_xpus";
    static constexpr const char *CgroupNameSysClass = "sys";
    static constexpr const char *CgroupNameUsrClass = "usr";
    static constexpr const char *CgroupNameSched0 = "sched0";
    static constexpr const char *CgroupNameSched1 = "sched1";
    static constexpr const char *CgroupNameSched2 = "sched2";

    static constexpr const char *CgroupCtrlMemory = "memory";
    static constexpr const char *CgroupCtrlCpu = "cpu";
    static constexpr const char *CgroupCtrlCpuset = "cpuset";
    static constexpr const char *CgroupCtrlCpuacct = "cpuacct";

    static constexpr const char *CgroupFuncRetStatusKey = "status";
    static constexpr const char *CgroupFuncRetStatusSuccess = "success";
    static constexpr const char CgroupControllerInputDelimit = ':';

    // Called via XcalaAPIs.
    enum class Type {
        System,
        External,
    };
    MustCheck Status process(const char *jsonInput,
                             char **retJsonOutput,
                             Type type);
    MustCheck Status parseOutput(const char *outStr);

    static MustCheck Status childNodeClassify(pid_t pid,
                                              char *processClass,
                                              char *processSched);
    static int CgroupPathCount;

    MustCheck static CgroupMgr *get() { return instance; }

    static MustCheck bool enabled();

    // Called during cluster bootstrap.
    static MustCheck Status init();

    // Called during cluster shutdown.
    void destroy();

  private:
    static constexpr const char *ModuleName = "CgroupMgr";

    static constexpr const char *CgroupNameKey = "cgroupName";
    static constexpr const char *CgroupCtrlKey = "cgroupController";
    static constexpr const char *CgroupParamsKey = "cgroupParams";
    static constexpr const char *CgroupNs = "/cgroups";
    static constexpr const NodeId DlmNode = 0;

    static CgroupMgr *instance;

    enum class CgroupAppState : int32_t {
        None,
        InProgress,
    };
    Atomic32 cgroupAppInProgress_;

    MustCheck Status parseSingleOutStr(const char *cgroupsAppName,
                                       const char *appOutStr);

    MustCheck bool runCgroupApp();
    void cgroupAppDone();

    CgroupMgr()
    {
        atomicWrite32(&cgroupAppInProgress_, (int32_t) CgroupAppState::None);
    }

    static char CgroupControllerPaths[CgroupControllerCount]
                                     [CgConfigPathLength];

    ~CgroupMgr() = default;

    CgroupMgr(const CgroupMgr &) = delete;
    CgroupMgr &operator=(const CgroupMgr &) = delete;

    class CgroupInitFsmStart : public FsmState
    {
      public:
        CgroupInitFsmStart(SchedulableFsm *schedFsm)
            : FsmState("CgroupInitFsmStart", schedFsm)
        {
        }
        virtual TraverseState doWork();
    };

    class CgroupInitFsmCompletion : public FsmState
    {
      public:
        CgroupInitFsmCompletion(SchedulableFsm *schedFsm)
            : FsmState("CgroupInitFsmCompletion", schedFsm)
        {
        }
        virtual TraverseState doWork();
    };

  public:
    class CgroupInitFsm : public SchedulableFsm
    {
        friend class CgroupInitFsmStartWork;
        friend class CgroupInitFsmCompletion;

      public:
        CgroupInitFsm()
            : SchedulableFsm("CgroupInitFsm", NULL),
              start_(this),
              completion_(this)
        {
            waitSem_.init(0);
            doneSem_.init(0);
            setNextState(&start_);
            atomicWrite64(&done_, 0);
        }
        virtual void done();

        CgroupInitFsmStart start_;
        CgroupInitFsmCompletion completion_;
        Semaphore waitSem_;
        Status retStatus_ = StatusOk;
        Semaphore doneSem_;
        Atomic64 done_;
        static CgroupInitFsm *cgInitFsm;
    };
    CgroupInitFsm cgInitFsm_;
    bool cgInitFsmIssued_ = false;
};

#endif  // _CGROUP_MGR_H_
