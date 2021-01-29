// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _INITTEARDOWN_H_
#define _INITTEARDOWN_H_

#include "primitives/Primitives.h"
#include "sys/XLog.h"
#include "operators/GenericTypes.h"
#include "bc/BufferCache.h"
#include "common/InitLevel.h"

// Flags to control initialization process.
typedef enum {
    InitFlagsNone = 0x00000000,
    InitFlagSkipRestoreUdfs = 0x01000000,  // Don't restore persisted UDFs.
} InitFlags;

//
// Responsible for bootstrapping a Xcalar process by initializing all needed
// modules. Can be used as a "root" singleton with all modules hanging off this
// one.
//
class InitTeardown final
{
  public:
    static MustCheck Status init(InitLevel level,
                                 SyslogFacility syslogFacility,
                                 const char *configFilePath,
                                 const char *pubSigningKeyFilePath,
                                 const char *argv0,
                                 InitFlags flag,
                                 NodeId myNodeId,
                                 int numActiveNodes,
                                 int numActiveOnPhysical,
                                 BufferCacheMgr::Type bcType);
    void teardown();

    MustCheck static InitTeardown *get() { return instance; }

    // Accessor for level_. Set once at the very beginning of init.
    MustCheck InitLevel getInitLevel() const { return level_; }

  private:
    static constexpr const char *ModuleName = "InitTeardown";
    static InitTeardown *instance;
    InitLevel level_;
    bool loadedConfigFiles_;
    bool dataTargetInitialized_;

    MustCheck Status initInternal(SyslogFacility syslogFacility,
                                  const char *configFilePath,
                                  const char *pubSigningKeyFilePath,
                                  const char *argv0,
                                  InitFlags flags,
                                  NodeId myNodeId,
                                  int numActiveNodes,
                                  int numActiveOnPhysical,
                                  BufferCacheMgr::Type bcType);
    void teardownInternal();
    void *earlyTeardown();

    InitTeardown();
    ~InitTeardown();

    // Disallow.
    InitTeardown(const InitTeardown &) = delete;
    InitTeardown &operator=(const InitTeardown &) = delete;
};

#endif  // _INITTEARDOWN_H_
