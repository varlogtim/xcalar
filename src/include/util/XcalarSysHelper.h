// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCALAR_SYS_HELPER_
#define _XCALAR_SYS_HELPER_

#include <inttypes.h>
#include "primitives/Macros.h"

#include "util/XcalarProcFsHelper.h"

class XcSysHelper
{
  public:
    uint64_t getPhysicalMemorySizeInBytes();

    MustCheck Status
    getSysTotalResourceSizeInBytes(uint64_t *sysTotalResSizeInBytes);

    long getNumOnlineCores();

    long getNumJiffiesPerSecond();

    static XcSysHelper *instance;

    static XcSysHelper *get();

    MustCheck XcProcFsHelper *getProcFsHelper();

    static MustCheck Status initSysHelper();

    static void tearDownSysHelper();

  private:
    static constexpr const char *moduleName = "XcSysHelper";
    uint64_t physicalMemorySize_ = 0;

    bool artificialNumCoresLimit = false;
    long numCores_ = -1;

    long numJiffiesPerSecond_ = -1;

    XcProcFsHelper *procFsHelper_ = NULL;

    XcSysHelper();
    ~XcSysHelper();

    MustCheck Status initSysHelperInternal();

    void tearDownSysHelperInternal();
};

#endif  // _XCALAR_SYS_HELPER_
