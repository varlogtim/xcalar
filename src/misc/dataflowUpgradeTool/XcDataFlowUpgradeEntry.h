// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFLOW_UPGRADE_TOOL_
#define _DATAFLOW_UPGRADE_TOOL_

#include <stdint.h>
#include <linux/limits.h>

#include <assert.h>
#include <err.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct _UpgradeToolOptions {
    char cfgFile[PATH_MAX];
    char dataflowInputFile[PATH_MAX];
    char dataflowOutputFile[PATH_MAX];
    bool verbose;
} UpgradeToolOptions;

typedef struct _UpgradeToolArgs {
    UpgradeToolOptions *options;
    Status status;
} UpgradeToolArgs;

#endif // _DATAFLOW_UPGRADE_TOOL_
