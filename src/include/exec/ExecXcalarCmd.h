// Copyright 2014-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _EXEC_XCALAR_CMD_H_
#define _EXEC_XCALAR_CMD_H_

#include "primitives/Primitives.h"
#include "WorkItem.h"

typedef void (*MainFn)(int argc,
                       char *argv[],
                       XcalarWorkItem *workItemIn,
                       bool prettyPrint,
                       bool interactive);
typedef void (*UsageFn)(int argc, char *argv[]);

struct ExecCommandMapping {
    const char *commandString;
    const char *commandDesc;
    MainFn main;
    UsageFn usage;
};

extern const ExecCommandMapping execCoreCommandMappings[];
extern const unsigned execNumCoreCommandMappings;

#endif  // _EXEC_XCALAR_CMD_H_
