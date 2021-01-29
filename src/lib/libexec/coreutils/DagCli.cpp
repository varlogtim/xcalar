// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "queryparser/QueryParser.h"

void
cliDagHelp(int argc, char *argv[])
{
    printf("Usage: %s [\"table name\"] ...\n", argv[0]);
}

void
cliDagMain(int argc,
           char *argv[],
           XcalarWorkItem *workItemIn,
           bool prettyPrint,
           bool interactive)
{
    assert(0);
}
