// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SIMNODE_H_
#define _SIMNODE_H_

#include "unistd.h"
#include "primitives/Primitives.h"

struct SimNodeChildren {
    pid_t pid;
    bool reaped;
};

extern SimNodeChildren *simNodeSpawnNodes(int numNodes,
                                          const char *configFile,
                                          const char *licenseFile,
                                          const char *outputDir);

extern SimNodeChildren *simNodeSpawnNode(int myNode,
                                         int numNodes,
                                         const char *configFile,
                                         const char *licenseFile,
                                         const char *outputDir);

extern void simNodeDeleteOutput(const char *outputDir, int numNodes);
extern int simNodeReapNodes(SimNodeChildren *children, int numNodes);
extern void simNodeKillNodes(SimNodeChildren *children, int numNodes);

#endif  // _SIMNODE_H_
