// Copyright 2014-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <assert.h>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <errno.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "config/Config.h"
#include "test/SimNode.h"
#include "util/System.h"
#include "util/MemTrack.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef MEMORY_TRACKING  // This avoids an 'unused-variable'
static const char *moduleName = "libsimnode";
#endif  // MEMORY_TRACKING

static SimNodeChildren *
simNodeSpawnNodesActual(int myNode,
                        int numNodes,
                        const char *configFile,
                        const char *licenseFile,
                        const char *outputDir,
                        bool singleton)
{
    char *argv[10];
    char usrNodeCmd[PATH_MAX];
    char usrNodeCmdArg1[20];
    char usrNodeCmdArg2[20];
    char usrNodeCmdArg3[20];
    char usrNodeCmdArg4[20];
    char usrNodeCmdArg5[20];
    char usrNodeCmdArg6[PATH_MAX];
    char usrNodeCmdArg7[20];
    char usrNodeCmdArg8[PATH_MAX];

    SimNodeChildren *children;
    pid_t pid;
    int ii;
    int exitCode = 0;
    bool clusterReady = false;
    unsigned timeoutSeconds = 120;
    int ret;
    extern char **environ;  // parent's environment to be passed to child

    if (singleton) {
        children =
            (SimNodeChildren *) memAllocExt(sizeof(*children) * 1, moduleName);
        pid = fork();
        if (pid == 0) {
            // I am the child
            argv[0] = usrNodeCmd;
            argv[1] = usrNodeCmdArg1;
            argv[2] = usrNodeCmdArg2;
            argv[3] = usrNodeCmdArg3;
            argv[4] = usrNodeCmdArg4;
            argv[5] = usrNodeCmdArg5;
            argv[6] = usrNodeCmdArg6;
            argv[7] = usrNodeCmdArg7;
            argv[8] = usrNodeCmdArg8;
            argv[9] = NULL;

            snprintf(argv[0], sizeof(usrNodeCmd), "usrnode");
            snprintf(argv[1], sizeof(usrNodeCmdArg1), "--nodeId");
            snprintf(argv[2], sizeof(usrNodeCmdArg2), "%d", myNode);
            snprintf(argv[3], sizeof(usrNodeCmdArg3), "--numNodes");
            snprintf(argv[4], sizeof(usrNodeCmdArg4), "%d", numNodes);
            snprintf(argv[5], sizeof(usrNodeCmdArg5), "--configFile");
            snprintf(argv[6], sizeof(usrNodeCmdArg6), "%s", configFile);
            snprintf(argv[7], sizeof(usrNodeCmdArg7), "--licenseFile");
            snprintf(argv[8], sizeof(usrNodeCmdArg8), "%s", licenseFile);

            ret = execvpe(usrNodeCmd, argv, environ);
            if (ret == -1) {
                fprintf(stderr,
                        "argv[0]: %s, argv[1]: %s, argv[2]: %s, argv[3]: %s\n"
                        "argv[4]: %s, argv[5]: %s, argv[6]: %s, argv[7]: %s\n"
                        "argv[8]: %s, argv[9]: %s",
                        argv[0],
                        argv[1],
                        argv[2],
                        argv[3],
                        argv[4],
                        argv[5],
                        argv[6],
                        argv[7],
                        argv[8],
                        argv[9]);
                perror("execve failed");
                fflush(stderr);
                assert(0);
            }
            assert(0);
            // @SymbolCheckIgnore
            exit(-1);
        } else {
            assert(pid > 0);
            children[0].pid = pid;
            children[0].reaped = false;
        }
        return children;
    }

    // Multi-node case below
    children = (SimNodeChildren *) memAllocExt(sizeof(*children) * numNodes,
                                               moduleName);
    assert(children != NULL);

    fprintf(stderr, "numNodes: %d\n", numNodes);

    for (ii = 0; ii < numNodes; ii++) {
        pid = fork();

        if (pid == 0) {
            // I am the child
            snprintf(usrNodeCmd,
                     sizeof(usrNodeCmd),
                     "stdbuf -i0 -o0 -e0 usrnode "
                     "--nodeId %d --numNodes %d --configFile %s "
                     "--licenseFile %s > \"%s/node.%d.out\" 2>&1",
                     ii,
                     numNodes,
                     configFile,
                     licenseFile,
                     outputDir,
                     ii);
            // printf("Executing %s\n", usrNodeCmd);
            execl("/bin/sh", "sh", "-c", usrNodeCmd, NULL);
            // We should not get here
            assert(0);
            // @SymbolCheckIgnore
            exit(-1);
        }

        // Parents code. Child does not reach here
        assert(pid > 0);
        children[ii].pid = pid;
        children[ii].reaped = false;
    }

    while (!clusterReady) {
        snprintf(usrNodeCmd, sizeof(usrNodeCmd), "xccli -c version");
        exitCode = system(usrNodeCmd);
        clusterReady = (exitCode == 0);
        if (!clusterReady) {
            if (timeoutSeconds == 0) {
                fprintf(stderr, "Timeout reached waiting for cluster ready!\n");
                // @SymbolCheckIgnore
                exit(-1);
            }
            sysSleep(1);
            timeoutSeconds--;
        }
    }
    return children;
}

SimNodeChildren *
simNodeSpawnNodes(int numNodes,
                  const char *configFile,
                  const char *licenseFile,
                  const char *outputDir)
{
    return simNodeSpawnNodesActual(0,
                                   numNodes,
                                   configFile,
                                   licenseFile,
                                   outputDir,
                                   false);
}

SimNodeChildren *
simNodeSpawnNode(int myNode,
                 int numNodes,
                 const char *configFile,
                 const char *licenseFile,
                 const char *outputDir)
{
    return simNodeSpawnNodesActual(myNode,
                                   numNodes,
                                   configFile,
                                   licenseFile,
                                   outputDir,
                                   true);
}

void
simNodeDeleteOutput(const char *outputDir, int numNodes)
{
    char path[255];
    char errMsg[255];
    unsigned ii;
    int ret;

    assert(numNodes >= 0);
    for (ii = 0; ii < (unsigned) numNodes; ii++) {
        snprintf(path, sizeof(path), "%s/node.%d.out", outputDir, ii);
        // @SymbolCheckIgnore
        ret = unlink(path);
        if (ret != 0) {
            snprintf(errMsg, sizeof(errMsg), "Error deleting %s: ", path);
            perror(errMsg);
        }
        assert(ret == 0);
    }
}

int
simNodeReapNodes(SimNodeChildren *children, int numNodes)
{
    int childStatus;
    int ii;
    pid_t pid;
    int ret = 0;

    // Now we wait for all the children
    for (ii = 0; ii < numNodes; ii++) {
        pid = waitpid(children[ii].pid, &childStatus, 0);
        if (errno == ECHILD) {
            ret = 1;
        } else {
            assert(pid == children[ii].pid);
        }
        // waitpid returns a 16bit code, but exit codes are 8 bits.
        childStatus /= 256;
        if (childStatus > 128 && childStatus <= (128 + 32)) {
            int signal = childStatus - 128;
            printf("Child %d (pid %d) returned status %d (signal %d)\n",
                   ii,
                   pid,
                   childStatus,
                   signal);
        } else {
            printf("Child %d (pid %d) returned status %d\n",
                   ii,
                   pid,
                   childStatus);
        }
        children[ii].reaped = true;
        if (childStatus != 0) {
            ret = 1;
        }
    }

    // Now let's ensure we reaped all our kids
    for (ii = 0; ii < numNodes; ii++) {
        assert(children[ii].reaped);
    }

    memFree(children);
    children = NULL;

    return ret;
}

void
simNodeKillNodes(SimNodeChildren *children, int numNodes)
{
    int ret;

    ret = system("pgrep -u `whoami` usrnode | xargs -r kill -9");
    assert(ret == 0);
    simNodeReapNodes(children, numNodes);
}
