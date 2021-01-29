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
#include "util/MemTrack.h"
#include "coreutils/CoreUtilsConstants.h"

using namespace cutil;

static Status getGroupIdMap(unsigned nodeId,
                            XcalarApiGetStatGroupIdMapOutput **groupIdMap);

static constexpr const char *moduleName = "cliStats";
static constexpr const char *unknownStatGroupName = "Unknown";

void
cliGetStatsHelp(int argc, char *argv[])
{
    printf("Usage: %s <nodeID> [statsGroupName] ...\n", argv[0]);
}

void
cliResetStatsHelp(int argc, char *argv[])
{
    printf("Usage: %s <nodeId>\n", argv[0]);
}

void
cliTopHelp(int argc, char *argv[])
{
    printf("Usage: %s\n", argv[0]);
}

void
cliResetStatsMain(int argc,
                  char *argv[],
                  XcalarWorkItem *workItemIn,
                  bool prettyPrint,
                  bool interactive)
{
    Status status = StatusUnknown;
    char *endPtr;
    unsigned nodeId;
    XcalarWorkItem *workItem = NULL;
    StatusCode *statusOutput = NULL;

    if (argc != MinArgc) {
        cliResetStatsHelp(argc, argv);
        status = StatusCliParseError;
        goto CommonExit;
    }

    nodeId = (unsigned) strtoul(argv[1], &endPtr, 0);
    if (endPtr == argv[1]) {
        printf("Error: <nodeId> is not an integer\n");
        status = StatusCliParseError;
        cliResetStatsHelp(argc, argv);
        goto CommonExit;
    }

    workItem = xcalarApiMakeResetStatWorkItem(nodeId);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    assert(workItem->outputSize ==
           XcalarApiSizeOfOutput(workItem->output->outputResult.noOutput));
    statusOutput = &workItem->output->hdr.status;

    if (*statusOutput == StatusOk.code()) {
        printf("Stats reset successful\n");
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(*statusOutput));
    }

    assert(status == StatusOk);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        statusOutput = NULL;
    }

    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}

static const char *
getStatGroupName(uint64_t groupId, XcalarApiGetStatGroupIdMapOutput *groupIdMap)
{
    assert(groupIdMap != NULL);
    assert(groupIdMap->truncated == false);

    for (uint64_t ii = 0; ii < groupIdMap->numGroupNames; ii++) {
        XcalarStatGroupInfo *currentGroup = &(groupIdMap->groupNameInfo[ii]);
        if (currentGroup->groupIdNum == groupId) {
            return currentGroup->statsGroupName;
        }
    }

    assert(0);
    return NULL;
}

void
cliGetStatsMain(int argc,
                char *argv[],
                XcalarWorkItem *workItemIn,
                bool prettyPrint,
                bool interactive)
{
    NodeId nodeId;
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetStatOutput *statOutput = NULL;
    XcalarApiGetStatGroupIdMapOutput *groupIdMap = NULL;
    char *endPtr = NULL;
    unsigned ii;

    if (argc < MinArgc) {
        status = StatusCliParseError;
        cliGetStatsHelp(argc, argv);
        goto CommonExit;
    }

    nodeId = (unsigned) strtoul(argv[1], &endPtr, 0);
    if (endPtr == argv[1]) {
        printf("Error: <nodeId> is not an integer\n");
        status = StatusCliParseError;
        cliGetStatsHelp(argc, argv);
        goto CommonExit;
    }

    // Get the mapping table between stat group IDs and names if we don't
    // already have it
    status = getGroupIdMap(nodeId, &groupIdMap);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGetStatWorkItem(nodeId);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    statOutput = &workItem->output->outputResult.statOutput;
    if (workItem->output->hdr.status != StatusOk.code()) {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
        goto CommonExit;
    }

    printf("NodeID: %u \n", nodeId);

    if (prettyPrint) {
        printf("...... Stats for node %u .....\n", nodeId);
        printf("%-20s%-20s%-40s%-20s%-20s\n",
               "Stat Life",
               "Stat Group Name",
               "Stat Name",
               "Stat Value",
               "Ref Value");
        printf(
            "======================================================="
            "======================================================="
            "==========================\n");

        for (ii = 0; ii < statOutput->numStats; ii++) {
            switch (statOutput->stats[ii].statType) {
            case StatCumulative:
                printf("%-20s%-40s%-20lu%-20s\n",
                       getStatGroupName(statOutput->stats[ii].groupId,
                                        groupIdMap),
                       statOutput->stats[ii].statName,
                       statOutput->stats[ii].statValue.statUint64,
                       " - ");
                break;
            case StatAbsoluteWithRefVal:
                printf("%-20s%-40s%-20u%-20u\n",
                       getStatGroupName(statOutput->stats[ii].groupId,
                                        groupIdMap),
                       statOutput->stats[ii].statName,
                       statOutput->stats[ii].statValue.statUint32.val,
                       statOutput->stats[ii].statValue.statMaxRefValUint32);
                break;
            case StatHWM:
            case StatAbsoluteWithNoRefVal:
                printf("%-20s%-40s%-20lu%-20s\n",
                       getStatGroupName(statOutput->stats[ii].groupId,
                                        groupIdMap),
                       statOutput->stats[ii].statName,
                       statOutput->stats[ii].statValue.statUint64,
                       " - ");
                break;
            }
        }

        printf(
            "======================================================="
            "======================================================="
            "==========================\n");
    } else {
        printf("NodeID: %u\n", nodeId);
        printf("%s\t%s\t%s\t%s\t%s\n",
               "Stat Life",
               "Stat Group Name",
               "Stat Name",
               "Stat Value",
               "Ref Value");
        for (ii = 0; ii < statOutput->numStats; ii++) {
            switch (statOutput->stats[ii].statType) {
            case StatCumulative:
                printf("\t\"%s\"\t\"%s\"\t%lu\n",
                       getStatGroupName(statOutput->stats[ii].groupId,
                                        groupIdMap),
                       statOutput->stats[ii].statName,
                       statOutput->stats[ii].statValue.statUint64);
                break;
            case StatAbsoluteWithRefVal:
                printf("\t\"%s\"\t\"%s\"\t%u\t%u\n",
                       getStatGroupName(statOutput->stats[ii].groupId,
                                        groupIdMap),
                       statOutput->stats[ii].statName,
                       statOutput->stats[ii].statValue.statUint32.val,
                       statOutput->stats[ii].statValue.statMaxRefValUint32);
                break;
            case StatHWM:
            case StatAbsoluteWithNoRefVal:
                printf("\t\"%s\"\t\"%s\"\t%lu\n",
                       getStatGroupName(statOutput->stats[ii].groupId,
                                        groupIdMap),
                       statOutput->stats[ii].statName,
                       statOutput->stats[ii].statValue.statUint64);
                break;
            }
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        statOutput = NULL;
    }

    if (groupIdMap != NULL) {
        memFree(groupIdMap);
    }

    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}

static Status
getGroupIdMap(unsigned nodeId, XcalarApiGetStatGroupIdMapOutput **groupIdMap)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusOk;
    XcalarApiGetStatGroupIdMapOutput *statGroupIdMapOutput = NULL;

    workItem = xcalarApiMakeGetStatGroupIdMapWorkItem(nodeId);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(workItem->output != NULL);

    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    statGroupIdMapOutput = &workItem->output->outputResult.statGroupIdMapOutput;

    assert(statGroupIdMapOutput->numGroupNames > 0);
    *groupIdMap = (XcalarApiGetStatGroupIdMapOutput *)
        memAllocExt(sizeof(XcalarApiGetStatGroupIdMapOutput) +
                        (sizeof(XcalarStatGroupInfo) *
                         statGroupIdMapOutput->numGroupNames),
                    moduleName);
    if (*groupIdMap == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memcpy(*groupIdMap,
           statGroupIdMapOutput,
           sizeof(XcalarApiGetStatGroupIdMapOutput) +
               (sizeof(XcalarStatGroupInfo) *
                statGroupIdMapOutput->numGroupNames));

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

void
cliTopMain(int argc,
           char *argv[],
           XcalarWorkItem *workItemIn,
           bool prettyPrint,
           bool interactive)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;

    uint64_t numNodes = 0;
    XcalarApiTopOutput *topOutput = NULL;
    XcalarApiTopOutputPerNode *perNodeTopOutput = NULL;

    workItem = xcalarApiMakeTopWorkItem(GetAllTopStats, XcalarApiTopAllNode);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    topOutput = &(workItem->output->outputResult.topOutput);
    status.fromStatusCode(topOutput->status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    numNodes = topOutput->numNodes;
    assert(numNodes > 0);

    perNodeTopOutput = topOutput->topOutputPerNode;
    for (unsigned ii = 0; ii < numNodes; ii++) {
        assert(perNodeTopOutput != NULL);
        fprintf(stdout, "============================================\n");
        fprintf(stdout, "NodeId: %lu \n", perNodeTopOutput->nodeId);
        fprintf(stdout,
                "parentCpuUsageInPercent: %lf \n",
                perNodeTopOutput->parentCpuUsageInPercent);
        fprintf(stdout,
                "childrenCpuUsageInPercent: %lf \n",
                perNodeTopOutput->childrenCpuUsageInPercent);
        fprintf(stdout,
                "memUsageInPercent: %lf \n",
                perNodeTopOutput->memUsageInPercent);
        fprintf(stdout,
                "memUsedInBytes: %lu \n",
                perNodeTopOutput->memUsedInBytes);
        fprintf(stdout,
                "totalAvailableMemInBytes: %lu \n",
                perNodeTopOutput->totalAvailableMemInBytes);
        fprintf(stdout,
                "networkRecvInBytesPerSec: %lu \n",
                perNodeTopOutput->networkRecvInBytesPerSec);
        fprintf(stdout,
                "networkSendInBytesPerSec: %lu \n",
                perNodeTopOutput->networkSendInBytesPerSec);
        fprintf(stdout, "xdbUsedBytes: %lu \n", perNodeTopOutput->xdbUsedBytes);
        fprintf(stdout,
                "xdbTotalBytes: %lu \n",
                perNodeTopOutput->xdbTotalBytes);
        fprintf(stdout, "============================================\n");
        perNodeTopOutput++;
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}
