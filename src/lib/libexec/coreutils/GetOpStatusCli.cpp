// Copyright 2019 Xcalar, Inc. All rights reserved.
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

void
cliGetOpStatusHelp(int argc, char *argv[])
{
    printf("Usage: %s <--realtime> [\"table name\"] \n", argv[0]);
}

void
printErrorStats(XcalarApiOpErrorStats *errorStats, XcalarApis api)
{
    OpEvalUdfErrorStats *opUdfErrorStats = NULL;
    uint64_t map_errs_bd_count = 0;
    printf("error stats: \n");
    switch (api) {
    case XcalarApiBulkLoad:
        printf("number file open failures:%lu\n",
               errorStats->loadErrorStats.numFileOpenFailure);
        printf("number dir open failures:%lu\n",
               errorStats->loadErrorStats.numDirOpenFailure);
        break;
    case XcalarApiIndex:
        printf("number of parssing errors: %lu\n",
               errorStats->indexErrorStats.numParseError);
        printf("number of field not existing errors: %lu\n",
               errorStats->indexErrorStats.numFieldNoExist);
        printf("number of type mismatches: %lu\n",
               errorStats->indexErrorStats.numTypeMismatch);

        printf("number of other errors: %lu\n",
               errorStats->indexErrorStats.numOtherError);
        break;
    case XcalarApiMap:
        opUdfErrorStats = &errorStats->evalErrorStats.evalUdfErrorStats;

        if (opUdfErrorStats->numEvalUdfError == 0) {
            printf("UDF: number of errors: %lu\n",
                   opUdfErrorStats->numEvalUdfError);
        } else {
            printf(
                "UDF: total number of errors: %lu; error break-down below:\n\n",
                opUdfErrorStats->numEvalUdfError);
            // break-down the total
            for (uint32_t jj = 0; jj < XcalarApiMaxFailureEvals; jj++) {
                printf("\tEval number %u:\n", jj);
                for (unsigned ii = 0; ii < XcalarApiMaxFailures; ii++) {
                    if (opUdfErrorStats->opFailureSummary[jj]
                            .failureSummInfo[ii]
                            .numRowsFailed == 0) {
                        break;
                    }
                    map_errs_bd_count += opUdfErrorStats->opFailureSummary[jj]
                                             .failureSummInfo[ii]
                                             .numRowsFailed;
                    printf("\t%lu failed due to:\n\n %s\n\n",
                           opUdfErrorStats->opFailureSummary[jj]
                               .failureSummInfo[ii]
                               .numRowsFailed,
                           opUdfErrorStats->opFailureSummary[jj]
                               .failureSummInfo[ii]
                               .failureDesc);
                }
            }

            if (map_errs_bd_count < opUdfErrorStats->numEvalUdfError) {
                printf("\nUDF: %lu errors remaining; fix above and re-run\n\n",
                       opUdfErrorStats->numEvalUdfError - map_errs_bd_count);
            }
        }

        // fall through to reporting XDF related errors for map
    case XcalarApiFilter:
        // XXX: check if UDF related error reporting is needed here
    case XcalarApiGroupBy:
    case XcalarApiAggregate:
        printf("XDF: number of unsubstituted errors: %lu\n",
               errorStats->evalErrorStats.evalXdfErrorStats.numUnsubstituted);
        printf("XDF: number of unspported type errors: %lu\n",
               errorStats->evalErrorStats.evalXdfErrorStats.numUnspportedTypes);
        printf("XDF: number of mixed type errors: %lu\n",
               errorStats->evalErrorStats.evalXdfErrorStats
                   .numMixedTypeNotSupported);
        printf("XDF: number of case errors: %lu\n",
               errorStats->evalErrorStats.evalXdfErrorStats.numEvalCastError);
        printf("XDF: number of div by zero  errors: %lu\n",
               errorStats->evalErrorStats.evalXdfErrorStats.numDivByZero);
        printf("XDF: number of misc eval function  errors: %lu\n",
               errorStats->evalErrorStats.evalXdfErrorStats.numMiscError);
        break;

    case XcalarApiJoin:
    case XcalarApiExport:
    case XcalarApiProject:
    case XcalarApiGetRowNum:
        printf("Error stats not available");
        break;
    default:
        assert(0);
    }
}

void
printOpDetail(XcalarApiOpDetails *opDetails, XcalarApis api)
{
    printf("number of work completed: %lu\n", opDetails->numWorkCompleted);
    printf("number of work total: %lu\n", opDetails->numWorkTotal);
    printf("cancel: %s\n", opDetails->cancelled ? "true" : "false");

    printErrorStats(&opDetails->errorStats, api);
}

static void
getPerNodeOpStats(const char *tableName)
{
    Status status;
    XcalarApiPerNodeOpStats *perNodeStats;
    XcalarWorkItem *workItem = NULL;

    if (strlen(tableName) > XcalarApiMaxTableNameLen) {
        status = StatusInvalidTableName;
        goto CommonExit;
    }

    workItem = xcalarApiMakePerNodeOpStatsWorkItem(tableName);
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

    if (workItem->output->hdr.status == StatusOk.code()) {
        perNodeStats = &workItem->output->outputResult.perNodeOpStatsOutput;
        for (uint64_t ii = 0; ii < perNodeStats->numNodes; ++ii) {
            printf("Node ID %lu\n", perNodeStats->nodeOpStats[ii].nodeId);
            if (perNodeStats->nodeOpStats[ii].status != StatusOk.code()) {
                printf("Error: %s\n",
                       strGetFromStatusCode(
                           perNodeStats->nodeOpStats[ii].status));
                continue;
            }

            printOpDetail(&perNodeStats->nodeOpStats[ii].opDetails,
                          perNodeStats->api);
        }
    } else {
        status.fromStatusCode(workItem->output->hdr.status);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}

static void
getOpStats(const char *tableName, const char *sessionName)
{
    Status status;
    XcalarWorkItem *workItem = NULL;

    if (strlen(tableName) > XcalarApiMaxTableNameLen) {
        status = StatusInvalidTableName;
        goto CommonExit;
    }

    workItem = xcalarApiMakeOpStatsWorkItem(tableName, sessionName);
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

    if (workItem->output->hdr.status == StatusOk.code()) {
        printOpDetail(&workItem->output->outputResult.opStatsOutput.opDetails,
                      workItem->output->outputResult.opStatsOutput.api);
    } else {
        status.fromStatusCode(workItem->output->hdr.status);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}

void
cliGetOpStatusMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive)
{
    if (argc < 2) {
        cliGetOpStatusHelp(argc, argv);
        return;
    }

    getOpStats(argv[1], argv[2]);

#ifdef OUT
    if (argc == 3) {
        if (strcasecmp(argv[1], "--pernode") == 0) {
            getPerNodeOpStats(argv[2]);
        } else {
            cliGetOpStatusHelp(argc, argv);
        }
    } else {
        getOpStats(argv[1]);
    }
#endif
}
