// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
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

#include "coreutils/CliCoreUtilsInt.h"
#include "util/MemTrack.h"

static void inspect(const char *datasetName,
                    const char *tableName,
                    bool isPrecise);
static constexpr const char *moduleName = "cliInspect";

void
cliInspectHelp(int argc, char *argv[])
{
    printf(
        "Usage:\n\t%s <tableName> [--precise]\n\t%s "
        "--dataset <datasetName> [--precise] \n",
        argv[0],
        argv[0]);
}

void
cliInspectMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    if (argc < 2 || argc > 4) {
        cliInspectHelp(argc, argv);
        return;
    }

    bool isPrecise = false;

    if (strcmp(argv[1], "--dataset") == 0) {
        if (argc < 3) {
            cliInspectHelp(argc, argv);
            return;
        }

        if (argc == 4 && strcmp(argv[3], "--precise") == 0) {
            isPrecise = true;
        }

        inspect(argv[2], NULL, isPrecise);
    } else {
        if (argc == 3 && strcmp(argv[2], "--precise") == 0) {
            isPrecise = true;
        }

        inspect(NULL, argv[1], isPrecise);
    }
}

void
inspect(const char *datasetName, const char *tableName, bool isPrecise)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiTableInput *apiTable = NULL;
    XcalarApiGetTableMetaOutput *getTableMetaOutput = NULL;
    const char *srcName = NULL;
    bool isTable;
    unsigned ii;
    uint64_t totalRows = 0;

    isTable = (tableName != NULL);
    srcName = (isTable) ? tableName : datasetName;

    // XXX - Make it possible to string together multiple xcalar apis
    // and just submit them at one go
    workItem = xcalarApiMakeGetTableMeta(srcName, isTable, isPrecise);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(workItem != NULL);
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    getTableMetaOutput = &workItem->output->outputResult.getTableMetaOutput;
    if (workItem->output->hdr.status != StatusOk.code()) {
        status.fromStatusCode(workItem->output->hdr.status);
        goto CommonExit;
    }

    printf("Inspecting %s %s\n", (isTable) ? "table" : "dataset", srcName);
    printf("---------------------------------------------\n");
    for (ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        printf("  Number of rows in node %u: %lu\n",
               ii,
               getTableMetaOutput->metas[ii].numRows);
        totalRows += getTableMetaOutput->metas[ii].numRows;

        if (isTable) {
            printf("  Number of Transpage sent in node %u: %ld\n",
                   ii,
                   getTableMetaOutput->metas[ii].numTransPageSent);

            printf("  Number of Transpage recv in node %u: %ld\n",
                   ii,
                   getTableMetaOutput->metas[ii].numTransPageRecv);

            printf("  Number of xdb page consued (in byte) in node %u: %ld\n",
                   ii,
                   getTableMetaOutput->metas[ii].xdbPageConsumedInBytes);

            printf("  Number of xdb page allocated (in byte) in node %u: %ld\n",
                   ii,
                   getTableMetaOutput->metas[ii].xdbPageAllocatedInBytes);
        }
    }
    printf("  Total: %lu\n", totalRows);
    xcalarApiFreeWorkItem(workItem);
    getTableMetaOutput = NULL;
    workItem = NULL;

    if (!isTable) {
        goto CommonExit;
    }
    assert(isTable);
    apiTable = NULL;

    apiTable =
        (XcalarApiTableInput *) memAllocExt(sizeof(*apiTable), moduleName);
    if (apiTable == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(apiTable != NULL);

    strlcpy(apiTable->tableName, tableName, sizeof(apiTable->tableName));

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetTableRefCount,
                                            apiTable,
                                            sizeof(*apiTable));
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (workItem->output->hdr.status != StatusOk.code()) {
        status.fromStatusCode(workItem->output->hdr.status);
        goto CommonExit;
    }

    printf("  Reference count: %lu\n",
           workItem->output->outputResult.getTableRefCountOutput.refCount);

    assert(status == StatusOk);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        apiTable = NULL;
        getTableMetaOutput = NULL;
    }

    if (apiTable != NULL) {
        memFree(apiTable);
        apiTable = NULL;
    }

    assert(getTableMetaOutput == NULL);

    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
