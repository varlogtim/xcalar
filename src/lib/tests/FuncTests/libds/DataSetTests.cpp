// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <stdio.h>
#include <libgen.h>
#include <assert.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <jansson.h>
#include <sys/mman.h>
#include <unistd.h>
#include <pwd.h>
#include <ftw.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "DataSetTests.h"
#include "DataSetTestsCommon.h"
#include "SourceTypeEnum.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "LibDsFuncTestConfig.h"

#include "test/QA.h"
#include "strings/String.h"

static constexpr const char *moduleName = "DataSetStress";

// Parameters
static uint64_t dataSetFuncStressUsers = 2;

static constexpr uint64_t MaxDagNodeNameLen = 256;
static constexpr uint64_t MaxNumKeys = 10;
static constexpr uint64_t MaxNumDatasets = 11;
static constexpr uint64_t MaxNumTables = 128;

enum loadType { loadYelp = 0, loadGdelt = 1, loadJson = 2, loadNtypes = 3 };

// Threads per user (representing concurrent loads per user)
static uint64_t dataSetFuncStressThreads = 3;

// Note that the number that's picked for dataSetFuncStressThreads is related to
// the number of different dataset URLs which are loaded, since each thread
// tries to load a different URL. So, if dataSetFuncStressThreads < loadNtypes,
// only the first dataSetFuncStressThreads URLs in the loadType enum defined
// above, will be loaded. If dataSetFuncStressThreads > loadNtypes, two
// different threads would load the same URL, since the threads pick the URLs
// in a round-robin manner from the list in loadType enum.

static char dfQaYelpReviewsUrl[256] = QaYelpReviewsUrl;
static char dfQaRandomJsonUrl[256] = QaRandomJsonUrl;
static char dfQaYelpUserUrl[256] = QaYelpUserUrl;
static char dsQaGDeltUrl[XcalarApiMaxUrlLen + 1] = QaGDeltUrl;

static char **destIp;
static uint64_t *destPort;
static char tuserName[LOGIN_NAME_MAX + 1] = "none";
static pid_t inputPid;
static NodeId inputNodeId;

struct CsvTable {
    unsigned colNum;
    const char *origKeyName;
    const char *newKeyName;
    DfFieldType keyType;
    bool columnRenamed;
};

static CsvTable gdeltTables[] = {
    {0, "column0", "GlobalEventID", DfString, false},
};

struct JsonTable {
    const char *tableName;
    char *url;
    const char *keyNames[MaxNumKeys];
};

static JsonTable yelpTables[] = {
    {"reviews", dfQaYelpReviewsUrl, {"user_id", "stars", NULL}},
    {"user", dfQaYelpUserUrl, {"user_id", "review_count", NULL}},
};

struct Datasets {
    char datasetName[MaxDagNodeNameLen + 1];
    char datasetUrl[XcalarApiMaxPathLen + 1];
};

struct ArgDataSetStressPerThread {
    pthread_t threadId;
    int threadIdLocal;
    char *userName;

    char dagNodeNameByTable[MaxNumTables][MaxDagNodeNameLen];
    unsigned numTablesIndexed = 0;
    Datasets datasets[MaxNumDatasets];
    unsigned numDataset = 0;
    char dagNodeNameByConstant[MaxNumTables][MaxDagNodeNameLen];
    unsigned numConstant = 0;
    Status retStatus;
    ArgDataSetStressPerThread() {}
};

struct ArgDataSetStressPerUser {
    pthread_t threadId;
    int userIdLocal;
    char userName[LOGIN_NAME_MAX + 1];
    // Here we just deliberately use one session for running concurrent receive
    // thread APIs. Note that UI always runs these APIs in a single threaded
    // fashion, but LRQs for instance with Breadth First Work Flow may run
    // concurrent APIs.
    char sessionName[256];
    Status retStatus;
    ArgDataSetStressPerUser() {}
};

static Status
newSession(pid_t pidIn,
           char *userNameIn,
           char *sessionNameIn,
           bool fork,
           char *forkSessionName,
           uint64_t *sessionIdOut)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionNewOutput *sessionNewOutput = NULL;

    workItem =
        xcalarApiMakeSessionNewWorkItem(sessionNameIn, fork, forkSessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "newSession: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userNameIn,
                                pidIn);
    sessionNewOutput = &workItem->output->outputResult.sessionNewOutput;

    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Create new session failed");
        if (status == StatusOk) {
            verify(sessionNewOutput != NULL);
            xSyslog(moduleName,
                    XlogErr,
                    "Status: %s Session error message: %s",
                    strGetFromStatus(workItem->output->hdr.status),
                    sessionNewOutput->error);
            status = workItem->output->hdr.status;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Status: %s",
                    strGetFromStatus(status));
        }
    } else {
        xSyslog(moduleName,
                XlogInfo,
                "Session created, ID = %lX",
                sessionNewOutput->sessionId);
        *sessionIdOut = sessionNewOutput->sessionId;
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }
    return status;
}

static Status
deleteSession(char *userName, char *sessionName, uint64_t sessionId)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    workItem = xcalarApiMakeSessionInactWorkItem(sessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "deleteSession: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    workItem->input->sessionDeleteInput.sessionId = sessionId;

    // The session must be inactive before it can be deleted
    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    sessionGenericOutput = &workItem->output->outputResult.sessionGenericOutput;

    if (status != StatusOk &&
        workItem->output->hdr.status != StatusSessionExists) {
        xSyslog(moduleName, XlogErr, "Session inact failed");
        verify(sessionGenericOutput != NULL);
        xSyslog(moduleName,
                XlogErr,
                "Status: %s Session error message: %s",
                strGetFromStatus(workItem->output->hdr.status),
                sessionGenericOutput->errorMessage);
        status = workItem->output->hdr.status;
        goto CommonExit;
    }

    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // Now we can delete the session
    workItem = xcalarApiMakeSessionDeleteWorkItem(sessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "deleteSession: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    workItem->input->sessionDeleteInput.sessionId = sessionId;

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    sessionGenericOutput = &workItem->output->outputResult.sessionGenericOutput;

    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Session delete failed");
        if (status == StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Status: %s Session error message: %s",
                    strGetFromStatus(workItem->output->hdr.status),
                    sessionGenericOutput->errorMessage);
            status = workItem->output->hdr.status;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Status: %s",
                    strGetFromStatus(status));
        }
        goto CommonExit;
    }
    xSyslog(moduleName, XlogInfo, "Session successfully deleted");
CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }
    return status;
}

static Status
listAndDeleteSession(char *userName, char *sessionName)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionListOutput *listOutput = NULL;
    uint64_t sessionId = 0;

    // We only want an exact match for the session name this test uses
    workItem = xcalarApiMakeSessionListWorkItem(sessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "listAndDeleteSession: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    listOutput = &workItem->output->outputResult.sessionListOutput;

    // We only expect:
    //   StatusOk - An exact match was found
    //   StatusSessionNotFound - there are no sessions

    if (status != StatusOk ||
        (workItem->output->hdr.status != StatusOk &&
         workItem->output->hdr.status != StatusSessionNotFound)) {
        xSyslog(moduleName, XlogErr, "Session list failed");
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName, XlogErr, "Status: %s", strGetFromStatus(status));
        goto CommonExit;
    }

    // If a matching session (and ONLY one session) was found, delete it

    if (listOutput->numSessions == 1) {
        sessionId = listOutput->sessions[0].sessionId;
        status = deleteSession(userName, sessionName, sessionId);
        if (status != StatusOk) {
            goto CommonExit;
        }
        sessionId = 0;
    } else {
        verify(listOutput->numSessions == 0);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }
    return status;
}

static void
makeTableName(char *buf,
              size_t bufLen,
              const char *datasetName,
              const char *tableName,
              const char *keyName,
              const char *dbOperator,
              const char *operation)
{
    verify(buf != NULL);
    verify(tableName != NULL);
    verify(keyName != NULL);

    if (dbOperator == NULL) {
        snprintf(buf, bufLen, "%s-%s-%s", datasetName, tableName, keyName);
    } else {
        verify(dbOperator != NULL);
        verify(operation != NULL);
        snprintf(buf,
                 bufLen,
                 "%s-%s-%s-%s-%s",
                 datasetName,
                 tableName,
                 keyName,
                 dbOperator,
                 operation);
    }
}

static Status
checkListTableByType(char *userName,
                     char (*dagNodeName)[MaxDagNodeNameLen],
                     unsigned numNode,
                     SourceType srcType)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiListDagNodesOutput *listNodesOutput;
    unsigned ii, jj;
    Status status = StatusOk;

    workItem = xcalarApiMakeListDagNodesWorkItem("*", srcType);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "checkListTableByType: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        xSyslog(moduleName, XlogErr, "MakeListDagNodes failed");
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName, XlogErr, "Status: %s", strGetFromStatus(status));
        verify(0);
    }
    verify(workItem->outputSize > 0);

    listNodesOutput = &workItem->output->outputResult.listNodesOutput;

    xSyslog(moduleName, XlogInfo, "Expecting to see:");
    for (ii = 0; ii < numNode; ii++) {
        xSyslog(moduleName, XlogInfo, "  %d. %s", ii + 1, dagNodeName[ii]);
    }

    xSyslog(moduleName, XlogInfo, "We got:");
    for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
        xSyslog(moduleName,
                XlogDebug,
                "  %d. %s",
                ii + 1,
                listNodesOutput->nodeInfo[ii].name);
    }

    for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
        for (jj = 0; jj < numNode; jj++) {
            if (strcmp(listNodesOutput->nodeInfo[ii].name, dagNodeName[jj]) ==
                0) {
                break;
            }
        }
    }
CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
checkListTables(ArgDataSetStressPerThread *argInfo)
{
    return checkListTableByType(argInfo->userName,
                                 argInfo->dagNodeNameByTable,
                                 argInfo->numTablesIndexed,
                                 SrcTable);
}

static Status
checkListConstants(ArgDataSetStressPerThread *argInfo)
{
    return checkListTableByType(argInfo->userName,
                                 argInfo->dagNodeNameByConstant,
                                 argInfo->numConstant,
                                 SrcConstant);
}

static Status
checkListDataset(ArgDataSetStressPerThread *argInfo)
{
    char datasetNamesList[MaxNumDatasets][MaxDagNodeNameLen];
    unsigned ii;

    for (ii = 0; ii < argInfo->numDataset; ii++) {
        strlcpy(datasetNamesList[ii],
                argInfo->datasets[ii].datasetName,
                sizeof(datasetNamesList[ii]));
    }

    return checkListTableByType(argInfo->userName,
                                 datasetNamesList,
                                 argInfo->numDataset,
                                 SrcDataset);
}

static Status
cleanupTables(ArgDataSetStressPerThread *argInfo)
{
    unsigned ii;
    Status status = StatusOk;
    char *userName = argInfo->userName;

    status = checkListDataset(argInfo);
    if (status != StatusOk) {
        return status;
    }

    status = checkListTables(argInfo);
    if (status != StatusOk) {
        return status;
    }

    status = checkListConstants(argInfo);
    if (status != StatusOk) {
        return status;
    }

    for (ii = 0; ii < argInfo->numTablesIndexed; ii++) {
        xSyslog(moduleName,
                XlogInfo,
                "Dropping table %s",
                argInfo->dagNodeNameByTable[ii]);
        status = xcalarApiDeleteTable(argInfo->dagNodeNameByTable[ii],
                                      destIp[inputNodeId],
                                      destPort[inputNodeId],
                                      userName,
                                      inputPid);
        if (status != StatusOk) {
            return status;
        }
    }

    for (ii = 0; ii < argInfo->numConstant; ii++) {
        xSyslog(moduleName,
                XlogInfo,
                "Dropping constant %s",
                argInfo->dagNodeNameByConstant[ii]);
        status = xcalarApiDeleteTable(argInfo->dagNodeNameByConstant[ii],
                                      destIp[inputNodeId],
                                      destPort[inputNodeId],
                                      userName,
                                      inputPid);
        if (status != StatusOk) {
            return status;
        }
    }
    return status;
}

static Status
cleanupDatasets(char *userName)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    XcalarApiDeleteDagNodesOutput *destroyDatasetsOutput;

    workItem = xcalarApiMakeDeleteDagNodesWorkItem("*", SrcDataset);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "MakeDeleteDagNodes Work Item creation failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName,
                XlogErr,
                "Delete Dag Nodes failed: %s",
                strGetFromStatus(status));
    } else {
        destroyDatasetsOutput =
            &workItem->output->outputResult.deleteDagNodesOutput;
        xSyslog(moduleName,
                XlogInfo,
                "Delete Dag Nodes count %llu",
                (unsigned long long) destroyDatasetsOutput->numNodes);
        for (uint64_t ii = 0; ii < destroyDatasetsOutput->numNodes; ++ii) {
            if (destroyDatasetsOutput->statuses[ii].status != StatusOk) {
                status = destroyDatasetsOutput->statuses[ii].status;
                xSyslog(moduleName,
                        XlogErr,
                        "Delete Dag Node for %s Id %llu state %s failed: %s",
                        destroyDatasetsOutput->statuses[ii].nodeInfo.name,
                        (unsigned long long) destroyDatasetsOutput->statuses[ii]
                            .nodeInfo.dagNodeId,
                        strGetFromDgDagState(
                            destroyDatasetsOutput->statuses[ii].nodeInfo.state),
                        strGetFromStatus(status));
            }
        }
    }

    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    workItem = xcalarApiMakeDatasetUnloadWorkItem("*");
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "MakeDatasetUnload Work Item creation failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName,
                XlogErr,
                "Unload Datasets failed: %s",
                strGetFromStatus(status));
    } else {
        XcalarApiDatasetUnloadOutput *datasetUnloadOutput;
        datasetUnloadOutput =
            &workItem->output->outputResult.datasetUnloadOutput;
        xSyslog(moduleName,
                XlogInfo,
                "Unload Datasets count %llu",
                (unsigned long long) datasetUnloadOutput->numDatasets);
        for (uint64_t ii = 0; ii < datasetUnloadOutput->numDatasets; ii++) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Unloading dataset %s Id %llu: %s",
                    datasetUnloadOutput->statuses[ii].dataset.name,
                    (unsigned long long) datasetUnloadOutput->statuses[ii]
                        .dataset.datasetId,
                    strGetFromStatus(datasetUnloadOutput->statuses[ii].status));
        }
    }
    status = StatusOk;

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }
    return status;
}

static Status
loadRandomJson(ArgDataSetStressPerThread *argInfo)
{
    const char *url = dfQaRandomJsonUrl;
    XcalarWorkItem *workItem = NULL;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    unsigned datasetIdx;
    DfLoadArgs loadArgs;
    Status status = StatusOk;

    xSyslog(moduleName, XlogInfo, "Loading %s", url);

    memZero(&loadArgs, sizeof(loadArgs));
    loadArgs.fileNamePattern[0] = '\0';
    loadArgs.recursive = false;
    loadArgs.maxSize = 0;
    loadArgs.csv.typedColumnsCount = 0;
    loadArgs.csv.schemaFile[0] = '\0';
    workItem =
        xcalarApiMakeBulkLoadWorkItem(url, NULL, DfFormatJson, &loadArgs);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "loadRandomJson: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                argInfo->userName,
                                inputPid);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName,
                XlogErr,
                "MakeBulkLoad: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    bulkLoadOutput = &workItem->output->outputResult.loadOutput;
    verify((uintptr_t) bulkLoadOutput ==
           (uintptr_t) &workItem->output->outputResult);
    verify(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));

    datasetIdx = argInfo->numDataset++;
    strlcpy(argInfo->datasets[datasetIdx].datasetName,
            bulkLoadOutput->dataset.name,
            sizeof(argInfo->datasets[datasetIdx].datasetName));
    strlcpy(argInfo->datasets[datasetIdx].datasetUrl,
            url,
            sizeof(argInfo->datasets[datasetIdx].datasetUrl));

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }
    return status;
}

static Status
loadAndIndexGdeltData(ArgDataSetStressPerThread *argInfo)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    XcalarApiNewTableOutput *indexOutput;
    unsigned tableIdx;
    unsigned ii;
    char dataset[DsDatasetNameLen + 1];
    unsigned datasetIdx = 0;
    char *userName = argInfo->userName;

    workItem =
        xcalarApiMakeBulkLoadWorkItem(dsQaGDeltUrl, NULL, DfFormatCsv, NULL);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "loadAndIndexGdeltData: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName,
                XlogErr,
                "MakeBulkLoad: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    bulkLoadOutput = &workItem->output->outputResult.loadOutput;
    verify((uintptr_t) bulkLoadOutput ==
           (uintptr_t) &workItem->output->outputResult);
    verify(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
    strlcpy(dataset, bulkLoadOutput->dataset.name, sizeof(dataset));

    datasetIdx = argInfo->numDataset++;
    strlcpy(argInfo->datasets[datasetIdx].datasetName,
            dataset,
            sizeof(argInfo->datasets[datasetIdx].datasetName));
    strlcpy(argInfo->datasets[datasetIdx].datasetUrl,
            dsQaGDeltUrl,
            sizeof(argInfo->datasets[datasetIdx].datasetUrl));

    xSyslog(moduleName,
            XlogInfo,
            "Loaded %s into dataset %s",
            dsQaGDeltUrl,
            dataset);
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasets, NULL, 0);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "loadAndIndexGdeltData: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp[inputNodeId],
                                destPort[inputNodeId],
                                userName,
                                inputPid);
    if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
        if (status == StatusOk) {
            status = workItem->output->hdr.status;
        }
        xSyslog(moduleName,
                XlogErr,
                "ListDatasets: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    for (ii = 0; ii < ArrayLen(gdeltTables); ii++) {
        char threadUniqueTableName[MaxDagNodeNameLen];
        snprintf(threadUniqueTableName,
                 MaxDagNodeNameLen,
                 "gdelt%lld",
                 (unsigned long long) argInfo->threadId);
        tableIdx = argInfo->numTablesIndexed++;
        makeTableName(argInfo->dagNodeNameByTable[tableIdx],
                      MaxDagNodeNameLen,
                      threadUniqueTableName,
                      "small",
                      gdeltTables[ii].origKeyName,
                      NULL,
                      NULL);

        xSyslog(moduleName,
                XlogInfo,
                "Indexing %s as %s",
                dataset,
                argInfo->dagNodeNameByTable[tableIdx]);

        Ordering ordering = Unordered;
        workItem =
            xcalarApiMakeIndexWorkItem(dataset,
                                       NULL,
                                       1,
                                       &gdeltTables[ii].origKeyName,
                                       NULL,
                                       NULL,
                                       &ordering,
                                       argInfo->dagNodeNameByTable[tableIdx],
                                       NULL,
                                       "p",
                                       false,
                                       false);
        if (workItem == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "loadAndIndexGdeltData: Failed %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = xcalarApiQueueWork(workItem,
                                    destIp[inputNodeId],
                                    destPort[inputNodeId],
                                    userName,
                                    inputPid);
        if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
            if (status == StatusOk) {
                status = workItem->output->hdr.status;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "MakeIndex: Failed %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        indexOutput = &workItem->output->outputResult.indexOutput;
        verify((uintptr_t) indexOutput ==
               (uintptr_t) &workItem->output->outputResult);
        verify(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));
        verify(strcmp(argInfo->dagNodeNameByTable[tableIdx],
                      indexOutput->tableName) == 0);

        xSyslog(moduleName,
                XlogInfo,
                "Indexed %s",
                argInfo->dagNodeNameByTable[tableIdx]);
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    return status;
}

static Status
loadAndIndexYelpData(ArgDataSetStressPerThread *argInfo)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    XcalarApiNewTableOutput *indexOutput;
    unsigned numTables = ArrayLen(yelpTables);
    unsigned ii, jj;
    char datasetName[DsDatasetNameLen + 1];
    unsigned tableIdx;
    const char *url;
    char *userName = argInfo->userName;

    for (ii = 0; ii < numTables; ii++) {
        url = yelpTables[ii].url;
        xSyslog(moduleName, XlogInfo, "Loading %s", url);
        workItem = xcalarApiMakeBulkLoadWorkItem(url, NULL, DfFormatJson, NULL);
        if (workItem == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "loadAndIndexYelpData: Failed %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        status = xcalarApiQueueWork(workItem,
                                    destIp[inputNodeId],
                                    destPort[inputNodeId],
                                    userName,
                                    inputPid);
        if (status != StatusOk || workItem->output->hdr.status != StatusOk) {
            if (status == StatusOk) {
                status = workItem->output->hdr.status;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "MakeBulkLoadWork: Failed %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        bulkLoadOutput = &workItem->output->outputResult.loadOutput;
        verify((uintptr_t) bulkLoadOutput ==
               (uintptr_t) &workItem->output->outputResult);
        verify(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
        strlcpy(datasetName, bulkLoadOutput->dataset.name, sizeof(datasetName));

        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;

        unsigned datasetIdx = argInfo->numDataset++;
        strlcpy(argInfo->datasets[datasetIdx].datasetName,
                datasetName,
                sizeof(argInfo->datasets[datasetIdx].datasetName));
        strlcpy(argInfo->datasets[datasetIdx].datasetUrl,
                url,
                sizeof(argInfo->datasets[datasetIdx].datasetUrl));

        for (jj = 0; jj < MaxNumKeys; jj++) {
            if (yelpTables[ii].keyNames[jj] == NULL) {
                break;
            }
            tableIdx = argInfo->numTablesIndexed++;
            char threadUniqueTableName[MaxDagNodeNameLen];
            snprintf(threadUniqueTableName,
                     MaxDagNodeNameLen,
                     "yelp%lld",
                     (unsigned long long) argInfo->threadId);
            makeTableName(argInfo->dagNodeNameByTable[tableIdx],
                          MaxDagNodeNameLen,
                          threadUniqueTableName,
                          yelpTables[ii].tableName,
                          yelpTables[ii].keyNames[jj],
                          NULL,
                          NULL);

            xSyslog(moduleName,
                    XlogInfo,
                    "Indexing dataset '%s' on key '%s' into '%s'",
                    datasetName,
                    yelpTables[ii].keyNames[jj],
                    argInfo->dagNodeNameByTable[tableIdx]);

            Ordering ordering = Unordered;
            workItem =
                xcalarApiMakeIndexWorkItem(datasetName,
                                           NULL,
                                           1,
                                           &yelpTables[ii].keyNames[jj],
                                           NULL,
                                           NULL,
                                           &ordering,
                                           argInfo
                                               ->dagNodeNameByTable[tableIdx],
                                           NULL,
                                           "p",
                                           false,
                                           false);
            if (workItem == NULL) {
                status = StatusNoMem;
                xSyslog(moduleName,
                        XlogErr,
                        "loadAndIndexYelpData: Failed %s",
                        strGetFromStatus(status));
                goto CommonExit;
            }

            status = xcalarApiQueueWork(workItem,
                                        destIp[inputNodeId],
                                        destPort[inputNodeId],
                                        userName,
                                        inputPid);
            if (status != StatusOk ||
                workItem->output->hdr.status != StatusOk) {
                if (status == StatusOk) {
                    status = workItem->output->hdr.status;
                }
                xSyslog(moduleName,
                        XlogErr,
                        "MakeIndex: Failed %s",
                        strGetFromStatus(status));
                goto CommonExit;
            }

            indexOutput = &workItem->output->outputResult.indexOutput;
            verify(strcmp(indexOutput->tableName,
                          argInfo->dagNodeNameByTable[tableIdx]) == 0);

            xSyslog(moduleName,
                    XlogInfo,
                    "Indexed dataset %s into %s completed",
                    datasetName,
                    argInfo->dagNodeNameByTable[tableIdx]);

            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }
CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    return status;
}

static void *
dataSetStressPerThread(void *arg)
{
    ArgDataSetStressPerThread *argInfo = (ArgDataSetStressPerThread *) arg;
    uint64_t threadNum = argInfo->threadId;
    Status status = StatusOk;

    xSyslog(moduleName, XlogInfo, "thread %lu begin tests.", threadNum);

    switch (argInfo->threadIdLocal % loadNtypes) {
    case loadYelp:
        xSyslog(moduleName,
                XlogInfo,
                "loadAndIndexYelpData thread %lu.",
                threadNum);
        status = loadAndIndexYelpData(argInfo);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "loadAndIndexYelpData thread %lu failed: %s",
                    threadNum,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        break;
    case loadGdelt:
        xSyslog(moduleName,
                XlogInfo,
                "loadAndIndexGdeltData thread %lu.",
                threadNum);
        status = loadAndIndexGdeltData(argInfo);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "loadAndIndexGdeltData thread %lu failed: %s",
                    threadNum,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        break;
    case loadJson:
        xSyslog(moduleName, XlogInfo, "loadRandomJson thread %lu.", threadNum);
        status = loadRandomJson(argInfo);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "loadRandomJson thread %lu failed: %s",
                    threadNum,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        break;
    default:
        break;
    };

    xSyslog(moduleName, XlogInfo, "cleanupTables thread %lu.", threadNum);
    status = cleanupTables(argInfo);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "cleanupTables thread %lu failed: %s",
                threadNum,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    xSyslog(moduleName,
            XlogInfo,
            "thread %lu ends tests with %s.\n",
            threadNum,
            strGetFromStatus(status));
    argInfo->retStatus = status;
    return NULL;
}

static void *
dataSetStressPerUser(void *arg)
{
    ArgDataSetStressPerUser *argInfo = (ArgDataSetStressPerUser *) arg;
    uint64_t threadNum = argInfo->threadId;
    Status status = StatusOk;
    ArgDataSetStressPerThread *targs = NULL;
    uint64_t sessionId = 0;
    char *userName = argInfo->userName;

    targs = (ArgDataSetStressPerThread *) memAlloc(
        sizeof(ArgDataSetStressPerThread) * dataSetFuncStressThreads);
    if (targs == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = listAndDeleteSession(userName, argInfo->sessionName);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = newSession(inputPid,
                        userName,
                        argInfo->sessionName,
                        false,
                        NULL,
                        &sessionId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "newSession creation failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < dataSetFuncStressThreads; ii++) {
        new (&targs[ii]) ArgDataSetStressPerThread();
        targs[ii].userName = userName;
        targs[ii].threadIdLocal = ii;
        status = Runtime::get()->createBlockableThread(&targs[ii].threadId,
                                                       NULL,
                                                       dataSetStressPerThread,
                                                       &targs[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    xSyslog(moduleName,
            XlogInfo,
            "dataSetStress start, %lu threads have been created.",
            dataSetFuncStressThreads);

    for (uint64_t ii = 0; ii < dataSetFuncStressThreads; ii++) {
        int ret = sysThreadJoin(targs[ii].threadId, NULL);
        assert(ret == 0);
        if (targs[ii].retStatus != StatusOk && status == StatusOk) {
            status = targs[ii].retStatus;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "dataSetStress end, %lu threads have been joined with %s.",
            dataSetFuncStressThreads,
            strGetFromStatus(status));

    // Out of resource is a valid/Success case for Functional tests.
    if (status == StatusNoMem) {
        status = StatusOk;
    }

    status = cleanupDatasets(userName);
    verify(status == StatusOk);

    status = deleteSession(userName, argInfo->sessionName, sessionId);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    xSyslog(moduleName,
            XlogInfo,
            "user thread %lu ends tests with %s.\n",
            threadNum,
            strGetFromStatus(status));
    argInfo->retStatus = status;
    return NULL;
}

Status
dataSetStress()
{
    Status status = StatusOk;
    Config *config = Config::get();
    const unsigned numActiveNodes = config->getActiveNodes();
    ArgDataSetStressPerUser *args = NULL;
    int ret = 0;

    inputNodeId = config->getMyNodeId();

    destIp = (char **) memAlloc(sizeof(char *) * numActiveNodes);
    if (destIp == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    destPort = (uint64_t *) memAlloc(sizeof(uint64_t) * numActiveNodes);
    if (destPort == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    for (unsigned ii = 0; ii < numActiveNodes; ii++) {
        destIp[ii] = config->getIpAddr(ii);
        destPort[ii] = config->getApiPort(ii);
    }

    args = (ArgDataSetStressPerUser *) memAlloc(
        sizeof(ArgDataSetStressPerUser) * dataSetFuncStressUsers);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    inputPid = getpid();
    ret = getlogin_r(tuserName, sizeof(tuserName));
    if (ret != 0) {
        uid_t uid;
        char *returnedPwd = NULL;
        size_t returnedPwdSize;
        struct passwd pwd;
        struct passwd *returnedPwdStruct;

        uid = getuid();
        returnedPwdSize = sysconf(_SC_GETPW_R_SIZE_MAX);
        assert(returnedPwdSize > 0);
        returnedPwd = (char *) memAllocExt(returnedPwdSize, moduleName);
        ret = getpwuid_r(uid,
                         &pwd,
                         returnedPwd,
                         returnedPwdSize,
                         &returnedPwdStruct);
        assert(ret == 0);
        strlcpy(tuserName, pwd.pw_name, sizeof(tuserName) - 1);
        memFree(returnedPwd);
    }

    for (uint64_t ii = 0; ii < dataSetFuncStressUsers; ii++) {
        // Supply unique userName and local userId for each user, since
        //
        // - this test could be run in a "allNodes" config
        // - a userName is tied to a specific node
        // - a user can have only one session active at one time

        new (&args[ii]) ArgDataSetStressPerUser();
        status = strSnprintf(args[ii].userName,
                             sizeof(args[ii].userName),
                             "%s%lu%lu",
                             tuserName,
                             inputNodeId,
                             ii);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "dataSetStress user %lu creation failed: %s",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        args[ii].userIdLocal = ii;

        assert(snprintf(args[ii].sessionName,
                        sizeof(args[ii].sessionName),
                        "DataSetFuncTestSession-n%u-u%lu",
                        config->getMyNodeId(),
                        ii) < (int) sizeof(args[ii].sessionName));

        status = Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       dataSetStressPerUser,
                                                       &args[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    xSyslog(moduleName,
            XlogInfo,
            "dataSetStress start, %lu threads (users) have been created.",
            dataSetFuncStressUsers);

    for (uint64_t ii = 0; ii < dataSetFuncStressUsers; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        assert(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "dataSetStress end, %lu threads (users) have been joined %s.",
            dataSetFuncStressUsers,
            strGetFromStatus(status));

CommonExit:
    if (destIp != NULL) {
        memFree(destIp);
        destIp = NULL;
    }

    if (destPort != NULL) {
        memFree(destPort);
        destPort = NULL;
    }

    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

Status
dataSetParseConfig(Config::Configuration *config,
                   char *key,
                   char *value,
                   bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key, strGetFromLibDsFuncTestConfig(LibDsFuncStressUsers)) ==
        0) {
        dataSetFuncStressUsers = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDsFuncTestConfig(
                              LibDsFuncStressThreads)) == 0) {
        // See block comment next to dataSetFuncStressThreads above about how
        // its value impacts which URLs end up being loaded by this test. It's
        // best to leave this config param alone, and change
        // dataSetFuncStressUsers for scale. But it's there to stress test the
        // per-user concurrent loads without worrying about realistic scenarios
        dataSetFuncStressThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDsFuncTestConfig(
                              LibDsQaYelpReviewsUrl)) == 0) {
        verify(strlcpy(dfQaYelpReviewsUrl, value, sizeof(dfQaYelpReviewsUrl)) <
               sizeof(dfQaYelpReviewsUrl));
    } else if (strcasecmp(key,
                          strGetFromLibDsFuncTestConfig(
                              LibDsQaRandomJsonUrl)) == 0) {
        verify(strlcpy(dfQaRandomJsonUrl, value, sizeof(dfQaRandomJsonUrl)) <
               sizeof(dfQaRandomJsonUrl));
    } else if (strcasecmp(key,
                          strGetFromLibDsFuncTestConfig(LibDsQaYelpUserUrl)) ==
               0) {
        verify(strlcpy(dfQaYelpUserUrl, value, sizeof(dfQaYelpUserUrl)) <
               sizeof(dfQaYelpUserUrl));
    } else if (strcasecmp(key,
                          strGetFromLibDsFuncTestConfig(LibDsQaGDeltUrl)) ==
               0) {
        verify(strlcpy(dsQaGDeltUrl, value, sizeof(dsQaGDeltUrl)) <
               sizeof(dsQaGDeltUrl));
    } else {
        status = StatusUsrNodeIncorrectParams;
    }
    return status;
}
