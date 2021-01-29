// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

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
#include "config/Config.h"
#include "primitives/Primitives.h"
#include "util/Random.h"
#include "util/System.h"
#include "libapis/LibApisSend.h"
#include "libapis/LibApisCommon.h"
#include "hash/Hash.h"
#include "JsonGenEnums.h"
#include "table/ResultSet.h"
#include "df/DataFormat.h"
#include "util/MemTrack.h"
#include "SourceTypeEnum.h"
#include "operators/Dht.h"
#include "log/Log.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "strings/String.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "xcalar/compute/localtypes/Dataflow.pb.h"

#include "test/QA.h"  // Must be last

extern const char *ExDefaultExportTargetName;
static const char *moduleName = "LibApisTests";

enum { NotMatch = false, Match = true };

enum {
    NotApplyNewFieldName = false,
    ApplyNewFieldName = true,
};

static Status checkListTables();
static Status edgeCasesTests();
static Status aggregateTest();
static Status queryTest();
static Status dagTest();
static Status createDagFailurTest();
static Status checkListConstants();
static Status dhtBasicTest();
static Status dropDatasetTest();
static Status userLen();

static TestCase testCases[] = {
    {"Invalid user test", userLen, TestCaseEnable, "5095"},
    {"Drop dataset test", dropDatasetTest, TestCaseEnable, ""},
    // Witness to Xc-1373
    {"creating DAG node failure test",
     createDagFailurTest,
     TestCaseEnable,
     "1373"},
    {"Dag test", dagTest, TestCaseEnable, ""},
    // XXX: Xc-1507 May need to disable "Check list tables" if problems
    //      manifest themselves with concurrent DAG updates from multiple
    //      threads sharing the same session.  The code currently allows
    //      sharing since DAG locking should protect it and there is no
    //      problem with conucrrent session updates.  However, there is a
    //      timing window with delete session.
    {"Check list tables", checkListTables, TestCaseEnable, ""},
    {"Edge case tests", edgeCasesTests, TestCaseEnable, ""},
    {"Aggregate test", aggregateTest, TestCaseEnable, ""},
    {"Query test", queryTest, TestCaseEnable, ""},
    {"Dht Basic Checks", dhtBasicTest, TestCaseEnable, ""},
};

enum {
    NumNodes = 3,
    MaxNumKeys = 10,
    MaxNumTables = 128,
    MaxNumDatasets = 11,
    MaxDagNodeNameLen = 256,
    queryTestNum = 5
};

static char *destIp[NumNodes];
static uint64_t destPort[NumNodes];

struct JsonTable {
    const char *tableName;
    const char *keyNames[MaxNumKeys];
};

struct CsvTable {
    unsigned colNum;
    const char *origKeyName;
    const char *newKeyName;
    DfFieldType keyType;
    bool columnRenamed;
};

#define RandomJsonMemDataset "ds-randomJsonMem"
static void loadRandomJson(const char *datasetName);

CsvTable gdeltTables[] = {
    {0, "column0", "GlobalEventID", DfString, false},
};

JsonTable yelpTables[] = {
    {"reviews", {"user_id", "stars", NULL}},
    {"user", {"user_id", "review_count", NULL}},
};

char dagNodeNameByTable[MaxNumTables][MaxDagNodeNameLen];
unsigned numTablesIndexed = 0;

struct Datasets {
    char name[MaxDagNodeNameLen + 1];
    char path[XcalarApiMaxPathLen + 1];
};

Datasets datasets[MaxNumDatasets];
unsigned numDataset = 0;

char dagNodeNameByConstant[MaxNumTables][MaxDagNodeNameLen];
unsigned numConstant = 0;

static bool yelpTablesLoaded = false;
static bool gdeltTablesLoaded = false;

pid_t inputPid = 0;
char userName[LOGIN_NAME_MAX + 1] = "xcalar";
uint64_t sessionId = 0;
char sessionName[] = "LibApisTests session";

static void
verifyTableExists(const char *tableName, const char *sessionName)
{
    XcalarWorkItem *workItem;
    Status status;
    workItem = xcalarApiMakeGetTableMeta(tableName, true, false, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output != NULL);
    assert(workItem->outputSize > 0);

    XcalarApiGetTableMetaOutput *getTableMetaOutput;
    uint64_t seqNumEntries;
    getTableMetaOutput = &workItem->output->outputResult.getTableMetaOutput;
    printf("getTableMeta: workItem->output->hdr.status: %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());
    assert(getTableMetaOutput->numMetas == NumNodes);

    seqNumEntries = 0;
    for (unsigned ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        seqNumEntries += getTableMetaOutput->metas[ii].numRows;
    }
    assert(seqNumEntries > 0);
}

static double
doAggregate(const char *srcTableName,
            const char *dstTableName,
            const char *aggStr)
{
    double returnedAnswer;
    json_t *jsonRecord, *jsonValue;
    json_error_t jsonError;
    const char *aggregateField = "Value";
    XcalarWorkItem *workItem;
    XcalarApiAggregateOutput *aggregateOutput;
    Status status;

    workItem = xcalarApiMakeAggregateWorkItem(srcTableName,
                                              dstTableName,
                                              aggStr,
                                              sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->outputSize > 0);

    aggregateOutput = &workItem->output->outputResult.aggregateOutput;
    printf("workItem->output->hdr.status = %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());

    if (dstTableName != NULL) {
        assert(strcmp(aggregateOutput->tableName, dstTableName) == 0);
    }

    jsonRecord = json_loads(aggregateOutput->jsonAnswer, 0, &jsonError);
    assert(jsonRecord != NULL);

    jsonValue = json_object_get(jsonRecord, aggregateField);
    assert(jsonValue != NULL);
    assert(json_typeof(jsonValue) == JSON_REAL);

    returnedAnswer = json_real_value(jsonValue);

    json_decref(jsonRecord);
    jsonRecord = jsonValue = NULL;

    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    aggregateOutput = NULL;

    return returnedAnswer;
}

// Create a new session to track our tables...
static Status
newSession(pid_t pidIn,
           char *userNameIn,
           char *sessionNameIn,
           uint64_t *sessionIdOut)
{
    Status status = StatusUnknown;

    XcalarWorkItem *workItem = NULL;

    XcalarApiSessionNewOutput *sessionNewOutput = NULL;

    workItem = xcalarApiMakeSessionNewWorkItem(sessionNameIn, false, NULL);
    status =
        xcalarApiQueueWork(workItem, destIp[0], destPort[0], userNameIn, pidIn);
    sessionNewOutput = &workItem->output->outputResult.sessionNewOutput;

    if (status != StatusOk || workItem->output->hdr.status != StatusOk.code()) {
        printf("Create new session failed\n");
        if (status == StatusOk) {
            assert(sessionNewOutput != NULL);
            printf("Status: %s\n Session error message: %s\n",
                   strGetFromStatusCode(workItem->output->hdr.status),
                   sessionNewOutput->error);
            status.fromStatusCode(workItem->output->hdr.status);
        } else {
            printf("Status: %s", strGetFromStatus(status));
        }
    } else {
        printf("Session created, ID = %lX \n", sessionNewOutput->sessionId);
        *sessionIdOut = sessionNewOutput->sessionId;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// Delete the sesion that we created at the beginning of the testcase
//
static void
deleteSession()
{
    Status status = StatusUnknown;
    char sessionName[256] = "\0";

    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;

    workItem = xcalarApiMakeSessionInactWorkItem(sessionName);
    workItem->input->sessionDeleteInput.sessionId = sessionId;

    // The session must be inactive before it can be deleted
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    if (status != StatusOk &&
        workItem->output->hdr.status != StatusSessionExists.code()) {
        printf("Session inact failed: %s\n", strGetFromStatus(status));
        sessionGenericOutput =
            &workItem->output->outputResult.sessionGenericOutput;
        printf("%s\n", sessionGenericOutput->errorMessage);
        assert(0);
    }
    xcalarApiFreeWorkItem(workItem);

    // Now we can delete the session
    workItem = xcalarApiMakeSessionDeleteWorkItem(sessionName);
    workItem->input->sessionDeleteInput.sessionId = sessionId;

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    if (status != StatusOk) {
        printf("Session delete failed: %s\n", strGetFromStatus(status));
        sessionGenericOutput =
            &workItem->output->outputResult.sessionGenericOutput;
        printf("%s\n", sessionGenericOutput->errorMessage);
        assert(0);
    }
    printf("Session successfully deleted\n");

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }
}

// Get a list of the user's active sessions (expect none).  If there's
// a session for LibApisTests, delete it because it will cause problems later.
static void
listAndDeleteSession()
{
    Status status = StatusUnknown;

    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionListOutput *listOutput = NULL;

    // We only want an exact match for the session name this test uses
    workItem = xcalarApiMakeSessionListWorkItem(sessionName);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    listOutput = &workItem->output->outputResult.sessionListOutput;

    // We only expect:
    //   StatusOk - An exact match was found
    //   StatusSessionNotFound - there are no sessions

    if (status != StatusOk ||
        (workItem->output->hdr.status != StatusOk.code() &&
         workItem->output->hdr.status != StatusSessionNotFound.code())) {
        printf("Session list failed\n");
        assert(0);
    }

    // If a matching session (and ONLY one session) was found, delete it

    if (listOutput->numSessions == 1) {
        sessionId = listOutput->sessions[0].sessionId;
        deleteSession();
        sessionId = 0;
    } else {
        assert(listOutput->numSessions == 0);
    }
    xcalarApiFreeWorkItem(workItem);
}

static void
compareDatasetName(const char *datasetNameIn, const char *datasetNameOut)
{
    char datasetNameFull[DagTypes::MaxNameLen];

    snprintf(datasetNameFull,
             sizeof(datasetNameFull),
             XcalarApiDatasetPrefix "%s",
             datasetNameIn);
    assert(strcmp(datasetNameFull, datasetNameOut) == 0);
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
    assert(buf != NULL);
    assert(tableName != NULL);
    assert(keyName != NULL);

    if (dbOperator == NULL) {
        snprintf(buf, bufLen, "%s-%s-%s", datasetName, tableName, keyName);
    } else {
        assert(dbOperator != NULL);
        assert(operation != NULL);
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

static bool
getDatasetIdx(const char *datasetUrl, unsigned *datasetIdx)
{
    unsigned ii;
    for (ii = 0; ii < numDataset; ii++) {
        if (strcmp(datasets[ii].path, datasetUrl) == 0) {
            *datasetIdx = ii;
            return true;
        }
    }
    return false;
}

static bool
getTableIdx(const char *datasetName,
            const char *tableName,
            const char *keyName,
            const char *dbOperator,
            const char *operation,
            unsigned *tableIdx)
{
    char tmpTableName[MaxDagNodeNameLen];
    unsigned ii;

    makeTableName(tmpTableName,
                  sizeof(tmpTableName),
                  datasetName,
                  tableName,
                  keyName,
                  dbOperator,
                  operation);

    for (ii = 0; ii < numTablesIndexed; ii++) {
        if (strcmp(dagNodeNameByTable[ii], tmpTableName) == 0) {
            *tableIdx = ii;
            return true;
        }
    }
    return false;
}

static void
loadRandomData(const char *tableNameIn, char *tableNameOut, size_t bufSize)
{
    XcalarWorkItem *workItem;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    XcalarApiNewTableOutput *indexOutput;
    size_t sizeToLoad;
    Status status;
    const char *tableName = NULL;
    char dataset[DsDatasetNameLen + 1];
    char tableNameBuf[MaxDagNodeNameLen];
    DfLoadArgs *loadArgs = new DfLoadArgs();  // let throw
    unsigned datasetIdx = numDataset++;
    size_t ret;

    if (tableNameIn == NULL) {
        assert(tableNameOut != NULL);
    }

    sizeToLoad = 4 * MB;
    memZero(loadArgs, sizeof(*loadArgs));

    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    ret = strlcpy(loadArgs->sourceArgsList[0].targetName,
                  "Default Shared Root",
                  sizeof(loadArgs->sourceArgsList[0].targetName));
    assert(ret <= sizeof(loadArgs->sourceArgsList[0].targetName));
    ret = snprintf(loadArgs->sourceArgsList[0].path,
                   sizeof(loadArgs->sourceArgsList[0].path),
                   "%s/jsonRandom",
                   qaGetQaDir());
    assert(ret <= sizeof(loadArgs->sourceArgsList[0].path));
    loadArgs->sourceArgsList[0].recursive = false;

    strlcpy(loadArgs->parseArgs.parserFnName,
            "default:parseJson",
            sizeof(loadArgs->parseArgs.parserFnName));
    strlcpy(loadArgs->parseArgs.parserArgJson,
            "{}",
            sizeof(loadArgs->parseArgs.parserArgJson));

    loadArgs->maxSize = sizeToLoad;
    snprintf(dataset, sizeof(dataset), "random data %s", tableNameIn);

    // Create the dataset meta
    workItem =
        xcalarApiMakeDatasetCreateWorkItem(dataset, loadArgs, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    xcalarApiFreeWorkItem(workItem);

    // Now load the dataset which will use the loadargs specified in the dataset
    // create.
    workItem = xcalarApiMakeBulkLoadWorkItem(dataset, loadArgs, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    bulkLoadOutput = &workItem->output->outputResult.loadOutput;
    assert((uintptr_t) bulkLoadOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
    assert(workItem->output->hdr.status == StatusOk.code());
    strlcpy(dataset, bulkLoadOutput->dataset.name, sizeof(dataset));
    xcalarApiFreeWorkItem(workItem);
    bulkLoadOutput = NULL;

    strlcpy(datasets[datasetIdx].name,
            dataset,
            sizeof(datasets[datasetIdx].name));

    strlcpy(datasets[datasetIdx].path,
            loadArgs->sourceArgsList[0].path,
            sizeof(datasets[datasetIdx].path));

    const char *keyName = "JgUnsignedInteger";
    workItem = xcalarApiMakeIndexWorkItem(dataset,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          tableNameIn,
                                          NULL,
                                          "p",
                                          false,
                                          false,
                                          sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    indexOutput = &workItem->output->outputResult.indexOutput;
    assert((uintptr_t) indexOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));
    assert(workItem->output->hdr.status == StatusOk.code());
    if (tableNameIn == NULL) {
        strlcpy(tableNameBuf, indexOutput->tableName, sizeof(tableNameBuf));
        tableName = tableNameBuf;
    } else {
        tableName = tableNameIn;
        assert(strcmp(tableNameIn, indexOutput->tableName) == 0);
    }
    assert(tableName[0] != '\0');
    strlcpy(tableNameOut, tableName, bufSize);

    xcalarApiFreeWorkItem(workItem);
    delete loadArgs;
}

void
loadAndIndexGdeltData()
{
    Status status;
    XcalarWorkItem *workItem;
    DfLoadArgs *loadArgs = new DfLoadArgs();  // let throw
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    XcalarApiNewTableOutput *indexOutput;
    unsigned tableIdx;
    // This will go away as soon as we don't require keys for load
    unsigned ii;
    char dataset[DsDatasetNameLen + 1];
    const char *datasetName = "gdelt";
    unsigned datasetIdx = numDataset++;
    size_t ret;

    memZero(loadArgs, sizeof(*loadArgs));

    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    ret = strlcpy(loadArgs->sourceArgsList[0].targetName,
                  "Default Shared Root",
                  sizeof(loadArgs->sourceArgsList[0].targetName));
    assert(ret <= sizeof(loadArgs->sourceArgsList[0].targetName));
    ret = snprintf(loadArgs->sourceArgsList[0].path,
                   sizeof(loadArgs->sourceArgsList[0].path),
                   "%s/jsonRandom",
                   qaGetQaDir());
    assert(ret <= sizeof(loadArgs->sourceArgsList[0].path));
    loadArgs->sourceArgsList[0].recursive = false;

    strlcpy(loadArgs->parseArgs.parserFnName,
            "default:parseCsv",
            sizeof(loadArgs->parseArgs.parserFnName));
    strlcpy(loadArgs->parseArgs.parserArgJson,
            "{}",
            sizeof(loadArgs->parseArgs.parserArgJson));

    loadArgs->maxSize = 100 * GB;

    // Create the dataset meta using the same API input as used for the bulk
    // load.
    workItem =
        xcalarApiMakeDatasetCreateWorkItem(datasetName, loadArgs, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    xcalarApiFreeWorkItem(workItem);

    // Now load the dataset using the meta specified in the create
    workItem =
        xcalarApiMakeBulkLoadWorkItem(datasetName, loadArgs, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    bulkLoadOutput = &workItem->output->outputResult.loadOutput;
    assert((uintptr_t) bulkLoadOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
    assert(workItem->output->hdr.status == StatusOk.code());
    strlcpy(dataset, bulkLoadOutput->dataset.name, sizeof(dataset));

    ret = strlcpy(datasets[datasetIdx].name,
                  dataset,
                  sizeof(datasets[datasetIdx].name));
    assert(ret <= sizeof(datasets[datasetIdx].name));

    ret = snprintf(datasets[datasetIdx].path,
                   sizeof(datasets[datasetIdx].path),
                   "%s/gdelt-small",
                   qaGetQaDir());
    assert(ret <= sizeof(datasets[datasetIdx].path));

    printf("Loaded %s into dataset %s\n", datasets[datasetIdx].path, dataset);
    xcalarApiFreeWorkItem(workItem);
    bulkLoadOutput = NULL;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasets, NULL, 0);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    XcalarApiListDatasetsOutput *listDatasetsOutput =
        &workItem->output->outputResult.listDatasetsOutput;

    for (ii = 0; ii < listDatasetsOutput->numDatasets; ii++) {
        if (strcmp(listDatasetsOutput->datasets[ii]
                       .loadArgs.sourceArgsList[0]
                       .path,
                   datasets[datasetIdx].path) == 0) {
            assert(strcmp(dataset, listDatasetsOutput->datasets[ii].name) == 0);
            break;
        }
    }
    xcalarApiFreeWorkItem(workItem);

    for (ii = 0; ii < ArrayLen(gdeltTables); ii++) {
        tableIdx = numTablesIndexed++;
        makeTableName(dagNodeNameByTable[tableIdx],
                      MaxDagNodeNameLen,
                      "gdelt",
                      "small",
                      gdeltTables[ii].origKeyName,
                      NULL,
                      NULL);
        printf("Indexing %s as %s\n", dataset, dagNodeNameByTable[tableIdx]);
        workItem = xcalarApiMakeIndexWorkItem(dataset,
                                              NULL,
                                              1,
                                              &gdeltTables[ii].origKeyName,
                                              NULL,
                                              NULL,
                                              NULL,
                                              dagNodeNameByTable[tableIdx],
                                              NULL,
                                              "p",
                                              false,
                                              false,
                                              sessionName);
        assert(workItem != NULL);

        status = xcalarApiQueueWork(workItem,
                                    destIp[0],
                                    destPort[0],
                                    userName,
                                    inputPid);
        assert(status == StatusOk);

        indexOutput = &workItem->output->outputResult.indexOutput;
        assert((uintptr_t) indexOutput ==
               (uintptr_t) &workItem->output->outputResult);
        assert(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));

        assert(workItem->output->hdr.status == StatusOk.code());
        assert(strcmp(dagNodeNameByTable[tableIdx], indexOutput->tableName) ==
               0);

        printf("Indexed %s\n", dagNodeNameByTable[tableIdx]);
        xcalarApiFreeWorkItem(workItem);
    }
    workItem = NULL;

    gdeltTablesLoaded = true;
    delete loadArgs;
}

uint64_t
count(const char *datasetName, const char *tableName)
{
    uint64_t ii, result = 0;
    Status status;
    bool isTable;
    const char *srcName = NULL;
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetTableMetaOutput *getTableMetaOutput;

    isTable = (tableName != NULL);
    srcName = (isTable) ? tableName : datasetName;

    workItem = xcalarApiMakeGetTableMeta(srcName, isTable, false, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output != NULL);
    assert(workItem->outputSize > 0);

    getTableMetaOutput = &workItem->output->outputResult.getTableMetaOutput;
    printf("getTableMetaOutput->status: %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());
    assert(getTableMetaOutput->numMetas == NumNodes);

    for (ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        result += getTableMetaOutput->metas[ii].numRows;
    }

    printf("%s %s has %llu records\n",
           (isTable) ? "Table" : "Dataset",
           srcName,
           (unsigned long long) result);

    xcalarApiFreeWorkItem(workItem);

    return result;
}

static void
waitForQuery(char *queryName)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;

    workItem = xcalarApiMakeQueryStateWorkItem(queryName, false);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    while (workItem->output->outputResult.queryStateOutput.queryState !=
               qrFinished &&
           workItem->output->outputResult.queryStateOutput.queryState !=
               qrError) {
        assert(workItem != NULL);
        xcalarApiFreeWorkItem(workItem);
        sysSleep(1);

        workItem = xcalarApiMakeQueryStateWorkItem(queryName, false);
        status = xcalarApiQueueWork(workItem,
                                    destIp[0],
                                    destPort[0],
                                    userName,
                                    inputPid);
        assert(status == StatusOk);
        assert(workItem->output->hdr.status == StatusOk.code());
    }

    assert(workItem != NULL);
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
}

static Status
submitQuery(char *query,
            char *queryName,
            size_t buffSize,
            const char *schedName)
{
    Status status = StatusOk;
    xcalar::compute::localtypes::Dataflow::ExecuteRequest executeReq;
    xcalar::compute::localtypes::Dataflow::ExecuteResponse executeResp;
    ProtoRequestMsg requestMsg;
    ProtoResponseMsg responseMsg;
    ServiceSocket socket;

    printf("submit query:%s \n", query);

    try {
        executeReq.set_job_name(queryName);
        executeReq.set_dataflow_str(query);
        executeReq.set_sched_name(schedName);
        executeReq.set_is_async(false);
        executeReq.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(userName);
        executeReq.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(sessionName);
        requestMsg.mutable_servic()->set_servicename("Dataflow");
        requestMsg.mutable_servic()->set_methodname("Execute");
        requestMsg.mutable_servic()->mutable_body()->PackFrom(executeReq);
        requestMsg.set_requestid(0);
        requestMsg.set_target(ProtoMsgTargetService);
    } catch (...) {
        status = StatusNoMem;
    }
    assert(status == StatusOk);

    status = socket.init();
    assert(status == StatusOk);
    status = socket.sendRequest(&requestMsg, &responseMsg);
    assert(status == StatusOk);
    bool success = responseMsg.servic().body().UnpackTo(&executeResp);
    printf("submitQuery returned: %s\n", strGetFromStatus(status));
    assert(success);

    if (queryName != NULL && buffSize > 0) {
        strlcpy(queryName, executeResp.job_name().c_str(), buffSize);
    }

    return status;
}

static QueryState
checkQueryState(char *queryName)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    QueryState queryState;
    Status queryStatus;
    XcalarApiQueryStateOutput *queryStateOutput;

    workItem = xcalarApiMakeQueryStateWorkItem(queryName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    assert(workItem->output->hdr.status == StatusOk.code());

    queryStateOutput = &workItem->output->outputResult.queryStateOutput;

    queryState = queryStateOutput->queryState;
    queryStatus.fromStatusCode(queryStateOutput->queryStatus);

    if (queryState == qrError) {
        printf("number of failed work: %lu\n",
               queryStateOutput->numFailedWorkItem);

        printf("number of completed work: %lu\n",
               queryStateOutput->numCompletedWorkItem);

        printf("number of queued work: %lu\n",
               queryStateOutput->numQueuedWorkItem);
    }

    printf("QueryStatus: %s\n", strGetFromStatus(queryStatus));

    xcalarApiFreeWorkItem(workItem);

    return queryState;
}

// Check that ApisRecv rejects requests with user names that are too short
// and that ApisSend rejects user names that are too long.  ApisRecv also
// checks for names that are too long, but it won't get past ApisSend so
// we can't test it directly unless we create our own socket to send the
// request to the user node.
static Status
userLen()
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;

    char userLenTestName[LOGIN_NAME_MAX + 1];
    char tooLongUserName[LOGIN_NAME_MAX + 2];

    // If the test gets by ApisRecv, session list will fail with an obvious
    // error without causing any further damage
    workItem = xcalarApiMakeSessionListWorkItem(sessionName);
    assert(workItem != NULL);

    // Zero length user name
    memset(&userLenTestName, 0, sizeof(userLenTestName));
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userLenTestName,
                                987654);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusInvalidUserNameLen.code());

    // Free the work item and get a new one instead of directly modifying the
    // userId block in the work item.
    xcalarApiFreeWorkItem(workItem);
    workItem = xcalarApiMakeSessionListWorkItem(sessionName);

    // User name that is too long (current detected by ApisSend).  Have to
    // specify a null terminated string that is too long to avoid address
    // sanitizer detecting stack-buffer-overflow.
    memset(&tooLongUserName, 'Z', sizeof(tooLongUserName));
    tooLongUserName[LOGIN_NAME_MAX + 1] = '\0';
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                tooLongUserName,
                                987654);
    assert(status == StatusOverflow);

    xcalarApiFreeWorkItem(workItem);

    return StatusOk;
}

static Status
dhtBasicTest()
{
    Status status;
    XcalarWorkItem *workItem;
    const char *dhtName = "yelpUsersAvgStarsDht";
    unsigned datasetIdx;
    bool foundDataset;
    const char *datasetName;
    const char *tableName1 = "dhtBasicTest/CustomDhtTable";
    const char *tableName2 = "dhtBasicTest/DefaultDhtTable";
    const char *keyName = "average_stars";
    unsigned ii;

    // Create Dht
    workItem = xcalarApiMakeCreateDhtWorkItem(dhtName, 5.0, 0.0, Unordered);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    printf("workItem->output->hdr.status: %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code() ||
           workItem->output->hdr.status == StatusNsObjAlreadyExists.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // Index yelpUsers stars with custom DHT
    XcalarApiNewTableOutput *indexOutput = NULL;
    char path[QaMaxQaDirLen + 1];
    size_t ret;
    ret = snprintf(path, sizeof(path), "%s/yelp/user", qaGetQaDir());
    assert(ret <= sizeof(path));
    foundDataset = getDatasetIdx(path, &datasetIdx);
    assert(foundDataset);
    datasetName = datasets[datasetIdx].name;
    printf("Indexing %s\n", datasetName);
    Ordering ordering = OrderingInvalid;
    workItem = xcalarApiMakeIndexWorkItem(datasetName,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          &ordering,
                                          tableName1,
                                          dhtName,
                                          "p",
                                          false,
                                          false,
                                          sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    indexOutput = &workItem->output->outputResult.indexOutput;
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));
    assert(strcmp(tableName1, indexOutput->tableName) == 0);
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    indexOutput = NULL;

    // Verify that the DHT is distributing across all nodes
    uint64_t totalCount = 0;
    XcalarApiGetTableMetaOutput *getTableMetaOutput = NULL;
    workItem = xcalarApiMakeGetTableMeta(tableName1, true, false, sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    getTableMetaOutput = &workItem->output->outputResult.getTableMetaOutput;

    printf("Table distribution for %s\n", tableName1);
    for (ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        printf("Node %u - %lu\n", ii, getTableMetaOutput->metas[ii].numRows);
        assert(getTableMetaOutput->metas[ii].numRows > 0);
        totalCount += getTableMetaOutput->metas[ii].numRows;
    }
    assert(totalCount == 70817);

    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    getTableMetaOutput = NULL;

    // Verify that a join cannot happen on another table hashed by another DHT
    workItem = xcalarApiMakeIndexWorkItem(datasetName,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          tableName2,
                                          NULL,
                                          "p",
                                          false,
                                          false,
                                          sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    indexOutput = &workItem->output->outputResult.indexOutput;
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));
    assert(strcmp(tableName2, indexOutput->tableName) == 0);
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    indexOutput = NULL;
    printf("Attempting to join %s with %s\n", tableName1, tableName2);
    workItem = xcalarApiMakeJoinWorkItem(tableName1,
                                         tableName2,
                                         "ShouldNotExists",
                                         InnerJoin,
                                         false,
                                         true,
                                         false,
                                         0,
                                         0,
                                         NULL,
                                         NULL,
                                         sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusJoinDhtMismatch.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // Drop the tables we created
    status = xcalarApiDeleteTable(tableName1,
                                  destIp[0],
                                  destPort[0],
                                  userName,
                                  inputPid,
                                  sessionName);
    assert(status == StatusOk);
    status = xcalarApiDeleteTable(tableName2,
                                  destIp[0],
                                  destPort[0],
                                  userName,
                                  inputPid,
                                  sessionName);
    assert(status == StatusOk);

    // Delete the DHT we created
    size_t dhtNameLen = strlen(dhtName);
    workItem = xcalarApiMakeDeleteDhtWorkItem(dhtName, dhtNameLen);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    printf("workItem->output->hdr.status: %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    return StatusOk;
}

static Status
createDagFailurTest()
{
    XcalarWorkItem *workItem;
    Status status;

    const char *keyName = "None existing key";
    workItem = xcalarApiMakeIndexWorkItem("None existing dataset",
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          "None existing table",
                                          NULL,
                                          "p",
                                          false,
                                          false,
                                          sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk ||
           workItem->output->hdr.status != StatusOk.code());

    assert(workItem->output->hdr.status == StatusDagNodeNotFound.code());

    xcalarApiFreeWorkItem(workItem);

    return StatusOk;
}

static Status
dagTest()
{
    Status status = StatusOk;
    XcalarApiDagOutput *dagOutput = NULL;
    char *dagQuery = new char[XcalarApiMaxQuerySize];  // let throw
    const char *leftTable = "indexLeft-dT";
    const char *rightTable = "indexRight-dT";
    const char *joinTable = "joinTable-dT";
    const char *filterTable = "filterTable-dT";
    const char *queryName = "dagTestQuery";

    XcalarWorkItem *workItem = NULL;
    XcalarApis apiArray[] = {
        [0] = XcalarApiFilter,
        [1] = XcalarApiJoin,
        [2] = XcalarApiIndex,
        [3] = XcalarApiIndex,
        [4] = XcalarApiBulkLoad,
        [5] = XcalarApiBulkLoad,
    };

    snprintf(dagQuery,
             XcalarApiMaxQuerySize,
             "index --key JgUnsignedInteger "
             "--dataset " XcalarApiDatasetPrefix
             "%s "
             "--prefix " XcalarApiDatasetPrefix
             "%s "
             "--dsttable %s;"
             "index --key JgSignedInteger "
             "--dataset " XcalarApiDatasetPrefix
             "%s "
             "--prefix p "
             "--dsttable %s;"
             "join --leftTable %s "
             "--rightTable %s "
             "--joinTable %s "
             "--leftRenameMap " XcalarApiDatasetPrefix
             "%s:dsLeft:0 "
             "-c;"
             "filter --srctable %s --eval \"gt(dsLeft::f0, 1)\" --dsttable %s",
             RandomJsonMemDataset,
             RandomJsonMemDataset,
             leftTable,
             RandomJsonMemDataset,
             rightTable,
             leftTable,
             rightTable,
             joinTable,
             RandomJsonMemDataset,
             joinTable,
             filterTable);

    status =
        submitQuery(dagQuery, (char *) queryName, 0, Runtime::NameSchedId0);
    assert(status == StatusOk);
    printf("Sleeping 5 seconds to allow query finish\n");
    sysSleep(5);

    QueryState queryState = checkQueryState((char *) queryName);
    printf("queryState: %s\n", strGetFromQueryState(queryState));
    assert(queryState == qrFinished);

    workItem = xcalarApiMakeQueryDeleteWorkItem(queryName);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    xcalarApiFreeWorkItem(workItem);

    // verify the last table is created
    workItem = xcalarApiMakeGetTableMeta(filterTable, true, false, sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    // Test non-existing dag
    workItem = xcalarApiMakeDagWorkItem("NonExistingTable", sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    printf("workItem->output->hdr.status = %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusDagNodeNotFound.code());
    xcalarApiFreeWorkItem(workItem);

    workItem = xcalarApiMakeDagWorkItem(filterTable, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    printf("status: %s\n", strGetFromStatus(status));
    assert(status == StatusOk);

    dagOutput = &workItem->output->outputResult.dagOutput;

    assert(workItem->output->hdr.status == StatusOk.code());

    for (unsigned ii = 0; ii < dagOutput->numNodes; ++ii) {
        assert(apiArray[ii] == dagOutput->node[ii]->hdr.api);
    }

    xcalarApiFreeWorkItem(workItem);

    status = xcalarApiDeleteTable(filterTable,
                                  destIp[0],
                                  destPort[0],
                                  userName,
                                  inputPid,
                                  sessionName);
    assert(status == StatusOk);
    status = xcalarApiDeleteTable(joinTable,
                                  destIp[0],
                                  destPort[0],
                                  userName,
                                  inputPid,
                                  sessionName);
    assert(status == StatusOk);
    status = xcalarApiDeleteTable(rightTable,
                                  destIp[0],
                                  destPort[0],
                                  userName,
                                  inputPid,
                                  sessionName);
    assert(status == StatusOk);
    status = xcalarApiDeleteTable(leftTable,
                                  destIp[0],
                                  destPort[0],
                                  userName,
                                  inputPid,
                                  sessionName);
    assert(status == StatusOk);
    delete[] dagQuery;

    return status;
}

// XXX: currently the queries are hard coded
static Status
queryTest()
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetTableMetaOutput *getTableMetaOutput;
    uint64_t queryNumEntries, seqNumEntries, ii;
    char *queryIndexLeft = new char[XcalarApiMaxQuerySize];   // let throw
    char *queryFilter = new char[XcalarApiMaxQuerySize];      // let throw
    char *queryMap = new char[XcalarApiMaxQuerySize];         // let throw
    char *queryIndexRight = new char[XcalarApiMaxQuerySize];  // let throw
    char *queryJoin = new char[XcalarApiMaxQuerySize];        // let throw
    char *query = new char[XcalarApiMaxQuerySize];            // let throw
    const char *filterStr =
        "gt(add(p::JgUnsignedInteger, p::JgSignedInteger), 100)";
    const char *evalString = "add(p::JgUnsignedInteger, p::JgSignedInteger)";

    enum {
        IndexTableLeft = 0,
        FilterTable,
        MapTable,
        IndexTableRight,
        JoinTable,
        Size,
    };

    enum {
        MapFieldName = 0,
        FieldNameSize,
    };

    const char *queryFieldNames[FieldNameSize] = {
        [MapFieldName] = "SumValue",
    };

    const char *query1TableNames[Size] = {
        [IndexTableLeft] = "indexTableLeft-qT1",
        [FilterTable] = "filterTable-qT1",
        [MapTable] = "mapTable-qT1",
        [IndexTableRight] = "indexTableRight-qT1",
        [JoinTable] = "joinTable-qT1",
    };

    const char *query2TableNames[Size] = {
        [IndexTableLeft] = "indexTableLeft-qT2",
        [FilterTable] = "filterTable-qT2",
        [MapTable] = "mapTable-qT2",
        [IndexTableRight] = "indexTableRight-qT2",
        [JoinTable] = "joinTable-qT2",
    };

    snprintf(queryIndexLeft,
             XcalarApiMaxQuerySize,
             "index --key JgString "
             "--dataset " XcalarApiDatasetPrefix "%s --dsttable %s --prefix p",
             RandomJsonMemDataset,
             query1TableNames[IndexTableLeft]);

    snprintf(queryFilter,
             XcalarApiMaxQuerySize,
             "filter --srctable %s --eval \"%s\" --dsttable %s",
             query1TableNames[IndexTableLeft],
             filterStr,
             query1TableNames[FilterTable]);

    snprintf(queryMap,
             XcalarApiMaxQuerySize,
             "map --eval \"%s\" "
             "--srctable %s --fieldName %s --dsttable %s",
             evalString,
             query1TableNames[FilterTable],
             queryFieldNames[MapFieldName],
             query1TableNames[MapTable]);

    snprintf(queryIndexRight,
             XcalarApiMaxQuerySize,
             "index --key JgString "
             "--dataset " XcalarApiDatasetPrefix "%s --dsttable %s --prefix p2",
             RandomJsonMemDataset,
             query1TableNames[IndexTableRight]);

    snprintf(queryJoin,
             XcalarApiMaxQuerySize,
             "join --leftTable %s "
             "--rightTable %s --joinTable %s --joinType innerJoin",
             query1TableNames[MapTable],
             query1TableNames[IndexTableRight],
             query1TableNames[JoinTable]);

    snprintf(query,
             XcalarApiMaxQuerySize,
             "%s; %s; %s; %s; %s",
             queryIndexLeft,
             queryFilter,
             queryMap,
             queryIndexRight,
             queryJoin);

    printf("test query = %s\n", query);

    char queryName[QrMaxNameLen] = "libApisSanityQueryTest";
    status = submitQuery(query, queryName, QrMaxNameLen, Runtime::NameSchedId0);
    assert(status == StatusOk);

    waitForQuery(queryName);

    workItem = xcalarApiMakeQueryStateWorkItem(queryName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    printf("Query State: %d\n",
           workItem->output->outputResult.queryStateOutput.queryState);
    assert(workItem->output->outputResult.queryStateOutput.queryStatus ==
           StatusOk.code());
    assert(workItem->output->outputResult.queryStateOutput.queryState ==
           qrFinished);
    assert(workItem->output->outputResult.queryStateOutput.numQueuedWorkItem ==
           0);
    assert(workItem->output->outputResult.queryStateOutput.numFailedWorkItem ==
           0);

    xcalarApiFreeWorkItem(workItem);

    workItem = xcalarApiMakeQueryDeleteWorkItem(queryName);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    queryNumEntries = count(NULL, query1TableNames[JoinTable]);
    printf("jointable entry = %llu \n", (unsigned long long) queryNumEntries);

    for (ii = 0; ii < Size; ++ii) {
        status = xcalarApiDeleteTable(query1TableNames[ii],
                                      destIp[0],
                                      destPort[0],
                                      userName,
                                      inputPid,
                                      sessionName);
        assert(status == StatusOk);
    }

    // Repeat the same query one by one

    XcalarApiNewTableOutput *indexOutput;
    XcalarApiNewTableOutput *filterOutput;
    XcalarApiNewTableOutput *mapOutput;
    XcalarApiNewTableOutput *joinOutput;

    char datasetFullName[DagTypes::MaxNameLen];

    // Index Left table
    snprintf(datasetFullName,
             DagTypes::MaxNameLen,
             XcalarApiDatasetPrefix "%s",
             RandomJsonMemDataset);

    const char *keyName = "JgString";
    workItem = xcalarApiMakeIndexWorkItem(datasetFullName,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          query2TableNames[IndexTableLeft],
                                          NULL,
                                          "p",
                                          false,
                                          false,
                                          sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    indexOutput = &workItem->output->outputResult.indexOutput;
    assert((uintptr_t) indexOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    // Filter operation
    workItem = xcalarApiMakeFilterWorkItem(filterStr,
                                           query2TableNames[IndexTableLeft],
                                           query2TableNames[FilterTable],
                                           sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output != NULL);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*filterOutput));

    filterOutput = &workItem->output->outputResult.filterOutput;
    assert(workItem->output->hdr.status == StatusOk.code());
    assert(strcmp(filterOutput->tableName, query2TableNames[FilterTable]) == 0);
    xcalarApiFreeWorkItem(workItem);

    // Map Operation
    workItem =
        xcalarApiMakeMapWorkItem(query2TableNames[FilterTable],
                                 query2TableNames[MapTable],
                                 false,
                                 1,
                                 (const char **) &evalString,
                                 (const char **) &queryFieldNames[MapFieldName],
                                 sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output != NULL);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*mapOutput));

    mapOutput = &workItem->output->outputResult.mapOutput;
    assert(workItem->output->hdr.status == StatusOk.code());
    assert(strcmp(mapOutput->tableName, query2TableNames[MapTable]) == 0);

    xcalarApiFreeWorkItem(workItem);

    // Index Right table
    workItem = xcalarApiMakeIndexWorkItem(datasetFullName,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          query2TableNames[IndexTableRight],
                                          NULL,
                                          datasetFullName,
                                          false,
                                          false,
                                          sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    indexOutput = &workItem->output->outputResult.indexOutput;
    assert((uintptr_t) indexOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*indexOutput));
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    XcalarApiRenameMap renameMap;
    snprintf(renameMap.oldName,
             sizeof(renameMap.oldName),
             XcalarApiDatasetPrefix "%s",
             RandomJsonMemDataset);
    snprintf(renameMap.newName, sizeof(renameMap.newName), "dsRight");
    renameMap.type = DfFatptr;

    // Join Operation
    workItem = xcalarApiMakeJoinWorkItem(query2TableNames[MapTable],
                                         query2TableNames[IndexTableRight],
                                         query2TableNames[JoinTable],
                                         InnerJoin,
                                         true,
                                         true,
                                         false,
                                         0,
                                         1,
                                         &renameMap,
                                         NULL,
                                         sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    joinOutput = &workItem->output->outputResult.joinOutput;
    assert(joinOutput != NULL);
    assert(workItem->output->hdr.status == StatusOk.code());

    xcalarApiFreeWorkItem(workItem);

    seqNumEntries = count(NULL, query2TableNames[JoinTable]);

    // Count entry and compare with the query operation result
    workItem = xcalarApiMakeGetTableMeta(query2TableNames[JoinTable],
                                         true,
                                         false,
                                         sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output != NULL);
    assert(workItem->outputSize > 0);

    getTableMetaOutput = &workItem->output->outputResult.getTableMetaOutput;
    assert(workItem->output->hdr.status == StatusOk.code());
    assert(getTableMetaOutput->numMetas == NumNodes);

    seqNumEntries = 0;
    for (ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        seqNumEntries += getTableMetaOutput->metas[ii].numRows;
    }

    printf("seq jointable entry = %llu \n", (unsigned long long) seqNumEntries);

    xcalarApiFreeWorkItem(workItem);
    assert(seqNumEntries == queryNumEntries);

    for (ii = 0; ii < Size; ++ii) {
        status = xcalarApiDeleteTable(query2TableNames[ii],
                                      destIp[0],
                                      destPort[0],
                                      userName,
                                      inputPid,
                                      sessionName);
        assert(status == StatusOk);
    }

    delete[] queryIndexLeft;
    delete[] queryFilter;
    delete[] queryMap;
    delete[] queryIndexRight;
    delete[] queryJoin;
    delete[] query;

    return status;
}

static int
ftwRmDir(const char *fpath,
         const struct stat *sb,
         int typeflag,
         struct FTW *ftwbuf)
{
    int ret;
    switch (typeflag) {
    case (FTW_F):
    case (FTW_D):
    case (FTW_DP):
        ret = remove(fpath);
        break;
    case (FTW_DNR):
        xSyslog(moduleName,
                XlogErr,
                "Cannot remove %s, directory cannot be read",
                fpath);
        ret = EACCES;
        break;
    default:
        ret = EIO;
        break;
    }
    return ret;
}

static Status
fileRmdir(const char *dirName, bool recursive)
{
    const int nopenfd = 20;  // max number of FDs ftw will consume at once

    int ret;
    if (recursive) {
        // Recursively traverse dir tree from the bottom up, deleting everything
        // ftw stands for File Tree Walk, FTW_DEPTH causes it to be bottom up
        ret = nftw(dirName, ftwRmDir, nopenfd, FTW_DEPTH);
    } else {
        ret = rmdir(dirName);
    }
    if (ret < 0) {
        return sysErrnoToStatus(errno);
    }

    return StatusOk;
}

static Status
getExt(char *src, char **extension)
{
    char sep = '/';
    char dot = '.';
    char *lastsep, *lastdot;

    assert(src != NULL);
    assert(extension != NULL);
    lastsep = strrchr(src, sep);
    lastdot = strrchr(src, dot);

    if (lastdot == NULL) {
        return StatusNoExtension;
    }

    if (lastsep != NULL && lastdot < lastsep) {
        // There is no file extension, do nothing
        return StatusNoExtension;
    }
    *extension = lastdot;
    assert(src[*extension - src] == dot);
    return StatusOk;
}

static Status
removeExt(char *src)
{
    Status status;
    char *extension;
    status = getExt(src, &extension);
    BailIfFailed(status);

    *extension = '\0';

    status = StatusOk;
CommonExit:
    return status;
}

static void
loadRandomJson(const char *datasetName)
{
    char path[QaMaxQaDirLen + 1];
    XcalarWorkItem *workItem;
    Status status;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    unsigned datasetIdx;
    DfLoadArgs *loadArgs = new DfLoadArgs();  // let throw
    size_t ret;

    ret = snprintf(path, sizeof(path), "%s/jsonRandom", qaGetQaDir());
    assert(ret <= sizeof(path));
    printf("Loading %s\n", path);

    memZero(loadArgs, sizeof(*loadArgs));
    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    strlcpy(loadArgs->sourceArgsList[0].targetName,
            "Default Shared Root",
            sizeof(loadArgs->sourceArgsList[0].targetName));
    strlcpy(loadArgs->sourceArgsList[0].path,
            path,
            sizeof(loadArgs->sourceArgsList[0].path));
    loadArgs->sourceArgsList[0].recursive = false;
    loadArgs->maxSize = 0;

    strlcpy(loadArgs->parseArgs.parserFnName,
            "default:parseJson",
            sizeof(loadArgs->parseArgs.parserFnName));
    strlcpy(loadArgs->parseArgs.parserArgJson,
            "{}",
            sizeof(loadArgs->parseArgs.parserArgJson));

    // Create the dataset meta using the same API input as specified for the
    // bulk load.
    workItem =
        xcalarApiMakeDatasetCreateWorkItem(datasetName, loadArgs, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    xcalarApiFreeWorkItem(workItem);

    // Now load the dataset using the meta data specified in the dataset create
    workItem =
        xcalarApiMakeBulkLoadWorkItem(datasetName, loadArgs, sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    printf("Status: %s\n", strGetFromStatus(status));
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    bulkLoadOutput = &workItem->output->outputResult.loadOutput;
    assert((uintptr_t) bulkLoadOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
    assert(workItem->output->hdr.status == StatusOk.code());

    datasetIdx = numDataset++;
    strlcpy(datasets[datasetIdx].name,
            bulkLoadOutput->dataset.name,
            sizeof(datasets[datasetIdx].name));
    strlcpy(datasets[datasetIdx].path, path, sizeof(datasets[datasetIdx].path));

    compareDatasetName(datasetName, bulkLoadOutput->dataset.name);
    xcalarApiFreeWorkItem(workItem);
    bulkLoadOutput = NULL;
    delete loadArgs;
}

static void
loadAndIndexYelpData()
{
    Status status;
    XcalarWorkItem *workItem;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    XcalarApiNewTableOutput *indexOutput;
    unsigned numTables = ArrayLen(yelpTables);
    unsigned ii, jj;
    char datasetName[DsDatasetNameLen + 1];
    unsigned tableIdx;
    char path[QaMaxQaDirLen + 1];

    for (ii = 0; ii < numTables; ii++) {
        DfLoadArgs *loadArgs = new DfLoadArgs();  // let throw
        memZero(loadArgs, sizeof(*loadArgs));

        size_t ret;
        ret = snprintf(path,
                       sizeof(path),
                       "%s/yelp/%s",
                       qaGetQaDir(),
                       yelpTables[ii].tableName);
        assert(ret <= sizeof(path));
        printf("Loading %s\n", path);

        loadArgs->sourceArgsListCount = 1;
        loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
        strlcpy(loadArgs->sourceArgsList[0].targetName,
                "Default Shared Root",
                sizeof(loadArgs->sourceArgsList[0].targetName));
        strlcpy(loadArgs->sourceArgsList[0].path,
                path,
                sizeof(loadArgs->sourceArgsList[0].path));
        loadArgs->sourceArgsList[0].recursive = false;
        loadArgs->maxSize = 0;

        strlcpy(loadArgs->parseArgs.parserFnName,
                "default:parseJson",
                sizeof(loadArgs->parseArgs.parserFnName));
        strlcpy(loadArgs->parseArgs.parserArgJson,
                "{}",
                sizeof(loadArgs->parseArgs.parserArgJson));

        snprintf(datasetName, sizeof(datasetName), "yelp data %i", ii);

        // Create the dataset meta using the same API input as specified for the
        // bulk load.
        workItem = xcalarApiMakeDatasetCreateWorkItem(datasetName,
                                                      loadArgs,
                                                      sessionName);
        assert(workItem != NULL);

        status = xcalarApiQueueWork(workItem,
                                    destIp[0],
                                    destPort[0],
                                    userName,
                                    inputPid);
        assert(status == StatusOk);
        xcalarApiFreeWorkItem(workItem);

        // Now load the dataset using the meta specified in the dataset create.
        workItem =
            xcalarApiMakeBulkLoadWorkItem(datasetName, loadArgs, sessionName);
        assert(workItem != NULL);
        status = xcalarApiQueueWork(workItem,
                                    destIp[0],
                                    destPort[0],
                                    userName,
                                    inputPid);
        assert(status == StatusOk);
        assert(workItem->output->hdr.status == StatusOk.code());
        bulkLoadOutput = &workItem->output->outputResult.loadOutput;
        assert((uintptr_t) bulkLoadOutput ==
               (uintptr_t) &workItem->output->outputResult);
        assert(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
        strlcpy(datasetName, bulkLoadOutput->dataset.name, sizeof(datasetName));
        xcalarApiFreeWorkItem(workItem);
        bulkLoadOutput = NULL;

        unsigned datasetIdx = numDataset++;
        strlcpy(datasets[datasetIdx].path,
                path,
                sizeof(datasets[datasetIdx].path));

        snprintf(datasets[datasetIdx].name,
                 sizeof(datasets[datasetIdx].name),
                 "%s",
                 datasetName);

        for (jj = 0; jj < MaxNumKeys; jj++) {
            if (yelpTables[ii].keyNames[jj] == NULL) {
                break;
            }
            tableIdx = numTablesIndexed++;
            makeTableName(dagNodeNameByTable[tableIdx],
                          MaxDagNodeNameLen,
                          "yelp",
                          yelpTables[ii].tableName,
                          yelpTables[ii].keyNames[jj],
                          NULL,
                          NULL);

            printf("Indexing dataset '%s' on key '%s' into '%s'\n",
                   datasetName,
                   yelpTables[ii].keyNames[jj],
                   dagNodeNameByTable[tableIdx]);
            workItem = xcalarApiMakeIndexWorkItem(datasetName,
                                                  NULL,
                                                  1,
                                                  &yelpTables[ii].keyNames[jj],
                                                  NULL,
                                                  NULL,
                                                  NULL,
                                                  dagNodeNameByTable[tableIdx],
                                                  NULL,
                                                  "p",
                                                  false,
                                                  false,
                                                  sessionName);
            assert(workItem != NULL);
            status = xcalarApiQueueWork(workItem,
                                        destIp[0],
                                        destPort[0],
                                        userName,
                                        inputPid);
            printf("Status: %s\n", strGetFromStatus(status));
            assert(status == StatusOk);
            indexOutput = &workItem->output->outputResult.indexOutput;
            assert(workItem->output->hdr.status == StatusOk.code());
            assert(strcmp(indexOutput->tableName,
                          dagNodeNameByTable[tableIdx]) == 0);

            printf("Indexed dataset %s into %s completed\n",
                   datasetName,
                   dagNodeNameByTable[tableIdx]);

            xcalarApiFreeWorkItem(workItem);
        }
        delete loadArgs;
    }

    yelpTablesLoaded = true;
}

static Status
checkListTableByType(char (*dagNodeName)[MaxDagNodeNameLen],
                     unsigned numNode,
                     SourceType srcType)
{
    XcalarWorkItem *workItem;
    XcalarApiListDagNodesOutput *listNodesOutput;
    unsigned ii, jj;
    Status status;

    // assert(yelpTablesLoaded);

    workItem = xcalarApiMakeListDagNodesWorkItem("*", srcType, sessionName);
    assert(workItem->input != NULL);
    assert(workItem->output == NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    assert(workItem->output != NULL);
    assert(workItem->outputSize > 0);

    assert(workItem->output->hdr.status == StatusOk.code());
    listNodesOutput = &workItem->output->outputResult.listNodesOutput;

    printf("Expecting to see: \n");
    for (ii = 0; ii < numNode; ii++) {
        printf("  %d. %s\n", ii + 1, dagNodeName[ii]);
    }

    printf("\nWe got: \n");
    for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
        printf("  %d. %s\n", ii + 1, listNodesOutput->nodeInfo[ii].name);
    }
    printf("\n");

    assert(listNodesOutput->numNodes == numNode);

    for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
        for (jj = 0; jj < numNode; jj++) {
            if (strcmp(listNodesOutput->nodeInfo[ii].name, dagNodeName[jj]) ==
                0) {
                break;
            }
        }
        assert(jj < numNode);
    }

    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    return StatusOk;
}

static Status
checkListTables()
{
    return checkListTableByType(dagNodeNameByTable, numTablesIndexed, SrcTable);
}

static Status
checkListConstants()
{
    return checkListTableByType(dagNodeNameByConstant,
                                numConstant,
                                SrcConstant);
}

static Status
checkListDataset()
{
    char datasetNamesList[numDataset][MaxDagNodeNameLen];
    unsigned ii;

    for (ii = 0; ii < numDataset; ii++) {
        strlcpy(datasetNamesList[ii],
                datasets[ii].name,
                sizeof(datasetNamesList[ii]));
    }

    return checkListTableByType(datasetNamesList, numDataset, SrcDataset);
}

static Status
aggregateTest()
{
    bool foundTable;
    unsigned srcTableIdx;
    unsigned dstTableIdx;
    double returnedAnswer;

    assert(yelpTablesLoaded);

    foundTable =
        getTableIdx("yelp", "user", "review_count", NULL, NULL, &srcTableIdx);
    assert(foundTable);

    // First let's get the count
    printf("Performing aggregate sum(fans) on %s\n",
           dagNodeNameByTable[srcTableIdx]);

    dstTableIdx = numConstant++;
    makeTableName(dagNodeNameByConstant[dstTableIdx],
                  MaxDagNodeNameLen,
                  "yelp",
                  "user",
                  RsDefaultConstantKeyName,
                  NULL,
                  NULL);

    returnedAnswer = doAggregate(dagNodeNameByTable[srcTableIdx],
                                 dagNodeNameByConstant[dstTableIdx],
                                 "sum(p::fans)");

    printf("returnedAnswer: %lf expected: %lf\n create aggregate table %s\n",
           returnedAnswer,
           QaYelpUserSumFans,
           dagNodeNameByConstant[dstTableIdx]);
    assert(returnedAnswer == QaYelpUserSumFans);

    return StatusOk;
}

static void
cleanupTables()
{
    unsigned ii;
    Status status;

    checkListDataset();
    checkListTables();
    checkListConstants();

    for (ii = 0; ii < numTablesIndexed; ii++) {
        fprintf(stderr, "Dropping table %s\n", dagNodeNameByTable[ii]);
        status = xcalarApiDeleteTable(dagNodeNameByTable[ii],
                                      destIp[0],
                                      destPort[0],
                                      userName,
                                      inputPid,
                                      sessionName);
        assert(status == StatusOk);
    }

    for (ii = 0; ii < numConstant; ii++) {
        fprintf(stderr, "Dropping constant %s\n", dagNodeNameByConstant[ii]);
        status = xcalarApiDeleteTable(dagNodeNameByConstant[ii],
                                      destIp[0],
                                      destPort[0],
                                      userName,
                                      inputPid,
                                      sessionName);
        assert(status == StatusOk);
    }
}

static void
cleanupDatasets()
{
    Status status;
    XcalarWorkItem *workItem, *workItem2;
    XcalarApiDeleteDagNodesOutput *destroyDatasetsOutput;

    workItem =
        xcalarApiMakeDeleteDagNodesWorkItem("*", SrcDataset, sessionName);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    destroyDatasetsOutput =
        &workItem->output->outputResult.deleteDagNodesOutput;

    for (uint64_t ii = 0; ii < destroyDatasetsOutput->numNodes; ++ii) {
        assert(destroyDatasetsOutput->statuses[ii].status == StatusOk.code());
    }

    xcalarApiFreeWorkItem(workItem);

    workItem = xcalarApiMakeDatasetUnloadWorkItem("*");
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    XcalarApiDatasetUnloadOutput *datasetUnloadOutput;
    datasetUnloadOutput = &workItem->output->outputResult.datasetUnloadOutput;
    for (uint64_t ii = 0; ii < datasetUnloadOutput->numDatasets; ii++) {
        printf("Unloading dataset %s\n",
               datasetUnloadOutput->statuses[ii].dataset.name);
        assert(datasetUnloadOutput->statuses[ii].status == StatusOk.code());
        // We shouldn't be seeing LRQ datasets here
        assert(strncmp(XcalarApiLrqPrefix,
                       datasetUnloadOutput->statuses[ii].dataset.name,
                       XcalarApiLrqPrefixLen) != 0);
        workItem2 = xcalarApiMakeDatasetDeleteWorkItem(
            datasetUnloadOutput->statuses[ii].dataset.name);
        assert(workItem2 != NULL);
        status = xcalarApiQueueWork(workItem2,
                                    destIp[0],
                                    destPort[0],
                                    userName,
                                    inputPid);
        assert(status == StatusOk);
        assert(workItem2->output->hdr.status == StatusOk.code());
        xcalarApiFreeWorkItem(workItem2);
        workItem2 = NULL;
    }
    xcalarApiFreeWorkItem(workItem);
}

static Status
edgeCasesTests()
{
    unsigned tableIdx1, tableIdx2;

    tableIdx1 = numTablesIndexed++;

    // Theoratically, if not given a name, this table would be loaded as
    // XcalarApiTempTablePrefix tableIdx and the next table would be
    // XcalarApiTempTablePrefix (tableIdx + 1).
    // We're going to try to mess around this naming by naming this table
    // XcalarApiTempTablePrefix (tableIdx + 1) and have the system name
    // the next table
    snprintf(dagNodeNameByTable[tableIdx1],
             sizeof(dagNodeNameByTable[tableIdx1]),
             "%s%u",
             XcalarApiTempTablePrefix,
             tableIdx1 + 1);
    loadRandomData(dagNodeNameByTable[tableIdx1], NULL, 0);
    printf("Creating table %s\n", dagNodeNameByTable[tableIdx1]);

    // Now have the system auto-generate name of this table
    tableIdx2 = numTablesIndexed++;
    loadRandomData(NULL,
                   dagNodeNameByTable[tableIdx2],
                   sizeof(dagNodeNameByTable[tableIdx2]));
    printf("System returned table name %s\n", dagNodeNameByTable[tableIdx2]);

    assert(strcmp(dagNodeNameByTable[tableIdx1],
                  dagNodeNameByTable[tableIdx2]) != 0);

    return StatusOk;
}

int
main(int argc, char *argv[])
{
    char fullCfgFilePath[255];
    Status status;
    const char *cwd = dirname(argv[0]);
    XcalarWorkItem *workItem;
    int ret;
    unsigned ii;

    uid_t uid;
    char *returnedPwd = NULL;
    size_t returnedPwdSize;
    struct passwd pwd;
    struct passwd *returnedPwdStruct;
    char *configFilePath = getenv("XCE_CONFIG");

    status = memTrackInit();
    assert(status == StatusOk);

    if (configFilePath == NULL) {
        const char *cfgFile = "test-config.cfg";
        snprintf(fullCfgFilePath,
                 sizeof(fullCfgFilePath),
                 "%s/%s",
                 cwd,
                 cfgFile);
    } else {
        strlcpy(fullCfgFilePath, configFilePath, sizeof(fullCfgFilePath));
    }

    status = Config::init();
    assert(status == StatusOk);

    Config *config = Config::get();

    status = config->loadConfigFiles(fullCfgFilePath);
    assert(status == StatusOk);
    for (ii = 0; ii < NumNodes; ++ii) {
        destIp[ii] = config->getIpAddr(ii);
        destPort[ii] = config->getApiPort(ii);
    }

    status = StatsLib::createSingleton();
    assert(status == StatusOk);

    status = LogLib::createSingleton();
    assert(status == StatusOk);

    inputPid = getpid();
    ret = getlogin_r(userName, sizeof(userName));
    if (ret != 0) {
        uid = getuid();
        returnedPwdSize = sysconf(_SC_GETPW_R_SIZE_MAX);
        returnedPwd = (char *) memAllocExt(returnedPwdSize, moduleName);
        ret = getpwuid_r(uid,
                         &pwd,
                         returnedPwd,
                         returnedPwdSize,
                         &returnedPwdStruct);
        assert(ret == 0);
        strlcpy(userName, pwd.pw_name, sizeof(userName) - 1);
        memFree(returnedPwd);
    }
    listAndDeleteSession();

    // Create a session for this set of tests
    status = newSession(inputPid, userName, sessionName, &sessionId);
    assert(status == StatusOk);

    // The new session is born inactive. There's no concept of a "first
    // session to be created must be active". So make this session active.

    workItem = xcalarApiMakeSessionActivateWorkItem(sessionName);
    assert(workItem);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    // Load yelp data
    loadAndIndexYelpData();

    // Load gdelt data
    loadAndIndexGdeltData();

    // Load randomJson data
    loadRandomJson(RandomJsonMemDataset);
    // And now we run the actual test cases
    int numTestCasesFailed;
    numTestCasesFailed = qaRunTestSuite(testCases,
                                        ArrayLen(testCases),
                                        TestCaseOptDisableIsPass);

    cleanupTables();
    cleanupDatasets();

    // Deactivate and delete our session
    deleteSession();

    LogLib::deleteSingleton();
    StatsLib::deleteSingleton();

    config->unloadConfigFiles();
    if (Config::get()) {
        Config::get()->destroy();
    }

    memTrackDestroy(true);

    xcalarExit(numTestCasesFailed);
}

Status
dropDatasetTest()
{
    char path[QaMaxQaDirLen + 1];
    XcalarWorkItem *workItem;
    Status status;
    XcalarApiDatasetGetMetaOutput *datasetGetMetaOutput;
    XcalarApiBulkLoadOutput *bulkLoadOutput;
    const char *datasetNameIn = "libapisSanity-dropDatasetTest-ds1";
    const char *tableName = "libapisSanity-dropDatasetTest-table1";
    char datasetName[DsDatasetNameLen + 1];

    const char *newSessionName = "newSession";
    uint64_t newSessionId;
    unsigned ii;

    // Create / Load a dataset
    size_t ret;
    ret = snprintf(path, sizeof(path), "%s/indexJoin/schedule", qaGetQaDir());
    assert(ret <= sizeof(path));
    printf("Loading %s\n", path);
    DfLoadArgs *loadArgs = new DfLoadArgs();  // let throw
    memZero(loadArgs, sizeof(*loadArgs));

    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    strlcpy(loadArgs->sourceArgsList[0].targetName,
            "Default Shared Root",
            sizeof(loadArgs->sourceArgsList[0].targetName));
    strlcpy(loadArgs->sourceArgsList[0].path,
            path,
            sizeof(loadArgs->sourceArgsList[0].path));
    loadArgs->sourceArgsList[0].recursive = false;
    loadArgs->maxSize = 0;

    strlcpy(loadArgs->parseArgs.parserFnName,
            "default:parseJson",
            sizeof(loadArgs->parseArgs.parserFnName));
    strlcpy(loadArgs->parseArgs.parserArgJson,
            "{}",
            sizeof(loadArgs->parseArgs.parserArgJson));

    // Create the dataset meta using the same API input as used for the bulk
    // load.
    workItem = xcalarApiMakeDatasetCreateWorkItem(datasetNameIn,
                                                  loadArgs,
                                                  sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    xcalarApiFreeWorkItem(workItem);

    // Get the dataset meta as a check that the create worked.
    workItem = xcalarApiMakeDatasetGetMetaWorkItem(datasetNameIn);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    datasetGetMetaOutput = &workItem->output->outputResult.datasetGetMetaOutput;
    assert((uintptr_t) datasetGetMetaOutput ==
           (uintptr_t) &workItem->output->outputResult);
    // output is variable sized
    assert(workItem->outputSize > XcalarApiSizeOfOutput(*datasetGetMetaOutput));
    printf("Dataset meta for '%s' is\n '%s'\n",
           datasetNameIn,
           datasetGetMetaOutput->datasetMeta);
    xcalarApiFreeWorkItem(workItem);
    datasetGetMetaOutput = NULL;

    // Do the actual dataset load
    workItem =
        xcalarApiMakeBulkLoadWorkItem(datasetNameIn, loadArgs, sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    bulkLoadOutput = &workItem->output->outputResult.loadOutput;
    assert((uintptr_t) bulkLoadOutput ==
           (uintptr_t) &workItem->output->outputResult);
    assert(workItem->outputSize == XcalarApiSizeOfOutput(*bulkLoadOutput));
    strlcpy(datasetName, bulkLoadOutput->dataset.name, sizeof(datasetName));
    xcalarApiFreeWorkItem(workItem);
    bulkLoadOutput = NULL;

    // Verify that we have the load node
    workItem =
        xcalarApiMakeListDagNodesWorkItem(datasetName, SrcDataset, sessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    XcalarApiListDagNodesOutput *listNodesOutput;
    listNodesOutput = &workItem->output->outputResult.listNodesOutput;
    printf("numNodes: %lu\n", listNodesOutput->numNodes);
    for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
        printf("%s - %s\n",
               listNodesOutput->nodeInfo[ii].name,
               strGetFromDgDagState(listNodesOutput->nodeInfo[ii].state));
    }
    assert(listNodesOutput->numNodes == 1);
    assert(strcmp(listNodesOutput->nodeInfo[0].name, datasetName) == 0);
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    listNodesOutput = NULL;

    // Create a new session
    status =
        newSession(inputPid, userName, (char *) newSessionName, &newSessionId);
    assert(status == StatusOk);

    // Keep the current session active while activating the newSession. That's
    // OK now since multiple sessions can be active concurrently!

    workItem = xcalarApiMakeSessionActivateWorkItem(newSessionName);
    assert(workItem);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    bool foundDataset = false;
    XcalarApiListDatasetsOutput *listDatasetsOutput = NULL;
    // We need to be able to see this dataset from the new session because
    // datasets are shared across sessions
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasets, NULL, 0);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    printf("workItem->output->hdr.status = %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());
    listDatasetsOutput = &workItem->output->outputResult.listDatasetsOutput;
    printf("Num datasets: %u\n", listDatasetsOutput->numDatasets);
    for (ii = 0; ii < listDatasetsOutput->numDatasets; ii++) {
        printf("Dataset %u:\n", ii);
        printf("  Name: %s\n", listDatasetsOutput->datasets[ii].name);
        if (strcmp(listDatasetsOutput->datasets[ii].name, datasetName) == 0) {
            foundDataset = true;
        }
    }
    xcalarApiFreeWorkItem(workItem);
    listDatasetsOutput = NULL;

    assert(foundDataset);

    // Try to drop the load node from this session. This operation should fail
    // because this node doesn't exist
    workItem = xcalarApiMakeDeleteDagNodesWorkItem(datasetName,
                                                   SrcDataset,
                                                   newSessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusDagNodeNotFound.code());

    XcalarApiDeleteDagNodesOutput *deleteDagNodesOutput;
    deleteDagNodesOutput = &workItem->output->outputResult.deleteDagNodesOutput;
    printf("Num nodes deleted: %lu\n", deleteDagNodesOutput->numNodes);
    for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
        printf("%s - %s\n",
               deleteDagNodesOutput->statuses[ii].nodeInfo.name,
               strGetFromStatusCode(deleteDagNodesOutput->statuses[ii].status));
    }
    assert(deleteDagNodesOutput->numNodes == 1);
    assert(strcmp(deleteDagNodesOutput->statuses[0].nodeInfo.name,
                  datasetName) == 0);
    assert(deleteDagNodesOutput->statuses[0].status ==
           StatusDagNodeNotFound.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    deleteDagNodesOutput = NULL;

    // Try to load a dataset with the same name. It will be successful
    // as we consider dataset names with the same name as being the
    // same dataset (as long as they are  prefixed with .XcalarDS.).
    printf("Trying to load %s with same name\n", path);
    memZero(loadArgs, sizeof(*loadArgs));

    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    strlcpy(loadArgs->sourceArgsList[0].targetName,
            "Default Shared Root",
            sizeof(loadArgs->sourceArgsList[0].targetName));
    strlcpy(loadArgs->sourceArgsList[0].path,
            path,
            sizeof(loadArgs->sourceArgsList[0].path));
    loadArgs->sourceArgsList[0].recursive = false;
    loadArgs->maxSize = 0;

    strlcpy(loadArgs->parseArgs.parserFnName,
            "default:parseJson",
            sizeof(loadArgs->parseArgs.parserFnName));
    strlcpy(loadArgs->parseArgs.parserArgJson,
            "{}",
            sizeof(loadArgs->parseArgs.parserArgJson));
    workItem =
        xcalarApiMakeBulkLoadWorkItem(datasetNameIn, loadArgs, newSessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    printf("status: %s\n", strGetFromStatus(status));
    assert(status == StatusOk);
    printf("workItem->output->hdr.status: %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    // Now let's try to index on the dataset that's not owned by our session.
    // This should automatically create a load node in our session to "induct"
    // it
    const char *keyName = "class_id";
    workItem = xcalarApiMakeIndexWorkItem(datasetName,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          tableName,
                                          NULL,
                                          "p",
                                          false,
                                          false,
                                          newSessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // Try to drop the dataset. Should succeed.
    // With datatflow 2.0 this is unloading the datasets.  The dataset meta
    // still remains.
    workItem = xcalarApiMakeDatasetUnloadWorkItem(datasetName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    XcalarApiDatasetUnloadOutput *datasetUnloadOutput;
    datasetUnloadOutput = &workItem->output->outputResult.datasetUnloadOutput;
    printf("Num datasets unloaded: %lu\n", datasetUnloadOutput->numDatasets);
    for (ii = 0; ii < datasetUnloadOutput->numDatasets; ii++) {
        printf("Dataset %u:\n", ii);
        printf("  Name: %s\n", datasetUnloadOutput->statuses[ii].dataset.name);
        printf("  Status: %s\n\n",
               strGetFromStatusCode(datasetUnloadOutput->statuses[ii].status));
    }
    assert(datasetUnloadOutput->numDatasets == 1);
    assert(strcmp(datasetUnloadOutput->statuses[0].dataset.name, datasetName) ==
           0);
    // 1 for each of the session's load node
    assert(datasetUnloadOutput->statuses[0].status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);
    datasetUnloadOutput = NULL;

    // Verify that the dataset no longer exists as it was unloaded above.
    foundDataset = false;
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasets, NULL, 0);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    printf("workItem->output->hdr.status = %s\n",
           strGetFromStatusCode(workItem->output->hdr.status));
    assert(workItem->output->hdr.status == StatusOk.code());
    listDatasetsOutput = &workItem->output->outputResult.listDatasetsOutput;
    printf("Num datasets: %u\n", listDatasetsOutput->numDatasets);
    for (ii = 0; ii < listDatasetsOutput->numDatasets; ii++) {
        printf("Dataset %u:\n", ii);
        printf("  Name: %s\n", listDatasetsOutput->datasets[ii].name);
        if (strcmp(listDatasetsOutput->datasets[ii].name, datasetName) == 0 &&
            listDatasetsOutput->datasets[ii].loadIsComplete &&
            listDatasetsOutput->datasets[ii].isListable) {
            foundDataset = true;
        }
    }
    xcalarApiFreeWorkItem(workItem);
    listDatasetsOutput = NULL;

    assert(!foundDataset);

    // Let's try to drop the load node again. It should fail because there's
    // still the outstanding index table
    printf("Deleting load nodes \"%s\"\n", datasetName);
    workItem = xcalarApiMakeDeleteDagNodesWorkItem(datasetName,
                                                   SrcDataset,
                                                   newSessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusDsDatasetInUse.code());

    deleteDagNodesOutput = &workItem->output->outputResult.deleteDagNodesOutput;
    printf("Num nodes deleted: %lu\n", deleteDagNodesOutput->numNodes);
    for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
        printf("%s - %s\n",
               deleteDagNodesOutput->statuses[ii].nodeInfo.name,
               strGetFromStatusCode(deleteDagNodesOutput->statuses[ii].status));
    }
    assert(deleteDagNodesOutput->numNodes == 1);
    assert(strcmp(deleteDagNodesOutput->statuses[0].nodeInfo.name,
                  datasetName) == 0);
    assert(deleteDagNodesOutput->statuses[0].status ==
           StatusDsDatasetInUse.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // Now let's drop the index table.
    printf("Deleting tables \"%s\"\n", tableName);
    workItem = xcalarApiMakeDeleteDagNodesWorkItem(tableName,
                                                   SrcTable,
                                                   newSessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    deleteDagNodesOutput = &workItem->output->outputResult.deleteDagNodesOutput;
    printf("Num nodes deleted: %lu\n", deleteDagNodesOutput->numNodes);
    for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
        printf("%s - %s\n",
               deleteDagNodesOutput->statuses[ii].nodeInfo.name,
               strGetFromStatusCode(deleteDagNodesOutput->statuses[ii].status));
    }
    assert(deleteDagNodesOutput->numNodes == 1);
    assert(strcmp(deleteDagNodesOutput->statuses[0].nodeInfo.name, tableName) ==
           0);
    assert(deleteDagNodesOutput->statuses[0].status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // And finally, let's drop the load node again. Should succeed this time
    printf("Deleting load nodes \"%s\"\n", datasetName);
    workItem = xcalarApiMakeDeleteDagNodesWorkItem(datasetName,
                                                   SrcDataset,
                                                   newSessionName);
    assert(workItem != NULL);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());

    deleteDagNodesOutput = &workItem->output->outputResult.deleteDagNodesOutput;
    printf("Num nodes deleted: %lu\n", deleteDagNodesOutput->numNodes);
    for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
        printf("%s - %s\n",
               deleteDagNodesOutput->statuses[ii].nodeInfo.name,
               strGetFromStatusCode(deleteDagNodesOutput->statuses[ii].status));
    }
    assert(deleteDagNodesOutput->numNodes == 1);
    assert(strcmp(deleteDagNodesOutput->statuses[0].nodeInfo.name,
                  datasetName) == 0);
    assert(deleteDagNodesOutput->statuses[0].status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    // Switch back to the original session
    workItem = xcalarApiMakeSessionInactWorkItem(newSessionName);
    assert(workItem);
    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    // Delete the newly created session
    workItem = xcalarApiMakeSessionDeleteWorkItem(newSessionName);
    assert(workItem);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    assert(workItem->output->hdr.status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);

    // XXX: Just a note that some of this has to be changed for the new datasets
    //
    // Verify that we don't forget about our load node even though it refers
    // to a dataset that has been marked for removal.

    workItem = xcalarApiMakeDeleteDagNodesWorkItem(datasetName,
                                                   SrcDataset,
                                                   sessionName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);

    deleteDagNodesOutput = &workItem->output->outputResult.deleteDagNodesOutput;
    printf("Num nodes deleted: %lu\n", deleteDagNodesOutput->numNodes);
    for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
        printf("%s - %s\n",
               deleteDagNodesOutput->statuses[ii].nodeInfo.name,
               strGetFromStatusCode(deleteDagNodesOutput->statuses[ii].status));
    }
    assert(deleteDagNodesOutput->numNodes == 1);
    assert(deleteDagNodesOutput->statuses[0].status == StatusOk.code());
    xcalarApiFreeWorkItem(workItem);
    deleteDagNodesOutput = NULL;

    // Now delete the dataset meta from the cluster
    workItem = xcalarApiMakeDatasetDeleteWorkItem(datasetName);
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                destIp[0],
                                destPort[0],
                                userName,
                                inputPid);
    assert(status == StatusOk);
    xcalarApiFreeWorkItem(workItem);

    delete loadArgs;
    return status;
}
