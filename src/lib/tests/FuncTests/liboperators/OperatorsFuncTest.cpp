// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <jansson.h>
#include <new>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <math.h>
#include <stdlib.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "dag/DagTypes.h"
#include "dag/DagLib.h"
#include "usr/Users.h"
#include "SourceTypeEnum.h"
#include "libapis/LibApisSend.h"
#include "libapis/LibApisRecv.h"
#include "dataset/Dataset.h"
#include "table/ResultSet.h"
#include "runtime/Runtime.h"
#include "runtime/Semaphore.h"
#include "operators/Operators.h"
#include "runtime/Spinlock.h"
#include "util/StringHashTable.h"
#include "LibOperatorsFuncTestConfig.h"
#include "util/Random.h"
#include "util/System.h"
#include "dag/DagTypes.h"
#include "queryparser/QueryParser.h"
#include "querymanager/QueryManager.h"
#include "parent/Parent.h"
#include "xdb/HashTree.h"
#include "service/PublishedTableService.h"
#include "xcalar/compute/localtypes/PublishedTable.pb.h"
#include "test/QA.h"  // Must be last

// ---- What this test is doing ----
// We simulate a bunch of users playing with Xcalar.
// Number of users controlled via "NumUsers"
// Among these bunch of users, some of them are responsible
// for loading in the datasets. We control this via "NumLoaders"
// Each user gets their own session and performs a set of operations.
// These set of operations are repeated over and over again.
// The number of times these set of operations are repeated is controlled
// via "NumCycles".
// At the start of each cycle, each user will pick a random table
// from his/her session to perform the set of operations on.
// The set of operations include: map, filter, groupBy, aggregate, project
// and getRowNum. The set of operations is performed in workerMain().
// The set of operations will create tables which gets added to the user's
// sesssion, from which they can be used as the starting point of the next
// set of operations.
// Every so often, tunable via "GarbageCollectionCycle", we will
// come in and drop all empty tables in the user's session.
// To allow each user to perform each operation in the set of operations
// in parallel, each user is allowed to recruit some workers to help.
// Number of workers each user is allowed to recruit is controlled via
// "NumWorkersPerUser"
// All randomness from this tests stems from 1 seed. This seed is printed
// at the start of the test. To reproduce a test, change "RandomSeed"
// from "DefaultSeed" to the desired seed. Please note that this is
// best-effort. Many "true" randomness like thread-interleaving is beyond
// our control

static const char *moduleName = "libFuncTest::libOperators";
static const char *RenamePrefix = "RN";

static XcalarApiUserId testUserId = {
    "OperatorsFuncTest",
    0xdeadbeef,
};

static uint64_t UseDefaultSeed = 0;
static uint64_t RandomSeed = UseDefaultSeed;

// Number of users to simulate
static uint32_t NumUsers = 16;
static Atomic64 usersReadyCount;

// Number of workers per user to simulate
static uint32_t NumWorkersPerUser = 2;

// Number of users who will actually load in a dataset
static uint32_t NumLoaders = 4;

// Default dataset
static char DatasetPath[QaMaxQaDirLen + 1] = "";
static DfFormatType DatasetType = DfFormatCsv;
static uint32_t DatasetSize = 100 * MB;

static char YelpDatasetPath[QaMaxQaDirLen + 1] = "";
static DfFormatType YelpDatasetType = DfFormatJson;
static uint32_t YelpDatasetSize = 100 * MB;

static char MemoryDatasetPath[XcalarApiMaxUrlLen + 1] = "memory://1000";
static DfFormatType MemoryDatasetType = DfFormatJson;
static uint32_t MemoryDatasetSize = 0;

static const char *MemoryPrefix = "memory://";
static unsigned MemoryPrefixLen = 9;

// Number of iterations before we drop all empty tables
static uint32_t GarbageCollectionCycle = 3;

// Number of cycles each user should work
static uint32_t NumCycles = 3;
static uint32_t NumCyclesPubTable = 3;

// When a worker is told to drop tables, probability of dropping
// any given table
static uint32_t PctChanceOfTableDrop = 50;

// When a worker is told to drop datasets, probability of dropping
// any given dataset
static uint32_t PctChanceOfDatasetDrop = 50;

// When a worker is told to drop publishTables, probability of dropping
// any given publish table
static uint32_t PctChanceOfPubTableDrop = 50;

// When a worker is told to delete publishTables, probability of deleting
// any given publish table
static uint32_t PctChanceOfPubTableDelete = 50;

// When a worker is told to drop variables, probability of dropp
// any given variable
static uint32_t PctChanceOfVariableDrop = 50;

// When a worker is told to project a table, probability of a column
// being dropped
static uint32_t PctChanceOfColDrop = 50;

// When a worker is told to export a table, probability of a column
// being exported
static uint32_t PctChanceOfColExport = 80;

// During a sort, percent chance of an ascending sort (instead of
// descending)
static uint32_t PctChanceOfAscendingSort = 50;

// When told to cancel operations, percent chance that an ongoing operation
// is cancelled
static uint32_t PctChanceOfCancel = 20;

// maximum amount of evaluations done in one map
static uint32_t MaxMapEvals = 5;

// keep retinas
static bool KeepRetinas = false;

// maximum amount of times we will try to get a more recent random table
static uint32_t MaxRandomTableLoops = 3;

static const char *UserIdPrefix = "operatorsUser-";

static uint32_t OperatorsFNFPercent = 10;
static size_t OpDefaultColsLoadMemWithSchema = 8;
static size_t OpMaxColsLoadMemWithSchema = OpDefaultColsLoadMemWithSchema;

// maximum number of random rows that we will check for valid columns in a table
static uint32_t MaxRowsToCheckForCols = 1000;

struct LoadMemUdfs {
    enum LoadMemType {
        LoadMemSimple = 0,
        LoadMemDefault,
        LoadMemWithSchema,
        LoadMemMax,
        LoadMemInvalid,
    };

    static constexpr const size_t LoadMemSrcSize = 2048;
    LoadMemType loadMemType;
    char loadMemUdfName[128];
    char loadMemUdfFnName[256];
    char loadMemSrc[LoadMemSrcSize];
    static bool isValidLoadMemUdfType(LoadMemType loadMemType);
    static void initLoadMemUdfs();
};

static LoadMemUdfs loadMemUdfs[LoadMemUdfs::LoadMemMax];

bool
LoadMemUdfs::isValidLoadMemUdfType(LoadMemUdfs::LoadMemType loadMemType)
{
    if (loadMemType >= LoadMemUdfs::LoadMemSimple &&
        loadMemType <= LoadMemUdfs::LoadMemWithSchema) {
        return true;
    } else {
        return false;
    }
}

void
LoadMemUdfs::initLoadMemUdfs()
{
    for (unsigned ii = LoadMemUdfs::LoadMemSimple; ii < LoadMemUdfs::LoadMemMax;
         ii++) {
        LoadMemUdfs *lm = &loadMemUdfs[ii];
        lm->loadMemType = (LoadMemUdfs::LoadMemType) ii;
        switch (ii) {
        case LoadMemUdfs::LoadMemSimple: {
            snprintf(lm->loadMemUdfName,
                     sizeof(lm->loadMemUdfName),
                     "loadMemSimple");
            snprintf(lm->loadMemUdfFnName,
                     sizeof(lm->loadMemUdfFnName),
                     "loadMemSimple:loadMemory");

            snprintf(lm->loadMemSrc,
                     sizeof(lm->loadMemSrc),
                     "import json\n"
                     "def loadMemory(fullPath, inStream):\n"
                     "    inObj = json.loads(inStream.read())\n"
                     "    startRow = inObj[\"startRow\"]\n"
                     "    numLocalRows = inObj[\"numRows\"]\n"
                     "    for ii in range(startRow, startRow + numLocalRows):\n"
                     "        rowDict = {\"int\": ii,\n"
                     "                   \"string\": str(ii),\n"
                     "                   \"float\": float(ii)\n"
                     "                  }\n"
                     "        yield rowDict\n");

            break;
        }

        case LoadMemUdfs::LoadMemDefault: {
            snprintf(lm->loadMemUdfName,
                     sizeof(lm->loadMemUdfName),
                     "loadMemDefault");
            snprintf(lm->loadMemUdfFnName,
                     sizeof(lm->loadMemUdfFnName),
                     "loadMemDefault:loadMemory");

            snprintf(lm->loadMemSrc,
                     sizeof(lm->loadMemSrc),
                     "import json\n"
                     "def loadMemory(fullPath, inStream):\n"
                     "    inObj = json.loads(inStream.read())\n"
                     "    startRow = inObj[\"startRow\"]\n"
                     "    numLocalRows = inObj[\"numRows\"]\n"
                     "    for ii in range(startRow, startRow + numLocalRows):\n"
                     "        rowDict = {\"rownum\": {\n"
                     "                   \"intcol\": ii,\n"
                     "                   \"stringcol\": str(ii),\n"
                     "                   \"floatcol\": float(ii)\n"
                     "               },\n"
                     "               \"cols\": {\n"
                     "                   \"array\": [0, 1, 2],\n"
                     "                   \"object\": {\"val0\": 0, \"val1\": "
                     "1, \"val2\": 2}\n"
                     "               },\n"
                     "               \"a\": {\"b\": ii},\n"
                     "               \"ab\": ii-1\n"
                     "              }\n"
                     "        if not (ii %% %u):\n"
                     "            rowDict['flaky'] = 1\n"
                     "        yield rowDict\n",
                     OperatorsFNFPercent);
            break;
        }

        case LoadMemUdfs::LoadMemWithSchema: {
            snprintf(lm->loadMemUdfName,
                     sizeof(lm->loadMemUdfName),
                     "loadMemRandParam");
            snprintf(lm->loadMemUdfFnName,
                     sizeof(lm->loadMemUdfFnName),
                     "loadMemRandParam:loadMemory");

            // Let's make the load streaming UDF do,
            // * Pick a schema randomly to exercise,
            // ** Fixed and variable packing.
            // ** Missing fields.
            // ** Mixed types.
            // ** Lots of columns.
            // * Pass in a param and use that to create a column called
            // "ParamVal".
            // * Faithfully add row number for each record.
            snprintf(lm->loadMemSrc,
                     sizeof(lm->loadMemSrc),
                     "import json\n"
                     "import ast\n"
                     "from datetime import datetime\n"
                     "def loadMemory(fullPath, inStream, schema):\n"
                     "    inObj = json.loads(inStream.read())\n"
                     "    startRow = inObj[\"startRow\"]\n"
                     "    numLocalRows = inObj[\"numRows\"]\n"
                     "    schemaDecoded = ast.literal_eval(schema)\n"
                     "    cols = schemaDecoded['Cols']\n"
                     "    paramVal = schemaDecoded['ParamVal']\n"
                     "    for ii in range(startRow, startRow + numLocalRows):\n"
                     "        rowDict = {}\n"
                     "        rowDict['RowNum'] = ii\n"
                     "        rowDict['ParamVal'] = paramVal\n"
                     "        for k, v in cols.items():\n"
                     "           if v == \"int\":\n"
                     "               rowDict[k] = int(ii)\n"
                     "           elif v == \"float\":\n"
                     "               rowDict[k] = float(ii)\n"
                     "           elif v == \"string\":\n"
                     "               rowDict[k] = str(ii)\n"
                     "           elif v == \"datetime\":\n"
                     "               rowDict[k] = "
                     "datetime.fromtimestamp(int(ii))\n"
                     "        yield rowDict\n");
            break;
        }
        default:
            assert(0);
            break;
        }
    }
}

static constexpr const char *PubTablePrefix = "PubTable--";

enum { AggregateNumSlots = 31 };

enum {
    MapAdd,
    MapAddNumeric,
    MapConcat,
    MapUdf,
    MapExplode,
    MapTimestamp,
    MapNumeric,
    MapMaxRandomArgs,
};

enum {
    FilterGt,
    FilterLt,
    FilterUdf,
};

enum {
    IndexDataset,
    IndexTable,
    IndexSort,
    IndexMaxRandomArgs,
};

enum {
    LoadDefault,
    LoadJson,
    LoadMemory,
    LoadMaxRandomArgs,
};

static pthread_t childrenKillerTid;
static sem_t childrenKillerSem;
static bool childrenKillerActive = false;
static uint64_t ChildrenKillerTimeout = 60 * 60 * USecsPerSec;

struct AggregateResult {
    StringHashTableHook hook;
    const char *getAggName() const { return output->tableName; }
    XcalarApiAggregateOutput *output = NULL;
};

static inline bool
isFatalRetinaExecuteStatus(Status status)
{
    return status == StatusImmediateNameCollision ||
           status == StatusFatptrPrefixCollision;
}

typedef StringHashTable<AggregateResult,
                        &AggregateResult::hook,
                        &AggregateResult::getAggName,
                        AggregateNumSlots,
                        hashStringFast>
    AggregateHashTable;

AggregateHashTable aggregateHashTable;

// XXX TODO Construction needs to be init ordered
Mutex aggregateHashTableLock;

static Status getRow(Dag *sessionGraph,
                     const char *tableName,
                     json_t **jsonRecordOut,
                     uint64_t rowNum,
                     XcalarApiGetTableMetaOutput *metaOut);
static Status getRandomColumn(Dag *sessionGraph,
                              const char *tableName,
                              char **colNameOut,
                              RandWeakHandle *randHandle);
static Status workerRetina(XcalarApiUserId *userId,
                           Dag *sessionGraph,
                           const char *tableName,
                           RandWeakHandle *randHandle);

static Status workerQuery(XcalarApiUserId *userId,
                          Dag *sessionGraph,
                          const char *tableName,
                          RandWeakHandle *randHandle);

static Status workerRename(XcalarApiUserId *userId,
                           Dag *sessionGraph,
                           const char *tableName);
static Status getRandomColumns(
    Dag *sessionGraph,
    const char *tableName,
    RandWeakHandle *randHandle,
    char (**columnsOut)[XcalarApiMaxFieldNameLen + 1],
    DfFieldType **columnTypesOut,
    int *numColsOut,
    uint32_t pctChanceOfPickingCol);
static void exportRetina(XcalarApiUserId *userId,
                         Dag *sessionGraph,
                         const char *retinaName);
static Status createNewPubTableFromDs(XcalarApiUserId *userId,
                                      Dag *sessionGraph,
                                      RandWeakHandle *randHandle,
                                      const char *publishedTableName,
                                      const char *datasetName);
static Status createNewPubTableFromTable(XcalarApiUserId *userId,
                                         Dag *sessionGraph,
                                         RandWeakHandle *randHandle,
                                         const char *publishedTableName,
                                         char *tableName);
static Status createUpdate(XcalarApiUserId *userId,
                           Dag *sessionGraph,
                           const char *publishedTableName,
                           unsigned cycle,
                           char updateTable[XcalarApiMaxTableNameLen + 1]);
static void workerUnpublishAll(XcalarApiUserId *userId,
                               Dag *sessionGraph,
                               RandWeakHandle *randHandle);

static Xid
getXidFromName(const char *tableName)
{
    const char *ptr = NULL;
    const char *prevPtr = NULL;

    ptr = tableName;
    while (ptr != NULL) {
        prevPtr = ptr;
        ptr = strchr(ptr + 1, '-');
    }

    return strtoul(prevPtr + 1, NULL, 10);
}

static Status
activateSession(XcalarApiUserId *userId)
{
    XcalarApiSessionActivateInput *sessionActivateInput = NULL;
    XcalarApiSessionGenericOutput *sessionActivateOutput = NULL;
    Status status = StatusUnknown;
    UserMgr *userMgr = UserMgr::get();

    sessionActivateInput = (XcalarApiSessionActivateInput *) memAlloc(
        sizeof(*sessionActivateInput));
    if (sessionActivateInput == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Insufficient memory to allocate "
                "sessionActivateInput",
                userId->userIdName);
        status = StatusNoMem;
        goto CommonExit;
    }

    strlcpy(sessionActivateInput->sessionName,
            userId->userIdName,
            sizeof(sessionActivateInput->sessionName));
    sessionActivateInput->sessionNameLength =
        strlen(sessionActivateInput->sessionName);
    sessionActivateInput->sessionId = 0;

    sessionActivateOutput = (XcalarApiSessionGenericOutput *) memAlloc(
        sizeof(*sessionActivateOutput));
    if (sessionActivateOutput == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Insufficient memory to allocate "
                "sessionActivateOutput",
                userId->userIdName);
        status = StatusNoMem;
        goto CommonExit;
    }

    status =
        userMgr->activate(userId, sessionActivateInput, sessionActivateOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Could not activate session. Status: %s (%s)",
                userId->userIdName,
                strGetFromStatus(status),
                sessionActivateOutput->errorMessage);
        goto CommonExit;
    }

CommonExit:
    if (sessionActivateOutput != NULL) {
        memFree(sessionActivateOutput);
        sessionActivateOutput = NULL;
    }
    if (sessionActivateInput != NULL) {
        memFree(sessionActivateInput);
        sessionActivateInput = NULL;
    }

    return status;
}

static Status
createSession(XcalarApiUserId *userId)
{
    XcalarApiSessionNewInput *sessionNewInput = NULL;
    XcalarApiSessionNewOutput *sessionNewOutput = NULL;
    Status status = StatusUnknown;
    UserMgr *userMgr = UserMgr::get();

    sessionNewInput =
        (XcalarApiSessionNewInput *) memAlloc(sizeof(*sessionNewInput));
    if (sessionNewInput == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Insufficient memory to allocate "
                "sessionNewInput",
                userId->userIdName);
        status = StatusNoMem;
        goto CommonExit;
    }

    sessionNewOutput =
        (XcalarApiSessionNewOutput *) memAlloc(sizeof(*sessionNewOutput));
    if (sessionNewOutput == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Insufficient memory to allocate "
                "sessionNewOutput",
                userId->userIdName);
        status = StatusNoMem;
        goto CommonExit;
    }

    strlcpy(sessionNewInput->sessionName,
            userId->userIdName,
            sizeof(sessionNewInput->sessionName));
    sessionNewInput->sessionNameLength = strlen(sessionNewInput->sessionName);
    sessionNewInput->fork = false;
    sessionNewInput->forkedSessionNameLength = 0;

    status = userMgr->create(userId, sessionNewInput, sessionNewOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Could not create session. Status: %s (%s)",
                userId->userIdName,
                strGetFromStatus(status),
                sessionNewOutput->error);
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "%s: Session created successfully. SessionId: %lu",
            userId->userIdName,
            sessionNewOutput->sessionId);

    status = StatusOk;
CommonExit:
    if (sessionNewOutput != NULL) {
        memFree(sessionNewOutput);
        sessionNewOutput = NULL;
    }

    if (sessionNewInput != NULL) {
        memFree(sessionNewInput);
        sessionNewInput = NULL;
    }

    return status;
}

static Status
processWorkItem(XcalarApiUserId *userId,
                Dag *sessionGraph,
                XcalarWorkItem *workItem)
{
    ApiHandler *apiHandler = NULL;
    Status status;
    Dag *qgraph = NULL;
    bool incOutstandingOps = false;

    qgraph = sessionGraph;

    status = xcApiCheckIfSufficientMem(workItem->api);
    BailIfFailed(status);

    status = xcApiGetApiHandler(&apiHandler, workItem->api);
    BailIfFailed(status);

    if ((apiHandler->needsSessionOrGraph() && qgraph == NULL) ||
        apiHandler->mayNeedSessionOrGraph()) {
        assert(workItem != NULL && workItem->userId != NULL &&
               (apiHandler->mayNeedSessionOrGraph() ||
                workItem->sessionInfo != NULL));

        // if api mayNeedSessionOrGraph then session info may be NULL - in which
        // case it doesn't really need a qgraph
        if (workItem->sessionInfo != NULL &&
            strlen(workItem->sessionInfo->sessionName) > 0 && qgraph == NULL) {
            status = UserMgr::get()
                         ->trackOutstandOps(workItem->userId,
                                            workItem->sessionInfo->sessionName,
                                            UserMgr::OutstandOps::Inc);
            BailIfFailed(status);
            incOutstandingOps = true;

            status = UserMgr::get()->getDag(workItem->userId,
                                            workItem->sessionInfo->sessionName,
                                            &qgraph);
            BailIfFailed(status);
        }
    }

    status = apiHandler->init(userId, workItem->sessionInfo, qgraph);
    BailIfFailed(status);

    status = apiHandler->setArg(workItem->input, workItem->inputSize);
    BailIfFailed(status);

    status = apiHandler->run(&workItem->output, &workItem->outputSize);
    BailIfFailed(status);

CommonExit:
    if (apiHandler != NULL) {
        delete apiHandler;
        apiHandler = NULL;
    }

    if (incOutstandingOps) {
        (void) UserMgr::get()
            ->trackOutstandOps(workItem->userId,
                               workItem->sessionInfo->sessionName,
                               UserMgr::OutstandOps::Dec);
        incOutstandingOps = false;
    }
    return status;
}

static Status
addLoadMemoryUdf(LoadMemUdfs::LoadMemType loadMemType, Dag *dag)
{
    Status status;
    XcalarWorkItem *workItem = NULL;
    XcalarApiUdfContainer *sessionContainer;

    assert(LoadMemUdfs::isValidLoadMemUdfType(loadMemType));
    LoadMemUdfs *lm = &loadMemUdfs[loadMemType];

    workItem = xcalarApiMakeUdfAddUpdateWorkItem(XcalarApiUdfAdd,
                                                 UdfTypePython,
                                                 lm->loadMemUdfName,
                                                 lm->loadMemSrc);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating add udf workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    assert(dag != NULL && dag->getSessionContainer() != NULL);

    sessionContainer = dag->getSessionContainer();

    workItem->userId =
        (XcalarApiUserId *) memAllocExt(sizeof(XcalarApiUserId), moduleName);

    if (workItem->userId == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "No memory for userId!");
        goto CommonExit;
    }

    workItem->sessionInfo = (XcalarApiSessionInfoInput *)
        memAllocExt(sizeof(XcalarApiSessionInfoInput), moduleName);

    if (workItem->sessionInfo == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "No memory for sessionInfo!");
        goto CommonExit;
    }
    workItem->sessionInfoSize = sizeof(XcalarApiSessionInfoInput);

    memcpy(workItem->userId,
           &sessionContainer->userId,
           sizeof(XcalarApiUserId));
    memcpy(workItem->sessionInfo,
           &sessionContainer->sessionInfo,
           sizeof(XcalarApiSessionInfoInput));

    status = processWorkItem(workItem->userId, NULL, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Add udf failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "add udf succeeded");

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
addLoadMemoryTarget()
{
    Status status;
    XcalarWorkItem *workItem = NULL;

    const char *addTarget =
        "{\"func\":\"addTarget\","
        "\"targetTypeId\":\"memory\","
        "\"targetName\":\"QA memory\","
        "\"targetParams\":{}}";
    workItem = xcalarApiMakeTargetWorkItem(addTarget);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating make target workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = processWorkItem(NULL, NULL, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "adding memory target failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "adding memory target succeeded");

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
cancelOp(XcalarApiUserId *userId, Dag *sessionGraph, const char *tableName)
{
    Status status;
    XcalarWorkItem *workItem = NULL;

    workItem = xcalarApiMakeCancelOpWorkItem(tableName);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating cancelOp workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "cancel %s failed, status: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "cancel %s succeeded", tableName);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
indexDataset(XcalarApiUserId *userId,
             Dag *sessionGraph,
             const char *datasetName,
             const char *keyName,
             const char *fatptrPrefixName,
             char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "indexDataset(%s, %p, %s, %s, %s) starting",
            userId->userIdName,
            sessionGraph,
            datasetName,
            keyName,
            fatptrPrefixName);

    workItem = xcalarApiMakeIndexWorkItem(datasetName,
                                          NULL,
                                          1,
                                          &keyName,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          fatptrPrefixName,
                                          false,
                                          false);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating indexDs workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailedMsg(moduleName,
                    status,
                    "indexDataset: xcApiGetOperatorHandlerInited return "
                    "status=%s",
                    strGetFromStatus(status));

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for indexDs: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "indexDataset(%s, %p, %s, %s, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                datasetName,
                keyName,
                fatptrPrefixName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.indexOutput.tableName,
            sizeof(workItem->output->outputResult.indexOutput.tableName));

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
indexTable(XcalarApiUserId *userId,
           Dag *sessionGraph,
           const char *tableName,
           unsigned numKeys,
           const char **keyNames,
           Ordering *orderings,
           char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "indexTable(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            tableName);

    workItem = xcalarApiMakeIndexWorkItem(NULL,
                                          tableName,
                                          numKeys,
                                          keyNames,
                                          NULL,
                                          NULL,
                                          orderings,
                                          NULL,
                                          NULL,
                                          NULL,
                                          false,
                                          false);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating indexTable workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for indexTable: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "indexTable(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "indexTable(%s, %p, %s) succeeded. "
            "Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            tableName,
            workItem->output->outputResult.indexOutput.tableName);

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.indexOutput.tableName,
            sizeof(workItem->output->outputResult.indexOutput.tableName));
CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
load(XcalarApiUserId *userId,
     Dag *sessionGraph,
     const char *datasetPath,
     DfFormatType datasetType,
     uint32_t datasetSize,
     const char *datasetName,
     LoadMemUdfs::LoadMemType loadMemType,
     char *parseArgsJson)
{
    XcalarWorkItem *workItem = NULL;
    QpLoad::LoadUrl loadUrl;

    DfLoadArgs *loadArgs = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    loadArgs = new (std::nothrow) DfLoadArgs();
    BailIfNull(loadArgs);
    memZero(loadArgs, sizeof(*loadArgs));
    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].recursive = false;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    loadArgs->maxSize = datasetSize;

    // We're in this limbo state where a lot of the config file still has
    // the nfs:// prefix, whereas with this change, we no longer have this
    // nfs:// prefix. Hence we need to support both
    status = loadUrl.parseFromString(datasetPath);
    if (status == StatusOk) {
        loadUrl.populateSourceArgs(&loadArgs->sourceArgsList[0]);
    } else {
        strlcpy(loadArgs->sourceArgsList[0].targetName,
                "Default Shared Root",
                sizeof(loadArgs->sourceArgsList[0].targetName));
        strlcpy(loadArgs->sourceArgsList[0].path,
                datasetPath,
                sizeof(loadArgs->sourceArgsList[0].path));
    }

    if (datasetType == DfFormatCsv) {
        strlcpy(loadArgs->parseArgs.parserFnName,
                "default:parseCsv",
                sizeof(loadArgs->parseArgs.parserFnName));
        strlcpy(loadArgs->parseArgs.parserArgJson,
                "{\"schemaMode\": \"none\", \"schemaFile\": \"\", "
                "\"typedColumns\": []}",
                sizeof(loadArgs->parseArgs.parserArgJson));
    } else {
        assert(datasetType == DfFormatJson);
        strlcpy(loadArgs->parseArgs.parserFnName,
                "default:parseJson",
                sizeof(loadArgs->parseArgs.parserFnName));
        strlcpy(loadArgs->parseArgs.parserArgJson,
                "{}",
                sizeof(loadArgs->parseArgs.parserArgJson));
    }

    if (strncmp(datasetPath, MemoryPrefix, MemoryPrefixLen) == 0) {
        LoadMemUdfs *lm = &loadMemUdfs[loadMemType];
        snprintf(loadArgs->parseArgs.parserFnName,
                 sizeof(loadArgs->parseArgs.parserFnName),
                 "%s",
                 lm->loadMemUdfFnName);
        switch (loadMemType) {
        case LoadMemUdfs::LoadMemSimple:  // pass through
        case LoadMemUdfs::LoadMemDefault:
            snprintf(loadArgs->parseArgs.parserArgJson,
                     sizeof(loadArgs->parseArgs.parserArgJson),
                     "{}");
            break;
        case LoadMemUdfs::LoadMemWithSchema: {
            snprintf(loadArgs->parseArgs.parserArgJson,
                     sizeof(loadArgs->parseArgs.parserArgJson),
                     "%s",
                     parseArgsJson);
            break;
        }
        default:
            break;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "load(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            datasetName);

    workItem = xcalarApiMakeBulkLoadWorkItem(datasetName, loadArgs);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Could not create BulkLoadWorkItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailedMsg(moduleName,
                    status,
                    "load: xcApiGetOperatorHandlerInited return status=%s",
                    strGetFromStatus(status));

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Could not create dag node for bulkLoad");
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "load(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "load(%s, %p, %s) succeeded. DatasetName: %s",
            userId->userIdName,
            sessionGraph,
            datasetName,
            workItem->output->outputResult.loadOutput.dataset.name);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (loadArgs) {
        delete loadArgs;
        loadArgs = NULL;
    }

    if (status != StatusOk && status != StatusDsDatasetLoaded) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "load return status=%s",
                strGetFromStatus(status));
    }
    return status;
}

static Status
getUniqueName(const char *prefix, char *name, size_t nameSize)
{
    size_t ret;
    Status status = StatusUnknown;

    ret = snprintf(name,
                   nameSize,
                   "%s-%u-%lu",
                   prefix,
                   sysGetTid(),
                   XidMgr::get()->xidGetNext());
    if (ret >= nameSize) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Name too long (%lu chars). Max is %lu chars",
                ret,
                nameSize - 1);
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}

static Status
synthesize(XcalarApiUserId *userId,
           Dag *sessionGraph,
           const char *srcTable,
           int numCols,
           XcalarApiRenameMap *renameMap,
           char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusOk;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    bool sameSession = true;
    Txn prevTxn = Txn();

    workItem = xcalarApiMakeSynthesizeWorkItem(srcTable,
                                               NULL,
                                               sameSession,
                                               numCols,
                                               renameMap);

    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate synthesize workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for synthesize: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "synthesize(%s, %p, %s, %d) failed: %s",
                userId->userIdName,
                sessionGraph,
                srcTable,
                numCols,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "synthesize(%s, %p, %s, %d) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            srcTable,
            numCols,
            workItem->output->outputResult.synthesizeOutput.tableName);

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.synthesizeOutput.tableName,
            sizeof(workItem->output->outputResult.synthesizeOutput.tableName));
CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }
    return status;
}

static Status
publishTable(XcalarApiUserId *userId,
             Dag *sessionGraph,
             const char *srcTable,
             const char *dstTable)
{
    XcalarWorkItem *workItem = NULL;
    Status status;

    xSyslog(moduleName,
            XlogInfo,
            "Starting publish %s from source %s",
            dstTable,
            srcTable);

    workItem = xcalarApiMakePublishWorkItem(srcTable, dstTable, 0, false);
    BailIfNull(workItem);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Publish table %s %s failed: %s",
                srcTable,
                dstTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Finished publish %s from source %s",
            dstTable,
            srcTable);

CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
unpublishTable(XcalarApiUserId *userId,
               Dag *sessionGraph,
               const char *srcTable,
               bool inactivate)
{
    XcalarWorkItem *workItem = NULL;
    Status status;

    xSyslog(moduleName,
            XlogInfo,
            "Starting unpublish %s inactivate: %d",
            srcTable,
            inactivate);

    workItem = xcalarApiMakeUnpublishWorkItem(srcTable, inactivate);
    BailIfNull(workItem);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Unpublish table %s inactivate %s failed: %s",
                srcTable,
                inactivate ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Finished unpublish %s inactivate: %d",
            srcTable,
            inactivate);

CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
coalesce(XcalarApiUserId *userId, Dag *sessionGraph, const char *srcTable)
{
    XcalarWorkItem *workItem = NULL;
    Status status;

    xSyslog(moduleName, XlogInfo, "Starting coalesce %s", srcTable);

    workItem = xcalarApiMakeCoalesceWorkItem(srcTable);
    BailIfNull(workItem);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Coalesce table %s failed: %s",
                srcTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "Finished coalesce %s", srcTable);
CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
select(XcalarApiUserId *userId,
       Dag *sessionGraph,
       const char *srcTable,
       int64_t batchId,
       const char *filterString,
       unsigned numColumns,
       char (*columns)[XcalarApiMaxFieldNameLen + 1],
       char dstTable[XcalarApiMaxTableNameLen + 1])
{
    OperatorHandler *operatorHandler = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    XcalarWorkItem *workItem = NULL;
    Status status;
    XcalarApiRenameMap renameMap[numColumns];
    Txn prevTxn = Txn();

    for (unsigned ii = 0; ii < numColumns; ii++) {
        strcpy(renameMap[ii].oldName, columns[ii]);
        strcpy(renameMap[ii].newName, columns[ii]);
        renameMap[ii].type = DfUnknown;
    }

    workItem = xcalarApiMakeSelectWorkItem(srcTable,
                                           NULL,
                                           0,
                                           batchId,
                                           filterString,
                                           numColumns,
                                           renameMap);
    BailIfNull(workItem);

    xSyslog(moduleName, XlogInfo, "Starting select %s", srcTable);

    do {
        sysUSleep(100);
        status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                               workItem,
                                               userId,
                                               sessionGraph,
                                               false,
                                               &prevTxn);
    } while (status == StatusAccess);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    BailIfFailed(status);

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "select(%s, %p, %s, %ld, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                srcTable,
                batchId,
                dstTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    strlcpy(dstTable,
            workItem->output->outputResult.selectOutput.tableName,
            XcalarApiMaxTableNameLen + 1);

    xSyslog(moduleName,
            XlogInfo,
            "Finished select %s into %s",
            srcTable,
            dstTable);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler) {
        delete operatorHandler;
        operatorHandler = NULL;
    }
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    return status;
}

static Status
update(XcalarApiUserId *userId,
       Dag *sessionGraph,
       const char *srcTable,
       const char *dstTable,
       int64_t *batchIdOut)
{
    XcalarWorkItem *workItem = NULL;
    Status status;
    time_t time = 0;

    workItem =
        xcalarApiMakeUpdateWorkItem(1, &srcTable, &dstTable, &time, false);
    BailIfNull(workItem);

    xSyslog(moduleName,
            XlogInfo,
            "Starting update %s from source %s",
            dstTable,
            srcTable);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "update table %s %s failed: %s",
                srcTable,
                dstTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Finished update %s from source %s",
            dstTable,
            srcTable);

    if (batchIdOut) {
        *batchIdOut = workItem->output->outputResult.updateOutput.batchIds[0];
    }
CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

Status
restore(XcalarApiUserId *userId, Dag *sessionGraph, const char *srcTable)
{
    XcalarWorkItem *workItem = NULL;
    Status status;

    workItem = xcalarApiMakeRestoreTableWorkItem(srcTable);
    BailIfNull(workItem);

    xSyslog(moduleName, XlogInfo, "Starting restore %s", srcTable);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "restore table %s failed: %s",
                srcTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "Finished restore %s", srcTable);

CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

Status
changeOwner(XcalarApiUserId *userId, Dag *sessionGraph, const char *srcTable)
{
    xcalar::compute::localtypes::PublishedTable::PublishedTableService
        ptService;
    Status status;
    xcalar::compute::localtypes::PublishedTable::ChangeOwnerRequest req;

    req.set_publishedtablename(srcTable);
    req.mutable_scope()->mutable_workbook()->mutable_name()->set_username(
        userId->userIdName);
    req.mutable_scope()->mutable_workbook()->mutable_name()->set_workbookname(
        userId->userIdName);

    xSyslog(moduleName, XlogInfo, "Starting change owner %s", srcTable);

    status = ptService.changeOwner(&req, nullptr);

    xSyslog(moduleName, XlogInfo, "Finished change owner %s", srcTable);

    return status;
}

Status
listPubTable(
    XcalarApiUserId *userId,
    Dag *sessionGraph,
    const char *srcTablePattern,
    xcalar::compute::localtypes::PublishedTable::ListTablesResponse &response)
{
    return HashTreeMgr::get()->listHashTrees(srcTablePattern,
                                             0,
                                             -1,
                                             0,
                                             &response);
}

static Status
executeRetina(XcalarApiUserId *userId,
              Dag *sessionGraph,
              const char *retinaName,
              RetinaDst *tableInfo,
              char dstTable[XcalarApiMaxTableNameLen + 1],
              RandWeakHandle *randHandle)
{
    ServiceMgr *serviceMgr = ServiceMgr::get();
    ProtoRequestMsg protoReqMsg;
    ProtoResponseMsg protoResMsg;
    ServiceRequest requestMsg;
    ServiceResponse responseMsg;
    Status status;
    Runtime::SchedId schedId = static_cast<Runtime::SchedId>(
        rndWeakGenerate64(randHandle) % Runtime::TotalSdkScheds);
    Runtime *rt = Runtime::get();
    const char *schedName = rt->getSchedNameFromId(schedId);
    Txn prevTxn = Txn();
    Txn newTxn;
    xcalar::compute::localtypes::Dataflow::ExecuteRequest executeReq;

    try {
        executeReq.set_dataflow_name(retinaName);
        executeReq.set_job_name(retinaName);
        executeReq.set_sched_name(schedName);
        executeReq.set_udf_user_name(userId->userIdName);
        executeReq.set_udf_session_name(userId->userIdName);
        executeReq.set_is_async(false);
        executeReq.set_optimized(true);
        executeReq.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(userId->userIdName);
        executeReq.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(userId->userIdName);
        // export to active session
        executeReq.set_export_to_active_session(true);
        executeReq.set_dest_table(dstTable);
        protoReqMsg.mutable_servic()->set_servicename("Dataflow");
        protoReqMsg.mutable_servic()->set_methodname("Execute");
        protoReqMsg.mutable_servic()->mutable_body()->PackFrom(executeReq);
        protoReqMsg.set_requestid(0);
        protoReqMsg.set_target(ProtoMsgTargetService);
    } catch (...) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate query request");
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "executeRetina(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            retinaName);

    newTxn = serviceMgr->generateTxn(&protoReqMsg.servic(), &status);
    assert(status == StatusOk);
    status = MsgMgr::get()->addTxnLog(newTxn);
    assert(status == StatusOk);
    Txn::setTxn(newTxn);
    status = serviceMgr->handleApi(&protoReqMsg, &protoResMsg);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "executeRetina(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                retinaName,
                strGetFromStatus(status));
        if (isFatalRetinaExecuteStatus(status)) {
            assert(0);
        }
        goto CommonExit;
    }

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    return status;
}

static Status
map(XcalarApiUserId *userId,
    Dag *sessionGraph,
    const char *tableName,
    unsigned numEvals,
    const char evalStrs[][XcalarApiMaxEvalStringLen + 1],
    const char newFieldNames[][XcalarApiMaxFieldNameLen + 1],
    bool icvMode,
    char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    const char *evalStrsTmp[numEvals];
    const char *fieldNamesTmp[numEvals];
    Txn prevTxn = Txn();

    for (unsigned ii = 0; ii < numEvals; ii++) {
        evalStrsTmp[ii] = evalStrs[ii];
        fieldNamesTmp[ii] = newFieldNames[ii];
    }

    xSyslog(moduleName,
            XlogInfo,
            "map(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            tableName);

    workItem = xcalarApiMakeMapWorkItem(tableName,
                                        NULL,
                                        icvMode,
                                        numEvals,
                                        evalStrsTmp,
                                        fieldNamesTmp);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Could not create dag node for map");
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "map(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "map(%s, %p, %s) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            tableName,
            workItem->output->outputResult.mapOutput.tableName);

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.mapOutput.tableName,
            sizeof(workItem->output->outputResult.mapOutput.tableName));

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
doUnion(XcalarApiUserId *userId,
        Dag *sessionGraph,
        unsigned numSrcTables,
        const char **srcTables,
        const char *dstTableName,
        unsigned *renameMapSizes,
        XcalarApiRenameMap **renameMap,
        bool dedup,
        UnionOperator utype)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    workItem = xcalarApiMakeUnionWorkItem(numSrcTables,
                                          srcTables,
                                          dstTableName,
                                          renameMapSizes,
                                          renameMap,
                                          dedup,
                                          utype);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Could not create dag node for join");
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    BailIfFailed(status);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
join(XcalarApiUserId *userId,
     Dag *sessionGraph,
     const char *leftTable,
     const char *rightTable,
     unsigned numLeftColumns,
     unsigned numRightColumns,
     XcalarApiRenameMap *renameMap,
     JoinOperator joinType)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "%s(%s, %p, %s, %s, %u, %u, %p) starting",
            strGetFromJoinOperator(joinType),
            userId->userIdName,
            sessionGraph,
            leftTable,
            rightTable,
            numLeftColumns,
            numRightColumns,
            renameMap);

    workItem = xcalarApiMakeJoinWorkItem(leftTable,
                                         rightTable,
                                         NULL,
                                         joinType,
                                         true,
                                         true,
                                         false,
                                         numLeftColumns,
                                         numRightColumns,
                                         renameMap,
                                         NULL);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Could not create dag node for join");
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s(%s, %p, %s, %s, %u, %u, %p) failed: %s",
                strGetFromJoinOperator(joinType),
                userId->userIdName,
                sessionGraph,
                leftTable,
                rightTable,
                numLeftColumns,
                numRightColumns,
                renameMap,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "%s(%s, %p, %s, %s, %u, %u, %p) succeeded. "
            "Dst Table: %s",
            strGetFromJoinOperator(joinType),
            userId->userIdName,
            sessionGraph,
            leftTable,
            rightTable,
            numLeftColumns,
            numRightColumns,
            renameMap,
            workItem->output->outputResult.joinOutput.tableName);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
groupBy(XcalarApiUserId *userId,
        Dag *sessionGraph,
        const char *tableName,
        unsigned numEvals,
        const char evalStrs[][XcalarApiMaxEvalStringLen + 1],
        const char newFieldNames[][XcalarApiMaxFieldNameLen + 1],
        bool includeSample,
        bool icvMode,
        bool groupAll,
        char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    const char *evalStrsTmp[numEvals];
    const char *fieldNamesTmp[numEvals];
    size_t ret = 0;
    XcalarApiGetTableMetaInput getTableMetaInput;
    size_t outputSize = 0;
    XcalarApiOutput *apiOut = NULL;
    getTableMetaInput.tableNameInput.isTable = true;
    getTableMetaInput.tableNameInput.nodeId = getXidFromName(tableName);
    getTableMetaInput.isPrecise = false;
    XcalarApiGetTableMetaOutput *tableMetaOut = NULL;
    unsigned numFloatKeys = 0;
    char(*keyFieldNames)[XcalarApiMaxFieldNameLen + 1] = NULL;
    char(*mapEvalStrings)[XcalarApiMaxEvalStringLen + 1] = NULL;
    char dstTable[XcalarApiMaxTableNameLen + 1];
    XcalarApiKeyInput *keys = NULL;
    Txn prevTxn = Txn();

    status = Operators::get()->getTableMeta(&getTableMetaInput,
                                            &apiOut,
                                            &outputSize,
                                            sessionGraph->getId(),
                                            &testUserId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting table meta for %s: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    tableMetaOut = &apiOut->outputResult.getTableMetaOutput;

    if (tableMetaOut->numKeys != 0) {
        keyFieldNames = new (std::nothrow) char[tableMetaOut->numKeys]
                                               [XcalarApiMaxFieldNameLen + 1];
        BailIfNull(keyFieldNames);

        mapEvalStrings = new (std::nothrow) char[tableMetaOut->numKeys]
                                                [XcalarApiMaxEvalStringLen + 1];
        BailIfNull(mapEvalStrings);

        keys = new (std::nothrow) XcalarApiKeyInput[tableMetaOut->numKeys];
        BailIfNull(keys);
    }

    for (unsigned ii = 0; ii < tableMetaOut->numKeys; ii++) {
        DfFieldAttrHeader *fah = &tableMetaOut->keyAttr[ii];
        strcpy(keys[ii].keyName, fah->name);
        strcpy(keys[ii].keyFieldName, fah->name);
        keys[ii].ordering = Unordered;
        keys[ii].type = DfUnknown;

        if (fah->type == DfFloat32 || fah->type == DfFloat64) {
            status = getUniqueName("gKey",
                                   keyFieldNames[ii],
                                   sizeof(keyFieldNames[ii]));
            BailIfFailed(status);

            char escapedKeyName[XcalarApiMaxEvalStringLen + 1];
            ret = strlcpy(escapedKeyName, fah->name, sizeof(escapedKeyName));
            if (ret >= sizeof(escapedKeyName)) {
                status = StatusNoBufs;
                xSyslog(moduleName,
                        XlogErr,
                        "Key name \"%s\" is too long",
                        fah->name);
                goto CommonExit;
            }

            status = DataFormat::escapeNestedDelim(escapedKeyName,
                                                   sizeof(escapedKeyName),
                                                   &ret);
            BailIfFailed(status);

            ret = snprintf(mapEvalStrings[ii],
                           sizeof(mapEvalStrings[ii]),
                           "int(%s)",
                           escapedKeyName);
            if (ret >= sizeof(mapEvalStrings[ii])) {
                status = StatusNoBufs;
                xSyslog(moduleName,
                        XlogErr,
                        "Eval string \"int(%s)\" is too long",
                        escapedKeyName);
                goto CommonExit;
            }
            numFloatKeys++;
            strcpy(keys[ii].keyFieldName, keyFieldNames[ii]);
        }
    }

    if (numFloatKeys != 0) {
        status = map(userId,
                     sessionGraph,
                     tableName,
                     numFloatKeys,
                     mapEvalStrings,
                     keyFieldNames,
                     false,
                     dstTable);
        BailIfFailed(status);

        const char *colNamesFinal[numFloatKeys];
        Ordering ordering[numFloatKeys];
        for (unsigned ii = 0; ii < numFloatKeys; ii++) {
            colNamesFinal[ii] = keyFieldNames[ii];
            ordering[ii] = Unordered;
        }

        status = indexTable(userId,
                            sessionGraph,
                            dstTable,
                            numFloatKeys,
                            colNamesFinal,
                            ordering,
                            dstTable);
        BailIfFailed(status);
    } else {
        strlcpy(dstTable, tableName, sizeof(dstTable));
    }

    for (unsigned ii = 0; ii < numEvals; ii++) {
        evalStrsTmp[ii] = evalStrs[ii];
        fieldNamesTmp[ii] = newFieldNames[ii];
    }

    xSyslog(moduleName,
            XlogInfo,
            "groupBy(%s, %p, %s, %s) starting",
            userId->userIdName,
            sessionGraph,
            dstTable,
            (includeSample ? "true" : "false"));

    workItem = xcalarApiMakeGroupByWorkItem(dstTable,
                                            NULL,
                                            tableMetaOut->numKeys,
                                            keys,
                                            includeSample,
                                            icvMode,
                                            false,
                                            numEvals,
                                            evalStrsTmp,
                                            fieldNamesTmp);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for groupBy: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "groupBy(%s, %p, %s, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                dstTable,
                (includeSample ? "true" : "false"),
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "groupBy(%s, %p, %s, %s) succeeded. Dst table: %s",
            userId->userIdName,
            sessionGraph,
            dstTable,
            (includeSample ? "true" : "false"),
            workItem->output->outputResult.groupByOutput.tableName);

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.groupByOutput.tableName,
            sizeof(workItem->output->outputResult.groupByOutput.tableName));

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    if (apiOut != NULL) {
        memFree(apiOut);
        apiOut = NULL;
    }

    if (keyFieldNames != NULL) {
        delete[] keyFieldNames;
        keyFieldNames = NULL;
    }

    if (keys != NULL) {
        delete[] keys;
        keys = NULL;
    }

    if (mapEvalStrings != NULL) {
        delete[] mapEvalStrings;
        mapEvalStrings = NULL;
    }

    return status;
}

static Status
filter(XcalarApiUserId *userId,
       Dag *sessionGraph,
       const char *tableName,
       const char *evalStr,
       char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "filter(%s, %p, %s, %s) starting",
            userId->userIdName,
            sessionGraph,
            tableName,
            evalStr);

    workItem = xcalarApiMakeFilterWorkItem(evalStr, tableName, NULL);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for filter: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "filter(%s, %p, %s, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                evalStr,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "filter(%s, %p, %s, %s) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            tableName,
            evalStr,
            workItem->output->outputResult.filterOutput.tableName);

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.mapOutput.tableName,
            sizeof(workItem->output->outputResult.mapOutput.tableName));

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
project(XcalarApiUserId *userId,
        Dag *sessionGraph,
        const char *tableName,
        int numCols,
        char (*columns)[XcalarApiMaxFieldNameLen + 1],
        char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "project(%s, %p, %s, %d, %p) starting",
            userId->userIdName,
            sessionGraph,
            tableName,
            numCols,
            columns);

    workItem = xcalarApiMakeProjectWorkItem(numCols, columns, tableName, NULL);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate project workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for project: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "project(%s, %p, %s, %d, %p) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                numCols,
                columns,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "project(%s, %p, %s, %d, %p) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            tableName,
            numCols,
            columns,
            workItem->output->outputResult.projectOutput.tableName);

    strlcpy(dstTableNameOut,
            workItem->output->outputResult.projectOutput.tableName,
            sizeof(workItem->output->outputResult.projectOutput.tableName));
CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
aggregate(XcalarApiUserId *userId,
          Dag *sessionGraph,
          const char *tableName,
          const char *evalStr,
          AggregateResult *resultOut)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "aggregate(%s, %p, %s, %s) starting",
            userId->userIdName,
            sessionGraph,
            tableName,
            evalStr);

    workItem = xcalarApiMakeAggregateWorkItem(tableName, NULL, evalStr);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for aggregate: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "aggregate(%s, %p, %s, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                evalStr,
                strGetFromStatus(status));
        goto CommonExit;
    }

    resultOut->output =
        (XcalarApiAggregateOutput *) memAllocExt(workItem->outputSize,
                                                 moduleName);
    if (resultOut->output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Could not allocate memory for aggregate result");
        goto CommonExit;
    }

    memcpy(resultOut->output,
           &workItem->output->outputResult.aggregateOutput,
           workItem->outputSize);

    xSyslog(moduleName,
            XlogInfo,
            "aggregate(%s, %p, %s, %s) succeeded",
            userId->userIdName,
            sessionGraph,
            tableName,
            evalStr);
CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static Status
getRowNum(XcalarApiUserId *userId,
          Dag *sessionGraph,
          const char *tableName,
          const char *dstColName,
          char dstTable[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "getRowNum(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            tableName);

    workItem = xcalarApiMakeGetRowNumWorkItem(tableName, NULL, dstColName);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for getRowNum: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getRowNum(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    strlcpy(dstTable,
            workItem->output->outputResult.getRowNumOutput.tableName,
            XcalarApiMaxTableNameLen + 1);

    xSyslog(moduleName,
            XlogInfo,
            "getRowNum(%s, %p, %s) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            tableName,
            workItem->output->outputResult.getRowNumOutput.tableName);
CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

static int
removeDirectory(const char *path)
{
    DIR *d = opendir(path);
    size_t pathLen = strlen(path);
    int r = -1;

    if (d) {
        struct dirent *p;
        r = 0;
        while (!r && (p = readdir(d))) {
            int r2 = -1;
            char *buf;
            size_t len;

            if (strcmp(p->d_name, ".") == 0 || strcmp(p->d_name, "..") == 0) {
                continue;
            }

            len = pathLen + strlen(p->d_name) + 2;
            buf = (char *) memAlloc(len);

            if (buf) {
                struct stat statbuf;

                snprintf(buf, len, "%s/%s", path, p->d_name);

                if (!stat(buf, &statbuf)) {
                    if (S_ISDIR(statbuf.st_mode)) {
                        r2 = removeDirectory(buf);
                    } else {
                        r2 = unlink(buf);
                    }
                }

                memFree(buf);
            }

            r = r2;
        }

        closedir(d);
    }

    if (!r) {
        r = rmdir(path);
    }

    return r;
}

static Status
exportTableAsRetina(XcalarApiUserId *userId,
                    Dag *sessionGraph,
                    const char *tableName,
                    const char *retinaName)
{
    Status status = StatusUnknown;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    int numCols = 0;
    XcalarWorkItem *workItem = NULL;
    RetinaDst *tableInfo = NULL;
    RandWeakHandle randHandle;

    status = getRandomColumns(sessionGraph,
                              tableName,
                              &randHandle,
                              &columns,
                              NULL,
                              &numCols,
                              100);
    BailIfFailed(status);

    tableInfo = (RetinaDst *) memAllocExt(sizeof(*tableInfo) +
                                              numCols * sizeof(ExColumnName),
                                          moduleName);
    BailIfNullWith(tableInfo, StatusNoMem);

    tableInfo->numColumns = numCols;
    tableInfo->target.isTable = true;
    strlcpy(tableInfo->target.name, tableName, sizeof(tableInfo->target.name));
    for (int ii = 0; ii < numCols; ii++) {
        strlcpy(tableInfo->columns[ii].name,
                columns[ii],
                sizeof(tableInfo->columns[ii].name));
        strlcpy(tableInfo->columns[ii].headerAlias,
                columns[ii],
                sizeof(tableInfo->columns[ii].headerAlias));
    }

    workItem =
        xcalarApiMakeMakeRetinaWorkItem(retinaName, 1, &tableInfo, 0, NULL);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate make retina workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "makeRetina(%s, %p, %s, %d, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                numCols,
                columns,
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    exportRetina(userId, sessionGraph, retinaName);

CommonExit:
    if (tableInfo != NULL) {
        memFree(tableInfo);
        tableInfo = NULL;
    }

    if (columns != NULL) {
        memFree(columns);
        columns = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

static Status
exportTable(XcalarApiUserId *userId,
            Dag *sessionGraph,
            const char *tableName,
            int numCols,
            const char (*columnsIn)[XcalarApiMaxFieldNameLen + 1],
            const char *fileName)
{
    Status status = StatusUnknown;
    size_t ret;
    int ii;
    ExColumnName *columns = NULL;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "exportTable(%s, %p, %s, %d, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            tableName,
            numCols,
            columnsIn,
            fileName);

    columns = (ExColumnName *) memAlloc(sizeof(*columns) * numCols);
    if (columns == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate columns "
                "(numCols: %d)",
                numCols);
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < numCols; ii++) {
        ret =
            strlcpy(columns[ii].name, columnsIn[ii], sizeof(columns[ii].name));
        if (ret >= sizeof(columns[ii].name)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Column name \"%s\" is too long (%lu chars) "
                    "Max is %lu chars",
                    columnsIn[ii],
                    ret,
                    sizeof(columns[ii].name) - 1);
            status = StatusNoBufs;
            goto CommonExit;
        }
        ret = strlcpy(columns[ii].headerAlias,
                      columnsIn[ii],
                      sizeof(columns[ii].headerAlias));
        if (ret >= sizeof(columns[ii].headerAlias)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Column name \"%s\" is too long (%lu chars) "
                    "Max is %lu chars",
                    columnsIn[ii],
                    ret,
                    sizeof(columns[ii].headerAlias) - 1);
            status = StatusNoBufs;
            goto CommonExit;
        }
    }

    // XXX maybe do not hard code driver name and params
    {
        char exportParams[512];
        snprintf(exportParams,
                 sizeof(exportParams),
                 "{\"file_path\": \"%s/%s\", \"target\": \"Default "
                 "Shared Root\"}",
                 XcalarConfig::get()->xcalarLogCompletePath_,
                 fileName);
        workItem = xcalarApiMakeExportWorkItem(tableName,
                                               NULL,
                                               numCols,
                                               columns,
                                               "single_csv",
                                               exportParams);
    }
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate export workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for export: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "exportTable(%s, %p, %s, %d, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                numCols,
                columnsIn,
                fileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "exportTable(%s, %p, %s, %d, %p, %s) succeeded",
            userId->userIdName,
            sessionGraph,
            tableName,
            numCols,
            columnsIn,
            fileName);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (columns != NULL) {
        memFree(columns);
        columns = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    // Should delete the export dag node no matter what
    if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
        Status status2 = sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                          DgDagStateError);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to drop and change dagNode (%lu) state to "
                    "Error: %s",
                    workspaceGraphNodeId,
                    strGetFromStatus(status2));
        }
    }

    return status;
}

static Status
indexRandomDataset(XcalarApiUserId *userId,
                   Dag *sessionGraph,
                   RandWeakHandle *randHandle,
                   char dstTableName1[XcalarApiMaxTableNameLen + 1],
                   char dstTableName2[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiListDatasetsOutput *listDatasetsOutput = NULL;
    size_t outputSize;
    int MaxTries = 100;
    int currentTry;
    unsigned datasetIdx = 0;
    bool datasetFound = false;
    char *keyName = NULL;
    char prefixName[XcalarApiMaxFieldNameLen + 1];
    status = getUniqueName("prefix", prefixName, sizeof(prefixName));
    BailIfFailed(status);

    for (currentTry = 0; currentTry < MaxTries; currentTry++) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }

        status = Dataset::get()->listDatasets(&output, &outputSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to dsListDatasets: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        listDatasetsOutput = &output->outputResult.listDatasetsOutput;
        if (listDatasetsOutput->numDatasets == 0) {
            sysSleep(DatasetSize / (100 * MB));
            continue;
        }

        for (datasetIdx = rndWeakGenerate64(randHandle) %
                          listDatasetsOutput->numDatasets;
             datasetIdx < listDatasetsOutput->numDatasets;
             datasetIdx++) {
            if (listDatasetsOutput->datasets[datasetIdx].loadIsComplete) {
                datasetFound = true;
                break;
            }
        }

        if (datasetFound) {
            break;
        } else {
            sysSleep(DatasetSize / (100 * MB));
        }
    }

    if (!datasetFound) {
        xSyslog(moduleName,
                XlogErr,
                "No datasets found after searching %u times",
                currentTry);
        status = StatusNoData;
        goto CommonExit;
    }

    status = getRandomColumn(sessionGraph,
                             listDatasetsOutput->datasets[datasetIdx].name,
                             &keyName,
                             randHandle);
    BailIfFailed(status);

    xSyslog(moduleName,
            XlogNote,
            "Indexing %s on key %s",
            listDatasetsOutput->datasets[datasetIdx].name,
            keyName);

    status = indexDataset(userId,
                          sessionGraph,
                          listDatasetsOutput->datasets[datasetIdx].name,
                          keyName,
                          prefixName,
                          dstTableName1);
    BailIfFailed(status);

    status = indexDataset(userId,
                          sessionGraph,
                          listDatasetsOutput->datasets[datasetIdx].name,
                          keyName,
                          prefixName,
                          dstTableName2);
    BailIfFailed(status);

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
        listDatasetsOutput = NULL;
    }

    if (keyName != NULL) {
        memFree(keyName);
    }

    return status;
}

static Status
getRandomTable(Dag *sessionGraph,
               char *tableNameOut,
               size_t maxTableNameLen,
               RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiListDagNodesOutput *listTablesOutput = NULL;
    size_t outputSize;
    unsigned tableIdx = 0;
    size_t ret;

    do {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            listTablesOutput = NULL;
        }

        status = sessionGraph->listDagNodeInfo("*",
                                               &output,
                                               &outputSize,
                                               SrcTable,
                                               &testUserId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error with dag->listDagNodeInfo: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        listTablesOutput = &output->outputResult.listNodesOutput;
        if (listTablesOutput->numNodes == 0) {
            status = StatusFunctionalTestNoTablesLeft;
            xSyslog(moduleName, XlogErr, "No tables found in session");
            goto CommonExit;
        }

        unsigned ii;
        for (ii = 0; ii < listTablesOutput->numNodes; ii++) {
            if (listTablesOutput->nodeInfo[ii].size > 0) {
                break;
            }
        }

        if (ii == listTablesOutput->numNodes) {
            status = StatusFunctionalTestNoTablesLeft;
            xSyslog(moduleName, XlogErr, "All tables are empty in session");
            goto CommonExit;
        }

        // weight the bigger indexes to get more recent tables
        unsigned loopCount = 0;
        do {
            tableIdx =
                rndWeakGenerate64(randHandle) % listTablesOutput->numNodes;
            loopCount++;
        } while (tableIdx <= listTablesOutput->numNodes / 2 &&
                 loopCount < MaxRandomTableLoops);
    } while (status != StatusOk);

    ret = strlcpy(tableNameOut,
                  listTablesOutput->nodeInfo[tableIdx].name,
                  maxTableNameLen + 1);
    if (ret >= maxTableNameLen + 1) {
        xSyslog(moduleName,
                XlogErr,
                "Table name (%s) is too long (%lu chars) "
                "Max is %lu chars",
                listTablesOutput->nodeInfo[tableIdx].name,
                strlen(listTablesOutput->nodeInfo[tableIdx].name),
                maxTableNameLen);
        status = StatusNoBufs;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
        listTablesOutput = NULL;
    }

    return status;
}

static Status
getRow(Dag *sessionGraph,
       const char *tableName,
       json_t **jsonRecordOut,
       uint64_t rowNum,
       XcalarApiGetTableMetaOutput *metaOut)
{
    Status status = StatusUnknown;
    XcalarApiMakeResultSetOutput *makeResultSetOutput = NULL;
    XcalarApiUdfContainer *sessionContainer =
        sessionGraph->getSessionContainer();

    uint64_t resultSetId = XidInvalid;
    uint64_t numEntries;

    json_t *jsonRecord = NULL;
    ResultSet *rs = NULL;

    makeResultSetOutput = (XcalarApiMakeResultSetOutput *) memAlloc(
        sizeof(*makeResultSetOutput) +
        sizeof(XcalarApiTableMeta) * Config::get()->getActiveNodes());
    if (makeResultSetOutput == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate resultSetOutput");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = ResultSetMgr::get()->makeResultSet(sessionGraph,
                                                tableName,
                                                false,
                                                makeResultSetOutput,
                                                &sessionContainer->userId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error creating result set during "
                "getRow for %s: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (metaOut != NULL) {
        XcalarApiGetTableMetaInput getTableMetaInput;
        size_t outputSize;
        XcalarApiOutput *apiOut;
        getTableMetaInput.tableNameInput.isTable = true;
        getTableMetaInput.tableNameInput.nodeId = getXidFromName(tableName);
        getTableMetaInput.isPrecise = false;

        status = Operators::get()->getTableMeta(&getTableMetaInput,
                                                &apiOut,
                                                &outputSize,
                                                sessionGraph->getId(),
                                                &testUserId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting table meta for %s: %s",
                    tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        memcpy(metaOut,
               &apiOut->outputResult.getTableMetaOutput,
               sizeof(*metaOut));

        // we aren't copying any of the node level metas
        metaOut->numMetas = 0;

        memFree(apiOut);
    }

    resultSetId = makeResultSetOutput->resultSetId;
    numEntries = makeResultSetOutput->numEntries;

    if (numEntries == 0) {
        status = StatusFunctionalTestTableEmpty;
        goto CommonExit;
    }

    rs = ResultSetMgr::get()->getResultSet(resultSetId);

    // resultset id can be invalid when the underlying table is gone
    if (!rs) {
        status = StatusInvalidResultSetId;
        goto CommonExit;
    }

    if (rowNum > 0) {
        status = rs->seek(rowNum);
        BailIfFailed(status);
    }

    json_t *entries;
    status = rs->getNext(1, &entries);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error at resultSetNext on table %s: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    jsonRecord = json_array_get(entries, 0);
    json_incref(jsonRecord);
    json_decref(entries);

    status = StatusOk;
CommonExit:
    if (rs) {
        ResultSetMgr::putResultSet(rs);
        rs = NULL;
    }

    if (resultSetId != XidInvalid) {
        ResultSetMgr::get()->freeResultSet(resultSetId);
        resultSetId = XidInvalid;
    }

    if (makeResultSetOutput != NULL) {
        memFree(makeResultSetOutput);
        makeResultSetOutput = NULL;
    }

    if (status != StatusOk) {
        if (jsonRecord != NULL) {
            json_decref(jsonRecord);
            jsonRecord = NULL;
        }
    }

    *jsonRecordOut = jsonRecord;

    return status;
}

static Status
getRandomVariable(RandWeakHandle *randHandle,
                  char variableName[XcalarApiMaxTableNameLen + 1])
{
    uint64_t x;
    x = rndWeakGenerate64(randHandle) % 100;
    if (x % 2 == 0) {
        // use the first aggResult
        AggregateResult *aggResult;

        aggregateHashTableLock.lock();
        AggregateHashTable::iterator it = aggregateHashTable.begin();
        aggResult = it.get();
        aggregateHashTableLock.unlock();

        if (aggResult != NULL) {
            snprintf(variableName,
                     XcalarApiMaxTableNameLen,
                     "%c%s",
                     OperatorsAggregateTag,
                     aggResult->output->tableName);
            return StatusOk;
        }
    }

    // just use a number
    snprintf(variableName, XcalarApiMaxTableNameLen, "%lu", x);

    return StatusOk;
}

static Status
getJsonColumn(const char *jsonKey,
              json_t *value,
              char *colName,
              size_t colNameLen,
              RandWeakHandle *randHandle)
{
    Status status = StatusOk;
    size_t ret;
    unsigned ii;
    ret = strlcpy(colName, jsonKey, colNameLen);
    if (ret >= colNameLen) {
        return StatusOverflow;
    }

    status = DataFormat::escapeNestedDelim(colName, colNameLen, &ret);
    if (status != StatusOk) {
        return status;
    }

    colName += ret;
    colNameLen -= ret;

    switch (json_typeof(value)) {
    case JSON_OBJECT: {
        size_t numCols = json_object_size(value);
        if (numCols == 0) {
            // this is an empty object
            return StatusInval;
        }

        ret = snprintf(colName, colNameLen, ".");
        if (ret >= colNameLen) {
            return StatusOverflow;
        }
        colName += ret;
        colNameLen -= ret;

        unsigned colIdx = rndWeakGenerate64(randHandle) % (numCols);
        void *iter = json_object_iter(value);

        for (ii = 0; ii < colIdx; ii++) {
            iter = json_object_iter_next(value, iter);
            assert(iter != NULL);
        }

        status = getJsonColumn(json_object_iter_key(iter),
                               json_object_iter_value(iter),
                               colName,
                               colNameLen,
                               randHandle);
        break;
    }
    case JSON_ARRAY: {
        size_t numCols = json_array_size(value);
        if (numCols == 0) {
            // this is an empty array
            return StatusInval;
        }

        unsigned colIdx = rndWeakGenerate64(randHandle) % (numCols);
        ret = snprintf(colName, colNameLen, "[%u]", colIdx);
        if (ret >= colNameLen) {
            return StatusOverflow;
        }

        break;
    }

    default:
        break;
    }

    return status;
}

static Status
getRandomColumn(Dag *sessionGraph,
                const char *tableName,
                char **colNameOut,
                RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;

    json_t *jsonRecord = NULL;
    void *jsonIter = NULL;

    char *colName = NULL;
    size_t numCols = 0;
    unsigned colIdx, ii;

    colName = (char *) memAlloc(XcalarApiMaxFieldNameLen + 1);
    if (colName == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = getRow(sessionGraph, tableName, &jsonRecord, 0, NULL);
    BailIfFailed(status);

    numCols = json_object_size(jsonRecord);

    if (numCols == 0) {
        xSyslog(moduleName, XlogErr, "Table %s has 0 columns", tableName);
        status = StatusInval;
        goto CommonExit;
    }

    colIdx = rndWeakGenerate64(randHandle) % (numCols);
    ii = 0;
    for (jsonIter = json_object_iter(jsonRecord); jsonIter != NULL;
         jsonIter = json_object_iter_next(jsonRecord, jsonIter)) {
        if (ii == colIdx) {
            status = getJsonColumn(json_object_iter_key(jsonIter),
                                   json_object_iter_value(jsonIter),
                                   colName,
                                   XcalarApiMaxFieldNameLen + 1,
                                   randHandle);
            break;
        }
        ii++;
    }

CommonExit:
    if (status != StatusOk) {
        if (colName != NULL) {
            memFree(colName);
            colName = NULL;
        }
    }
    *colNameOut = colName;

    if (jsonRecord != NULL) {
        json_decref(jsonRecord);
        jsonRecord = NULL;
    }

    return status;
}

Status
getRowCount(Dag *sessionGraph, const char *tableName, uint64_t *rowCount)
{
    DagTypes::NamedInput getTableMetaInput;
    getTableMetaInput.isTable = true;
    getTableMetaInput.nodeId = getXidFromName(tableName);
    return Operators::get()->getRowCount(&getTableMetaInput,
                                         sessionGraph->getId(),
                                         rowCount,
                                         &testUserId);
}

Status
getDatasetRowCount(Dag *sessionGraph,
                   const char *datasetName,
                   uint64_t *rowCount)
{
    Dataset *ds = Dataset::get();
    DagTypes::NamedInput getTableMetaInput;
    Status status = StatusUnknown;
    DsDataset *dataset = NULL;
    DatasetRefHandle dsRefHandle;

    status = ds->openHandleToDatasetByName(datasetName,
                                           &testUserId,
                                           &dataset,
                                           LibNsTypes::ReaderShared,
                                           &dsRefHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error retrieivng dataset for \"%s\": %s",
                datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    getTableMetaInput.xid = dataset->getDatasetId();
    getTableMetaInput.isTable = false;

    status = Operators::get()->getRowCount(&getTableMetaInput,
                                           sessionGraph->getId(),
                                           rowCount,
                                           &testUserId);
CommonExit:
    if (dataset != NULL) {
        Status status2;
        status2 = ds->closeHandleToDataset(&dsRefHandle);
        if (status == StatusOk) {
            status = status2;
        }
    }
    return status;
}

static Status
getRandomColumns(Dag *sessionGraph,
                 const char *tableName,
                 RandWeakHandle *randHandle,
                 char (**columnsOut)[XcalarApiMaxFieldNameLen + 1],
                 DfFieldType **columTypesOut,
                 int *numColsOut,
                 uint32_t pctChanceOfPickingCol)
{
    Status status = StatusUnknown;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    DfFieldType *columnTypes = NULL;
    json_t *jsonRecord = NULL;
    void *jsonIter;
    size_t numColsTotal = 0;
    int numCols = 0;
    uint64_t numRows, ii, rowIdx;
    XcalarApiGetTableMetaOutput *meta = NULL;

    meta = new (std::nothrow) XcalarApiGetTableMetaOutput();
    BailIfNull(meta);

    status = getRowCount(sessionGraph, tableName, &numRows);
    BailIfFailed(status);

    if (numRows == 0) {
        status = StatusFunctionalTestTableEmpty;
        xSyslog(moduleName,
                XlogInfo,
                "No valid columns as table %s is empty",
                tableName);
        goto CommonExit;
    }

    // get a row with some columns in it. We limit this to
    // MaxRowsToCheckForCols number of rows. This is to avoid
    // cases where we just spin here looking for valid columns
    // in a large (mostly) empty table.
    for (ii = 0; ii < MaxRowsToCheckForCols; ii++) {
        if (jsonRecord != NULL) {
            json_decref(jsonRecord);
            jsonRecord = NULL;
        }

        rowIdx = rndWeakGenerate64(randHandle) % (numRows);
        status = getRow(sessionGraph, tableName, &jsonRecord, rowIdx, meta);
        BailIfFailed(status);

        // First get total number of columns in a row
        numColsTotal = json_object_size(jsonRecord);
        if (numColsTotal > 0) {
            break;
        }
    }

    if (numColsTotal == 0) {
        status = StatusFunctionalTestTableEmpty;
        xSyslog(moduleName,
                XlogInfo,
                "Could not find valid columns for table %s",
                tableName);
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Found valid columns for table %s in %lu iterations",
            tableName,
            ii);

    // Allocate for the worst case
    columns = (char(*)[XcalarApiMaxFieldNameLen + 1])
        memAlloc(sizeof(*columns) * numColsTotal);
    if (columns == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate columns "
                "(NumCols: %lu)",
                numColsTotal);
        goto CommonExit;
    }

    columnTypes = (DfFieldType *) memAlloc(sizeof(*columnTypes) * numColsTotal);
    BailIfNull(columnTypes);

    numCols = 0;

    for (jsonIter = json_object_iter(jsonRecord); jsonIter != NULL;
         jsonIter = json_object_iter_next(jsonRecord, jsonIter)) {
        if ((rndWeakGenerate64(randHandle) % 100) < pctChanceOfPickingCol) {
            status = getJsonColumn(json_object_iter_key(jsonIter),
                                   json_object_iter_value(jsonIter),
                                   columns[numCols],
                                   sizeof(columns[numCols]),
                                   randHandle);
            BailIfFailed(status);

            if (strstr(columns[numCols], DfFatptrPrefixDelimiter)) {
                columnTypes[numCols] = DfFatptr;
            } else {
                char unescapedColumn[XcalarApiMaxFieldNameLen + 1];
                strcpy(unescapedColumn, columns[numCols]);

                DataFormat::unescapeNestedDelim(unescapedColumn);

                for (ii = 0; ii < meta->numValues; ii++) {
                    if (meta->valueAttrs[ii].type != DfFatptr &&
                        strcmp(unescapedColumn, meta->valueAttrs[ii].name) ==
                            0) {
                        columnTypes[numCols] = meta->valueAttrs[ii].type;
                        assert(isValidDfFieldType(columnTypes[numCols]));
                    }
                }
            }

            numCols++;
        }
    }
CommonExit:
    if (status != StatusOk) {
        if (columns != NULL) {
            memFree(columns);
            columns = NULL;
        }
        if (columnTypes != NULL) {
            memFree(columnTypes);
            columnTypes = NULL;
        }
        numCols = 0;
    }

    if (jsonRecord != NULL) {
        json_decref(jsonRecord);
        jsonRecord = NULL;
    }

    *numColsOut = numCols;
    *columnsOut = columns;

    if (columTypesOut == NULL) {
        if (columnTypes != NULL) {
            memFree(columnTypes);
            columnTypes = NULL;
        }
    } else {
        *columTypesOut = columnTypes;
    }

    if (meta) {
        delete meta;
        meta = NULL;
    }
    return status;
}

static Status
workerLoad(XcalarApiUserId *userId,
           Dag *sessionGraph,
           Atomic64 *loadJobRan,
           RandWeakHandle *randHandle)
{
    Status status;
    char user[LOGIN_NAME_MAX + 1];
    char datasetName[XcalarApiMaxTableNameLen + 1];
    size_t ret;
    char newDatasetName[sizeof(datasetName) + strlen(XcalarApiDatasetPrefix) +
                        1];

    ret = snprintf(user, sizeof(user), "ds-%s", userId->userIdName);
    assert(ret < sizeof(user));

    status = getUniqueName(user, datasetName, sizeof(datasetName));
    BailIfFailed(status);

    switch (atomicInc64(loadJobRan) % LoadMaxRandomArgs) {
    case LoadDefault:
        status = load(userId,
                      sessionGraph,
                      DatasetPath,
                      DatasetType,
                      DatasetSize,
                      datasetName,
                      LoadMemUdfs::LoadMemInvalid,
                      NULL);
        break;

    case LoadJson:
        status = load(userId,
                      sessionGraph,
                      YelpDatasetPath,
                      YelpDatasetType,
                      YelpDatasetSize,
                      datasetName,
                      LoadMemUdfs::LoadMemInvalid,
                      NULL);
        break;

    case LoadMemory:
        char memoryDatasetPath[40];
        uint64_t expectedRowCount;
        expectedRowCount = (rndWeakGenerate64(randHandle) % 3000) + 1000;
        snprintf(memoryDatasetPath,
                 sizeof(memoryDatasetPath),
                 "%s%lu",
                 MemoryPrefix,
                 expectedRowCount);
        status = load(userId,
                      sessionGraph,
                      memoryDatasetPath,
                      MemoryDatasetType,
                      MemoryDatasetSize,
                      datasetName,
                      LoadMemUdfs::LoadMemDefault,
                      NULL);
        if (status == StatusOk) {
            snprintf(newDatasetName,
                     sizeof(newDatasetName),
                     "%s%s",
                     XcalarApiDatasetPrefix,
                     datasetName);
            uint64_t rowCount;
            status =
                getDatasetRowCount(sessionGraph, newDatasetName, &rowCount);
            if (status == StatusOk) {
                assert(rowCount == expectedRowCount);
            }
        }
        break;

    default:
        assert(0);
        break;
    }

CommonExit:
    return status;
}

static Status
workerMap(XcalarApiUserId *userId,
          Dag *sessionGraph,
          const char *tableName,
          Atomic64 *mapJobRan,
          RandWeakHandle *randHandle)
{
    char *colName = NULL, *colName2 = NULL;
    unsigned numEvals = rndWeakGenerate64(randHandle) % MaxMapEvals + 1;
    char filterString[XcalarApiMaxEvalStringLen + 1];
    char dstTable1[XcalarApiMaxTableNameLen + 1];
    char newFieldNames1[numEvals][XcalarApiMaxFieldNameLen + 1];
    char dstTable2[XcalarApiMaxTableNameLen + 1];
    char newFieldNames2[numEvals][XcalarApiMaxFieldNameLen + 1];

    char filterTable[XcalarApiMaxTableNameLen + 1];

    char icvCol[XcalarApiMaxFieldNameLen + 1];
    char icvTable[XcalarApiMaxTableNameLen + 1];

    uint64_t rowCountIcvTable;

    char evalStrings[numEvals][XcalarApiMaxEvalStringLen + 1];
    Status status = StatusUnknown;
    size_t ret;

    for (unsigned ii = 0; ii < numEvals; ii++) {
        char variable[XcalarApiMaxTableNameLen + 1];

        status = getRandomColumn(sessionGraph, tableName, &colName, randHandle);
        BailIfFailed(status);

        status =
            getRandomColumn(sessionGraph, tableName, &colName2, randHandle);
        BailIfFailed(status);

        status = getRandomVariable(randHandle, variable);
        BailIfFailed(status);

        status = getUniqueName("col",
                               newFieldNames1[ii],
                               sizeof(newFieldNames1[ii]));
        BailIfFailed(status);

        status = getUniqueName("col",
                               newFieldNames2[ii],
                               sizeof(newFieldNames2[ii]));
        BailIfFailed(status);

        char *field1 = colName;
        char *field2;
        if (atomicInc64(mapJobRan) % 2) {
            field2 = colName2;
        } else {
            field2 = variable;
        }

        switch (atomicInc64(mapJobRan) % MapMaxRandomArgs) {
        case MapAdd: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "add(float(%s), float(%s))",
                           field1,
                           field2);
            if (ret >= sizeof(evalStrings[ii])) {
                status = StatusNoBufs;
                xSyslog(moduleName,
                        XlogErr,
                        "Eval string add(float(%s), float(%s)) "
                        "is too long",
                        field1,
                        field2);
                goto CommonExit;
            }
            break;
        }

        case MapAddNumeric: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "add(numeric(%s), numeric(%s))",
                           field1,
                           field2);
            if (ret >= sizeof(evalStrings[ii])) {
                status = StatusNoBufs;
                xSyslog(moduleName,
                        XlogErr,
                        "Eval string add(numeric(%s), numeric(%s)) "
                        "is too long",
                        field1,
                        field2);
                goto CommonExit;
            }
            break;
        }

        case MapConcat: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "concat(string(%s), string(%s))",
                           field1,
                           field2);
            if (ret >= sizeof(evalStrings[ii])) {
                status = StatusNoBufs;
                xSyslog(moduleName,
                        XlogErr,
                        "Eval string concat(string(%s), string(%s)) "
                        "is too long",
                        field1,
                        field2);
                goto CommonExit;
            }
            break;
        }

        case MapExplode: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "explodeString(string(%s), \".\")",
                           field1);
            break;
        }

        case MapTimestamp: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "timestamp(%s)",
                           field1);
            break;
        }

        case MapNumeric: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "numeric(%s)",
                           field1);
            break;
        }

        case MapUdf: {
            ret = snprintf(evalStrings[ii],
                           sizeof(evalStrings[ii]),
                           "default:multiJoin(%s, %s)",
                           field1,
                           field2);
            if (ret >= sizeof(evalStrings[ii])) {
                status = StatusNoBufs;
                xSyslog(moduleName,
                        XlogErr,
                        "Eval string default:multiJoin(%s, %s) "
                        "is too long",
                        field1,
                        field2);
                goto CommonExit;
            }

            break;
        }

        default:
            assert(0);
            goto CommonExit;
            break;
        }

        memFree(colName);
        colName = NULL;
        memFree(colName2);
        colName2 = NULL;
    }

    // do the map twice and check that they produce the same result
    status = map(userId,
                 sessionGraph,
                 tableName,
                 numEvals,
                 evalStrings,
                 newFieldNames1,
                 false,
                 dstTable1);
    BailIfFailed(status);

    status = map(userId,
                 sessionGraph,
                 dstTable1,
                 numEvals,
                 evalStrings,
                 newFieldNames2,
                 false,
                 dstTable2);
    BailIfFailed(status);

    // If explodeString is one of the evals, then skip
    // the row count check. This is because doing explodeString
    // map operation twice on the same column will not yield tables
    // with same column count
    for (unsigned ii = 0; ii < numEvals; ii++) {
        if (strstr(evalStrings[ii], "explodeString")) {
            goto CommonExit;
        }
    }

    char *dstCol1;
    char *dstCol2;

    // pick one of the evaluations at random to check
    int idx;
    idx = rndWeakGenerate64(randHandle) % numEvals;

    dstCol1 = newFieldNames1[idx];
    dstCol2 = newFieldNames2[idx];

    Ordering ordering;
    ordering = Ascending;

    status = indexTable(userId,
                        sessionGraph,
                        dstTable1,
                        1,
                        (const char **) &dstCol1,
                        &ordering,
                        dstTable1);
    BailIfFailed(status);

    status = indexTable(userId,
                        sessionGraph,
                        dstTable2,
                        1,
                        (const char **) &dstCol2,
                        &ordering,
                        dstTable2);
    BailIfFailed(status);

    snprintf(filterString, sizeof(filterString), "eq(%s,%s)", dstCol1, dstCol2);
    status = filter(userId, sessionGraph, dstTable2, filterString, filterTable);
    BailIfFailed(status);

    uint64_t rowCountMapTable, rowCountFilterTable;

    status = getRowCount(sessionGraph, dstTable2, &rowCountMapTable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTable2,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = getRowCount(sessionGraph, filterTable, &rowCountFilterTable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                filterTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // If they're not equal and we're doing floating-point math, we may have NaN
    // rows.
    if (rowCountMapTable != rowCountFilterTable &&
        strncmp(evalStrings[idx], "add(float", 9) == 0) {
        char nanTable1[XcalarApiMaxFieldNameLen + 1];
        char nanTable2[XcalarApiMaxFieldNameLen + 1];
        char nanFilterString1[XcalarApiMaxFieldNameLen + 1];
        char nanFilterString2[XcalarApiMaxFieldNameLen + 1];
        uint64_t nanRowCount1;
        uint64_t nanRowCount2;

        snprintf(nanFilterString1,
                 sizeof(nanFilterString1),
                 "isNan(%s)",
                 dstCol1);
        snprintf(nanFilterString2,
                 sizeof(nanFilterString2),
                 "isNan(%s)",
                 dstCol2);
        status = filter(userId,
                        sessionGraph,
                        dstTable1,
                        nanFilterString1,
                        nanTable1);
        BailIfFailed(status);

        status = filter(userId,
                        sessionGraph,
                        dstTable2,
                        nanFilterString2,
                        nanTable2);
        BailIfFailed(status);

        status = getRowCount(sessionGraph, nanTable1, &nanRowCount1);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting row count for NaN table %s: %s",
                    nanTable1,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = getRowCount(sessionGraph, nanTable2, &nanRowCount2);
        BailIfFailed(status);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting row count for NaN table %s: %s",
                    nanTable2,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (nanRowCount1 == nanRowCount2) {
            rowCountFilterTable += nanRowCount1;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "NaN row counts do not match: %lu/%lu",
                    nanRowCount1,
                    nanRowCount2);
        }
    }
    // number of rows should remain unchanged
    if (rowCountMapTable != rowCountFilterTable) {
        status = getUniqueName("col", icvCol, sizeof(icvCol));
        BailIfFailed(status);

        // there may have been a type error or something, check the icv
        status = map(userId,
                     sessionGraph,
                     dstTable2,
                     1,
                     &filterString,
                     &icvCol,
                     true,
                     icvTable);
        BailIfFailed(status);
        status = getRowCount(sessionGraph, icvTable, &rowCountIcvTable);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error getting row count for %s: %s",
                    icvTable,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // include the errored rows
        rowCountFilterTable += rowCountIcvTable;
    }

    if (rowCountMapTable != rowCountFilterTable) {
        char column[XcalarApiMaxFieldNameLen + 1];
        char exportTable1[XcalarApiMaxFieldNameLen + 1];
        char exportTable2[XcalarApiMaxFieldNameLen + 1];

        xSyslog(moduleName,
                XlogErr,
                "Tables do not match.  %lu of %lu rows match.",
                rowCountFilterTable,
                rowCountMapTable);

        strlcpy(column, dstCol1, sizeof(column));
        getUniqueName("mapTable1", exportTable1, sizeof(exportTable1));
        strlcat(exportTable1, ".csv", sizeof(exportTable1));

        getUniqueName("mapTable2", exportTable2, sizeof(exportTable2));
        strlcat(exportTable2, ".csv", sizeof(exportTable2));

        status = exportTable(userId,
                             sessionGraph,
                             dstTable2,
                             1,
                             &column,
                             exportTable1);

        strlcpy(column, dstCol2, sizeof(column));
        status = exportTable(userId,
                             sessionGraph,
                             dstTable2,
                             1,
                             &column,
                             exportTable2);

        char retinaName[XcalarApiMaxFileNameLen + 1];
        getUniqueName("retina-", retinaName, sizeof(retinaName));
        status =
            exportTableAsRetina(userId, sessionGraph, filterTable, retinaName);

        assert(0);
    }

CommonExit:
    if (colName != NULL) {
        memFree(colName);
        colName = NULL;
    }

    return status;
}

static Status
createJoinRenameMap(Dag *sessionGraph,
                    const char *leftTable,
                    const char *rightTable,
                    RandWeakHandle *randHandle,
                    unsigned *numLeftColumnsOut,
                    unsigned *numRightColumnsOut,
                    XcalarApiRenameMap **renameMapOut)
{
    Status status = StatusUnknown;
    XcalarApiMakeResultSetOutput **makeResultSetOutputs = NULL;
    unsigned numLeftColumns = 0;
    unsigned numRightColumns = 0;
    unsigned totalNumColumns = 0;
    const char *tables[] = {leftTable, rightTable};
    unsigned *numColumns[] = {&numLeftColumns, &numRightColumns};
    uint64_t *resultSetIds = NULL;
    unsigned ii, jj, kk;
    XcalarApiRenameMap *renameMap = NULL;
    XcalarApiUdfContainer *sessionContainer =
        sessionGraph->getSessionContainer();

    assertStatic(ArrayLen(tables) == ArrayLen(numColumns));

    resultSetIds =
        (uint64_t *) memAlloc(sizeof(*resultSetIds) * ArrayLen(tables));
    if (resultSetIds == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate resultSetIds. "
                "(NumTables: %lu)",
                ArrayLen(tables));
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < ArrayLen(tables); ii++) {
        resultSetIds[ii] = XidInvalid;
    }

    makeResultSetOutputs = (XcalarApiMakeResultSetOutput **) memAlloc(
        sizeof(*makeResultSetOutputs) * ArrayLen(tables));
    if (makeResultSetOutputs == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate "
                "makeResultSetOutputs. (NumTables: %lu)",
                ArrayLen(tables));
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < ArrayLen(tables); ii++) {
        makeResultSetOutputs[ii] = NULL;
    }

    XcalarApiGetTableMetaInput getTableMetaInput;
    size_t outputSize;
    XcalarApiOutput *apiOut;
    totalNumColumns = 0;
    for (ii = 0; ii < ArrayLen(tables); ii++) {
        getTableMetaInput.tableNameInput.isTable = true;
        getTableMetaInput.tableNameInput.nodeId = getXidFromName(tables[ii]);
        getTableMetaInput.isPrecise = false;
        status = Operators::get()->getTableMeta(&getTableMetaInput,
                                                &apiOut,
                                                &outputSize,
                                                sessionGraph->getId(),
                                                &testUserId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting table meta for %s: %s",
                    tables[ii],
                    strGetFromStatus(status));
            goto CommonExit;
        }

        makeResultSetOutputs[ii] = (XcalarApiMakeResultSetOutput *) memAlloc(
            XcalarApiSizeOfOutput(*(makeResultSetOutputs[ii])) + outputSize +
            XdbMgr::bcSize());
        if (makeResultSetOutputs[ii] == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate "
                    "resultSetOutput");
            status = StatusNoMem;
            memFree(apiOut);
            goto CommonExit;
        }

        memcpy(&makeResultSetOutputs[ii]->metaOutput,
               &apiOut->outputResult.getTableMetaOutput,
               sizeof(apiOut->outputResult.getTableMetaOutput));
        memFree(apiOut);

        status = ResultSetMgr::get()->makeResultSet(sessionGraph,
                                                    tables[ii],
                                                    false,
                                                    makeResultSetOutputs[ii],
                                                    &sessionContainer->userId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error creating result set during "
                    "createJoinRenameMap for %s: %s",
                    tables[ii],
                    strGetFromStatus(status));
        }

        resultSetIds[ii] = makeResultSetOutputs[ii]->resultSetId;
        totalNumColumns += makeResultSetOutputs[ii]->metaOutput.numValues;
        assert(makeResultSetOutputs[ii]->metaOutput.numValues <=
                   TupleMaxNumValuesPerRecord &&
               "numValues of makeResultSetOutputs is corrupt");
    }

    renameMap =
        (XcalarApiRenameMap *) memAlloc(sizeof(*renameMap) * totalNumColumns);
    if (renameMap == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate renameMap. "
                "(totalNumColumns: %u)",
                totalNumColumns);
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(renameMap, sizeof(*renameMap) * totalNumColumns);

    kk = 0;
    for (ii = 0; ii < ArrayLen(tables); ii++) {
        for (jj = 0; jj < makeResultSetOutputs[ii]->metaOutput.numValues;
             jj++) {
            DfFieldAttrHeader *valueAttrs;
            valueAttrs = makeResultSetOutputs[ii]->metaOutput.valueAttrs;

            renameMap[kk].type = valueAttrs[jj].type;
            strlcpy(renameMap[kk].oldName,
                    valueAttrs[jj].name,
                    sizeof(renameMap[kk].oldName));
            if (valueAttrs[jj].type == DfFatptr) {
                status = getUniqueName("dsPrefix",
                                       renameMap[kk].newName,
                                       sizeof(renameMap[kk].newName));
            } else {
                status = getUniqueName("col",
                                       renameMap[kk].newName,
                                       sizeof(renameMap[kk].newName));
            }
            BailIfFailed(status);
            kk++;
        }
        *numColumns[ii] = makeResultSetOutputs[ii]->metaOutput.numValues;
    }

    assert(kk == totalNumColumns);

    if (totalNumColumns > 0) {
        status = StatusOk;
    } else {
        status = StatusFunctionalTestTableEmpty;
    }
CommonExit:
    if (status != StatusOk) {
        if (renameMap != NULL) {
            memFree(renameMap);
            renameMap = NULL;
        }
        numLeftColumns = 0;
        numRightColumns = 0;
    }

    if (resultSetIds != NULL) {
        for (ii = 0; ii < ArrayLen(tables); ii++) {
            if (resultSetIds[ii] != XidInvalid) {
                ResultSetMgr::get()->freeResultSet(resultSetIds[ii]);
                resultSetIds[ii] = XidInvalid;
            }
        }
        memFree(resultSetIds);
        resultSetIds = NULL;
    }

    if (makeResultSetOutputs != NULL) {
        for (ii = 0; ii < ArrayLen(tables); ii++) {
            if (makeResultSetOutputs[ii] != NULL) {
                memFree(makeResultSetOutputs[ii]);
                makeResultSetOutputs[ii] = NULL;
            }
        }
        memFree(makeResultSetOutputs);
        makeResultSetOutputs = NULL;
    }

    *numLeftColumnsOut = numLeftColumns;
    *numRightColumnsOut = numRightColumns;
    *renameMapOut = renameMap;

    return status;
}

static Status
workerChangeRuntimeParams(XcalarApiUserId *userId,
                          Dag *sessionGraph,
                          RandWeakHandle *randHandle,
                          bool setDefault = false)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    const char *schedName[Runtime::TotalSdkScheds];
    uint32_t cpuReservedPct[Runtime::TotalSdkScheds];
    RuntimeType rtType[Runtime::TotalSdkScheds];
    Txn currentTxn, newTxn;

    memZero(schedName, sizeof(schedName));
    memZero(cpuReservedPct, sizeof(cpuReservedPct));
    memset(rtType, (int) RuntimeTypeInvalid, sizeof(rtType));

    for (uint32_t ii = 0; ii < Runtime::TotalSdkScheds; ii++) {
        switch (static_cast<Runtime::SchedId>(ii)) {
        case Runtime::SchedId::Sched0:
            schedName[ii] = Runtime::NameSchedId0;
            if (setDefault) {
                cpuReservedPct[ii] = Runtime::MaxCpusReservedInPct;
            } else {
                cpuReservedPct[ii] = (rndWeakGenerate64(randHandle) %
                                      Runtime::MaxCpusReservedInPct) +
                                     1;
            }
            rtType[ii] = RuntimeType::RuntimeTypeThroughput;
            break;
        case Runtime::SchedId::Sched1:
            schedName[ii] = Runtime::NameSchedId1;
            if (setDefault) {
                cpuReservedPct[ii] = Runtime::MaxCpusReservedInPct;
                rtType[ii] = RuntimeType::RuntimeTypeLatency;
            } else {
                cpuReservedPct[ii] = (rndWeakGenerate64(randHandle) %
                                      Runtime::MaxCpusReservedInPct) +
                                     1;
                rtType[ii] = (rndWeakGenerate64(randHandle) % 2)
                                 ? RuntimeType::RuntimeTypeThroughput
                                 : RuntimeType::RuntimeTypeLatency;
            }
            break;
        case Runtime::SchedId::Sched2:
            schedName[ii] = Runtime::NameSchedId2;
            if (setDefault) {
                cpuReservedPct[ii] = Runtime::MaxCpusReservedInPct;
            } else {
                cpuReservedPct[ii] = (rndWeakGenerate64(randHandle) %
                                      Runtime::MaxCpusReservedInPct) +
                                     1;
            }
            rtType[ii] = RuntimeType::RuntimeTypeLatency;
            break;
        case Runtime::SchedId::Immediate:
            schedName[ii] = Runtime::NameImmediate;
            if (setDefault) {
                cpuReservedPct[ii] = Runtime::MaxImmediateCpusReservedInPct;
            } else {
                // Runtime::SchedId::Immediate threads cannot be decreased.
                cpuReservedPct[ii] = Runtime::MaxImmediateCpusReservedInPct;
            }
            rtType[ii] = RuntimeType::RuntimeTypeImmediate;
            break;
        default:
            assert(0 && "Unknown Type");
            break;
        }
    }

    char paramVal[32];
    char paramName[128];
    snprintf(paramName, sizeof(paramName), "RuntimeMixedModeMinCores");
    snprintf(paramVal,
             sizeof(paramVal),
             "%u",
             (unsigned) XcSysHelper::get()->getNumOnlineCores());

    workItem = xcalarApiMakeSetConfigParamWorkItem(paramName, paramVal);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating set config workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "setting config param '%s' to '%s' failed, status: %s",
                paramName,
                paramVal,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xcalarApiFreeWorkItem(workItem);

    workItem = xcalarApiMakeRuntimeSetParamWorkItem(schedName,
                                                    cpuReservedPct,
                                                    rtType,
                                                    Runtime::TotalSdkScheds);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error creating runtime set param workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    // WorkItem needs to be scheduled on the immediate threads to change a
    // scheduler's thread count.
    currentTxn = Txn::currentTxn();
    newTxn = Txn::newImmediateTxn();
    Txn::setTxn(newTxn);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        Txn::setTxn(currentTxn);
        xSyslog(moduleName,
                XlogErr,
                "runtime set param failed, status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    Txn::setTxn(currentTxn);
    xSyslog(moduleName, XlogInfo, "Runtime set param succeeded");

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    return status;
}

static Status
workerJoin(XcalarApiUserId *userId,
           Dag *sessionGraph,
           const char *tableName1,
           const char *tableName2,
           RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    const char *leftTable = NULL;
    const char *rightTable = NULL;
    bool retry = false;
    unsigned numLeftColumns = 0;
    unsigned numRightColumns = 0;
    XcalarApiRenameMap *renameMap = NULL;

    leftTable = tableName1;
    rightTable = tableName2;

    for (int joinTypeInt = 0; joinTypeInt < JoinOperatorLen; joinTypeInt++) {
        const JoinOperator joinType = (JoinOperator) joinTypeInt;
        assert(isValidJoinOperator(joinType));

        do {
            retry = false;
            status = join(userId,
                          sessionGraph,
                          leftTable,
                          rightTable,
                          numLeftColumns,
                          numRightColumns,
                          renameMap,
                          (JoinOperator) joinType);
            if (status == StatusOk) {
                // It succeeded on the first try!
                break;
            }

            // Ok why did it fail?
            switch (status.code()) {
            case StatusCodeImmediateNameCollision:
            case StatusCodeFatptrPrefixCollision:
                assert(renameMap == NULL &&
                       "renameMap did not fix name collision!");
                status = createJoinRenameMap(sessionGraph,
                                             tableName1,
                                             tableName2,
                                             randHandle,
                                             &numLeftColumns,
                                             &numRightColumns,
                                             &renameMap);
                BailIfFailed(status);

                if (renameMap != NULL) {
                    retry = true;
                }
                break;
            default:
                goto CommonExit;
            }
        } while (retry);
    }

    status = StatusOk;
CommonExit:
    if (renameMap != NULL) {
        memFree(renameMap);
        renameMap = NULL;
    }

    return status;
}

static Status
workerUnion(XcalarApiUserId *userId,
            Dag *sessionGraph,
            const char *tableName1,
            const char *tableName2,
            RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    int numCols = 0;
    DfFieldType *colTypes = NULL;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    unsigned numSrcTables = 2;
    const char *srcTables[numSrcTables];
    char indexTables[numSrcTables][XcalarApiMaxTableNameLen + 1];
    unsigned renameMapSizes[numSrcTables];
    XcalarApiRenameMap *renameMap[numSrcTables];
    srcTables[0] = tableName1;
    srcTables[1] = tableName2;

    memZero(renameMap, numSrcTables * sizeof(*renameMap));

    // XXX TODO need to test more than 2 source table unions.
    // loop over dedup options
    for (int ii = 0; ii < 2; ii++) {
        bool dedup = (ii != 0);

        for (int utype = 0; utype < UnionOperatorLen; utype++) {
            // retrieve columns for each src table
            for (unsigned ii = 0; ii < numSrcTables; ii++) {
                if (renameMap[ii] != NULL) {
                    memFree(renameMap[ii]);
                    renameMap[ii] = NULL;
                }
            }
            for (unsigned ii = 0; ii < numSrcTables; ii++) {
                const char *src;
                if (ii == 0) {
                    src = tableName1;
                } else {
                    src = tableName2;
                }

                status = getRandomColumns(sessionGraph,
                                          src,
                                          randHandle,
                                          &columns,
                                          &colTypes,
                                          &numCols,
                                          PctChanceOfColExport);
                BailIfFailed(status);

                int numColsFinal = 0;
                for (int jj = 0; jj < numCols; jj++) {
                    // union works with unescaped columns
                    DataFormat::unescapeNestedDelim(columns[jj]);

                    char *prefixPtr =
                        strstr(columns[jj], DfFatptrPrefixDelimiter);
                    if (prefixPtr) {
                        // Skip prefixed columns, since rename map needs to
                        // strictly have only derived type.
                        continue;
                    }

                    int kk;
                    for (kk = 0; kk < numColsFinal; kk++) {
                        if (strcmp(columns[kk], columns[jj]) == 0) {
                            break;
                        }
                    }

                    if (kk == numColsFinal) {
                        // found a new column
                        if (numColsFinal != jj) {
                            strcpy(columns[numColsFinal], columns[jj]);
                            colTypes[numColsFinal] = colTypes[jj];
                        }

                        numColsFinal++;
                    }
                }

                numCols = numColsFinal;
                if (numCols == 0) {
                    // Could not find any suitable column
                    goto CommonExit;
                }

                renameMapSizes[ii] = numCols;
                renameMap[ii] = (XcalarApiRenameMap *) memAlloc(
                    numCols * sizeof(*renameMap[0]));
                BailIfNull(renameMap);
                memZero(renameMap[ii], numCols * sizeof(*renameMap[0]));

                for (int jj = 0; jj < numCols; jj++) {
                    strcpy(renameMap[ii][jj].oldName, columns[jj]);
                    strcpy(renameMap[ii][jj].newName, columns[jj]);
                    renameMap[ii][jj].type = colTypes[jj];
                    renameMap[ii][jj].isKey = false;
                    assert(isValidDfFieldType(colTypes[jj]));
                }

                memFree(columns);
                columns = NULL;

                memFree(colTypes);
                colTypes = NULL;
            }

            // match the columns in the first map with something of the same
            // type in the second, keep track of the columns you want to union
            unsigned numKeys = 0;
            const char *unionKeys[renameMapSizes[0]];

            for (unsigned ii = 0; ii < renameMapSizes[0]; ii++) {
                const char *dstName = renameMap[0][ii].newName;
                DfFieldType dstType = renameMap[0][ii].type;

                for (unsigned jj = 0; jj < renameMapSizes[1]; jj++) {
                    if (dstType == renameMap[1][jj].type) {
                        // found a matching type, union the 2 columns by giving
                        // them the same name
                        strcpy(renameMap[1][jj].newName, dstName);
                        unsigned kk;
                        for (kk = 0; kk < numKeys; kk++) {
                            if (strcmp(unionKeys[kk], dstName) == 0) {
                                break;
                            }
                        }
                        if (kk == numKeys) {
                            unionKeys[numKeys++] = dstName;
                        }
                        break;
                    }
                }
            }

            if (dedup || utype != UnionStandard) {
                if (numKeys == 0) {
                    continue;
                }

                for (unsigned ii = 0; ii < numSrcTables; ii++) {
                    // trim the rename map to only include the keys and index
                    const char *srcKeys[numKeys];
                    unsigned cols = 0;
                    for (unsigned jj = 0; jj < renameMapSizes[ii]; jj++) {
                        for (unsigned kk = 0; cols < numKeys && kk < numKeys;
                             kk++) {
                            if (strcmp(renameMap[ii][jj].newName,
                                       unionKeys[kk]) == 0) {
                                // this column is a key
                                renameMap[ii][cols] = renameMap[ii][jj];
                                srcKeys[cols] = renameMap[ii][cols].oldName;
                                cols++;
                                break;
                            }
                        }
                    }

                    if (cols == 0) {
                        status = StatusInval;
                        goto CommonExit;
                    }

                    renameMapSizes[ii] = cols;

                    // index on the keys
                    Ordering orderings[cols];
                    for (unsigned ii = 0; ii < cols; ii++) {
                        orderings[ii] = Unordered;
                    }

                    status = indexTable(userId,
                                        sessionGraph,
                                        srcTables[ii],
                                        cols,
                                        srcKeys,
                                        orderings,
                                        indexTables[ii]);
                    BailIfFailed(status);

                    srcTables[ii] = indexTables[ii];
                }
            }

            status = doUnion(userId,
                             sessionGraph,
                             numSrcTables,
                             srcTables,
                             NULL,
                             renameMapSizes,
                             renameMap,
                             dedup,
                             (UnionOperator) utype);
            BailIfFailed(status);
        }
    }

    status = StatusOk;
CommonExit:
    for (unsigned ii = 0; ii < numSrcTables; ii++) {
        if (renameMap[ii] != NULL) {
            memFree(renameMap[ii]);
            renameMap[ii] = NULL;
        }
    }

    if (columns) {
        memFree(columns);
        columns = NULL;
    }
    if (colTypes) {
        memFree(colTypes);
        colTypes = NULL;
    }

    return status;
}

static Status
cancelOperations(XcalarApiUserId *userId,
                 Dag *sessionGraph,
                 RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiListDagNodesOutput *listTablesOutput = NULL;
    size_t outputSize;
    unsigned ii, jj, numCancelled = 0, numFailed = 0;
    SourceType srcTypes[] = {SrcTable, SrcDataset, SrcConstant};
    LibNsTypes::PathInfoOut *pathInfo = NULL;

    for (jj = 0; jj < ArrayLen(srcTypes); jj++) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            listTablesOutput = NULL;
        }

        status = sessionGraph->listDagNodeInfo("*",
                                               &output,
                                               &outputSize,
                                               srcTypes[jj],
                                               &testUserId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error with dgListDagNodeInfo: %s (srcType: %s)",
                    strGetFromStatus(status),
                    strGetFromSourceType(srcTypes[jj]));
            goto CommonExit;
        }

        listTablesOutput = &output->outputResult.listNodesOutput;
        if (listTablesOutput->numNodes == 0) {
            status = StatusFunctionalTestNoTablesLeft;
            xSyslog(moduleName, XlogErr, "No tables found in session");
            goto CommonExit;
        }

        for (ii = 0; ii < listTablesOutput->numNodes; ii++) {
            if (listTablesOutput->nodeInfo[ii].state == DgDagStateProcessing &&
                ((rndWeakGenerate64(randHandle) % 100) < PctChanceOfCancel)) {
                status = cancelOp(userId,
                                  sessionGraph,
                                  listTablesOutput->nodeInfo[ii].name);
                if (status == StatusOk) {
                    numCancelled++;
                } else {
                    numFailed++;
                }
            }
        }
    }

    // also cancel any ongoing queries
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    snprintf(fullyQualName,
             LibNsTypes::MaxPathNameLen,
             "%s%s",
             QueryManager::NsPrefix,
             "*");

    size_t numPaths;
    numPaths = LibNs::get()->getPathInfo(fullyQualName, &pathInfo);

    for (unsigned ii = 0; ii < numPaths; ii++) {
        if ((rndWeakGenerate64(randHandle) % 100) < PctChanceOfCancel) {
            const char *queryName =
                pathInfo[ii].pathName + strlen(QueryManager::NsPrefix);

            // This query prefix is used by system test. When operators
            // functest and systemtest run in parallel, we need to skip them.
            // Ensure that this prefix is in sync in both the test suites.
            if (strstr(queryName, "systemTestQuery")) {
                continue;
            }

            status = QueryManager::get()->requestQueryCancel(queryName);
            if (status == StatusOk) {
                numCancelled++;
            } else {
                numFailed++;
            }
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "cancelOperations report: %u cancelled, %u failed",
            numCancelled,
            numFailed);

CommonExit:
    if (pathInfo) {
        memFree(pathInfo);
    }

    if (output != NULL) {
        memFree(output);
        output = NULL;
        listTablesOutput = NULL;
    }

    return status;
}

static Status
workerArchive(XcalarApiUserId *userId, Dag *sessionGraph, const char *tableName)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;

    workItem = xcalarApiMakeArchiveTablesWorkItem(true, false, 1, &tableName);
    BailIfNull(workItem);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "archive %s failed, status: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
workerRename(XcalarApiUserId *userId, Dag *sessionGraph, const char *tableName)
{
    Status status;
    XcalarWorkItem *workItem = NULL;
    char newName[XcalarApiMaxTableNameLen + 1];

    // prepend a unique rename prefix
    if (strstr(tableName, RenamePrefix)) {
        uint64_t rnId = strtol(tableName + strlen(RenamePrefix), NULL, 10);
        const char *suffix = tableName + strlen(RenamePrefix) +
                             (unsigned) (floor(log10(rnId)) + 1);
        rnId++;

        snprintf(newName,
                 sizeof(newName),
                 "%s%lu%s",
                 RenamePrefix,
                 rnId,
                 suffix);
    } else {
        snprintf(newName,
                 sizeof(newName),
                 "%s%d%s",
                 RenamePrefix,
                 0,
                 tableName);
    }

    workItem = xcalarApiMakeRenameNodeWorkItem(tableName, newName);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating rename workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "rename %s to %s failed, status: %s",
                tableName,
                newName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "rename %s to %s succeeded",
            tableName,
            newName);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
scrubAndDeleteTables(XcalarApiUserId *userId,
                     Dag *sessionGraph,
                     RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiMakeResultSetOutput *makeResultSetOutput = NULL;
    XcalarApiListDagNodesOutput *listTablesOutput = NULL;
    size_t outputSize;
    unsigned ii, jj;
    SourceType srcTypes[] = {SrcTable, SrcDataset, SrcConstant};
    uint32_t pctChanceOfDrop[] = {PctChanceOfTableDrop,
                                  PctChanceOfDatasetDrop,
                                  PctChanceOfVariableDrop};
    XcalarApiUdfContainer *sessionContainer =
        sessionGraph->getSessionContainer();

    makeResultSetOutput = (XcalarApiMakeResultSetOutput *) memAlloc(
        sizeof(*makeResultSetOutput) +
        sizeof(XcalarApiTableMeta) * Config::get()->getActiveNodes());
    if (makeResultSetOutput == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate resultSetOutput");
        status = StatusNoMem;
        goto CommonExit;
    }

    for (jj = 0; jj < ArrayLen(srcTypes); jj++) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            listTablesOutput = NULL;
        }

        status = sessionGraph->listDagNodeInfo("*",
                                               &output,
                                               &outputSize,
                                               srcTypes[jj],
                                               &testUserId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error with dgListDagNodeInfo: %s (srcType: %s)",
                    strGetFromStatus(status),
                    strGetFromSourceType(srcTypes[jj]));
            goto CommonExit;
        }

        listTablesOutput = &output->outputResult.listNodesOutput;
        if (listTablesOutput->numNodes == 0) {
            status = StatusFunctionalTestNoTablesLeft;
            xSyslog(moduleName, XlogErr, "No tables found in session");
            goto CommonExit;
        }

        for (ii = 0; ii < listTablesOutput->numNodes; ii++) {
            if (listTablesOutput->nodeInfo[ii].state != DgDagStateError &&
                ((rndWeakGenerate64(randHandle) % 100) >=
                 pctChanceOfDrop[jj])) {
                if (srcTypes[jj] == SrcConstant) {
                    continue;
                }

                status =
                    ResultSetMgr::get()
                        ->makeResultSet(sessionGraph,
                                        listTablesOutput->nodeInfo[ii].name,
                                        false,
                                        makeResultSetOutput,
                                        &sessionContainer->userId);
                if (status != StatusOk) {
                    // Not a fatal error. Log it and keep going
                    xSyslog(moduleName,
                            XlogErr,
                            "Error creating result set during "
                            "scrubAndDeleteTables for %s: %s",
                            listTablesOutput->nodeInfo[ii].name,
                            strGetFromStatus(status));
                    continue;
                }

                uint64_t numEntries = makeResultSetOutput->numEntries;
                ResultSetMgr::get()->freeResultSet(
                    makeResultSetOutput->resultSetId);
                if (numEntries > 0) {
                    // Not empty. Keep going
                    continue;
                }
            }

            uint64_t numRefs = 0;
            DagTypes::DagRef *refList = NULL;
            status = sessionGraph->dropNode(listTablesOutput->nodeInfo[ii].name,
                                            srcTypes[jj],
                                            &numRefs,
                                            &refList);
            if (status != StatusOk) {
                char refs[numRefs * (DagTypes::MaxNameLen + 1) + 1];
                refs[0] = '\0';
                // refList can be NULL when under out of memory conditions
                if (refList) {
                    for (uint64_t kk = 0; kk < numRefs; kk++) {
                        strlcat(refs, refList[kk].name, sizeof(refs));
                        strlcat(refs, ",", sizeof(refs));
                    }
                }

                // Not a fatal error. Log it and keep going
                xSyslog(moduleName,
                        XlogErr,
                        "Error dropping %s (%s): %s. "
                        "%lu refs outstanding: %s",
                        listTablesOutput->nodeInfo[ii].name,
                        strGetFromSourceType(srcTypes[jj]),
                        strGetFromStatus(status),
                        numRefs,
                        refs);
                memFree(refList);
                continue;
            }

            if (srcTypes[jj] == SrcConstant) {
                // remove aggregate from saved results
                AggregateResult *aggResult;

                aggregateHashTableLock.lock();
                aggResult = aggregateHashTable.remove(
                    listTablesOutput->nodeInfo[ii].name);
                aggregateHashTableLock.unlock();

                if (aggResult != NULL) {
                    memFree(aggResult->output);
                    memFree(aggResult);
                }
            }

            xSyslog(moduleName,
                    XlogInfo,
                    "Dropped %s (%s) successfully",
                    listTablesOutput->nodeInfo[ii].name,
                    strGetFromSourceType(srcTypes[jj]));
        }
    }

    workerUnpublishAll(userId, sessionGraph, randHandle);

    status = StatusOk;
CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
        listTablesOutput = NULL;
    }

    if (makeResultSetOutput != NULL) {
        memFree(makeResultSetOutput);
        makeResultSetOutput = NULL;
    }

    return status;
}

static Status
workerGroupBy(XcalarApiUserId *userId,
              Dag *sessionGraph,
              const char *tableName,
              RandWeakHandle *randHandle)
{
    char *colName = NULL;
    unsigned numEvals = rndWeakGenerate64(randHandle) % MaxMapEvals + 1;
    char dstTableName[XcalarApiMaxTableNameLen + 1];

    char filterTable[XcalarApiMaxTableNameLen + 1];
    uint64_t rowCountFilterTable;

    char(*newFieldNames)[XcalarApiMaxFieldNameLen + 1] = NULL;
    char(*evalStrings)[XcalarApiMaxEvalStringLen + 1] = NULL;
    char *aggString = NULL;
    char *filterString = NULL;

    Status status = StatusUnknown;
    size_t ret;
    bool includeSample = false;
    bool groupAll = rndWeakGenerate64(randHandle) % 2;
    AggregateResult result;
    result.output = NULL;
    status = getRandomColumn(sessionGraph, tableName, &colName, randHandle);
    BailIfFailed(status);

    newFieldNames =
        new (std::nothrow) char[numEvals][XcalarApiMaxFieldNameLen + 1];
    BailIfNull(newFieldNames);

    evalStrings =
        new (std::nothrow) char[numEvals][XcalarApiMaxEvalStringLen + 1];
    BailIfNull(evalStrings);

    aggString = new (std::nothrow) char[XcalarApiMaxEvalStringLen + 1];
    BailIfNull(aggString);

    filterString = new (std::nothrow) char[XcalarApiMaxEvalStringLen + 1];
    BailIfNull(filterString);

    for (unsigned ii = 0; ii < numEvals; ii++) {
        status =
            getUniqueName("col", newFieldNames[ii], sizeof(newFieldNames[ii]));
        BailIfFailed(status);

        ret = snprintf(evalStrings[ii],
                       sizeof(evalStrings[ii]),
                       "count(%s)",
                       colName);
        if (ret >= sizeof(evalStrings[ii])) {
            status = StatusNoBufs;
            xSyslog(moduleName,
                    XlogErr,
                    "Eval string \"count(%s)\" is too long",
                    colName);
            goto CommonExit;
        }
    }

    status = groupBy(userId,
                     sessionGraph,
                     tableName,
                     numEvals,
                     evalStrings,
                     newFieldNames,
                     includeSample,
                     false,
                     groupAll,
                     dstTableName);
    BailIfFailed(status);

    // pick one of the evaluations at random to check
    int idx;
    idx = rndWeakGenerate64(randHandle) % numEvals;

    snprintf(aggString,
             XcalarApiMaxEvalStringLen + 1,
             "sum(%s)",
             newFieldNames[idx]);

    status = aggregate(userId, sessionGraph, dstTableName, aggString, &result);
    BailIfFailed(status);

    uint64_t sum;
    sum = strtoul(strstr(result.output->jsonAnswer, ":") + 1, NULL, 10);

    uint64_t rowCount;
    status = getRowCount(sessionGraph, tableName, &rowCount);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (sum != rowCount) {
        // there may have been some empty rows, count them
        snprintf(filterString,
                 XcalarApiMaxEvalStringLen + 1,
                 "not(exists(%s))",
                 colName);
        status =
            filter(userId, sessionGraph, tableName, filterString, filterTable);
        BailIfFailed(status);

        status = getRowCount(sessionGraph, filterTable, &rowCountFilterTable);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting row count for %s: %s",
                    filterTable,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // exclude the empty rows
        rowCount -= rowCountFilterTable;
    }

    if (sum != rowCount) {
        char column[XcalarApiMaxFieldNameLen + 1];
        strlcpy(column, colName, sizeof(column));

        status = exportTable(userId,
                             sessionGraph,
                             tableName,
                             1,
                             &column,
                             "groupByOrigTable.csv");

        strlcpy(column, newFieldNames[idx], sizeof(column));
        status = exportTable(userId,
                             sessionGraph,
                             dstTableName,
                             1,
                             &column,
                             "groupByDstTable.csv");

        char retinaName[XcalarApiMaxFileNameLen + 1];
        getUniqueName("retina-", retinaName, sizeof(retinaName));
        status =
            exportTableAsRetina(userId, sessionGraph, dstTableName, retinaName);
        assert(0);
    }

CommonExit:
    if (colName != NULL) {
        memFree(colName);
        colName = NULL;
    }
    if (result.output != NULL) {
        memFree(result.output);
        result.output = NULL;
    }

    if (newFieldNames) {
        delete[] newFieldNames;
        newFieldNames = NULL;
    }
    if (evalStrings) {
        delete[] evalStrings;
        evalStrings = NULL;
    }
    if (aggString) {
        delete[] aggString;
        aggString = NULL;
    }
    if (filterString) {
        delete[] filterString;
        filterString = NULL;
    }

    return status;
}

static Status
indexTableRandomCol(XcalarApiUserId *userId,
                    Dag *sessionGraph,
                    const char *tableName,
                    RandWeakHandle *randHandle,
                    char dstTableName1[XcalarApiMaxTableNameLen + 1],
                    char dstTableName2[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    // get up to 3 keys
    unsigned numKeys = rndWeakGenerate64(randHandle) % 3 + 1;
    char *colNames[numKeys];
    memZero(colNames, sizeof(colNames));

    Ordering ordering[numKeys];

    unsigned numKeysFinal = 0;
    const char *colNamesFinal[numKeys];

    for (unsigned ii = 0; ii < numKeys; ii++) {
        status =
            getRandomColumn(sessionGraph, tableName, &colNames[ii], randHandle);
        BailIfFailed(status);

        ordering[ii] = Unordered;
    }

    // deduplicate the keys
    unsigned jj;
    for (unsigned ii = 0; ii < numKeys; ii++) {
        for (jj = 0; jj < numKeysFinal; jj++) {
            if (strcmp(colNamesFinal[jj], colNames[ii]) == 0) {
                // duplicate
                break;
            }
        }

        if (jj == numKeysFinal) {
            colNamesFinal[numKeysFinal++] = colNames[ii];
        }
    }

    status = indexTable(userId,
                        sessionGraph,
                        tableName,
                        numKeysFinal,
                        colNamesFinal,
                        ordering,
                        dstTableName1);
    BailIfFailed(status);

    status = indexTable(userId,
                        sessionGraph,
                        tableName,
                        numKeysFinal,
                        colNamesFinal,
                        ordering,
                        dstTableName2);
    BailIfFailed(status);
CommonExit:
    for (unsigned ii = 0; ii < numKeys; ii++) {
        if (colNames[ii] != NULL) {
            memFree(colNames[ii]);
            colNames[ii] = NULL;
        }
    }

    return status;
}

static Status
workerFilter(XcalarApiUserId *userId,
             Dag *sessionGraph,
             const char *tableName,
             RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    char *colName = NULL;
    char dstTable1[XcalarApiMaxTableNameLen + 1];
    char dstTable2[XcalarApiMaxTableNameLen + 1];
    char variable[XcalarApiMaxTableNameLen + 1];
    char evalString[XcalarApiMaxEvalStringLen + 1];
    size_t ret;
    const char *op;

    status = getRandomColumn(sessionGraph, tableName, &colName, randHandle);
    BailIfFailed(status);

    status = getRandomVariable(randHandle, variable);
    BailIfFailed(status);

    switch (rndWeakGenerate64(randHandle) % 3) {
    case FilterGt:
        op = "gt";
        break;
    case FilterLt:
        op = "lt";
        break;
    case FilterUdf:
        op = "default:multiJoin";
        break;
    }

    ret = snprintf(evalString,
                   sizeof(evalString),
                   "%s(float(%s), float(%s))",
                   op,
                   colName,
                   variable);
    if (ret >= sizeof(evalString)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Eval string \"%s(float(%s), float(%s))\" "
                "is too long",
                op,
                colName,
                variable);
        goto CommonExit;
    }

    status = filter(userId, sessionGraph, tableName, evalString, dstTable1);
    BailIfFailed(status);

    status = filter(userId, sessionGraph, tableName, evalString, dstTable2);
    BailIfFailed(status);

    uint64_t rowCount1, rowCount2;

    status = getRowCount(sessionGraph, dstTable1, &rowCount1);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTable1,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = getRowCount(sessionGraph, dstTable2, &rowCount2);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTable2,
                strGetFromStatus(status));
        goto CommonExit;
    }

    while (rowCount1 != rowCount2) {
        char column[XcalarApiMaxFieldNameLen + 1];
        strlcpy(column, colName, sizeof(column));
        char exportTableSrc[XcalarApiMaxFieldNameLen + 1];
        char exportTable1[XcalarApiMaxFieldNameLen + 1];
        char exportTable2[XcalarApiMaxFieldNameLen + 1];

        getUniqueName("filterTableSrc", exportTableSrc, sizeof(exportTableSrc));
        strlcat(exportTableSrc, ".csv", sizeof(exportTableSrc));

        getUniqueName("filterTable1", exportTable1, sizeof(exportTable1));
        strlcat(exportTable1, ".csv", sizeof(exportTable1));

        getUniqueName("filterTable2", exportTable2, sizeof(exportTable2));
        strlcat(exportTable2, ".csv", sizeof(exportTable2));

        status = exportTable(userId,
                             sessionGraph,
                             tableName,
                             1,
                             &column,
                             exportTableSrc);

        status = exportTable(userId,
                             sessionGraph,
                             dstTable1,
                             1,
                             &column,
                             exportTable1);

        status = exportTable(userId,
                             sessionGraph,
                             dstTable2,
                             1,
                             &column,
                             exportTable2);

        char retinaName[XcalarApiMaxFileNameLen + 1];
        getUniqueName("retina-", retinaName, sizeof(retinaName));
        status =
            exportTableAsRetina(userId, sessionGraph, dstTable1, retinaName);
        assert(0);
    }

CommonExit:
    if (colName != NULL) {
        memFree(colName);
        colName = NULL;
    }

    return status;
}

static Status
projectRandomCols(XcalarApiUserId *userId,
                  Dag *sessionGraph,
                  const char *tableName,
                  RandWeakHandle *randHandle,
                  char *dstTable,
                  uint32_t pctChanceOfPickingCol)
{
    Status status = StatusUnknown;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    int numCols = 0;

    status = getRandomColumns(sessionGraph,
                              tableName,
                              randHandle,
                              &columns,
                              NULL,
                              &numCols,
                              pctChanceOfPickingCol);
    BailIfFailed(status);

    status =
        project(userId, sessionGraph, tableName, numCols, columns, dstTable);
CommonExit:
    if (columns != NULL) {
        memFree(columns);
        columns = NULL;
    }

    return status;
}

static Status
workerAggregate(XcalarApiUserId *userId,
                Dag *sessionGraph,
                const char *tableName,
                RandWeakHandle *randHandle,
                AggregateResult *resultOut)
{
    Status status = StatusUnknown;
    char *colName = NULL;
    char evalString[XcalarApiMaxEvalStringLen + 1];
    AggregateResult result2;
    result2.output = NULL;

    size_t ret;

    status = getRandomColumn(sessionGraph, tableName, &colName, randHandle);
    BailIfFailed(status);

    ret = snprintf(evalString, sizeof(evalString), "count(%s)", colName);
    if (ret >= sizeof(evalString)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Eval string \"count(%s)\" is too long",
                colName);
        goto CommonExit;
    }

    status = aggregate(userId, sessionGraph, tableName, evalString, resultOut);
    BailIfFailed(status);

    status = aggregate(userId, sessionGraph, tableName, evalString, &result2);
    BailIfFailed(status);

    if (strcmp(resultOut->output->jsonAnswer, result2.output->jsonAnswer) !=
        0) {
        char column[XcalarApiMaxFieldNameLen + 1];
        char exportTable1[XcalarApiMaxFieldNameLen + 1];

        strlcpy(column, colName, sizeof(column));
        getUniqueName("aggTable", exportTable1, sizeof(exportTable1));
        strlcat(exportTable1, ".csv", sizeof(exportTable1));

        status = exportTable(userId,
                             sessionGraph,
                             tableName,
                             1,
                             &column,
                             exportTable1);

        char retinaName[XcalarApiMaxFileNameLen + 1];
        getUniqueName("retina-", retinaName, sizeof(retinaName));
        status =
            exportTableAsRetina(userId, sessionGraph, tableName, retinaName);
        assert(0);
    }

CommonExit:
    if (colName != NULL) {
        memFree(colName);
        colName = NULL;
    }

    if (result2.output != NULL) {
        memFree(result2.output);
        result2.output = NULL;
    }

    return status;
}

static Status
sortTableRandomCol(XcalarApiUserId *userId,
                   Dag *sessionGraph,
                   const char *tableName,
                   RandWeakHandle *randHandle,
                   char colNameOut[XcalarApiMaxFieldNameLen + 1],
                   char dstTableName1[XcalarApiMaxTableNameLen + 1],
                   char dstTableName2[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    // get up to 3 keys
    unsigned numKeys = rndWeakGenerate64(randHandle) % 3 + 1;
    char *colNames[numKeys];
    memZero(colNames, sizeof(colNames));

    Ordering ordering[numKeys];

    unsigned numKeysFinal = 0;
    const char *colNamesFinal[numKeys];

    for (unsigned ii = 0; ii < numKeys; ii++) {
        status =
            getRandomColumn(sessionGraph, tableName, &colNames[ii], randHandle);
        BailIfFailed(status);

        if ((rndWeakGenerate64(randHandle) % 100) < PctChanceOfAscendingSort) {
            ordering[ii] = Ascending;
        } else {
            ordering[ii] = Descending;
        }
    }

    // deduplicate the keys
    unsigned jj;
    for (unsigned ii = 0; ii < numKeys; ii++) {
        for (jj = 0; jj < numKeysFinal; jj++) {
            if (strcmp(colNamesFinal[jj], colNames[ii]) == 0) {
                // duplicate
                break;
            }
        }

        if (jj == numKeysFinal) {
            colNamesFinal[numKeysFinal++] = colNames[ii];
        }
    }

    status = indexTable(userId,
                        sessionGraph,
                        tableName,
                        numKeysFinal,
                        colNamesFinal,
                        ordering,
                        dstTableName1);
    BailIfFailed(status);

    status = indexTable(userId,
                        sessionGraph,
                        tableName,
                        numKeysFinal,
                        colNamesFinal,
                        ordering,
                        dstTableName2);
    BailIfFailed(status);

CommonExit:
    for (unsigned ii = 0; ii < numKeys; ii++) {
        if (colNames[ii] != NULL) {
            memFree(colNames[ii]);
            colNames[ii] = NULL;
        }
    }

    return status;
}

static Status
workerIndex(XcalarApiUserId *userId,
            Dag *sessionGraph,
            const char *tableName,
            Atomic64 *indexJobRan,
            RandWeakHandle *randHandle)
{
    Status status;
    char dstTableName1[XcalarApiMaxTableNameLen + 1];
    char dstTableName2[XcalarApiMaxTableNameLen + 1];
    char keyName[XcalarApiMaxFieldNameLen + 1];
    bool verifySort = false;
    size_t ret;

    switch (atomicInc64(indexJobRan) % IndexMaxRandomArgs) {
    case IndexDataset:
        status = indexRandomDataset(userId,
                                    sessionGraph,
                                    randHandle,
                                    dstTableName1,
                                    dstTableName2);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error while indexing random dataset: "
                    "%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        break;

    case IndexTable:
        status = indexTableRandomCol(userId,
                                     sessionGraph,
                                     tableName,
                                     randHandle,
                                     dstTableName1,
                                     dstTableName2);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error while indexing table %s: %s",
                    tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        break;

    case IndexSort:
        status = sortTableRandomCol(userId,
                                    sessionGraph,
                                    tableName,
                                    randHandle,
                                    keyName,
                                    dstTableName1,
                                    dstTableName2);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error while sorting table %s: %s",
                    tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        verifySort = true;
        break;

    default:
        NotReached();
    }

    uint64_t rowCount1;
    uint64_t rowCount2;
    status = getRowCount(sessionGraph, dstTableName1, &rowCount1);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTableName1,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = getRowCount(sessionGraph, dstTableName2, &rowCount2);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTableName2,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (rowCount1 != rowCount2) {
        char retinaName[XcalarApiMaxFileNameLen + 1];
        getUniqueName(dstTableName1, retinaName, sizeof(retinaName));

        exportTableAsRetina(userId, sessionGraph, dstTableName1, retinaName);
        assert(0);
    }

    if (verifySort) {
        char gbTable1[XcalarApiMaxTableNameLen + 1];
        char gbTable2[XcalarApiMaxTableNameLen + 1];
        char colName[XcalarApiMaxFieldNameLen + 1];
        char evalString[XcalarApiMaxEvalStringLen + 1];

        status = getUniqueName("col", colName, sizeof(colName));
        BailIfFailed(status);
        ret = snprintf(evalString, sizeof(evalString), "count(%s)", keyName);
        if (ret >= sizeof(evalString)) {
            status = StatusNoBufs;
            xSyslog(moduleName,
                    XlogErr,
                    "Eval string \"count(%s)\" is too long",
                    colName);
            goto CommonExit;
        }

        status = groupBy(userId,
                         sessionGraph,
                         dstTableName1,
                         1,
                         &evalString,
                         &colName,
                         false,
                         false,
                         false,
                         gbTable1);
        BailIfFailed(status);

        status = groupBy(userId,
                         sessionGraph,
                         dstTableName2,
                         1,
                         &evalString,
                         &colName,
                         false,
                         false,
                         false,
                         gbTable2);
        BailIfFailed(status);

        status = getRowCount(sessionGraph, gbTable1, &rowCount1);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting row count for %s: %s",
                    gbTable1,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = getRowCount(sessionGraph, gbTable2, &rowCount2);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error geting row count for %s: %s",
                    gbTable2,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        char retinaName[XcalarApiMaxFileNameLen + 1];
        getUniqueName(dstTableName1, retinaName, sizeof(retinaName));

        exportTableAsRetina(userId, sessionGraph, dstTableName1, retinaName);
        assert(rowCount1 == rowCount2);
    }

CommonExit:
    return status;
}

static Status
exportTableRandomCols(XcalarApiUserId *userId,
                      Dag *sessionGraph,
                      const char *tableName,
                      RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    int numCols = 0;
    char fileNameTmp[XcalarApiMaxFileNameLen + 1];
    char fileName[XcalarApiMaxFileNameLen + 1];
    char pathName[XcalarApiMaxPathLen + 1];
    size_t ret;

    status = getRandomColumns(sessionGraph,
                              tableName,
                              randHandle,
                              &columns,
                              NULL,
                              &numCols,
                              PctChanceOfColExport);
    BailIfFailed(status);

    status = getUniqueName("exportFile", fileNameTmp, sizeof(fileNameTmp));
    BailIfFailed(status);

    snprintf(pathName,
             sizeof(pathName),
             "%s/export/%s",
             XcalarConfig::get()->xcalarRootCompletePath_,
             fileNameTmp);

    ret = snprintf(fileName, sizeof(fileName), "%s.csv", fileNameTmp);
    if (ret >= sizeof(fileName)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Export file name \"%s.csv\" too long (%lu chars). "
                "Max is %lu chars",
                fileNameTmp,
                ret,
                sizeof(fileName));
        goto CommonExit;
    }

    status = exportTable(userId,
                         sessionGraph,
                         tableName,
                         numCols,
                         columns,
                         fileName);
    BailIfFailed(status);

    removeDirectory(pathName);
CommonExit:
    if (columns != NULL) {
        memFree(columns);
        columns = NULL;
    }

    return status;
}

static void
exportRetina(XcalarApiUserId *userId, Dag *sessionGraph, const char *retinaName)
{
    Status status;
    XcalarWorkItem *workItem = NULL;
    char path[XcalarApiMaxFileNameLen + 1];
    int fd;
    unsigned ret;
    // Store the file in the log dir so that it's available in support bundle.
    snprintf(path,
             sizeof(path),
             "%s/%s.tar.gz",
             XcalarConfig::get()->xcalarLogCompletePath_,
             retinaName);

    workItem = xcalarApiMakeExportRetinaWorkItem(retinaName);
    BailIfNullWith(workItem, StatusNoMem);

    do {
        status = processWorkItem(userId, sessionGraph, workItem);
    } while (status != StatusOk);

    fd = open(path, O_CREAT | O_RDWR, S_IRWXU);
    assert(fd != -1);
    ret = write(fd,
                workItem->output->outputResult.exportRetinaOutput.retina,
                workItem->output->outputResult.exportRetinaOutput.retinaCount);
    assert(ret ==
           workItem->output->outputResult.exportRetinaOutput.retinaCount);
    close(fd);

CommonExit:
    xSyslog(moduleName,
            XlogInfo,
            "exportRetina(%s, %p, %s): %s",
            userId->userIdName,
            sessionGraph,
            retinaName,
            strGetFromStatus(status));

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
}

static void
unloadDatasets(XcalarApiUserId *userId,
               Dag *sessionGraph,
               const char *datasetName)
{
    Status status;
    XcalarWorkItem *workItem = NULL;
    char fullDatasetName[DagTypes::MaxNameLen + 1];
    size_t ret;

    ret = snprintf(fullDatasetName,
                   sizeof(fullDatasetName),
                   XcalarApiDatasetPrefix "%s",
                   datasetName);
    assert(ret < sizeof(fullDatasetName));

    workItem = xcalarApiMakeDatasetUnloadWorkItem(fullDatasetName);
    BailIfNullWith(workItem, StatusNoMem);

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "DatasetUnload(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "DatasetUnload(%s, %p, %s) succeeded",
            userId->userIdName,
            sessionGraph,
            datasetName);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
}

// Based on the BatchId passed in, this does data validation on read committed
// or repeatable reads.
static void
imdDataVerification(XcalarApiUserId *userId,
                    Dag *sessionGraph,
                    const char *publishedTable,
                    int64_t batchId,
                    const char *filterString,
                    unsigned numColumns,
                    char (*columns)[XcalarApiMaxFieldNameLen + 1],
                    unsigned paramVal,
                    RandWeakHandle *randHandle)
{
    Status status = StatusOk;
    char selectBefore[XcalarApiMaxTableNameLen + 1];
    char selectAfter[XcalarApiMaxTableNameLen + 1];
    char aggEvalString[XcalarApiMaxEvalStringLen + 1];
    AggregateResult resultBefore;
    AggregateResult resultAfter;
    resultBefore.output = NULL;
    resultAfter.output = NULL;
    char(*tmpCols)[XcalarApiMaxFieldNameLen + 1] = NULL;
    DfFieldType *tmpColTypes = NULL;
    int tmpNumCols = 0;
    char paramValCol[XcalarApiMaxFieldNameLen + 1];
    snprintf(paramValCol, sizeof(paramValCol), "ParamVal");

    status = select(userId,
                    sessionGraph,
                    publishedTable,
                    batchId,
                    filterString,
                    numColumns,
                    columns,
                    selectBefore);
    BailIfFailed(status);

    status = select(userId,
                    sessionGraph,
                    publishedTable,
                    batchId,
                    filterString,
                    numColumns,
                    columns,
                    selectAfter);
    BailIfFailed(status);

    uint64_t rowCountBefore, rowCountAfter;
    status = getRowCount(sessionGraph, selectBefore, &rowCountBefore);
    BailIfFailed(status);

    status = getRowCount(sessionGraph, selectAfter, &rowCountAfter);
    BailIfFailed(status);

    if (rowCountBefore != rowCountAfter) {
        status = StatusFailed;
        goto DumpAsCsv;
    }

    snprintf(aggEvalString,
             XcalarApiMaxEvalStringLen + 1,
             "sum(%s)",
             paramValCol);

    status = aggregate(userId,
                       sessionGraph,
                       selectBefore,
                       aggEvalString,
                       &resultBefore);
    BailIfFailed(status);

    status = aggregate(userId,
                       sessionGraph,
                       selectAfter,
                       aggEvalString,
                       &resultAfter);
    BailIfFailed(status);

    uint64_t sum, sumBefore, sumAfter;
    sum = paramVal * rowCountBefore;
    sumBefore =
        strtoul(strstr(resultBefore.output->jsonAnswer, ":") + 1, NULL, 10);
    sumAfter =
        strtoul(strstr(resultAfter.output->jsonAnswer, ":") + 1, NULL, 10);

    if (sum != sumBefore || sumBefore != sumAfter) {
        status = StatusFailed;
        goto DumpAsCsv;
    }

    assert(status == StatusOk);
    goto CommonExit;

DumpAsCsv:
    for (unsigned ii = 0; ii < 2; ii++) {
        char tmp[XcalarApiMaxFileNameLen + 1];
        char fileName[XcalarApiMaxFileNameLen + 1];
        char *table = NULL;

        if (tmpCols) {
            memFree(tmpCols);
            tmpCols = NULL;
        }
        if (tmpColTypes) {
            memFree(tmpColTypes);
            tmpColTypes = NULL;
        }
        if (ii == 0) {
            table = selectBefore;
            snprintf(tmp,
                     sizeof(tmp),
                     "%s-select-before-%s",
                     publishedTable,
                     table);
        } else {
            table = selectAfter;
            snprintf(tmp,
                     sizeof(tmp),
                     "%s-select-after-%s",
                     publishedTable,
                     table);
        }
        status = getRandomColumns(sessionGraph,
                                  table,
                                  randHandle,
                                  &tmpCols,
                                  &tmpColTypes,
                                  &tmpNumCols,
                                  100);
        if (status != StatusOk) {
            break;
        }

        getUniqueName(tmp, fileName, sizeof(fileName));
        strlcat(fileName, ".csv", sizeof(fileName));

        status = exportTable(userId,
                             sessionGraph,
                             table,
                             tmpNumCols,
                             tmpCols,
                             fileName);
        if (status != StatusOk) {
            break;
        }
    }
    assert(0 && "data corruption");

CommonExit:
    if (tmpCols) {
        memFree(tmpCols);
        tmpCols = NULL;
    }
    if (tmpColTypes) {
        memFree(tmpColTypes);
        tmpColTypes = NULL;
    }
    if (resultBefore.output) {
        memFree(resultBefore.output);
        resultBefore.output = NULL;
    }
    if (resultAfter.output) {
        memFree(resultAfter.output);
        resultAfter.output = NULL;
    }
}

static unsigned
retAddedValFromPubTableName(char *pubTableName)
{
    unsigned paramVal = 0;
    char *pch;
    char *saveptr = NULL;
    pch = strtok_r(pubTableName, "_", &saveptr);
    while (pch != NULL) {
        paramVal = (unsigned) atoi(pch);
        break;
    }
    return paramVal;
}

static void
generateParseArgsForLoad(RandWeakHandle *randHandle,
                         unsigned paramVal,
                         char *parseArgs,
                         size_t length)
{
    snprintf(parseArgs,
             length,
             "{\"schema\": \"{'ParamVal': %u, 'Cols': {",
             paramVal);
    size_t offset = strlen(parseArgs);
    unsigned numCols =
        rndWeakGenerate64(randHandle) % OpMaxColsLoadMemWithSchema + 1;
    for (unsigned ii = 0; ii < numCols; ii++) {
        unsigned val = rndWeakGenerate64(randHandle) % 3;
        if (val == 0) {
            snprintf(parseArgs + offset,
                     length - offset,
                     " 'Col%u': 'int'",
                     ii);
        } else if (val == 1) {
            snprintf(parseArgs + offset,
                     length - offset,
                     " 'Col%u': 'float'",
                     ii);
        } else {
            snprintf(parseArgs + offset,
                     length - offset,
                     " 'Col%u': 'string'",
                     ii);
        }
        offset = strlen(parseArgs);
        assert(offset < length);
        if (ii < numCols - 1) {
            snprintf(parseArgs + offset, length - offset, ",");
            offset = strlen(parseArgs);
            assert(offset < length);
        }
    }
    snprintf(parseArgs + offset, length - offset, "}}\"}");
    offset = strlen(parseArgs);
    assert(offset < length);
}

static Status
workerPublish(XcalarApiUserId *userId,
              Dag *sessionGraph,
              RandWeakHandle *randHandle)
{
    static constexpr const unsigned NumEvals = 3;

    Status status = StatusOk;
    char(*evalString)[XcalarApiMaxEvalStringLen + 1] = NULL;
    char publishedTable[XcalarApiMaxTableNameLen + 1];
    Ordering ordering = Unordered;
    char keyCol[XcalarApiMaxFieldNameLen + 1];
    char *keyNames[1];
    snprintf(keyCol, sizeof(keyCol), "RowNum");
    keyNames[0] = keyCol;
    XcalarWorkItem *workItem = NULL;
    uint64_t origRows = 0;
    char newField[NumEvals][XcalarApiMaxFieldNameLen + 1];
    char dstTable[XcalarApiMaxTableNameLen + 1];
    bool newPubTable = true;
    unsigned paramVal = 0;
    int64_t batchId = 0;
    char(*tmpCols)[XcalarApiMaxFieldNameLen + 1] = NULL;
    DfFieldType *tmpColTypes = NULL;
    int tmpNumCols = 0;
    XcalarApiRenameMap *renameMap = NULL;
    char paramValCol[XcalarApiMaxFieldNameLen + 1];
    snprintf(paramValCol, sizeof(paramValCol), "ParamVal");
    char parseArgsJson[XcalarApiMaxPathLen + 1];

    evalString =
        new (std::nothrow) char[NumEvals][XcalarApiMaxEvalStringLen + 1];
    BailIfNull(evalString);

    snprintf(evalString[0], sizeof(evalString[0]), "int(%s)", keyCol);
    snprintf(newField[0], sizeof(newField[0]), "%s", keyCol);
    snprintf(evalString[1], sizeof(evalString[1]), "int(1)");
    snprintf(newField[1], sizeof(newField[1]), "XcalarRankOver");
    snprintf(evalString[2], sizeof(evalString[2]), "int(1)");
    snprintf(newField[2], sizeof(newField[2]), "XcalarOpCode");

    //
    // Use some randomization to either create a new publish table or just
    // choose an existing publish table.
    //
    if (rndWeakGenerate64(randHandle) % 2) {
        // Pick an existing table.
        unsigned tableIdx;
        char pubTablePattern[XcalarApiMaxFieldNameLen + 1];

        // List the publish tables created by our session and pick a candidate.
        snprintf(pubTablePattern,
                 sizeof(pubTablePattern),
                 "%s%s--*",
                 PubTablePrefix,
                 userId->userIdName);

        xcalar::compute::localtypes::PublishedTable::ListTablesResponse
            listTables;
        status =
            listPubTable(userId, sessionGraph, pubTablePattern, listTables);
        BailIfFailed(status);

        if (listTables.tables_size() > 0) {
            tableIdx =
                (rndWeakGenerate64(randHandle) % listTables.tables_size());
            snprintf(publishedTable,
                     sizeof(publishedTable),
                     "%s",
                     listTables.tables(tableIdx).name().c_str());
            newPubTable = false;

            // Remember the load paramVal to prepare the update downstream.
            paramVal = retAddedValFromPubTableName(publishedTable);
        }
    }

    //
    // Create new publish table
    //
    if (newPubTable) {
        char pubTablePattern[XcalarApiMaxFieldNameLen + 1];
        char prefixName[XcalarApiMaxFieldNameLen + 1];
        char datasetName[XcalarApiMaxTableNameLen + 1];
        char dsName[XcalarApiMaxTableNameLen + 1];
        char prefixedKey[XcalarApiMaxFieldNameLen + 1];
        char prefixedParamVal[XcalarApiMaxFieldNameLen + 1];

        status = getUniqueName("prefix", prefixName, sizeof(prefixName));
        BailIfFailed(status);

        snprintf(prefixedKey,
                 sizeof(prefixedKey),
                 "%s::%s",
                 prefixName,
                 keyCol);
        snprintf(prefixedParamVal,
                 sizeof(prefixedParamVal),
                 "%s::%s",
                 prefixName,
                 paramValCol);

        // Pick some paramVal for load that would be used to change
        // parameterize the dataset.
        paramVal = ((unsigned) rndWeakGenerate64(randHandle) % 1000 + 1);

        // Include the paramVal in the publish table name. This will be used
        // for data verification by selects.
        snprintf(pubTablePattern,
                 sizeof(pubTablePattern),
                 "%s%s_%u",
                 PubTablePrefix,
                 userId->userIdName,
                 paramVal);

        status = getUniqueName(pubTablePattern,
                               publishedTable,
                               sizeof(publishedTable));
        BailIfFailed(status);

        status =
            getUniqueName(userId->userIdName, datasetName, sizeof(datasetName));
        BailIfFailed(status);

        generateParseArgsForLoad(randHandle,
                                 paramVal,
                                 parseArgsJson,
                                 sizeof(parseArgsJson));

        status = load(userId,
                      sessionGraph,
                      MemoryDatasetPath,
                      MemoryDatasetType,
                      MemoryDatasetSize,
                      datasetName,
                      LoadMemUdfs::LoadMemWithSchema,
                      parseArgsJson);
        BailIfFailed(status);

        snprintf(dsName, sizeof(dsName), ".XcalarDS.%s", datasetName);
        status = indexDataset(userId,
                              sessionGraph,
                              dsName,
                              keyCol,
                              prefixName,
                              dstTable);
        BailIfFailed(status);

        status = getRandomColumns(sessionGraph,
                                  dstTable,
                                  randHandle,
                                  &tmpCols,
                                  &tmpColTypes,
                                  &tmpNumCols,
                                  100);
        BailIfFailed(status);

        renameMap = new (std::nothrow) XcalarApiRenameMap[tmpNumCols];
        BailIfNull(renameMap);

        for (int ii = 0; ii < tmpNumCols; ii++) {
            renameMap[ii].type = tmpColTypes[ii];
            if (renameMap[ii].type == DfFatptr) {
                renameMap[ii].type = DfUnknown;
            }
            status = strStrlcpy(renameMap[ii].oldName,
                                tmpCols[ii],
                                sizeof(renameMap[ii].oldName));
            BailIfFailed(status);

            if (!strcmp(prefixedKey, renameMap[ii].oldName)) {
                status = strStrlcpy(renameMap[ii].newName,
                                    keyCol,
                                    sizeof(renameMap[ii].oldName));
                BailIfFailed(status);
                renameMap[ii].isKey = true;
            } else if (!strcmp(prefixedParamVal, renameMap[ii].oldName)) {
                status = strStrlcpy(renameMap[ii].newName,
                                    paramValCol,
                                    sizeof(renameMap[ii].oldName));
                BailIfFailed(status);
                renameMap[ii].isKey = false;
            } else {
                status = getUniqueName("rename-col",
                                       renameMap[ii].newName,
                                       sizeof(renameMap[ii].newName));
                BailIfFailed(status);
                renameMap[ii].isKey = false;
            }
        }

        status = synthesize(userId,
                            sessionGraph,
                            dstTable,
                            tmpNumCols,
                            renameMap,
                            dstTable);
        BailIfFailed(status);

        status = map(userId,
                     sessionGraph,
                     dstTable,
                     NumEvals,
                     evalString,
                     newField,
                     false,
                     dstTable);
        BailIfFailed(status);

        status = indexTable(userId,
                            sessionGraph,
                            dstTable,
                            1,
                            (const char **) keyNames,
                            &ordering,
                            dstTable);
        BailIfFailed(status);

        status = publishTable(userId, sessionGraph, dstTable, publishedTable);
        BailIfFailed(status);

        status = getRowCount(sessionGraph, dstTable, &origRows);
        BailIfFailed(status);
    }

    //
    // Issue update
    //
    char filterString[128];

    snprintf(filterString, sizeof(filterString), "eq(mod(RowNum, 100), 0)");
    status = select(userId,
                    sessionGraph,
                    publishedTable,
                    HashTree::InvalidBatchId,
                    "",
                    0,
                    NULL,
                    dstTable);
    BailIfFailed(status);

    if (tmpCols) {
        memFree(tmpCols);
        tmpCols = NULL;
    }
    if (tmpColTypes) {
        memFree(tmpColTypes);
        tmpColTypes = NULL;
    }
    if (renameMap) {
        delete[] renameMap;
        renameMap = NULL;
    }
    status = getRandomColumns(sessionGraph,
                              dstTable,
                              randHandle,
                              &tmpCols,
                              &tmpColTypes,
                              &tmpNumCols,
                              25);
    BailIfFailed(status);

    if (!tmpNumCols) {
        status = StatusOk;
        goto CommonExit;
    }

    renameMap = new (std::nothrow) XcalarApiRenameMap[tmpNumCols];
    BailIfNull(renameMap);

    for (int ii = 0; ii < tmpNumCols; ii++) {
        renameMap[ii].type = tmpColTypes[ii];
        if (renameMap[ii].type == DfFatptr) {
            renameMap[ii].type = DfUnknown;
        }
        status = strStrlcpy(renameMap[ii].oldName,
                            tmpCols[ii],
                            sizeof(renameMap[ii].oldName));
        BailIfFailed(status);
        if (!strcmp(keyCol, renameMap[ii].oldName)) {
            status = strStrlcpy(renameMap[ii].newName,
                                renameMap[ii].oldName,
                                sizeof(renameMap[ii].oldName));
            BailIfFailed(status);
            renameMap[ii].isKey = true;
        } else if (!strcmp(paramValCol, renameMap[ii].oldName)) {
            status = strStrlcpy(renameMap[ii].newName,
                                paramValCol,
                                sizeof(renameMap[ii].oldName));
            BailIfFailed(status);
            renameMap[ii].isKey = false;
        } else {
            status = getUniqueName("rename-col",
                                   renameMap[ii].newName,
                                   sizeof(renameMap[ii].newName));
            BailIfFailed(status);
            renameMap[ii].isKey = false;
        }
    }

    status = synthesize(userId,
                        sessionGraph,
                        dstTable,
                        tmpNumCols,
                        renameMap,
                        dstTable);
    BailIfFailed(status);

    // Choose Insert or Modify or Delete opcode.
    unsigned opCode;
    opCode = rndWeakGenerate64(randHandle) % 3;
    snprintf(evalString[1], sizeof(evalString[1]), "int(%u)", opCode);

    status = map(userId,
                 sessionGraph,
                 dstTable,
                 NumEvals,
                 evalString,
                 newField,
                 false,
                 dstTable);
    BailIfFailed(status);

    status = indexTable(userId,
                        sessionGraph,
                        dstTable,
                        1,
                        (const char **) keyNames,
                        &ordering,
                        dstTable);
    BailIfFailed(status);

    status = update(userId, sessionGraph, dstTable, publishedTable, &batchId);
    BailIfFailed(status);

    //
    // Do data verification
    //
    imdDataVerification(userId,
                        sessionGraph,
                        publishedTable,
                        batchId,
                        "",
                        0,
                        NULL,
                        paramVal,
                        randHandle);
CommonExit:
    if (evalString) {
        delete[] evalString;
        evalString = NULL;
    }
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    if (tmpCols) {
        memFree(tmpCols);
        tmpCols = NULL;
    }
    if (tmpColTypes) {
        memFree(tmpColTypes);
        tmpColTypes = NULL;
    }
    if (renameMap) {
        delete[] renameMap;
        renameMap = NULL;
    }
    return status;
}

static Status
workerSelect(XcalarApiUserId *userId,
             Dag *sessionGraph,
             RandWeakHandle *randHandle)
{
    static constexpr const unsigned NumCols = 2;

    Status status = StatusOk;
    unsigned tableIdx = 0;
    char pubTablePattern[XcalarApiMaxFieldNameLen + 1];
    xcalar::compute::localtypes::PublishedTable::ListTablesResponse listTables;
    const xcalar::compute::localtypes::PublishedTable::ListTablesResponse::
        TableInfo *tableInfo;
    snprintf(pubTablePattern, sizeof(pubTablePattern), "%s*", PubTablePrefix);
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    char(*selectColumns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    bool includeFilter = rndWeakGenerate64(randHandle) % 2;
    char filterString[128];
    unsigned paramVal = 0;

    if (includeFilter) {
        snprintf(filterString, sizeof(filterString), "eq(XcalarOpCode, 1)");
    } else {
        filterString[0] = '\0';
    }

    columns = new (std::nothrow) char[NumCols][XcalarApiMaxFieldNameLen + 1];
    BailIfNull(columns);

    snprintf(columns[0], sizeof(columns[0]), "key");
    snprintf(columns[1], sizeof(columns[1]), "XcalarOpCode");

    // List publish tables created by us and pick a candidate.
    status = listPubTable(userId, sessionGraph, pubTablePattern, listTables);
    BailIfFailed(status);

    if (listTables.tables_size() == 0) {
        status = StatusOk;
        goto CommonExit;
    }

    for (int ii = 0; ii < listTables.tables_size(); ii++) {
        if (listTables.tables(ii).active() &&
            rndWeakGenerate64(randHandle) % 2 == 0) {
            tableIdx = ii;
        }
    }

    if (listTables.tables(tableIdx).active()) {
        status = StatusOk;
        goto CommonExit;
    }

    tableInfo = &listTables.tables(tableIdx);
    if (tableInfo->updates_size() == 0) {
        status = StatusOk;
        goto CommonExit;
    }

    unsigned numSelectCols;
    numSelectCols = rndWeakGenerate64(randHandle) % tableInfo->values_size();

    if (numSelectCols > 0) {
        // pick some random columns
        selectColumns = new (
            std::nothrow) char[numSelectCols][XcalarApiMaxFieldNameLen + 1];
        BailIfNull(selectColumns);

        unsigned count;
        count = 0;

        for (int ii = 0; ii < tableInfo->values_size(); ii++) {
            if (rndWeakGenerate64(randHandle) % 3 == 0) {
                strlcpy(selectColumns[count++],
                        tableInfo->values(ii).name().c_str(),
                        sizeof(*selectColumns));
            }

            if (count == numSelectCols) {
                break;
            }
        }

        numSelectCols = count;
    }

    // Choose a random batch to select and validate it's rows count.
    unsigned updIdx;
    updIdx = (rndWeakGenerate64(randHandle) % tableInfo->updates_size());
    int64_t myBatchId;
    myBatchId = tableInfo->updates(updIdx).batchid();

    // Do data verification
    paramVal = retAddedValFromPubTableName((char *) tableInfo->name().c_str());
    imdDataVerification(userId,
                        sessionGraph,
                        tableInfo->name().c_str(),
                        myBatchId,
                        filterString,
                        numSelectCols,
                        selectColumns,
                        paramVal,
                        randHandle);

CommonExit:
    if (columns) {
        delete[] columns;
        columns = NULL;
    }
    if (selectColumns) {
        delete[] selectColumns;
        selectColumns = NULL;
    }

    return status;
}

static Status
workerRestore(XcalarApiUserId *userId,
              Dag *sessionGraph,
              RandWeakHandle *randHandle)
{
    static constexpr const unsigned NumCols = 2;

    Status status = StatusOk;
    xcalar::compute::localtypes::PublishedTable::ListTablesResponse listTables;
    const xcalar::compute::localtypes::PublishedTable::ListTablesResponse::
        TableInfo *tableInfo;
    unsigned tableIdx;
    char pubTablePattern[XcalarApiMaxFieldNameLen + 1];
    snprintf(pubTablePattern, sizeof(pubTablePattern), "%s*", PubTablePrefix);
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;

    columns = new (std::nothrow) char[NumCols][XcalarApiMaxFieldNameLen + 1];
    BailIfNull(columns);

    snprintf(columns[0], sizeof(columns[0]), "key");
    snprintf(columns[1], sizeof(columns[1]), "XcalarOpCode");

    // List publish tables created by us and pick a candidate.
    status = listPubTable(userId, sessionGraph, pubTablePattern, listTables);
    BailIfFailed(status);

    if (listTables.tables_size() == 0) {
        status = StatusOk;
        goto CommonExit;
    }
    tableIdx = (rndWeakGenerate64(randHandle) % listTables.tables_size());
    tableInfo = &listTables.tables(tableIdx);

    // Choose a random batch to select and validate it's rows count.
    unsigned updIdx;
    if (tableInfo->updates_size() == 0) {
        status = StatusOk;
        goto CommonExit;
    }
    updIdx = (rndWeakGenerate64(randHandle) % tableInfo->updates_size());
    int64_t myBatchId;
    myBatchId = tableInfo->updates(updIdx).batchid();
    char selectTableBefore[XcalarApiMaxTableNameLen + 1];
    char selectTableAfter[XcalarApiMaxTableNameLen + 1];

    status = select(userId,
                    sessionGraph,
                    tableInfo->name().c_str(),
                    myBatchId,
                    "",
                    0,
                    NULL,
                    selectTableBefore);
    BailIfFailed(status);

    uint64_t beforeRows;
    status = getRowCount(sessionGraph, selectTableBefore, &beforeRows);
    BailIfFailed(status);

    // Issue inactivate and restore on publish table.
    status =
        unpublishTable(userId, sessionGraph, tableInfo->name().c_str(), true);
    BailIfFailed(status);
    status = restore(userId, sessionGraph, tableInfo->name().c_str());
    BailIfFailed(status);
    status = changeOwner(userId, sessionGraph, tableInfo->name().c_str());
    BailIfFailed(status);

    status = select(userId,
                    sessionGraph,
                    tableInfo->name().c_str(),
                    myBatchId,
                    "",
                    0,
                    NULL,
                    selectTableAfter);
    BailIfFailed(status);

    uint64_t afterRows;
    status = getRowCount(sessionGraph, selectTableAfter, &afterRows);
    BailIfFailed(status);

    if (beforeRows != afterRows) {
        for (unsigned ii = 0; ii < 2; ii++) {
            char tmp[XcalarApiMaxFileNameLen + 1];
            char fileName[XcalarApiMaxFileNameLen + 1];
            char *table = NULL;
            if (ii == 0) {
                table = selectTableBefore;
                snprintf(tmp,
                         sizeof(tmp),
                         "%s-select-before-%s",
                         tableInfo->name().c_str(),
                         table);
            } else {
                table = selectTableAfter;
                snprintf(tmp,
                         sizeof(tmp),
                         "%s-select-after-%s",
                         tableInfo->name().c_str(),
                         table);
            }
            getUniqueName(tmp, fileName, sizeof(fileName));
            strlcat(fileName, ".csv", sizeof(fileName));
            status = exportTable(userId,
                                 sessionGraph,
                                 table,
                                 NumCols,
                                 columns,
                                 fileName);
        }
        assert(0 && "rows count must match");
    }

    // Issue coalesce on publish table.
    status = coalesce(userId, sessionGraph, tableInfo->name().c_str());
    BailIfFailed(status);

CommonExit:
    if (columns) {
        delete[] columns;
        columns = NULL;
    }
    return status;
}

static void
workerUnpublishAll(XcalarApiUserId *userId,
                   Dag *sessionGraph,
                   RandWeakHandle *randHandle)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;

    xcalar::compute::localtypes::PublishedTable::ListTablesResponse listTables;
    const xcalar::compute::localtypes::PublishedTable::ListTablesResponse::
        TableInfo *tableInfo;
    char pubTablePattern[XcalarApiMaxFieldNameLen + 1];
    snprintf(pubTablePattern, sizeof(pubTablePattern), "%s*", PubTablePrefix);

    status = listPubTable(userId, sessionGraph, pubTablePattern, listTables);
    BailIfFailed(status);

    for (int ii = 0; ii < listTables.tables_size(); ii++) {
        tableInfo = &listTables.tables(ii);
        if ((randHandle &&
             rndWeakGenerate64(randHandle) % (100 / PctChanceOfPubTableDrop)) ||
            randHandle == NULL) {
            unpublishTable(userId,
                           sessionGraph,
                           tableInfo->name().c_str(),
                           true);
        }
        if ((randHandle && rndWeakGenerate64(randHandle) %
                               (100 / PctChanceOfPubTableDelete)) ||
            randHandle == NULL) {
            unpublishTable(userId,
                           sessionGraph,
                           tableInfo->name().c_str(),
                           false);
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    return;
}

static Status
workerRetina(XcalarApiUserId *userId,
             Dag *sessionGraph,
             const char *tableName,
             RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    int numCols = 0;
    char retinaName[XcalarApiMaxFileNameLen + 1];
    char dstTable[XcalarApiMaxTableNameLen + 1];
    XcalarWorkItem *workItem = NULL;
    RetinaDst *tableInfo = NULL;
    RetinaSrcTable srcTableInfo;
    XcalarApiOutput *getDagOutput = NULL;
    size_t outputSize;

    bool nestedRetina = false;
    unsigned srcNodeIdx;
    XcalarApiDagNode *srcNode = NULL;
    XcalarApiDagOutput *dagOutput;
    char tmpRetinaName[sizeof(retinaName)];
    tmpRetinaName[0] = '\0';

    if (tableName[0] == '\0') {
        return StatusOk;
    }

    status = getRandomColumns(sessionGraph,
                              tableName,
                              randHandle,
                              &columns,
                              NULL,
                              &numCols,
                              PctChanceOfColExport);
    BailIfFailed(status);

    status = sessionGraph->getDagByName((char *) tableName,
                                        Dag::TableScope::LocalOnly,
                                        &getDagOutput,
                                        &outputSize);
    BailIfFailed(status);

    dagOutput = &getDagOutput->outputResult.dagOutput;

    for (unsigned ii = 0; ii < dagOutput->numNodes; ii++) {
        if (dagOutput->node[ii]->hdr.api == XcalarApiExecuteRetina) {
            nestedRetina = true;
            break;
        }
    }

    snprintf(dstTable, sizeof(dstTable), "%s", tableName);

    status = getUniqueName("retina-", retinaName, sizeof(retinaName));
    BailIfFailed(status);

    xSyslog(moduleName,
            XlogInfo,
            "starting makeRetina(%s, %p, %s, %d, %p, %s)",
            userId->userIdName,
            sessionGraph,
            tableName,
            numCols,
            columns,
            retinaName);

    tableInfo = (RetinaDst *) memAllocExt(sizeof(*tableInfo) +
                                              numCols * sizeof(ExColumnName),
                                          moduleName);
    BailIfNullWith(tableInfo, StatusNoMem);

    tableInfo->numColumns = numCols;
    tableInfo->target.isTable = true;
    strlcpy(tableInfo->target.name, tableName, sizeof(tableInfo->target.name));
    for (int ii = 0; ii < numCols; ii++) {
        strlcpy(tableInfo->columns[ii].name,
                columns[ii],
                sizeof(tableInfo->columns[ii].name));

        status = getUniqueName("retCol",
                               tableInfo->columns[ii].headerAlias,
                               sizeof(tableInfo->columns[ii].headerAlias));
        BailIfFailed(status);
    }

    // XXX: disable source nodes with nested retinas for now
    if (!nestedRetina && rndWeakGenerate64(randHandle) % 2) {
        // Make retina with src node
        srcNodeIdx = rndWeakGenerate64(randHandle) %
                     getDagOutput->outputResult.dagOutput.numNodes;
        srcNode = getDagOutput->outputResult.dagOutput.node[srcNodeIdx];

        strlcpy(srcTableInfo.source.name,
                srcNode->hdr.name,
                sizeof(srcNode->hdr.name));
        strlcpy(srcTableInfo.dstName,
                srcNode->hdr.name,
                sizeof(srcNode->hdr.name));

        workItem = xcalarApiMakeMakeRetinaWorkItem(retinaName,
                                                   1,
                                                   &tableInfo,
                                                   1,
                                                   &srcTableInfo);
        if (workItem == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate make retina workItem");
            status = StatusNoMem;
            goto CommonExit;
        }

        status = processWorkItem(userId, sessionGraph, workItem);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "makeRetina(%s, %p, %s, %d, %p, %s) failed: %s",
                    userId->userIdName,
                    sessionGraph,
                    tableName,
                    numCols,
                    columns,
                    retinaName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        xcalarApiFreeWorkItem(workItem);

        // also create a retina without the synthesize node, add a suffix
        snprintf(tmpRetinaName,
                 sizeof(tmpRetinaName),
                 "%s-noSynthesize",
                 retinaName);

        workItem = xcalarApiMakeMakeRetinaWorkItem(tmpRetinaName,
                                                   1,
                                                   &tableInfo,
                                                   0,
                                                   NULL);
        if (workItem == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate make retina workItem");
            status = StatusNoMem;
            goto CommonExit;
        }
    } else {
        workItem =
            xcalarApiMakeMakeRetinaWorkItem(retinaName, 1, &tableInfo, 0, NULL);
        if (workItem == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate make retina workItem");
            status = StatusNoMem;
            goto CommonExit;
        }
    }

    status = processWorkItem(userId, sessionGraph, workItem);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "makeRetina(%s, %p, %s, %d, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                numCols,
                columns,
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // export the retina's so that they are persisted for test artifacts.
    // check if we are using a retina that starts from a source table and
    // persist the retina that generates the source table as well
    exportRetina(userId, sessionGraph, retinaName);
    if (tmpRetinaName[0] != '\0') {
        exportRetina(userId, sessionGraph, tmpRetinaName);
    }

    xSyslog(moduleName,
            XlogInfo,
            "makeRetina(%s, %p, %s, %d, %p, %s) succeeded",
            userId->userIdName,
            sessionGraph,
            tableName,
            numCols,
            columns,
            retinaName);

    uint64_t rowCountSrc;
    status = getRowCount(sessionGraph, tableName, &rowCountSrc);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = executeRetina(userId,
                           sessionGraph,
                           retinaName,
                           tableInfo,
                           dstTable,
                           randHandle);
    BailIfFailed(status);

    uint64_t rowCountDst;
    status = getRowCount(sessionGraph, dstTable, &rowCountDst);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogWarn,
            "user %s: executeRetina %s succeeded. "
            "DstTable: %s has %lu rows. OrigTable: %s has %lu rows",
            userId->userIdName,
            retinaName,
            dstTable,
            rowCountDst,
            tableName,
            rowCountSrc);

    if (rowCountSrc != rowCountDst) {
        XcalarApiOutput *getRetinaOutput;
        XcalarApiOutput *queryStateOutput;

        size_t outSize;
        char exportTable1[XcalarApiMaxFieldNameLen + 1];
        char exportTable2[XcalarApiMaxFieldNameLen + 1];

        // Get the generated retina so that it's available in the core.
        status =
            DagLib::get()->getRetina(retinaName, &getRetinaOutput, &outSize);
        (void) status;

        // Get the queryStateOutput so that it is available in the core.
        status = QueryManager::get()->requestQueryState(&queryStateOutput,
                                                        &outSize,
                                                        retinaName,
                                                        true);
        (void) status;

        getUniqueName("retSrc", exportTable1, sizeof(exportTable1));
        strlcat(exportTable1, ".csv", sizeof(exportTable1));
        exportTable(userId,
                    sessionGraph,
                    tableName,
                    numCols,
                    columns,
                    exportTable1);

        for (int ii = 0; ii < numCols; ii++) {
            DataFormat::replaceFatptrPrefixDelims(columns[ii]);
        }

        getUniqueName("retDst", exportTable2, sizeof(exportTable2));
        strlcat(exportTable2, ".csv", sizeof(exportTable2));
        exportTable(userId,
                    sessionGraph,
                    dstTable,
                    numCols,
                    columns,
                    exportTable2);

        assert(0);
    }

CommonExit:
    if (tableInfo != NULL) {
        memFree(tableInfo);
        tableInfo = NULL;
    }

    if (columns != NULL) {
        memFree(columns);
        columns = NULL;
    }

    if (getDagOutput != NULL) {
        memFree(getDagOutput);
        getDagOutput = NULL;
    }

    if (workItem != NULL) {
        Status tmpStatus;
        xcalarApiFreeWorkItem(workItem);
        workItem = xcalarApiMakeQueryDeleteWorkItem(retinaName);
        if (workItem == NULL) {
            return status;
        }
        tmpStatus = processWorkItem(userId, sessionGraph, workItem);
        if (tmpStatus != StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "queryDelete(%s, %p, %s, %d, %p, %s) failed: %s",
                    userId->userIdName,
                    sessionGraph,
                    tableName,
                    numCols,
                    columns,
                    retinaName,
                    strGetFromStatus(status));
        }
        xcalarApiFreeWorkItem(workItem);

        if (!KeepRetinas) {
            workItem = xcalarApiMakeDeleteRetinaWorkItem(retinaName);
            if (workItem == NULL) {
                return status;
            }

            tmpStatus = processWorkItem(userId, sessionGraph, workItem);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogInfo,
                        "deleteRetina(%s, %p, %s, %d, %p, %s) failed: %s",
                        userId->userIdName,
                        sessionGraph,
                        tableName,
                        numCols,
                        columns,
                        retinaName,
                        strGetFromStatus(status));
            }
            xcalarApiFreeWorkItem(workItem);

            if (strlen(tmpRetinaName) > 0) {
                workItem = xcalarApiMakeDeleteRetinaWorkItem(tmpRetinaName);
                if (workItem == NULL) {
                    return status;
                }

                tmpStatus = processWorkItem(userId, sessionGraph, workItem);
                if (tmpStatus != StatusOk) {
                    xSyslog(moduleName,
                            XlogInfo,
                            "deleteRetina(%s, %p, %s, %d, %p, %s) failed: %s",
                            userId->userIdName,
                            sessionGraph,
                            tableName,
                            numCols,
                            columns,
                            retinaName,
                            strGetFromStatus(status));
                }

                xcalarApiFreeWorkItem(workItem);
            }
        }
        workItem = NULL;
    }

    return status;
}

static Status
workerQuery(XcalarApiUserId *userId,
            Dag *sessionGraph,
            const char *tableName,
            RandWeakHandle *randHandle)
{
    Status status = StatusUnknown;
    char queryName[XcalarApiMaxFileNameLen + 1];
    char dstTable[XcalarApiMaxTableNameLen + 1];
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeName name;
    strlcpy(name, tableName, sizeof(name));
    Dag *queryGraph = NULL;
    char *queryStr = NULL;
    size_t queryStrLen;
    xcalar::compute::localtypes::Dataflow::ExecuteRequest executeRequest;
    xcalar::compute::localtypes::Dataflow::ExecuteResponse executeResponse;
    Runtime::SchedId schedId = static_cast<Runtime::SchedId>(
        rndWeakGenerate64(randHandle) % Runtime::TotalSdkScheds);
    Runtime *rt = Runtime::get();
    const char *schedName = rt->getSchedNameFromId(schedId);

    if (tableName[0] == '\0') {
        return StatusOk;
    }

    status = getUniqueName("query-", queryName, sizeof(queryName));
    BailIfFailed(status);

    status = sessionGraph->cloneDag(&queryGraph,
                                    DagTypes::QueryGraph,
                                    sessionGraph->getSessionContainer(),
                                    1,
                                    &name,
                                    0,
                                    NULL,
                                    Dag::ConvertUDFNamesToRelative);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to clone dag for table %s",
                    tableName);

    DagTypes::NodeId queryGraphNodeId;
    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    BailIfFailed(status);

    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        char oldName[XcalarApiMaxTableNameLen + 1];
        char newName[XcalarApiMaxTableNameLen + 1];

        status = queryGraph->getDagNodeName(queryGraphNodeId,
                                            oldName,
                                            sizeof(oldName));
        BailIfFailed(status);

        status = getUniqueName("table-", newName, sizeof(newName));
        BailIfFailed(status);

        status = queryGraph->renameDagNodeLocalEx(oldName, newName);
        BailIfFailed(status);

        if (strcmp(oldName, tableName) == 0) {
            strlcpy(dstTable, newName, sizeof(dstTable));
        }

        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        BailIfFailed(status);
    }

    status =
        QueryParser::get()->reverseParse(queryGraph, &queryStr, &queryStrLen);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to reverse parse dag for table %s",
                    tableName);

    xSyslog(moduleName,
            XlogInfo,
            "starting query(%s, %p, %s, %s)",
            userId->userIdName,
            sessionGraph,
            tableName,
            queryName);

    try {
        executeRequest.set_dataflow_str(queryStr);
        executeRequest.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(userId->userIdName);
        executeRequest.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(userId->userIdName);
        executeRequest.set_job_name(queryName);
        executeRequest.set_is_async(false);
        executeRequest.set_sched_name(schedName);
        executeRequest.set_collect_stats(false);
        executeRequest.set_pin_results(false);
        executeRequest.set_optimized(false);
    } catch (...) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate execute request");
        goto CommonExit;
    }

    status =
        QueryManager::get()->processDataflow(&executeRequest, &executeResponse);
    BailIfFailedMsg(moduleName,
                    status,
                    "failed to execute query(%s, %p, %s, %s)",
                    userId->userIdName,
                    sessionGraph,
                    tableName,
                    queryName);

    uint64_t rowCountSrc;
    status = getRowCount(sessionGraph, tableName, &rowCountSrc);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    uint64_t rowCountDst;
    status = getRowCount(sessionGraph, dstTable, &rowCountDst);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error geting row count for %s: %s",
                dstTable,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogWarn,
            "user %s: executeQuery %s succeeded. "
            "DstTable: %s has %lu rows. OrigTable: %s has %lu rows",
            userId->userIdName,
            queryName,
            dstTable,
            rowCountDst,
            tableName,
            rowCountSrc);

    if (rowCountSrc != rowCountDst) {
        XcalarApiOutput *queryStateOutput;

        size_t outSize;

        // Get the queryStateOutput so that it is available in the core.
        status = QueryManager::get()->requestQueryState(&queryStateOutput,
                                                        &outSize,
                                                        queryName,
                                                        true);
        (void) status;

        assert(0);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    Status tmpStatus;
    workItem = xcalarApiMakeQueryDeleteWorkItem(queryName);
    if (workItem == NULL) {
        return status;
    }
    tmpStatus = processWorkItem(userId, sessionGraph, workItem);
    if (tmpStatus != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "queryDelete(%s, %p, %s, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                tableName,
                queryName,
                strGetFromStatus(status));
    }
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;
    if (queryStr != NULL) {
        memFreeJson(queryStr);
        queryStr = NULL;
    }
    return status;
}

struct WorkerThreadArgs {
    RandWeakHandle randHandle;
    XcalarApiUserId *employer;
    Dag *sessionGraph;
    Semaphore *workerSem;
    bool semInited;
    Semaphore personalSem;
    bool exitNow;
    bool isAvailable;
    XcalarApis task;
    Atomic64 *indexJobRan;
    Atomic64 *mapJobRan;
    Atomic64 *loadJobRan;
    // When we start having ops that take in n tables as input,
    // where n >> 2, then just listing all the tableNames here
    // might be a bad idea
    char tableName[XcalarApiMaxTableNameLen + 1];
    char tableName2[XcalarApiMaxTableNameLen + 1];
};

struct UserThreadArgs {
    RandWeakHandle randHandle;
    bool permissionToGo;
    XcalarApiUserId userId;
    Dag *sessionGraph;
    bool isLoader;
    Semaphore startSem;
    bool semInited;
};

// Is it serious enough to let our boss know?
static inline bool
isFatalError(Status status)
{
    return status != StatusOk && status != StatusNoMem &&
           status != StatusDgNodeInUse && status != StatusJoinTypeMismatch &&
           status != StatusCanceled && status != StatusNsNotFound &&
           status != StatusAggregateNoSuchField &&
           status != StatusDagNodeDropped;
}

static void *
workerMain(void *threadArgsIn)
{
    WorkerThreadArgs *threadArgs = (WorkerThreadArgs *) threadArgsIn;
    Status status = StatusOk;
    Status tmpStatus = StatusOk;
    AggregateResult *aggResult = NULL;
    XcalarWorkItem *workItem = NULL;
    char dstTable[XcalarApiMaxTableNameLen + 1];

    xSyslog(moduleName,
            XlogInfo,
            "Hello! I work for %s.",
            threadArgs->employer->userIdName);

    while (!threadArgs->exitNow) {
        // Let my employer know I'm available
        threadArgs->isAvailable = true;
        threadArgs->workerSem->post();

        // Wait for him to assign me work
        threadArgs->personalSem.semWait();

        // Employer woke me up. Am I fired?
        if (threadArgs->exitNow) {
            break;
        }

        // Nope. Time to work
        xSyslog(moduleName,
                XlogNote,
                "Received orders to perform %s",
                strGetFromXcalarApis(threadArgs->task));

        const char *tables[2];
        tables[0] = (const char *) threadArgs->tableName;
        tables[1] = (const char *) threadArgs->tableName2;

        workItem = xcalarApiMakeArchiveTablesWorkItem(false, false, 2, tables);
        if (workItem == NULL) {
            status = StatusNoMem;
            continue;
        }

        // proceed whether or not the unarchive worked
        processWorkItem(threadArgs->employer,
                        threadArgs->sessionGraph,
                        workItem);

        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;

        switch (threadArgs->task) {
        case XcalarApiCancelOp:
            tmpStatus = cancelOperations(threadArgs->employer,
                                         threadArgs->sessionGraph,
                                         &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while cancelling operations: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiBulkLoad:
            tmpStatus = workerLoad(threadArgs->employer,
                                   threadArgs->sessionGraph,
                                   threadArgs->loadJobRan,
                                   &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while doing load: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiMap:
            tmpStatus = workerMap(threadArgs->employer,
                                  threadArgs->sessionGraph,
                                  threadArgs->tableName,
                                  threadArgs->mapJobRan,
                                  &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while doing map: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiJoin:
            tmpStatus = workerJoin(threadArgs->employer,
                                   threadArgs->sessionGraph,
                                   threadArgs->tableName,
                                   threadArgs->tableName2,
                                   &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while performing join: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiUnion:
            tmpStatus = workerUnion(threadArgs->employer,
                                    threadArgs->sessionGraph,
                                    threadArgs->tableName,
                                    threadArgs->tableName2,
                                    &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while performing union: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiDeleteObjects:
            tmpStatus = scrubAndDeleteTables(threadArgs->employer,
                                             threadArgs->sessionGraph,
                                             &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while scrubbing the tables: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiGroupBy:
            tmpStatus = workerGroupBy(threadArgs->employer,
                                      threadArgs->sessionGraph,
                                      threadArgs->tableName,
                                      &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while grouping by: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiIndex:
            tmpStatus = workerIndex(threadArgs->employer,
                                    threadArgs->sessionGraph,
                                    threadArgs->tableName,
                                    threadArgs->indexJobRan,
                                    &threadArgs->randHandle);
            if (isFatalError(tmpStatus) && status == StatusOk) {
                status = tmpStatus;
            }

            break;

        case XcalarApiFilter:
            tmpStatus = workerFilter(threadArgs->employer,
                                     threadArgs->sessionGraph,
                                     threadArgs->tableName,
                                     &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while filtering table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiProject:
            tmpStatus = projectRandomCols(threadArgs->employer,
                                          threadArgs->sessionGraph,
                                          threadArgs->tableName,
                                          &threadArgs->randHandle,
                                          dstTable,
                                          PctChanceOfColDrop);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while projecting table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiAggregate:
            aggResult =
                (AggregateResult *) memAllocExt(sizeof(*aggResult), moduleName);

            new (aggResult) AggregateResult();
            tmpStatus = workerAggregate(threadArgs->employer,
                                        threadArgs->sessionGraph,
                                        threadArgs->tableName,
                                        &threadArgs->randHandle,
                                        aggResult);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while aggregating table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            if (tmpStatus == StatusOk) {
                // save result to be used in subsequent maps
                aggregateHashTableLock.lock();
                tmpStatus = aggregateHashTable.insert(aggResult);
                assert(tmpStatus == StatusOk);
                aggregateHashTableLock.unlock();
            } else {
                if (aggResult->output) {
                    memFree(aggResult->output);
                    aggResult->output = NULL;
                }
                memFree(aggResult);
            }

            break;

        case XcalarApiGetRowNum:
            char dstColName[XcalarApiMaxFieldNameLen + 1];
            status = getUniqueName("col", dstColName, sizeof(dstColName));
            if (status != StatusOk) {
                break;
            }

            tmpStatus = getRowNum(threadArgs->employer,
                                  threadArgs->sessionGraph,
                                  threadArgs->tableName,
                                  dstColName,
                                  dstTable);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while getting row num on table %s: "
                        "%s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiExport:
            tmpStatus = exportTableRandomCols(threadArgs->employer,
                                              threadArgs->sessionGraph,
                                              threadArgs->tableName,
                                              &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while exporting table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiExecuteRetina:
            tmpStatus = workerRetina(threadArgs->employer,
                                     threadArgs->sessionGraph,
                                     threadArgs->tableName,
                                     &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing retina table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiQuery:
            tmpStatus = workerQuery(threadArgs->employer,
                                    threadArgs->sessionGraph,
                                    threadArgs->tableName,
                                    &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing query table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiRenameNode:
            tmpStatus = workerRename(threadArgs->employer,
                                     threadArgs->sessionGraph,
                                     threadArgs->tableName);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing rename table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiArchiveTables:
            tmpStatus = workerArchive(threadArgs->employer,
                                      threadArgs->sessionGraph,
                                      threadArgs->tableName);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing archive table %s: %s",
                        threadArgs->tableName,
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiPublish:
            tmpStatus = workerPublish(threadArgs->employer,
                                      threadArgs->sessionGraph,
                                      &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing publish table: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiSelect:
            tmpStatus = workerSelect(threadArgs->employer,
                                     threadArgs->sessionGraph,
                                     &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing select on publish table: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiRestoreTable:
            tmpStatus = workerRestore(threadArgs->employer,
                                      threadArgs->sessionGraph,
                                      &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing restore table: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        case XcalarApiRuntimeSetParam:
            tmpStatus = workerChangeRuntimeParams(threadArgs->employer,
                                                  threadArgs->sessionGraph,
                                                  &threadArgs->randHandle);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing change runtime params: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            tmpStatus = workerChangeRuntimeParams(threadArgs->employer,
                                                  threadArgs->sessionGraph,
                                                  &threadArgs->randHandle,
                                                  true);
            if (tmpStatus != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error while executing change runtime params: %s",
                        strGetFromStatus(tmpStatus));
                if (isFatalError(tmpStatus) && status == StatusOk) {
                    status = tmpStatus;
                }
            }

            break;

        default:
            xSyslog(moduleName,
                    XlogErr,
                    "I'm not paid enough to do %s",
                    strGetFromXcalarApis(threadArgs->task));
            break;
        }
    }

    xSyslog(moduleName, XlogInfo, "Goodbye");

    return (void *) status.code();
}

static void
delegateOp(const char *tableName,
           const char *tableName2,
           XcalarApis task,
           pthread_t workerThreads[],
           WorkerThreadArgs workerThreadArgs[],
           unsigned numWorkers,
           Semaphore *workerSem)
{
    unsigned ii;

    // Wait for an availble worker
    workerSem->semWait();

    // At least 1 worker is available since we woke up
    for (ii = 0; ii < numWorkers; ii++) {
        if (workerThreadArgs[ii].isAvailable) {
            // Give him his orders
            workerThreadArgs[ii].task = task;
            strlcpy(workerThreadArgs[ii].tableName,
                    tableName,
                    sizeof(workerThreadArgs[ii].tableName));
            strlcpy(workerThreadArgs[ii].tableName2,
                    tableName2,
                    sizeof(workerThreadArgs[ii].tableName2));
            workerThreadArgs[ii].isAvailable = false;
            xSyslog(moduleName,
                    XlogNote,
                    "Giving %s on tables [%s, %s] to %lu",
                    strGetFromXcalarApis(task),
                    tableName,
                    tableName2,
                    workerThreads[ii]);
            workerThreadArgs[ii].personalSem.post();
            return;
        }
    }

    NotReached();
}

static void *
userMain(void *threadArgsIn)
{
    UserThreadArgs *threadArgs = (UserThreadArgs *) threadArgsIn;
    WorkerThreadArgs *workerThreadArgs = NULL;
    pthread_t *workerThreads = NULL;
    Status status = StatusOk;
    uint64_t ii, jj;
    Semaphore workerSem;
    unsigned numWorkerThreadsSpawned = 0;
    Atomic64 indexJobRan;
    Atomic64 mapJobRan;
    Atomic64 loadJobRan;
    char dstTable1[XcalarApiMaxTableNameLen + 1];
    bool datasetLoaded = false;
    char datasetPattern[XcalarApiMaxTableNameLen + 1];
    size_t ret;

    XcalarApis ops[] = {
        XcalarApiBulkLoad,
        XcalarApiMap,
        XcalarApiFilter,
        XcalarApiGroupBy,
        XcalarApiAggregate,
        XcalarApiProject,
        // Disabled due to Xc-8791. Generating a row num will assign different
        // row:number matching in interactive and LRQ, invalidating the
        // workerRetina's correctness checking
        // XcalarApiGetRowNum,
        XcalarApiIndex,
        XcalarApiJoin,
        XcalarApiUnion,
        XcalarApiExport,
        XcalarApiCancelOp,
        XcalarApiExecuteRetina,
        XcalarApiQuery,
        XcalarApiRenameNode,
        XcalarApiPublish,
        XcalarApiSelect,
        XcalarApiRestoreTable,
        XcalarApiRuntimeSetParam,
    };

    threadArgs->startSem.semWait();
    if (!threadArgs->permissionToGo) {
        status = StatusOk;
        goto CommonExit;
    }

    // All righty! First day of work. What do you got for me?
    xSyslog(moduleName,
            XlogNote,
            "Hello from thread %s",
            threadArgs->userId.userIdName);

    atomicWrite64(&indexJobRan, rand() % IndexMaxRandomArgs);
    atomicWrite64(&mapJobRan, rand() % MapMaxRandomArgs);
    atomicWrite64(&loadJobRan, rand() % LoadMaxRandomArgs);

    if (threadArgs->isLoader) {
        // I've been volunteered to do a load. Shall name the dataset
        // after me.
        switch (atomicInc64(&loadJobRan) % LoadMaxRandomArgs) {
        case LoadDefault:
            status = load(&threadArgs->userId,
                          threadArgs->sessionGraph,
                          DatasetPath,
                          DatasetType,
                          DatasetSize,
                          threadArgs->userId.userIdName,
                          LoadMemUdfs::LoadMemInvalid,
                          NULL);
            break;

        case LoadJson:
            status = load(&threadArgs->userId,
                          threadArgs->sessionGraph,
                          YelpDatasetPath,
                          YelpDatasetType,
                          YelpDatasetSize,
                          threadArgs->userId.userIdName,
                          LoadMemUdfs::LoadMemInvalid,
                          NULL);
            break;

        case LoadMemory:
            status = load(&threadArgs->userId,
                          threadArgs->sessionGraph,
                          MemoryDatasetPath,
                          MemoryDatasetType,
                          MemoryDatasetSize,
                          threadArgs->userId.userIdName,
                          LoadMemUdfs::LoadMemDefault,
                          NULL);
            break;

        default:
            assert(0);
            break;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "%s: Could not perform load: %s",
                    threadArgs->userId.userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        datasetLoaded = true;
    }

    // Let's start hiring and building our team
    errno = 0;
    new (&workerSem) Semaphore(0);

    workerThreads =
        (pthread_t *) memAlloc(sizeof(*workerThreads) * NumWorkersPerUser);
    if (workerThreads == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "%s: Insufficient memory to allocate workerThreads "
                "(numWorkers: %u)",
                threadArgs->userId.userIdName,
                NumWorkersPerUser);
        goto CommonExit;
    }

    workerThreadArgs = (WorkerThreadArgs *) memAlloc(sizeof(*workerThreadArgs) *
                                                     NumWorkersPerUser);
    if (workerThreadArgs == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "%s: Insufficient memory to allocate "
                "workerThreadArgs (numWorkers: %u)",
                threadArgs->userId.userIdName,
                NumWorkersPerUser);
        goto CommonExit;
    }

    for (ii = 0; ii < NumWorkersPerUser; ii++) {
        workerThreadArgs[ii].semInited = false;
    }

    for (ii = 0; ii < NumWorkersPerUser; ii++) {
        errno = 0;
        new (&workerThreadArgs[ii].personalSem) Semaphore(0);
        workerThreadArgs[ii].semInited = true;
        workerThreadArgs[ii].workerSem = &workerSem;
        workerThreadArgs[ii].exitNow = false;
        workerThreadArgs[ii].isAvailable = false;
        workerThreadArgs[ii].indexJobRan = &indexJobRan;
        workerThreadArgs[ii].mapJobRan = &mapJobRan;
        workerThreadArgs[ii].loadJobRan = &loadJobRan;
        workerThreadArgs[ii].employer = &threadArgs->userId;
        workerThreadArgs[ii].sessionGraph = threadArgs->sessionGraph;
        rndInitWeakHandle(&workerThreadArgs[ii].randHandle,
                          rndWeakGenerate64(&threadArgs->randHandle));

        errno = 0;
        status = Runtime::get()->createBlockableThread(&workerThreads[ii],
                                                       NULL,
                                                       workerMain,
                                                       &workerThreadArgs[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
        numWorkerThreadsSpawned++;
    }

    // Let's pick a random dataset to index, as a starting point
    status = indexRandomDataset(&threadArgs->userId,
                                threadArgs->sessionGraph,
                                &threadArgs->randHandle,
                                dstTable1,
                                dstTable1);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Could not index random dataset: %s",
                threadArgs->userId.userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    atomicInc64(&usersReadyCount);

    // We have 1 table in our session now. We can start doing work on it
    for (ii = 0; ii < NumCycles; ii++) {
        char tableName[XcalarApiMaxTableNameLen + 1];
        char tableName2[XcalarApiMaxTableNameLen + 1];
        status = getRandomTable(threadArgs->sessionGraph,
                                tableName,
                                sizeof(tableName) - 1,
                                &threadArgs->randHandle);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "%s: Could not get random table: %s",
                    threadArgs->userId.userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // Note that this 2nd table could be the same table as our first
        // table. That's fine. Joins should be capable of self-join.
        status = getRandomTable(threadArgs->sessionGraph,
                                tableName2,
                                sizeof(tableName2) - 1,
                                &threadArgs->randHandle);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "%s: Could not get 2nd random table: %s",
                    threadArgs->userId.userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        for (jj = 0; jj < ArrayLen(ops); jj++) {
            // Get some of our workers on it. This function might block
            delegateOp(tableName,
                       tableName2,
                       ops[jj],
                       workerThreads,
                       workerThreadArgs,
                       NumWorkersPerUser,
                       &workerSem);
        }

        if (ii % GarbageCollectionCycle == 0) {
            delegateOp("",
                       "",
                       XcalarApiDeleteObjects,
                       workerThreads,
                       workerThreadArgs,
                       NumWorkersPerUser,
                       &workerSem);
        }
    }
CommonExit:
    if (workerThreadArgs != NULL) {
        for (ii = 0; ii < numWorkerThreadsSpawned; ii++) {
            void *tmp;  // Because sizeof(void *) > sizeof(status)
            Status tmpStatus;
            workerThreadArgs[ii].exitNow = true;
            workerThreadArgs[ii].personalSem.post();
            sysThreadJoin(workerThreads[ii], &tmp);
            tmpStatus.fromStatusCode((StatusCode)(uintptr_t) tmp);
            if (tmpStatus != StatusOk && status == StatusOk) {
                status = tmpStatus;
            }
        }

        for (ii = 0; ii < NumWorkersPerUser; ii++) {
            if (workerThreadArgs[ii].semInited) {
                workerThreadArgs[ii].personalSem.~Semaphore();
                workerThreadArgs[ii].semInited = false;
            }
        }

        memFree(workerThreadArgs);
        workerThreadArgs = NULL;
    }

    if (workerThreads != NULL) {
        memFree(workerThreads);
        workerThreads = NULL;
    }

    if (datasetLoaded) {
        // Clean up the main dataset that was loaded
        assert(threadArgs->isLoader);
        unloadDatasets(&threadArgs->userId,
                       threadArgs->sessionGraph,
                       threadArgs->userId.userIdName);
    }

    // Clean up any workerLoad datasets created by this "user"
    ret = snprintf(datasetPattern,
                   sizeof(datasetPattern),
                   "ds-%s*",
                   threadArgs->userId.userIdName);
    assert(ret < sizeof(datasetPattern));
    unloadDatasets(&threadArgs->userId,
                   threadArgs->sessionGraph,
                   datasetPattern);

    workerUnpublishAll(&threadArgs->userId, threadArgs->sessionGraph, NULL);

    xSyslog(moduleName,
            XlogNote,
            "%s: Goodbye!",
            threadArgs->userId.userIdName);

    return (void *) status.code();
}

static void *
childrenKillerThread(void *threadArgsIn)
{
    Parent *par = Parent::get();
    Status status;

    while (childrenKillerActive) {
        status = sysSemTimedWait(&childrenKillerSem, ChildrenKillerTimeout);
        if (status == StatusOk || status == StatusTimedOut) {
            if (atomicRead64(&usersReadyCount) == NumUsers) {
                par->killAllChildren();
            }
            continue;
        } else {
            break;
        }
    }

    return (void *) NULL;
}

Status
operatorsFuncTestBasicMain()
{
    UserThreadArgs *userThreadArgs = NULL;
    pthread_t *userThreads = NULL;
    Status status = StatusUnknown;
    unsigned ii, jj, numLoaders = 0, numUsersInited = 0;
    uint64_t seed;
    RandWeakHandle masterRandHandle;
    int ret;
    SourceType srcTypes[] = {SrcTable, SrcConstant, SrcExport, SrcDataset};

    LoadMemUdfs::initLoadMemUdfs();

    if (DatasetPath[0] == '\0') {
        size_t ret;
        ret = snprintf(DatasetPath,
                       sizeof(DatasetPath),
                       "%s/gdelt-small",
                       qaGetQaDir());
        assert(ret <= sizeof(DatasetPath));
    }

    if (YelpDatasetPath[0] == '\0') {
        size_t ret;
        ret = snprintf(YelpDatasetPath,
                       sizeof(YelpDatasetPath),
                       "%s/yelp/user",
                       qaGetQaDir());
        assert(ret <= sizeof(YelpDatasetPath));
    }

    verify(sem_init(&childrenKillerSem, 0, 0) == 0);
    childrenKillerActive = true;
    atomicWrite64(&usersReadyCount, 0);
    status = Runtime::get()->createBlockableThread(&childrenKillerTid,
                                                   NULL,
                                                   childrenKillerThread,
                                                   NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "createBlockableThread failed: %s",
                strGetFromStatus(status));
    }
    assert(status == StatusOk);
    BailIfFailed(status);

    assert(NumLoaders <= NumUsers);

    if (RandomSeed == UseDefaultSeed) {
        struct timespec ts;
        ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        if (ret == 0) {
            seed = clkTimespecToNanos(&ts);
        } else {
            seed = sysGetTid();
        }
    } else {
        seed = RandomSeed;
    }

    addLoadMemoryTarget();

    xSyslog(moduleName,
            XlogInfo,
            "Seed used for this run: %lu. NumUsers: %u",
            seed,
            NumUsers);
    rndInitWeakHandle(&masterRandHandle, seed);

    userThreads = (pthread_t *) memAlloc(sizeof(*userThreads) * NumUsers);
    if (userThreads == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate userThreads "
                "(numUsers: %u)",
                NumUsers);
        status = StatusNoMem;
        goto CommonExit;
    }

    userThreadArgs =
        (UserThreadArgs *) memAlloc(sizeof(*userThreadArgs) * NumUsers);
    if (userThreadArgs == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate userThreadArgs "
                "(numUsers: %u)",
                NumUsers);
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < NumUsers; ii++) {
        userThreadArgs[ii].sessionGraph = NULL;
        userThreadArgs[ii].semInited = false;
        userThreadArgs[ii].permissionToGo = false;
        new (&userThreadArgs[ii].startSem) Semaphore(0);
    }

    numUsersInited = 0;
    numLoaders = 0;
    for (ii = 0; ii < NumUsers; ii++) {
        rndInitWeakHandle(&userThreadArgs[ii].randHandle,
                          rndWeakGenerate64(&masterRandHandle));

        errno = 0;

        snprintf(userThreadArgs[ii].userId.userIdName,
                 sizeof(userThreadArgs[ii].userId.userIdName),
                 "%s%u-node%u",
                 UserIdPrefix,
                 ii,
                 Config::get()->getMyNodeId());
        userThreadArgs[ii].userId.userIdUnique =
            (uint32_t) hashStringFast(userThreadArgs[ii].userId.userIdName);
        xSyslog(moduleName,
                XlogNote,
                "Username: %s, UserIdUnique: %u",
                userThreadArgs[ii].userId.userIdName,
                userThreadArgs[ii].userId.userIdUnique);

        status = UserMgr::get()->getDag(&userThreadArgs[ii].userId,
                                        NULL,
                                        &userThreadArgs[ii].sessionGraph);
        if (status == StatusSessionNotFound ||
            status == StatusSessionUsrNotExist) {
            status = createSession(&userThreadArgs[ii].userId);
            // A new session is born inactive; so activate it first
            // If the session already exists, activate it (this may fail if
            // it's already active - probably need to add code to ignore such
            // a failure)
            if (status == StatusOk || status == StatusSessionExists) {
                status = activateSession(&userThreadArgs[ii].userId);
            }
            if (status != StatusOk) {
                break;
            }
            status = UserMgr::get()->getDag(&userThreadArgs[ii].userId,
                                            NULL,
                                            &userThreadArgs[ii].sessionGraph);
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "getDag returned status: %s",
                    strGetFromStatus(status));
            break;
        }

        for (unsigned uu = LoadMemUdfs::LoadMemSimple;
             uu < LoadMemUdfs::LoadMemMax;
             uu++) {
            addLoadMemoryUdf((LoadMemUdfs::LoadMemType) uu,
                             userThreadArgs[ii].sessionGraph);
        }

        if (numLoaders < NumLoaders) {
            userThreadArgs[ii].isLoader = true;
            numLoaders++;
        } else {
            userThreadArgs[ii].isLoader = false;
        }

        errno = 0;
        status = Runtime::get()->createBlockableThread(&userThreads[ii],
                                                       NULL,
                                                       userMain,
                                                       &userThreadArgs[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
        numUsersInited++;
    }

    if (numUsersInited < NumUsers) {
        assert(status != StatusOk);
        for (ii = 0; ii < numUsersInited; ii++) {
            userThreadArgs[ii].permissionToGo = false;
            userThreadArgs[ii].startSem.post();
            sysThreadJoin(userThreads[ii], NULL);
        }
        goto CommonExit;
    }

    assert(numUsersInited == NumUsers);
    for (ii = 0; ii < numUsersInited; ii++) {
        userThreadArgs[ii].permissionToGo = true;
        userThreadArgs[ii].startSem.post();
    }

    status = StatusOk;
    for (ii = 0; ii < numUsersInited; ii++) {
        Status threadStatus;
        sysThreadJoin(userThreads[ii], (void **) &threadStatus);
        // Just convert every status to StatusOk now to get operatorsFuncTest
        // working. We'll log it if a serious error occurred
        if (isFatalError(threadStatus)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Terminating with fatal error: %s",
                    strGetFromStatus(threadStatus));
        }
        threadStatus = StatusOk;

        if (threadStatus != StatusOk && status == StatusOk) {
            status = threadStatus;
        }
    }

    BailIfFailed(status);

CommonExit:
    if (userThreadArgs != NULL) {
        for (ii = 0; ii < NumUsers; ii++) {
            if (userThreadArgs[ii].semInited) {
                userThreadArgs[ii].startSem.~Semaphore();
                userThreadArgs[ii].semInited = false;
            }

            if (userThreadArgs[ii].sessionGraph != NULL) {
                Dag *sessionGraph = userThreadArgs[ii].sessionGraph;
                Status tmpStatus;
                XcalarApiOutput *output = NULL;
                size_t outputSize;

                for (jj = 0; jj < ArrayLen(srcTypes); jj++) {
                    tmpStatus = sessionGraph->bulkDropNodes("*",
                                                            &output,
                                                            &outputSize,
                                                            srcTypes[jj],
                                                            &testUserId);
                    if (tmpStatus != StatusOk) {
                        xSyslog(moduleName,
                                XlogErr,
                                "Error while dropping *. SrcType: %s, "
                                "Status: %s",
                                strGetFromSourceType(srcTypes[jj]),
                                strGetFromStatus(tmpStatus));
                        if (status == StatusOk) {
                            status = tmpStatus;
                        }
                    }

                    if (output != NULL) {
                        memFree(output);
                        output = NULL;
                    }
                }
                workerUnpublishAll(&userThreadArgs[ii].userId,
                                   userThreadArgs[ii].sessionGraph,
                                   NULL);
            }
        }
        memFree(userThreadArgs);
        userThreadArgs = NULL;
    }

    if (userThreads != NULL) {
        memFree(userThreads);
        userThreads = NULL;
    }

    if (childrenKillerActive) {
        childrenKillerActive = false;
        memBarrier();
        verify(sem_post(&childrenKillerSem) == 0);
        sysThreadJoin(childrenKillerTid, NULL);
    }

    verify(sem_destroy(&childrenKillerSem) == 0);

    if (status == StatusConnReset) {
        // XPU killer thread may kill XPUs based on some policy and that's
        // an expected behavior of the product and is not an error scenario.
        // So just suppress the error code here.
        status = StatusOk;

        // XXX Note that may be in the future, we may change what error code
        // XPU pops up.
    }

    return status;
}

Status
operatorsStressParseConfig(Config::Configuration *config,
                           char *key,
                           char *value,
                           bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibOperatorsFuncTestConfig(
                       LibOperatorsGarbageCollectionCycle)) == 0) {
        GarbageCollectionCycle = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsNumCycles)) == 0) {
        NumCycles = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsNumCyclesPubTable)) == 0) {
        NumCyclesPubTable = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsNumUsers)) == 0) {
        NumUsers = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsNumWorkersPerUser)) == 0) {
        NumWorkersPerUser = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsNumLoaders)) == 0) {
        NumLoaders = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsDatasetPath)) == 0) {
        strlcpy(DatasetPath, value, sizeof(DatasetPath));
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsYelpDatasetPath)) == 0) {
        strlcpy(YelpDatasetPath, value, sizeof(YelpDatasetPath));
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsMemoryDatasetPath)) == 0) {
        strlcpy(MemoryDatasetPath, value, sizeof(MemoryDatasetPath));
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsDatasetSize)) == 0) {
        DatasetSize = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfTableDrop)) == 0) {
        PctChanceOfTableDrop = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfDatasetDrop)) == 0) {
        PctChanceOfDatasetDrop = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfPubTableDrop)) == 0) {
        PctChanceOfPubTableDrop = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfPubTableDel)) == 0) {
        PctChanceOfPubTableDelete = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfVariableDrop)) == 0) {
        PctChanceOfVariableDrop = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfColDrop)) == 0) {
        PctChanceOfColDrop = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfColExport)) == 0) {
        PctChanceOfColExport = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfAscendingSort)) == 0) {
        PctChanceOfAscendingSort = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsPctChanceOfCancel)) == 0) {
        PctChanceOfCancel = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsKeepRetinas)) == 0) {
        KeepRetinas = true;
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsChildrenKillerTimeoutSecs)) == 0) {
        ChildrenKillerTimeout = USecsPerSec * strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsFNFPercent)) == 0) {
        OperatorsFNFPercent = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsMaxColsLoadMemWithSchema)) == 0) {
        OpMaxColsLoadMemWithSchema = strtoll(value, NULL, 0);
        if (OpMaxColsLoadMemWithSchema == 0 ||
            OpMaxColsLoadMemWithSchema > TupleMaxNumValuesPerRecord) {
            OpMaxColsLoadMemWithSchema = OpDefaultColsLoadMemWithSchema;
        }
    } else if (strcasecmp(key,
                          strGetFromLibOperatorsFuncTestConfig(
                              LibOperatorsMaxRowsToCheckForCols)) == 0) {
        MaxRowsToCheckForCols = strtoll(value, NULL, 0);
    }
    return status;
}

static Status
createNewPubTableFromTable(XcalarApiUserId *userId,
                           Dag *sessionGraph,
                           RandWeakHandle *randHandle,
                           const char *publishedTableName,
                           char *tableName)
{
    Status status;
    char keyCol[XcalarApiMaxFieldNameLen + 1];
    Ordering ordering = Unordered;
    char(*evalString)[XcalarApiMaxEvalStringLen + 1] = NULL;
    char newField[XcalarApiMaxFieldNameLen + 1];
    char *keyNames[1];

    evalString = new (std::nothrow) char[1][XcalarApiMaxEvalStringLen + 1];
    BailIfNull(evalString);

    snprintf(keyCol, sizeof(keyCol), "key");
    keyNames[0] = keyCol;

    status = getRowNum(userId, sessionGraph, tableName, keyCol, tableName);
    BailIfFailed(status);

    status = project(userId, sessionGraph, tableName, 1, &keyCol, tableName);
    BailIfFailed(status);

    snprintf(*evalString, XcalarApiMaxEvalStringLen + 1, "int(key)");
    snprintf(newField, sizeof(newField), "int");

    status = map(userId,
                 sessionGraph,
                 tableName,
                 1,
                 evalString,
                 &newField,
                 false,
                 tableName);
    BailIfFailed(status);

    snprintf(*evalString, XcalarApiMaxEvalStringLen + 1, "string(key)");
    snprintf(newField, sizeof(newField), "string");

    status = map(userId,
                 sessionGraph,
                 tableName,
                 1,
                 evalString,
                 &newField,
                 false,
                 tableName);
    BailIfFailed(status);

    snprintf(*evalString, XcalarApiMaxEvalStringLen + 1, "float(key)");
    snprintf(newField, sizeof(newField), "float");

    status = map(userId,
                 sessionGraph,
                 tableName,
                 1,
                 evalString,
                 &newField,
                 false,
                 tableName);
    BailIfFailed(status);

    status = indexTable(userId,
                        sessionGraph,
                        tableName,
                        1,
                        (const char **) keyNames,
                        &ordering,
                        tableName);
    BailIfFailed(status);

    status = publishTable(userId, sessionGraph, tableName, publishedTableName);
    BailIfFailed(status);

CommonExit:
    if (evalString) {
        delete[] evalString;
    }
    return status;
}

static Status
createNewPubTableFromDs(XcalarApiUserId *userId,
                        Dag *sessionGraph,
                        RandWeakHandle *randHandle,
                        const char *publishedTableName,
                        const char *datasetName)
{
    Status status = StatusOk;
    char dstTable[XcalarApiMaxTableNameLen + 1];

    status = load(userId,
                  sessionGraph,
                  MemoryDatasetPath,
                  MemoryDatasetType,
                  MemoryDatasetSize,
                  datasetName,
                  LoadMemUdfs::LoadMemDefault,
                  NULL);
    BailIfFailed(status);

    status = indexRandomDataset(userId,
                                sessionGraph,
                                randHandle,
                                dstTable,
                                dstTable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s: Could not index random dataset: %s",
                userId->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = createNewPubTableFromTable(userId,
                                        sessionGraph,
                                        randHandle,
                                        publishedTableName,
                                        dstTable);
    BailIfFailed(status);

CommonExit:
    return status;
}

static Status
createUpdate(XcalarApiUserId *userId,
             Dag *sessionGraph,
             const char *publishedTableName,
             unsigned cycle,
             char updateTable[XcalarApiMaxTableNameLen + 1])
{
    Status status;
    char selectTable[XcalarApiMaxTableNameLen + 1];
    unsigned numEvals = 6;
    const char *keyCol = "key";
    Ordering ordering = Unordered;
    unsigned newKey = cycle + 1000;
    char(*evalString)[XcalarApiMaxEvalStringLen + 1] = NULL;
    char newField[numEvals][XcalarApiMaxFieldNameLen + 1];

    // unsigned tableIdx;
    char pubTablePattern[XcalarApiMaxFieldNameLen + 1];
    xcalar::compute::localtypes::PublishedTable::ListTablesResponse listTables;
    const xcalar::compute::localtypes::PublishedTable::ListTablesResponse::
        TableInfo *tableInfo;
    char(*columns)[XcalarApiMaxFieldNameLen + 1] = NULL;
    unsigned numCols = 0;

    // List the publish tables created by our session and pick a candidate.
    snprintf(pubTablePattern,
             sizeof(pubTablePattern),
             "%s",
             publishedTableName);

    status = listPubTable(userId, sessionGraph, pubTablePattern, listTables);
    BailIfFailedMsg(moduleName,
                    status,
                    "createUpdate: listPubTable pubTablePattern=%s status=%s",
                    pubTablePattern,
                    strGetFromStatus(status));
    if (listTables.tables_size() != 1) {
        xSyslog(moduleName,
                XlogErr,
                "createUpdate: tables_size !=1; size=%d",
                listTables.tables_size());
        status = StatusInval;
        goto CommonExit;
    }

    tableInfo = &listTables.tables(0);

    columns = (char(*)[XcalarApiMaxFieldNameLen + 1])
        memAlloc(sizeof(*columns) * tableInfo->values_size());
    if (columns == NULL) {
        xSyslog(moduleName, XlogErr, "createUpdate: columns is NULL");
        status = StatusNoMem;
        goto CommonExit;
    }

    for (int ii = 0; ii < tableInfo->values_size(); ii++) {
        // skip XcalarBathcId column as the source table for the update
        // table call cannot have this column.
        if (strcmp(tableInfo->values(ii).name().c_str(),
                   XcalarBatchIdColumnName) == 0) {
            continue;
        }
        strlcpy(columns[numCols++],
                tableInfo->values(ii).name().c_str(),
                sizeof(*columns));
    }

    evalString =
        new (std::nothrow) char[numEvals][XcalarApiMaxEvalStringLen + 1];
    BailIfNull(evalString);

    status = select(userId,
                    sessionGraph,
                    publishedTableName,
                    HashTree::InvalidBatchId,
                    "",
                    numCols,
                    columns,
                    selectTable);
    BailIfFailed(status);

    snprintf(evalString[0], XcalarApiMaxEvalStringLen + 1, "int(add(key, 1))");
    snprintf(newField[0], sizeof(newField[0]), "key");

    snprintf(evalString[1], XcalarApiMaxEvalStringLen + 1, "add(float, 1)");
    snprintf(newField[1], sizeof(newField[1]), "float");

    snprintf(evalString[2], XcalarApiMaxEvalStringLen + 1, "int(add(int, 1))");
    snprintf(newField[2], sizeof(newField[2]), "int");

    snprintf(evalString[3],
             XcalarApiMaxEvalStringLen + 1,
             "ifStr(eq(key, %d), string, string(div(1, 0)))",
             newKey);
    snprintf(newField[3], sizeof(newField[3]), "string");

    snprintf(evalString[4], XcalarApiMaxEvalStringLen + 1, "int(1)");
    snprintf(newField[4], sizeof(newField[4]), "XcalarRankOver");

    snprintf(evalString[5], XcalarApiMaxEvalStringLen + 1, "int(1)");
    snprintf(newField[5], sizeof(newField[5]), "XcalarOpCode");

    status = map(userId,
                 sessionGraph,
                 selectTable,
                 numEvals,
                 evalString,
                 newField,
                 false,
                 updateTable);
    BailIfFailed(status);

    status = indexTable(userId,
                        sessionGraph,
                        updateTable,
                        1,
                        &keyCol,
                        &ordering,
                        updateTable);
    BailIfFailed(status);

CommonExit:
    if (columns) {
        memFree(columns);
        columns = NULL;
    }

    if (evalString) {
        delete[] evalString;
        evalString = NULL;
    }
    return status;
}

// Each user creates a published table then starts randomly updating any
// available tables in the system
static void *
publishedTableUserMain(void *threadArgsIn)
{
    UserThreadArgs *threadArgs = (UserThreadArgs *) threadArgsIn;
    Atomic64 loadJobRan;
    Status status;
    unsigned numCols = 5;
    char *eval = NULL;
    uint64_t rowCountExpected[NumCyclesPubTable];
    char(*columns)[XcalarApiMaxFieldNameLen + 1] =
        (char(*)[XcalarApiMaxFieldNameLen + 1])
            memAlloc(sizeof(*columns) * numCols);
    BailIfNull(columns);

    snprintf(columns[0], sizeof(columns[0]), "key");
    snprintf(columns[1], sizeof(columns[1]), "int");
    snprintf(columns[2], sizeof(columns[2]), "float");
    snprintf(columns[3], sizeof(columns[3]), "string");
    snprintf(columns[4], sizeof(columns[4]), "XcalarOpCode");

    char fileName[XcalarApiMaxFileNameLen + 1];
    char publishedTable[XcalarApiMaxTableNameLen + 1];

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableUserMain: NumCyclesPubTable=%d",
            NumCyclesPubTable);
    eval = new (std::nothrow) char[XcalarApiMaxEvalStringLen + 1];
    BailIfNull(eval);

    status =
        getUniqueName(PubTablePrefix, publishedTable, sizeof(publishedTable));
    BailIfFailed(status);
    atomicWrite64(&loadJobRan, rand() % LoadMaxRandomArgs);

    status = createNewPubTableFromDs(&threadArgs->userId,
                                     threadArgs->sessionGraph,
                                     &threadArgs->randHandle,
                                     publishedTable,
                                     threadArgs->userId.userIdName);
    BailIfFailed(status);

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableUserMain: after createNewPubTableFromDs");
    for (unsigned cc = 1; cc <= NumCyclesPubTable; cc++) {
        char updateTable[XcalarApiMaxTableNameLen + 1];
        status = createUpdate(&threadArgs->userId,
                              threadArgs->sessionGraph,
                              publishedTable,
                              cc,
                              updateTable);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: createUpdate return status=%s",
                        strGetFromStatus(status));

        snprintf(fileName, sizeof(fileName), "update%d.csv", cc);
        status = exportTable(&threadArgs->userId,
                             threadArgs->sessionGraph,
                             updateTable,
                             numCols,
                             columns,
                             fileName);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: 1st exploreTable return "
                        "status=%s",
                        strGetFromStatus(status));

        status = update(&threadArgs->userId,
                        threadArgs->sessionGraph,
                        updateTable,
                        publishedTable,
                        NULL);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: update return status=%s",
                        strGetFromStatus(status));

        status = getRowCount(threadArgs->sessionGraph,
                             updateTable,
                             &rowCountExpected[cc - 1]);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: getRowCount return status=%s",
                        strGetFromStatus(status));

        status = unpublishTable(&threadArgs->userId,
                                threadArgs->sessionGraph,
                                publishedTable,
                                true);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: unpublishTable return "
                        "status=%s",
                        strGetFromStatus(status));

        status = restore(&threadArgs->userId,
                         threadArgs->sessionGraph,
                         publishedTable);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: restore return status=%s",
                        strGetFromStatus(status));

        status = changeOwner(&threadArgs->userId,
                             threadArgs->sessionGraph,
                             publishedTable);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: changeOwner return status=%s",
                        strGetFromStatus(status));
    }

    xSyslog(moduleName, XlogInfo, "publishedTableUserMain: after 1st for loop");
    for (unsigned cc = 1; cc <= NumCyclesPubTable; cc++) {
        xSyslog(moduleName,
                XlogInfo,
                "publishedTableUserMain: inside 2nd for loop cc=%u",
                cc);
        char selectTable[XcalarApiMaxTableNameLen + 1];
        status = select(&threadArgs->userId,
                        threadArgs->sessionGraph,
                        publishedTable,
                        cc,
                        "",
                        0,
                        NULL,
                        selectTable);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: select return status=%s",
                        strGetFromStatus(status));

        uint64_t rows;
        status = getRowCount(threadArgs->sessionGraph, selectTable, &rows);
        BailIfFailed(status);

        snprintf(fileName, sizeof(fileName), "test%d.csv", cc);
        status = exportTable(&threadArgs->userId,
                             threadArgs->sessionGraph,
                             selectTable,
                             numCols,
                             columns,
                             fileName);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: 2nd exportTable return "
                        "status=%s",
                        strGetFromStatus(status));
        if (rows != rowCountExpected[cc - 1] + 1) {
            assert(0 && "rows count must match");
        }

        char filterTable[XcalarApiMaxTableNameLen + 1];
        snprintf(eval, XcalarApiMaxEvalStringLen + 1, "eq(float, int)");

        status = filter(&threadArgs->userId,
                        threadArgs->sessionGraph,
                        selectTable,
                        eval,
                        filterTable);
        BailIfFailedMsg(moduleName,
                        status,
                        "publishedTableUserMain: filter return status=%s",
                        strGetFromStatus(status));

        status = getRowCount(threadArgs->sessionGraph, filterTable, &rows);
        BailIfFailed(status);

        if (rows != 1000 + cc) {
            status = exportTable(&threadArgs->userId,
                                 threadArgs->sessionGraph,
                                 selectTable,
                                 numCols,
                                 columns,
                                 fileName);
            BailIfFailedMsg(moduleName,
                            status,
                            "publishedTableUserMain: 3rd exportTable return "
                            "status=%s",
                            strGetFromStatus(status));
            assert(0);
        }
    }
    xSyslog(moduleName, XlogInfo, "publishedTableUserMain: after 2nd for loop");
    status =
        coalesce(&threadArgs->userId, threadArgs->sessionGraph, publishedTable);
    BailIfFailedMsg(moduleName,
                    status,
                    "publishedTableUserMain: coalesce return status=%s",
                    strGetFromStatus(status));

CommonExit:
    unpublishTable(&threadArgs->userId,
                   threadArgs->sessionGraph,
                   publishedTable,
                   false);
    if (eval) {
        delete[] eval;
    }

    unloadDatasets(&threadArgs->userId,
                   threadArgs->sessionGraph,
                   threadArgs->userId.userIdName);

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableUserMain: done status=%s",
            strGetFromStatus(status));
    return (void *) status.code();
}

Status
publishedTableTest()
{
    Status status;
    pthread_t userThreads[NumUsers];
    UserThreadArgs args[NumUsers];

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableTest: NumUsers=%d NumCyclesPubTable=%d",
            NumUsers,
            NumCyclesPubTable);

    LoadMemUdfs::initLoadMemUdfs();
    addLoadMemoryTarget();

    for (unsigned ii = 0; ii < NumUsers; ii++) {
        snprintf(args[ii].userId.userIdName,
                 sizeof(args[ii].userId.userIdName),
                 "%s%u-node%u",
                 UserIdPrefix,
                 ii,
                 Config::get()->getMyNodeId());
        args[ii].userId.userIdUnique =
            (uint32_t) hashStringFast(args[ii].userId.userIdName);
        xSyslog(moduleName,
                XlogNote,
                "Username: %s, UserIdUnique: %u",
                args[ii].userId.userIdName,
                args[ii].userId.userIdUnique);

        status = UserMgr::get()->getDag(&args[ii].userId,
                                        NULL,
                                        &args[ii].sessionGraph);
        if (status == StatusSessionNotFound ||
            status == StatusSessionUsrNotExist) {
            status = createSession(&args[ii].userId);
            // A new session is born inactive; so activate it first
            // If the session already exists, activate it (this may fail if
            // it's already active - probably need to add code to ignore such
            // a failure)
            if (status == StatusOk || status == StatusSessionExists) {
                status = activateSession(&args[ii].userId);
            }
            if (status != StatusOk) {
                break;
            }
            status = UserMgr::get()->getDag(&args[ii].userId,
                                            NULL,
                                            &args[ii].sessionGraph);
        }
        for (unsigned uu = LoadMemUdfs::LoadMemSimple;
             uu < LoadMemUdfs::LoadMemMax;
             uu++) {
            addLoadMemoryUdf((LoadMemUdfs::LoadMemType) uu,
                             args[ii].sessionGraph);
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableTest: before createBlockableThread");
    for (unsigned ii = 0; ii < NumUsers; ii++) {
        status = Runtime::get()->createBlockableThread(&userThreads[ii],
                                                       NULL,
                                                       publishedTableUserMain,
                                                       &args[ii]);
        assert(status == StatusOk);
    }

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableTest: after createBlockableThread");
    for (unsigned ii = 0; ii < NumUsers; ii++) {
        Status threadStatus;
        sysThreadJoin(userThreads[ii], (void **) &threadStatus);

        xSyslog(moduleName,
                XlogInfo,
                "publishedTableTest: sysThreadJoin ii=%u threadStatus=%s",
                ii,
                strGetFromStatus(threadStatus));
        if (threadStatus != StatusOk && status == StatusOk) {
            status = threadStatus;
        }
    }
    BailIfFailed(status);

CommonExit:
    SourceType srcTypes[] = {SrcTable, SrcConstant, SrcExport, SrcDataset};

    for (unsigned ii = 0; ii < NumUsers; ii++) {
        if (args[ii].sessionGraph != NULL) {
            Dag *sessionGraph = args[ii].sessionGraph;
            Status tmpStatus;
            XcalarApiOutput *output = NULL;
            size_t outputSize;

            for (unsigned jj = 0; jj < ArrayLen(srcTypes); jj++) {
                tmpStatus = sessionGraph->bulkDropNodes("*",
                                                        &output,
                                                        &outputSize,
                                                        srcTypes[jj],
                                                        &testUserId);
                if (tmpStatus != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Error while dropping *. SrcType: %s, "
                            "Status: %s",
                            strGetFromSourceType(srcTypes[jj]),
                            strGetFromStatus(tmpStatus));
                    if (status == StatusOk) {
                        status = tmpStatus;
                    }
                }

                if (output != NULL) {
                    memFree(output);
                    output = NULL;
                }
            }
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "publishedTableTest: done status=%s",
            strGetFromStatus(status));
    return status;
}
