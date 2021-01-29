// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//
#include <sys/stat.h>

#include "StrlFunc.h"
#include "msg/Xid.h"
#include "primitives/Primitives.h"
#include "runtime/Runtime.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "config/Config.h"
#include "querymanager/QueryManager.h"
#include "QueryStateEnums.h"
#include "util/System.h"
#include "dag/DagLib.h"
#include "libapis/WorkItem.h"
#include "optimizer/Optimizer.h"
#include "dag/DagTypes.h"
#include "LibQmFuncTestConfig.h"
#include "usr/Users.h"
#include "dataset/Dataset.h"
#include "udf/UserDefinedFunction.h"
#include "test/FuncTests/QueryManagerTests.h"

#include "test/QA.h"

Xid QueryManagerTests::testId = XidInvalid;
XcalarApiUserId QueryManagerTests::sessionUser;
uint64_t QueryManagerTests::sessionId = 1;
bool QueryManagerTests::sessionCreated = false;
bool QueryManagerTests::paramsInited = false;
QueryManagerTests::Params QueryManagerTests::params =
    QueryManagerTests::DefaultParams;
bool QueryManagerTests::datasetLoaded = false;
char QueryManagerTests::datasetName[Dataset::MaxNameLen + 1];

typedef struct {
    const char *retinaDir;
    const char *retinaFile;
    const char *retinaName;
} RetinaMapping;
RetinaMapping retinaMappings[] =
    {{
         .retinaDir = QueryManagerTests::params.nonstopRetinaDir,
         .retinaFile = QueryManagerTestsRetina::NonStoppingRetinaFile,
         .retinaName = QueryManagerTestsRetina::NonStoppingRetinaName,
     },
     {
         .retinaDir = QueryManagerTests::params.errorRetinaDir,
         .retinaFile = QueryManagerTestsRetina::ErrorRetinaFile,
         .retinaName = QueryManagerTestsRetina::ErrorRetinaName,
     }};

static constexpr const char *moduleName = "libQueryManagerTest";

void
QueryManagerTests::initParams()
{
    if (paramsInited) {
        return;
    }

    size_t ret;
    if (params.loadPath[0] == '\0') {
        ret = snprintf(params.loadPath,
                       sizeof(params.loadPath),
                       "%s/yelp/user",
                       qaGetQaDir());
        assert(ret <= sizeof(params.loadPath));
    }

    if (params.nonstopRetinaDir[0] == '\0') {
        ret = snprintf(params.nonstopRetinaDir,
                       sizeof(params.nonstopRetinaDir),
                       "%s",
                       qaGetQaDir());
        assert(ret <= sizeof(params.nonstopRetinaDir));
    }

    if (params.errorRetinaDir[0] == '\0') {
        ret = snprintf(params.errorRetinaDir,
                       sizeof(params.errorRetinaDir),
                       "%s",
                       qaGetQaDir());
        assert(ret <= sizeof(params.errorRetinaDir));
    }

    paramsInited = true;
}

QueryManagerTests::QueryManagerTests() : testThreadsSem_(0)
{
    initParams();
}

Status
QueryManagerTests::initTest()
{
    Status status = StatusOk;

    initParams();
    testId = XidMgr::get()->xidGetNext();

    if (!sessionCreated) {
        status = createSession();
        assert(status == StatusOk);
    }

    status = uploadUdfs();
    assert(status == StatusOk);

    if (!datasetLoaded) {
        status = loadDataset();
        assert(status == StatusOk);
    }

    status = uploadRetinas();
    assert(status == StatusOk);

    return status;
}

Status
QueryManagerTests::stringQueryTest()
{
    QueryManagerTestsQueryString testInstance;
    Status status = StatusOk;

    status = testInstance.runTests();

    return status;
}

Status
QueryManagerTests::retinaQueryTest()
{
    QueryManagerTestsRetina testInstance;
    Status status = StatusOk;

    status = testInstance.runTests();

    return status;
}

void
QueryManagerTests::cleanoutTest()
{
    Status status = StatusUnknown;

    if (datasetLoaded) {
        XcalarApiOutput *apiOutput = NULL;
        size_t apiOutputSize = 0;

        xSyslog(moduleName, XlogDebug, "delete dataset: %s", datasetName);
        status = Dataset::get()->unloadDatasets(datasetName,
                                                &apiOutput,
                                                &apiOutputSize);
        if (status == StatusOk) {
            datasetLoaded = false;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed delete dataset '%s': %s",
                    datasetName,
                    strGetFromStatus(status));
        }

        if (apiOutput != NULL) {
            memFree(apiOutput);
        }
    }

    if (sessionCreated) {
        XcalarApiSessionDeleteInput sessionDeleteInput;
        XcalarApiSessionGenericOutput sessionDeleteOutput;

        memZero(&sessionDeleteInput, sizeof(sessionDeleteInput));
        memZero(&sessionDeleteOutput, sizeof(sessionDeleteOutput));

        sessionDeleteInput.sessionNameLength = strlen(SessionName);
        strlcpy(sessionDeleteInput.sessionName,
                SessionName,
                sessionDeleteInput.sessionNameLength + 1);

        Status status = UserMgr::get()->inactivate(&sessionUser,
                                                   &sessionDeleteInput,
                                                   &sessionDeleteOutput);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed Session '%s' Usr '%s' inactivation: %s",
                    sessionDeleteInput.sessionName,
                    sessionUser.userIdName,
                    strGetFromStatus(status));
        }

        status = UserMgr::get()->doDelete(&sessionUser,
                                          &sessionDeleteInput,
                                          &sessionDeleteOutput);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed Session '%s' Usr '%s' deletetion: %s",
                    sessionDeleteInput.sessionName,
                    sessionUser.userIdName,
                    strGetFromStatus(status));
        }

        sessionCreated = false;
    }

    for (unsigned ii = 0; ii < ArrayLen(retinaMappings); ii++) {
        deleteRetina(retinaMappings[ii].retinaName);
    }
}

// Returns the number of usecs that the caller should sleep based
// on the prior time.  Use an exponential backoff.
// The initial call should be with zero specified.
// XXX: add some randomization
uint64_t
QueryManagerTests::retryTimeUSecs(uint64_t oldValue)
{
    uint64_t newValue;

    if (oldValue == 0) {
        newValue = RetryTimeUSecs;
    } else {
        newValue = oldValue * 2;

        if (newValue > MaxRetryTimeUSecs) {
            // Determine why things escalated to this point
            // assert(0);
            newValue = MaxRetryTimeUSecs;
        }
    }

    return newValue;
}

void *
QueryManagerTests::checkQueryState(void *arg)
{
    QueryManager *qm = QueryManager::get();
    bool stillRunning = true;
    const char *queryName = (const char *) arg;
    Status status;
    uint64_t sleepTimeUSecs;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    XcalarApiQueryStateOutput *queryStateOutput;

    // Keep checking the state of the query until it gets cancelled
    sleepTimeUSecs = 0;

    while (stillRunning) {
        status = qm->requestQueryState(&output, &outputSize, queryName, false);
        if (status == StatusApiWouldBlock) {
            if (sleepTimeUSecs == MaxRetryTimeUSecs) {
                // Already at the maximum retry time.  Something
                // must be wrong.
                xSyslog(moduleName,
                        XlogErr,
                        "Exceeded retry time for StatusApiWouldBlock");
                goto CommonExit;
            } else {
                // Wait a bit before trying again to allow apis to drain.
                sleepTimeUSecs = retryTimeUSecs(sleepTimeUSecs);
                xSyslog(moduleName,
                        XlogInfo,
                        "Sleeping for %lu USecs before retrying request for"
                        " state of query '%s'",
                        sleepTimeUSecs,
                        queryName);
                sysUSleep(sleepTimeUSecs);
                continue;
            }
        } else if (status != StatusOk) {
            // For a variety of reasons, requestQueryState may fail, e.g. when
            // trying to make a request on a query that has already been deleted
            goto CommonExit;
        }

        queryStateOutput = &output->outputResult.queryStateOutput;
        assert(queryStateOutput->queryState == qrNotStarted ||
               queryStateOutput->queryState == qrProcessing ||
               queryStateOutput->queryState == qrCancelled ||
               queryStateOutput->queryState == qrError);

        if (queryStateOutput->queryState == qrCancelled ||
            queryStateOutput->queryState == qrError) {
            stillRunning = false;
        }
        memFree(output);
        output = NULL;
    }

    // Check that the cancelled query remains cancelled.
    sleepTimeUSecs = 0;

    for (uint64_t ii = 0; ii < params.numLoopCheckingState; ++ii) {
        status = qm->requestQueryState(&output, &outputSize, queryName, false);
        if (status == StatusApiWouldBlock) {
            if (sleepTimeUSecs == MaxRetryTimeUSecs) {
                // Already at the maximum retry time.  Something
                // must be wrong.
                xSyslog(moduleName,
                        XlogErr,
                        "Exceeded retry time for StatusApiWouldBlock");
                goto CommonExit;
            } else {
                // Wait a bit before trying again to allow apis to drain.
                sleepTimeUSecs = retryTimeUSecs(sleepTimeUSecs);
                xSyslog(moduleName,
                        XlogInfo,
                        "Sleeping for %lu USecs before retrying request for"
                        "state of query '%s'",
                        sleepTimeUSecs,
                        queryName);
                sysUSleep(sleepTimeUSecs);

                // Decrement index so we don't lose a loop iteration.
                ii--;
                continue;
            }
        } else if (status != StatusOk) {
            goto CommonExit;
        }

        queryStateOutput = &output->outputResult.queryStateOutput;
        if (!stillRunning) {
            assert(queryStateOutput->queryState == qrCancelled ||
                   queryStateOutput->queryState == qrError);
        }

        memFree(output);
        output = NULL;
    }

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    return NULL;
}

void *
QueryManagerTests::cancelQuery(void *arg)
{
    QueryManager *qm = QueryManager::get();
    const char *queryName = (const char *) arg;
    Status status;

    for (uint64_t ii = 0; ii < params.numLoopCancelQuery; ++ii) {
        status = qm->requestQueryCancel(queryName);
        // Syslog the error as in optimized builds it's not easy/possible
        // to figure out the value of "status".  But filter out the
        // "already cancelled" error.
        if (status != StatusOk && status != StatusOperationCancelled) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed Query '%s' cancel: %s",
                    queryName,
                    strGetFromStatus(status));
        }
    }

    return NULL;
}

void *
QueryManagerTests::testThread()
{
    Status status = StatusUnknown;
    char queryName[QueryManager::QueryMaxNameLen + 1];
    pthread_t *rqStateThreadHandle = NULL;
    pthread_t *cancelThreadHandle = NULL;
    bool queryExecuted = false;
    QueryManager *qm = QueryManager::get();
    unsigned numStateRequestThreadsSpawned = 0;
    unsigned numCancellationThreadsSpawned = 0;

    rqStateThreadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) *
                                      params.numStateRequestThread,
                                  moduleName);
    assert(rqStateThreadHandle != NULL);

    cancelThreadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) *
                                      params.numCancellationThread,
                                  moduleName);
    assert(cancelThreadHandle != NULL);

    // Wait for all testThread to be spawned before we start the test
    testThreadsSem_.semWait();

    status = executeRandomQuery(queryName, sizeof(queryName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to execute query: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    queryExecuted = true;

    // spawn thread to query state
    for (unsigned ii = 0; ii < params.numStateRequestThread; ii++) {
        status = Runtime::get()->createBlockableThread(&rqStateThreadHandle[ii],
                                                       NULL,
                                                       checkQueryState,
                                                       (void *) queryName);
        if (status != StatusOk) {
            break;
        }
        numStateRequestThreadsSpawned++;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Spawned %u stateRequestThreads",
            numStateRequestThreadsSpawned);

    if (numStateRequestThreadsSpawned > 0) {
        // sleep a while before cancelling the query
        int sleepTime = (rand() % MaxSleepTime) + MinSleepTime;
        xSyslog(moduleName, XlogInfo, "Sleeping for %d seconds", sleepTime);
        sysSleep(sleepTime);
    }

    for (unsigned ii = 0; ii < params.numCancellationThread; ii++) {
        status = Runtime::get()->createBlockableThread(&cancelThreadHandle[ii],
                                                       NULL,
                                                       cancelQuery,
                                                       (void *) queryName);
        if (status != StatusOk) {
            break;
        }

        numCancellationThreadsSpawned++;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Spawned %u cancellationThreads",
            numStateRequestThreadsSpawned);

    if (numCancellationThreadsSpawned == 0) {
        // I have to cancel it myself. Didn't manage to recruit anyone during
        // the recession :-(
        cancelQuery((void *) queryName);
    }

CommonExit:
    if (cancelThreadHandle != NULL) {
        for (unsigned ii = 0; ii < numCancellationThreadsSpawned; ii++) {
            sysThreadJoin(cancelThreadHandle[ii], NULL);
        }
        memFree(cancelThreadHandle);
    }

    if (rqStateThreadHandle != NULL) {
        for (unsigned ii = 0; ii < numStateRequestThreadsSpawned; ii++) {
            sysThreadJoin(rqStateThreadHandle[ii], NULL);
        }
        memFree(rqStateThreadHandle);
    }

    if (queryExecuted) {
        status = qm->requestQueryDelete(queryName);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed Query '%s' delete: %s",
                    queryName,
                    strGetFromStatus(status));
        }
        queryExecuted = false;
    }

    return NULL;
}

Status
QueryManagerTests::runTests()
{
    Status status = StatusOk;
    pthread_t *threadHandle = NULL;

    threadHandle = (pthread_t *) memAllocExt(sizeof(pthread_t) *
                                                 params.numQueryCreationThread,
                                             moduleName);
    assert(threadHandle != NULL);

    for (unsigned ii = 0; ii < params.numLoop; ii++) {
        for (unsigned jj = 0; jj < params.numQueryCreationThread; jj++) {
            // half of the thread will run error query, the other will run
            // non-stopping query
            status =
                Runtime::get()
                    ->createBlockableThread(&threadHandle[jj],
                                            this,
                                            &QueryManagerTests::testThread);
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogDebug,
                "%u threads have been created.",
                params.numQueryCreationThread);

        for (unsigned jj = 0; jj < params.numQueryCreationThread; jj++) {
            testThreadsSem_.post();
        }

        for (unsigned jj = 0; jj < params.numQueryCreationThread; jj++) {
            sysThreadJoin(threadHandle[jj], NULL);
        }

        xSyslog(moduleName,
                XlogDebug,
                "%u threads have been joined.",
                params.numQueryCreationThread);
    }

    if (threadHandle != NULL) {
        memFree(threadHandle);
        threadHandle = NULL;
    }

    return status;
}

Status
QueryManagerTestsQueryString::executeRandomQuery(char *queryNameBuf,
                                                 size_t queryNameBufSize)
{
    Status status = StatusUnknown;
    QueryManager *qm = QueryManager::get();
    char *queryString = NULL;
    Xid xid;
    bool queryExecuted = false;
    xcalar::compute::localtypes::Dataflow::ExecuteRequest execDataflowInput;
    xcalar::compute::localtypes::Dataflow::ExecuteResponse execDataflowOutput;
    Runtime::SchedId schedId =
        static_cast<Runtime::SchedId>(rand() % Runtime::TotalFastPathScheds);

    queryString = (char *) memAllocExt(XcalarApiMaxQuerySize, moduleName);
    BailIfNull(queryString);

    xid = XidMgr::get()->xidGetNext();
    switch (xid % 2) {
    case 0:
        snprintf(queryNameBuf,
                 queryNameBufSize,
                 "%s-%lu",
                 NonStoppingQueryName,
                 xid);
        snprintf(queryString,
                 XcalarApiMaxQuerySize - 1,
                 NonStoppingQuerySkel,
                 QueryManagerTests::datasetName,
                 xid,
                 xid,
                 xid,
                 NonStoppingUdfModule);
        break;
    case 1:
        snprintf(queryNameBuf, queryNameBufSize, "%s-%lu", ErrorQueryName, xid);
        strlcpy(queryString, ErrorQuery, XcalarApiMaxQuerySize - 1);
        break;
    }
    try {
        execDataflowInput.set_job_name(queryNameBuf);
        execDataflowInput.set_dataflow_str(queryString);
        execDataflowInput.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(QueryManagerTests::sessionUser.userIdName);
        execDataflowInput.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(QueryManagerTests::SessionName);
        execDataflowInput.set_is_async(true);
        execDataflowInput.set_sched_name(Runtime::getSchedNameFromId(schedId));
        execDataflowInput.set_collect_stats(true);
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = qm->processDataflow(&execDataflowInput, &execDataflowOutput);
    if (status != StatusOk) {
        goto CommonExit;
    }
    queryExecuted = true;
CommonExit:
    if (queryString != NULL) {
        memFree(queryString);
        queryString = NULL;
    }

    if (status != StatusOk && queryExecuted) {
        status = qm->requestQueryDelete(queryNameBuf);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed Query '%s' delete: %s",
                    queryNameBuf,
                    strGetFromStatus(status));
        }
        queryExecuted = false;
    }

    return status;
}

Status
QueryManagerTests::createSession()
{
    assert(!sessionCreated);
    Status status = StatusUnknown;
    XcalarApiSessionInfoInput sessionInfo;
    XcalarApiSessionActivateInput sessionActInput;
    XcalarApiSessionGenericOutput sessionActOutput;

    memZero(&sessionUser, sizeof(sessionUser));
    sessionUser.userIdUnique = (uint32_t) hashStringFast(SessionUserName);
    strcpy(sessionUser.userIdName, SessionUserName);

    XcalarApiSessionNewInput sessionInput;
    XcalarApiSessionNewOutput sessionOutput;

    memZero(&sessionInput, sizeof(sessionInput));
    memZero(&sessionOutput, sizeof(sessionOutput));

    sessionInput.sessionNameLength = strlen(SessionName);
    strlcpy(sessionInput.sessionName,
            SessionName,
            sessionInput.sessionNameLength + 1);

    status =
        UserMgr::get()->create(&sessionUser, &sessionInput, &sessionOutput);
    if (status != StatusOk && status != StatusSessionExists) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create session '%s': %s",
                sessionInput.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    sessionCreated = true;

    memZero(&sessionActInput, sizeof(sessionActInput));
    sessionActInput.sessionId = 0;
    strlcpy(sessionActInput.sessionName,
            SessionName,
            sizeof(sessionActInput.sessionName));
    sessionActInput.sessionNameLength = strlen(SessionName);

    status = UserMgr::get()->activate(&sessionUser,
                                      &sessionActInput,
                                      &sessionActOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to activate session '%s': %s",
                sessionActInput.sessionName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    strlcpy(sessionInfo.sessionName,
            SessionName,
            sizeof(sessionInfo.sessionName));
    sessionInfo.sessionNameLength = strlen(SessionName);
    sessionInfo.sessionId = 0;

    status = UserMgr::get()->getSessionId(&sessionUser, &sessionInfo);
    assert(status == StatusOk);

    sessionId = sessionInfo.sessionId;

    xSyslog(moduleName,
            XlogDebug,
            "Session created, session = %s, ID = %lu , usrId = %u",
            SessionName,
            sessionId,
            sessionUser.userIdUnique);

    status = StatusOk;
CommonExit:
    return status;
}

Status
QueryManagerTests::uploadUdfs()
{
    assert(sessionCreated);

    XcalarApiUdfContainer udfContainer;
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiSessionInfoInput sessionInfo;

    sessionInfo.sessionId = sessionId;
    strlcpy(sessionInfo.sessionName,
            SessionName,
            sizeof(sessionInfo.sessionName));
    sessionInfo.sessionNameLength = strlen(SessionName);

    status = UserDefinedFunction::initUdfContainer(&udfContainer,
                                                   &sessionUser,
                                                   &sessionInfo,
                                                   NULL);
    BailIfFailed(status);

    workItem = xcalarApiMakeUdfAddUpdateWorkItem(
        XcalarApiUdfAdd,
        UdfTypePython,
        QueryManagerTestsQueryString::NonStoppingUdfModule,
        QueryManagerTestsQueryString::NonStoppingUdfSource);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Error creating add udf workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status =
        UserDefinedFunction::get()->addUdf(&workItem->input->udfAddUpdateInput,
                                           &udfContainer,
                                           &output,
                                           &outputSize);
    if (status == StatusUdfModuleAlreadyExists) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            outputSize = 0;
        }
        status = UserDefinedFunction::get()
                     ->updateUdf(&workItem->input->udfAddUpdateInput,
                                 &udfContainer,
                                 &output,
                                 &outputSize);
    }
    BailIfFailed(status);

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

Status
QueryManagerTests::loadDataset()
{
    Status status = StatusOk;
    char *loadQuery = NULL;
    char queryName[QueryManager::QueryMaxNameLen + 1];
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    XcalarApiQueryStateOutput *queryStateOutput = NULL;
    QueryState state = qrNotStarted;
    QueryManager *qm = QueryManager::get();
    xcalar::compute::localtypes::Dataflow::ExecuteRequest execDataflowInput;
    xcalar::compute::localtypes::Dataflow::ExecuteResponse execDataflowOutput;

    Semaphore waiter(0);

    loadQuery = (char *) memAllocExt(XcalarApiMaxQuerySize, moduleName);
    assert(loadQuery != NULL);

    snprintf(queryName, sizeof(queryName), "loadQuery-%lu", testId);
    snprintf(datasetName, sizeof(datasetName), ".XcalarDS.%s", queryName);
    snprintf(loadQuery,
             XcalarApiMaxQuerySize,
             LoadQuerySkel,
             queryName,
             params.loadPath);

    try {
        execDataflowInput.set_job_name(queryName);
        execDataflowInput.set_dataflow_str(loadQuery);
        execDataflowInput.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(QueryManagerTests::sessionUser.userIdName);
        execDataflowInput.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(QueryManagerTests::SessionName);
        execDataflowInput.set_is_async(true);
        execDataflowInput.set_sched_name(
            Runtime::getSchedNameFromId(Runtime::SchedId::Sched0));
        execDataflowInput.set_collect_stats(true);
    } catch (std::exception &e) {
        status = StatusNoMem;
    }
    assert(status == StatusOk);

    status = qm->processDataflow(&execDataflowInput, &execDataflowOutput);
    assert(status == StatusOk);

    xSyslog(moduleName,
            XlogInfo,
            "queryName : %s, query : %s",
            queryName,
            loadQuery);

    do {
        output = NULL;
        status = qm->requestQueryState(&output, &outputSize, queryName, false);
        assert(status == StatusOk);
        assert(output != NULL);

        queryStateOutput = &output->outputResult.queryStateOutput;
        assert(queryStateOutput->queryStatus == StatusOk.code());

        state = queryStateOutput->queryState;
        waiter.timedWait(USecsPerSec);
        memFree(output);
        output = NULL;
        assert(state != qrError);
    } while (state != qrFinished);

    xSyslog(moduleName, XlogInfo, "Loaded dataset for '%s'", queryName);

    status = qm->requestQueryDelete(queryName);
    if (status == StatusStatsCollectionInProgress) {
        // statusStatsCollectionInProgress indicates that the delete was
        // deferred (and query was marked for deletion) since stats collection
        // was still in progress. We don't want to fail due to this.
        status = StatusOk;
    }

    assert(status == StatusOk);

    datasetLoaded = true;

    memFree(loadQuery);
    loadQuery = NULL;

    return status;
}

Status
QueryManagerTests::uploadRetinas()
{
    Status status = StatusOk;
    bool overwriteExistingUdf = true;
    char retinaPath[XcalarApiMaxUrlLen + 1];
    int ret;

    for (unsigned ii = 0; ii < ArrayLen(retinaMappings); ii++) {
        snprintf(retinaPath,
                 sizeof(retinaPath),
                 "%s/%s",
                 retinaMappings[ii].retinaDir,
                 retinaMappings[ii].retinaFile);
        ret = access(retinaPath, R_OK);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName, XlogErr, "can't access retina: %s", retinaPath);
            goto CommonExit;
        }
        xSyslog(moduleName,
                XlogInfo,
                "uploading nonstopRetina: %s",
                retinaPath);

        deleteRetina(retinaMappings[ii].retinaName);

        status = uploadRetina(retinaPath,
                              retinaMappings[ii].retinaName,
                              overwriteExistingUdf);
        assert(status == StatusOk);
    }

CommonExit:
    return status;
}

void
QueryManagerTests::deleteRetina(const char *retinaName)
{
    Status status;
    DagLib *dagLib = DagLib::get();

    status = dagLib->deleteRetina(retinaName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete retina '%s': %s",
                retinaName,
                strGetFromStatus(status));
    }
}

Status
QueryManagerTests::uploadRetina(const char *path,
                                const char *retinaName,
                                bool force)
{
    uint8_t *retinaBuf = NULL;
    size_t retinaBufSize = 0;
    struct stat statBuf;
    size_t ret;
    Status status = StatusOk;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    DagLib *dagLib = DagLib::get();
    XcalarWorkItem *workItem = NULL;
    bool found = false;
    XcalarApiListRetinasOutput *listRetinasOutput;

    ret = stat(path, &statBuf);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status =
        DagLib::loadRetinaFromFile(path, &retinaBuf, &retinaBufSize, &statBuf);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeImportRetinaWorkItem(retinaName,
                                                 force,
                                                 false,
                                                 NULL,
                                                 NULL,
                                                 retinaBufSize,
                                                 retinaBuf);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = dagLib->importRetina(&workItem->input->importRetinaInput,
                                  &output,
                                  &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    memFree(output);
    output = NULL;

    status = dagLib->listRetinas("*", &output, &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    listRetinasOutput = &output->outputResult.listRetinasOutput;
    for (uint64_t ii = 0; ii < listRetinasOutput->numRetinas; ++ii) {
        found =
            (strncmp(listRetinasOutput->retinaDescs[ii].retinaName,
                     retinaName,
                     sizeof(listRetinasOutput->retinaDescs[ii].retinaName)) ==
             0);
        if (found) {
            break;
        }
    }

    if (!found) {
        status = StatusRetinaParseError;
        goto CommonExit;
    }

CommonExit:
    if (retinaBuf != NULL) {
        memFree(retinaBuf);
        retinaBuf = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    return status;
}

Status
QueryManagerTestsRetina::executeRandomQuery(char *queryNameBuf,
                                            size_t queryNameBufSize)
{
    const char *retinaName;
    Xid xid;
    Status status = StatusUnknown;
    bool queryCreated = false;
    xcalar::compute::localtypes::Dataflow::ExecuteRequest execDataflowInput;
    xcalar::compute::localtypes::Dataflow::ExecuteResponse execDataflowOutput;
    QueryManager *qm = QueryManager::get();

    typedef struct Parameter {
        const char *parameterName;
        const char *parameterValue;
    } Parameter;

    Parameter params[] = {
        {.parameterName = LoadPathParameter,
         .parameterValue = QueryManagerTests::params.loadPath}};

    xid = XidMgr::get()->xidGetNext();
    int retinaId = xid % ArrayLen(retinaMappings);
    retinaName = retinaMappings[retinaId].retinaName;
    snprintf(queryNameBuf, queryNameBufSize, "%s-%lu", retinaName, xid);

    try {
        execDataflowInput.set_dataflow_name(retinaName);
        execDataflowInput.set_job_name(queryNameBuf);
        execDataflowInput.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(QueryManagerTests::sessionUser.userIdName);
        execDataflowInput.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(QueryManagerTests::SessionName);
        execDataflowInput.set_udf_user_name(
            QueryManagerTests::sessionUser.userIdName);
        execDataflowInput.set_udf_session_name(QueryManagerTests::SessionName);
        execDataflowInput.set_is_async(true);
        execDataflowInput.set_sched_name("");
        execDataflowInput.set_optimized(true);
        if ((rand() % 2) == 0) {
            execDataflowInput.set_execution_mode(
                xcalar::compute::localtypes::XcalarEnumType::BFS);
        } else {
            execDataflowInput.set_execution_mode(
                xcalar::compute::localtypes::XcalarEnumType::DFS);
        }

        for (unsigned ii = 0; ii < ArrayLen(params); ii++) {
            xcalar::compute::localtypes::Dataflow::Parameter *dfParam;
            dfParam = execDataflowInput.add_parameters();
            dfParam->set_name(params[ii].parameterName);
            dfParam->set_value(params[ii].parameterValue);
        }

        execDataflowInput.set_export_to_active_session(false);
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Populating execDataflowInput throws an exception: "
                "%s",
                e.what());
        goto CommonExit;
    }

    status = qm->processDataflow(&execDataflowInput, &execDataflowOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "processDataflow '%s' returned unexpected status: %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    queryCreated = true;

    assert(strcmp(queryNameBuf, execDataflowOutput.job_name().c_str()) == 0);

CommonExit:
    if (status != StatusOk) {
        if (queryCreated) {
            Status status2 =
                QueryManager::get()->requestQueryDelete(queryNameBuf);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to delete query '%s': %s",
                        queryNameBuf,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

Status
QueryManagerTests::parseConfig(Config::Configuration *config,
                               char *key,
                               char *value,
                               bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibQmFuncTestConfig(
                       LibQmNumQueryCreationThread)) == 0) {
        params.numQueryCreationThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key, strGetFromLibQmFuncTestConfig(LibQmNumLoop)) ==
               0) {
        params.numLoop = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmNumStateRequestThread)) == 0) {
        params.numStateRequestThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmNumCancellationThread)) == 0) {
        params.numCancellationThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmNumLoopCheckingState)) == 0) {
        params.numLoopCheckingState = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmNumLoopCancelQuery)) == 0) {
        params.numLoopCancelQuery = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(LibQmQaYelpUserPath)) ==
               0) {
        size_t ret = strlcpy(params.loadPath, value, sizeof(params.loadPath));
        assert(ret < sizeof(params.loadPath));
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmQaNonstopRetinaDir)) == 0) {
        size_t ret = strlcpy(params.nonstopRetinaDir,
                             value,
                             sizeof(params.nonstopRetinaDir));
        assert(ret < sizeof(params.nonstopRetinaDir));
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmQaErrorRetinaDir)) == 0) {
        size_t ret = strlcpy(params.errorRetinaDir,
                             value,
                             sizeof(params.errorRetinaDir));
        assert(ret < sizeof(params.errorRetinaDir));
    } else if (strcasecmp(key,
                          strGetFromLibQmFuncTestConfig(
                              LibQmQueryLatencyOptimized)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            params.queryLatencyOptimized = true;
        } else if (strcasecmp(value, "false")) {
            params.queryLatencyOptimized = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else {
        status = StatusUsrNodeIncorrectParams;
    }
    return status;
}
