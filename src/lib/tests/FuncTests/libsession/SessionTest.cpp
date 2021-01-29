// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <unistd.h>

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "util/System.h"
#include "sys/XLog.h"  // Xcalar system log support
#include "session/Sessions.h"
#include "usr/Users.h"
#include "libapis/WorkItem.h"
#include "libapis/LibApisRecv.h"
#include "SourceTypeEnum.h"
#include "runtime/Runtime.h"
#include "LibSessionFuncTestConfig.h"
#include "dataset/Dataset.h"
#include "queryparser/QueryParser.h"
#include "SessionTest.h"

#include "test/QA.h"  // Must be last

static int numThread = 4;
static int numSessionPerThread = 2;
static char loadPath[QaMaxQaDirLen + 1] = "";

static const char *moduleName = "stressSessionTest";
static const char *loadName = "ds";
static const char *indexTableName = "indexTable";

static XcalarApiUserId testUserId = {
    "SessionTest",
    0xdeadbeef,
};

static void
cleanupDatasets()
{
    Status status = StatusOk;
    XcalarApiOutput *apiOutput = NULL;
    size_t apiOutputSize = 0;

    status = Dataset::get()->unloadDatasets("*", &apiOutput, &apiOutputSize);
    if (apiOutput != NULL) {
        memFree(apiOutput);
    }
    // Since two instances of the test could be running against the same
    // cluster (in --allNodes config), the wild card delete could fail since a
    // concurrent instance may have been able to delete all datasets before
    // this one. So a wildcard delete is inherently racy with multiple
    // instances, and rather than fail the test, just log the status (it may be
    // an unexpected status that'd be useful to have in the logs just in case).
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete datasets: %s",
                strGetFromStatus(status));
    }
}

void
TestSession::activate()
{
    Status status;
    XcalarApiSessionGenericOutput genericSessionOutput;
    XcalarApiSessionActivateInput activateSessionInput;
    Dag *dag;
    XcalarApiOutput *output = NULL;
    XcalarApiOutput *outputOut = NULL;
    size_t sessionListOutputSize;
    uint64_t outputSize = 0;

    char errorMsg[256];
    memZero(&activateSessionInput, sizeof(activateSessionInput));

    verify(strlcpy(activateSessionInput.sessionName,
                   sessionName_,
                   sizeof(activateSessionInput.sessionName)) <
           sizeof(activateSessionInput.sessionName));
    activateSessionInput.sessionNameLength =
        strlen(activateSessionInput.sessionName);

    // list session to check sessionId
    status = userMgr_->list(&sessionUser_,
                            sessionName_,
                            &outputOut,
                            &sessionListOutputSize);
    BailIfFailed(status);
    assert(outputOut->outputResult.sessionListOutput.numSessions == 1);
    assert(outputOut->outputResult.sessionListOutput.sessions[0].sessionId ==
           sessionId_);

    memFree(outputOut);
    outputOut = NULL;

    status = userMgr_->activate(&sessionUser_,
                                &activateSessionInput,
                                &genericSessionOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to activate session %s: %s",
                sessionName_,
                strGetFromStatus(status));
        isValid_ = false;
        goto CommonExit;
    }

    snprintf(errorMsg,
             sizeof(errorMsg),
             "Session activate completed successfully");
    assert(strncmp(genericSessionOutput.errorMessage,
                   errorMsg,
                   sizeof(genericSessionOutput.errorMessage)) == 0);

    status = userMgr_->getDag(&sessionUser_, sessionName_, &dag);
    assert(status == StatusOk);

    status = dag->listDagNodeInfo(indexTableName,
                                  &output,
                                  &outputSize,
                                  SrcTable,
                                  &testUserId);
    BailIfFailed(status);

    // newly activate session can't have any nodes - assert this
    assert(output->outputResult.listNodesOutput.numNodes == 0);

    memFree(output);
    output = NULL;

    status = dag->listDagNodeInfo(prefixDatasetName_,
                                  &output,
                                  &outputSize,
                                  SrcDataset,
                                  &testUserId);
    BailIfFailed(status);

    // newly activate session can't have any nodes - assert this
    assert(output->outputResult.listNodesOutput.numNodes == 0);
    memFree(output);
    output = NULL;

CommonExit:

    if (outputOut != NULL) {
        memFree(outputOut);
        outputOut = NULL;
    }
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
}

// Set checkDag to true if session should have a dag with operation results
void
TestSession::inactivate(bool checkDag)
{
    Status status;
    XcalarApiSessionGenericOutput genericSessionOutput;
    XcalarApiSessionDeleteInput deleteSessionInput;
    XcalarApiOutput *output = NULL;
    XcalarApiOutput *outputOut = NULL;
    size_t sessionListOutputSize;
    Dag *dag;
    uint64_t outputSize = 0;

    char errorMsg[256];
    memZero(&deleteSessionInput, sizeof(deleteSessionInput));

    verify(strlcpy(deleteSessionInput.sessionName,
                   sessionName_,
                   sizeof(deleteSessionInput.sessionName)) <
           sizeof(deleteSessionInput.sessionName));

    deleteSessionInput.noCleanup = false;
    deleteSessionInput.sessionNameLength =
        strlen(deleteSessionInput.sessionName);

    // list session to check sessionID
    status = userMgr_->list(&sessionUser_,
                            sessionName_,
                            &outputOut,
                            &sessionListOutputSize);
    BailIfFailed(status);
    assert(outputOut->outputResult.sessionListOutput.numSessions == 1);
    assert(outputOut->outputResult.sessionListOutput.sessions[0].sessionId ==
           sessionId_);

    memFree(outputOut);
    outputOut = NULL;

    if (isValid_ && checkDag) {
        status = userMgr_->getDag(&sessionUser_, sessionName_, &dag);
        assert(status == StatusOk);

        status = dag->listDagNodeInfo(indexTableName,
                                      &output,
                                      &outputSize,
                                      SrcTable,
                                      &testUserId);
        BailIfFailed(status);

        assert(output->outputResult.listNodesOutput.numNodes == 1);

        memFree(output);
        output = NULL;

        status = dag->listDagNodeInfo(prefixDatasetName_,
                                      &output,
                                      &outputSize,
                                      SrcDataset,
                                      &testUserId);
        BailIfFailed(status);

        // there should be dataset nodes in this dag
        assert(output->outputResult.listNodesOutput.numNodes == 1);
        memFree(output);
        output = NULL;
    }

    status = userMgr_->inactivate(&sessionUser_,
                                  &deleteSessionInput,
                                  &genericSessionOutput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to inactivate session %s: %s",
                sessionName_,
                strGetFromStatus(status));
        isValid_ = false;
        goto CommonExit;
    }

    if (isValid_) {
        status = userMgr_->getDag(&sessionUser_, sessionName_, &dag);
        assert(dag == NULL);  // inactive session loses its DAG
    }

    // check delete msg
    // With bulk inactivate change, the error message can no longer be about a
    // specific session - but a count (similar to delete).
    if (isValid_) {
        snprintf(errorMsg, sizeof(errorMsg), "1 session(s) inactivated");
    } else {
        snprintf(errorMsg, sizeof(errorMsg), "No sessions inactivated.");
    }
    assert(strncmp(genericSessionOutput.errorMessage,
                   errorMsg,
                   sizeof(genericSessionOutput.errorMessage)) == 0);

CommonExit:
    if (outputOut) {
        memFree(outputOut);
        outputOut = NULL;
    }
    if (output) {
        memFree(output);
        output = NULL;
    }
}

void
TestSession::downloadUpload()
{
    Status status = StatusOk;
    XcalarApiSessionDownloadInput downloadInput;
    XcalarApiSessionUploadInput *uploadInput = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    size_t uploadInputSize;
    XcalarApiSessionNewOutput sessionNewOutput;
    XcalarApiSessionDownloadOutput *downloadOutput;

    downloadInput.sessionId = 0;
    verify(strlcpy(downloadInput.sessionName,
                   sessionName_,
                   sizeof(downloadInput.sessionName)) <
           sizeof(downloadInput.sessionName));
    downloadInput.sessionNameLength = strlen(downloadInput.sessionName);
    downloadInput.pathToAdditionalFiles[0] = '\0';

    status =
        userMgr_->download(&sessionUser_, &downloadInput, &output, &outputSize);
    BailIfFailed(status);

    // Upload what was downloaded except to a new session with a new name.

    downloadOutput = &output->outputResult.sessionDownloadOutput;

    uploadInputSize =
        sizeof(*uploadInput) + downloadOutput->sessionContentCount;

    uploadInput = (XcalarApiSessionUploadInput *) memAllocExt(uploadInputSize,
                                                              moduleName);
    BailIfNull(uploadInput);

    verify(strlcpy(uploadInput->sessionName,
                   uploadedSessionName_,
                   sizeof(uploadInput->sessionName)) <
           sizeof(uploadInput->sessionName));
    uploadInput->sessionNameLength = strlen(uploadInput->sessionName);
    uploadInput->pathToAdditionalFiles[0] = '\0';
    uploadInput->sessionContentCount = downloadOutput->sessionContentCount;
    memcpy(&uploadInput->sessionContent,
           downloadOutput->sessionContent,
           downloadOutput->sessionContentCount);

    status = userMgr_->upload(&sessionUser_, uploadInput, &sessionNewOutput);
    BailIfFailed(status);

    // Will need to clean up the uploaded session.
    successfulUpload_ = true;

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
    if (uploadInput != NULL) {
        memFree(uploadInput);
        uploadInput = NULL;
    }
}

static void *
sessionTestPerThread(void *arg)
{
    pid_t tid = sysGetTid();
    TestSession testSessions[numSessionPerThread];

    // Initialize all sessions before the testing begins
    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        thisSession->init((int) tid, ii);
    }

    // Perform the first batch of operations on this session
    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        if (!thisSession->isValid()) {
            continue;
        }
        thisSession->testOperations();
    }

    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        if (!thisSession->isValid()) {
            continue;
        }
        // set the checkDag flag to true since testOperations() above should've
        // resulted in dag containing the results of these operations
        thisSession->inactivate(true);
    }

    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        if (!thisSession->isValid()) {
            continue;
        }
        thisSession->downloadUpload();
    }

    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        if (!thisSession->isValid()) {
            continue;
        }
        thisSession->rename();
    }

    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        if (!thisSession->isValid()) {
            continue;
        }
        thisSession->activate();
    }

    int numLeft = 0;
    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        if (!thisSession->isValid()) {
            continue;
        }
        // set the checkDag flag to false since the dag should be empty given
        // that the sessions were re-activated after testOperations() ran -
        // thereby losing the DAG (via the first inactivate above).
        thisSession->inactivate(false);
        numLeft++;
    }

    xSyslog(moduleName,
            XlogErr,
            "Session Stress (tid %i) %i of %i test sessions completed all ops",
            tid,
            numLeft,
            numSessionPerThread);

    // Cleanup
    for (int ii = 0; ii < numSessionPerThread; ii++) {
        TestSession *thisSession = &testSessions[ii];
        thisSession->doCleanup();
    }

    return NULL;
}

bool
TestSession::init(int tid, int seqNum)
{
    Config *config = Config::get();

    snprintf(sessionUser_.userIdName,
             sizeof(sessionUser_.userIdName),
             "sessionStressTestUsr-node%uThread%d",
             config->getMyNodeId(),
             tid);
    sessionUser_.userIdUnique = seqNum + 1;

    userMgr_ = UserMgr::get();

    // create a dataset and table
    snprintf(datasetName_,
             sizeof(datasetName_),
             "%s-%d-%d-node%d",
             loadName,
             tid,
             seqNum,
             config->getMyNodeId());

    snprintf(prefixDatasetName_,
             sizeof(prefixDatasetName_),
             XcalarApiDatasetPrefix "%s",
             datasetName_);
    isValid_ = true;
    return true;
}

void
TestSession::rename()
{
    Status status;
    XcalarApiSessionGenericOutput genericSessionOutput;
    XcalarApiSessionRenameInput sessionRenameInput;
    memZero(&sessionRenameInput, sizeof(sessionRenameInput));

    strlcpy(sessionRenameInput.origSessionName,
            sessionName_,
            sizeof(sessionRenameInput));
    sessionRenameInput.origSessionNameLength =
        strlen(sessionRenameInput.origSessionName);

    snprintf(sessionRenameInput.sessionName,
             sizeof(sessionRenameInput.sessionName),
             "%s-%u-rename",
             sessionUser_.userIdName,
             sessionUser_.userIdUnique);

    sessionRenameInput.sessionNameLength =
        strlen(sessionRenameInput.sessionName);

    status = userMgr_->rename(&sessionUser_,
                              &sessionRenameInput,
                              &genericSessionOutput);
    BailIfFailed(status);

    strlcpy(sessionName_, sessionRenameInput.sessionName, sizeof(sessionName_));

    assert(strncmp(genericSessionOutput.errorMessage,
                   "Session renamed successfully",
                   sizeof(genericSessionOutput.errorMessage)) == 0);
CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to rename session '%s' to '%s' status '%s'",
                sessionRenameInput.origSessionName,
                sessionRenameInput.sessionName,
                strGetFromStatus(status));
    }
}

void
TestSession::doCleanup()
{
    Status status;
    char errorMsg[256];
    XcalarApiSessionDeleteInput deleteSessionInput;
    XcalarApiSessionGenericOutput genericSessionOutput;
    memZero(&deleteSessionInput, sizeof(deleteSessionInput));

    // Delete the main session
    strlcpy(deleteSessionInput.sessionName,
            sessionName_,
            sizeof(deleteSessionInput.sessionName));

    deleteSessionInput.noCleanup = false;
    deleteSessionInput.sessionNameLength =
        strlen(deleteSessionInput.sessionName);

    status = userMgr_->doDelete(&sessionUser_,
                                &deleteSessionInput,
                                &genericSessionOutput);
    BailIfFailed(status);

    snprintf(errorMsg, sizeof(errorMsg), "%d session(s) deleted", 1);
    assert(strncmp(genericSessionOutput.errorMessage,
                   errorMsg,
                   sizeof(genericSessionOutput.errorMessage)) == 0);

    if (successfulUpload_) {
        // Delete the uploaded session
        strlcpy(deleteSessionInput.sessionName,
                uploadedSessionName_,
                sizeof(deleteSessionInput.sessionName));
        deleteSessionInput.noCleanup = false;
        deleteSessionInput.sessionNameLength =
            strlen(deleteSessionInput.sessionName);

        status = userMgr_->doDelete(&sessionUser_,
                                    &deleteSessionInput,
                                    &genericSessionOutput);
        BailIfFailed(status);

        snprintf(errorMsg, sizeof(errorMsg), "%d session(s) deleted", 1);
        assert(strncmp(genericSessionOutput.errorMessage,
                       errorMsg,
                       sizeof(genericSessionOutput.errorMessage)) == 0);
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to clean up after session '%s': status '%s'",
                sessionName_,
                strGetFromStatus(status));
    }
}

void
TestSession::testOperations()
{
    Status status;
    Dag *dag;
    XcalarApiSessionNewInput sessionInput;
    XcalarApiSessionNewOutput sessionOutput;
    XcalarApiSessionDeleteInput deleteSessionInput;
    XcalarApiSessionActivateInput activateSessionInput;
    XcalarApiSessionGenericOutput genericSessionOutput;
    OperatorHandler *operatorHandler = NULL;
    XcalarWorkItem *loadWorkItem = NULL;
    XcalarWorkItem *indexWorkItem = NULL;
    XcalarApiOutput *outputOut = NULL;
    DagTypes::NodeId workspaceGraphNodeId;
    DfLoadArgs *loadArgs = NULL;
    QpLoad::LoadUrl loadUrl;
    // just use a dummmy non-NULL prevTxn param to make sure
    // xcApiGetOperatorHandlerInited() uses a new txn for the
    // subsequent operation - but since this test doesn't really have a txn, no
    // need to restore the prev txn
    Txn prevTxn = Txn();

    loadArgs = new (std::nothrow) DfLoadArgs();
    BailIfNull(loadArgs);

    static_assert(sizeof(sessionInput.sessionName) == sizeof(sessionName_),
                  "session name sizes should be the same");

    snprintf(sessionName_,
             sizeof(sessionName_),
             "%s-%u",
             sessionUser_.userIdName,
             sessionUser_.userIdUnique);

    // Session name that will be used for upload
    snprintf(uploadedSessionName_,
             sizeof(uploadedSessionName_),
             "%s-%lu-uploaded",
             sessionName_,
             XidMgr::get()->xidGetNext());

    verify(strlcpy(sessionInput.sessionName,
                   sessionName_,
                   sizeof(sessionInput.sessionName)) <
           sizeof(sessionInput.sessionName));

    sessionInput.fork = false;
    sessionInput.forkedSessionNameLength = 0;

    status = userMgr_->create(&sessionUser_, &sessionInput, &sessionOutput);
    BailIfFailed(status);
    sessionId_ = sessionOutput.sessionId;

    xSyslog(moduleName,
            XlogDebug,
            "session: %s, id:%lX has been created.\n",
            sessionInput.sessionName,
            sessionOutput.sessionId);

    memZero(loadArgs, sizeof(*loadArgs));
    loadArgs->sourceArgsListCount = 1;
    loadArgs->sourceArgsList[0].recursive = false;
    loadArgs->sourceArgsList[0].fileNamePattern[0] = '\0';
    loadArgs->maxSize = 0;

    // We're in this limbo state where a lot of the config file still has
    // the nfs:// prefix, whereas with this change, we no longer have this
    // nfs:// prefix. Hence we need to support both
    status = loadUrl.parseFromString(loadPath);
    if (status == StatusOk) {
        loadUrl.populateSourceArgs(&loadArgs->sourceArgsList[0]);
    } else {
        strlcpy(loadArgs->sourceArgsList[0].targetName,
                "Default Shared Root",
                sizeof(loadArgs->sourceArgsList[0].targetName));
        strlcpy(loadArgs->sourceArgsList[0].path,
                loadPath,
                sizeof(loadArgs->sourceArgsList[0].path));
    }

    strlcpy(loadArgs->parseArgs.parserFnName,
            "default:parseJson",
            sizeof(loadArgs->parseArgs.parserFnName));
    strlcpy(loadArgs->parseArgs.parserArgJson,
            "{}",
            sizeof(loadArgs->parseArgs.parserArgJson));

    loadWorkItem = xcalarApiMakeBulkLoadWorkItem(datasetName_, loadArgs);
    assert(loadWorkItem != NULL);

    indexWorkItem =
        xcalarApiMakeIndexWorkItem(prefixDatasetName_,
                                   NULL,
                                   1,
                                   (const char **) &DsDefaultDatasetKeyName,
                                   NULL,
                                   NULL,
                                   NULL,
                                   indexTableName,
                                   NULL,
                                   "p",
                                   false,
                                   false);
    assert(indexWorkItem != NULL);

    // session is born inactive - so activate it first

    memZero(&activateSessionInput, sizeof(activateSessionInput));
    activateSessionInput.sessionId = sessionOutput.sessionId;

    status = userMgr_->activate(&sessionUser_,
                                &activateSessionInput,
                                &genericSessionOutput);
    assert(status == StatusOk);

    status = userMgr_->getDag(&sessionUser_, sessionName_, &dag);
    assert(status == StatusOk);

    assert(operatorHandler == NULL);
    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           loadWorkItem,
                                           &sessionUser_,
                                           dag,
                                           false,
                                           &prevTxn);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to init operator handler for load of '%s': %s",
                datasetName_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create load dag node for '%s': %s",
                datasetName_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        operatorHandler->run(&loadWorkItem->output, &loadWorkItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to load %s (%s): %s",
                datasetName_,
                loadPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    delete operatorHandler;
    operatorHandler = NULL;
    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           indexWorkItem,
                                           &sessionUser_,
                                           dag,
                                           false,
                                           &prevTxn);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to init operator handler for index of '%s': %s",
                datasetName_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create index dag node for '%s': %s",
                datasetName_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&indexWorkItem->output,
                                  &indexWorkItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to index %s: %s",
                prefixDatasetName_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    delete operatorHandler;
    operatorHandler = NULL;

    xcalarApiFreeWorkItem(loadWorkItem);
    loadWorkItem = NULL;
    xcalarApiFreeWorkItem(indexWorkItem);
    indexWorkItem = NULL;

    size_t sessionListOutputSize;

    status = userMgr_->list(&sessionUser_,
                            sessionInput.sessionName,
                            &outputOut,
                            &sessionListOutputSize);
    BailIfFailed(status);
    assert(outputOut->outputResult.sessionListOutput.numSessions == 1);
    assert(outputOut->outputResult.sessionListOutput.sessions[0].sessionId ==
           sessionId_);

    assert(strncmp(outputOut->outputResult.sessionListOutput.sessions[0].state,
                   "Active",
                   sizeof(outputOut->outputResult.sessionListOutput.sessions[0]
                              .state)) == 0);

    memFree(outputOut);
    outputOut = NULL;

    memZero(&deleteSessionInput, sizeof(deleteSessionInput));
    deleteSessionInput.sessionId = sessionOutput.sessionId;
    deleteSessionInput.noCleanup = false;

    status = userMgr_->persist(&sessionUser_,
                               &deleteSessionInput,
                               &outputOut,
                               &sessionListOutputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to persist session %s: %s",
                sessionName_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    memFree(outputOut);
    outputOut = NULL;

CommonExit:
    if (loadArgs) {
        delete loadArgs;
        loadArgs = NULL;
    }
    if (operatorHandler) {
        delete operatorHandler;
        operatorHandler = NULL;
    }
    if (loadWorkItem) {
        xcalarApiFreeWorkItem(loadWorkItem);
        loadWorkItem = NULL;
    }
    if (indexWorkItem) {
        xcalarApiFreeWorkItem(indexWorkItem);
        indexWorkItem = NULL;
    }
    if (outputOut) {
        memFree(outputOut);
        outputOut = NULL;
    }
    // This part of the test can no longer continue
    // NOTE: this may leak whatever failed
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Setting session invalid due to errors");
        isValid_ = false;
    }
}

Status
sessionStress()
{
    pthread_t *threadHandle;
    Status status = StatusOk;
    size_t ret;

    if (loadPath[0] == '\0') {
        ret =
            snprintf(loadPath, sizeof(loadPath), "%s/yelp/user", qaGetQaDir());
        assert(ret <= sizeof(loadPath));
    }

    threadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) * numThread, moduleName);
    assert(threadHandle != NULL);

    for (int ii = 0; ii < numThread; ii++) {
        status = Runtime::get()->createBlockableThread(&threadHandle[ii],
                                                       NULL,
                                                       sessionTestPerThread,
                                                       NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    xSyslog(moduleName, XlogDebug, "%u threads has been created.\n", numThread);

    for (int ii = 0; ii < numThread; ii++) {
        sysThreadJoin(threadHandle[ii], NULL);
    }
    memFree(threadHandle);
    threadHandle = NULL;

    xSyslog(moduleName, XlogDebug, "%u threads has been joined.\n", numThread);

    cleanupDatasets();

    return status;
}

Status
libsessionFuncTestParseConfig(Config::Configuration *config,
                              char *key,
                              char *value,
                              bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibSessionFuncTestConfig(
                       LibSessionFuncTestNumThread)) == 0) {
        numThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibSessionFuncTestConfig(
                              LibSessionFuncTestNumSessionPerThread)) == 0) {
        numSessionPerThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibSessionFuncTestConfig(
                              LibSessionQaYelpUserPath)) == 0) {
        size_t ret = strlcpy(loadPath, value, sizeof(loadPath));
        assert(ret < sizeof(loadPath));
    } else {
        status = StatusUsrNodeIncorrectParams;
    }
    return status;
}
