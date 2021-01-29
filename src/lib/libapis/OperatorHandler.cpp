// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisRecv.h"
#include "libapis/ApiHandler.h"
#include "libapis/OperatorHandler.h"
#include "util/Stopwatch.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "queryparser/QueryParser.h"
#include "strings/String.h"
#include "optimizer/Optimizer.h"
#include "table/TableNs.h"
#include "usr/Users.h"

static constexpr const char *moduleName = "libapis::operatorHandler";

OperatorHandler::OperatorHandler(XcalarApis api)
    : ApiHandler(api),
      dstNodeId_(DagTypes::InvalidDagNodeId),
      parentNodeIdsToBeProvided_(false)
{
    xdbMgr_ = XdbMgr::get();
    op_ = Operators::get();
    query_[0] = '\0';
}

OperatorHandler::~OperatorHandler()
{
    if (parentNodeIds_ != NULL) {
        TableNsMgr *tnsMgr = TableNsMgr::get();
        UserMgr *usrMgr = UserMgr::get();
        for (unsigned ii = 0; ii < numParents_; ii++) {
            if (parentRefAcquired_ && parentRefAcquired_[ii]) {
                parentGraphs_[ii]->putDagNodeRefById(parentNodeIds_[ii]);
                parentRefAcquired_[ii] = false;
            }

            if (parentTableTrack_ && parentTableTrack_[ii].tableHandleValid) {
                tnsMgr->closeHandleToNs(&parentTableTrack_[ii].tableHandle);
                parentTableTrack_[ii].tableHandleValid = false;
            }

            // Source session ops is tracked only if it's not already a local
            // session.
            if (parentGraphIds_ &&
                DagTypes::DagIdInvalid != parentGraphIds_[ii] &&
                dstGraph_->getId() != parentGraphIds_[ii]) {
                Status status =
                    usrMgr->trackOutstandOps(&parentSessionContainers_[ii],
                                             UserMgr::OutstandOps::Dec);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed trackOutstandOps userName '%s', session "
                            "'%s', "
                            "Id %ld: %s",
                            parentSessionContainers_[ii].userId.userIdName,
                            parentSessionContainers_[ii]
                                .sessionInfo.sessionName,
                            parentSessionContainers_[ii].sessionInfo.sessionId,
                            strGetFromStatus(status));
                }
            }
        }
        delete[] parentNodeIds_;
        parentNodeIds_ = NULL;
    }

    if (parentSessionContainers_ != NULL) {
        delete[] parentSessionContainers_;
        parentSessionContainers_ = NULL;
    }

    if (parentGraphs_ != NULL) {
        delete[] parentGraphs_;
        parentGraphs_ = NULL;
    }

    if (parentGraphIds_ != NULL) {
        delete[] parentGraphIds_;
        parentGraphIds_ = NULL;
    }

    if (parentNodeIdBuf_ != NULL) {
        delete[] parentNodeIdBuf_;
        parentNodeIdBuf_ = NULL;
    }

    if (parentRefAcquired_ != NULL) {
        delete[] parentRefAcquired_;
        parentRefAcquired_ = NULL;
    }

    if (parentTableTrack_ != NULL) {
        delete[] parentTableTrack_;
        parentTableTrack_ = NULL;
    }
}

Status
OperatorHandler::setQuery(XcalarApiInput *input)
{
    Status status = StatusOk;
    QueryCmdParser *cmdParser = NULL;
    char *queryStr = NULL;
    json_t *query = json_array();
    BailIfNull(query);

    cmdParser = QueryParser::get()->getCmdParser(api_);
    if (cmdParser == NULL) {
        query_[0] = '\0';
    } else {
        json_error_t err;
        status = cmdParser->reverseParse(api_,
                                         "",
                                         "",
                                         DgDagStateCreated,
                                         input,
                                         NULL,
                                         &err,
                                         query);
        if (status != StatusOk) {
            snprintf(query_,
                     sizeof(query_),
                     "Query parse failed: %s",
                     strGetFromStatus(status));
            goto CommonExit;
        }

        queryStr = json_dumps(query, 0);
        BailIfNull(queryStr);

        status = strStrlcpy(query_, queryStr, sizeof(query_));
        BailIfFailed(status);
    }

CommonExit:
    if (query) {
        json_decref(query);
        query = NULL;
    }

    if (queryStr) {
        memFreeJson(queryStr);
        queryStr = NULL;
    }

    return status;
}

Status
OperatorHandler::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = setArg(input, inputSize, false);
    setQuery(input);
    return status;
}

Status
OperatorHandler::setArgQueryGraph(XcalarApiInput *input, size_t inputSize)
{
    Status status = setArg(input, inputSize, true);
    setQuery(input);
    return status;
}

Status
OperatorHandler::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    return run(outputOut, outputSizeOut, NULL);
}

Status
OperatorHandler::run(XcalarApiOutput **outputOut,
                     size_t *outputSizeOut,
                     void *optimizerContext)
{
    Status status = StatusUnknown;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    Stopwatch stopwatch;
    const char *dstNodeName = NULL;
    bool dstNodeRefAcquired = false;
    OpStatus *opStatus = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    char idBuf[sizeof(uint64_t) * 4];
    bool addedToNs = false;
    size_t parentNodeIdBufSize = 1 + numParents_ * sizeof(idBuf);
    TableNsMgr *tnsMgr = TableNsMgr::get();
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();
    DagTypes::NodeId *failureTabs[XcalarApiMaxFailureEvals];

    parentNodeIdBuf_ = new (std::nothrow) char[parentNodeIdBufSize];
    BailIfNull(parentNodeIdBuf_);
    parentNodeIdBuf_[0] = '\0';

    // all initialized to false
    parentRefAcquired_ = new (std::nothrow) bool[numParents_];
    BailIfNull(parentRefAcquired_);
    memZero(parentRefAcquired_, numParents_ * sizeof(bool));

    parentTableTrack_ =
        new (std::nothrow) TableNsMgr::TableHandleTrack[numParents_];
    BailIfNull(parentTableTrack_);

    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        failureTabs[ii] = NULL;  // first init it to help the frees at end
    }

    // You'll need to have created a corresponding dag node in the sessionGraph
    // before we'll even attempt to run the operator
    assert(dstNodeId_ != DagTypes::InvalidDagNodeId);

    // dstNodeName may be empty or NULL. We rely on nodeId here only.
    dstNodeName = getDstNodeName();

    status = xcApiCheckIfSufficientMem(api_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeRefById(dstNodeId_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not get refCount to dagNode: %s (%lu) (status: %s)",
                dstNodeName,
                dstNodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }
    dstNodeRefAcquired = true;

    if (dstGraphType_ == DagTypes::WorkspaceGraph &&
        tnsMgr->isTableIdValid(dstTableId_)) {
        // Hoist the table name to NS
        dstTableTrack_.tableId = dstTableId_;
        status = tnsMgr->addToNs(sessionContainer,
                                 dstTableTrack_.tableId,
                                 dstNodeName,
                                 dstGraph_->getId(),
                                 dstNodeId_);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to publish table %s %ld to table namespace: %s",
                        dstNodeName,
                        dstTableTrack_.tableId,
                        strGetFromStatus(status));
        addedToNs = true;

        // Get exclusive access to the table in the namespace, since we are
        // still creating it and we don't want any resultSets on a table still
        // under creation.
        status = tnsMgr->openHandleToNs(sessionContainer,
                                        dstTableTrack_.tableId,
                                        LibNsTypes::ReadSharedWriteExclWriter,
                                        &dstTableTrack_.tableHandle,
                                        TableNsMgr::OpenNoSleep);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %s %ld: %s",
                        dstNodeName,
                        dstTableTrack_.tableId,
                        strGetFromStatus(status));
        dstTableTrack_.tableHandleValid = true;
    }

    // We need to grab our parent's reference and check its state, before we
    // change our own state to Processing. This is because there is a window
    // between when we change our state to processing and when we get our
    // parents reference, that the parent could now be in the processing of
    // being dropped. If my parent is a load node, it'll fail the check that
    // there are no descendants in the Processing or Ready state.
    for (unsigned ii = 0; ii < numParents_; ii++) {
        if (!(getFlags() & DoNotGetParentRef)) {
            // get dag ref on parents to prevent them from getting deleted
            DgDagState dgState;
            status =
                parentGraphs_[ii]->getDagNodeStateAndRef(parentNodeIds_[ii],
                                                         &dgState);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to get dagStateAndRef for dagNode: %s (%lu) "
                        "(status: %s)",
                        dstNodeName,
                        dstNodeId_,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            parentRefAcquired_[ii] = true;

            if (dgState != DgDagStateReady) {
                xSyslog(moduleName,
                        XlogErr,
                        "Node (%lu) not ready for dagNode: %s (%lu) "
                        "(status: %s)",
                        parentNodeIds_[ii],
                        dstNodeName,
                        dstNodeId_,
                        strGetFromStatus(status));
                status = StatusDgDagNodeNotReady;
                goto CommonExit;
            }
            status = parentGraphs_[ii]
                         ->getTableIdFromNodeId(parentNodeIds_[ii],
                                                &parentTableTrack_[ii].tableId);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed getTableIdFromNodeId for dagNode %lu: "
                            "%s",
                            parentNodeIds_[ii],
                            strGetFromStatus(status));

            if (dstGraphType_ == DagTypes::WorkspaceGraph &&
                tnsMgr->isTableIdValid(parentTableTrack_[ii].tableId)) {
                // Get shared read access to all the operator source tables.
                status =
                    tnsMgr->openHandleToNs(sessionContainer,
                                           parentTableTrack_[ii].tableId,
                                           LibNsTypes::ReaderShared,
                                           &parentTableTrack_[ii].tableHandle,
                                           TableNsMgr::OpenSleepInUsecs);
                BailIfFailedMsg(moduleName,
                                status,
                                "Failed to open handle to table %ld: %s",
                                parentTableTrack_[ii].tableId,
                                strGetFromStatus(status));
                parentTableTrack_[ii].tableHandleValid = true;
            }
        }

        snprintf(idBuf, sizeof(idBuf), "(%lu)", parentNodeIds_[ii]);
        strlcat(parentNodeIdBuf_, idBuf, parentNodeIdBufSize);
    }

    status = dstGraph_->changeDagNodeState(dstNodeId_, DgDagStateProcessing);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not change dagNode %s (%lu) state to Processing. "
                "Status: %s",
                dstNodeName,
                dstNodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (needsXdb()) {
        status = createXdb(optimizerContext);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not createXdb dagNode %s (%lu) "
                    "Status: %s",
                    dstNodeName,
                    dstNodeId_,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "%s %s(%lu) with parents %s started by user '%s'",
            strGetFromXcalarApis(api_),
            dstNodeName,
            dstNodeId_,
            parentNodeIdBuf_,
            userId_->userIdName);

    stopwatch.restart();

    // XXX: one idea to improve the failureTabs interface is to let the callee
    // or operator which implements runHandler, do the allocation and return
    // the faiureTables (since most operators don't generate failure tables)
    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        failureTabs[ii] =
            (DagTypes::NodeId *) memAlloc(sizeof(DagTypes::NodeId));
        BailIfNull(failureTabs[ii]);
        *failureTabs[ii] = DagTypes::InvalidDagNodeId;
    }
    status = runHandler(&output, &outputSize, optimizerContext, failureTabs);

    if (status == StatusOk) {
        status = dstGraph_->updateOpDetails(dstNodeId_,
                                            failureTabs,
                                            userId_,
                                            sessionInfo_);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not updateOpDetails dagNode %s (%lu) "
                    "Status: %s",
                    dstNodeName,
                    dstNodeId_,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        assert(dstNodeRefAcquired);
        status = dstGraph_->getOpStatus(dstNodeId_, &opStatus);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not getOpStatus dagNode %s (%lu) "
                    "Status: %s",
                    dstNodeName,
                    dstNodeId_,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (unlikely(status == StatusConnReset)) {
        // most likely this is due to XPU death - this is more about returning
        // an error that translates to a more meaningful message for the
        // outside world - hence the translation here.
        status = StatusXpuDeath;
    }

CommonExit:
    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        if (failureTabs[ii] != NULL) {
            memFree(failureTabs[ii]);
            failureTabs[ii] = NULL;
        }
    }
    if (status == StatusOk) {
        assert(dstNodeRefAcquired);
        status = dstGraph_->changeDagNodeState(dstNodeId_, DgDagStateReady);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not change dagNode %s (%lu) state to Ready. "
                    "Status: %s",
                    dstNodeName,
                    dstNodeId_,
                    strGetFromStatus(status));
        }
    }

    if (status != StatusOk) {
        if (addedToNs && status != StatusTableAlreadyExists) {
            tnsMgr->removeFromNs(dstGraph_->getSessionContainer(),
                                 dstTableTrack_.tableId,
                                 dstNodeName);
        }
        if (dstNodeRefAcquired) {
            Status status2;
            // Only safe to change to error state if we have the reference to it
            status2 =
                dstGraph_->changeDagNodeState(dstNodeId_, DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode %s (%lu) state to "
                        "Error: %s",
                        dstNodeName,
                        dstNodeId_,
                        strGetFromStatus(status2));
            }
        }

        if (needsXdb() && xdbCreated_) {
            assert(dstXdbId_ != XdbIdInvalid);
            XdbMgr::get()->xdbDrop(dstXdbId_);
        }
    }

    if (dstNodeRefAcquired) {
        dstGraph_->putDagNodeRefById(dstNodeId_);
        dstNodeRefAcquired = false;
    }

    // Release the exclusive access handle.
    if (dstTableTrack_.tableHandleValid) {
        tnsMgr->closeHandleToNs(&dstTableTrack_.tableHandle);
        dstTableTrack_.tableHandleValid = false;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);
    if (status == StatusOk) {
        if (output != NULL) {
            // The dstNodeName might have been updated. E.g. prefixed, etc.
            dstNodeName = getDstNodeName(output);
        }
        xSyslog(moduleName,
                XlogInfo,
                "%s %s(%lu) (rows: %lu) (size: %lu) (query: %s) "
                "finished in %lu:%02lu:%02lu.%03lu",
                strGetFromXcalarApis(api_),
                dstNodeName,
                dstNodeId_,
                opStatus->atomicOpDetails.numRowsTotal,
                opStatus->atomicOpDetails.sizeTotal,
                query_,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);
        StatsLib::statAtomicIncr64(Operators::operationsSucceeded);
    } else {
        xSyslog(moduleName,
                XlogErr,
                "%s %s(%lu) (query: %s) "
                "failed in %lu:%02lu:%02lu.%03lu: %s",
                strGetFromXcalarApis(api_),
                dstNodeName,
                dstNodeId_,
                query_,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
        StatsLib::statAtomicIncr64(Operators::operationsFailed);
    }
    return status;
}

Status
OperatorHandler::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                               DagTypes::GraphType srcGraphType)
{
    Status status = StatusUnknown;
    uint64_t srcSessionOpsIdx = 0;
    UserMgr *usrMgr = UserMgr::get();
    DagLib *dlib = DagLib::get();

    assert(dstGraph_ != NULL);

    // Did we catch you lying? Did you promise to provide us the parentNodeIds
    // but backed out in the end? Please don't lie to us
    assert(!parentNodeIdsToBeProvided_);

    status = getParentNodes(&numParents_,
                            &parentSessionContainers_,
                            &parentNodeIds_,
                            &parentGraphIds_);
    if (status == StatusDagNodeNotFound &&
        dstGraphType_ == DagTypes::QueryGraph) {
        // In the query graph, each node can be root. So doesn't matter
        // if this node has no parents
        assert(numParents_ == 0 && parentNodeIds_ == NULL &&
               parentGraphs_ == NULL);
        status = StatusOk;
    } else if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not find parent nodes for %s: %s",
                getDstNodeName(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numParents_) {
        parentGraphs_ = new (std::nothrow) Dag *[numParents_];
        BailIfNullMsg(parentGraphs_,
                      StatusNoMem,
                      moduleName,
                      "Failed createDagNode: %s",
                      strGetFromStatus(status));

        for (uint64_t ii = 0; ii < numParents_; ii++) {
            // Source session ops is tracked only if it's not already a local
            // session.
            if (dstGraph_->getId() != parentGraphIds_[ii]) {
                // XXX TODO
                // * Note that lookup of Session for ops tracking may fail, if
                // the session belongs to a remote cluster-node. This is a known
                // limitation of this implementation.
                // * Need a clean way to track Session ops for non-local
                // cluster-node.
                status = usrMgr->trackOutstandOps(&parentSessionContainers_[ii],
                                                  UserMgr::OutstandOps::Inc);
                BailIfFailedMsg(moduleName,
                                status,
                                "Failed trackOutstandOps userName '%s', "
                                "session "
                                "'%s', Id %ld: %s",
                                parentSessionContainers_[ii].userId.userIdName,
                                parentSessionContainers_[ii]
                                    .sessionInfo.sessionName,
                                parentSessionContainers_[ii]
                                    .sessionInfo.sessionId,
                                strGetFromStatus(status));
            }
            srcSessionOpsIdx++;

            // Now the parent DAG is guaranteed to be live.
            verify((parentGraphs_[ii] =
                        dlib->getDagLocal(parentGraphIds_[ii])) != NULL);
        }
    }

    status = createDagNode(&dstNodeId_,
                           srcGraphType,
                           getDstNodeName(),
                           numParents_,
                           parentGraphs_,
                           parentNodeIds_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node %s: %s",
                getDstNodeName(),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (dstNodeIdOut != NULL) {
        *dstNodeIdOut = dstNodeId_;
    }

    if (status != StatusOk) {
        for (uint64_t ii = 0; ii < srcSessionOpsIdx; ii++) {
            // Source session ops is tracked only if it's not already a local
            // session.
            assert(parentGraphIds_[ii] != DagTypes::DagIdInvalid);
            if (dstGraph_->getId() != parentGraphIds_[ii]) {
                Status status2 =
                    usrMgr->trackOutstandOps(&parentSessionContainers_[ii],
                                             UserMgr::OutstandOps::Dec);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed trackOutstandOps userName '%s', session "
                            "'%s', "
                            "Id %ld: %s",
                            parentSessionContainers_[ii].userId.userIdName,
                            parentSessionContainers_[ii]
                                .sessionInfo.sessionName,
                            parentSessionContainers_[ii].sessionInfo.sessionId,
                            strGetFromStatus(status2));
                }
            }
        }

        for (uint64_t ii = 0; ii < numParents_; ii++) {
            parentGraphIds_[ii] = DagTypes::DagIdInvalid;
        }
    }

    return status;
}

void
OperatorHandler::setParents(uint64_t numParents,
                            DagTypes::NodeId *parentNodeIds,
                            XcalarApiUdfContainer *parentSessionContainers,
                            DagTypes::DagId *parentGraphIds,
                            Dag **parentGraphs)
{
    numParents_ = numParents;
    parentNodeIds_ = parentNodeIds;
    parentSessionContainers_ = parentSessionContainers;
    parentGraphIds_ = parentGraphIds;
    parentGraphs_ = parentGraphs;
}

Status
OperatorHandler::copySrcXdbMeta(void *optimizerContext,
                                XdbMeta *srcMeta,
                                NewTupleMeta *tupMeta,
                                unsigned &numDatasets,
                                DsDatasetId *&datasetIds,
                                unsigned &numImmediates,
                                const char **&immediateNames,
                                unsigned &numFatptrs,
                                const char **&fatptrPrefixNames)
{
    Status status = StatusOk;
    const NewTupleMeta *srcTupMeta;
    unsigned ii;

    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t srcNumFields = srcTupMeta->getNumFields();
    numDatasets = srcMeta->numDatasets;

    for (ii = 0; ii < numDatasets; ii++) {
        datasetIds[ii] = srcMeta->datasets[ii]->getDatasetId();
    }

    if (optimizerContext != NULL) {
        // Copy only the fields we need from srcTable
        status =
            Optimizer::populateValuesDescWithFieldsRequired(optimizerContext,
                                                            0,
                                                            NULL,
                                                            NULL,
                                                            srcTupMeta,
                                                            tupMeta,
                                                            srcMeta,
                                                            immediateNames,
                                                            numImmediates,
                                                            fatptrPrefixNames,
                                                            numFatptrs);
        BailIfFailed(status);
    } else {
        // copy over prefix names
        numFatptrs = 0;

        for (ii = 0; ii < srcNumFields; ii++) {
            if (srcTupMeta->getFieldType(ii) == DfFatptr) {
                fatptrPrefixNames[numFatptrs] =
                    srcMeta->kvNamedMeta.valueNames_[ii];
                numFatptrs++;
            }
        }

        // copy over all the immediates from srcTable
        numImmediates = 0;

        tupMeta->setNumFields(srcNumFields);
        for (ii = 0; ii < srcNumFields; ii++) {
            tupMeta->setFieldType(srcTupMeta->getFieldType(ii), ii);
            if (srcTupMeta->getFieldType(ii) != DfFatptr) {
                immediateNames[numImmediates] =
                    srcMeta->kvNamedMeta.valueNames_[ii];
                numImmediates++;
            }
        }
        assert(numImmediates == srcMeta->numImmediates);
    }

CommonExit:
    return status;
}

// creates a copy of srcXdb with some extra immediates
Status
OperatorHandler::createSrcXdbCopy(
    XdbId srcXdbId,
    XdbId dstXdbId,
    DfFieldType *immediateTypesIn,
    char *immediateNamesIn[XcalarApiMaxFieldNameLen + 1],
    unsigned numImmediatesIn,
    void *optimizerContext)
{
    unsigned numDatasets = 0;
    DsDatasetId datasetIds[TupleMaxNumValuesPerRecord];
    XdbMeta *srcMeta = NULL;
    const NewTupleMeta *tupMeta;
    NewTupleMeta tupMetaTmp;
    Status status;
    const char *immediateNames[TupleMaxNumValuesPerRecord];
    const char *fatptrPrefixNames[TupleMaxNumValuesPerRecord];
    unsigned numImmediates = 0, numFatptrs = 0;
    unsigned ii, jj;
    XdbMgr *xdbMgr = XdbMgr::get();

    // verified the dagNode beforehand
    status = xdbMgr->xdbGet(srcXdbId, NULL, &srcMeta);
    assert(status == StatusOk);

    // copy over src keys
    const char *keyNames[srcMeta->numKeys];
    DfFieldType keyTypes[srcMeta->numKeys];
    int keyIndexes[srcMeta->numKeys];
    Ordering keyOrdering[srcMeta->numKeys];

    unsigned numKeys = 0;
    bool keyReplaced = false;

    for (ii = 0; ii < srcMeta->numKeys; ii++) {
        // check if we are replacing any keys
        for (jj = 0; jj < numImmediatesIn; jj++) {
            if (strcmp(immediateNamesIn[jj], srcMeta->keyAttr[ii].name) == 0) {
                keyReplaced = true;
                break;
            }
        }

        if (jj == numImmediatesIn) {
            // keep this key
            keyNames[numKeys] = srcMeta->keyAttr[ii].name;
            keyTypes[numKeys] = srcMeta->keyAttr[ii].type;
            keyIndexes[numKeys] = srcMeta->keyAttr[ii].valueArrayIndex;
            keyOrdering[numKeys] = srcMeta->keyAttr[ii].ordering;

            numKeys++;
        }
    }

    // if we replaced any of the keys, reset to the default key
    if (keyReplaced) {
        numKeys = 1;
        keyNames[0] = DsDefaultDatasetKeyName;
        keyTypes[0] = DfInt64;
        keyIndexes[0] = NewTupleMeta::DfInvalidIdx;
        keyOrdering[0] = Random;
    }

    const NewTupleMeta *srcTupMeta;
    size_t srcNumFields;

    // copy over datasets
    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    srcNumFields = srcTupMeta->getNumFields();
    numDatasets = srcMeta->numDatasets;
    for (ii = 0; ii < numDatasets; ii++) {
        datasetIds[ii] = srcMeta->datasets[ii]->getDatasetId();
    }

    if (optimizerContext != NULL) {
        status =
            Optimizer::populateValuesDescWithFieldsRequired(optimizerContext,
                                                            numKeys,
                                                            keyIndexes,
                                                            keyNames,
                                                            srcTupMeta,
                                                            &tupMetaTmp,
                                                            srcMeta,
                                                            immediateNames,
                                                            numImmediates,
                                                            fatptrPrefixNames,
                                                            numFatptrs);
        BailIfFailed(status);
    } else {
        // copy over prefix names
        for (ii = 0; ii < srcNumFields; ii++) {
            if (srcTupMeta->getFieldType(ii) == DfFatptr) {
                fatptrPrefixNames[numFatptrs] =
                    srcMeta->kvNamedMeta.valueNames_[ii];
                numFatptrs++;
            }
        }

        // copy over all the immediates from srcTable
        tupMetaTmp.setNumFields(srcNumFields);
        for (ii = 0; ii < srcNumFields; ii++) {
            tupMetaTmp.setFieldType(srcTupMeta->getFieldType(ii), ii);
            if (srcTupMeta->getFieldType(ii) != DfFatptr) {
                immediateNames[numImmediates] =
                    srcMeta->kvNamedMeta.valueNames_[ii];
                numImmediates++;
            }
        }
        assert(numImmediates == srcMeta->numImmediates);
    }

    for (ii = 0; ii < numImmediatesIn; ii++) {
        // append the new immediates
        unsigned immSeen = 0;

        // check if we are replacing an existing immediate
        for (jj = 0; jj < tupMetaTmp.getNumFields(); jj++) {
            if (tupMetaTmp.getFieldType(jj) != DfFatptr) {
                if (strncmp(immediateNamesIn[ii],
                            immediateNames[immSeen],
                            XcalarApiMaxFieldNameLen) == 0) {
                    break;
                }
                immSeen++;
            }
        }

        if (jj == tupMetaTmp.getNumFields()) {
            // add new immediate
            tupMetaTmp.setNumFields(tupMetaTmp.getNumFields() + 1);
            numImmediates++;

            if (tupMetaTmp.getNumFields() > TupleMaxNumValuesPerRecord) {
                xSyslog(moduleName,
                        XlogErr,
                        "Number of columns (%lu) exceeds max num cols (%d)",
                        tupMetaTmp.getNumFields(),
                        TupleMaxNumValuesPerRecord);
                status = StatusFieldLimitExceeded;
                goto CommonExit;
            }
        }

        tupMetaTmp.setFieldType(immediateTypesIn[ii], jj);
        immediateNames[immSeen] = immediateNamesIn[ii];
    }

    tupMeta = &tupMetaTmp;

    status = xdbMgr->xdbCreate(dstXdbId,
                               numKeys,
                               keyNames,
                               keyTypes,
                               keyIndexes,
                               keyOrdering,
                               tupMeta,
                               datasetIds,
                               numDatasets,
                               &immediateNames[0],
                               numImmediates,
                               fatptrPrefixNames,
                               numFatptrs,
                               XdbGlobal,
                               srcMeta->dhtId);
CommonExit:
    return status;
}

Status
OperatorHandler::getSourceDagNode(const char *srcTableName,
                                  Dag *dstGraph,
                                  XcalarApiUdfContainer *retSessionContainer,
                                  DagTypes::DagId *retSrcGraphId,
                                  DagTypes::NodeId *retSrcNodeId)
{
    Status status;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    TableMgr *tmgr = TableMgr::get();
    bool isGlobal = false;

    *retSrcGraphId = DagTypes::DagIdInvalid;
    *retSrcNodeId = DagTypes::InvalidDagNodeId;

    if (tnsMgr->isValidFullyQualTableName(srcTableName)) {
        // Supplied srcTableName turned out to be fully qualified name, so pick
        // up the DagId, DagNodeId and SessionContainer information from the
        // Table object.
        status = tmgr->getDagNodeInfo(srcTableName,
                                      *retSrcGraphId,
                                      *retSrcNodeId,
                                      retSessionContainer,
                                      isGlobal);

        // XXX TODO ENG-9062
        // With Synthesize sameSession flag, we cannot enfore global Table
        // condition.
        // if (status == StatusOk && !isGlobal &&
        //    *retSrcGraphId != dstGraph->getId()) {
        //     status = StatusTableNotGlobal;
        // }

        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getSourceDagNode for \"%s\": %s",
                        srcTableName,
                        strGetFromStatus(status));
    } else {
        // Supplied srcTableName turned out to be session scoped, so pick up the
        // DagId, DagNodeId and SessionContainer information from the dstGraph
        // Dag which belongs to the local session.
        status = dstGraph->getDagNodeId(srcTableName,
                                        Dag::TableScope::FullyQualOrLocal,
                                        retSrcNodeId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getSourceDagNode for \"%s\": %s",
                        srcTableName,
                        strGetFromStatus(status));
        UserDefinedFunction::copyContainers(retSessionContainer,
                                            dstGraph->getSessionContainer());
        *retSrcGraphId = dstGraph->getId();
    }

CommonExit:
    return status;
}
