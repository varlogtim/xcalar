// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerExecuteRetina.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "dag/DagLib.h"
#include "msg/Xid.h"
#include "xdb/Xdb.h"
#include "operators/XcalarEval.h"
#include "util/Stopwatch.h"
#include "ns/LibNs.h"
#include "udf/UserDefinedFunction.h"
#include "usr/Users.h"

OperatorHandlerExecuteRetina::OperatorHandlerExecuteRetina(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

OperatorHandlerExecuteRetina::~OperatorHandlerExecuteRetina()
{
    TableNsMgr *tnsMgr = TableNsMgr::get();
    if (srcRefsAcquired_) {
        for (unsigned ii = 0; ii < numSrcNodes_; ii++) {
            if (srcRefsAcquired_[ii]) {
                dstGraph_->putDagNodeRefById(srcNodeIds_[ii]);
                srcRefsAcquired_[ii] = false;
            }
        }
        memFree(srcRefsAcquired_);
        srcRefsAcquired_ = NULL;
    }

    if (srcHandleTrack_) {
        for (unsigned ii = 0; ii < numSrcNodes_; ii++) {
            if (srcHandleTrack_[ii].tableHandleValid) {
                tnsMgr->closeHandleToNs(&srcHandleTrack_[ii].tableHandle);
                srcHandleTrack_[ii].tableHandleValid = false;
            }
        }
        delete[] srcHandleTrack_;
        srcHandleTrack_ = NULL;
    }

    if (retina_ != NULL) {
        DagLib::get()->putRetinaInt(retina_);
        retina_ = NULL;
    }

    if (srcNodeIds_) {
        memFree(srcNodeIds_);
        srcNodeIds_ = NULL;
    }

    if (pathOpened_) {
        Status status2 = LibNs::get()->close(nsHandle_, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    input_->retinaName,
                    strGetFromStatus(status2));
        }
        if (retinaImported_) {
            Status status2 = DagLib::get()->deleteRetina(input_->retinaName);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to delete batch dataflow '%s': %s",
                        input_->retinaName,
                        strGetFromStatus(status2));
            }
        }
    }
}

OperatorHandler::Flags
OperatorHandlerExecuteRetina::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | IsOperator);
}

const char *
OperatorHandlerExecuteRetina::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerExecuteRetina::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.executeRetinaOutput.tableName;
}

Status
OperatorHandlerExecuteRetina::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    // this should never be called, run is used instead
    assert(0);
    return StatusUnimpl;
}

Status
OperatorHandlerExecuteRetina::run(XcalarApiOutput **outputOut,
                                  size_t *outputSizeOut,
                                  void *optimizerContext)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;

    xSyslog(moduleName,
            XlogErr,
            "OperatorHandlerExecuteRetina api is deprecated");
    assert(0);

    *outputOut = output;
    *outputSizeOut = outputSize;
    return StatusInval;
}

// XXX TODO This is kept around for upgrade case. Should we just delete all
// this code and break upgrade, since it's not needed anyway?
Status
OperatorHandlerExecuteRetina::setArg(XcalarApiInput *input,
                                     size_t inputSize,
                                     bool parentNodeIdsToBeProvided)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->executeRetinaInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->executeRetinaInput;
    size_t expectedInputSize;
    XcalarEval *xcalarEval = XcalarEval::get();
    LibNs *libNs = LibNs::get();
    RetinaNsObject *retinaNsObject = NULL;
    RetinaId retinaId;
    XcalarApiDagOutput *listDagsOut = NULL;
    char retinaPathName[LibNsTypes::MaxPathNameLen + 1];
    char **srcNames = NULL;
    size_t outputSize;
    XcalarApis api = XcalarApiSynthesize;
    XcalarApiUdfContainer *udfContainer = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    int ret;
    dstTableId_ = tnsMgr->genTableId();

    expectedInputSize =
        sizeof(*input_) +
        (sizeof(input_->parameters[0]) * input_->numParameters) +
        input_->exportRetinaBufSize;
    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes (numParamters: %lu)",
                inputSize,
                expectedInputSize,
                input_->numParameters);
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->numParameters > XcalarApiMaxNumParameters) {
        xSyslog(moduleName,
                XlogErr,
                "numParameters (%lu) exceeds max num parameters: %u",
                input_->numParameters,
                XcalarApiMaxNumParameters);
        status = StatusInval;
        goto CommonExit;
    }

    for (ssize_t ii = 0; ii < input_->numParameters; ii++) {
        if (input_->parameters[ii].parameterName[0] == '\0' ||
            input_->parameters[ii].parameterValue[0] == '\0') {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "%s [%ld] has invalid parameterName '%s'"
                          " parameterValue '%s'",
                          input_->retinaName,
                          ii,
                          input_->parameters[ii].parameterName,
                          input_->parameters[ii].parameterValue);
            status = StatusInval;
            goto CommonExit;
        }
    }

    if (!xcalarEval->isValidTableName(input_->dstTable.tableName)) {
        xSyslog(moduleName,
                XlogErr,
                "Destination table name (%s) is not valid",
                input_->dstTable.tableName);
        status = StatusInval;
        goto CommonExit;
    }

    // input_->retinaName isn't necessary to validate since the libNs
    // invocations below will end up validating the name. There are two
    // scenarios:
    // 0. This is an attempt to execute an external dataflow from XD- so there's
    //    no embedded retina (i.e. input_->exportRetinaBuf == NULL). if the name
    //    is invalid, the libNs->open below will fail, and so will the attempt
    //    to import the retina (there's no embedded retina).
    // 1. This is a retina replay - so there's an embedded retina in
    //    input_->exportRetinaBuf. So this is a replay of a previously executed
    //    retina in which case, the name is synthetic and drawn from the name
    //    embedded in the retina itself - the open is expected to fail, followed
    //    by the import - which creates/makes the retina temporarily, with the
    //    new synthetic name. Name validation isn't needed here, since the
    //    retina is being made for the first time (each time), and any name
    //    supplied, in a sense, is fine, since the name isn't expected to be
    //    visible - it's just a way to address it during the execute retina
    //    operation.

    ret = snprintf(retinaPathName,
                   LibNsTypes::MaxPathNameLen + 1,
                   "%s%s",
                   RetinaNsObject::PathPrefix,
                   input_->retinaName);
    if (ret < 0 || ret >= (int) (LibNsTypes::MaxPathNameLen + 1)) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina and gets the object from
    // where we'll obtain the Retina ID associated with the retina name.
    nsHandle_ = libNs->open(retinaPathName,
                            LibNsTypes::ReaderShared,
                            (NsObject **) &retinaNsObject,
                            &status);
    if (status == StatusNsNotFound || status == StatusPendingRemoval) {
        status = DagLib::get()->importRetinaFromExecute(input_);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to import batch dataflow '%s': %s",
                    input_->retinaName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        nsHandle_ = libNs->open(retinaPathName,
                                LibNsTypes::ReaderShared,
                                (NsObject **) &retinaNsObject,
                                &status);
        assert(status == StatusOk);  // it just got published in import!
        retinaImported_ = true;  // remove synthetic retina after execute's done
    }
    if (status != StatusOk) {
        if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened_ = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    if (userId_ != NULL) {
        input_->userId = *userId_;
    }

    if (strlen(input_->udfUserName) > 0 && strlen(input_->udfSessionName) > 0) {
        // A json-retina is being executed (i.e. a retina imported from json
        // which has no UDFs of its own). In this case, a udfContainer must be
        // constructed which is needed for the call to getRetinaInt() which
        // needs to parse the serialized dag (which is in the golden
        // template, in json layout) with this container.

        status = UserMgr::get()->getUdfContainer(input_->udfUserName,
                                                 input_->udfSessionName,
                                                 &udfContainer);
        assert(status == StatusOk || udfContainer == NULL);
        BailIfFailed(status);
    }

    status = DagLib::get()->getRetinaInt(retinaId, udfContainer, &retina_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = retina_->dag->listAvailableNodes("*",
                                              &listDagsOut,
                                              &outputSize,
                                              1,
                                              &api);
    BailIfFailed(status);

    // extract only the synthesize nodes that want tables from our session
    numSrcNodes_ = 0;

    srcNames = (char **) memAlloc(listDagsOut->numNodes * sizeof(*srcNames));
    BailIfNull(srcNames);

    for (unsigned ii = 0; ii < listDagsOut->numNodes; ii++) {
        if (listDagsOut->node[ii]->input->synthesizeInput.sameSession) {
            // the source for this synthesize is part of the retina itself
            // and does not exist in the current session
            continue;
        }

        status =
            DagLib::get()->variableSubst(listDagsOut->node[ii]->input,
                                         XcalarApiSynthesize,
                                         input_->numParameters,
                                         input_->parameters,
                                         retina_->parameterHashTableByName);
        BailIfFailed(status);

        srcNames[numSrcNodes_++] =
            listDagsOut->node[ii]->input->synthesizeInput.source.name;
    }

    srcRefsAcquired_ =
        (bool *) memAlloc(sizeof(*srcRefsAcquired_) * numSrcNodes_);
    BailIfNull(srcRefsAcquired_);
    memZero(srcRefsAcquired_, sizeof(*srcRefsAcquired_) * numSrcNodes_);

    srcNodeIds_ = (Xid *) memAlloc(sizeof(*srcNodeIds_) * numSrcNodes_);
    BailIfNull(srcNodeIds_);

    srcHandleTrack_ =
        new (std::nothrow) TableNsMgr::TableHandleTrack[numSrcNodes_];
    BailIfNull(srcHandleTrack_);

    retina_->sessionDag = dstGraph_;

    // If dstGraph is a query, then we can make any operator the root
    // (i.e. operators don't need to have parents)
    if (!parentNodeIdsToBeProvided) {
        for (unsigned ii = 0; ii < numSrcNodes_; ii++) {
            // XXX TODO Do we need to support fully qualified table names?
            status = dstGraph_->getDagNodeId(srcNames[ii],
                                             Dag::TableScope::LocalOnly,
                                             &srcNodeIds_[ii]);
            BailIfFailedTxnMsg(moduleName,
                               status,
                               "Failed to get source %s: %s",
                               srcNames[ii],
                               strGetFromStatus(status));

            status = dstGraph_->getDagNodeRefById(srcNodeIds_[ii]);
            BailIfFailedTxnMsg(moduleName,
                               status,
                               "Failed to grab ref for source %s: %s",
                               srcNames[ii],
                               strGetFromStatus(status));

            srcRefsAcquired_[ii] = true;

            status =
                dstGraph_->getTableIdFromNodeId(srcNodeIds_[ii],
                                                &srcHandleTrack_[ii].tableId);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed getTableIdFromNodeId for dagNode %lu: %s",
                            srcNodeIds_[ii],
                            strGetFromStatus(status));

            if (tnsMgr->isTableIdValid(srcHandleTrack_[ii].tableId)) {
                status =
                    tnsMgr->openHandleToNs(dstGraph_->getSessionContainer(),
                                           srcHandleTrack_[ii].tableId,
                                           LibNsTypes::ReaderShared,
                                           &srcHandleTrack_[ii].tableHandle,
                                           TableNsMgr::OpenSleepInUsecs);
                BailIfFailedMsg(moduleName,
                                status,
                                "Failed to open handle to table %ld: %s",
                                srcHandleTrack_[ii].tableId,
                                strGetFromStatus(status));
                srcHandleTrack_[ii].tableHandleValid = true;
            }

            DagNodeTypes::Node *dagNode;
            status = dstGraph_->lookupNodeById(srcNodeIds_[ii], &dagNode);
            assert(status == StatusOk);

            if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiAggregate) {
                status = StatusInval;
                BailIfFailedTxnMsg(moduleName,
                                   status,
                                   "%s is not a valid source, "
                                   "api not supported",
                                   srcNames[ii]);
            }
        }
    }

    status = StatusOk;
CommonExit:
    if (srcNames) {
        memFree(srcNames);
        srcNames = NULL;
    }

    if (listDagsOut) {
        memFree(listDagsOut);
        listDagsOut = NULL;
    }

    return status;
}

Status
OperatorHandlerExecuteRetina::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    *numParentsOut = 0;
    *parentNodeIdsOut = NULL;
    *parentGraphIdsOut = NULL;
    *sessionContainersOut = NULL;
    return StatusOk;
}

Status
OperatorHandlerExecuteRetina::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                            DagTypes::GraphType srcGraphType,
                                            const char *nodeNameIn,
                                            uint64_t numParents,
                                            Dag **parentGraphs,
                                            DagTypes::NodeId *parentNodeIds)
{
    Status status = StatusUnknown;
    DagTypes::NodeId dstNodeId = DagTypes::InvalidDagNodeId;
    size_t ret;

    ret = strlcpy(nodeName_, nodeNameIn, sizeof(nodeName_));
    if (ret >= sizeof(nodeName_)) {
        xSyslog(moduleName,
                XlogErr,
                "nodeName \"%s\" is too long (%lu chars). Max is "
                "%lu chars",
                nodeNameIn,
                strlen(nodeNameIn),
                sizeof(nodeName_) - 1);
        status = StatusNoBufs;
        goto CommonExit;
    }

    if (dstGraphType_ == DagTypes::QueryGraph) {
        // If we're not exporting a table, then the transplant wouldn't
        // happen. So we need to create a dagNode here.
        status = dstGraph_->createNewDagNode(api_,
                                             apiInput_,
                                             inputSize_,
                                             dstXdbId_,
                                             dstTableId_,
                                             nodeName_,
                                             numParents,
                                             parentGraphs,
                                             parentNodeIds,
                                             &dstNodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create deleteObj node: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        // If we're transplanting a dagNode from the executed retina
        // to the sessionGraph, the transplanted dagNode will be
        // created in the run() func itself

        // copy over the parent node ids into srcNodeIds array
        for (unsigned ii = 0; ii < numParents; ii++) {
            srcNodeIds_[ii] = parentNodeIds[ii];
        }
        status = StatusOk;
    }

    dstNodeId_ = dstNodeId;
CommonExit:
    if (status != StatusOk) {
        if (dstNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                dstGraph_->dropAndChangeState(dstNodeId, DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        dstNodeId,
                        strGetFromStatus(status2));
            }
            dstNodeId = DagTypes::InvalidDagNodeId;
        }
    }

    *dstNodeIdOut = dstNodeId;
    return status;
}
