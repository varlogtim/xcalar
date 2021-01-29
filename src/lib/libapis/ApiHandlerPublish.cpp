// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerPublish.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "xdb/HashTree.h"
#include "sys/XLog.h"
#include "msg/Xid.h"
#include "operators/XcalarEval.h"

ApiHandlerPublish::ApiHandlerPublish(XcalarApis api) : ApiHandler(api) {}

ApiHandlerPublish::~ApiHandlerPublish()
{
    TableNsMgr *tnsMgr = TableNsMgr::get();
    if (parentRefAcquired_) {
        dstGraph_->putDagNodeRefById(parentId_);
        parentRefAcquired_ = false;
    }
    if (handleTrack_.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack_.tableHandle);
        handleTrack_.tableHandleValid = false;
    }
}

ApiHandler::Flags
ApiHandlerPublish::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph);
}

Status
ApiHandlerPublish::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusOk;
    size_t size, numRows;
    DagNodeTypes::Node *srcNode;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    Stopwatch stopwatch;
    const XcalarApiUdfContainer *sessionContainer =
        dstGraph_->getSessionContainer();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    status = dstGraph_->lookupNodeById(input_->srcTable.tableId, &srcNode);
    assert(status == StatusOk);

    size = srcNode->opStatus.atomicOpDetails.sizeTotal;
    numRows = srcNode->opStatus.atomicOpDetails.numRowsTotal;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s failed to"
                      " allocate output (Required size: %lu bytes): %s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      outputSize,
                      strGetFromStatus(status));
        outputSize = 0;
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Publish table creation src table %s dst table %s started by"
            " user '%s', workbook '%s'",
            input_->srcTable.tableName,
            input_->dstTable.tableName,
            sessionContainer->userId.userIdName,
            sessionContainer->sessionInfo.sessionName);
    stopwatch.restart();

    status = HashTreeMgr::get()->createHashTree(input_->srcTable,
                                                hashTreeId_,
                                                input_->dstTable.tableName,
                                                input_->unixTS,
                                                input_->dropSrc,
                                                size,
                                                numRows,
                                                dstGraph_);

    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Publish table creation src table %s dst table %s started by"
                " user '%s', workbook '%s' finished in %lu:%02lu:%02lu.%03lu",
                input_->srcTable.tableName,
                input_->dstTable.tableName,
                sessionContainer->userId.userIdName,
                sessionContainer->sessionInfo.sessionName,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Publish table creation src table %s dst table %s started by"
                " user '%s', workbook '%s' failed in %lu:%02lu:%02lu.%03lu: %s",
                input_->srcTable.tableName,
                input_->dstTable.tableName,
                sessionContainer->userId.userIdName,
                sessionContainer->sessionInfo.sessionName,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(status == StatusOk);

    if (parentRefAcquired_) {
        dstGraph_->putDagNodeRefById(parentId_);
        parentRefAcquired_ = false;

        if (input_->dropSrc) {
            status = dstGraph_->dropNode(input_->srcTable.tableName,
                                         SrcTable,
                                         NULL,
                                         NULL,
                                         true);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop node %s during publish",
                        input_->srcTable.tableName);
                // non-fatal
                status = StatusOk;
            }
        }

        if (handleTrack_.tableHandleValid) {
            tnsMgr->closeHandleToNs(&handleTrack_.tableHandle);
            handleTrack_.tableHandleValid = false;
        }
    }

CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerPublish::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status;
    apiInput_ = input;
    input_ = &apiInput_->publishInput;
    inputSize_ = inputSize;
    hashTreeId_ = XidMgr::get()->xidGetNext();
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();

    if (!xcalarEval->isValidTableName(input_->dstTable.tableName)) {
        status = StatusInval;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s is not valid: "
                      "%s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    if (input_->dstTable.tableName[0] == '\0') {
        int ret = snprintf(input_->dstTable.tableName,
                           DagTypes::MaxNameLen + 1,
                           XcalarTempDagNodePrefix "%lu",
                           hashTreeId_);
        if (ret >= DagTypes::MaxNameLen + 1) {
            status = StatusOverflow;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Publish table src table %s dst table %s generation"
                          " failed: %s",
                          input_->srcTable.tableName,
                          input_->dstTable.tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (input_->unixTS == 0) {
        input_->unixTS = time(NULL);
    }

    status = dstGraph_->getDagNodeIds(input_->srcTable.tableName,
                                      Dag::TableScope::LocalOnly,
                                      &input_->srcTable.tableId,
                                      NULL,
                                      &handleTrack_.tableId);
    if (status == StatusDagNodeNotFound) {
        status = StatusTableNotFound;
    }
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s failed to "
                      "retrieve"
                      " dagNodeId: %s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    DgDagState state;
    status = dstGraph_->getDagNodeStateAndRef(input_->srcTable.tableId, &state);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s failed to get"
                      " dagNode state and ref: %s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    parentId_ = input_->srcTable.tableId;
    parentRefAcquired_ = true;

    if (state != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s failed on"
                      " dagNode state %d: %s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      state,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    status = tnsMgr->openHandleToNs(sessionContainer,
                                    handleTrack_.tableId,
                                    LibNsTypes::ReaderShared,
                                    &handleTrack_.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    handleTrack_.tableId,
                    strGetFromStatus(status));
    handleTrack_.tableHandleValid = true;

    // we will be dropping the src slots as we go, mark it as cleaned
    if (input_->dropSrc) {
        DagNodeTypes::Node *dagNode = NULL;

        status = dstGraph_->lookupNodeByName(input_->srcTable.tableName,
                                             &dagNode,
                                             Dag::TableScope::LocalOnly,
                                             true);
        BailIfFailed(status);

        dstGraph_->lock();

        // 1 ref for creation plus our ref
        if (refRead(&dagNode->refCount) > 2) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "%s has outstanding references, cannot drop",
                          input_->srcTable.tableName);
            dstGraph_->unlock();
            goto CommonExit;
        }

        dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateCleaned;
        dstGraph_->unlock();
    }

    input_->srcTable.xdbId =
        dstGraph_->getXdbIdFromNodeId(input_->srcTable.tableId);

    XdbMeta *srcMeta;
    const NewTupleMeta *srcTupMeta;
    status = XdbMgr::get()->xdbGet(input_->srcTable.xdbId, NULL, &srcMeta);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s failed: %s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    if (srcMeta->dhtId != XidMgr::XidSystemUnorderedDht) {
        status = StatusInval;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Publish table src table %s dst table %s failed,"
                      " must use system unordered dht: %s",
                      input_->srcTable.tableName,
                      input_->dstTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;

    // table cannot have fatptrs or XcalarBatchId column
    for (unsigned ii = 0; ii < srcTupMeta->getNumFields(); ii++) {
        if (srcTupMeta->getFieldType(ii) == DfFatptr) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Publish table src table %s dst table %s failed,"
                          " cannot publish table with fatptrs: %s",
                          input_->srcTable.tableName,
                          input_->dstTable.tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                   XcalarBatchIdColumnName) == 0) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Publish table src table %s dst table %s failed,"
                          " source table cannot have XcalarBatchId column: %s",
                          input_->srcTable.tableName,
                          input_->dstTable.tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        // table cannot have duplicate field names (case insensitive)
        for (unsigned jj = ii + 1; jj < srcTupMeta->getNumFields(); jj++) {
            if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                       srcMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Publish table src table %s dst table %s failed,"
                              " source table has duplicated names %s, %s",
                              input_->srcTable.tableName,
                              input_->dstTable.tableName,
                              srcMeta->kvNamedMeta.valueNames_[ii],
                              srcMeta->kvNamedMeta.valueNames_[jj]);
                goto CommonExit;
            }
        }
    }
CommonExit:
    return status;
}
