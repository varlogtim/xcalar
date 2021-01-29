// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerUpdate.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "xdb/HashTree.h"
#include "sys/XLog.h"
#include "ns/LibNs.h"
#include "udf/UserDefinedFunction.h"

ApiHandlerUpdate::ApiHandlerUpdate(XcalarApis api) : ApiHandler(api) {}

ApiHandlerUpdate::~ApiHandlerUpdate()
{
    TableNsMgr *tnsMgr = TableNsMgr::get();
    if (refs_) {
        for (unsigned ii = 0; ii < numPublishedTableRefs_; ii++) {
            if (refs_[ii].handleInit) {
                HashTreeMgr::get()
                    ->closeHandleToHashTree(&refs_[ii].handle,
                                            input_->updates[ii]
                                                .srcTable.tableId,
                                            input_->updates[ii]
                                                .srcTable.tableName);
            }
        }

        memFree(refs_);
        refs_ = NULL;
    }

    if (parents_) {
        for (unsigned ii = 0; ii < numParents_; ii++) {
            if (parents_[ii].ref) {
                dstGraph_->putDagNodeRefById(parents_[ii].parentNodeId);
                parents_[ii].ref = false;
            }

            if (handleTrack_ && handleTrack_[ii].tableHandleValid) {
                tnsMgr->closeHandleToNs(&handleTrack_[ii].tableHandle);
                handleTrack_[ii].tableHandleValid = false;
            }
        }

        memFree(parents_);
        parents_ = NULL;
    }

    if (handleTrack_) {
        delete[] handleTrack_;
        handleTrack_ = NULL;
    }
}

ApiHandler::Flags
ApiHandlerUpdate::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph);
}

Status
ApiHandlerUpdate::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusOk;
    HashTreeMgr *htreeMgr = HashTreeMgr::get();
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        XcalarApiUdfContainer *curSessionContainer =
            &refs_[ii].handle.sessionContainer;

        if (!UserDefinedFunction::containersMatch(sessionContainer,
                                                  curSessionContainer)) {
            status = StatusPTUpdatePermDenied;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed udfContainersMatch src table %s publish table %s "
                    "%lu: %s",
                    input_->updates[ii].srcTable.tableName,
                    input_->updates[ii].dstTable.tableName,
                    input_->updates[ii].dstTable.tableId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    outputSize = XcalarApiSizeOfOutput(output->outputResult.updateOutput) +
                 input_->numUpdates *
                     sizeof(*output->outputResult.updateOutput.batchIds);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output (Required size: %lu "
                "bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    // Perform in-memory updates on all publish tables.
    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        DagNodeTypes::Node *dagNode;
        status = dstGraph_->lookupNodeById(input_->updates[ii].srcTable.tableId,
                                           &dagNode);
        assert(status == StatusOk);

        status =
            htreeMgr
                ->updateInMemHashTree(input_->updates[ii].srcTable,
                                      input_->updates[ii].dstTable.tableId,
                                      input_->updates[ii].dstTable.tableName,
                                      input_->updates[ii].unixTS,
                                      input_->updates[ii].dropSrc,
                                      dagNode->opStatus.atomicOpDetails
                                          .sizeTotal,
                                      dagNode->opStatus.atomicOpDetails
                                          .numRowsTotal,
                                      refs_[ii].newBatchId,
                                      HashTreeMgr::UpdateReason::RegularUpdate);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to updateInMemHashTree src table %s "
                        "publish table %s %lu: %s",
                        input_->updates[ii].srcTable.tableName,
                        input_->updates[ii].dstTable.tableName,
                        input_->updates[ii].dstTable.tableId,
                        strGetFromStatus(status));
        refs_[ii].tabUpdated = true;
    }

    // XXX TODO
    // There is a finite window where if the XCE comes down while the update
    // persistence is in progress, the update TXN semantics will be violated
    // and the Publish tables in this update will be rendered inconsistent.
    // Currently the assumption is that upon XCE restart, all the Publish
    // tables will be restored from the original datasource from Time-0.
    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        DagNodeTypes::Node *dagNode;
        status = dstGraph_->lookupNodeById(input_->updates[ii].srcTable.tableId,
                                           &dagNode);
        assert(status == StatusOk);

        status = htreeMgr->persistUpdate(input_->updates[ii].srcTable,
                                         dstGraph_,
                                         input_->updates[ii].unixTS,
                                         refs_[ii].newBatchId,
                                         input_->updates[ii].dstTable.tableName,
                                         input_->updates[ii].dstTable.tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to persistUpdate src table %s "
                        "publish table %s %lu: %s",
                        input_->updates[ii].srcTable.tableName,
                        input_->updates[ii].dstTable.tableName,
                        input_->updates[ii].dstTable.tableId,
                        strGetFromStatus(status));
    }

    status = issueCommit();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed update: %s",
                    strGetFromStatus(status));

    // BatchIds corresponding to the updates for each Publish table is returned
    // as API output.
    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        output->outputResult.updateOutput.batchIds[ii] = refs_[ii].newBatchId;
    }
    output->outputResult.updateOutput.numUpdates = input_->numUpdates;

    if (parents_) {
        for (unsigned ii = 0; ii < numParents_; ii++) {
            if (parents_[ii].ref) {
                dstGraph_->putDagNodeRefById(parents_[ii].parentNodeId);
                parents_[ii].ref = false;
            }

            if (handleTrack_[ii].tableHandleValid) {
                tnsMgr->closeHandleToNs(&handleTrack_[ii].tableHandle);
                handleTrack_[ii].tableHandleValid = false;
            }

            if (input_->updates[ii].dropSrc) {
                status =
                    dstGraph_->dropNode(input_->updates[ii].srcTable.tableName,
                                        SrcTable,
                                        NULL,
                                        NULL,
                                        true);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to drop node %s during update",
                            input_->updates[ii].srcTable.tableName);
                    // non-fatal
                    status = StatusOk;
                }
            }
        }

        memFree(parents_);
        parents_ = NULL;
    }

CommonExit:
    if (status != StatusOk) {
        for (int ii = input_->numUpdates - 1; ii >= 0; ii--) {
            if (!refs_[ii].tabUpdated) {
                // Nothing to revert.
                continue;
            }
            Status status2 =
                htreeMgr->revertHashTree(refs_[ii].newBatchId,
                                         input_->updates[ii].dstTable.tableId,
                                         input_->updates[ii].dstTable.tableName,
                                         input_->updates[ii].unixTS);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to revert hash tree src table %s publish table "
                        "%s %lu: %s",
                        input_->updates[ii].srcTable.tableName,
                        input_->updates[ii].dstTable.tableName,
                        input_->updates[ii].dstTable.tableId,
                        strGetFromStatus(status2));
            }
        }
    }

    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerUpdate::setArg(XcalarApiInput *input, size_t inputSize)
{
    XdbMeta *srcMeta;
    const NewTupleMeta *srcTupMeta;
    XdbMeta *dstMeta;
    const NewTupleMeta *dstTupMeta;
    HashTree *hashTree;
    XdbMgr *xdbMgr = XdbMgr::get();
    Status status = StatusOk;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();

    apiInput_ = input;
    input_ = &apiInput_->updateInput;
    inputSize_ = inputSize;

    numParents_ = input_->numUpdates;

    if (numParents_ > MaxNumUpdates) {
        status = StatusInval;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed update, since numUpdates %u exceeds max allowed "
                      "%u: %s",
                      numParents_,
                      MaxNumUpdates,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    handleTrack_ =
        new (std::nothrow) TableNsMgr::TableHandleTrack[input_->numUpdates];
    BailIfNullMsg(handleTrack_,
                  StatusNoMem,
                  moduleName,
                  "Failed update: %s",
                  strGetFromStatus(status));

    refs_ = (PublishedTableRef *) memAlloc(input_->numUpdates * sizeof(*refs_));
    BailIfNullMsg(refs_,
                  StatusNoMem,
                  moduleName,
                  "Failed update: %s",
                  strGetFromStatus(status));

    memZero(refs_, input_->numUpdates * sizeof(*refs_));
    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        refs_[ii].newBatchId = HashTree::InvalidBatchId;
        refs_[ii].tabUpdated = false;
    }

    parents_ = (ParentRef *) memAlloc(input_->numUpdates * sizeof(*parents_));
    BailIfNullMsg(parents_,
                  StatusNoMem,
                  moduleName,
                  "Failed update: %s",
                  strGetFromStatus(status));

    memZero(parents_, input_->numUpdates * sizeof(*parents_));

    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        bool opCodeFound = false;
        bool rankOverFound = false;

        // setup timestamp if none provided
        if (input_->updates[ii].unixTS == 0) {
            input_->updates[ii].unixTS = time(NULL);
        }

        XcalarApiTableInput *src = &input_->updates[ii].srcTable;
        XcalarApiTableInput *dst = &input_->updates[ii].dstTable;

        status = dstGraph_->getDagNodeId(src->tableName,
                                         Dag::TableScope::LocalOnly,
                                         &src->tableId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Error retrieving dagNodeId for source table \"%s\" "
                          "publish table \"%s\": %s",
                          src->tableName,
                          dst->tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        src->xdbId = dstGraph_->getXdbIdFromNodeId(src->tableId);

        unsigned refIdx;
        for (refIdx = 0; refIdx < numPublishedTableRefs_; refIdx++) {
            if (strcmp(dst->tableName, refs_[refIdx].name) == 0) {
                // already grabbed a ref for this table
                break;
            }
        }

        if (refIdx == numPublishedTableRefs_) {
            status = HashTreeMgr::get()
                         ->openHandleToHashTree(dst->tableName,
                                                LibNsTypes::WriterExcl,
                                                &refs_[refIdx].handle,
                                                HashTreeMgr::OpenRetry::True);
            if (status != StatusOk) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Error retrieving hashtree for source \"%s\" "
                              "publish table \"%s\": %s",
                              src->tableName,
                              dst->tableName,
                              strGetFromStatus(status));
                goto CommonExit;
            }
            refs_[refIdx].handleInit = true;
            refs_[refIdx].name = dst->tableName;
            refs_[refIdx].numRefs = 1;
            numPublishedTableRefs_++;

            if (!refs_[refIdx].handle.active) {
                status = StatusPubTableInactive;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Error retrieving hashtree for source \"%s\" "
                              "publish table \"%s\": %s",
                              src->tableName,
                              dst->tableName,
                              strGetFromStatus(status));
                goto CommonExit;
            }

            if (refs_[refIdx].handle.restoring) {
                status = StatusPubTableRestoring;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Error retrieving hashtree for source \"%s\" "
                              "publish table \"%s\": %s",
                              src->tableName,
                              dst->tableName,
                              strGetFromStatus(status));
                goto CommonExit;
            }

            refs_[ii].newBatchId = refs_[refIdx].handle.currentBatchId + 1;
        } else {
            // We need to know the handle information for processing individual
            // updates. So just transfer this information from before.
            refs_[ii] = refs_[refIdx];
            refs_[ii].handleInit = false;  // Used to cleanout only
            refs_[ii].newBatchId =
                refs_[refIdx].handle.currentBatchId + 1 + refs_[refIdx].numRefs;
            refs_[refIdx].numRefs++;
        }

        DgDagState state;
        Xid hashTreeId = refs_[refIdx].handle.hashTreeId;
        dst->tableId = hashTreeId;

        status = dstGraph_->getDagNodeStateAndRef(src->tableId, &state);
        BailIfFailedMsg(moduleName,
                        status,
                        "Error getting DagNode state for source \"%s\" publish "
                        "table \"%s\": %s",
                        src->tableName,
                        dst->tableName,
                        strGetFromStatus(status));

        parents_[ii].ref = true;
        parents_[ii].parentNodeId = src->tableId;

        if (state != DgDagStateReady) {
            status = StatusDgDagNodeNotReady;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "DagNode state for source \"%s\" not ready publish "
                          "table \"%s\": %s",
                          src->tableName,
                          dst->tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        status = dstGraph_->getTableIdFromNodeId(src->tableId,
                                                 &handleTrack_[ii].tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getTableIdFromNodeId for dagNode %lu: %s",
                        src->tableId,
                        strGetFromStatus(status));

        status = tnsMgr->openHandleToNs(sessionContainer,
                                        handleTrack_[ii].tableId,
                                        LibNsTypes::ReaderShared,
                                        &handleTrack_[ii].tableHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %ld: %s",
                        handleTrack_[ii].tableId,
                        strGetFromStatus(status));
        handleTrack_[ii].tableHandleValid = true;

        // we will be dropping the src slots as we go, mark it as cleaned
        if (input_->updates[ii].dropSrc) {
            DagNodeTypes::Node *dagNode = NULL;

            status = dstGraph_->lookupNodeByName(src->tableName,
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
                              src->tableName);
                dstGraph_->unlock();
                goto CommonExit;
            }

            dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateCleaned;
            dstGraph_->unlock();
        }

        status = xdbMgr->xdbGet(src->xdbId, NULL, &srcMeta);
        BailIfFailedMsg(moduleName,
                        status,
                        "Error getting src meta for source table \"%s\" "
                        "publish table \"%s\": %s",
                        src->tableName,
                        dst->tableName,
                        strGetFromStatus(status));

        hashTree = HashTreeMgr::get()->getHashTreeById(hashTreeId);
        if (hashTree == NULL) {
            status = StatusPubTableNameNotFound;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Error retrieving hashTree for source table \"%s\" "
                          "publish table \"%s\": %s",
                          src->tableName,
                          dst->tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        dstMeta = hashTree->getXdbMeta();

        // dht must match
        if (srcMeta->dhtId != dstMeta->dhtId) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Dht mismatch source table \"%s\" publish table "
                          "\"%s\"",
                          src->tableName,
                          dst->tableName);
            goto CommonExit;
        }

        // keys must match
        if (srcMeta->numKeys != dstMeta->numKeys) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Num keys mismatch, source has %u keys"
                          ", dest has %u keys",
                          srcMeta->numKeys,
                          dstMeta->numKeys);
            goto CommonExit;
        }

        for (unsigned ii = 0; ii < srcMeta->numKeys; ii++) {
            if (strcmp(dstMeta->keyAttr[ii].name, srcMeta->keyAttr[ii].name) !=
                0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Key idx %u mismatch, source key %s does not "
                              "match"
                              "published table key %s",
                              ii,
                              srcMeta->keyAttr[ii].name,
                              dstMeta->keyAttr[ii].name);
                goto CommonExit;
            }
        }

        srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
        dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;

        // make sure the schemas match and src has
        // XcalarOpCode and XcalarRankOver, but not XcalarBatchId
        for (unsigned ii = 0; ii < srcTupMeta->getNumFields(); ii++) {
            if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                       XcalarBatchIdColumnName) == 0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "source table cannot have XcalarBatchId column, "
                              "source table \"%s\" publish table \"%s\"",
                              src->tableName,
                              dst->tableName);
                goto CommonExit;
            }

            if (!opCodeFound && strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                                       XcalarOpCodeColumnName) == 0) {
                opCodeFound = true;
            }

            if (!rankOverFound && strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                                         XcalarRankOverColumnName) == 0) {
                rankOverFound = true;
            }

            for (unsigned jj = 0; jj < dstTupMeta->getNumFields(); jj++) {
                if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                           dstMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                    if (srcTupMeta->getFieldType(ii) !=
                        dstTupMeta->getFieldType(jj)) {
                        status = StatusInval;
                        xSyslogTxnBuf(
                            moduleName,
                            XlogErr,
                            "Type mismatch for column %s: %s != %s, "
                            "source table \"%s\" publish table \"%s\"",
                            srcMeta->kvNamedMeta.valueNames_[ii],
                            strGetFromDfFieldType(srcTupMeta->getFieldType(ii)),
                            strGetFromDfFieldType(dstTupMeta->getFieldType(ii)),
                            src->tableName,
                            dst->tableName);

                        goto CommonExit;
                    }
                }
            }
        }

        if (!opCodeFound) {
            status = StatusMissingXcalarOpCode;
            goto CommonExit;
        }

        if (!rankOverFound) {
            status = StatusMissingXcalarRankOver;
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

Status
ApiHandlerUpdate::issueCommit()
{
    Status status = StatusOk;
    int64_t *commitBatchIds = NULL;

    HashTreeRefHandle **refHandles =
        new (std::nothrow) HashTreeRefHandle *[input_->numUpdates];
    BailIfNullMsg(refHandles,
                  StatusNoMem,
                  moduleName,
                  "Failed update: %s",
                  strGetFromStatus(status));

    commitBatchIds = new (std::nothrow) int64_t[input_->numUpdates];
    BailIfNullMsg(commitBatchIds,
                  StatusNoMem,
                  moduleName,
                  "Failed update: %s",
                  strGetFromStatus(status));

    for (unsigned ii = 0; ii < input_->numUpdates; ii++) {
        refHandles[ii] = &refs_[ii].handle;
        commitBatchIds[ii] = refs_[ii].newBatchId;
    }

    status = HashTreeMgr::get()->updateCommit(input_->numUpdates,
                                              refHandles,
                                              commitBatchIds);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed update: %s",
                    strGetFromStatus(status));

CommonExit:
    if (refHandles) {
        delete[] refHandles;
        refHandles = NULL;
    }

    if (commitBatchIds) {
        delete[] commitBatchIds;
        commitBatchIds = NULL;
    }
    return status;
}
