// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <new>

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "msg/Message.h"
#include "config/Config.h"
#include "usrnode/UsrNode.h"
#include "operators/Operators.h"
#include "operators/OperatorsTypes.h"
#include "operators/GenericTypes.h"
#include "df/DataFormat.h"
#include "xdb/Xdb.h"
#include "dataset/Dataset.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "callout/Callout.h"
#include "table/ResultSet.h"
#include "util/IntHashTable.h"
#include "msg/Xid.h"
#include "constants/XcalarConfig.h"
#include "ns/LibNs.h"
#include "ns/LibNsTypes.h"
#include "strings/String.h"
#include "operators/OperatorsXdbPageOps.h"

static constexpr const char *moduleName = "resultSet";

ResultSet::~ResultSet()
{
    if (dagId_ != XidInvalid && dagNodeId_ != XidInvalid) {
        Dag *dag = DagLib::get()->getDagLocal(dagId_);
        if (dag) {
            dag->putDagNodeRefById(dagNodeId_);
        }
    }
}

DatasetResultSet::~DatasetResultSet()
{
    Status status = Dataset::get()->closeHandleToDataset(&refHandle_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Failed Result set init dataset %s close: %s",
                dataset_->name_,
                strGetFromStatus(status));
    }
}

void
ResultSet::countEntriesLocal(MsgEphemeral *ephemeral, void *payload)
{
    MsgCountInput *in = (MsgCountInput *) payload;
    uint64_t numEntries;
    Status status = StatusOk;

    if (in->srcType == SrcTable) {
        status = Operators::get()
                     ->operatorsCountLocal(in->xid,
                                           Operators::LocalOperatorsNotUnique,
                                           &numEntries);
        assert(status == StatusOk);
    } else {
        assert(in->srcType == SrcDataset);

        DsDataset *dataset = Dataset::get()->getDatasetFromId(in->xid, &status);
        assert(status == StatusOk);

        DataPageIndex *pageIndex =
            in->errorDs ? dataset->errorPageIndex_ : dataset->pageIndex_;
        numEntries = pageIndex->getNumRecords();
    }

    ephemeral->payloadLength = sizeof(numEntries);
    *(uint64_t *) payload = numEntries;
    ephemeral->status = status;
}

void
ResultSet::countEntriesComplete(MsgEphemeral *eph, void *payload)
{
    CountEntriesEph *srcEph = (CountEntriesEph *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::getMsgDstNodeId(eph);
    srcEph->nodeStatus[dstNodeId] = eph->status;
    if (eph->status == StatusOk) {
        uint64_t numEntries = *(uint64_t *) payload;
        srcEph->numEntriesPerNode[dstNodeId] = numEntries;
    }
}

Status
ResultSet::populateNumEntries(Xid xid, bool errorDs)
{
    Status status;
    MsgCountInput msgInput;
    msgInput.srcType = srcType_;
    msgInput.xid = xid;
    msgInput.errorDs = errorDs;
    unsigned numNodes = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    CountEntriesEph srcEph;
    MsgEphemeral eph;

    nodeStatus = new (std::nothrow) Status[numNodes];
    BailIfNull(nodeStatus);

    srcEph.numEntriesPerNode = numEntriesPerNode_;
    srcEph.nodeStatus = nodeStatus;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &msgInput,
                                      sizeof(msgInput),
                                      0,
                                      TwoPcFastPath,
                                      TwoPcCallId::Msg2pcCountResultSetEntries1,
                                      &srcEph,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcCountResultSetEntries,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);

    for (unsigned ii = 0; ii < numNodes; ii++) {
        if (srcEph.nodeStatus[ii] != StatusOk) {
            status = srcEph.nodeStatus[ii];
            goto CommonExit;
        }
        numEntries_ += numEntriesPerNode_[ii];
    }

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

void
ResultSet::getNodeIdAndLocalOffset(NodeId *dstNodeId, uint64_t *localOffset)
{
    uint64_t offset = currentEntry_;
    assert(currentEntry_ < numEntries_);
    NodeId nodeId;
    unsigned numNodes = Config::get()->getActiveNodes();

    for (nodeId = 0; nodeId < numNodes; nodeId++) {
        if (offset < numEntriesPerNode_[nodeId]) {
            break;
        }

        offset -= numEntriesPerNode_[nodeId];
    }

    *dstNodeId = nodeId;
    *localOffset = offset;
}

void
ResultSet::getNextRemoteCompletion(MsgEphemeral *eph, void *payload)
{
    MsgNextOutput *out = (MsgNextOutput *) eph->ephemeral;
    if (out->status == StatusUnknown) {
        // This can only happen if the remote node failed to allocate a payload
        // We need to propagate this status up
        assert(eph->status != StatusOk);
        out->status = eph->status;
    }
}

void
ResultSet::getNextRemoteHandler(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    MsgNextInput rsInput = *(MsgNextInput *) payload;
    MsgNextOutput *out = (MsgNextOutput *) payload;
    char *strVal = NULL;
    out->numEntries = 0;

    size_t bufLen = 0;
    size_t bufRemaining = ResultSetMgr::ResultSetNextBufSize - sizeof(*out);

    json_t *entries = json_array();
    BailIfNull(entries);

    if (rsInput.srcType == SrcDataset) {
        DsDataset *dataset =
            Dataset::get()->getDatasetFromId(rsInput.xid, &status);
        assert(dataset);

        DataPageIndex *index =
            rsInput.errorDs ? dataset->errorPageIndex_ : dataset->pageIndex_;

        uint64_t numEntriesAdded;
        status = DatasetResultSet::getNextLocal(entries,
                                                rsInput.localOffset,
                                                index,
                                                rsInput.numEntriesRemaining,
                                                &numEntriesAdded);
        BailIfFailed(status);

        assert(json_array_size(entries) == numEntriesAdded);
    } else {
        Xdb *xdb;
        status = XdbMgr::get()->xdbGet(rsInput.xid, &xdb, NULL);
        assert(status == StatusOk);

        uint64_t numEntriesAdded;
        status = TableResultSet::getNextLocal(entries,
                                              rsInput.localOffset,
                                              xdb,
                                              rsInput.numEntriesRemaining,
                                              &numEntriesAdded);
        BailIfFailed(status);

        assert(json_array_size(entries) == numEntriesAdded);
    }

    uint64_t ii;
    json_t *val;
    json_array_foreach (entries, ii, val) {
        strVal = json_dumps(val, JSON_COMPACT | JSON_PRESERVE_ORDER);
        BailIfNull(strVal);

        int ret = snprintf(&out->entries[bufLen], bufRemaining, "%s", strVal);
        if (ret >= (int) bufRemaining) {
            // this is the maximum number of rows our buffer could hold
            status = StatusOk;
            break;
        }
        memFree(strVal);
        strVal = NULL;

        // +1 for null byte
        bufLen += ret + 1;
        bufRemaining -= ret + 1;
    }

    out->numEntries = ii;

CommonExit:
    if (strVal) {
        memFree(strVal);
        strVal = NULL;
    }
    if (entries) {
        json_decref(entries);
        entries = NULL;
    }

    out->status = status;
    eph->status = status;

    eph->payloadLength = sizeof(*out) + bufLen;
}

Status
ResultSet::getNextRemote(NodeId dstNode,
                         json_t *entries,
                         uint64_t localOffset,
                         uint64_t numEntriesRemaining,
                         uint64_t *numEntriesAdded)
{
    Status status = StatusOk;
    int ret;
    MsgEphemeral eph;
    json_t *rec = NULL;

    MsgNextOutput *out = (MsgNextOutput *) memAlloc(
        sizeof(*out) + ResultSetMgr::ResultSetNextBufSize);
    BailIfNull(out);

    out->status = StatusUnknown;

    MsgNextInput rsInput;
    rsInput.srcType = srcType_;
    rsInput.xid = xid_;
    rsInput.errorDs = errorDs_;

    rsInput.numEntriesRemaining = numEntriesRemaining;
    rsInput.localOffset = localOffset;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &rsInput,
                                      sizeof(rsInput),
                                      ResultSetMgr::ResultSetNextBufSize,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcNextResultSet1,
                                      out,
                                      TwoPcZeroCopyOutput);

    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcNextResultSet,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  dstNode,
                                  TwoPcClassNonNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);
    if (out->status != StatusOk) {
        status = out->status;
        goto CommonExit;
    }

    {
        // process returned output buf
        const char *cur = out->entries;
        for (uint64_t ii = 0; ii < out->numEntries; ii++) {
            json_error_t error;
            assert(rec == NULL);
            rec = json_loads(cur, 0, &error);
            if (rec == NULL) {
                status = StatusJsonError;
                goto CommonExit;
            }
            BailIfNull(rec);

            ret = json_array_append_new(entries, rec);
            BailIfFailedWith(ret, StatusJsonError);

            rec = NULL;
            cur += strlen(cur) + 1;
        }

        *numEntriesAdded = out->numEntries;
    }

CommonExit:
    if (out) {
        memFree(out);
        out = NULL;
    }

    if (rec) {
        json_decref(rec);
        rec = NULL;
    }

    if (status != StatusOk) {
        *numEntriesAdded = 0;
    }

    return status;
}

Status
AggregateResultSet::getNext(uint64_t numEntries, json_t **entriesArrayOut)
{
    Status status = StatusOk;
    json_t *record = NULL;
    int ret;

    json_t *entries = json_array();
    BailIfNull(entries);

    DfFieldValue val;
    status = aggVar_->getValue(&val);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error extracting result from scalar: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    record = json_object();
    BailIfNull(record);

    status = DataFormat::get()->addJsonFieldImm(record,
                                                NULL,
                                                RsDefaultConstantKeyName,
                                                aggVar_->fieldType,
                                                &val);
    BailIfFailed(status);

    ret = json_array_append_new(entries, record);
    BailIfFailedWith(ret, StatusJsonError);

    record = NULL;

CommonExit:
    *entriesArrayOut = entries;

    if (record) {
        json_decref(record);
    }

    return status;
}

Status
DatasetResultSet::getNext(uint64_t numEntriesRequested,
                          json_t **entriesArrayOut)
{
    Status status = StatusOk;
    json_t *record = NULL;
    NodeId dstNode;
    uint64_t localOffset;
    uint64_t numEntriesRemaining = numEntriesRequested;
    NodeId myNodeId = Config::get()->getMyNodeId();

    json_t *entries = json_array();
    BailIfNull(entries);

    while (numEntriesRemaining > 0 && currentEntry_ < numEntries_) {
        uint64_t entriesAdded = 0;
        getNodeIdAndLocalOffset(&dstNode, &localOffset);

        if (myNodeId == dstNode) {
            if (errorDs_) {
                status = getNextLocal(entries,
                                      localOffset,
                                      dataset_->errorPageIndex_,
                                      numEntriesRemaining,
                                      &entriesAdded);
                BailIfFailed(status);
            } else {
                status = getNextLocal(entries,
                                      localOffset,
                                      dataset_->pageIndex_,
                                      numEntriesRemaining,
                                      &entriesAdded);
                BailIfFailed(status);
            }
        } else {
            status = getNextRemote(dstNode,
                                   entries,
                                   localOffset,
                                   numEntriesRemaining,
                                   &entriesAdded);
            BailIfFailed(status);
        }

        numEntriesRemaining -= entriesAdded;
        currentEntry_ += entriesAdded;
    }

CommonExit:
    *entriesArrayOut = entries;

    if (record) {
        json_decref(record);
    }

    return status;
}

Status
DatasetResultSet::getNextLocal(json_t *entries,
                               uint64_t localOffset,
                               DataPageIndex *index,
                               uint64_t numEntries,
                               uint64_t *numEntriesAdded)
{
    Status status = StatusOk;
    DataPageIndex::RecordIterator recIter(index);
    json_t *recJson = NULL;
    const char *fieldName;
    ReaderRecord record;
    uint64_t entry = 0;

    recIter.seek(localOffset);

    for (entry = 0; entry < numEntries; entry++) {
        int ret;
        // Actually get the next record from the index
        if (unlikely(!recIter.getNext(&record))) {
            break;
        }

        recJson = json_object();
        BailIfNull(recJson);

        int foundFields = 0;
        int ii = 0;
        // XXX: use a field iterator here
        while (foundFields < record.getNumFields()) {
            DataValueReader field;
            status = record.getFieldByIdx(ii, &fieldName, &field);
            if (status == StatusDfFieldNoExist) {
                status = StatusOk;
                ++ii;
                continue;
            } else if (status != StatusOk) {
                goto CommonExit;
            }
            assert(fieldName);
            status = field.getAsJsonField(recJson, NULL, fieldName);
            BailIfFailed(status);
            ++foundFields;
            ++ii;
        }

        ret = json_array_append_new(entries, recJson);
        BailIfFailedWith(ret, StatusJsonError);
        recJson = NULL;
    }

CommonExit:
    if (recJson) {
        json_decref(recJson);
        recJson = NULL;
    }

    *numEntriesAdded = entry;

    return status;
}

Status
ResultSet::seek(uint64_t position)
{
    if (position >= numEntries_) {
        xSyslog(moduleName,
                XlogWarn,
                "Invlaid Position: %lu, numEntries_: %lu",
                position,
                numEntries_);
        return StatusPositionExceedResultSetSize;
    }

    currentEntry_ = position;
    return StatusOk;
}
