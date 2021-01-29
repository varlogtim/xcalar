// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "operators/XcalarEval.h"
#include "df/DataFormat.h"
#include "util/Math.h"
#include "operators/Xdf.h"
#include "df/DataFormat.h"
#include "dataset/Dataset.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "xdb/Xdb.h"
#include "operators/Operators.h"
#include "operators/OperatorsTypes.h"
#include "stat/Statistics.h"
#include "sys/XLog.h"
#include "transport/TransportPage.h"
#include "libapis/LibApisCommon.h"
#include "datapage/DataPage.h"
#include "datapage/DataPageIndex.h"

unsigned nodeIdNumBits;
unsigned recIdNumBits;

struct GetFieldValuesFatptrCompletionEph {
    DfFieldType *recordFieldType;
    Status *status;
    void *value;
    size_t valueBufSize;
};

// Stats
StatHandle dfGetFieldValuesFatptrCompletionCount;
StatHandle dfGetFieldValuesFatptrCount;

void
DataFormat::getFieldValuesFatptrCompletion(MsgEphemeral *eph, void *payload)
{
    GetFieldValuesFatptrCompletionEph *completionEph;
    MsgFatPointerKeysOutput *msgFatptrOutput =
        (MsgFatPointerKeysOutput *) payload;

    completionEph = (GetFieldValuesFatptrCompletionEph *) eph->ephemeral;
    *(completionEph->status) = eph->status;
    if (eph->status == StatusOk) {
        *(completionEph->recordFieldType) = msgFatptrOutput->recordFieldType;

        assert(msgFatptrOutput->valueSize <= completionEph->valueBufSize);

        memcpy(completionEph->value,
               &msgFatptrOutput->value[0],
               msgFatptrOutput->valueSize);
    }
}

// This dereferences the FatPtr, potentially going to another node.
// Populates value, numValuesReturned, numValuesRemaining, recordFieldType
Status
DataFormat::getFieldValuesFatptr(const DfRecordFatptr recFatptr,
                                 const char *fieldName,
                                 const unsigned startValueNum,
                                 DfFieldValueArray *value,
                                 const size_t valueBufSize,
                                 DfFieldType *recordFieldType)
{
    Status status = StatusOk;
    const DataPageIndex *index;
    const DfRecordId recordId = fatptrGetRecordId(recFatptr);
    const NodeId dstNodeId = fatptrGetNodeId(recFatptr);
    DfRecordId indexRecId;
    ReaderRecord record;

    if (dstNodeId != Config::get()->getMyNodeId()) {
        // Remote
        MsgFatPointerKeysInput msgFatptrInput;
        GetFieldValuesFatptrCompletionEph completionEph;

        msgFatptrInput.fatptr = recFatptr;
        strlcpy(msgFatptrInput.fieldName,
                fieldName,
                sizeof(msgFatptrInput.fieldName));
        msgFatptrInput.startValueNum = startValueNum;
        msgFatptrInput.valueSize = valueBufSize;

        completionEph.recordFieldType = recordFieldType;
        completionEph.status = &status;
        completionEph.value = value;
        completionEph.valueBufSize = valueBufSize;

        MsgEphemeral eph;
        MsgMgr::get()
            ->twoPcEphemeralInit(&eph,
                                 &msgFatptrInput,
                                 sizeof(msgFatptrInput),
                                 0,
                                 TwoPcSlowPath,
                                 TwoPcCallId::Msg2pcGetKeysUsingFatptr1,
                                 &completionEph,
                                 (TwoPcBufLife)(TwoPcMemCopyInput |
                                                TwoPcMemCopyOutput));

        TwoPcHandle twoPcHandle;
        Status twoPcStatus;
        twoPcStatus =
            MsgMgr::get()->twoPc(&twoPcHandle,
                                 MsgTypeId::Msg2pcGetKeysUsingFatptr,
                                 TwoPcDoNotReturnHandle,
                                 &eph,
                                 (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                    MsgRecvHdrPlusPayload),
                                 TwoPcSyncCmd,
                                 TwoPcSingleNode,
                                 dstNodeId,
                                 TwoPcClassNonNested);
        if (twoPcStatus != StatusOk) {
            status = twoPcStatus;
        }
        assert(!twoPcHandle.twoPcHandle);

        return status;
    }

    index = lookupIndex(recordId, &indexRecId);
    if (index == NULL) {
        assert(0);
        return StatusDfBadRecordId;
    }

    // get the record and extract desired field values
    DfFieldValue fieldValue;
    char tmpStr[DfMaxFieldValueSize];
    fieldValue.stringValTmp = tmpStr;

    index->getRecordByNum(indexRecId, &record);

    // XXX - get field by index instead; nontrivial since index differs per page
    DataValueReader dFieldValue;
    status = record.getFieldByName(fieldName, &dFieldValue, NULL);
    if (status == StatusDfFieldNoExist) {
        goto CommonExit;
    } else if (status != StatusOk) {
        // XXX - this should be fatal
        goto CommonExit;
    }

    status = dFieldValue.getAsFieldValue(sizeof(tmpStr),
                                         &fieldValue,
                                         recordFieldType);
    if (status == StatusDfFieldNoExist) {
        goto CommonExit;
    } else if (status != StatusOk) {
        // XXX - this should be fatal
        goto CommonExit;
    }

    {
        status = setFieldValueInArray(value,
                                      valueBufSize,
                                      *recordFieldType,
                                      0,
                                      fieldValue);
        if (status == StatusDfFieldNoExist) {
            goto CommonExit;
        } else if (status != StatusOk) {
            // XXX - this should be fatal
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

void
DataFormat::fatptrInit(uint64_t maxNodes, uint64_t maxRecordsPerNode)
{
    uint64_t numNodeIds = mathNearestPowerOf2(maxNodes);
    uint64_t numRecIds = mathNearestPowerOf2(maxRecordsPerNode);

    assert((uint64_t) mathLog((float64_t) 3, (float64_t) 27) == 3);

    nodeIdNumBits = (unsigned) mathLog((float64_t) 2, (float64_t) numNodeIds);
    assert(1UL << nodeIdNumBits == numNodeIds);
    recordFatptrNodeIdShift_ = 0;
    recordFatptrNodeIdMask_ = (numNodeIds - 1) << recordFatptrNodeIdShift_;

    recIdNumBits = (unsigned) mathLog((float64_t) 2, (float64_t) numRecIds);
    assert(1UL << recIdNumBits == numRecIds);
    recordFatptrRecIdShift_ = nodeIdNumBits;
    recordFatptrRecIdMask_ = (numRecIds - 1) << recordFatptrRecIdShift_;

    recordFatptrMask_ = (recordFatptrNodeIdMask_ | recordFatptrRecIdMask_);
    recordFatptrNumBits_ = nodeIdNumBits + recIdNumBits;

    assert(recordFatptrNumBits_ <= BitsPerUInt64);
}

Status DataFormat::extractFields(
    const NewKeyValueEntry *srcKvEntry,  // Contains our fatptrs/immediates
    const NewTupleMeta *tupleMeta,       // Metadata for KeyValueEntry
    // length of variableNames, validIndicesMap, fieldHandles
    int numVariables,
    DemystifyVariable *variables,
    Scalar **scratchPadScalars,    // Necessary, primary output
    NewKeyValueEntry *dstKvEntry)  // References scratchPadScalars;
{                                  // stores no raw data directly
    Status status = StatusOk;
    bool extractedValue;

    dstKvEntry->tuple_.setInvalid(0, numVariables);
    for (int ii = 0; ii < numVariables; ii++) {
        DemystifyVariable *variable = &variables[ii];
        Scalar *scalar = scratchPadScalars[ii];
        DfFieldValue valueTmp;

        assert(variable->accessor.nameDepth > 0);
        assert(variable->accessor.names != NULL);
        assert(variable->possibleEntries != NULL);
        assert(scalar != NULL);

        status = getFieldValue(srcKvEntry,
                               tupleMeta,
                               variable,
                               scalar,
                               &extractedValue);
        BailIfFailed(status);
        if (extractedValue) {
            valueTmp.scalarVal = scalar;
            dstKvEntry->tuple_.set(ii, valueTmp, DfScalarObj);
        }
    }
CommonExit:
    return status;
}

Status
DataFormat::fatptrToJsonRemote(NodeId nodeId,
                               DfRecordFatptr recFatptr,
                               const char *fatptrPrefix,
                               json_t **fatptrJson)
{
    Status status;
    int ret;
    MsgFatPointerInput msgFatptrInput;
    json_error_t jsonError;
    MsgEphemeral eph;
    FatptrToJsonBuf fatptrToJsonBuf;

    *fatptrJson = NULL;

    msgFatptrInput.fatptr = recFatptr;
    ret = strlcpy(msgFatptrInput.fatptrPrefixName,
                  fatptrPrefix,
                  sizeof(msgFatptrInput.fatptrPrefixName));
    assert(ret <= (int) sizeof(msgFatptrInput.fatptrPrefixName));

    eph.status = StatusOk;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &msgFatptrInput,
                                      sizeof(msgFatptrInput),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcGetDataUsingFatptr1,
                                      &fatptrToJsonBuf,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcGetDataUsingFatptr,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  nodeId,
                                  TwoPcClassNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);

    if (fatptrToJsonBuf.status != StatusOk) {
        status = fatptrToJsonBuf.status;
        goto CommonExit;
    }

    // This really -should- succeed, since we control the input. We can't
    // assert because memory errors can also happen
    *fatptrJson = json_loads(fatptrToJsonBuf.buf, 0, &jsonError);
    BailIfNull(*fatptrJson);

CommonExit:
    if (fatptrToJsonBuf.buf != NULL) {
        memFree(fatptrToJsonBuf.buf);
        fatptrToJsonBuf.buf = NULL;
    }
    if (status != StatusOk) {
        if (*fatptrJson != NULL) {
            json_decref(*fatptrJson);
            *fatptrJson = NULL;
        }
    }
    return status;
}

Status
DataFormat::fatptrToJson(DfRecordFatptr recFatptr,
                         const char *fatptrPrefix,
                         json_t **fatptrJson)
{
    Status status = StatusOk;
    *fatptrJson = NULL;
    const DfRecordId recordId = fatptrGetRecordId(recFatptr);
    const NodeId residentNodeId = fatptrGetNodeId(recFatptr);
    DfRecordId accessId;

    if (residentNodeId != Config::get()->getMyNodeId()) {
        status = fatptrToJsonRemote(residentNodeId,
                                    recFatptr,
                                    fatptrPrefix,
                                    fatptrJson);
        BailIfFailed(status);
    } else {
        const DataPageIndex *index;
        index = lookupIndex(recordId, &accessId);
        if (index == NULL) {
            assert(0);
            return StatusDfBadRecordId;
        }

        ReaderRecord record;
        index->getRecordByNum((int64_t) accessId, &record);
        *fatptrJson = json_object();
        BailIfNull(*fatptrJson);

        const char *fieldName;
        int foundFields = 0;
        int ii = 0;
        // XXX: use a field iterator here
        while (foundFields < record.getNumFields()) {
            DataValueReader dValue;
            status = record.getFieldByIdx(ii, &fieldName, &dValue);
            if (status == StatusDfFieldNoExist) {
                status = StatusOk;
                ++ii;
                continue;
            } else if (status != StatusOk) {
                goto CommonExit;
            }
            assert(fieldName);
            status =
                dValue.getAsJsonField(*fatptrJson, fatptrPrefix, fieldName);
            BailIfFailed(status);
            ++foundFields;
            ++ii;
        }
    }
CommonExit:
    if (status != StatusOk) {
        if (*fatptrJson != NULL) {
            json_decref(*fatptrJson);
            *fatptrJson = NULL;
        }
    }

    return status;
}
