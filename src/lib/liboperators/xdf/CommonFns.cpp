// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "df/DataFormat.h"
#include "XdfInt.h"
#include "operators/Xdf.h"
#include "msg/MessageTypes.h"
#include "operators/GenericTypes.h"
#include "operators/Operators.h"
#include "config/Config.h"
#include "xdb/Xdb.h"
#include "operators/XcalarEval.h"
#include "util/MemTrack.h"
#include "XcalarEvalInt.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"
#include "libapis/LibApisRecv.h"

static constexpr const char *moduleName = "liboperators";

Status
xdfGetBoolFromArgv(int argc, bool vals[], Scalar *argv[])
{
    int ii;
    Status status;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        assert(argv[ii]->fieldNumValues == 1);
        assert(argv[ii]->fieldType == DfBoolean);
        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        vals[ii] = fieldVal.boolVal;
    }

    return StatusOk;
}

Status
xdfGetFloat64FromArgv(int argc, float64_t vals[], Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        if (argv[ii] == NULL) {
            continue;
        }

        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        status = DataFormat::fieldToFloat64(fieldVal,
                                            argv[ii]->fieldType,
                                            &vals[ii]);
        if (status != StatusOk) {
            return status;
        }
    }

    return status;
}

Status
xdfGetNumericFromArgv(int argc, XlrDfp vals[], Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        if (argv[ii] == NULL) {
            continue;
        }

        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        status = DataFormat::fieldToNumeric(fieldVal,
                                            argv[ii]->fieldType,
                                            &vals[ii]);
        if (status != StatusOk) {
            return status;
        }
    }

    return status;
}

Status
xdfGetInt64FromArgv(int argc, int64_t vals[], Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        if (argv[ii] == NULL) {
            continue;
        }

        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        status =
            DataFormat::fieldToInt64(fieldVal, argv[ii]->fieldType, &vals[ii]);
        if (status != StatusOk) {
            return status;
        }
    }

    return status;
}

Status
xdfGetTimevalFromArgv(int argc, DfTimeval vals[], Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        if (argv[ii] == NULL) {
            continue;
        }

        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }

        vals[ii] = fieldVal.timeVal;
    }

    return status;
}

void
xdfGetTmFromTimeval(DfTimeval *timeval, tm *time)
{
    time_t unixTs = timeval->ms / 1000;
    unixTs = unixTs + timeval->tzoffset * 60 * 60;
    gmtime_r(&unixTs, time);
    time->tm_gmtoff = -timeval->tzoffset * 60 * 60;
    time->tm_zone = "";
}

void
xdfUpdateTimevalFromTm(tm *time, DfTimeval *timeval)
{
    int32_t offset = -time->tm_gmtoff;
    int32_t tz = offset / 60 / 60;
    // restore the value because timegm will modify it in place
    time_t unixTs = timegm(time);
    unixTs = unixTs - offset;
    int milliseconds = timeval->ms % 1000;
    if (unlikely(milliseconds < 0)) {
        unixTs--;
        milliseconds = 1000 + milliseconds;
    }

    timeval->ms = unixTs * 1000 + milliseconds;
    timeval->tzoffset = tz;
}

Status
xdfGetUInt64FromArgv(int argc, uint64_t vals[], Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        assert(argv[ii]->fieldNumValues == 1);
        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        status =
            DataFormat::fieldToUInt64(fieldVal, argv[ii]->fieldType, &vals[ii]);
        if (status != StatusOk) {
            return status;
        }
    }

    return status;
}

Status
xdfGetUIntptrFromArgv(int argc, uintptr_t vals[], Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        assert(argv[ii]->fieldNumValues == 1);
        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        status = DataFormat::fieldToUIntptr(fieldVal,
                                            argv[ii]->fieldType,
                                            &vals[ii]);
        if (status != StatusOk) {
            return status;
        }
    }

    return status;
}

Status
xdfGetStringFromArgv(int argc,
                     const char *vals[],
                     int stringLens[],
                     Scalar *argv[])
{
    int ii;
    Status status = StatusInval;
    DfFieldValue fieldVal;

    for (ii = 0; ii < argc; ii++) {
        if (argv[ii] == NULL || argv[ii]->fieldUsedSize == 0) {
            continue;
        }

        status = argv[ii]->getValue(&fieldVal);
        if (status != StatusOk) {
            return status;
        }
        vals[ii] = fieldVal.stringVal.strActual;
        assert(fieldVal.stringVal.strSize < INT32_MAX);
        stringLens[ii] = (int) fieldVal.stringVal.strSize - 1;
    }

    return status;
}

Status
xdfMakeNullIntoScalar(Scalar *out)
{
    DfFieldValue fieldVal;
    fieldVal.boolVal = false;
    return out->setValue(fieldVal, DfNull);
}

Status
xdfMakeBoolIntoScalar(Scalar *out, bool in)
{
    DfFieldValue fieldVal;
    fieldVal.boolVal = in;
    return out->setValue(fieldVal, DfBoolean);
}

Status
xdfMakeInt64IntoScalar(Scalar *out, int64_t in)
{
    DfFieldValue fieldVal;
    fieldVal.int64Val = in;
    return out->setValue(fieldVal, DfInt64);
}

Status
xdfMakeFloatIntoScalar(Scalar *out, float in)
{
    DfFieldValue fieldVal;
    fieldVal.float32Val = in;
    return out->setValue(fieldVal, DfFloat32);
}

Status
xdfMakeFloat64IntoScalar(Scalar *out, float64_t in)
{
    DfFieldValue fieldVal;
    fieldVal.float64Val = in;
    return out->setValue(fieldVal, DfFloat64);
}

Status
xdfMakeNumericIntoScalar(Scalar *out, XlrDfp in)
{
    DfFieldValue fieldVal;
    fieldVal.numericVal = in;
    return out->setValue(fieldVal, DfMoney);
}

Status
xdfMakeUInt64IntoScalar(Scalar *out, uint64_t in)
{
    DfFieldValue fieldVal;
    fieldVal.uint64Val = in;
    return out->setValue(fieldVal, DfUInt64);
}

Status
xdfMakeTimestampIntoScalar(Scalar *out, DfTimeval in)
{
    DfFieldValue fieldVal;
    fieldVal.timeVal = in;
    return out->setValue(fieldVal, DfTimespec);
}

Status
xdfMakeStringIntoScalar(Scalar *out, const char *in, size_t inLen)
{
    DfFieldValue fieldVal;

    assert(strlen(in) == inLen);
    fieldVal.stringVal.strActual = in;
    fieldVal.stringVal.strSize = inLen + 1;

    return out->setValue(fieldVal, DfString);
}

void
xdfAggComplete(XdfAggregateHandlers aggHandler,
               XdfAggregateAccumulators *srcAcc,
               Status status,
               Status srcAccStatus,
               AggregateContext *aggContext)
{
    assert(aggHandler < numParallelOpHandlers);

    if (status == StatusOk && srcAccStatus == StatusOk) {
        status =
            parallelOpHandlers[aggHandler].globalFn(aggContext->acc, srcAcc);
        if (aggContext->acc->status == StatusAggregateAccNotInited ||
            status != StatusOk) {
            aggContext->acc->status = status;
        }
        return;
    } else if (srcAccStatus != StatusAggregateAccNotInited &&
               XcalarEval::get()->isFatalError(srcAccStatus)) {
        // srcAccStatus == StatusAggregateAccNotInited just means
        // no work was done. aggContext->acc.status has already been
        // inited to StatusAggregateAccNotInited, so if none of the worker
        // parallelDo found anything, aggContext->acc.status to remain
        // as expected to StatusAggregateAccNotInited
        // If any of the worker parallelDo encountered an error, then we want
        // to preserve it in aggContext->acc->status. Hence, we shall not
        // allow StatusOk or StatusAggregateAccNotInited to
        // overwrite aggContext->acc->status
        assert(srcAccStatus != StatusOk);
        assert(srcAccStatus != StatusAggregateAccNotInited);
        aggContext->acc->status = srcAccStatus;
        return;
    }

    if (status != StatusOk) {
        assert(status != StatusAggregateAccNotInited);
        aggContext->acc->status = status;
    }
}

void
aggregateAndUpdate(ScalarGroupIter *groupIter,
                   XdfAggregateHandlers aggHandler,
                   XdfAggregateAccumulators *acc,
                   AggregateContext *aggContext,
                   void *broadcastPacket,
                   size_t broadcastPacketSize,
                   int argc,
                   Scalar **argv,
                   Scalar **scratchPadScalars,
                   NewKeyValueEntry *kvEntry,
                   OpEvalErrorStats *errorStats)
{
    Status aggStatus;
    Status status;
    XdfAggregateAccumulators tmpAcc;
    status =
        xdfLocalInit(aggHandler, &tmpAcc, broadcastPacket, broadcastPacketSize);
    assert(status == StatusOk);

    aggStatus =
        XcalarEval::get()->getArgs(argv, scratchPadScalars, kvEntry, groupIter);
    if (aggStatus == StatusOk) {
        aggStatus = xdfLocalAction(aggHandler, &tmpAcc, argc, argv);
    }

    if (aggStatus != StatusOk) {
        Operators::get()->updateEvalXdfErrorStats(aggStatus, errorStats);
    }

    xdfAggComplete(aggHandler, &tmpAcc, status, aggStatus, aggContext);
}

static Status
aggregateLocalInt(ScalarGroupIter *groupIter,
                  XdfAggregateHandlers aggHandler,
                  XdfAggregateAccumulators *acc,
                  AggregateContext *aggContext,
                  void *broadcastPacket,
                  size_t broadcastPacketSize)
{
    Status status;
    int argc;
    uint64_t count = 0;
    NewKeyValueEntry kvEntry;
    OpStatus *opStatus = groupIter->opStatus;
    Scalar *argv[groupIter->numArgs];
    TableCursor xdbCursor;
    bool xdbCursorInit = false;
    XdbMgr *xdbMgr = XdbMgr::get();
    OpEvalErrorStats *evalErrorStats = NULL;

    evalErrorStats =
        (OpEvalErrorStats *) memAllocExt(sizeof(OpEvalErrorStats), moduleName);
    memset(evalErrorStats, 0, sizeof(OpEvalErrorStats));

    argc = groupIter->numArgs;

    Xdb *srcXdb = groupIter->srcXdb;

    status = xdbMgr->createCursorFast(srcXdb, 0, &xdbCursor);
    if (status != StatusOk && status != StatusNoData) {
        goto CommonExit;
    }
    new (&kvEntry) NewKeyValueEntry(xdbCursor.kvMeta_);
    status = StatusOk;
    xdbCursorInit = true;

    while ((status = xdbCursor.getNext(&kvEntry)) == StatusOk) {
        aggregateAndUpdate(groupIter,
                           aggHandler,
                           acc,
                           aggContext,
                           broadcastPacket,
                           broadcastPacketSize,
                           argc,
                           argv,
                           NULL,
                           &kvEntry,
                           // XXX: evalErrorStats not propagated beyond here?
                           // what's the point?
                           evalErrorStats);
        count++;

        if (unlikely(count % XcalarConfig::GlobalStateCheckInterval == 0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }
        }
    }
    if (status != StatusNoData && status != StatusOk) {
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (evalErrorStats != NULL) {
        memFree(evalErrorStats);
        evalErrorStats = NULL;
    }
    if (xdbCursorInit) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    return status;
}

Status
parallelDo(ScalarGroupIter *groupIter,
           XdfAggregateHandlers aggHandler,
           XdfAggregateAccumulators *acc,
           void *broadcastPacket,
           size_t broadcastPacketSize)
{
    Status status = StatusUnknown;
    AggregateContext aggContext;

    aggContext.acc = acc;
    aggContext.aggregateHandler = aggHandler;
    status =
        xdfLocalInit(aggHandler, acc, broadcastPacket, broadcastPacketSize);
    assert(status == StatusOk);

    switch (groupIter->cursorType) {
    case ScalarLocalCursor: {
        if (groupIter->parallelize) {
            status = parallelAggregateLocal(groupIter,
                                            aggHandler,
                                            acc,
                                            broadcastPacket,
                                            broadcastPacketSize);
        } else {
            status = aggregateLocalInt(groupIter,
                                       aggHandler,
                                       acc,
                                       &aggContext,
                                       broadcastPacket,
                                       broadcastPacketSize);
        }
        break;
    }

    case ScalarGlobalCursor: {
        MsgXdfParallelDoInput *msgInput;
        size_t auxiliaryPacketSize;
        size_t msgInputSize;

        auxiliaryPacketSize =
            XcalarEval::get()->computeAuxiliaryPacketSize(groupIter) +
            broadcastPacketSize;

        msgInputSize = sizeof(*msgInput) + auxiliaryPacketSize;
        assert(msgInputSize < XdbMgr::bcSize());

        msgInput =
            (MsgXdfParallelDoInput *) memAllocExt(msgInputSize, moduleName);
        assert(msgInput != NULL);
        if (msgInput == NULL) {
            status = StatusNoMem;
            break;
        }

        msgInput->groupIter = *groupIter;
        // Need to zero out all the local pointers
        msgInput->groupIter.args = NULL;
        msgInput->groupIter.scalarValues = NULL;

        msgInput->aggHandler = aggHandler;
        msgInput->broadcastPacketSize = broadcastPacketSize;
        msgInput->auxiliaryPacketSize = auxiliaryPacketSize;

        // Time to populate the auxiliary packet
        size_t bytesCopied = 0;
        XcalarEval::get()->packAuxiliaryPacket(groupIter,
                                               msgInput->auxiliaryPacket,
                                               auxiliaryPacketSize,
                                               &bytesCopied);

        // Now we copy over the broadcastPacket
        assert(auxiliaryPacketSize <= bytesCopied + broadcastPacketSize);
        if (broadcastPacket != NULL) {
            memcpy(&msgInput->auxiliaryPacket[bytesCopied],
                   broadcastPacket,
                   broadcastPacketSize);
            bytesCopied += broadcastPacketSize;
        }

        assert(bytesCopied == auxiliaryPacketSize);

        // XXX FIXME need to check twoPc() statusArray
        MsgEphemeral eph;
        MsgMgr::get()->twoPcEphemeralInit(&eph,
                                          msgInput,
                                          msgInputSize,
                                          0,
                                          TwoPcSlowPath,
                                          TwoPcCallId::Msg2pcXdfParallelDo1,
                                          &aggContext,
                                          (TwoPcBufLife)(TwoPcMemCopyInput |
                                                         TwoPcMemCopyOutput));

        TwoPcHandle twoPcHandle;
        status = MsgMgr::get()->twoPc(&twoPcHandle,
                                      MsgTypeId::Msg2pcXdfParallelDo,
                                      TwoPcDoNotReturnHandle,
                                      &eph,
                                      (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                         MsgRecvHdrPlusPayload),
                                      TwoPcSyncCmd,
                                      TwoPcAllNodes,
                                      TwoPcIgnoreNodeId,
                                      TwoPcClassNonNested);
#ifdef DEBUG
        if (status == StatusOk) {
            assert(!twoPcHandle.twoPcHandle);
        }
#endif  // DEBUG

        memFree(msgInput);
        msgInput = NULL;

        break;
    }
    case ScalarSameKeyCursor:  // Fall through. Should not see SameKeyCursor
                               // here
    default:
        assert(0);
        break;
    }

    assert(status != StatusUnknown);

    return status;
}

void
xdfMsgParallelDo(MsgEphemeral *eph, void *payload)
{
    MsgXdfParallelDoInput *msgInput;
    MsgXdfParallelDoOutput msgOutput;
    ScalarGroupIter groupIter;
    Status status = StatusUnknown;
    bool msgOutputInited = false;
    void *broadcastPacket = NULL;
    Scalar **scalarValues = NULL;
    ScalarRecordArgument *args = NULL;
    int *argMapping = NULL;
    XdbMeta *srcMeta;
    Xdb *srcXdb;
    size_t payloadLength = 0;

    msgInput = (MsgXdfParallelDoInput *) payload;

    size_t bytesProcessed = 0;
    scalarValues =
        (Scalar **) memAllocExt(sizeof(*scalarValues) *
                                    msgInput->groupIter.numScalarValues,
                                moduleName);
    if (scalarValues == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // XXX Remove unnecessary xdbGet() calls and use perTxnInfo
    status =
        XdbMgr::get()->xdbGet(msgInput->groupIter.srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    XcalarEval::get()->unpackAuxiliaryPacket(&msgInput->groupIter,
                                             msgInput->auxiliaryPacket,
                                             &bytesProcessed,
                                             scalarValues,
                                             &args,
                                             &argMapping);

    // Get broadcastPacket
    broadcastPacket = &msgInput->auxiliaryPacket[bytesProcessed];
    bytesProcessed += msgInput->broadcastPacketSize;

    assert(bytesProcessed == msgInput->auxiliaryPacketSize);

    groupIter.setInput(ScalarLocalCursor,
                       msgInput->groupIter.srcXdbId,
                       true,
                       srcXdb,
                       srcMeta,
                       argMapping,
                       NULL,
                       false);

    groupIter.numScalarValues = msgInput->groupIter.numScalarValues;
    groupIter.scalarValues = scalarValues;
    groupIter.numArgs = msgInput->groupIter.numArgs;
    groupIter.args = args;
    groupIter.enforceArgCheck = msgInput->groupIter.enforceArgCheck;
    groupIter.dagId = msgInput->groupIter.dagId;
    groupIter.dstXdbId = msgInput->groupIter.dstXdbId;

    status = parallelDo(&groupIter,
                        msgInput->aggHandler,
                        &msgOutput.acc,
                        broadcastPacket,
                        msgInput->broadcastPacketSize);
    msgOutputInited = true;

    assert(sizeof(msgOutput) <= XdbMgr::bcSize());
    payloadLength = sizeof(msgOutput);
    memcpy(payload, &msgOutput, sizeof(msgOutput));

CommonExit:
    if (scalarValues != NULL) {
        memFree(scalarValues);
        scalarValues = NULL;
    }

    if (!msgOutputInited) {
        assert(status != StatusOk);
        Status tmpStatus;
        tmpStatus = xdfLocalInit(msgInput->aggHandler,
                                 &msgOutput.acc,
                                 broadcastPacket,
                                 msgInput->broadcastPacketSize);
        assert(tmpStatus == StatusOk);
        // XXX: is a temp fix until local error can be sent as is
        memcpy(payload, &msgOutput, sizeof(msgOutput));
        payloadLength = sizeof(msgOutput);
    }

    if (status == StatusNoData) {
        // Not an error
        status = StatusOk;
    }

    eph->setAckInfo(status, payloadLength);
}

void
xdfMsgParallelDoComplete(MsgEphemeral *eph, void *payload)
{
    MsgXdfParallelDoOutput *msgOutput = NULL;
    AggregateContext *aggContext = (AggregateContext *) eph->ephemeral;

    if (payload != NULL) {
        msgOutput = (MsgXdfParallelDoOutput *) payload;
        aggContext->lock.lock();
        xdfAggComplete(aggContext->aggregateHandler,
                       &msgOutput->acc,
                       eph->status,
                       msgOutput->acc.status,
                       aggContext);
        aggContext->lock.unlock();
    } else {
        assert(eph->status != StatusOk);
        // We need lock here to avoid race where we're about to
        // update the status to NotOk, but just as we're done
        // setting it to NotOk, it gets overwritten to Ok above
        aggContext->lock.lock();
        aggContext->acc->status = eph->status;
        aggContext->lock.unlock();
    }
}

Status
xdfLocalInit(XdfAggregateHandlers aggregateHandler,
             XdfAggregateAccumulators *acc,
             void *broadcastPacketIn,
             size_t broadcastPacketSize)
{
    void *broadcastPacket;
    broadcastPacket = (broadcastPacketSize > 0) ? broadcastPacketIn : NULL;
    assert(aggregateHandler < numParallelOpHandlers);
    acc->status = StatusAggregateAccNotInited;
    return parallelOpHandlers[aggregateHandler]
        .localInitFn(acc, broadcastPacket, broadcastPacketSize);
}

Status
xdfLocalAction(XdfAggregateHandlers aggregateHandler,
               XdfAggregateAccumulators *acc,
               int argc,
               Scalar *argv[])
{
    Status status;
    assert(aggregateHandler < numParallelOpHandlers);
    status = parallelOpHandlers[aggregateHandler].localFn(acc, argc, argv);
    assert(status == StatusOk || status == StatusAggregateLocalFnNeedArgument ||
           status == StatusXdfTypeUnsupported || status == StatusInval ||
           status == StatusAstWrongNumberOfArgs);
    if (acc->status != StatusOk) {
        acc->status = status;
    }
    return status;
}
