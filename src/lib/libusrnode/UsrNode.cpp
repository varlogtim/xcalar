// Copyright 2013-2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <unistd.h>

#include "primitives/Primitives.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "usrnode/UsrNode.h"
#include "xdb/Xdb.h"
#include "util/Random.h"
#include "dataset/Dataset.h"
#include "util/System.h"
#include "operators/Operators.h"
#include "libapis/LibApisCommon.h"
#include "df/DataFormat.h"
#include "util/MemTrack.h"
#include "dag/DagLib.h"
#include "dag/DagTypes.h"
#include "libapis/LibApisRecv.h"
#include "sys/XLog.h"
#include "operators/OperatorsApiWrappers.h"
#include "libapis/ApiHandlerGetStat.h"

static StatHandle loadDsHandle;
static StatGroupId statsGrpId;
static constexpr const char *moduleName = "libusrnode";
static bool usrNodeInited = false;

Status
usrNodeNew()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    // Basic checks and assertions
    xSyslog(moduleName,
            XlogInfo,
            "myNodeId is %d",
            Config::get()->getMyNodeId());

    status = statsLib->initNewStatGroup("libusrnode", &statsGrpId, 1);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&loadDsHandle);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "loadDs",
                                         loadDsHandle,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    usrNodeInited = true;

CommonExit:
    if (status != StatusOk) {
        // XXX Xc-2747 Cleanup on failure.
    }
    return status;
}

// XXX - need to invoke this from usrnode & test programs
void
usrNodeDestroy()
{
}

void
usrNodeStatusComplete(MsgEphemeral *eph, void *ignore)
{
    Status *statusOutput = (Status *) eph->ephemeral;
    assert(statusOutput != NULL);
    *statusOutput = eph->status;
}

void
usrNodeStatusCompleteSingleNode(MsgEphemeral *eph, void *ignore)
{
    Status *statusArray = (Status *) eph->ephemeral;
    statusArray[0] = eph->status;
}

void
usrNodeStatusArrayComplete(MsgEphemeral *eph, void *ignore)
{
    Status *statusArray = (Status *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);
    statusArray[dstNodeId] = eph->status;
}

void
usrNodeGetTableMetaComplete(MsgEphemeral *eph, void *payload)
{
    XcalarApiOutput *output = (XcalarApiOutput *) eph->ephemeral;
    assert(output != NULL);

    XcalarApiGetTableMetaOutput *getTableMetaOutput =
        &output->outputResult.getTableMetaOutput;

    assert((uintptr_t) getTableMetaOutput == (uintptr_t) &output->outputResult);

    int dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);

    assert((unsigned) dstNodeId < getTableMetaOutput->numMetas);

    if (eph->status == StatusOk) {
        memcpy(&getTableMetaOutput->metas[dstNodeId],
               payload,
               sizeof(XcalarApiTableMeta));
    } else {
        getTableMetaOutput->metas[dstNodeId].status = eph->status.code();
    }
}

void
usrNodeMsg2pcGetDataUsingFatptr(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    size_t numBytesCopied = 0;
    char *recordStr = NULL;
    json_t *record = NULL;
    MsgFatPointerInput *msgFatptrInput;
    MsgFatPointerOutput *msgFatptrOutput;
    size_t maxPossibleSize;
    size_t payloadLength = 0;

    msgFatptrInput = (MsgFatPointerInput *) payload;
    msgFatptrOutput = (MsgFatPointerOutput *) payload;
    assert(msgFatptrInput == &msgFatptrOutput->input);

    status = DataFormat::get()->fatptrToJson(msgFatptrInput->fatptr,
                                             msgFatptrInput->fatptrPrefixName,
                                             &record);
    BailIfFailed(status);

    assert(json_typeof(record) == JSON_OBJECT);
    recordStr = json_dumps(record, JSON_COMPACT | JSON_PRESERVE_ORDER);
    BailIfNull(recordStr);

    // if recordStr is bigger than XdbMgr::bcSize() -
    // sizeof(*msgFatptrOutput)
    // we will be shipping back partial data, needs fix, use streaming instead
    // FIXME: Xc-8599

    maxPossibleSize = XdbMgr::bcSize() - sizeof(*msgFatptrOutput);
    numBytesCopied = strlcpy(msgFatptrOutput->data, recordStr, maxPossibleSize);
    if (numBytesCopied >= maxPossibleSize) {
        // this will never be entered!
        // maxPossibleSize includes \0 but numBytesCopied doesnt
        status = StatusNoBufs;
        goto CommonExit;
    }

    msgFatptrOutput->bytesCopied = numBytesCopied + 1;
    payloadLength = sizeof(*msgFatptrOutput) + numBytesCopied + 1;

CommonExit:
    if (record != NULL) {
        json_decref(record);
        record = NULL;
    }
    if (recordStr != NULL) {
        memFree(recordStr);
        recordStr = NULL;
    }

    xcAssertIf((status != StatusOk), (payloadLength == 0));

    eph->setAckInfo(status, payloadLength);
}

void
usrNodeMsg2pcGetKeysUsingFatptr(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    MsgFatPointerKeysInput *msgFatptrInput;
    MsgFatPointerKeysOutput *msgFatptrOutput = NULL;
    size_t msgFatptrOutputSize = 0;
    size_t payloadLength = 0;

    // Only the remote fetch of data is handled here.
    msgFatptrInput = (MsgFatPointerKeysInput *) payload;

    if (msgFatptrInput->valueSize >
        (XdbMgr::bcSize() - sizeof(*msgFatptrOutput))) {
        msgFatptrInput->valueSize =
            (XdbMgr::bcSize() - sizeof(*msgFatptrOutput));
    }

    msgFatptrOutputSize = sizeof(*msgFatptrOutput) + msgFatptrInput->valueSize;
    if (msgFatptrOutputSize > XdbMgr::bcSize()) {
        status = StatusOverflow;
        goto CommonExit;
    }

    msgFatptrOutput = (MsgFatPointerKeysOutput *) memAlloc(msgFatptrOutputSize);
    if (msgFatptrOutput == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    msgFatptrOutput->valueSize = msgFatptrInput->valueSize;

    status = DataFormat::get()
                 ->getFieldValuesFatptr(msgFatptrInput->fatptr,
                                        msgFatptrInput->fieldName,
                                        msgFatptrInput->startValueNum,
                                        (DfFieldValueArray *) &msgFatptrOutput
                                            ->value[0],
                                        msgFatptrInput->valueSize,
                                        &msgFatptrOutput->recordFieldType);
    if (status != StatusOk) {
        goto CommonExit;
    }

    payloadLength = msgFatptrOutputSize;
    assert(payloadLength <= XdbMgr::bcSize());
    memcpy(payload, msgFatptrOutput, msgFatptrOutputSize);

CommonExit:
    if (msgFatptrOutput != NULL) {
        memFree(msgFatptrOutput);
        msgFatptrOutput = NULL;
    }

    xcAssertIf((status != StatusOk), (payloadLength == 0));

    eph->setAckInfo(status, payloadLength);
}

void
usrNodeMsg2pcLogLevelSet(MsgEphemeral *eph, void *payload)
{
    MsgLogLevelSetInput *llsInput;
    uint32_t logLevel;
    uint32_t logFlushLevel;
    int32_t logFlushPeriod;
    Status status = StatusOk;

    llsInput = (MsgLogLevelSetInput *) payload;
    logLevel = llsInput->logLevel;
    logFlushLevel = llsInput->logFlushLevel;
    logFlushPeriod = llsInput->logFlushPeriod;

    if (logLevel != XlogInval) {
        xsyslogSetLevel(logLevel);
    }
    if (logFlushLevel != XlogFlushNone) {
        xSyslogFlush();
    }
    if (logFlushPeriod == -1 || logFlushPeriod > 0) {
        status = xSyslogFlushPeriodic(logFlushPeriod);
    }
    eph->setAckInfo(status, 0);
}
void
usrNodeMsg2pcLogLevelSetComplete(MsgEphemeral *eph, void *payload)
{
    Status *statusOutput = (Status *) eph->ephemeral;
    assert(statusOutput != NULL);
    *statusOutput = eph->status;
}

void
usrNodeMsg2pcSetConfig(MsgEphemeral *eph, void *payload)
{
    MsgSetConfigInput *input = (MsgSetConfigInput *) payload;
    Status status;

    status = Config::get()->setConfigParameterLocal(input->paramName,
                                                    input->paramValue);

    eph->setAckInfo(status, 0);
}

void
usrNodeDoFatPointerWork(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    MsgFatPointerOutput *msgFatptrOutput = NULL;
    FatptrToJsonBuf *fatptrToJsonBuf = NULL;

    assert(eph != NULL);
    fatptrToJsonBuf = (FatptrToJsonBuf *) eph->ephemeral;
    fatptrToJsonBuf->bytesCopied = 0;
    fatptrToJsonBuf->buf = NULL;

    if (eph->status != StatusOk) {
        status = eph->status;
        goto CommonExit;
    }

    assert(payload != NULL);
    msgFatptrOutput = (MsgFatPointerOutput *) payload;

    assert(msgFatptrOutput->bytesCopied > 0);
    assert(msgFatptrOutput->bytesCopied <= XdbMgr::bcSize());

    fatptrToJsonBuf->buf = (char *) memAlloc(msgFatptrOutput->bytesCopied);
    if (fatptrToJsonBuf->buf == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    fatptrToJsonBuf->bytesCopied = msgFatptrOutput->bytesCopied;
    memcpy(fatptrToJsonBuf->buf,
           msgFatptrOutput->data,
           msgFatptrOutput->bytesCopied);

CommonExit:
    xcAssertIf((status != StatusOk), (fatptrToJsonBuf->buf == NULL));
    xcAssertIf((status != StatusOk), (fatptrToJsonBuf->bytesCopied == 0));

    fatptrToJsonBuf->status = status;
}

void
usrNodeTopComplete(MsgEphemeral *eph, void *payload)
{
    XcalarApiTopOutput *topOutput = NULL;
    int dstNodeId;

    assert(eph != NULL);

    topOutput = (XcalarApiTopOutput *) eph->ephemeral;

    dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);

    assert((unsigned) dstNodeId < topOutput->numNodes);

    if (eph->status == StatusOk) {
        assert(payload != NULL);
        memcpy(&topOutput->topOutputPerNode[dstNodeId],
               payload,
               sizeof(XcalarApiTopOutputPerNode));
        topOutput->topOutputPerNode[dstNodeId].nodeId = dstNodeId;
    } else {
        topOutput->status = eph->status.code();
    }
}
