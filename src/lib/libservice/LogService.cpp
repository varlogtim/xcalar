// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "service/LogService.h"

using namespace xcalar::compute::localtypes::log;
using namespace xcalar::compute::localtypes::XcalarEnumType;

LogService::LogService() {}

ServiceAttributes
LogService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return (sattr);
}

Status
LogService::getLevel(const google::protobuf::Empty *empty,
                     GetLevelResponse *resp)
{
    Status status;
    try {
        resp->set_log_level(static_cast<XcalarEnumType::XcalarSyslogMsgLevel>(
            xsyslogGetLevel()));
        resp->set_log_flush_period_sec(xsyslogGetFlushPeriod());
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        xSyslog(moduleName,
                XlogErr,
                "Failed setting response for \"%s\": %s",
                resp->DebugString().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
LogService::setLevel(const SetLevelRequest *req, google::protobuf::Empty *empty)
{
    Status status = StatusUnknown;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();
    NodeId myNodeId = Config::get()->getMyNodeId();
    uint32_t logLevel;
    uint32_t logFlushLevel;
    Status statusLogLevelSet;
    MsgLogLevelSetInput input;
    size_t inputSize = sizeof(input);

    input.logLevel = req->log_level();
    input.logFlush = req->log_flush();
    input.logFlushLevel = req->log_flush_level();
    input.logFlushPeriod = req->log_flush_period_sec();

    logLevel = input.logLevel;
    logFlushLevel = input.logFlushLevel;
    statusLogLevelSet = StatusOk;

    if (logLevel > XlogInval) {
        xSyslog(moduleName, XlogErr, "Invalid Log Level %d", logLevel);
        status = StatusLogLevelSetInvalid;
        goto CommonExit;
    }

    assert(logLevel != XlogVerbose && logLevel <= XlogInval);

    msgMgr->twoPcEphemeralInit(&eph,
                               &input,
                               inputSize,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcLogLevelSet1,
                               &statusLogLevelSet,
                               (TwoPcBufLife)(TwoPcMemCopyInput));

    if (logFlushLevel == XlogFlushLocal) {
        status = msgMgr->twoPc(&twoPcHandle,
                               MsgTypeId::Msg2pcLogLevelSet,
                               TwoPcDoNotReturnHandle,
                               &eph,
                               (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                  MsgRecvHdrOnly),
                               TwoPcSyncCmd,
                               TwoPcSingleNode,
                               myNodeId,
                               TwoPcClassNonNested);
    } else {
        assert(logFlushLevel == XlogFlushNone ||
               logFlushLevel == XlogFlushGlobal);
        status = msgMgr->twoPc(&twoPcHandle,
                               MsgTypeId::Msg2pcLogLevelSet,
                               TwoPcDoNotReturnHandle,
                               &eph,
                               (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                  MsgRecvHdrOnly),
                               TwoPcSyncCmd,
                               TwoPcAllNodes,
                               TwoPcIgnoreNodeId,
                               TwoPcClassNonNested);
    }
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    if (status == StatusOk) {
        status = statusLogLevelSet;
    }

CommonExit:
    return status;
}
