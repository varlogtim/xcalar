// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/StatsService.h"
#include "stat/Statistics.h"
#include "libapis/LibApisCommon.h"
#include "msg/TwoPcFuncDefs.h"
#include "sys/XLog.h"

using namespace xcalar::compute::localtypes::Stats;

ServiceAttributes
StatsService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    sattr.syslogRequest = false;
    sattr.syslogResponse = false;
    return sattr;
}

Status
StatsService::getLibstats(const GetLibstatsRequest *request,
                          GetLibstatsResponse *response)
{
    Status status;
    try {
        status = StatsLib::get()->getStatsCommonProto(response, NULL, 0, true);
    } catch (std::exception &e) {
        status = StatusNoMem;
    }
    return status;
}

Status
StatsService::getStats(const GetStatsRequest *request,
                       GetStatsResponse *response)
{
    Status status;
    if (!request->getfromallnodes()) {
        try {
            status =
                StatsLib::get()->getPerfStats(request->getmeta(),
                                              response->add_perhoststats());
        } catch (std::exception &e) {
            status = StatusNoMem;
        }
        return status;
    }

    MsgEphemeral eph;
    MsgMgr *msgMgr = MsgMgr::get();
    StatsLib::GetStatsOutput output;
    output.status = StatusOk;
    output.response = response;

    bool getMeta = request->getmeta();

    msgMgr->twoPcEphemeralInit(&eph,
                               &getMeta,
                               sizeof(getMeta),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcGetPerfStats1,
                               &output,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcGetPerfStats,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcAllNodes,
                           TwoPcIgnoreNodeId,
                           TwoPcClassNonNested);
    if (status == StatusOk) {
        return output.status;
    } else {
        return status;
    }
}

Status
StatsService::resetStats(const ResetStatsRequest *request,
                         ResetStatsResponse *response)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    Config *config = Config::get();
    MsgMgr *msgMgr = MsgMgr::get();
    ResetStatsInput resetInput;
    resetInput.resetCumulativeStats = request->resetcumulativestats();
    resetInput.resetHwmStats = request->resethwmstats();

    msgMgr->twoPcEphemeralInit(&eph,
                               &resetInput,
                               sizeof(resetInput),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcResetStat1,
                               NULL,
                               (TwoPcBufLife)(TwoPcMemCopyInput));

    if (request->nodeid() >= 0) {
        if (request->nodeid() >= config->getActiveNodes()) {
            status = StatusNoSuchNode;
            goto CommonExit;
        }
        status = msgMgr->twoPc(&twoPcHandle,
                               MsgTypeId::Msg2pcResetStat,
                               TwoPcDoNotReturnHandle,
                               &eph,
                               (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                  MsgRecvHdrOnly),
                               TwoPcSyncCmd,
                               TwoPcSingleNode,
                               request->nodeid(),
                               TwoPcClassNonNested);
    } else {
        status = msgMgr->twoPc(&twoPcHandle,
                               MsgTypeId::Msg2pcResetStat,
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

CommonExit:
    return status;
}