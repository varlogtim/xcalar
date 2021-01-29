// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisEnums.h"
#include "dag/DagTypes.h"
#include "dag/Dag.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "udf/UserDefinedFunction.h"

static constexpr const char *moduleName = "libapis::apiHandler";

ApiHandler::ApiHandler(XcalarApis api)
    : userId_(NULL),
      api_(api),
      apiInput_(NULL),
      inputSize_(0),
      dstGraph_(NULL),
      dstGraphType_(DagTypes::InvalidGraph),
      sessionInfo_(NULL)
{
}

ApiHandler::~ApiHandler()
{
    if (txnLogAdded_) {
        MsgMgr::get()->deleteTxnLog(txn_);
    }
}

XcalarApiInput *
ApiHandler::getInput()
{
    return apiInput_;
}

size_t
ApiHandler::getInputSize()
{
    return inputSize_;
}

XcalarApis
ApiHandler::getApi()
{
    return api_;
}

bool
ApiHandler::needsAck()
{
    return (getFlags() & NeedsAck) != 0;
}

bool
ApiHandler::mayNeedSessionOrGraph()
{
    return (getFlags() & MayNeedSessionOrGraph) != 0;
}

bool
ApiHandler::needsSessionOrGraph()
{
    return (getFlags() & NeedsSessionOrGraph) != 0;
}

bool
ApiHandler::needsToRunImmediately()
{
    return (getFlags() & NeedsToRunImmediately) != 0;
}

bool
ApiHandler::needsToRunInline()
{
    // enable this bit iff you're sure the api will not block
    // this will be executed on the listener thread
    return (getFlags() & NeedsToRunInline) != 0;
}

bool
ApiHandler::hasAsync()
{
    return (getFlags() & HasAsync) != 0;
}

bool
ApiHandler::isOperator()
{
    return (getFlags() & IsOperator) != 0;
}

bool
ApiHandler::needsXdb()
{
    return (getFlags() & NeedsXdb) != 0;
}

Status
ApiHandler::init(XcalarApiUserId *userId,
                 XcalarApiSessionInfoInput *sessionInfo,
                 Dag *dstGraph)
{
    DagTypes::GraphType dagType;
    XcalarApiUdfContainer *sessionContainer = NULL;

    dagType = dstGraph ? dstGraph->getDagType() : DagTypes::InvalidGraph;

    if (needsSessionOrGraph() && dstGraph == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "graphHandle required for apiHandler %s",
                strGetFromXcalarApis(api_));
        return StatusInval;
    }

    if (needsSessionOrGraph() || mayNeedSessionOrGraph()) {
        if (dagType == DagTypes::WorkspaceGraph && userId == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "userId required for apiHandler %s",
                    strGetFromXcalarApis(api_));
            return StatusInval;
        }
    }

    dstGraph_ = dstGraph;
    dstGraphType_ = dagType;

    if (dstGraph != NULL) {
        sessionContainer = dstGraph->getSessionContainer();
    }

    if (sessionContainer == NULL || (!dstGraph->inSession())) {
        userId_ = userId;
        sessionInfo_ = sessionInfo;
    } else {
        // if both dstGraph and sessionInfo are supplied, and the dstGraph
        // is in a session, the sessionNames in both must match; same for
        // user
        assert(sessionInfo == NULL || sessionInfo->sessionNameLength == 0 ||
               strcmp(dstGraph->getSessionContainer()->sessionInfo.sessionName,
                      sessionInfo->sessionName) == 0);
        assert(userId == NULL ||
               strcmp(dstGraph->getSessionContainer()->userId.userIdName,
                      UserDefinedFunction::PublishedDfUserName) == 0 ||
               strcmp(dstGraph->getSessionContainer()->userId.userIdName,
                      userId->userIdName) == 0);

        if (userId == NULL) {
            userId_ = &(dstGraph->getSessionContainer()->userId);
        } else {
            userId_ = userId;
        }

        // we already checked that the sessionInfo param and dstGraph's
        // udfContainer's session match. So using the latter's info is fine, and
        // it provides the sessionId as well, which is needed
        sessionInfo_ = &(dstGraph->getSessionContainer()->sessionInfo);
    }

    return StatusOk;
}

Status
ApiHandler::serializeOutput(XcalarApiOutput *buf, size_t bufSize)
{
    return StatusOk;
}

Status
ApiHandler::setImmediateTxnAndTxnLog()
{
    Status status;

    assert(!txn_.valid());

    txn_ = Txn::newImmediateTxn();
    Txn::setTxn(txn_);
    status = MsgMgr::get()->addTxnLog(txn_);
    BailIfFailed(status);

    txnLogAdded_ = true;

CommonExit:
    return status;
}

Status
ApiHandler::setTxnAndTxnLog(Txn::Mode mode, Runtime::SchedId rtSchedId)
{
    Status status;

    assert(!txn_.valid());

    txn_ = Txn::newTxn(mode, rtSchedId);
    status = MsgMgr::get()->addTxnLog(txn_);
    BailIfFailed(status);

    Txn::setTxn(txn_);  // set new txn only when returning success
    txnLogAdded_ = true;

CommonExit:
    return status;
}
