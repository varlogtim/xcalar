// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApisRecvObject.h"
#include "libapis/ApiHandler.h"
#include "libapis/OperatorHandler.h"
#include "libapis/LibApisEnums.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisRecv.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "bc/BufferCache.h"
#include "runtime/Semaphore.h"
#include "sys/Socket.h"
#include "usr/Users.h"

static constexpr const char *moduleName = "libapis::apisRecvObj";

ApisRecvObject::ApisRecvObject(ApiHandler *apiHandler,
                               ApisRecvClient *apisRecvClient,
                               BcHandle *apisRecvObjectBc,
                               Semaphore *sem)
    : Schedulable("ApisRecvObject")
{
    apiHandler_ = apiHandler;
    output_ = NULL;
    outputSize_ = 0;
    apiStatus_ = StatusUnknown;
    sessionGraph_ = NULL;
    workspaceGraphNodeId_ = DagTypes::InvalidDagNodeId;
    apisRecvObjectBc_ = apisRecvObjectBc;
    apisRecvClient_ = apisRecvClient;
    sem_ = sem;
    api_ = apisRecvClient->workItem_.api;
}

void
ApisRecvObject::run()
{
    Status status = StatusUnknown;
    OperatorHandler *operatorHandler = NULL;
    XcalarWorkItem *workItem = &apisRecvClient_->workItem_;
    bool incOutstandingOps = false;
    char *sessionName =
        workItem->sessionInfo ? workItem->sessionInfo->sessionName : NULL;

    assert(apiHandler_->txnLogAdded_ == true);

    if (apiHandler_->needsSessionOrGraph() ||
        apiHandler_->mayNeedSessionOrGraph()) {
        // sessionInfo is required unless the workItem is coming from legacy,
        // frozen infrastructure (e.g. xccli).  In that case the session
        // library still maintains the "last active" sematics.
        if ((workItem->legacyClient == false) &&
            apiHandler_->needsSessionOrGraph() &&
            (sessionName == NULL || strlen(sessionName) == 0)) {
            status = StatusSessionNameMissing;
            goto CommonExit;
        }

        if (workItem->legacyClient == true ||
            (sessionName != NULL && strlen(sessionName) > 0)) {
            //
            // This conditional code is for APIs marked with the
            // MayNeedSessionOrGraph flag. For such APIs, the absence of a
            // session name implies that the particular API invocation does not
            // need a session or dag (e.g. the UdfAdd API is marked with this
            // flag - when a workbook UDF is being added, the API is supplied a
            // session name (which will trigger this code), but if the same API
            // is invoked for shared UDFs, the session name can be left empty -
            // in which case this code will be skipped.
            //
            // However, for legacyClient APIs, empty session names still need
            // the dag (e.g. BulkLoad API invoked from legacy client with an
            // empty session name). So for such APIs, do this always.

            status =
                UserMgr::get()->trackOutstandOps(workItem->userId,
                                                 sessionName,
                                                 UserMgr::OutstandOps::Inc);
            BailIfFailed(status);
            incOutstandingOps = true;

            status = UserMgr::get()->getDag(workItem->userId,
                                            sessionName,
                                            &sessionGraph_);
            BailIfFailed(status);
        }
    }

    // this sets up sessionInfo_, userId_, dstGraph_, and dstGraphType_
    status = apiHandler_->init(workItem->userId,
                               workItem->sessionInfo,
                               sessionGraph_);
    BailIfFailed(status);

    // Please note that apiHandler merely borrows input. Doesn't actually
    // free it. You still need to manually free it.
    status = apiHandler_->setArg(workItem->input, workItem->inputSize);
    BailIfFailed(status);

    if (apiHandler_->isOperator()) {
        operatorHandler = dynamic_cast<OperatorHandler *>(apiHandler_);
        assert(operatorHandler != NULL);
        if (operatorHandler == NULL) {
            status = StatusUnimpl;
            xSyslog(moduleName,
                    XlogErr,
                    "Api handler %s is corrupted",
                    strGetFromXcalarApis(api_));
            goto CommonExit;
        }

        // only allow delete operations on queryGraph
        if (sessionGraph_->getDagType() == DagTypes::QueryGraph &&
            workItem->api != XcalarApiDeleteObjects) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Cannot perform operation on an inactive session");
            goto CommonExit;
        }

        assert(operatorHandler->needsSessionOrGraph());
        status = operatorHandler->createDagNode(&workspaceGraphNodeId_,
                                                DagTypes::InvalidGraph);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create dag node: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        status = operatorHandler->run(&output_, &outputSize_);
    } else {
        status = apiHandler_->run(&output_, &outputSize_);
    }

CommonExit:
    if (status != StatusOk) {
        if (workspaceGraphNodeId_ != DagTypes::InvalidDagNodeId) {
            if (sessionGraph_ != NULL) {
                // doneDag() may have deleted sessionGraph_ - see call above
                Status status2 =
                    sessionGraph_->dropAndChangeState(workspaceGraphNodeId_,
                                                      DgDagStateError);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to drop and change dagNode (%lu) state "
                            " to Error: %s",
                            workspaceGraphNodeId_,
                            strGetFromStatus(status2));
                }
                workspaceGraphNodeId_ = DagTypes::InvalidDagNodeId;
            }
        }
    }

    if (incOutstandingOps) {
        (void) UserMgr::get()->trackOutstandOps(workItem->userId,
                                                sessionName,
                                                UserMgr::OutstandOps::Dec);
        incOutstandingOps = false;
    }

    apiStatus_ = status;
}

void
ApisRecvObject::setApiStatus(Status apiStatus)
{
    apiStatus_ = apiStatus;
}

Status
ApisRecvObject::setTxnAndTxnLog()
{
    Status status;

    assert(apiHandler_->needsToRunImmediately());

    status = apiHandler_->setImmediateTxnAndTxnLog();

    apiStatus_ = status;

    return status;
}

void
ApisRecvObject::sendOutputToClient()
{
    Status status = StatusOk;

    if (apiHandler_->needsAck()) {
        // Emergency output struct alloc'ed on stack
        XcalarApiOutputHeader tmpOutput;
        bool outputAlloced = false;

        assert(output_ == NULL || outputSize_ > 0);
        if (output_ != NULL && outputSize_ == 0) {
            // We should never ever encounter this situation. But oh well,
            // we need to handle this anyway.
            memFree(output_);
            output_ = NULL;
            apiStatus_ = StatusCorruptedOutputSize;
            // This will fall through to use the default emergency output struct
        }

        if (output_ != NULL) {
            status = apiHandler_->serializeOutput(output_, outputSize_);

            // Failed to serialize. Use emergency output struct
            if (status != StatusOk) {
                apiStatus_ = status;
                memFree(output_);
                output_ = NULL;
            }
        }

        if (output_ == NULL) {
            outputSize_ = sizeof(tmpOutput);
            output_ = (XcalarApiOutput *) &tmpOutput;
            outputAlloced = false;
            assert((uintptr_t) &output_->hdr == (uintptr_t) output_);
        } else {
            outputAlloced = true;
        }

        apiHandler_->stopwatch_.stop();
        output_->hdr.elapsed.milliseconds =
            apiHandler_->stopwatch_.getElapsedNSecs() / NSecsPerMSec;
        output_->hdr.status = apiStatus_.code();
        output_->hdr.log[0] = '\0';

        if (apiHandler_->txnLogAdded_) {
            MsgMgr::TxnLog *txnLog =
                MsgMgr::get()->getTxnLog(apiHandler_->txn_);

            if (txnLog->numMessages > 0) {
                // XXX: only return the first message for now
                snprintf(output_->hdr.log,
                         sizeof(output_->hdr.log),
                         "%s",
                         txnLog->messages[0]);
            }
        }

        // invoke apiHandler destructor before acking client
        delete apiHandler_;
        apiHandler_ = NULL;

        status = apisRecvClient_->sendOutput(output_, outputSize_);
        if (status != StatusOk) {
            // There's nothing we can do if we failed to send the response back
            // Just log it
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to send response \"%s\" back to client: %s",
                    strGetFromStatus(apiStatus_),
                    strGetFromStatus(status));
        }

        if (outputAlloced) {
            memFree(output_);
            output_ = NULL;
            outputAlloced = false;
        } else {
            assert(output_ == (XcalarApiOutput *) &tmpOutput);
            output_ = NULL;
        }
    }
}

void
ApisRecvObject::done()
{
    BcHandle *apisRecvObjectBc = NULL;
    XcalarApis api = this->api_;

    if (apiHandler_->needsToRunImmediately()) {
        StatsLib::statAtomicDecr64(apisStatImmedOutstanding);
    }

    // cleans up apiHandler_
    sendOutputToClient();

    delete apisRecvClient_;
    apisRecvClient_ = NULL;

    Semaphore *sem = sem_;

    apisRecvObjectBc = apisRecvObjectBc_;
    this->~ApisRecvObject();
    apisRecvObjectBc->freeBuf(this);

    StatsLib::statAtomicIncr64(apisStatDoneCumulative[(int) api]);
    StatsLib::statAtomicDecr64(apisStatOutstanding);

    assert(atomicRead64(&apisOutstanding) > 0);
    if (atomicDec64(&apisOutstanding) == 0) {
        // I'm last guy. Need to wake the listener teardown thread
        sem->post();
    }
}
