// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdarg.h>
#include "StrlFunc.h"
#include "UdfParent.h"
#include "UdfLocal.h"
#include "parent/Parent.h"
#include "ParentChild.h"
#include "UdfParentChild.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "LibUdfConstants.h"
#include "strings/String.h"

using namespace udf;

//
// Random helper functions.
//
void
UdfParent::err(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    xSyslogV(ModuleName,
             XlogErr,
             TxnBufNoCopy,
             NotFromSignalHandler,
             format,
             ap);
    va_end(ap);
}

Status
UdfParent::addOrUpdate(ParentChild *child,
                       char *moduleName,
                       UdfModuleSrc *input,
                       UdfError *error)
{
    Status status = StatusOk;
    ParentChildBuf pcBuf;
    LocalConnection::Response *response = NULL;
    const ProtoResponseMsg *responseMsg;
    ProtoChildRequest childRequest;
    char *moduleNameDup = NULL;

    moduleNameDup = strAllocAndCopy(moduleName);
    BailIfNull(moduleNameDup);

    // If moduleName is default, or a UDF module, the input->moduleName being
    // supplied to the child, must contain this (absolute path) name. But this
    // isn't needed for built-ins. This is needed to make UDF references in
    // the child work against these module names.
    if (!input->isBuiltin || strcmp(basename(moduleNameDup), "default") == 0) {
        strlcpy(input->moduleName, moduleName, sizeof(input->moduleName));
    }

    try {
        if (input->sourceSize != 0) {
            // send the UDF source code too
            pcBuf.set_buf(input, udfModuleSrcSize(input));  // can throw
        } else {
            // do not send the UDF source code
            pcBuf.set_buf(input, sizeof(*input));  // can throw
        }
        childRequest.set_func(ChildFuncUdfAdd);
        childRequest.set_allocated_pcbuf(&pcBuf);
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s failed send command allocation for "
                "child"
                " (pid: %u):%s, %s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status),
                e.what());
        goto CommonExit;
    }

    status =
        child->sendSync(&childRequest, LocalMsg::timeoutUSecsConfig, &response);
    childRequest.release_pcbuf();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s failed to send command to child "
                "(pid: %u):%s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    responseMsg = response->get();
    status.fromStatusCode((StatusCode) responseMsg->status());
    if (status == StatusUdfModuleAlreadyExists) {
        // Module already exists. Try updating.
        response->refPut();
        response = NULL;
        responseMsg = NULL;

        childRequest.set_func(ChildFuncUdfUpdate);
        childRequest.set_allocated_pcbuf(&pcBuf);

        status = child->sendSync(&childRequest,
                                 LocalMsg::timeoutUSecsConfig,
                                 &response);
        childRequest.release_pcbuf();
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function parent %s child (pid: %u) send async "
                    "failed:%s",
                    moduleName,
                    child->getPid(),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        responseMsg = response->get();
        status.fromStatusCode((StatusCode) responseMsg->status());
        responseMsg = NULL;
    }

    if (status != StatusOk && !responseMsg->error().empty()) {
        assert(error->message_ == NULL);
        error->message_ = strAllocAndCopy(responseMsg->error().c_str());
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s child (pid: %u) addOrUpdate "
                "failed:%s",
                moduleName,
                child->getPid(),
                error->message_);
    }

CommonExit:
    if (moduleNameDup != NULL) {
        memFree(moduleNameDup);
    }
    if (response != NULL) {
        response->refPut();
    }
    return status;
}

// Ask some childnode which functions a module makes available.
Status
UdfParent::listFunctions(ParentChild *child,
                         const char *moduleName,
                         XcalarEvalFnDesc **functionsOut,
                         size_t *functionsCountOut)
{
    Status status = StatusOk;
    ProtoChildRequest childRequest;
    ParentChildBuf pcBuf;
    XcalarEvalFnDesc *functions = NULL;
    size_t functionsCount;
    LocalConnection::Response *response = NULL;
    const ProtoResponseMsg *responseMsg;

    const char *responseBuf;
    size_t responseBufSize;

    try {
        pcBuf.set_buf(moduleName, strlen(moduleName) + 1);  // can throw
        childRequest.set_func(ChildFuncUdfListFunctions);
        childRequest.set_allocated_pcbuf(&pcBuf);
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s child (pid: %u) list functions "
                "sendSync"
                " allocation failed:%s, %s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status),
                e.what());
        goto CommonExit;
    }

    status =
        child->sendSync(&childRequest, LocalMsg::timeoutUSecsConfig, &response);
    childRequest.release_pcbuf();
    if (status != StatusOk) {
        err("Failed to send message to child with PID %u: '%s'",
            child->getPid(),
            strGetFromStatus(status));
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s child (pid: %u) list functions "
                "sendSync"
                " failed:%s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    responseMsg = response->get();
    status.fromStatusCode((StatusCode) responseMsg->status());
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s child (pid: %u) list functions "
                "sendSync"
                " failed:%s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    {
        const ProtoParentChildResponse &parentChildResponse =
            responseMsg->parentchild();
        const ParentChildBuf &parentChildBuf = parentChildResponse.pcbuf();
        responseBuf = parentChildBuf.buf().c_str();
        responseBufSize = parentChildBuf.buf().size();
    }

    functionsCount = *(uint64_t *) responseBuf;
    if (functionsCount > 0) {
        if (functionsCount * sizeof(functions[0]) > responseBufSize) {
            status = StatusOverflow;
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function parent %s child (pid: %u) list functions "
                    "failed:%s",
                    moduleName,
                    child->getPid(),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        responseBuf = responseBuf + sizeof(uint64_t);

        functions = (XcalarEvalFnDesc *) memAlloc(functionsCount *
                                                  sizeof(functions[0]));
        BailIfNull(functions);

        memcpy(functions, responseBuf, functionsCount * sizeof(functions[0]));
    }

    *functionsCountOut = functionsCount;
    *functionsOut = functions;
    assert(status == StatusOk);

CommonExit:
    if (response != NULL) {
        response->refPut();
    }
    return status;
}

// Adds module to free child to verify that it's usable.
Status
UdfParent::stageAndListFunctions(char *moduleName,
                                 UdfModuleSrc *input,
                                 UdfError *error,
                                 XcalarEvalFnDesc **functions,
                                 size_t *functionsCount)
{
    Status status;

    ParentChild *child = NULL;

    // XXX Avoid choosing a child that has ever seen a function of this name.
    //     Requires ability to list modules on child (along with version).
    status = Parent::get()->getChildren(&child,
                                        1,
                                        ParentChild::Level::User,
                                        ParentChild::getXpuSchedId(
                                            Txn::currentTxn().rtSchedId_));
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s stageAndListFunctions failed to "
                "grab a free"
                " child:%s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = addOrUpdate(child, input->moduleName, input, error);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function parent %s stageAndListFunctions failed "
                "addOrUpdate:%s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (functions != NULL) {
        status =
            listFunctions(child, input->moduleName, functions, functionsCount);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function parent %s stageAndListFunctions failed"
                    " listFunctions:%s",
                    moduleName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (child != NULL) {
        if (status != StatusOk) {
            child->abortConnection(status);
        }
        Parent::get()->putChild(child);
    }
    return status;
}
