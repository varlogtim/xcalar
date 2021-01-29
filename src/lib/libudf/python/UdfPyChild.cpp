// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>

#include "UdfPyChild.h"
#include "UdfPythonImpl.h"
#include "UdfPyFunction.h"
#include "UdfParentChild.h"
#include "Child.h"
#include "udf/UdfPython.h"
#include "udf/UdfError.h"
#include "strings/String.h"
#include "LibUdfConstants.h"
#include "libapis/LibApisCommon.h"
#include "sys/XLog.h"

using namespace udf;

//
// This file handles requests incoming from usrnode related to python.
//

static UdfPythonImpl *udfPython = NULL;
static const char *ModuleName = "libpychild";

void
udfPyChildDestroy()
{
}

Status
udfPyChildInit(UdfPythonImpl *udfPythonImpl)
{
    udfPython = udfPythonImpl;
    return StatusOk;
}

static void
udfPyChildListFunctions(const ProtoRequestMsg *request,
                        ProtoResponseMsg *response)
{
    Status status = StatusOk;
    char *moduleName = NULL;
    XcalarApiOutput *output = NULL;
    XcalarApiListXdfsOutput *listXdfsOutput;
    const ProtoChildRequest &childRequest = request->child();
    const ParentChildBuf &childBufRequest = childRequest.pcbuf();
    const char *requestBuf = childBufRequest.buf().c_str();
    size_t requestBufSize = childBufRequest.buf().size();
    uint8_t *responseBuf = NULL;
    size_t responseBufSize = 0;

    size_t moduleNameLen = strnlen((char *) requestBuf, requestBufSize);
    if (moduleNameLen > XcalarApiMaxUdfModuleNameLen) {
        status = StatusUdfModuleInvalidName;
        goto CommonExit;
    }

    moduleName = strAllocAndCopy((char *) requestBuf);
    BailIfNull(moduleName);

    status = udfPython->listModule(moduleName, &output);
    BailIfFailed(status);

    listXdfsOutput = &output->outputResult.listXdfsOutput;
    if (listXdfsOutput->numXdfs > 0) {
        size_t listXdfsOutputSize =
            sizeof(listXdfsOutput->fnDescs[0]) * listXdfsOutput->numXdfs;
        responseBufSize = listXdfsOutputSize + sizeof(uint64_t);
    } else {
        responseBufSize = sizeof(uint64_t);
    }

    responseBuf = (uint8_t *) memAlloc(responseBufSize);
    BailIfNull(responseBuf);

    // First 8 bytes: function count.
    *(uint64_t *) responseBuf = listXdfsOutput->numXdfs;
    if (listXdfsOutput->numXdfs != 0) {
        memcpy(responseBuf + sizeof(uint64_t),
               listXdfsOutput->fnDescs,
               responseBufSize - sizeof(uint64_t));
    }

    try {
        ProtoParentChildResponse *childResponse =
            response->mutable_parentchild();
        ParentChildBuf *childBufResponse = childResponse->mutable_pcbuf();
        childBufResponse->set_buf((char *) responseBuf, responseBufSize);
    } catch (std::exception &e) {
        status = StatusNoMem;
        if (response->has_parentchild()) {
            ProtoParentChildResponse childResponse = response->parentchild();
            if (childResponse.has_pcbuf()) {
                ParentChildBuf childBufResponse = childResponse.pcbuf();
                childBufResponse.clear_buf();
            }
            response->clear_parentchild();
        }
        xSyslog(ModuleName,
                XlogErr,
                "Failed to set protobuf error message: %s",
                e.what());
        goto CommonExit;
    }
    status = StatusOk;

CommonExit:
    if (moduleName != NULL) {
        memFree(moduleName);
    }
    if (output != NULL) {
        memFree(output);
    }
    if (responseBuf != NULL) {
        memFree(responseBuf);
    }
    response->set_status(status.code());
}

static void
udfPyChildAddOrUpdate(const ProtoRequestMsg *request,
                      ProtoResponseMsg *response)
{
    Status status;
    const ProtoChildRequest &childRequest = request->child();
    const ParentChildBuf &childBufRequest = childRequest.pcbuf();
    const char *requestBuf = childBufRequest.buf().c_str();
    UdfError error;
    UdfModuleSrc *input = (UdfModuleSrc *) requestBuf;

    switch (childRequest.func()) {
    case ChildFuncUdfAdd:
        status = udfPython->addModule(input, &error);
        break;
    case ChildFuncUdfUpdate:
        status =
            udfPython->updateModule(input->moduleName, input->source, &error);
        break;
    default:
        assert(false);
        status = StatusUnimpl;
        break;
    }

    if (status != StatusOk && error.message_ != NULL &&
        *error.message_ != '\0') {
        try {
            response->set_error(error.message_);
        } catch (std::exception &e) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to set protobuf error message: %s",
                    e.what());
            // Do nothing. At least we got the status.
        }
    }

    response->set_status(status.code());
}

void
udfPyChildDispatch(LocalConnection *connection,
                   const ProtoRequestMsg *request,
                   ProtoResponseMsg *response)
{
    switch (request->child().func()) {
    case ChildFuncUdfAdd:
    case ChildFuncUdfUpdate:
        udfPyChildAddOrUpdate(request, response);
        break;

    case ChildFuncUdfListFunctions:
        udfPyChildListFunctions(request, response);
        break;

    default:
        assert(false);
        response->set_status(StatusUnimpl.code());
        break;
    }
}
