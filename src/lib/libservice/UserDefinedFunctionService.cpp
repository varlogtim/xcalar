// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/UserDefinedFunctionService.h"
#include "udf/UserDefinedFunction.h"
#include "libapis/LibApisCommon.h"
#include "ns/LibNs.h"

// API to return the resolution of a UDF given its moduleName and scope. The
// moduleName is searched in the set of paths in UserDefinedFunction::UdfPath[]
// and the fully qualified path for the first hit found, is returned.

using namespace xcalar::compute::localtypes::UDF;
using namespace xcalar::compute::localtypes::Workbook;

UserDefinedFunctionService::UserDefinedFunctionService()
{
    udf_ = UserDefinedFunction::get();
    assert(udf_ != NULL);
}

ServiceAttributes
UserDefinedFunctionService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
UserDefinedFunctionService::getResolution(
    const GetResolutionRequest *getResolutionRequest,
    GetResolutionResponse *getResolutionResponse)
{
    Status status;
    char *fullyQName = NULL;
    size_t fullyQNameSize = 0;
    XcalarApiUdfContainer udfContainer;

    status =
        UserDefinedFunction::initUdfContainerFromScope(&udfContainer,
                                                       &getResolutionRequest
                                                            ->udfmodule()
                                                            .scope());
    BailIfFailed(status);

    status = udf_->getResolutionUdf((char *) getResolutionRequest->udfmodule()
                                        .name()
                                        .c_str(),
                                    &udfContainer,
                                    &fullyQName,
                                    &fullyQNameSize);
    BailIfFailed(status);

    // This is a service API for external use so the internal libNs nodeId
    // prefix in the fullyQName must be stripped out (similar to ListXdfs).
    status = LibNs::get()->setFQNStripNodeId(fullyQName, fullyQNameSize);
    BailIfFailed(status);

    // Not the size of the string in fullyQName may have changed so update size
    fullyQNameSize = strlen(fullyQName);

    try {
        getResolutionResponse->mutable_fqmodname()->set_text(fullyQName,
                                                             fullyQNameSize);
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (fullyQName != NULL) {
        memFree(fullyQName);
        fullyQName = NULL;
    }
    return status;
}
