// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/EchoService.h"

using namespace xcalar::compute::localtypes::Echo;

ServiceAttributes
EchoService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
EchoService::echoMessage(const EchoRequest *echoIn, EchoResponse *echoInfo)
{
    Status status;

    try {
        echoInfo->set_echoed(echoIn->echo());
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
EchoService::echoErrorMessage(const EchoErrorRequest *errorIn,
                              google::protobuf::Empty *empty)
{
    return StatusInval;
}
