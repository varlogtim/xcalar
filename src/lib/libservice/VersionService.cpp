// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/VersionService.h"
#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "common/Version.h"
#include "util/MemTrack.h"
#include "libapis/LibApisRecv.h"

using namespace xcalar::compute::localtypes::Version;

VersionService::VersionService() {}

ServiceAttributes
VersionService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return (sattr);
}

Status
VersionService::getVersion(const google::protobuf::Empty *empty,
                           GetVersionResponse *response)
{
    Status status;
    try {
        response->set_version(versionGetFullStr());
        response->set_thrift_version_signature_full(
            versionGetApiStr(versionGetApiSig()));
        response->set_thrift_version_signature_short(versionGetApiSig());
        response->set_xcrpc_version_signature_full(
            versionGetXcrpcStr(versionGetXcrpcSig()));
        response->set_xcrpc_version_signature_short(versionGetXcrpcSig());
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Allocating getVersion response throw exception: %s",
                e.what());
    }
    return status;
}
