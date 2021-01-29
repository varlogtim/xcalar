// Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/ConnectorsService.h"
#include "usr/Users.h"

using namespace xcalar::compute::localtypes::Connectors;
using namespace xcalar::compute::localtypes::Workbook;

ConnectorsService::ConnectorsService() {}

ServiceAttributes
ConnectorsService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
ConnectorsService::removeFile(const RemoveFileRequest *request,
                              google::protobuf::Empty *empty)
{
    xSyslog(ModuleName,
            XlogInfo,
            "removeFile() request: target='%s', path='%s', user='%s'",
            request->path().c_str(),
            request->target_name().c_str(),
            request->scope().workbook().name().username().c_str());
    Status status = AppLoader::removeFile(request, nullptr);
    if (status.ok()) {
        return status;
    } else {
        xSyslog(ModuleName,
                XlogErr,
                "removeFile() FAILED: status='%s'",
                strGetFromStatus(status));
        return status;
    }
}

Status
ConnectorsService::listFiles(const ListFilesRequest *request,
                             ListFilesResponse *response)
{
    xSyslog(ModuleName,
            XlogInfo,
            "listFiles() request: target='%s', path='%s', "
            "fileNamePattern='%s', recursive='%s', paged='%s', "
            "continuationToken='%s'",
            request->sourceargs().targetname().c_str(),
            request->sourceargs().path().c_str(),
            request->sourceargs().filenamepattern().c_str(),
            request->sourceargs().recursive() ? "true" : "false",
            request->paged() ? "true" : "false",
            request->continuationtoken().c_str());
    Status status = AppLoader::listFiles(request, response);
    if (status.ok()) {
        return status;
    } else {
        xSyslog(ModuleName,
                XlogErr,
                "listFiles() FAILED: status='%s'",
                strGetFromStatus(status));
        return status;
    }
}
