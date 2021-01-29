// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/QueryService.h"
#include "ns/LibNs.h"
#include "querymanager/QueryManager.h"
#include "strings/String.h"

using namespace xcalar::compute::localtypes::Query;

ServiceAttributes
QueryService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    // Just scheduling of DF is on Immediate Runtime, but the actual query
    // execution is parameterized.
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
QueryService::list(const ListRequest *req, ListResponse *resp)
{
    Status status = StatusOk;
    const char *namePattern = req->name_pattern().c_str();
    LibNsTypes::PathInfoOut *pathInfo = NULL;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    size_t numQueries;
    size_t prefixLen = strlen(QueryManager::NsPrefix);
    XcalarApiOutput *output = NULL;
    size_t outputSize;

    status = strSnprintf(fullyQualName,
                         LibNsTypes::MaxPathNameLen,
                         "%s%s",
                         QueryManager::NsPrefix,
                         namePattern);
    BailIfFailed(status);

    numQueries = LibNs::get()->getPathInfo(fullyQualName, &pathInfo);

    try {
        for (unsigned ii = 0; ii < numQueries; ii++) {
            // Strip off the path prefix
            const char *queryName = &pathInfo[ii].pathName[prefixLen];
            status = QueryManager::get()->requestQueryState(&output,
                                                            &outputSize,
                                                            queryName,
                                                            false);
            if (status == StatusQrQueryAlreadyDeleted ||
                status == StatusQrQueryNotExist) {
                status = StatusOk;
                continue;
            }
            BailIfFailed(status);

            QueryInfo *queryInfo = resp->add_queries();
            queryInfo->set_name(queryName);
            queryInfo->set_milliseconds_elapsed(
                pathInfo[ii].millisecondsSinceCreation);

            queryInfo->set_state(strGetFromQueryState(
                output->outputResult.queryStateOutput.queryState));
            memFree(output);
        }
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (pathInfo != NULL) {
        memFree(pathInfo);
        pathInfo = NULL;
    }
    return status;
}
