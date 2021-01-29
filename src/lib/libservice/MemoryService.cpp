// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "service/MemoryService.h"
#include "usr/Users.h"
#include "dag/Dag.h"

using namespace xcalar::compute::localtypes::memory;

MemoryService::MemoryService() {}

ServiceAttributes
MemoryService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return (sattr);
}

Status
MemoryService::getUsage(const GetUsageRequest *req, GetUsageResponse *resp)
{
    UserMgr *userMgr = UserMgr::get();
    Status status = StatusUnknown;
    XcalarApiOutput *tmpOutput = NULL;
    size_t tmpOutputSize = 0;
    XcalarApiUserId userId;
    bool trackOpsToSession = false;

    if (req->scope().specifier_case() != Workbook::WorkbookScope::kWorkbook) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "req scope is not set as kWorkbook for \"%s\": %s",
                req->DebugString().c_str(),
                strGetFromStatus(status));
        return status;
    }
    status = strStrlcpy(userId.userIdName,
                        req->scope().workbook().name().username().c_str(),
                        sizeof(userId.userIdName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error in copying userIdName \"%s\": %s",
                req->DebugString().c_str(),
                strGetFromStatus(status));
        return status;
    }

    status = userMgr->list(&userId, "*", &tmpOutput, &tmpOutputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error in calling usrMgr->list \"%s\": %s",
                req->DebugString().c_str(),
                strGetFromStatus(status));
        return status;
    }

    if (strlen(userId.userIdName) == 0) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "User name must be specified for \"%s\": %s",
                req->DebugString().c_str(),
                strGetFromStatus(status));
        return status;
    }

    auto workbookSpec = req->scope().workbook();
    for (unsigned ii = 0;
         ii < tmpOutput->outputResult.sessionListOutput.numSessions;
         ii++) {
        workbookSpec.mutable_name()->set_workbookname(
            tmpOutput->outputResult.sessionListOutput.sessions[ii].name);
        status =
            userMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
        BailIfFailed(status);
        trackOpsToSession = true;

        Dag *sessionGraph = NULL;

        // skip inactive workbooks for this user - they wouldn't use any table
        // memory so it'd be a waste to get its dag, only to find no tables
        if (strncmp(tmpOutput->outputResult.sessionListOutput.sessions[ii]
                        .state,
                    "Active",
                    sizeof(
                        tmpOutput->outputResult.sessionListOutput.sessions[ii]
                            .state)) == 0) {
            status =
                userMgr->sessionGetDagById(&userId,
                                           &sessionGraph,
                                           tmpOutput->outputResult
                                               .sessionListOutput.sessions[ii]
                                               .sessionId);
            BailIfFailed(status);
        }

        if (sessionGraph != NULL) {
            XcalarApiOutput *tableList = nullptr;
            size_t size = 0;
            status = sessionGraph->listDagNodeInfo("*",
                                                   &tableList,
                                                   &size,
                                                   SrcTable,
                                                   &userId);
            BailIfFailed(status);

            try {
                SessionMemoryUsage *sessionMemoryUsage =
                    resp->mutable_user_memory()->add_session_memories();
                sessionMemoryUsage->set_session_name(
                    tmpOutput->outputResult.sessionListOutput.sessions[ii]
                        .name);
                for (uint32_t j = 0;
                     j < tableList->outputResult.listNodesOutput.numNodes;
                     j++) {
                    XcalarApiDagNodeInfo *tableOutput =
                        &tableList->outputResult.listNodesOutput.nodeInfo[j];
                    TableMemoryUsage *tableMemoryUsage =
                        sessionMemoryUsage->add_table_memories();
                    tableMemoryUsage->set_table_name(tableOutput->name);
                    tableMemoryUsage->set_table_id(tableOutput->dagNodeId);
                    tableMemoryUsage->set_total_bytes(tableOutput->size);
                }
            } catch (std::exception &e) {
                status.fromStatusCode(StatusCodeNoMem);
                xSyslog(moduleName,
                        XlogErr,
                        "Failed setting response for \"%s\": %s",
                        req->DebugString().c_str(),
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
        status =
            userMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Dec);
        BailIfFailed(status);
        trackOpsToSession = false;
    }

    try {
        resp->mutable_user_memory()
            ->mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_username(userId.userIdName);
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        xSyslog(moduleName,
                XlogErr,
                "Failed setting userIdName in response for \"%s\": %s",
                req->DebugString().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (tmpOutput != NULL) {
        memFree(tmpOutput);
        tmpOutput = NULL;
    }
    if (trackOpsToSession == true) {
        status =
            userMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Dec);
    }
    return status;
}
