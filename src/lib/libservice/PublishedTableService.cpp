// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/PublishedTableService.h"
#include "xdb/HashTree.h"
#include "usr/Users.h"

using namespace xcalar::compute::localtypes::PublishedTable;

PublishedTableService::PublishedTableService()
{
    htMgr_ = HashTreeMgr::get();
}

ServiceAttributes
PublishedTableService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
PublishedTableService::select(const SelectRequest *selectRequest,
                              SelectResponse *selectResponse)
{
    Status status = StatusOk;

    return status;
}

Status
PublishedTableService::listTables(const ListTablesRequest *listTablesRequest,
                                  ListTablesResponse *response)
{
    return htMgr_->listHashTrees(listTablesRequest->namepattern().c_str(),
                                 listTablesRequest->maxupdatecount(),
                                 listTablesRequest->updatestartbatchid(),
                                 listTablesRequest->maxselectcount(),
                                 response);
}

Status
PublishedTableService::changeOwner(const ChangeOwnerRequest *req,
                                   google::protobuf::Empty *empty)
{
    Status status;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    Stopwatch stopwatch;
    Workbook::WorkbookSpecifier workbookSpec = req->scope().workbook();
    UserMgr *usrMgr = UserMgr::get();
    bool inc = false;

    status =
        usrMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
    BailIfFailedMsg(ModuleName, status, "Unable to increment outstanding ops "
                    "for PublishedTableService::changeOwner: %s",
                    strGetFromStatus(status));
    inc = true;

    if (req->scope().specifier_case() != Workbook::WorkbookScope::kWorkbook) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "req scope is not set as kWorkbook for \"%s\": %s",
                req->DebugString().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(ModuleName,
            XlogInfo,
            "Publish table %s user %s session %s change ownership started",
            req->publishedtablename().c_str(),
            req->scope().workbook().name().username().c_str(),
            req->scope().workbook().name().workbookname().c_str());

    stopwatch.restart();
    status =
        htMgr_
            ->changeOwnership(req->publishedtablename().c_str(),
                              req->scope().workbook().name().username().c_str(),
                              req->scope()
                                  .workbook()
                                  .name()
                                  .workbookname()
                                  .c_str());

    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);
    if (status == StatusOk) {
        xSyslog(ModuleName,
                XlogInfo,
                "Publish table %s user %s session %s change ownership finished "
                "in %lu:%02lu:%02lu.%03lu",
                req->publishedtablename().c_str(),
                req->scope().workbook().name().username().c_str(),
                req->scope().workbook().name().workbookname().c_str(),
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);
    } else {
        xSyslog(ModuleName,
                XlogErr,
                "Publish table %s user %s session %s change ownership failed "
                "in %lu:%02lu:%02lu.%03lu: %s",
                req->publishedtablename().c_str(),
                req->scope().workbook().name().username().c_str(),
                req->scope().workbook().name().workbookname().c_str(),
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(status == StatusOk);

CommonExit:
    if (inc) {
        Status status2 = usrMgr->trackOutstandOps(&workbookSpec,
            UserMgr::OutstandOps::Dec);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Unable to decrement outstanding ops for "
                    "PublishedTableService::changeOwner: %s",
                    strGetFromStatus(status2));
        }
    }
    return status;
}
