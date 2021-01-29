// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//

#include "libapis/ProtobufUtil.h"
#include "libapis/ApiHandler.h"
#include "libapis/OperatorHandler.h"
#include "StrlFunc.h"
#include "usr/Users.h"

Status
protobufutil::setupSessionScope(
    const xcalar::compute::localtypes::Workbook::WorkbookScope *scope,
    Dag **retDag,
    bool *retTrackOpsToSession)
{
    Status status;

    switch (scope->specifier_case()) {
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kGlobl: {
        // NOOP, since there is no workbook associated here
        break;
    }
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kWorkbook: {
        auto workbookSpec = scope->workbook();
        status = UserMgr::get()->trackOutstandOps(&workbookSpec,
                                                  UserMgr::OutstandOps::Inc);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed setupSessionScope: %s",
                        strGetFromStatus(status));

        *retTrackOpsToSession = true;
        if (retDag != NULL) {
            status = UserMgr::get()->getDag(&workbookSpec, retDag);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed setupSessionScope: %s",
                            strGetFromStatus(status));
        }
        break;
    }

    default: {
        status = StatusInval;
        goto CommonExit;
    }
    }

CommonExit:
    return status;
}

void
protobufutil::teardownSessionScope(
    const xcalar::compute::localtypes::Workbook::WorkbookScope *scope)
{
    Status status = StatusOk;

    switch (scope->specifier_case()) {
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kGlobl:
        // NOOP, since there is no workbook associated here
        break;
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kWorkbook: {
        auto workbookSpec = scope->workbook();
        status = UserMgr::get()->trackOutstandOps(&workbookSpec,
                                                  UserMgr::OutstandOps::Dec);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed teardownSessionScope: %s",
                        strGetFromStatus(status));
        break;
    }
    default: {
        status = StatusInval;
        goto CommonExit;
    }
    }

CommonExit:
    return;
}
