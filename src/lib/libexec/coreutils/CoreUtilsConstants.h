// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _COREUTILSCONSTANTS_H
#define _COREUTILSCONSTANTS_H

namespace cutil
{
//
// Cancel related
//
static constexpr int MinArgc = 2;

//
// Cat related
//
enum { DontPrint = false, PrintRecords = true };

//
// Dht related
//
enum DhtAction {
    DhtCreate,
    DhtDelete,
};

//
// List Xdfs related
//
enum { ShowDetails = true, DontShowDetails = false };

//
// Shopping Cart related
//
enum SelectionMode {
    BrowseCart = 0,
    BrowseFields = 1,
    BrowseDatasets = 2,
};

//
// Shutdown related
//
enum ShutdownAction {
    ShutdownQuiesce,
    ShutdownForce,
};

//
// Source related
//
enum SourceCommand {
    SourceCmdInvalid,
    SourceCmdList,
};

}  // namespace cutil

#endif  // _COREUTILSCONSTANTS_H
