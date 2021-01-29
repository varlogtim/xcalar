// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//

#ifndef _PROTOBUF_UTIL_H_
#define _PROTOBUF_UTIL_H_

#include "libapis/LibApisCommon.h"
#include <cstring>
#include <google/protobuf/message.h>
#include "session/Sessions.h"

namespace protobufutil
{
static constexpr const char *moduleName = "protobufUtil";

MustCheck Status setupSessionScope(
    const xcalar::compute::localtypes::Workbook::WorkbookScope *scope,
    Dag **retDag,
    bool *retTrackOpsToSession);

void teardownSessionScope(
    const xcalar::compute::localtypes::Workbook::WorkbookScope *scope);
}  // namespace protobufutil
#endif
