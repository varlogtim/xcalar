// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORSAPIWRAPPERS_H
#define _OPERATORSAPIWRAPPERS_H

#include "libapis/LibApisCommon.h"
#include "export/DataTargetTypes.h"
#include "dag/DagTypes.h"
#include "dataset/DatasetTypes.h"
#include "transport/TransportPage.h"
#include "operators/OperatorsTypes.h"

struct OperatorsApiInput {
    DagTypes::DagId dagId;
    OperatorFlag flags;
    char userIdName[LOGIN_NAME_MAX + 1];
    uint64_t sessionId;
    size_t apiInputSize;
    size_t perNodeCookiesSize;
    char buf[0];
};

struct OperatorsStatusInput {
    DagTypes::NodeId dagNodeId;
    DagTypes::DagId dagId;
};

#endif  // _OPERATORSAPIWRAPPERS_H
