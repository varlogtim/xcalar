// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBAPIS_RECV_H_
#define _LIBAPIS_RECV_H_

#include "dag/DagLib.h"
#include "libapis/LibApisCommon.h"
#include "libapis/OperatorHandler.h"
#include "libapis/LibApisEnums.h"
#include "libapis/ApisRecvObject.h"
#include "msg/MessageTypes.h"
#include "msg/Xid.h"

extern Atomic64 apisOutstanding;
extern StatHandle apisStatDoneCumulative[XcalarApisLen];
extern StatHandle apisStatOutstanding;
extern StatHandle apisStatImmedOutstanding;

static constexpr const char *XcalarUsrName = "xcalar";
static constexpr const char *JenkinsUsrName = "jenkins";
static constexpr const char *RootUsrName = "root";
static constexpr const char *UsrNodeShutdownTrigUsrName =
    "usrNodeShutdownTrigger";
static constexpr const unsigned int UsrNodeShutdownTrigUsrIdUnique = 0xc001cafe;
static constexpr const int MaxNumApisConnections = 1024;

void *xcApiStartListener(void *unused);

void xcApiDestroyApiInputFromDagNode(Dag *dag,
                                     XcalarWorkItem *workItem,
                                     DagTypes::NodeId dagNodeId);
Status xcApiConstructApiInputFromDagNode(Dag *dag,
                                         XcalarWorkItem *workItem,
                                         DagTypes::NodeId dagNodeId);

Status xcApiGetOperatorHandlerInited(OperatorHandler **operatorHandlerOut,
                                     XcalarWorkItem *workItem,
                                     XcalarApiUserId *userId,
                                     Dag *dstGraph,
                                     bool preserveDatasetUuid,
                                     Txn *prevTxn);

Status xcApiGetOperatorHandlerUninited(OperatorHandler **operatorHandlerOut,
                                       XcalarApis api,
                                       bool preserveDatasetUuid);

Status xcApiCheckIfSufficientMem(XcalarApis api);
Status xcApiGetApiHandler(ApiHandler **apiHandlerOut, XcalarApis api);

MustCheck bool usrNodeNormalShutdown();
MustCheck bool usrNodeForceShutdown();
MustCheck bool apisRecvInitialized();
void setUsrNodeForceShutdown();
void unsetUsrNodeForceShutdown();
void setApisRecvInitialized();
MustCheck bool isShutdownInProgress();

#endif  // _LIBAPIS_RECV_H_
