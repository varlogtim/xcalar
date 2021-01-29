// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _USRNODE_H_
#define _USRNODE_H_

#include "msg/Message.h"
#include "dataset/DatasetTypes.h"

Status usrNodeNew();
void usrNodeDestroy();
Status usrNodeStressStart();
void usrNodeLoadDataset(MsgEphemeral *eph, void *payload);
void usrNodeStatusComplete(MsgEphemeral *eph, void *payload);
void usrNodeStatusArrayComplete(MsgEphemeral *eph, void *payload);
void usrNodeGetTableMetaComplete(MsgEphemeral *eph, void *payload);
void usrNodeRangePartitionComplete(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcGetDataUsingFatptr(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcGetKeysUsingFatptr(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcXcalarApiFilter(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcXcalarApiGroupBy(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcXcalarApiJoin(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcXcalarApiGetTableMeta(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcComputeRangePartition(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcFreeTable(MsgEphemeral *eph, void *payload);
void usrNodeDoFatPointerWork(MsgEphemeral *eph, void *payload);
void usrNodeTopComplete(MsgEphemeral *eph, void *payload);
void usrNodeStatusCompleteSingleNode(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcLogLevelSet(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcLogLevelSetComplete(MsgEphemeral *eph, void *payload);
void usrNodeMsg2pcSetConfig(MsgEphemeral *eph, void *payload);

#endif  // _USRNODE_H_
