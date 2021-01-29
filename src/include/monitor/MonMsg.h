// Copyright 2015, 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonMsg.h: Messages sent between cluster monitor instances

#ifndef _MON_MSG_H_
#define _MON_MSG_H_

enum MsgOp {
    INVALID_OP = 0,
    // Heartbeat sent from master to slave.
    HEARTBEAT_OP = 1,
    // Response to heartbeat sent from slave to master.
    HEARTBEAT_REPLY_OP = 2,
    // Sent by new slave to master to get added to the cluster.
    NEW_SLAVE_OP = 3,
    // Sent by master to slaves (and also to notification clients)
    // when the state of a node changes.
    NEW_CLUSTER_STATUS_PREPARE_OP = 4,
    // Sent by slaves to master in response to prepare op.
    NEW_CLUSTER_STATUS_PREPARE_ACK_OP = 5,
    // Sent by master to slaves (and also to notification clients)
    // to commit change in cluster.
    NEW_CLUSTER_STATUS_COMMIT_OP = 6,
    // Sent by usrnode to xcmonitor once connection is established.
    NOTIFIER_ATTACH_OP = 7,
    // Notification sent when new state is SlaveMonitor or MasterMonitor.
    MONITOR_ENABLED_OP = 8,
    // Notification sent when old state is SlaveMonitor or MasterMonitor
    // and the new state is neither of the two.
    MONITOR_DISABLED_OP = 9,
    // This must be the last enum
    MONITOR_HIGHEST_OP_NUM,
};

struct MsgHdr {
    uint8_t op;
    uint8_t flags;
    uint8_t _pad;
    uint8_t _pad2;
    uint32_t messageLength;
} __attribute((packed));

#endif
