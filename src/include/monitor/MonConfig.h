// Copyright 2015, 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonConfig.h: Monitor cluster internal configuation data

#ifndef _MONCONFIG_H_
#define _MONCONFIG_H_

#include <sys/socket.h>
#include <netinet/in.h>
#include "util/System.h"

#include "sys/Socket.h"

// See https://www.ietf.org/rfc/rfc1035.txt section 2.3.4
// RFC value plus one for null terminator
#define MAX_HOST_NAME_LENGTH 256

typedef int64_t ConfigNodeId;
typedef enum ConfigHostStatus {
    CONFIG_HOST_STATUS_UNKNOWN,
    CONFIG_HOST_UP,
    CONFIG_HOST_DOWN,
    CONFIG_HOST_PAUSED
} ConfigHostStatus;

typedef struct ConfigInfo {
    ConfigInfo()
    {
        nodeId = -1;
        hostName[0] = 0;
        port = -1;
        status = CONFIG_HOST_STATUS_UNKNOWN;
    }

    ConfigNodeId nodeId;
    char hostName[MAX_HOST_NAME_LENGTH];
    int port;
    SocketAddr sockAddr;
    ConfigHostStatus status;
} ConfigInfo;

#endif
