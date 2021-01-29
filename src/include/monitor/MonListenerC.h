// Copyright 2015, 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonListenerC.h: C interface to the cluster monitor program

#ifndef _MONLISTENERC_H
#define _MONLISTENERC_H

#include "monitor/MonConfig.h"

struct MonitorListenFuncs {
    void (*monitorEnabled)(void);
    void (*monitorDisabled)(void);
    void (*clusterStateChangePending)(ConfigInfo *ciArray, uint32_t ciCount);
    void (*clusterStateChangeComplete)(void);
};

void monitorListen(int port, struct MonitorListenFuncs *listenFuncs);

#endif
