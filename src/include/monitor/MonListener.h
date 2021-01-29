// Copyright 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonListener.h: C++ class interface to the cluster monitor

#ifndef _MONLISTENER_H
#define _MONLISTENER_H

#include "monitor/MonRefCounted.h"
#include "monitor/MonConfig.h"

class MonitorListener
{
  public:
    void Listen(int port);

    virtual void MonitorEnabled() = 0;
    virtual void MonitorDisabled() = 0;
    virtual void ClusterStateChangePending(ConfigInfo *ciArray,
                                           uint32_t ciCount) = 0;
    virtual void ClusterStateChangeComplete() = 0;
};

#endif
