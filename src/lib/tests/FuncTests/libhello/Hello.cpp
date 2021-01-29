// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "primitives/Primitives.h"
#include "sys/XLog.h"

#include "test/QA.h"

Status
helloMain()
{
    xSyslog("Hello.cpp", XlogInfo, "Hello Func Test");
    return StatusOk;
}
