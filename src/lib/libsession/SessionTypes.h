// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SESSIONTYPES_H_
#define _SESSIONTYPES_H_

#include "session/Sessions.h"
#include "ns/LibNsTypes.h"
#include "ns/LibNs.h"

enum {
    NoDag = false,
    WriteDag = true,
    ExistSession = false,
    CreateSession = true,
};

#endif  // _SESSIONTYPES_H_
