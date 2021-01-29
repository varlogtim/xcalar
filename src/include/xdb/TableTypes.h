// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TABLETYPES_H
#define _TABLETYPES_H

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "libapis/LibApisConstants.h"

typedef uint64_t XcalarApiTableId;  // only used externally
typedef Xid XdbId;

enum {
    XcalarApiTableIdInvalid = 0,  // only used externally
    TableIdInvalid = 0,
    XdbIdInvalid = 0,
    TableMaxNameLen = XcalarApiMaxTableNameLen,
};

#define TableHandleInvalid (NULL)

#endif  // _TABLETYPES_H
