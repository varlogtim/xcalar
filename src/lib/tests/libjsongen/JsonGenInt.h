// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _JSONGENINT_H_
#define _JSONGENINT_H_

#include "test/JsonGen.h"

enum {
    // assuming no more than 100000000 field;
    // MaxSingleFieldLen = sizeof(element ) + log10(100000000)
    JsonSchemaContentLen = 64,
    EmptyRatioLimit = 99,
    MaxJsonStringLen = 99,
    BufLen = 200 * MB,
};

enum { NotArray = false, IsArray = true };

#endif  // _JSONGENINT_H_
