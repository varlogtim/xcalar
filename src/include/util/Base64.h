// Copyright 2016-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _BASE64_H
#define _BASE64_H

#include "primitives/Primitives.h"

extern size_t base64BufSizeMax(const size_t bufInSize);
extern Status base64Decode(const char bufIn[],
                           size_t bufInSize,
                           uint8_t **bufOut,
                           size_t *bufOutSizeOut);
extern Status base64Encode(const uint8_t bufIn[],
                           size_t bufInSize,
                           char **bufOut,
                           size_t *bufOutSizeOut);

#endif  // _BASE64_H
