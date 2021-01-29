// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORS_HASH_H_
#define _OPERATORS_HASH_H_

#include "primitives/Primitives.h"

extern uint64_t operatorsHashByInt64(HashKey intIn);
extern uint64_t operatorsHashByString(const char *stringIn);
extern uint64_t operatorsHashByHour(HashKey tsIn);
extern uint64_t operatorsHashByBoolPtr(HashKey boolIn);
extern uint64_t operatorsHashByInt32Ptr(HashKey intIn);
extern uint64_t operatorsHashByInt64(HashKey intIn);
extern uint64_t operatorsHashByInt64Ptr(HashKey intIn);
extern uint64_t operatorsHashByFloat32Ptr(HashKey floatIn);
extern uint64_t operatorsHashByFloat64Ptr(HashKey floatIn);
extern void operatorsHashByStringInverse(char *keyBuf, uint64_t *hash);

#endif  // _OPERATORS_HASH_H
