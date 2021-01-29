// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _BASETYPES_H_
#define _BASETYPES_H_

#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <wchar.h>
#include <limits.h>  // For CHAR_BIT

#include "config.h"

typedef uint8_t Opaque;

#if SIZEOF_FLOAT == 4
typedef float float32_t;
#else
#error "cannot define 32-bit float type"
#endif

#if SIZEOF_DOUBLE == 8
typedef double float64_t;
#elif SIZEOF_LONG_DOUBLE == 8
typedef long double float64_t;
#else
#error "cannot define 64-bit float type"
#endif

#endif  // _BASETYPES_H_
