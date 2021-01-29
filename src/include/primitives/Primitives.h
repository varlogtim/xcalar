// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PRIMITIVES_H_
#define _PRIMITIVES_H_

// provides access to all primitive types, constants, macros, and functions in
// the Xcalar system

// avoid conflicts with thrift's config.h
#undef PACKAGE_BUGREPORT
#undef PACKAGE_NAME
#undef PACKAGE_STRING
#undef PACKAGE_TARNAME
#undef PACKAGE_URL
#undef PACKAGE_VERSION

#include "primitives/BaseTypes.h"
#include "primitives/Macros.h"
#include "primitives/PConstants.h"
#include "primitives/LogMessages.h"
#include "primitives/Status.h"
#include "primitives/Subsys.h"
#include "primitives/Assert.h"
#include "primitives/LogLevel.h"

#endif  // _PRIMITIVES_H_
