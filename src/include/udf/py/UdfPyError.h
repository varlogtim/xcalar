// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFPYERROR_H_
#define _UDFPYERROR_H_

#include "udf/UdfError.h"

// Following routine will update error with a python error (if one has
// occurred), or the error string passed in via nonPyErrStr (if no python error
// can be detected).
void udfPyErrorCreate(UdfError *error, char *nonPyErrStr);
#endif  // _UDFPYERROR_H_
