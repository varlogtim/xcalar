// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFERROR_H_
#define _UDFERROR_H_

#include "primitives/Primitives.h"
#include "util/MemTrack.h"

//
// This header defines a simple error type for use in UDFs. Creation of these
// error objects is deferred to the different UDF implementations.
//
class UdfError
{
  public:
    UdfError() = default;
    ~UdfError()
    {
        if (message_ != NULL) {
            memFree((void *) message_);
            message_ = NULL;
        }
    }

    const char *message_ = NULL;
};

#endif  // _UDFERROR_H_
