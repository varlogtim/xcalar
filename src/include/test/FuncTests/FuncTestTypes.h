// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#ifndef FUNCTESTTYPES_H
#define FUNCTESTTYPES_H

class FuncTestTypes
{
  public:
    static size_t constexpr MaxTestNameLen = 255;

    struct FuncTestOutput {
        char testName[MaxTestNameLen + 1];
        StatusCode status;
    };
};

#endif  // FUNCTESTTYPES_H
