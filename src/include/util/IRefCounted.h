// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef IREFCOUNTED_H
#define IREFCOUNTED_H

//
// Interface that's useful when something needs to know a class is ref-counted
// but doesn't necessarily know its type. Does not actually implement any
// ref-counting.
//

class IRefCounted
{
  public:
    virtual ~IRefCounted() {}

    virtual void refGet() = 0;
    virtual void refPut() = 0;
};

#endif  // IREFCOUNTED_H
