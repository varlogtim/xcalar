// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef IPARENTNOTIFY_H
#define IPARENTNOTIFY_H

#include "primitives/Primitives.h"

//
// Interface that allows libparent consumers to be notified of important events
// in their childs' lives.
//

class IParentNotify
{
  public:
    virtual ~IParentNotify() {}

    // Invoked on unexpected termination of child process.
    virtual void onDeath() = 0;

    virtual void refGet() = 0;
    virtual void refPut() = 0;
};

#endif  // IPARENTNOTIFY_H
