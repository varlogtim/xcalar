// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LOCALMSGINT_H
#define LOCALMSGINT_H

#include "primitives/Primitives.h"
#include "sys/XLog.h"

namespace localmsg
{
static constexpr const char *ModuleName = "liblocalmsg";

static void
closeSocket(int fd)
{
    assert(fd > 0);

    int ret;
    do {
        ret = ::close(fd);
    } while (ret == -1 && errno == EINTR);
}

}  // namespace localmsg

#endif  // LOCALMSGINT_H
