// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <errno.h>
#include <stdio.h>

#include "primitives/Primitives.h"
#include "util/Uuid.h"
#include "util/System.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libutil::uuid";

Status
uuidGenerate(Uuid *uuidIn)
{
    Status status = StatusUnknown;
    FILE *fp = NULL;
    size_t ret;
    unsigned ii;

    errno = 0;
    fp = fopen("/dev/urandom", "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to open /dev/urandom: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    errno = 0;
    ret = fread(&uuidIn->buf[0],
                sizeof(uuidIn->buf[ii]),
                ArrayLen(uuidIn->buf),
                fp);
    if (ret != ArrayLen(uuidIn->buf)) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Error reading /dev/urandom: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    return status;
}

bool
uuidCmp(Uuid *uuid1, Uuid *uuid2)
{
    return uuid1->buf[0] == uuid2->buf[0] && uuid1->buf[1] == uuid2->buf[1];
}
