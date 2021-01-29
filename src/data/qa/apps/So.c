// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/resource.h>

void
disableCoreDumping()
{
    struct rlimit coreLimit;
    coreLimit.rlim_cur = 0;
    coreLimit.rlim_max = 0;
    setrlimit(RLIMIT_CORE, &coreLimit);
}

int
xmain(const char *inStr, char **outStr, char **errStr)
{
    *outStr = malloc(1024);
    **outStr = '\0';

    if (strcmp(inStr, "hello") == 0) {
        snprintf(*outStr, 1024, "hello cruel world");
    } else if (strcmp(inStr, "go boom") == 0) {
        disableCoreDumping();
        // Add 1 to all integers with value 5.
        *(int *)5 = 6;
    } else if (strcmp(inStr, "error") == 0) {
        *errStr = malloc(1024);
        snprintf(*errStr, 1024, "something bad happened");
        return(1);
    } else if (strcmp(inStr, "badptrpls") == 0) {
        disableCoreDumping();
        free(*outStr);
        *outStr = (char *)5;
        *errStr = (char *)-1;
    } else {
        exit(1);
    }
    return(0);
}
