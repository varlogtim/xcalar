 // Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef XCALAR_MAIN_H
#define XCALAR_MAIN_H

#include "primitives/Primitives.h"

__attribute__((format(printf, 1, 2))) void printError(const char * format, ...);

MustCheck int mainUsrnode(int argc, char *argv[]);

MustCheck int mainChildnode(int argc, char *argv[]);

MustCheck Status setCoreDumpSize(rlim_t size);

MustCheck Status redirectStdIo(const char *fnamePrefix);

#endif // XCALAR_MAIN_H
