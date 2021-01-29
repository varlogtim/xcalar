// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LOG_DUMP_H_
#define _LOG_DUMP_H_

#include <stdint.h>
#include <linux/limits.h>

typedef struct {
    char directory[PATH_MAX];
    char prefix[PATH_MAX];
    char cfgFile[PATH_MAX];
    uint64_t seqNum;
    bool content;
    bool genTest;
    bool printXlrDir;
    bool upgrade;
    bool verbose;
    bool dumplmdb;
} LdOptions;

MustCheck Status upgradeDurables(LdOptions *options);

#endif // _LOG_DUMP_H_
