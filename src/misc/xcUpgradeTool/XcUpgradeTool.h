// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XC_UPGRADE_TOOL_
#define _XC_UPGRADE_TOOL_

#include <stdint.h>
#include <linux/limits.h>

#include <assert.h>
#include <err.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include "util/StringHashTable.h"

struct UpgradeToolOptions {
    char cfgFile[PATH_MAX];
    bool force;
};

struct UpgradeToolArgs {
    UpgradeToolOptions *options;
    Status status;
};

// Hash table to keep track of bad sessions
struct SessionEntry {
    StringHashTableHook hook;
    char sessionName[SessionNameLen + 1];
    const char *
    getSessionName() const
    {
        return sessionName;
    };
    void del();
};
typedef StringHashTable<SessionEntry,
                        &SessionEntry::hook,
                        &SessionEntry::getSessionName,
                        7,  // slots
                        hashStringFast>
    SessionHashTable;

// Hash table to keep track of users with bad sessions
struct UserSessionEntry {
    StringHashTableHook hook;
    char userName[LOGIN_NAME_MAX + 1];
    SessionHashTable sessionHt;
    const char *
    getUserName() const
    {
        return userName;
    };
    void del();
};
typedef StringHashTable<UserSessionEntry,
                        &UserSessionEntry::hook,
                        &UserSessionEntry::getUserName,
                        47,  // slots
                        hashStringFast>
    UserSessionHashTable;

#endif  // _XC_UPGRADE_TOOL_
