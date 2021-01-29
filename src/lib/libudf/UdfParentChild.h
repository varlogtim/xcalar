// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFPARENTCHILD_H_
#define _UDFPARENTCHILD_H_

struct UdfResultGetInput {
    ParentChildShmPtr ptr;
    uintptr_t childPtr;
    size_t offset;  // Start at this spot in output.
};

struct UdfResultFreeInput {
    uintptr_t childPtr;
};

struct UdfExecOnLoadOutput {
    ParentChildShmPtr ptr;
    size_t bytes;
    bool isError;
};

struct UdfExecOnLoadResponse {
    size_t bytes;
    // NULL if output placed into xfer space.
    uintptr_t childPtr;
};

#endif  // _UDFPARENTCHILD_H_
