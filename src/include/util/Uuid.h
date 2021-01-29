// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UUID_H_
#define _UUID_H_

// 128-bit Uuid
struct Uuid {
    uint64_t buf[2];
};

extern Status uuidGenerate(Uuid *uuidIn);
extern bool uuidCmp(Uuid *uuid1, Uuid *uuid2);

#endif  // _UUID_H
