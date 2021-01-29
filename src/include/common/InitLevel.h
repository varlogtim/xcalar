// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _INITLEVEL_H_
#define _INITLEVEL_H_

// InitLevel is a property of an executable that defines what level of
// functionality it will make use of. Higher levels will make more components
// available to the binary.
enum class InitLevel {
    Invalid,
    Cli,
    Basic,
    Config,
    ChildNode,
    UsrNode,
    UsrNodeWithChildNode,
};

#endif  // _INITLEVEL_H_
