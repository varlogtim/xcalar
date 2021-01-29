// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#ifndef _LIBDATAFORMATCONSTANTS_H_
#define _LIBDATAFORMATCONSTANTS_H_

namespace df
{
//
// Type Conversion related
//
static constexpr const char *nullStrValue = "null";
enum {
    NullStrValueLen = 4,
};

//
// Internal to Data Format
//

enum {
    NumRangePages = 100 * KB,
    RangePageSize = 1 * MB,
};

}  // namespace df

#endif  // _LIBDATAFORMATCONSTANTS_H_
