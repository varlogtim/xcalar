// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBUDFCONSTANTS_H_
#define _LIBUDFCONSTANTS_H_

#include "udf/UdfTypes.h"

namespace udf
{
//
// Udf Error related
//
enum UdfErrorFlatVersion {
    UdfErrorFlatVersion1 = 1,
    UdfErrorFlatVersionCurrent = UdfErrorFlatVersion1,
};

//
// Udf Persist related
//
enum {
    UdfPersistVersion1 = 1,
    UdfPersistBlockAlign = 16,
};

static constexpr const char *pySourceFileSuffix = ".py";
static constexpr const char *metaFileSuffix = ".xlrudf";
static constexpr const char *udfNewVerTmpSufix = "_XC_NEW_VER_TMP";
static constexpr const char *sourcesPyName = "python";

}  // namespace udf

#endif  // _LIBUDFCONSTANTS_H_
