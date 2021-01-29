// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFTYPES_H_
#define _UDFTYPES_H_

#include "libapis/LibApisConstants.h"
#include "UdfTypeEnums.h"
#include "operators/GenericTypes.h"
#include "log/Log.h"

// Versioned module name format <moduleName>-<versionId>
static constexpr const uint64_t UdfVersionedModuleName =
    XcalarApiMaxUdfModuleNameLen + 1 + UInt64MaxStrLen;

// Full path to the leaf udf dir is:
//
// <sharedRoot>/<userName>/<workbookName>/LogLib::UdfWkBookDirName
//
// Following constant calculates max len of a string that has this path

static constexpr const uint64_t MaxUdfPathLen =
    XcalarApiMaxPathLen + 1 + LOGIN_NAME_MAX + 1 + XcalarApiSessionNameLen + 1 +
    sizeof(LogLib::UdfWkBookDirName);

static constexpr const uint64_t UdfVersionedFQFname =
    UdfVersionedModuleName + XcalarApiMaxUdfFuncNameLen + 2;

struct UdfLoadArgs {
    char fullyQualifiedFnName[UdfVersionedFQFname];
};

struct UdfModuleSrc {
    UdfType type;
    bool isBuiltin;
    char moduleName[XcalarApiMaxUdfModuleNameLen + 1];
    char modulePath[MaxUdfPathLen + 1];
    size_t sourceSize;
    char source[0];
};

struct UdfNwFlushPath {
    bool isCreate;
    size_t pathSize;
    char path[0];
    // DO NOT ADD ANY FIELD HERE! pathSize is followed by actual path
};

static inline size_t
udfModuleSrcSize(UdfModuleSrc *module)
{
    return sizeof(*module) + module->sourceSize;
}

#endif  // _UDFTYPES_H_
