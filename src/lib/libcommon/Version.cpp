// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>

#include "StrlFunc.h"
#include "libapis/XcalarApiVersionSignature.h"
#include "libapis/XcrpcApiVersionSignature.h"
#include "assert.h"

static const int VersionSignatureBufLen = 100;

static char fullVersionString[VersionSignatureBufLen];

void
versionInit()
{
// runtime init to force the compiler to include it in the data segment
#ifdef DEBUG
    strlcpy(fullVersionString,
            "xcalar-" XLRVERSIONPRETTY " (Debug)",
            sizeof(fullVersionString));
#else
    strlcpy(fullVersionString,
            "xcalar-" XLRVERSIONPRETTY,
            sizeof(fullVersionString));
#endif
}

const char *
versionGetFullStr()
{
    assert(strlen(fullVersionString) < VersionSignatureBufLen);

    return fullVersionString;
}

const char *
versionGetStr()
{
    assert(strlen(XLRVERSIONPRETTY) < VersionSignatureBufLen);

    return XLRVERSIONPRETTY;
}

const char *
versionGetApiStr(XcalarApiVersion apiVer)
{
    const char *apiVerStr =
        strGetFromXcalarApiVersion((XcalarApiVersion) apiVer);

    assert(strlen(apiVerStr) < VersionSignatureBufLen);

    return (apiVerStr);
}

const char *
versionGetXcrpcStr(XcRpcApiVersion apiVer)
{
    const char *apiVerStr = strGetFromXcRpcApiVersion((XcRpcApiVersion) apiVer);

    assert(strlen(apiVerStr) < VersionSignatureBufLen);

    return apiVerStr;
}

XcalarApiVersion
versionGetApiSig()
{
    return XcalarApiVersionSignature;
}

XcRpcApiVersion
versionGetXcrpcSig(void)
{
    return (ProtoAPIVersionSignature);
}
