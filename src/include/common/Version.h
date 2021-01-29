// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _VERSION_H_
#define _VERSION_H_

#include "XcalarApiVersionSignature.h"
#include "XcrpcApiVersionSignature.h"

extern void versionInit();
extern const char* versionGetFullStr();
extern const char* versionGetStr();
extern const char* versionGetApiStr(XcalarApiVersion apiVer);
extern const char* versionGetXcrpcStr(XcRpcApiVersion apiVer);
extern XcalarApiVersion versionGetApiSig();
extern XcRpcApiVersion versionGetXcrpcSig();

#endif  // _VERSION_H_
