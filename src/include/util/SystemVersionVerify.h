// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SYSTEMVERSIONVERIFY_H_
#define _SYSTEMVERSIONVERIFY_H_

#include "primitives/Primitives.h"

class VersionVerify final
{
  public:
    static MustCheck Status verifyVersion(const char *version);

  private:
    static constexpr int BigPrime = 742559;
    static constexpr const char *ModuleName = "versionVerify";

    static MustCheck Status jsonifyInput(const char *version, char **outstr);
    static MustCheck Status parseOutput(const char *outStr, const char *errStr);
    static MustCheck Status parseSingleOutStr(const char *appOutStr);

    VersionVerify() = default;
    ~VersionVerify() = default;
    VersionVerify(const VersionVerify &) = delete;
    VersionVerify &operator=(const VersionVerify &) = delete;

    MustCheck Status init(const char *version);
    MustCheck Status run();

    const char *version_;
};

#endif  // _SYSTEMVERSIONVERIFY_H_
