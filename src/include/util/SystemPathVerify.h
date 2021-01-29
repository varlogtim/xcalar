// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SYSTEMPATHVERIFY_H_
#define _SYSTEMPATHVERIFY_H_

#include "primitives/Primitives.h"

class PathVerify final
{
  public:
    static MustCheck Status verifyAll(const char *paths[],
                                      int numPaths,
                                      bool *allShared);
    static MustCheck Status verifyPath(const char *path, bool *isShared);

  private:
    static constexpr int BigPrime = 222857;
    static constexpr const char *ModuleName = "pathVerify";

    static MustCheck Status jsonifyInput(const char *paths[],
                                         int numPaths,
                                         char **outStr);
    static MustCheck Status parseOutput(const char *outStr,
                                        const char *errStr,
                                        bool *allShared);
    static MustCheck Status parseSingleOutStr(const char *appOutStr,
                                              bool *allShared);

    PathVerify() = default;
    ~PathVerify() = default;
    PathVerify(const PathVerify &) = delete;
    PathVerify &operator=(const PathVerify &) = delete;

    // Takes a soft-reference to paths. paths must stay valid for the lifetime
    // of this object
    MustCheck Status init(const char *paths[], int numPaths);

    MustCheck Status run(bool *allShared);

    const char **paths_;
    int numPaths_;
};

#endif  // _SYSTEMPATHVERIFY_H_
