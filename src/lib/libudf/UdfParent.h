// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFPARENT_H_
#define _UDFPARENT_H_

#include "primitives/Primitives.h"
#include "udf/UdfError.h"
#include "udf/UdfTypes.h"
#include "operators/XcalarEvalTypes.h"
#include "runtime/Spinlock.h"
#include "runtime/Semaphore.h"

class UdfLocal;
class ParentChild;

//
// This class is responsible for ferrying UDF module definitions and execution
// requests between usrnode and childnode.
//
class UdfParent final
{
  public:
    UdfParent(UdfLocal *udfLocal) : udfLocal_(udfLocal) {}

    ~UdfParent() = default;

    MustCheck Status addOrUpdate(ParentChild *child,
                                 char *moduleNameVersioned,
                                 UdfModuleSrc *input,
                                 UdfError *error);
    MustCheck Status stageAndListFunctions(char *moduleNameVersioned,
                                           UdfModuleSrc *input,
                                           UdfError *error,
                                           XcalarEvalFnDesc **functions,
                                           size_t *functionsCount);

  private:
    static constexpr const char *ModuleName = "libudf";
    UdfLocal *udfLocal_;
    bool isInit_;

    void err(const char *format, ...);
    MustCheck Status listFunctions(ParentChild *child,
                                   const char *moduleNameVersioned,
                                   XcalarEvalFnDesc **functions,
                                   size_t *functionsCount);

    // Disallow.
    UdfParent(const UdfParent &) = delete;
    UdfParent &operator=(const UdfParent &) = delete;
};

#endif  // _UDFPARENT_H_
