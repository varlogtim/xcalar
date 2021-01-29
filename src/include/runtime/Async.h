// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef ASYNC_H
#define ASYNC_H

#include "runtime/Promise.h"

template <typename Ret, typename F, typename... Args>
Status
async(Future<Ret> *future, F func, Args... args)
{
    Status status;
    FuncPromise<Ret, F, Args...> *prom = NULL;

    prom = new (std::nothrow) FuncPromise<Ret, F, Args...>(func, args...);
    BailIfNull(prom);

    *future = prom->getFuture();

    status = Runtime::get()->schedule(prom);
    BailIfFailed(status);

    // We have now succeeded.
CommonExit:
    return status;
}

template <typename Ret, typename I, typename M, typename... Args>
Status
asyncObj(Future<Ret> *future, I *instance, M method, Args... args)
{
    Status status;
    ObjectPromise<Ret, I, M, Args...> *prom = NULL;

    prom = new (std::nothrow)
        ObjectPromise<Ret, I, M, Args...>(instance, method, args...);
    BailIfNull(prom);

    *future = prom->getFuture();

    status = Runtime::get()->schedule(prom);
    BailIfFailed(status);

CommonExit:
    return status;
}

#endif  // ASYNC_H
