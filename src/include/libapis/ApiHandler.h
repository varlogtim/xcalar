// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_H
#define _APIHANDLER_H

#include "primitives/Primitives.h"
#include "libapis/LibApisEnums.h"
#include "dag/DagTypes.h"
#include "util/Stopwatch.h"
#include "runtime/Txn.h"

union TwoPcHandle;
union XcalarApiInput;
struct XcalarApiOutput;
struct XcalarApiUserId;
struct XcalarApiSessionInfoInput;

class ApiHandler
{
    friend class OperatorHandler;

  public:
    ApiHandler(XcalarApis api);
    virtual ~ApiHandler();

    Stopwatch stopwatch_;
    Txn txn_;
    bool txnLogAdded_ = false;

    enum AsyncMode : bool {
        Sync = false,
        Async = true,
    };

    XcalarApiInput *getInput();
    size_t getInputSize();
    XcalarApis getApi();

    virtual Status run(XcalarApiOutput **output, size_t *outputSize) = 0;
    Status init(XcalarApiUserId *userId,
                XcalarApiSessionInfoInput *sessionInfo,
                Dag *dstGraph);

    Status setTxnAndTxnLog(Txn::Mode mode, Runtime::SchedId rtSchedId);
    Status setImmediateTxnAndTxnLog();

    virtual Status setArg(XcalarApiInput *input, size_t inputSize) = 0;
    virtual Status serializeOutput(XcalarApiOutput *buf, size_t bufSize);

    bool needsAck();  // Only shutdownLocal doesn't
    bool needsSessionOrGraph();
    bool mayNeedSessionOrGraph();
    bool needsToRunImmediately();
    bool needsToRunInline();
    bool hasAsync();
    bool isOperator();
    bool needsXdb();

  protected:
    XcalarApiUserId *userId_ = NULL;
    XcalarApis api_;
    XcalarApiInput *apiInput_ = NULL;
    size_t inputSize_ = 0;
    Dag *dstGraph_ = NULL;
    DagTypes::GraphType dstGraphType_;
    XcalarApiSessionInfoInput *sessionInfo_ = NULL;

    enum Flags : uint64_t {
        NoFlags = 0,
        NeedsAck = 1,
        NeedsSessionOrGraph = 1 << 1,
        NeedsToRunImmediately = 1 << 2,
        HasAsync = 1 << 3,
        IsOperator = 1 << 4,
        NeedsXdb = 1 << 5,
        // enable this bit iff you're sure the api will not block
        // this will be executed on the listener thread
        NeedsToRunInline = 1 << 6,
        DoNotGetParentRef = 1 << 7,
        MayNeedSessionOrGraph = 1 << 8,
    };

  private:
    virtual Flags getFlags() = 0;
};

#endif  // _APIHANDLER_H
