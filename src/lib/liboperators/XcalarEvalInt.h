#ifndef _XCALAR_EVAL_INT_H
#define _XCALAR_EVAL_INT_H

#include "operators/XcalarEval.h"
#include "operators/XcalarEvalTypes.h"

struct XcalarEvalFuncListInfo {
    StringHashTableHook hook;
    XcalarEvalFnDesc fnDesc;
    const char *getFnName() const;
    void del();
};

static constexpr const uint64_t XcalarEvalFuncListHashSlots = 89;

typedef StringHashTable<XcalarEvalFuncListInfo,
                        &XcalarEvalFuncListInfo::hook,
                        &XcalarEvalFuncListInfo::getFnName,
                        XcalarEvalFuncListHashSlots,
                        hashStringFast>
    XcalarEvalFuncListHashTable;

struct AggregateContext {
    XdfAggregateAccumulators *acc;
    XdfAggregateHandlers aggregateHandler;
    Mutex lock;
};

enum {
    // Do you want XcalarEval to ensure that the args are valid?
    EnforceArgCheck = true,
    DontEnforceArgCheck = false,
};

extern XcalarEvalRegisteredFn builtInFns[];
extern const size_t builtInFnsCount;

Status parallelAggregateLocal(ScalarGroupIter *groupIter,
                              XdfAggregateHandlers aggHandler,
                              XdfAggregateAccumulators *acc,
                              void *broadcastPacket,
                              size_t broadcastPacketSize);

Status parallelEvalLocal(ScalarGroupIter *groupIter,
                         XdbId outputScalarXdbId,
                         XcalarEvalRegisteredFn *registeredFn);

void xdfAggComplete(XdfAggregateHandlers aggHandler,
                    XdfAggregateAccumulators *srcAcc,
                    Status status,
                    Status srcAccStatus,
                    AggregateContext *aggContext);

void aggregateAndUpdate(ScalarGroupIter *groupIter,
                        XdfAggregateHandlers aggHandler,
                        XdfAggregateAccumulators *acc,
                        AggregateContext *aggContext,
                        void *broadcastPacket,
                        size_t broadcastPacketSize,
                        int argc,
                        Scalar **argv,
                        Scalar **scratchPadScalars,
                        NewKeyValueEntry *kvEntry,
                        OpEvalErrorStats *errorStats);

#endif  // _XCALAR_EVAL_H
