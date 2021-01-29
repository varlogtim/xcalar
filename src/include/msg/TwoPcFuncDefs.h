// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TWOPCFUNCDEFS_H_
#define _TWOPCFUNCDEFS_H_

// Messages may have the following types:
// Singletons: standalone messages, typically failures
// Request-response: self descriptive
// 2PC with strong ordering - distributed mutex
//
// 2PC: A 2PC cmd goes through a Cmd -> CmdComplete lifecycle.
// Note: Keep the order of these commands the same as the impl in
// Message.c::recvData().
// Note: The 2PC enums are used algorithmically. For example, the code
// assumes that
// Msg2pcRpcGetStat + TwoPcCmdCompletionOffset == Msg2pcRpcGetStatComplete.
// Note: Keep the enums numbered for easy debug.
//
// 2PC with total ordering - basically a distributed mutex:
// If multiple twoPc() calls with the same msgTypeId are competing, this allows
// mutual exclusion (global consensus). This is implemented using nested 2pc
// calls. The outer 2pc call directs the actual 2pc call to a particular
// node that is chosen using config->getMyDlmNode(msgTypeId). Different
// msgTypeIds are distributed using a hash across the cluster. This means there
// is a DLM master for each msgTypeId.
// Anyone using total ordering must declare 4 enums in the MsgTypeId table
// below: 2 for the outer 2pc and 2 for the inner 2pc call.
// Note: If you are looking to pose on the podium as the killer
// distributed system person then go back to twoPc() that gives you
// more flexibility in the consistency v/s concurrency tradeoffs.
// twoPcStrongOrder() is expensive like a 2pc call is supposed
// to be. Rarely does one need global mutual exclusion with total ordering.
// So first focus on identifying if your algorithm is a good candidate for
// twoPcStrongOrder().

enum class MsgTypeId : uint32_t {
    // Note: Do not stick any enums before this!
    MsgTypeIdFirstEnum,

    // Singletons
    MsgNull,
    MsgWaitingOn2pcBarrier,

    // Barrier 2pc: Used only at startup and is not efficient for fast path.
    Msg2pcBarrier,
    Msg2pcBarrierComplete,

    // Fast path request-response 2pc.
    Msg2pcNextResultSet,
    Msg2pcNextResultSetComplete,

    Msg2pcGetDataUsingFatptr,
    Msg2pcGetDataUsingFatptrComplete,

    Msg2pcRpcGetStat,
    Msg2pcRpcGetStatComplete,

    Msg2pcCountResultSetEntries,
    Msg2pcCountResultSetEntriesComplete,

    Msg2pcXcalarApiAggregate,
    Msg2pcXcalarApiAggregateComplete,

    Msg2pcXcalarApiProject,
    Msg2pcXcalarApiProjectComplete,

    Msg2pcXcalarApiFilter,
    Msg2pcXcalarApiFilterComplete,

    Msg2pcXcalarApiGroupBy,
    Msg2pcXcalarApiGroupByComplete,

    Msg2pcXcalarApiJoin,
    Msg2pcXcalarApiJoinComplete,

    Msg2pcXcalarApiGetTableMeta,
    Msg2pcXcalarApiGetTableMetaComplete,

    Msg2pcXcalarApiCancel,
    Msg2pcXcalarApiCancelComplete,

    Msg2pcXcalarApiGetOpStatus,
    Msg2pcXcalarApiGetOpStatusComplete,

    Msg2pcResetStat,
    Msg2pcResetStatComplete,

    Msg2pcXcalarApiIndex,
    Msg2pcXcalarApiIndexComplete,

    Msg2pcXcalarApiSynthesize,
    Msg2pcXcalarApiSynthesizeComplete,

    Msg2pcGetStatGroupIdMap,
    Msg2pcGetStatGroupIdMapComplete,

    Msg2pcGetKeysUsingFatptr,
    Msg2pcGetKeysUsingFatptrComplete,

    Msg2pcXcalarApiMap,
    Msg2pcXcalarApiMapComplete,

    Msg2pcXcalarApiUnion,
    Msg2pcXcalarApiUnionComplete,

    Msg2pcXcalarApiGetRowNum,
    Msg2pcXcalarApiGetRowNumComplete,

    // XXX XcalarApiTop should be part of statModule.
    // Should not be independent
    Msg2pcXcalarApiTop,
    Msg2pcXcalarApiTopComplete,

    Msg2pcXdfParallelDo,
    Msg2pcXdfParallelDoComplete,

    Msg2pcDropXdb,
    Msg2pcDropXdbComplete,

    Msg2pcDeserializeXdbPages,
    Msg2pcDeserializeXdbPagesComplete,

    Msg2pcSerDesXdb,
    Msg2pcSerDesXdbComplete,

    Msg2pcQueryState,
    Msg2pcQueryStateComplete,

    Msg2pcQueryCancel,
    Msg2pcQueryCancelComplete,

    Msg2pcQueryDelete,
    Msg2pcQueryDeleteComplete,

    Msg2pcXdbLoadDone,
    Msg2pcXdbLoadDoneComplete,

    Msg2pcDlmResolveKeyType,
    Msg2pcDlmResolveKeyTypeComplete,

    Msg2pcXcalarAggregateEvalDistributeEvalFn,
    Msg2pcXcalarAggregateEvalDistributeEvalFnComplete,

    Msg2pcDlmUpdateKvStore,
    Msg2pcDlmUpdateKvStoreComplete,

    Msg2pcDlmUpdateNs,
    Msg2pcDlmUpdateNsComplete,

    Msg2pcPubTableDlm,
    Msg2pcPubTableDlmComplete,

    Msg2pcDatasetReference,
    Msg2pcDatasetReferenceComplete,

    Msg2pcDlmRetinaTemplate,
    Msg2pcDlmRetinaTemplateComplete,

    Msg2pcExportMakeScalarTable,
    Msg2pcExportMakeScalarTableComplete,

    // The next 4 enums are intended for unit test.  They are essentially
    // no-ops for normal usage.
    Msg2pcTestImmed,
    Msg2pcTestImmedComplete,

    Msg2pcTestRecv,
    Msg2pcTestRecvComplete,

    Msg2pcGvmBroadcast,
    Msg2pcGvmBroadcastComplete,

    Msg2pcFuncTest,
    Msg2pcFuncTestComplete,

    Msg2pcStream,
    Msg2pcStreamComplete,

    Msg2pcDemystifySourcePage,
    Msg2pcDemystifySourcePageComplete,

    Msg2pcProcessScalarPage,
    Msg2pcProcessScalarPageComplete,

    Msg2pcIndexPage,
    Msg2pcIndexPageComplete,

    Msg2pcLogLevelSet,
    Msg2pcLogLevelSetComplete,

    Msg2pcSetConfig,
    Msg2pcSetConfigComplete,

    Msg2pcGetRows,
    Msg2pcGetRowsComplete,

    Msg2pcGetPerfStats,
    Msg2pcGetPerfStatsComplete,

    Msg2pcAddOrUpdateUdfDlm,
    Msg2pcAddOrUpdateUdfDlmComplete,

    // Note: Do not stick any enums after this!
    MsgTypeIdLastEnum,
};

// Every 2pc call can have a unique signature. The enums below capture
// unique 2pc calls. Each of these calls have a family of functions that
// can be set up for doing 2pc work.
// Here is an example of how you can define new enums:
// If there are 3 flavors of Msg2pcUpdateGvm 2pc calls then define
// Msg2pcUpdateGvm1, Msg2pcUpdateGvm2, Msg2pcUpdateGvm3, and  define
// function signatures in src/lib/libmsg/TwoPcFuncDefs.c.
// Note: the enums start at 1000 for debug so we do not get screwed by gcc not
// enforcing enum types.
enum class TwoPcCallId : uint32_t {
    TwoPcCallIdFirstEnum = 1000,  // Do not add before this enum.

    Msg2pcNextResultSet1,
    Msg2pcGetDataUsingFatptr1,
    Msg2pcRpcGetStat1,
    Msg2pcCountResultSetEntries1,
    Msg2pcXcalarApiFilter1,
    Msg2pcXcalarApiGroupBy1,
    Msg2pcXcalarApiJoin1,
    Msg2pcXcalarApiGetTableMeta1,
    Msg2pcResetStat1,
    Msg2pcXcalarApiIndex1,
    Msg2pcXcalarApiSynthesize1,
    Msg2pcGetStatGroupIdMap1,
    Msg2pcGetKeysUsingFatptr1,
    Msg2pcXcalarApiMap1,
    Msg2pcXcalarApiUnion1,
    Msg2pcXcalarApiTop1,
    Msg2pcXdfParallelDo1,
    Msg2pcXcalarApiProject1,
    Msg2pcDropXdb1,
    Msg2pcDeserializeXdbPages1,
    Msg2pcQueryState1,
    Msg2pcXdbLoadDone1,
    Msg2pcDlmResolveKeyType1,
    Msg2pcFuncTest1,
    Msg2pcExportMakeScalarTable1,
    Msg2pcTwoPcBarrier,
    Msg2pcXcalarAggregateEvalDistributeEvalFn1,
    Msg2pcTestImmed1,
    Msg2pcTestRecv1,
    Msg2pcTestImmedFuncTest,
    Msg2pcTestRecvFuncTest,
    Msg2pcDlmUpdateNs1,
    Msg2pcDlmKvStoreOp1,
    Msg2pcDlmRetinaTemplate1,
    Msg2pcXcalarApiAggregate1,
    Msg2pcStreamAction,
    Msg2pcXcalarApiGetRowNum1,
    Msg2pcXcalarApiCancel1,
    Msg2pcXcalarApiGetOpStatus1,
    Msg2pcQueryCancel1,
    // FIXME: Msg2pcDemystifySourcePage1 and Msg2pcProcessScalarPage1
    // needs to be moved out from here
    Msg2pcDemystifySourcePage1,
    Msg2pcProcessScalarPage1,
    Msg2pcIndexPage1,
    CallIdGvmBroadcast,
    Msg2pcLogLevelSet1,
    Msg2pcQueryDelete1,
    Msg2pcDatasetReference1,
    Msg2pcSetConfig1,
    Msg2pcDeserializeXdb,
    Msg2pcSerializeXdb,
    Msg2pcGetRows1,
    Msg2pcPubTableDlm1,
    Msg2pcGetPerfStats1,
    Msg2pcAddOrUpdateUdfDlm1,
    TwoPcCallIdLastEnum,  // Do not add after this enum.
};

#endif  // _TWOPCFUNCDEFS_H_
