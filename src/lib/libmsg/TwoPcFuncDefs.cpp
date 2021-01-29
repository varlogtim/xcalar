// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <unistd.h>

#include "primitives/Primitives.h"
#include "msg/TwoPcFuncDefs.h"
#include "TwoPcInt.h"
#include "msg/Message.h"
#include "MessageInt.h"
#include "stat/Statistics.h"
#include "usrnode/UsrNode.h"
#include "msg/Message.h"
#include "xdb/Xdb.h"
#include "querymanager/QueryManager.h"
#include "dag/DagLib.h"
#include "operators/Operators.h"
#include "df/DataFormat.h"
#include "operators/Xdf.h"
#include "operators/XcalarEval.h"
#include "dataset/Dataset.h"
#include "util/MemTrack.h"
#include "kvstore/KvStore.h"
#include "udf/UserDefinedFunction.h"
#include "MessageInt.h"
#include "gvm/Gvm.h"
#include "msgstream/MsgStream.h"
#include "ns/LibNs.h"
#include "dataset/Dataset.h"
#include "usr/Users.h"
#include "xdb/HashTree.h"

static constexpr const char *moduleName = "libmsg";
TwoPcMgr *TwoPcMgr::instance = NULL;

TwoPcMgr *
TwoPcMgr::get()
{
    return instance;
}

Status
TwoPcMgr::init()
{
    uint32_t ii = 0;
    uint32_t statIndex = 0;

    assert(instance == NULL);
    instance = (TwoPcMgr *) memAllocExt(sizeof(TwoPcMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) TwoPcMgr();

    TwoPcOp *op = instance->twoPcOp_;
    Status status = StatusOk;

    assert((uint32_t) MsgTypeId::MsgTypeIdLastEnum <= TwoPcMaxCmds);
    assert((uint32_t) TwoPcCallId::TwoPcCallIdLastEnum <= TwoPcMaxCmds);

    for (uint32_t jj = (uint32_t) MsgTypeId::MsgTypeIdFirstEnum + 1;
         jj < (uint32_t) MsgTypeId::MsgTypeIdLastEnum;
         jj++) {
        op[jj].init((MsgTypeId) jj);
    }

    memZero(instance->twoPcAction_, sizeof(TwoPcAction *) * TwoPcMaxCmds);

    StatsLib *statsLib = StatsLib::get();
    status = statsLib->initNewStatGroup("uk.msgNumInvocationsCount",
                                        &instance->count2pcInvokedStatGrpId_,
                                        Num2PCs);
    BailIfFailed(status);
    status = statsLib->initNewStatGroup("uk.msgNumCompletionsCount",
                                        &instance->count2pcFinishedStatGrpId_,
                                        Num2PCs);
    BailIfFailed(status);

    status = statsLib->initNewStatGroup("uk.msgNumErrorsCount",
                                        &instance->count2pcErrorsStatGrpId_,
                                        Num2PCs);
    BailIfFailed(status);

    status =
        statsLib->initNewStatGroup("uk.msgRemoteInvocationsCount",
                                   &instance->countRemote2pcEntryStatGrpId_,
                                   Num2PCs);
    BailIfFailed(status);

    status =
        statsLib->initNewStatGroup("uk.msgRemoteFinishedCount",
                                   &instance->countRemote2pcFinishedStatGrpId_,
                                   Num2PCs);
    BailIfFailed(status);

    status =
        statsLib->initNewStatGroup("uk.msgRemoteErrorCount",
                                   &instance->countRemote2pcErrorsStatGrpId_,
                                   Num2PCs);
    BailIfFailed(status);

    for (ii = (uint32_t) TwoPcCallId::TwoPcCallIdFirstEnum + 1;
         ii < (uint32_t) TwoPcCallId::TwoPcCallIdLastEnum;
         ii++) {
        statIndex = ii - ((uint32_t) TwoPcCallId::TwoPcCallIdFirstEnum + 1);
        instance->numLocal2PCInvoked_[statIndex] = NULL;
        instance->numLocal2PCFinished_[statIndex] = NULL;
        instance->numLocal2PCErrors_[statIndex] = NULL;
        instance->numRemote2PCEntry_[statIndex] = NULL;
        instance->numRemote2PCFinished_[statIndex] = NULL;
        instance->numRemote2PCErrors_[statIndex] = NULL;

        switch ((TwoPcCallId) ii) {
        case TwoPcCallId::Msg2pcNextResultSet1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcNextResultSet1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcNextResultSet1();
            status = instance->initStat(statIndex, "ResulSetNext");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcGetDataUsingFatptr1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcGetDataUsingFatptr1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcGetDataUsingFatptr1();
            // Note: ftptr used instead of fat pointer since the stats
            //       names will be visible externally
            status = instance->initStat(statIndex, "GetDataUsingFtptr");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcRpcGetStat1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcRpcGetStat1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcRpcGetStat1();
            status = instance->initStat(statIndex, "RPCStats");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcCountResultSetEntries1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcCountResultSetEntries1),
                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii])
                TwoPcMsg2pcCountResultSetEntries1();
            status = instance->initStat(statIndex, "CountResultSetEntries");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiFilter1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiFilter1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiFilter1();
            status = instance->initStat(statIndex, "Filter");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiGroupBy1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiGroupBy1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiGroupBy1();
            status = instance->initStat(statIndex, "GroupBy");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiJoin1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiJoin1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiJoin1();
            status = instance->initStat(statIndex, "Join");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiGetTableMeta1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiGetTableMeta1),
                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii])
                TwoPcMsg2pcXcalarApiGetTableMeta1();
            status = instance->initStat(statIndex, "GetTableMeta");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcResetStat1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcResetStat1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcResetStat1();
            status = instance->initStat(statIndex, "RPCResetStats");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiIndex1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiIndex1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiIndex1();
            status = instance->initStat(statIndex, "TableCreate");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiSynthesize1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiSynthesize1),
                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiSynthesize1();
            status = instance->initStat(statIndex, "Synthesize");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcGetStatGroupIdMap1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcGetStatGroupIdMap1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcGetStatGroupIdMap1();
            status = instance->initStat(statIndex, "GetStatGroupMap");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcGetKeysUsingFatptr1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcGetKeysUsingFatptr1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcGetKeysUsingFatptr1();
            status = instance->initStat(statIndex, "GetKeysUsingFtptr");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiMap1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiMap1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiMap1();
            status = instance->initStat(statIndex, "Map");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiUnion1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiUnion1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiUnion1();
            status = instance->initStat(statIndex, "Union");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiTop1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiTop1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiTop1();
            status = instance->initStat(statIndex, "Msg2pcXcalarApiTop");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXdfParallelDo1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXdfParallelDo1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXdfParallelDo1();
            status = instance->initStat(statIndex, "XDFParallelOperation");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiProject1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiProject1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiProject1();
            status = instance->initStat(statIndex, "Project");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDropXdb1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcDropXdb1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDropXdb1();
            status = instance->initStat(statIndex, "DropTable");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDeserializeXdbPages1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcDeserializeXdbPages1),
                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDeserializeXdbPages1();
            status = instance->initStat(statIndex, "DeserializeXdbPages");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcSerializeXdb:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcSerializeXdb),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcSerializeXdb();
            status = instance->initStat(statIndex, "SerializeXdb");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDeserializeXdb:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcDeserializeXdb),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDeserializeXdb();
            status = instance->initStat(statIndex, "DeserializeXdb");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcQueryState1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcQueryState1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcQueryState1();
            status = instance->initStat(statIndex, "GetBatchDFGState");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXdbLoadDone1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXdbLoadDone1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXdbLoadDone1();
            status = instance->initStat(statIndex, "TableCreationCompletion");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDlmResolveKeyType1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcDlmResolveKeyType1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDlmResolveKeyType1();
            status = instance->initStat(statIndex, "ResolveKeyType");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcExportMakeScalarTable1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcExportMakeScalarTable1),
                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii])
                TwoPcMsg2pcExportMakeScalarTable1();
            status =
                instance->initStat(statIndex, "CreateScalarTableForExport");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcTwoPcBarrier:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcTwoPcBarrier),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcTwoPcBarrier();
            status =
                instance->initStat(statIndex, "Multi-nodeOperationBarrier");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarAggregateEvalDistributeEvalFn1:
            instance->twoPcAction_[ii] = (TwoPcAction *) memAllocExt(
                sizeof(TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1),
                moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii])
                TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1();
            status = instance->initStat(statIndex, "AggregateEvaluation");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcTestImmed1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcTestImmed1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcTestImmed1();
            status = instance->initStat(statIndex, "TestFunction1Immediate");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcTestRecv1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcTestRecv1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcTestRecv1();
            status =
                instance->initStat(statIndex, "TestFunction1NormalProcessing");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcTestImmedFuncTest:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcTestImmedFuncTest), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcTestImmedFuncTest();
            status =
                instance->initStat(statIndex, "TestFunctionStressImmediate");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcTestRecvFuncTest:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcTestRecvFuncTest),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcTestRecvFuncTest();
            status = instance->initStat(statIndex,
                                        "TestFunctionStressNormalProcessing");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDlmKvStoreOp1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcDlmUpdateKvStore1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDlmUpdateKvStore1();
            status = instance->initStat(statIndex, "KvStoreUpdate");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiAggregate1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiAggregate1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiAggregate1();
            status = instance->initStat(statIndex, "Aggregate");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiGetRowNum1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiGetRowNum1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiGetRowNum1();
            status = instance->initStat(statIndex, "GetRowNumber");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiCancel1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcXcalarApiCancel1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiCancel1();
            status = instance->initStat(statIndex, "CancelOperation");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcXcalarApiGetOpStatus1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcXcalarApiGetOpStatus1),
                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcXcalarApiGetOpStatus1();
            status = instance->initStat(statIndex, "GetOperationStatus");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcQueryCancel1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcQueryCancel1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcQueryCancel1();
            status = instance->initStat(statIndex, "CancelBatchQuery");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcQueryDelete1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcQueryDelete1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcQueryDelete1();
            status = instance->initStat(statIndex, "DeleteBatchQuery");
            BailIfFailed(status);
            break;

        case TwoPcCallId::CallIdGvmBroadcast:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcCallIdGvmBroadcast),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcCallIdGvmBroadcast();
            status = instance->initStat(statIndex, "GVM2Broadcast");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcFuncTest1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcFuncTest1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcFuncTest1();
            status = instance->initStat(statIndex, "FunctionalTest");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcGetRows1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcGetRows1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcGetRows1();
            status = instance->initStat(statIndex, "FunctionalTest");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcGetPerfStats1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcGetPerfStats1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcGetPerfStats1();
            status = instance->initStat(statIndex, "GetPerfStats");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcStreamAction:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(Msg2pcStreamActionImpl),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) Msg2pcStreamActionImpl();

            status = instance->initStat(statIndex, "MessageStream");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDlmUpdateNs1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcDlmUpdateNs1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDlmUpdateNs1();

            status = instance->initStat(statIndex, "LibNs");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDatasetReference1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcDatasetReference1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDatasetReference1();

            status = instance->initStat(statIndex, "DatasetReference");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcDlmRetinaTemplate1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcDlmRetinaTemplate1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcDlmRetinaTemplate1();

            status = instance->initStat(statIndex, "RetinaTemplate");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcLogLevelSet1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcLogLevelSet1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcLogLevelSet1();
            status = instance->initStat(statIndex, "SetConfig");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcSetConfig1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcSetConfig1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcSetConfig1();
            status = instance->initStat(statIndex, "LogLevelSet");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcPubTableDlm1:
            instance->twoPcAction_[ii] =
                (TwoPcAction *) memAllocExt(sizeof(TwoPcMsg2pcPubTableDlm1),
                                            moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcPubTableDlm1();

            status = instance->initStat(statIndex, "PubTableDlm");
            BailIfFailed(status);
            break;

        // FIXME: the following need to be removed once they are moved out
        // from TwoPcCallId enum, there is no twoPcAction_ for them either
        case TwoPcCallId::Msg2pcDemystifySourcePage1:
            status = instance->initStat(statIndex, "DemystifyPage");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcProcessScalarPage1:
            status = instance->initStat(statIndex, "ProcessScalarPage");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcIndexPage1:
            status = instance->initStat(statIndex, "IndexPage");
            BailIfFailed(status);
            break;

        case TwoPcCallId::Msg2pcAddOrUpdateUdfDlm1:
            instance->twoPcAction_[ii] = (TwoPcAction *)
                memAllocExt(sizeof(TwoPcMsg2pcAddOrUpdateUdfDlm1), moduleName);
            if (!instance->twoPcAction_[ii]) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->twoPcAction_[ii]) TwoPcMsg2pcAddOrUpdateUdfDlm1();

            status = instance->initStat(statIndex, "UdfAddOrUpdateDlm");
            BailIfFailed(status);
            break;

        default:
            assert(0);
            break;
        }
    }

CommonExit:
    if (status != StatusOk) {
        for (uint32_t jj = (uint32_t) TwoPcCallId::TwoPcCallIdFirstEnum + 1;
             jj < ii;
             jj++) {
            if ((jj == (uint32_t) TwoPcCallId::Msg2pcDemystifySourcePage1) ||
                (jj == (uint32_t) TwoPcCallId::Msg2pcProcessScalarPage1) ||
                (jj == (uint32_t) TwoPcCallId::Msg2pcIndexPage1)) {
                continue;
            }

            instance->twoPcAction_[jj]->~TwoPcAction();
            memFree(instance->twoPcAction_[jj]);
            instance->twoPcAction_[jj] = NULL;
        }
    } else {
#ifdef DEBUG
        // Check that all 2PCs had their stats handle initialized
        for (uint32_t jj = 0; jj < Num2PCs; jj++) {
            assert(instance->numLocal2PCInvoked_[jj] != NULL);
            assert(instance->numLocal2PCFinished_[jj] != NULL);
            assert(instance->numLocal2PCErrors_[jj] != NULL);
            assert(instance->numRemote2PCEntry_[jj] != NULL);
            assert(instance->numRemote2PCFinished_[jj] != NULL);
            assert(instance->numRemote2PCErrors_[jj] != NULL);
        }
#endif  // DEBUG
    }

    return status;
}

Status
TwoPcMgr::initStat(uint32_t statIndex, const char *name)
{
    Status status;

    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initStatHandle(&numLocal2PCInvoked_[statIndex]);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(count2pcInvokedStatGrpId_,
                                         name,
                                         numLocal2PCInvoked_[statIndex],
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numLocal2PCFinished_[statIndex]);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(count2pcFinishedStatGrpId_,
                                         name,
                                         numLocal2PCFinished_[statIndex],
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numLocal2PCErrors_[statIndex]);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(count2pcErrorsStatGrpId_,
                                         name,
                                         numLocal2PCErrors_[statIndex],
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numRemote2PCEntry_[statIndex]);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(countRemote2pcEntryStatGrpId_,
                                         name,
                                         numRemote2PCEntry_[statIndex],
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numRemote2PCFinished_[statIndex]);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(countRemote2pcFinishedStatGrpId_,
                                         name,
                                         numRemote2PCFinished_[statIndex],
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numRemote2PCErrors_[statIndex]);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(countRemote2pcErrorsStatGrpId_,
                                         name,
                                         numRemote2PCErrors_[statIndex],
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    return status;
}

void
TwoPcMgr::destroy()
{
    for (uint32_t ii = (uint32_t) TwoPcCallId::TwoPcCallIdFirstEnum + 1;
         ii < (uint32_t) TwoPcCallId::TwoPcCallIdLastEnum;
         ii++) {
        if (instance->twoPcAction_[ii]) {
            this->twoPcAction_[ii]->~TwoPcAction();
            memFree(this->twoPcAction_[ii]);
            this->twoPcAction_[ii] = NULL;
        }
    }
    instance->~TwoPcMgr();
    memFree(instance);
    instance = NULL;
}

void
TwoPcOp::init(MsgTypeId msgTypeId)
{
    // For a given 2PC type, the failIfResourceShortage_ flag is used in
    // Message.cpp to deliberately fail sending of 2PC or processing of received
    // 2PC on reaching a certain threshold of resource consumption.
    //
    // Note that failIfResourceShortage_ is by default set to false. As far as
    // processing completions are concerned, we need to process these anyway to
    // prevent deadlocks and don't even look at this flag. However if it's a 2PC
    // request, we allow setting of failIfResourceShortage_ to true.
    switch (msgTypeId) {
    case MsgTypeId::MsgNull:
        // None.
        break;

    case MsgTypeId::MsgWaitingOn2pcBarrier:
        // None.
        break;

    case MsgTypeId::Msg2pcBarrier:
        // Barriers are used during cluster boot and shutdown, so don't fail on
        // resource shortage.
        recvDataFromSocket_ = true;
        break;

    case MsgTypeId::Msg2pcBarrierComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcNextResultSet:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcNextResultSetComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcGetDataUsingFatptr:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcGetDataUsingFatptrComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcRpcGetStat:
        recvDataFromSocket_ = true;
        // Allow Get Stats even on resource shortage to get System visibility.
        break;

    case MsgTypeId::Msg2pcRpcGetStatComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcCountResultSetEntries:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcCountResultSetEntriesComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiAggregate:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiAggregateComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiProject:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiProjectComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiFilter:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiFilterComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGroupBy:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGroupByComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiJoin:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiJoinComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGetTableMeta:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGetTableMetaComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiCancel:
        recvDataFromSocket_ = true;
        // Releases resources, so don't fail on resource shortage.
        break;

    case MsgTypeId::Msg2pcXcalarApiCancelComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGetOpStatus:
        recvDataFromSocket_ = true;
        // Pulls Stats. We need this even on resource shortage to get System
        // visibility.
        break;

    case MsgTypeId::Msg2pcXcalarApiGetOpStatusComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcResetStat:
        recvDataFromSocket_ = true;
        // Allow reset stats even if there is resource shortage.
        break;

    case MsgTypeId::Msg2pcResetStatComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiIndex:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiIndexComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiSynthesize:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiSynthesizeComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcGetStatGroupIdMap:
        recvDataFromSocket_ = true;
        // Allow get stat group Id map even on resource shortage
        break;

    case MsgTypeId::Msg2pcGetStatGroupIdMapComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcGetKeysUsingFatptr:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcGetKeysUsingFatptrComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiMap:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiMapComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiUnion:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiUnionComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGetRowNum:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiGetRowNumComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarApiTop:
        recvDataFromSocket_ = true;
        // We need this even when short on resources to gain visibility
        // into the system.
        break;

    case MsgTypeId::Msg2pcGetPerfStats:
        recvDataFromSocket_ = true;
        // We need this even when short on resources to gain visibility
        // into the system.
        failIfResourceShortage_ = false;
        break;

    case MsgTypeId::Msg2pcXcalarApiTopComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXdfParallelDo:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXdfParallelDoComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDropXdb:
        recvDataFromSocket_ = true;
        // We are releasing resource here, so don't fail on resource shortage.
        break;

    case MsgTypeId::Msg2pcDropXdbComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDeserializeXdbPages:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDeserializeXdbPagesComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcSerDesXdb:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcSerDesXdbComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcQueryState:
        recvDataFromSocket_ = true;
        // Stats is a pass through even on resource shortage.
        break;

    case MsgTypeId::Msg2pcQueryStateComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcQueryCancel:
        recvDataFromSocket_ = true;
        // We are releasing resource here, so don't fail on resource shortage.
        break;

    case MsgTypeId::Msg2pcQueryCancelComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcQueryDelete:
        recvDataFromSocket_ = true;
        // We are releasing resource here, so don't fail on resource shortage.
        break;

    case MsgTypeId::Msg2pcQueryDeleteComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXdbLoadDone:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXdbLoadDoneComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDlmResolveKeyType:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDlmResolveKeyTypeComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarAggregateEvalDistributeEvalFn:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcXcalarAggregateEvalDistributeEvalFnComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDlmUpdateKvStore:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDlmUpdateKvStoreComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDatasetReference:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDatasetReferenceComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcPubTableDlm:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcPubTableDlmComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDlmUpdateNs:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDlmUpdateNsComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDlmRetinaTemplate:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDlmRetinaTemplateComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcExportMakeScalarTable:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcExportMakeScalarTableComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcTestImmed:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcTestImmedComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcTestRecv:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcTestRecvComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcGvmBroadcast:
        recvDataFromSocket_ = true;
        // GVM is used for cleanouts too, so don't fail on resource shortage.
        break;

    case MsgTypeId::Msg2pcGvmBroadcastComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcFuncTest:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcFuncTestComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcGetRows:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcGetRowsComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcStream:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcStreamComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcDemystifySourcePage:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcDemystifySourcePageComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcProcessScalarPage:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcProcessScalarPageComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcIndexPage:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcIndexPageComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcLogLevelSet:
        recvDataFromSocket_ = true;
        // Set log level for syslogs, so don't fail on resource shortage
        break;

    case MsgTypeId::Msg2pcLogLevelSetComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcSetConfig:
        recvDataFromSocket_ = true;
        // Sets xcalar config, so don't fail on resource shortage
        break;

    case MsgTypeId::Msg2pcSetConfigComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcGetPerfStatsComplete:
        recvDataFromSocketComp_ = true;
        break;

    case MsgTypeId::Msg2pcAddOrUpdateUdfDlm:
        recvDataFromSocket_ = true;
        failIfResourceShortage_ = true;
        break;

    case MsgTypeId::Msg2pcAddOrUpdateUdfDlmComplete:
        recvDataFromSocketComp_ = true;
        failIfResourceShortage_ = false;
        break;

    default:
        assert(0 && "Missing support for MsgTypeId");
        break;
    }
}

bool
TwoPcOp::getRecvDataFromSocket()
{
    return recvDataFromSocket_;
}

bool
TwoPcOp::getRecvDataFromSocketComp()
{
    return recvDataFromSocketComp_;
}

bool
TwoPcOp::getFailIfResourceShortage()
{
    return failIfResourceShortage_;
}
