// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "msg/TwoPcFuncDefs.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "usrnode/UsrNode.h"
#include "operators/Operators.h"
#include "operators/Xdf.h"
#include "ns/LibNs.h"
#include "xdb/Xdb.h"
#include "dag/DagLib.h"
#include "operators/XcalarEval.h"
#include "kvstore/KvStore.h"
#include "udf/UserDefinedFunction.h"
#include "querymanager/QueryManager.h"
#include "stat/Statistics.h"
#include "dataset/Dataset.h"
#include "table/ResultSet.h"
#include "usr/Users.h"
#include "util/ProtoWrap.h"
#include "table/Table.h"

// XXX Move the implementations to the respective sub-system and delete this
// file.

void
TwoPcMsg2pcNextResultSet1::schedLocalWork(MsgEphemeral *ephemeral,
                                          void *payload)
{
    ResultSet::getNextRemoteHandler(ephemeral, payload);
}

void
TwoPcMsg2pcNextResultSet1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                void *payload)
{
    ResultSet::getNextRemoteCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcNextResultSet1::recvDataCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    ResultSet::getNextRemoteCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcGetDataUsingFatptr1::schedLocalWork(MsgEphemeral *ephemeral,
                                               void *payload)
{
    usrNodeMsg2pcGetDataUsingFatptr(ephemeral, payload);
}

void
TwoPcMsg2pcGetDataUsingFatptr1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                     void *payload)
{
    assert(0 && "Local path is impossible");
}

void
TwoPcMsg2pcGetDataUsingFatptr1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    usrNodeDoFatPointerWork(ephemeral, payload);
}

void
TwoPcMsg2pcRpcGetStat1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    StatsLib::get()->getStatsLocal(ephemeral);
}

void
TwoPcMsg2pcRpcGetStat1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                             void *payload)
{
    StatsLib::get()->getStatsCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcRpcGetStat1::recvDataCompletion(MsgEphemeral *ephemeral,
                                           void *payload)
{
    StatsLib::get()->getStatsCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcCountResultSetEntries1::schedLocalWork(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    ResultSet::countEntriesLocal(ephemeral, payload);
}

void
TwoPcMsg2pcCountResultSetEntries1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                        void *payload)
{
    ResultSet::countEntriesComplete(ephemeral, payload);
}

void
TwoPcMsg2pcCountResultSetEntries1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                      void *payload)
{
    ResultSet::countEntriesComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiFilter1::schedLocalWork(MsgEphemeral *ephemeral,
                                            void *payload)
{
    Operators::get()->operatorsFilterLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiFilter1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiFilter1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGroupBy1::schedLocalWork(MsgEphemeral *ephemeral,
                                             void *payload)
{
    Operators::get()->operatorsGroupByLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGroupBy1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGroupBy1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiJoin1::schedLocalWork(MsgEphemeral *ephemeral,
                                          void *payload)
{
    Operators::get()->operatorsJoinLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiJoin1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiJoin1::recvDataCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetTableMeta1::schedLocalWork(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    Operators::get()->operatorsGetTableMetaMsgLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetTableMeta1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                        void *payload)
{
    usrNodeGetTableMetaComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetTableMeta1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                      void *payload)
{
    usrNodeGetTableMetaComplete(ephemeral, payload);
}

void
TwoPcMsg2pcLogLevelSet1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    usrNodeMsg2pcLogLevelSet(ephemeral, payload);
}

void
TwoPcMsg2pcLogLevelSet1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    usrNodeMsg2pcLogLevelSetComplete(ephemeral, payload);
}

void
TwoPcMsg2pcLogLevelSet1::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    usrNodeMsg2pcLogLevelSetComplete(ephemeral, payload);
}

void
TwoPcMsg2pcSetConfig1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    usrNodeMsg2pcSetConfig(ephemeral, payload);
}

void
TwoPcMsg2pcSetConfig1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcSetConfig1::recvDataCompletion(MsgEphemeral *ephemeral,
                                          void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcResetStat1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    ResetStatsInput *resetInput = (ResetStatsInput *) payload;
    StatsLib::resetStats(resetInput->resetHwmStats,
                         resetInput->resetCumulativeStats);
}

void
TwoPcMsg2pcResetStat1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
}

void
TwoPcMsg2pcResetStat1::recvDataCompletion(MsgEphemeral *ephemeral,
                                          void *payload)
{
}

void
TwoPcMsg2pcXcalarApiIndex1::schedLocalWork(MsgEphemeral *ephemeral,
                                           void *payload)
{
    Operators::get()->operatorsIndexLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiIndex1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiIndex1::recvDataCompletion(MsgEphemeral *ephemeral,
                                               void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiSynthesize1::schedLocalWork(MsgEphemeral *ephemeral,
                                                void *payload)
{
    Operators::get()->operatorsSynthesizeLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiSynthesize1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                      void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiSynthesize1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                    void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcGetStatGroupIdMap1::schedLocalWork(MsgEphemeral *ephemeral,
                                              void *payload)
{
    StatsLib::get()->getStatsGroupIdMapLocal(ephemeral);
}

void
TwoPcMsg2pcGetStatGroupIdMap1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                    void *payload)
{
    StatsLib::get()->getStatsCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcGetStatGroupIdMap1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    StatsLib::get()->getStatsCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcGetKeysUsingFatptr1::schedLocalWork(MsgEphemeral *ephemeral,
                                               void *payload)
{
    usrNodeMsg2pcGetKeysUsingFatptr(ephemeral, payload);
}

void
TwoPcMsg2pcGetKeysUsingFatptr1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                     void *payload)
{
    DataFormat::get()->getFieldValuesFatptrCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcGetKeysUsingFatptr1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    DataFormat::get()->getFieldValuesFatptrCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiMap1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    Operators::get()->operatorsMapLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiMap1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                               void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiMap1::recvDataCompletion(MsgEphemeral *ephemeral,
                                             void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiUnion1::schedLocalWork(MsgEphemeral *ephemeral,
                                           void *payload)
{
    Operators::get()->operatorsUnionLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiUnion1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiUnion1::recvDataCompletion(MsgEphemeral *ephemeral,
                                               void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiTop1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    StatsLib::get()->top(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiTop1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                               void *payload)
{
    usrNodeTopComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiTop1::recvDataCompletion(MsgEphemeral *ephemeral,
                                             void *payload)
{
    usrNodeTopComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXdfParallelDo1::schedLocalWork(MsgEphemeral *ephemeral,
                                          void *payload)
{
    xdfMsgParallelDo(ephemeral, payload);
}

void
TwoPcMsg2pcXdfParallelDo1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                void *payload)
{
    xdfMsgParallelDoComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXdfParallelDo1::recvDataCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    xdfMsgParallelDoComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiProject1::schedLocalWork(MsgEphemeral *ephemeral,
                                             void *payload)
{
    Operators::get()->operatorsProjectLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiProject1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiProject1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcDropXdb1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    XdbMgr::get()->xdbDropLocal(ephemeral, payload);
}

void
TwoPcMsg2pcDropXdb1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                          void *payload)
{
}

void
TwoPcMsg2pcDropXdb1::recvDataCompletion(MsgEphemeral *ephemeral, void *payload)
{
}

void
TwoPcMsg2pcDeserializeXdbPages1::schedLocalWork(MsgEphemeral *ephemeral,
                                                void *payload)
{
    XdbMgr::get()->xdbDeserializeAllPagesLocal(ephemeral, payload);
}

void
TwoPcMsg2pcDeserializeXdbPages1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                      void *payload)
{
    Status *statusArray = (Status *) ephemeral->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(ephemeral);
    statusArray[dstNodeId] = ephemeral->status;
}

void
TwoPcMsg2pcDeserializeXdbPages1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                    void *payload)
{
    Status *statusArray = (Status *) ephemeral->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(ephemeral);
    statusArray[dstNodeId] = ephemeral->status;
}

void
TwoPcMsg2pcSerializeXdb::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    XdbId xdbId = *(XdbId *) payload;
    ephemeral->status = XdbMgr::get()->xdbSerializeLocal(xdbId);
}

void
TwoPcMsg2pcSerializeXdb::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    Status *statusArray = (Status *) ephemeral->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(ephemeral);
    statusArray[dstNodeId] = ephemeral->status;
}

void
TwoPcMsg2pcSerializeXdb::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    Status *statusArray = (Status *) ephemeral->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(ephemeral);
    statusArray[dstNodeId] = ephemeral->status;
}

void
TwoPcMsg2pcDeserializeXdb::schedLocalWork(MsgEphemeral *ephemeral,
                                          void *payload)
{
    XdbId xdbId = *(XdbId *) payload;
    ephemeral->status = XdbMgr::get()->xdbDeserializeLocal(xdbId);
}

void
TwoPcMsg2pcDeserializeXdb::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                void *payload)
{
    Status *statusArray = (Status *) ephemeral->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(ephemeral);
    statusArray[dstNodeId] = ephemeral->status;
}

void
TwoPcMsg2pcDeserializeXdb::recvDataCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    Status *statusArray = (Status *) ephemeral->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(ephemeral);
    statusArray[dstNodeId] = ephemeral->status;
}

void
TwoPcMsg2pcQueryState1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    QueryManager::get()->requestQueryStateLocal(ephemeral, payload);
}

void
TwoPcMsg2pcQueryState1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                             void *payload)
{
    QueryManager::get()->requestQueryStateCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcQueryState1::recvDataCompletion(MsgEphemeral *ephemeral,
                                           void *payload)
{
    QueryManager::get()->requestQueryStateCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcXdbLoadDone1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    XdbMgr::get()->xdbLoadDoneLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXdbLoadDone1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXdbLoadDone1::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcDlmResolveKeyType1::schedLocalWork(MsgEphemeral *ephemeral,
                                              void *payload)
{
    XdbMgr::get()->xdbResolveKeyTypeDLM(ephemeral, payload);
}

void
TwoPcMsg2pcDlmResolveKeyType1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                    void *payload)
{
    XdbMgr::get()->xdbResolveKeyTypeDLMCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDlmResolveKeyType1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    XdbMgr::get()->xdbResolveKeyTypeDLMCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcExportMakeScalarTable1::schedLocalWork(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    Operators::get()->operatorsExportLocal(ephemeral, payload);
}

void
TwoPcMsg2pcExportMakeScalarTable1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                        void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcExportMakeScalarTable1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                      void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcTwoPcBarrier::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    MsgMgr::get()->setMsgNodeState(ephemeral, payload);
}

void
TwoPcMsg2pcTwoPcBarrier::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcTwoPcBarrier::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1::schedLocalWork(
    MsgEphemeral *ephemeral, void *payload)
{
    XcalarEval::aggregateMsgDistributeEvalFnWrapper(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1::schedLocalCompletion(
    MsgEphemeral *ephemeral, void *payload)
{
    XcalarEval::aggregateMsgPopulateStatusWrapper(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarAggregateEvalDistributeEvalFn1::recvDataCompletion(
    MsgEphemeral *ephemeral, void *payload)
{
    XcalarEval::aggregateMsgPopulateStatusWrapper(ephemeral, payload);
}

void
TwoPcMsg2pcDlmUpdateKvStore1::schedLocalWork(MsgEphemeral *ephemeral,
                                             void *payload)
{
    KvStoreLib::get()->updateDlm(ephemeral, payload);
}

void
TwoPcMsg2pcDlmUpdateKvStore1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    KvStoreLib::get()->dlmUpdateKvStoreCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDlmUpdateKvStore1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    KvStoreLib::get()->dlmUpdateKvStoreCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDlmUpdateNs1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    LibNs::get()->updateDlm(ephemeral, payload);
}

void
TwoPcMsg2pcDlmUpdateNs1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    LibNs::get()->dlmUpdateNsCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDlmUpdateNs1::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    LibNs::get()->dlmUpdateNsCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDatasetReference1::schedLocalWork(MsgEphemeral *ephemeral,
                                             void *payload)
{
    Dataset::get()->dlmDatasetReferenceMsg(ephemeral, payload);
}

void
TwoPcMsg2pcDatasetReference1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    Dataset::get()->dlmDatasetReferenceCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDatasetReference1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    Dataset::get()->dlmDatasetReferenceCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDlmRetinaTemplate1::schedLocalWork(MsgEphemeral *ephemeral,
                                              void *payload)
{
    DagLib::get()->dlmRetinaTemplateMsg(ephemeral, payload);
}

void
TwoPcMsg2pcDlmRetinaTemplate1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                    void *payload)
{
    DagLib::get()->dlmRetinaTemplateCompletion(ephemeral, payload);
}

void
TwoPcMsg2pcDlmRetinaTemplate1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    DagLib::RetinaTemplateMsgResult *output =
        (DagLib::RetinaTemplateMsgResult *) ephemeral->ephemeral;
    DagLib::RetinaTemplateReturnedPayload *retinaTemplateReturnedPayload = NULL;

    output->status = ephemeral->status;
    if (output->status != StatusOk) {
        return;
    }

    switch (output->operation) {
    case DagLib::AddTemplate:
    case DagLib::UpdateTemplate:
    case DagLib::DeleteTemplate:
        // No output returned
        break;
    case DagLib::GetTemplate:
        retinaTemplateReturnedPayload =
            (DagLib::RetinaTemplateReturnedPayload *) payload;

        assert(output->retinaTemplate == NULL);
        output->retinaTemplate =
            memAlloc(retinaTemplateReturnedPayload->templateSize);
        if (output->retinaTemplate == NULL) {
            output->status = StatusNoMem;
        } else {
            output->outputSize = retinaTemplateReturnedPayload->templateSize;
            memcpy(output->retinaTemplate,
                   retinaTemplateReturnedPayload->retinaTemplate,
                   retinaTemplateReturnedPayload->templateSize);
        }
        break;
    default:
        assert(0 && "Developer forgot to add handling");
        break;
    }
}

void
TwoPcMsg2pcXcalarApiAggregate1::schedLocalWork(MsgEphemeral *ephemeral,
                                               void *payload)
{
    Operators::get()->operatorsAggregateLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiAggregate1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                     void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiAggregate1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetRowNum1::schedLocalWork(MsgEphemeral *ephemeral,
                                               void *payload)
{
    Operators::get()->operatorsGetRowNumLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetRowNum1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                     void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetRowNum1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                   void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiCancel1::schedLocalWork(MsgEphemeral *ephemeral,
                                            void *payload)
{
    DagLib::get()->cancelOpLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiCancel1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                  void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiCancel1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                void *payload)
{
    usrNodeStatusArrayComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetOpStatus1::schedLocalWork(MsgEphemeral *ephemeral,
                                                 void *payload)
{
    DagLib::get()->getXcalarApiOpStatusLocal(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetOpStatus1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                                       void *payload)
{
    DagLib::get()->getXcalarApiOpStatusComplete(ephemeral, payload);
}

void
TwoPcMsg2pcXcalarApiGetOpStatus1::recvDataCompletion(MsgEphemeral *ephemeral,
                                                     void *payload)
{
    DagLib::get()->getXcalarApiOpStatusComplete(ephemeral, payload);
}

void
TwoPcMsg2pcQueryCancel1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    QueryManager::get()->requestQueryCancelLocal(ephemeral, payload);
}

void
TwoPcMsg2pcQueryCancel1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    usrNodeStatusComplete(ephemeral, payload);
}

void
TwoPcMsg2pcQueryCancel1::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    usrNodeStatusComplete(ephemeral, payload);
}

void
TwoPcMsg2pcQueryDelete1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    QueryManager::get()->requestQueryDeleteLocal(ephemeral, payload);
}

void
TwoPcMsg2pcQueryDelete1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                              void *payload)
{
    usrNodeStatusComplete(ephemeral, payload);
}

void
TwoPcMsg2pcQueryDelete1::recvDataCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
}

void
TwoPcMsg2pcFuncTest1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    FuncTestDriver::get()->processTwoPcRequest(ephemeral, payload);
}

void
TwoPcMsg2pcFuncTest1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                           void *payload)
{
    FuncTestDriver::get()->processTwoPcComplete(ephemeral, payload);
}

void
TwoPcMsg2pcFuncTest1::recvDataCompletion(MsgEphemeral *ephemeral, void *payload)
{
    FuncTestDriver::get()->processTwoPcComplete(ephemeral, payload);
}

void
TwoPcMsg2pcGetRows1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    Status status = StatusOk;
    XdbGetLocalRowsRequest req;
    ProtoParentChildResponse response;
    size_t size = 0;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    ephemeral->payloadToDistribute = NULL;

    status = pbParseFromArray(&req, payload, ephemeral->payloadLength);
    BailIfFailed(status);

    status = TableMgr::getRowsLocal(&req, &response);
    BailIfFailed(status);

    size = response.ByteSizeLong();
    ephemeral->payloadToDistribute = memAlloc(size);
    BailIfNull(ephemeral->payloadToDistribute);

    status =
        pbSerializeToArray(&response, ephemeral->payloadToDistribute, size);
    BailIfFailed(status);

CommonExit:
    ephemeral->payloadLength = (status == StatusOk) ? size : 0;
    ephemeral->status = status;
}

void
TwoPcMsg2pcGetRows1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                          void *payload)
{
}

void
TwoPcMsg2pcGetRows1::recvDataCompletion(MsgEphemeral *ephemeral, void *payload)
{
    GetRowsOutput *output = (GetRowsOutput *) ephemeral->ephemeral;

    output->status =
        pbParseFromArray(output->response, payload, ephemeral->payloadLength);
}
