// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/OperatorService.h"
#include "operators/Operators.h"
#include "dag/DagLib.h"
#include "libapis/ProtobufUtil.h"

using namespace xcalar::compute::localtypes::Operator;

ServiceAttributes
OperatorService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
OperatorService::mergeSetupHelper(Dag *curDag,
                                  const char *tableName,
                                  DagTypes::NodeId *retDagNodeId,
                                  XdbId *retXdbId,
                                  bool *retDagRefAcquired,
                                  TableNsMgr::TableHandleTrack *retHandleTrack,
                                  LibNsTypes::NsOpenFlags nsOpenFlag)
{
    Status status;
    DgDagState state;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    status = curDag->getDagNodeIds(tableName,
                                   Dag::TableScope::LocalOnly,
                                   retDagNodeId,
                                   retXdbId,
                                   &retHandleTrack->tableId);

    if (status == StatusDagNodeNotFound) {
        status = StatusTableNotFound;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getDagNodeIds tableName %s: %s",
                    tableName,
                    strGetFromStatus(status));

    status = curDag->getDagNodeStateAndRef(*retDagNodeId, &state);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getDagNodeStateAndRef for table %s tableId %ld "
                    "dagNodeId %ld: %s",
                    tableName,
                    retHandleTrack->tableId,
                    *retDagNodeId,
                    strGetFromStatus(status));

    if (state != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getDagNodeStateAndRef for table %s tableId %ld "
                    "dagNodeId %ld state %u: %s",
                    tableName,
                    retHandleTrack->tableId,
                    *retDagNodeId,
                    state,
                    strGetFromStatus(status));
    *retDagRefAcquired = true;

    status = tnsMgr->openHandleToNs(curDag->getSessionContainer(),
                                    retHandleTrack->tableId,
                                    nsOpenFlag,
                                    &retHandleTrack->tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %s %ld: %s",
                    tableName,
                    retHandleTrack->tableId,
                    strGetFromStatus(status));
    retHandleTrack->tableHandleValid = true;

CommonExit:
    return status;
}

void
OperatorService::mergeTeardownHelper(bool *dagRefAcquired,
                                     Dag *curDag,
                                     DagTypes::NodeId dagNodeId,
                                     const char *tableName,
                                     TableNsMgr::TableHandleTrack *handleTrack)
{
    TableNsMgr *tnsMgr = TableNsMgr::get();
    if (*dagRefAcquired) {
        curDag->putDagNodeRefById(dagNodeId);
        *dagRefAcquired = false;
    }

    if (handleTrack->tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack->tableHandle);
        handleTrack->tableHandleValid = false;
    }
}

Status
OperatorService::opMerge(const MergeRequest *request,
                         google::protobuf::Empty *empty)
{
    Status status;
    const xcalar::compute::localtypes::Workbook::WorkbookScope
        *scope[Operators::MergeNumTables] = {NULL};
    Dag *curDag[Operators::MergeNumTables] = {NULL};
    bool trackOpsToSession[Operators::MergeNumTables] = {false};
    bool dagRefAcquired[Operators::MergeNumTables] = {false};
    const char *tableName[Operators::MergeNumTables];
    DagTypes::NodeId dagNodeId[Operators::MergeNumTables];
    TableNsMgr::TableHandleTrack handleTrack[Operators::MergeNumTables];
    XdbId xdbId[Operators::MergeNumTables];
    TableNsMgr::IdHandle *idHandle[Operators::MergeNumTables] = {NULL};
    Stopwatch stopwatch;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;

    // Round robin Merge operator on all the operator runtime schedulers, i.e.
    // Sched0, Sched1 and Sched2.
    Runtime::SchedId rtSchedId = static_cast<Runtime::SchedId>(
        atomicInc64(&mergeOpRoundRobin_) %
        static_cast<int>(Runtime::SchedId::Immediate));
    Txn savedTxn = Txn::currentTxn();
    Txn schedTxn = Txn(savedTxn.id_,
                       savedTxn.mode_,
                       rtSchedId,
                       Runtime::get()->getType(rtSchedId));
    Txn::setTxn(schedTxn);

    tableName[Operators::MergeDeltaTable] = request->delta_table().c_str();
    scope[Operators::MergeDeltaTable] = &request->delta_scope();

    tableName[Operators::MergeTargetTable] = request->target_table().c_str();
    scope[Operators::MergeTargetTable] = &request->target_scope();

    // XXX Todo May be too restrictive!
    if (!strcmp(tableName[Operators::MergeDeltaTable],
                tableName[Operators::MergeTargetTable])) {
        status = StatusInval;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed opMerge for delta table %s, target table %s, since "
                    "delta table and target table have to be different:%s",
                    tableName[Operators::MergeDeltaTable],
                    tableName[Operators::MergeTargetTable],
                    strGetFromStatus(status));

    // For the given delta table,
    // * Get the session scope and track merge op.
    // * Get Dag ref and look up state. Verify if state is ready.
    // * Get read only handle to table name.
    status = protobufutil::
        setupSessionScope(scope[Operators::MergeDeltaTable],
                          &curDag[Operators::MergeDeltaTable],
                          &trackOpsToSession[Operators::MergeDeltaTable]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed setupSessionScope for delta table %s, target table "
                    "%s:%s",
                    tableName[Operators::MergeDeltaTable],
                    tableName[Operators::MergeTargetTable],
                    strGetFromStatus(status));
    trackOpsToSession[Operators::MergeDeltaTable] = true;

    status = mergeSetupHelper(curDag[Operators::MergeDeltaTable],
                              tableName[Operators::MergeDeltaTable],
                              &dagNodeId[Operators::MergeDeltaTable],
                              &xdbId[Operators::MergeDeltaTable],
                              &dagRefAcquired[Operators::MergeDeltaTable],
                              &handleTrack[Operators::MergeDeltaTable],
                              LibNsTypes::ReaderShared);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed on delta table %s getRef and locking, target table "
                    "%s:%s",
                    tableName[Operators::MergeDeltaTable],
                    tableName[Operators::MergeTargetTable],
                    strGetFromStatus(status));

    // For the given target table,
    // * Get the session scope and track merge op.
    // * Get Dag ref and look up state. Verify if state is ready.
    // * Get readWrite handle to table name.
    //
    // Note that the invariant here is that the delta table session and target
    // table session needs to be owned on the same cluster node.
    status = protobufutil::
        setupSessionScope(scope[Operators::MergeTargetTable],
                          &curDag[Operators::MergeTargetTable],
                          &trackOpsToSession[Operators::MergeTargetTable]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed setupSessionScope for target table %s, delta table "
                    "%s:%s",
                    tableName[Operators::MergeTargetTable],
                    tableName[Operators::MergeDeltaTable],
                    strGetFromStatus(status));
    trackOpsToSession[Operators::MergeTargetTable] = true;

    status = mergeSetupHelper(curDag[Operators::MergeTargetTable],
                              tableName[Operators::MergeTargetTable],
                              &dagNodeId[Operators::MergeTargetTable],
                              &xdbId[Operators::MergeTargetTable],
                              &dagRefAcquired[Operators::MergeTargetTable],
                              &handleTrack[Operators::MergeTargetTable],
                              LibNsTypes::WriterExcl);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed on target table %s getRef and locking, delta table "
                    "%s:%s",
                    tableName[Operators::MergeTargetTable],
                    tableName[Operators::MergeDeltaTable],
                    strGetFromStatus(status));

    if (!UserDefinedFunction::
            containersMatch(curDag[Operators::MergeTargetTable]
                                ->getSessionContainer(),
                            curDag[Operators::MergeDeltaTable]
                                ->getSessionContainer())) {
        status = StatusInval;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Workbook scope for delta table %s, target table %s must "
                    "match:%s",
                    tableName[Operators::MergeDeltaTable],
                    tableName[Operators::MergeTargetTable],
                    strGetFromStatus(status));

    idHandle[Operators::MergeDeltaTable] =
        &handleTrack[Operators::MergeDeltaTable].tableHandle;
    idHandle[Operators::MergeTargetTable] =
        &handleTrack[Operators::MergeTargetTable].tableHandle;

    xSyslog(moduleName,
            XlogInfo,
            "Starting merge on target table %s tableId %ld dagNodeId %ld, "
            "delta table %s tableId %ld dagNodeId %ld",
            tableName[Operators::MergeTargetTable],
            handleTrack[Operators::MergeTargetTable].tableId,
            dagNodeId[Operators::MergeTargetTable],
            tableName[Operators::MergeDeltaTable],
            handleTrack[Operators::MergeDeltaTable].tableId,
            dagNodeId[Operators::MergeDeltaTable]);

    stopwatch.restart();
    status = Operators::get()->merge(tableName, xdbId, idHandle);
    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);

    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Merge on target table %s tableId %ld dagNodeId %ld, delta "
                "table %s tableId %ld dagNodeId %ld finished in "
                "%lu:%02lu:%02lu.%03lu",
                tableName[Operators::MergeTargetTable],
                handleTrack[Operators::MergeTargetTable].tableId,
                dagNodeId[Operators::MergeTargetTable],
                tableName[Operators::MergeDeltaTable],
                handleTrack[Operators::MergeDeltaTable].tableId,
                dagNodeId[Operators::MergeDeltaTable],
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);
    } else {
        xSyslog(moduleName,
                XlogInfo,
                "Starting merge on target table %s tableId %ld dagNodeId %ld, "
                "delta table %s tableId %ld dagNodeId %ld failed in "
                "%lu:%02lu:%02lu.%03lu: %s",
                tableName[Operators::MergeTargetTable],
                handleTrack[Operators::MergeTargetTable].tableId,
                dagNodeId[Operators::MergeTargetTable],
                tableName[Operators::MergeDeltaTable],
                handleTrack[Operators::MergeDeltaTable].tableId,
                dagNodeId[Operators::MergeDeltaTable],
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
    }

CommonExit:
    mergeTeardownHelper(&dagRefAcquired[Operators::MergeDeltaTable],
                        curDag[Operators::MergeDeltaTable],
                        dagNodeId[Operators::MergeDeltaTable],
                        tableName[Operators::MergeDeltaTable],
                        &handleTrack[Operators::MergeDeltaTable]);

    if (trackOpsToSession[Operators::MergeDeltaTable]) {
        protobufutil::teardownSessionScope(scope[Operators::MergeDeltaTable]);
        trackOpsToSession[Operators::MergeDeltaTable] = false;
    }

    mergeTeardownHelper(&dagRefAcquired[Operators::MergeTargetTable],
                        curDag[Operators::MergeTargetTable],
                        dagNodeId[Operators::MergeTargetTable],
                        tableName[Operators::MergeTargetTable],
                        &handleTrack[Operators::MergeTargetTable]);

    if (trackOpsToSession[Operators::MergeTargetTable]) {
        protobufutil::teardownSessionScope(scope[Operators::MergeTargetTable]);
        trackOpsToSession[Operators::MergeTargetTable] = false;
    }

    Txn::setTxn(savedTxn);
    return status;
}
