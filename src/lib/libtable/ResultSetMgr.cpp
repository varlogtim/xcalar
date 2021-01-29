// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <new>

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "msg/Message.h"
#include "config/Config.h"
#include "usrnode/UsrNode.h"
#include "operators/Operators.h"
#include "operators/GenericTypes.h"
#include "df/DataFormat.h"
#include "xdb/Xdb.h"
#include "dataset/Dataset.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "callout/Callout.h"
#include "table/ResultSet.h"
#include "util/IntHashTable.h"
#include "msg/Xid.h"
#include "constants/XcalarConfig.h"
#include "ns/LibNs.h"
#include "ns/LibNsTypes.h"
#include "strings/String.h"
#include "usr/Users.h"

ResultSetMgr *ResultSetMgr::instance = NULL;

ResultSetMgr *
ResultSetMgr::get()
{
    return instance;
}

Status
ResultSetMgr::init()
{
    assert(instance == NULL);
    instance = (ResultSetMgr *) memAllocExt(sizeof(ResultSetMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) ResultSetMgr();

    return instance->initInternal();
}

Status
ResultSetMgr::initInternal()
{
    Status status = StatusOk;
    init_ = true;

    return status;
}

void
ResultSetMgr::destroy()
{
    init_ = false;
    rsHashTable_.removeAll(&ResultSet::destroy);

    instance->~ResultSetMgr();
    memFree(instance);
    instance = NULL;
}

Status
ResultSetMgr::makeResultSet(Dag *dag,
                            const char *targetName,
                            bool errorDs,
                            XcalarApiMakeResultSetOutput *makeResultSetOutput,
                            XcalarApiUserId *user)
{
    ResultSet *resultSet = NULL;
    Status status;
    ResultSetId resultSetId = XidInvalid;
    DagTypes::NodeId dagNodeId = DagTypes::InvalidDagNodeId;
    DsDataset *dataset = NULL;
    SourceType rsSrcType;
    DsDatasetId datasetId = DsDatasetIdInvalid;
    XdbId xdbId = XdbIdInvalid;
    XcalarApiDagNode *dagNode = NULL;
    bool haveDagRef = false;
    bool haveDsRef = false;
    Dataset *ds = Dataset::get();
    DatasetRefHandle dsRefHandle;
    TableNsMgr::TableHandleTrack handleTrack;
    XcalarApiUdfContainer *sessionContainer = dag->getSessionContainer();
    TableNsMgr *tnsMgr = TableNsMgr::get();
    bool trackRs = false;

    resultSetId = XidMgr::get()->xidGetNext();

    status = dag->getDagNodeId(targetName,
                               Dag::TableScope::FullyQualOrLocal,
                               &dagNodeId);
    if (status == StatusOk) {
        status = dag->getDagNode(dagNodeId, &dagNode);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(dagNode != NULL);

        if (dagNode->hdr.api == XcalarApiBulkLoad) {
            rsSrcType = SrcDataset;
            status = ds->openHandleToDatasetByName(dagNode->input->loadInput
                                                       .datasetName,
                                                   user,
                                                   &dataset,
                                                   LibNsTypes::ReaderShared,
                                                   &dsRefHandle);
            if (status != StatusOk) {
                goto CommonExit;
            }
            datasetId = dataset->getDatasetId();
            haveDsRef = true;
        } else {
            if (dagNode->hdr.api == XcalarApiAggregate) {
                rsSrcType = SrcConstant;
            } else {
                rsSrcType = SrcTable;
            }

            DgDagState dagState;

            status = dag->getDagNodeStateAndRef(dagNodeId, &dagState);
            if (status != StatusOk) {
                status = StatusDgDagNodeNotReady;
                goto CommonExit;
            }
            haveDagRef = true;
            if (dagState != DgDagStateReady) {
                status = StatusDgDagNodeNotReady;
                goto CommonExit;
            }

            status = dag->getTableIdFromNodeId(dagNodeId, &handleTrack.tableId);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed getTableIdFromNodeId for dagNode %lu: %s",
                            dagNodeId,
                            strGetFromStatus(status));

            // Get shared read access to all the operator source tables.
            status = tnsMgr->openHandleToNs(sessionContainer,
                                            handleTrack.tableId,
                                            LibNsTypes::ReaderShared,
                                            &handleTrack.tableHandle,
                                            TableNsMgr::OpenSleepInUsecs);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed to open handle to table %ld: %s",
                            handleTrack.tableId,
                            strGetFromStatus(status));
            handleTrack.tableHandleValid = true;

            status =
                UserMgr::get()->trackResultset(resultSetId,
                                               handleTrack.tableId,
                                               &sessionContainer->userId,
                                               &sessionContainer->sessionInfo);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed to trackResultset rsId %ld tableId %ld: %s",
                            resultSetId,
                            handleTrack.tableId,
                            strGetFromStatus(status));
            trackRs = true;

            xdbId = dag->getXdbIdFromNodeId(dagNodeId);
        }
    } else {
        status = ds->openHandleToDatasetByName(targetName,
                                               user,
                                               &dataset,
                                               LibNsTypes::ReaderShared,
                                               &dsRefHandle);
        if (status == StatusOk) {
            rsSrcType = SrcDataset;
            datasetId = dataset->getDatasetId();
            haveDsRef = true;
        } else {
            goto CommonExit;
        }
    }

    if (!haveDagRef) {
        dagNodeId = DagTypes::InvalidDagNodeId;
    }

    switch (rsSrcType) {
    case SrcConstant: {
        Scalar *scalar;

        status = dag->getScalarResult(dagNodeId, &scalar);
        BailIfFailed(status);

        resultSet = new (std::nothrow) AggregateResultSet(resultSetId,
                                                          dagNodeId,
                                                          dag->getId(),
                                                          handleTrack,
                                                          scalar);
        BailIfNull(resultSet);

        resultSet->numEntries_ = 1;
        break;
    }
    case SrcTable: {
        Xdb *xdb;
        XdbMeta *xdbMeta;
        status = XdbMgr::get()->xdbGet(xdbId, &xdb, &xdbMeta);
        BailIfFailed(status);

        resultSet = new (std::nothrow) TableResultSet(resultSetId,
                                                      dagNodeId,
                                                      dag->getId(),
                                                      handleTrack,
                                                      xdb,
                                                      xdbMeta);
        BailIfNull(resultSet);

        status = resultSet->populateNumEntries(xdbId, errorDs);
        BailIfFailed(status);
        break;
    }
    case SrcDataset: {
        resultSet = new (std::nothrow) DatasetResultSet(resultSetId,
                                                        dagNodeId,
                                                        dag->getId(),
                                                        dataset,
                                                        dsRefHandle,
                                                        errorDs);
        BailIfNull(resultSet);

        status = resultSet->populateNumEntries(datasetId, errorDs);
        BailIfFailed(status);
        break;
    }
    default:
        assert(0);
        status = StatusUnimpl;
        goto CommonExit;
    }

    lock_.lock();
    // Should always work - key is unique XID
    verifyOk(rsHashTable_.insert(resultSet));
    lock_.unlock();

    makeResultSetOutput->numEntries = resultSet->numEntries_;
    makeResultSetOutput->resultSetId = resultSetId;
    status = StatusOk;

CommonExit:
    if (dagNode != NULL) {
        memFree(dagNode);
        dagNode = NULL;
    }

    if (status != StatusOk) {
        if (haveDsRef) {
            Status status2 = ds->closeHandleToDataset(&dsRefHandle);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "Failed Result set init dataset %s close: %s",
                        dataset->name_,
                        strGetFromStatus(status2));
            }
            dataset = NULL;
            datasetId = DsDatasetIdInvalid;
            haveDsRef = false;
        }

        if (resultSet != NULL) {
            // Destructor does decRef on DagNode.
            delete resultSet;
            resultSet = NULL;
            haveDagRef = false;
        }

        if (haveDagRef) {
            assert(dagNodeId != DagTypes::InvalidDagNodeId);
            dag->putDagNodeRefById(dagNodeId);
            haveDagRef = false;
        }

        if (handleTrack.tableHandleValid) {
            tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
            handleTrack.tableHandleValid = false;
        }

        if (trackRs) {
            UserMgr::get()->untrackResultset(resultSetId,
                                             handleTrack.tableId,
                                             &sessionContainer->userId,
                                             &sessionContainer->sessionInfo);
        }
    }

    return status;
}

void
ResultSetMgr::freeResultSet(ResultSetId resultSetId, bool trackRs)
{
    ResultSet *rs;
    lock_.lock();
    rs = rsHashTable_.remove(resultSetId);
    lock_.unlock();
    if (rs) {
        putResultSet(rs, trackRs);  // ref was initialized to 1 on make
    }
}

ResultSet *
ResultSetMgr::getResultSet(ResultSetId resultSetId)
{
    ResultSet *rs;

    lock_.lock();
    rs = rsHashTable_.find(resultSetId);

    if (rs) {
        atomicInc32(&rs->ref_);
    }
    lock_.unlock();

    return rs;
}

void
ResultSetMgr::putResultSet(ResultSet *rs, bool trackRs)
{
    int ref = atomicDec32(&rs->ref_);

    if (ref == 0) {
        if (!trackRs) {
            rs->skipTrackRs();
        }
        delete rs;
    }
}

TableResultSet::~TableResultSet()
{
    TableNsMgr *tnsMgr = TableNsMgr::get();
    if (handleTrack_.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack_.tableHandle);
        if (trackRs_) {
            UserMgr::get()
                ->untrackResultset(resultSetId_,
                                   handleTrack_.tableId,
                                   &handleTrack_.tableHandle.sessionContainer
                                        .userId,
                                   &handleTrack_.tableHandle.sessionContainer
                                        .sessionInfo);
        }
        handleTrack_.tableHandleValid = false;
    }
}

AggregateResultSet::~AggregateResultSet()
{
    TableNsMgr *tnsMgr = TableNsMgr::get();
    if (handleTrack_.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack_.tableHandle);
        if (trackRs_) {
            UserMgr::get()
                ->untrackResultset(resultSetId_,
                                   handleTrack_.tableId,
                                   &handleTrack_.tableHandle.sessionContainer
                                        .userId,
                                   &handleTrack_.tableHandle.sessionContainer
                                        .sessionInfo);
        }
        handleTrack_.tableHandleValid = false;
    }

    Scalar::freeScalar(aggVar_);
}
