// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RESULTSET_H
#define _RESULTSET_H

#include "msg/MessageTypes.h"
#include "demystify/Demystify.h"
#include "SourceTypeEnum.h"
#include "libapis/LibApisCommon.h"
#include "table/TableNs.h"

typedef Xid ResultSetId;
static constexpr const char *RsDefaultConstantKeyName = "constant";

class ResultSet
{
    friend class ResultSetMgr;
    friend class TableResultSet;
    friend class DatasetResultSet;
    friend class AggregateResultSet;

  public:
    ResultSet(ResultSetId rsId,
              SourceType srcType,
              DagTypes::NodeId dagNodeId,
              DagTypes::DagId dagId,
              Xid xid,
              bool errorDs)
    {
        resultSetId_ = rsId;
        srcType_ = srcType;
        dagNodeId_ = dagNodeId;
        xid_ = xid;
        errorDs_ = errorDs;
        dagId_ = dagId;

        memZero(&numEntriesPerNode_, sizeof(numEntriesPerNode_));
        atomicWrite32(&ref_, 1);
    }
    void destroy() { delete this; }
    virtual ~ResultSet();

    ResultSetId getResultSetId() const { return resultSetId_; };

    // return a json array of entries with at most numEntries
    virtual MustCheck Status getNext(uint64_t numEntries,
                                     json_t **entriesArrayOut) = 0;

    MustCheck Status seek(uint64_t position);

    MustCheck Status populateNumEntries(Xid xid, bool errorDs);
    // 2pc funcs
    static void countEntriesLocal(MsgEphemeral *ephemeral, void *payload);
    static void countEntriesComplete(MsgEphemeral *ephemeral, void *payload);
    static void getNextRemoteHandler(MsgEphemeral *ephemeral, void *payload);
    static void getNextRemoteCompletion(MsgEphemeral *ephemeral, void *payload);

    struct CountEntriesEph {
        size_t *numEntriesPerNode;
        Status *nodeStatus;
    };

    size_t numEntries_ = 0;
    // XXX Needs to be a function of numNodes in the cluster instead.
    size_t numEntriesPerNode_[MaxNodes];

    void skipTrackRs() { trackRs_ = false; }

  private:
    SourceType srcType_;
    ResultSetId resultSetId_;
    DagTypes::NodeId dagNodeId_;
    DagTypes::DagId dagId_;
    Xid xid_;
    bool errorDs_;
    TableNsMgr::TableHandleTrack handleTrack_;
    bool trackRs_ = true;
    IntHashTableHook hook_;
    Mutex lock_;

    uint64_t currentEntry_ = 0;
    Atomic32 ref_;

    struct MsgCountInput {
        SourceType srcType;
        Xid xid;
        bool errorDs;
    };

    struct MsgNextInput {
        SourceType srcType;
        Xid xid;
        bool errorDs;

        uint64_t localOffset;
        uint64_t numEntriesRemaining;
    };

    struct MsgNextOutput {
        Status status;
        uint64_t numEntries;
        // json stringified records seperated by '\0'
        char entries[0];
    };

    void getNodeIdAndLocalOffset(NodeId *dstNodeId, uint64_t *localOffset);
    Status getNextRemote(NodeId dstNode,
                         json_t *entries,
                         uint64_t localOffset,
                         uint64_t numEntries,
                         uint64_t *numEntriesAdded);
};

class ResultSetDemystifySend : public DemystifySend<::ResultSetDemystifySend>
{
  public:
    ResultSetDemystifySend(void *localCookie,
                           TransportPage **demystifyPages,
                           XdbId dstXdbId,
                           bool *validIndices,
                           TransportPageHandle *transPageHandle)
        : DemystifySend<::ResultSetDemystifySend>(
              DemystifyMgr::Op::ResultSet,
              localCookie,
              DemystifySend<::ResultSetDemystifySend>::SendFatptrsOnly,
              demystifyPages,
              dstXdbId,
              DfOpRowMetaPtr,
              NewTupleMeta::DfInvalidIdx,
              validIndices,
              transPageHandle,
              NULL,
              NULL)
    {
    }

    MustCheck Status processDemystifiedRow(void *localCookie,
                                           NewKeyValueEntry *srcKvEntry,
                                           DfFieldValue rowMetaField,
                                           Atomic32 *countToDecrement);
};

class TableResultSet : public ResultSet
{
  public:
    struct RowMeta {
        json_t *rowArray;
        uint64_t recNum;
        Atomic32 numIssued;
    };

    TableResultSet(ResultSetId rsId,
                   DagTypes::NodeId dagNodeId,
                   DagTypes::DagId dagId,
                   TableNsMgr::TableHandleTrack handleTrack,
                   Xdb *xdb,
                   XdbMeta *xdbMeta)
        : ResultSet(rsId, SrcTable, dagNodeId, dagId, xdbMeta->xdbId, false),
          xdb_(xdb),
          xdbMeta_(xdbMeta),
          handleTrack_(handleTrack)
    {
    }
    virtual ~TableResultSet();

    Status getNext(uint64_t numEntries, json_t **entriesArrayOut) override;

    Status getByKey(json_t *rows, DfFieldValue key, DfFieldType keyType);

    static Status getNextLocal(json_t *entries,
                               uint64_t localOffset,
                               Xdb *xdb,
                               uint64_t numEntries,
                               uint64_t *numEntriesAdded);

  private:
    Xdb *xdb_;
    XdbMeta *xdbMeta_;
    TableNsMgr::TableHandleTrack handleTrack_;

    static Status demystifyRows(TableCursor *cur,
                                uint64_t numEntries,
                                XdbMeta *srcMeta,
                                XdbId dstXdbId);

    static Status demystifyRowsIntoXdb(Xdb *xdb,
                                       uint64_t localOffset,
                                       uint64_t numEntries,
                                       Xid *xdbIdOut);
};

class DatasetResultSet : public ResultSet
{
  public:
    DatasetResultSet(ResultSetId rsId,
                     DagTypes::NodeId dagNodeId,
                     DagTypes::DagId dagId,
                     DsDataset *ds,
                     DatasetRefHandle refHandle,
                     bool errorDs)
        : ResultSet(rsId,
                    SrcDataset,
                    dagNodeId,
                    dagId,
                    ds->datasetId_,
                    errorDs),
          dataset_(ds),
          refHandle_(refHandle)
    {
    }

    virtual ~DatasetResultSet();

    Status getNext(uint64_t numEntries, json_t **entriesArrayOut) override;

    static Status getNextLocal(json_t *entries,
                               uint64_t localOffset,
                               DataPageIndex *index,
                               uint64_t numEntries,
                               uint64_t *numEntriesAdded);

  private:
    DsDataset *dataset_;
    DatasetRefHandle refHandle_;
};

class AggregateResultSet : public ResultSet
{
  public:
    AggregateResultSet(ResultSetId rsId,
                       DagTypes::NodeId dagNodeId,
                       DagTypes::DagId dagId,
                       TableNsMgr::TableHandleTrack handleTrack,
                       Scalar *scalar)
        : ResultSet(rsId, SrcConstant, dagNodeId, dagId, XidInvalid, false),
          aggVar_(scalar),
          handleTrack_(handleTrack)
    {
    }

    virtual ~AggregateResultSet();

    Status getNext(uint64_t numEntries, json_t **entriesArrayOut) override;

  private:
    Scalar *aggVar_;
    TableNsMgr::TableHandleTrack handleTrack_;
};

class ResultSetMgr
{
  public:
    static constexpr size_t ResultSetNextBufSize = 2 * MB;

    static MustCheck ResultSetMgr *get();
    static MustCheck Status init();
    void destroy();

    MustCheck Status
    makeResultSet(Dag *dag,
                  const char *targetName,
                  bool errorDs,
                  XcalarApiMakeResultSetOutput *makeResultSetOutput,
                  XcalarApiUserId *user);

    void freeResultSet(ResultSetId resultSetId, bool trackRs = true);

    // every get must be followed by a put
    ResultSet *getResultSet(ResultSetId resultSetId);
    static void putResultSet(ResultSet *rs, bool trackRs = true);

  private:
    bool init_ = false;
    static ResultSetMgr *instance;
    static constexpr uint64_t NumHashSlots = 257;
    static constexpr const char *moduleName = "librs";

    Mutex lock_;
    IntHashTable<ResultSetId,
                 ResultSet,
                 &ResultSet::hook_,
                 &ResultSet::getResultSetId,
                 NumHashSlots,
                 hashIdentity>
        rsHashTable_;

    MustCheck Status initInternal();

    // Keep this private, use init instead
    ResultSetMgr() {}

    // Keep this private, use destroy instead
    ~ResultSetMgr() {}

    ResultSetMgr(const ResultSetMgr &) = delete;
    ResultSetMgr &operator=(const ResultSetMgr &) = delete;
};

#endif  // _RESULT_SET_H
