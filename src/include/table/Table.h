// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TABLE_CLASS_H_
#define _TABLE_CLASS_H_

#include "operators/GenericTypes.h"
#include "primitives/Primitives.h"
#include "xdb/TableTypes.h"
#include "xdb/DataModelTypes.h"
#include "xdb/Xdb.h"
#include "xdb/Merge.h"
#include "localmsg/LocalConnection.h"
#include "table/TableNs.h"
#include "xcalar/compute/localtypes/Table.pb.h"
#include "runtime/CondVar.h"

using namespace xcalar::compute::localtypes::Table;
using namespace xcalar::compute::localtypes::ColumnAttribute;

// Table (as TableObj) is an abstraction to represent XDB Table in external
// interfaces. It links Table name with user and user session on one side and
// DAG node and XBD table object on other. This abstraction is used to control
// access to the Table (over TableNsMgr). TableMgr provides management for
// TableObj and interfaces to protobuf Parent/Child. Also, TableObj and TableMgr
// support TableService interface and merge operation. Both, TableMgr and
// TableObj, play service role in merge operation

class TableMgr final
{
  public:
    static MustCheck TableMgr *get();
    static MustCheck Status init();
    void destroy();

    // This structure is used for association between table column name
    // and column type and its order in the table scheme.
    // The structure is used in ColumnInfoTable to store this association
    // XXX: Move hash table to separate class (hash table's 'final' attribute
    //      prevets it), add create/destroy methods to the hash table class.
    class ColumnInfo {
    public:
        StringHashTableHook hook; // Service field to be used in hash table
                                  // (do not use it directly!)
    private:
        uint64_t idxInTable_; // Index of the column (back reference to metadata,
                             // used for verification only)
        char name_[XcalarApiMaxFieldNameLen + 1]; // Column name
        DfFieldType type_; // Column type
    public:

        uint64_t getIdxInTable() const { return idxInTable_; }

        const char *getName() const { return name_; }

        DfFieldType getType() const { return type_; }

        void destroy() { delete this; }

        ColumnInfo(uint64_t idxInTableIn,
                   const char *nameIn,
                   DfFieldType typeIn)
        {
            idxInTable_ = idxInTableIn;
            type_ = typeIn;
            strlcpy(name_, nameIn, sizeof(name_));
        }
    };

    static constexpr unsigned DefaultColumnHashTableSlots = 31;

    // Hash table type to store ColumnInfo and access by name.
    typedef StringHashTable<ColumnInfo,
                            &ColumnInfo::hook,
                            &ColumnInfo::getName,
                            DefaultColumnHashTableSlots>
        ColumnInfoTable;

    // Initilizes ColumnInfoTable object with copy of column information
    // from srcTupNamedMeta
    // XXX May be move to ColumnInfoTable? StringHashTable should not be final.
    static MustCheck Status createColumnInfoTable(
        const NewKeyValueNamedMeta *srcTupNamedMeta, // in: Metadata for table,
                                        // based on which ColumnInfo is created
        ColumnInfoTable *tableOut); // out: pointer to empty hash table
                                    //      to be initialized

    // Clean ColumnInfoTable up.
    static void destroyColumnInfoTable(ColumnInfoTable *table)
    {
        table->removeAll(&ColumnInfo::destroy);
    }

    // Part of API (ParentChild interface through protobuf API).
    // Dispatches external requests. Currently supported messages:
    // 1. XdbGetLocalRows: call is transferred to getRowsLocal or getRowsRemote
    //    Returns data for selected columns from requested rows.
    // 2. XdbGetMeta: call is transferred to getXdbMeta.
    //    Returns list of pairs: column name and column type.
    MustCheck Status onRequestMsg(
        LocalMsgRequestHandler *reqHandler, // Not used
        LocalConnection *connection, // Not used
        const ProtoRequestMsg *request, // Request structure from proto
        ProtoResponseMsg *response); // Response to be returned
                                     // to proto interface

    // Note: Not public
    // Returns rows, stored on this node only. Called from
    // onRequestMsg/TableMgr::TableCursorSchedulable::run only.
    // Eventually, calls getRowsLocal on remote node specified by
    // getRowsRequest->nodeid()
    static MustCheck Status getRowsRemote(
        const XdbGetLocalRowsRequest *getRowsRequest, // Proto message
        ProtoParentChildResponse *parentResponse);    // Proto response

    // Note: Not public, used in Table Messaging stack
    // Called from onRequestMsg and TwoPcMsg2pcGetRows1::schedLocalWork
    // (getRowsRemote path)
    // Retrieves requested rows from current node. Rows are garateed to be
    // located on the current node.
    static MustCheck Status getRowsLocal(
        const XdbGetLocalRowsRequest *getRowsRequest, // Proto message
        ProtoParentChildResponse *parentResponse);  // Proto response

    // Note: Not fully public: Used only in TableNs::addToNs
    // Creates new TableObj for given tableId. The object is created on each
    // node using GVM broadcast. Dag node is associated with the table.
    static MustCheck Status addTableObj(
        TableNsMgr::TableId tableId, // Table ID whch will be associated with
                                     // the TableObj
        const char *fullyQualName, // Fully qualified name of the table
        DagTypes::DagId dagId, // DAG associated with TableObj
        DagTypes::NodeId dagNodeId, // DAG node associated with TableObj
        const XcalarApiUdfContainer *sessionContainer); // Session associated
                                                        // with TableObj.

    // Note: It must be called from TableNs only!!!
    static void removeTableObj(TableNsMgr::TableId tableId);

    // Note: Not public, used in Table Messaging stack
    // Creates and adds TableObj object to current node. Used only by TableGvm
    // as subsequent call from addTableObj.
    MustCheck Status
    addTableObjLocal(TableNsMgr::TableId tableId,
                     DagTypes::DagId dagId,
                     DagTypes::NodeId dagNodeId,
                     const char *fullyQualName,
                     const XcalarApiUdfContainer *sessionContainer);

    // Note: Not public, used in Table Messaging stack
    // Removes TableObject on current node. Used only in TableGvm as subsequent
    // call from removeTableObj
    void removeTableObjLocal(TableNsMgr::TableId tableId);

    // TableObj object
    // Supports information exchange in ParentChild API and assists with
    // Transaction commiting. The object existance is propagated to other nodes
    // through TableNsMgr.
    class TableObj
    {
      public:
        IntHashTableHook idHook_; // Note: Not public
        StringHashTableHook nameHook_; // Note: Not public

        // Note: Not public (accessed from TableMgr)
        TableObj(
            TableNsMgr::TableId tableIdIn, // Table ID created by
                                           // TableNsMgr::genTableId
            DagTypes::DagId dagIdIn, // Dag, associated with the table
            DagTypes::NodeId dagNodeIdIn, // Dag node, associated with the table
            const XcalarApiUdfContainer *sessionContainer) // UDF API container,
                                                    // associated with the table
            : tableId_(tableIdIn), dagId_(dagIdIn), dagNodeId_(dagNodeIdIn)
        {
            UserDefinedFunction::copyContainers(&sessionContainer_,
                                                sessionContainer);
        }
        ~TableObj()
        {
            colTable_.removeAll(&ColumnInfo::destroy);
            imdColTable_.removeAll(&ColumnInfo::destroy);
            if (mergeInfo_ != NULL) {
                delete mergeInfo_;
                mergeInfo_ = NULL;
            }
        }

        TableNsMgr::TableId getId() const { return tableId_; }

        const char *getName() const { return fullyQualName_; }

        void destroy() { delete this; }

        // Returns pointer to MergeInfo, previosly set by setMergeInfo
        // XXX: Used for passing MergeInfo from Operators::mergeInitLocal to
        //      Operators::mergePrepareLocal, Operators::mergePostCommitLocal
        //      Operators::mergeAbortLocal. Should be moved to operation
        //      context.
        MustCheck MergeMgr::MergeInfo *getMergeInfo();

        // Note: called from Operators::mergeInitLocal only to pass MergeInfo
        //       to other stages of merge.
        // Sets pointer to MergeInfo. Must be called only once.
        // Pointer is stored only per node's instance and is not propagated to
        // other nodes.
        void setMergeInfo(MergeMgr::MergeInfo *mergeInfo);

        // Updates column information hash table, based on meta.
        // Should be called once, next calls to this function will be ignored
        // Note: Called from Operators::mergeInitLocal only!
        MustCheck Status initColTable(XdbMeta *meta);

        // Updates Insert-Modify-Delete column information hash table.
        // Should be called once, next calls to this function will be ignored
        // Note: Used to pass IMD column information from
        // Operators::mergeInitLocal to Operators::mergePrepare,
        // Operators::mergePrepareLocal, Operators::mergePostCommitLocal
        // Operators::mergeAbortLocal
        // Assumtion is used: table is created by query or operator and never
        //                    merged-to again.
        MustCheck Status initImdColTable(XdbMeta *meta);

        // Note: Move information set by initColTable on
        // Operators::mergeInitLocal to other merge stages
        // Operators::mergePrepareLocal, Operators::mergePostCommitLocal,
        // Operators::mergeAbortLocal
        // Returns pointer to Column information hash table
        MustCheck ColumnInfoTable *getColTable();

        // Returns pointer to Insert-Modify-Delete Column information hash table
        MustCheck ColumnInfoTable *getImdColTable();

        // Note: Not public (called from TableMgr::addTableObjLocal only)
        MustCheck Status setTableName(const char *fullyQualName);

        // Redirects request to xdb and retrieves table's schema and attributes.
        // Called from TableService::tableMeta and part of interface.
        // XXX: should be 'const', but UserMgr interface prevents it.
        MustCheck Status getTableMeta(TableNsMgr::TableHandleTrack *handleTrack,
                                      Dag *sessionGraph,
                                      TableSchema *tableSchema,
                                      TableAttributes *tableAttrs);

        // Redirects request to Dag and collects aggregated statistics per node
        // and per table. Also redirects request to xdb and retrieves table's
        // schema and attributes.
        MustCheck Status getTableAggrStats(
            Dag *sessionGraph, // Dag session to retrieve aggregated statistics
            TableAggregatedStats *aggrStats) const; // out

        // Collects statistics information from XDB. Subsequently calls
        // Operators::operatorsGetTableMetaMsgLocal on each node.
        // Information is collected per each node.
        MustCheck Status getTablePerNodeStats(
            Dag *sessionGraph, // pointer to Dag
            google::protobuf::Map<std::string, TableStatsPerNode>
                *perNodeStats); // list of statistical information: string is
                // node name in "Node-###" format, where ### is squential node
                // number.

        // Returns dag graph ID
        MustCheck DagTypes::DagId getDagId() { return dagId_; }
        // Returns dag node ID
        MustCheck DagTypes::NodeId getDagNodeId() { return dagNodeId_; }
        // Returns user session reference which holds the table
        MustCheck XcalarApiUdfContainer getSessionContainer()
        {
            return sessionContainer_;
        }

      private:
        XcalarApiUdfContainer sessionContainer_;

        // XXX colTable_ and imdColTable_ are used to pass information
        //     between Operations::merge stages. Should be to be part of merge
        //     operation context.
        ColumnInfoTable colTable_;
        ColumnInfoTable imdColTable_; // imd insert-modify-delete;

        TableNsMgr::TableId tableId_ = TableNsMgr::InvalidTableId;
        char fullyQualName_[LibNsTypes::MaxPathNameLen];
        char tableName_[LibNsTypes::MaxPathNameLen];
        DagTypes::DagId dagId_ = DagTypes::DagIdInvalid;
        DagTypes::NodeId dagNodeId_ = DagTypes::InvalidDagNodeId;
        MergeMgr::MergeInfo *mergeInfo_ = NULL;
    };

    // TODO: 3 blocks
    // Returns previously createred TableObj with specified tableId
    // If the TableObj is not found, returns nullptr.
    MustCheck TableObj *getTableObj(TableNsMgr::TableId tableId);

    // Not used, but keep for completeness
    // Returns previously createred TableObj with specified fully qualified name
    // If the TableObj is not found, returns nullptr.
    MustCheck TableObj *getTableObj(const char *fullyQualName);

    // Note: Not public, Used only in TableNs only
    // Returns tableId of previously createred TableObj with specified fully
    // qualified name. If the table is not found, returns invalid ID
    // (TableNsMgr::InvalidTableId).
    MustCheck TableNsMgr::TableId getTableIdFromName(const char *fullyQualName);

    // Note: Not used, but keep for completeness
    // Returns fully qualified name of the tablem found by table ID
    // XXX should be 'const', but tableLock_ prevent it. The tableLock_ should
    //     be 'mutable'
    MustCheck Status getTableNameFromId(
        TableNsMgr::TableId tableId, // in: ID of Table to serach
        char *retFullyQualName, // in: Pointer buffer,
                                // out: Fully qualified name of the table
        size_t fqnBufLen);      // in: Size of the buffer

    // Finds TableObj by fully qualified name and returns Dag and dag node IDs,
    // User session information and indicator if object is published
    // (retIsGlobal)
    MustCheck Status getDagNodeInfo(
        const char *fullyQualName, // in: fully qualified name of TableObj
        DagTypes::DagId &retDagId, // out: dag ID associated with the TableObj
        DagTypes::NodeId &retDagNodeId,// out: dag node ID associated
                                       //      with the TableObj
        XcalarApiUdfContainer *retSessionContainer, // out: User session
                                                // associated with the TableObj
        bool &retIsGlobal); // out: true - if TableOb was published,
                            //      false -otherwise

    // The object is intended for aggregating of information from Session,
    // Dag and Table.
    struct FqTableState {
        const char *tableName;
        XcalarApiUdfContainer sessionContainer;
        Dag *graph = NULL;
        DagTypes::DagId graphId = DagTypes::DagIdInvalid;
        DagTypes::NodeId nodeId = DagTypes::InvalidDagNodeId;

        // this setup is used when we have sessionGraph in hand and
        // had already increased the ref of its session
        MustCheck Status setUp(const char *srcTableName, Dag *apiSessionGraph);
        MustCheck Status setUp(
            const char *srcTableName,
            const xcalar::compute::localtypes::Workbook::WorkbookScope *scope);
        FqTableState() = default;
        ~FqTableState();

      private:
        bool cleanOut = false;
    };

  private:
    static TableMgr *instance_;
    MustCheck Status initInternal();

    static MustCheck Status
    getRowsAsProto(Xdb *xdb,
                   XdbMeta *xdbMeta,
                   const XdbGetLocalRowsRequest *getRowsRequest,
                   XdbGetLocalRowsResponse *response);

    static MustCheck Status
    getRowsAsDatapage(Xdb *xdb,
                      XdbMeta *xdbMeta,
                      const XdbGetLocalRowsRequest *getRowsRequest,
                      XdbGetLocalRowsResponse *response);

    static MustCheck Status getXdbMeta(const XdbGetMetaRequest *getMetaRequest,
                                ProtoParentChildResponse *parentResponse);

    static constexpr unsigned TableIdHashTableSlots = 13;
    typedef IntHashTable<TableNsMgr::TableId,
                         TableObj,
                         &TableObj::idHook_,
                         &TableObj::getId,
                         TableIdHashTableSlots,
                         hashIdentity>
        TableIdHashTable;
    TableIdHashTable tableIdHashTable_;
    Mutex tableLock_;

    static constexpr unsigned TableNameHashTableSlots = 13;
    typedef StringHashTable<TableObj,
                            &TableObj::nameHook_,
                            &TableObj::getName,
                            TableNameHashTableSlots>
        TableNameHashTable;
    TableNameHashTable tableNameHashTable_;

    Atomic64 tabCursorRoundRobin_;

    // Needs to be local in Table.cpp
    class TableCursorSchedulable : public Schedulable
    {
      public:
        TableCursorSchedulable(const ProtoRequestMsg *request,
                               ProtoParentChildResponse *parentResponse)
            : Schedulable("TableCursoreSchedulable")
        {
            request_ = request;
            parentResponse_ = parentResponse;
        }

        virtual ~TableCursorSchedulable() {}

        Mutex lock_;
        CondVar cv_;
        Status status_;
        bool workDone_ = false;
        const ProtoRequestMsg *request_ = NULL;
        ProtoParentChildResponse *parentResponse_ = NULL;
        virtual void run();
        virtual void done();

        void waitUntilDone()
        {
            lock_.lock();
            while (!workDone_) {
                cv_.wait(&lock_);
            }
            lock_.unlock();
        }
    };

    // Keep this private, use init instead
    TableMgr() {}

    // Keep this private, use destroy instead
    ~TableMgr() {}

    TableMgr(const TableMgr &) = delete;
    TableMgr &operator=(const TableMgr &) = delete;
};

// Note: Not public, used in Table Manager messaging stack
struct GetRowsOutput {
    Status status;
    ProtoParentChildResponse *response;
};

#endif
