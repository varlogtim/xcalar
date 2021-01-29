// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <inttypes.h>
#include <vector> // For test only
#include "util/System.h"
#include "sys/XLog.h"
#include "config/Config.h"
#include "usr/Users.h"
#include "msg/Xid.h"
#include "xdb/Xdb.h"
#include "dag/Dag.h"
#include "dag/DagLib.h"
#include "table/TableNs.h"
#include "table/Table.h"
#include "test/FuncTests/TableTests.h"
#include "LibTableFuncTestConfig.h"

constexpr double pingPeriodSec = 0.05;

static const char *moduleName = "tableTests";

static uint64_t tbNumberOfThreads = 16;
static size_t tbNumberOfPublishingTables = 100;
static size_t tbNumberOfPublishingSessions = 10;
static size_t tbNumberOfStressPublish = 10;
static size_t tbNumberOfOnRequestMsgTables = 100;
static size_t tbNumberOfStressTables = 1000;
static size_t tbNumberOfStressSessions = 10;
static size_t tbNumberOfSanityIterations = 10;

static double StressTimeSec = 12;


////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Table Manager and Table NS Manager related tests

//#define LOCAL_DEBUG

#ifdef LOCAL_DEBUG
#define LOCAL_BAILOUT_RETURN(ret_status, assertMsg) return (ret_status)
#else
#define LOCAL_BAILOUT_RETURN(ret_status, assertMsg) assert(false && assertMsg)
#endif

#define LOCAL_STATUS_BAILOUT(msg, ...) \
    if ((status) != StatusOk) { \
        xSyslog(moduleName, \
                XlogErr, \
                msg ": %s", \
                __VA_ARGS__ \
                strGetFromStatus(status)); \
        LOCAL_BAILOUT_RETURN(status, "Error detected"); \
    }

#define LOCAL_BAILOUT(cond, msg, ...) \
    if (!(cond)) { \
        xSyslog(moduleName, \
                XlogErr, \
                msg, ## __VA_ARGS__); \
        LOCAL_BAILOUT_RETURN(StatusUnknown, "Test failed"); \
    }

struct TableNamesList {
    char **tableNames = nullptr;
    size_t numTables;

    ~TableNamesList() {
        destroy();
    }

    void destroy()
    {
        if (tableNames != nullptr) {
            for (size_t ii = 0; ii < numTables; ii++) {
                delete[] tableNames[ii];
            }
            delete[] tableNames;
            tableNames = nullptr;
        }
    }
};

static void WaitWithFlagPing(volatile bool* stopFlag, double sleepSec)
{
    int pingPeriods = ceil(sleepSec / pingPeriodSec);
    for (int ii = 0; ii < pingPeriods; ii++) {
        sysUSleep(pingPeriodSec * USecsPerSec);
        if (*stopFlag) {
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// class for holding and controlling user and session information
class TestUser {
public:
    const char *getUserName() const
    {
        return userName_.c_str();
    }

    const char *getWorkBookName() const
    {
        return sessionName_.c_str();
    }

    Status
    createUserSession(const char *userName, const char *sessionName)
    {
        int nodeId = Config::get()->getMyNodeId();

        char userNameNode[256];
        snprintf(userNameNode, sizeof(userNameNode), "%snode%04d",
                userName, nodeId);

        char sessionNameNode[256];
        snprintf(sessionNameNode, sizeof(sessionNameNode),
                 "%snode%04d", sessionName, nodeId);

        userName_ = userNameNode;
        sessionName_ = sessionNameNode;

        Status status = StatusOk;
        strlcpy(userId_.userIdName, userNameNode, sizeof(userId_.userIdName));
        userId_.userIdUnique = --uniqueId_;

        status = createSession(&userId_, sessionNameNode);
        LOCAL_STATUS_BAILOUT("Failure in createSession");

        XcalarApiSessionInfoInput sessInfo;
        status = activateSession(&userId_, sessionNameNode);
        LOCAL_STATUS_BAILOUT("Failure in activateSession");

        strlcpy(
            sessInfo.sessionName, sessionNameNode, sizeof(sessInfo.sessionName));
        sessInfo.sessionNameLength = strlen(sessInfo.sessionName);
        sessInfo.sessionId = --uniqueId_;

        status = UserDefinedFunction::initUdfContainer(&sessionContainer_,
                                                       &userId_,
                                                       &sessInfo,
                                                       NULL);
        LOCAL_STATUS_BAILOUT("Failed to create User session");

        return status;
    }

    XcalarApiUdfContainer* getSessionContainer()
    {
        return &sessionContainer_;
    }

    Status cleanUpSession()
    {
        Status status = StatusOk;
        if (sessionContainer_.sessionInfo.sessionNameLength > 0) {
            status = inactivateSession(
                &userId_, sessionContainer_.sessionInfo.sessionName);
            LOCAL_STATUS_BAILOUT("Could not inactivate session");
            status = deleteSession(
                &userId_, sessionContainer_.sessionInfo.sessionName);
            LOCAL_STATUS_BAILOUT("Could not delete session");
            sessionContainer_.sessionInfo.sessionNameLength = 0;
        }
        return status;
    }

    ~TestUser()
    {
        cleanUpSession();
    }

private:
    XcalarApiUdfContainer sessionContainer_;
    XcalarApiUserId userId_;
    static int uniqueId_;
    std::string userName_;
    std::string sessionName_;

    static Status
    activateSession(XcalarApiUserId *userId, const char *sessionName)
    {
        XcalarApiSessionActivateInput sessionActivateInput;
        XcalarApiSessionGenericOutput sessionActivateOutput;
        Status status = StatusUnknown;
        UserMgr *userMgr = UserMgr::get();

        strlcpy(
            sessionActivateInput.sessionName,
            sessionName,
            sizeof(sessionActivateInput.sessionName));
        sessionActivateInput.sessionNameLength
            = strlen(sessionActivateInput.sessionName);
        sessionActivateInput.sessionId = 0;

        status = userMgr->activate(
            userId, &sessionActivateInput, &sessionActivateOutput);
        LOCAL_STATUS_BAILOUT("Could not activate session");

        return status;
    }

    static Status
    createSession(XcalarApiUserId *userId, const char *sessionName)
    {
        XcalarApiSessionNewInput sessionNewInput;
        XcalarApiSessionNewOutput sessionNewOutput;
        Status status = StatusOk;
        UserMgr *userMgr = UserMgr::get();

        strlcpy(
            sessionNewInput.sessionName,
            sessionName,
            sizeof(sessionNewInput.sessionName));

        sessionNewInput.sessionNameLength = strlen(sessionNewInput.sessionName);
        sessionNewInput.fork = false;
        sessionNewInput.forkedSessionNameLength = 0;

        status = userMgr->create(userId, &sessionNewInput, &sessionNewOutput);
        LOCAL_STATUS_BAILOUT("Could not create session");

        xSyslog(moduleName,
                XlogInfo,
                "%s: Session created successfully. SessionId: %lu",
                userId->userIdName,
                sessionNewOutput.sessionId);

        return status;
    }

    static Status
    inactivateSession(XcalarApiUserId *userId, const char *sessionName)
    {
        XcalarApiSessionDeleteInput deleteSessionInput;
        XcalarApiSessionGenericOutput genericSessionOutput;
        memZero(&deleteSessionInput, sizeof(deleteSessionInput));
        Status status = StatusOk;
        UserMgr *userMgr = UserMgr::get();

        // Delete the main session
        strlcpy(
            deleteSessionInput.sessionName,
            sessionName,
            sizeof(deleteSessionInput.sessionName));
        deleteSessionInput.noCleanup = false;
        deleteSessionInput.sessionNameLength
            = strlen(deleteSessionInput.sessionName);

        status = userMgr->inactivate(
            userId, &deleteSessionInput, &genericSessionOutput);
        LOCAL_STATUS_BAILOUT("Could not inactivate session");

        return status;
    }

    static Status
    deleteSession(XcalarApiUserId *userId, const char *sessionName)
    {
        XcalarApiSessionDeleteInput deleteSessionInput;
        XcalarApiSessionGenericOutput genericSessionOutput;
        memZero(&deleteSessionInput, sizeof(deleteSessionInput));
        Status status = StatusOk;
        UserMgr *userMgr = UserMgr::get();

        // Delete the main session
        strlcpy(
            deleteSessionInput.sessionName,
            sessionName,
            sizeof(deleteSessionInput.sessionName));
        deleteSessionInput.noCleanup = false;
        deleteSessionInput.sessionNameLength
            = strlen(deleteSessionInput.sessionName);

        status = userMgr->doDelete(userId,
                                   &deleteSessionInput,
                                   &genericSessionOutput);
        LOCAL_STATUS_BAILOUT("Could not delete session");
        return status;
    }
};

int TestUser::uniqueId_ = 9990;

////////////////////////////////////////////////////////////////////////////////
// class for holding and controlling table related information,
// including references to dag graph, dag node and XDB table.
// When table is created NS table, dag, dag node and XDB table are created
// as well. When table is destroyed, NS table, dag, dag node and XDB table are
// destroyed too
class TestTable {
public:
    struct ColumnInfo {
        std::string name;
        DfFieldType type;
    };
    Status createTable(
        TestUser* pUser,
        const char *namePrefix,
        int idx,
        int keyIdx,
        std::initializer_list<ColumnInfo> columns);

    const char *getName() const { return tableName_; }

    TestUser* getUser() { return pUser_; }
    const TestUser* getUser() const { return pUser_; }

    XdbId getXdbId() const { return xdbId_; }
    DagTypes::DagId getDagId() const { return dagId_; }
    DagTypes::NodeId getDagNodeId() const { return dagNodeId_; }
    TableNsMgr::TableId getTableId() const { return tableId_; }

    int getColumnsNumb() const { return (int)columns_.size(); }
    std::string getColumnName(int idx) const { return columns_[idx].name; }
    DfFieldType getColumnType(int idx) const { return columns_[idx].type; }
    std::string getColumnTypeStr(int idx) const
    {
        return strGetFromDfFieldType(columns_[idx].type);
    }
    int getKeyIdx() const { return keyIdx_; }

    int64_t getRowsNumber() const { return 1; }

    Xdb *getXdb()
    {
        Xdb *xdb = nullptr;
        Status status = XdbMgr::get()->xdbGet(xdbId_, &xdb, nullptr);
        assert(status==StatusOk && "Failure in xdbGet");
        return xdb;
    }

    ~TestTable()
    {
        destroy();
    }

    Status destroy();
private:
    TestUser *pUser_ = nullptr;
    XdbId xdbId_ = XdbIdInvalid;
    std::vector<ColumnInfo> columns_;
    int keyIdx_;
    DagTypes::DagId dagId_ = DagTypes::DagIdInvalid;
    DagTypes::NodeId dagNodeId_ = DagTypes::InvalidDagNodeId;
    TableNsMgr::TableId tableId_ = TableNsMgr::InvalidTableId;
    bool tableInitialized_ = false;

    char tableName_[LibNsTypes::MaxPathNameLen];

    Status createXdb(
        const std::initializer_list<ColumnInfo>& columns,
        int keyIdx);
    Status createDagNode(const char *name);
    Status createNsTable();
};

Status
TestTable::createXdb(
    const std::initializer_list<ColumnInfo>& columns,
    int keyIdx)
{
    columns_ = columns;
    keyIdx_ = keyIdx;
    Status status = StatusOk;
    XdbId local_xdbId = XidMgr::get()->xidGetNext();

    XdbMgr *xdbMgr = XdbMgr::get();

    NewTupleMeta tupMeta;

    std::vector<const char*> immediateNamesPtrs;
    size_t numFields = columns_.size();
    tupMeta.setNumFields(numFields);
    for (size_t ii = 0; ii < numFields; ii++) {
        tupMeta.setFieldType(columns_[ii].type, ii);
        immediateNamesPtrs.emplace_back(columns_[ii].name.c_str());
    }

    status = xdbMgr->xdbCreate(local_xdbId,
                            immediateNamesPtrs[keyIdx],
                            columns_[keyIdx].type,
                            keyIdx,
                            &tupMeta,
                            NULL,
                            0,
                            immediateNamesPtrs.data(),
                            numFields,
                            NULL,
                            0,
                            Unordered,
                            XdbGlobal,
                            DhtInvalidDhtId);
    LOCAL_STATUS_BAILOUT("Failure in xdbCreate");
    xdbId_ = local_xdbId;

    Xdb *xdb = nullptr;
    status = xdbMgr->xdbGet(xdbId_, &xdb, nullptr);
    LOCAL_STATUS_BAILOUT("Failure in xdbGet");

    NewTupleValues valueArray;
    DfFieldValue value;

    const char* buff = "just a test";
    value.stringVal.strActual = buff;
    value.stringVal.strSize = strlen(buff) + 1;

    DfFieldValue key;
    key.int64Val = 1;
    valueArray.set(0, value, DfString);
    valueArray.set(1, key, DfInt64);

    status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
    LOCAL_STATUS_BAILOUT("Failure in xdbInsertKv");

    status = xdbMgr->xdbLoadDone(xdbId_);
    LOCAL_STATUS_BAILOUT("Failure in xdbLoadDone");

    return status;
}

Status
TestTable::createDagNode(const char *name)
{
    Status status = StatusOk;
    DagLib *dagLib = DagLib::get();

    Dag *dag;
    status = dagLib->createNewDag(
        3, DagTypes::WorkspaceGraph, pUser_->getSessionContainer(), &dag);
    LOCAL_STATUS_BAILOUT("Failed to create Dag");
    dagId_ = dag->getId();

    static XcalarApiInput* dummyDagApiInput
        = (XcalarApiInput*)memCalloc(1, sizeof(*dummyDagApiInput));

    status = dag->createNewDagNode(XcalarApiKeyAddOrReplace,
                                   dummyDagApiInput,
                                   (sizeof(((XcalarApiInput *) 0x0)->keyAddOrReplaceInput)),
                                   xdbId_,
                                   tableId_,
                                   (char *) name,
                                   0,
                                   NULL,
                                   NULL,
                                   &dagNodeId_);
    LOCAL_STATUS_BAILOUT("Failed to create Dag node");

    return status;
}

Status
TestTable::createNsTable()
{
    Status status = StatusOk;
    TableNsMgr *tableNsMgr = TableNsMgr::get();

    status = tableNsMgr->addToNs(pUser_->getSessionContainer(),
                                 tableId_,
                                 tableName_,
                                 dagId_,
                                 dagNodeId_);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::addToNs adding table1");
    return status;
}

Status
TestTable::createTable(
        TestUser* pUser,
        const char *namePrefix,
        int idx,
        int keyIdx,
        std::initializer_list<ColumnInfo> columns)
{
    LOCAL_BAILOUT( tableInitialized_==false,
        "Table %s:%d must be initialized only once", namePrefix, idx);

    snprintf(tableName_, LibNsTypes::MaxPathNameLen, "%s%04dnode%d",
        namePrefix, idx, Config::get()->getMyNodeId());

    pUser_ = pUser;
    Status status = StatusOk;
    TableNsMgr *tableNsMgr = TableNsMgr::get();

    LOCAL_BAILOUT(tableNsMgr->isTableIdValid(tableId_)==false,
        "Table should have invalid ID before initialization");
    tableId_ = tableNsMgr->genTableId();
    LOCAL_BAILOUT(tableNsMgr->isTableIdValid(tableId_),
        "Table has invalid ID");

    status = createXdb(columns, keyIdx);
    LOCAL_STATUS_BAILOUT("Can't initialize XDB table");

    status = createDagNode(tableName_);
    LOCAL_STATUS_BAILOUT("Can't initialize Dag node");

    status = createNsTable();
    LOCAL_STATUS_BAILOUT("Can't initialize Table NS");

    tableInitialized_ = true;

    return status;
}


Status TestTable::destroy()
{
    Status status = StatusOk;

    if (dagId_ != DagTypes::DagIdInvalid) {
        DagLib *dagLib = DagLib::get();
        Dag* dag = dagLib->lookupDag(dagId_);
        status = dagLib->destroyDag(dag, DagTypes::DestroyDeleteNodes);
        if (status != StatusOk) {
            xSyslog(moduleName, XlogErr,
                "Can't destory Dag: %s", strGetFromStatus(status));
        }
        dagId_ = DagTypes::DagIdInvalid;
    }

    if (dagNodeId_ != DagTypes::InvalidDagNodeId) {
        // Should be destroyed by destroyDag
        dagNodeId_ = DagTypes::InvalidDagNodeId;
    }

    if (xdbId_ != XdbIdInvalid) {
        XdbMgr *xdbMgr = XdbMgr::get();
        xdbMgr->xdbDrop(xdbId_);
        xdbId_ = XdbIdInvalid;
    }

    if (tableInitialized_) {
        // TODO: Should be destroyed by destroyDag, but it is not!
        TableNsMgr *tableNsMgr = TableNsMgr::get();
        tableNsMgr->removeFromNs(
            pUser_->getSessionContainer(), tableId_, tableName_);
        tableId_ = TableNsMgr::InvalidTableId;
        tableInitialized_ = false;
    }

    return status;
}


class TestTableList {
public:
    TestTableList(size_t tablesCount)
    {
        list_ = new TestTable[tablesCount];
    }

    ~TestTableList()
    {
        delete [] list_;
        list_ = nullptr;
    }

    TestTable* get() { return list_; }

    TestTable& operator[](size_t idx)
    {
        return list_[idx];
    }

    const TestTable& operator[](size_t idx) const
    {
        return list_[idx];
    }
private:
    TestTable* list_ = nullptr;
};

struct TestTheTableConfig {
    bool lockUser = true;
    bool testMeta = true;
    bool testAggStats = true;
    bool testPerNodeStats = true;
    bool logEnabled = true;
};

struct TestTheTable1UserFast: public TestTheTableConfig {
    TestTheTable1UserFast() {
        lockUser = false;
        testMeta = false;
        testAggStats = false;
        testPerNodeStats = false;
        logEnabled = false;
    }
};

struct TestTheTableMUserStress: public TestTheTableConfig {
    TestTheTableMUserStress() {
        lockUser = true;
        testMeta = true;
        testAggStats = true;
        testPerNodeStats = false;
        logEnabled = false;
    }
};

////////////////////////////////////////////////////////////////////////////////
// Plain portion to test majority of interface function for Table object and
// related functions from Table Manager and Table NS Manager.
static Status
TestTheTable(
    TestUser* pUser, // User which owns the table
    TestTable* pTable, // Tested table
    bool isSared, // Expected global status
    bool useScope,// Use Workbook scop to lock user, Dag - otherwise
    TestTheTableConfig config = TestTheTableConfig()
    )
{
    Status status = StatusOk;
    TableNsMgr *tableNsMgr = TableNsMgr::get();
    TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;
    TableNsMgr::TableId invTableId = TableNsMgr::InvalidTableId;

    char newFullyQualName[LibNsTypes::MaxPathNameLen];
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    char tableName[LibNsTypes::MaxPathNameLen];

    status = TableNsMgr::getFQN(newFullyQualName,
                                sizeof(newFullyQualName),
                                pUser->getSessionContainer(),
                                pTable->getName());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::getFQN unprotected");

    status = TableMgr::get()->getTableNameFromId(
        pTable->getTableId(), fullyQualName, sizeof(fullyQualName));
    LOCAL_STATUS_BAILOUT(
        "Failure in TableNsMgr::getTableNameFromId unprotected");

    LOCAL_BAILOUT(strcmp(fullyQualName, newFullyQualName) == 0,
        "TableMgr::FqTableState has wrong Dag ID");

    struct GuardTableHandleTrack: public TableNsMgr::TableHandleTrack {
        TableNsMgr *tableNsMgr;
        GuardTableHandleTrack(TableNsMgr *tableNsMgr):
            tableNsMgr(tableNsMgr)
            {}
        void close()
        {
            if (tableHandleValid) {
                tableNsMgr->closeHandleToNs(&tableHandle);
                tableHandleValid = false;
            }
        }
        ~GuardTableHandleTrack() {
            close();
        }
    } handleTrack(tableNsMgr);
    handleTrack.tableId = pTable->getTableId();

    if (config.lockUser) {
        //Lock the User session container
        TableMgr::FqTableState fqTableState;

        if (useScope) {
            if (isSared) {
                xcalar::compute::localtypes::Workbook::WorkbookScope scope;
                scope.mutable_globl();
                status  = fqTableState.setUp(newFullyQualName, &scope);
                LOCAL_STATUS_BAILOUT(
                    "Failure in TableMgr::FqTableState::setUp (scope, global)");
            } else {
                xcalar::compute::localtypes::Workbook::WorkbookScope scope;
                auto pWbook = scope.mutable_workbook();
                auto pNameSpec = pWbook->mutable_name();
                pNameSpec->set_username(pUser->getUserName());
                pNameSpec->set_workbookname(pUser->getWorkBookName());
                status  = fqTableState.setUp(newFullyQualName, &scope);
                LOCAL_STATUS_BAILOUT(
                    "Failure in TableMgr::FqTableState::setUp (scope, global)");
            }
        }
        else {
            Dag* dag = DagLib::get()->lookupDag(pTable->getDagId());
            status  = fqTableState.setUp(newFullyQualName, dag);
            LOCAL_STATUS_BAILOUT(
                "Failure in TableMgr::FqTableState::setUp (dag)");
        }

        LOCAL_BAILOUT(fqTableState.graphId == pTable->getDagId(),
            "TableMgr::FqTableState has wrong Dag ID");
        LOCAL_BAILOUT(fqTableState.nodeId == pTable->getDagNodeId(),
            "TableMgr::FqTableState has wrong Dag node ID");
        LOCAL_BAILOUT(fqTableState.nodeId == pTable->getDagNodeId(),
            "TableMgr::FqTableState has wrong Dag node ID");
        LOCAL_BAILOUT(UserDefinedFunction::containersMatch(
            &fqTableState.sessionContainer,
            pUser->getSessionContainer()),
            "TableMgr::FqTableState has wrong sessionContainer");

        // TODO request Session container from the Table and compare with User's
        status = tableNsMgr->openHandleToNs(pUser->getSessionContainer(),
                                            handleTrack.tableId,
                                            LibNsTypes::ReaderShared,
                                            &handleTrack.tableHandle,
                                            TableNsMgr::OpenSleepInUsecs);
        LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::openHandleToNs for table");
        handleTrack.tableHandleValid = true;
    } else {
        // TODO request Session container from the Table and compare with User's
        status = tableNsMgr->openHandleToNs(pUser->getSessionContainer(),
                                            handleTrack.tableId,
                                            LibNsTypes::ReaderShared,
                                            &handleTrack.tableHandle,
                                            TableNsMgr::OpenSleepInUsecs);
        LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::openHandleToNs for table");
        handleTrack.tableHandleValid = true;
    }
    TableMgr::TableObj *tableObj
        = TableMgr::get()->getTableObj(handleTrack.tableId);
    LOCAL_BAILOUT(tableObj != nullptr,
        "TableMgr::getTableObj does not return table");

    TableMgr::TableObj *testTableObj
        = TableMgr::get()->getTableObj(fullyQualName);
    LOCAL_BAILOUT(testTableObj != nullptr,
        "TableMgr::getTableObj does not return table");

    LOCAL_BAILOUT(tableObj == testTableObj,
        "versions of TableMgr::getTableObj return different tables");

    tableId = pTable->getTableId();

    LOCAL_BAILOUT(tableObj->getId() == tableId,
        "Table has wrong TableNS ID");
    {
        XcalarApiUdfContainer sessionContainer
            = tableObj->getSessionContainer();
        LOCAL_BAILOUT(
            UserDefinedFunction::containersMatch(
                &sessionContainer, pUser->getSessionContainer()),
            "Table has wrong sessionContainer");
    }

    strncpy(tableName, pTable->getName(), LibNsTypes::MaxPathNameLen);
    fullyQualName[0] = 0;
    status = TableNsMgr::getFQN(fullyQualName,
                                sizeof(fullyQualName),
                                pUser->getSessionContainer(),
                                pTable->getName());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::getFQN protected");
    LOCAL_BAILOUT(
        strncmp(
            newFullyQualName,
            fullyQualName,
            LibNsTypes::MaxPathNameLen
        )==0,
        "Table has wrong FQN name");

    DagTypes::DagId dagId;
    DagTypes::NodeId dagNodeId;
    XcalarApiUdfContainer sessionContainer;
    bool isGlobal = !isSared; // Just in case, if value will not be updated.
    status = TableMgr::get()->getDagNodeInfo(fullyQualName,
                                                dagId,
                                                dagNodeId,
                                                &sessionContainer,
                                                isGlobal);
    LOCAL_STATUS_BAILOUT("Failure in TableMgr::getDagNodeInfo for table");
    LOCAL_BAILOUT(pTable->getDagId() == dagId,
        "TableMgr::getDagNodeInfo returns wrong dagId");
    LOCAL_BAILOUT(pTable->getDagNodeId() == dagNodeId,
        "TableMgr::getDagNodeInfo returns wrong dagNodeId");
    LOCAL_BAILOUT(
        UserDefinedFunction::containersMatch(
            pUser->getSessionContainer(), &sessionContainer),
        "TableMgr::getDagNodeInfo returns wrong sessionContainer");
    LOCAL_BAILOUT(isGlobal == isSared,
        "TableMgr::getDagNodeInfo returns wrong isGlobal");

    Dag* dag = DagLib::get()->lookupDag(dagId);
    LOCAL_BAILOUT(dag != nullptr,
        "DagLib::lookupDag does not return dag");

    if (config.testMeta) {// Metadata test
        TableSchema tableSchema;
        TableAttributes tableAttrs;

        status = tableObj->getTableMeta(&handleTrack,
                                        dag,
                                        &tableSchema,
                                        &tableAttrs);
        LOCAL_STATUS_BAILOUT("Failure in TableObj::getTableMeta");

        if (config.logEnabled) {
            xSyslog(moduleName, XlogInfo,
                "tableSchema.column_attributes.size %d",
                tableSchema.column_attributes().size());

            xSyslog(moduleName, XlogInfo,
                "tableSchema.column_attributes.size %d",
                tableSchema.column_attributes().size());
        }

        LOCAL_BAILOUT(
            tableSchema.column_attributes().size() == pTable->getColumnsNumb(),
            "Table has wrong number of columns: read %u expected %u",
            tableSchema.column_attributes().size(),
            pTable->getColumnsNumb());

        for (int ii = 0; ii < tableSchema.column_attributes().size(); ii++) {

            if (config.logEnabled) {
                xSyslog(moduleName, XlogInfo,
                    ">> tableSchema.column_attributes[%d] name %s type %s value_array_idx %u",
                    ii,
                    tableSchema.column_attributes()[ii].name().c_str(),
                    tableSchema.column_attributes()[ii].type().c_str(),
                    tableSchema.column_attributes()[ii].value_array_idx());
            }

            LOCAL_BAILOUT(
                tableSchema.column_attributes()[ii].name() == pTable->getColumnName(ii),
                "Table has wrong name of column: '%s' expected '%s'",
                tableSchema.column_attributes()[ii].name().c_str(),
                pTable->getColumnName(ii).c_str());

            LOCAL_BAILOUT(
                tableSchema.column_attributes()[ii].type() == pTable->getColumnTypeStr(ii),
                "Table has wrong type of column: %s expected %s",
                tableSchema.column_attributes()[ii].type().c_str(),
                pTable->getColumnTypeStr(ii).c_str());
        }

        if (config.logEnabled) {
            xSyslog(moduleName, XlogInfo,
                "tableSchema.key_attributes.size %d",
                tableSchema.key_attributes().size());
        }

        LOCAL_BAILOUT(tableSchema.key_attributes().size() == 1,
            "Table has wrong number of keys");

        for (int ii = 0; ii < tableSchema.key_attributes().size(); ii++) {
            int kk = pTable->getKeyIdx();

            if (config.logEnabled) {
                xSyslog(moduleName, XlogInfo,
                    ">> tableSchema.key_attributes[%d] name %s ordering %s value_array_idx %u type %s",
                    ii,
                    tableSchema.key_attributes()[ii].name().c_str(),
                    tableSchema.key_attributes()[ii].ordering().c_str(),
                    tableSchema.key_attributes()[ii].value_array_idx(),
                    tableSchema.key_attributes()[ii].type().c_str());
            }

            LOCAL_BAILOUT(
                tableSchema.key_attributes()[ii].name() == pTable->getColumnName(kk),
                "Table has wrong key name");
            LOCAL_BAILOUT(
                tableSchema.key_attributes()[ii].ordering() == "Unordered",
                "Table has wrong key ordering");
            LOCAL_BAILOUT(
                tableSchema.key_attributes()[ii].type() == pTable->getColumnTypeStr(kk),
                "Table has wrong key type");
        }

        if (config.logEnabled) {
            xSyslog(moduleName, XlogInfo,
                "tableAttrs table_id %s xdb_id %" PRId64 " state %s pinned %s shared %s datasets %d result_set_ids %d",
                tableAttrs.table_name().c_str(),
                tableAttrs.xdb_id(),
                tableAttrs.state().c_str(),
                tableAttrs.pinned()?"true":"false",
                tableAttrs.shared()?"true":"false",
                tableAttrs.datasets().size(),
                tableAttrs.result_set_ids().size());
        }

        LOCAL_BAILOUT(tableAttrs.table_name() == pTable->getName(),
            "Table has wrong name");
        LOCAL_BAILOUT(tableAttrs.xdb_id() == pTable->getXdbId(),
            "Table has wrong xdb id");
        LOCAL_BAILOUT(tableAttrs.shared() == isSared,
            "Table has sharing state");
    }
    if (config.testAggStats) { // getTableAggrStats. TODO Needs verification.

        TableAggregatedStats aggregatedStats;

        status = tableObj->getTableAggrStats(dag, &aggregatedStats);
        LOCAL_STATUS_BAILOUT("Failure in TableObj::getTableAggrStats");

        if (config.logEnabled) {
            xSyslog(moduleName, XlogInfo,
                "aggregatedStats total_records_count %" PRIu64 " total_size_in_bytes %" PRIu64 ,
                aggregatedStats.total_records_count(),
                aggregatedStats.total_size_in_bytes());

            std::stringstream sstr;
            for (int ii = 0; ii < aggregatedStats.rows_per_node().size(); ii++) {
                if (ii != 0) sstr << ",";
                sstr << aggregatedStats.rows_per_node(ii);
            }
            xSyslog(moduleName, XlogInfo,
                "aggregatedStats.rows_per_node %d {%s}",
                aggregatedStats.rows_per_node().size(),
                sstr.str().c_str());

            sstr.str("");
            sstr.clear();
            for (int ii = 0; ii < aggregatedStats.size_in_bytes_per_node().size(); ii++)
            {
                if (ii != 0) sstr << ",";
                sstr << aggregatedStats.size_in_bytes_per_node(ii);
            }
            xSyslog(moduleName, XlogInfo,
                "aggregatedStats.size_in_bytes_per_node %d {%s}",
                aggregatedStats.size_in_bytes_per_node().size(),
                sstr.str().c_str());
        }
    }
    if (config.testPerNodeStats) { // getTablePerNodeStats. TODO Needs verification.

        google::protobuf::Map<std::string, TableStatsPerNode> perNodeStats;

        status = tableObj->getTablePerNodeStats(dag, &perNodeStats);
        LOCAL_STATUS_BAILOUT("Failure in TableObj::getTablePerNodeStats");

        if (config.logEnabled) {
            std::stringstream sstr;
            for (auto& it: perNodeStats) {
                auto& node = it.second;
                sstr << "[" << it.first << ":";
                sstr << " status:" << node.status();
                sstr << " num_rows:" << node.num_rows();
                sstr << " num_pages:" << node.num_pages();
                sstr << " num_slots:" << node.num_slots();
                sstr << " size_in_bytes:" << node.size_in_bytes();
                sstr << " rows_per_slot: " << node.rows_per_slot().size() << " {";;
                bool first = true;
                for (auto& iit: node.rows_per_slot()) {
                    if (first == false) sstr << ",";
                    sstr << "[" << iit.first << ":" << iit.second << "]";
                    first = true;
                }
                sstr << "}";
                sstr << " pages_per_slot: " << node.pages_per_slot().size() << " {";
                first = true;
                for (auto& iit: node.pages_per_slot()) {
                    if (first == false) sstr << ",";
                    sstr << "[" << iit.first << ":" << iit.second << "]";
                    first = true;
                }
                sstr << "}";
                sstr << " pages_consumed_in_bytes:"
                        << node.pages_consumed_in_bytes();
                sstr << " pages_allocated_in_bytes:"
                        << node.pages_allocated_in_bytes();
                sstr << " pages_sent:" << node.pages_sent();
                sstr << " pages_received:" << node.pages_received();
                sstr << " ]";
            }
            xSyslog(moduleName, XlogInfo,
                "perNodeStats %zu {%s}",
                perNodeStats.size(),
                sstr.str().c_str());
        }
    }

    bool testGlobal = tableNsMgr->isGlobal(fullyQualName, status);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::isGlobal");
    LOCAL_BAILOUT(testGlobal == isSared,
        "Table has wrong global status");
    bool isFQN = TableNsMgr::isValidFullyQualTableName(fullyQualName);
    LOCAL_BAILOUT(isFQN, "FQN Table name test fails");
    bool isNotFQN = TableNsMgr::isValidFullyQualTableName(tableName) == false;
    LOCAL_BAILOUT(isNotFQN, "None FQN Table name test fails");

    struct TableNameGuard {
        char* newTableName = nullptr;
        ~TableNameGuard() {
            delete [] newTableName;
        }
    } tableNameGuard;
    status = TableNsMgr::getNameFromFqTableName(fullyQualName,
        tableNameGuard.newTableName);

    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::getNameFromFqTableName");
    LOCAL_BAILOUT(
        strncmp(
            tableNameGuard.newTableName,
            tableName,
            LibNsTypes::MaxPathNameLen
        ) == 0,
        "Worng table name");

    LOCAL_BAILOUT(tableNsMgr->isTableIdValid(tableId),
        "Table NS manager isTableIdValid failed with tableID");
    LOCAL_BAILOUT(tableNsMgr->isTableIdValid(invTableId) == false,
        "Table NS manager isTableIdValid failed with invalid tableID");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
// Plain test: analog of tableNamespaceTest
// + testing metadata through TestTheTable

static Status
CheckTablesInList(
    const TableNamesList& tableList,
    const TestTable& table1,
    const TestTable& table2,
    bool shouldPresent,
    XcalarApiUdfContainer* pSessionContainer = nullptr) // means it's global
{
    Status status = StatusOk;

    std::unordered_set<std::string> tableNamesSet;
    std::string tableNamesString;

    std::string table1name, table2name;

    if (pSessionContainer) {
        char fullyQualName[LibNsTypes::MaxPathNameLen];

        status = TableNsMgr::getFQN(fullyQualName,
                                    sizeof(fullyQualName),
                                    pSessionContainer,
                                    table1.getName());
        LOCAL_STATUS_BAILOUT("Can't get FQN for table1");
        table1name = fullyQualName;

        status = TableNsMgr::getFQN(fullyQualName,
                                    sizeof(fullyQualName),
                                    pSessionContainer,
                                    table2.getName());
        LOCAL_STATUS_BAILOUT("Can't get FQN for table2");
        table2name = fullyQualName;
    } else {
        table1name = table1.getName();
        table2name = table2.getName();
    }

    for (size_t ii = 0; ii < tableList.numTables; ++ii) {
        tableNamesSet.insert(tableList.tableNames[ii]);
        tableNamesString.append(tableList.tableNames[ii]);
        tableNamesString.append(" ");
    }

    if (shouldPresent) {
        LOCAL_BAILOUT(tableNamesSet.find(table1name.c_str()) != tableNamesSet.end(),
            "%s is not found", table1name.c_str());
        LOCAL_BAILOUT(tableNamesSet.find(table2name.c_str()) != tableNamesSet.end(),
            "%s is not found", table2name.c_str());
    } else {
        LOCAL_BAILOUT(tableNamesSet.find(table1name.c_str()) == tableNamesSet.end(),
            "%s should not be found", table1name.c_str());
        LOCAL_BAILOUT(tableNamesSet.find(table2name.c_str()) == tableNamesSet.end(),
            "%s should not be found", table2name.c_str());
    }

    xSyslog(moduleName,
            XlogInfo,
            "Flag '%s'. Found the following session tables: %s",
            shouldPresent?"tables should be present":
                          "tables should not be present",
            tableNamesString.c_str());

    return status;
}

static Status
TableAddNsTableTest(TestUser* pUser)
{
    Status status = StatusOk;
    TableNsMgr *tableNsMgr = TableNsMgr::get();

    TestTable table1, table2;

    status = table1.createTable(
        pUser,
        "table",
        1/*table idx*/,
        1/*Index of the key column*/,
        {
            {"f11", DfString},
            {"f12", DfInt64}
        });
    LOCAL_STATUS_BAILOUT("Can't create table1");

    status = table2.createTable(
        pUser,
        "table",
        2/*table idx*/,
        1/*Index of the key column*/,
        {
            {"f21", DfString},
            {"f22", DfInt64}
        });
    LOCAL_STATUS_BAILOUT("Can't create table2");

    TableNamesList tableList;

    status = tableNsMgr->listGlobalTables("*",
                                          tableList.tableNames,
                                          tableList.numTables);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listGlobalTables")

    CheckTablesInList(
        tableList, table1, table2, false, pUser->getSessionContainer());
    LOCAL_STATUS_BAILOUT("Failure in CheckTablesInList");

    tableList.destroy();

    status = tableNsMgr->listSessionTables("*",
                                           tableList.tableNames,
                                           tableList.numTables,
                                           pUser->getSessionContainer());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listSessionTables");

    CheckTablesInList(tableList, table1, table2, true);
    LOCAL_STATUS_BAILOUT("Failure in CheckTablesInList");

    status = TestTheTable(pUser, &table1, false, true);
    LOCAL_STATUS_BAILOUT("Failure in TestTheTable for table1");

    status = TestTheTable(pUser, &table2, false, false);
    LOCAL_STATUS_BAILOUT("Failure in TestTheTable for table2");

    status = tableNsMgr->publishTable(
        pUser->getSessionContainer(), table1.getName());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::publishTable for table1");

    status = tableNsMgr->publishTable(
        pUser->getSessionContainer(), table2.getName());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::publishTable for table2");

    status = TestTheTable(pUser, &table1, true, true);
    LOCAL_STATUS_BAILOUT("Failure in TestTheTable for table1");

    status = TestTheTable(pUser, &table2, true, false);
    LOCAL_STATUS_BAILOUT("Failure in TestTheTable for table2");

    tableList.destroy();

    status = tableNsMgr->listGlobalTables("*",
                                          tableList.tableNames,
                                          tableList.numTables);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listGlobalTables");

    CheckTablesInList(
        tableList, table1, table2, true, pUser->getSessionContainer());
    LOCAL_STATUS_BAILOUT("Failure in CheckTablesInList");

    status = tableNsMgr->unpublishTable(
        pUser->getSessionContainer(),
        table1.getName());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::unpublishTable for table1");
    status = tableNsMgr->unpublishTable(
        pUser->getSessionContainer(),
        table2.getName());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::unpublishTable for table2");

    tableList.destroy();

    status = tableNsMgr->listGlobalTables("*",
                                          tableList.tableNames,
                                          tableList.numTables);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listGlobalTables");

    CheckTablesInList(
        tableList, table1, table2, false, pUser->getSessionContainer());
    LOCAL_STATUS_BAILOUT("Failure in CheckTablesInList");

    status = table1.destroy();
    LOCAL_STATUS_BAILOUT("Failure in TestTable::destroy table1");
    status = table2.destroy();
    LOCAL_STATUS_BAILOUT("Failure in TestTable::destroy table2");

    tableList.destroy();

    status = tableNsMgr->listSessionTables("*",
                                           tableList.tableNames,
                                           tableList.numTables,
                                           pUser->getSessionContainer());
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listSessionTables");

    CheckTablesInList(tableList, table1, table2, false);
    LOCAL_STATUS_BAILOUT("Failure in CheckTablesInList");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Stress test to run TestTheTable mutiple times in multi-thread environment
struct OpenCloseThreadInfo {
    uint32_t idx;
    TestTable* pTable;
    TestUser* pUser;
    pthread_t threadId;
    int count;
    Status status;
    volatile const bool* stopFlag;
};

static void *
StressTableOpenClose(void *threadIndexIn)
{
    OpenCloseThreadInfo* threadInfo = (OpenCloseThreadInfo *) threadIndexIn;
    int count = 0;
    try {
        do {
            count++;
            Status status = TestTheTable(
                threadInfo->pUser,
                threadInfo->pTable,
                false,
                (threadInfo->idx & 1) == 0,// Use scope for even threads
                TestTheTable1UserFast());

            if (status != StatusOk) {
                threadInfo->status = status;
                xSyslog(moduleName, XlogErr,
                    "Error in TestTheTable stress: %s",
                    strGetFromStatus(status));
                break;
            }
        } while(*threadInfo->stopFlag == false);
    } catch(...) {
        xSyslog(moduleName, XlogErr, "Exception in TestTheTable stress");
        assert(false && "Exception in TestTheTable stress");
    }
    threadInfo->count = count;
    return nullptr;
}

// Runs many threads refer to one table.
// Each thread runs open/get metadata/close operation concurrently.
static Status
StressTableAddNsMetaTest(TestUser* pUser, double sleepSec)
{
    Status status = StatusOk;
    int ret;
    volatile bool stopFlag = false;
    OpenCloseThreadInfo threads[tbNumberOfThreads];

    TestTable table;

    status = table.createTable(
        pUser,
        "table",
        1/*table idx*/,
        1/*Index of the key column*/,
        {
            {"f11", DfString},
            {"f12", DfInt64}
        });
    LOCAL_STATUS_BAILOUT(
        "Failure in StressTableAddNsMetaTest to create stress table");

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        threads[ii].idx = ii;
        threads[ii].stopFlag = &stopFlag;
        threads[ii].pUser = pUser;
        threads[ii].status = StatusOk;
        threads[ii].count = 0;
        threads[ii].pTable = &table;
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        threads[ii].idx = ii;
        status = Runtime::get()->createBlockableThread(&threads[ii].threadId,
                                                       NULL,
                                                       StressTableOpenClose,
                                                       &threads[ii]);
        LOCAL_STATUS_BAILOUT("createBlockableThread failed");
    }

    sysSleep(ceil(sleepSec));
    stopFlag = true;

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        ret = sysThreadJoin(threads[ii].threadId, NULL);
    }

    bool threadCountsAreOK = true;
    int totalCount = 0;
    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        totalCount += threads[ii].count;
        if (threads[ii].count <= 0) {
            xSyslog(moduleName, XlogErr,
                "Thread %d has worng count: %d",
                ii,
                threads[ii].count);
            threadCountsAreOK = false;
        }
        xSyslog(moduleName, XlogInfo,
            "TestTheTable was called %d times in thread %d",
            threads[ii].count,
            ii);

        if (threads[ii].status != StatusOk) {
            status = threads[ii].status;
            xSyslog(moduleName, XlogErr,
                "Thread %d failed with status: %s",
                ii,
                strGetFromStatus(status));
        }
    }
    LOCAL_STATUS_BAILOUT("One or more threads failed");
    LOCAL_BAILOUT(threadCountsAreOK,
        "One or more threads have 0 couns");

    xSyslog(moduleName, XlogInfo,
        "TestTheTable was called %d times", totalCount);

    status = table.destroy();
    LOCAL_STATUS_BAILOUT("Failure in TestTable to destroy stress table");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Random table test.
// Each table goes through steps:
// 1. Generate new Table ID
// 2. Add Table to Name space
// 3. Publish the table
// 4. Check if the table is in global list
// 5. Unpublish the table
// 6. Check if the table is in global list
// 7. Repeat steps 3-6 10 time
// 8. Remove table from NS
// 9. Start over from 1
// Each step is performed by separate thread, but not concurrently.
// Each thread:
// 1. Select table randomly
// 2. if the table is already under processing: go to 1
// 3. Process next step for the table
// 4. Release table processing status
// 5. If there is no stop flag: Start over with 1.
//
struct TestTablePublishing {
    enum class State {
        genId,
        addNS,
        publish,
        checkGlobal,
        unpublish,
        checkLocal,
        removeNs,
        invalid
    };

    // pSession is created and assigned only once and never changed.
    Status Start(const char *name, XcalarApiUdfContainer* pSession)
    {
        Status status = StatusOk;

        atomicWrite32(&busyFlag_, 0);
        pSessionContainer_ = pSession;
        tableName_ = name;
        tableId_ = TableNsMgr::InvalidTableId;
        state_ = State::genId; // Start from generating ID
        count_ = 0;
        iterCount_  = 0;
        errorStatus_ = StatusOk;
        memZero(totalTime_, sizeof(totalTime_));
        memZero(totalCount_, sizeof(totalCount_));

        char fullyQualName[LibNsTypes::MaxPathNameLen];
        status = TableNsMgr::getFQN(fullyQualName,
                                    sizeof(fullyQualName),
                                    pSessionContainer_,
                                    tableName_.c_str());
        LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::getFQN pablishing stress");
        tableFQName_ = fullyQualName;
        return status;
    }

    bool tryLock()
    {
        return atomicXchg32(&busyFlag_, 1) == 0;
    }

    void release()
    {
        atomicWrite32(&busyFlag_, 0);
    }

    TableNsMgr::TableId tableId() const
    {
        return tableId_;
    }

    void SetErrorStatus(Status status)
    {
        errorStatus_ = status;
    }

    Status errorStatus() const
    {
        return errorStatus_;
    }

    uint64_t count() const
    {
        return count_;
    }

    int64_t totalTime(size_t state) const
    {
        return totalTime_[state];
    }

    int64_t totalCount(size_t state) const
    {
        return totalCount_[state];
    }

    Status process();

    Status processGenId();
    Status processAddNS();
    Status processPublish();
    Status processCheckGlobal();
    Status processUnpublish();
    Status processCheckLocal();
    Status processRemoveNs();
private:
    Atomic32 busyFlag_;
    XcalarApiUdfContainer* pSessionContainer_;
    std::string tableName_;
    std::string tableFQName_;
    TableNsMgr::TableId tableId_;
    State state_;
    uint64_t count_; // Total number of iterations
    int iterCount_; // Inner iteration count
    Status errorStatus_;
    int64_t totalTime_[(int)State::invalid];
    int64_t totalCount_[(int)State::invalid];
};

Status TestTablePublishing::process()
{
    struct timespec timeStart;
    struct timespec timeEnd;
    struct timespec elapsed;
    int oldState = (int)state_;

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    Status status = StatusOk;
    switch(state_) {
    case State::genId:
        status = processGenId();
        break;
    case State::addNS:
        status = processAddNS();
        break;
    case State::publish:
        status = processPublish();
        break;
    case State::checkGlobal:
        status = processCheckGlobal();
        break;
    case State::unpublish:
        status = processUnpublish();
        break;
    case State::checkLocal:
        status = processCheckLocal();
        break;
    case State::removeNs:
        status = processRemoveNs();
        break;
    case State::invalid:
    default:
        xSyslog(moduleName, XlogErr,
            "TestTablePublishing::process detects unknow state code %d",
            (int)state_);
        assert(false && "Unknown table test state code");
    }
    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    elapsed.tv_sec = timeEnd.tv_sec - timeStart.tv_sec;
    elapsed.tv_nsec = timeEnd.tv_nsec - timeStart.tv_nsec;

    int64_t workTime = elapsed.tv_sec * NSecsPerSec + elapsed.tv_nsec;
    totalTime_[oldState] += workTime;
    totalCount_[oldState]++;

    return status;
}

Status TestTablePublishing::processGenId()
{
    Status status = StatusOk;
    state_ = State::invalid;

    LOCAL_BAILOUT(TableNsMgr::get()->isTableIdValid(tableId_)==false,
        "Table ID should have invalid ID before initialization");
    tableId_ = TableNsMgr::get()->genTableId();
    LOCAL_BAILOUT(TableNsMgr::get()->isTableIdValid(tableId_),
        "Expected valid table ID");

    state_ = State::addNS;
    return status;
}

Status TestTablePublishing::processAddNS()
{
    Status status = StatusOk;
    state_ = State::invalid;
    status = TableNsMgr::get()->addToNs(pSessionContainer_,
                                        tableId_,
                                        tableName_.c_str(),
                                        DagTypes::DagIdInvalid,
                                        DagTypes::InvalidDagNodeId);
    LOCAL_STATUS_BAILOUT("Add NS table failed");

    iterCount_ = tbNumberOfStressPublish;
    state_ = State::publish;
    return status;
}

Status TestTablePublishing::processPublish()
{
    Status status = StatusOk;
    state_ = State::removeNs;

    status = TableNsMgr::get()->publishTable(
        pSessionContainer_, tableName_.c_str());
    LOCAL_STATUS_BAILOUT("Publish NS table failed");

    state_ = State::checkGlobal;
    return status;
}

Status TestTablePublishing::processCheckGlobal()
{
    Status status = StatusOk;
    state_ = State::removeNs;

    TableNamesList tableList;

    status = TableNsMgr::get()->listGlobalTables("*",
                                                 tableList.tableNames,
                                                 tableList.numTables);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listGlobalTables")

    bool found = false;
    for (size_t ii = 0; ii < tableList.numTables; ++ii) {
        if (tableFQName_ == tableList.tableNames[ii]) {
            found = true;
        }
    }
    LOCAL_BAILOUT(found, "Global table is not found");

    state_ = State::unpublish;
    return status;
}

Status TestTablePublishing::processUnpublish()
{
    Status status = StatusOk;
    state_ = State::removeNs;

    status = TableNsMgr::get()->unpublishTable(
        pSessionContainer_, tableName_.c_str());
    LOCAL_STATUS_BAILOUT("Publish NS table failed");

    state_ = State::checkLocal;
    return status;
}

Status TestTablePublishing::processCheckLocal()
{
    Status status = StatusOk;
    state_ = State::removeNs;

    TableNamesList tableList;

    status = TableNsMgr::get()->listSessionTables("*",
                                                  tableList.tableNames,
                                                  tableList.numTables,
                                                  pSessionContainer_);
    LOCAL_STATUS_BAILOUT("Failure in TableNsMgr::listSessionTables");

    bool found = false;
    for (size_t ii = 0; ii < tableList.numTables; ++ii) {
        if (tableName_ == tableList.tableNames[ii]) {
            found = true;
        }
    }
    LOCAL_BAILOUT(found, "Local table is not found");

    if (--iterCount_) {
        state_ = State::publish;
    }
    return status;
}

Status TestTablePublishing::processRemoveNs()
{
    state_ = State::invalid;

    TableNsMgr::get()->removeFromNs(
        pSessionContainer_, tableId_, tableName_.c_str());

    tableId_ = TableNsMgr::InvalidTableId;
    state_ = State::genId; // start over with new table
    count_++;

    return StatusOk;
}

struct PublishingThreadInfo {
    int idx;
    pthread_t threadId;
    TestTablePublishing *tableArray;
    volatile const bool* stopFlag;
    volatile bool* errorFlag;
    uint64_t loops;
    uint64_t steps;
};

static void *
StressTablePublishing(void *threadIndexIn)
{
    PublishingThreadInfo* threadInfo = (PublishingThreadInfo*) threadIndexIn;
    static thread_local std::default_random_engine generator(
        777 + threadInfo->idx);
    std::uniform_int_distribution<int> distribution(0,
        tbNumberOfPublishingTables-1);

    TestTablePublishing *tableArray = threadInfo->tableArray;

    while (*threadInfo->stopFlag == false) {
        threadInfo->loops++;
        int index = distribution(generator);
        if (tableArray[index].tryLock()) {
            threadInfo->steps++;
            Status status = StatusOk;
            status = tableArray[index].process();
            if (status != StatusOk) {
                // It's an emergency: do not process the table anymore,
                // keep it busy. Report error status to host and continue.
                tableArray[index].SetErrorStatus(status);
                *threadInfo->errorFlag = true;
            } else {
                // Release the table
                tableArray[index].release();
            }
        }
    }
    return nullptr;
}

// Each thread runs add/delete NS table publish/unpublish and get table list
static Status
StressTablePublishingTest(double sleepSec)
{
    Status status = StatusOk;
    int ret;
    volatile bool stopFlag = false;
    volatile bool errorFlag = false;
    PublishingThreadInfo threads[tbNumberOfThreads];

    class UserInfo {
    public:
        void init(int idx)
        {
            Config *config = Config::get();

            snprintf(userId_.userIdName,
                     sizeof(userId_.userIdName),
                     "stressUser%04dn%04d",
                     idx,
                     config->getMyNodeId());

            userId_.userIdUnique = XidMgr::get()->xidGetNext();

            snprintf(sessInfo_.sessionName,
                     sizeof(sessInfo_.sessionName),
                     "stressSession%04dnode%04d",
                     idx,
                     config->getMyNodeId());

            sessInfo_.sessionNameLength = strlen(sessInfo_.sessionName);
            sessInfo_.sessionId = XidMgr::get()->xidGetNext();

            verifyOk(UserDefinedFunction::initUdfContainer(
                                                        &sessionContainer_,
                                                        &userId_,
                                                        &sessInfo_,
                                                        NULL));
        }

        XcalarApiUdfContainer* getSessionContainer()
        {
            return &sessionContainer_;
        }
    private:
        XcalarApiUserId userId_;
        XcalarApiSessionInfoInput sessInfo_;
        XcalarApiUdfContainer sessionContainer_;
    };

    UserInfo users[tbNumberOfPublishingSessions];

    for (uint32_t ii = 0; ii < tbNumberOfPublishingSessions; ++ii) {
        users[ii].init(ii);
    }

    class PublisingTables {
    public:
        PublisingTables()
        {
            list_ = new TestTablePublishing[tbNumberOfPublishingTables];
        }
        ~PublisingTables()
        {
            delete [] list_;
            list_ = nullptr;
        }

        TestTablePublishing* get() { return list_; }

        TestTablePublishing& operator[](size_t idx)
        {
            return list_[idx];
        }

        const TestTablePublishing& operator[](size_t idx) const
        {
            return list_[idx];
        }
    private:
        TestTablePublishing* list_;
    } tables;

    for (uint32_t ii = 0; ii < tbNumberOfPublishingTables; ++ii) {
        char TableName[32];
        snprintf(TableName, sizeof(TableName), "stressTable%04d", (int)ii);
        tables[ii].Start(
            TableName,
            users[ii % tbNumberOfPublishingSessions].getSessionContainer());
        LOCAL_STATUS_BAILOUT("Failure in stress Table initialization");
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        threads[ii].idx = ii;
        threads[ii].tableArray = tables.get();
        threads[ii].stopFlag = &stopFlag;
        threads[ii].errorFlag = &errorFlag;
        threads[ii].loops = 0;
        threads[ii].steps = 0;
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        status = Runtime::get()->createBlockableThread(&threads[ii].threadId,
                                                       NULL,
                                                       StressTablePublishing,
                                                       &threads[ii]);
        LOCAL_STATUS_BAILOUT("createBlockableThread failed");
    }

    WaitWithFlagPing(&errorFlag, sleepSec);

    stopFlag = true;

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        ret = sysThreadJoin(threads[ii].threadId, NULL);
    }

    // Check is there was an error
    int totalCount = 0;
    constexpr int StateCount = (int)TestTablePublishing::State::invalid;
    int64_t sumWorkTime[StateCount] = {};
    int64_t cntWorkTime[StateCount] = {};
    for (uint32_t ii = 0; ii < tbNumberOfPublishingTables; ++ii) {
        totalCount += tables[ii].count();
        for (int state = 0; state < StateCount; state++) {
            sumWorkTime[state] += tables[ii].totalTime(state);
            cntWorkTime[state] += tables[ii].totalCount(state);
        }
        if (tables[ii].errorStatus() != StatusOk) {
            status = tables[ii].errorStatus();
            xSyslog(moduleName, XlogErr,
                "Table test %d failed with status: %s",
                ii,
                strGetFromStatus(status));
        }
        if (tables[ii].tableId() != TableNsMgr::InvalidTableId)
        {
            Status status = tables[ii].processRemoveNs();
            LOCAL_STATUS_BAILOUT("processRemoveNs failed on post processing");
        }
    }
    LOCAL_STATUS_BAILOUT("One or more threads failed");

    for (int state = 0; state < StateCount; state++) {
        double av = cntWorkTime[state]?
            (double)sumWorkTime[state] / cntWorkTime[state]:
            0.0;
        xSyslog(moduleName, XlogInfo,
            "Table publishing state %d ran %ld times, %f msec",
            state,
            (long)cntWorkTime[state],
            av/NSecsPerMSec);
    }

    xSyslog(moduleName, XlogInfo,
        "Table publishing test ran through %d tables", totalCount);

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Plain OnRequestMsg (GetMeta message) test
static Status
TestOnRequestMsgGetMeta(TestTable* pTable)
{
    Status status = StatusOk;

    ProtoRequestMsg reqMsg;
    ProtoParentRequest* parent = reqMsg.mutable_parent();
    parent->set_func(ParentFuncXdbGetMeta);
    ParentAppRequest* appReq = parent->mutable_app();
    XdbGetMetaRequest* xdbGetMeta = appReq->mutable_xdbgetmeta();

    xdbGetMeta->set_xdbid(pTable->getXdbId());

    ProtoResponseMsg respMsg;
    status = TableMgr::get()->onRequestMsg(nullptr, nullptr, &reqMsg, &respMsg);
    LOCAL_STATUS_BAILOUT("Failure in TableMgr::onRequestMsg");

    LOCAL_BAILOUT(respMsg.has_parentchild(),
        "message response has no parentchild field");
    LOCAL_BAILOUT(respMsg.parentchild().has_xdbgetmeta(),
        "message response has no xdbgetmeta field");

    const XdbGetMetaResponse& rsp = respMsg.parentchild().xdbgetmeta();
    xSyslog(moduleName, XlogInfo,
        "onRequestMsg returns xdbid %" PRId64 " numrowspernode %d columns %d",
        rsp.xdbid(),
        rsp.numrowspernode().size(),
        rsp.columns().size());

    std::stringstream sstr;
    int64_t totalRows = 0;
    for(int ii = 0; ii < rsp.numrowspernode().size(); ii++) {
        if (ii != 0) sstr << ", ";
        sstr << rsp.numrowspernode(ii);
        totalRows += rsp.numrowspernode(ii);
    }
    xSyslog(moduleName, XlogInfo,
        "numrowspernode %d: {%s} total %" PRId64 " rows",
        rsp.numrowspernode().size(),
        sstr.str().c_str(), totalRows);
    LOCAL_BAILOUT(totalRows == pTable->getRowsNumber(),
        "Table has wrong number of rows");

    sstr.str("");
    sstr.clear();
    for(int ii = 0; ii < rsp.columns().size(); ii++) {
        if (ii != 0) sstr << ", ";
        sstr << "[" << rsp.columns(ii).name() << "," << rsp.columns(ii).type() << "]";
        LOCAL_BAILOUT(rsp.columns(ii).name() == pTable->getColumnName(ii),
            "Table has wrong name of column");
        LOCAL_BAILOUT(rsp.columns(ii).type() == pTable->getColumnTypeStr(ii),
            "Table has wrong type of column");
    }
    xSyslog(moduleName, XlogInfo,
        "columns %d: {%s}",
        rsp.columns().size(),
        sstr.str().c_str());

    return status;
}

static Status
TestOnRequestMsgGetMetaPlain(TestUser* pUser)
{
    Status status = StatusOk;

    TestTable table;
    status = table.createTable(
        pUser,
        "tableGerMeta",
        1/*table idx*/,
        1/*Index of the key column*/,
        {
            {"ff1", DfString},
            {"ff2", DfInt64}
        });
    LOCAL_STATUS_BAILOUT("Failure in TestTable to create message table");

    status = TestOnRequestMsgGetMeta(&table);
    LOCAL_STATUS_BAILOUT("Failure in TestOnRequestMsgGetMeta");

    status = table.destroy();
    LOCAL_STATUS_BAILOUT("Failure in TestTable to destroy stress table");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
// Plain OnRequestMsg (GetLocalRows message) test
static Status
TestOnRequestMsgGetLocalRows(TestTable* pTable)
{
    Status status = StatusOk;

    ProtoRequestMsg reqMsg;

    ProtoParentRequest* parent = reqMsg.mutable_parent();
    parent->set_func(ParentFuncXdbGetLocalRows);
    ParentAppRequest* appReq = parent->mutable_app();
    XdbGetLocalRowsRequest* xdbGetLocalRows = appReq->mutable_xdbgetlocalrows();

    xdbGetLocalRows->set_xdbid(pTable->getXdbId());
    xdbGetLocalRows->set_startrow(0);
    xdbGetLocalRows->set_numrows(10);
    xdbGetLocalRows->set_nodeid(Config::get()->getMyNodeId());
    xdbGetLocalRows->set_asdatapage(false);
    for (int ii = 0; ii < pTable->getColumnsNumb(); ii++) {
        xdbGetLocalRows->add_columns(pTable->getColumnName(ii));
    }

    ProtoResponseMsg respMsg;
    status = TableMgr::get()->onRequestMsg(nullptr, nullptr, &reqMsg, &respMsg);
    LOCAL_STATUS_BAILOUT("Failure in TableMgr::onRequestMsg");

    LOCAL_BAILOUT(respMsg.has_parentchild(),
        "message response has no parentchild field");
    LOCAL_BAILOUT(respMsg.parentchild().has_xdbgetlocalrows(),
        "message response has no xdbgetlocalrows field");
    LOCAL_BAILOUT(respMsg.parentchild().xdbgetlocalrows().has_rows(),
        "message response has no rows field");

    const XdbGetLocalRowsResponse& rsp = respMsg.parentchild().xdbgetlocalrows();
    const XdbGetLocalRowsResponse::RowBatch& rows = rsp.rows();
    xSyslog(moduleName, XlogInfo,
        "onRequestMsg returns rows %d", rows.rows().size());

    for (int ii = 0; ii < rows.rows().size(); ii++) {
        std::stringstream sstr;
        const ProtoRow& row = rows.rows(ii);
        for (auto& it: row.fields()) {
            sstr << "(" << it.first << ":";
            switch (it.second.dataValue_case()) {
            case ProtoFieldValue::kStringVal:
                sstr << it.second.stringval().c_str();
                break;
            case ProtoFieldValue::kInt64Val:
                sstr << it.second.int64val();
                break;
            default:
                xSyslog(moduleName, XlogErr,
                    "wrong it.second.dataValue_case() %d",
                    (int)it.second.dataValue_case());
                LOCAL_BAILOUT(false,
                    "Wrong column type has been recieved");
            }
            sstr << ")";
        }
        xSyslog(moduleName, XlogInfo,
            "row %d; fields %zu '%s'",
            ii,
            row.fields().size(),
            sstr.str().c_str());
    }

    return status;
}

static Status
TestOnRequestMsgGetLocalRowsPlain(TestUser* pUser)
{
    Status status = StatusOk;

    TestTable table;
    status = table.createTable(
        pUser,
        "tableGetLocalRows",
        2/*table idx*/,
        1/*Index of the key column*/,
        {
            {"ff1", DfString},
            {"ff2", DfInt64}
        });
    LOCAL_STATUS_BAILOUT("Failure in TestTable to create message table");

    status = TestOnRequestMsgGetLocalRows(&table);
    LOCAL_STATUS_BAILOUT("Failure in TestOnRequestMsgGetLocalRows");

    status = table.destroy();
    LOCAL_STATUS_BAILOUT("Failure in TestTable to destroy stress table");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Stress OnRequestMsg (GetMeta and GetLocalRows messages) test

struct OnRequestMsgThreadInfo {
    int threadIdx;
    pthread_t threadId;
    TestTable *tableArray;
    volatile const bool* stopFlag;
    volatile bool* errorFlag;
    uint64_t loops;
    int64_t countGetMeta;
    int64_t timeGetMeta;
    int64_t countGetLocalRows;
    int64_t timeGetLocalRows;
    Status status;

    void Init(
        TestTable *tableArray,
        volatile const bool* stopFlag,
        volatile bool* errorFlag)
    {
        this->tableArray = tableArray;
        this->stopFlag = stopFlag;
        this->errorFlag = errorFlag;
        loops = 0;
        countGetMeta = 0;
        timeGetMeta = 0;
        countGetLocalRows = 0;
        timeGetLocalRows = 0;
        status = StatusOk;
    }
};

static void *
StressTableOnRequestMsg(void *threadIndexIn)
{
    OnRequestMsgThreadInfo* threadInfo
        = (OnRequestMsgThreadInfo*) threadIndexIn;
    static thread_local std::default_random_engine generator(
        555 + threadInfo->threadIdx);
    std::uniform_int_distribution<int> distribution(0,
        tbNumberOfOnRequestMsgTables-1);
    std::uniform_int_distribution<int> bin(0,1);

    TestTable *tableArray = threadInfo->tableArray;

    while (*threadInfo->stopFlag == false) {
        threadInfo->loops++;
        int index = distribution(generator);
        int func = bin(generator);
        TestTable *pTable = tableArray + index;

        Status status = StatusOk;

        struct timespec timeStart;
        struct timespec timeEnd;
        struct timespec elapsed;

        clock_gettime(CLOCK_MONOTONIC, &timeStart);

        if (func) {
            status = TestOnRequestMsgGetMeta(pTable);
            threadInfo->countGetMeta++;
        } else {
            status = TestOnRequestMsgGetLocalRows(pTable);
            threadInfo->countGetLocalRows++;
        }

        clock_gettime(CLOCK_MONOTONIC, &timeEnd);
        elapsed.tv_sec = timeEnd.tv_sec - timeStart.tv_sec;
        elapsed.tv_nsec = timeEnd.tv_nsec - timeStart.tv_nsec;

        int64_t workTime = elapsed.tv_sec * NSecsPerSec + elapsed.tv_nsec;
        if (func) {
            threadInfo->timeGetMeta += workTime;
        } else {
            threadInfo->timeGetLocalRows += workTime;
        }

        if (status != StatusOk) {
            xSyslog(moduleName, XlogErr,
                "Table OnRequestMsg test %d failed with status: %s",
                threadInfo->threadIdx,
                strGetFromStatus(status));
            threadInfo->status = status;
            *threadInfo->errorFlag = true;
        }
    }
    return nullptr;
}

static Status
StressTableOnRequestMsgTest(TestUser* pUser, double sleepSec)
{
    Status status = StatusOk;
    int ret;
    volatile bool stopFlag = false;
    volatile bool errorFlag = false;
    OnRequestMsgThreadInfo threads[tbNumberOfThreads];

    TestTableList tables(tbNumberOfOnRequestMsgTables);

    for (uint32_t ii = 0; ii < tbNumberOfOnRequestMsgTables; ++ii) {
        char TableName[32];
        snprintf(TableName, sizeof(TableName), "stressTable%04d", (int)ii);
        status = tables[ii].createTable(
            pUser,
            TableName,
            ii+1000/*table idx*/,
            1/*Index of the key column*/,
            {
                {"FF1", DfString},
                {"FF2", DfInt64}
            });
        LOCAL_STATUS_BAILOUT("Failure in stress Table initialization");
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        threads[ii].Init(tables.get(), &stopFlag, &errorFlag);
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        status = Runtime::get()->createBlockableThread(
            &threads[ii].threadId,
            NULL,
            StressTableOnRequestMsg,
            &threads[ii]);
        LOCAL_STATUS_BAILOUT("createBlockableThread failed");
    }

    WaitWithFlagPing(&errorFlag, sleepSec);

    stopFlag = true;

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        ret = sysThreadJoin(threads[ii].threadId, NULL);
    }

    // Check is there was an error
    int totalCount = 0;
    int64_t sumWorkTimeGetMeta = 0;
    int64_t cntWorkTimeGetMeta = 0;
    int64_t sumWorkTimeGetLocalRows = 0;
    int64_t cntWorkTimeGetLocalRows = 0;
    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        totalCount += threads[ii].loops;
        sumWorkTimeGetMeta += threads[ii].timeGetMeta;
        cntWorkTimeGetMeta += threads[ii].countGetMeta;
        sumWorkTimeGetLocalRows += threads[ii].timeGetLocalRows;
        cntWorkTimeGetLocalRows += threads[ii].countGetLocalRows;
        if (threads[ii].status != StatusOk) {
            status = threads[ii].status;
            xSyslog(moduleName, XlogErr,
                "Table test thread %d failed with status: %s",
                ii,
                strGetFromStatus(status));
        }
    }
    LOCAL_STATUS_BAILOUT("One or more threads failed");

    double av = cntWorkTimeGetMeta ?
        (double)sumWorkTimeGetMeta / cntWorkTimeGetMeta:
        0.0;
    xSyslog(moduleName, XlogInfo,
        "Table OnRequestMsg GetMeta ran %ld time, average %f msec",
        (long)cntWorkTimeGetMeta,
        av/NSecsPerMSec);

    av = cntWorkTimeGetLocalRows ?
        (double)sumWorkTimeGetLocalRows / cntWorkTimeGetLocalRows:
        0.0;
    xSyslog(moduleName, XlogInfo,
        "Table OnRequestMsg GetLocalRows ran %ld times, average %f msec",
        (long)cntWorkTimeGetLocalRows,
        av/NSecsPerMSec);

    xSyslog(moduleName, XlogInfo,
        "Table OnRequestMsg test ran through %d tables", totalCount);

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Operation merge related tests.
// Methods are not thread safe and there is no reason for stress test.
// Indirectly tests TableMgr::ColumnInfoTable

static Status
CheckTableColumnInfo(TestTable* pTable, TableMgr::ColumnInfoTable *colInfo)
{
    Status status = StatusOk;
    struct ColumnInfoTest {
        std::string name;
        uint64_t idx;
        DfFieldType type;

        ColumnInfoTest(uint64_t idx, std::string name, DfFieldType type):
            name(name), idx(idx), type(type) {}

        ColumnInfoTest(const TableMgr::ColumnInfo *imdColInfo):
            ColumnInfoTest(
                imdColInfo->getIdxInTable(),
                imdColInfo->getName(),
                imdColInfo->getType()
            ) {}

        bool operator == (const ColumnInfoTest& other) const
        {
            return other.name == name
                && other.idx == idx
                && other.type == type;
        }

        bool operator < (const ColumnInfoTest& other) const
        {
            return other.idx < idx;
        }
    };
    std::vector<ColumnInfoTest> fromColumns;
    std::vector<ColumnInfoTest> fromTable;

    for (auto iter = colInfo->begin(); iter.get() != NULL; iter.next()) {
        TableMgr::ColumnInfo *imdColInfo = iter.get();
        fromColumns.emplace_back(imdColInfo);
    }

    for (int ii = 0; ii < pTable->getColumnsNumb(); ii++) {
        fromTable.emplace_back(
            ii, pTable->getColumnName(ii), pTable->getColumnType(ii));
    }

    std::sort(fromColumns.begin(), fromColumns.end());
    std::sort(fromTable.begin(), fromTable.end());

    LOCAL_BAILOUT(fromTable.size() == fromColumns.size(),
        "Original table info size does not match to column info size");

    for (size_t ii = 0; ii < fromColumns.size(); ii++) {
        LOCAL_BAILOUT(fromTable[ii] == fromColumns[ii],
            "Original table info does not match to column info");
    }

    return status;
}

struct TestXdbMeta {
    XdbMeta* xdbMeta = nullptr;
    TestXdbMeta(const TestTable* pTable)
    {
        // XdbMeta and NewTupleMeta have no proper constructors:
        // use this strange way to insure zeroing fields

        xdbMeta = (XdbMeta *)new char[sizeof(XdbMeta)];
        memZero(xdbMeta, sizeof(XdbMeta));
        new (xdbMeta) XdbMeta;

        NewKeyValueNamedMeta* tupNamedMeta = &xdbMeta->kvNamedMeta;

        NewTupleMeta *tupMeta = (NewTupleMeta *)new char[sizeof(NewTupleMeta)];
        memZero(tupMeta, sizeof(NewTupleMeta));
        new (tupMeta) NewTupleMeta;
        tupNamedMeta->kvMeta_.tupMeta_ = tupMeta;

        unsigned numCols = pTable->getColumnsNumb();
        tupMeta->setNumFields(numCols);

        for (unsigned ii = 0; ii < numCols; ii++) {
            tupMeta->setFieldType( pTable->getColumnType(ii), ii);
            strncpy(tupNamedMeta->valueNames_[ii],
                    pTable->getColumnName(ii).c_str(),
                    sizeof(tupNamedMeta->valueNames_[ii]));
        }
    }

    ~TestXdbMeta() {
        delete xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
        delete xdbMeta;
    }
};

static Status
TestMergeFunctionsPlain(TestUser* pUser)
{
    Status status = StatusOk;

    TestTable tgtTable;
    TestTable imdTable;

    status = tgtTable.createTable(
        pUser,
        "tgtTable",
        11/*table idx*/,
        1/*Index of the key column*/,
        {
            {"fff1", DfString},
            {"fff2", DfInt64},
        });
    LOCAL_STATUS_BAILOUT("Failure in TestTable to create message table");

    status = imdTable.createTable(
        pUser,
        "imdTable",
        11/*table idx*/,
        1/*Index of the key column*/,
        {
            {"fff1", DfString},
            {"fff2", DfInt64},
        });
    LOCAL_STATUS_BAILOUT("Failure in TestTable to create message table");

    TableMgr::TableObj *tableObj
        = TableMgr::get()->getTableObj(tgtTable.getTableId());
    LOCAL_BAILOUT(tableObj != nullptr,
        "TableMgr::getTableObj does not return table");

    MergeMgr::MergeInfo* pMergeInfo = new MergeMgr::MergeInfo;

    tableObj->setMergeInfo(pMergeInfo);
    MergeMgr::MergeInfo* minfo1 = tableObj->getMergeInfo();
    LOCAL_BAILOUT(minfo1 == pMergeInfo,
        "TableMgr::TableObj::getMergeInfo returns wrong data (1)");

    TestXdbMeta trgMeta(&tgtTable);

    status = tableObj->initColTable(trgMeta.xdbMeta);
    LOCAL_STATUS_BAILOUT("Failure in initImdColTable TargetXdbMeta");

    TestXdbMeta imdMeta(&imdTable);

    status = tableObj->initImdColTable(imdMeta.xdbMeta);
    LOCAL_STATUS_BAILOUT("Failure in initImdColTable ImdCommittedXdbMeta");

    TableMgr::ColumnInfoTable *colTgt = tableObj->getColTable();
    LOCAL_BAILOUT(colTgt != nullptr,
        "TableMgr::getColTable does not return column table");

    status = CheckTableColumnInfo(&tgtTable, colTgt);
    LOCAL_STATUS_BAILOUT("Failure in CheckTableColumnInfo (target)");

    TableMgr::ColumnInfoTable *colDel = tableObj->getImdColTable();
    LOCAL_BAILOUT(colDel != nullptr,
        "TableMgr::getColTable does not return column table");

    status = CheckTableColumnInfo(&imdTable, colDel);
    LOCAL_STATUS_BAILOUT("Failure in CheckTableColumnInfo (delta)");

    MergeMgr::MergeInfo* minfo2 = tableObj->getMergeInfo();
    LOCAL_BAILOUT(minfo2 == pMergeInfo,
        "TableMgr::TableObj::getMergeInfo returns wrong data (2)");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// ColumnInfoTable tests
struct TestTupNamedMeta {
    NewKeyValueNamedMeta* tupNamedMeta = nullptr;
    TestTupNamedMeta(
        const std::vector<std::string>& names,
        const std::vector<DfFieldType>& types)
    {
        assert(names.size() == types.size());
        // NewKeyValueNamedMeta has no proper constructors:
        // use this strange way to insure zeroing fields

        tupNamedMeta =
            (NewKeyValueNamedMeta *)new char[sizeof(NewKeyValueNamedMeta)];
        memZero(tupNamedMeta, sizeof(NewKeyValueNamedMeta));
        new (tupNamedMeta) NewKeyValueNamedMeta;

        NewTupleMeta *tupMeta = (NewTupleMeta *)new char[sizeof(NewTupleMeta)];
        memZero(tupMeta, sizeof(NewTupleMeta));
        new (tupMeta) NewTupleMeta;
        tupNamedMeta->kvMeta_.tupMeta_ = tupMeta;

        unsigned numCols = types.size();
        tupMeta->setNumFields(numCols);

        for (unsigned ii = 0; ii < numCols; ii++) {
            tupMeta->setFieldType( types[ii], ii);
            strncpy(tupNamedMeta->valueNames_[ii],
                    names[ii].c_str(),
                    sizeof(tupNamedMeta->valueNames_[ii]));
        }
    }

    ~TestTupNamedMeta() {
        delete tupNamedMeta->kvMeta_.tupMeta_;
        delete tupNamedMeta;
    }
};

static Status
TestColumnInfoPlain()
{
    Status status = StatusOk;
    std::vector<std::string> names;
    std::vector<DfFieldType> types;
    names.reserve(TupleMaxNumValuesPerRecord);
    types.reserve(TupleMaxNumValuesPerRecord);

    constexpr unsigned DfFieldType_ARRAYSIZE = ((unsigned)DfMoney+1);

    for (unsigned ii = 0; ii < TupleMaxNumValuesPerRecord; ii++) {
        char name[32];
        snprintf(name, sizeof(name), "testName%04u", ii);
        names.emplace_back(name);
        types.emplace_back((DfFieldType)(ii % DfFieldType_ARRAYSIZE));
    }
    TestTupNamedMeta tupNamedMeta(names, types);

    TableMgr::ColumnInfoTable tableOut;

    status = TableMgr::createColumnInfoTable(
        tupNamedMeta.tupNamedMeta, &tableOut);
    LOCAL_STATUS_BAILOUT("TableMgr::createColumnInfoTable failed");

    for (unsigned ii = 0; ii < TupleMaxNumValuesPerRecord; ii++) {
        TableMgr::ColumnInfo* info = tableOut.find(names[ii].c_str());

        LOCAL_BAILOUT(info != nullptr,
            "TableMgr::ColumnInfoTable can't find %s", names[ii].c_str());

        LOCAL_BAILOUT(info->getIdxInTable() == ii,
            "TableMgr::ColumnInfoTable column %s has wrong type %d (expected %d)",
            names[ii].c_str(),
            (int)info->getIdxInTable(),
            (int)ii);

        LOCAL_BAILOUT(info->getType() == types[ii],
            "TableMgr::ColumnInfoTable column %s has wrong type %d (expected %d)",
            names[ii].c_str(),
            (int)info->getType(),
            (int)types[ii]);
    }

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Multi-table stress test based on TestTheTable

struct MultiTableThreadInfo {
    int threadIdx;
    pthread_t threadId;
    TestTable *tableArray;
    volatile const bool* stopFlag;
    volatile bool* errorFlag;
    uint64_t loops;
    Status status;

    void Init(
        TestTable *tableArray,
        volatile const bool* stopFlag,
        volatile bool* errorFlag)
    {
        this->tableArray = tableArray;
        this->stopFlag = stopFlag;
        this->errorFlag = errorFlag;
        loops = 0;
        status = StatusOk;
    }
};

static void *
StressMultiTable(void *threadIndexIn)
{
    MultiTableThreadInfo* threadInfo
        = (MultiTableThreadInfo*) threadIndexIn;
    static thread_local std::default_random_engine generator(
        555 + threadInfo->threadIdx);
    std::uniform_int_distribution<int> distribution(
        0, tbNumberOfStressTables-1);
    std::uniform_int_distribution<int> bin(0,1);

    TestTable *tableArray = threadInfo->tableArray;

    while (*threadInfo->stopFlag == false) {
        threadInfo->loops++;
        int index = distribution(generator);

        TestTable *pTable = tableArray + index;

        bool useScope = bin(generator) == 1;

        Status status = StatusOk;

        status = TestTheTable(
            pTable->getUser(),
            pTable,
            false,
            useScope,
            TestTheTableMUserStress());

        if (status != StatusOk) {
            xSyslog(moduleName, XlogErr,
                "Table Multi Table test %d failed with status: %s",
                threadInfo->threadIdx,
                strGetFromStatus(status));
            threadInfo->status = status;
            *threadInfo->errorFlag = true;
        }
    }
    return nullptr;
}

static Status
StressMultiTableTest(double sleepSec)
{
    Status status = StatusOk;
    int ret;
    volatile bool stopFlag = false;
    volatile bool errorFlag = false;
    MultiTableThreadInfo threads[tbNumberOfThreads];

    TestUser users[tbNumberOfStressSessions];

    for (unsigned ii = 0; ii < tbNumberOfStressSessions; ii++) {
        char userName[256];
        char sessionName[256];
        snprintf(userName, sizeof(userName), "multiUser%04u", ii);
        snprintf(sessionName, sizeof(sessionName), "multiSession%04u", ii);
        status = users[ii].createUserSession(userName, sessionName);
        LOCAL_STATUS_BAILOUT("Failure in stress User initialization");
    }

    TestTableList tables(tbNumberOfStressTables);

    for (uint32_t ii = 0; ii < tbNumberOfStressTables; ++ii) {
        char TableName[256];
        snprintf(TableName, sizeof(TableName), "stressTable%04d", (int)ii);
        status = tables[ii].createTable(
            &users[ii % tbNumberOfStressSessions],
            "multiTable",
            ii/*table idx*/,
            1/*Index of the key column*/,
            {
                {"FF1", DfString},
                {"FF2", DfInt64}
            });
        LOCAL_STATUS_BAILOUT("Failure in stress Table initialization");
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        threads[ii].Init(tables.get(), &stopFlag, &errorFlag);
    }

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        status = Runtime::get()->createBlockableThread(
            &threads[ii].threadId,
            NULL,
            StressMultiTable,
            &threads[ii]);
        LOCAL_STATUS_BAILOUT("createBlockableThread failed");
    }

    WaitWithFlagPing(&errorFlag, sleepSec);

    stopFlag = true;

    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        ret = sysThreadJoin(threads[ii].threadId, NULL);
    }

    // Check is there was an error
    int totalCount = 0;
    for (uint32_t ii = 0; ii < tbNumberOfThreads; ++ii) {
        totalCount += threads[ii].loops;
        if (threads[ii].status != StatusOk) {
            status = threads[ii].status;
            xSyslog(moduleName, XlogErr,
                "Multi Table test thread %d failed with status: %s",
                ii,
                strGetFromStatus(status));
        }
    }
    LOCAL_STATUS_BAILOUT("One or more threads failed");

    xSyslog(moduleName, XlogInfo,
        "Table Multi Table test ran through %d cycles", totalCount);

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Functions, called by Functional test facility

Status
TableTestsSanity()
{
    Status status = StatusOk;
    xSyslog(moduleName, XlogInfo,
        "TableTestsMain::Starting buf$ sanity test");

    TestUser user;

    status = user.createUserSession("user001", "session001");
    LOCAL_STATUS_BAILOUT("Can't create User session");

    for (size_t repeat = 1; repeat <= tbNumberOfSanityIterations; repeat++ ) {
        status = TestOnRequestMsgGetMetaPlain(&user);
        LOCAL_STATUS_BAILOUT("TestOnRequestMsgGetMetaPlain failed");

        status = TestOnRequestMsgGetLocalRowsPlain(&user);
        LOCAL_STATUS_BAILOUT("TestOnRequestMsgGetLocalRowsPlain failed");

        status = TableAddNsTableTest(&user);
        LOCAL_STATUS_BAILOUT("TableAddNsTableTest failed");

        status = TestMergeFunctionsPlain(&user);
        LOCAL_STATUS_BAILOUT("TestMergeFunctionsPlain failed");

        status = TestColumnInfoPlain();
        LOCAL_STATUS_BAILOUT("TestColumnInfoPlain failed");
    }

    status = user.cleanUpSession();
    LOCAL_STATUS_BAILOUT("Failure in User::cleanUpSession");

    xSyslog(moduleName, XlogInfo,
        "TableTestsMain::Starting buf$ sanity test is OK");

    return status;
}

Status
TableTestsStress()
{
    Status status = StatusOk;
    xSyslog(moduleName, XlogInfo,
        "TableTestsMain::Starting buf$ stress test");

    TestUser user;
    status = user.createUserSession("user001", "session001");
    LOCAL_STATUS_BAILOUT("Can't create User session");

    double sleepSec = StressTimeSec / 4;

    status = StressTableOnRequestMsgTest(&user, sleepSec);
    LOCAL_STATUS_BAILOUT("StressTableOnRequestMsgTest failed");

    status = StressTableAddNsMetaTest(&user, sleepSec);
    LOCAL_STATUS_BAILOUT("StressTableAddNsMetaTest failed");

    status = StressTablePublishingTest(sleepSec);
    LOCAL_STATUS_BAILOUT("StressTablePublishingTest failed");

    status = StressMultiTableTest(sleepSec);
    LOCAL_STATUS_BAILOUT("StressMultiTableTest failed");

    status = user.cleanUpSession();
    LOCAL_STATUS_BAILOUT("Failure in User::cleanUpSession");

    xSyslog(moduleName, XlogInfo,
        "TableTestsMain::Starting buf$ sanity test is OK");

    return status;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Configuration function, called by Functional test facility

Status
TableTestsParseConfig(Config::Configuration *config,
                           char *key,
                           char *value,
                           bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibTableFuncTestConfig(
                       LibTableFuncTestNumberOfThreads)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfThreads = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestNumberOfPublishingTables)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfPublishingTables = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestNumberOfPublishingSessions)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfPublishingSessions = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTesttbNumberOfStressPublish)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfStressPublish = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestNumberOfOnRequestMsgTables)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfOnRequestMsgTables = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestNumberOfStressTables)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfStressTables = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestNumberOfStressSessions)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfStressSessions = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestNumberOfSanityIterations)) == 0) {
        int64_t val = strtoll(value, NULL, 0);
        if (val >= 1) {
            tbNumberOfSanityIterations = val;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibTableFuncTestConfig(
                              LibTableFuncTestStressTimeSec)) == 0) {
        StressTimeSec = strtod(value, NULL);
        if (StressTimeSec < 0) {
            status = StatusUsrNodeIncorrectParams;
        }
    } else {
        status = StatusUsrNodeIncorrectParams;
    }

    xSyslog(moduleName,
            XlogDebug,
            "%s changed %s to %s",
            (status == StatusOk ? "Successfully" : "Unsuccessfully"),
            key,
            value);

    return status;
}
