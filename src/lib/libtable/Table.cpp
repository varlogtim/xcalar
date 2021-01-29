// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <math.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "msg/Message.h"
#include "xdb/Xdb.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "util/Math.h"
#include "df/DataFormat.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "msg/Xid.h"
#include "transport/TransportPage.h"
#include "gvm/Gvm.h"
#include "ns/LibNs.h"
#include "cursor/Cursor.h"
#include "table/Table.h"
#include "table/TableGvm.h"
#include "table/ResultSet.h"
#include "dataformat/DataFormatJson.h"
#include "util/ProtoWrap.h"
#include "libapis/OperatorHandler.h"
#include "dag/Dag.h"
#include "dag/DagLib.h"
#include "usr/Users.h"
#include "libapis/ProtobufUtil.h"

static constexpr const char *moduleName = "tableMgr";

TableMgr *TableMgr::instance_ = NULL;

TableMgr *
TableMgr::get()
{
    return instance_;
}

Status
TableMgr::init()
{
    assert(instance_ == NULL);
    instance_ = new (std::nothrow) TableMgr();
    if (instance_ == NULL) {
        return StatusNoMem;
    }
    return instance_->initInternal();
}

Status
TableMgr::initInternal()
{
    Status status = StatusOk;

    status = TableNsMgr::init();
    BailIfFailed(status);

    status = TableGvm::init();
    BailIfFailed(status);

    atomicWrite64(&tabCursorRoundRobin_, 0);

CommonExit:
    if (status != StatusOk) {
        instance_->destroy();
    }
    return status;
}

// Called once at server shutdown. This function waits for all Xdbs
// to be dropped before it frees up Xdb meta data.
void
TableMgr::destroy()
{
    if (TableGvm::get() != NULL) {
        TableGvm::get()->destroy();
    }
    if (TableNsMgr::get() != NULL) {
        TableNsMgr::get()->destroy();
    }

    tableNameHashTable_.removeAll(NULL);
    tableIdHashTable_.removeAll(&TableObj::destroy);
    delete instance_;
    instance_ = NULL;
}

Status
TableMgr::createColumnInfoTable(const NewKeyValueNamedMeta *srcTupNamedMeta,
                                ColumnInfoTable *tableOut)
{
    Status status = StatusOk;
    const NewTupleMeta *srcTupMeta = srcTupNamedMeta->kvMeta_.tupMeta_;
    unsigned numCols = srcTupMeta->getNumFields();

    for (unsigned ii = 0; ii < numCols; ii++) {
        unsigned idxInTable = ii;
        ColumnInfo *colInfo;

        colInfo =
            new (std::nothrow) ColumnInfo(idxInTable,
                                          srcTupNamedMeta->valueNames_[ii],
                                          srcTupMeta->getFieldType(ii));
        BailIfNull(colInfo);

        tableOut->insert(colInfo);
    }

CommonExit:
    if (status != StatusOk) {
        tableOut->removeAll(&ColumnInfo::destroy);
    }
    return status;
}

void
TableMgr::TableCursorSchedulable::run()
{
    Status status;
    XdbGetLocalRowsRequest req;
    try {
        req = request_->parent().app().xdbgetlocalrows();
    } catch (std::bad_alloc) {
        status = StatusNoMem;
        goto CommonExit;
    } catch (std::exception &e) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

    if (req.nodeid() == (int) Config::get()->getMyNodeId()) {
        status = TableMgr::getRowsLocal(&req, parentResponse_);
        BailIfFailed(status);
    } else {
        status = TableMgr::getRowsRemote(&req, parentResponse_);
        BailIfFailed(status);
    }
CommonExit:
    status_ = status;
}

void
TableMgr::TableCursorSchedulable::done()
{
    lock_.lock();
    workDone_ = true;
    cv_.broadcast();
    lock_.unlock();
}

Status
TableMgr::onRequestMsg(LocalMsgRequestHandler *reqHandler,
                       LocalConnection *connection,
                       const ProtoRequestMsg *request,
                       ProtoResponseMsg *response)
{
    Status status = StatusOk;
    Txn savedTxn = Txn::currentTxn();

    ProtoParentChildResponse *parentResponse =
        new (std::nothrow) ProtoParentChildResponse();
    BailIfNull(parentResponse);

    switch (request->parent().func()) {
    case ParentFuncXdbGetLocalRows: {
        // Round robin TableCursor on all the operator runtime schedulers, i.e.
        // Sched0, Sched1 and Sched2.
        Runtime::SchedId rtSchedId = static_cast<Runtime::SchedId>(
            atomicInc64(&tabCursorRoundRobin_) %
            static_cast<int>(Runtime::SchedId::Immediate));

        Txn curTxn = savedTxn;
        curTxn.rtSchedId_ = rtSchedId;
        curTxn.rtType_ = RuntimeTypeThroughput;
        Txn::setTxn(curTxn);

        TableCursorSchedulable sched(request, parentResponse);
        status = Runtime::get()->schedule(&sched);
        BailIfFailed(status);

        sched.waitUntilDone();

        try {
            response->set_allocated_parentchild(parentResponse);
            parentResponse = NULL;

            response->set_status(StatusOk.code());
        } catch (std::exception &e) {
            status = StatusNoMem;
            goto CommonExit;
        }
        break;
    }

    case ParentFuncXdbGetMeta: {
        Txn curTxn = savedTxn;
        curTxn.rtSchedId_ = Runtime::SchedId::Immediate;
        curTxn.rtType_ = RuntimeTypeImmediate;
        Txn::setTxn(curTxn);

        XdbGetMetaRequest req = request->parent().app().xdbgetmeta();

        status = getXdbMeta(&req, parentResponse);
        BailIfFailed(status);

        try {
            response->set_allocated_parentchild(parentResponse);
            parentResponse = NULL;

            response->set_status(StatusOk.code());
        } catch (std::exception &e) {
            status = StatusNoMem;
            goto CommonExit;
        }
        break;
    }
    default:
        assert(0);
        status = StatusUnimpl;
        break;
    }

CommonExit:
    if (parentResponse) {
        delete parentResponse;
        parentResponse = NULL;
    }

    response->set_status(status.code());
    Txn::setTxn(savedTxn);
    return status;
}

Status
TableMgr::getXdbMeta(const XdbGetMetaRequest *getMetaRequest,
                     ProtoParentChildResponse *parentResponse)
{
    XdbGetMetaResponse *response;
    Status status;
    Xdb *xdb;
    XdbMeta *xdbMeta;
    TableNsMgr::TableHandleTrack handleTrack;

    status = XdbMgr::get()->xdbGet(getMetaRequest->xdbid(), &xdb, &xdbMeta);
    BailIfFailed(status);

    try {
        response = parentResponse->mutable_xdbgetmeta();
        TableResultSet rs(XidInvalid,
                          XidInvalid,
                          XidInvalid,
                          handleTrack,
                          xdb,
                          xdbMeta);

        status = rs.populateNumEntries(xdbMeta->xdbId, false);
        BailIfFailed(status);

        response->set_xdbid(getMetaRequest->xdbid());

        for (unsigned ii = 0; ii < Config::get()->getActiveNodes(); ii++) {
            response->add_numrowspernode(rs.numEntriesPerNode_[ii]);
        }

        for (unsigned ii = 0; ii < XdbMgr::getNumFields(xdbMeta); ii++) {
            XdbColumnDesc *col = response->add_columns();
            col->set_name(xdbMeta->kvNamedMeta.valueNames_[ii]);
            col->set_type(strGetFromDfFieldType(
                xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii)));
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
TableMgr::getRowsRemote(const XdbGetLocalRowsRequest *getRowsRequest,
                        ProtoParentChildResponse *parentResponse)
{
    Status status;
    MsgEphemeral eph;
    MsgMgr *msgMgr = MsgMgr::get();
    size_t size = getRowsRequest->ByteSizeLong();
    GetRowsOutput output;
    void *buf = NULL;
    output.status = StatusOk;
    output.response = parentResponse;

    if (getRowsRequest->nodeid() >= (int) Config::get()->getActiveNodes()) {
        status = StatusInval;
        goto CommonExit;
    }

    buf = memAlloc(size);
    BailIfNull(buf);

    status = pbSerializeToArray(getRowsRequest, buf, size);
    BailIfFailed(status);

    msgMgr->twoPcEphemeralInit(&eph,
                               buf,
                               size,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcGetRows1,
                               &output,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcGetRows,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           getRowsRequest->nodeid(),
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = output.status;
    BailIfFailed(status);

CommonExit:
    if (buf) {
        memFree(buf);
    }

    return status;
}

Status
TableMgr::getRowsLocal(const XdbGetLocalRowsRequest *getRowsRequest,
                       ProtoParentChildResponse *parentResponse)
{
    XdbGetLocalRowsResponse *response;
    Status status;
    Xdb *xdb;
    XdbMeta *xdbMeta;
    bool refAcquired = false;

    // this is coming from an SDK call which has made no guarentees that the
    // table won't be dropped. Make sure to grab ref on the XDB
    status =
        XdbMgr::get()->xdbGetWithRef(getRowsRequest->xdbid(), &xdb, &xdbMeta);
    BailIfFailed(status);
    refAcquired = true;

    try {
        response = parentResponse->mutable_xdbgetlocalrows();
    } catch (std::bad_alloc) {
        status = StatusNoMem;
        goto CommonExit;
    } catch (std::exception &e) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

    if (getRowsRequest->asdatapage()) {
        status = getRowsAsDatapage(xdb, xdbMeta, getRowsRequest, response);
        BailIfFailed(status);
    } else {
        status = getRowsAsProto(xdb, xdbMeta, getRowsRequest, response);
        BailIfFailed(status);
    }

CommonExit:
    if (refAcquired) {
        XdbMgr::xdbPutRef(xdb);
    }

    return status;
}

Status
TableMgr::getRowsAsProto(Xdb *srcXdb,
                         XdbMeta *srcMeta,
                         const XdbGetLocalRowsRequest *getRowsRequest,
                         XdbGetLocalRowsResponse *response)
{
    Status status = StatusOk;
    bool cursorInit = false;
    TableCursor cur;
    const NewTupleMeta *tupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t fieldsInTable = tupMeta->getNumFields();
    NewKeyValueEntry srcKvEntry(&srcMeta->kvNamedMeta.kvMeta_);
    int *columnIndecies = NULL;

    status = CursorManager::get()->createOnTable(srcMeta->xdbId,
                                                 Unordered,
                                                 Cursor::IncrementRefOnly,
                                                 &cur);
    BailIfFailed(status);

    cursorInit = true;

    status = cur.seek(getRowsRequest->startrow(), Cursor::SeekToAbsRow);
    BailIfFailed(status);

    columnIndecies = new (std::nothrow) int[getRowsRequest->columns_size()];
    BailIfNull(columnIndecies);

    // Let's look up the indecies for each of our columns so that we can reuse
    // it for all records
    for (int ii = 0; ii < getRowsRequest->columns_size(); ii++) {
        bool found = false;
        for (size_t jj = 0; jj < tupMeta->getNumFields(); jj++) {
            if (strcmp(getRowsRequest->columns(ii).c_str(),
                       srcMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                found = true;
                columnIndecies[ii] = jj;
                break;
            }
        }
        if (!found) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "No such column '%s': %s",
                    getRowsRequest->columns(ii).c_str(),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    for (int64_t numRecs = 0; numRecs < getRowsRequest->numrows(); numRecs++) {
        status = cur.getNext(&srcKvEntry);
        BailIfFailed(status);

        try {
            ProtoRow *protoRow = response->mutable_rows()->add_rows();

            for (int ii = 0; ii < getRowsRequest->columns_size(); ii++) {
                int columnIndex = columnIndecies[ii];
                assert(columnIndex < (int) tupMeta->getNumFields());
                ProtoFieldValue protoVal;

                DfFieldType type = tupMeta->getFieldType(columnIndex);
                DfFieldValue fieldVal;
                bool isValid;
                fieldVal = srcKvEntry.tuple_.get(columnIndex,
                                                 fieldsInTable,
                                                 type,
                                                 &isValid);

                // invalid fields are left unset
                if (isValid) {
                    status =
                        DataFormat::fieldToProto(fieldVal, type, &protoVal);
                    BailIfFailed(status);
                }
                google::protobuf::MapPair<std::string, ProtoFieldValue>
                    protoRowEntry(getRowsRequest->columns(ii), protoVal);
                protoRow->mutable_fields()->insert(protoRowEntry);
            }
        } catch (std::exception &e) {
            status = StatusNoMem;
            goto CommonExit;
        }
    }

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (cursorInit) {
        CursorManager::get()->destroy(&cur);
        cursorInit = false;
    }
    if (columnIndecies != NULL) {
        delete[] columnIndecies;
    }

    return status;
}

Status
TableMgr::getRowsAsDatapage(Xdb *srcXdb,
                            XdbMeta *srcMeta,
                            const XdbGetLocalRowsRequest *getRowsRequest,
                            XdbGetLocalRowsResponse *response)
{
    Status status = StatusOk;
    bool cursorInit = false;
    TableCursor cur;
    const NewTupleMeta *tupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t fieldsInTable = tupMeta->getNumFields();
    NewKeyValueEntry srcKvEntry(&srcMeta->kvNamedMeta.kvMeta_);
    int *columnIndecies = NULL;
    DataPageWriter writer;
    DataPageWriter::FieldMetaCache fieldMetaCache;
    uint8_t *serializedPage = NULL;

    status = CursorManager::get()->createOnTable(srcMeta->xdbId,
                                                 Unordered,
                                                 Cursor::IncrementRefOnly,
                                                 &cur);
    BailIfFailed(status);

    cursorInit = true;

    status = cur.seek(getRowsRequest->startrow(), Cursor::SeekToAbsRow);
    BailIfFailed(status);

    columnIndecies = new (std::nothrow) int[getRowsRequest->columns_size()];
    BailIfNull(columnIndecies);

    // Let's look up the indecies for each of our columns so that we can reuse
    // it for all records
    for (int ii = 0; ii < getRowsRequest->columns_size(); ii++) {
        bool found = false;
        for (size_t jj = 0; jj < tupMeta->getNumFields(); jj++) {
            if (strcmp(getRowsRequest->columns(ii).c_str(),
                       srcMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                found = true;
                columnIndecies[ii] = jj;
                break;
            }
        }
        if (!found) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "No such column '%s': %s",
                    getRowsRequest->columns(ii).c_str(),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // XXX maybe have a better constant here? we could also refactor to not have
    // a constant at all and simply allow the page to be as big as it needs for
    // the number of requested rows.
    //
    // Limit the size of our page
    status = writer.init(100 * MB);
    BailIfFailed(status);

    status = fieldMetaCache.init(getRowsRequest->columns_size(), &writer);
    BailIfFailed(status);

    // Populate our field meta cache so it is used across all records
    for (int ii = 0; ii < getRowsRequest->columns_size(); ii++) {
        int columnIndex = columnIndecies[ii];
        assert(columnIndex < (int) tupMeta->getNumFields());

        DfFieldType dfType = tupMeta->getFieldType(columnIndex);
        if (dfType != DfScalarObj) {
            ValueType valueType = dfTypeToValueType(dfType);
            fieldMetaCache.setField(ii,
                                    getRowsRequest->columns(ii).c_str(),
                                    valueType);
        }
    }

    for (int64_t numRecs = 0; numRecs < getRowsRequest->numrows(); numRecs++) {
        DataPageWriter::PageStatus pageStatus;
        DataPageWriter::Record *record;
        status = writer.newRecord(&record);
        BailIfFailed(status);

        record->setFieldNameCache(&fieldMetaCache);

        status = cur.getNext(&srcKvEntry);
        BailIfFailed(status);

        for (int ii = 0; ii < getRowsRequest->columns_size(); ii++) {
            DataValue fieldValue;
            int columnIndex = columnIndecies[ii];
            assert(columnIndex < (int) tupMeta->getNumFields());

            DfFieldType type = tupMeta->getFieldType(columnIndex);
            DfFieldValue tupleVal;
            bool isValid;
            tupleVal = srcKvEntry.tuple_.get(columnIndex,
                                             fieldsInTable,
                                             type,
                                             &isValid);

            // invalid fields are left unset
            if (isValid) {
                status = fieldValue.setFromFieldValue(&tupleVal, type);
                BailIfFailed(status);
                if (type != DfScalarObj) {
                    // We statically know this field's type, thus we can use
                    // the fieldMetaCache
                    status = record->addFieldByIndex(ii, &fieldValue);
                    BailIfFailed(status);
                } else {
                    // XXX This is adding fields by name whenever we have a
                    // Scalar. This is relatively inefficient and there may
                    // be a more optimal algorithm here.
                    TypedDataValue typedValue;
                    typedValue.value_ = fieldValue;
                    typedValue.type_ =
                        dfTypeToValueType(tupleVal.scalarVal->fieldType);
                    status = record->addFieldByName(getRowsRequest->columns(ii)
                                                        .c_str(),
                                                    &typedValue);
                    BailIfFailed(status);
                }
            }
        }
        status = writer.commit(record, &pageStatus, NULL);
        BailIfFailed(status);

        if (pageStatus == DataPageWriter::PageStatus::Full) {
            // Let's just let this page have less records than requested
            break;
        }
    }

    serializedPage = new (std::nothrow) uint8_t[writer.pageSize()];
    BailIfNull(serializedPage);

    writer.serializeToPage(serializedPage);

    try {
        response->set_datapage(serializedPage, writer.pageSize());
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }
    if (serializedPage) {
        delete[] serializedPage;
        serializedPage = NULL;
    }

    if (cursorInit) {
        CursorManager::get()->destroy(&cur);
        cursorInit = false;
    }
    if (columnIndecies != NULL) {
        delete[] columnIndecies;
    }

    return status;
}

Status
TableMgr::addTableObj(TableNsMgr::TableId tableId,
                      const char *fullyQualName,
                      DagTypes::DagId dagId,
                      DagTypes::NodeId dagNodeId,
                      const XcalarApiUdfContainer *sessionContainer)
{
    Status status;
    TableGvm::AddTableObj *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(TableGvm::AddTableObj));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed addTableObj tableId %ld: %s",
                  tableId,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed addTableObj tableId %ld: %s",
                  tableId,
                  strGetFromStatus(status));

    input = (TableGvm::AddTableObj *) gPayload->buf;
    input->tableId = tableId;
    input->dagId = dagId;
    input->dagNodeId = dagNodeId;
    verifyOk(strStrlcpy(input->fullyQualName,
                        fullyQualName,
                        sizeof(input->fullyQualName)));
    UserDefinedFunction::copyContainers(&input->sessionContainer,
                                        sessionContainer);

    gPayload->init(TableGvm::get()->getGvmIndex(),
                   (uint32_t) TableGvm::Action::AddObj,
                   sizeof(TableGvm::AddTableObj));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed addTableObj tableId %ld: %s",
                    tableId,
                    strGetFromStatus(status));

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    return status;
}

void
TableMgr::removeTableObj(TableNsMgr::TableId tableId)
{
    Status status;
    TableGvm::RemoveTableObj *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(TableGvm::RemoveTableObj));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed removeTableObj tableId %ld: %s",
                  tableId,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed removeTableObj tableId %ld: %s",
                  tableId,
                  strGetFromStatus(status));

    input = (TableGvm::RemoveTableObj *) gPayload->buf;
    input->tableId = tableId;

    gPayload->init(TableGvm::get()->getGvmIndex(),
                   (uint32_t) TableGvm::Action::RemoveObj,
                   sizeof(TableGvm::RemoveTableObj));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed removeTableObj tableId %ld: %s",
                    tableId,
                    strGetFromStatus(status));

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
}

Status
TableMgr::addTableObjLocal(TableNsMgr::TableId tableId,
                           DagTypes::DagId dagId,
                           DagTypes::NodeId dagNodeId,
                           const char *fullyQualName,
                           const XcalarApiUdfContainer *sessionContainer)
{
    Status status;
    bool locked = false;
    TableObj *obj = new (std::nothrow)
        TableObj(tableId, dagId, dagNodeId, sessionContainer);
    BailIfNullMsg(obj,
                  status,
                  moduleName,
                  "Failed addTableObjLocal tableId %ld, %s: %s",
                  tableId,
                  fullyQualName,
                  strGetFromStatus(status));

    status = obj->setTableName(fullyQualName);
    BailIfNullMsg(obj,
                  status,
                  moduleName,
                  "Failed addTableObjLocal tableId %ld, %s: %s",
                  tableId,
                  fullyQualName,
                  strGetFromStatus(status));

    locked = true;
    tableLock_.lock();
    status = tableIdHashTable_.insert(obj);
    if (status == StatusExist) {
        status = StatusTableAlreadyExists;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed addTableObjLocal tableId %ld, %s: %s",
                    tableId,
                    fullyQualName,
                    strGetFromStatus(status));

    status = tableNameHashTable_.insert(obj);
    if (status == StatusExist) {
        status = StatusTableAlreadyExists;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed addTableObjLocal tableId %ld, %s: %s",
                    tableId,
                    fullyQualName,
                    strGetFromStatus(status));

CommonExit:
    if (locked) {
        tableLock_.unlock();
    }
    return status;
}

void
TableMgr::removeTableObjLocal(TableNsMgr::TableId tableId)
{
    tableLock_.lock();
    TableObj *obj = tableIdHashTable_.remove(tableId);
    if (obj == NULL) {
        xSyslog(moduleName,
                XlogInfo,
                "TableMgr::removeTableObjLocal tableId %ld not found",
                tableId);
    } else {
        TableObj *objFromName = tableNameHashTable_.find(obj->getName());
        if (objFromName == obj) {
            // Handle the case when cleanout is happening b/c of
            // StatusTableAlreadyExists case.
            tableNameHashTable_.remove(obj->getName());
        }
        obj->destroy();
    }
    tableLock_.unlock();
}

TableMgr::TableObj *
TableMgr::getTableObj(TableNsMgr::TableId tableId)
{
    TableMgr::TableObj *tobj = NULL;
    auto guard = tableLock_.take();
    tobj = tableIdHashTable_.find(tableId);
    return tobj;
}

TableMgr::TableObj *
TableMgr::getTableObj(const char *fullyQualName)
{
    TableMgr::TableObj *tobj = NULL;
    auto guard = tableLock_.take();
    tobj = tableNameHashTable_.find(fullyQualName);
    return tobj;
}

MergeMgr::MergeInfo *
TableMgr::TableObj::getMergeInfo()
{
    return mergeInfo_;
}

void
TableMgr::TableObj::setMergeInfo(MergeMgr::MergeInfo *mergeInfo)
{
    assert(mergeInfo_ == NULL);
    mergeInfo_ = mergeInfo;
}

Status
TableMgr::TableObj::initColTable(XdbMeta *meta)
{
    if (colTable_.getSize()) {
        return StatusOk;
    } else {
        return TableMgr::createColumnInfoTable(&meta->kvNamedMeta, &colTable_);
    }
}

Status
TableMgr::TableObj::initImdColTable(XdbMeta *meta)
{
    if (imdColTable_.getSize()) {
        return StatusOk;
    } else {
        return TableMgr::createColumnInfoTable(&meta->kvNamedMeta,
                                               &imdColTable_);
    }
}

TableMgr::ColumnInfoTable *
TableMgr::TableObj::getColTable()
{
    return &colTable_;
}

TableMgr::ColumnInfoTable *
TableMgr::TableObj::getImdColTable()
{
    return &imdColTable_;
}

Status
TableMgr::TableObj::setTableName(const char *fullyQualName)
{
    Status status;
    char *tableName = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    status = strStrlcpy(fullyQualName_, fullyQualName, sizeof(fullyQualName_));
    BailIfFailed(status);

    status = tnsMgr->getNameFromFqTableName(fullyQualName, tableName);
    BailIfFailed(status);

    status = strStrlcpy(tableName_, tableName, sizeof(tableName_));
    BailIfFailed(status);

CommonExit:
    delete[] tableName;
    return (status);
}

Status
TableMgr::TableObj::getTableMeta(TableNsMgr::TableHandleTrack *handleTrack,
                                 Dag *sessionGraph,
                                 TableSchema *tableSchema,
                                 TableAttributes *tableAttrs)
{
    Status status;
    XdbMeta *xdbMeta = NULL;
    XdbId xdbId;
    bool isPinned = false;
    DgDagState state;
    ResultSetId *resultSetIds = NULL;
    unsigned numRsIds = 0;

    DCHECK(handleTrack->tableHandleValid);
    DCHECK(sessionGraph != NULL);

    status = sessionGraph->getXdbId(tableName_,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &xdbId);
    BailIfFailed(status);

    status = XdbMgr::get()->xdbGet(xdbId, NULL, &xdbMeta);
    BailIfFailed(status);

    // populate table schema
    try {
        // columns
        const NewTupleMeta *srcTupMeta = xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
        for (unsigned ii = 0; ii < srcTupMeta->getNumFields(); ii++) {
            ColumnAttributeProto *colAttr =
                tableSchema->add_column_attributes();
            colAttr->set_name(xdbMeta->kvNamedMeta.valueNames_[ii]);
            colAttr->set_type(
                strGetFromDfFieldType(srcTupMeta->getFieldType(ii)));
            colAttr->set_value_array_idx(ii);
        }

        // keys
        for (unsigned ii = 0; ii < xdbMeta->numKeys; ii++) {
            DfFieldAttrHeader *attr = &xdbMeta->keyAttr[ii];

            KeyAttributeProto *keyAttr = tableSchema->add_key_attributes();
            keyAttr->set_name(attr->name);
            keyAttr->set_ordering(strGetFromOrdering(attr->ordering));
            keyAttr->set_value_array_idx(attr->valueArrayIndex);
            keyAttr->set_type(strGetFromDfFieldType(attr->type));
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // populate table attributes
    try {
        tableAttrs->set_table_name(tableName_);
        tableAttrs->set_table_id(tableId_);
        tableAttrs->set_xdb_id(xdbId);
        tableAttrs->set_shared(handleTrack->tableHandle.isGlobal);

        // DagNode state
        status = sessionGraph->isDagNodePinned(dagNodeId_, &isPinned);
        BailIfFailed(status);
        tableAttrs->set_pinned(isPinned);
        status = sessionGraph->getDagNodeState(dagNodeId_, &state);
        BailIfFailed(status);
        tableAttrs->set_state(strGetFromDgDagState(state));

        // Datasets and resultsets
        for (unsigned ii = 0; ii < xdbMeta->numDatasets; ii++) {
            tableAttrs->add_datasets(xdbMeta->datasets[ii]->getDatasetName());
        }
        status = UserMgr::get()
                     ->getResultSetIdsByTableId(tableId_,
                                                &sessionContainer_.userId,
                                                &sessionContainer_.sessionInfo,
                                                &resultSetIds,
                                                &numRsIds);
        BailIfFailed(status);
        for (unsigned ii = 0; ii < numRsIds; ii++) {
            tableAttrs->add_result_set_ids(resultSetIds[ii]);
        }

    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (resultSetIds != NULL) {
        delete[] resultSetIds;
        resultSetIds = NULL;
    }

    return status;
}

// XXX ENG-8502 these aggregated stats should be made available
// in the tableObj itself
Status
TableMgr::TableObj::getTableAggrStats(Dag *sessionGraph,
                                      TableAggregatedStats *aggrStats) const
{
    Status status;
    XcalarApiDagNode *dagNode = NULL;

    DCHECK(sessionGraph != NULL);
    status = sessionGraph->getDagNode(dagNodeId_, &dagNode);
    BailIfFailed(status);

    try {
        aggrStats->set_total_size_in_bytes(dagNode->localData.sizeTotal);
        aggrStats->set_total_records_count(dagNode->localData.numRowsTotal);
        for (unsigned ii = 0; ii < dagNode->localData.numNodes; ii++) {
            aggrStats->add_rows_per_node(dagNode->localData.numRowsPerNode[ii]);
            aggrStats->add_size_in_bytes_per_node(
                dagNode->localData.sizePerNode[ii]);
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (dagNode != NULL) {
        memFree(dagNode);
        dagNode = NULL;
    }
    return status;
}

Status
TableMgr::TableObj::getTablePerNodeStats(
    Dag *sessionGraph,
    google::protobuf::Map<std::string, TableStatsPerNode> *perNodeStatsMap)
{
    Status status;
    XcalarApiOutput *out = NULL;
    XdbId xdbId;
    size_t outputSize;
    XcalarApiGetTableMetaOutput *getTableMetaOutput = NULL;
    GetTableMetaInput getTableMetaInput;
    MsgEphemeral eph;

    DCHECK(sessionGraph != NULL);

    status = sessionGraph->getXdbId(tableName_,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &xdbId);
    BailIfFailed(status);

    // setup input
    getTableMetaInput.isTable = true;
    getTableMetaInput.xid = xdbId;
    getTableMetaInput.isPrecise = true;
    getTableMetaInput.nodeId = dagNodeId_;

    outputSize = XcalarApiSizeOfOutput(*getTableMetaOutput) +
                 (sizeof(getTableMetaOutput->metas[0]) *
                  Config::get()->getActiveNodes());
    out = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (out == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(out, outputSize);
    getTableMetaOutput = &out->outputResult.getTableMetaOutput;
    getTableMetaOutput->numMetas = Config::get()->getActiveNodes();

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &getTableMetaInput,
                                      sizeof(getTableMetaInput),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcXcalarApiGetTableMeta1,
                                      out,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcXcalarApiGetTableMeta,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);

    for (unsigned ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        if (getTableMetaOutput->metas[ii].status != StatusOk.code()) {
            status.fromStatusCode(getTableMetaOutput->metas[ii].status);
            goto CommonExit;
        }
    }

    try {
        for (unsigned ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
            TableStatsPerNode perNodeStats;
            perNodeStats.set_status(
                strGetFromStatus(Status(getTableMetaOutput->metas[ii].status)));
            perNodeStats.set_num_rows(getTableMetaOutput->metas[ii].numRows);
            perNodeStats.set_num_pages(getTableMetaOutput->metas[ii].numPages);
            perNodeStats.set_num_slots(getTableMetaOutput->metas[ii].numSlots);
            perNodeStats.set_size_in_bytes(getTableMetaOutput->metas[ii].size);

            for (uint64_t jj = 0; jj < getTableMetaOutput->metas[ii].numSlots;
                 jj++) {
                uint64_t numPagesPerSlot =
                    getTableMetaOutput->metas[ii].numPagesPerSlot[jj];
                if (numPagesPerSlot > 0) {
                    google::protobuf::MapPair<google::protobuf::uint32,
                                              google::protobuf::uint64>
                        pageEntry(jj, numPagesPerSlot);
                    perNodeStats.mutable_pages_per_slot()->insert(pageEntry);

                    google::protobuf::MapPair<google::protobuf::uint32,
                                              google::protobuf::uint64>
                        rowEntry(jj,
                                 getTableMetaOutput->metas[ii]
                                     .numRowsPerSlot[jj]);
                    perNodeStats.mutable_rows_per_slot()->insert(rowEntry);
                }
            }

            perNodeStats.set_pages_consumed_in_bytes(
                getTableMetaOutput->metas[ii].xdbPageConsumedInBytes);
            perNodeStats.set_pages_allocated_in_bytes(
                getTableMetaOutput->metas[ii].xdbPageAllocatedInBytes);
            perNodeStats.set_pages_sent(
                getTableMetaOutput->metas[ii].numTransPageSent);
            perNodeStats.set_pages_received(
                getTableMetaOutput->metas[ii].numTransPageRecv);

            google::protobuf::MapPair<google::protobuf::string,
                                      TableStatsPerNode>
                perNodeStatsEntry("Node-" + std::to_string(ii), perNodeStats);
            perNodeStatsMap->insert(perNodeStatsEntry);
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }
CommonExit:
    if (out != NULL) {
        memFree(out);
        out = NULL;
    }
    return status;
}

TableNsMgr::TableId
TableMgr::getTableIdFromName(const char *fullyQualName)
{
    TableMgr::TableObj *tobj = NULL;
    TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;
    auto guard = tableLock_.take();

    tobj = tableNameHashTable_.find(fullyQualName);
    if (tobj != NULL) {
        tableId = tobj->getId();
    }

    return tableId;
}

Status
TableMgr::getTableNameFromId(TableNsMgr::TableId tableId,
                             char *retFullyQualName,
                             size_t fqnBufLen)
{
    Status status;
    TableMgr::TableObj *tobj = NULL;
    retFullyQualName[0] = '\0';
    auto guard = tableLock_.take();

    tobj = tableIdHashTable_.find(tableId);
    if (tobj != NULL) {
        status = strStrlcpy(retFullyQualName, tobj->getName(), fqnBufLen);
    } else {
        status = StatusTableIdNotFound;
    }

    return status;
}

Status
TableMgr::getDagNodeInfo(const char *fullyQualName,
                         DagTypes::DagId &retDagId,
                         DagTypes::NodeId &retDagNodeId,
                         XcalarApiUdfContainer *retSessionContainer,
                         bool &retIsGlobal)
{
    Status status;
    XcalarApiUdfContainer sessionContainer;
    TableMgr::TableObj *tobj = NULL;
    retDagId = DagTypes::DagIdInvalid;
    retDagNodeId = DagTypes::InvalidDagNodeId;
    retIsGlobal = false;

    tableLock_.lock();

    tobj = tableNameHashTable_.find(fullyQualName);
    if (tobj != NULL) {
        retDagId = tobj->getDagId();
        retDagNodeId = tobj->getDagNodeId();

        if (retSessionContainer) {
            sessionContainer = tobj->getSessionContainer();
            UserDefinedFunction::copyContainers(retSessionContainer,
                                                &sessionContainer);
        }
    } else {
        status = StatusTableNameNotFound;
    }

    tableLock_.unlock();

    bool global = TableNsMgr::get()->isGlobal(fullyQualName, status);
    if (status != StatusOk) {
        return status;
    }
    retIsGlobal = global;

    return status;
}

Status
TableMgr::FqTableState::setUp(const char *srcTableName, Dag *apiSessionGraph)
{
    this->tableName = srcTableName;
    DagTypes::DagId inputGraphId = apiSessionGraph->getId();

    Status status = OperatorHandler::getSourceDagNode(srcTableName,
                                                      apiSessionGraph,
                                                      &this->sessionContainer,
                                                      &this->graphId,
                                                      &this->nodeId);
    BailIfFailedMsg(moduleName,
                    status,
                    "FqTableState::setUp tableName '%s': %s",
                    srcTableName,
                    strGetFromStatus(status));

    verify((this->graph = DagLib::get()->getDagLocal(this->graphId)) != NULL);

    if (inputGraphId != this->graphId) {
        status = UserMgr::get()->trackOutstandOps(&this->sessionContainer,
                                                  UserMgr::OutstandOps::Inc);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed trackOutstandOps userName '%s', session '%s', "
                        "Id %ld: %s",
                        this->sessionContainer.userId.userIdName,
                        this->sessionContainer.sessionInfo.sessionName,
                        this->sessionContainer.sessionInfo.sessionId,
                        strGetFromStatus(status));
        this->cleanOut = true;
    }

CommonExit:
    return (status);
}

Status
TableMgr::FqTableState::setUp(
    const char *srcTableName,
    const xcalar::compute::localtypes::Workbook::WorkbookScope *scope)
{
    Status status;
    Dag *inputGraph = NULL;
    DagTypes::DagId inputGraphId = DagTypes::DagIdInvalid;
    this->tableName = srcTableName;
    bool trackOpsToSession = false;
    bool isGlobal = false;
    switch (scope->specifier_case()) {
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kGlobl:
        status = TableMgr::get()->getDagNodeInfo(srcTableName,
                                                 this->graphId,
                                                 this->nodeId,
                                                 &this->sessionContainer,
                                                 isGlobal);
        if (status == StatusOk && !isGlobal) {
            status = StatusTableNotGlobal;
        }
        break;
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kWorkbook:
        status = protobufutil::setupSessionScope(scope,
                                                 &inputGraph,
                                                 &trackOpsToSession);
        BailIfFailed(status);

        status = OperatorHandler::getSourceDagNode(srcTableName,
                                                   inputGraph,
                                                   &this->sessionContainer,
                                                   &this->graphId,
                                                   &this->nodeId);
        break;
    default:
        status = StatusInval;
        break;
    }

    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to get dag node info in FqTableState::setUp "
                    "for \"%s\": %s",
                    srcTableName,
                    strGetFromStatus(status));

    verify((this->graph = DagLib::get()->getDagLocal(this->graphId)) != NULL);

    if (inputGraphId != this->graphId) {
        status = UserMgr::get()->trackOutstandOps(&this->sessionContainer,
                                                  UserMgr::OutstandOps::Inc);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed trackOutstandOps userName '%s', "
                        "session '%s', Id %ld: %s",
                        this->sessionContainer.userId.userIdName,
                        this->sessionContainer.sessionInfo.sessionName,
                        this->sessionContainer.sessionInfo.sessionId,
                        strGetFromStatus(status));
    } else {
        trackOpsToSession = false;
    }
    this->cleanOut = true;

CommonExit:
    if (trackOpsToSession) {
        protobufutil::teardownSessionScope(scope);
        trackOpsToSession = false;
    }
    return status;
}

TableMgr::FqTableState::~FqTableState()
{
    if (!this->cleanOut) {
        return;
    }

    Status status = UserMgr::get()->trackOutstandOps(&this->sessionContainer,
                                                     UserMgr::OutstandOps::Dec);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed trackOutstandOps userName '%s', session '%s', Id "
                "%ld: "
                "%s",
                this->sessionContainer.userId.userIdName,
                this->sessionContainer.sessionInfo.sessionName,
                this->sessionContainer.sessionInfo.sessionId,
                strGetFromStatus(status));
    }
}
