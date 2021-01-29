// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "usr/Users.h"
#include "service/ResultSetService.h"
#include "dag/Dag.h"
#include "dataset/Dataset.h"
#include "operators/Operators.h"
#include "table/ResultSet.h"
#include "OrderingEnums.h"
#include "table/ResultSet.h"
#include "dataformat/DataFormatJson.h"
#include "dag/DagTypes.h"
#include "libapis/ProtobufUtil.h"

// TODO: Rename the ResultSet class to ResultSetImpl
typedef ResultSet ResultSetImpl;

using namespace xcalar::compute::localtypes::ResultSet;
using namespace xcalar::compute::localtypes::Workbook;

ResultSetService::ResultSetService()
{
    userMgr_ = UserMgr::get();
}

ServiceAttributes
ResultSetService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
ResultSetService::make(const ResultSetMakeRequest *rsMakeRequest,
                       ResultSetMakeResponse *rsMakeResponse)
{
    Status status;
    DsDataset *dataset = NULL;
    Dataset *ds = Dataset::get();
    DatasetRefHandle dsRefHandle;
    XcalarApiMakeResultSetOutput *makeResultSetOutput = NULL;
    size_t outputSize = 0;
    bool trackOpsToSession = false;
    auto scope = rsMakeRequest->scope();
    TableMgr::FqTableState fqTableState;
    Dag *srcGraph = NULL;
    XcalarApiUserId *user = NULL;

    status = fqTableState.setUp(rsMakeRequest->name().c_str(), &scope);
    if (status == StatusOk) {
        srcGraph = fqTableState.graph;
        user = &fqTableState.sessionContainer.userId;
    } else if (status == StatusDagNodeNotFound) {
        // This happens when we try to make a result set on a dataset
        // which we have not "inducted" into our session yet.
        status = protobufutil::setupSessionScope(&rsMakeRequest->scope(),
                                                 &srcGraph,
                                                 &trackOpsToSession);
        if (srcGraph == NULL) {
            status = StatusInval;
        }
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed result set make, error setupSessionScope for "
                        "\"%s\": %s",
                        rsMakeRequest->name().c_str(),
                        strGetFromStatus(status));
        user = &srcGraph->getSessionContainer()->userId;

        status = ds->openHandleToDatasetByName(rsMakeRequest->name().c_str(),
                                               user,
                                               &dataset,
                                               LibNsTypes::ReaderShared,
                                               &dsRefHandle);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed result set make, error retrieving dataset for "
                        "\"%s\": %s",
                        rsMakeRequest->name().c_str(),
                        strGetFromStatus(status));
    }

    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set make, error fqTableState.setUp for "
                    "\"%s\": %s",
                    rsMakeRequest->name().c_str(),
                    strGetFromStatus(status));

    outputSize = XcalarApiSizeOfOutput(*makeResultSetOutput);

    makeResultSetOutput =
        (XcalarApiMakeResultSetOutput *) memAllocExt(outputSize, ModuleName);
    BailIfNullMsg(makeResultSetOutput,
                  StatusNoMem,
                  ModuleName,
                  "Failed result set make for \"%s\": %s",
                  rsMakeRequest->name().c_str(),
                  strGetFromStatus(status));
    memZero(makeResultSetOutput, outputSize);

    status = ResultSetMgr::get()->makeResultSet(srcGraph,
                                                rsMakeRequest->name().c_str(),
                                                rsMakeRequest->error_dataset(),
                                                makeResultSetOutput,
                                                user);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set make for \"%s\": %s",
                    rsMakeRequest->name().c_str(),
                    strGetFromStatus(status));

    try {
        rsMakeResponse->set_result_set_id(makeResultSetOutput->resultSetId);
        rsMakeResponse->set_num_rows(makeResultSetOutput->numEntries);
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        xSyslog(ModuleName,
                XlogErr,
                "Failed result set make for \"%s\": %s",
                rsMakeRequest->name().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (dataset != NULL) {
        Status status2 = ds->closeHandleToDataset(&dsRefHandle);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Error closing dataset for \"%s\": %s",
                    rsMakeRequest->name().c_str(),
                    strGetFromStatus(status2));
        }
    }
    if (trackOpsToSession) {
        protobufutil::teardownSessionScope(&rsMakeRequest->scope());
        trackOpsToSession = false;
    }
    if (makeResultSetOutput != NULL) {
        memFree(makeResultSetOutput);
        makeResultSetOutput = NULL;
    }
    return status;
}

Status
ResultSetService::release(const ResultSetReleaseRequest *rsReleaseRequest,
                          google::protobuf::Empty *empty)
{
    Status status = StatusOk;
    bool trackOpsToSession = false;

    status = protobufutil::setupSessionScope(&rsReleaseRequest->scope(),
                                             NULL,
                                             &trackOpsToSession);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set release for %ld: %s",
                    rsReleaseRequest->result_set_id(),
                    strGetFromStatus(status));

    ResultSetMgr::get()->freeResultSet(rsReleaseRequest->result_set_id());

CommonExit:
    if (trackOpsToSession) {
        protobufutil::teardownSessionScope(&rsReleaseRequest->scope());
        trackOpsToSession = false;
    }
    return status;
}

Status
ResultSetService::next(const ResultSetNextRequest *rsNextRequest,
                       ResultSetNextResponse *rsNextResponse)
{
    Status status = StatusOk;
    json_t *rows = NULL;
    ResultSetImpl *rs = NULL;
    bool trackOpsToSession = false;

    status = protobufutil::setupSessionScope(&rsNextRequest->scope(),
                                             NULL,
                                             &trackOpsToSession);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set next for %ld: %s",
                    rsNextRequest->result_set_id(),
                    strGetFromStatus(status));

    rs = ResultSetMgr::get()->getResultSet(rsNextRequest->result_set_id());
    BailIfNullMsg(rs,
                  StatusInvalidResultSetId,
                  ModuleName,
                  "Failed result set next for %ld: %s",
                  rsNextRequest->result_set_id(),
                  strGetFromStatus(status));

    status = rs->getNext(rsNextRequest->num_rows(), &rows);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set next for %ld: %s",
                    rsNextRequest->result_set_id(),
                    strGetFromStatus(status));

    try {
        unsigned ii;
        json_t *row, *val;
        const char *key;
        json_array_foreach (rows, ii, row) {
            ProtoRow *protoRow = rsNextResponse->add_rows();
            ResultSetNextResponse::RowMeta *rowMeta =
                rsNextResponse->add_metas();

            json_object_foreach (row, key, val) {
                rowMeta->add_column_names(key);
                ProtoFieldValue protoVal;
                status = JsonFormatOps::convertJsonToProto(val, &protoVal);
                BailIfFailedMsg(ModuleName,
                                status,
                                "Failed result set next for %ld: %s",
                                rsNextRequest->result_set_id(),
                                strGetFromStatus(status));
                google::protobuf::MapPair<std::string, ProtoFieldValue>
                    protoRowEntry(std::string(key), protoVal);
                protoRow->mutable_fields()->insert(protoRowEntry);
            }
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed result set next for %ld: %s",
                rsNextRequest->result_set_id(),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (rs) {
        ResultSetMgr::get()->putResultSet(rs);
    }
    if (trackOpsToSession) {
        protobufutil::teardownSessionScope(&rsNextRequest->scope());
        trackOpsToSession = false;
    }
    if (rows) {
        json_decref(rows);
        rows = NULL;
    }
    return status;
}

Status
ResultSetService::seek(const ResultSetSeekRequest *rsSeekRequest,
                       google::protobuf::Empty *empty)
{
    Status status = StatusOk;
    ResultSetImpl *rs = NULL;
    bool trackOpsToSession = false;

    status = protobufutil::setupSessionScope(&rsSeekRequest->scope(),
                                             NULL,
                                             &trackOpsToSession);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set seek for %ld: %s",
                    rsSeekRequest->result_set_id(),
                    strGetFromStatus(status));

    rs = ResultSetMgr::get()->getResultSet(rsSeekRequest->result_set_id());
    BailIfNullMsg(rs,
                  StatusInvalidResultSetId,
                  ModuleName,
                  "Failed result set seek for %ld: %s",
                  rsSeekRequest->result_set_id(),
                  strGetFromStatus(status));

    status = rs->seek(rsSeekRequest->row_index());
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed result set seek for %ld: %s",
                    rsSeekRequest->result_set_id(),
                    strGetFromStatus(status));

CommonExit:
    if (trackOpsToSession) {
        protobufutil::teardownSessionScope(&rsSeekRequest->scope());
        trackOpsToSession = false;
    }
    if (rs) {
        ResultSetMgr::get()->putResultSet(rs);
    }
    return status;
}
