// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "primitives/Primitives.h"
#include "dag/DagTypes.h"
#include "dag/DagLib.h"
#include "libapis/LibApisCommon.h"
#include "util/CmdParser.h"
#include "util/MemTrack.h"
#include "QueryParserEnums.h"
#include "queryparser/QueryParser.h"
#include "libapis/LibApisRecv.h"
#include "libapis/WorkItem.h"
#include "sys/XLog.h"
#include "optimizer/Optimizer.h"
#include "strings/String.h"

QueryParser *QueryParser::instance = NULL;
static constexpr const char *moduleName = "libqueryparser";

QueryParser *
QueryParser::get()
{
    return QueryParser::instance;
}

Status
QueryParser::init()
{
    Status status;
    assert(instance == NULL);
    instance = new (std::nothrow) QueryParser;
    if (instance == NULL) {
        return StatusNoMem;
    }

    status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }
    return status;
}

Status
QueryParser::initInternal()
{
    uint64_t ii;
    Status status = StatusUnknown;
    QueryParserEnum parserEnum;
    XcalarApis reverseParserEnum;

    for (ii = 0; ii < ArrayLen(this->cmdParsers); ii++) {
        this->cmdParsers[ii] = NULL;
    }

    this->cmdParsers[XcalarApiFunctionInvalid] = new (std::nothrow) QpInvalid();
    if (this->cmdParsers[XcalarApiFunctionInvalid] == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < ArrayLen(this->parserMap); ii++) {
        this->parserMap[ii] = this->cmdParsers[XcalarApiFunctionInvalid];
    }

    for (ii = 0; ii < ArrayLen(this->reverseParserMap); ii++) {
        this->reverseParserMap[ii] = this->cmdParsers[XcalarApiFunctionInvalid];
    }

    for (ii = 0; ii < ArrayLen(this->cmdParsers); ii++) {
        switch (ii) {
        case XcalarApiBulkLoad:
            this->cmdParsers[ii] = new (std::nothrow) QpLoad();
            parserEnum = QpLoadCmd;
            reverseParserEnum = XcalarApiBulkLoad;
            break;

        case XcalarApiIndex:
            this->cmdParsers[ii] = new (std::nothrow) QpIndex();
            parserEnum = QpIndexCmd;
            reverseParserEnum = XcalarApiIndex;
            break;

        case XcalarApiProject:
            this->cmdParsers[ii] = new (std::nothrow) QpProject();
            parserEnum = QpProjectCmd;
            reverseParserEnum = XcalarApiProject;
            break;

        case XcalarApiGetRowNum:
            this->cmdParsers[ii] = new (std::nothrow) QpGetRowNum();
            parserEnum = QpGetRowNumCmd;
            reverseParserEnum = XcalarApiGetRowNum;
            break;

        case XcalarApiFilter:
            this->cmdParsers[ii] = new (std::nothrow) QpFilter();
            parserEnum = QpFilterCmd;
            reverseParserEnum = XcalarApiFilter;
            break;

        case XcalarApiGroupBy:
            this->cmdParsers[ii] = new (std::nothrow) QpGroupBy();
            parserEnum = QpGroupByCmd;
            reverseParserEnum = XcalarApiGroupBy;
            break;

        case XcalarApiJoin:
            this->cmdParsers[ii] = new (std::nothrow) QpJoin();
            parserEnum = QpJoinCmd;
            reverseParserEnum = XcalarApiJoin;
            break;

        case XcalarApiUnion:
            this->cmdParsers[ii] = new (std::nothrow) QpUnion();
            parserEnum = QpUnionCmd;
            reverseParserEnum = XcalarApiUnion;
            break;

        case XcalarApiAggregate:
            this->cmdParsers[ii] = new (std::nothrow) QpAggregate();
            parserEnum = QpAggregateCmd;
            reverseParserEnum = XcalarApiAggregate;
            break;

        case XcalarApiMap:
            this->cmdParsers[ii] = new (std::nothrow) QpMap();
            parserEnum = QpMapCmd;
            reverseParserEnum = XcalarApiMap;
            break;

        case XcalarApiSynthesize:
            this->cmdParsers[ii] = new (std::nothrow) QpSynthesize();
            parserEnum = QpSynthesizeCmd;
            reverseParserEnum = XcalarApiSynthesize;
            break;

        case XcalarApiRenameNode:
            this->cmdParsers[ii] = new (std::nothrow) QpRename();
            parserEnum = QpRenameCmd;
            reverseParserEnum = XcalarApiRenameNode;
            break;

        case XcalarApiExport:
            this->cmdParsers[ii] = new (std::nothrow) QpExport();
            parserEnum = QpExportCmd;
            reverseParserEnum = XcalarApiExport;
            break;

        case XcalarApiDeleteObjects:
            this->cmdParsers[ii] = new (std::nothrow) QpDrop();
            parserEnum = QpDropCmd;
            reverseParserEnum = XcalarApiDeleteObjects;
            break;

        case XcalarApiDatasetDelete:
            this->cmdParsers[ii] = new (std::nothrow) QpDelist();
            parserEnum = QpDelistCmd;
            reverseParserEnum = XcalarApiDatasetDelete;
            break;

        case XcalarApiExecuteRetina:
            this->cmdParsers[ii] = new (std::nothrow) QpExecuteRetina();
            parserEnum = QpExecuteRetinaCmd;
            reverseParserEnum = XcalarApiExecuteRetina;
            break;

        case XcalarApiSelect:
            this->cmdParsers[ii] = new (std::nothrow) QpSelect();
            parserEnum = QpSelectCmd;
            reverseParserEnum = XcalarApiSelect;
            break;

        case XcalarApiFunctionInvalid:
            continue;
            break;

        default:
            assert(this->cmdParsers[ii] == NULL);
            continue;
            break;
        }

        if (this->cmdParsers[ii] == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate cmdParser for %s (%lu)",
                    strGetFromXcalarApis((XcalarApis) ii),
                    ii);
            status = StatusNoMem;
            goto CommonExit;
        }

        this->parserMap[parserEnum] = this->cmdParsers[ii];
        this->reverseParserMap[reverseParserEnum] = this->cmdParsers[ii];
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        for (ii = 0; ii < ArrayLen(cmdParsers); ii++) {
            if (cmdParsers[ii] != NULL) {
                delete cmdParsers[ii];
                cmdParsers[ii] = NULL;
            }
        }
    }
    return status;
}

void
QueryParser::destroy()
{
    uint64_t ii;

    if (instance == NULL) {
        return;
    }

    for (ii = 0; ii < ArrayLen(this->cmdParsers); ii++) {
        if (this->cmdParsers[ii] != NULL) {
            delete this->cmdParsers[ii];
            this->cmdParsers[ii] = NULL;
        }
    }

    delete this;
    instance = NULL;
}

QueryCmdParser *
QueryParser::getCmdParser(QueryParserEnum qp)
{
    assert(isValidQueryParserEnum(qp));
    if (isValidQueryParserEnum(qp) && this->parserMap[qp]->isValid()) {
        return this->parserMap[qp];
    } else {
        return NULL;
    }
}

QueryCmdParser *
QueryParser::getCmdParser(XcalarApis api)
{
    assert(isValidXcalarApis(api));
    if (isValidXcalarApis(api) && this->reverseParserMap[api]->isValid()) {
        return this->reverseParserMap[api];
    } else {
        return NULL;
    }
}

Status
QueryParser::reverseParse(Dag *queryGraph, json_t **queryOut)
{
    Status status = StatusUnknown;
    DagTypes::NodeId queryGraphNodeId;
    DagNodeTypes::Node *dagNode = NULL;
    json_error_t err;

    json_t *query = json_array();
    BailIfNull(query);

    assert(queryGraph != NULL);

    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    BailIfFailed(status);

    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        QueryCmdParser *cmdParser;

        status = queryGraph->lookupNodeById(queryGraphNodeId, &dagNode);
        if (status != StatusOk) {
            goto CommonExit;
        }

        cmdParser = getCmdParser(dagNode->dagNodeHdr.apiDagNodeHdr.api);
        if (cmdParser == NULL) {
            status = StatusInval;
            goto CommonExit;
        }

        status =
            cmdParser->reverseParse(dagNode->dagNodeHdr.apiDagNodeHdr.api,
                                    dagNode->dagNodeHdr.apiDagNodeHdr.comment,
                                    dagNode->dagNodeHdr.apiDagNodeHdr.tag,
                                    dagNode->dagNodeHdr.apiDagNodeHdr.state,
                                    dagNode->dagNodeHdr.apiInput,
                                    dagNode->annotations,
                                    &err,
                                    query);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to parse %s: %s, "
                          "source %s line %d, column %d, position %d: %s",
                          dagNode->dagNodeHdr.apiDagNodeHdr.name,
                          strGetFromStatus(status),
                          err.source,
                          err.line,
                          err.column,
                          err.position,
                          err.text);

            goto CommonExit;
        }

        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        BailIfFailed(status);
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (query) {
            json_decref(query);
            query = NULL;
        }
    } else {
        *queryOut = query;
    }

    return status;
}

Status
QueryParser::reverseParse(Dag *queryGraph,
                          char **queryStrOut,
                          size_t *queryStrLenOut)
{
    Status status;

    char *queryStr = NULL;
    json_t *query = NULL;

    status = reverseParse(queryGraph, &query);
    BailIfFailed(status);

    queryStr = json_dumps(query, 0);
    BailIfNull(queryStr);

CommonExit:
    if (query) {
        json_decref(query);
        query = NULL;
    }

    if (status == StatusOk) {
        *queryStrOut = queryStr;
        *queryStrLenOut = strlen(queryStr);
    }

    return status;
}

Status
QueryParser::parseWorkItem(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    unsigned ii;
    XcalarWorkItem *workItem = NULL;
    Status status = StatusOk;
    QueryCmdParser *cmdParser;
    QueryParserEnum qp;

    assert(argc > 0);
    assert(argv != NULL);

    for (ii = 0; ii < QueryParserEnumLen; ii++) {
        qp = (QueryParserEnum) ii;
        if (strcmp(argv[0], strGetFromQueryParserEnum(qp)) == 0) {
            cmdParser = getCmdParser(qp);
            assert(cmdParser != NULL);
            if (cmdParser == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Could not find cmdParser for \"%s\"",
                        argv[0]);
                status = StatusInval;
                goto CommonExit;
            }

            status = cmdParser->parse(argc, argv, &workItem);
            if (status != StatusOk) {
                assert(workItem == NULL);
                goto CommonExit;
            }
            break;
        }
    }

    if (ii == QueryParserEnumLen) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    assert(workItem != NULL);
    *workItemOut = workItem;

CommonExit:

    return status;
}

Status
QueryParser::cmdParserParse(const char *query, Dag *queryGraph)
{
    OperatorHandler *operatorHandler = NULL;
    CmdParserCursor *cmdParserCursor = NULL;
    Status status;
    int cliArgc = 0;
    char **cliArgv = NULL;
    XcalarWorkItem *workItem = NULL;
    Txn prevTxn = Txn();

    status = cmdInitParseCmdCursor(&cmdParserCursor);
    if (status != StatusOk) {
        goto CommonExit;
    }

    while (true) {
        if (prevTxn.valid()) {
            MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
            prevTxn = Txn();  // reset prevTxn for exit case
        }
        if (operatorHandler != NULL) {
            delete operatorHandler;
            operatorHandler = NULL;
        }

        status =
            cmdParseQuery((char *) query, &cliArgc, &cliArgv, cmdParserCursor);

        if (status == StatusNoData) {
            status = StatusOk;
            break;
        } else if (status != StatusOk && status != StatusMore) {
            goto CommonExit;
        }

        if (cliArgc == 0) {
            assert(cliArgv == NULL);
        } else {
            assert(cliArgv != NULL);
            status = parseWorkItem(cliArgc, cliArgv, &workItem);

            for (int ii = 0; ii < cliArgc; ii++) {
                assert(cliArgv[ii] != NULL);
                memFree(cliArgv[ii]);
                cliArgv[ii] = NULL;
            }
            memFree(cliArgv);
            cliArgv = NULL;
            if (status != StatusOk) {
                goto CommonExit;
            }

            assert(operatorHandler == NULL);
            status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                                   workItem,
                                                   NULL,
                                                   queryGraph,
                                                   false,
                                                   &prevTxn);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to get operator handler for api %s: %s",
                        strGetFromXcalarApis(workItem->api),
                        strGetFromStatus(status));
                goto CommonExit;
            }

            status =
                operatorHandler->createDagNode(NULL, DagTypes::InvalidGraph);
            if (status != StatusOk) {
                goto CommonExit;
            }

            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
        prevTxn = Txn();
    }
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (cmdParserCursor != NULL) {
        cmdFreeParseCmdCursor(&cmdParserCursor);
    }

    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    assert(cmdParserCursor == NULL);

    return status;
}

Status
QueryParser::jsonParse(const char *query, Dag *queryGraph)
{
    Status status = StatusOk;
    OperatorHandler *operatorHandler = NULL;
    XcalarWorkItem *workItem = NULL;
    Txn savedTxn;
    json_error_t err;
    json_t *root = json_loads(query, 0, &err);
    Txn prevTxn = Txn();

    if (root == NULL) {
        status = StatusJsonError;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to parse json query, "
                      "source %s line %d, column %d, position %d: %s",
                      err.source,
                      err.line,
                      err.column,
                      err.position,
                      err.text);
        goto CommonExit;
    }

    if (json_typeof(root) != JSON_ARRAY) {
        status = StatusJsonError;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to parse json query: root must be an array");
        goto CommonExit;
    }

    size_t ii;
    json_t *op, *opName, *args, *comment, *tag, *state, *annotations;
    const char *opNameStr;
    XcalarApis api;
    QueryCmdParser *qp;

    json_array_foreach (root, ii, op) {
        if (prevTxn.valid()) {
            MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
            prevTxn = Txn();  // reset prevTxn for exit case
        }
        opName = json_object_get(op, QueryCmdParser::OperationKey);
        if (opName == NULL) {
            status = StatusJsonQueryParseError;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to get %s, index %lu",
                          QueryCmdParser::OperationKey,
                          ii);
            goto CommonExit;
        }

        opNameStr = json_string_value(opName);
        if (opName == NULL) {
            status = StatusJsonQueryParseError;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to get %s, index %lu",
                          QueryCmdParser::OperationKey,
                          ii);
            goto CommonExit;
        }

        args = json_object_get(op, QueryCmdParser::ArgsKey);
        if (args == NULL) {
            status = StatusJsonQueryParseError;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to get %s, index %lu",
                          QueryCmdParser::ArgsKey,
                          ii);
            goto CommonExit;
        }

        api = strToXcalarApis(opNameStr);
        if (!isValidXcalarApis(api)) {
            status = StatusJsonQueryParseError;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "%s is not a valid operation name",
                          opNameStr);
            goto CommonExit;
        }

        qp = getCmdParser(api);
        assert(qp != NULL);

        status = qp->parseJson(args, &err, &workItem);
        if (status != StatusOk) {
            if (status == StatusJsonQueryParseError) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Failed to parse api %s, index %lu, "
                              "source %s line %d, column %d, position %d: %s",
                              opNameStr,
                              ii,
                              err.source,
                              err.line,
                              err.column,
                              err.position,
                              err.text);
            } else if (status == StatusLegacyTargetNotFound) {
                // This happens during legacy export target to export driver
                // upgrade. We ignore the export node in this case. We don't use
                // the `do nothing` export node, otherwise users may not notice
                // the failure until they run the dataflow.

                xSyslogTxnBuf(moduleName,
                              XlogWarn,
                              "Failed to upgrade legacy export target because "
                              "the orignal target wasn't found. The target "
                              "will be removed from the dataflow");

                status = StatusOk;
                continue;
            }
            goto CommonExit;
        }

        status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                               workItem,
                                               NULL,
                                               queryGraph,
                                               false,
                                               &prevTxn);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to get operator handler for api %s, index "
                          "%lu: %s",
                          opNameStr,
                          ii,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        status = operatorHandler->createDagNode(NULL, DagTypes::InvalidGraph);
        if (status == StatusDgDagAlreadyExists) {
            status = StatusOk;
        }

        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to create dag node for api %s, index %lu: %s",
                          opNameStr,
                          ii,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        comment = json_object_get(op, QueryCmdParser::CommentKey);
        if (comment != NULL) {
            const char *commentStr = json_string_value(comment);
            if (commentStr != NULL && strlen(commentStr) > 0) {
                status =
                    queryGraph->commentDagNodeLocalEx(operatorHandler
                                                          ->getDstNodeName(),
                                                      commentStr);
                assert(status == StatusOk);
            }
        }

        tag = json_object_get(op, QueryCmdParser::TagKey);
        if (tag != NULL) {
            const char *tagStr = json_string_value(tag);
            if (tagStr != NULL && strlen(tagStr) > 0) {
                status = queryGraph->tagDagNodeLocalEx(operatorHandler
                                                           ->getDstNodeName(),
                                                       tagStr);
                assert(status == StatusOk);
            }
        }

        state = json_object_get(op, QueryCmdParser::StateKey);
        if (state != NULL) {
            const char *stateStr = json_string_value(state);
            if (stateStr != NULL && strlen(stateStr) > 0) {
                DgDagState dagState = strToDgDagState(stateStr);
                DagTypes::NodeId dagNodeId;

                if (!isValidDgDagState(dagState)) {
                    status = StatusJsonQueryParseError;
                    // can't use StatusRetinaParseError since parse could be
                    // invoked for a query not in a retina, and
                    // StatusDagNodeNotFound isn't appropriate either since it
                    // does not indicate the error is hit during parsing, as
                    // opposed to during lookupNodeByName
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "Invalid state '%s' for api '%s': '%s'",
                                  stateStr,
                                  opNameStr,
                                  strGetFromStatus(status));
                    goto CommonExit;
                }

                status =
                    queryGraph->getDagNodeId(operatorHandler->getDstNodeName(),
                                             Dag::TableScope::LocalOnly,
                                             &dagNodeId);
                assert(status == StatusOk);

                status = queryGraph->changeDagNodeState(dagNodeId, dagState);
                assert(status == StatusOk);
            }
        }

        annotations = json_object_get(op, QueryCmdParser::AnnotationsKey);
        if (annotations != NULL) {
            DagNodeTypes::Node *node;
            status =
                queryGraph->lookupNodeByName(operatorHandler->getDstNodeName(),
                                             &node,
                                             Dag::TableScope::LocalOnly);
            assert(status == StatusOk);

            status = Optimizer::get()->parseAnnotations(annotations,
                                                        &node->annotations);
            BailIfFailed(status);
        }

        // operatorHandler must be destroyed before workItem is freed!!
        // It may reference state in the operatorHandler (e.g. for
        // OperatorHandlerExecuteRetina, it's the input_ structure) which is
        // freed by xcalarApiFreeWorkItem (which frees workItem->input among
        // other things).
        if (operatorHandler != NULL) {
            delete operatorHandler;
            operatorHandler = NULL;
        }

        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
        prevTxn = Txn();
    }
    if (root != NULL) {
        json_decref(root);
        root = NULL;
    }

    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

Status
QueryParser::parse(const char *query,
                   XcalarApiUdfContainer *udfContainer,
                   Dag **queryGraphOut,
                   uint64_t *numQueryGraphNodesOut)
{
    Status status = StatusUnknown;
    Dag *queryGraph = NULL;
    DagLib *dagLib = DagLib::get();

    assert(query != NULL);
    assert(queryGraphOut != NULL);

    // XXX We need to be able to create a local dag handle
    status = dagLib->createNewDag(128,
                                  DagTypes::QueryGraph,
                                  udfContainer,
                                  &queryGraph);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (query[0] == '[') {
        status = jsonParse(query, queryGraph);
    } else {
        status = cmdParserParse(query, queryGraph);
    }

CommonExit:
    if (status != StatusOk) {
        if (queryGraph != NULL) {
            Status status2 =
                dagLib->destroyDag(queryGraph, DagTypes::DestroyDeleteNodes);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to destroy Dag '%lu': %s",
                        queryGraph->getId(),
                        strGetFromStatus(status2));
            }
            queryGraph = NULL;
        }
    } else {
        *numQueryGraphNodesOut = queryGraph->getNumNode();
        *queryGraphOut = queryGraph;
    }
    return status;
}

Status
QueryCmdParser::reverseParse(XcalarApis api,
                             const char *comment,
                             const char *tag,
                             DgDagState state,
                             const XcalarApiInput *input,
                             void *annotations,
                             json_error_t *err,
                             json_t *query)
{
    Status status = StatusOk;
    int ret;
    json_t *args = NULL, *op = NULL, *annotationsJson = NULL;

    status = reverseParse(input, err, &args);
    BailIfFailed(status);

    annotationsJson = json_object();
    BailIfNull(annotationsJson);

    if (annotations) {
        status =
            Optimizer::reverseParseAnnotations(annotations, annotationsJson);
        BailIfFailed(status);
    }

    op = json_pack_ex(err,
                      0,
                      JsonOpFormatString,
                      OperationKey,
                      strGetFromXcalarApis(api),
                      CommentKey,
                      comment,
                      TagKey,
                      tag,
                      StateKey,
                      strGetFromDgDagState(state),
                      ArgsKey,
                      args,
                      AnnotationsKey,
                      annotationsJson);
    args = NULL;
    annotationsJson = NULL;
    BailIfNullWith(op, StatusJsonQueryParseError);

    ret = json_array_append_new(query, op);
    BailIfFailedWith(ret, StatusJsonQueryParseError);
    op = NULL;

CommonExit:
    if (args) {
        json_decref(args);
        args = NULL;
    }

    if (op) {
        json_decref(op);
        op = NULL;
    }

    if (annotationsJson) {
        json_decref(annotationsJson);
        annotationsJson = NULL;
    }

    return status;
}

Status
QueryCmdParser::parseColumnsArray(json_t *columns,
                                  json_error_t *err,
                                  XcalarApiRenameMap *columnsOut,
                                  unsigned numKeys,
                                  const char *keyNames[])
{
    unsigned ii;
    json_t *val;
    int ret;
    Status status;

    json_array_foreach (columns, ii, val) {
        const char *type = "DfUnknown", *oldName = "", *newName = "";

        ret = json_unpack_ex(val,
                             err,
                             0,
                             JsonUnpackColumnFormatString,
                             SourceColumnKey,
                             &oldName,
                             DestColumnKey,
                             &newName,
                             ColumnTypeKey,
                             &type);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        columnsOut[ii].type = strToDfFieldType(type);
        if (!isValidDfFieldType(columnsOut[ii].type)) {
            status = StatusInval;
            BailIfFailedTxnMsg(moduleName,
                               StatusInval,
                               "%s is not a valid field type",
                               type);
        }

        status = strStrlcpy(columnsOut[ii].oldName,
                            oldName,
                            sizeof(columnsOut[ii].oldName));
        BailIfFailed(status);

        if (strlen(newName) == 0) {
            newName = oldName;
        }

        strlcpy(columnsOut[ii].newName,
                newName,
                sizeof(columnsOut[ii].newName));
        BailIfFailed(status);

        // check to see if this column is a key
        columnsOut[ii].isKey = false;
        for (unsigned kk = 0; kk < numKeys; kk++) {
            if (strcmp(columnsOut[ii].oldName, keyNames[kk]) == 0) {
                columnsOut[ii].isKey = true;
            }
        }
    }

CommonExit:
    return status;
}
