// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <jansson.h>
#include <google/protobuf/arena.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "dag/DagLib.h"
#include "xdb/TableTypes.h"
#include "ns/LibNs.h"
#include "dag/DagTypes.h"
#include "queryparser/QueryParser.h"
#include "queryeval/QueryEvaluate.h"
#include "msg/Message.h"
#include "bc/BufferCache.h"
#include "dataset/Dataset.h"
#include "hash/Hash.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "optimizer/Optimizer.h"
#include "df/DataFormat.h"
#include "strings/String.h"
#include "udf/UserDefinedFunction.h"
#include "operators/XcalarEval.h"
#include "common/Version.h"
#include "log/Log.h"
#include "util/IntHashTable.h"
#include "LibDagConstants.h"
#include "msg/Xid.h"
#include "dag/DagNodeTypes.h"
#include "querymanager/QueryManager.h"
#include "DurableVersions.h"
#include "subsys/DurableDag.pb.h"
#include "durable/Durable.h"
#include "export/DataTarget.h"
#include "session/Sessions.h"
#include "WorkItem.h"
#include "libapis/LibApisCommon.h"
#include "util/ProtoWrap.h"
#include "usr/Users.h"

using namespace dagLib;
using namespace xcalar::internal;

extern const char *ExDefaultExportTargetName;

#define MakeRetinaParamHdrSize(makeRetinaParam) \
    (sizeof(*(makeRetinaParam)) - sizeof((makeRetinaParam)->retinaInput))

static const char *moduleName = "libdag::retina";

Status
DagLib::convertSynthesizeNodes(Dag *dstDag,
                               Dag *srcDag,
                               XcalarApiMakeRetinaInput *retinaInput)
{
    Status status = StatusOk;
    DagNodeTypes::Node *srcNode;
    bool refAcquired = false;
    DagTypes::NodeId srcId = XidInvalid;
    XdbMeta *srcMeta = NULL;
    XcalarWorkItem *workItem = NULL;
    unsigned numColumns = 0;
    XcalarApiRenameMap *columns = NULL;
    TableNsMgr::TableHandleTrack handleTrack;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    XcalarApiUdfContainer *srcSessionContainer = srcDag->getSessionContainer();

    for (unsigned ii = 0; ii < retinaInput->numSrcTables; ii++) {
        // Find the source node in the supplied dstDag
        status = dstDag->lookupNodeByName(retinaInput->srcTables[ii].dstName,
                                          &srcNode,
                                          Dag::TableScope::LocalOnly,
                                          true);
        BailIfFailed(status);

        srcId = srcNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;

        // grab the ref as we are touching the backing xdb meta
        status = dstDag->getDagNodeRefById(srcId);
        BailIfFailed(status);

        refAcquired = true;

        status = dstDag->getTableIdFromNodeId(srcId, &handleTrack.tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getTableIdFromNodeId for dagNode %lu: %s",
                        srcId,
                        strGetFromStatus(status));

        if (tnsMgr->isTableIdValid(handleTrack.tableId)) {
            status = tnsMgr->openHandleToNs(srcSessionContainer,
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
        }

        // pull it's current schema from the xdbMeta if it has one.
        // this is saved in the synthesize input
        status = XdbMgr::get()->xdbGet(srcNode->xdbId, NULL, &srcMeta);

        srcNode->dagNodeHdr.apiDagNodeHdr.api = XcalarApiSynthesize;

        // generate a dummy synthesizeInput, this will be fixed in the optimizer
        if (srcMeta != NULL) {
            // if source was a table, save off the original schema
            numColumns = srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();

            columns =
                (XcalarApiRenameMap *) memAlloc(numColumns * sizeof(*columns));
            BailIfNull(columns);

            for (unsigned ii = 0; ii < numColumns; ii++) {
                status = strStrlcpy(columns[ii].oldName,
                                    srcMeta->kvNamedMeta.valueNames_[ii],
                                    sizeof(columns[ii].oldName));
                BailIfFailed(status);
                status = strStrlcpy(columns[ii].newName,
                                    srcMeta->kvNamedMeta.valueNames_[ii],
                                    sizeof(columns[ii].newName));
                BailIfFailed(status);
                columns[ii].type =
                    srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii);
                columns[ii].isKey = false;
            }
        } else {
            numColumns = 0;
            columns = NULL;
        }

        workItem =
            xcalarApiMakeSynthesizeWorkItem(retinaInput->srcTables[ii]
                                                .source.name,
                                            retinaInput->srcTables[ii].dstName,
                                            false,
                                            numColumns,
                                            columns);
        BailIfNull(workItem);

        if (columns) {
            memFree(columns);
            columns = NULL;
        }

        Dag::dagApiFree(srcNode);
        srcNode->dagNodeHdr.apiInput = Dag::dagApiAlloc(workItem->inputSize);
        srcNode->dagNodeHdr.apiDagNodeHdr.inputSize = workItem->inputSize;
        BailIfNull(srcNode->dagNodeHdr.apiInput);
        status = Dag::sparseMemCpy(srcNode->dagNodeHdr.apiInput,
                                   workItem->input,
                                   workItem->inputSize);
        BailIfFailed(status);

        if (workItem) {
            if (workItem->input) {
                memFree(workItem->input);
                workItem->input = NULL;
            }
            memFree(workItem);
            workItem = NULL;
        }

        // Synthesize does not have any parents within the same graph,
        // remove any parents associated with the previous operation

        DagNodeTypes::NodeIdListElt *parentElt;
        parentElt = srcNode->parentsList;
        while (parentElt != NULL) {
            DagNodeTypes::Node *parentNode;

            // remove yourself from parent's children
            status = dstDag->lookupNodeById(parentElt->nodeId, &parentNode);
            assert(status == StatusOk);

            assert(parentNode->numChild > 0);
            dstDag->removeNodeFromIdList(&parentNode->childrenList, srcNode);
            parentNode->numChild--;

            // remove parent from yourself
            srcNode->parentsList = parentElt->next;
            delete parentElt;
            parentElt = srcNode->parentsList;
        }

        srcNode->numParent = 0;

        dstDag->putDagNodeRefById(srcId);
        refAcquired = false;

        if (handleTrack.tableHandleValid) {
            tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
            handleTrack.tableHandleValid = false;
        }
    }

    status = StatusOk;
CommonExit:
    if (columns) {
        memFree(columns);
        columns = NULL;
    }

    if (workItem) {
        if (workItem->input) {
            memFree(workItem->input);
            workItem->input = NULL;
        }
        memFree(workItem);
        workItem = NULL;
    }

    if (refAcquired) {
        dstDag->putDagNodeRefById(srcId);
        refAcquired = false;
    }

    if (handleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
        handleTrack.tableHandleValid = false;
    }
    return status;
}

Status
DagLib::makeDefaultExportParams(const char *exportName, char **paramString)
{
    Status status;
    *paramString = NULL;

    // This may change in the future, so for right now we won't expose the
    // caller to the fact that this is actually a constant
    *paramString = strAllocAndCopy("{}");
    BailIfNull(*paramString);

CommonExit:
    if (status != StatusOk) {
        if (*paramString) {
            memFree(*paramString);
            *paramString = NULL;
        }
    }
    return status;
}

Status
DagLib::appendExportNode(Dag *dag, unsigned numTables, RetinaDst **tableArray)
{
    XcalarApiInput *apiInput = NULL;
    XcalarApiExportInput *exportInput = NULL;
    size_t exportInputSize;
    uint64_t ii;
    int jj;
    size_t ret;
    Status status = StatusUnknown;
    DagTypes::NodeId targetNodeId;
    char *driverParams = NULL;

    status = StatusOk;
    for (ii = 0; ii < numTables; ++ii) {
        status = dag->getDagNodeId(tableArray[ii]->target.name,
                                   Dag::TableScope::LocalOnly,
                                   &targetNodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get dagNodeId for node \"%s\": %s",
                    tableArray[ii]->target.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // check if export node already exists before appending
        DagNodeTypes::Node *exportNode;
        char exportName[TableMaxNameLen + 1];
        snprintf(exportName,
                 sizeof(exportName),
                 "%s%s",
                 XcalarApiLrqExportPrefix,
                 tableArray[ii]->target.name);

        status = dag->lookupNodeByName(exportName,
                                       &exportNode,
                                       Dag::TableScope::LocalOnly,
                                       true);
        if (status == StatusOk) {
            tableArray[ii]->target.nodeId =
                exportNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;

            status = strStrlcpy(tableArray[ii]->target.name,
                                exportName,
                                sizeof(tableArray[ii]->target.name));
            BailIfFailed(status);
            continue;
        }

        status = dag->getDagNodeId(tableArray[ii]->target.name,
                                   Dag::TableScope::LocalOnly,
                                   &targetNodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get dagNodeId for node \"%s\": %s",
                    tableArray[ii]->target.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        exportInputSize =
            sizeof(*exportInput) +
            (tableArray[ii]->numColumns * sizeof(exportInput->meta.columns[0]));
        // Leave apiInput on heap; it's only used for src copy in
        // createSingleNode
        apiInput = (XcalarApiInput *) memAlloc(exportInputSize);
        if (apiInput == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        exportInput = &apiInput->exportInput;
        assert((uintptr_t) exportInput == (uintptr_t) apiInput);

        strlcpy(exportInput->exportName,
                exportName,
                sizeof(exportInput->exportName));

        ret = strlcpy(exportInput->srcTable.tableName,
                      tableArray[ii]->target.name,
                      sizeof(exportInput->srcTable.tableName));
        if (ret >= sizeof(exportInput->srcTable.tableName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error: %s is too long (%lu bytes). Maximum is %lu bytes",
                    tableArray[ii]->target.name,
                    strlen(tableArray[ii]->target.name) + 1,
                    sizeof(exportInput->srcTable.tableName));
            status = StatusOverflow;
            goto CommonExit;
        }

        // XXX maybe do not hard code this ?
        ret = strlcpy(exportInput->meta.driverName,
                      "do_nothing",
                      sizeof(exportInput->meta.driverName));
        assert(ret < sizeof(exportInput->meta.driverName));

        status =
            makeDefaultExportParams(tableArray[ii]->target.name, &driverParams);
        BailIfFailed(status);
        assert(driverParams != NULL && strlen(driverParams) > 0);

        ret = strlcpy(exportInput->meta.driverParams,
                      driverParams,
                      sizeof(exportInput->meta.driverParams));
        if (ret >= sizeof(exportInput->meta.driverParams)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error: '%s' is too long (%lu bytes). Maximum is %lu "
                    "bytes",
                    driverParams,
                    strlen(driverParams),
                    sizeof(exportInput->meta.driverParams));
            status = StatusOverflow;
            goto CommonExit;
        }
        assert(ret < sizeof(exportInput->meta.driverParams));

        exportInput->meta.numColumns = tableArray[ii]->numColumns;
        for (jj = 0; jj < tableArray[ii]->numColumns; jj++) {
            ret = strlcpy(exportInput->meta.columns[jj].name,
                          tableArray[ii]->columns[jj].name,
                          sizeof(exportInput->meta.columns[jj].name));
            if (ret >= sizeof(exportInput->meta.columns[jj].name)) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error: %s is too long (%lu bytes). Maximum is %lu "
                        "bytes",
                        tableArray[ii]->columns[jj].name,
                        strlen(tableArray[ii]->columns[jj].name) + 1,
                        sizeof(exportInput->meta.columns[jj].name));
                status = StatusOverflow;
                goto CommonExit;
            }

            ret = strlcpy(exportInput->meta.columns[jj].headerAlias,
                          tableArray[ii]->columns[jj].headerAlias,
                          sizeof(exportInput->meta.columns[jj].headerAlias));
            if (ret >= sizeof(exportInput->meta.columns[jj].headerAlias)) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error: %s is too long (%lu bytes). Maximum is %lu "
                        "bytes",
                        tableArray[ii]->columns[jj].headerAlias,
                        strlen(tableArray[ii]->columns[jj].headerAlias) + 1,
                        sizeof(exportInput->meta.columns[jj].headerAlias));
                status = StatusOverflow;
                goto CommonExit;
            }
        }

        status = strStrlcpy(tableArray[ii]->target.name,
                            exportName,
                            sizeof(tableArray[ii]->target.name));
        BailIfFailed(status);

        status = dag->appendExportDagNode(tableArray[ii]->target.nodeId,
                                          exportName,
                                          targetNodeId,
                                          exportInputSize,
                                          apiInput);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to append export node %lu (parent %lu) "
                    "table name '%s': %s",
                    tableArray[ii]->target.nodeId,
                    targetNodeId,
                    exportName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        exportInput = NULL;
    }

CommonExit:

    if (apiInput != NULL) {
        memFree(apiInput);
        apiInput = NULL;
    }
    if (driverParams != NULL) {
        memFree(driverParams);
        driverParams = NULL;
    }

    return status;
}

void
DagLib::destroyRetinaInfo(RetinaInfo *retinaInfo)
{
    uint64_t ii;

    assert(retinaInfo != NULL);

    if (retinaInfo->numTables > 0) {
        assert(retinaInfo->tableArray != NULL);
        for (ii = 0; ii < retinaInfo->numTables; ii++) {
            assert(retinaInfo->tableArray[ii] != NULL);
            memFree(retinaInfo->tableArray[ii]);
            retinaInfo->tableArray[ii] = NULL;
        }
    }

    if (retinaInfo->tableArray != NULL) {
        memFree(retinaInfo->tableArray);
        retinaInfo->tableArray = NULL;
    }

    if (retinaInfo->columnHints != NULL) {
        memFree(retinaInfo->columnHints);
        retinaInfo->columnHints = NULL;
    }

    if (retinaInfo->numUdfModules > 0) {
        assert(retinaInfo->udfModulesArray != NULL);
        for (ii = 0; ii < retinaInfo->numUdfModules; ii++) {
            assert(retinaInfo->udfModulesArray[ii] != NULL);
            memFree(retinaInfo->udfModulesArray[ii]);
            retinaInfo->udfModulesArray[ii] = NULL;
        }
    }

    if (retinaInfo->udfModulesArray != NULL) {
        memFree(retinaInfo->udfModulesArray);
        retinaInfo->udfModulesArray = NULL;
    }

    if (retinaInfo->queryStr != NULL) {
        memFree(retinaInfo->queryStr);
        retinaInfo->queryStr = NULL;
    }

    if (retinaInfo->queryGraph != NULL) {
        Status status2 =
            DagLib::get()->destroyDag(retinaInfo->queryGraph,
                                      DagTypes::DestroyDeleteNodes);
        assert(status2 == StatusOk);
        retinaInfo->queryGraph = NULL;
    }

    memFree(retinaInfo);
    retinaInfo = NULL;
}

Status
DagLib::allocRetinaInfo(const char *retinaName, RetinaInfo **retinaInfoOut)
{
    RetinaInfo *retinaInfo = NULL;
    Status status = StatusOk;
    size_t ret;

    assert(retinaName != NULL);
    assert(retinaInfoOut != NULL);

    retinaInfo = (RetinaInfo *) memAlloc(sizeof(*retinaInfo));
    BailIfNull(retinaInfo);

    retinaInfo->queryGraph = NULL;
    retinaInfo->numTables = 0;
    retinaInfo->tableArray = NULL;
    retinaInfo->numUdfModules = 0;
    retinaInfo->udfModulesArray = NULL;
    retinaInfo->columnHints = NULL;
    retinaInfo->numColumnHints = 0;
    retinaInfo->queryStr = NULL;

    ret = strlcpy(retinaInfo->retinaDesc.retinaName,
                  retinaName,
                  sizeof(retinaInfo->retinaDesc.retinaName));
    if (ret >= sizeof(retinaInfo->retinaDesc.retinaName)) {
        xSyslog(moduleName, XlogErr, "Dataflow Name too long");
        status = StatusOverflow;
        goto CommonExit;
    }

    *retinaInfoOut = retinaInfo;

CommonExit:

    if (status != StatusOk) {
        if (retinaInfo != NULL) {
            destroyRetinaInfo(retinaInfo);
            retinaInfo = NULL;
        }
    }

    return status;
}

Status
DagLib::populateUdfModulesArray(json_t *jsonRecord,
                                const char *retinaName,
                                RetinaInfo *retinaInfo,
                                ArchiveManifest *manifest,
                                const json_t *archiveChecksum)
{
    Status status = StatusUnknown;
    json_t *jsonUdfsArray, *jsonUdf, *jsonUdfModuleName, *jsonUdfFileName;
    json_t *jsonUdfType;
    uint64_t numUdfModulesPopulated = 0, ii;
    uint64_t numUdfModules = 0;
    UdfModuleSrc **udfModulesArray = NULL;

    assert(jsonRecord != NULL);
    assert(retinaName != NULL);
    assert(retinaInfo != NULL);
    assert(manifest != NULL);

    jsonUdfsArray = json_object_get(jsonRecord, "udfs");
    if (jsonUdfsArray == NULL) {
        // No udfs required. Not an error
        status = StatusOk;
        goto CommonExit;
    }

    if (!json_is_array(jsonUdfsArray)) {
        // If the field UDF is present though, then it has to be an array
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": udfs field is not an array",
                retinaName);
        status = StatusRetinaParseError;
        goto CommonExit;
    }

    numUdfModules = json_array_size(jsonUdfsArray);
    udfModulesArray =
        (UdfModuleSrc **) memAlloc(sizeof(*udfModulesArray) * numUdfModules);
    if (udfModulesArray == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < numUdfModules; ii++) {
        void *udfSrc;
        size_t udfSrcSize, udfDstSize;
        const char *udfModuleName;
        const char *udfFileName;
        const char *udfTypeStr;
        UdfType udfType;
        bool needNullTerminator = false;

        jsonUdf = json_array_get(jsonUdfsArray, ii);
        if (jsonUdf == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not retrieve udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonUdfModuleName = json_object_get(jsonUdf, "moduleName");
        if (jsonUdfModuleName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not get moduleName in udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        udfModuleName = json_string_value(jsonUdfModuleName);
        if (udfModuleName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Module name is not a valid string "
                    "in udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonUdfFileName = json_object_get(jsonUdf, "fileName");
        if (jsonUdfFileName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not get fileName in udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        udfFileName = json_string_value(jsonUdfFileName);
        if (udfFileName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Filename is not a valid string in "
                    "udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonUdfType = json_object_get(jsonUdf, "udfType");
        if (jsonUdfType == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not get udfType in udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        udfTypeStr = json_string_value(jsonUdfType);
        if (udfTypeStr == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": udfType is not a valid string in "
                    "udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        udfType = strToUdfType(udfTypeStr);
        if (!isValidUdfType(udfType)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Unknown udfType \"%s\" in udf %lu",
                    retinaName,
                    udfTypeStr,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        status = archiveGetFileData(manifest,
                                    udfFileName,
                                    &udfSrc,
                                    &udfSrcSize,
                                    archiveChecksum);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving %s (%s)",
                    udfFileName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        udfDstSize = udfSrcSize;
        if (((char *) udfSrc)[udfSrcSize - 1] != '\0') {
            needNullTerminator = true;
            udfDstSize++;
        }

        if (udfDstSize > (XcalarApiMaxUdfSourceLen + 1)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": module source is too big",
                    udfFileName);
            status = StatusOverflow;
            goto CommonExit;
        }

        udfModulesArray[ii] = (UdfModuleSrc *) memAlloc(
            sizeof(*udfModulesArray[ii]) + udfDstSize);
        if (udfModulesArray[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numUdfModulesPopulated = ii + 1;

        udfModulesArray[ii]->type = udfType;

        // Built-in modules must be loaded at start.
        udfModulesArray[ii]->isBuiltin = false;

        size_t ret;
        ret = strlcpy(udfModulesArray[ii]->moduleName,
                      udfModuleName,
                      sizeof(udfModulesArray[ii]->moduleName));
        if (ret >= sizeof(udfModulesArray[ii]->moduleName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": moduleName is too long",
                    udfModuleName);
            status = StatusOverflow;
            goto CommonExit;
        }

        if (needNullTerminator) {
            memcpy(udfModulesArray[ii]->source, udfSrc, udfSrcSize);
            udfModulesArray[ii]->source[udfSrcSize] = '\0';
            ret = udfSrcSize;
        } else {
            // strlcpy should have NULL-terminated it
            ret = strlcpy(udfModulesArray[ii]->source,
                          (char *) udfSrc,
                          udfDstSize);
        }

        if (ret >= udfDstSize) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Length of src (%lu chars) longer "
                    "than expected. Expected %lu chars",
                    udfModuleName,
                    ret,
                    udfDstSize);
            status = StatusUdfModuleInvalidSource;
            goto CommonExit;
        }

        udfModulesArray[ii]->sourceSize =
            strlen((char *) udfModulesArray[ii]->source) + 1;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (udfModulesArray != NULL) {
            for (ii = 0; ii < numUdfModulesPopulated; ii++) {
                assert(udfModulesArray[ii] != NULL);
                memFree(udfModulesArray[ii]);
                udfModulesArray[ii] = NULL;
            }
            memFree(udfModulesArray);
            udfModulesArray = NULL;
        }
    } else {
        retinaInfo->numUdfModules = numUdfModules;
        retinaInfo->udfModulesArray = udfModulesArray;
        udfModulesArray = NULL;
    }

    assert(udfModulesArray == NULL);
    return status;
}
Status
DagLib::populateJsonSchemaHints(const char *retinaName,
                                json_t *retinaInfoJson,
                                Dag::DagNodeListElt *loadNodesListAnchor,
                                unsigned numColumnHints,
                                Column *columnHints)
{
    Status status = StatusOk;
    int ret;
    json_t *jsonSchemaHints = NULL, *jsonColumn = NULL, *jsonColumnName = NULL,
           *jsonColumnType = NULL;
    Dag::DagNodeListElt *loadNodeListElt;
    ParseArgs *parseArgs;
    XdbLoadArgs *loadArgs;

    jsonSchemaHints = json_array();
    BailIfNull(jsonSchemaHints);

    loadNodeListElt = loadNodesListAnchor->next;
    while (loadNodeListElt != loadNodesListAnchor) {
        parseArgs = &loadNodeListElt->node->dagNodeHdr.apiInput->loadInput
                         .loadArgs.parseArgs;

        loadArgs = &loadNodeListElt->node->dagNodeHdr.apiInput->loadInput
                        .loadArgs.xdbLoadArgs;

        for (unsigned ii = 0; ii < parseArgs->fieldNamesCount; ii++) {
            jsonColumn = json_object();
            BailIfNull(jsonColumn);

            jsonColumnName = json_string(parseArgs->fieldNames[ii]);
            BailIfNull(jsonColumnName);

            jsonColumnType = json_string(
                strGetFromDfFieldType(loadArgs->valueDesc.valueType[ii]));
            BailIfNull(jsonColumnType);

            ret = json_object_set_new(jsonColumn, "columnName", jsonColumnName);
            if (ret != 0) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to set "
                        "\"name\" attribute",
                        retinaName);
                goto CommonExit;
            }
            jsonColumnName = NULL;

            ret = json_object_set_new(jsonColumn, "type", jsonColumnType);
            if (ret != 0) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to set "
                        "\"type\" attribute",
                        retinaName);
                goto CommonExit;
            }
            jsonColumnType = NULL;

            ret = json_array_append_new(jsonSchemaHints, jsonColumn);
            if (ret != 0) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to add jsonColumn to "
                        "jsonSchemaHints",
                        retinaName);
                goto CommonExit;
            }
            jsonColumn = NULL;
        }

        loadNodeListElt = loadNodeListElt->next;
    }

    for (unsigned ii = 0; ii < numColumnHints; ii++) {
        jsonColumn = json_object();
        BailIfNull(jsonColumn);

        jsonColumnName = json_string(columnHints[ii].columnName);
        BailIfNull(jsonColumnName);

        jsonColumnType =
            json_string(strGetFromDfFieldType(columnHints[ii].type));
        BailIfNull(jsonColumnType);

        ret = json_object_set_new(jsonColumn, "columnName", jsonColumnName);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to set "
                    "\"name\" attribute",
                    retinaName);
            goto CommonExit;
        }
        jsonColumnName = NULL;

        ret = json_object_set_new(jsonColumn, "type", jsonColumnType);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to set "
                    "\"type\" attribute",
                    retinaName);
            goto CommonExit;
        }
        jsonColumnType = NULL;

        ret = json_array_append_new(jsonSchemaHints, jsonColumn);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to add jsonColumn to "
                    "jsonSchemaHints",
                    retinaName);
            goto CommonExit;
        }
        jsonColumn = NULL;
    }

    ret = json_object_set_new(retinaInfoJson, "schema hints", jsonSchemaHints);
    if (ret != 0) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": "
                "Failed to set \"schema hints\" attribute",
                retinaName);
        goto CommonExit;
    }

    jsonSchemaHints = NULL;  // Ref handed to retinaInfoJson

CommonExit:
    if (jsonColumnName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumnName);
        jsonColumnName = NULL;
    }

    if (jsonColumnType != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumnName);
        jsonColumnName = NULL;
    }

    if (jsonColumn != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumn);
        jsonColumn = NULL;
    }

    if (jsonSchemaHints != NULL) {
        assert(status != StatusOk);
        json_decref(jsonSchemaHints);
        jsonSchemaHints = NULL;
    }

    return status;
}

Status
DagLib::populateSchemaHints(json_t *jsonRecord,
                            const char *retinaName,
                            RetinaInfo *retinaInfo)
{
    Status status = StatusUnknown;
    json_t *jsonSchemaHints, *jsonColumn, *jsonColumnName;
    json_t *jsonColumnType;
    uint64_t ii;
    uint64_t numColumns = 0;
    Column *columns = NULL;

    assert(jsonRecord != NULL);
    assert(retinaName != NULL);
    assert(retinaInfo != NULL);

    jsonSchemaHints = json_object_get(jsonRecord, "schema hints");
    if (jsonSchemaHints == NULL) {
        // No schema hints required. Not an error
        status = StatusOk;
        goto CommonExit;
    }

    if (!json_is_array(jsonSchemaHints)) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": schema hints field is not an array",
                retinaName);
        status = StatusRetinaParseError;
        goto CommonExit;
    }

    numColumns = json_array_size(jsonSchemaHints);
    columns = (Column *) memAlloc(sizeof(*columns) * numColumns);
    if (columns == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < numColumns; ii++) {
        const char *columnName;
        const char *columnTypeStr;
        DfFieldType columnType;

        jsonColumn = json_array_get(jsonSchemaHints, ii);
        if (jsonColumn == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not retrieve udf %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonColumnName = json_object_get(jsonColumn, "columnName");
        if (jsonColumnName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not get columnName %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        columnName = json_string_value(jsonColumnName);
        if (columnName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Column name is not a valid string "
                    "in column %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonColumnType = json_object_get(jsonColumn, "type");
        if (jsonColumnType == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not get columnType %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        columnTypeStr = json_string_value(jsonColumnType);
        if (columnTypeStr == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": columnType is not a valid string in "
                    "column %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        columnType = strToDfFieldType(columnTypeStr);
        if (!isValidDfFieldType(columnType)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Unknown columnType \"%s\"",
                    retinaName,
                    columnTypeStr);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        columns[ii].type = columnType;

        size_t ret;
        ret = strlcpy((char *) columns[ii].columnName,
                      columnName,
                      sizeof(columns[ii].columnName));
        if (ret >= sizeof(columns[ii].columnName)) {
            xSyslog(columnName,
                    XlogErr,
                    "Error parsing \"%s\": columnName is too long",
                    columnName);
            status = StatusOverflow;
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (columns != NULL) {
            memFree(columns);
            columns = NULL;
        }
    } else {
        retinaInfo->numColumnHints = numColumns;
        retinaInfo->columnHints = columns;
        columns = NULL;
    }

    assert(columns == NULL);
    return status;
}

Status
DagLib::populateTableArray(json_t *jsonRecord,
                           const char *retinaName,
                           RetinaInfo *retinaInfo)
{
    Status status = StatusUnknown;
    json_t *jsonTableArray, *jsonTable, *jsonTableName, *jsonTableColumns;
    json_t *jsonColumn, *jsonColumnName, *jsonColumnAlias;
    uint64_t numTables, numTablesPopulated, ii, jj;
    size_t ret;
    RetinaDst **tableArray = NULL;

    assert(jsonRecord != NULL);
    assert(retinaName != NULL);
    assert(retinaInfo != NULL);

    jsonTableArray = json_object_get(jsonRecord, "tables");
    if (jsonTableArray == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": Could not retrieve set of tables in "
                "dataflowInfo.json",
                retinaName);
        status = StatusRetinaParseError;
        goto CommonExit;
    }

    if (!json_is_array(jsonTableArray)) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": Tables field is not an array",
                retinaName);
        status = StatusRetinaParseError;
        goto CommonExit;
    }

    numTables = json_array_size(jsonTableArray);
    tableArray = (RetinaDst **) memAlloc(sizeof(*tableArray) * numTables);
    if (tableArray == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    numTablesPopulated = 0;
    for (ii = 0; ii < numTables; ii++) {
        uint64_t numColumns;
        const char *tableName;

        jsonTable = json_array_get(jsonTableArray, ii);
        if (jsonTable == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not retrieve table %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonTableName = json_object_get(jsonTable, "name");
        if (jsonTableName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not find table name in table "
                    "%lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        tableName = json_string_value(jsonTableName);
        if (tableName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Table name is not a valid string in "
                    "table %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        jsonTableColumns = json_object_get(jsonTable, "columns");
        if (jsonTableColumns == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Could not find table columns in "
                    "table %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        if (!json_is_array(jsonTableColumns)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Columns is not an array in "
                    "table %lu",
                    retinaName,
                    ii);
            status = StatusRetinaParseError;
            goto CommonExit;
        }

        numColumns = json_array_size(jsonTableColumns);
        if (numColumns > TupleMaxNumValuesPerRecord) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": numColumns is too big in table %lu "
                    "(Max %d)",
                    retinaName,
                    ii,
                    TupleMaxNumValuesPerRecord);
            status = StatusOverflow;
            goto CommonExit;
        }
        if (numColumns == 0) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": numColumns cannot be 0 in table %lu",
                    retinaName,
                    ii);
            status = StatusExportNoColumns;
            goto CommonExit;
        }

        tableArray[ii] = (RetinaDst *) memAlloc(
            sizeof(*tableArray[ii]) +
            (sizeof(tableArray[ii]->columns[0]) * numColumns));
        if (tableArray[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numTablesPopulated = (ii + 1);

        ret = strlcpy(tableArray[ii]->target.name,
                      tableName,
                      sizeof(tableArray[ii]->target.name));
        if (ret >= sizeof(tableArray[ii]->target.name)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Table name is too long in table %lu",
                    retinaName,
                    ii);
            status = StatusOverflow;
            goto CommonExit;
        }
        tableArray[ii]->target.isTable = true;

        for (jj = 0; jj < numColumns; jj++) {
            const char *columnName;
            const char *columnAlias;

            jsonColumn = json_array_get(jsonTableColumns, jj);
            if (jsonColumn == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": Could not get table column %lu "
                        "in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusRetinaParseError;
                goto CommonExit;
            }

            jsonColumnName = json_object_get(jsonColumn, "columnName");
            if (jsonColumnName == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": Could not get column name in "
                        "column %lu in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusRetinaParseError;
                goto CommonExit;
            }

            columnName = json_string_value(jsonColumnName);
            if (columnName == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": Column name is not a valid "
                        "string in column %lu in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusRetinaParseError;
                goto CommonExit;
            }

            jsonColumnAlias = json_object_get(jsonColumn, "headerAlias");
            if (jsonColumnAlias == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": Could not get column alias in "
                        "column %lu in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusRetinaParseError;
                goto CommonExit;
            }

            columnAlias = json_string_value(jsonColumnAlias);
            if (columnAlias == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": headerAlias is not a valid "
                        "string in column %lu in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusRetinaParseError;
                goto CommonExit;
            }

            ret = strlcpy(tableArray[ii]->columns[jj].name,
                          columnName,
                          sizeof(tableArray[ii]->columns[jj].name));
            if (ret >= sizeof(tableArray[ii]->columns[jj].name)) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": columnName is too long in column"
                        " %lu in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusOverflow;
                goto CommonExit;
            }

            ret = strlcpy(tableArray[ii]->columns[jj].headerAlias,
                          columnAlias,
                          sizeof(tableArray[ii]->columns[jj].headerAlias));
            if (ret >= sizeof(tableArray[ii]->columns[jj].headerAlias)) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error parsing \"%s\": columnAlias is too long in "
                        "column %lu in table %lu",
                        retinaName,
                        jj,
                        ii);
                status = StatusOverflow;
                goto CommonExit;
            }
        }
        tableArray[ii]->numColumns = (int) numColumns;
    }

    retinaInfo->numTables = numTables;
    retinaInfo->tableArray = tableArray;
    tableArray = NULL;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (tableArray != NULL) {
            for (ii = 0; ii < numTablesPopulated; ii++) {
                assert(tableArray[ii] != NULL);
                memFree(tableArray[ii]);
                tableArray[ii] = NULL;
            }
            memFree(tableArray);
            tableArray = NULL;
        }
    }
    assert(tableArray == NULL);
    return status;
}

// Using jansson for now to parse the json file. Can be replaced
// with DataFormat once we support Objects and Array natively
Status
DagLib::populateRetinaInfo(json_t *retinaJson,
                           const char *retinaName,
                           RetinaInfo *retinaInfo,
                           ArchiveManifest *manifest)
{
    Status status = StatusUnknown;
    json_t *jsonQueryStr;
    const char *jsonQueryCStr;
    char *queryStr = NULL;
    size_t queryStrLen;

    assert(retinaInfo != NULL);

    status = populateTableArray(retinaJson, retinaName, retinaInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = populateSchemaHints(retinaJson, retinaName, retinaInfo);
    BailIfFailed(status);

    if (manifest != NULL) {
        status = populateUdfModulesArray(retinaJson,
                                         retinaName,
                                         retinaInfo,
                                         manifest,
                                         NULL);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    jsonQueryStr = json_object_get(retinaJson, "query");
    if (jsonQueryStr == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": Error getting query string",
                retinaName);
        status = StatusRetinaParseError;
        goto CommonExit;
    }

    if (json_typeof(jsonQueryStr) == JSON_STRING) {
        // this means we are dealing with a string, and we'll be using the old
        // way of query parsing
        jsonQueryCStr = json_string_value(jsonQueryStr);

        queryStrLen = strlen(jsonQueryCStr);
        queryStr = strAllocAndCopyWithSize(jsonQueryCStr, queryStrLen + 1);
        if (queryStr == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        retinaInfo->queryStr = queryStr;
        queryStr = NULL;
    } else if (json_typeof(jsonQueryStr) == JSON_ARRAY) {
        retinaInfo->queryStr = json_dumps(jsonQueryStr, JSON_COMPACT);
        BailIfNull(retinaInfo->queryStr);
    } else {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (queryStr != NULL) {
            memFree(queryStr);
            queryStr = NULL;
        }
    }

    assert(queryStr == NULL);

    return status;
}

Status
DagLib::parseRetinaFileToJson(void *buf,
                              size_t bufSize,
                              const char *retinaName,
                              json_t **retJsonOut)
{
    Status status = StatusUnknown;
    ArchiveManifest *manifest = NULL;
    void *retinaInfoFileContents = NULL;
    size_t retinaInfoFileContentsSize = 0;
    json_error_t jsonError;
    json_t *jsonRecord = NULL;

    assert(buf != NULL);

    *retJsonOut = NULL;  // init return info

    if (buf == NULL || bufSize == 0) {
        xSyslog(moduleName,
                XlogErr,
                "Dataflow \"%s\" has NULL buf OR bufSize of 0",
                retinaName);
        status = StatusEmptyFile;
        goto CommonExit;
    }

    status = archiveUnpack(buf, bufSize, &manifest);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(manifest != NULL);

    // Need to support both retinaInfo.json(file name that was used in the past)
    // and dataflowInfo.json, so as to maintain backward compatibility with
    // older retinas.
    status = archiveGetFileData(manifest,
                                "dataflowInfo.json",
                                &retinaInfoFileContents,
                                &retinaInfoFileContentsSize,
                                NULL);
    if (status != StatusOk) {
        status = archiveGetFileData(manifest,
                                    "retinaInfo.json",
                                    &retinaInfoFileContents,
                                    &retinaInfoFileContentsSize,
                                    NULL);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "dataflowInfo.json not present in batch dataflow "
                          "file");
            goto CommonExit;
        }
    }
    assert(retinaInfoFileContents != NULL);

    jsonRecord = json_loadb((const char *) retinaInfoFileContents,
                            retinaInfoFileContentsSize,
                            0,
                            &jsonError);
    if (jsonRecord == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": Error parsing dataflowInfo.json at line "
                "%d: %s",
                retinaName,
                jsonError.line,
                jsonError.text);
        status = StatusRetinaParseError;
        goto CommonExit;
    }
    *retJsonOut = jsonRecord;
    jsonRecord = NULL;  // returned to caller

CommonExit:
    if (manifest != NULL) {
        archiveFreeManifest(manifest);
        manifest = NULL;
        retinaInfoFileContents = NULL;
    }
    if (jsonRecord) {
        json_decref(jsonRecord);
        jsonRecord = NULL;
    }
    return status;
}

Status
DagLib::parseRetinaFile(void *buf,
                        size_t bufSize,
                        const char *retinaName,
                        RetinaInfo **retinaInfoOut)
{
    Status status = StatusUnknown;
    json_t *retinaInfoFileContents = NULL;
    ArchiveManifest *manifest = NULL;
    RetinaInfo *retinaInfo = NULL;

    if (buf == NULL || bufSize == 0) {
        xSyslog(moduleName,
                XlogErr,
                "Dataflow \"%s\" has NULL buf OR bufSize of 0",
                retinaName);
        status = StatusEmptyFile;
        goto CommonExit;
    }

    status = parseRetinaFileToJson(buf,
                                   bufSize,
                                   retinaName,
                                   &retinaInfoFileContents);
    BailIfFailed(status);

    status = allocRetinaInfo(retinaName, &retinaInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(retinaInfo != NULL);

    status = archiveUnpack(buf, bufSize, &manifest);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(manifest != NULL);

    status = populateRetinaInfo(retinaInfoFileContents,
                                retinaName,
                                retinaInfo,
                                manifest);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *retinaInfoOut = retinaInfo;
    retinaInfo = NULL;  // Hand over to caller
    status = StatusOk;
CommonExit:
    if (manifest != NULL) {
        archiveFreeManifest(manifest);
        manifest = NULL;
    }
    assert(manifest == NULL);
    if (status != StatusOk) {
        if (retinaInfo != NULL) {
            destroyRetinaInfo(retinaInfo);
            retinaInfo = NULL;
        }
    }
    if (retinaInfoFileContents) {
        json_decref(retinaInfoFileContents);
        retinaInfoFileContents = NULL;
    }

    assert(retinaInfo == NULL);
    assert(retinaInfoFileContents == NULL);

    return status;
}

Status
DagLib::populateImportRetinaOutput(XcalarApiOutput *output,
                                   size_t outputSize,
                                   uint64_t numUdfModules,
                                   XcalarApiOutput *uploadUdfOutputArray[],
                                   size_t uploadUdfOutputSizeArray[])
{
    Status status = StatusUnknown;
    XcalarApiImportRetinaOutput *importRetinaOutput;
    uint64_t ii;
    uintptr_t bufCursor;
    XcalarApiUdfAddUpdateOutput **udfModuleStatuses;
    XcalarApiUdfAddUpdateOutput *addUpdateOutput;
    size_t bytesConsumed = 0;

    // uploadUdfOutputArray and uploadUdfOutputSizeArray could be NULL
    assert(output != NULL);
    importRetinaOutput = &output->outputResult.importRetinaOutput;

    bytesConsumed = XcalarApiSizeOfOutput(*importRetinaOutput);
    if (bytesConsumed > outputSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    importRetinaOutput->numUdfModules = numUdfModules;
    importRetinaOutput->bufSize =
        outputSize - XcalarApiSizeOfOutput(*importRetinaOutput);
    importRetinaOutput->udfModuleStatuses = NULL;
    if (numUdfModules == 0) {
        status = StatusOk;
        goto CommonExit;
    }

    assert(uploadUdfOutputArray != NULL);
    assert(uploadUdfOutputSizeArray != NULL);

    bytesConsumed += sizeof(udfModuleStatuses[0]) * numUdfModules;
    if (bytesConsumed > outputSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    udfModuleStatuses =
        (XcalarApiUdfAddUpdateOutput **) importRetinaOutput->buf;
    bufCursor = (uintptr_t) importRetinaOutput->buf +
                (sizeof(udfModuleStatuses[0]) * numUdfModules);
    for (ii = 0; ii < numUdfModules; ii++) {
        bytesConsumed += uploadUdfOutputSizeArray[ii];
        if (bytesConsumed > outputSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        addUpdateOutput =
            &uploadUdfOutputArray[ii]->outputResult.udfAddUpdateOutput;

        memcpy((void *) bufCursor,
               addUpdateOutput,
               uploadUdfOutputSizeArray[ii]);
        udfModuleStatuses[ii] = (XcalarApiUdfAddUpdateOutput *) bufCursor;
        bufCursor += uploadUdfOutputSizeArray[ii];
    }

    assert(bytesConsumed == outputSize);
    importRetinaOutput->udfModuleStatuses = udfModuleStatuses;
    status = StatusOk;
CommonExit:
    output->hdr.status = status.code();
    return status;
}

Status
DagLib::searchQueryGraph(Dag *queryGraph,
                         EvalUdfModuleSet *udfModules,
                         uint64_t *numUdfModules,
                         Dag::DagNodeListElt *exportNodesListAnchor,
                         Dag::DagNodeListElt *synthesizeNodesListAnchor,
                         Dag::DagNodeListElt *loadNodesListAnchor)
{
    Status status = StatusUnknown;
    DagTypes::NodeId queryGraphNodeId = DagTypes::InvalidDagNodeId;
    const char *evalString;
    XcalarEvalAstCommon ast;
    bool astCreated = false;
    Dag::DagNodeListElt *dagNodeListElt = NULL;
    DagNodeTypes::Node *dagNode = NULL;
    XcalarApiUdfContainer *udfContainerLocal = NULL;
    uint64_t numUdfModulesLocal = 0;
    char absolutePathUdf[LibNsTypes::MaxPathNameLen + 1];

    assert(queryGraph != NULL);

    udfContainerLocal = queryGraph->getUdfContainer();

    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    BailIfFailed(status);

    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->lookupNodeById(queryGraphNodeId, &dagNode);
        assert(status == StatusOk);

        switch (dagNode->dagNodeHdr.apiDagNodeHdr.api) {
        case XcalarApiSynthesize: {
            // this is a pre-created node, add it to ready nodes list
            if (synthesizeNodesListAnchor == NULL) {
                break;
            }

            dagNodeListElt =
                (Dag::DagNodeListElt *) memAlloc(sizeof(*dagNodeListElt));
            if (dagNodeListElt == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            dagNodeListElt->node = dagNode;

            dagNodeListElt->prev = synthesizeNodesListAnchor->prev;
            dagNodeListElt->next = synthesizeNodesListAnchor;

            synthesizeNodesListAnchor->prev->next = dagNodeListElt;
            synthesizeNodesListAnchor->prev = dagNodeListElt;

            dagNodeListElt = NULL;  // Ref given to list
            break;
        }
        case XcalarApiExport:
            if (exportNodesListAnchor == NULL) {
                break;
            }

            dagNodeListElt =
                (Dag::DagNodeListElt *) memAlloc(sizeof(*dagNodeListElt));
            if (dagNodeListElt == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            dagNodeListElt->node = dagNode;

            dagNodeListElt->prev = exportNodesListAnchor->prev;
            dagNodeListElt->next = exportNodesListAnchor;

            exportNodesListAnchor->prev->next = dagNodeListElt;
            exportNodesListAnchor->prev = dagNodeListElt;

            dagNodeListElt = NULL;  // Ref given to list
            break;
        case XcalarApiBulkLoad: {
            if (udfModules == NULL) {
                break;
            }

            const char *fqUdfName = dagNode->dagNodeHdr.apiInput->loadInput
                                        .loadArgs.parseArgs.parserFnName;
            if (fqUdfName[0] != '\0') {
                status =
                    UserDefinedFunction::get()->getUdfName(absolutePathUdf,
                                                           sizeof(
                                                               absolutePathUdf),
                                                           (char *) fqUdfName,
                                                           udfContainerLocal,
                                                           false);
                BailIfFailed(status);

                status = XcalarEval::get()
                             ->getUdfModuleFromLoad(absolutePathUdf,
                                                    udfModules,
                                                    &numUdfModulesLocal);
                if (status != StatusOk) {
                    goto CommonExit;
                }
            }

            // add node to load nodes list
            if (loadNodesListAnchor == NULL) {
                break;
            }

            dagNodeListElt =
                (Dag::DagNodeListElt *) memAlloc(sizeof(*dagNodeListElt));
            if (dagNodeListElt == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            dagNodeListElt->node = dagNode;

            dagNodeListElt->prev = loadNodesListAnchor->prev;
            dagNodeListElt->next = loadNodesListAnchor;

            loadNodesListAnchor->prev->next = dagNodeListElt;
            loadNodesListAnchor->prev = dagNodeListElt;

            dagNodeListElt = NULL;  // Ref given to list

            break;
        }
        case XcalarApiExecuteRetina: {
            if (udfModules == NULL) {
                break;
            }
            EvalUdfModuleSet udfModulesTmp;

            status =
                getUdfModulesFromRetina(dagNode->dagNodeHdr.apiInput
                                            ->executeRetinaInput.retinaName,
                                        &udfModulesTmp);
            if (status == StatusRetinaNotFound) {
                status = StatusOk;
                break;
            }
            BailIfFailed(status);

            EvalUdfModule *module;
            for (EvalUdfModuleSet::iterator it = udfModulesTmp.begin();
                 (module = it.get()) != NULL;
                 it.next()) {
                if (udfModules->find(module->moduleName) != NULL) {
                    module->del();
                    break;
                }

                // module passed from udfModulesTmp to parent udfModulesSet
                verifyOk(udfModules->insert(module));
                numUdfModulesLocal++;
            }
        }
        case XcalarApiMap: {
            if (udfModules == NULL) {
                break;
            }
            XcalarApiMapInput *mapInput =
                &dagNode->dagNodeHdr.apiInput->mapInput;
            for (unsigned ii = 0; ii < mapInput->numEvals; ii++) {
                status = XcalarEval::get()->parseEvalStr(mapInput->evalStrs[ii],
                                                         XcalarFnTypeEval,
                                                         udfContainerLocal,
                                                         &ast,
                                                         NULL,
                                                         NULL,
                                                         NULL,
                                                         NULL);
                if (status != StatusOk) {
                    if (status == StatusAstNoSuchFunction) {
                        // non-fatal, try extracting udfs for the next eval
                        // XXX: see comment below about Filter API and SDK-370
                        status = StatusOk;
                        continue;
                    } else {
                        goto CommonExit;
                    }
                }

                astCreated = true;
                status = XcalarEval::get()->getUdfModules(ast.rootNode,
                                                          udfModules,
                                                          &numUdfModulesLocal);
                if (status != StatusOk) {
                    goto CommonExit;
                }

                assert(astCreated);
                XcalarEval::get()->freeCommonAst(&ast);
                astCreated = false;
            }
            break;
        }
        default:
            if (udfModules == NULL) {
                break;
            }

            evalString =
                xcalarApiGetEvalStringFromInput(dagNode->dagNodeHdr
                                                    .apiDagNodeHdr.api,
                                                dagNode->dagNodeHdr.apiInput);
            if (evalString == NULL) {
                break;
            }

            status = XcalarEval::get()->parseEvalStr(evalString,
                                                     XcalarFnTypeAgg,
                                                     udfContainerLocal,
                                                     &ast,
                                                     NULL,
                                                     NULL,
                                                     NULL,
                                                     NULL);

            if (status != StatusOk) {
                if (status == StatusAstNoSuchFunction &&
                    dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiFilter) {
                    // XXX: See JIRA Issue SDK-370. Ignore missing UDF failures
                    // for the filter operation - this is a quick fix for the
                    // following scenario:
                    // - retina is imported as json (so UDF container isn't the
                    //   retina, but a session) - this is a 'jsonRetina'
                    // - jsonRetina is executed
                    // - if exportToActiveSession is true, the retina is
                    //   exported into an embedded executeRetina dag node, for
                    //   which a call to searchQueryGraph occurs with the udf
                    //   container set to the retina's dag - so UDFs will not
                    //   be found since in the case of jsonRetinas, the UDFs
                    //   aren't in the retina - but this embedded/exported
                    //   retina isn't useful in any case - nor does it need to
                    //   include any UDFs - since the use-case is a json-retina
                    //   - so the errors can be ignored
                    // The risk behind this solution is that other code paths
                    // which call searchQueryGraph for normal retinas may
                    // succeed despite missing UDFs, and they may fail later
                    // when the retina is executed. However, the risk is almost
                    // non-existent since searchQueryGraph is now used only for
                    // retina->dag, and for normal retinas, the retina would
                    // always contain the UDFs so failures due to missing UDFs
                    // aren't really possible. The same issue is true for Map
                    // UDFs which also skips UDF resolution failures (see
                    // above).
                    //
                    // The right solution seems to be: for jsonRetinas,
                    // remove the retina-export at the end of a jsonRetina's
                    // execution (see OperatorHandlerExecuteRetina::run and its
                    // check for input_->exportToActiveSession) which will avoid
                    // its call to searchQueryGraph, but retain the output of
                    // the tableName in the executeRetinaOutput.
                    //
                    status = StatusOk;
                    break;
                }
                goto CommonExit;
            }

            astCreated = true;
            status = XcalarEval::get()->getUdfModules(ast.rootNode,
                                                      udfModules,
                                                      &numUdfModulesLocal);
            if (status != StatusOk) {
                goto CommonExit;
            }

            assert(astCreated);
            XcalarEval::get()->freeCommonAst(&ast);
            astCreated = false;

            break;
        }
        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        BailIfFailed(status);
    }

    status = StatusOk;
CommonExit:
    if (numUdfModules != NULL) {
        *numUdfModules = numUdfModulesLocal;
    }
    if (dagNodeListElt != NULL) {
        assert(status != StatusOk);
        memFree(dagNodeListElt);
        dagNodeListElt = NULL;
    }

    if (astCreated) {
        assert(status != StatusOk);
        XcalarEval::get()->freeCommonAst(&ast);
        astCreated = false;
    }

    return status;
}

Status
DagLib::changeDatasetUserName(const char *oldDatasetName,
                              const char *userName,
                              char **newDatasetNameOut)
{
    Status status = StatusOk;
    size_t ret;
    char *newDatasetName;

    *newDatasetNameOut = NULL;

    // Expected format is .XccalarDS.[TableName:]<user>[.NNNNN].dsName
    // Changes <user> to userName

    newDatasetName =
        (char *) memAllocExt(XcalarApiDsDatasetNameLen + 1, moduleName);
    BailIfNull(newDatasetName);

    // Copy the old to the new in case we don't need to convert or the
    // expected format doesn't match.
    ret = strlcpy(newDatasetName, oldDatasetName, XcalarApiDsDatasetNameLen);
    assert(ret < XcalarApiDsDatasetNameLen);

    // Don't change the username if not provided.
    if (userName[0] != '\0' && strncmp(oldDatasetName,
                                       XcalarApiDatasetPrefix,
                                       XcalarApiDatasetPrefixLen) == 0) {
        // Use %.*s format - length ("headLen") instead of NULL terminated
        // string for .XccalarDS.[TableName:] and point tail to [.NNN].dsName.
        // Then following suffices:
        // snprintf(newName, sizeof(newName), "%.*s%s%s", headLen, oldName,
        //     userName, tail)
        char *preUser;     // char before <user>
        uint8_t headLen;   // length of string before <user>
        char *tail;        // points to char after <user>
        char *lastPeriod;  // points to last period in entire string
        uint8_t preDsLen;  // strlen of <user>[.NNNN] (b/w preUser and .dsName)

        preUser =
            (char *) strchr(oldDatasetName + XcalarApiDatasetPrefixLen, ':');
        if (preUser == NULL) {
            preUser = (char *) oldDatasetName + XcalarApiDatasetPrefixLen - 1;
        }
        headLen = preUser - oldDatasetName + 1;
        lastPeriod = rindex(preUser, '.');  // point to . in .dsname
        assert(lastPeriod >= preUser);      // since preUser[0] == '.'
        if (lastPeriod != preUser) {
            if (lastPeriod == preUser + 1) {
                preDsLen = 1;  // memrchr(3) doesn't like 0 length
            } else {
                preDsLen = lastPeriod - preUser - 1;
            }
            // Look backwards for a '.' from preUser + preDsLen to get the tail
            tail = (char *) memrchr(preUser, '.', preDsLen);
            if (tail == preUser) {
                // .XcalarDs.<user>.dsname: memrchr() would point tail to char
                // before <user> (== preUser) - fix this.
                tail = lastPeriod;
            }
            snprintf(newDatasetName,
                     XcalarApiDsDatasetNameLen,
                     "%.*s%s%s",
                     headLen,
                     oldDatasetName,
                     userName,
                     tail);
        }  // else (preUser == lastPeriod so name's .XcalarDs.dsname - skip)
    }

    *newDatasetNameOut = newDatasetName;
    newDatasetName = NULL;

CommonExit:

    if (newDatasetName != NULL) {
        memFree(newDatasetName);
        newDatasetName = NULL;
    }

    return status;
}

// NOTE: This function exists just to support workbook upgrade from 1.4.x
// query graphs to DF2 dataflows. It does two things: replaces user-name in
// dataset names with the current user-name, and uploads UDFs from embedded
// execute-retina nodes in the query graph. Since the latter may clash with the
// workbook's UDFs, the UDF names are prefixed with the retina name, and
// expServer will fix all UDF references in the exec-retina-node query, with
// the prefixed names (so expServer code must use the same prefixing algo).
//
// (XXX: In case of error in adding a UDF during the fixup, eventually a new
// error code should be added which is a non-fatal failure so XD can display
// appropriate warnings to the user for this scenario. For now, we're logging
// the error and continuing so the workbook successfully uploads)
//
// For the dataset name fixup:
//
// This function walks the provided dag looking for load nodes and for index
// nodes.  The dag was created from an uploaded workbook which could have been
// created by a different user.  The load/index nodes have the dataset name
// containing the creator's user name and, if different, is changed to the user
// name of the uploader.  This way the uploader has a different dataset name
// and thus will redo the actual load.  If the uploader is the same user that
// created the workbook then it is ok to use the existing dataset.

Status
DagLib::fixupQueryGraphForWorkbookUpload(Dag *queryGraph)
{
    Status status = StatusUnknown;
    DagTypes::NodeId queryGraphNodeId = DagTypes::InvalidDagNodeId;
    DagNodeTypes::Node *dagNode = NULL;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    DsDataset *dataset;
    int ret;
    bool dsOpened = false;
    bool dsStateFlipped = false;
    char *origDatasetName;
    char *newDatasetName = NULL;
    const char *userName;
    XcalarApiUdfContainer *udfContainer = NULL;
    RetinaInfo *retinaInfo = NULL;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize = 0;
    uint64_t ii;
    char *prefixedModuleName = NULL;

    udfContainer = &queryGraph->udfContainer_;
    userName = udfContainer->userId.userIdName;

    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to find first dagnode in query graph: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->lookupNodeById(queryGraphNodeId, &dagNode);
        assert(status == StatusOk);

        if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiBulkLoad) {
            // Validate assumption that the two names are the same for a
            // bulk load.
            assert(
                strcmp(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                       dagNode->dagNodeHdr.apiInput->loadInput.datasetName) ==
                0);
            origDatasetName =
                dagNode->dagNodeHdr.apiInput->loadInput.datasetName;
            // Determine the new dataset name to use based on the user
            // doing the upload.  If the uploader is the creator of the
            // workbook the name will be the same.
            status = changeDatasetUserName(origDatasetName,
                                           userName,
                                           &newDatasetName);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to change dataset name '%s' to user '%s: %s",
                        origDatasetName,
                        userName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            if (strcmp(origDatasetName, newDatasetName) != 0) {
                // The dataset name has been changed to reflect the user that
                // is uploading the workbook. Update the dataset name in the
                // dag node API input

                // renameDagNode expects that the dag node state is not
                // Dropped. Temporarily flip the state.
                if (dagNode->dagNodeHdr.apiDagNodeHdr.state ==
                    DgDagStateDropped) {
                    dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateReady;
                    dsStateFlipped = true;
                }

                status = queryGraph->renameDagNode(dagNode->dagNodeHdr
                                                       .apiDagNodeHdr.name,
                                                   newDatasetName);

                if (dsStateFlipped) {
                    dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateDropped;
                    dsStateFlipped = false;
                }

                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to rename dag node from '%s' to '%s': %s",
                            dagNode->dagNodeHdr.apiDagNodeHdr.name,
                            newDatasetName,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
            }

            // Protect access to the dataset...if it exists.  It may exist as
            // the originator of the workbook, or if the same workbook has been
            // loaded more than once.
            ret = snprintf(fullyQualName,
                           LibNsTypes::MaxPathNameLen,
                           "%s%s",
                           Dataset::DsPrefix,
                           newDatasetName);
            if (ret < 0 || ret >= (int) sizeof(fullyQualName)) {
                status = StatusNameTooLong;
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to fixup dataset '%s': %s",
                        newDatasetName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            nsHandle =
                libNs->open(fullyQualName, LibNsTypes::ReaderShared, &status);
            if (status == StatusOk) {
                dsOpened = true;
                dataset =
                    Dataset::get()->getDatasetFromName(newDatasetName, &status);
                if (status != StatusOk) {
                    assert(0);
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to open dataset '%s' in LibNs: %s",
                            newDatasetName,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                // Copy the dataset uuid.  This allows using an existing
                // dataset with the same name (meaning the same user) which
                // avoids loading the dataset again.
                dagNode->dagNodeHdr.apiInput->loadInput.loadArgs.datasetUuid =
                    dataset->loadArgs_.datasetUuid;
                status = libNs->close(nsHandle, NULL);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to close dataset '%s': %s",
                            newDatasetName,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                dsOpened = false;
            }
            memFree(newDatasetName);
            newDatasetName = NULL;
        } else if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiIndex) {
            if (!dagNode->dagNodeHdr.apiInput->indexInput.source.isTable) {
                origDatasetName =
                    dagNode->dagNodeHdr.apiInput->indexInput.source.name;
                status = changeDatasetUserName(origDatasetName,
                                               userName,
                                               &newDatasetName);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to change dataset name '%s' to "
                            "user '%s': %s",
                            origDatasetName,
                            userName,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                if (strcmp(origDatasetName, newDatasetName) != 0) {
                    // Update the dataset name in the dag node.
                    strlcpy(dagNode->dagNodeHdr.apiInput->indexInput.source
                                .name,
                            newDatasetName,
                            XcalarApiDsDatasetNameLen);
                }
                memFree(newDatasetName);
                newDatasetName = NULL;
            }
        } else if (dagNode->dagNodeHdr.apiDagNodeHdr.api ==
                   XcalarApiExecuteRetina) {
            // Upload any UDFs stored in the embedded execute-retina node.
            // Ignore dups with workbook UDFs if the code's identical - log the
            // event if not, and in DEBUG, fail the operation. In non-debug,
            // keep going (XXX: with the expectation that eventually a new error
            // will be added which is non-fatal, for XD to present this to user
            // appropriately). Note that the likelihood of DUPs is almost
            // non-existent since the UDF names being added from the retina
            // will be pre-fixed with the retina's name

            const XcalarApiExecuteRetinaInput *retInput =
                &dagNode->dagNodeHdr.apiInput->executeRetinaInput;

            if (retInput->exportRetinaBuf != NULL && udfContainer != NULL) {
                // if the udfContainer is supplied, the caller is interested
                // in getting the retina's UDFs added to the container - so
                // do this work but keep going in case the UDF module name
                // conflicts with one of the same name in the session.
                status = DagLib::get()
                             ->parseRetinaFile(retInput->exportRetinaBuf,
                                               retInput->exportRetinaBufSize,
                                               retInput->retinaName,
                                               &retinaInfo);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "ExecuteRetina node expansion failed to parse"
                            " the retina '%s' when processing "
                            "session '%s': %s",
                            retInput->retinaName,
                            udfContainer->sessionInfo.sessionName,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                for (ii = 0; ii < retinaInfo->numUdfModules; ii++) {
                    UdfModuleSrc *newModule = retinaInfo->udfModulesArray[ii];
                    assert(!newModule->isBuiltin);
                    // skip default module
                    if (strncmp(newModule->moduleName,
                                UserDefinedFunction::DefaultModuleName,
                                strlen(
                                    UserDefinedFunction::DefaultModuleName)) !=
                        0) {
                        if (addUdfOutput != NULL) {
                            memFree(addUdfOutput);
                            addUdfOutput = NULL;
                        }

                        // Prefix the UDF name with the retina's name, before
                        // adding it to the workbook, to avoid name conflicts.
                        // The references to the UDF(s) in the query inside the
                        // retina must also be fixed-up to reference the new
                        // name - the latter is done by the expServer when
                        // processing the DF2 upgrade (so the table names and
                        // UDF names in a retina get prefixed with the retina
                        // name by the expServer during upgrade) which occurs
                        // later when the upgrade app is launched

                        status =
                            UserDefinedFunction::get()
                                ->prefixUdfNameWithRetina(retInput->retinaName,
                                                          newModule->moduleName,
                                                          &prefixedModuleName);

                        if (status != StatusOk) {
                            xSyslogTxnBuf(
                                moduleName,
                                XlogDebug,
                                "ExecuteRetina node expansion for retina "
                                "'%s', failed to prefix UDF module name '%s', "
                                "in session '%s': '%s'",
                                retInput->retinaName,
                                newModule->moduleName,
                                udfContainer->sessionInfo.sessionName,
                                strGetFromStatus(status));
#ifdef DEBUG
                            goto CommonExit;  // bail out in DEBUG mode
#endif
                        }

                        strlcpy(newModule->moduleName,
                                prefixedModuleName,
                                sizeof(newModule->moduleName));

                        status = UserDefinedFunction::get()
                                     ->addUdfIgnoreDup(newModule,
                                                       udfContainer,
                                                       &addUdfOutput,
                                                       &addUdfOutputSize);
                        if (status != StatusOk) {
                            xSyslogTxnBuf(
                                moduleName,
                                XlogDebug,
                                "ExecuteRetina node expansion for retina "
                                "'%s', failed to add UDF module name '%s', "
                                "to session '%s': '%s'",
                                retInput->retinaName,
                                newModule->moduleName,
                                udfContainer->sessionInfo.sessionName,
                                strGetFromStatus(status));
#ifdef DEBUG
                            goto CommonExit;  // bail out in DEBUG mode
#endif
                            assert(0);
                            //
                            // Two different ER nodes can't have the same
                            // retina name in a workbook (even if both nodes
                            // were a result of execution from the same retina)
                            // because the table name to which the export
                            // occurred, must be different, and this name is
                            // already appended in the retina's name
                            // (retInput->retinaName). This does mean UDF code
                            // duplication though - if the same retina has been
                            // executed multiple times, with export-to-table,
                            // the retina's UDFs will appear multiple times -
                            // identical copies - one set for each ER
                            // node...but at least there wouldn't be any
                            // clashes - this can be cleaned up by the user
                            // after upgrade using DF2 operations. In DEBUG,
                            // this should be asserted. In production, log and
                            // keep going so at least upgrade finishes as much
                            // as possible.
                            //
                            // XXX: return a special error code indicating
                            // non-fatal problems eventually caller (e.g. XD)
                            // can warn user about bad upgrade - we want to
                            // continue and upgrade as much as possible, logging
                            // any errors encountered - so user can inspect logs
                            // later for all the issues encountered and XD can
                            // warn user about the fact that there are some
                            // issues they should check the logs
                        }
                    }
                }
            }
        }
        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get next dag node for query graph: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = StatusOk;

CommonExit:
    if (addUdfOutput) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }
    if (prefixedModuleName) {
        memFree(prefixedModuleName);
        prefixedModuleName = NULL;
    }
    if (dsOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close dataset '%s': %s",
                    newDatasetName,
                    strGetFromStatus(status));
        }
    }
    if (retinaInfo != NULL) {
        destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }
    if (newDatasetName != NULL) {
        memFree(newDatasetName);
        newDatasetName = NULL;
    }

    return status;
}

Status
DagLib::populateJsonTableArray(const char *retinaName,
                               json_t *retinaInfoJson,
                               Dag::DagNodeListElt *exportNodesListAnchor)
{
    Status status = StatusUnknown;
    json_t *jsonTablesArray = NULL;
    json_t *jsonTable = NULL;
    json_t *jsonTableName = NULL;
    json_t *jsonTableColumns = NULL;
    json_t *jsonColumn = NULL;
    json_t *jsonColumnName = NULL;
    json_t *jsonColumnAlias = NULL;
    Dag::DagNodeListElt *exportNodeListElt;
    XcalarApiExportInput *exportInput;
    int ret = 0, ii;

    jsonTablesArray = json_array();
    if (jsonTablesArray == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Failed to create tables array",
                retinaName);
        goto CommonExit;
    }

    exportNodeListElt = exportNodesListAnchor->next;
    while (exportNodeListElt != exportNodesListAnchor) {
        exportInput =
            &exportNodeListElt->node->dagNodeHdr.apiInput->exportInput;

        assert(jsonTable == NULL);
        jsonTable = json_object();
        if (jsonTable == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to create json table "
                    "object",
                    retinaName);
            goto CommonExit;
        }

        assert(jsonTableName == NULL);
        jsonTableName = json_string(exportInput->srcTable.tableName);
        if (jsonTableName == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to convert \"%s\" to json "
                    "string",
                    retinaName,
                    exportInput->srcTable.tableName);
            goto CommonExit;
        }

        ret = json_object_set(jsonTable, "name", jsonTableName);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to set \"name\" attribute",
                    retinaName);
            goto CommonExit;
        }
        json_decref(jsonTableName);
        jsonTableName = NULL;

        assert(jsonTableColumns == NULL);
        jsonTableColumns = json_array();
        if (jsonTableColumns == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to create columns array",
                    retinaName);
            goto CommonExit;
        }

        for (ii = 0; ii < exportInput->meta.numColumns; ii++) {
            assert(jsonColumn == NULL);
            jsonColumn = json_object();
            if (jsonColumn == NULL) {
                status = StatusNoMem;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to create json column "
                        "object",
                        retinaName);
                goto CommonExit;
            }

            assert(jsonColumnName == NULL);
            jsonColumnName = json_string(exportInput->meta.columns[ii].name);
            if (jsonColumnName == NULL) {
                status = StatusNoMem;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to convert \"%s\" to "
                        "json string",
                        retinaName,
                        exportInput->meta.columns[ii].name);
                goto CommonExit;
            }

            ret = json_object_set(jsonColumn, "columnName", jsonColumnName);
            if (ret != 0) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to set \"columnName\" "
                        "attribute",
                        retinaName);
                goto CommonExit;
            }
            json_decref(jsonColumnName);
            jsonColumnName = NULL;

            jsonColumnAlias =
                json_string(exportInput->meta.columns[ii].headerAlias);
            if (jsonColumnAlias == NULL) {
                status = StatusNoMem;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to convert \"%s\" to "
                        "json string",
                        retinaName,
                        exportInput->meta.columns[ii].headerAlias);
                goto CommonExit;
            }

            ret = json_object_set(jsonColumn, "headerAlias", jsonColumnAlias);
            if (ret != 0) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to set \"headerAlias\" "
                        "attribute",
                        retinaName);
                goto CommonExit;
            }
            json_decref(jsonColumnAlias);
            jsonColumnAlias = NULL;

            ret = json_array_append(jsonTableColumns, jsonColumn);
            if (ret != 0) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Error exporting \"%s\": Failed to add jsonColumn to "
                        "jsonTableColumns",
                        retinaName);
                goto CommonExit;
            }
            json_decref(jsonColumn);
            jsonColumn = NULL;
        }

        ret = json_object_set(jsonTable, "columns", jsonTableColumns);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to set \"columns\" "
                    "attribute",
                    retinaName);
            goto CommonExit;
        }
        json_decref(jsonTableColumns);
        jsonTableColumns = NULL;

        ret = json_array_append(jsonTablesArray, jsonTable);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to add jsonTable to "
                    "jsonTableArray",
                    retinaName);
            goto CommonExit;
        }
        json_decref(jsonTable);
        jsonTable = NULL;

        exportNodeListElt = exportNodeListElt->next;
    }

    ret = json_object_set(retinaInfoJson, "tables", jsonTablesArray);
    if (ret != 0) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Failed to set \"tables\" attribute",
                retinaName);
        goto CommonExit;
    }
    json_decref(jsonTablesArray);
    jsonTablesArray = NULL;  // Ref handed to retinaInfoJson

    status = StatusOk;
CommonExit:
    if (jsonColumnAlias != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumnAlias);
        jsonColumnAlias = NULL;
    }

    if (jsonColumnName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumnName);
        jsonColumnName = NULL;
    }

    if (jsonColumn != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumn);
        jsonColumn = NULL;
    }

    if (jsonTableColumns != NULL) {
        assert(status != StatusOk);
        json_decref(jsonTableColumns);
        jsonTableColumns = NULL;
    }

    if (jsonTableName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonTableName);
        jsonTableName = NULL;
    }

    if (jsonTable != NULL) {
        assert(status != StatusOk);
        json_decref(jsonTable);
        jsonTable = NULL;
    }

    if (jsonTablesArray != NULL) {
        assert(status != StatusOk);
        // Jansson automatically decrefs all the other references
        // that jsonTablesArray might have on other json objects
        json_decref(jsonTablesArray);
        jsonTablesArray = NULL;
    }

    return status;
}

Status
DagLib::populateJsonUdfModulesArray(const char *retinaName,
                                    const char *modulePathPrefix,
                                    json_t *retinaInfoJson,
                                    ArchiveManifest *manifest,
                                    XcalarApiUdfContainer *udfContainer,
                                    EvalUdfModuleSet *udfModules,
                                    json_t *archiveChecksum)
{
    json_t *jsonUdfsArray = NULL;
    json_t *jsonUdf = NULL;
    json_t *jsonUdfModuleName = NULL;
    json_t *jsonUdfFileName = NULL;
    json_t *jsonUdfType = NULL;

    Status status = StatusUnknown;
    int ret;
    XcalarApiUdfGetInput udfGetInput;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    UdfModuleSrc *module = NULL;
    char modulePath[sizeof(module->moduleName) + strlen(modulePathPrefix) +
                    sizeof(".py")];

    EvalUdfModuleSet::iterator it(*udfModules);

    assert(retinaName != NULL);
    assert(retinaInfoJson != NULL);
    assert(manifest != NULL);

    jsonUdfsArray = json_array();
    if (jsonUdfsArray == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not create jsonUdfsArray",
                retinaName);
        goto CommonExit;
    }

    for (; it.get() != NULL; it.next()) {
        assertStatic(sizeof(udfGetInput.moduleName) ==
                     sizeof(it.get()->moduleName));

        verify(strlcpy(udfGetInput.moduleName,
                       it.get()->getName(),
                       sizeof(udfGetInput.moduleName)) <
               sizeof(udfGetInput.moduleName));

        status = UserDefinedFunction::get()->getUdf(&udfGetInput,
                                                    udfContainer,
                                                    &output,
                                                    &outputSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": getUdf returned %s while "
                    "retrieving \"%s\"",
                    retinaName,
                    strGetFromStatus(status),
                    it.get()->getName());
            // non-fatal. move on to the next udf
            status = StatusOk;
            continue;
        }

        module = &output->outputResult.udfGetOutput;
        // Should have been validated before it could have been uploaded
        assert(isValidUdfType(module->type));

        ret = snprintf(modulePath,
                       sizeof(modulePath),
                       "%s%s%s",
                       modulePathPrefix,
                       module->moduleName,
                       strGetFromUdfSuffix((UdfSuffix) module->type));
        if (ret < 0 || ret >= (int) sizeof(modulePath)) {
            status = StatusNoBufs;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": modulePath \"%s%s%s\" is too "
                    "long",
                    retinaName,
                    modulePathPrefix,
                    module->moduleName,
                    strGetFromUdfSuffix((UdfSuffix) module->type));
            goto CommonExit;
        }

        bool nullTerminated;
        nullTerminated = (module->source[module->sourceSize - 1] == '\0');
        status =
            archiveAddFileToManifest(modulePath,
                                     module->source,
                                     (nullTerminated) ? module->sourceSize - 1
                                                      : module->sourceSize,
                                     manifest,
                                     archiveChecksum);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Could not add \"%s\" to "
                    "manifest (%s)",
                    retinaName,
                    module->moduleName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        assert(jsonUdf == NULL);
        jsonUdf = json_object();
        if (jsonUdf == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Could not create jsonUdf",
                    retinaName);
            goto CommonExit;
        }

        assert(jsonUdfModuleName == NULL);
        jsonUdfModuleName = json_string(module->moduleName);
        if (jsonUdfModuleName == NULL) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to convert \"%s\" to "
                    "json string",
                    retinaName,
                    module->moduleName);
            goto CommonExit;
        }

        ret = json_object_set(jsonUdf, "moduleName", jsonUdfModuleName);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Could not set \"moduleName\" "
                    "attribute",
                    moduleName);
            goto CommonExit;
        }
        json_decref(jsonUdfModuleName);
        jsonUdfModuleName = NULL;

        assert(jsonUdfFileName == NULL);
        jsonUdfFileName = json_string(modulePath);
        if (jsonUdfFileName == NULL) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to convert \"%s\" to "
                    "json string",
                    retinaName,
                    modulePath);
            goto CommonExit;
        }

        ret = json_object_set(jsonUdf, "fileName", jsonUdfFileName);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Could not set \"fileName\" "
                    "attribute",
                    retinaName);
            goto CommonExit;
        }
        json_decref(jsonUdfFileName);
        jsonUdfFileName = NULL;

        assert(jsonUdfType == NULL);
        jsonUdfType = json_string(strGetFromUdfType(module->type));
        if (jsonUdfType == NULL) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to convert \"%s\" to "
                    "json string",
                    retinaName,
                    strGetFromUdfType(module->type));
            goto CommonExit;
        }

        ret = json_object_set(jsonUdf, "udfType", jsonUdfType);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Could not set \"udfType\" "
                    "attribute",
                    retinaName);
            goto CommonExit;
        }
        json_decref(jsonUdfType);
        jsonUdfType = NULL;

        ret = json_array_append(jsonUdfsArray, jsonUdf);
        if (ret != 0) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Error exporting \"%s\": Failed to add jsonUdf to "
                    "jsonUdfsArray",
                    retinaName);
            goto CommonExit;
        }
        json_decref(jsonUdf);
        jsonUdf = NULL;

        assert(output != NULL);
        assert(outputSize > 0);
        memFree(output);
        output = NULL;
        outputSize = 0;
    }

    ret = json_object_set(retinaInfoJson, "udfs", jsonUdfsArray);
    if (ret != 0) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not set \"udfs\" attribute",
                retinaName);
        goto CommonExit;
    }
    json_decref(jsonUdfsArray);
    jsonUdfsArray = NULL;

    status = StatusOk;
CommonExit:
    if (jsonUdfType != NULL) {
        assert(status != StatusOk);
        json_decref(jsonUdfType);
        jsonUdfType = NULL;
    }

    if (jsonUdfFileName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonUdfFileName);
        jsonUdfFileName = NULL;
    }

    if (jsonUdfModuleName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonUdfModuleName);
        jsonUdfModuleName = NULL;
    }

    if (jsonUdf != NULL) {
        assert(status != StatusOk);
        json_decref(jsonUdf);
        jsonUdf = NULL;
    }

    if (jsonUdfsArray != NULL) {
        assert(status != StatusOk);
        json_decref(jsonUdfsArray);
        jsonUdfsArray = NULL;
    }

    if (output != NULL) {
        assert(status != StatusOk);
        memFree(output);
        output = NULL;
    }

    return status;
}

Status
DagLib::populateJsonQueryStr(const char *retinaName,
                             json_t *retinaInfoJson,
                             DgRetina *retina)
{
    int ret;
    Status status = StatusOk;
    json_t *query = NULL;

    assert(retinaName != NULL);
    assert(retinaInfoJson != NULL);

    status = QueryParser::get()->reverseParse(retina->dag, &query);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": queryReverseParse returned %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = json_object_set_new(retinaInfoJson, "query", query);
    BailIfFailedWith(ret, StatusNoMem);
    query = NULL;

CommonExit:
    if (query) {
        json_decref(query);
    }
    return status;
}

Status
DagLib::addRetinaInfoJsonToManifest(const char *retinaName,
                                    json_t *retinaInfoJson,
                                    ArchiveManifest *manifest)
{
    Status status = StatusUnknown;
    json_t *jsonXcalarVersion = NULL;
    json_t *jsonRetinaVersion = NULL;
    char *jsonStr = NULL;
    int ret;

    assert(retinaName != NULL);
    assert(retinaInfoJson != NULL);
    assert(manifest != NULL);

    // Let's add some version info before we materialize it
    jsonXcalarVersion = json_string(versionGetStr());
    if (jsonXcalarVersion == NULL) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not convert version string "
                "\"%s\" to json string",
                retinaName,
                versionGetStr());
        goto CommonExit;
    }

    ret = json_object_set(retinaInfoJson, "xcalarVersion", jsonXcalarVersion);
    if (ret != 0) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Failed to set \"xcalarVersion\" "
                "attribute",
                retinaName);
        goto CommonExit;
    }
    json_decref(jsonXcalarVersion);
    jsonXcalarVersion = NULL;

    jsonRetinaVersion = json_integer(RetinaVersion);
    if (jsonRetinaVersion == NULL) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not convert Dataflow version: %d"
                " to json integer",
                retinaName,
                RetinaVersion);
        goto CommonExit;
    }

    ret = json_object_set(retinaInfoJson, "dataflowVersion", jsonRetinaVersion);
    if (ret != 0) {
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Failed to set \"dataflowVersion\" "
                "attribute",
                retinaName);
        goto CommonExit;
    }
    json_decref(jsonRetinaVersion);
    jsonRetinaVersion = NULL;

    // We're ready to convert the json_t into a string buffer now
    jsonStr = json_dumps(retinaInfoJson, JSON_INDENT(4) | JSON_ENSURE_ASCII);
    if (jsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not convert json_t to string",
                retinaName);
        goto CommonExit;
    }

    // Add the string buffer as a file
    status = archiveAddFileToManifest("dataflowInfo.json",
                                      jsonStr,
                                      strlen(jsonStr),
                                      manifest,
                                      NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not add dataflowInfo.json to "
                "manifest: %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (jsonStr != NULL) {
        memFreeJson(jsonStr);  // Allocated by json library.
        jsonStr = NULL;
    }

    if (jsonRetinaVersion != NULL) {
        assert(status != StatusOk);
        json_decref(jsonRetinaVersion);
        jsonRetinaVersion = NULL;
    }

    if (jsonXcalarVersion != NULL) {
        assert(status != StatusOk);
        json_decref(jsonXcalarVersion);
        jsonXcalarVersion = NULL;
    }

    return status;
}

Status
DagLib::exportRetinaInt(DgRetina *retina,
                        XcalarApiExportRetinaInput *exportRetinaInput,
                        XcalarApiOutput **outputOut,
                        size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    json_t *retinaInfoJson = NULL;
    EvalUdfModuleSet udfModules;
    Dag::DagNodeListElt exportNodesListAnchor;
    Dag::DagNodeListElt loadNodesListAnchor;
    Dag::DagNodeListElt synthesizeNodesListAnchor;
    Dag::DagNodeListElt *dagNodeListElt = NULL;
    ArchiveManifest *archiveManifest = NULL;
    void *retinaBuf = NULL;
    size_t retinaBufSize = 0;
    XcalarApiOutput *output = NULL;
    XcalarApiExportRetinaOutput *exportRetinaOutput = NULL;
    size_t outputSize = 0;

    exportNodesListAnchor.next = exportNodesListAnchor.prev =
        &exportNodesListAnchor;

    loadNodesListAnchor.next = loadNodesListAnchor.prev = &loadNodesListAnchor;

    synthesizeNodesListAnchor.next = synthesizeNodesListAnchor.prev =
        &synthesizeNodesListAnchor;

    archiveManifest = archiveNewEmptyManifest();
    if (archiveManifest == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not create new empty manifest",
                exportRetinaInput->retinaName);
        goto CommonExit;
    }

    retinaInfoJson = json_object();
    if (retinaInfoJson == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not create DataflowInfoJson "
                "root",
                exportRetinaInput->retinaName);
        goto CommonExit;
    }

    status = searchQueryGraph(retina->dag,
                              &udfModules,
                              NULL,
                              &exportNodesListAnchor,
                              &synthesizeNodesListAnchor,
                              &loadNodesListAnchor);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": searchQueryGraph returned %s",
                exportRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Get target tables from export nodes
    status = populateJsonTableArray(exportRetinaInput->retinaName,
                                    retinaInfoJson,
                                    &exportNodesListAnchor);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = populateJsonSchemaHints(exportRetinaInput->retinaName,
                                     retinaInfoJson,
                                     &loadNodesListAnchor,
                                     retina->numColumnHints,
                                     retina->columnHints);
    BailIfFailed(status);

    status = populateJsonUdfModulesArray(exportRetinaInput->retinaName,
                                         "udfs/",
                                         retinaInfoJson,
                                         archiveManifest,
                                         retina->dag->getUdfContainer(),
                                         &udfModules,
                                         NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Populate retinaInfoJson with queryStr
    status = populateJsonQueryStr(exportRetinaInput->retinaName,
                                  retinaInfoJson,
                                  retina);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Now convert retinaInfoJson into a JSON file and add that to the manifest
    status = addRetinaInfoJsonToManifest(exportRetinaInput->retinaName,
                                         retinaInfoJson,
                                         archiveManifest);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Pack manifest to tar.gz file. Let user decide if they want the buffer
    // containing the file contents, or they want the buffer written to a file
    status = archivePack(archiveManifest, &retinaBuf, &retinaBufSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Failed to create dataflow.tar.gz: %s",
                exportRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    outputSize = XcalarApiSizeOfOutput(*exportRetinaOutput) + retinaBufSize;

    output = (XcalarApiOutput *) memAlloc(outputSize);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": Could not malloc exportDataflowOutuput"
                " of size %lu bytes",
                exportRetinaInput->retinaName,
                outputSize);
        goto CommonExit;
    }
    exportRetinaOutput = &output->outputResult.exportRetinaOutput;

    exportRetinaOutput->retinaCount = retinaBufSize;
    memcpy(exportRetinaOutput->retina, retinaBuf, retinaBufSize);

    status = StatusOk;
CommonExit:
    if (retinaInfoJson != NULL) {
        json_decref(retinaInfoJson);
        retinaInfoJson = NULL;
    }

    if (retinaBuf != NULL) {
        memFree(retinaBuf);
        retinaBuf = NULL;
    }

    while (exportNodesListAnchor.next != &exportNodesListAnchor) {
        dagNodeListElt = exportNodesListAnchor.next;
        dagNodeListElt->prev->next = dagNodeListElt->next;
        dagNodeListElt->next->prev = dagNodeListElt->prev;

        memFree(dagNodeListElt);
        dagNodeListElt = NULL;
    }

    while (synthesizeNodesListAnchor.next != &synthesizeNodesListAnchor) {
        dagNodeListElt = synthesizeNodesListAnchor.next;
        dagNodeListElt->prev->next = dagNodeListElt->next;
        dagNodeListElt->next->prev = dagNodeListElt->prev;

        memFree(dagNodeListElt);
        dagNodeListElt = NULL;
    }

    while (loadNodesListAnchor.next != &loadNodesListAnchor) {
        dagNodeListElt = loadNodesListAnchor.next;
        dagNodeListElt->prev->next = dagNodeListElt->next;
        dagNodeListElt->next->prev = dagNodeListElt->prev;

        memFree(dagNodeListElt);
        dagNodeListElt = NULL;
    }

    udfModules.removeAll(&EvalUdfModule::del);

    if (archiveManifest != NULL) {
        archiveFreeManifest(archiveManifest);
        archiveManifest = NULL;
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            exportRetinaOutput = NULL;
        }
        outputSize = 0;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
DagLib::exportRetina(XcalarApiExportRetinaInput *exportRetinaInput,
                     XcalarApiOutput **outputOut,
                     size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    DgRetina *retina = NULL;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 exportRetinaInput->retinaName) >=
        (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, NULL, &retina);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": getDataflowInt returned %s",
                exportRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        exportRetinaInt(retina, exportRetinaInput, outputOut, outputSizeOut);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": exportRetinaInt returned %s",
                exportRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }

    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    return status;
}

Status
DagLib::importRetina(XcalarApiImportRetinaInput *importRetinaInput,
                     XcalarApiOutput **importRetinaOutputOut,
                     size_t *importRetinaOutputSizeOut)
{
    Status status = StatusUnknown;
    void *retina;
    size_t retinaCount;
    const char *retinaName;
    bool overwriteExistingUdf;
    RetinaInfo *retinaInfo = NULL;
    uint64_t numUdfModules = 0;
    size_t totalUdfModulesStatusesSize = 0;
    XcalarApiOutput **uploadUdfOutputArray = NULL;
    XcalarApiMakeRetinaInput *makeRetinaInput = NULL;
    size_t makeRetinaInputSize = 0;
    XcalarApiOutput *importRetinaOutput = NULL;
    size_t importRetinaOutputSize = 0;
    size_t *outputSizeArray = NULL;
    Dag *queryGraph = NULL;
    uint64_t numQueryGraphNodes;
    uint64_t ii;
    size_t columnHintsOffset;
    XcalarApiUdfContainer *udfContainer = NULL;
    json_t *retinaJson = NULL;
    json_error_t jsonError;
    bool retinaHasUDFs = true;

    assert(importRetinaInput != NULL);

    retina = importRetinaInput->retina;
    retinaCount = importRetinaInput->retinaCount;
    retinaName = importRetinaInput->retinaName;
    overwriteExistingUdf = importRetinaInput->overwriteExistingUdf;

    if (importRetinaInput->loadRetinaJson) {
        status = allocRetinaInfo(retinaName, &retinaInfo);
        BailIfFailed(status);

        // subtract 1 for the null terminating char
        retinaJson =
            json_loadb((const char *) retina, retinaCount - 1, 0, &jsonError);
        if (retinaJson == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing \"%s\": Error parsing dataflowInfo.json at "
                    "line %d: %s",
                    retinaName,
                    jsonError.line,
                    jsonError.text);
            status = StatusRetinaParseError;
            goto CommonExit;
        }
        status = populateRetinaInfo(retinaJson, retinaName, retinaInfo, NULL);
        BailIfFailed(status);
        retinaHasUDFs = false;
    } else {
        status = parseRetinaFile(retina, retinaCount, retinaName, &retinaInfo);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    makeRetinaInputSize = xcalarApiSizeOfRetinaInputHdr(retinaInfo->numTables);
    for (ii = 0; ii < retinaInfo->numTables; ii++) {
        makeRetinaInputSize +=
            xcalarApiSizeOfRetinaDst(retinaInfo->tableArray[ii]->numColumns);
    }

    // set columnHints to point to after tableArray
    columnHintsOffset = makeRetinaInputSize;
    makeRetinaInputSize +=
        xcalarApiSizeOfRetinaColumnHints(retinaInfo->numColumnHints);

    makeRetinaInput =
        (XcalarApiMakeRetinaInput *) memAlloc(makeRetinaInputSize);
    if (makeRetinaInput == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    for (ii = 0; ii < retinaInfo->numTables; ii++) {
        makeRetinaInput->tableArray[ii] = retinaInfo->tableArray[ii];
    }
    makeRetinaInput->numTargetTables = retinaInfo->numTables;
    makeRetinaInput->numSrcTables = 0;

    // populate column hints
    makeRetinaInput->numColumnHints = retinaInfo->numColumnHints;
    makeRetinaInput->columnHints =
        (Column *) ((size_t) makeRetinaInput + columnHintsOffset);
    memcpy(makeRetinaInput->columnHints,
           retinaInfo->columnHints,
           xcalarApiSizeOfRetinaColumnHints(retinaInfo->numColumnHints));

    assertStatic(sizeof(retinaInfo->retinaDesc.retinaName) ==
                 sizeof(makeRetinaInput->retinaName));
    strlcpy(makeRetinaInput->retinaName,
            retinaInfo->retinaDesc.retinaName,
            sizeof(makeRetinaInput->retinaName));

    numUdfModules = retinaInfo->numUdfModules;
    udfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              moduleName);
    BailIfNull(udfContainer);

    if (strlen(importRetinaInput->udfUserName) > 0 &&
        strlen(importRetinaInput->udfSessionName) > 0) {
        // user supplied the udfContainer of a session
        XcalarApiUdfContainer *udfContainerFromSession = NULL;
        status =
            UserMgr::get()->getUdfContainer(importRetinaInput->udfUserName,
                                            importRetinaInput->udfSessionName,
                                            &udfContainerFromSession);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "could not find container for user %s session %s",
                          importRetinaInput->udfUserName,
                          importRetinaInput->udfSessionName);
            goto CommonExit;
        }
        UserDefinedFunction::copyContainers(udfContainer,
                                            udfContainerFromSession);
    } else {
        // Initialize a udf container based on the retina
        // For now, retinas are in the global namespace - so userIdName, and
        // sessionName must be NULL strings. When we do allow retinas to be
        // imported into a workbook, these will need to be set correctly.
        status = UserDefinedFunction::initUdfContainer(udfContainer,
                                                       NULL,
                                                       NULL,
                                                       retinaName);
        BailIfFailed(status);
    }

    if (numUdfModules > 0) {
        assert(retinaHasUDFs == true);
        // With the new per-retina UDF namespace, if a UDF module already exists
        // in the retina's namespace, it'll be over-written of course (since
        // this doesn't affect any one else's UDF, doing this would seem to
        // match the user's intent). Note that a retina's UDFs will not be
        // permissible to be modified via XD constraints. In the XCE backend
        // there are no such restrictions, so XD can allow such mods if needed
        // in the future simply by supplying the absolute path to the UDFs. For
        // now, the user must download the retina, the UDF modified in the
        // downloaded retina, and the retina re-uploaded - the retina will need
        // to be re-named.
        // XXX: With the new retina UDF namespace, how can status ever be
        // StatusUdfModuleAlreadyExists? Check this, and replace with assert
        // that this status can't be returned, if there are no scenarios
        status =
            UserDefinedFunction::get()->bulkAddUdf(retinaInfo->udfModulesArray,
                                                   numUdfModules,
                                                   udfContainer,
                                                   &uploadUdfOutputArray,
                                                   &outputSizeArray);
        // If module already exists, we can just use it. Failure to upload
        // is not a problem
        status = (status == StatusUdfModuleAlreadyExists) ? StatusOk : status;
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    status = QueryParser::get()->parse(retinaInfo->queryStr,
                                       udfContainer,
                                       &queryGraph,
                                       &numQueryGraphNodes);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Now we finally do the import
    status = makeRetina(queryGraph,
                        makeRetinaInput,
                        makeRetinaInputSize,
                        retinaHasUDFs);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to make Dataflow '%s' during import: %s",
                retinaName,
                strGetFromStatus(status));
        // The original code keeps going....
    }

CommonExit:
    // We need to alloc and populate the output struct for this function
    totalUdfModulesStatusesSize = 0;
    if (uploadUdfOutputArray == NULL) {
        numUdfModules = 0;
    } else {
        assert(outputSizeArray != NULL);
        for (ii = 0; ii < numUdfModules; ii++) {
            // the output size takes in the output hdr into account,
            // but we are only copying the output result here
            outputSizeArray[ii] -= offsetof(XcalarApiOutput, outputResult);
            totalUdfModulesStatusesSize += outputSizeArray[ii];
        }
    }

    importRetinaOutputSize =
        XcalarApiSizeOfOutput(
            importRetinaOutput->outputResult.importRetinaOutput) +
        DagLib::sizeOfImportRetinaOutputBuf(numUdfModules,
                                            totalUdfModulesStatusesSize);
    importRetinaOutput = (XcalarApiOutput *) memAlloc(importRetinaOutputSize);
    if (importRetinaOutput == NULL) {
        // No matter what the previous status was, failure to alloc
        // an output means we return StatusNoMem
        status = StatusNoMem;
    } else {
        // Because we sized it right
        verifyOk(populateImportRetinaOutput(importRetinaOutput,
                                            importRetinaOutputSize,
                                            numUdfModules,
                                            uploadUdfOutputArray,
                                            outputSizeArray));
    }

    // Now we perform the clean up for all data structs alloc'ed in this
    // function
    if (uploadUdfOutputArray != NULL) {
        for (ii = 0; ii < numUdfModules; ii++) {
            assert(uploadUdfOutputArray[ii] != NULL);
            memFree(uploadUdfOutputArray[ii]);
            uploadUdfOutputArray[ii] = NULL;
        }
        memFree(uploadUdfOutputArray);
        uploadUdfOutputArray = NULL;
    }

    if (queryGraph != NULL) {
        Status status2 = destroyDag(queryGraph, DagTypes::DestroyDeleteNodes);
        assert(status2 == StatusOk);
        queryGraph = NULL;
    }

    if (makeRetinaInput != NULL) {
        memFree(makeRetinaInput);
        makeRetinaInput = NULL;
    }

    if (retinaInfo != NULL) {
        destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }

    if (outputSizeArray != NULL) {
        memFree(outputSizeArray);
    }

    if (udfContainer != NULL) {
        memFree(udfContainer);
    }

    if (retinaJson != NULL) {
        json_decref(retinaJson);
        retinaJson = NULL;
    }

    // We have to return an output regardless, even if output == NULL
    *importRetinaOutputOut = importRetinaOutput;
    *importRetinaOutputSizeOut = importRetinaOutputSize;

    return status;
}

Status
DagLib::makeRetina(Dag *srcDag,
                   XcalarApiMakeRetinaInput *makeRetinaInput,
                   size_t inputSize,
                   bool hasUDFs)
{
    Status status = StatusUnknown;
    Status status2 = StatusUnknown;
    Dag *dstDag = NULL;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaId retinaId = XidInvalid;
    RetinaNsObject retinaNsObject(XidInvalid);
    bool addedToDlm = false;
    bool publishedToNS = false;
    RetinaTemplate *retinaTemplate = NULL;
    size_t retinaTemplateSize;
    char *serializedDag = NULL;
    size_t serializedDagSize = 0;
    uint8_t *ptr = NULL;
    DagTypes::NodeName *targetNodesNameArray = NULL;
    DagTypes::NodeName *sourceNodesNameArray = NULL;
    EvalUdfModuleSet udfModules;
    XcalarApiUdfContainer *retinaUdfContainer = NULL;
    uint64_t ii;

    assert(makeRetinaInput != NULL);
    MakeRetinaParam *makeRetinaParam = NULL;
    size_t makeRetinaParamSize;
    RetinaInfo **retinaInfos = NULL;
    unsigned numRetinas = 0;
    json_t *retinaQueryJson = NULL;

    if (makeRetinaInput->numTargetTables == 0) {
        status = StatusInval;
        goto CommonExit;
    }

    for (ii = 0; ii < makeRetinaInput->numTargetTables; ii++) {
        if (makeRetinaInput->tableArray[ii]->numColumns == 0) {
            status = StatusExportNoColumns;
            goto CommonExit;
        }
    }

    targetNodesNameArray = (DagTypes::NodeName *) memAlloc(
        makeRetinaInput->numTargetTables * sizeof(targetNodesNameArray[0]));
    BailIfNull(targetNodesNameArray);

    for (ii = 0; ii < makeRetinaInput->numTargetTables; ii++) {
        // check if export node already exists
        DagNodeTypes::Node *exportNode;
        char exportName[TableMaxNameLen + 1];
        status = strSnprintf(exportName,
                             sizeof(exportName),
                             "%s%s",
                             XcalarApiLrqExportPrefix,
                             makeRetinaInput->tableArray[ii]->target.name);
        BailIfFailed(status);

        status = srcDag->lookupNodeByName(exportName,
                                          &exportNode,
                                          Dag::TableScope::LocalOnly,
                                          true);
        if (status == StatusOk) {
            status = strStrlcpy(targetNodesNameArray[ii],
                                exportName,
                                sizeof(targetNodesNameArray[ii]));
            BailIfFailed(status);
        } else {
            assertStatic(sizeof(targetNodesNameArray[ii]) <=
                         sizeof(makeRetinaInput->tableArray[ii]->target));
            status = strStrlcpy(targetNodesNameArray[ii],
                                makeRetinaInput->tableArray[ii]->target.name,
                                sizeof(targetNodesNameArray[ii]));
            BailIfFailed(status);
        }
    }

    // Note that although any embedded retinas in the srcDag are obtained via
    // the following call, and copied into retinaInfos, the real work of copying
    // the retinas' UDFs into the new retina's DAG's UDF container, occurs in
    // copyRetinaToNewDag() during the cloneDag() call, as part of the
    // ExpandRetina flag processing.
    status = srcDag->getRetinas(makeRetinaInput->numTargetTables,
                                targetNodesNameArray,
                                &numRetinas,
                                &retinaInfos);
    BailIfFailed(status);

    sourceNodesNameArray = (DagTypes::NodeName *) memAlloc(
        makeRetinaInput->numSrcTables * sizeof(sourceNodesNameArray[0]));
    BailIfNull(sourceNodesNameArray);

    for (unsigned ii = 0; ii < makeRetinaInput->numSrcTables; ii++) {
        strlcpy(sourceNodesNameArray[ii],
                makeRetinaInput->srcTables[ii].dstName,
                sizeof(sourceNodesNameArray[ii]));
    }

    // Init the retina UDF container in which the new dag dstDag will be housed.
    //
    // No userId, or workbook; just the retina name is passed to init container
    // in the future, when we have retinas inside workbooks, all three will be
    // needed

    retinaUdfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              moduleName);
    BailIfNull(retinaUdfContainer);

    status = UserDefinedFunction::initUdfContainer(retinaUdfContainer,
                                                   NULL,
                                                   NULL,
                                                   makeRetinaInput->retinaName);
    BailIfFailed(status);

    // makeRetina is called in one of two ways:
    //
    // - by importRetina() (or retina upload) - the retina's UDFs have already
    //   been populated into the retina namespace and the dstDag will be
    //   attached to the new retina container (== what's in retinaUdfContainer)
    //   in the call to cloneDag() below. Although it might seem that
    //   cloneDag() isn't strictly necessary since the retina's DAG has
    //   already been created and doesn't need to be cloned, note that the clone
    //   IS needed to prune the retina's DAG using targetNodesNameArray[]
    //   calculated above. Alternatively, we'd need to invent a prune operation,
    //   separate from clone, to carry this out - which doesn't seem worth
    //   doing.
    //
    // - by the make-retina API call (typically from XD when a user creates a
    //   retina from a workbook's dataflow)- in this case, the UDFs haven't been
    //   populated into the retina namespace yet, and need to be so that
    //   attempts to execute the new retina work. The source DAG (or the one
    //   being copied), typically the workbook's DAG from which a retina is to
    //   be made, has two types of UDFs:
    //   (A) Those embedded in a execute-retina API node
    //   (B) Those in the source DAG's UDF container and being used in normal
    //       DAG nodes (i.e. Map, or BulkLoad API nodes)
    //
    //   Both types of UDFs must be copied into dagOut's UDF container (in this
    //   case, it'd be retinaUdfContainer). Copying of (A) will occur in
    //   copyRetinaToNewDag(), and the copying of (B) will occur in
    //   copyDagToNewDag(). These routines are called during cloneDag() -
    //   in response to the use of ExpandRetina, and the CopyUDFs clone flags,
    //   respectively.
    //

    // Create destination dag for retina, expand any nested retinas and
    // convert names appropriately
    //

    status =
        srcDag->cloneDag(&dstDag,
                         DagTypes::QueryGraph,
                         retinaUdfContainer,
                         makeRetinaInput->numTargetTables,
                         targetNodesNameArray,
                         makeRetinaInput->numSrcTables,
                         sourceNodesNameArray,
                         (Dag::CloneFlags)(Dag::ExpandRetina | Dag::ResetState |
                                           (hasUDFs ? Dag::CopyUDFs : 0) |
                                           Dag::ConvertUDFNamesToRelative));
    BailIfFailed(status);

    // When we can choose the export node from the sessions' dag,
    // directly, we'll do that.  But for now, we just manually
    // append an export node.
    status = appendExportNode(dstDag,
                              makeRetinaInput->numTargetTables,
                              makeRetinaInput->tableArray);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add export node to Dataflow '%s': %s",
                makeRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = convertSynthesizeNodes(dstDag, srcDag, makeRetinaInput);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to convert synthesize nodes in Dataflow '%s': %s",
                makeRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = allocMakeRetinaParam(makeRetinaInput,
                                  inputSize,
                                  numRetinas,
                                  retinaInfos,
                                  &makeRetinaParam,
                                  &makeRetinaParamSize);
    BailIfFailed(status);

    makeRetinaParam->srcDagId = srcDag->getId();
    makeRetinaParam->dstDagId = dstDag->getId();

    status = QueryParser::get()->reverseParse(dstDag, &retinaQueryJson);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to reverseParse Dag (%lu) for Dataflow '%s': %s",
                dstDag->getId(),
                makeRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    serializedDag = json_dumps(retinaQueryJson, 0);
    BailIfNull(serializedDag);

    // Add 1 for NULL terminator (serializedDag is just a json string) and
    // we use a memcpy to copy it, which is agnostic of C strings
    serializedDagSize = strlen(serializedDag) + 1;

    retinaId = XidMgr::get()->xidGetNext();
    makeRetinaParam->retinaId = retinaId;

    retinaTemplateSize =
        sizeof(RetinaTemplate) + makeRetinaParamSize + serializedDagSize;

    retinaTemplate =
        (RetinaTemplate *) memAllocExt(retinaTemplateSize, moduleName);
    if (retinaTemplate == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate %zu bytes for Dataflow '%s' template",
                retinaTemplateSize,
                makeRetinaInput->retinaName);
        goto CommonExit;
    }

    // Package up the retina's golden template
    retinaTemplate->retinaId = retinaId;
    retinaTemplate->totalSize = retinaTemplateSize;
    retinaTemplate->makeRetinaParamSize = makeRetinaParamSize;
    retinaTemplate->serializedDagSize = serializedDagSize;
    retinaTemplate->numRetinaUpdates = 0;
    retinaTemplate->updates = NULL;
    ptr = (uint8_t *) &retinaTemplate->serializedContent;
    memcpy((void *) ptr, makeRetinaParam, makeRetinaParamSize);
    ptr = (uint8_t *) ((uintptr_t) ptr + makeRetinaParamSize);
    memcpy((void *) ptr, serializedDag, serializedDagSize);

    // Send the retina to the DLM node for safe-keeping.
    status = addRetinaTemplate(retinaId, retinaTemplate);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add Dataflow '%s' template: %s",
                makeRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    addedToDlm = true;

    // Reinitialize the NS object with the ID
    new (&retinaNsObject) RetinaNsObject(retinaId);

    // Publish the retina name into the global name space along with the
    // object containing the retina ID.  Retina consumers will open the
    // name space to get the retina ID as well as assure protection
    // during their access of the retina.

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 makeRetinaInput->retinaName) >=
        (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    nsId = libNs->publish(retinaPathName, &retinaNsObject, &status);
    if (status != StatusOk) {
        if (status == StatusExist) {
            status = StatusRetinaAlreadyExists;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to publish '%s': %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    publishedToNS = true;

CommonExit:

    if (makeRetinaParam != NULL) {
        memFree(makeRetinaParam);
        makeRetinaParam = NULL;
    }

    if (serializedDag != NULL) {
        memFree(serializedDag);
        serializedDag = NULL;
    }

    if (retinaQueryJson != NULL) {
        json_decref(retinaQueryJson);
        retinaQueryJson = NULL;
    }

    if (retinaTemplate != NULL) {
        memFree(retinaTemplate);
        retinaTemplate = NULL;
    }

    if (dstDag != NULL) {
        // The Dag has been dehydrated and included in the retina golden
        // template which now resides on the dlm node.  It will get recreated
        // whenever a retina is hydrated.  Thus the Dag is no longer needed.
        status2 = destroyDag(dstDag, DagTypes::DestroyDeleteNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy Dag for Dataflow %s: %s",
                    makeRetinaInput->retinaName,
                    strGetFromStatus(status2));
        }
        dstDag = NULL;
    }

    if (targetNodesNameArray != NULL) {
        memFree(targetNodesNameArray);
        targetNodesNameArray = NULL;
    }

    if (sourceNodesNameArray != NULL) {
        memFree(sourceNodesNameArray);
        sourceNodesNameArray = NULL;
    }

    if (retinaUdfContainer != NULL) {
        memFree(retinaUdfContainer);
        retinaUdfContainer = NULL;
    }

    if (retinaInfos) {
        for (ii = 0; ii < numRetinas; ii++) {
            if (retinaInfos[ii]) {
                DagLib::get()->destroyRetinaInfo(retinaInfos[ii]);
                retinaInfos[ii] = NULL;
            }
        }

        memFree(retinaInfos);
        retinaInfos = NULL;
    }

    if (status != StatusOk) {
        if (publishedToNS) {
            bool objRemoved;
            assert(nsId != LibNsTypes::NsInvalidId);
            status2 = libNs->remove(nsId, &objRemoved);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to remove Dataflow '%s'(ID %lu, NS ID %lu): %s",
                        retinaPathName,
                        retinaId,
                        nsId,
                        strGetFromStatus(status2));
            }
        }
        if (addedToDlm) {
            Status status2 = deleteRetinaTemplate(retinaId);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to delete Dataflow '%s'(ID %lu, NS ID %lu) "
                        "during clean up:%s",
                        retinaPathName,
                        retinaId,
                        nsId,
                        strGetFromStatus(status2));
            }
        }
    }

    assert(dstDag == NULL);

    return status;
}

Status
DagLib::deleteRetina(const char *retinaName)
{
    Status status;
    RetinaId retinaId;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    bool pathOpened = false;
    bool objDeleted;
    EvalUdfModuleSet udfModules;
    EvalUdfModule *module;
    XcalarApiUdfDeleteInput deleteUdfInput;
    UserDefinedFunction *udfLib = UserDefinedFunction::get();
    DgRetina *retina = NULL;
    char *modNameDup = NULL;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // A successful read-write open guarantees that we're the only
    // opener and can thus delete the retina.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::WriterExcl,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (read-write): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, NULL, &retina);
    // When deleting a retina, its dag is gotten for the pure purposes of
    // deleting any UDFs contained in the retina's dag. Since the retina might
    // reference UDFs not contained inside it (e.g. for a retina imported as
    // json (aka 'json-retina', this is true), the call to getRetinaInt() might
    // fail since the UDF reference in its query graph might not be resolvable
    // (since the retina's dag container doesn't have these UDFs) - this is fine
    // - all this means is that we don't need to delete any UDFs, and the
    // deletion of the retina can proceed without having to delete any UDFs.
    if (status != StatusOk && status != StatusAstNoSuchFunction) {
        // failure other than StatusAstNoSuchFunction - bail
        goto CommonExit;
    } else if (status == StatusOk) {
        // if a retina is successfully gotten, then search it for UDFs to delete
        // and delete them
        status =
            searchQueryGraph(retina->dag, &udfModules, NULL, NULL, NULL, NULL);
        BailIfFailed(status);

        for (EvalUdfModuleSet::iterator it = udfModules.begin();
             (module = it.get()) != NULL;
             it.next()) {
            if (modNameDup != NULL) {
                memFree(modNameDup);
            }
            modNameDup = strAllocAndCopy(module->moduleName);
            if (strncmp(basename(modNameDup),
                        UserDefinedFunction::DefaultModuleName,
                        strlen(UserDefinedFunction::DefaultModuleName)) == 0 ||
                strncmp(module->moduleName,
                        UserDefinedFunction::SharedUDFsDirPath,
                        strlen(UserDefinedFunction::SharedUDFsDirPath)) == 0) {
                // XXX:
                // There are some alternatives here:
                // (A) searchQueryGraph above must never return modules whose
                // fully
                //     qualified path comes from any namespace other than
                //     retina->dag->getUdfContainer() space - in which case this
                //     strncmp() to skip over shared and default UDFs isn't
                //     needed
                // (B) This code path here should either skip any modules which
                // come
                //     from different paths, as done above, OR choose only those
                //     which come from retina->dag->getUdfContainer() space
                //
                //  For now, choose (B), skipping shared modules including
                //  default They're never OK to delete when a retina's being
                //  deleted
                continue;
            }
            strlcpy(deleteUdfInput.moduleName,
                    module->moduleName,
                    sizeof(deleteUdfInput.moduleName));
            Status status2 = udfLib->deleteUdf(&deleteUdfInput,
                                               retina->dag->getUdfContainer());
            if (status2 != StatusOk && status2 != StatusUdfModuleNotFound) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to delete UDF %s in dataflow '%s' (ID %lu): %s",
                        module->moduleName,
                        retinaName,
                        retinaId,
                        strGetFromStatus(status2));
                // Not a hard error here.
            }
        }
    }  // else status == StatusAstNoSuchFunction -> no UDFs to delete; proceed
       // to retina deletion

    status = deleteRetinaTemplate(retinaId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete Dataflow template '%s' (ID %lu): %s",
                retinaName,
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = libNs->remove(retinaPathName, &objDeleted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to remove '%s': %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(objDeleted == false);

    status = QueryManager::get()->requestQueryDelete(retinaName);
    if (status != StatusOk && (status != StatusQrQueryNotExist ||
                               status != StatusStatsCollectionInProgress)) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete retina's query %s: %s",
                retinaName,
                strGetFromStatus(status));
        // non-fatal
    }

    status = StatusOk;

CommonExit:
    if (modNameDup != NULL) {
        memFree(modNameDup);
        modNameDup = NULL;
    }
    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, &objDeleted);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        } else if (objDeleted) {
            xSyslog(moduleName,
                    XlogDebug,
                    "Successfully deleted '%s'",
                    retinaPathName);
        }
    }
    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }
    udfModules.removeAll(&EvalUdfModule::del);

    return status;
}

Status
DagLib::listRetinas(const char *namePattern,
                    XcalarApiOutput **apiOutputOut,
                    size_t *outputSizeOut)
{
    Status status = StatusOk;
    size_t outputSize;
    XcalarApiOutput *output = NULL;
    unsigned ii;
    LibNsTypes::PathInfoOut *pathInfo = NULL;
    char pattern[LibNsTypes::MaxPathNameLen];
    size_t numRetinaIds;
    unsigned ret;
    LibNs *libNs = LibNs::get();

    ret = snprintf(pattern,
                   sizeof(pattern),
                   "%s%s",
                   RetinaNsObject::PathPrefix,
                   namePattern);
    assert(ret < sizeof(pattern));

    XcalarApiListRetinasOutput *listRetinasOutput = NULL;

    assert(apiOutputOut != NULL);
    assert(outputSizeOut != NULL);

    numRetinaIds = libNs->getPathInfo(pattern, &pathInfo);

    outputSize = XcalarApiSizeOfOutput(*listRetinasOutput) +
                 (sizeof(listRetinasOutput->retinaDescs[0]) * numRetinaIds);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    listRetinasOutput = &output->outputResult.listRetinasOutput;

    for (ii = 0; ii < numRetinaIds; ii++) {
        if (!pathInfo[ii].pendingRemoval) {
            // Strip off the path prefix
            strlcpy(listRetinasOutput->retinaDescs[ii].retinaName,
                    &pathInfo[ii].pathName[strlen(RetinaNsObject::PathPrefix)],
                    sizeof(listRetinasOutput->retinaDescs[ii].retinaName));
        }
    }

    listRetinasOutput->numRetinas = numRetinaIds;

    *apiOutputOut = output;
    *outputSizeOut = outputSize;

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
    }
    if (pathInfo != NULL) {
        memFree(pathInfo);
        pathInfo = NULL;
    }

    return status;
}

Status
DagLib::isValidRetinaName(const char *retinaName)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    char retinaPathName[LibNsTypes::MaxPathNameLen];

    // Create a fully qualified path name using the supplied retina name.
    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    nsId = libNs->getNsId(retinaPathName);
    if (nsId == LibNsTypes::NsInvalidId) {
        status = StatusRetinaNotFound;
    }

CommonExit:

    return status;
}

// Returns a copy of the retina template for the specified retina Id.
Status
DagLib::getRetinaTemplate(const RetinaId retinaId, RetinaTemplate **templateOut)
{
    Status status = StatusOk;
    NodeId dlmNode = retinaId % Config::get()->getActiveNodes();
    RetinaTemplate *srcRetinaTemplate = NULL;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();

    RetinaTemplateMsg retinaTemplateMsg;
    retinaTemplateMsg.operation = GetTemplate;
    retinaTemplateMsg.retinaId = retinaId;

    RetinaTemplateMsgResult retinaTemplateMsgResult;
    retinaTemplateMsgResult.operation = GetTemplate;

    msgMgr->twoPcEphemeralInit(&eph,
                               &retinaTemplateMsg,
                               sizeof(RetinaTemplateMsg),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDlmRetinaTemplate1,
                               &retinaTemplateMsgResult,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDlmRetinaTemplate,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed twoPc to obtain Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = retinaTemplateMsgResult.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to obtain Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(retinaTemplateMsgResult.outputSize != 0);
    assert(retinaTemplateMsgResult.retinaTemplate != NULL);
    srcRetinaTemplate =
        (RetinaTemplate *) retinaTemplateMsgResult.retinaTemplate;
    // updates was a local pointer and is invalid here
    srcRetinaTemplate->updates = NULL;
    *templateOut = srcRetinaTemplate;

CommonExit:
    return status;
}

// Do the update in the hash table.  This is done "transactionally" in that it
// is guaranteed either the new template is in use (update success) or the old
// remains (update failure).
Status
DagLib::updateTemplateInHT(const RetinaId retinaId,
                           const RetinaUpdateInput *retinaUpdateInput)
{
    Status status = StatusOk;
    RetinaIdToTemplateHTEntry *htEntry;
    RetinaTemplate *oldTemplate = NULL;
    RetinaTemplate *newTemplate = NULL;
    size_t newTemplateSize;
    bool locked = false;
    void *srcPtr;
    void *dstPtr;

    retinaIdToTemplateHTLock_.lock();
    locked = true;

    htEntry = retinaIdToTemplateHT_.find(retinaId);
    if (htEntry == NULL) {
        status = StatusRetinaNotFound;
        goto CommonExit;
    }

    oldTemplate = htEntry->retinaTemplate;

    // replacing the old serialized dag with the new one
    newTemplateSize = oldTemplate->totalSize - oldTemplate->serializedDagSize +
                      retinaUpdateInput->serializedDagSize;
    newTemplate = (RetinaTemplate *) memAllocExt(newTemplateSize, moduleName);
    if (newTemplate == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // Move the old content to the new template
    newTemplate->retinaId = oldTemplate->retinaId;
    newTemplate->totalSize = newTemplateSize;
    newTemplate->makeRetinaParamSize = oldTemplate->makeRetinaParamSize;
    newTemplate->serializedDagSize = retinaUpdateInput->serializedDagSize;

    // XXX: just keep track of the number of updates for now, when we
    // implement proper undo/redo we can keep track of the actual updates
    newTemplate->updates = oldTemplate->updates;
    newTemplate->numRetinaUpdates = oldTemplate->numRetinaUpdates + 1;

    // Move makeRetinaParam
    dstPtr = (void *) &newTemplate->serializedContent;
    srcPtr = (void *) &oldTemplate->serializedContent;
    memcpy(dstPtr, srcPtr, oldTemplate->makeRetinaParamSize);

    dstPtr = (void *) ((uintptr_t) dstPtr + oldTemplate->makeRetinaParamSize);
    // Use the updated serialized Dag.
    srcPtr = (void *) &retinaUpdateInput->serializedDag;
    memcpy(dstPtr, srcPtr, retinaUpdateInput->serializedDagSize);

    // Do the swap in the hash table
    htEntry->retinaTemplate = newTemplate;
    memFree(oldTemplate);

CommonExit:
    if (locked) {
        retinaIdToTemplateHTLock_.unlock();
        locked = false;
    }

    return status;
}

Status
DagLib::addRetinaTemplate(const RetinaId retinaId,
                          const RetinaTemplate *retinaTemplate)
{
    Status status = StatusOk;
    NodeId dlmNode = retinaId % Config::get()->getActiveNodes();
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();
    size_t retinaTemplateMsgSize =
        sizeof(RetinaTemplateMsg) + retinaTemplate->totalSize;
    RetinaTemplateMsgResult retinaTemplateMsgResult;

    RetinaTemplateMsg *retinaTemplateMsg =
        (RetinaTemplateMsg *) memAllocExt(retinaTemplateMsgSize, moduleName);
    BailIfNull(retinaTemplateMsg);

    retinaTemplateMsg->operation = AddTemplate;
    retinaTemplateMsg->retinaId = retinaId;
    memcpy(&retinaTemplateMsg->retinaAddInput.retinaTemplate,
           retinaTemplate,
           retinaTemplate->totalSize);
    retinaTemplateMsg->retinaAddInput.retinaTemplateSize =
        retinaTemplate->totalSize;

    retinaTemplateMsgResult.operation = AddTemplate;

    msgMgr->twoPcEphemeralInit(&eph,
                               retinaTemplateMsg,
                               retinaTemplateMsgSize,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDlmRetinaTemplate1,
                               &retinaTemplateMsgResult,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDlmRetinaTemplate,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrOnly),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed twoPc to update Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = retinaTemplateMsgResult.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to update Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (retinaTemplateMsg != NULL) {
        memFree(retinaTemplateMsg);
        retinaTemplateMsg = NULL;
    }

    return status;
}

// Sends the updated serialized dag for the retina to the dlm node.  The new
// serialized dag will replace the old one.
Status
DagLib::updateRetinaTemplate(const RetinaId retinaId,
                             const RetinaUpdateInput *retinaUpdateInput)
{
    Status status = StatusOk;
    assert(retinaId != XidInvalid);
    NodeId dlmNode = retinaId % Config::get()->getActiveNodes();
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();
    size_t retinaTemplateMsgSize =
        sizeof(RetinaTemplateMsg) + retinaUpdateInput->serializedDagSize;
    RetinaTemplateMsgResult retinaTemplateMsgResult;
    RetinaTemplateMsg *retinaTemplateMsg =
        (RetinaTemplateMsg *) memAllocExt(retinaTemplateMsgSize, moduleName);
    BailIfNull(retinaTemplateMsg);

    retinaTemplateMsg->operation = UpdateTemplate;
    retinaTemplateMsg->retinaId = retinaId;
    memcpy(&retinaTemplateMsg->retinaUpdateInput,
           retinaUpdateInput,
           sizeof(RetinaUpdateInput) + retinaUpdateInput->serializedDagSize);

    retinaTemplateMsgResult.operation = UpdateTemplate;

    msgMgr->twoPcEphemeralInit(&eph,
                               retinaTemplateMsg,
                               retinaTemplateMsgSize,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDlmRetinaTemplate1,
                               &retinaTemplateMsgResult,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDlmRetinaTemplate,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrOnly),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed twoPc to update Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = retinaTemplateMsgResult.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to update Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (retinaTemplateMsg != NULL) {
        memFree(retinaTemplateMsg);
        retinaTemplateMsg = NULL;
    }

    return status;
}

// Deletes the retina template associated with the specified retina ID.  The
// caller is responsible for protecting this operation by having a read-write
// open on the retina name.
Status
DagLib::deleteRetinaTemplate(const RetinaId retinaId)
{
    Status status = StatusOk;
    assert(retinaId != XidInvalid);
    NodeId dlmNode = retinaId % Config::get()->getActiveNodes();
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();

    RetinaTemplateMsg retinaTemplateMsg;
    retinaTemplateMsg.operation = DeleteTemplate;
    retinaTemplateMsg.retinaId = retinaId;

    RetinaTemplateMsgResult retinaTemplateMsgResult;
    retinaTemplateMsgResult.operation = DeleteTemplate;

    msgMgr->twoPcEphemeralInit(&eph,
                               &retinaTemplateMsg,
                               sizeof(RetinaTemplateMsg),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcDlmRetinaTemplate1,
                               &retinaTemplateMsgResult,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcDlmRetinaTemplate,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrOnly),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed twoPc to delete Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = retinaTemplateMsgResult.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete Dataflow template '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
DagLib::getRetinaIntFromSerializedDag(
    const char *retinaStr,
    size_t retinaSize,
    char *retinaName,
    XcalarApiUdfContainer *sessionUdfContainer,
    DgRetina **retinaOut)
{
    Status status = StatusOk;
    json_t *retinaJson = NULL;
    json_error_t jsonError;
    RetinaInfo *retinaInfo = NULL;
    DagLib::DgRetina *retina = NULL;
    Dag *srcDag = NULL;
    uint64_t numQueryGraphNodes = 0;
    uint64_t ii = 0;
    DagTypes::NodeName *targetNodesNameArray = NULL;
    XcalarApiUdfContainer *udfContainerForParse = NULL;
    XcalarApiUdfContainer *retinaUdfContainer = NULL;

    status = allocRetinaInfo(retinaName, &retinaInfo);
    BailIfFailed(status);
    retinaJson = json_loadb(retinaStr, retinaSize - 1, 0, &jsonError);
    if (retinaJson == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\": Error parsing dataflow at "
                "line %d: %s",
                retinaName,
                jsonError.line,
                jsonError.text);
        status = StatusRetinaParseError;
        goto CommonExit;
    }
    status = populateRetinaInfo(retinaJson, retinaName, retinaInfo, NULL);
    BailIfFailed(status);
    if (retinaInfo->numTables == 0) {
        status = StatusInval;
        goto CommonExit;
    }
    for (ii = 0; ii < retinaInfo->numTables; ii++) {
        if (retinaInfo->tableArray[ii]->numColumns == 0) {
            status = StatusExportNoColumns;
            goto CommonExit;
        }
    }

    // setup udfContainer
    retinaUdfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              moduleName);
    BailIfNull(retinaUdfContainer);
    // no userId, or workbook; just the retina name is passed to init
    // container
    status = UserDefinedFunction::initUdfContainer(retinaUdfContainer,
                                                   NULL,
                                                   NULL,
                                                   retinaName);
    BailIfFailed(status);
    if (sessionUdfContainer != NULL) {
        udfContainerForParse = sessionUdfContainer;
    } else {
        udfContainerForParse = retinaUdfContainer;
    }

    status = QueryParser::get()->parse(retinaInfo->queryStr,
                                       udfContainerForParse,
                                       &srcDag,
                                       &numQueryGraphNodes);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // fix up export nodes first
    targetNodesNameArray = (DagTypes::NodeName *) memAlloc(
        retinaInfo->numTables * sizeof(targetNodesNameArray[0]));
    BailIfNull(targetNodesNameArray);

    for (ii = 0; ii < retinaInfo->numTables; ii++) {
        // check if export node already exists
        DagNodeTypes::Node *exportNode;
        char exportName[TableMaxNameLen + 1];
        status = strSnprintf(exportName,
                             sizeof(exportName),
                             "%s%s",
                             XcalarApiLrqExportPrefix,
                             retinaInfo->tableArray[ii]->target.name);
        BailIfFailed(status);

        status = srcDag->lookupNodeByName(exportName,
                                          &exportNode,
                                          Dag::TableScope::LocalOnly,
                                          true);
        if (status == StatusOk) {
            status = strStrlcpy(targetNodesNameArray[ii],
                                exportName,
                                sizeof(targetNodesNameArray[ii]));
            BailIfFailed(status);
        } else {
            assertStatic(sizeof(targetNodesNameArray[ii]) <=
                         sizeof(retinaInfo->tableArray[ii]->target));
            status = strStrlcpy(targetNodesNameArray[ii],
                                retinaInfo->tableArray[ii]->target.name,
                                sizeof(targetNodesNameArray[ii]));
            BailIfFailed(status);
        }
    }
    status =
        appendExportNode(srcDag, retinaInfo->numTables, retinaInfo->tableArray);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add export node to Dataflow '%s': %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Now build the DgRetina from the retinaInfo built above
    retinaSize = sizeof(*retina) +
                 (sizeof(retina->targetNodeId[0]) * retinaInfo->numTables);
    retina = (DagLib::DgRetina *) memAllocExt(retinaSize, moduleName);
    BailIfNull(retina);

    new (retina) DagLib::DgRetina();

    retina->columnHints =
        (Column *) memAllocExt(retinaInfo->numColumnHints *
                                   sizeof(*retina->columnHints),
                               moduleName);
    BailIfNull(retina->columnHints);

    retina->numColumnHints = retinaInfo->numColumnHints;
    memcpy(retina->columnHints,
           retinaInfo->columnHints,
           retinaInfo->numColumnHints * sizeof(*retina->columnHints));

    retina->dag = srcDag;
    retina->numTargets = retinaInfo->numTables;

    for (ii = 0; ii < retinaInfo->numTables; ++ii) {
        // check if export node already exists
        DagNodeTypes::Node *exportNode;
        char exportName[TableMaxNameLen + 1];
        status = strSnprintf(exportName,
                             sizeof(exportName),
                             "%s%s",
                             XcalarApiLrqExportPrefix,
                             retinaInfo->tableArray[ii]->target.name);
        BailIfFailed(status);
        status = srcDag->lookupNodeByName(exportName,
                                          &exportNode,
                                          Dag::TableScope::LocalOnly,
                                          true);
        if (status == StatusOk) {
            status = strStrlcpy(targetNodesNameArray[ii],
                                exportName,
                                sizeof(targetNodesNameArray[ii]));
            BailIfFailed(status);
        } else {
            assertStatic(sizeof(targetNodesNameArray[ii]) <=
                         sizeof(retinaInfo->tableArray[ii]->target));
            status = strStrlcpy(targetNodesNameArray[ii],
                                retinaInfo->tableArray[ii]->target.name,
                                sizeof(targetNodesNameArray[ii]));
            BailIfFailed(status);
        }
        status =
            retina->dag->getDagNodeId(retinaInfo->tableArray[ii]->target.name,
                                      Dag::TableScope::LocalOnly,
                                      &retina->targetNodeId[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get target %s: %s",
                    retinaInfo->tableArray[ii]->target.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }
    status = populateParamHashTables(retina);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to populate param hash tables for Dataflow '%s': %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // The retina's dag's udf container must of course be the retinaUdfContainer
    // always
    retina->dag->initContainer(retinaUdfContainer);

    xSyslog(moduleName,
            XlogDebug,
            "Hydrated Dataflow '%lu' user '%s', workbook '%s', retinaName '%s'",
            retina->id,
            retinaUdfContainer->userId.userIdName,
            retinaUdfContainer->sessionInfo.sessionName,
            retinaUdfContainer->retinaName);

    *retinaOut = retina;

CommonExit:
    if (retinaUdfContainer != NULL) {
        memFree(retinaUdfContainer);
        retinaUdfContainer = NULL;
    }

    if (retinaInfo != NULL) {
        destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }

    if (targetNodesNameArray != NULL) {
        memFree(targetNodesNameArray);
        targetNodesNameArray = NULL;
    }

    if (status != StatusOk) {
        if (retina != NULL) {
            destroyRetina(retina);
            retina = NULL;
        }
    }

    if (retinaJson != NULL) {
        json_decref(retinaJson);
        retinaJson = NULL;
    }

    return (status);
}

// Returns a copy of the retina associated with the specified Id.  It is up
// to the caller to have opened the retina path name in LibNs to protect the
// access.
Status
DagLib::getRetinaInt(const RetinaId retinaId,
                     XcalarApiUdfContainer *sessionUdfContainer,
                     DgRetina **retinaOut)
{
    Status status = StatusOk;
    DgRetina *retina = NULL;
    RetinaTemplate *retinaTemplate = NULL;
    MakeRetinaParam *makeRetinaParam = NULL;
    size_t makeRetinaParamSize;
    size_t makeRetinaParamHdrSize;
    uint8_t *ptr;
    XcalarApiMakeRetinaInput *retinaInput = NULL;
    char *serializedDag;
    Dag *srcDag = NULL;
    size_t retinaSize;
    DagTypes::NodeName *targetNodesNameArray = NULL;
    XcalarApiUdfContainer *retinaUdfContainer = NULL;
    XcalarApiUdfContainer *udfContainerForParse = NULL;
    uint64_t numQueryGraphNodes = 0;

    assert(retinaId != XidInvalid);
    assert(retinaOut != NULL);

    // Get the golden template from the dlm node
    status = getRetinaTemplate(retinaId, &retinaTemplate);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get Dataflow '%lu' template: %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Hydrate the retina from the template

    makeRetinaParamHdrSize = MakeRetinaParamHdrSize(makeRetinaParam);
    makeRetinaParamSize = retinaTemplate->makeRetinaParamSize;

    ptr = (uint8_t *) &retinaTemplate->serializedContent;
    makeRetinaParam = (MakeRetinaParam *) ptr;
    retinaInput = &makeRetinaParam->retinaInput;

    ptr = (uint8_t *) ((uintptr_t) ptr + makeRetinaParamSize);
    serializedDag = (char *) ptr;

    status = xcalarApiDeserializeRetinaInput(retinaInput,
                                             makeRetinaParamSize -
                                                 makeRetinaParamHdrSize);
    // retinaInput should have already been sanity checked.
    assert(status == StatusOk);

    // Now we're ready to construct the retina.

    // First construct a udf Container that's valid for the retina's dag, and
    // attached to the retina's dag. In addition, this can be used even to parse
    // to deserialize the dag from 'serializedDag', if a sessionUdfContainer
    // hasn't been supplied to this routine. Note that the call to
    // getRetinaInt() may be coming from the executeRetina path which may be
    // executing a 'json-retina' that has been imported from json, and has no
    // UDFs of its own (and so needs a container which the caller must supply to
    // executeRetina, and which is passed down to getRetinaInt() here).
    //

    retinaUdfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              moduleName);
    BailIfNull(retinaUdfContainer);

    // no userId, or workbook; just the retina name is passed to init
    // container
    status = UserDefinedFunction::initUdfContainer(retinaUdfContainer,
                                                   NULL,
                                                   NULL,
                                                   makeRetinaParam->retinaInput
                                                       .retinaName);
    BailIfFailed(status);

    // For parsing, the udfContainer to be used depends on whether this is in
    // the context of executing a 'json-retina' which has no UDFs of its own (in
    // which case the container is passed down in 'sessionUdfContainer') or a
    // normal retina. In the latter case, just use the normal retinaUdfContainer
    // constructed above.
    if (sessionUdfContainer != NULL) {
        udfContainerForParse = sessionUdfContainer;
    } else {
        udfContainerForParse = retinaUdfContainer;
    }

    status = QueryParser::get()->parse(serializedDag,
                                       udfContainerForParse,
                                       &srcDag,
                                       &numQueryGraphNodes);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to parse serialized Dag for Dataflow '%lu'",
                    retinaId);

    retinaSize = sizeof(*retina) + (sizeof(retina->targetNodeId[0]) *
                                    retinaInput->numTargetTables);
    retina = (DgRetina *) memAllocExt(retinaSize, moduleName);
    BailIfNull(retina);

    new (retina) DgRetina();

    retina->columnHints =
        (Column *) memAllocExt(retinaInput->numColumnHints *
                                   sizeof(*retina->columnHints),
                               moduleName);
    BailIfNull(retina->columnHints);

    retina->numColumnHints = retinaInput->numColumnHints;
    memcpy(retina->columnHints,
           retinaInput->columnHints,
           retinaInput->numColumnHints * sizeof(*retina->columnHints));

    retina->dag = srcDag;
    retina->numTargets = retinaInput->numTargetTables;

    for (unsigned ii = 0; ii < retinaInput->numTargetTables; ++ii) {
        status =
            retina->dag->getDagNodeId(retinaInput->tableArray[ii]->target.name,
                                      Dag::TableScope::LocalOnly,
                                      &retina->targetNodeId[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get target %s: %s",
                    retinaInput->tableArray[ii]->target.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = populateParamHashTables(retina);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to populate param hash tables for Dataflow '%lu': %s",
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    retina->id = retinaTemplate->retinaId;

    // The retina's dag's udf container must of course be the retinaUdfContainer
    // always
    retina->dag->initContainer(retinaUdfContainer);

    xSyslog(moduleName,
            XlogDebug,
            "Hydrated Dataflow '%lu' user '%s', workbook '%s', retinaName '%s'",
            retina->id,
            retinaUdfContainer->userId.userIdName,
            retinaUdfContainer->sessionInfo.sessionName,
            retinaUdfContainer->retinaName);

    *retinaOut = retina;

CommonExit:
    if (targetNodesNameArray != NULL) {
        memFree(targetNodesNameArray);
        targetNodesNameArray = NULL;
    }

    if (retinaTemplate != NULL) {
        memFree(retinaTemplate);
        retinaTemplate = NULL;
    }

    if (retinaUdfContainer != NULL) {
        memFree(retinaUdfContainer);
    }

    if (status != StatusOk) {
        if (retina != NULL) {
            destroyRetina(retina);
            retina = NULL;
        }
    }

    return status;
}

void
DagLib::putRetinaInt(DgRetina *retina)
{
    Status status = StatusOk;

    if (retina->dag != NULL) {
        status = destroyDag(retina->dag, DagTypes::DestroyDeleteNodes);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy Dag '%lu': %s",
                    retina->dag->getId(),
                    strGetFromStatus(status));
        }
        retina->dag = NULL;
    }

    destroyRetina(retina);
}

Status
DagLib::getRetinaJson(const char *retinaName,
                      XcalarApiUdfContainer *udfContainer,
                      XcalarApiOutput **outputOut,
                      size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    DgRetina *retina = NULL;
    XcalarApiGetRetinaJsonOutput *getRetinaJsonOutput = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;
    json_t *retinaInfoJson = NULL;
    char *retinaInfoJsonStr = NULL;
    Dag::DagNodeListElt exportNodesListAnchor;
    Dag::DagNodeListElt *dagNodeListElt = NULL;

    exportNodesListAnchor.next = exportNodesListAnchor.prev =
        &exportNodesListAnchor;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina and gets the object from
    // where we'll obtain the Retina ID associated with the retina name.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, udfContainer, &retina);
    if (status != StatusOk) {
        goto CommonExit;
    }

    retinaInfoJson = json_object();
    if (retinaInfoJson == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error getting \"%s\": Could not create DataflowInfoJson "
                "root",
                retinaName);
        goto CommonExit;
    }

    status = searchQueryGraph(retina->dag,
                              NULL,
                              NULL,
                              &exportNodesListAnchor,
                              NULL,
                              NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error exporting \"%s\": searchQueryGraph returned %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Get target tables from export nodes
    status = populateJsonTableArray(retinaName,
                                    retinaInfoJson,
                                    &exportNodesListAnchor);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Populate retinaInfoJson with queryStr
    status = populateJsonQueryStr(retinaName, retinaInfoJson, retina);
    if (status != StatusOk) {
        goto CommonExit;
    }

    retinaInfoJsonStr = json_dumps(retinaInfoJson, 0);
    BailIfNull(retinaInfoJsonStr);

    size_t jsonStrLen;

    jsonStrLen = strlen(retinaInfoJsonStr) + 1;

    outputSize = jsonStrLen + XcalarApiSizeOfOutput(*getRetinaJsonOutput);

    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    getRetinaJsonOutput = &output->outputResult.getRetinaJsonOutput;
    getRetinaJsonOutput->retinaJsonLen = jsonStrLen;

    status = strStrlcpy(getRetinaJsonOutput->retinaJson,
                        retinaInfoJsonStr,
                        jsonStrLen);
    assert(status == StatusOk);

    *outputOut = output;
    *outputSizeOut = outputSize;
    status = StatusOk;

CommonExit:
    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }

    if (retinaInfoJson != NULL) {
        json_decref(retinaInfoJson);
        retinaInfoJson = NULL;
    }

    if (retinaInfoJsonStr != NULL) {
        memFree(retinaInfoJsonStr);
        retinaInfoJsonStr = NULL;
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            outputSize = 0;
        }
    }

    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    while (exportNodesListAnchor.next != &exportNodesListAnchor) {
        dagNodeListElt = exportNodesListAnchor.next;
        dagNodeListElt->prev->next = dagNodeListElt->next;
        dagNodeListElt->next->prev = dagNodeListElt->prev;
        memFree(dagNodeListElt);
        dagNodeListElt = NULL;
    }

    return status;
}

Status
DagLib::getRetinaObj(const char *retinaName, DgRetina **retinaOut)
{
    Status status = StatusUnknown;
    DgRetina *retina = NULL;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina and gets the object from
    // where we'll obtain the Retina ID associated with the retina name.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, NULL, &retina);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    *retinaOut = retina;

    return status;
}

Status
DagLib::getRetina(const char *retinaName,
                  XcalarApiOutput **outputOut,
                  size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    DgRetina *retina = NULL;
    XcalarApiGetRetinaOutput *getRetinaOutput = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    unsigned ii, jj;
    uint64_t numNode = 0;
    bool retinaLockAcquired = false;
    void **dagNodesSelected = NULL;
    unsigned nodeIdx = 0;
    size_t hdrSize = 0;
    size_t retinaDagSize = 0;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;
    XcalarApiOutput **serializedDag = NULL;
    size_t *serializedDagOutputSize = NULL;
    size_t totalVariableBufSize = 0;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina and gets the object from
    // where we'll obtain the Retina ID associated with the retina name.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, NULL, &retina);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(retina != NULL);
    retina->lock.lock();
    retinaLockAcquired = true;

    serializedDag = (XcalarApiOutput **)
        memAllocExt(retina->numTargets * sizeof(XcalarApiOutput *), moduleName);

    if (serializedDag == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    serializedDagOutputSize =
        (size_t *) memAllocExt(retina->numTargets * sizeof(size_t), moduleName);

    if (serializedDagOutputSize == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < retina->numTargets; ++ii) {
        status = retina->dag->getDagById(retina->targetNodeId[ii],
                                         &serializedDag[ii],
                                         &serializedDagOutputSize[ii]);
        if (status != StatusOk) {
            for (jj = 0; jj < retina->numTargets; ++jj) {
                if (jj < ii) {
                    memFree(serializedDag[jj]);
                }
                serializedDag[jj] = NULL;
            }
            goto CommonExit;
        }

        // get the amount of memory taken up by the variable bufs
        // (apiInputs, parents, children)
        totalVariableBufSize +=
            serializedDag[ii]->outputResult.dagOutput.bufSize -
            Dag::sizeOfDagOutput(serializedDag[ii]
                                     ->outputResult.dagOutput.numNodes,
                                 0,
                                 0,
                                 0);

        numNode += serializedDag[ii]->outputResult.dagOutput.numNodes;
    }

    hdrSize = XcalarApiSizeOfOutput(*getRetinaOutput) -
              sizeof(getRetinaOutput->retina.retinaDag);
    retinaDagSize = Dag::sizeOfDagOutput(numNode, totalVariableBufSize);
    outputSize = hdrSize + retinaDagSize;

    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    dagNodesSelected =
        (void **) memAllocExt(sizeof(*dagNodesSelected) * numNode, moduleName);
    if (dagNodesSelected == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    getRetinaOutput = &output->outputResult.getRetinaOutput;

    for (ii = 0; ii < retina->numTargets; ++ii) {
        for (jj = 0; jj < serializedDag[ii]->outputResult.dagOutput.numNodes;
             ++jj) {
            dagNodesSelected[nodeIdx++] =
                serializedDag[ii]->outputResult.dagOutput.node[jj];
        }
    }
    assert(numNode == nodeIdx);

    status = DagLib::copyDagNodesToDagOutput(&getRetinaOutput->retina.retinaDag,
                                             retinaDagSize,
                                             dagNodesSelected,
                                             numNode,
                                             retina->dag->getUdfContainer(),
                                             DagNodeTypeXcalarApiDagNode);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to copy Dag nodes: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    strlcpy(getRetinaOutput->retina.retinaDesc.retinaName,
            retinaName,
            sizeof(getRetinaOutput->retina.retinaDesc.retinaName));

    *outputOut = output;
    *outputSizeOut = outputSize;
    status = StatusOk;

CommonExit:
    if (dagNodesSelected != NULL) {
        memFree(dagNodesSelected);
        dagNodesSelected = NULL;
    }

    if (serializedDag != NULL) {
        for (ii = 0; ii < retina->numTargets; ++ii) {
            if (serializedDag[ii] != NULL) {
                memFree(serializedDag[ii]);
            }

            serializedDag[ii] = NULL;
        }
        memFree(serializedDag);
        serializedDag = NULL;
    }

    if (serializedDagOutputSize != NULL) {
        memFree(serializedDagOutputSize);
        serializedDagOutputSize = NULL;
    }

    if (retinaLockAcquired) {
        assert(retina != NULL);
        retina->lock.unlock();
        retinaLockAcquired = false;
    }

    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            outputSize = 0;
        }
    }

    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    return status;
}

Status
DagLib::populateParamHashTables(DgRetina *retina)
{
    void *ptr;
    DagTypes::NodeId nodeId;
    Status status;
    DagNodeTypes::Node *node;
    typedef enum { InVariable, NotInVariable } State;
    State state;
    ParameterHashEltByName *parameterHashEltByName = NULL;
    char **strings = NULL;
    size_t *bufSizes = NULL;
    unsigned numStrings = 0;
    char *parameterNames[XcalarApiMaxNumParameters];
    memZero(parameterNames, sizeof(parameterNames));

    for (unsigned ii = 0; ii < XcalarApiMaxNumParameters; ii++) {
        parameterNames[ii] = (char *) memAlloc(DfMaxFieldNameLen + 1);
        BailIfNull(parameterNames[ii]);
    }

    ptr = memAllocExt(sizeof(*retina->parameterHashTableByName), moduleName);
    if (ptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    retina->parameterHashTableByName = new (ptr) ParamHashTableByName();

    nodeId = retina->dag->hdr_.firstNode;
    while (nodeId != DagTypes::InvalidDagNodeId) {
        unsigned paramIdx = 0;

        status = retina->dag->lookupNodeById(nodeId, &node);
        if (status != StatusOk) {
            assert(0);
            goto CommonExit;
        }

        status =
            getParamStrFromXcalarApiInput(node->dagNodeHdr.apiInput,
                                          node->dagNodeHdr.apiDagNodeHdr.api,
                                          &strings,
                                          &bufSizes,
                                          &numStrings);
        if (status == StatusXcalarApiNotParameterizable) {
            nodeId = node->dagNodeHdr.dagNodeOrder.next;
            continue;
        }
        BailIfFailed(status);

        for (unsigned ii = 0; ii < numStrings; ii++) {
            state = NotInVariable;
            const char *string = strings[ii];
            unsigned idx = 0;

            while (*string != '\0') {
                switch (state) {
                case NotInVariable:
                    if (*string == RetinaDefaultVariableStartMarker) {
                        idx = 0;
                        state = InVariable;
                    }
                    break;
                case InVariable:
                    if (*string == RetinaDefaultVariableEndMarker) {
                        state = NotInVariable;
                        if (paramIdx >= XcalarApiMaxNumParameters) {
                            xSyslog(moduleName,
                                    XlogErr,
                                    "Too many parameters. Max is %u",
                                    XcalarApiMaxNumParameters);
                            return StatusRetinaTooManyParameters;
                        }

                        parameterNames[paramIdx][idx] = '\0';
                        idx = 0;
                        paramIdx++;
                    } else {
                        if (idx >= DfMaxFieldNameLen) {
                            parameterNames[paramIdx][idx] = '\0';
                            xSyslog(moduleName,
                                    XlogErr,
                                    "Parameter name \"%s...\" is too long. Max "
                                    "is %u characters",
                                    parameterNames[paramIdx],
                                    DfMaxFieldNameLen);
                            return StatusNoBufs;
                        }
                        parameterNames[paramIdx][idx++] = *string;
                    }
                    break;
                }
                string++;
            }
        }

        // Add in the new parameters associated with this node
        for (unsigned ii = 0; ii < paramIdx; ii++) {
            parameterHashEltByName =
                retina->parameterHashTableByName->find(parameterNames[ii]);
            if (parameterHashEltByName == NULL) {
                parameterHashEltByName =
                    new (std::nothrow) ParameterHashEltByName;
                if (parameterHashEltByName == NULL) {
                    status = StatusNoMem;
                    goto CommonExit;
                }

                parameterHashEltByName->retina = retina;

                retina->numParameters++;
                refInit(&parameterHashEltByName->refCount,
                        freeParameterHashEltByName);

                strlcpy(parameterHashEltByName->parameter.parameterName,
                        parameterNames[ii],
                        sizeof(
                            parameterHashEltByName->parameter.parameterName));
                parameterHashEltByName->parameter.parameterValue[0] = '\0';

                retina->parameterHashTableByName->insert(
                    parameterHashEltByName);
            }
        }

        nodeId = node->dagNodeHdr.dagNodeOrder.next;

        memFree(strings);
        strings = NULL;

        memFree(bufSizes);
        bufSizes = NULL;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (parameterHashEltByName != NULL) {
            refPut(&parameterHashEltByName->refCount);
            parameterHashEltByName = NULL;
        }
    }

    for (unsigned ii = 0; ii < XcalarApiMaxNumParameters; ii++) {
        if (parameterNames[ii]) {
            memFree(parameterNames[ii]);
            parameterNames[ii] = NULL;
        }
    }

    if (strings) {
        memFree(strings);
        strings = NULL;
    }

    if (bufSizes) {
        memFree(bufSizes);
        bufSizes = NULL;
    }

    return status;
}

// Caller must hold retina lock
void
DagLib::freeParameterHashEltByName(RefCount *refCount)
{
    ParameterHashEltByName *parameterHashEltByName;
    DgRetina *retina;

    parameterHashEltByName =
        ContainerOf(refCount, ParameterHashEltByName, refCount);
    retina = parameterHashEltByName->retina;

    retina->parameterHashTableByName->remove(parameterHashEltByName->getName());
    retina->numParameters--;

    delete parameterHashEltByName;
    parameterHashEltByName = NULL;
    retina = NULL;
}

Status
DagLib::variableSubst(XcalarApiInput *input,
                      XcalarApis paramType,
                      uint64_t numParameters,
                      const XcalarApiParameter parameters[],
                      ParamHashTableByName *defaultParametersHashTable)
{
    Status status = StatusUnknown;
    char **stringsToSubst = NULL;
    size_t *bufSizes = NULL;
    char *stringCursor;
    char *variableStartDelim, *variableEndDelim;
    char *tempBuffer = NULL;
    const char *parameterValue;
    bool parameterFound;
    size_t bufSizeCopied, charsInSrc, maxCharsToCopy, charsCopied;
    char parameterName[sizeof(parameters[0].parameterName)];
    unsigned ii, jj, numStrings;

    status = getParamStrFromXcalarApiInput(input,
                                           paramType,
                                           &stringsToSubst,
                                           &bufSizes,
                                           &numStrings);
    if (status != StatusOk) {
        if (status == StatusXcalarApiNotParameterizable) {
            // Not an error. Just jump out
            status = StatusOk;
        }
        goto CommonExit;
    }

    for (ii = 0; ii < numStrings; ii++) {
        tempBuffer = (char *) memAllocExt(bufSizes[ii], moduleName);
        if (tempBuffer == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        memZero(tempBuffer, bufSizes[ii]);

        bufSizeCopied = 0;
        stringCursor = stringsToSubst[ii];
        while (*stringCursor != '\0' && bufSizeCopied <= bufSizes[ii]) {
            variableStartDelim =
                strchr(stringCursor, RetinaDefaultVariableStartMarker);
            if (variableStartDelim == NULL) {
                maxCharsToCopy = bufSizes[ii] - bufSizeCopied - 1;
                charsInSrc = strlcpy(tempBuffer + bufSizeCopied,
                                     stringCursor,
                                     maxCharsToCopy + 1);
                charsCopied = xcMin(charsInSrc, maxCharsToCopy);
                stringCursor += charsCopied;
                bufSizeCopied += charsCopied;
                break;
            }

            maxCharsToCopy = xcMin((size_t)(variableStartDelim - stringCursor),
                                   bufSizes[ii] - bufSizeCopied - 1);
            charsInSrc = strlcpy(tempBuffer + bufSizeCopied,
                                 stringCursor,
                                 maxCharsToCopy + 1);
            charsCopied = xcMin(maxCharsToCopy, charsInSrc);
            stringCursor += charsCopied;
            bufSizeCopied += charsCopied;
            if (bufSizeCopied >= bufSizes[ii]) {
                break;
            }

            variableEndDelim =
                strchr(stringCursor, RetinaDefaultVariableEndMarker);
            if (variableEndDelim == NULL) {
                maxCharsToCopy = bufSizes[ii] - bufSizeCopied - 1;
                charsInSrc = strlcpy(tempBuffer + bufSizeCopied,
                                     stringCursor,
                                     maxCharsToCopy + 1);
                charsCopied = xcMin(maxCharsToCopy, charsInSrc);
                stringCursor += charsCopied;
                bufSizeCopied += charsCopied;
                break;
            }

            assert(variableEndDelim > variableStartDelim);
            strlcpy(parameterName,
                    variableStartDelim + 1,
                    variableEndDelim - (variableStartDelim + 1) + 1);

            parameterFound = false;
            for (jj = 0; jj < numParameters; jj++) {
                if (strcmp(parameterName, parameters[jj].parameterName) == 0) {
                    parameterFound = true;
                    parameterValue = parameters[jj].parameterValue;
                    break;
                }
            }

            if (!parameterFound && defaultParametersHashTable) {
                ParameterHashEltByName *parameterHashElt = NULL;

                // See if we can find a default value in the hashtable
                parameterHashElt =
                    defaultParametersHashTable->find(parameterName);
                if (parameterHashElt != NULL) {
                    // Yes we found a default value!
                    parameterFound = true;
                    parameterValue = parameterHashElt->parameter.parameterValue;
                }
            }

            if (parameterFound) {
                bufSizeCopied += strlcpy(tempBuffer + bufSizeCopied,
                                         parameterValue,
                                         bufSizes[ii] - bufSizeCopied);
                stringCursor = variableEndDelim + 1;
            } else {
                assert(stringCursor == variableStartDelim);
                maxCharsToCopy =
                    xcMin((size_t)(variableEndDelim - stringCursor),
                          bufSizes[ii] - bufSizeCopied - 1);
                charsInSrc = strlcpy(tempBuffer + bufSizeCopied,
                                     stringCursor,
                                     maxCharsToCopy + 1);
                charsCopied = xcMin(maxCharsToCopy, charsInSrc);
                stringCursor += charsCopied;
                bufSizeCopied += charsCopied;
                if (bufSizeCopied >= bufSizes[ii]) {
                    break;
                }
                assert(stringCursor == variableEndDelim);
            }
        }

        if (bufSizeCopied >= bufSizes[ii]) {
            status = StatusOverflow;
            goto CommonExit;
        }

        strlcpy(stringsToSubst[ii], tempBuffer, bufSizes[ii]);
        memFree(tempBuffer);
        tempBuffer = NULL;
    }

    status = StatusOk;

CommonExit:
    if (tempBuffer != NULL) {
        memFree(tempBuffer);
        tempBuffer = NULL;
    }

    if (stringsToSubst) {
        memFree(stringsToSubst);
        stringsToSubst = NULL;
    }

    if (bufSizes) {
        memFree(bufSizes);
        bufSizes = NULL;
    }

    return status;
}

Status
DagLib::updateRetina(const char *retinaName,
                     const char *retinaJsonStr,
                     size_t retinaJsonStrLen,
                     XcalarApiUdfContainer *udfContainer)
{
    Status status = StatusUnknown;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;
    char *serializedDag = NULL;
    size_t serializedDagSize = 0;
    uint8_t *ptr;
    Dag *updateDag = NULL;
    json_error_t err;
    json_t *retJson = NULL, *queryStrJson = NULL;
    RetinaUpdateInput *retinaUpdateInput = NULL;
    size_t retinaUpdateInputSize = 0;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This read-write open protects our access to the Retina.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::WriterExcl,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (read-write): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    // build a dag based on the input retinaJson
    retJson = json_loads(retinaJsonStr, 0, &err);
    if (retJson == NULL) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to update batch dataflow %s, json parse error "
                      "source %s line %d, column %d, position %d: %s",
                      retinaName,
                      err.source,
                      err.line,
                      err.column,
                      err.position,
                      err.text);
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    queryStrJson = json_object_get(retJson, "query");
    if (queryStrJson == NULL) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to update batch dataflow %s, "
                      "missing query object",
                      retinaName);
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    // Serialize the Dag so that it is generic and can be hydrated
    // on any node.
    serializedDag = json_dumps(queryStrJson, 0);
    if (serializedDag == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to serialize Dag '%lu' for Dataflow %s (%lu): %s",
                updateDag->getId(),
                retinaName,
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Add 1 for NULL terminator (serializedDag is just a json string) and
    // we use a memcpy to copy it, which is agnostic of C strings
    serializedDagSize = strlen(serializedDag) + 1;

    // Build the update information that will be sent to the dlm node.

    retinaUpdateInputSize = sizeof(RetinaUpdateInput) + serializedDagSize;

    retinaUpdateInput =
        (RetinaUpdateInput *) memAllocExt(retinaUpdateInputSize, moduleName);
    if (retinaUpdateInput == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // Send the updated Dag to the dlm node to update the golden template.

    retinaUpdateInput->retinaMsgHdr.retinaId = retinaId;
    retinaUpdateInput->serializedDagSize = serializedDagSize;
    ptr = (uint8_t *) &retinaUpdateInput->serializedDag;
    memcpy((void *) ptr, serializedDag, serializedDagSize);

    // Send the updated Dag to the dlm node to update the golden template.
    status = updateRetinaTemplate(retinaId, retinaUpdateInput);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to update Dataflow template '%s' (ID %lu): %s",
                retinaName,
                retinaId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
        pathOpened = false;  // whether close fails or not, mark as not needed
    }

CommonExit:
    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    if (serializedDag != NULL) {
        memFree(serializedDag);
        serializedDag = NULL;
    }

    if (updateDag != NULL) {
        Status status2 = destroyDag(updateDag, DestroyDeleteNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy dag: %s",
                    strGetFromStatus(status2));
        }

        updateDag = NULL;
    }

    if (retJson) {
        json_decref(retJson);
        retJson = NULL;
    }

    if (retinaUpdateInput != NULL) {
        memFree(retinaUpdateInput);
        retinaUpdateInput = NULL;
    }

    return status;
}

Status
DagLib::variableSubst(Dag *dag,
                      unsigned numParameters,
                      const XcalarApiParameter *parameters)
{
    NodeId dagNodeId;
    DagNodeTypes::Node *dagNode;
    Status status = StatusOk;

    dagNodeId = dag->getFirstNodeIdInOrder();
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = dag->lookupNodeById(dagNodeId, &dagNode);
        assert(status == StatusOk);

        status = variableSubst(dagNode->dagNodeHdr.apiInput,
                               dagNode->dagNodeHdr.apiDagNodeHdr.api,
                               numParameters,
                               parameters,
                               NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to substitute variable in Dag '%lu': %s",
                    dagNodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;
    }

CommonExit:
    return status;
}

Status
DagLib::instantiateRetina(DgRetina *retina,
                          unsigned numParameters,
                          XcalarApiParameter *parameters,
                          XcalarApiUdfContainer *udfContainer,
                          Dag **instanceDagOut)
{
    Status status;
    Dag *instanceDag = NULL;
    NodeId dagNodeId;
    DagNodeTypes::Node *dagNode;
    DagTypes::NodeName *targetNodesNameArray = NULL;
    Optimizer::QueryHints queryHints;
    assert(udfContainer != NULL);

    targetNodesNameArray = (DagTypes::NodeName *) memAlloc(
        retina->numTargets * sizeof(targetNodesNameArray[0]));
    BailIfNull(targetNodesNameArray);

    for (unsigned ii = 0; ii < retina->numTargets; ii++) {
        status = retina->dag->getDagNodeName(retina->targetNodeId[ii],
                                             targetNodesNameArray[ii],
                                             sizeof(targetNodesNameArray[ii]));
        assert(status == StatusOk);
    }

    status = retina->dag->cloneDag(&instanceDag,
                                   DagTypes::QueryGraph,
                                   udfContainer,
                                   retina->numTargets,
                                   targetNodesNameArray,
                                   0,
                                   NULL,
                                   (Dag::CloneFlags)(Dag::ResetState));
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Let's do the variable substitution in the instantiated Dag
    if (numParameters > 0) {
        dagNodeId = instanceDag->getFirstNodeIdInOrder();
        while (dagNodeId != DagTypes::InvalidDagNodeId) {
            status = instanceDag->lookupNodeById(dagNodeId, &dagNode);
            assert(status == StatusOk);

            status = variableSubst(dagNode->dagNodeHdr.apiInput,
                                   dagNode->dagNodeHdr.apiDagNodeHdr.api,
                                   numParameters,
                                   parameters,
                                   retina->parameterHashTableByName);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to substitute variable in Dag '%lu': %s",
                        dagNodeId,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;
        }
    }

    // Optimize the dag
    Optimizer::get()->hintsInit(&queryHints, retina);
    status = Optimizer::get()->optimize(instanceDag, &queryHints);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "queryOptimize failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (instanceDag != NULL) {
            Status tmpStatus;
            tmpStatus = destroyDag(instanceDag, DagTypes::DestroyDeleteNodes);
            assert(tmpStatus == StatusOk);
            instanceDag = NULL;
        }
    }
    if (targetNodesNameArray != NULL) {
        memFree(targetNodesNameArray);
        targetNodesNameArray = NULL;
    }

    *instanceDagOut = instanceDag;

    return status;
}

// most of the code in executeRetinaInt and executeRetina is reflected
// in QueryManager::processDataflow, these two methods are currently only
// used by published table restore
Status
DagLib::executeRetinaInt(DgRetina *retina,
                         const XcalarApiUserId *userId,
                         XcalarApiExecuteRetinaInput *executeRetinaInput,
                         Dag **outputDag)
{
    Status status = StatusUnknown;
    Dag *instanceDag = NULL;
    Runtime::SchedId schedId = Runtime::SchedId::MaxSched;
    XcalarApiUdfContainer *udfContainer = NULL;

    if (strlen(executeRetinaInput->udfUserName) > 0 &&
        strlen(executeRetinaInput->udfSessionName) > 0) {
        status =
            UserMgr::get()->getUdfContainer(executeRetinaInput->udfUserName,
                                            executeRetinaInput->udfSessionName,
                                            &udfContainer);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "could not find container for user %s session %s",
                          executeRetinaInput->udfUserName,
                          executeRetinaInput->udfSessionName);
            goto CommonExit;
        }
    } else {
        udfContainer = retina->dag->getUdfContainer();
    }

    // Instantiate retina
    status =
        instantiateRetina(retina,
                          executeRetinaInput->numParameters,
                          (XcalarApiParameter *) executeRetinaInput->parameters,
                          udfContainer,
                          &instanceDag);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "instantiateBatchDataflow failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Fix up the column names
    status = instanceDag->convertNamesToImmediate();
    BailIfFailed(status);

    if (executeRetinaInput->exportToActiveSession) {
        if (retina->numTargets > 1) {
            xSyslog(moduleName,
                    XlogErr,
                    "Dataflow %s cannot export more than "
                    "one table to active session",
                    executeRetinaInput->retinaName);
            status = StatusInval;
            goto CommonExit;
        }

        status = instanceDag->convertLastExportNode();
        BailIfFailed(status);

        retina->targetNodeId[0] = instanceDag->hdr_.lastNode;
    }

    if (executeRetinaInput->schedName[0] == '\0') {
        schedId = Runtime::SchedId::Sched0;  // Default is Sched0
    } else {
        schedId =
            Runtime::getSchedIdFromName(executeRetinaInput->schedName,
                                        sizeof(executeRetinaInput->schedName));
    }

    if (static_cast<uint8_t>(schedId) >= Runtime::TotalFastPathScheds) {
        status = StatusInval;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Scheduler name %s is invalid: %s",
                      executeRetinaInput->schedName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    status = QueryManager::get()
                 ->processQueryGraph(userId,
                                     &instanceDag,
                                     retina->numTargets,
                                     retina->targetNodeId,
                                     outputDag,
                                     executeRetinaInput->queryName,
                                     udfContainer->sessionInfo.sessionName,
                                     schedId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "queryEvaluate failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (outputDag != NULL) {
        status = (*outputDag)->copyOpDetails(retina->dag);
        assert(status == StatusOk);
    }

CommonExit:
    if (instanceDag != NULL) {
        Status status2 = destroyDag(instanceDag, DagTypes::DestroyDeleteNodes);
        assert(status2 == StatusOk);
        instanceDag = NULL;
    }

    return status;
}

Status
DagLib::executeRetina(const XcalarApiUserId *userId,
                      XcalarApiExecuteRetinaInput *executeRetinaInput,
                      Dag **outputDag)
{
    Status status = StatusUnknown;
    DgRetina *retina = NULL;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;
    XcalarApiUdfContainer *udfContainer = NULL;

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 executeRetinaInput->retinaName) >=
        (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    if (strlen(executeRetinaInput->udfUserName) > 0 &&
        strlen(executeRetinaInput->udfSessionName) > 0) {
        // A json-retina is being executed (i.e. a retina imported from json
        // which has no UDFs of its own). In this case, a udfContainer must be
        // constructed which is needed for the call to getRetinaInt() which
        // needs to parse the serialized dag (which is in the golden
        // template, in json layout) with this container.

        status =
            UserMgr::get()->getUdfContainer(executeRetinaInput->udfUserName,
                                            executeRetinaInput->udfSessionName,
                                            &udfContainer);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "could not find container for user %s session %s",
                          executeRetinaInput->udfUserName,
                          executeRetinaInput->udfSessionName);
            goto CommonExit;
        }
    }

    status = getRetinaInt(retinaId, udfContainer, &retina);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getDataflowInt failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = executeRetinaInt(retina, userId, executeRetinaInput, outputDag);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "executeBatchDataflowInt failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }

    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    return status;
}

Status
DagLib::listParametersInRetina(const char *retinaName,
                               XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    DgRetina *retina = NULL;
    XcalarApiOutput *output = NULL;
    XcalarApiListParametersInRetinaOutput *listParametersInRetinaOutput = NULL;
    size_t outputSize;
    unsigned ii;
    ParameterHashEltByName *parameterHashElt;
    LibNs *libNs = LibNs::get();
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    LibNsTypes::NsHandle nsHandle;
    RetinaId retinaId;
    bool pathOpened = false;

    assert(retinaName != NULL);
    assert(outputSizeOut != NULL);

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina.
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, NULL, &retina);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(retina != NULL);
    retina->lock.lock();

    outputSize = XcalarApiSizeOfOutput(*listParametersInRetinaOutput) +
                 (sizeof(listParametersInRetinaOutput->parameters[0]) *
                  retina->numParameters);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        retina->lock.unlock();
        goto CommonExit;
    }

    listParametersInRetinaOutput =
        &output->outputResult.listParametersInRetinaOutput;

    ii = 0;

    assert(retina->parameterHashTableByName != NULL);

    for (ParamHashTableByName::iterator it =
             retina->parameterHashTableByName->begin();
         (parameterHashElt = it.get()) != NULL;
         it.next()) {
        strlcpy(listParametersInRetinaOutput->parameters[ii].parameterName,
                parameterHashElt->parameter.parameterName,
                sizeof(listParametersInRetinaOutput->parameters[ii]
                           .parameterName));
        strlcpy(listParametersInRetinaOutput->parameters[ii].parameterValue,
                parameterHashElt->parameter.parameterValue,
                sizeof(listParametersInRetinaOutput->parameters[ii]
                           .parameterValue));
        ii++;
    }

    retina->lock.unlock();

    assert(ii == retina->numParameters);

    assert(status == StatusOk);
    output->hdr.status = StatusOk.code();
    listParametersInRetinaOutput->numParameters = retina->numParameters;

    *outputSizeOut = outputSize;
    *outputOut = output;

CommonExit:

    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }

    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
    }

    return status;
}

void
DagLib::ParameterHashEltByName::del()
{
    delete this;
}

void
DagLib::destroyRetina(DgRetina *retina)
{
    if (retina != NULL) {
        if (retina->parameterHashTableByName != NULL) {
            retina->parameterHashTableByName->removeAll(
                &ParameterHashEltByName::del);

            retina->parameterHashTableByName->~ParamHashTableByName();
            memFree(retina->parameterHashTableByName);
        }

        if (retina->columnHints != NULL) {
            memFree(retina->columnHints);
        }

        retina->~DgRetina();
        memFree(retina);
    }
}

Status
DagLib::loadRetinaFromFile(const char *retinaPath,
                           uint8_t **retinaBufOut,
                           size_t *retinaBufSizeOut,
                           struct stat *statBuf)
{
    FILE *fp = NULL;
    void *fileContents = NULL;
    size_t fileSize = 0;
    Status status = StatusUnknown;

    assert(retinaPath != NULL);
    assert(retinaBufOut != NULL);
    assert(retinaBufSizeOut != NULL);

    // @SymbolCheckIgnore
    errno = 0;
    fp = fopen(retinaPath, "r");
    if (fp == NULL) {
        fprintf(stderr, "Could not open %s\n", retinaPath);
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    fileSize = statBuf->st_size;

    fileContents = (void *) memAlloc(fileSize);
    if (fileContents == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    if (fread(fileContents, 1, fileSize, fp) != fileSize) {
        fprintf(stderr, "Error reading %s\n", retinaPath);
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    *retinaBufOut = (uint8_t *) fileContents;
    *retinaBufSizeOut = fileSize;
    fileContents = NULL;
    status = StatusOk;
CommonExit:
    if (fileContents != NULL) {
        assert(status != StatusOk);
        memFree(fileContents);
        fileContents = NULL;
    }

    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    return status;
}

// This function is called by the twoPc infrastructure on the destination
// node via schedLocalWork.
void
DagLib::dlmRetinaTemplateMsg(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    RetinaTemplateMsg *retinaTemplateMsg = (RetinaTemplateMsg *) payload;
    size_t numBytesUsedInPayload = 0;
    RetinaTemplateOp retinaOp = retinaTemplateMsg->operation;

    assert(eph != NULL);
    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    eph->payloadToDistribute = NULL;

    retinaTemplateDlmLock_.lock();

    switch (retinaOp) {
    case AddTemplate:
        status = addRetinaTemplateLocal((RetinaTemplateMsg *) payload);
        break;
    case GetTemplate:
        status = getRetinaTemplateLocal(payload,
                                        (RetinaTemplateReturnedPayload **) &eph
                                            ->payloadToDistribute,
                                        &numBytesUsedInPayload);
        break;
    case UpdateTemplate:
        status = updateRetinaTemplateLocal((RetinaTemplateMsg *) payload);
        break;
    case DeleteTemplate:
        status = deleteRetinaTemplateLocal((RetinaTemplateMsg *) payload);
        break;
    default:
        assert(0);
        status = StatusInval;
    }

    retinaTemplateDlmLock_.unlock();

    if ((retinaOp != GetTemplate) || (status != StatusOk)) {
        assert(numBytesUsedInPayload == 0);
    }

    eph->setAckInfo(status, numBytesUsedInPayload);
}

Status
DagLib::addRetinaTemplateLocal(RetinaTemplateMsg *retinaTemplateMsg)
{
    Status status = StatusOk;
    Config *config = Config::get();
    RetinaId retinaId;
    size_t retinaTemplateSize;
    RetinaIdToTemplateHTEntry *htEntry = NULL;
    bool addedToHT = false;

    retinaId = retinaTemplateMsg->retinaId;
    assert(retinaId % config->getActiveNodes() == config->getMyNodeId());

    htEntry = new (std::nothrow) RetinaIdToTemplateHTEntry;
    BailIfNull(htEntry);

    htEntry->retinaId = retinaId;
    retinaTemplateSize = retinaTemplateMsg->retinaAddInput.retinaTemplateSize;
    htEntry->retinaTemplate =
        (RetinaTemplate *) memAllocExt(retinaTemplateSize, moduleName);
    BailIfNull(htEntry->retinaTemplate);
    memcpy(htEntry->retinaTemplate,
           retinaTemplateMsg->retinaAddInput.retinaTemplate,
           retinaTemplateSize);

    retinaIdToTemplateHTLock_.lock();

    assert(retinaIdToTemplateHT_.find(retinaId) == NULL);

    status = retinaIdToTemplateHT_.insert(htEntry);
    assert(status == StatusOk);
    addedToHT = true;

    retinaIdToTemplateHTLock_.unlock();

CommonExit:
    if (status != StatusOk) {
        if (addedToHT) {
            retinaIdToTemplateHTLock_.lock();
            htEntry = retinaIdToTemplateHT_.remove(retinaId);
            retinaIdToTemplateHTLock_.unlock();
            assert(htEntry != NULL);
            memFree(htEntry->retinaTemplate);
            htEntry->retinaTemplate = NULL;
            delete htEntry;
            htEntry = NULL;
        }
    }

    return status;
}

Status
DagLib::getRetinaTemplateLocal(void *payload,
                               RetinaTemplateReturnedPayload **payloadOut,
                               size_t *payloadOutSize)
{
    Status status = StatusOk;
    RetinaId retinaId;
    RetinaIdToTemplateHTEntry *htEntry;
    RetinaTemplate *retinaTemplate = NULL;
    size_t outputSize = 0;
    RetinaTemplateReturnedPayload *output = NULL;
    bool htLocked = false;

    RetinaTemplateMsg *retinaTemplateMsg = (RetinaTemplateMsg *) payload;

    retinaId = retinaTemplateMsg->retinaId;

    retinaIdToTemplateHTLock_.lock();
    htLocked = true;

    htEntry = retinaIdToTemplateHT_.find(retinaId);
    if (htEntry != NULL) {
        retinaTemplate = htEntry->retinaTemplate;
    }

    if (retinaTemplate == NULL) {
        status = StatusRetinaNotFound;
        goto CommonExit;
    }

    // Cannot access retinaTemplateMsg once we start writing to the
    // output payload.

    outputSize = sizeof(*output) + retinaTemplate->totalSize;
    output = (RetinaTemplateReturnedPayload *) memAlloc(outputSize);
    BailIfNull(output);

    output->retinaId = retinaId;
    output->templateSize = retinaTemplate->totalSize;

    memcpy(&output->retinaTemplate, retinaTemplate, retinaTemplate->totalSize);

CommonExit:
    if (htLocked) {
        retinaIdToTemplateHTLock_.unlock();
        htLocked = false;
    }
    *payloadOut = output;
    *payloadOutSize = (status == StatusOk) ? outputSize : 0;

    return status;
}

Status
DagLib::updateRetinaTemplateLocal(RetinaTemplateMsg *retinaTemplateMsg)
{
    RetinaId retinaId;
    RetinaUpdateInput *retinaUpdateInput;

    assert(retinaTemplateMsg != NULL);

    retinaId = retinaTemplateMsg->retinaId;
    retinaUpdateInput = &retinaTemplateMsg->retinaUpdateInput;

    return updateTemplateInHT(retinaId, retinaUpdateInput);
}

Status
DagLib::deleteRetinaTemplateLocal(RetinaTemplateMsg *retinaTemplateMsg)
{
    Status status = StatusOk;
    RetinaId retinaId;
    RetinaIdToTemplateHTEntry *htEntry;

    assert(retinaTemplateMsg != NULL);

    retinaId = retinaTemplateMsg->retinaId;

    retinaIdToTemplateHTLock_.lock();

    htEntry = retinaIdToTemplateHT_.remove(retinaId);

    retinaIdToTemplateHTLock_.unlock();

    assert(htEntry != NULL);
    if (htEntry == NULL) {
        status = StatusRetinaNotFound;
        goto CommonExit;
    }

    // free the updates stack
    while (htEntry->retinaTemplate->updates != NULL) {
        RetinaUpdate *update = htEntry->retinaTemplate->updates->next;
        memFree(htEntry->retinaTemplate->updates);

        htEntry->retinaTemplate->updates = update;
    }

    memFree(htEntry->retinaTemplate);
    htEntry->retinaTemplate = NULL;

    delete htEntry;
    htEntry = NULL;

CommonExit:

    return status;
}

void
DagLib::dlmRetinaTemplateCompletion(MsgEphemeral *eph, void *payload)
{
    RetinaTemplateMsgResult *retinaTemplateMsgResult =
        (RetinaTemplateMsgResult *) eph->ephemeral;
    RetinaTemplateReturnedPayload *retinaTemplateReturnedPayload = NULL;

    retinaTemplateMsgResult->status = eph->status;
    if (retinaTemplateMsgResult->status != StatusOk) {
        return;
    }

    switch (retinaTemplateMsgResult->operation) {
    case AddTemplate:
    case UpdateTemplate:
    case DeleteTemplate:
        // No output returned
        break;
    case GetTemplate:
        retinaTemplateReturnedPayload =
            (RetinaTemplateReturnedPayload *) payload;

        // Payload is returned only when getting the template
        assert(retinaTemplateMsgResult->retinaTemplate == NULL);
        retinaTemplateMsgResult->retinaTemplate =
            memAlloc(retinaTemplateReturnedPayload->templateSize);
        if (retinaTemplateMsgResult->retinaTemplate == NULL) {
            retinaTemplateMsgResult->status = StatusNoMem;
            return;
        }
        memcpy(retinaTemplateMsgResult->retinaTemplate,
               &retinaTemplateReturnedPayload->retinaTemplate,
               retinaTemplateReturnedPayload->templateSize);
        retinaTemplateMsgResult->outputSize =
            retinaTemplateReturnedPayload->templateSize;
        break;
    default:
        assert(0 && "Developer forgot to add handling");
        break;
    }
}

Status
DagLib::importRetinaFromExecute(XcalarApiExecuteRetinaInput *executeRetinaInput)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    XcalarWorkItem *workItem = NULL;
    Status status;
    LibNs *libNs2 = LibNs::get();
    const char *retinaName = executeRetinaInput->retinaName;
#ifdef DEBUG
    Status status2;
    LibNsTypes::NsHandle nsHandle;
    char retinaPathName[LibNsTypes::MaxPathNameLen];

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // Since this is called only when the retina name isn't found in libNS
    // (during ExecuteRetina whose retina is embedded and needs a new name),
    // the following call to libNs is purely to make the assertion below.
    nsHandle = libNs2->open(retinaPathName, LibNsTypes::ReaderShared, &status);

    assert(status != StatusOk);

    status2 = libNs2->close(nsHandle, NULL);
    if (status2 != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close '%s': %s",
                retinaPathName,
                strGetFromStatus(status2));
    }
#endif

    // importRetinaFromExecute() is called only during ExecuteRetina. libNs
    // only has the user-created, user-visible names so far, published by the
    // retinaRestore() -> importRetinaFromFile() code flow during boot). See
    // large comment for Xc-10832 in OperatorHandlerExecuteRetina.cpp. The
    // following will import the retina temporarily during execute-retina.

    workItem = xcalarApiMakeImportRetinaWorkItem(retinaName,
                                                 false,
                                                 false,
                                                 NULL,
                                                 NULL,
                                                 executeRetinaInput
                                                     ->exportRetinaBufSize,
                                                 (uint8_t *) executeRetinaInput
                                                     ->exportRetinaBuf);
    BailIfNull(workItem);

    status = DagLib::get()->importRetina(&workItem->input->importRetinaInput,
                                         &output,
                                         &outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error while importing Dataflow \"%s\": %s",
                retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    return status;
}

Status
DagLib::getUdfModulesFromRetina(char *retinaName, EvalUdfModuleSet *udfModules)
{
    Status status;
    LibNs *libNs2 = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    bool pathOpened = false;

    DgRetina *retina = NULL;
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    RetinaNsObject *retinaNsObject = NULL;
    RetinaId retinaId;
    XcalarApiUserId dummyUserId;
    memZero(&dummyUserId, sizeof(dummyUserId));

    if (snprintf(retinaPathName,
                 LibNsTypes::MaxPathNameLen,
                 "%s%s",
                 RetinaNsObject::PathPrefix,
                 retinaName) >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // This open protects our access to the Retina.
    nsHandle = libNs2->open(retinaPathName,
                            LibNsTypes::ReaderShared,
                            (NsObject **) &retinaNsObject,
                            &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusPendingRemoval) {
            status = StatusRetinaNotFound;
        } else if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;

    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    status = getRetinaInt(retinaId, NULL, &retina);
    BailIfFailed(status);

    status = searchQueryGraph(retina->dag, udfModules, NULL, NULL, NULL, NULL);
    BailIfFailed(status);

CommonExit:
    if (pathOpened) {
        Status status2 = libNs2->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaPathName,
                    strGetFromStatus(status2));
        }
    }

    if (retina != NULL) {
        putRetinaInt(retina);
        retina = NULL;
    }

    if (retinaNsObject != NULL) {
        memFree(retinaNsObject);
        retinaNsObject = NULL;
    }

    return status;
}

Status
DagLib::transplantRetinaDag(
    const XcalarApiExecuteRetinaInput *executeRetinaInput, Dag *instanceDag)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    DagTypes::NodeId targetNodeId;
    DagTypes::NodeId dagNodeId;
    char prefix[XcalarApiMaxTableNameLen + 1];
    char tmpName[XcalarApiMaxTableNameLen + 1];
    size_t ret;

    assert(executeRetinaInput->exportToActiveSession);

    status = instanceDag->convertLastExportNode();
    BailIfFailed(status);

    dagNodeId = instanceDag->hdr_.firstNode;
    targetNodeId = instanceDag->hdr_.lastNode;

    strlcpy(prefix, executeRetinaInput->dstTable.tableName, sizeof(prefix));

    // get rid of any hashtags for the UI
    char *ptr;
    ptr = strchr(prefix, '#');
    if (ptr != NULL) {
        *ptr = '\0';
    }
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = instanceDag->lookupNodeById(dagNodeId, &dagNode);
        assert(status == StatusOk);

        if (dagNodeId == targetNodeId) {
            status = instanceDag->renameDagNodeLocal(dagNode->dagNodeHdr
                                                         .apiDagNodeHdr.name,
                                                     executeRetinaInput
                                                         ->dstTable.tableName);
            BailIfFailed(status);
        } else if (dagNode->dagNodeHdr.apiDagNodeHdr.api !=
                   XcalarApiAggregate) {
            // rename everything except aggregates
            const char *name;
            const char *datasetName = dagNode->dagNodeHdr.apiDagNodeHdr.name;

            if (strncmp(datasetName,
                        XcalarApiDatasetPrefix,
                        strlen(XcalarApiDatasetPrefix)) == 0) {
                // put the dataset prefix before the retina prefix
                name = datasetName + XcalarApiDatasetPrefixLen;
                ret = snprintf(tmpName,
                               sizeof(tmpName),
                               "%s%s:%s",
                               XcalarApiDatasetPrefix,
                               prefix,
                               name);
            } else {
                ret = snprintf(tmpName,
                               sizeof(tmpName),
                               "%s:%s",
                               prefix,
                               dagNode->dagNodeHdr.apiDagNodeHdr.name);
            }
            if (ret >= sizeof(tmpName)) {
                status = StatusNameTooLong;
                xSyslog(moduleName,
                        XlogErr,
                        "Name %s:%s too long, max is %lu chars",
                        prefix,
                        dagNode->dagNodeHdr.apiDagNodeHdr.name,
                        sizeof(tmpName));
                goto CommonExit;
            }

            status = instanceDag->renameDagNodeLocal(dagNode->dagNodeHdr
                                                         .apiDagNodeHdr.name,
                                                     tmpName);
            BailIfFailed(status);
        }

        dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;
    }

CommonExit:
    return status;
}

Status
DagLib::copyNodesFromRetinaDag(Dag *retinaDag, Dag *dstDag)
{
    Status status;
    DagTypes::NodeId dagNodeId;
    DagTypes::NodeId targetNodeId;
    DagNodeTypes::Node *dagNode = NULL;
    Dag::ClonedNodeElt **clonedNodeHashBase = NULL;
    uint64_t numNodesCreated = 0;

    clonedNodeHashBase = retinaDag->allocateCloneNodeHashBase();
    if (clonedNodeHashBase == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    targetNodeId = retinaDag->hdr_.lastNode;

    status = retinaDag->lookupNodeById(targetNodeId, &dagNode);
    assert(status == StatusOk);

    status = retinaDag->copyDagToNewDag(clonedNodeHashBase,
                                        &dagNode,
                                        1,
                                        dstDag,
                                        0,
                                        NULL,
                                        (Dag::CloneFlags)(Dag::CopyAnnotations |
                                                          Dag::ResetState),
                                        &numNodesCreated);
    BailIfFailed(status);

    // search for any synthesize nodes and remove them
    dagNodeId = dstDag->hdr_.firstNode;
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = dstDag->lookupNodeById(dagNodeId, &dagNode);
        assert(status == StatusOk);

        dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;

        if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiSynthesize) {
            continue;
        }

        XcalarApiSynthesizeInput *in =
            &dagNode->dagNodeHdr.apiInput->synthesizeInput;
        if (in->sameSession) {
            continue;
        }

        assert(dagNode->numParent == 0);

        DagNodeTypes::NodeIdListElt *childElt = NULL;
        DagNodeTypes::Node *parentNode = NULL;
        DagTypes::NodeId curDagNodeId =
            dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;

        status = dstDag->lookupNodeByName(in->source.name,
                                          &parentNode,
                                          Dag::TableScope::LocalOnly,
                                          true);
        BailIfFailed(status);

        char *parentName = parentNode->dagNodeHdr.apiDagNodeHdr.name;
        DagTypes::NodeId parentNodeId =
            parentNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;

        // fix parent child relations and childs' apiInputs
        childElt = dagNode->childrenList;
        while (childElt != NULL) {
            DagNodeTypes::NodeIdListElt *elt = NULL;
            DagNodeTypes::Node *childNode = NULL;

            status = dstDag->lookupNodeById(childElt->nodeId, &childNode);
            assert(status == StatusOk);

            // add new child to parent
            status = dstDag->addChildNode(parentNode, childNode);
            BailIfFailed(status);

            // fixup child apiInputs
            DagLib::renameSrcApiInput(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                      parentName,
                                      childNode);

            // replace myself with my parent in child
            elt = childNode->parentsList;
            do {
                if (elt->nodeId == curDagNodeId) {
                    elt->nodeId = parentNodeId;
                    break;
                }
                elt = elt->next;
            } while (elt != NULL);

            childElt = childElt->next;
        }

        // clean up my child list
        childElt = dagNode->childrenList;
        while (childElt != NULL) {
            DagNodeTypes::NodeIdListElt *tmp = childElt->next;

            delete childElt;
            childElt = tmp;
        }
        dagNode->childrenList = NULL;
        dagNode->numChild = 0;

        // delete myself
        status = dstDag->deleteDagNodeById(dagNode->dagNodeHdr.apiDagNodeHdr
                                               .dagNodeId,
                                           false);
        BailIfFailed(status);
    }

CommonExit:
    if (clonedNodeHashBase != NULL) {
        retinaDag->deleteClonedNodeHashTable(clonedNodeHashBase);
    }

    return status;
}

Status
DagLib::copyRetinaToNewDag(
    const XcalarApiExecuteRetinaInput *executeRetinaInput, Dag *dstDag)
{
    Status status;
    RetinaInfo *retinaInfo = NULL;
    uint64_t numNodes;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize = 0;
    uint64_t ii;

    status = parseRetinaFile(executeRetinaInput->exportRetinaBuf,
                             executeRetinaInput->exportRetinaBufSize,
                             executeRetinaInput->retinaName,
                             &retinaInfo);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to parse batch dataflow file for batch "
                    "dataflow %s: %s",
                    executeRetinaInput->retinaName,
                    strGetFromStatus(status));

    // Add this exec-retina node's UDFs to dstDag's container which in this case
    // must be a retina container
    for (ii = 0; ii < retinaInfo->numUdfModules; ii++) {
        UdfModuleSrc *newModule = retinaInfo->udfModulesArray[ii];
        assert(!newModule->isBuiltin);
        // skip default module
        if (strncmp(newModule->moduleName,
                    UserDefinedFunction::DefaultModuleName,
                    strlen(UserDefinedFunction::DefaultModuleName)) != 0) {
            XcalarApiUdfContainer *dstUdfContainer = dstDag->getUdfContainer();

            // assert that dst UDF container is that for a global retina
            assert(strlen(dstUdfContainer->retinaName) != 0);
            assert(dstUdfContainer->sessionInfo.sessionNameLength == 0);
            if (addUdfOutput != NULL) {
                memFree(addUdfOutput);
                addUdfOutput = NULL;
            }
            status =
                UserDefinedFunction::get()->addUdfIgnoreDup(newModule,
                                                            dstUdfContainer,
                                                            &addUdfOutput,
                                                            &addUdfOutputSize);
            BailIfFailed(status);  // See comment below about failing here

            // XXX: The collapsing of the embedded execute-retina's UDF
            // container into the destination DAG's container could result in
            // UDF name collision. Bail out for now. This can be resolved by
            // renaming the UDF modules coming from the embedded execute-retina
            // node - both the modules themselves and the references in the
            // retina's qgraph.  However, this is not being done currently since
            // this is considered overly complex and unlikely to occur in
            // practice - if it *does* occur frequently and for valid reasons,
            // the renaming logic can be added. Until this support is added, if
            // the user does run into this (it'd be only during a "make retina"
            // operation), the user would need to do the UDF module renaming
            // themselves - by downloading the workbook (from which the 'make
            // retina' is being attempted), and examining all UDF modules in all
            // containers: the workbook's container, and each embedded
            // execute-retina node's container (by expanding the execute-retina
            // node's retina using base64decode, and then untar'ing the
            // resultant .tar.gz). For each UDF modulename which conflicts, the
            // module should be renamed, and references to the name fixed to use
            // the new name. One simple strategy may be to just prefix each
            // conflicting UDF's name in an embedded execute-retina node, with
            // the name of the retina, and fix all references in the retina's
            // qgraph to that module name.
        }
    }

    status = QueryParser::get()->parse(retinaInfo->queryStr,
                                       dstDag->getUdfContainer(),
                                       &retinaInfo->queryGraph,
                                       &numNodes);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to parse queryStr for retina %s: %s",
                    executeRetinaInput->retinaName,
                    strGetFromStatus(status));

    status = variableSubst(retinaInfo->queryGraph,
                           executeRetinaInput->numParameters,
                           executeRetinaInput->parameters);
    BailIfFailed(status);

    // Rename to avoid name collisions and remove export nodes
    status = transplantRetinaDag(executeRetinaInput, retinaInfo->queryGraph);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to transplant batch dataflow dag for batch "
                    "dataflow %s: %s",
                    executeRetinaInput->retinaName,
                    strGetFromStatus(status));

    // Finally copy the nodes from the edited instance dag to the new dag
    status = copyNodesFromRetinaDag(retinaInfo->queryGraph, dstDag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to copy batch dataflow dag for batch "
                    "dataflow %s: %s",
                    executeRetinaInput->retinaName,
                    strGetFromStatus(status));

CommonExit:
    if (retinaInfo) {
        destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }
    if (addUdfOutput != NULL) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }
    return status;
}

Status
DagLib::allocMakeRetinaParam(XcalarApiMakeRetinaInput *makeRetinaInput,
                             size_t inputSize,
                             unsigned numRetinas,
                             RetinaInfo **retinaInfos,
                             MakeRetinaParam **makeRetinaParamOut,
                             size_t *makeRetinaParamSizeOut)
{
    Status status;
    MakeRetinaParam *makeRetinaParam = NULL;
    size_t makeRetinaParamSize;
    size_t makeRetinaParamHdrSize;
    size_t retinaInputSize = inputSize;

    uint64_t numTargetTables = makeRetinaInput->numTargetTables;
    RetinaDst **targetTables = makeRetinaInput->tableArray;

    uint64_t numColumnHints = makeRetinaInput->numColumnHints;
    Column *columnHints = NULL;

    bool needFree = false;

    if (numRetinas > 0) {
        // combine the info from the current makeRetinaInput with the info from
        // all the retinaInfos that are encapsulated in this retina

        // update counts
        for (unsigned ii = 0; ii < numRetinas; ii++) {
            numColumnHints += retinaInfos[ii]->numColumnHints;
        }

        needFree = true;

        columnHints =
            (Column *) memAlloc(numColumnHints * sizeof(*columnHints));
        BailIfNull(columnHints);

        // merge columnHints arrays and update retinaInputSize
        memcpy(columnHints,
               makeRetinaInput->columnHints,
               makeRetinaInput->numColumnHints * sizeof(*columnHints));

        unsigned columnHintIdx = makeRetinaInput->numColumnHints;

        for (unsigned ii = 0; ii < numRetinas; ii++) {
            memcpy(&columnHints[columnHintIdx],
                   retinaInfos[ii]->columnHints,
                   retinaInfos[ii]->numColumnHints * sizeof(*columnHints));
            columnHintIdx += retinaInfos[ii]->numColumnHints;
            retinaInputSize +=
                retinaInfos[ii]->numColumnHints * sizeof(*columnHints);
        }
    } else {
        columnHints = makeRetinaInput->columnHints;
    }

    makeRetinaParamHdrSize = MakeRetinaParamHdrSize(makeRetinaParam);
    makeRetinaParamSize = makeRetinaParamHdrSize + retinaInputSize;
    makeRetinaParam =
        (MakeRetinaParam *) memAllocExt(makeRetinaParamSize, moduleName);
    if (makeRetinaParam == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiSerializeRetinaInput(&makeRetinaParam->retinaInput,
                                           retinaInputSize,
                                           makeRetinaInput->retinaName,
                                           numTargetTables,
                                           targetTables,
                                           makeRetinaInput->numSrcTables,
                                           makeRetinaInput->srcTables,
                                           numColumnHints,
                                           columnHints);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to serialize batch dataflow '%s' input: %s",
                makeRetinaInput->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (needFree) {
        memFree(columnHints);
        columnHints = NULL;
    }

    if (status != StatusOk) {
        if (makeRetinaParam) {
            memFree(makeRetinaParam);
            makeRetinaParam = NULL;
        }
    } else {
        *makeRetinaParamOut = makeRetinaParam;
        *makeRetinaParamSizeOut = makeRetinaParamSize;
    }

    return status;
}
