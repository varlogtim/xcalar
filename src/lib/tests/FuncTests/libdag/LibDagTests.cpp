// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
#include <stdio.h>
#include <cstdlib>
#include <getopt.h>
#include <unistd.h>
#include <libgen.h>

#include "StrlFunc.h"
#include "dag/DagLib.h"
#include "dag/DagNodeTypes.h"
#include "config/Config.h"
#include "xdb/Xdb.h"
#include "dag/DagTypes.h"
#include "optimizer/Optimizer.h"
#include "libapis/LibApisCommon.h"
#include "msg/Message.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "util/ProtoWrap.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "runtime/Runtime.h"
#include "RandDag.h"
#include "log/Log.h"
#include "LibDagFuncTestConfig.h"
#include "util/System.h"
#include "subsys/DurableDag.pb.h"
#include "durable/Durable.h"
#include "DurableVersions.h"

#include "test/QA.h"

static const char *moduleName = "libDagTest";

static size_t
pbUnionSizeXcalarApiInput(const int unionType)
{
    switch (unionType) {
    case XcalarApiBulkLoad:
        return (sizeof(((XcalarApiInput *) 0x0)->loadInput));
    case XcalarApiIndex:
        return (sizeof(((XcalarApiInput *) 0x0)->indexInput));
    case XcalarApiGetTableMeta:
        return (sizeof(((XcalarApiInput *) 0x0)->getTableMetaInput));
    case XcalarApiResultSetNext:
        return (sizeof(((XcalarApiInput *) 0x0)->resultSetNextInput));
    case XcalarApiJoin:
        return (sizeof(((XcalarApiInput *) 0x0)->joinInput));
    case XcalarApiUnion:
        return (sizeof(((XcalarApiInput *) 0x0)->unionInput));
    case XcalarApiProject:
        return (sizeof(((XcalarApiInput *) 0x0)->projectInput));
    case XcalarApiFilter:
        return (sizeof(((XcalarApiInput *) 0x0)->filterInput));
    case XcalarApiGroupBy:
        return (sizeof(((XcalarApiInput *) 0x0)->groupByInput));
    case XcalarApiResultSetAbsolute:
        return (sizeof(((XcalarApiInput *) 0x0)->resultSetAbsoluteInput));
    case XcalarApiFreeResultSet:
        return (sizeof(((XcalarApiInput *) 0x0)->freeResultSetInput));
    case XcalarApiArchiveTables:
        return (sizeof(((XcalarApiInput *) 0x0)->archiveTablesInput));
    case XcalarApiMakeResultSet:
        return (sizeof(((XcalarApiInput *) 0x0)->makeResultSetInput));
    case XcalarApiMap:
        return (sizeof(((XcalarApiInput *) 0x0)->mapInput));
    case XcalarApiGetRowNum:
        return (sizeof(((XcalarApiInput *) 0x0)->getRowNumInput));
    case XcalarApiSynthesize:
        return (sizeof(((XcalarApiInput *) 0x0)->synthesizeInput));
    case XcalarApiAggregate:
        return (sizeof(((XcalarApiInput *) 0x0)->aggregateInput));
    case XcalarApiListExportTargets:
        return (sizeof(((XcalarApiInput *) 0x0)->listTargetsInput));
    case XcalarApiExport:
        return (sizeof(((XcalarApiInput *) 0x0)->exportInput));
    case XcalarApiListFiles:
        return (sizeof(((XcalarApiInput *) 0x0)->listFilesInput));
    case XcalarApiMakeRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->makeRetinaInput));
    case XcalarApiGetRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->getRetinaInput));
    case XcalarApiGetRetinaJson:
        return (sizeof(((XcalarApiInput *) 0x0)->getRetinaJsonInput));
    case XcalarApiExecuteRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->executeRetinaInput));
    case XcalarApiUpdateRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->updateRetinaInput));
    case XcalarApiListParametersInRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->listParametersInRetinaInput));
    case XcalarApiKeyAddOrReplace:
        return (sizeof(((XcalarApiInput *) 0x0)->keyAddOrReplaceInput));
    case XcalarApiKeyAppend:
        return (sizeof(((XcalarApiInput *) 0x0)->keyAppendInput));
    case XcalarApiKeySetIfEqual:
        return (sizeof(((XcalarApiInput *) 0x0)->keySetIfEqualInput));
    case XcalarApiKeyDelete:
        return (sizeof(((XcalarApiInput *) 0x0)->keyDeleteInput));
    case XcalarApiTop:
        return (sizeof(((XcalarApiInput *) 0x0)->topInput));
    case XcalarApiSessionNew:
        return (sizeof(((XcalarApiInput *) 0x0)->sessionNewInput));
    case XcalarApiSessionDelete:
        return (sizeof(((XcalarApiInput *) 0x0)->sessionDeleteInput));
    case XcalarApiSessionRename:
        return (sizeof(((XcalarApiInput *) 0x0)->sessionRenameInput));
    case XcalarApiShutdown:
        return (sizeof(((XcalarApiInput *) 0x0)->shutdownInput));
    case XcalarApiListXdfs:
        return (sizeof(((XcalarApiInput *) 0x0)->listXdfsInput));
    case XcalarApiRenameNode:
        return (sizeof(((XcalarApiInput *) 0x0)->renameNodeInput));
    case XcalarApiCreateDht:
        return (sizeof(((XcalarApiInput *) 0x0)->createDhtInput));
    case XcalarApiDeleteDht:
        return (sizeof(((XcalarApiInput *) 0x0)->deleteDhtInput));
    case XcalarApiDeleteRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->deleteRetinaInput));
    case XcalarApiUdfAdd:
        return (sizeof(((XcalarApiInput *) 0x0)->udfAddUpdateInput));
    case XcalarApiUdfGet:
        return (sizeof(((XcalarApiInput *) 0x0)->udfGetInput));
    case XcalarApiUdfDelete:
        return (sizeof(((XcalarApiInput *) 0x0)->udfDeleteInput));
    case XcalarApiImportRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->importRetinaInput));
    case XcalarApiPreview:
        return (sizeof(((XcalarApiInput *) 0x0)->previewInput));
    case XcalarApiExportRetina:
        return (sizeof(((XcalarApiInput *) 0x0)->exportRetinaInput));
    case XcalarApiGetQuery:
        return (sizeof(((XcalarApiInput *) 0x0)->getQueryInput));
    case XcalarApiSetConfigParam:
        return (sizeof(((XcalarApiInput *) 0x0)->setConfigParamInput));
    case XcalarApiPacked:
        return (sizeof(((XcalarApiInput *) 0x0)->appSetInput));
    case XcalarApiAppRun:
        return (sizeof(((XcalarApiInput *) 0x0)->appRunInput));
    case XcalarApiAppReap:
        return (sizeof(((XcalarApiInput *) 0x0)->appReapInput));
    case XcalarApiGetIpAddr:
        return (sizeof(((XcalarApiInput *) 0x0)->getIpAddrInput));
    case XcalarApiSupportGenerate:
        return (sizeof(((XcalarApiInput *) 0x0)->supportGenerateInput));
    case XcalarApiTagDagNodes:
        return (sizeof(((XcalarApiInput *) 0x0)->tagDagNodesInput));
    case XcalarApiCommentDagNodes:
        return (sizeof(((XcalarApiInput *) 0x0)->commentDagNodesInput));
    case XcalarApiListDatasetUsers:
        return (sizeof(((XcalarApiInput *) 0x0)->listDatasetUsersInput));
    case XcalarApiKeyList:
        return (sizeof(((XcalarApiInput *) 0x0)->keyListInput));
    case XcalarApiListUserDatasets:
        return (sizeof(((XcalarApiInput *) 0x0)->listUserDatasetsInput));
    case XcalarApiTarget:
        return (sizeof(((XcalarApiInput *) 0x0)->targetInput));
    case XcalarApiGetDatasetsInfo:
        return (sizeof(((XcalarApiInput *) 0x0)->getDatasetsInfoInput));
    case XcalarApiSessionDownload:
        return (sizeof(((XcalarApiInput *) 0x0)->sessionDownloadInput));
    case XcalarApiSessionUpload:
        return (sizeof(((XcalarApiInput *) 0x0)->sessionUploadInput));
    case XcalarApiPublish:
        return (sizeof(((XcalarApiInput *) 0x0)->publishInput));
    case XcalarApiUpdate:
        return (sizeof(((XcalarApiInput *) 0x0)->updateInput));
    case XcalarApiSelect:
        return (sizeof(((XcalarApiInput *) 0x0)->selectInput));
    case XcalarApiUnpublish:
        return (sizeof(((XcalarApiInput *) 0x0)->unpublishInput));
    case XcalarApiCoalesce:
        return (sizeof(((XcalarApiInput *) 0x0)->coalesceInput));
    case XcalarApiRestoreTable:
        return (sizeof(((XcalarApiInput *) 0x0)->restoreTableInput));
    case XcalarApiSessionActivate:
        return (sizeof(((XcalarApiInput *) 0x0)->sessionActivateInput));
    case XcalarApiRuntimeSetParam:
        return (sizeof(((XcalarApiInput *) 0x0)->runtimeSetParamInput));
    case XcalarApiDatasetCreate:
        return (sizeof(((XcalarApiInput *) 0x0)->datasetCreateInput));
    case XcalarApiDatasetDelete:
        return (sizeof(((XcalarApiInput *) 0x0)->datasetDeleteInput));
    case XcalarApiDatasetUnload:
        return (sizeof(((XcalarApiInput *) 0x0)->datasetUnloadInput));
    case XcalarApiDatasetGetMeta:
        return (sizeof(((XcalarApiInput *) 0x0)->datasetGetMetaInput));
    case XcalarApiListRetinas:
        return (sizeof(((XcalarApiInput *) 0x0)->listRetinasInput));
    default:
        throw std::invalid_argument("Unsupported enum");
    }
}

static XcalarApiUserId testUserId = {
    "LibDagTests",
    0xdeadbeef,
};

static uint64_t numLoopRandomTest = 2;    // Number to repeat this test
static uint64_t numThreadRandomTest = 2;  // Each thread test one queryGraph
static uint64_t numNodesPerMatrix = 10;   // Number of node per matrix
// Each thread will create a new matrix
static uint64_t numThreadCreateNewMatrix = 2;
// Number of thread that works on one matrix
static uint64_t numThreadScaningMatrix = 2;
// after every deletionFrequency node created, one node will be deleted
static uint64_t deletionFrequency = 4;
static uint64_t maxRetryForRandomNode = 5;

enum DagTestCase {
    DagTestRefCount = 0,
    DagTestNodeState,
    DagTestSetScalar,
    DagTestGetGarbage,  // Those API will return garbage without a real
                        // operation
    DagTestError,       // Those API will return error status

    // must be the last one
    DagTestCaseLength,
};

struct DagNodeInfo {
    const char *name;
    const char *dagNodeName;
    uint64_t numParent;
    const char *parentName[DagTypes::MaxNameLen + 1];
    XcalarApis api;
};

static XcalarApis apiArray[] = {XcalarApiIndex,
                                XcalarApiMap,
                                XcalarApiFilter,
                                XcalarApiGroupBy,
                                XcalarApiJoin,
                                XcalarApiProject,
                                XcalarApiGetRowNum,
                                XcalarApiSynthesize,
                                XcalarApiBulkLoad,
                                XcalarApiAggregate,
                                XcalarApiExport};

static const char *dummyInputStr = "dummyApiInput";
static int dagNumArray0;
static int dagNumArray1;

static DagNodeInfo dagArray0[10] = {
    [0] = {"DS1-loadNode0",
           XcalarApiDatasetPrefix "DS1-loadNode0",
           0,
           {NULL},
           XcalarApiBulkLoad},
    [1] = {"DS1-loadNode1",
           XcalarApiDatasetPrefix "DS1-loadNode1",
           0,
           {NULL},
           XcalarApiBulkLoad},
    [2] = {"DS1-indexNode0",
           "DS1-indexNode0",
           1,
           {XcalarApiDatasetPrefix "DS1-loadNode0"},
           XcalarApiIndex},
    [3] = {"DS1-indexNode1",
           "DS1-indexNode1",
           1,
           {XcalarApiDatasetPrefix "DS1-loadNode1"},
           XcalarApiIndex},
    [4] = {"DS1-filterNode0",
           "DS1-filterNode0",
           1,
           {"DS1-indexNode0"},
           XcalarApiFilter},
    [5] = {"DS1-filterNode1",
           "DS1-filterNode1",
           1,
           {"DS1-indexNode1"},
           XcalarApiFilter},
    [6] = {"DS1-joinNode",
           "DS1-joinNode",
           2,
           {"DS1-filterNode0", "DS1-filterNode1"},
           XcalarApiJoin},
    [7] = {"DS1-indexNode2",
           "DS1-indexNode2",
           1,
           {XcalarApiDatasetPrefix "DS1-loadNode1"},
           XcalarApiIndex},
    [8] = {"DS1-indexNode3",
           "DS1-indexNode3",
           1,
           {XcalarApiDatasetPrefix "DS1-loadNode1"},
           XcalarApiIndex},
    [9] = {"DS1-loadNode2",
           XcalarApiDatasetPrefix "DS1-loadNode2",
           0,
           {NULL},
           XcalarApiBulkLoad},
};

static DagNodeInfo dagArray1[5] = {
    [0] = {"DS2-loadNode1",
           XcalarApiDatasetPrefix "DS2-loadNode1",
           0,
           {NULL},
           XcalarApiBulkLoad},
    [1] = {"DS2-loadNode2",
           XcalarApiDatasetPrefix "DS2-loadNode2",
           0,
           {NULL},
           XcalarApiBulkLoad},
    [2] = {"DS2-indexNode1",
           "DS2-indexNode1",
           1,
           {XcalarApiDatasetPrefix "DS2-loadNode1"},
           XcalarApiIndex},
    [3] = {"DS2-indexNode2",
           "DS2-indexNode2",
           1,
           {XcalarApiDatasetPrefix "DS2-loadNode2"},
           XcalarApiIndex},
    [4] = {"DS2-joinNode",
           "DS2-joinNode",
           2,
           {"DS2-indexNode1", "DS2-indexNode2"},
           XcalarApiJoin},
};

static Status
setUp(Dag **dag)
{
    XcalarApiInput *dummyApiInput = new XcalarApiInput();
    memset(dummyApiInput, 0, sizeof(*dummyApiInput));
    DagTypes::NodeId *parentNodeId = NULL;
    size_t ret = strlcpy(dummyApiInput->dagTableNameInput.tableInput,
                         dummyInputStr,
                         sizeof(*dummyApiInput));
    assert(ret < sizeof(*dummyApiInput));

    DagTypes::NodeId dagNodeId = DagTypes::InvalidDagNodeId;
    Status status;
    DagLib *dagLib = DagLib::get();

    // preload dag operation
    status = dagLib->createNewDag(128, DagTypes::WorkspaceGraph, NULL, dag);
    assert(status == StatusOk);
    printf("create first dag handleID:%lu\n", (*dag)->getId());

    dagNumArray0 = (int) (sizeof(dagArray0) / sizeof(dagArray0[0]));
    dagNumArray1 = (int) (sizeof(dagArray1) / sizeof(dagArray1[0]));

    for (int ii = 0; ii < dagNumArray0; ++ii) {
        assert(parentNodeId == NULL);
        size_t parentNodeIdSize =
            sizeof(*parentNodeId) * dagArray0[ii].numParent;
        Dag *parentGraphs[dagArray0[ii].numParent];
        parentNodeId = (DagTypes::NodeId *) memAlloc(parentNodeIdSize);
        assert(parentNodeId != NULL);
        for (unsigned jj = 0; jj < dagArray0[ii].numParent; ++jj) {
            status = (*dag)->getDagNodeId(dagArray0[ii].parentName[jj],
                                          Dag::TableScope::LocalOnly,
                                          &parentNodeId[jj]);
            assert(status == StatusOk);
            parentGraphs[jj] = (*dag);
        }

        size_t inputSize = pbUnionSizeXcalarApiInput(dagArray0[ii].api);
        dagNodeId = DagTypes::InvalidDagNodeId;
        status = (*dag)->createNewDagNode(dagArray0[ii].api,
                                          dummyApiInput,
                                          inputSize,
                                          XdbIdInvalid,
                                          TableNsMgr::InvalidTableId,
                                          (char *) dagArray0[ii].dagNodeName,
                                          dagArray0[ii].numParent,
                                          parentGraphs,
                                          parentNodeId,
                                          &dagNodeId);
        assert(status == StatusOk);
        memFree(parentNodeId);
        parentNodeId = NULL;
    }

    for (int ii = 0; ii < dagNumArray1; ++ii) {
        assert(parentNodeId == NULL);
        size_t parentNodeIdSize =
            sizeof(*parentNodeId) * dagArray1[ii].numParent;
        parentNodeId = (DagTypes::NodeId *) memAlloc(parentNodeIdSize);
        assert(parentNodeId != NULL);
        Dag *parentGraphs[dagArray1[ii].numParent];

        for (unsigned jj = 0; jj < dagArray1[ii].numParent; ++jj) {
            status = (*dag)->getDagNodeId(dagArray1[ii].parentName[jj],
                                          Dag::TableScope::LocalOnly,
                                          &parentNodeId[jj]);
            assert(status == StatusOk);
            parentGraphs[jj] = (*dag);
        }
        size_t inputSize = pbUnionSizeXcalarApiInput(dagArray0[ii].api);
        dagNodeId = DagTypes::InvalidDagNodeId;
        status = (*dag)->createNewDagNode(dagArray1[ii].api,
                                          dummyApiInput,
                                          inputSize,
                                          XdbIdInvalid,
                                          TableNsMgr::InvalidTableId,
                                          (char *) dagArray1[ii].dagNodeName,
                                          dagArray1[ii].numParent,
                                          parentGraphs,
                                          parentNodeId,
                                          &dagNodeId);
        assert(status == StatusOk);
        memFree(parentNodeId);
        parentNodeId = NULL;
    }

    delete dummyApiInput;
    dummyApiInput = NULL;
    return StatusOk;
}

static Status
tearDown(Dag *dag)
{
    DagLib *dagLib = DagLib::get();

    Status status = dagLib->destroyDag(dag, DagTypes::DestroyDeleteNodes);
    assert(status == StatusOk);
    return StatusOk;
}

struct ListNodeName {
    const char *name;
    bool found;
};

static Status
listNodeTestForDag(Dag *dag, DagNodeInfo *dagArray, uint64_t totalDagNum)
{
    XcalarApiDagOutput *listDagsOut = NULL;
    size_t outputSizeOut;
    Status status;
    ListNodeName *listNodeNameArray = NULL;

    printf("totalDagNum = %lu", totalDagNum);

    listNodeNameArray =
        (ListNodeName *) memAllocExt(sizeof(ListNodeName) * totalDagNum,
                                     moduleName);
    assert(listNodeNameArray != NULL);
    for (uint64_t ii = 0; ii < totalDagNum; ++ii) {
        listNodeNameArray[ii].name = dagArray[ii].dagNodeName;
        listNodeNameArray[ii].found = false;
    }

    XcalarApis api = XcalarApiUnknown;
    printf("list handle ID:%lu \n", dag->getId());
    status =
        dag->listAvailableNodes("*", &listDagsOut, &outputSizeOut, 1, &api);

    assert(status == StatusOk);

    for (uint64_t ii = 0; ii < listDagsOut->numNodes; ++ii) {
        for (uint64_t jj = 0; jj < totalDagNum; ++jj) {
            if (strcmp(listDagsOut->node[ii]->hdr.name,
                       listNodeNameArray[jj].name) == 0) {
                listNodeNameArray[jj].found = true;
                printf("found dag node:%s\n", listDagsOut->node[ii]->hdr.name);
            }
        }
    }

    for (uint64_t ii = 0; ii < totalDagNum; ++ii) {
        assert(listNodeNameArray[ii].found);
    }

    memFree(listDagsOut);
    memFree(listNodeNameArray);

    return status;
}

static Status
listAllNodes(Dag *dag)
{
    Status status;
    status = listNodeTestForDag(dag, dagArray0, dagNumArray0);
    assert(status == StatusOk);

    status = listNodeTestForDag(dag, dagArray1, dagNumArray1);
    assert(status == StatusOk);

    return StatusOk;
}

static Status
changeStateTest(Dag *dag)
{
    Status status;
    XcalarApiDagOutput *listDagsOut = NULL;
    size_t outputSizeOut;

    XcalarApis api = XcalarApiUnknown;
    status =
        dag->listAvailableNodes("*", &listDagsOut, &outputSizeOut, 1, &api);

    assert(status == StatusOk);

    for (uint64_t ii = 0; ii < listDagsOut->numNodes; ++ii) {
        status = dag->changeDagNodeState(listDagsOut->node[ii]->hdr.dagNodeId,
                                         DgDagStateCreated);
        assert(status == StatusOk);
    }

    memFree(listDagsOut);
    listDagsOut = NULL;

    status =
        dag->listAvailableNodes("*", &listDagsOut, &outputSizeOut, 1, &api);

    assert(status == StatusOk);

    for (uint64_t ii = 0; ii < listDagsOut->numNodes; ++ii) {
        assert(listDagsOut->node[ii]->hdr.state == DgDagStateCreated);

        status = dag->changeDagNodeState(listDagsOut->node[ii]->hdr.dagNodeId,
                                         DgDagStateReady);
        assert(status == StatusOk);
    }

    memFree(listDagsOut);
    listDagsOut = NULL;

    status =
        dag->listAvailableNodes("*", &listDagsOut, &outputSizeOut, 1, &api);

    assert(status == StatusOk);

    for (uint64_t ii = 0; ii < listDagsOut->numNodes; ++ii) {
        assert(listDagsOut->node[ii]->hdr.state == DgDagStateReady);
    }

    memFree(listDagsOut);

    return status;
}

static Status
getDagTest(Dag *dag)
{
    Status status;
    int dagArrayJoinOrder0[] = {6, 5, 4, 3, 2, 1, 0};
    int dagArrayJoinOrder1[] = {4, 3, 2, 1, 0};
    XcalarApiDagOutput *dagOutput;
    XcalarApiOutput *output;
    size_t outputSize;

    status = dag->getDagByName((char *) dagArray0[6].dagNodeName,
                               Dag::TableScope::LocalOnly,
                               &output,
                               &outputSize);

    dagOutput = &output->outputResult.dagOutput;

    for (uint64_t ii = 0; ii < dagOutput->numNodes; ++ii) {
        uint64_t alternativeIdx = (ii + (ii % 2) * 2 - 1);
        bool found = false;

        if (strcmp(dagOutput->node[ii]->hdr.name,
                   dagArray0[dagArrayJoinOrder0[ii]].dagNodeName) == 0) {
            found = true;
        }

        if (!found &&
            strcmp(dagOutput->node[ii]->hdr.name,
                   dagArray0[dagArrayJoinOrder0[alternativeIdx]].dagNodeName) ==
                0) {
            found = true;
        }

        assert(found);
        assert(strcmp(dagOutput->node[ii]->input->dagTableNameInput.tableInput,
                      dagArray0[0].dagNodeName) == 0 ||
               strcmp(dagOutput->node[ii]->input->dagTableNameInput.tableInput,
                      dummyInputStr) == 0);
    }

    memFree(output);
    output = NULL;
    dagOutput = NULL;

    status = dag->getDagByName((char *) dagArray1[4].dagNodeName,
                               Dag::TableScope::LocalOnly,
                               &output,
                               &outputSize);

    dagOutput = &output->outputResult.dagOutput;
    for (uint64_t ii = 0; ii < dagOutput->numNodes; ++ii) {
        uint64_t alternativeIdx = (ii + (ii % 2) * 2 - 1);
        assert(
            strcmp(dagOutput->node[ii]->hdr.name,
                   dagArray1[dagArrayJoinOrder1[ii]].dagNodeName) == 0 ||
            strcmp(dagOutput->node[ii]->hdr.name,
                   dagArray1[dagArrayJoinOrder1[alternativeIdx]].dagNodeName) ==
                0);
    }
    memFree(output);
    output = NULL;

    return status;
}

static Status
getChildrenTest(Dag *dag)
{
    Status status;
    XcalarApiDagOutput *output = NULL;
    size_t outputSize;
    ListNodeName childArray[3] = {{"DS1-indexNode1", false},
                                  {"DS1-indexNode2", false},
                                  {"DS1-indexNode3", false}};
    int childArrayNum = (int) (sizeof(childArray) / sizeof(childArray[0]));

    status = dag->getChildDagNode((char *) dagArray0[6].dagNodeName,
                                  &output,
                                  &outputSize);
    assert(status == StatusOk);
    assert(output->numNodes == 0);
    memFree(output);

    status = dag->getChildDagNode((char *) dagArray0[1].dagNodeName,
                                  &output,
                                  &outputSize);
    assert(status == StatusOk);

    for (uint64_t ii = 0; ii < output->numNodes; ++ii) {
        for (int jj = 0; jj < childArrayNum; ++jj) {
            if (strcmp(output->node[ii]->hdr.name, childArray[jj].name) == 0) {
                childArray[jj].found = true;
                printf("found child dag node:%s\n", output->node[ii]->hdr.name);
            }
        }
    }

    for (int ii = 0; ii < childArrayNum; ++ii) {
        assert(childArray[ii].found);
    }

    memFree(output);
    return status;
}

static Status
listDagNodeTest(Dag *handleId, uint64_t numNode, ListNodeName *nodeNameArray)
{
    Status status;
    XcalarApiListDagNodesOutput *listNodesOut;
    XcalarApiOutput *output = NULL;
    size_t outputSizeOut;
    bool found;

    status = handleId->listDagNodeInfo("DS*",
                                       &output,
                                       &outputSizeOut,
                                       SrcTable,
                                       &testUserId);
    assert(status == StatusOk);
    assert(output != NULL);

    listNodesOut = &output->outputResult.listNodesOutput;

    for (uint64_t ii = 0; ii < listNodesOut->numNodes; ++ii) {
        found = false;
        for (unsigned jj = 0; jj < numNode; ++jj) {
            if (strcmp(listNodesOut->nodeInfo[ii].name,
                       nodeNameArray[jj].name) == 0) {
                nodeNameArray[jj].found = true;
                printf("found table dag node:%s\n",
                       listNodesOut->nodeInfo[jj].name);
                found = true;
            }
        }
        assert(found);
    }

    for (unsigned ii = 0; ii < numNode; ++ii) {
        assert(nodeNameArray[ii].found);
    }

    memFree(output);
    return status;
}

static Status
listTableTest(Dag *dag)
{
    Status status;
    ListNodeName tableArray[10] = {
        [0] = {"DS1-indexNode0", false},
        [1] = {"DS1-indexNode1", false},
        [2] = {"DS1-indexNode2", false},
        [3] = {"DS1-indexNode3", false},
        [4] = {"DS1-filterNode0", false},
        [5] = {"DS1-filterNode1", false},
        [6] = {"DS1-joinNode", false},
        [7] = {"DS2-indexNode1", false},
        [8] = {"DS2-indexNode2", false},
        [9] = {"DS2-joinNode", false},
    };

    int tableArrayNum = (int) (sizeof(tableArray) / sizeof(tableArray[0]));

    status = listDagNodeTest(dag, tableArrayNum, tableArray);

    return status;
}

static Status
getDagInOrderTest(Dag *dag)
{
    Status status;
    DagTypes::NodeId dagNodeId;
    char dagNodeName[DagTypes::MaxNameLen];
    unsigned idxDagArray0 = 0;
    unsigned idxDagArray1 = 0;

    status = dag->getFirstDagInOrder(&dagNodeId);
    assert(status == StatusOk);

    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status =
            dag->getDagNodeName(dagNodeId, dagNodeName, sizeof(dagNodeName));
        assert(status == StatusOk);

        if (idxDagArray0 < ArrayLen(dagArray0)) {
            assert(strcmp(dagNodeName, dagArray0[idxDagArray0].dagNodeName) ==
                   0);
            idxDagArray0++;
        } else {
            assert(strcmp(dagNodeName, dagArray1[idxDagArray1].dagNodeName) ==
                   0);
            idxDagArray1++;
        }

        status = dag->getNextDagInOrder(dagNodeId, &dagNodeId);
        assert(status == StatusOk);
    }
    assert(idxDagArray0 == ArrayLen(dagArray0));
    assert(idxDagArray1 == ArrayLen(dagArray1));

    return StatusOk;
}

static Status
dropNodeTest(Dag *dag)
{
    Status status;
    DagTypes::NodeId dagNodeId;

    // delete node that has no child node
    status = dag->getDagNodeId("DS1-indexNode2",
                               Dag::TableScope::LocalOnly,
                               &dagNodeId);
    assert(status == StatusOk);

    status = dag->dropNode("DS1-indexNode2", SrcTable, NULL, NULL);
    assert(status == StatusOk);

    status =
        dag->getDagNodeId("DS1-indexNode2", Dag::TableScope::LocalOnly, NULL);
    assert(status == StatusDagNodeNotFound);

    status = dag->getDagNodeName(dagNodeId, NULL, 0);
    assert(status == StatusOk);

    // delete node that has child node
    status = dag->getDagNodeId("DS1-indexNode1",
                               Dag::TableScope::LocalOnly,
                               &dagNodeId);
    assert(status == StatusOk);

    XcalarApiOutput *output;
    size_t outputSize;
    status = dag->bulkDropNodes("DS1-indexNode1",
                                &output,
                                &outputSize,
                                SrcTable,
                                &testUserId);
    assert(status == StatusOk);
    memFree(output);
    output = NULL;

    status =
        dag->getDagNodeId("DS1-indexNode1", Dag::TableScope::LocalOnly, NULL);
    assert(status == StatusDagNodeNotFound);

    status = dag->getDagNodeName(dagNodeId, NULL, 0);
    assert(status == StatusOk);

    status = dag->getDagByName((char *) "DS1-joinNode",
                               Dag::TableScope::LocalOnly,
                               &output,
                               &outputSize);
    assert((output->outputResult.dagOutput.node[3]->hdr.state ==
            DgDagStateDropped) ||
           (output->outputResult.dagOutput.node[4]->hdr.state ==
            DgDagStateDropped));

    memFree(output);

    return StatusOk;
}

static Status
liboptTest()
{
    Dag *handleId;
    Status status;
    uint64_t opArraySize;
    DagTypes::NodeId *parentNodeId = NULL;
    DagLib *dagLib = DagLib::get();

    DagNodeInfo opArray[21] = {
        [0] = {"OP-loadNode0",
               XcalarApiDatasetPrefix "OP-loadNode0",
               0,
               {NULL},
               XcalarApiBulkLoad},
        [1] = {"OP-loadNode1",
               XcalarApiDatasetPrefix "OP-loadNode1",
               0,
               {NULL},
               XcalarApiBulkLoad},
        [2] = {"OP-loadNode2",
               XcalarApiDatasetPrefix "OP-loadNode2",
               0,
               {NULL},
               XcalarApiBulkLoad},
        [3] = {"OP-loadNode3",
               XcalarApiDatasetPrefix "OP-loadNode3",
               0,
               {NULL},
               XcalarApiBulkLoad},

        [4] = {"OP-indexNode0",
               "OP-indexNode0",
               1,
               {XcalarApiDatasetPrefix "OP-loadNode0"},
               XcalarApiIndex},
        [5] = {"OP-indexNode1",
               "OP-indexNode1",
               1,
               {XcalarApiDatasetPrefix "OP-loadNode0"},
               XcalarApiIndex},
        [6] = {"OP-indexNode2",
               "OP-indexNode2",
               1,
               {XcalarApiDatasetPrefix "OP-loadNode1"},
               XcalarApiIndex},
        [7] = {"OP-indexNode3",
               "OP-indexNode3",
               1,
               {XcalarApiDatasetPrefix "OP-loadNode1"},
               XcalarApiIndex},
        [8] = {"OP-indexNode4",
               "OP-indexNode4",
               1,
               {XcalarApiDatasetPrefix "OP-loadNode2"},
               XcalarApiIndex},
        [9] = {"OP-indexNode5",
               "OP-indexNode5",
               1,
               {XcalarApiDatasetPrefix "OP-loadNode2"},
               XcalarApiIndex},
        [10] = {"OP-indexNode6",
                "OP-indexNode6",
                1,
                {XcalarApiDatasetPrefix "OP-loadNode3"},
                XcalarApiIndex},
        [11] = {"OP-indexNode7",
                "OP-indexNode7",
                1,
                {XcalarApiDatasetPrefix "OP-loadNode3"},
                XcalarApiIndex},

        [12] = {"OP-joinNode0",
                "OP-joinNode0",
                2,
                {"OP-indexNode0", "OP-indexNode2"},
                XcalarApiJoin},

        [13] = {"OP-joinNode1",
                "OP-joinNode1",
                2,
                {"OP-indexNode1", "OP-indexNode3"},
                XcalarApiJoin},

        [14] = {"OP-joinNode2",
                "OP-joinNode2",
                2,
                {"OP-indexNode2", "OP-indexNode4"},
                XcalarApiJoin},

        [15] = {"OP-joinNode3",
                "OP-joinNode3",
                2,
                {"OP-joinNode2", "OP-indexNode5"},
                XcalarApiJoin},

        [16] = {"OP-joinNode4",
                "OP-joinNode4",
                2,
                {"OP-joinNode0", "OP-joinNode3"},
                XcalarApiJoin},

        [17] = {"OP-joinNode5",
                "OP-joinNode5",
                2,
                {"OP-joinNode1", "OP-joinNode4"},
                XcalarApiJoin},
        [18] = {"OP-joinNode6",
                "OP-joinNode6",
                2,
                {"OP-joinNode5", "OP-indexNode6"},
                XcalarApiJoin},

        [19] = {"OP-joinNode7",
                "OP-joinNode7",
                2,
                {"OP-joinNode6", "OP-indexNode7"},
                XcalarApiJoin},

        [20] = {"OP-joinNode8",
                "OP-joinNode8",
                2,
                {"OP-joinNode7", "OP-joinNode2"},
                XcalarApiJoin},
    };

    opArraySize = sizeof(opArray) / sizeof(opArray[0]);

    status =
        dagLib->createNewDag(23, DagTypes::WorkspaceGraph, NULL, &handleId);
    assert(status == StatusOk);
    printf("create first dag handleID:%lu\n", handleId->getId());

    DagTypes::NodeId dagNodeId = DagTypes::InvalidDagNodeId;
    DagTypes::NodeId targetNodeIdArray[1];

    for (unsigned ii = 0; ii < opArraySize; ++ii) {
        assert(parentNodeId == NULL);
        size_t parentNodeIdSize = sizeof(*parentNodeId) * opArray[ii].numParent;
        parentNodeId = (DagTypes::NodeId *) memAlloc(parentNodeIdSize);
        assert(parentNodeId != NULL);
        Dag *parentGraphs[opArray[ii].numParent];
        for (unsigned jj = 0; jj < opArray[ii].numParent; ++jj) {
            status = handleId->getDagNodeId(opArray[ii].parentName[jj],
                                            Dag::TableScope::LocalOnly,
                                            &parentNodeId[jj]);
            assert(status == StatusOk);
            parentGraphs[jj] = handleId;
        }

        dagNodeId = DagTypes::InvalidDagNodeId;
        status = handleId->createNewDagNode(opArray[ii].api,
                                            NULL,
                                            0,
                                            XdbIdInvalid,
                                            TableNsMgr::InvalidTableId,
                                            (char *) opArray[ii].dagNodeName,
                                            opArray[ii].numParent,
                                            parentGraphs,
                                            parentNodeId,
                                            &dagNodeId);
        assert(status == StatusOk);

        if (ii == (opArraySize - 1)) {
            targetNodeIdArray[0] = dagNodeId;
            printf("targetNodeId = %lu\n", targetNodeIdArray[0]);
        }

        printf("create node name : %s, node Id: %lu \n",
               opArray[ii].name,
               dagNodeId);

        memFree(parentNodeId);
        parentNodeId = NULL;
    }

    // uint64_t numQueuedTask, numRunningTask, numFinishedTask, numFailedTask;
    // This can not be run without instanciate usernode
    // status = queryOptimizer(handleId, numTarget, targetNodeIdArray,
    //                        &numQueuedTask, &numRunningTask, &numFinishedTask,
    //                        &numFailedTask);
    assert(status == StatusOk);

    status = dagLib->destroyDag(handleId, DagTypes::DestroyDeleteAndCleanNodes);
    assert(status == StatusOk);

    handleId = NULL;

    return status;
}

static Status
variableSizeDagNodeTest()
{
    Dag *dag;
    Status status;
    uint64_t ii;
    DagLib *dagLib = DagLib::get();

    typedef struct {
        uint64_t header;
        uint64_t footer;
    } SkinnyStruct;

    typedef struct {
        uint64_t header;
        const char *fats;
        uint64_t footer;
    } FatStruct;

    SkinnyStruct skinnyStruct = {.header = 0xdeadbeef, .footer = 0xcafebabe};

    FatStruct fatStruct;
    fatStruct.header = 0xdeadbeef;
    fatStruct.fats = "Hello World";
    fatStruct.footer = 0xcafebabe;

    typedef struct {
        const char *name;
        XcalarApiInput *input;
        size_t inputSize;
        DagTypes::NodeId dagNodeId;
    } Node;

    Node nodes[] = {
        {
            .name = "Skinny",
            .input = (XcalarApiInput *) &skinnyStruct,
            .inputSize = sizeof(skinnyStruct),
            .dagNodeId = 0,
        },
        {
            .name = "Fat",
            .input = (XcalarApiInput *) &fatStruct,
            .inputSize = sizeof(fatStruct),
            .dagNodeId = 0,
        },
    };

    status = dagLib->createNewDag(3, DagTypes::WorkspaceGraph, NULL, &dag);
    assert(status == StatusOk);

    // Create
    for (ii = 0; ii < ArrayLen(nodes); ii++) {
        nodes[ii].dagNodeId = DagTypes::InvalidDagNodeId;
        status = dag->createNewDagNode(XcalarApiIndex,
                                       nodes[ii].input,
                                       nodes[ii].inputSize,
                                       XdbIdInvalid,
                                       TableNsMgr::InvalidTableId,
                                       (char *) nodes[ii].name,
                                       0,
                                       NULL,
                                       NULL,
                                       &nodes[ii].dagNodeId);
        assert(status == StatusOk);
    }

    // Check
    for (ii = 0; ii < ArrayLen(nodes); ii++) {
        XcalarApiOutput *output;
        size_t outputSize;

        status = dag->getDagByName((char *) nodes[ii].name,
                                   Dag::TableScope::LocalOnly,
                                   &output,
                                   &outputSize);
        assert(status == StatusOk);
        assert(output->outputResult.dagOutput.numNodes == 1);
        assert(output->outputResult.dagOutput.node[0]->hdr.inputSize ==
               nodes[ii].inputSize);

        switch (ii) {
        case 0: {
            SkinnyStruct *candidateSkinny;
            candidateSkinny =
                (SkinnyStruct *) output->outputResult.dagOutput.node[0]->input;
            printf("Candidate skinny's header: %lx, footer: %lx\n",
                   candidateSkinny->header,
                   candidateSkinny->footer);
            assert(candidateSkinny->header == skinnyStruct.header);
            assert(candidateSkinny->footer == skinnyStruct.footer);
            break;
        }
        case 1: {
            FatStruct *candidateFat;
            candidateFat =
                (FatStruct *) output->outputResult.dagOutput.node[0]->input;
            printf("Candidate fat's header: %lx, footer: %lx\n",
                   candidateFat->header,
                   candidateFat->footer);
            assert(candidateFat->header == fatStruct.header);
            assert(candidateFat->footer == fatStruct.footer);
            break;
        }
        default:
            assert(0);
        }

        memFree(output);
        output = NULL;
    }

    // Delete
    for (ii = 0; ii < ArrayLen(nodes); ii++) {
        status = dag->deleteDagNodeById(nodes[ii].dagNodeId,
                                        DagTypes::DeleteNodeCompletely);
        assert(status == StatusOk);
    }

    status = dagLib->destroyDag(dag, DagTypes::DestroyNone);
    assert(status == StatusOk);
    dag = NULL;

    return StatusOk;
}

static void *
sanityTest(void *arg)
{
    Status status;

    Dag *globalDagId = NULL;

    setUp(&globalDagId);

    status = listAllNodes(globalDagId);
    assert(status == StatusOk);

    status = changeStateTest(globalDagId);
    assert(status == StatusOk);

    status = getDagTest(globalDagId);
    assert(status == StatusOk);

    status = getChildrenTest(globalDagId);
    assert(status == StatusOk);

    status = listTableTest(globalDagId);
    assert(status == StatusOk);

    status = getDagInOrderTest(globalDagId);
    assert(status == StatusOk);

    status = listAllNodes(globalDagId);
    assert(status == StatusOk);

    status = changeStateTest(globalDagId);
    assert(status == StatusOk);

    status = getDagTest(globalDagId);
    assert(status == StatusOk);

    status = getChildrenTest(globalDagId);
    assert(status == StatusOk);

    status = listTableTest(globalDagId);
    assert(status == StatusOk);

    status = getDagInOrderTest(globalDagId);
    assert(status == StatusOk);

    status = dropNodeTest(globalDagId);
    assert(status == StatusOk);

    status = liboptTest();
    assert(status == StatusOk);

    status = variableSizeDagNodeTest();
    assert(status == StatusOk);

    tearDown(globalDagId);

    return NULL;
}

Status
dgSanityTest()
{
    pthread_t *threadHandle;
    Status status = StatusOk;
    unsigned numThread = 64;
    uint64_t loop = 8;

    threadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) * numThread, moduleName);
    assert(threadHandle != NULL);

    for (unsigned ii = 0; ii < loop; ++ii) {
        for (unsigned jj = 0; jj < numThread; jj++) {
            status = Runtime::get()->createBlockableThread(&threadHandle[jj],
                                                           NULL,
                                                           sanityTest,
                                                           NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "createBlockableThread failed: %s",
                        strGetFromStatus(status));
            }
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogDebug,
                "%u threads have been created.\n",
                numThread);

        for (unsigned jj = 0; jj < numThread; jj++) {
            sysThreadJoin(threadHandle[jj], NULL);
        }

        xSyslog(moduleName,
                XlogDebug,
                "%u threads have been joined.\n",
                numThread);
    }

    if (threadHandle != NULL) {
        memFree(threadHandle);
    }

    return status;
}

static void
insertNewRandDag(RandDag *randDag, Dag *dag)
{
    uint64_t numNodes = randDag->getNumNode();
    uint64_t numParent;
    Status status;
    XcalarApis dummyApi = XcalarApiIndex;
    XcalarApiInput *dummyApiInput = new XcalarApiInput();
    memset(dummyApiInput, 0, sizeof(*dummyApiInput));
    pid_t tid = sysGetTid();
    size_t inputSize = pbUnionSizeXcalarApiInput(dummyApi);

    DagTypes::NodeId parentNodeIdArray[numNodes];

    for (uint64_t ii = 0; ii < numNodes; ++ii) {
        DagTypes::NodeId nodeId = DagTypes::InvalidDagNodeId;
        randDag->getParentNodeIds(ii, parentNodeIdArray, &numParent);
        char name[255];
        snprintf(name, sizeof(name), "%d-%lu", tid, ii);
        Dag *parentGraphs[numParent];
        for (uint64_t jj = 0; jj < numParent; jj++) {
            parentGraphs[jj] = dag;
        }
        status = dag->createNewDagNode(dummyApi,
                                       dummyApiInput,
                                       inputSize,
                                       XdbIdInvalid,
                                       TableNsMgr::InvalidTableId,
                                       name,
                                       numParent,
                                       parentGraphs,
                                       parentNodeIdArray,
                                       &nodeId);
        assert(status == StatusOk);
        randDag->saveNodeId(ii, nodeId);
    }
    assert(dag->getNumNode() == numNodes);
    delete dummyApiInput;
    dummyApiInput = NULL;
}

// As the node are created randomly, we may not always find a valid node, in
// which case, the nodeIdOut will be InvalidNodeId
void
getRandomNodeId(RandHandle *rndHandle,
                RandDag *randDag,
                DagTypes::NodeId *nodeIdOut,
                uint64_t *randDagNodeIdOut,
                Dag *dag)
{
    uint64_t randDagId = 0;
    uint64_t retryCount = 0;
    uint64_t numNodes = randDag->getNumNode();
    DagTypes::NodeId nodeId;
    bool found = false;

    while (retryCount < maxRetryForRandomNode) {
        randDagId = rndGenerate32(rndHandle) % numNodes;
        nodeId = randDag->getNodeId(randDagId);

        if ((nodeId != DagTypes::InvalidDagNodeId) &&
            (!randDag->nodeDeleted(randDagId))) {
            found = true;
            break;
        }
        retryCount++;
    }

    *randDagNodeIdOut = randDagId;
    *nodeIdOut = found ? nodeId : (DagTypes::NodeId) DagTypes::InvalidDagNodeId;
}

static void
verifyDagRelation(Dag *dag,
                  RandDag *randDag,
                  DagTypes::NodeId nodeId,
                  uint64_t randDagId)
{
    dag->lock();
    DagNodeTypes::Node *nodeOut;
    Status status = dag->lookupNodeById(nodeId, &nodeOut);
    assert(status == StatusOk);
    dag->unlock();
    nodeOut = NULL;

    XcalarApiDagNode *apiDagNodeOut;
    status = dag->getDagNode(nodeId, &apiDagNodeOut);
    assert(status == StatusOk);
    memFree(apiDagNodeOut);
    apiDagNodeOut = NULL;

    DagTypes::NodeId *srcNodeArrayOut;
    uint64_t numSrcNodesOut;
    status = dag->getRootNodeIds(&srcNodeArrayOut, &numSrcNodesOut);
    assert(status == StatusOk);

    randDag->verifyRootNodeIds(srcNodeArrayOut, numSrcNodesOut);

    memFree(srcNodeArrayOut);
    srcNodeArrayOut = NULL;

    DagTypes::NodeId *childNodesArrayOut;
    uint64_t numChildOut;

    char parentNameOut[256];
    status = dag->getDagNodeName(nodeId, parentNameOut, 256);
    assert(status == StatusOk);

    status = dag->getChildDagNodeId(nodeId, &childNodesArrayOut, &numChildOut);
    assert(status == StatusOk);

    uint64_t numChildNodeLeft = 0;
    randDag->verifyChildNodeIds(randDagId,
                                childNodesArrayOut,
                                numChildOut,
                                &numChildNodeLeft);
    if (numChildNodeLeft != 0) {
        xSyslog(moduleName,
                XlogDebug,
                "debug:: verifying childnode on node:%s",
                parentNameOut);
        for (uint64_t ii = 0; ii < numChildOut; ++ii) {
            if (childNodesArrayOut[ii] != DagTypes::InvalidDagNodeId) {
                char nameOut[256];
                Status status =
                    dag->getDagNodeName(childNodesArrayOut[ii], nameOut, 256);
                assert(status == StatusOk);
                xSyslog(moduleName,
                        XlogDebug,
                        "debug:: childnode :%s is not found",
                        nameOut);
                uint64_t missingIdx =
                    randDag->getNodeIdx(childNodesArrayOut[ii]);

                xSyslog(moduleName,
                        XlogDebug,
                        "debug:: childnode idx %lu",
                        missingIdx);
            }
        }
        assert(0);
    }

    if (childNodesArrayOut != NULL) {
        memFree(childNodesArrayOut);
        childNodesArrayOut = NULL;
    }

    DagTypes::NodeName nodeName;
    status = dag->getDagNodeName(nodeId, nodeName, sizeof(nodeName));
    assert(status == StatusOk);

    XcalarApiDagOutput *output = NULL;
    size_t outputSize;

    status = dag->getChildDagNode(nodeName, &output, &outputSize);
    assert(status == StatusOk);
    if (output->numNodes > 0) {
        DagTypes::NodeId childNodesArray[output->numNodes];
        for (uint64_t jj = 0; jj < output->numNodes; ++jj) {
            childNodesArray[jj] = output->node[jj]->hdr.dagNodeId;
        }
        uint64_t numChildLeft = 0;
        randDag->verifyChildNodeIds(randDagId,
                                    childNodesArray,
                                    output->numNodes,
                                    &numChildLeft);
    }
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    DagTypes::NodeId *parentNodesArrayOut;
    uint64_t numParentOut;

    status =
        dag->getParentDagNodeId(nodeId, &parentNodesArrayOut, &numParentOut);
    assert(status == StatusOk);

    randDag->verifyParentNodeIds(randDagId, parentNodesArrayOut, numParentOut);

    if (parentNodesArrayOut != NULL) {
        memFree(parentNodesArrayOut);
        parentNodesArrayOut = NULL;
    }

    status = dag->getParentDagNode(nodeId, &output, &outputSize);
    assert(status == StatusOk);
    if (output->numNodes > 0) {
        DagTypes::NodeId parentNodesArray[output->numNodes];
        for (uint64_t jj = 0; jj < output->numNodes; ++jj) {
            parentNodesArray[jj] = output->node[jj]->hdr.dagNodeId;
        }
        randDag->verifyParentNodeIds(randDagId,
                                     parentNodesArray,
                                     output->numNodes);
    }
    memFree(output);

    xSyslog(moduleName, XlogInfo, "TestGetDagRelation");
}

static void
verifyDagNameAndGetDag(pid_t prefixTid,
                       DagTypes::NodeId nodeId,
                       uint64_t randDagId,
                       Dag *dag,
                       RandDag *randDag)
{
    DagTypes::NodeName name;

    DagTypes::NodeName targetName;
    DagTypes::NodeName newName;

    snprintf(newName, sizeof(newName), "%d-%lu-new", prefixTid, randDagId);

    snprintf(targetName, sizeof(targetName), "%d-%lu", prefixTid, randDagId);

    Status status = dag->getDagNodeName(nodeId, name, sizeof(name));
    assert(status == StatusOk);
    assert(strncmp(name, targetName, sizeof(name)) == 0);

    DagTypes::NodeId dagNodeIdOut;
    status = dag->getDagNodeId(name, Dag::TableScope::LocalOnly, &dagNodeIdOut);
    assert(status == StatusOk);
    assert(dagNodeIdOut == nodeId);

    // Test get DAG
    XcalarApiOutput *outputOut = NULL;
    size_t outputSizeOut;

    status = dag->getDagByName(name,
                               Dag::TableScope::LocalOnly,
                               &outputOut,
                               &outputSizeOut);
    assert(status == StatusOk);

    uint64_t numNodesForGetDag = outputOut->outputResult.dagOutput.numNodes;

    uint64_t randDagIdArray[numNodesForGetDag];

    randDag->getRandDag(randDagId, numNodesForGetDag, randDagIdArray);

    XcalarApiOutput *outputOut2 = NULL;
    size_t outputSizeOut2;
    status = dag->getDagById(nodeId, &outputOut2, &outputSizeOut2);
    assert(status == StatusOk);

    for (uint64_t ii = 0; ii < numNodesForGetDag; ++ii) {
        assert(outputOut->outputResult.dagOutput.node[ii]->hdr.dagNodeId ==
               outputOut2->outputResult.dagOutput.node[ii]->hdr.dagNodeId);
        uint64_t jj;
        for (jj = 0; jj < randDag->getNumNode(); jj++) {
            if (randDag->getNodeId(jj) ==
                outputOut->outputResult.dagOutput.node[ii]->hdr.dagNodeId) {
                break;
            }
        }
        assert(jj < randDag->getNumNode());
    }

    if (outputOut != NULL) {
        memFree(outputOut);
        outputOut = NULL;
    };

    if (outputOut2 != NULL) {
        memFree(outputOut2);
        outputOut2 = NULL;
    }

    xSyslog(moduleName, XlogInfo, "TestDagName");
}

struct RandCreateArg {
    Dag *dag;
    RandDag *randDag;
    pid_t prefixTid;  // the driver thread tid is encoded in the dagnode name
};

static void
verifyListDag(Dag *dag)
{
    XcalarApiOutput *outputOut = NULL;
    size_t outputSizeOut;
    XcalarApiListDagNodesOutput *listNodesOutput;

    Status status = dag->listDagNodeInfo("*",
                                         &outputOut,
                                         &outputSizeOut,
                                         SrcDataset,
                                         &testUserId);
    assert(status == StatusOk);

    listNodesOutput = &outputOut->outputResult.listNodesOutput;
    for (uint64_t ii = 0; ii < listNodesOutput->numNodes; ++ii) {
        DagTypes::NodeId nodeId = listNodesOutput->nodeInfo[ii].dagNodeId;
        XcalarApis api;
        status = dag->getDagNodeApi(nodeId, &api);
        assert(status == StatusOk);
        assert(api == XcalarApiBulkLoad);
    }

    if (outputOut != NULL) {
        memFree(outputOut);
        outputOut = NULL;
    }

    status = dag->listDagNodeInfo("*",
                                  &outputOut,
                                  &outputSizeOut,
                                  SrcConstant,
                                  &testUserId);
    assert(status == StatusOk);

    listNodesOutput = &outputOut->outputResult.listNodesOutput;
    for (uint64_t ii = 0; ii < listNodesOutput->numNodes; ++ii) {
        DagTypes::NodeId nodeId = listNodesOutput->nodeInfo[ii].dagNodeId;
        XcalarApis api;
        status = dag->getDagNodeApi(nodeId, &api);
        assert(status == StatusOk);
        assert(api == XcalarApiAggregate);
    }

    if (outputOut != NULL) {
        memFree(outputOut);
        outputOut = NULL;
    }

    status = dag->listDagNodeInfo("*",
                                  &outputOut,
                                  &outputSizeOut,
                                  SrcExport,
                                  &testUserId);
    assert(status == StatusOk);

    listNodesOutput = &outputOut->outputResult.listNodesOutput;
    for (uint64_t ii = 0; ii < listNodesOutput->numNodes; ++ii) {
        DagTypes::NodeId nodeId = listNodesOutput->nodeInfo[ii].dagNodeId;
        XcalarApis api;
        status = dag->getDagNodeApi(nodeId, &api);
        assert(status == StatusOk);
        assert(api == XcalarApiExport);
    }

    if (outputOut != NULL) {
        memFree(outputOut);
        outputOut = NULL;
    }

    status = dag->listDagNodeInfo("*",
                                  &outputOut,
                                  &outputSizeOut,
                                  SrcTable,
                                  &testUserId);
    assert(status == StatusOk);

    listNodesOutput = &outputOut->outputResult.listNodesOutput;
    for (uint64_t ii = 0; ii < listNodesOutput->numNodes; ++ii) {
        DagTypes::NodeId nodeId = listNodesOutput->nodeInfo[ii].dagNodeId;
        XcalarApis api;
        status = dag->getDagNodeApi(nodeId, &api);
        assert(status == StatusOk);
        assert(api != XcalarApiExport && api != XcalarApiAggregate &&
               api != XcalarApiBulkLoad);
    }

    if (outputOut != NULL) {
        memFree(outputOut);
        outputOut = NULL;
    }
}

static void *
randCreateNewNode(void *arg)
{
    RandCreateArg *createArg = (RandCreateArg *) arg;
    Dag *dag = createArg->dag;
    RandDag *randDag = createArg->randDag;
    pid_t prefixTid = createArg->prefixTid;
    uint64_t numNodes = randDag->getNumNode();
    DagTypes::NodeId parentNodeIdArray[numNodes];
    pid_t tid = sysGetTid();
    uint64_t numParent;
    uint64_t creationCount = 0;
    Status status;

    RandHandle rndHandle;
    rndInitHandle(&rndHandle, tid);

    for (uint64_t ii = 0; ii < (numNodesPerMatrix * 2); ++ii) {
        uint64_t randIdx;

        if (tid % 2 == 0 || ii >= numNodes) {
            randIdx = rndGenerate32(&rndHandle) % numNodes;
        } else {
            randIdx = ii;
        }
        if (randDag->getNodeId(randIdx) != 0) {
            continue;
        }

        if (!randDag->reserveNode(randIdx)) {
            continue;
        }

        if (randDag->getNodeId(randIdx) != 0) {
            randDag->releaseNode(randIdx);
            continue;
        }

        randDag->getParentNodeIds(randIdx, parentNodeIdArray, &numParent);

        char name[255];
        snprintf(name, sizeof(name), "%d-%lu", prefixTid, randIdx);

        assert(dag->getDagNodeId(name, Dag::TableScope::LocalOnly, NULL) !=
               StatusOk);

        uint64_t dummyApiIdx = rndGenerate32(&rndHandle) % ArrayLen(apiArray);
        XcalarApis dummyApi = apiArray[dummyApiIdx];
        XcalarApiInput *dummyApiInput = new XcalarApiInput();
        memset(dummyApiInput, 0, sizeof(*dummyApiInput));
        DagTypes::NodeId nodeId = DagTypes::InvalidDagNodeId;

        xSyslog(moduleName, XlogDebug, "trying to create node:%s", name);
        size_t inputSize = pbUnionSizeXcalarApiInput(dummyApi);
        Dag *parentGraphs[numParent];
        for (uint64_t jj = 0; jj < numParent; jj++) {
            parentGraphs[jj] = dag;
        }
        status = dag->createNewDagNode(dummyApi,
                                       dummyApiInput,
                                       inputSize,
                                       XdbIdInvalid,
                                       TableNsMgr::InvalidTableId,
                                       name,
                                       numParent,
                                       parentGraphs,
                                       parentNodeIdArray,
                                       &nodeId);
        delete dummyApiInput;
        dummyApiInput = NULL;
        if (status == StatusOk) {
            creationCount++;
            randDag->saveNodeId(randIdx, nodeId);

            randDag->releaseNode(randIdx);

            // Test node deletion
            if (creationCount % deletionFrequency == 0) {
                DagTypes::NodeId nodeId;
                uint64_t randDagId = rndGenerate32(&rndHandle) % numNodes;

                if ((randDag->getNodeId(randDagId) == 0) ||
                    (randDag->nodeDeleted(randDagId))) {
                    continue;
                }

                if (!randDag->reserveNode(randDagId)) {
                    continue;
                }

                if ((randDag->getNodeId(randDagId) == 0) ||
                    (randDag->nodeDeleted(randDagId))) {
                    randDag->releaseNode(randDagId);
                    continue;
                }

                nodeId = randDag->getNodeId(randDagId);

                DagTypes::NodeName name;
                status = dag->getDagNodeName(nodeId, name, sizeof(name));

                if (status != StatusOk) {
                    randDag->releaseNode(randDagId);
                    continue;
                }

                xSyslog(moduleName,
                        XlogDebug,
                        "debug: trying to delete node: %s (%lu)",
                        name,
                        nodeId);

                status = dag->deleteDagNodeById(nodeId, false);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogDebug,
                            "debug:fail to delete node: %s (%lu)",
                            name,
                            nodeId);
                    randDag->releaseNode(randDagId);
                    continue;
                }

                xSyslog(moduleName,
                        XlogDebug,
                        "debug:delete node: %s (%lu)",
                        name,
                        nodeId);
                randDag->deleteNode(randDagId);
                randDag->releaseNode(randDagId);
            }
        } else {
            assert(dag->getDagNodeId(name, Dag::TableScope::LocalOnly, NULL) !=
                   StatusOk);
            xSyslog(moduleName,
                    XlogDebug,
                    "trying to create node:%s failed: %s",
                    name,
                    strGetFromStatus(status));

            randDag->releaseNode(randIdx);
            continue;
        }

        // Test random DAG operation
        uint64_t randDagId;
        nodeId = DagTypes::InvalidDagNodeId;
        getRandomNodeId(&rndHandle, randDag, &nodeId, &randDagId, dag);
        if (nodeId == DagTypes::InvalidDagNodeId) {
            continue;
        }

        if (!randDag->reserveNode(randDagId)) {
            continue;
        }
        nodeId = randDag->getNodeId(randDagId);
        if ((nodeId == DagTypes::InvalidDagNodeId) ||
            (randDag->nodeDeleted(randDagId))) {
            randDag->releaseNode(randDagId);
            continue;
        }

        DagTestCase testCase =
            (DagTestCase)(rndGenerate32(&rndHandle) % DagTestCaseLength);
        switch (testCase) {
        case DagTestRefCount: {
            uint64_t refCountInit;
            status = dag->readDagNodeRefById(nodeId, &refCountInit);
            assert(status == StatusOk);
            status = dag->getDagNodeRefById(nodeId);
            assert(status == StatusOk);

            uint64_t refCountInc;
            status = dag->readDagNodeRefById(nodeId, &refCountInc);
            assert(status == StatusOk);
            assert(refCountInc == (refCountInit + 1));
            dag->putDagNodeRefById(nodeId);

            uint64_t refCountDec;
            status = dag->readDagNodeRefById(nodeId, &refCountDec);
            assert(status == StatusOk);
            assert(refCountDec == refCountInit);

            xSyslog(moduleName, XlogInfo, "DagTestRefCount");
            break;
        }

        case DagTestNodeState: {
            uint64_t refCountInit;
            status = dag->readDagNodeRefById(nodeId, &refCountInit);
            assert(status == StatusOk);

            DgDagState state;
            status = dag->getDagNodeStateAndRef(nodeId, &state);
            assert(status == StatusOk);
            assert(state == DgDagStateCreated);

            uint64_t refCountInc;
            status = dag->readDagNodeRefById(nodeId, &refCountInc);
            assert(status == StatusOk);
            assert(refCountInc == (refCountInit + 1));
            dag->putDagNodeRefById(nodeId);

            xSyslog(moduleName, XlogInfo, "DagTestNodeState");
            break;
        }

        case DagTestSetScalar: {
            Scalar *scalar =
                Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
            assert(scalar != NULL);
            scalar->fieldType = DfInt64;
            scalar->fieldNumValues = 1;
            scalar->fieldVals.int64Val[0] = 0xdeadbeef;
            scalar->fieldUsedSize = sizeof(uint64_t);

            status = dag->setScalarResult(nodeId, scalar);
            assert(status == StatusOk);

            Scalar *scalarOut = NULL;
            XcalarApis api;
            status = dag->getDagNodeApi(nodeId, &api);
            assert(status == StatusOk);

            Status status = dag->getScalarResult(nodeId, &scalarOut);
            assert(status == StatusOk);

            assert(scalarOut->fieldVals.int64Val[0] ==
                   scalar->fieldVals.int64Val[0]);

            Scalar::freeScalar(scalarOut);
            Scalar::freeScalar(scalar);
            break;
        }

        case DagTestGetGarbage: {
            XcalarApiOutput *output = NULL;
            size_t outputSize;

            status = dag->getPerNodeOpStats(nodeId, &output, &outputSize);
            assert(status == StatusOk);

            if (output != NULL) {
                memFree(output);
                output = NULL;
            }

            status = dag->getDagNodeRefById(nodeId);
            assert(status == StatusOk);

            status =
                dag->getOpStats(nodeId, DgDagStateReady, &output, &outputSize);

            assert(status == StatusOk);

            dag->putDagNodeRefById(nodeId);

            if (output != NULL) {
                memFree(output);
                output = NULL;
            }

            OpStatus *opStatus = NULL;
            status = dag->getOpStatus(nodeId, &opStatus);
            assert(status == StatusOk);

            DagTypes::NodeId *ftab[XcalarApiMaxFailureEvals];

            for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
                ftab[ii] = NULL;
            }
            status = dag->updateOpDetails(nodeId, ftab, NULL, NULL);
            assert(status == StatusOk);

            DagTypes::NodeName nodeName;
            status = dag->getDagNodeName(nodeId, nodeName, sizeof(nodeName));
            assert(status == StatusOk);

            status = dag->cancelOp(nodeName);
            assert(status == StatusOk || status == StatusQrQueryNameInvalid);

            xSyslog(moduleName, XlogInfo, "DagTestGetGarbage");
            break;
        }

        case DagTestError: {
            XcalarApis api;
            status = dag->getDagNodeApi(nodeId, &api);
            assert(status == StatusOk);
            if (api == XcalarApiBulkLoad) {
                Status status;
                DsDatasetId datasetIdOut;
                status = dag->getDatasetIdFromDagNodeId(nodeId, &datasetIdOut);
                assert(status == StatusDsNotFound);

                xSyslog(moduleName, XlogInfo, "DagTestError");
            }

            break;
        }

        default:
            assert(0);
        }
        randDag->releaseNode(randDagId);
    }
    return NULL;
}

static void *
mutiThreadAddDelete(void *arg)
{
    uint64_t numNodes = numNodesPerMatrix;
    uint64_t numThread = numThreadScaningMatrix;
    RandHandle rndHandle;
    pid_t tid = sysGetTid();
    pthread_t *threadHandle;
    Status status;

    threadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) * numThread, moduleName);
    assert(threadHandle != NULL);

    rndInitHandle(&rndHandle, tid);

    Dag *dag = (Dag *) arg;
    assert(dag != NULL);

    RandDag *randDag = (RandDag *) memAllocExt(sizeof(*randDag), moduleName);
    assert(randDag != NULL);

    new (randDag) RandDag(numNodes);

    RandCreateArg randCreateArg;
    randCreateArg.dag = dag;
    randCreateArg.randDag = randDag;
    randCreateArg.prefixTid = tid;

    for (unsigned ii = 0; ii < numThread; ii++) {
        status = Runtime::get()->createBlockableThread(&threadHandle[ii],
                                                       NULL,
                                                       randCreateNewNode,
                                                       &randCreateArg);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    for (unsigned ii = 0; ii < numThread; ii++) {
        sysThreadJoin(threadHandle[ii], NULL);
    }

    // Test DAG node relationship correctness
    for (uint64_t ii = 0; ii < (numNodesPerMatrix / 2); ++ii) {
        uint64_t randDagId;
        DagTypes::NodeId nodeId = DagTypes::InvalidDagNodeId;
        getRandomNodeId(&rndHandle, randDag, &nodeId, &randDagId, dag);
        if (nodeId == DagTypes::InvalidDagNodeId) {
            continue;
        }

        verifyDagNameAndGetDag(tid, nodeId, randDagId, dag, randDag);
        verifyDagRelation(dag, randDag, nodeId, randDagId);
    }

    randDag->~RandDag();
    memFree(randDag);

    memFree(threadHandle);
    return NULL;
}

void *
randomTest(void *arg)
{
    Status status;
    uint64_t numNodes = numNodesPerMatrix;
    uint64_t numSlot =
        mathGetPrime(numNodesPerMatrix * numThreadScaningMatrix + 1);
    Dag *dag;
    DagLib *dagLib = DagLib::get();

    // Use DagTypes::QueryGraph type since this dag will be
    // serialized/deserialized
    status =
        dagLib->createNewDag(numSlot, DagTypes::WorkspaceGraph, NULL, &dag);
    assert(status == StatusOk);

    RandDag *randDag = (RandDag *) memAllocExt(sizeof(*randDag), moduleName);

    assert(randDag != NULL);

    new (randDag) RandDag(numNodes);
    insertNewRandDag(randDag, dag);

    // testing methods that work on the whole DAG

    // test change all states
    status = dag->changeAllNodesState(DgDagStateCreated);
    assert(status == StatusOk);
    for (uint64_t ii = 0; ii < numNodes; ++ii) {
        DagTypes::NodeId dagNodeId = randDag->getNodeId(ii);
        DgDagState state;
        assert(dag->getDagNodeState(dagNodeId, &state) == StatusOk);
        assert(state == DgDagStateCreated);

        // change the state to ready so that the clone dag operation could
        // proceed
        status = dag->changeDagNodeState(dagNodeId, DgDagStateReady);
        assert(status == StatusOk);
    }

    // test getting DAG in chronological order

    DagTypes::NodeId dagNodeIdInDag;
    for (uint64_t ii = 0; ii < numNodes; ++ii) {
        DagTypes::NodeId dagNodeIdInRand = randDag->getNodeId(ii);

        if (ii == 0) {
            status = dag->getFirstDagInOrder(&dagNodeIdInDag);
            assert(status == StatusOk);
        } else {
            status = dag->getNextDagInOrder(dagNodeIdInDag, &dagNodeIdInDag);
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogInfo,
                "dagNodeIdInDag: %lu, idx: %lu",
                dagNodeIdInDag,
                ii);
        assert(dagNodeIdInDag == dagNodeIdInRand);
    }

    // test clone DAG
    DagTypes::NodeName *activeNodes = NULL;
    uint64_t numActiveNodes = 0;
    status = dag->getActiveDagNodes(DagTypes::WorkspaceGraph,
                                    &activeNodes,
                                    NULL,
                                    &numActiveNodes,
                                    DagTypes::IncludeNodeStateProcessing);
    assert(status == StatusOk);

    Dag *clonedDag;
    status = dag->cloneDag(&clonedDag,
                           DagTypes::QueryGraph,
                           dag->getSessionContainer(),
                           numActiveNodes,
                           activeNodes,
                           0,
                           NULL,
                           Dag::CloneFlagsNone);
    assert(status == StatusOk);

    if (numActiveNodes != 0) {
        DagTypes::NodeName *activeNodesCloned = NULL;
        uint64_t numActiveNodesCloned = 0;

        status =
            clonedDag->getActiveDagNodes(DagTypes::QueryGraph,
                                         &activeNodesCloned,
                                         NULL,
                                         &numActiveNodesCloned,
                                         DagTypes::IncludeNodeStateProcessing);
        assert(status == StatusOk);
        assert(numActiveNodes == numActiveNodesCloned);
        // nodes might not be in the same order

        memFree(activeNodesCloned);
    }

    if (activeNodes != NULL) {
        memFree(activeNodes);
        activeNodes = NULL;
    }
    status = dagLib->destroyDag(clonedDag, DagTypes::DestroyDeleteNodes);
    assert(status == StatusOk);
    clonedDag = NULL;
    uint64_t numThread = numThreadCreateNewMatrix;
    pthread_t *threadHandle;

    threadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) * numThread, moduleName);

    assert(threadHandle != NULL);

    // Test randomly add and delete node
    for (unsigned ii = 0; ii < numThread; ii++) {
        assert(dag != NULL);
        status = Runtime::get()->createBlockableThread(&threadHandle[ii],
                                                       NULL,
                                                       mutiThreadAddDelete,
                                                       (void *) dag);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    for (unsigned ii = 0; ii < numThread; ii++) {
        sysThreadJoin(threadHandle[ii], NULL);
    }

    verifyListDag(dag);

    XcalarApiOutput *output = NULL;
    size_t outputSizeOut;

    status =
        dag->bulkDropNodes("*", &output, &outputSizeOut, SrcTable, &testUserId);
    assert(status == StatusOk);
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    status = dag->bulkDropNodes("*",
                                &output,
                                &outputSizeOut,
                                SrcConstant,
                                &testUserId);
    assert(status == StatusOk);
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    status = dag->bulkDropNodes("*",
                                &output,
                                &outputSizeOut,
                                SrcExport,
                                &testUserId);
    assert(status == StatusOk);
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    status = dag->bulkDropNodes("*",
                                &output,
                                &outputSizeOut,
                                SrcDataset,
                                &testUserId);
    assert(status == StatusOk);
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    memFree(threadHandle);

    randDag->~RandDag();
    memFree(randDag);
    status = dagLib->destroyDag(dag, DagTypes::DestroyDeleteNodes);
    assert(status == StatusOk);

    return NULL;
}

Status
dgRandomTest()
{
    pthread_t *threadHandle;
    Status status = StatusOk;
    unsigned numThread = numThreadRandomTest;
    uint64_t loop = numLoopRandomTest;

    threadHandle =
        (pthread_t *) memAllocExt(sizeof(pthread_t) * numThread, moduleName);
    assert(threadHandle != NULL);

    for (unsigned ii = 0; ii < loop; ++ii) {
        for (unsigned jj = 0; jj < numThread; jj++) {
            status = Runtime::get()->createBlockableThread(&threadHandle[jj],
                                                           NULL,
                                                           randomTest,
                                                           NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "createBlockableThread failed: %s",
                        strGetFromStatus(status));
            }
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogDebug,
                "%u threads have been created.\n",
                numThread);

        for (unsigned jj = 0; jj < numThread; jj++) {
            sysThreadJoin(threadHandle[jj], NULL);
        }

        xSyslog(moduleName,
                XlogDebug,
                "%u threads have been joined.\n",
                numThread);
    }

    memFree(threadHandle);

    return status;
}

Status
libDagTestParseConfig(Config::Configuration *config,
                      char *key,
                      char *value,
                      bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibDagFuncTestConfig(LibDagNumLoopRandomTest)) ==
        0) {
        numLoopRandomTest = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDagFuncTestConfig(
                              LibDagNumThreadRandomTest)) == 0) {
        numThreadRandomTest = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDagFuncTestConfig(
                              LibDagNumNodesPerMatrix)) == 0) {
        numNodesPerMatrix = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDagFuncTestConfig(
                              LibDagNumThreadCreateNewMatrix)) == 0) {
        numThreadCreateNewMatrix = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDagFuncTestConfig(
                              LibDagDeletionFrequency)) == 0) {
        deletionFrequency = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDagFuncTestConfig(
                              LibDagMaxRetryForRandomNode)) == 0) {
        maxRetryForRandomNode = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibDagFuncTestConfig(
                              LibDagNumScanThreadPerMatrix)) == 0) {
        numThreadScaningMatrix = strtoll(value, NULL, 0);
    } else {
        status = StatusConfigInvalid;
    }
    return status;
}
