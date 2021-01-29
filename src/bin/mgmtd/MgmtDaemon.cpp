// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>
#include <sys/resource.h>
#include <new>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <libgen.h>
#include <sys/stat.h>
#include <time.h>
// Thrift Libs
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/THttpServer.h>
// Xcalar Libs
#include "XcalarApiService.h"
#include "LibApisCommon_constants.h"
#include "operators/GenericTypes.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "config/Config.h"
#include "test/SimNode.h"
#include "table/ResultSet.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "operators/DhtTypes.h"
#include "support/SupportBundle.h"
#include "util/Base64.h"
#include "stat/Statistics.h"
#include "test/FuncTests/FuncTestDriver.h"
#include "StrlFunc.h"
#include "SysStatsHelper.h"
#include "strings/String.h"
#include "session/Sessions.h"
#include "runtime/Runtime.h"
#include "xcalar/compute/localtypes/KvStore.pb.h"
#include "xcalar/compute/localtypes/App.pb.h"
#include "xcalar/compute/localtypes/memory.pb.h"
#include "xcalar/compute/localtypes/UDF.pb.h"
#include "xcalar/compute/localtypes/Cgroup.pb.h"
#include "xcalar/compute/localtypes/log.pb.h"
#include "xcalar/compute/localtypes/Query.pb.h"
#include "xcalar/compute/localtypes/Table.pb.h"
#include "xcalar/compute/localtypes/PublishedTable.pb.h"
#include "xcalar/compute/localtypes/ResultSet.pb.h"
#include "xcalar/compute/localtypes/Dataflow.pb.h"
#include "xcalar/compute/localtypes/Connectors.pb.h"
#include "xcalar/compute/localtypes/Version.pb.h"
#include "dataformat/DataFormatJson.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;

static pthread_mutex_t mgmtdLock;
static SimNodeChildren *simNodeChildren = NULL;
static char tmpDirName[256];
static const char *progName;
static bool cfgLoaded = false;
static const char *moduleName = "mgmtdaemon";
static char pathToConfigFile[XcalarApiMaxPathLen + 1];
static const char destIpDefault[MaxHostName + 1] = "localhost";
static int destPort = 18552;
static const char *destIp = destIpDefault;
static NodeId nodeId = 0;
static SysStatsHelper *sysStatsHelper = NULL;

// Thrift server metadata.
class XcalarApiServiceHandler;
shared_ptr<XcalarApiServiceHandler> thriftHandler;
shared_ptr<TProcessor> thriftProcessor;
shared_ptr<TServerTransport> thirftServerTransport;
shared_ptr<TTransportFactory> thriftTransportFactory;
shared_ptr<TProtocolFactory> thriftProtocolFactory;
shared_ptr<ThreadManager> thriftThreadManager;
shared_ptr<PosixThreadFactory> thriftThreadFactory;
TThreadPoolServer *thriftThreadPoolServer = NULL;

// Monitor Thrift server status.
pthread_t mgmtdStatusThreadtid;
sem_t mgmtdStatusThreadSem;
// Thrift server status is polled every 5 seconds.
uint64_t mgmtdStatusThreadSleepUsec = 5 * 1000 * 1000;
// Thrift server vital stats.
ThreadManager::STATE mgmtdThdMgrState;
size_t mgmtdThdMgrPendingTaskCount = 0;     // Pending tasks
size_t mgmtdThdMgrTotalTaskCount = 0;       // Total tasks outstanding
size_t mgmtdThdMgrPendingTaskCountMax = 0;  // Pending tasks high water mark
static constexpr size_t MgmtdThdMgrThreadCountMin = 32;     // Minimum workers
static constexpr size_t MgmtdThdMgrThreadCountMax = 256;    // Max workers.
size_t mgmtdThdMgrWorkerCount = MgmtdThdMgrThreadCountMin;  // Current workers
size_t mgmtdThdMgrIdleWorkerCount = MgmtdThdMgrThreadCountMin;  // Idle workers
// Avoid Hysteresis by making the increment and decrement steps different.
static constexpr size_t MgmtdThdMgrThreadCountIncStep = 7;
static constexpr size_t MgmtdThdMgrThreadCountDecStep = 3;
static constexpr const size_t MaxNumUpdates = 1000;

// Use SIGABRT received to initiate shutdown
static bool mgmtdReceivedSigAbort = false;

static Status getNumNodes(const char *userIdName,
                          const unsigned int userIdUnique,
                          unsigned *numNodes);

static void updateSessionGenericOutput(
    XcalarApiSessionGenericOutputT *sessionOutputXd,
    XcalarApiSessionGenericOutput *sessionGenericOutput);

static void
mgmtdSigAbortHandler(int sig, siginfo_t *siginfo, void *ptr)
{
    assert(sig == SIGABRT);
    mgmtdReceivedSigAbort = true;
    memBarrier();

    xSyslog(moduleName, XlogErr, "Received SIGABRT, initiate shutdown");
}

void *
mgmtdStatusThread(void *ignore)
{
    Status status = StatusOk;
    XcalarSyslogMsgLevel inputLevel;

    while (true) {
        status =
            sysSemTimedWait(&mgmtdStatusThreadSem, mgmtdStatusThreadSleepUsec);
        assert(status == StatusTimedOut || status == StatusIntr ||
               status == StatusOk);

        if (mgmtdReceivedSigAbort == true) {
            // Stop serving thrift requests from XI.
            thriftThreadPoolServer->stop();
            xSyslog(moduleName, XlogErr, "Stopping the thrift server now");
            // Set really small timeout
            thriftThreadPoolServer->setTimeout(1);
            break;
        }

        // read the new counts, which could change after timeout
        mgmtdThdMgrIdleWorkerCount = thriftThreadManager->idleWorkerCount();
        mgmtdThdMgrWorkerCount = thriftThreadManager->workerCount();

        // Need not grab any lock here, since Thrift server code is supposed to
        // be re-entrant.
        if ((mgmtdThdMgrIdleWorkerCount > MgmtdThdMgrThreadCountDecStep) &&
            (mgmtdThdMgrWorkerCount - MgmtdThdMgrThreadCountDecStep >=
             MgmtdThdMgrThreadCountMin)) {
            // Remove workers; too many sitting idle.
            thriftThreadManager->removeWorker(MgmtdThdMgrThreadCountDecStep);
        } else if ((mgmtdThdMgrIdleWorkerCount == 0) &&
                   (mgmtdThdMgrWorkerCount + MgmtdThdMgrThreadCountIncStep <=
                    MgmtdThdMgrThreadCountMax)) {
            // Add workers; pending task build up is observed here.
            thriftThreadManager->addWorker(MgmtdThdMgrThreadCountIncStep);
        }

        // Acrue stats for next iteration.
        mgmtdThdMgrIdleWorkerCount = thriftThreadManager->idleWorkerCount();
        mgmtdThdMgrPendingTaskCount = thriftThreadManager->pendingTaskCount();
        mgmtdThdMgrTotalTaskCount = thriftThreadManager->totalTaskCount();
        mgmtdThdMgrPendingTaskCountMax =
            thriftThreadManager->pendingTaskCountMax();
        mgmtdThdMgrWorkerCount = thriftThreadManager->workerCount();
        mgmtdThdMgrState = thriftThreadManager->state();

        if (mgmtdThdMgrIdleWorkerCount > 0) {
            inputLevel = XlogDebug;
        } else {
            inputLevel = XlogNote;
        }
        xSyslog(moduleName,
                inputLevel,
                "Status: State %u, Idle Worker Count %lu, Worker Count %lu,"
                " Pending Task Count %lu, Total Task Count %lu,"
                " Pending Task Count Max %lu",
                (uint32_t) mgmtdThdMgrState,
                mgmtdThdMgrIdleWorkerCount,
                mgmtdThdMgrWorkerCount,
                mgmtdThdMgrPendingTaskCount,
                mgmtdThdMgrTotalTaskCount,
                mgmtdThdMgrPendingTaskCountMax);
    }

    return NULL;
}

// This is a wrapper around strlcpy that throws an exception if the buffer
// is too small
static MustInline void
copyParamString(char *dest, const char *src, size_t size)
{
    XcalarApiException exception;
    size_t ret;
    ret = strlcpy(dest, src, size);
    if (ret >= size) {
        exception.status = StatusT::StatusParameterTooLong;
        throw exception;
    }
}

static inline void
copyDhtArgs(DhtArgsT &dhtArgsDst, DhtArgs *dhtArgsSrc)
{
    dhtArgsDst.upperBound = dhtArgsSrc->upperBound;
    dhtArgsDst.lowerBound = dhtArgsSrc->lowerBound;
}

static inline void
copyXcalarApiSingleQueryOut(XcalarApiSingleQueryT &dstQuery,
                            XcalarApiSingleQuery *srcQuery)
{
    dstQuery.singleQuery = srcQuery->singleQuery;
    dstQuery.status = (StatusT::type) srcQuery->status;
}

static inline void
copyDagNamedInput(XcalarApiNamedInputT &dst, DagTypes::NamedInput *src)
{
    dst.isTable = src->isTable;
    dst.name = src->name;
    dst.nodeId = std::to_string(src->nodeId);
}

static MustInline void
copyXcalarApiTableOut(XcalarApiTableInputT &dstTable,
                      XcalarApiTableInput *srcTable)
{
    dstTable.tableName = srcTable->tableName;
    dstTable.tableId = std::to_string(srcTable->tableId);
}

static inline void
copyXcalarApiNodeInfoOut(XcalarApiDagNodeInfoT &dstNode,
                         XcalarApiDagNodeInfo *srcNode)
{
    dstNode.name = srcNode->name;
    dstNode.dagNodeId = std::to_string(srcNode->dagNodeId);
    dstNode.state = (DgDagStateT::type) srcNode->state;
    dstNode.size = srcNode->size;
    dstNode.api = (XcalarApisT::type) srcNode->api;
    dstNode.pinned = srcNode->pinned;
}

static void
copyXcalarApiGetTableMeta(bool isTable,
                          XcalarApiGetTableMetaOutputT &dstMeta,
                          XcalarApiGetTableMetaOutput *srcMeta)
{
    dstMeta.numDatasets = srcMeta->numDatasets;
    dstMeta.numResultSets = srcMeta->numResultSets;
    dstMeta.numKeys = srcMeta->numKeys;
    dstMeta.numValues = srcMeta->numValues;
    dstMeta.numImmediates = srcMeta->numImmediates;
    dstMeta.ordering = (XcalarOrderingT::type) srcMeta->ordering;
    dstMeta.numMetas = srcMeta->numMetas;
    dstMeta.xdbId = std::to_string(srcMeta->xdbId);

    int64_t ii, jj;
    for (ii = 0; ii < dstMeta.numKeys; ii++) {
        DfFieldAttrHeaderT keyAttr;

        keyAttr.name = srcMeta->keyAttr[ii].name;
        keyAttr.type = (DfFieldTypeT::type) srcMeta->keyAttr[ii].type;
        keyAttr.valueArrayIndex = srcMeta->keyAttr[ii].valueArrayIndex;
        keyAttr.ordering = strGetFromOrdering(srcMeta->keyAttr[ii].ordering);

        dstMeta.keyAttr.push_back(keyAttr);
    }

    for (ii = 0; ii < dstMeta.numDatasets; ii++) {
        dstMeta.datasets.push_back(srcMeta->datasets[ii]);
    }

    for (ii = 0; ii < dstMeta.numResultSets; ii++) {
        dstMeta.resultSetIds.push_back(
            std::to_string(srcMeta->resultSetIds[ii]));
    }

    for (ii = 0; ii < dstMeta.numValues; ii++) {
        DfFieldAttrHeaderT attrOut;
        attrOut.name = srcMeta->valueAttrs[ii].name;
        attrOut.type = (DfFieldTypeT::type) srcMeta->valueAttrs[ii].type;
        attrOut.valueArrayIndex = ii;
        dstMeta.valueAttrs.push_back(attrOut);
    }

    for (ii = 0; ii < dstMeta.numMetas; ii++) {
        XcalarApiTableMetaT metaOut;
        XcalarApiTableMeta *metaIn;

        metaIn = &srcMeta->metas[ii];
        metaOut.numRows = metaIn->numRows;
        metaOut.numPages = metaIn->numPages;
        metaOut.numSlots = metaIn->numSlots;
        metaOut.size = metaIn->size;
        assert(isTable || metaIn->numSlots == 0);

        for (jj = 0; jj < metaOut.numSlots; jj++) {
            metaOut.numRowsPerSlot.push_back(metaIn->numRowsPerSlot[jj]);
            metaOut.numPagesPerSlot.push_back(metaIn->numPagesPerSlot[jj]);
        }
        metaOut.numTransPageSent = metaIn->numTransPageSent;
        metaOut.numTransPageRecv = metaIn->numTransPageRecv;
        metaOut.xdbPageConsumedInBytes = metaIn->xdbPageConsumedInBytes;
        metaOut.xdbPageAllocatedInBytes = metaIn->xdbPageAllocatedInBytes;

        dstMeta.metas.push_back(metaOut);
    }
}

static MustInline void
copyXcalarApiDfLoadArgs(XcalarApiDfLoadArgsT &loadArgsDst,
                        DfLoadArgs *loadArgsSrc)
{
    for (int ii = 0; ii < loadArgsSrc->sourceArgsListCount; ii++) {
        DataSourceArgsT sourceArgs;
        sourceArgs.targetName = loadArgsSrc->sourceArgsList[ii].targetName;
        sourceArgs.path = loadArgsSrc->sourceArgsList[ii].path;
        sourceArgs.recursive = loadArgsSrc->sourceArgsList[ii].recursive;
        sourceArgs.fileNamePattern =
            loadArgsSrc->sourceArgsList[ii].fileNamePattern;
        loadArgsDst.sourceArgsList.push_back(sourceArgs);
    }

    loadArgsDst.parseArgs.parserFnName = loadArgsSrc->parseArgs.parserFnName;
    loadArgsDst.parseArgs.parserArgJson = loadArgsSrc->parseArgs.parserArgJson;
    loadArgsDst.parseArgs.fileNameFieldName =
        loadArgsSrc->parseArgs.fileNameFieldName;
    loadArgsDst.parseArgs.recordNumFieldName =
        loadArgsSrc->parseArgs.recordNumFieldName;
    loadArgsDst.parseArgs.allowRecordErrors =
        loadArgsSrc->parseArgs.allowRecordErrors;
    loadArgsDst.parseArgs.allowFileErrors =
        loadArgsSrc->parseArgs.allowFileErrors;

    loadArgsDst.size = loadArgsSrc->maxSize;

    for (unsigned ii = 0; ii < loadArgsSrc->parseArgs.fieldNamesCount; ii++) {
        XcalarApiColumnT col;

        col.sourceColumn = loadArgsSrc->parseArgs.oldNames[ii];
        col.destColumn = loadArgsSrc->parseArgs.fieldNames[ii];
        col.columnType =
            strGetFromDfFieldType(loadArgsSrc->parseArgs.types[ii]);

        loadArgsDst.parseArgs.schema.push_back(col);
    }
}

static inline void
copyXcalarApiDatasetOut(XcalarApiDatasetT &dstDataset,
                        XcalarApiDataset *srcDataset)
{
    copyXcalarApiDfLoadArgs(dstDataset.loadArgs, &srcDataset->loadArgs);
    dstDataset.datasetId = std::to_string(srcDataset->datasetId);
    dstDataset.name = srcDataset->name;
    dstDataset.loadIsComplete = srcDataset->loadIsComplete;
    dstDataset.isListable = srcDataset->isListable;
}

static inline void
copyXcalarApiGetDatasetsInfoOut(XcalarApiDatasetsInfoT &dstDatasetsInfo,
                                XcalarApiDatasetsInfo *srcDatasetsInfo)
{
    dstDatasetsInfo.datasetName = srcDatasetsInfo->datasetName;
    dstDatasetsInfo.downSampled = srcDatasetsInfo->downSampled;
    dstDatasetsInfo.totalNumErrors = srcDatasetsInfo->totalNumErrors;
    dstDatasetsInfo.datasetSize = srcDatasetsInfo->datasetSize;
    dstDatasetsInfo.numColumns = srcDatasetsInfo->numColumns;
    for (unsigned ii = 0; ii < dstDatasetsInfo.numColumns; ii++) {
        XcalarApiColumnInfoT col;
        col.name = srcDatasetsInfo->columns[ii].name;
        col.type = strGetFromDfFieldType(srcDatasetsInfo->columns[ii].type);
        dstDatasetsInfo.columns.push_back(col);
    }
}

// For a dataset, copy info for a user
static inline void
copyXcalarApiDatasetUserOut(XcalarApiDatasetUserT &dstDatasetUser,
                            XcalarApiDatasetUser *srcDatasetUser)
{
    dstDatasetUser.userId.userIdName = srcDatasetUser->userId.userIdName;
    dstDatasetUser.referenceCount = srcDatasetUser->referenceCount;
}

// For a user, copy info for a dataset
static inline void
copyXcalarApiUserDatasetsOut(XcalarApiUserDatasetT &dstUserDataset,
                             XcalarApiUserDataset *srcUserDataset)
{
    dstUserDataset.datasetName = srcUserDataset->datasetName;
    dstUserDataset.isLocked = srcUserDataset->isLocked;
}

static inline void
copyXcalarEvalFnDescOut(XcalarEvalFnDescT &fnDescOut,
                        XcalarEvalFnDesc *fnDescIn)
{
    int ii;
    int numArgs;
    fnDescOut.fnName = fnDescIn->fnName;
    fnDescOut.fnDesc = fnDescIn->fnDesc;
    fnDescOut.category = (FunctionCategoryT::type) fnDescIn->category;
    fnDescOut.numArgs = fnDescIn->numArgs;
    fnDescOut.outputType = (DfFieldTypeT::type) fnDescIn->outputType;
    numArgs = fnDescIn->numArgs;
    numArgs = (numArgs < 0) ? numArgs * -1 : numArgs;
    for (ii = 0; ii < numArgs; ii++) {
        XcalarEvalArgDescT argDesc;
        argDesc.argDesc = fnDescIn->argDescs[ii].argDesc;
        argDesc.typesAccepted = fnDescIn->argDescs[ii].typesAccepted;
        argDesc.isSingletonValue = fnDescIn->argDescs[ii].isSingletonValue;
        argDesc.argType =
            (XcalarEvalArgTypeT::type) fnDescIn->argDescs[ii].argType;

        switch (fnDescIn->argDescs[ii].argType) {
        case RequiredArg:
            argDesc.minArgs = 1;
            argDesc.maxArgs = 1;
            break;
        case OptionalArg:
            argDesc.minArgs = 0;
            argDesc.maxArgs = 1;
            break;
        case VariableArg:
            argDesc.minArgs = fnDescIn->argDescs[ii].minArgs;
            argDesc.maxArgs = fnDescIn->argDescs[ii].maxArgs;
            break;
        default:
            assert(0);
        }

        fnDescOut.argDescs.push_back(argDesc);
    }
}

static inline void
copyXcalarApiSessionOut(XcalarApiSessionT &sessionOut,
                        XcalarApiSession *sessionIn)
{
    char sessionIdStr[SessionMgr::MaxTextHexIdSize];

    sessionOut.name = sessionIn->name;
    sessionOut.description = sessionIn->description;
    sessionOut.state = sessionIn->state;
    sessionOut.info = sessionIn->info;
    sessionOut.activeNode = sessionIn->activeNode;
    snprintf(sessionIdStr, sizeof(sessionIdStr), "%lX", sessionIn->sessionId);
    sessionOut.sessionId = sessionIdStr;
}

static StatusT::type
copyExportMetaInput(const ExExportMetaT *metaIn,
                    ExExportTargetHdr &target,
                    ExInitExportSpecificInput &specInput)
{
    // Setup target
    target.type = (ExTargetType) metaIn->target.type;
    copyParamString(target.name,
                    metaIn->target.name.c_str(),
                    sizeof(target.name));
    // Copy in specificInput for each export target type
    switch (target.type) {
    case ExTargetSFType:
        specInput.sfInput.format =
            (DfFormatType) metaIn->specificInput.sfInput.format;
        copyParamString(specInput.sfInput.fileName,
                        metaIn->specificInput.sfInput.fileName.c_str(),
                        sizeof(specInput.sfInput.fileName));

        specInput.sfInput.headerType =
            (ExSFHeaderType) metaIn->specificInput.sfInput.headerType;

        specInput.sfInput.splitRule.type =
            (ExSFFileSplitType) metaIn->specificInput.sfInput.splitRule.type;
        switch (specInput.sfInput.splitRule.type) {
        case ExSFFileSplitNone:
            specInput.sfInput.splitRule.spec.maxSize = 0xda7aba5e;
            break;
        case ExSFFileSplitSize:
            specInput.sfInput.splitRule.spec.maxSize =
                metaIn->specificInput.sfInput.splitRule.spec.maxSize;
            break;
        case ExSFFileSplitForceSingle:
            specInput.sfInput.splitRule.spec.maxSize = 0xda7aba5e;
            break;
        default:
            return StatusT::StatusUnimpl;
        }
        switch (specInput.sfInput.format) {
        case DfFormatCsv:
            specInput.sfInput.formatArgs.csv.fieldDelim =
                metaIn->specificInput.sfInput.formatArgs.csv.fieldDelim.at(0);
            specInput.sfInput.formatArgs.csv.recordDelim =
                metaIn->specificInput.sfInput.formatArgs.csv.recordDelim.at(0);
            specInput.sfInput.formatArgs.csv.quoteDelim =
                metaIn->specificInput.sfInput.formatArgs.csv.quoteDelim.at(0);
            break;
        case DfFormatSql:
            copyParamString(specInput.sfInput.formatArgs.sql.tableName,
                            metaIn->specificInput.sfInput.formatArgs.sql
                                .tableName.c_str(),
                            sizeof(specInput.sfInput.formatArgs.sql.tableName));
            specInput.sfInput.formatArgs.sql.createTable =
                metaIn->specificInput.sfInput.formatArgs.sql.createTable;
            specInput.sfInput.formatArgs.sql.dropTable =
                metaIn->specificInput.sfInput.formatArgs.sql.dropTable;
            break;
        case DfFormatJson:
            specInput.sfInput.formatArgs.json.array =
                metaIn->specificInput.sfInput.formatArgs.json.array;
            break;
        default:
            return StatusT::StatusUnimpl;
        }
        break;  // ExTargetSFType
        // XXX - Dedupe with SFType
    case ExTargetUDFType:
        specInput.udfInput.format =
            (DfFormatType) metaIn->specificInput.udfInput.format;
        copyParamString(specInput.udfInput.fileName,
                        metaIn->specificInput.udfInput.fileName.c_str(),
                        sizeof(specInput.udfInput.fileName));
        specInput.udfInput.headerType =
            (ExSFHeaderType) metaIn->specificInput.udfInput.headerType;

        switch (specInput.udfInput.format) {
        case DfFormatCsv:
            specInput.udfInput.formatArgs.csv.fieldDelim =
                metaIn->specificInput.udfInput.formatArgs.csv.fieldDelim.at(0);
            specInput.udfInput.formatArgs.csv.recordDelim =
                metaIn->specificInput.udfInput.formatArgs.csv.recordDelim.at(0);
            specInput.udfInput.formatArgs.csv.quoteDelim =
                metaIn->specificInput.udfInput.formatArgs.csv.quoteDelim.at(0);
            break;
        case DfFormatSql:
            copyParamString(specInput.udfInput.formatArgs.sql.tableName,
                            metaIn->specificInput.udfInput.formatArgs.sql
                                .tableName.c_str(),
                            sizeof(
                                specInput.udfInput.formatArgs.sql.tableName));
            specInput.udfInput.formatArgs.sql.createTable =
                metaIn->specificInput.udfInput.formatArgs.sql.createTable;
            specInput.udfInput.formatArgs.sql.dropTable =
                metaIn->specificInput.udfInput.formatArgs.sql.dropTable;
            break;
        case DfFormatJson:
            specInput.udfInput.formatArgs.json.array =
                metaIn->specificInput.udfInput.formatArgs.json.array;
            break;
        default:
            return StatusT::StatusUnimpl;
        }
        break;  // ExTargetUDFType
    default:
        return StatusT::StatusUnimpl;
        break;
    }

    return StatusT::StatusOk;
}

static void
copyXcalarApiTime(XcalarApiTimeT &timeOut, XcalarApiTime *timeIn)
{
    timeOut.milliseconds = timeIn->milliseconds;
}

static void
copyXcalarApiDagNodeOut(XcalarApiDagNodeT &dstNode, XcalarApiDagNode *srcNode)
{
    XcalarApiException exception;
    dstNode.api = (XcalarApisT::type) srcNode->hdr.api;
    dstNode.dagNodeId = std::to_string(srcNode->hdr.dagNodeId);
    dstNode.name.name = srcNode->hdr.name;
    dstNode.tag = srcNode->hdr.tag;
    dstNode.comment = srcNode->hdr.comment;
    dstNode.state = (DgDagStateT::type) srcNode->hdr.state;

    dstNode.numWorkCompleted = srcNode->localData.numWorkCompleted;
    dstNode.numWorkTotal = srcNode->localData.numWorkTotal;
    dstNode.numNodes = srcNode->localData.numNodes;
    dstNode.numRowsTotal = srcNode->localData.numRowsTotal;
    dstNode.sizeTotal = srcNode->localData.sizeTotal;
    dstNode.xdbBytesRequired = srcNode->localData.xdbBytesRequired;
    dstNode.xdbBytesConsumed = srcNode->localData.xdbBytesConsumed;

    for (unsigned ii = 0; ii < (unsigned) dstNode.numNodes; ii++) {
        dstNode.sizePerNode.push_back(srcNode->localData.sizePerNode[ii]);
        uint64_t curr_rcvd = 0;

        dstNode.numRowsPerNode.push_back(srcNode->localData.numRowsPerNode[ii]);
        curr_rcvd = srcNode->localData.numTransPagesReceivedPerNode[ii];
        dstNode.numTransPagesReceivedPerNode.push_back(curr_rcvd);
        dstNode.numTransPageRecv += curr_rcvd;
        dstNode.hashSlotSkewPerNode.push_back(
            srcNode->localData.hashSlotSkewPerNode[ii]);
    }

    dstNode.opFailureInfo.numRowsFailedTotal =
        srcNode->localData.opFailureInfo.numRowsFailedTotal;

    // Even though numRowsFailedTotal may be 0, need to copy the break-down
    // fields, even if they'd be all NULL, since we can't leave junk in the
    // thrift string (fdesc.failureDesc) - otherwise, python wrappers to copy
    // from the thrift structs fall over in the auto-generated code for thrift
    // (see LibApisCommon/ttypes.py in buildOut dir)
    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        FailureSummaryT fSumm;
        for (int jj = 0; jj < XcalarApiMaxFailures; jj++) {
            FailureDescT fdesc;
            fdesc.numRowsFailed =
                srcNode->localData.opFailureInfo.opFailureSummary[ii]
                    .failureSummInfo[jj]
                    .numRowsFailed;
            fdesc.failureDesc =
                srcNode->localData.opFailureInfo.opFailureSummary[ii]
                    .failureSummInfo[jj]
                    .failureDesc;
            fSumm.failureSummInfo.push_back(fdesc);
        }
        fSumm.failureSummName =
            srcNode->localData.opFailureInfo.opFailureSummary[ii]
                .failureSummName;
        dstNode.opFailureInfo.opFailureSummary.push_back(fSumm);
    }

    dstNode.numParents = srcNode->numParents;
    dstNode.numChildren = srcNode->numChildren;
    dstNode.log = srcNode->log;
    dstNode.status = (StatusT::type) srcNode->status;
    dstNode.pinned = srcNode->hdr.pinned;
    dstNode.startTime = srcNode->operatorStartTime;
    dstNode.endTime = srcNode->operatorEndTime;

    // following fields are obsolete but can't be removed due to thrift
    // constraints. Just clear them
    dstNode.numTransPageSent = 0;  // obsolete

    for (unsigned ii = 0; ii < srcNode->numParents; ii++) {
        dstNode.parents.push_back(std::to_string(srcNode->parents[ii]));
    }

    for (unsigned ii = 0; ii < srcNode->numChildren; ii++) {
        dstNode.children.push_back(std::to_string(srcNode->children[ii]));
    }

    copyXcalarApiTime(dstNode.elapsed, &srcNode->elapsed);

    dstNode.inputSize = srcNode->hdr.inputSize;

    switch ((XcalarApis) dstNode.api) {
    case XcalarApiBulkLoad: {
        dstNode.input.loadInput.dest = srcNode->input->loadInput.datasetName;
        copyXcalarApiDfLoadArgs(dstNode.input.loadInput.loadArgs,
                                &srcNode->input->loadInput.loadArgs);
        dstNode.input.loadInput.dagNodeId =
            std::to_string(srcNode->input->loadInput.dagNodeId);
        dstNode.input.__isset.loadInput = true;
        break;
    }
    case XcalarApiIndex: {
        XcalarApiIndexInput *input = &srcNode->input->indexInput;

        dstNode.input.indexInput.source = input->source.name;
        dstNode.input.indexInput.dest = input->dstTable.tableName;

        for (unsigned ii = 0; ii < input->numKeys; ii++) {
            XcalarApiKeyT key;
            key.name = input->keys[ii].keyName;
            key.type = strGetFromDfFieldType(input->keys[ii].type);
            key.keyFieldName = input->keys[ii].keyFieldName;
            key.ordering = strGetFromOrdering(input->keys[ii].ordering);

            dstNode.input.indexInput.key.push_back(key);
        }

        dstNode.input.indexInput.dhtName = input->dhtName;
        dstNode.input.indexInput.prefix = input->fatptrPrefixName;
        dstNode.input.indexInput.delaySort = input->delaySort;
        dstNode.input.indexInput.broadcast = input->broadcast;
        dstNode.input.__isset.indexInput = true;
        break;
    }
    case XcalarApiJoin: {
        dstNode.input.joinInput.source.push_back(
            srcNode->input->joinInput.leftTable.tableName);
        dstNode.input.joinInput.source.push_back(
            srcNode->input->joinInput.rightTable.tableName);
        dstNode.input.joinInput.dest =
            srcNode->input->joinInput.joinTable.tableName;
        dstNode.input.joinInput.evalString =
            srcNode->input->joinInput.filterString;

        dstNode.input.joinInput.joinType =
            strGetFromJoinOperator(srcNode->input->joinInput.joinType);
        dstNode.input.joinInput.columns.push_back(
            std::vector<XcalarApiColumnT>(0));
        dstNode.input.joinInput.columns.push_back(
            std::vector<XcalarApiColumnT>(0));

        for (unsigned ii = 0; ii < (srcNode->input->joinInput.numLeftColumns +
                                    srcNode->input->joinInput.numRightColumns);
             ii++) {
            XcalarApiColumnT tmp;
            tmp.__set_sourceColumn(
                srcNode->input->joinInput.renameMap[ii].oldName);
            tmp.__set_destColumn(
                srcNode->input->joinInput.renameMap[ii].newName);
            tmp.__set_columnType(strGetFromDfFieldType(
                srcNode->input->joinInput.renameMap[ii].type));

            if (ii < srcNode->input->joinInput.numLeftColumns) {
                dstNode.input.joinInput.columns[0].push_back(tmp);
            } else {
                dstNode.input.joinInput.columns[1].push_back(tmp);
            }
        }
        dstNode.input.__isset.joinInput = true;
        break;
    }
    case XcalarApiUnion: {
        xcalarApiDeserializeUnionInput(&srcNode->input->unionInput);
        for (unsigned ii = 0; ii < srcNode->input->unionInput.numSrcTables;
             ii++) {
            dstNode.input.unionInput.source.push_back(
                srcNode->input->unionInput.srcTables[ii].tableName);
        }

        dstNode.input.unionInput.dest =
            srcNode->input->unionInput.dstTable.tableName;
        dstNode.input.unionInput.dedup = srcNode->input->unionInput.dedup;
        dstNode.input.unionInput.unionType =
            strGetFromUnionOperator(srcNode->input->unionInput.unionType);
        for (unsigned ii = 0; ii < (srcNode->input->unionInput.numSrcTables);
             ii++) {
            dstNode.input.unionInput.columns.push_back(
                std::vector<XcalarApiColumnT>(0));

            for (unsigned jj = 0;
                 jj < srcNode->input->unionInput.renameMapSizes[ii];
                 jj++) {
                XcalarApiColumnT tmp;
                tmp.__set_sourceColumn(
                    srcNode->input->unionInput.renameMap[ii][jj].oldName);
                tmp.__set_destColumn(
                    srcNode->input->unionInput.renameMap[ii][jj].newName);
                tmp.__set_columnType(strGetFromDfFieldType(
                    srcNode->input->unionInput.renameMap[ii][jj].type));

                dstNode.input.unionInput.columns[ii].push_back(tmp);
            }
        }
        dstNode.input.__isset.unionInput = true;
        break;
    }
    case XcalarApiProject:
        for (unsigned ii = 0; ii < srcNode->input->projectInput.numColumns;
             ii++) {
            dstNode.input.projectInput.columns.push_back(
                srcNode->input->projectInput.columnNames[ii]);
        }
        dstNode.input.projectInput.source =
            srcNode->input->projectInput.srcTable.tableName;
        dstNode.input.projectInput.dest =
            srcNode->input->projectInput.dstTable.tableName;
        dstNode.input.__isset.projectInput = true;
        break;

    case XcalarApiFilter: {
        XcalarApiEvalT eval;
        eval.evalString = srcNode->input->filterInput.filterStr;

        dstNode.input.filterInput.eval.push_back(eval);
        dstNode.input.filterInput.source =
            srcNode->input->filterInput.srcTable.tableName;
        dstNode.input.filterInput.dest =
            srcNode->input->filterInput.dstTable.tableName;
        dstNode.input.__isset.filterInput = true;
        break;
    }
    case XcalarApiGroupBy:
        xcalarApiDeserializeGroupByInput(&srcNode->input->groupByInput);
        dstNode.input.groupByInput.source =
            srcNode->input->groupByInput.srcTable.tableName;
        dstNode.input.groupByInput.dest =
            srcNode->input->groupByInput.dstTable.tableName;

        dstNode.input.groupByInput.includeSample =
            srcNode->input->groupByInput.includeSrcTableSample;
        dstNode.input.groupByInput.groupAll =
            srcNode->input->groupByInput.groupAll;

        dstNode.input.groupByInput.icv = srcNode->input->groupByInput.icvMode;
        dstNode.input.groupByInput.newKeyField =
            srcNode->input->groupByInput.keys[0].keyFieldName;
        for (unsigned ii = 0; ii < srcNode->input->groupByInput.numEvals;
             ii++) {
            XcalarApiEvalT eval;
            eval.evalString = srcNode->input->groupByInput.evalStrs[ii];
            eval.newField = srcNode->input->groupByInput.newFieldNames[ii];

            dstNode.input.groupByInput.eval.push_back(eval);
        }
        dstNode.input.__isset.groupByInput = true;
        break;

    case XcalarApiSynthesize:
        dstNode.input.synthesizeInput.source =
            srcNode->input->synthesizeInput.source.name;
        dstNode.input.synthesizeInput.dest =
            srcNode->input->synthesizeInput.dstTable.tableName;

        for (unsigned ii = 0; ii < srcNode->input->synthesizeInput.columnsCount;
             ii++) {
            XcalarApiColumnT col;
            XcalarApiRenameMap *colIn =
                &srcNode->input->synthesizeInput.columns[ii];

            col.sourceColumn = colIn->oldName;
            col.destColumn = colIn->newName;
            col.columnType = strGetFromDfFieldType(colIn->type);

            dstNode.input.synthesizeInput.columns.push_back(col);
        }
        dstNode.input.__isset.synthesizeInput = true;
        break;

    case XcalarApiSelect:
        dstNode.input.selectInput.source =
            srcNode->input->selectInput.srcTable.tableName;
        dstNode.input.selectInput.dest =
            srcNode->input->selectInput.dstTable.tableName;
        dstNode.input.selectInput.minBatchId =
            srcNode->input->selectInput.minBatchId;
        dstNode.input.selectInput.maxBatchId =
            srcNode->input->selectInput.maxBatchId;

        dstNode.input.__isset.selectInput = true;
        break;

    case XcalarApiMap:
        xcalarApiDeserializeMapInput(&srcNode->input->mapInput);

        dstNode.input.mapInput.source =
            srcNode->input->mapInput.srcTable.tableName;
        dstNode.input.mapInput.dest =
            srcNode->input->mapInput.dstTable.tableName;

        dstNode.input.mapInput.icv = srcNode->input->mapInput.icvMode;
        for (unsigned ii = 0; ii < srcNode->input->mapInput.numEvals; ii++) {
            XcalarApiEvalT eval;
            eval.evalString = srcNode->input->mapInput.evalStrs[ii];
            eval.newField = srcNode->input->mapInput.newFieldNames[ii];

            dstNode.input.mapInput.eval.push_back(eval);
        }
        dstNode.input.__isset.mapInput = true;
        break;

    case XcalarApiGetRowNum:
        dstNode.input.getRowNumInput.source =
            srcNode->input->getRowNumInput.srcTable.tableName;
        dstNode.input.getRowNumInput.dest =
            srcNode->input->getRowNumInput.dstTable.tableName;

        dstNode.input.getRowNumInput.newField =
            srcNode->input->getRowNumInput.newFieldName;
        dstNode.input.__isset.getRowNumInput = true;
        break;

    case XcalarApiCreateDht:
        dstNode.input.createDhtInput.dhtName =
            srcNode->input->createDhtInput.dhtName;
        copyDhtArgs(dstNode.input.createDhtInput.dhtArgs,
                    &srcNode->input->createDhtInput.dhtArgs);
        dstNode.input.__isset.createDhtInput = true;
        break;

    case XcalarApiAggregate: {
        XcalarApiEvalT eval;
        eval.evalString = srcNode->input->aggregateInput.evalStr;

        dstNode.input.aggregateInput.eval.push_back(eval);
        dstNode.input.aggregateInput.source =
            srcNode->input->aggregateInput.srcTable.tableName;
        dstNode.input.aggregateInput.dest =
            srcNode->input->aggregateInput.dstTable.tableName;
        dstNode.input.__isset.aggregateInput = true;
        break;
    }
    case XcalarApiExport: {
        XcalarApiExportInput *exInput = &srcNode->input->exportInput;

        dstNode.input.exportInput.source =
            srcNode->input->exportInput.srcTable.tableName;
        dstNode.input.exportInput.dest = srcNode->input->exportInput.exportName;
        dstNode.input.exportInput.driverName =
            srcNode->input->exportInput.meta.driverName;
        dstNode.input.exportInput.driverParams =
            srcNode->input->exportInput.meta.driverParams;

        for (int ii = 0; ii < exInput->meta.numColumns; ii++) {
            XcalarApiExportColumnT col;
            col.columnName = exInput->meta.columns[ii].name;
            col.headerName = exInput->meta.columns[ii].headerAlias;
            dstNode.input.exportInput.columns.push_back(col);
        }

        dstNode.input.__isset.exportInput = true;
        break;
    }

    case XcalarApiDeleteObjects:
        dstNode.input.deleteDagNodeInput.srcType =
            (SourceTypeT::type) srcNode->input->deleteDagNodesInput.srcType;
        dstNode.input.deleteDagNodeInput.namePattern =
            srcNode->input->deleteDagNodesInput.namePattern;
        dstNode.input.deleteDagNodeInput.deleteCompletely =
            srcNode->input->deleteDagNodesInput.deleteCompletely;
        dstNode.input.__isset.deleteDagNodeInput = true;
        break;

    default:
        assert(0);
        exception.status = StatusT::StatusUnimpl;
        throw exception;
    }
}

static void
copyXcalarApiOpDetails(XcalarApiOpDetailsT &dstOpDetails,
                       XcalarApiOpDetails *srcOpDetails,
                       XcalarApis api)
{
    XcalarApiException exception;

    dstOpDetails.numWorkCompleted = srcOpDetails->numWorkCompleted;
    dstOpDetails.numWorkTotal = srcOpDetails->numWorkTotal;
    dstOpDetails.numRowsTotal = srcOpDetails->numRowsTotal;
    dstOpDetails.cancelled = srcOpDetails->cancelled;

    switch (api) {
    case XcalarApiBulkLoad: {
        dstOpDetails.errorStats.loadErrorStats.numFileOpenFailure =
            srcOpDetails->errorStats.loadErrorStats.numFileOpenFailure;
        dstOpDetails.errorStats.loadErrorStats.numDirOpenFailure =
            srcOpDetails->errorStats.loadErrorStats.numDirOpenFailure;
        dstOpDetails.errorStats.__isset.loadErrorStats = true;
        break;
    }
    case XcalarApiIndex: {
        dstOpDetails.errorStats.indexErrorStats.numParseError =
            srcOpDetails->errorStats.indexErrorStats.numParseError;

        dstOpDetails.errorStats.indexErrorStats.numFieldNoExist =
            srcOpDetails->errorStats.indexErrorStats.numFieldNoExist;

        dstOpDetails.errorStats.indexErrorStats.numTypeMismatch =
            srcOpDetails->errorStats.indexErrorStats.numTypeMismatch;

        dstOpDetails.errorStats.indexErrorStats.numOtherError =
            srcOpDetails->errorStats.indexErrorStats.numOtherError;
        dstOpDetails.errorStats.__isset.indexErrorStats = true;
        break;
    }
    case XcalarApiMap: {
        // in multi-eval case, if there are some XDF evals, and some UDF evals,
        // and some XDFs fail and some UDFs fail, then total no. of failed rows
        // == numEvalUdfError + OpEvalXdfErrorStats.numTotal;

        OpEvalUdfErrorStats *srcUdfErr =
            &srcOpDetails->errorStats.evalErrorStats.evalUdfErrorStats;

        EvalUdfErrorStatsT *dstUdfErr =
            &dstOpDetails.errorStats.evalErrorStats.evalUdfErrorStats;

        dstUdfErr->numEvalUdfError = srcUdfErr->numEvalUdfError;

        // Even though numEvalUdfError may be 0, need to copy the break-down
        // fields, even if they'd be all NULL, since we can't leave junk in the
        // thrift string (fdesc.failureDesc) - otherwise, python wrappers to
        // copy from the thrift structs fall over in the auto-generated code for
        // thrift (see LibApisCommon/ttypes.py in buildOut dir)
        for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
            FailureSummaryT fSumm;
            for (int jj = 0; jj < XcalarApiMaxFailures; jj++) {
                FailureDescT fdesc;
                fdesc.numRowsFailed = srcUdfErr->opFailureSummary[ii]
                                          .failureSummInfo[jj]
                                          .numRowsFailed;
                fdesc.failureDesc = srcUdfErr->opFailureSummary[ii]
                                        .failureSummInfo[jj]
                                        .failureDesc;
                fSumm.failureSummInfo.push_back(fdesc);
            }
            fSumm.failureSummName =
                srcUdfErr->opFailureSummary[ii].failureSummName;
            dstUdfErr->opFailureSummary.push_back(fSumm);
        }
        // fall through to update XDF related errors, if any
    }
    case XcalarApiFilter:
        // XXX: even filter may have evalUdfErrorStats? If so, add UDF errs here
    case XcalarApiGroupBy:
    case XcalarApiAggregate: {
        dstOpDetails.errorStats.evalErrorStats.evalXdfErrorStats.numTotal =
            srcOpDetails->errorStats.evalErrorStats.evalXdfErrorStats.numTotal;

        dstOpDetails.errorStats.evalErrorStats.evalXdfErrorStats
            .numUnsubstituted = srcOpDetails->errorStats.evalErrorStats
                                    .evalXdfErrorStats.numUnsubstituted;

        dstOpDetails.errorStats.evalErrorStats.evalXdfErrorStats
            .numUnspportedTypes = srcOpDetails->errorStats.evalErrorStats
                                      .evalXdfErrorStats.numUnspportedTypes;

        dstOpDetails.errorStats.evalErrorStats.evalXdfErrorStats
            .numMixedTypeNotSupported =
            srcOpDetails->errorStats.evalErrorStats.evalXdfErrorStats
                .numMixedTypeNotSupported;
        dstOpDetails.errorStats.evalErrorStats.evalXdfErrorStats
            .numEvalCastError = srcOpDetails->errorStats.evalErrorStats
                                    .evalXdfErrorStats.numEvalCastError;
        dstOpDetails.errorStats.evalErrorStats.evalXdfErrorStats.numMiscError =
            srcOpDetails->errorStats.evalErrorStats.evalXdfErrorStats
                .numMiscError;
        dstOpDetails.errorStats.__isset.evalErrorStats = true;
        break;
    }
    default:
        // no-op
        break;
    }
}

static void
copyXcalarApiUdfAddUpdateOutput(
    XcalarApiUdfAddUpdateOutputT &udfAddUpdateOutputOut,
    XcalarApiUdfAddUpdateOutput *udfAddUpdateOutputIn)
{
    udfAddUpdateOutputOut.status = (StatusT::type) udfAddUpdateOutputIn->status;

    udfAddUpdateOutputOut.moduleName = udfAddUpdateOutputIn->moduleName;

    if (udfAddUpdateOutputIn->error.messageSize > 0) {
        udfAddUpdateOutputOut.error.message = udfAddUpdateOutputIn->error.data;
    } else {
        udfAddUpdateOutputOut.error.message = "";
    }

    if (udfAddUpdateOutputIn->error.tracebackSize > 0) {
        udfAddUpdateOutputOut.error.traceback =
            udfAddUpdateOutputIn->error.data +
            udfAddUpdateOutputIn->error.messageSize;
    } else {
        udfAddUpdateOutputOut.error.traceback = "";
    }
}

static void
copyXcalarApiConfigParamOut(XcalarApiConfigParamT &dstParam,
                            XcalarApiConfigParam *srcParam)
{
    dstParam.paramName = srcParam->paramName;
    dstParam.paramValue = srcParam->paramValue;
    dstParam.visible = srcParam->visible;
    dstParam.changeable = srcParam->changeable;
    dstParam.restartRequired = srcParam->restartRequired;
    dstParam.defaultValue = srcParam->defaultValue;
}

static void
reapNodesInt(const char *userIdName, const unsigned int userIdUnique)
{
    int ret;
    char errMsg[512] = "\0";
    char const *errMsgPtr;
    unsigned numNodes = 0;

    if (simNodeChildren != NULL) {
        Status status = getNumNodes(userIdName, userIdUnique, &numNodes);
        // this code path is entered only via mgmtd tests, which will
        // be modified soon to not invoke this
        assert(status == StatusOk);
        assert(numNodes > 0);

        simNodeReapNodes(simNodeChildren, numNodes);
        simNodeChildren = NULL;

        simNodeDeleteOutput(tmpDirName, numNodes);
        ret = rmdir(tmpDirName);
        if (ret != 0) {
            errMsgPtr = strerror_r(errno, errMsg, sizeof(errMsg));
            if (errMsgPtr == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error deleting temporary directory.");
                xSyslog(moduleName, XlogErr, "Return code was: %d", ret);
                xSyslog(moduleName, XlogErr, "Error message retrieval failed");
            } else {
                xSyslog(moduleName,
                        XlogErr,
                        "Error deleting temporary directory: %s",
                        errMsgPtr);
            }
        }
        numNodes = 0;
    }
}

static void
reapNodes(const char *userIdName, const unsigned int userIdUnique)
{
    // @SymbolCheckIgnore
    pthread_mutex_lock(&mgmtdLock);

    reapNodesInt(userIdName, userIdUnique);

    // @SymbolCheckIgnore
    pthread_mutex_unlock(&mgmtdLock);
}

// XXX If routing to other than node 0 is implemented, all API reqests for
//     a given session *must* go to the same node.
static void
apiRoutePolicy(NodeId *nodeIdInOut,
               const char **destIp,
               bool useLocalhost,
               int *destPort)
{
    assert(cfgLoaded);
    Config *config = Config::get();

    // Get the local node for this host
    NodeId nodeId = config->getLowestNodeIdOnThisHost();

    if (useLocalhost) {
        *destIp = destIpDefault;
    } else {
        *destIp = config->getIpAddr(nodeId);
    }
    *destPort = config->getApiPort(nodeId);
    *nodeIdInOut = nodeId;
}

Status
getNumNodes(const char *userIdName,
            const unsigned int userIdUnique,
            unsigned *numNodes)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem;

    assert(cfgLoaded == true);
    assert(numNodes != NULL);
    *numNodes = 0;

    workItem = xcalarApiMakeGetNumNodesWorkItem();
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Unable to allocate workItem for (%s) failed: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp,
                                destPort,
                                userIdName,
                                userIdUnique);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "queueWork(%s) failed: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status.fromStatusCode(workItem->status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(workItem->outputSize ==
           XcalarApiSizeOfOutput(XcalarApiGetNumNodesOutput));
    *numNodes =
        (unsigned) workItem->output->outputResult.getNumNodesOutput.numNodes;
CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    return status;
}

// If "support generate" execution fails on usrnode, this method
// attempts to generate via mgmtd. Must also be passed status of
// attempted request to usrnode.
Status
generateSupportBundle(XcalarWorkItem *workItem, Status statusFailure)
{
    // XXX Remove once implemented by monitor.
    Status status = statusFailure;
    char supportId[XcalarApiUuidStrLen + 1];
    char bundlePath[SupportBundlePathSize];
    uint64_t supportCaseId = 0;

    try {
        Status genStatus;

        // Get the Support Case Id from the workItem input
        supportCaseId = workItem->input->supportGenerateInput.supportCaseId;

        genStatus = SupportBundle::get()->supportGenerate(supportId,
                                                          sizeof(supportId),
                                                          bundlePath,
                                                          sizeof(bundlePath),
                                                          256,
                                                          supportCaseId);
        if (genStatus == StatusOk || genStatus == StatusSupportBundleNotSent) {
            // Work item output is being produced by mgmtd in this case.
            // Allocate output struct.
            assert(workItem->output == NULL);
            workItem->output = (XcalarApiOutput *)
                memCallocExt(1,
                             XcalarApiSizeOfOutput(
                                 XcalarApiSupportGenerateOutput) +
                                 strlen(bundlePath) + 1,
                             "XcalarApiServiceHandler::generateSupportBundle");
            if (workItem->output == NULL) {
                return statusFailure;
            }

            copyParamString(workItem->output->outputResult.supportGenerateOutput
                                .supportId,
                            supportId,
                            sizeof(workItem->output->outputResult
                                       .supportGenerateOutput.supportId));
            copyParamString(workItem->output->outputResult.supportGenerateOutput
                                .bundlePath,
                            bundlePath,
                            strlen(bundlePath) + 1);
            if (genStatus == StatusSupportBundleNotSent) {
                workItem->output->outputResult.supportGenerateOutput
                    .supportBundleSent = false;
            } else {
                workItem->output->outputResult.supportGenerateOutput
                    .supportBundleSent = true;
            }

            workItem->output->hdr.status = StatusOk.code();
            status = StatusOk;
        } else {
            status = statusFailure;
        }
    } catch (...) {
        status = statusFailure;
    }

    return status;
}

//
// Convert the workbook scope in a thrift struct (which has only a scope enum
// and not also workbook info such as user and session names which are implied
// to come from out-of-band), into a protobuf message equivalent. Since the
// protobuf message must contain all scope related info (like user and session
// names), this info must also be passed to the convert routine via the
// additional params "userId" and "sessionName"
//
static Status
convertScope(const XcalarApiWorkbookScopeT::type thriftScope,
             const std::string *userId,
             const std::string *sessionName,
             xcalar::compute::localtypes::Workbook::WorkbookScope *protoScope)
{
    Status status;

    switch (thriftScope) {
    case XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeGlobal: {
        // Ignore return value; this sets the fact that this is global
        protoScope->mutable_globl();
        status = StatusOk;
        break;
    }
    case XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeSession: {
        auto name_spec = protoScope->mutable_workbook()->mutable_name();
        name_spec->set_username(*userId);
        name_spec->set_workbookname(*sessionName);
        status = StatusOk;
        break;
    }
    default: {
        status = StatusUnimpl;
        goto CommonExit;
    }
    }

CommonExit:
    return status;
}

static Status
convertThriftInputToProto(const XcalarApiWorkItemT *workItemIn,
                          ProtoRequestMsg *requestMsg)
{
    Status status;

    switch (workItemIn->api) {
    case XcalarApisT::XcalarApiKeyLookup: {
        xcalar::compute::localtypes::KvStore::LookupRequest req;
        status = convertScope(workItemIn->input.keyLookupInput.scope,
                              &workItemIn->userId,
                              &workItemIn->sessionName,
                              req.mutable_key()->mutable_scope());
        BailIfFailed(status);
        req.mutable_key()->set_name(workItemIn->input.keyLookupInput.key);
        requestMsg->mutable_servic()->set_servicename("KvStore");
        requestMsg->mutable_servic()->set_methodname("Lookup");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiDriver: {
        xcalar::compute::localtypes::App::DriverRequest req;
        req.set_input_json(workItemIn->input.driverInput.inputJson);

        requestMsg->mutable_servic()->set_servicename("App");
        requestMsg->mutable_servic()->set_methodname("Driver");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiGetMemoryUsage: {
        xcalar::compute::localtypes::memory::GetUsageRequest req;
        req.mutable_scope()->mutable_workbook()->mutable_name()->set_username(
            workItemIn->input.memoryUsageInput.userName);

        requestMsg->mutable_servic()->set_servicename("Memory");
        requestMsg->mutable_servic()->set_methodname("GetUsage");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiPtChangeOwner: {
        xcalar::compute::localtypes::PublishedTable::ChangeOwnerRequest req;
        req.set_publishedtablename(
            workItemIn->input.ptChangeOwnerInput.publishTableName);
        req.mutable_scope()->mutable_workbook()->mutable_name()->set_username(
            workItemIn->input.ptChangeOwnerInput.userIdName);
        req.mutable_scope()
            ->mutable_workbook()
            ->mutable_name()
            ->set_workbookname(
                workItemIn->input.ptChangeOwnerInput.sessionName);

        requestMsg->mutable_servic()->set_servicename("PublishedTable");
        requestMsg->mutable_servic()->set_methodname("ChangeOwner");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    // UdfGetRes
    case XcalarApisT::XcalarApiUdfGetResolution: {
        xcalar::compute::localtypes::UDF::GetResolutionRequest req;
        status = convertScope(workItemIn->input.udfGetResInput.scope,
                              &workItemIn->userId,
                              &workItemIn->sessionName,
                              req.mutable_udfmodule()->mutable_scope());
        BailIfFailed(status);
        req.mutable_udfmodule()->set_name(
            workItemIn->input.udfGetResInput.moduleName);
        requestMsg->mutable_servic()->set_servicename("UserDefinedFunction");
        requestMsg->mutable_servic()->set_methodname("GetResolution");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiQueryList: {
        xcalar::compute::localtypes::Query::ListRequest listReq;
        listReq.set_name_pattern(workItemIn->input.queryListInput.namePattern);
        requestMsg->mutable_servic()->set_servicename("Query");
        requestMsg->mutable_servic()->set_methodname("List");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(listReq);
        break;
    }
    case XcalarApisT::XcalarApiAddIndex: {
        xcalar::compute::localtypes::Table::IndexRequest req;
        req.set_table_name(workItemIn->input.indexRequestInput.tableName);
        req.set_key_name(workItemIn->input.indexRequestInput.keyName);
        requestMsg->mutable_servic()->set_servicename("Table");
        requestMsg->mutable_servic()->set_methodname("AddIndex");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiRemoveIndex: {
        xcalar::compute::localtypes::Table::IndexRequest req;
        req.set_table_name(workItemIn->input.indexRequestInput.tableName);
        req.set_key_name(workItemIn->input.indexRequestInput.keyName);
        requestMsg->mutable_servic()->set_servicename("Table");
        requestMsg->mutable_servic()->set_methodname("RemoveIndex");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiListTables: {
        //
        // XXX Use constants for now since the caller doesn't set the limits.
        //     Should probably be a config variable.
        //
        const uint32_t maxUpdateCount = 128;
        const uint32_t maxSelectCount = 128;
        xcalar::compute::localtypes::PublishedTable::ListTablesRequest req;
        req.set_namepattern(workItemIn->input.listTablesInput.namePattern);
        req.set_updatestartbatchid(
            workItemIn->input.listTablesInput.updateStartBatchId);
        req.set_maxupdatecount(
            workItemIn->input.listTablesInput.getUpdates ? maxUpdateCount : 0);
        req.set_maxselectcount(
            workItemIn->input.listTablesInput.getSelects ? maxSelectCount : 0);
        requestMsg->mutable_servic()->set_servicename("PublishedTable");
        requestMsg->mutable_servic()->set_methodname("ListTables");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiMakeResultSet: {
        xcalar::compute::localtypes::ResultSet::ResultSetMakeRequest req;
        req.set_name(workItemIn->input.makeResultSetInput.dagNode.name);
        req.set_error_dataset(workItemIn->input.makeResultSetInput.errorDs);
        // XcalarApiMakeResultSet needs session scope.
        status =
            convertScope(XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeSession,
                         &workItemIn->userId,
                         &workItemIn->sessionName,
                         req.mutable_scope());
        BailIfFailed(status);
        requestMsg->mutable_servic()->set_servicename("ResultSet");
        requestMsg->mutable_servic()->set_methodname("Make");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiResultSetNext: {
        xcalar::compute::localtypes::ResultSet::ResultSetNextRequest req;
        req.set_result_set_id(
            stoull(workItemIn->input.resultSetNextInput.resultSetId));
        req.set_num_rows(workItemIn->input.resultSetNextInput.numRecords);
        // XcalarApiResultSetNext does not need session scope on thrift side, so
        // just leave it as global scope. However we will force session scope
        // on the proto side to help track outstanding ops in a session.
        status =
            convertScope(XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeGlobal,
                         NULL,
                         NULL,
                         req.mutable_scope());
        BailIfFailed(status);
        requestMsg->mutable_servic()->set_servicename("ResultSet");
        requestMsg->mutable_servic()->set_methodname("Next");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiResultSetAbsolute: {
        xcalar::compute::localtypes::ResultSet::ResultSetSeekRequest req;
        req.set_result_set_id(
            stoull(workItemIn->input.resultSetAbsoluteInput.resultSetId));
        req.set_row_index(workItemIn->input.resultSetAbsoluteInput.position);
        // XcalarApiResultSetAbsolute does not need session scope on thrift
        // side, so just leave it as global scope. However we will force session
        // scope on the proto side to help track outstanding ops in a session.
        status =
            convertScope(XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeGlobal,
                         NULL,
                         NULL,
                         req.mutable_scope());
        BailIfFailed(status);
        requestMsg->mutable_servic()->set_servicename("ResultSet");
        requestMsg->mutable_servic()->set_methodname("Seek");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiFreeResultSet: {
        xcalar::compute::localtypes::ResultSet::ResultSetReleaseRequest req;
        req.set_result_set_id(
            stoull(workItemIn->input.freeResultSetInput.resultSetId));
        // XcalarApiFreeResultSet does not need session scope on thrift side, so
        // just leave it as global scope. However we will force session scope
        // on the proto side to help track outstanding ops in a session.
        status =
            convertScope(XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeGlobal,
                         NULL,
                         NULL,
                         req.mutable_scope());
        BailIfFailed(status);
        requestMsg->mutable_servic()->set_servicename("ResultSet");
        requestMsg->mutable_servic()->set_methodname("Release");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiLogLevelGet: {
        google::protobuf::Empty empty;
        requestMsg->mutable_servic()->set_servicename("Log");
        requestMsg->mutable_servic()->set_methodname("GetLevel");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(empty);
        break;
    }
    case XcalarApisT::XcalarApiLogLevelSet: {
        xcalar::compute::localtypes::log::SetLevelRequest req;
        req.set_log_level(static_cast<::xcalar::compute::localtypes::
                                          XcalarEnumType::XcalarSyslogMsgLevel>(
            workItemIn->input.logLevelSetInput.logLevel));
        req.set_log_flush(workItemIn->input.logLevelSetInput.logFlush);
        req.set_log_flush_level(
            static_cast<::xcalar::compute::localtypes::XcalarEnumType::
                            XcalarSyslogFlushLevel>(
                workItemIn->input.logLevelSetInput.logFlushLevel));
        req.set_log_flush_period_sec(
            workItemIn->input.logLevelSetInput.logFlushPeriod);

        requestMsg->mutable_servic()->set_servicename("Log");
        requestMsg->mutable_servic()->set_methodname("SetLevel");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiQuery: {
        xcalar::compute::localtypes::Dataflow::ExecuteRequest req;
        status =
            convertScope(XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeSession,
                         &workItemIn->userId,
                         &workItemIn->sessionName,
                         req.mutable_scope());
        BailIfFailed(status);

        req.set_dataflow_str(workItemIn->input.queryInput.queryStr);
        req.set_job_name(workItemIn->input.queryInput.queryName);
        req.set_udf_user_name(workItemIn->input.queryInput.udfUserName);
        req.set_udf_session_name(workItemIn->input.queryInput.udfSessionName);
        req.set_is_async(workItemIn->input.queryInput.isAsync);
        req.set_sched_name(workItemIn->input.queryInput.schedName);
        req.set_collect_stats(workItemIn->input.queryInput.collectStats);
        req.set_pin_results(workItemIn->input.queryInput.pinResults);
        req.set_optimized(false);

        requestMsg->mutable_servic()->set_servicename("Dataflow");
        requestMsg->mutable_servic()->set_methodname("Execute");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiExecuteRetina: {
        xcalar::compute::localtypes::Dataflow::ExecuteRequest req;
        status =
            convertScope(XcalarApiWorkbookScopeT::XcalarApiWorkbookScopeSession,
                         &workItemIn->userId,
                         &workItemIn->sessionName,
                         req.mutable_scope());
        BailIfFailed(status);
        req.set_dataflow_name(workItemIn->input.executeRetinaInput.retinaName);
        req.set_job_name(workItemIn->input.executeRetinaInput.queryName);
        req.set_udf_user_name(workItemIn->input.executeRetinaInput.udfUserName);
        req.set_udf_session_name(
            workItemIn->input.executeRetinaInput.udfSessionName);
        // Making calls coming from thrift to always synchronous
        req.set_is_async(false);
        req.set_sched_name(workItemIn->input.executeRetinaInput.schedName);
        if (!workItemIn->input.executeRetinaInput.dest.empty()) {
            req.set_export_to_active_session(true);
        } else {
            req.set_export_to_active_session(false);
        }
        req.set_dest_table(workItemIn->input.executeRetinaInput.dest);
        req.set_optimized(true);

        // parameters
        std::vector<XcalarApiParameterT> workItemParams =
            workItemIn->input.executeRetinaInput.parameters;

        for (const auto &paramInput :
             workItemIn->input.executeRetinaInput.parameters) {
            try {
                xcalar::compute::localtypes::Dataflow::Parameter *protoParam =
                    req.add_parameters();
                protoParam->set_name(paramInput.paramName);
                protoParam->set_value(paramInput.paramValue);
            } catch (std::exception) {
                status = StatusNoMem;
                goto CommonExit;
            }
        }
        requestMsg->mutable_servic()->set_servicename("Dataflow");
        requestMsg->mutable_servic()->set_methodname("Execute");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiListFiles: {
        xcalar::compute::localtypes::Connectors::ListFilesRequest req;
        req.mutable_sourceargs()->set_targetname(
            workItemIn->input.listFilesInput.sourceArgs.targetName);
        req.mutable_sourceargs()->set_path(
            workItemIn->input.listFilesInput.sourceArgs.path);
        req.mutable_sourceargs()->set_filenamepattern(
            workItemIn->input.listFilesInput.sourceArgs.fileNamePattern);
        req.mutable_sourceargs()->set_recursive(
            workItemIn->input.listFilesInput.sourceArgs.recursive);

        requestMsg->mutable_servic()->set_servicename("Connectors");
        requestMsg->mutable_servic()->set_methodname("ListFiles");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(req);
        break;
    }
    case XcalarApisT::XcalarApiGetVersion: {
        google::protobuf::Empty empty;
        requestMsg->mutable_servic()->set_servicename("Version");
        requestMsg->mutable_servic()->set_methodname("GetVersion");
        requestMsg->mutable_servic()->mutable_body()->PackFrom(empty);
        break;
    }
    default: {
        status = StatusUnimpl;
        goto CommonExit;
    }  // default for api
    }  // api

CommonExit:
    return status;
}

static Status
convertProtoRowToRecords(
    XcalarApiResultSetNextOutputT *dstNextOut,
    google::protobuf::RepeatedPtrField<ProtoRow> *srcProtoRows)
{
    Status status;
    char *strVal = NULL;
    json_t *jsonVal = NULL;
    json_t *recJson = NULL;
    auto iterRow = srcProtoRows->begin();

    json_t *entries = json_array();
    BailIfNull(entries);

    for (; iterRow != srcProtoRows->end(); iterRow++) {
        int ret;
        assert(recJson == NULL);
        recJson = json_object();
        BailIfNull(recJson);

        auto row = (*iterRow);
        for (auto &pair : row.fields()) {
            auto colName = pair.first;
            auto field = pair.second;
            json_t *thisJson = NULL;

            status = JsonFormatOps::convertProtoToJson(&field, &thisJson);
            BailIfFailed(status);

            // This steals the reference to thisJson
            ret = json_object_set_new(recJson, colName.c_str(), thisJson);
            if (ret != 0) {
                json_decref(thisJson);
                thisJson = NULL;
                status = StatusNoMem;
                goto CommonExit;
            }

            thisJson = NULL;
        }

        ret = json_array_append_new(entries, recJson);
        BailIfFailedWith(ret, StatusNoMem);
        recJson = NULL;  // reference passed to entries
    }

    uint64_t ii;
    json_t *val;
    json_array_foreach (entries, ii, val) {
        strVal = json_dumps(val, 0);
        BailIfNull(strVal);

        xSyslog(moduleName, XlogVerbose, "RS: %s", strVal);

        dstNextOut->values.push_back(std::string(strVal));
        memFree(strVal);
        strVal = NULL;
    }

CommonExit:
    if (strVal) {
        memFree(strVal);
        strVal = NULL;
    }
    if (jsonVal) {
        json_decref(jsonVal);
        jsonVal = NULL;
    }
    if (recJson) {
        json_decref(recJson);
        recJson = NULL;
    }
    if (entries) {
        json_decref(entries);
        entries = NULL;
    }
    return status;
}

static Status
convertProtoResponseToThrift(XcalarApisT::type api,
                             const ProtoResponseMsg *responseMsg,
                             XcalarApiWorkItemResult *workItem)
{
    Status status;

    if (responseMsg->status() != 0) {
        workItem->output.hdr.log = responseMsg->error();
        workItem->output.hdr.status = (StatusT::type) responseMsg->status();
        goto CommonExit;
    }

    switch (api) {
    case XcalarApisT::XcalarApiKeyLookup: {
        xcalar::compute::localtypes::KvStore::LookupResponse response;
        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }

        workItem->output.outputResult.keyLookupOutput.value =
            response.value().text();
        workItem->output.outputResult.__isset.keyLookupOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiDriver: {
        xcalar::compute::localtypes::App::DriverResponse response;
        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }

        workItem->output.outputResult.driverOutput.outputJson =
            response.output_json();
        workItem->output.outputResult.__isset.driverOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiGetMemoryUsage: {
        xcalar::compute::localtypes::memory::GetUsageResponse response;
        XcalarApiUserMemoryUsageT *userMemory =
            &workItem->output.outputResult.memoryUsageOutput.userMemory;

        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        userMemory->userName =
            response.user_memory().scope().workbook().name().username();
        for (const auto &sessionMemory :
             response.user_memory().session_memories()) {
            XcalarApiSessionMemoryUsageT sessionMemoryUsage;
            sessionMemoryUsage.sessionName = sessionMemory.session_name();
            for (const auto &tableMemory : sessionMemory.table_memories()) {
                XcalarApiTableMemoryUsageT tableMemoryUsage;
                tableMemoryUsage.tableName = tableMemory.table_name();
                tableMemoryUsage.tableId = tableMemory.table_id();
                tableMemoryUsage.totalBytes = tableMemory.total_bytes();
                sessionMemoryUsage.tableMemory.push_back(tableMemoryUsage);
            }
            userMemory->sessionMemory.push_back(sessionMemoryUsage);
        }
        workItem->output.outputResult.__isset.memoryUsageOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiPtChangeOwner:  // NOOP
        break;
    case XcalarApisT::XcalarApiUdfGetResolution: {
        xcalar::compute::localtypes::UDF::GetResolutionResponse response;
        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.udfGetResOutput.udfResPath =
            response.fqmodname().text();
        workItem->output.outputResult.__isset.udfGetResOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiQueryList: {
        xcalar::compute::localtypes::Query::ListResponse listResp;
        bool success = responseMsg->servic().body().UnpackTo(&listResp);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }

        for (auto queryInfo : listResp.queries()) {
            XcalarApiQueryInfoT infoOut;

            infoOut.name = queryInfo.name();
            infoOut.elapsed.milliseconds = queryInfo.milliseconds_elapsed();
            infoOut.state = queryInfo.state();

            workItem->output.outputResult.queryListOutput.queries.push_back(
                infoOut);
        }

        workItem->output.outputResult.__isset.queryListOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiAddIndex:
    case XcalarApisT::XcalarApiRemoveIndex:
        break;
    case XcalarApisT::XcalarApiListTables: {
        xcalar::compute::localtypes::PublishedTable::ListTablesResponse resp;
        bool success = responseMsg->servic().body().UnpackTo(&resp);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.listTablesOutput.numTables =
            resp.tables_size();
        for (const auto &tableIn : resp.tables()) {
            XcalarApiTableInfoT table;

            table.name = tableIn.name();
            table.numPersistedUpdates = tableIn.numpersistedupdates();
            table.sizeTotal = tableIn.sizetotal();
            table.numRowsTotal = tableIn.numrowstotal();
            table.oldestBatchId = tableIn.oldestbatchid();
            table.nextBatchId = tableIn.nextbatchid();
            table.source.source = tableIn.srctablename();
            table.source.dest = tableIn.name();
            table.active = tableIn.active();
            table.restoring = tableIn.restoring();
            table.userIdName = tableIn.useridname();
            table.sessionName = tableIn.sessionname();

            for (const auto &key : tableIn.keys()) {
                XcalarApiColumnInfoT col;
                col.name = key.name();
                col.type = key.type();
                table.keys.push_back(col);
            }

            for (const auto &value : tableIn.values()) {
                XcalarApiColumnInfoT col;
                col.name = value.name();
                col.type = value.type();
                table.values.push_back(col);
            }

            for (const auto &updateIn : tableIn.updates()) {
                XcalarApiUpdateInfoT update;

                update.source = updateIn.srctablename();
                update.batchId = updateIn.batchid();
                update.startTS = updateIn.startts();
                update.size = updateIn.size();
                update.numRows = updateIn.numrows();
                update.numInserts = updateIn.numinserts();
                update.numUpdates = updateIn.numupdates();
                update.numDeletes = updateIn.numdeletes();

                table.updates.push_back(update);
            }

            for (const auto &selectIn : tableIn.selects()) {
                XcalarApiSelectInputT select;

                select.source = tableIn.name();
                select.dest = selectIn.dsttablename();
                select.minBatchId = selectIn.minbatchid();
                select.maxBatchId = selectIn.maxbatchid();

                table.selects.push_back(select);
            }

            for (const auto &indexIn : tableIn.indexes()) {
                XcalarApiIndexInfoT index;

                index.key.name = indexIn.key().name();
                index.key.type = indexIn.key().type();
                index.uptime.milliseconds = indexIn.uptimems();
                index.sizeEstimate = indexIn.sizeestimate();

                table.indices.push_back(index);
            }

            workItem->output.outputResult.listTablesOutput.tables.push_back(
                table);
        }
        workItem->output.outputResult.__isset.listTablesOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiMakeResultSet: {
        xcalar::compute::localtypes::ResultSet::ResultSetMakeResponse response;
        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.makeResultSetOutput.resultSetId =
            std::to_string(response.result_set_id());
        workItem->output.outputResult.makeResultSetOutput.numEntries =
            response.num_rows();
        workItem->output.outputResult.__isset.makeResultSetOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiResultSetNext: {
        xcalar::compute::localtypes::ResultSet::ResultSetNextResponse response;
        bool success = responseMsg->servic().body().UnpackTo(&response);
        XcalarApiResultSetNextOutputT *nextOut = NULL;
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        nextOut = &workItem->output.outputResult.resultSetNextOutput;
        nextOut->numValues = response.rows_size();
        status = convertProtoRowToRecords(nextOut, response.mutable_rows());
        if (status != StatusOk) {
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.__isset.resultSetNextOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiGetVersion: {
        xcalar::compute::localtypes::Version::GetVersionResponse response;

        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.getVersionOutput.version =
            response.version();
        workItem->output.outputResult.getVersionOutput.apiVersionSignatureFull =
            response.thrift_version_signature_full();

        workItem->output.outputResult.getVersionOutput
            .apiVersionSignatureShort =
            (XcalarApiVersionT::type) response.thrift_version_signature_short();
        workItem->output.outputResult.__isset.getVersionOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiResultSetAbsolute:  // NOOP
    case XcalarApisT::XcalarApiFreeResultSet:      // NOOP
        break;
    case XcalarApisT::XcalarApiQuery: {
        xcalar::compute::localtypes::Dataflow::ExecuteResponse executeResp;
        bool success = responseMsg->servic().body().UnpackTo(&executeResp);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.queryOutput.queryName =
            executeResp.job_name();
        workItem->output.outputResult.__isset.queryOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiExecuteRetina: {
        xcalar::compute::localtypes::Dataflow::ExecuteResponse response;
        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        // XXX passing empty string here
        workItem->output.outputResult.executeRetinaOutput.tableName = "";
        workItem->output.outputResult.__isset.executeRetinaOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiListFiles: {
        xcalar::compute::localtypes::Connectors::ListFilesResponse resp;
        bool success = responseMsg->servic().body().UnpackTo(&resp);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.listFilesOutput.numFiles =
            resp.files_size();

        for (const auto &fileIn : resp.files()) {
            XcalarApiFileAttrT attr;
            XcalarApiFileT file;

            attr.isDirectory = fileIn.isdir();
            attr.size = fileIn.size();
            attr.mtime = fileIn.mtime();

            file.attr = attr;
            file.name = fileIn.name();

            workItem->output.outputResult.listFilesOutput.files.push_back(file);
        }
        workItem->output.outputResult.__isset.listFilesOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiLogLevelGet: {
        xcalar::compute::localtypes::log::GetLevelResponse response;

        bool success = responseMsg->servic().body().UnpackTo(&response);
        if (!success) {
            status = StatusProtobufDecodeError;
            workItem->output.hdr.status = (StatusT::type) responseMsg->status();
            goto CommonExit;
        }
        workItem->output.outputResult.logLevelGetOutput.logLevel =
            response.log_level();
        workItem->output.outputResult.logLevelGetOutput.logFlushPeriod =
            response.log_flush_period_sec();
        workItem->output.outputResult.__isset.logLevelGetOutput = true;
        break;
    }
    case XcalarApisT::XcalarApiLogLevelSet:  // NOOP
        break;
    default: {
        status = StatusUnimpl;
        goto CommonExit;
        break;
    }
    }

CommonExit:
    return status;
}

static Status
queueServiceApi(const XcalarApiWorkItemT *requestWorkItem,
                XcalarApiWorkItemResult *responseWorkItem)
{
    Status status;
    ProtoRequestMsg requestMsg;
    ProtoResponseMsg responseMsg;
    ServiceSocket socket;
    XcalarApisT::type api = requestWorkItem->api;
    Stopwatch stopwatch;

    requestMsg.set_requestid(0);
    requestMsg.set_target(ProtoMsgTargetService);

    status = convertThriftInputToProto(requestWorkItem, &requestMsg);
    BailIfFailed(status);

    status = socket.init();
    BailIfFailed(status);

    status = socket.sendRequest(&requestMsg, &responseMsg);
    BailIfFailed(status);

    status = convertProtoResponseToThrift(api, &responseMsg, responseWorkItem);
    BailIfFailed(status);

CommonExit:
    Status status2;
    status2.fromStatusCode((StatusCode) responseMsg.status());
    unsigned long hours, minutes, seconds, milliseconds;
    stopwatch.stop();
    stopwatch.getPrintableTime(hours, minutes, seconds, milliseconds);

    xSyslog(moduleName,
            XlogInfo,
            "%s:%s(%s) return in "
            "%lu:%02lu:%02lu.%03lu: status: %s",
            __FILE__,
            __func__,
            strGetFromXcalarApis((XcalarApis) api),
            hours,
            minutes,
            seconds,
            milliseconds,
            strGetFromStatus(status2));

    return status;
}

static Status parseCPlusPlusWorkItem(const XcalarApiWorkItemT &workItemIn,
                                     const XcalarApisT::type api,
                                     XcalarWorkItem **workItemRetp);

static Status
queueLegacyApi(const XcalarApiWorkItemT *requestWorkItem,
               XcalarApiWorkItemResult *responseWorkItem)
{
    Status status;
    XcalarApiException exception;
    XcalarWorkItem *workItem = NULL;
    status = parseCPlusPlusWorkItem(*requestWorkItem,
                                    requestWorkItem->api,
                                    &workItem);
    if (status != StatusOk) {
        assert(workItem == NULL);
        exception.status = (StatusT::type) status.code();
        throw exception;
    }

    // XXX Allow users to change the destIp and destPort here via the
    // GUI
    status = xcalarApiQueueWork(workItem,
                                destIp,
                                destPort,
                                requestWorkItem->userId.c_str(),
                                requestWorkItem->userIdUnique);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s:%s() queueWork(%s) failed: %s",
                __FILE__,
                __func__,
                strGetFromXcalarApis((XcalarApis) requestWorkItem->api),
                strGetFromStatus(status));

        if (requestWorkItem->api ==
            (XcalarApisT::type) XcalarApiSupportGenerate) {
            status = generateSupportBundle(workItem, status);
        }

        if (requestWorkItem->api != (XcalarApisT::type) XcalarApiShutdown) {
            exception.status = (StatusT::type) status.code();
            xcalarApiFreeWorkItem(workItem);
            throw exception;
        }
    }

    if (workItem->output != NULL) {
        responseWorkItem->output.hdr.status =
            (StatusT::type) workItem->output->hdr.status;
        responseWorkItem->output.hdr.elapsed.milliseconds =
            workItem->output->hdr.elapsed.milliseconds;
        responseWorkItem->output.hdr.log = workItem->output->hdr.log;
    }

    // Only output is the hdr, which we've extracted
    if (workItem->outputSize == sizeof(workItem->output->hdr)) {
        if ((XcalarApis) requestWorkItem->api != XcalarApiBulkLoad &&
            (XcalarApis) requestWorkItem->api != XcalarApiArchiveTables) {
            // Done with APIs that cannot handle the "emergency" output.
            goto CommonExit;
        }
    }

    //
    // APIs which have output regardless of success/failure.
    //

    switch ((XcalarApis) requestWorkItem->api) {
    case XcalarApiUdfAdd:
    case XcalarApiUdfUpdate:
        copyXcalarApiUdfAddUpdateOutput(responseWorkItem->output.outputResult
                                            .udfAddUpdateOutput,
                                        &workItem->output->outputResult
                                             .udfAddUpdateOutput);
        responseWorkItem->output.outputResult.__isset.udfAddUpdateOutput = true;
        break;
    case XcalarApiBulkLoad:
        char datasetIdStr[40];
        if (workItem->outputSize < sizeof(XcalarApiBulkLoadOutput)) {
            // XCE returned truncated, likely "emergency", output.
            assert(workItem->output->hdr.status != StatusOk.code());
            // Just make up the content
            snprintf(datasetIdStr, sizeof(datasetIdStr), "%d", 0);
            responseWorkItem->output.outputResult.loadOutput.dataset.datasetId =
                datasetIdStr;
            responseWorkItem->output.outputResult.loadOutput.numFiles = 0;
            responseWorkItem->output.outputResult.loadOutput.numBytes = 0;
            responseWorkItem->output.outputResult.loadOutput.dataset.name = "";
            responseWorkItem->output.outputResult.loadOutput.errorString = "";
            responseWorkItem->output.outputResult.loadOutput.errorFile = "";
        } else {
            snprintf(datasetIdStr,
                     sizeof(datasetIdStr),
                     "%lu",
                     workItem->output->outputResult.loadOutput.dataset
                         .datasetId);
            responseWorkItem->output.outputResult.loadOutput.dataset.datasetId =
                datasetIdStr;
            responseWorkItem->output.outputResult.loadOutput.numFiles =
                workItem->output->outputResult.loadOutput.numFiles;
            responseWorkItem->output.outputResult.loadOutput.numBytes =
                workItem->output->outputResult.loadOutput.numBytes;
            responseWorkItem->output.outputResult.loadOutput.dataset.name =
                workItem->output->outputResult.loadOutput.dataset.name;
            responseWorkItem->output.outputResult.loadOutput.errorString =
                workItem->output->outputResult.loadOutput.errorString;
            responseWorkItem->output.outputResult.loadOutput.errorFile =
                workItem->output->outputResult.loadOutput.errorFile;
        }
        responseWorkItem->output.outputResult.__isset.loadOutput = true;
        break;
    case XcalarApiAppReap:
        if (workItem->output->outputResult.appReapOutput.outStrSize > 0) {
            responseWorkItem->output.outputResult.appReapOutput.outStr =
                workItem->output->outputResult.appReapOutput.str;
        } else {
            responseWorkItem->output.outputResult.appReapOutput.outStr = "";
        }
        if (workItem->output->outputResult.appReapOutput.errStrSize > 0) {
            responseWorkItem->output.outputResult.appReapOutput.errStr =
                workItem->output->outputResult.appReapOutput.str +
                workItem->output->outputResult.appReapOutput.outStrSize;
        } else {
            responseWorkItem->output.outputResult.appReapOutput.errStr = "";
        }
        responseWorkItem->output.outputResult.__isset.appReapOutput = true;
        break;

    case XcalarApiDeleteObjects: {
        responseWorkItem->output.outputResult.deleteDagNodesOutput.numNodes =
            workItem->output->outputResult.deleteDagNodesOutput.numNodes;

        // statuses is a variable array of variable length structs,
        // need to use a cursor.
        uint8_t *cursor;
        cursor = (uint8_t *) &workItem->output->outputResult
                     .deleteDagNodesOutput.statuses[0];
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.deleteDagNodesOutput.numNodes;
             ii++) {
            XcalarApiDeleteDagNodeStatusT deleteNodeStatus;
            XcalarApiDeleteDagNodeStatus *nodeStatusIn =
                (XcalarApiDeleteDagNodeStatus *) cursor;
            size_t nodeStatusSize = sizeof(*nodeStatusIn);

            copyXcalarApiNodeInfoOut(deleteNodeStatus.nodeInfo,
                                     &nodeStatusIn->nodeInfo);
            deleteNodeStatus.status = (StatusT::type) nodeStatusIn->status;
            deleteNodeStatus.numRefs = nodeStatusIn->numRefs;

            for (int64_t jj = 0; jj < deleteNodeStatus.numRefs; jj++) {
                DagRefT ref;
                ref.type = (DagRefTypeT::type) nodeStatusIn->refs[jj].type;
                ref.name = nodeStatusIn->refs[jj].name;
                ref.xid = std::to_string(nodeStatusIn->refs[jj].xid);

                cursor += sizeof(nodeStatusIn->refs[jj]);
                deleteNodeStatus.refs.push_back(ref);
            }

            cursor += nodeStatusSize;
            responseWorkItem->output.outputResult.deleteDagNodesOutput.statuses
                .push_back(deleteNodeStatus);
        }
        responseWorkItem->output.outputResult.__isset.deleteDagNodesOutput =
            true;
        break;
    }

    case XcalarApiArchiveTables: {
        uint64_t numNodes;
        if (workItem->output->hdr.status ==
            StatusSerializationIsDisabled.code()) {
            // "Emergency" output was returned which is effectively
            // garbage
            assert(workItem->outputSize == sizeof(XcalarApiOutputHeader));
            numNodes = 0;
        } else {
            numNodes =
                workItem->output->outputResult.archiveTablesOutput.numNodes;
        }

        responseWorkItem->output.outputResult.archiveTablesOutput.numNodes =
            numNodes;

        // statuses is a variable array of variable length structs,
        // need to use a cursor.
        uint8_t *cursor;
        cursor = (uint8_t *) &workItem->output->outputResult.archiveTablesOutput
                     .statuses[0];
        for (unsigned ii = 0; ii < numNodes; ii++) {
            XcalarApiDeleteDagNodeStatusT deleteNodeStatus;
            XcalarApiDeleteDagNodeStatus *nodeStatusIn =
                (XcalarApiDeleteDagNodeStatus *) cursor;
            size_t nodeStatusSize = sizeof(*nodeStatusIn);

            copyXcalarApiNodeInfoOut(deleteNodeStatus.nodeInfo,
                                     &nodeStatusIn->nodeInfo);
            deleteNodeStatus.status = (StatusT::type) nodeStatusIn->status;
            deleteNodeStatus.numRefs = nodeStatusIn->numRefs;

            for (int64_t jj = 0; jj < deleteNodeStatus.numRefs; jj++) {
                DagRefT ref;
                ref.type = (DagRefTypeT::type) nodeStatusIn->refs[jj].type;
                ref.name = nodeStatusIn->refs[jj].name;
                ref.xid = std::to_string(nodeStatusIn->refs[jj].xid);

                cursor += sizeof(nodeStatusIn->refs[jj]);
                deleteNodeStatus.refs.push_back(ref);
            }

            cursor += nodeStatusSize;
            responseWorkItem->output.outputResult.archiveTablesOutput.statuses
                .push_back(deleteNodeStatus);
        }
        responseWorkItem->output.outputResult.__isset.archiveTablesOutput =
            true;
        break;
    }

    case XcalarApiSessionNew:
        if ((workItem->output != NULL) &&
            workItem->output->hdr.status ==
                StatusSessionUsrAlreadyExists.code()) {
            updateSessionGenericOutput(&(responseWorkItem->output.outputResult
                                             .sessionNewOutput
                                             .sessionGenericOutput),
                                       &(workItem->output->outputResult
                                             .sessionNewOutput
                                             .sessionGenericOutput));
        }
        responseWorkItem->output.outputResult.__isset.sessionGenericOutput =
            true;
        break;

    // these use workItem->output->outputResult.sessionListOutput
    case XcalarApiSessionPersist:
    case XcalarApiSessionList:
        if (workItem->output != NULL) {
            if (workItem->output->hdr.status ==
                StatusSessionUsrAlreadyExists.code()) {
                updateSessionGenericOutput(&(responseWorkItem->output
                                                 .outputResult.sessionListOutput
                                                 .sessionGenericOutput),
                                           &(workItem->output->outputResult
                                                 .sessionListOutput
                                                 .sessionGenericOutput));
            } else if (workItem->output->hdr.status ==
                       StatusSessListIncomplete.code()) {
                responseWorkItem->output.outputResult.sessionListOutput
                    .numSessions = workItem->output->outputResult
                                       .sessionListOutput.numSessions;
                for (unsigned ii = 0; ii < workItem->output->outputResult
                                               .sessionListOutput.numSessions;
                     ii++) {
                    XcalarApiSessionT session;
                    copyXcalarApiSessionOut(session,
                                            &workItem->output->outputResult
                                                 .sessionListOutput
                                                 .sessions[ii]);
                    responseWorkItem->output.outputResult.sessionListOutput
                        .sessions.push_back(session);
                }
            }
        }
        responseWorkItem->output.outputResult.__isset.sessionListOutput = true;
        break;

    // these use workItem->output->outputResult.sessionGenericOutput
    case XcalarApiSessionInact:
    case XcalarApiSessionDelete:
    case XcalarApiSessionRename:
    case XcalarApiSessionActivate:
        if ((workItem->output != NULL) &&
            workItem->output->hdr.status ==
                StatusSessionUsrAlreadyExists.code()) {
            updateSessionGenericOutput(&(responseWorkItem->output.outputResult
                                             .sessionGenericOutput),
                                       &(workItem->output->outputResult
                                             .sessionGenericOutput));
        }
        responseWorkItem->output.outputResult.__isset.sessionGenericOutput =
            true;
        break;

    case XcalarApiRestoreTable:
        if (workItem->output != NULL) {
            for (unsigned ii = 0;
                 ii <
                 workItem->output->outputResult.restoreTableOutput.numTables;
                 ii++) {
                responseWorkItem->output.outputResult.restoreTableOutput
                    .dependencies.push_back(
                        workItem->output->outputResult.restoreTableOutput
                            .tableDependencies[ii]);
            }
        }
        responseWorkItem->output.outputResult.__isset.restoreTableOutput = true;
        break;

    default:
        break;
    }

    if (responseWorkItem->output.hdr.status != StatusT::StatusOk) {
        goto CommonExit;
    }

    //
    // APIs which only have output on success.
    //

    switch ((XcalarApis) requestWorkItem->api) {
    case XcalarApiGetTableMeta:
        copyXcalarApiGetTableMeta(workItem->input->getTableMetaInput
                                      .tableNameInput.isTable,
                                  responseWorkItem->output.outputResult
                                      .getTableMetaOutput,
                                  &workItem->output->outputResult
                                       .getTableMetaOutput);
        responseWorkItem->output.outputResult.__isset.getTableMetaOutput = true;
        break;

    case XcalarApiListExportTargets:
        responseWorkItem->output.outputResult.listTargetsOutput.numTargets =
            workItem->output->outputResult.listTargetsOutput.numTargets;
        for (int ii = 0;
             ii < workItem->output->outputResult.listTargetsOutput.numTargets;
             ii++) {
            ExExportTargetT exportTargetOut;
            ExExportTarget *exportTargetIn;

            exportTargetIn =
                &workItem->output->outputResult.listTargetsOutput.targets[ii];
            exportTargetOut.hdr.type =
                (ExTargetTypeT::type) exportTargetIn->hdr.type;
            exportTargetOut.hdr.name = exportTargetIn->hdr.name;

            switch (exportTargetIn->hdr.type) {
            case ExTargetSFType:
                exportTargetOut.specificInput.sfInput.url =
                    exportTargetIn->specificInput.sfInput.url;
                exportTargetOut.specificInput.__isset.sfInput = true;
                break;
            case ExTargetUDFType:
                exportTargetOut.specificInput.udfInput.url =
                    exportTargetIn->specificInput.udfInput.url;
                exportTargetOut.specificInput.udfInput.appName =
                    exportTargetIn->specificInput.udfInput.appName;
                exportTargetOut.specificInput.__isset.udfInput = true;
                break;
            default:
                assert(0);
                exception.status = StatusT::StatusUnimpl;
                throw exception;
            }

            responseWorkItem->output.outputResult.listTargetsOutput.targets
                .push_back(exportTargetOut);
        }
        responseWorkItem->output.outputResult.__isset.listTargetsOutput = true;
        break;

    case XcalarApiListDagNodeInfo:
        responseWorkItem->output.outputResult.listNodesOutput.numNodes =
            workItem->output->outputResult.listNodesOutput.numNodes;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.listNodesOutput.numNodes;
             ii++) {
            XcalarApiDagNodeInfoT nodeInfo;
            copyXcalarApiNodeInfoOut(nodeInfo,
                                     &workItem->output->outputResult
                                          .listNodesOutput.nodeInfo[ii]);
            responseWorkItem->output.outputResult.listNodesOutput.nodeInfo
                .push_back(nodeInfo);
        }
        responseWorkItem->output.outputResult.__isset.listNodesOutput = true;
        break;

    case XcalarApiListDatasets:
        responseWorkItem->output.outputResult.listDatasetsOutput.numDatasets =
            workItem->output->outputResult.listDatasetsOutput.numDatasets;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.listDatasetsOutput.numDatasets;
             ii++) {
            XcalarApiDatasetT dataset;
            copyXcalarApiDatasetOut(dataset,
                                    &workItem->output->outputResult
                                         .listDatasetsOutput.datasets[ii]);
            responseWorkItem->output.outputResult.listDatasetsOutput.datasets
                .push_back(dataset);
        }
        responseWorkItem->output.outputResult.__isset.listDatasetsOutput = true;
        break;

    case XcalarApiGetDatasetsInfo:
        responseWorkItem->output.outputResult.getDatasetsInfoOutput
            .numDatasets =
            workItem->output->outputResult.getDatasetsInfoOutput.numDatasets;
        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.getDatasetsInfoOutput.numDatasets;
             ii++) {
            XcalarApiDatasetsInfoT datasetsInfo;
            copyXcalarApiGetDatasetsInfoOut(datasetsInfo,
                                            &workItem->output->outputResult
                                                 .getDatasetsInfoOutput
                                                 .datasets[ii]);
            responseWorkItem->output.outputResult.getDatasetsInfoOutput.datasets
                .push_back(datasetsInfo);
        }
        responseWorkItem->output.outputResult.__isset.getDatasetsInfoOutput =
            true;
        break;

    case XcalarApiDatasetGetMeta:
        responseWorkItem->output.outputResult.datasetGetMetaOutput.datasetMeta =
            workItem->output->outputResult.datasetGetMetaOutput.datasetMeta;
        responseWorkItem->output.outputResult.__isset.datasetGetMetaOutput =
            true;
        break;

    case XcalarApiDatasetUnload:
        responseWorkItem->output.outputResult.datasetUnloadOutput.numDatasets =
            workItem->output->outputResult.datasetUnloadOutput.numDatasets;
        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.datasetUnloadOutput.numDatasets;
             ii++) {
            XcalarApiDatasetUnloadStatusT datasetUnloadStatus;
            copyXcalarApiDatasetOut(datasetUnloadStatus.dataset,
                                    &workItem->output->outputResult
                                         .datasetUnloadOutput.statuses[ii]
                                         .dataset);
            datasetUnloadStatus.status =
                (StatusT::type) workItem->output->outputResult
                    .datasetUnloadOutput.statuses[ii]
                    .status;
            responseWorkItem->output.outputResult.datasetUnloadOutput.statuses
                .push_back(datasetUnloadStatus);
        }
        responseWorkItem->output.outputResult.__isset.datasetUnloadOutput =
            true;
        break;

    case XcalarApiShutdown:
        reapNodes(requestWorkItem->userId.c_str(),
                  requestWorkItem->userIdUnique);
        responseWorkItem->output.hdr.status = (StatusT::type) StatusOk.code();
        break;

    case XcalarApiGetStatByGroupId:
        // this api has been deprecated
        assert(0);
        break;

    case XcalarApiGetStat:
        responseWorkItem->output.outputResult.statOutput.numStats =
            workItem->output->outputResult.statOutput.numStats;
        responseWorkItem->output.outputResult.statOutput.truncated =
            workItem->output->outputResult.statOutput.truncated;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.statOutput.numStats;
             ii++) {
            XcalarApiStatT stat;

            stat.statName =
                workItem->output->outputResult.statOutput.stats[ii].statName;

            stat.groupId =
                workItem->output->outputResult.statOutput.stats[ii].groupId;

            assertStatic(
                sizeof(workItem->output->outputResult.statOutput.stats[ii]
                           .statValue) == sizeof(uint64_t));

            assertStatic(sizeof(stat.statValue) >= sizeof(uint64_t));
            memcpy(&stat.statValue,
                   &workItem->output->outputResult.statOutput.stats[ii]
                        .statValue,
                   sizeof(uint64_t));

            assertStatic(
                sizeof(stat.statType) ==
                sizeof(workItem->output->outputResult.statOutput.stats[ii]
                           .statType));
            stat.statType =
                workItem->output->outputResult.statOutput.stats[ii].statType;

            responseWorkItem->output.outputResult.statOutput.stats.push_back(
                stat);
        }
        responseWorkItem->output.outputResult.__isset.statOutput = true;
        break;

    case XcalarApiIndex:
        responseWorkItem->output.outputResult.indexOutput.tableName =
            workItem->output->outputResult.indexOutput.tableName;
        responseWorkItem->output.outputResult.__isset.indexOutput = true;
        break;

    case XcalarApiMap:
        responseWorkItem->output.outputResult.mapOutput.tableName =
            workItem->output->outputResult.mapOutput.tableName;
        responseWorkItem->output.outputResult.__isset.mapOutput = true;
        break;

    case XcalarApiSynthesize:
        responseWorkItem->output.outputResult.synthesizeOutput.tableName =
            workItem->output->outputResult.synthesizeOutput.tableName;
        responseWorkItem->output.outputResult.__isset.synthesizeOutput = true;
        break;

    case XcalarApiSelect:
        responseWorkItem->output.outputResult.selectOutput.tableName =
            workItem->output->outputResult.selectOutput.tableName;
        responseWorkItem->output.outputResult.__isset.selectOutput = true;
        break;

    case XcalarApiGetRowNum:
        responseWorkItem->output.outputResult.getRowNumOutput.tableName =
            workItem->output->outputResult.getRowNumOutput.tableName;
        responseWorkItem->output.outputResult.__isset.getRowNumOutput = true;
        break;

    case XcalarApiProject:
        responseWorkItem->output.outputResult.projectOutput.tableName =
            workItem->output->outputResult.projectOutput.tableName;
        responseWorkItem->output.outputResult.__isset.projectOutput = true;
        break;

    case XcalarApiFilter:
        responseWorkItem->output.outputResult.filterOutput.tableName =
            workItem->output->outputResult.filterOutput.tableName;
        responseWorkItem->output.outputResult.__isset.filterOutput = true;
        break;

    case XcalarApiJoin:
        responseWorkItem->output.outputResult.joinOutput.tableName =
            workItem->output->outputResult.joinOutput.tableName;
        responseWorkItem->output.outputResult.__isset.joinOutput = true;
        break;

    case XcalarApiUnion:
        responseWorkItem->output.outputResult.unionOutput.tableName =
            workItem->output->outputResult.unionOutput.tableName;
        responseWorkItem->output.outputResult.__isset.unionOutput = true;
        break;

    case XcalarApiGroupBy:
        responseWorkItem->output.outputResult.groupByOutput.tableName =
            workItem->output->outputResult.groupByOutput.tableName;
        responseWorkItem->output.outputResult.__isset.groupByOutput = true;
        break;

    case XcalarApiGetTableRefCount:
        responseWorkItem->output.outputResult.getTableRefCountOutput.refCount =
            workItem->output->outputResult.getTableRefCountOutput.refCount;
        responseWorkItem->output.outputResult.__isset.getTableRefCountOutput =
            true;
        break;

    case XcalarApiQueryState:
        responseWorkItem->output.outputResult.queryStateOutput.queryState =
            (QueryStateT::type)
                workItem->output->outputResult.queryStateOutput.queryState;
        responseWorkItem->output.outputResult.queryStateOutput.queryStatus =
            (StatusT::type)
                workItem->output->outputResult.queryStateOutput.queryStatus;

        responseWorkItem->output.outputResult.queryStateOutput
            .numQueuedWorkItem =
            workItem->output->outputResult.queryStateOutput.numQueuedWorkItem;
        responseWorkItem->output.outputResult.queryStateOutput
            .numCompletedWorkItem = workItem->output->outputResult
                                        .queryStateOutput.numCompletedWorkItem;
        responseWorkItem->output.outputResult.queryStateOutput
            .numFailedWorkItem =
            workItem->output->outputResult.queryStateOutput.numFailedWorkItem;
        responseWorkItem->output.outputResult.queryStateOutput.queryNodeId =
            workItem->output->outputResult.queryStateOutput.queryNodeId;
        copyXcalarApiTime(responseWorkItem->output.outputResult.queryStateOutput
                              .elapsed,
                          &workItem->output->outputResult.queryStateOutput
                               .elapsed);
        responseWorkItem->output.outputResult.queryStateOutput.queryGraph
            .numNodes =
            workItem->output->outputResult.queryStateOutput.queryGraph.numNodes;
        for (unsigned ii = 0; ii < workItem->output->outputResult
                                       .queryStateOutput.queryGraph.numNodes;
             ii++) {
            XcalarApiDagNodeT dagNode;
            copyXcalarApiDagNodeOut(dagNode,
                                    workItem->output->outputResult
                                        .queryStateOutput.queryGraph.node[ii]);
            responseWorkItem->output.outputResult.queryStateOutput.queryGraph
                .node.push_back(dagNode);
        }
        responseWorkItem->output.outputResult.__isset.queryStateOutput = true;
        break;

    case XcalarApiGetDag:
        responseWorkItem->output.outputResult.dagOutput.numNodes =
            workItem->output->outputResult.dagOutput.numNodes;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.dagOutput.numNodes;
             ii++) {
            XcalarApiDagNodeT dagNode;
            copyXcalarApiDagNodeOut(dagNode,
                                    workItem->output->outputResult.dagOutput
                                        .node[ii]);
            responseWorkItem->output.outputResult.dagOutput.node.push_back(
                dagNode);
        }

        responseWorkItem->output.outputResult.__isset.dagOutput = true;
        break;

    case XcalarApiAggregate:
        responseWorkItem->output.outputResult.aggregateOutput.tableName =
            workItem->output->outputResult.aggregateOutput.tableName;
        responseWorkItem->output.outputResult.aggregateOutput.jsonAnswer =
            workItem->output->outputResult.aggregateOutput.jsonAnswer;
        responseWorkItem->output.outputResult.__isset.aggregateOutput = true;
        break;

    case XcalarApiListRetinas:
        responseWorkItem->output.outputResult.listRetinasOutput.numRetinas =
            workItem->output->outputResult.listRetinasOutput.numRetinas;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.listRetinasOutput.numRetinas;
             ii++) {
            DagRetinaDescT retinaDesc;
            retinaDesc.retinaName =
                workItem->output->outputResult.listRetinasOutput.retinaDescs[ii]
                    .retinaName;
            responseWorkItem->output.outputResult.listRetinasOutput.retinaDescs
                .push_back(retinaDesc);
        }
        responseWorkItem->output.outputResult.__isset.listRetinasOutput = true;
        break;

        break;

    case XcalarApiImportRetina:
        responseWorkItem->output.outputResult.importRetinaOutput.numUdfModules =
            workItem->output->outputResult.importRetinaOutput.numUdfModules;
        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.importRetinaOutput.numUdfModules;
             ii++) {
            XcalarApiUdfAddUpdateOutputT udfModuleStatusOut;
            XcalarApiUdfAddUpdateOutput *udfModuleStatusIn;
            udfModuleStatusIn = workItem->output->outputResult
                                    .importRetinaOutput.udfModuleStatuses[ii];
            copyXcalarApiUdfAddUpdateOutput(udfModuleStatusOut,
                                            udfModuleStatusIn);
            responseWorkItem->output.outputResult.importRetinaOutput
                .udfModuleStatuses.push_back(udfModuleStatusOut);
        }
        responseWorkItem->output.outputResult.__isset.importRetinaOutput = true;
        break;

    case XcalarApiExportRetina: {
        XcalarApiExportRetinaOutput *exportRetinaOutput;
        Status tmpStatus;
        char *retinaBuf = NULL;
        size_t retinaBufSize = 0;

        exportRetinaOutput = &workItem->output->outputResult.exportRetinaOutput;

        tmpStatus = base64Encode(exportRetinaOutput->retina,
                                 exportRetinaOutput->retinaCount,
                                 &retinaBuf,
                                 &retinaBufSize);
        if (tmpStatus != StatusOk) {
            responseWorkItem->output.hdr.status =
                (StatusT::type) tmpStatus.code();
        } else {
            std::string retinaString(retinaBuf, retinaBufSize);
            responseWorkItem->output.outputResult.exportRetinaOutput.retina =
                retinaString;
            responseWorkItem->output.outputResult.exportRetinaOutput
                .retinaCount = retinaBufSize;
            // allocated by base64Encode()
            memFree(retinaBuf);
        }
        responseWorkItem->output.outputResult.__isset.exportRetinaOutput = true;
        break;
    }

    case XcalarApiListXdfs:
        responseWorkItem->output.outputResult.listXdfsOutput.numXdfs =
            workItem->output->outputResult.listXdfsOutput.numXdfs;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.listXdfsOutput.numXdfs;
             ii++) {
            XcalarEvalFnDescT fnDesc;
            copyXcalarEvalFnDescOut(fnDesc,
                                    &workItem->output->outputResult
                                         .listXdfsOutput.fnDescs[ii]);
            responseWorkItem->output.outputResult.listXdfsOutput.fnDescs
                .push_back(fnDesc);
        }
        responseWorkItem->output.outputResult.__isset.listXdfsOutput = true;
        break;

    case XcalarApiSessionPersist:
    case XcalarApiSessionList:
        responseWorkItem->output.outputResult.sessionListOutput.numSessions =
            workItem->output->outputResult.sessionListOutput.numSessions;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.sessionListOutput.numSessions;
             ii++) {
            XcalarApiSessionT session;
            copyXcalarApiSessionOut(session,
                                    &workItem->output->outputResult
                                         .sessionListOutput.sessions[ii]);
            responseWorkItem->output.outputResult.sessionListOutput.sessions
                .push_back(session);
        }
        responseWorkItem->output.outputResult.__isset.sessionListOutput = true;
        break;

    case XcalarApiSessionNew: {
        char sessionIdStr[SessionMgr::MaxTextHexIdSize];
        snprintf(sessionIdStr,
                 sizeof(sessionIdStr),
                 "%lX",
                 workItem->output->outputResult.sessionNewOutput.sessionId);
        responseWorkItem->output.outputResult.sessionNewOutput.sessionId =
            sessionIdStr;
        responseWorkItem->output.outputResult.__isset.sessionNewOutput = true;
        break;
    }

    case XcalarApiGetRetina: {
        XcalarApiDagOutput *retinaDag;

        responseWorkItem->output.outputResult.getRetinaOutput.retina.retinaDesc
            .retinaName = workItem->output->outputResult.getRetinaOutput.retina
                              .retinaDesc.retinaName;

        retinaDag =
            &workItem->output->outputResult.getRetinaOutput.retina.retinaDag;
        responseWorkItem->output.outputResult.getRetinaOutput.retina.retinaDag
            .numNodes = retinaDag->numNodes;
        for (unsigned ii = 0; ii < retinaDag->numNodes; ii++) {
            XcalarApiDagNodeT dagNode;
            copyXcalarApiDagNodeOut(dagNode, retinaDag->node[ii]);
            responseWorkItem->output.outputResult.getRetinaOutput.retina
                .retinaDag.node.push_back(dagNode);
        }

        responseWorkItem->output.outputResult.__isset.getRetinaOutput = true;
        break;
    }

    case XcalarApiGetRetinaJson: {
        responseWorkItem->output.outputResult.getRetinaJsonOutput.retinaJson =
            workItem->output->outputResult.getRetinaJsonOutput.retinaJson;

        responseWorkItem->output.outputResult.__isset.getRetinaJsonOutput =
            true;
        break;
    }

    case XcalarApiListParametersInRetina: {
        XcalarApiListParametersInRetinaOutput *listParametersInRetinaOutput;

        listParametersInRetinaOutput =
            &workItem->output->outputResult.listParametersInRetinaOutput;

        responseWorkItem->output.outputResult.listParametersInRetinaOutput
            .numParameters = listParametersInRetinaOutput->numParameters;

        for (unsigned ii = 0; ii < listParametersInRetinaOutput->numParameters;
             ii++) {
            XcalarApiParameterT parameter;
            parameter.paramName =
                listParametersInRetinaOutput->parameters[ii].parameterName;
            parameter.paramValue =
                listParametersInRetinaOutput->parameters[ii].parameterValue;
            responseWorkItem->output.outputResult.listParametersInRetinaOutput
                .parameters.push_back(parameter);
        }
        responseWorkItem->output.outputResult.__isset
            .listParametersInRetinaOutput = true;
        break;
    }

    case XcalarApiGetStatGroupIdMap:
        responseWorkItem->output.outputResult.statGroupIdMapOutput
            .numGroupNames =
            workItem->output->outputResult.statGroupIdMapOutput.numGroupNames;

        responseWorkItem->output.outputResult.statGroupIdMapOutput.truncated =
            workItem->output->outputResult.statGroupIdMapOutput.truncated;

        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.statGroupIdMapOutput.numGroupNames;
             ii++) {
            XcalarStatGroupInfoT statGroupInfoToPush;
            statGroupInfoToPush.groupIdNum =
                workItem->output->outputResult.statGroupIdMapOutput
                    .groupNameInfo[ii]
                    .groupIdNum;
            statGroupInfoToPush.totalSingleStats =
                workItem->output->outputResult.statGroupIdMapOutput
                    .groupNameInfo[ii]
                    .totalSingleStats;
            statGroupInfoToPush.statsGroupName =
                workItem->output->outputResult.statGroupIdMapOutput
                    .groupNameInfo[ii]
                    .statsGroupName;

            responseWorkItem->output.outputResult.statGroupIdMapOutput
                .groupNameInfoArray.push_back(statGroupInfoToPush);
        }
        responseWorkItem->output.outputResult.__isset.statGroupIdMapOutput =
            true;
        break;

    case XcalarApiKeyList:
        responseWorkItem->output.outputResult.keyListOutput.numKeys =
            workItem->output->outputResult.keyListOutput.numKeys;

        for (int jj = 0;
             jj < workItem->output->outputResult.keyListOutput.numKeys;
             jj++) {
            responseWorkItem->output.outputResult.keyListOutput.keys.push_back(
                workItem->output->outputResult.keyListOutput.keys[jj]);
        }
        responseWorkItem->output.outputResult.__isset.keyListOutput = true;
        break;

    case XcalarApiGetQuery:
        responseWorkItem->output.outputResult.getQueryOutput.query =
            workItem->output->outputResult.getQueryOutput.query;
        responseWorkItem->output.outputResult.__isset.getQueryOutput = true;
        break;

    case XcalarApiStartFuncTests:
        responseWorkItem->output.outputResult.startFuncTestOutput.numTests =
            workItem->output->outputResult.startFuncTestOutput.numTests;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.startFuncTestOutput.numTests;
             ii++) {
            XcalarApiFuncTestOutputT funcTestOutput;
            FuncTestTypes::FuncTestOutput *funcTestOutputIn;

            funcTestOutputIn = &workItem->output->outputResult
                                    .startFuncTestOutput.testOutputs[ii];
            funcTestOutput.testName = funcTestOutputIn->testName;
            funcTestOutput.status = (StatusT::type) funcTestOutputIn->status;
            responseWorkItem->output.outputResult.startFuncTestOutput
                .testOutputs.push_back(funcTestOutput);
        }
        responseWorkItem->output.outputResult.__isset.startFuncTestOutput =
            true;
        break;

    case XcalarApiListFuncTests:
        responseWorkItem->output.outputResult.listFuncTestOutput.numTests =
            workItem->output->outputResult.listFuncTestOutput.numTests;
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.listFuncTestOutput.numTests;
             ii++) {
            responseWorkItem->output.outputResult.listFuncTestOutput.testNames
                .push_back(workItem->output->outputResult.listFuncTestOutput
                               .testName[ii]);
        }
        responseWorkItem->output.outputResult.__isset.listFuncTestOutput = true;
        break;

    case XcalarApiUdfGet: {
        UdfModuleSrc *udfGetOutput =
            &workItem->output->outputResult.udfGetOutput;

        responseWorkItem->output.outputResult.udfGetOutput.type =
            (UdfTypeT::type) udfGetOutput->type;

        responseWorkItem->output.outputResult.udfGetOutput.moduleName =
            udfGetOutput->moduleName;
        responseWorkItem->output.outputResult.udfGetOutput.source =
            udfGetOutput->source;

        responseWorkItem->output.outputResult.__isset.udfGetOutput = true;
        break;
    }

    case XcalarApiUdfDelete: {
        break;
    }

    // Session delete will return session not found if either there are
    // no sessions or there was not an exact match.  UI wants to be able
    // to ignore those cases, so we will change the status to StatusOk.
    case XcalarApiSessionDelete: {
        if (workItem->output->hdr.status == StatusSessionNotFound.code()) {
            workItem->output->hdr.status = StatusOk.code();
            responseWorkItem->output.hdr.status =
                (StatusT::type) StatusOk.code();
        }
        break;
    }

    case XcalarApiSupportGenerate: {
        responseWorkItem->output.outputResult.supportGenerateOutput.supportId =
            workItem->output->outputResult.supportGenerateOutput.supportId;
        responseWorkItem->output.outputResult.supportGenerateOutput.bundlePath =
            workItem->output->outputResult.supportGenerateOutput.bundlePath;
        responseWorkItem->output.outputResult.supportGenerateOutput
            .supportBundleSent = workItem->output->outputResult
                                     .supportGenerateOutput.supportBundleSent;
        responseWorkItem->output.outputResult.__isset.supportGenerateOutput =
            true;
        break;
    }

    case XcalarApiGetOpStats: {
        responseWorkItem->output.outputResult.opStatsOutput.api =
            (XcalarApisT::type)
                workItem->output->outputResult.opStatsOutput.api;

        copyXcalarApiOpDetails(responseWorkItem->output.outputResult
                                   .opStatsOutput.opDetails,
                               &workItem->output->outputResult.opStatsOutput
                                    .opDetails,
                               workItem->output->outputResult.opStatsOutput
                                   .api);
        responseWorkItem->output.outputResult.__isset.opStatsOutput = true;
        break;
    }
    case XcalarApiGetPerNodeOpStats: {
        XcalarApiPerNodeOpStats *perNodeOpStats =
            &workItem->output->outputResult.perNodeOpStatsOutput;
        responseWorkItem->output.outputResult.perNodeOpStatsOutput.numNodes =
            perNodeOpStats->numNodes;
        responseWorkItem->output.outputResult.perNodeOpStatsOutput.api =
            (XcalarApisT::type) perNodeOpStats->api;
        for (unsigned ii = 0; ii < perNodeOpStats->numNodes; ++ii) {
            XcalarApiNodeOpStatsT nodeOpStats;
            nodeOpStats.status =
                (StatusT::type) perNodeOpStats->nodeOpStats[ii].status;

            nodeOpStats.nodeId = perNodeOpStats->nodeOpStats[ii].nodeId;

            copyXcalarApiOpDetails(nodeOpStats.opDetails,
                                   &perNodeOpStats->nodeOpStats[ii].opDetails,
                                   perNodeOpStats->api);

            responseWorkItem->output.outputResult.perNodeOpStatsOutput
                .nodeOpStats.push_back(nodeOpStats);
        }
        responseWorkItem->output.outputResult.__isset.perNodeOpStatsOutput =
            true;
        break;
    }
    case XcalarApiTarget: {
        responseWorkItem->output.outputResult.targetOutput.outputJson =
            workItem->output->outputResult.targetOutput.outputJson;
        responseWorkItem->output.outputResult.__isset.targetOutput = true;
        break;
    }
    case XcalarApiPreview: {
        responseWorkItem->output.outputResult.previewOutput.outputJson =
            workItem->output->outputResult.previewOutput.outputJson;
        responseWorkItem->output.outputResult.__isset.previewOutput = true;
        break;
    }
    case XcalarApiExecuteRetina: {
        responseWorkItem->output.outputResult.executeRetinaOutput.tableName =
            workItem->output->outputResult.executeRetinaOutput.tableName;
        responseWorkItem->output.outputResult.__isset.executeRetinaOutput =
            true;
        break;
    }
    case XcalarApiGetConfigParams: {
        responseWorkItem->output.outputResult.getConfigParamsOutput.numParams =
            workItem->output->outputResult.getConfigParamsOutput.numParams;
        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.getConfigParamsOutput.numParams;
             ii++) {
            XcalarApiConfigParamT param;
            copyXcalarApiConfigParamOut(param,
                                        &workItem->output->outputResult
                                             .getConfigParamsOutput
                                             .parameter[ii]);
            responseWorkItem->output.outputResult.getConfigParamsOutput
                .parameter.push_back(param);
        }
        responseWorkItem->output.outputResult.__isset.getConfigParamsOutput =
            true;
        break;
    }
    case XcalarApiAppRun:
        responseWorkItem->output.outputResult.appRunOutput.appGroupId =
            std::to_string(
                workItem->output->outputResult.appRunOutput.appGroupId);
        responseWorkItem->output.outputResult.__isset.appRunOutput = true;
        break;

    case XcalarApiGetIpAddr:
        responseWorkItem->output.outputResult.getIpAddrOutput.ipAddr =
            workItem->output->outputResult.getIpAddrOutput.ipAddr;
        responseWorkItem->output.outputResult.__isset.getIpAddrOutput = true;
        break;

    case XcalarApiListDatasetUsers:
        responseWorkItem->output.outputResult.listDatasetUsersOutput
            .usersCount =
            workItem->output->outputResult.listDatasetUsersOutput.usersCount;
        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.listDatasetUsersOutput.usersCount;
             ii++) {
            XcalarApiDatasetUserT datasetUser;
            copyXcalarApiDatasetUserOut(datasetUser,
                                        &workItem->output->outputResult
                                             .listDatasetUsersOutput.user[ii]);
            responseWorkItem->output.outputResult.listDatasetUsersOutput.user
                .push_back(datasetUser);
        }
        responseWorkItem->output.outputResult.__isset.listDatasetUsersOutput =
            true;
        break;

    case XcalarApiListUserDatasets:
        responseWorkItem->output.outputResult.listUserDatasetsOutput
            .numDatasets =
            workItem->output->outputResult.listUserDatasetsOutput.numDatasets;
        for (unsigned ii = 0;
             ii <
             workItem->output->outputResult.listUserDatasetsOutput.numDatasets;
             ii++) {
            XcalarApiUserDatasetT userDataset;
            copyXcalarApiUserDatasetsOut(userDataset,
                                         &workItem->output->outputResult
                                              .listUserDatasetsOutput
                                              .datasets[ii]);
            responseWorkItem->output.outputResult.listUserDatasetsOutput
                .datasets.push_back(userDataset);
        }
        responseWorkItem->output.outputResult.__isset.listUserDatasetsOutput =
            true;
        break;

    case XcalarApiGetNumNodes:
        responseWorkItem->output.outputResult.getNumNodesOutput.numNodes =
            workItem->output->outputResult.getNumNodesOutput.numNodes;
        responseWorkItem->output.outputResult.__isset.getNumNodesOutput = true;
        break;

    case XcalarApiSessionDownload: {
        XcalarApiSessionDownloadOutput *sessionDownloadOutput;
        Status tmpStatus;
        char *sessionContentBuf = NULL;
        size_t sessionContentSize = 0;

        sessionDownloadOutput =
            &workItem->output->outputResult.sessionDownloadOutput;
        tmpStatus = base64Encode(sessionDownloadOutput->sessionContent,
                                 sessionDownloadOutput->sessionContentCount,
                                 &sessionContentBuf,
                                 &sessionContentSize);
        if (tmpStatus != StatusOk) {
            responseWorkItem->output.hdr.status =
                (StatusT::type) tmpStatus.code();
        } else {
            std::string sessionString(sessionContentBuf, sessionContentSize);
            responseWorkItem->output.outputResult.sessionDownloadOutput
                .sessionContent = sessionString;
            responseWorkItem->output.outputResult.sessionDownloadOutput
                .sessionContentCount = sessionContentSize;
            // allocated by base64Encode()
            memFree(sessionContentBuf);
        }
        responseWorkItem->output.outputResult.__isset.sessionDownloadOutput =
            true;
        break;
    }

    case XcalarApiSessionUpload: {
        char sessionIdStr[SessionMgr::MaxTextHexIdSize];
        snprintf(sessionIdStr,
                 sizeof(sessionIdStr),
                 "%lX",
                 workItem->output->outputResult.sessionNewOutput.sessionId);
        responseWorkItem->output.outputResult.sessionNewOutput.sessionId =
            sessionIdStr;
        responseWorkItem->output.outputResult.__isset.sessionNewOutput = true;
        break;
    }

    case XcalarApiUpdate: {
        for (unsigned ii = 0;
             ii < workItem->output->outputResult.updateOutput.numUpdates;
             ii++) {
            responseWorkItem->output.outputResult.updateOutput.batchIds
                .push_back(
                    workItem->output->outputResult.updateOutput.batchIds[ii]);
        }
        responseWorkItem->output.outputResult.__isset.updateOutput = true;
        break;
    }

    case XcalarApiCreateDht:
    case XcalarApiDeleteDht:
    case XcalarApiDeleteRetina:
    case XcalarApiUpdateRetina:
    case XcalarApiResetStat:
    case XcalarApiExport:
    case XcalarApiRenameNode:
    case XcalarApiTagDagNodes:
    case XcalarApiCommentDagNodes:
    case XcalarApiMakeRetina:
    case XcalarApiKeyAddOrReplace:
    case XcalarApiKeyAppend:
    case XcalarApiKeySetIfEqual:
    case XcalarApiKeyDelete:
    case XcalarApiSessionInact:
    case XcalarApiSessionActivate:
    case XcalarApiSessionRename:
    case XcalarApiCancelOp:
    case XcalarApiQueryCancel:
    case XcalarApiQueryDelete:
    case XcalarApiSetConfigParam:
    case XcalarApiAppSet:
    case XcalarApiDatasetCreate:
    case XcalarApiDatasetDelete:
    case XcalarApiPublish:
    case XcalarApiUnpublish:
    case XcalarApiCoalesce:
    case XcalarApiRuntimeSetParam:
        // do nothing
        break;

    case XcalarApiUdfAdd:
    case XcalarApiUdfUpdate:
    case XcalarApiBulkLoad:
    case XcalarApiAppReap:
    case XcalarApiDeleteObjects:
    case XcalarApiArchiveTables:
    case XcalarApiRestoreTable:
        // Already handled.
        break;

    case XcalarApiRuntimeGetParam: {
        uint8_t schedParamsCount = workItem->output->outputResult
                                       .runtimeGetParamOutput.schedParamsCount;
        assert(Runtime::TotalSdkScheds == schedParamsCount);
        for (uint8_t ii = 0; ii < schedParamsCount; ii++) {
            XcalarApiSchedParamT schedParam;
            schedParam.__set_schedName(
                workItem->output->outputResult.runtimeGetParamOutput
                    .schedParams[ii]
                    .schedName);
            schedParam.__set_cpusReservedInPercent(
                workItem->output->outputResult.runtimeGetParamOutput
                    .schedParams[ii]
                    .cpusReservedInPct);

            RuntimeType rtType =
                workItem->output->outputResult.runtimeGetParamOutput
                    .schedParams[ii]
                    .runtimeType;

            RuntimeTypeT::type rtTypeT = RuntimeTypeT::Invalid;

            switch (rtType) {
            case RuntimeTypeLatency:
                rtTypeT = RuntimeTypeT::Latency;
                break;
            case RuntimeTypeThroughput:
                rtTypeT = RuntimeTypeT::Throughput;
                break;
            case RuntimeTypeImmediate:
                rtTypeT = RuntimeTypeT::Immediate;
                break;
            case RuntimeTypeInvalid:  // pass through
            default:
                rtTypeT = RuntimeTypeT::Invalid;
                break;
            }

            schedParam.__set_runtimeType(rtTypeT);

            responseWorkItem->output.outputResult.runtimeGetParamOutput
                .schedParams.push_back(schedParam);
        }
        responseWorkItem->output.outputResult.__isset.runtimeGetParamOutput =
            true;
        break;
    }

    default:
        exception.status = StatusT::StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "%s:%s() unimplemented api:%s",
                __FILE__,
                __func__,
                strGetFromXcalarApis((XcalarApis) requestWorkItem->api));
        xcalarApiFreeWorkItem(workItem);
        assert(0);
        throw exception;
    }

CommonExit:
    if (workItem != NULL) {
        if (workItem->output != NULL) {
            uint64_t milliseconds = workItem->output->hdr.elapsed.milliseconds;

            uint64_t hours = milliseconds / (MSecsPerSec * SecsPerHour);
            milliseconds -= hours * (MSecsPerSec * SecsPerHour);

            uint64_t minutes = milliseconds / (MSecsPerSec * SecsPerMinute);
            milliseconds -= minutes * (MSecsPerSec * SecsPerMinute);

            uint64_t seconds = milliseconds / (MSecsPerSec);
            milliseconds -= seconds * (MSecsPerSec);

            xSyslog(moduleName,
                    XlogInfo,
                    "%s:%s(%s) return in "
                    "%lu:%02lu:%02lu.%03lu: status: %s",
                    __FILE__,
                    __func__,
                    strGetFromXcalarApis((XcalarApis) requestWorkItem->api),
                    hours,
                    minutes,
                    seconds,
                    milliseconds,
                    strGetFromStatusCode(workItem->output->hdr.status));
        }

        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

static Status
parseCPlusPlusWorkItem(const XcalarApiWorkItemT &workItemIn,
                       const XcalarApisT::type api,
                       XcalarWorkItem **workItemRetp)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    unsigned ii;
    XcalarApiException exception;
    size_t ret;
    DfLoadArgs *loadArgs = NULL;

    switch ((XcalarApis) api) {
    case XcalarApiGetVersion:
        workItem = xcalarApiMakeGetVersionWorkItem();
        break;

    case XcalarApiBulkLoad: {
        loadArgs = (DfLoadArgs *) memCalloc(1, sizeof(*loadArgs));
        BailIfNull(loadArgs);
        memZero(loadArgs, sizeof(*loadArgs));

        if (workItemIn.input.loadInput.dest.size() == 0) {
            // invalid dataset name
            status = StatusInval;
            goto CommonExit;
        }

        copyParamString(loadArgs->parseArgs.parserArgJson,
                        workItemIn.input.loadInput.loadArgs.parseArgs
                            .parserArgJson.c_str(),
                        sizeof(loadArgs->parseArgs.parserArgJson));

        // XXX structure of workItem allocation prevents us from avoid this
        // extra copy without non-intuitively partially populating loadArgs
        copyParamString(loadArgs->parseArgs.parserFnName,
                        workItemIn.input.loadInput.loadArgs.parseArgs
                            .parserFnName.c_str(),
                        sizeof(loadArgs->parseArgs.parserFnName));

        copyParamString(loadArgs->parseArgs.fileNameFieldName,
                        workItemIn.input.loadInput.loadArgs.parseArgs
                            .fileNameFieldName.c_str(),
                        sizeof(loadArgs->parseArgs.fileNameFieldName));

        copyParamString(loadArgs->parseArgs.recordNumFieldName,
                        workItemIn.input.loadInput.loadArgs.parseArgs
                            .recordNumFieldName.c_str(),
                        sizeof(loadArgs->parseArgs.recordNumFieldName));

        loadArgs->parseArgs.allowFileErrors =
            workItemIn.input.loadInput.loadArgs.parseArgs.allowFileErrors;

        loadArgs->parseArgs.allowRecordErrors =
            workItemIn.input.loadInput.loadArgs.parseArgs.allowRecordErrors;

        int numSourceArgs =
            workItemIn.input.loadInput.loadArgs.sourceArgsList.size();
        for (int ii = 0; ii < numSourceArgs; ii++) {
            DataSourceArgs *sourceArgsDst = &loadArgs->sourceArgsList[ii];
            const DataSourceArgsT sourceArgsSrc =
                workItemIn.input.loadInput.loadArgs.sourceArgsList[ii];
            ret = strlcpy(sourceArgsDst->targetName,
                          sourceArgsSrc.targetName.c_str(),
                          sizeof(sourceArgsDst->targetName));
            if (ret >= sizeof(sourceArgsDst->targetName)) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            ret = strlcpy(sourceArgsDst->path,
                          sourceArgsSrc.path.c_str(),
                          sizeof(sourceArgsDst->path));
            if (ret >= sizeof(sourceArgsDst->path)) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            ret = strlcpy(sourceArgsDst->fileNamePattern,
                          sourceArgsSrc.fileNamePattern.c_str(),
                          sizeof(sourceArgsDst->fileNamePattern));
            if (ret >= sizeof(sourceArgsDst->fileNamePattern)) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            sourceArgsDst->recursive = sourceArgsSrc.recursive;
        }
        loadArgs->sourceArgsListCount = numSourceArgs;

        loadArgs->maxSize = workItemIn.input.loadInput.loadArgs.size;
        loadArgs->parseArgs.fieldNamesCount =
            workItemIn.input.loadInput.loadArgs.parseArgs.schema.size();

        for (ii = 0; ii < loadArgs->parseArgs.fieldNamesCount; ii++) {
            const XcalarApiColumnT *columnIn =
                &workItemIn.input.loadInput.loadArgs.parseArgs.schema[ii];

            strlcpy(loadArgs->parseArgs.oldNames[ii],
                    columnIn->sourceColumn.c_str(),
                    sizeof(loadArgs->parseArgs.oldNames[ii]));

            strlcpy(loadArgs->parseArgs.fieldNames[ii],
                    columnIn->sourceColumn.c_str(),
                    sizeof(loadArgs->parseArgs.fieldNames[ii]));

            loadArgs->parseArgs.types[ii] =
                strToDfFieldType(columnIn->columnType.c_str());
        }

        workItem = xcalarApiMakeBulkLoadWorkItem(workItemIn.input.loadInput.dest
                                                     .c_str(),
                                                 loadArgs);

        break;
    }
    case XcalarApiIndex: {
        unsigned numKeys = workItemIn.input.indexInput.key.size();
        if (numKeys == 0 || numKeys >= TupleMaxNumValuesPerRecord) {
            exception.status = StatusT::StatusInval;
            throw exception;
        }

        DfFieldType keyType[numKeys];
        const char *keyName[numKeys];
        const char *keyFieldName[numKeys];
        Ordering keyOrdering[numKeys];

        for (ii = 0; ii < numKeys; ii++) {
            keyType[ii] = strToDfFieldType(
                workItemIn.input.indexInput.key[ii].type.c_str());
            if (!isValidDfFieldType(keyType[ii])) {
                keyType[ii] = DfUnknown;
            }

            keyName[ii] = workItemIn.input.indexInput.key[ii].name.c_str();
            keyFieldName[ii] =
                workItemIn.input.indexInput.key[ii].keyFieldName.c_str();
            keyOrdering[ii] = strToOrdering(
                workItemIn.input.indexInput.key[ii].ordering.c_str());
        }

        workItem =
            xcalarApiMakeIndexWorkItem(NULL,
                                       workItemIn.input.indexInput.source
                                           .c_str(),
                                       numKeys,
                                       keyName,
                                       keyFieldName,
                                       keyType,
                                       keyOrdering,
                                       workItemIn.input.indexInput.dest.c_str(),
                                       workItemIn.input.indexInput.dhtName
                                           .c_str(),
                                       workItemIn.input.indexInput.prefix
                                           .c_str(),
                                       workItemIn.input.indexInput.delaySort,
                                       workItemIn.input.indexInput.broadcast);
        break;
    }

    case XcalarApiDatasetCreate: {
        loadArgs = (DfLoadArgs *) memCalloc(1, sizeof(*loadArgs));
        BailIfNull(loadArgs);
        memZero(loadArgs, sizeof(*loadArgs));

        if (workItemIn.input.datasetCreateInput.dest.size() == 0) {
            // invalid dataset name
            status = StatusInval;
            goto CommonExit;
        }

        copyParamString(loadArgs->parseArgs.parserArgJson,
                        workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                            .parserArgJson.c_str(),
                        sizeof(loadArgs->parseArgs.parserArgJson));

        // XXX structure of workItem allocation prevents us from avoiding this
        // extra copy without non-intuitively partially populating loadArgs
        copyParamString(loadArgs->parseArgs.parserFnName,
                        workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                            .parserFnName.c_str(),
                        sizeof(loadArgs->parseArgs.parserFnName));

        copyParamString(loadArgs->parseArgs.fileNameFieldName,
                        workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                            .fileNameFieldName.c_str(),
                        sizeof(loadArgs->parseArgs.fileNameFieldName));

        copyParamString(loadArgs->parseArgs.recordNumFieldName,
                        workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                            .recordNumFieldName.c_str(),
                        sizeof(loadArgs->parseArgs.recordNumFieldName));

        loadArgs->parseArgs.allowFileErrors =
            workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                .allowFileErrors;

        loadArgs->parseArgs.allowRecordErrors =
            workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                .allowRecordErrors;

        int numSourceArgs =
            workItemIn.input.datasetCreateInput.loadArgs.sourceArgsList.size();
        for (int ii = 0; ii < numSourceArgs; ii++) {
            DataSourceArgs *sourceArgsDst = &loadArgs->sourceArgsList[ii];
            const DataSourceArgsT sourceArgsSrc =
                workItemIn.input.datasetCreateInput.loadArgs.sourceArgsList[ii];
            ret = strlcpy(sourceArgsDst->targetName,
                          sourceArgsSrc.targetName.c_str(),
                          sizeof(sourceArgsDst->targetName));
            if (ret >= sizeof(sourceArgsDst->targetName)) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            ret = strlcpy(sourceArgsDst->path,
                          sourceArgsSrc.path.c_str(),
                          sizeof(sourceArgsDst->path));
            if (ret >= sizeof(sourceArgsDst->path)) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            ret = strlcpy(sourceArgsDst->fileNamePattern,
                          sourceArgsSrc.fileNamePattern.c_str(),
                          sizeof(sourceArgsDst->fileNamePattern));
            if (ret >= sizeof(sourceArgsDst->fileNamePattern)) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            sourceArgsDst->recursive = sourceArgsSrc.recursive;
        }
        loadArgs->sourceArgsListCount = numSourceArgs;

        loadArgs->maxSize = workItemIn.input.datasetCreateInput.loadArgs.size;
        loadArgs->parseArgs.fieldNamesCount =
            workItemIn.input.datasetCreateInput.loadArgs.parseArgs.schema
                .size();

        for (ii = 0; ii < loadArgs->parseArgs.fieldNamesCount; ii++) {
            const XcalarApiColumnT *columnIn =
                &workItemIn.input.datasetCreateInput.loadArgs.parseArgs
                     .schema[ii];

            strlcpy(loadArgs->parseArgs.oldNames[ii],
                    columnIn->sourceColumn.c_str(),
                    sizeof(loadArgs->parseArgs.oldNames[ii]));

            strlcpy(loadArgs->parseArgs.fieldNames[ii],
                    columnIn->sourceColumn.c_str(),
                    sizeof(loadArgs->parseArgs.fieldNames[ii]));

            loadArgs->parseArgs.types[ii] =
                strToDfFieldType(columnIn->columnType.c_str());
        }

        workItem = xcalarApiMakeDatasetCreateWorkItem(workItemIn.input
                                                          .datasetCreateInput
                                                          .dest.c_str(),
                                                      loadArgs);
        break;
    }

    case XcalarApiDatasetGetMeta:
        workItem = xcalarApiMakeDatasetGetMetaWorkItem(
            workItemIn.input.datasetGetMetaInput.datasetName.c_str());
        break;

    case XcalarApiDatasetDelete:
        workItem = xcalarApiMakeDatasetDeleteWorkItem(
            workItemIn.input.datasetDeleteInput.datasetName.c_str());
        break;

    case XcalarApiDatasetUnload:
        workItem = xcalarApiMakeDatasetUnloadWorkItem(
            workItemIn.input.datasetUnloadInput.datasetNamePattern.c_str());
        break;

    case XcalarApiDeleteObjects:
        workItem =
            xcalarApiMakeDeleteDagNodesWorkItem(workItemIn.input
                                                    .deleteDagNodeInput
                                                    .namePattern.c_str(),
                                                (SourceType) workItemIn.input
                                                    .deleteDagNodeInput.srcType,
                                                NULL,
                                                workItemIn.input
                                                    .deleteDagNodeInput
                                                    .deleteCompletely);
        break;

    case XcalarApiArchiveTables: {
        unsigned numTables =
            workItemIn.input.archiveTablesInput.tableNames.size();
        if (numTables == 0) {
            exception.status = StatusT::StatusInval;
            throw exception;
        }

        const char **tableNames =
            (const char **) memCalloc(1, numTables * sizeof(*tableNames));
        if (tableNames == NULL) {
            exception.status = StatusT::StatusNoMem;
            throw exception;
        }

        for (unsigned ii = 0; ii < numTables; ii++) {
            tableNames[ii] =
                workItemIn.input.archiveTablesInput.tableNames[ii].c_str();
        }

        workItem =
            xcalarApiMakeArchiveTablesWorkItem(workItemIn.input
                                                   .archiveTablesInput.archive,
                                               workItemIn.input
                                                   .archiveTablesInput
                                                   .allTables,
                                               numTables,
                                               tableNames);
        memFree(tableNames);

        break;
    }

    case XcalarApiListExportTargets:
        workItem = xcalarApiMakeListExportTargetsWorkItem(
            workItemIn.input.listTargetsInput.targetTypePattern.c_str(),
            workItemIn.input.listTargetsInput.targetNamePattern.c_str());
        break;

    case XcalarApiExport: {
        unsigned numColumns = workItemIn.input.exportInput.columns.size();

        if (numColumns > TupleMaxNumValuesPerRecord) {
            exception.status = StatusT::StatusExportTooManyColumns;
            throw exception;
        }

        ExColumnName columns[numColumns];

        // Copy the thrift c++ strings into a c struct
        for (ii = 0; ii < numColumns; ii++) {
            copyParamString(columns[ii].name,
                            workItemIn.input.exportInput.columns[ii]
                                .columnName.c_str(),
                            sizeof(columns[ii].name));
            copyParamString(columns[ii].headerAlias,
                            workItemIn.input.exportInput.columns[ii]
                                .headerName.c_str(),
                            sizeof(columns[ii].headerAlias));
        }

        // Make the work item, now that we have all component structures
        workItem = xcalarApiMakeExportWorkItem(workItemIn.input.exportInput
                                                   .source.c_str(),
                                               workItemIn.input.exportInput.dest
                                                   .c_str(),
                                               numColumns,
                                               columns,
                                               workItemIn.input.exportInput
                                                   .driverName.c_str(),
                                               workItemIn.input.exportInput
                                                   .driverParams.c_str());
        break;
    }

    case XcalarApiGetTableMeta:
        workItem = xcalarApiMakeGetTableMeta(workItemIn.input.getTableMetaInput
                                                 .tableNameInput.name.c_str(),
                                             workItemIn.input.getTableMetaInput
                                                 .tableNameInput.isTable,
                                             workItemIn.input.getTableMetaInput
                                                 .isPrecise);
        break;

    case XcalarApiListDagNodeInfo:
        workItem =
            xcalarApiMakeListDagNodesWorkItem(workItemIn.input.listDagNodesInput
                                                  .namePattern.c_str(),
                                              (SourceType) workItemIn.input
                                                  .listDagNodesInput.srcType);
        break;

    case XcalarApiQueryState:
        workItem =
            xcalarApiMakeQueryStateWorkItem(workItemIn.input.queryStateInput
                                                .queryName.c_str(),
                                            workItemIn.input.queryStateInput
                                                .detailedStats);
        break;

    case XcalarApiQueryCancel:
        workItem = xcalarApiMakeQueryCancelWorkItem(
            workItemIn.input.queryStateInput.queryName.c_str());
        break;

    case XcalarApiQueryDelete:
        workItem = xcalarApiMakeQueryDeleteWorkItem(
            workItemIn.input.queryStateInput.queryName.c_str());
        break;

    case XcalarApiGetDag:
        workItem = xcalarApiMakeDagWorkItem(
            workItemIn.input.dagTableNameInput.tableInput.c_str());
        break;

    case XcalarApiCancelOp:
        workItem = xcalarApiMakeCancelOpWorkItem(
            workItemIn.input.dagTableNameInput.tableInput.c_str());
        break;

    case XcalarApiGetPerNodeOpStats:
        workItem = xcalarApiMakePerNodeOpStatsWorkItem(
            workItemIn.input.dagTableNameInput.tableInput.c_str());
        break;

    case XcalarApiGetOpStats:
        workItem = xcalarApiMakeOpStatsWorkItem(
            workItemIn.input.dagTableNameInput.tableInput.c_str());
        break;

    case XcalarApiListDatasets:
        workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasets, NULL, 0);
        break;

    case XcalarApiGetDatasetsInfo:
        workItem = xcalarApiMakeGetDatasetsInfoWorkItem(
            workItemIn.input.getDatasetsInfoInput.datasetsNamePattern.c_str());
        break;

    case XcalarApiShutdown:
        workItem = xcalarApiMakeShutdownWorkItem(false, /* local */
                                                 workItemIn.input.shutdownInput
                                                     .doShutdown);
        break;

    case XcalarApiGetStat:
        workItem =
            xcalarApiMakeGetStatWorkItem(workItemIn.input.statInput.nodeId);
        break;

    case XcalarApiResetStat:
        workItem =
            xcalarApiMakeResetStatWorkItem(workItemIn.input.statInput.nodeId);
        break;

    case XcalarApiJoin: {
        unsigned numLeftCols = workItemIn.input.joinInput.columns[0].size();
        unsigned numRightCols = workItemIn.input.joinInput.columns[1].size();
        XcalarApiRenameMap columns[numLeftCols + numRightCols];
        unsigned jj = 0;

        // Copy the thrift c++ strings into a c struct
        for (ii = 0; ii < numLeftCols; ii++) {
            copyParamString(columns[jj].oldName,
                            workItemIn.input.joinInput.columns[0][ii]
                                .sourceColumn.c_str(),
                            sizeof(columns[jj].oldName));

            copyParamString(columns[jj].newName,
                            workItemIn.input.joinInput.columns[0][ii]
                                .destColumn.c_str(),
                            sizeof(columns[jj].newName));

            columns[jj].type = strToDfFieldType(
                workItemIn.input.joinInput.columns[0][ii].columnType.c_str());
            columns[jj].isKey = false;
            jj++;
        }

        for (ii = 0; ii < numRightCols; ii++) {
            copyParamString(columns[jj].oldName,
                            workItemIn.input.joinInput.columns[1][ii]
                                .sourceColumn.c_str(),
                            sizeof(columns[jj].oldName));

            copyParamString(columns[jj].newName,
                            workItemIn.input.joinInput.columns[1][ii]
                                .destColumn.c_str(),
                            sizeof(columns[jj].newName));

            columns[jj].type = strToDfFieldType(
                workItemIn.input.joinInput.columns[1][ii].columnType.c_str());
            columns[jj].isKey = false;
            jj++;
        }

        assert(jj == numLeftCols + numRightCols);

        workItem =
            xcalarApiMakeJoinWorkItem(workItemIn.input.joinInput.source[0]
                                          .c_str(),
                                      workItemIn.input.joinInput.source[1]
                                          .c_str(),
                                      workItemIn.input.joinInput.dest.c_str(),
                                      strToJoinOperator(
                                          workItemIn.input.joinInput.joinType
                                              .c_str()),
                                      true,
                                      workItemIn.input.joinInput.keepAllColumns,
                                      false,
                                      numLeftCols,
                                      numRightCols,
                                      columns,
                                      workItemIn.input.joinInput.evalString
                                          .c_str());
        break;
    }
    case XcalarApiUnion: {
        unsigned numSrcTables = workItemIn.input.unionInput.source.size();
        unsigned numRenameMaps = workItemIn.input.unionInput.columns.size();

        if (numRenameMaps != numSrcTables) {
            exception.status = StatusT::StatusInval;
            throw exception;
        }

        const char *srcNames[numSrcTables];
        unsigned columnsSizes[numSrcTables];
        XcalarApiRenameMap *columns[numSrcTables];
        unsigned jj = 0;

        try {
            // Copy the thrift c++ strings into a c struct
            for (ii = 0; ii < numSrcTables; ii++) {
                size_t size = workItemIn.input.unionInput.columns[ii].size();
                srcNames[ii] = workItemIn.input.unionInput.source[ii].c_str();
                columnsSizes[ii] = size;
                columns[ii] = (XcalarApiRenameMap *)
                    memCalloc(1, size * sizeof(*columns[ii]));
                if (columns[ii] == NULL) {
                    exception.status = StatusT::StatusNoMem;
                    throw exception;
                }
                for (jj = 0; jj < size; jj++) {
                    copyParamString(columns[ii][jj].oldName,
                                    workItemIn.input.unionInput.columns[ii][jj]
                                        .sourceColumn.c_str(),
                                    sizeof(columns[ii][jj].oldName));

                    copyParamString(columns[ii][jj].newName,
                                    workItemIn.input.unionInput.columns[ii][jj]
                                        .destColumn.c_str(),
                                    sizeof(columns[ii][jj].newName));

                    columns[ii][jj].type = strToDfFieldType(
                        workItemIn.input.unionInput.columns[ii][jj]
                            .columnType.c_str());
                    columns[ii][jj].isKey = false;
                }
            }
        } catch (...) {
            for (jj = 0; jj < ii; jj++) {
                assert(columns[jj] != NULL);
                memFree(columns[jj]);
                columns[jj] = NULL;
            }
            throw;
        }

        UnionOperator unionType =
            strToUnionOperator(workItemIn.input.unionInput.unionType.c_str());
        workItem =
            xcalarApiMakeUnionWorkItem(numSrcTables,
                                       srcNames,
                                       workItemIn.input.unionInput.dest.c_str(),
                                       columnsSizes,
                                       columns,
                                       workItemIn.input.unionInput.dedup,
                                       unionType);
        break;
    }

    case XcalarApiProject: {
        unsigned numColumns = workItemIn.input.projectInput.columns.size();
        if (numColumns > TupleMaxNumValuesPerRecord) {
            break;
        }
        char columns[numColumns][DfMaxFieldNameLen + 1];

        // Copy the thrift c++ strings into a c struct
        for (ii = 0; ii < numColumns; ii++) {
            copyParamString(columns[ii],
                            workItemIn.input.projectInput.columns[ii].c_str(),
                            sizeof(columns[ii]));
        }

        workItem = xcalarApiMakeProjectWorkItem(numColumns,
                                                columns,
                                                workItemIn.input.projectInput
                                                    .source.c_str(),
                                                workItemIn.input.projectInput
                                                    .dest.c_str());
        break;
    }
    case XcalarApiFilter:
        workItem =
            xcalarApiMakeFilterWorkItem(workItemIn.input.filterInput.eval[0]
                                            .evalString.c_str(),
                                        workItemIn.input.filterInput.source
                                            .c_str(),
                                        workItemIn.input.filterInput.dest
                                            .c_str());
        break;

    case XcalarApiGroupBy: {
        unsigned numEvals = workItemIn.input.groupByInput.eval.size();
        if (numEvals >= TupleMaxNumValuesPerRecord) {
            exception.status = StatusT::StatusEvalStringTooLong;
            throw exception;
        }

        const char *evalStrs[numEvals];
        const char *newFieldNames[numEvals];
        unsigned numKeys = 0;
        XcalarApiKeyInput key;
        memZero(&key, sizeof(key));

        for (ii = 0; ii < numEvals; ii++) {
            evalStrs[ii] =
                workItemIn.input.groupByInput.eval[ii].evalString.c_str();
            newFieldNames[ii] =
                workItemIn.input.groupByInput.eval[ii].newField.c_str();
        }

        // legacy query, only newKeyFieldName was specified
        if (strlen(workItemIn.input.groupByInput.newKeyField.c_str()) > 0) {
            key.keyName[0] = '\0';
            strlcpy(key.keyFieldName,
                    workItemIn.input.groupByInput.newKeyField.c_str(),
                    sizeof(key.keyFieldName));
            numKeys = 1;
        }

        workItem =
            xcalarApiMakeGroupByWorkItem(workItemIn.input.groupByInput.source
                                             .c_str(),
                                         workItemIn.input.groupByInput.dest
                                             .c_str(),
                                         numKeys,
                                         &key,
                                         workItemIn.input.groupByInput
                                             .includeSample,
                                         workItemIn.input.groupByInput.icv,
                                         workItemIn.input.groupByInput.groupAll,
                                         numEvals,
                                         evalStrs,
                                         newFieldNames);
        break;
    }
    case XcalarApiAggregate:
        workItem =
            xcalarApiMakeAggregateWorkItem(workItemIn.input.aggregateInput
                                               .source.c_str(),
                                           workItemIn.input.aggregateInput.dest
                                               .c_str(),
                                           workItemIn.input.aggregateInput
                                               .eval[0]
                                               .evalString.c_str());
        break;

    case XcalarApiRenameNode:
        workItem =
            xcalarApiMakeRenameNodeWorkItem(workItemIn.input.renameNodeInput
                                                .oldName.c_str(),
                                            workItemIn.input.renameNodeInput
                                                .newName.c_str());
        break;

    case XcalarApiTagDagNodes: {
        unsigned numNodes = workItemIn.input.tagDagNodesInput.dagNodes.size();
        DagTypes::NamedInput dagNodes[numNodes];

        size_t ret;
        for (unsigned ii = 0; ii < numNodes; ii++) {
            ret = strlcpy(dagNodes[ii].name,
                          workItemIn.input.tagDagNodesInput.dagNodes[ii]
                              .name.c_str(),
                          XcalarApiMaxTableNameLen);
            if (ret >= XcalarApiMaxTableNameLen) {
                exception.status = StatusT::StatusNameTooLong;
                throw exception;
            }

            if (strlen(workItemIn.input.tagDagNodesInput.dagNodes[ii]
                           .nodeId.c_str()) > 0) {
                dagNodes[ii].nodeId = std::stoul(
                    workItemIn.input.tagDagNodesInput.dagNodes[ii].nodeId);
            }
        }

        if (strlen(workItemIn.input.tagDagNodesInput.tag.c_str()) >=
            XcalarApiMaxDagNodeTagLen) {
            exception.status = StatusT::StatusNameTooLong;
            throw exception;
        }

        workItem =
            xcalarApiMakeTagDagNodesWorkItem(numNodes,
                                             dagNodes,
                                             workItemIn.input.tagDagNodesInput
                                                 .tag.c_str());
        break;
    }

    case XcalarApiCommentDagNodes: {
        unsigned numNodes = workItemIn.input.commentDagNodesInput.numDagNodes;
        char dagNodeNames[numNodes][XcalarApiMaxTableNameLen];

        size_t ret;
        for (unsigned ii = 0; ii < numNodes; ii++) {
            ret = strlcpy(dagNodeNames[ii],
                          workItemIn.input.commentDagNodesInput.dagNodeNames[ii]
                              .c_str(),
                          XcalarApiMaxTableNameLen);
            if (ret >= XcalarApiMaxTableNameLen) {
                exception.status = StatusT::StatusNameTooLong;
                throw exception;
            }
        }

        if (strlen(workItemIn.input.commentDagNodesInput.comment.c_str()) >=
            XcalarApiMaxDagNodeCommentLen) {
            exception.status = StatusT::StatusNameTooLong;
            throw exception;
        }

        workItem =
            xcalarApiMakeCommentDagNodesWorkItem(numNodes,
                                                 dagNodeNames,
                                                 workItemIn.input
                                                     .commentDagNodesInput
                                                     .comment.c_str());
        break;
    }

    case XcalarApiGetTableRefCount: {
        XcalarApiTableInput *input;
        input =
            (XcalarApiTableInput *) memCallocExt(1, sizeof(*input), moduleName);
        if (input == NULL) {
            exception.status = StatusT::StatusNoMem;
            throw exception;
        }

        try {
            copyParamString(input->tableName,
                            workItemIn.input.getTableRefCountInput.tableName
                                .c_str(),
                            sizeof(input->tableName));
        } catch (...) {
            memFree(input);
            input = NULL;
            throw;
        }

        workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetTableRefCount,
                                                input,
                                                sizeof(*input));
        break;
    }

    case XcalarApiGetStatByGroupId:
        // this api has been deprecated
        assert(0);
        break;

    case XcalarApiGetStatGroupIdMap:
        workItem = xcalarApiMakeGetStatGroupIdMapWorkItem(
            workItemIn.input.statInput.nodeId);
        break;

    case XcalarApiMap: {
        unsigned numEvals = workItemIn.input.mapInput.eval.size();
        if (numEvals >= TupleMaxNumValuesPerRecord) {
            exception.status = StatusT::Status2Big;
            throw exception;
        }

        const char *evalStrs[numEvals];
        const char *newFieldNames[numEvals];

        for (ii = 0; ii < numEvals; ii++) {
            evalStrs[ii] =
                workItemIn.input.mapInput.eval[ii].evalString.c_str();
            newFieldNames[ii] =
                workItemIn.input.mapInput.eval[ii].newField.c_str();
        }

        workItem =
            xcalarApiMakeMapWorkItem(workItemIn.input.mapInput.source.c_str(),
                                     workItemIn.input.mapInput.dest.c_str(),
                                     workItemIn.input.mapInput.icv,
                                     numEvals,
                                     evalStrs,
                                     newFieldNames);

        break;
    }
    case XcalarApiSynthesize: {
        unsigned numColumns = workItemIn.input.synthesizeInput.columns.size();
        if (numColumns > TupleMaxNumValuesPerRecord) {
            exception.status = StatusT::Status2Big;
            throw exception;
        }

        XcalarApiRenameMap columns[numColumns];

        for (ii = 0; ii < numColumns; ii++) {
            const XcalarApiColumnT *columnIn =
                &workItemIn.input.synthesizeInput.columns[ii];

            strlcpy(columns[ii].oldName,
                    columnIn->sourceColumn.c_str(),
                    sizeof(columns[ii].oldName));

            strlcpy(columns[ii].newName,
                    columnIn->destColumn.c_str(),
                    sizeof(columns[ii].newName));

            columns[ii].type = strToDfFieldType(columnIn->columnType.c_str());
            columns[ii].isKey = false;
        }
        workItem =
            xcalarApiMakeSynthesizeWorkItem(workItemIn.input.synthesizeInput
                                                .source.c_str(),
                                            workItemIn.input.synthesizeInput
                                                .dest.c_str(),
                                            true,
                                            numColumns,
                                            columns);
        break;
    }
    case XcalarApiSelect: {
        unsigned numColumns = workItemIn.input.selectInput.columns.size();
        if (numColumns > TupleMaxNumValuesPerRecord) {
            exception.status = StatusT::Status2Big;
            throw exception;
        }

        XcalarApiRenameMap columns[numColumns];

        for (ii = 0; ii < numColumns; ii++) {
            const XcalarApiColumnT *columnIn =
                &workItemIn.input.selectInput.columns[ii];

            strlcpy(columns[ii].oldName,
                    columnIn->sourceColumn.c_str(),
                    sizeof(columns[ii].oldName));

            strlcpy(columns[ii].newName,
                    columnIn->destColumn.c_str(),
                    sizeof(columns[ii].newName));

            columns[ii].type = strToDfFieldType(columnIn->columnType.c_str());
            columns[ii].isKey = false;
        }

        workItem =
            xcalarApiMakeSelectWorkItem(workItemIn.input.selectInput.source
                                            .c_str(),
                                        workItemIn.input.selectInput.dest
                                            .c_str(),
                                        workItemIn.input.selectInput.minBatchId,
                                        workItemIn.input.selectInput.maxBatchId,
                                        workItemIn.input.selectInput
                                            .filterString.c_str(),
                                        numColumns,
                                        columns,
                                        workItemIn.input.selectInput.limitRows);
        break;
    }
    case XcalarApiPublish: {
        workItem =
            xcalarApiMakePublishWorkItem(workItemIn.input.publishInput.source
                                             .c_str(),
                                         workItemIn.input.publishInput.dest
                                             .c_str(),
                                         workItemIn.input.publishInput.unixTS,
                                         workItemIn.input.publishInput.dropSrc);
        break;
    }
    case XcalarApiRestoreTable: {
        workItem = xcalarApiMakeRestoreTableWorkItem(
            workItemIn.input.restoreTableInput.publishedTableName.c_str());
        break;
    }
    case XcalarApiUpdate: {
        unsigned numUpdates = workItemIn.input.updateInput.updates.size();
        if (numUpdates > MaxNumUpdates) {
            exception.status = StatusT::Status2Big;
            throw exception;
        }

        const char *sources[numUpdates];
        const char *dests[numUpdates];
        time_t times[numUpdates];

        for (unsigned ii = 0; ii < numUpdates; ii++) {
            sources[ii] =
                workItemIn.input.updateInput.updates[ii].source.c_str();
            dests[ii] = workItemIn.input.updateInput.updates[ii].dest.c_str();
            times[ii] = workItemIn.input.updateInput.updates[ii].unixTS;
        }

        workItem =
            xcalarApiMakeUpdateWorkItem(numUpdates,
                                        sources,
                                        dests,
                                        times,
                                        workItemIn.input.updateInput.updates[0]
                                            .dropSrc);
        break;
    }
    case XcalarApiUnpublish: {
        workItem =
            xcalarApiMakeUnpublishWorkItem(workItemIn.input.unpublishInput
                                               .source.c_str(),
                                           workItemIn.input.unpublishInput
                                               .inactivateOnly);
        break;
    }
    case XcalarApiCoalesce: {
        workItem = xcalarApiMakeCoalesceWorkItem(
            workItemIn.input.coalesceInput.source.c_str());
        break;
    }
    case XcalarApiRuntimeSetParam: {
        uint32_t numSchedParams =
            workItemIn.input.runtimeSetParamInput.schedParams.size();
        if (numSchedParams != Runtime::TotalSdkScheds) {
            exception.status = StatusT::StatusRuntimeSetParamInvalid;
            throw exception;
        }

        const char *schedName[Runtime::TotalSdkScheds];
        uint32_t cpuReservedPct[Runtime::TotalSdkScheds];
        RuntimeType rtType[Runtime::TotalSdkScheds];
        for (uint32_t ii = 0; ii < numSchedParams; ii++) {
            schedName[ii] =
                workItemIn.input.runtimeSetParamInput.schedParams.at(ii)
                    .schedName.c_str();
            RuntimeTypeT::type rtTypeT =
                workItemIn.input.runtimeSetParamInput.schedParams.at(ii)
                    .runtimeType;
            switch (rtTypeT) {
            case RuntimeTypeT::Latency:
                rtType[ii] = RuntimeTypeLatency;
                break;
            case RuntimeTypeT::Throughput:
                rtType[ii] = RuntimeTypeThroughput;
                break;
            case RuntimeTypeT::Immediate:
                rtType[ii] = RuntimeTypeImmediate;
                break;
            case RuntimeTypeT::Invalid:  // pass through
            default:
                rtType[ii] = RuntimeTypeInvalid;
                break;
            }
            cpuReservedPct[ii] =
                workItemIn.input.runtimeSetParamInput.schedParams.at(ii)
                    .cpusReservedInPercent;
        }

        workItem = xcalarApiMakeRuntimeSetParamWorkItem(schedName,
                                                        cpuReservedPct,
                                                        rtType,
                                                        numSchedParams);
        break;
    }
    case XcalarApiRuntimeGetParam: {
        workItem = xcalarApiMakeRuntimeGetParamWorkItem();
        break;
    }
    case XcalarApiGetRowNum:
        workItem =
            xcalarApiMakeGetRowNumWorkItem(workItemIn.input.getRowNumInput
                                               .source.c_str(),
                                           workItemIn.input.getRowNumInput.dest
                                               .c_str(),
                                           workItemIn.input.getRowNumInput
                                               .newField.c_str());
        break;

    case XcalarApiKeyList:
        workItem = xcalarApiMakeKeyListWorkItem((XcalarApiKeyScope) workItemIn
                                                    .input.keyListInput.scope,
                                                workItemIn.input.keyListInput
                                                    .keyRegex.c_str());
        break;

    case XcalarApiKeyAddOrReplace:
        workItem = xcalarApiMakeKeyAddOrReplaceWorkItem(
            (XcalarApiKeyScope) workItemIn.input.keyAddOrReplaceInput.scope,
            workItemIn.input.keyAddOrReplaceInput.kvPair.key.c_str(),
            workItemIn.input.keyAddOrReplaceInput.kvPair.value.c_str(),
            workItemIn.input.keyAddOrReplaceInput.persist);
        break;

    case XcalarApiKeyAppend:
        workItem =
            xcalarApiMakeKeyAppendWorkItem((XcalarApiKeyScope) workItemIn.input
                                               .keyAppendInput.scope,
                                           workItemIn.input.keyAppendInput.key
                                               .c_str(),
                                           workItemIn.input.keyAppendInput
                                               .suffix.c_str());
        break;

    case XcalarApiKeySetIfEqual:
        workItem = xcalarApiMakeKeySetIfEqualWorkItem(
            (XcalarApiKeyScope) workItemIn.input.keySetIfEqualInput.scope,
            workItemIn.input.keySetIfEqualInput.persist,
            (uint32_t) workItemIn.input.keySetIfEqualInput.countSecondaryPairs,
            workItemIn.input.keySetIfEqualInput.keyCompare.c_str(),
            workItemIn.input.keySetIfEqualInput.valueCompare.c_str(),
            workItemIn.input.keySetIfEqualInput.valueReplace.c_str(),
            workItemIn.input.keySetIfEqualInput.keySecondary.c_str(),
            workItemIn.input.keySetIfEqualInput.valueSecondary.c_str());
        break;

    case XcalarApiKeyDelete:
        workItem =
            xcalarApiMakeKeyDeleteWorkItem((XcalarApiKeyScope) workItemIn.input
                                               .keyDeleteInput.scope,
                                           workItemIn.input.keyDeleteInput.key
                                               .c_str());
        break;

    case XcalarApiMakeRetina: {
        uint64_t ii;
        int jj;
        uint64_t numTable = workItemIn.input.makeRetinaInput.numTables;
        uint64_t numSrcTable = workItemIn.input.makeRetinaInput.numSrcTables;

        if (numTable >= XcalarApiRetinaMaxNumTables) {
            workItem = NULL;
            break;
        }

        RetinaDst *tableArray[numTable];
        RetinaSrcTable srcTableArray[numSrcTable];

        for (ii = 0; ii < numTable; ii++) {
            tableArray[ii] = NULL;
        }

        try {
            for (ii = 0; ii < numSrcTable; ii++) {
                strlcpy(srcTableArray[ii].source.name,
                        workItemIn.input.makeRetinaInput.srcTables[ii]
                            .source.c_str(),
                        sizeof(srcTableArray[ii].source.name));
                strlcpy(srcTableArray[ii].dstName,
                        workItemIn.input.makeRetinaInput.srcTables[ii]
                            .dstName.c_str(),
                        sizeof(srcTableArray[ii].dstName));
            }

            for (ii = 0; ii < numTable; ++ii) {
                XcalarApiRetinaDstT srcTable;

                srcTable = workItemIn.input.makeRetinaInput.tableArray[ii];

                tableArray[ii] = (RetinaDst *)
                    memCalloc(1, xcalarApiSizeOfRetinaDst(srcTable.numColumns));
                if (tableArray[ii] == NULL) {
                    exception.status = StatusT::StatusNoMem;
                    throw exception;
                }

                copyParamString(tableArray[ii]->target.name,
                                srcTable.target.name.c_str(),
                                sizeof(tableArray[ii]->target.name));

                tableArray[ii]->numColumns = srcTable.numColumns;
                for (jj = 0; jj < srcTable.numColumns; jj++) {
                    copyParamString(tableArray[ii]->columns[jj].name,
                                    srcTable.columns[jj].name.c_str(),
                                    sizeof(tableArray[ii]->columns[jj].name));
                    copyParamString(tableArray[ii]->columns[jj].headerAlias,
                                    srcTable.columns[jj].headerAlias.c_str(),
                                    sizeof(tableArray[ii]
                                               ->columns[jj]
                                               .headerAlias));
                }
            }
            workItem =
                xcalarApiMakeMakeRetinaWorkItem(workItemIn.input.makeRetinaInput
                                                    .retinaName.c_str(),
                                                workItemIn.input.makeRetinaInput
                                                    .numTables,
                                                tableArray,
                                                numSrcTable,
                                                srcTableArray);
        } catch (...) {
            for (jj = 0; jj < (int) ii; jj++) {
                assert(tableArray[jj] != NULL);
                memFree(tableArray[jj]);
                tableArray[jj] = NULL;
            }
            throw;
        }

        // Even if we didn't have an exception, we still need to free
        // but we don't throw any exception
        for (jj = 0; jj < (int) numTable; jj++) {
            assert(tableArray[jj] != NULL);
            memFree(tableArray[jj]);
            tableArray[jj] = NULL;
        }

        break;
    }
    case XcalarApiDeleteRetina: {
        workItem = xcalarApiMakeDeleteRetinaWorkItem(
            workItemIn.input.deleteRetinaInput.delRetInput.c_str());
        break;
    }
    case XcalarApiImportRetina: {
        uint8_t *retinaBuf = NULL;
        bool bufAlloced = false;
        size_t retinaBufSize = 0;
        Status tmpStatus;

        if (workItemIn.input.importRetinaInput.loadRetinaJson) {
            retinaBuf =
                (uint8_t *)
                    workItemIn.input.importRetinaInput.retinaJson.c_str();
            retinaBufSize =
                strlen(workItemIn.input.importRetinaInput.retinaJson.c_str()) +
                1;
        } else {
            // the retina is base64 encoded, need to decode
            tmpStatus =
                base64Decode(workItemIn.input.importRetinaInput.retina.data(),
                             workItemIn.input.importRetinaInput.retinaCount,
                             &retinaBuf,
                             &retinaBufSize);

            if (tmpStatus != StatusOk) {
                exception.status = (StatusT::type) tmpStatus.code();
                xSyslog(moduleName,
                        XlogErr,
                        "base64Decode retina failed with status %s",
                        strGetFromStatus(tmpStatus));
                throw exception;
            }
            bufAlloced = true;
        }

        workItem = xcalarApiMakeImportRetinaWorkItem(
            workItemIn.input.importRetinaInput.retinaName.c_str(),
            workItemIn.input.importRetinaInput.overwriteExistingUdf,
            workItemIn.input.importRetinaInput.loadRetinaJson,
            workItemIn.input.importRetinaInput.udfUserName.c_str(),
            workItemIn.input.importRetinaInput.udfSessionName.c_str(),
            retinaBufSize,
            retinaBuf);
        if (bufAlloced) {
            memFree(retinaBuf);
            retinaBuf = NULL;
        }
        break;
    }

    case XcalarApiExportRetina:
        workItem = xcalarApiMakeExportRetinaWorkItem(
            workItemIn.input.exportRetinaInput.retinaName.c_str());
        break;

    case XcalarApiListRetinas:
        workItem = xcalarApiMakeListRetinasWorkItem(
            workItemIn.input.listRetinasInput.namePattern.c_str());
        break;

    case XcalarApiGetRetina:
        workItem = xcalarApiMakeGetRetinaWorkItem(
            workItemIn.input.getRetinaInput.retInput.c_str());
        break;

    case XcalarApiGetRetinaJson:
        workItem = xcalarApiMakeGetRetinaJsonWorkItem(
            workItemIn.input.getRetinaJsonInput.retinaName.c_str());
        break;

    case XcalarApiUpdateRetina: {
        workItem =
            xcalarApiMakeUpdateRetinaWorkItem(workItemIn.input.updateRetinaInput
                                                  .retinaName.c_str(),
                                              workItemIn.input.updateRetinaInput
                                                  .retinaJson.c_str(),
                                              workItemIn.input.updateRetinaInput
                                                      .retinaJson.length() +
                                                  1);
        break;
    }

    case XcalarApiListParametersInRetina:
        workItem = xcalarApiMakeListParametersInRetinaWorkItem(
            workItemIn.input.listParametersInRetinaInput.listRetInput.c_str());
        break;

    case XcalarApiListXdfs:
        workItem = xcalarApiMakeListXdfsWorkItem(workItemIn.input.listXdfsInput
                                                     .fnNamePattern.c_str(),
                                                 workItemIn.input.listXdfsInput
                                                     .categoryPattern.c_str());
        break;

    case XcalarApiUdfAdd:
    case XcalarApiUdfUpdate:
        workItem =
            xcalarApiMakeUdfAddUpdateWorkItem((XcalarApis) api,
                                              (UdfType) workItemIn.input
                                                  .udfAddUpdateInput.type,
                                              workItemIn.input.udfAddUpdateInput
                                                  .moduleName.c_str(),
                                              workItemIn.input.udfAddUpdateInput
                                                  .source.c_str());
        break;

    case XcalarApiUdfGet:
        workItem = xcalarApiMakeUdfGetWorkItem(
            workItemIn.input.udfGetInput.moduleName.c_str());
        break;

    case XcalarApiUdfDelete:
        workItem = xcalarApiMakeUdfDeleteWorkItem(
            workItemIn.input.udfDeleteInput.moduleName.c_str());
        break;

    case XcalarApiSessionList:
        workItem = xcalarApiMakeSessionListWorkItem(
            workItemIn.input.sessionListInput.sesListInput.c_str());
        break;

    case XcalarApiSessionNew:
        workItem =
            xcalarApiMakeSessionNewWorkItem(workItemIn.input.sessionNewInput
                                                .sessionName.c_str(),
                                            workItemIn.input.sessionNewInput
                                                .fork,
                                            workItemIn.input.sessionNewInput
                                                .forkedSessionName.c_str());
        break;

    // Session delete ingores the noCleanup field in sessionDeleteInput
    case XcalarApiSessionDelete:
        workItem = xcalarApiMakeSessionDeleteWorkItem(
            workItemIn.input.sessionDeleteInput.sessionName.c_str());
        break;

    case XcalarApiSessionRename:
        workItem =
            xcalarApiMakeSessionRenameWorkItem(workItemIn.input
                                                   .sessionRenameInput
                                                   .sessionName.c_str(),
                                               workItemIn.input
                                                   .sessionRenameInput
                                                   .origSessionName.c_str());
        break;

    case XcalarApiSessionActivate:
        workItem = xcalarApiMakeSessionActivateWorkItem(
            workItemIn.input.sessionActivateInput.sessionName.c_str());
        break;

    // Inact takes the same input as session delete except that it
    // propagates the noCleanup field (which must be "false).
    case XcalarApiSessionInact:
        workItem = xcalarApiMakeSessionDeleteWorkItem(
            workItemIn.input.sessionDeleteInput.sessionName.c_str());
        if (workItem != NULL) {
            workItem->api = XcalarApiSessionInact;
            assert(workItemIn.input.sessionDeleteInput.noCleanup == false);
            workItem->input->sessionDeleteInput.noCleanup =
                workItemIn.input.sessionDeleteInput.noCleanup;
        }
        break;

    // Persist takes the same input as session delete, returns the same
    // output as session list.  It ignores the noCleanup field in
    // sessionDeleteInput.
    case XcalarApiSessionPersist:
        workItem = xcalarApiMakeSessionPersistWorkItem(
            workItemIn.input.sessionDeleteInput.sessionName.c_str());
        break;

    case XcalarApiSessionDownload:
        workItem =
            xcalarApiMakeSessionDownloadWorkItem(workItemIn.input
                                                     .sessionDownloadInput
                                                     .sessionName.c_str(),
                                                 workItemIn.input
                                                     .sessionDownloadInput
                                                     .pathToAdditionalFiles
                                                     .c_str());
        break;

    case XcalarApiSessionUpload: {
        uint8_t *sessionBuf = NULL;
        size_t sessionBufSize = 0;
        Status tmpStatus;

        tmpStatus = base64Decode(workItemIn.input.sessionUploadInput
                                     .sessionContent.data(),
                                 workItemIn.input.sessionUploadInput
                                     .sessionContentCount,
                                 &sessionBuf,
                                 &sessionBufSize);
        if (tmpStatus != StatusOk) {
            exception.status = (StatusT::type) tmpStatus.code();
            xSyslog(moduleName,
                    XlogErr,
                    "base64Decode session '%s' failed with status: %s",
                    workItemIn.input.sessionUploadInput.sessionName.c_str(),
                    strGetFromStatus(tmpStatus));
            throw exception;
        }

        workItem = xcalarApiMakeSessionUploadWorkItem(workItemIn.input
                                                          .sessionUploadInput
                                                          .sessionName.c_str(),
                                                      workItemIn.input
                                                          .sessionUploadInput
                                                          .pathToAdditionalFiles
                                                          .c_str(),
                                                      sessionBufSize,
                                                      sessionBuf);
        memFree(sessionBuf);
        sessionBuf = NULL;
        break;
    }

    case XcalarApiGetQuery: {
        XcalarWorkItem *workItemTmp = NULL;

        if (workItemIn.origApi == XcalarApisT::XcalarApiGetQuery) {
            exception.status = StatusT::StatusInval;
            xSyslog(moduleName, XlogErr, "origApi can't be XcalarApiGetQuery");
            throw exception;
        }

        status = parseCPlusPlusWorkItem(workItemIn,
                                        workItemIn.origApi,
                                        &workItemTmp);

        if (status != StatusOk) {
            assert(workItemTmp == NULL);
            exception.status = (StatusT::type) status.code();
            throw exception;
        }

        workItem = xcalarApiMakeGetQueryWorkItem(workItemTmp->api,
                                                 workItemTmp->input,
                                                 workItemTmp->inputSize);
        xcalarApiFreeWorkItem(workItemTmp);
        workItemTmp = NULL;
        if (workItem == NULL) {
            exception.status = StatusT::StatusNoMem;
            xSyslog(moduleName, XlogErr, "No memory for workItem!");
            throw exception;
        }

        break;
    }

    case XcalarApiCreateDht:
        workItem =
            xcalarApiMakeCreateDhtWorkItem(workItemIn.input.createDhtInput
                                               .dhtName.c_str(),
                                           workItemIn.input.createDhtInput
                                               .dhtArgs.upperBound,
                                           workItemIn.input.createDhtInput
                                               .dhtArgs.lowerBound,
                                           (Ordering)
                                               workItemIn.input.createDhtInput
                                                   .dhtArgs.ordering);
        break;

    case XcalarApiDeleteDht:
        size_t dhtNameLen;

        dhtNameLen = strlen(workItemIn.input.deleteDhtInput.dhtName.c_str());
        workItem =
            xcalarApiMakeDeleteDhtWorkItem(workItemIn.input.deleteDhtInput
                                               .dhtName.c_str(),
                                           dhtNameLen);
        break;

    case XcalarApiSupportGenerate:
        workItem =
            xcalarApiMakeSupportGenerateWorkItem(workItemIn.input
                                                     .supportGenerateInput
                                                     .generateMiniBundle,
                                                 workItemIn.input
                                                     .supportGenerateInput
                                                     .supportCaseId);
        break;

    case XcalarApiTarget:
        workItem = xcalarApiMakeTargetWorkItem(
            workItemIn.input.targetInput.inputJson.c_str());
        break;

    case XcalarApiPreview:
        workItem = xcalarApiMakePreviewWorkItem(
            workItemIn.input.previewInput.inputJson.c_str());
        break;

    case XcalarApiStartFuncTests: {
        const char **testNamePatterns = NULL;
        if (workItemIn.input.startFuncTestInput.numTestPatterns >
            XcalarApiMaxNumFuncTests) {
            exception.status = StatusT::StatusFunctionalTestNumFuncTestExceeded;
            xSyslog(moduleName,
                    XlogErr,
                    "%s:%s() (%s) Too many testPatterns (%u). Max is %u",
                    __FILE__,
                    __func__,
                    strGetFromXcalarApis((XcalarApis) workItemIn.api),
                    workItemIn.input.startFuncTestInput.numTestPatterns,
                    XcalarApiMaxNumFuncTests);
            throw exception;
        }
        testNamePatterns = (const char **)
            memCalloc(1,
                      sizeof(*testNamePatterns) *
                          workItemIn.input.startFuncTestInput.numTestPatterns);
        if (testNamePatterns == NULL) {
            exception.status = StatusT::StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "%s:%s() (%s) Insufficient memory to allocate "
                    "testNamePatterns",
                    __FILE__,
                    __func__,
                    strGetFromXcalarApis((XcalarApis) workItemIn.api));
            throw exception;
        }

        for (int ii = 0;
             ii < workItemIn.input.startFuncTestInput.numTestPatterns;
             ii++) {
            testNamePatterns[ii] =
                workItemIn.input.startFuncTestInput.testNamePatterns[ii]
                    .c_str();
        }

        workItem = xcalarApiMakeStartFuncTestWorkItem(
            workItemIn.input.startFuncTestInput.parallel,
            workItemIn.input.startFuncTestInput.runAllTests,
            workItemIn.input.startFuncTestInput.runOnAllNodes,
            workItemIn.input.startFuncTestInput.numTestPatterns,
            testNamePatterns);
        memFree(testNamePatterns);
        break;
    }

    case XcalarApiListFuncTests:
        workItem = xcalarApiMakeListFuncTestWorkItem(
            workItemIn.input.listFuncTestInput.namePattern.c_str());
        break;

    case XcalarApiGetConfigParams:
        workItem = xcalarApiMakeGetConfigParamsWorkItem();
        break;

    case XcalarApiSetConfigParam:
        workItem = xcalarApiMakeSetConfigParamWorkItem(workItemIn.input
                                                           .setConfigParamInput
                                                           .paramName.c_str(),
                                                       workItemIn.input
                                                           .setConfigParamInput
                                                           .paramValue.c_str());
        if (workItem != NULL) {
            // Check if this is setting the loglevel and if so, set our
            // loglevel to the same value.
            // Note this doesn't change the loglevel for other mgmtd
            // processes.
            if (strcasecmp(workItemIn.input.setConfigParamInput.paramName
                               .c_str(),
                           XcalarConfig::ClusterLogLevelParamName) == 0) {
                parseAndProcessLogLevel(
                    workItemIn.input.setConfigParamInput.paramValue.c_str());
                // The workItem gets passed through to the backend which
                // will report any errors (e.g. invalid value) to the user.
            }
        }
        break;

    case XcalarApiAppSet:
        workItem = xcalarApiMakeAppSetWorkItem(workItemIn.input.appSetInput.name
                                                   .c_str(),
                                               workItemIn.input.appSetInput
                                                   .hostType.c_str(),
                                               workItemIn.input.appSetInput.duty
                                                   .c_str(),
                                               workItemIn.input.appSetInput
                                                   .execStr.c_str());
        break;

    case XcalarApiAppRun:
        workItem =
            xcalarApiMakeAppRunWorkItem(workItemIn.input.appRunInput.name
                                            .c_str(),
                                        workItemIn.input.appRunInput.isGlobal,
                                        workItemIn.input.appRunInput.inStr
                                            .c_str());
        break;

    case XcalarApiAppReap:
        workItem =
            xcalarApiMakeAppReapWorkItem(stoull(workItemIn.input.appReapInput
                                                    .appGroupId),
                                         workItemIn.input.appReapInput.cancel);
        break;

    case XcalarApiGetIpAddr:
        workItem = xcalarApiMakeGetIpAddrWorkItem(
            workItemIn.input.getIpAddrInput.nodeId);
        break;

    case XcalarApiListDatasetUsers:
        workItem = xcalarApiMakeListDatasetUsersWorkItem(
            workItemIn.input.listDatasetUsersInput.datasetName.c_str());
        break;

    case XcalarApiListUserDatasets:
        workItem = xcalarApiMakeListUserDatasetsWorkItem(
            workItemIn.input.listUserDatasetsInput.userIdName.c_str());
        break;

    case XcalarApiGetNumNodes:
        workItem = xcalarApiMakeGetNumNodesWorkItem();
        break;

    default:
        status = StatusUnimpl;
        xSyslog(moduleName,
                XlogErr,
                "%s:%s() unimplemented api %s",
                __FILE__,
                __func__,
                strGetFromXcalarApis((XcalarApis) workItemIn.api));
        goto CommonExit;
    }

    // If the API supplies session information (name, ID, etc.) in
    // workItemIn, this must be added to the core XcalarWorkItem, which
    // serves as a common structure to pass this information to the backend
    // API (instead of extending the backend API's signature to take this
    // information).
    //
    // In general, session information is needed for APIs which must be
    // supported for inactive sessions - since there can be many inactive
    // sessions, the session identification information must be available to
    // such APIs, instead of assuming there's only the one active session
    // for the invoking user. In the future, if multiple active sessions per
    // user are enabled, the session ID infromation would be needed by any
    // API targeting a session, to identify the target from the list of
    // active sessions.

    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "No memory for workItem!");
        goto CommonExit;
    } else if (workItemIn.sessionName.c_str() != NULL &&
               strlen(workItemIn.sessionName.c_str()) > 0) {
        workItem->sessionInfo = (XcalarApiSessionInfoInput *)
            memCallocExt(1, sizeof(XcalarApiSessionInfoInput), moduleName);
        if (workItem->sessionInfo == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName, XlogErr, "No memory for sessionInfo!");
            assert(workItem->output == NULL);
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
            goto CommonExit;
        }
        workItem->sessionInfoSize = sizeof(XcalarApiSessionInfoInput);
        ret = strlcpy(workItem->sessionInfo->sessionName,
                      workItemIn.sessionName.c_str(),
                      XcalarApiSessionNameLen + 1);
        if (ret >= XcalarApiSessionNameLen + 1) {
            status = StatusOverflow;
            xSyslog(moduleName, XlogErr, "sessionName too long!");
            xcalarApiFreeWorkItem(workItem);
            goto CommonExit;
        }
        workItem->sessionInfo->sessionNameLength =
            strlen(workItemIn.sessionName.c_str());
        workItem->sessionInfo->sessionId = 0;
    }

CommonExit:
    if (loadArgs) {
        memFree(loadArgs);
        loadArgs = NULL;
    }

    *workItemRetp = workItem;
    return status;
}

class XcalarApiServiceHandler : virtual public XcalarApiServiceIf
{
  private:
  public:
    XcalarApiServiceHandler()
    {
        // Your initialization goes here
    }

    bool isLegacyApi(XcalarApisT::type api)
    {
        constexpr const XcalarApisT::type serviceApis[] =
            {// Will enable in a followup commit
             XcalarApisT::XcalarApiKeyLookup,
             XcalarApisT::XcalarApiLogLevelGet,
             XcalarApisT::XcalarApiLogLevelSet,
             XcalarApisT::XcalarApiUdfGetResolution,
             XcalarApisT::XcalarApiDriver,
             XcalarApisT::XcalarApiGetMemoryUsage,
             XcalarApisT::XcalarApiPtChangeOwner,
             XcalarApisT::XcalarApiQuery,
             XcalarApisT::XcalarApiQueryList,
             XcalarApisT::XcalarApiAddIndex,
             XcalarApisT::XcalarApiRemoveIndex,
             XcalarApisT::XcalarApiMakeResultSet,
             XcalarApisT::XcalarApiResultSetNext,
             XcalarApisT::XcalarApiResultSetAbsolute,
             XcalarApisT::XcalarApiFreeResultSet,
             XcalarApisT::XcalarApiListTables,
             XcalarApisT::XcalarApiExecuteRetina,
             XcalarApisT::XcalarApiListFiles,
             XcalarApisT::XcalarApiGetVersion};
        bool isLegacy = true;
        for (int ii = 0; ii < (int) ArrayLen(serviceApis); ii++) {
            if (api == serviceApis[ii]) {
                isLegacy = false;
                break;
            }
        }
        return isLegacy;
    }

    void queueWork(XcalarApiWorkItemResult &_return,
                   const XcalarApiWorkItemT &workItemIn)
    {
        Status status;
        XcalarApiException exception;
        // XXX replace with something real
        bool legacyApi = isLegacyApi(workItemIn.api);

        if ((workItemIn.api == XcalarApisT::XcalarApiTop) ||
            (workItemIn.api == XcalarApisT::XcalarApiPerNodeTop)) {
            status = sysStatsHelper->getTopResults(&workItemIn,
                                                   &_return,
                                                   destIp,
                                                   destPort);
            _return.output.hdr.status = (StatusT::type) status.code();
            goto CommonExit;
        }

        // we dont log for Top and perNodeTop: which is handled above
        xSyslog(moduleName,
                XlogInfo,
                "%s:%s(%s) enter",
                __FILE__,
                __func__,
                strGetFromXcalarApis((XcalarApis) workItemIn.api));

        if (legacyApi) {
            status = queueLegacyApi(&workItemIn, &_return);
        } else {
            status = queueServiceApi(&workItemIn, &_return);
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "%s:%s() queueWork(%s) failed: %s",
                    __FILE__,
                    __func__,
                    strGetFromXcalarApis((XcalarApis) workItemIn.api),
                    strGetFromStatus(status));
            XcalarApiException exception;
            exception.status = (StatusT::type) status.code();
            throw exception;
        }
    CommonExit:
        return;
    }
};

// This is a callback that TServerSocket will call with the listened FD
// before calling ::listen
void
setupForListen(int socketFd)
{
    int socketFlags = fcntl(socketFd, F_GETFD);
    if (socketFlags == -1) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get thrift socket fcntl flags: %s",
                strGetFromStatus(sysErrnoToStatus(errno)));
        return;
    }
    socketFlags |= FD_CLOEXEC;
    if (fcntl(socketFd, F_SETFD, socketFlags) == -1) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set FD_CLOEXEC on thrift socket: %s",
                strGetFromStatus(sysErrnoToStatus(errno)));
        return;
    }
}

int
main(int argc, char **argv)
{
    int port;
    Status status = StatusOk;
    const char *logdirPath;
    TServerSocket *socket = NULL;

    // @SymbolCheckIgnore
    int ret = pthread_mutex_init(&mgmtdLock, NULL);
    assert(ret == 0);

    status = memTrackInit();
    if (status != StatusOk) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "Error: memtrack initialization failed: %s",
                strGetFromStatus(status));
        return status.code();
    }

    status = Config::init();
    if (status != StatusOk) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "Error: Config initialization failed: %s",
                strGetFromStatus(status));
        return status.code();
    }
    Config *config = Config::get();

    if (argc > 1) {
        if (strlcpy(pathToConfigFile, argv[1], sizeof(pathToConfigFile)) >=
            sizeof(pathToConfigFile)) {
            xSyslog(moduleName,
                    XlogErr,
                    "xcmgmtd: path to configuration file '%s' is "
                    "too long",
                    pathToConfigFile);
            status = StatusOverflow;
            goto CommonExit;
        }
        status = config->loadLogInfoFromConfig(argv[1]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "xcmgmtd: couldn't init log info; config is \'%s\'",
                    argv[1]);
            goto CommonExit;
        }
    } else {
        xSyslog(moduleName,
                XlogErr,
                "xcmgmtd: missing configuration; please run"
                " with a configuration file");
        status = StatusNoConfigFile;
        goto CommonExit;
    }

    logdirPath = config->getLogDirPath();
    status = xsyslogInit(SyslogFacilityMgmtD, logdirPath);
    if (status != StatusOk) {
        // @SymbolCheckIgnore (can't use xSyslog at this point)
        fprintf(stderr,
                "Error initializaing message logging. Status = %d\n",
                status.code());
        fflush(stderr);
        return status.code();
    }
    xsyslogFacilityBooted();

    /*
     * After xSyslog is inited, any exit must go through xSyslogDestroy().
     */
    xSyslog(moduleName, XlogNote, "xcmgmtd starting...");
    progName = argv[0];

    status = config->loadConfigFiles(argv[1]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Error: xcmgmtd: configuration file \'%s\' does"
                    " not exist",
                    argv[1]);
    port = config->getThriftPort();
    cfgLoaded = true;

    // Install SIGABRT handler.
    struct sigaction act;
    memZero(&act, sizeof(act));
    act.sa_sigaction = &mgmtdSigAbortHandler;
    act.sa_flags = SA_SIGINFO;
    if (sigaction(SIGABRT, &act, 0)) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to set sigabort: %s\n",
                strGetFromStatus(status));
        goto CommonExit;
    }

    socket = new (std::nothrow) TServerSocket(port);
    if (socket == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "Failed to allocate TServerSocket");
        goto CommonExit;
    }

    socket->setListenCallback(setupForListen);

    socket->setKeepAlive(true);

    thriftHandler = shared_ptr<XcalarApiServiceHandler>(
        new (std::nothrow) XcalarApiServiceHandler());
    if (thriftHandler == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }

    thriftProcessor = shared_ptr<TProcessor>(
        new (std::nothrow) XcalarApiServiceProcessor(thriftHandler));
    if (thriftProcessor == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }
    thirftServerTransport = shared_ptr<TServerTransport>(socket);
    socket = NULL;

    thriftTransportFactory = shared_ptr<TTransportFactory>(
        new (std::nothrow) THttpServerTransportFactory());
    if (thriftTransportFactory == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }

    thriftProtocolFactory =
        shared_ptr<TProtocolFactory>(new (std::nothrow) TJSONProtocolFactory());
    if (thriftProtocolFactory == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }

    thriftThreadFactory =
        shared_ptr<PosixThreadFactory>(new (std::nothrow) PosixThreadFactory());
    if (thriftThreadFactory == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }

    thriftThreadManager =
        ThreadManager::newSimpleThreadManager(MgmtdThdMgrThreadCountMin);
    thriftThreadManager->threadFactory(thriftThreadFactory);
    thriftThreadManager->start();

    thriftThreadPoolServer =
        new ((std::nothrow)) TThreadPoolServer(thriftProcessor,
                                               thirftServerTransport,
                                               thriftTransportFactory,
                                               thriftProtocolFactory,
                                               thriftThreadManager);
    if (thriftThreadPoolServer == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ret = sem_init(&mgmtdStatusThreadSem, 0, 0);
    assert(ret == 0);

    apiRoutePolicy(&nodeId, &destIp, true, &destPort);

    sysStatsHelper = new (std::nothrow) SysStatsHelper();
    if (sysStatsHelper == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Error bootstrapping mgmt daemon: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = sysStatsHelper->initStatsHelper();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error bootstrapping mgmt daemon: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    ret = pthread_create(&mgmtdStatusThreadtid, NULL, mgmtdStatusThread, NULL);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Error: xcmgmtd: Spawning mgmtd Status Thread failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Start serving thrift requests from XI.
    thriftThreadPoolServer->serve();

    // Note that we now have SIGABRT signal handler to initiate shutdown.
    // This will facilitate stopping the thrift server.
    xSyslog(moduleName, XlogNote, "Starting shutdown");

    // @SymbolCheckIgnore
    pthread_join(mgmtdStatusThreadtid, NULL);

    // Start shutdown from here.
    if (cfgLoaded) {
        config->unloadConfigFiles();
        cfgLoaded = false;
    }

    // @SymbolCheckIgnore
    ret = pthread_mutex_destroy(&mgmtdLock);
    assert(ret == 0);

    if (Config::get()) {
        Config::get()->destroy();
    }

    memTrackDestroy(true);

    xSyslog(moduleName, XlogNote, "Shutdown complete");

CommonExit:
    if (sysStatsHelper != NULL) {
        sysStatsHelper->tearDownStatsHelper();
        delete sysStatsHelper;
    }

    xsyslogDestroy();

    return status.code();
}

void
updateSessionGenericOutput(XcalarApiSessionGenericOutputT *sessionOutputXd,
                           XcalarApiSessionGenericOutput *sessionGenericOutput)
{
    sessionOutputXd->outputAdded = sessionGenericOutput->outputAdded;

    if (sessionGenericOutput->outputAdded == true) {
        sessionOutputXd->nodeId = sessionGenericOutput->nodeId;
        sessionOutputXd->ipAddr = sessionGenericOutput->ipAddr;
        sessionOutputXd->errorMessage = sessionGenericOutput->errorMessage;
    }
}
