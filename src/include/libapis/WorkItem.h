// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _WORKITEM_H_
#define _WORKITEM_H_

#include "udf/UdfTypes.h"
#include "libapis/LibApisCommon.h"

// XXX We avoid default args in CPP functions to avoid copy/paste errors.
XcalarWorkItem *xcalarApiMakeGetQueryWorkItem(XcalarApis api,
                                              XcalarApiInput *apiInput,
                                              size_t apiInputSize);
XcalarWorkItem *xcalarApiMakeCreateDhtWorkItem(const char *dhtName,
                                               float64_t upperBound,
                                               float64_t lowerBound,
                                               Ordering ordering);
XcalarWorkItem *xcalarApiMakeDeleteDhtWorkItem(const char *dhtName,
                                               size_t dhtNameLen);
XcalarWorkItem *xcalarApiMakeListXdfsWorkItem(const char *fnNamePattern,
                                              const char *categoryPattern);
XcalarWorkItem *xcalarApiMakeGetNumNodesWorkItem();
XcalarWorkItem *xcalarApiMakeGetMemoryUsageWorkItem(const char *userName,
                                                    int64_t userId);
XcalarWorkItem *xcalarApiMakeExportRetinaWorkItem(const char *retinaName);
XcalarWorkItem *xcalarApiMakeImportRetinaWorkItem(const char *retinName,
                                                  bool overwriteExistingUdf,
                                                  bool loadRetinaJson,
                                                  const char *udfUserName,
                                                  const char *udfSessionName,
                                                  size_t retinaCount,
                                                  uint8_t retina[]);
XcalarWorkItem *xcalarApiMakeListParametersInRetinaWorkItem(
    const char *retinaNameIn);
XcalarWorkItem *xcalarApiMakeUpdateRetinaWorkItem(const char *retinaName,
                                                  const char *retinaJson,
                                                  size_t retinaJsonLen);
XcalarWorkItem *xcalarApiMakeGetRetinaWorkItem(const char *retinaNameIn);
XcalarWorkItem *xcalarApiMakeGetRetinaJsonWorkItem(const char *retinaNameIn);
XcalarWorkItem *xcalarApiMakeDeleteRetinaWorkItem(const char *retinaNameIn);
XcalarWorkItem *xcalarApiMakeExecuteRetinaWorkItem(
    const char *retinaName,
    const char *queryName,
    bool exportToActiveSession,
    const char *schedName,
    const char *newTableName,
    unsigned numParameters,
    XcalarApiParameter parameters[],
    const char *udfUserName,
    const char *udfSessionName,
    const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeListRetinasWorkItem(const char *namePattern);
XcalarWorkItem *xcalarApiMakeMakeRetinaWorkItem(const char *retinaName,
                                                uint64_t numTable,
                                                RetinaDst *tableArray[],
                                                uint64_t numSrcTables,
                                                RetinaSrcTable *srcTables,
                                                const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeListExportTargetsWorkItem(const char *typePattern,
                                                       const char *namePattern);
XcalarWorkItem *xcalarApiMakeExportWorkItem(const char *tableName,
                                            const char *exportName,
                                            int numColumns,
                                            const ExColumnName *columns,
                                            const char *driverName,
                                            const char *driverParams,
                                            const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeAggregateWorkItem(const char *srcTableName,
                                               const char *dstTableName,
                                               const char *evalStr,
                                               const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeMapWorkItem(const char *srcTableName,
                                         const char *dstTableName,
                                         bool icvMode,
                                         uint32_t numEvals,
                                         const char *evalStr[],
                                         const char *newFieldName[],
                                         const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeGetVersionWorkItem();

XcalarWorkItem *xcalarApiMakeTopWorkItem(
    XcalarApiTopRequestType topStatsRequestType, XcalarApiTopType topType);

XcalarWorkItem *xcalarApiMakeListDagNodesWorkItem(
    const char *namePattern,
    SourceType srcType,
    const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakePerNodeOpStatsWorkItem(
    const char *tableName, const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeQueryStateWorkItem(const char *queryName,
                                                bool detailedStats = true);
XcalarWorkItem *xcalarApiMakeQueryCancelWorkItem(const char *queryName);
XcalarWorkItem *xcalarApiMakeQueryDeleteWorkItem(const char *queryName);
XcalarWorkItem *xcalarApiMakeShutdownWorkItem(bool local, bool force);
XcalarWorkItem *xcalarApiMakeFreeResultSetWorkItem(uint64_t resultSetId);
XcalarWorkItem *xcalarApiMakeResultSetAbsoluteWorkItem(uint64_t resultSetId,
                                                       uint64_t position);
XcalarWorkItem *xcalarApiMakeGetDataDictWorkItem(const char *tableName,
                                                 DfFormatType formatType);
XcalarWorkItem *xcalarApiMakeRenameNodeWorkItem(const char *oldName,
                                                const char *newName,
                                                const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeTagDagNodesWorkItem(
    unsigned numNodes,
    DagTypes::NamedInput *dagNodes,
    const char *tag,
    const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeCommentDagNodesWorkItem(
    unsigned numNodes,
    char dagNodeNames[0][XcalarApiMaxTableNameLen],
    const char *comment,
    const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeGroupByWorkItem(const char *tableName,
                                             const char *groupByTableName,
                                             unsigned numKeys,
                                             XcalarApiKeyInput *keys,
                                             bool includeSrcTableSample,
                                             bool icvMode,
                                             bool groupAll,
                                             uint32_t numEvals,
                                             const char *evalStr[],
                                             const char *newFieldName[],
                                             const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeProjectWorkItem(
    int numColumns,
    char (*columns)[DfMaxFieldNameLen + 1],
    const char *srcTableName,
    const char *dstTableName,
    const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeGetRowNumWorkItem(const char *srcTableName,
                                               const char *dstTableName,
                                               const char *newFieldName,
                                               const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeFilterWorkItem(const char *filterStr,
                                            const char *srcTableName,
                                            const char *dstTableName,
                                            const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeUnionWorkItem(unsigned numSrcTables,
                                           const char **srcTables,
                                           const char *dstTable,
                                           unsigned *renameMapSizes,
                                           XcalarApiRenameMap **renameMap,
                                           bool dedup,
                                           UnionOperator unionType,
                                           const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeJoinWorkItem(const char *leftTableName,
                                          const char *rightTableName,
                                          const char *joinTableName,
                                          JoinOperator joinType,
                                          bool collisionCheck,
                                          bool keepAllColumns,
                                          bool nullSafe,
                                          unsigned numLeftColumns,
                                          unsigned numRightColumns,
                                          XcalarApiRenameMap *renameMap,
                                          const char *filterString,
                                          const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeBulkLoadWorkItem(const char *name,
                                              const DfLoadArgs *loadArgs,
                                              const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeIndexWorkItem(const char *srcDatasetName,
                                           const char *srcTableName,
                                           unsigned numKeys,
                                           const char **keyName,
                                           const char **keyFieldName,
                                           DfFieldType *keyType,
                                           Ordering *keyOrdering,
                                           const char *dstTableName,
                                           const char *dhtName,
                                           const char *fatptrPrefixName,
                                           bool delaySort,
                                           bool broadcast,
                                           const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeSynthesizeWorkItem(const char *sourceName,
                                                const char *dstTableName,
                                                bool sameSession,
                                                unsigned numColumns,
                                                XcalarApiRenameMap *columns,
                                                const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeSelectWorkItem(const char *srcTableName,
                                            const char *dstTableName,
                                            int64_t minBatchId,
                                            int64_t maxBatchId,
                                            const char *filterString,
                                            unsigned numColumns,
                                            XcalarApiRenameMap *columns,
                                            uint64_t limitRows = 0,
                                            const char *evalJsonStr = NULL,
                                            const char *joinTableName = NULL,
                                            bool createIndex = false,
                                            const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeUpdateWorkItem(unsigned numUpdates,
                                            const char **srcTableNames,
                                            const char **dstTableNames,
                                            time_t *times,
                                            bool dropSrc,
                                            const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakePublishWorkItem(const char *srcTableName,
                                             const char *dstTableName,
                                             time_t time,
                                             bool dropSrc,
                                             const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeUnpublishWorkItem(const char *srcTableName,
                                               bool inactivate);
XcalarWorkItem *xcalarApiMakeRestoreTableWorkItem(
    const char *publishedTableName);
XcalarWorkItem *xcalarApiMakeCoalesceWorkItem(const char *srcTableName);

// If isTable is false, this represents a dataset
XcalarWorkItem *xcalarApiMakeGetTableMeta(const char *objectName,
                                          bool isTable,
                                          bool isPrecise,
                                          const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeResultSetWorkItem(const char *dagNodeName,
                                               bool errorDs,
                                               const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeResetStatWorkItem(int64_t nodeId);
XcalarWorkItem *xcalarApiMakeGetStatWorkItem(int64_t nodeId);
XcalarWorkItem *xcalarApiMakeGetStatGroupIdMapWorkItem(int64_t nodeId);
XcalarWorkItem *xcalarApiMakeResultSetNextWorkItem(
    uint64_t resultSetId, unsigned numRecords, const char *sessionName = NULL);
void xcalarApiFreeWorkItem(XcalarWorkItem *workItem);
XcalarWorkItem *xcalarApiMakeGenericWorkItem(XcalarApis api,
                                             void *input,
                                             size_t inputSize);
XcalarWorkItem *xcalarApiMakeListFilesWorkItem(const char *targetName,
                                               const char *path,
                                               const char *fileNamePattern,
                                               bool recursive);

XcalarWorkItem *xcalarApiMakeDagWorkItem(const char *tableName,
                                         const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeCancelOpWorkItem(const char *tableName,
                                              const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeKeyListWorkItem(XcalarApiKeyScope scope,
                                             const char *keyRegex);
XcalarWorkItem *xcalarApiMakeKeyAddOrReplaceWorkItem(XcalarApiKeyScope scope,
                                                     const char *key,
                                                     const char *value,
                                                     bool persist);
XcalarWorkItem *xcalarApiMakeKeyAppendWorkItem(XcalarApiKeyScope scope,
                                               const char *key,
                                               const char *suffix);
XcalarWorkItem *xcalarApiMakeKeySetIfEqualWorkItem(XcalarApiKeyScope scope,
                                                   bool persist,
                                                   uint32_t countSecondaryPairs,
                                                   const char *keyCompare,
                                                   const char *valueCompare,
                                                   const char *valueReplace,
                                                   const char *keySecondary,
                                                   const char *valueSecondary);
XcalarWorkItem *xcalarApiMakeKeyDeleteWorkItem(XcalarApiKeyScope scope,
                                               const char *key);
XcalarWorkItem *xcalarApiMakeUdfAddUpdateWorkItem(
    XcalarApis api,
    UdfType type,
    const char *moduleName,
    const char *source,
    const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeUdfGetWorkItem(const char *moduleName);
XcalarWorkItem *xcalarApiMakeUdfDeleteWorkItem(const char *moduleName,
                                               const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeSessionNewWorkItem(const char *sessionName,
                                                bool fork,
                                                const char *forkedSessionName);
XcalarWorkItem *xcalarApiMakeSessionListWorkItem(
    const char *sessionNamePattern);
XcalarWorkItem *xcalarApiMakeSessionRenameWorkItem(const char *sessionName,
                                                   const char *origSessionName);
XcalarWorkItem *xcalarApiMakeSessionInactWorkItem(
    const char *sessionNamePattern);
XcalarWorkItem *xcalarApiMakeSessionDeleteWorkItem(
    const char *sessionNamePattern);
XcalarWorkItem *xcalarApiMakeSessionPersistWorkItem(
    const char *sessionNamePattern);
XcalarWorkItem *xcalarApiMakeSupportGenerateWorkItem(bool generateMiniBundle,
                                                     uint64_t supportCaseId);

XcalarWorkItem *xcalarApiMakeDeleteDagNodesWorkItem(
    const char *namePattern,
    SourceType srcType,
    const char *sessionName = NULL,
    bool deleteCompletely = false);
XcalarWorkItem *xcalarApiMakeArchiveTablesWorkItem(
    bool archive,
    bool allTables,
    unsigned numTables,
    const char **tableNames,
    const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeOpStatsWorkItem(const char *tableName,
                                             const char *sessionName = NULL);

XcalarWorkItem *xcalarApiMakeTargetWorkItem(const char *inputJson);
XcalarWorkItem *xcalarApiMakeDriverWorkItem(const char *inputJson);
XcalarWorkItem *xcalarApiMakePreviewWorkItem(const char *inputJson);
XcalarWorkItem *xcalarApiMakeStartFuncTestWorkItem(
    bool parallel,
    bool runAllTests,
    bool runOnAllNodes,
    unsigned numTestPatterns,
    const char *testNamePatterns[]);
XcalarWorkItem *xcalarApiMakeListFuncTestWorkItem(const char *namePattern);
XcalarWorkItem *xcalarApiMakeRuntimeGetParamWorkItem();
XcalarWorkItem *xcalarApiMakeRuntimeSetParamWorkItem(const char **schedName,
                                                     uint32_t *cpuReservedInPct,
                                                     RuntimeType *rtType,
                                                     uint32_t numSchedParams);
XcalarWorkItem *xcalarApiMakeGetConfigParamsWorkItem();
XcalarWorkItem *xcalarApiMakeSetConfigParamWorkItem(const char *paramName,
                                                    const char *paramValue);
XcalarWorkItem *xcalarApiMakeAppSetWorkItem(const char *name,
                                            const char *hostType,
                                            const char *duty,
                                            const char *exec);
XcalarWorkItem *xcalarApiMakeAppRunWorkItem(const char *name,
                                            bool isGlobal,
                                            const char *inStr);
XcalarWorkItem *xcalarApiMakeAppReapWorkItem(uint64_t appGroupId, bool cancel);
XcalarWorkItem *xcalarApiMakeLogLevelSetWorkItem(uint32_t level,
                                                 uint32_t logFlushLevel,
                                                 int32_t logFlushPeriod);
XcalarWorkItem *xcalarApiMakeLogLevelGetWorkItem();
XcalarWorkItem *xcalarApiMakeGetIpAddrWorkItem(NodeId nodeId);
XcalarWorkItem *xcalarApiMakeListDatasetUsersWorkItem(const char *datasetName);
XcalarWorkItem *xcalarApiMakeListUserDatasetsWorkItem(const char *userIdName);
XcalarWorkItem *xcalarApiMakeGetDatasetsInfoWorkItem(
    const char *datasetNamePattern);
XcalarWorkItem *xcalarApiMakeDatasetCreateWorkItem(
    const char *datasetName,
    const DfLoadArgs *loadArgs,
    const char *sessionName = NULL);
XcalarWorkItem *xcalarApiMakeDatasetDeleteWorkItem(const char *datasetName);
XcalarWorkItem *xcalarApiMakeDatasetUnloadWorkItem(
    const char *datasetNamePattern);
XcalarWorkItem *xcalarApiMakeDatasetGetMetaWorkItem(const char *datasetName);
XcalarWorkItem *xcalarApiMakeSessionDownloadWorkItem(
    const char *sessionName, const char *pathToAdditionalFiles);
XcalarWorkItem *xcalarApiMakeSessionUploadWorkItem(
    const char *sessionName,
    const char *pathToAdditionalFiles,
    size_t sessionContentCount,
    uint8_t sessionContent[]);

XcalarWorkItem *xcalarApiMakeSessionActivateWorkItem(const char *sessionName);

#endif  // _WORKITEM_H_
