# Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# ******************************************************************
# *********** MUST BE KEPT IN SYNC WITH LibApisCommon.h ***********
# *********** NOTE: except for APIs using the new Thrift->Proto model *****
# ******************************************************************
#

include "Status.thrift"
include "DataFormatEnums.thrift"
include "DataTargetEnums.thrift"
include "DataTargetTypes.thrift"
include "LibApisEnums.thrift"
include "LibApisConstants.thrift"
include "OrderingEnums.thrift"
include "JoinOpEnums.thrift"
include "UnionOpEnums.thrift"
include "XcalarEvalEnums.thrift"
include "XcalarApiVersionSignature.thrift"
include "QueryStateEnums.thrift"
include "DagStateEnums.thrift"
include "DagRefTypeEnums.thrift"
include "FunctionCategory.thrift"
include "SourceTypeEnum.thrift"
include "DagTypes.thrift"
include "UdfTypes.thrift"
include "CsvLoadArgsEnums.thrift"

typedef DataTargetTypes.ExExportTargetT XcalarApiExExportTargetInputT;
typedef UdfTypes.UdfModuleSrcT XcalarApiUdfAddInputT;
typedef DataTargetTypes.ExExportTargetHdrT XcalarApiExExportTargetHdrInputT;

const string XcalarOpCodeColumnNameT = "XcalarOpCode";

exception XcalarApiException {
  1: Status.StatusT status
}

// ****** do not update XcalarEvalArgDescT without also updating ******
// ****** XcalarEvalTypes.h                                      ******
struct XcalarEvalArgDescT {
  1: string argDesc
  2: i64 typesAccepted
  3: bool isSingletonValue
  4: XcalarEvalEnums.XcalarEvalArgTypeT argType
  5: i32 minArgs
  6: i32 maxArgs
}

// ****** do not update XcalarEvalFnDescT without also updating  ******
// ****** XcalarEvalTypes.h                                      ******
struct XcalarEvalFnDescT {
  1: string fnName
  2: string fnDesc
  3: FunctionCategory.FunctionCategoryT category
  4: i32 numArgs
  5: list<XcalarEvalArgDescT> argDescs
  6: DataFormatEnums.DfFieldTypeT outputType
}

// ****** do not update DfFieldAttrHeaderT without also updating ******
// ****** DataFormatTypes.h                                      ******
struct DfFieldAttrHeaderT {
  1: string name
  2: DataFormatEnums.DfFieldTypeT type
  3: i32 valueArrayIndex
  4: string ordering
}

// ****** do not update XcalarApiDatasetIdT without also updating ******
// ****** DatasetTypes.h                                          ******
typedef string XcalarApiDatasetIdT
const string XcalarApiDatasetIdInvalidT = ""

typedef string XcalarApiTableIdT
const string XcalarApiTableIdInvalidT = ""

typedef string XcalarApiDagNodeIdT
const string XcalarApiDagNodeIdInvalidT = ""

struct XcalarApiTimeT {
  1: i64 milliseconds
}

struct XcalarApiColumnT {
  1: string sourceColumn
  2: string destColumn
  3: string columnType
}

struct XcalarApiFileAttrT {
  1: bool isDirectory
  2: i64 size
  3: i64 mtime
}

struct XcalarApiFileT {
  1: XcalarApiFileAttrT attr
  2: string name
}

struct XcalarApiListFilesOutputT {
  1: i64 numFiles
  2: list<XcalarApiFileT> files
}

struct XcalarApiListXdfsInputT {
  1: string fnNamePattern
  2: string categoryPattern
}

struct XcalarApiUdfErrorT {
  1: string message
  2: string traceback
}

struct XcalarApiUdfAddUpdateOutputT {
  1: Status.StatusT status
  2: string moduleName
  3: XcalarApiUdfErrorT error
}

struct XcalarApiUdfGetInputT {
  1: string moduleName
}

enum XcalarApiWorkbookScopeT {
    XcalarApiWorkbookScopeInvalid = 0,
    XcalarApiWorkbookScopeGlobal = 1,
    XcalarApiWorkbookScopeUser = 2,
    XcalarApiWorkbookScopeSession = 3
}

struct XcalarApiUdfGetResInputT {
  1: XcalarApiWorkbookScopeT scope
  2: string moduleName
}

struct XcalarApiUdfGetResOutputT {
  1: string udfResPath
}

struct XcalarApiUdfDeleteInputT {
  1: string moduleName
}

struct XcalarApiListXdfsOutputT {
  1: i32 numXdfs
  2: list<XcalarEvalFnDescT> fnDescs
}

const i64 XcalarApiMaxKeyLenT = 255

struct XcalarApiKeyValuePairT {
  1: string key
  2: string value
}

struct XcalarApiKeyAddOrReplaceInputT {
  1: XcalarApiWorkbookScopeT scope
  2: bool persist
  3: XcalarApiKeyValuePairT kvPair
}

struct XcalarApiKeyAppendInputT {
  1: XcalarApiWorkbookScopeT scope
  2: string key
  3: string suffix
}

struct XcalarApiKeySetIfEqualInputT {
  1: XcalarApiWorkbookScopeT scope
  2: bool persist
  3: i32 countSecondaryPairs
  4: string keyCompare
  5: string valueCompare
  6: string valueReplace
  7: string keySecondary
  8: string valueSecondary
}

struct XcalarApiKeyLookupInputT {
  1: XcalarApiWorkbookScopeT scope
  2: string key
}

struct XcalarApiKeyLookupOutputT {
  1: string value
}

struct XcalarApiKeyListInputT {
  1: XcalarApiWorkbookScopeT scope
  2: string keyRegex
}

struct XcalarApiKeyListOutputT {
  1: i32 numKeys
  2: list<string> keys
}

struct XcalarApiKeyDeleteInputT {
  1: XcalarApiWorkbookScopeT scope
  2: string key
}

struct XcalarApiTableInputT {
  1: string tableName
  2: XcalarApiTableIdT tableId
}

struct DataSourceArgsT {
  1: string targetName
  2: string path
  3: string fileNamePattern
  4: bool recursive
}

struct ParseArgsT {
  1: string parserFnName
  2: string parserArgJson
  3: string fileNameFieldName
  4: string recordNumFieldName
  5: bool allowRecordErrors
  6: bool allowFileErrors
  7: list<XcalarApiColumnT> schema
}

struct XcalarApiDfLoadArgsT {
  1: list<DataSourceArgsT> sourceArgsList
  2: ParseArgsT parseArgs
  3: i64 size
}

struct XcalarApiDatasetT {
  1: XcalarApiDfLoadArgsT loadArgs
  // XXX: datasetId is internal facing. Should not be exposed
  2: XcalarApiDatasetIdT datasetId
  3: string name
  4: bool loadIsComplete
  5: bool isListable
  6: string udfName
}

struct XcalarApiColumnInfoT {
  1: string name
  2: string type
}

struct XcalarApiDatasetsInfoT {
  1: string datasetName
  2: bool downSampled
  3: i64 totalNumErrors
  4: i64 datasetSize
  5: i64 numColumns
  6: list<XcalarApiColumnInfoT > columns
}

const string XcalarApiDefaultRecordDelimT = "\n";
const string XcalarApiDefaultFieldDelimT = "\t";
const string XcalarApiDefaultQuoteDelimT = '"';
const string XcalarApiDefaultCsvParserNameT = "default:parseCsv";
const string XcalarApiDefaultJsonParserNameT = "default:parseJson";

struct XcalarApiUdfLoadArgsT {
  1: string fullyQualifiedFnName
}

struct XcalarApiListFilesInputT {
  1: DataSourceArgsT sourceArgs
}

// **************************** Export **************************** //
struct XcalarApiListExportTargetsInputT {
  1: string targetTypePattern
  2: string targetNamePattern
}

struct XcalarApiListExportTargetsOutputT {
  1: i64 numTargets
  2: list<DataTargetTypes.ExExportTargetT> targets
}

struct XcalarApiExportColumnT {
  1: string columnName
  2: string headerName
}

struct XcalarApiExportInputT {
  1: string source
  2: string dest
  3: list<XcalarApiExportColumnT> columns
  4: string driverName
  5: string driverParams
}
// ************************** End Export ************************** //


// ***************************** Apps ***************************** //

struct XcalarApiAppSetInputT {
  1: string name
  2: string hostType
  3: string duty
  4: string execStr
}

struct XcalarApiAppRunInputT {
  1: string name
  2: bool isGlobal
  4: string inStr
}

struct XcalarApiAppRunOutputT {
  1: string appGroupId
}

struct XcalarApiAppReapInputT {
  1: string appGroupId
  2: bool cancel
}

struct XcalarApiAppReapOutputT {
  1: string outStr
  2: string errStr
}

// *************************** End Apps *************************** //

struct XcalarApiEvalT {
  1: string evalString
  2: string newField
}

struct XcalarApiKeyT {
  1: string name
  2: string type
  3: string keyFieldName
  4: string ordering
}

struct XcalarApiDriverInputT {
  1: string inputJson
}

struct XcalarApiDriverOutputT {
  1: string outputJson
}

struct XcalarApiTargetInputT {
  1: string inputJson
}

struct XcalarApiTargetOutputT {
  1: string outputJson
}

struct XcalarApiPreviewInputT {
  1: string inputJson
}

struct XcalarApiPreviewOutputT {
  1: string outputJson
}

struct XcalarApiBulkLoadInputT {
  1: string dest
  2: XcalarApiDfLoadArgsT loadArgs
  3: XcalarApiDagNodeIdT dagNodeId
}

// Must be the same as bulk load input
struct XcalarApiDatasetCreateInputT {
  1: string dest
  2: XcalarApiDfLoadArgsT loadArgs
  3: XcalarApiDagNodeIdT dagNodeId
}

struct XcalarApiIndexInputT {
  1: string source
  2: string dest
  3: list<XcalarApiKeyT> key
  4: string prefix
  5: string dhtName
  6: bool delaySort
  7: bool broadcast
}

struct XcalarApiStatInputT {
  1: i64 nodeId
}

struct XcalarApiDagNameT {
  1: string name
}

struct XcalarApiRetinaDstT {
  1: i32 numColumns
  2: DagTypes.XcalarApiNamedInputT target
  3: list<DataTargetTypes.ExColumnNameT> columns
}

struct XcalarApiRetinaSrcTableT {
  1: string source
  2: string dstName
}

struct XcalarApiMakeRetinaInputT {
  1: string retinaName
  2: i64 numTables
  3: list<XcalarApiRetinaDstT> tableArray
  4: i64 numSrcTables
  5: list<XcalarApiRetinaSrcTableT> srcTables
}

struct XcalarApiGetRetinaInputT {
  1: string retInput
}

struct XcalarApiGetRetinaJsonInputT {
  1: string retinaName
}

struct XcalarApiProjectInputT {
  1: string source
  2: string dest
  3: list<string> columns
}

struct XcalarApiFilterInputT {
  1: string source
  2: string dest
  3: list<XcalarApiEvalT> eval
}

struct XcalarApiGroupByInputT {
  1: string source
  2: string dest
  3: list<XcalarApiEvalT> eval
  4: string newKeyField
  5: bool includeSample
  6: bool icv
  7: bool groupAll
}

struct XcalarApiAggregateInputT {
  1: string source
  2: string dest
  3: list<XcalarApiEvalT> eval
}

struct XcalarApiRenameNodeInputT {
  1: string oldName
  2: string newName
}

struct XcalarApiMakeResultSetInputT {
  1: bool errorDs
  2: DagTypes.XcalarApiNamedInputT dagNode
}

struct XcalarApiResultSetNextInputT {
  1: string resultSetId
  2: i64 numRecords
}

struct XcalarApiFreeResultSetInputT {
  1: string resultSetId
}

struct XcalarApiStatT {
  1: string statName
  2: i64 statValue
  3: i32 statType
  4: i64 groupId
}

struct XcalarApiJoinInputT {
  1: list<string> source
  2: string dest
  3: string joinType
  4: list<list<XcalarApiColumnT>> columns
  5: string evalString
  6: bool keepAllColumns
}

struct XcalarApiUnionInputT {
  1: list<string> source
  2: string dest
  3: bool dedup
  4: list<list<XcalarApiColumnT>> columns
  5: string unionType
}

struct XcalarApiResultSetAbsoluteInputT {
  1: string resultSetId
  2: i64 position
}

struct XcalarApiParameterT {
  1: string paramName
  2: string paramValue
}

struct XcalarApiParamLoadT {
  1: string datasetUrl
  2: string namePattern
}

struct XcalarApiParamSynthesizeT {
  1: string source
}

struct XcalarApiParamFilterT {
  1: string filterStr
}

struct XcalarApiParamExportT {
  1: string fileName
  2: string targetName
  3: DataTargetEnums.ExTargetTypeT targetType
}

union XcalarApiParamInputArgsT {
  1: XcalarApiParamLoadT   paramLoad
  2: XcalarApiParamFilterT paramFilter
  3: XcalarApiParamExportT paramExport
  4: XcalarApiParamSynthesizeT   paramSynthesize
}

struct XcalarApiParamInputT {
  1: LibApisEnums.XcalarApisT paramType
  2: XcalarApiParamInputArgsT paramInputArgs
}

struct XcalarApiUpdateRetinaInputT {
  1: string retinaName
  2: string retinaJson
}

struct XcalarApiListParametersInRetinaOutputT {
  1: i64 numParameters
  2: list<XcalarApiParameterT> parameters
}

struct XcalarApiExecuteRetinaInputT {
  1: string retinaName
  2: string queryName
  3: string dest
  4: list<XcalarApiParameterT> parameters
  5: string schedName
  6: string udfUserName
  7: string udfSessionName
}

struct XcalarApiGetStatOutputT {
  1: i64 numStats
  2: bool truncated
  3: list<XcalarApiStatT> stats
}

struct XcalarApiMapInputT {
  1: string source
  2: string dest
  3: list<XcalarApiEvalT> eval
  4: bool icv
}

struct XcalarApiGetRowNumInputT {
  1: string source
  2: string dest
  3: string newField
}

struct XcalarApiQueryNameInputT {
  1: string queryName
   2: bool detailedStats
}

struct XcalarStatGroupInfoT {
  1: i64 groupIdNum
  2: i64 totalSingleStats
  3: string statsGroupName
}

struct XcalarApiGetStatGroupIdMapOutputT {
    1: i64 numGroupNames
    2: bool truncated
    3: list<XcalarStatGroupInfoT> groupNameInfoArray
}

struct XcalarApiTableMetaT {
  1: i64 numRows
  2: i64 numPages
  3: i64 numSlots
  4: i64 size
  5: list<i64> numRowsPerSlot
  6: list<i64> numPagesPerSlot
  7: i64 xdbPageConsumedInBytes
  8: i64 xdbPageAllocatedInBytes
  9: i64 numTransPageSent
  10: i64 numTransPageRecv
}

struct XcalarApiGetTableMetaOutputT {
  1: i32 numDatasets
  2: list<string> datasets
  3: i32 numResultSets
  4: list<string> resultSetIds
  5: i32 numKeys
  6: list<DfFieldAttrHeaderT> keyAttr
  7: i32 numValues
  8: i32 numImmediates
  9: list<DfFieldAttrHeaderT> valueAttrs
  10: OrderingEnums.XcalarOrderingT ordering
  11: i64 numMetas
  12: list<XcalarApiTableMetaT> metas
  13: string xdbId
}

struct XcalarApiMakeResultSetOutputT {
  1: string resultSetId
  2: i64 numEntries
  3: XcalarApiGetTableMetaOutputT metaOutput
}

struct XcalarApiResultSetNextOutputT {
  1: i64 numValues
  2: list<string> values
}

struct XcalarApiDagNodeInfoT {
  1: string name
  2: XcalarApiDagNodeIdT dagNodeId
  3: DagStateEnums.DgDagStateT state
  4: i64 size
  5: LibApisEnums.XcalarApisT api
  6: bool pinned
}

struct XcalarApiListDagNodesOutputT {
  1: i64 numNodes
  2: list<XcalarApiDagNodeInfoT> nodeInfo
}

struct XcalarApiSessionGenericOutputT {
  1: bool outputAdded
  2: i64 nodeId
  3: string ipAddr
  4: string errorMessage
}

struct XcalarApiSessionPersistOutputT {
  1: XcalarApiSessionGenericOutputT sessionGenericOutput;
}

// This API must complete as fast as possible.
struct XcalarApiListDatasetsOutputT {
  1: i32 numDatasets
  2: list<XcalarApiDatasetT> datasets
}

// This API can do time-consuming operations such as twoPcs to all
// the nodes.
struct XcalarApiGetDatasetsInfoOutputT {
  1: i32 numDatasets
  2: list<XcalarApiDatasetsInfoT> datasets
}

struct XcalarApiDeleteDagNodeStatusT {
  1: XcalarApiDagNodeInfoT nodeInfo
  2: Status.StatusT status
  3: i64 numRefs
  4: list<DagTypes.DagRefT> refs
}

struct XcalarApiDeleteDagNodeOutputT {
  1: i64 numNodes
  2: list<XcalarApiDeleteDagNodeStatusT> statuses
}

struct XcalarApiDatasetUnloadStatusT {
  1: XcalarApiDatasetT dataset
  2: Status.StatusT status
}

struct XcalarApiDatasetUnloadOutputT {
  1: i64 numDatasets
  2: list<XcalarApiDatasetUnloadStatusT> statuses
}

struct XcalarApiNewTableOutputT {
  1: string tableName
}

struct XcalarApiGetTableRefCountOutputT {
  1: i64 refCount
}

struct XcalarApiQueryOutputT {
  1: string queryName
}

struct XcalarApiBulkLoadOutputT {
  1: XcalarApiDatasetT dataset
  2: i64 numFiles
  3: i64 numBytes
  4: string errorString
  5: string errorFile
}

struct XcalarApiGetVersionOutputT {
  1: string version
  2: string apiVersionSignatureFull
  3: XcalarApiVersionSignature.XcalarApiVersionT apiVersionSignatureShort
}

struct XcalarApiAggregateOutputT {
  1: string tableName
  2: string jsonAnswer
}

struct XcalarApiSingleQueryT {
  1: string  singleQuery
  2: Status.StatusT status
}

enum XcalarApiTopRequestTypeT {
    XcalarApiTopRequestTypeInvalid = 0,
    GetCpuAndNetworkTopStats = 1,
    GetAllTopStats = 2,
}

struct XcalarApiTopInputT {
  1: i64 measureIntervalInMs
  2: i64 cacheValidityInMs
  3: XcalarApiTopRequestTypeT topStatsRequestType
}

struct XcalarApiTopOutputPerNodeT {
  1: i64 nodeId
  2: double cpuUsageInPercent
  3: double memUsageInPercent
  4: i64 memUsedInBytes
  5: i64 totalAvailableMemInBytes
  6: i64 networkRecvInBytesPerSec
  7: i64 networkSendInBytesPerSec
  8: i64 xdbUsedBytes
  9: i64 xdbTotalBytes
  10: double parentCpuUsageInPercent
  11: double childrenCpuUsageInPercent
  12: i64 numCores
  13: i64 sysSwapUsedInBytes
  14: i64 sysSwapTotalInBytes
  15: i64 uptimeInSeconds
  16: i64 datasetUsedBytes
  17: i64 sysMemUsedInBytes
  18: i64 publishedTableUsedBytes
}

struct XcalarApiTopOutputT {
  1: Status.StatusT status
  2: i64 numNodes
  3: list<XcalarApiTopOutputPerNodeT> topOutputPerNode
}

struct XcalarApiQueryInputT {
  # see comment for "sameSession" in LibApisCommon.h for XcalarApiQueryInput
  1: bool sameSession
  2: string queryName
  3: string queryStr
  4: bool bailOnError
  5: string schedName
  6: bool isAsync
  7: string udfUserName
  8: string udfSessionName
  9: bool pinResults
  10: bool collectStats
}

struct XcalarApiQueryListInputT {
  1: string namePattern
}

struct XcalarApiUserIdT {
  1: string userIdName
}

struct XcalarApiSessionNewInputT {
  1: string sessionName
  2: bool fork
  3: string forkedSessionName
}

struct XcalarApiSessionNewOutputT {
  1: XcalarApiSessionGenericOutputT sessionGenericOutput
  2: string sessionId
}

struct XcalarApiSessionDeleteInputT {
  1: string sessionName
  // This option must be false.
  2: bool noCleanup
}

struct XcalarApiPtChangeOwnerInputT {
  1: string publishTableName
  2: string userIdName
  3: string sessionName
}

struct XcalarApiSessionActivateInputT {
  1: string sessionName
}

enum RuntimeTypeT {
    Latency = 0,
    Throughput = 1,
    Immediate = 2,
    Invalid = 3,
}

struct XcalarApiSchedParamT {
  1: string schedName
  2: i32 cpusReservedInPercent
  3: RuntimeTypeT runtimeType
}

struct XcalarApiRuntimeSetParamInputT {
  1: list<XcalarApiSchedParamT> schedParams
}

struct XcalarApiSessionRenameInputT {
  1: string sessionName
  2: string origSessionName
}

struct XcalarApiSessionDownloadInputT {
  1: string sessionName
  2: string pathToAdditionalFiles
}

struct XcalarApiSessionDownloadOutputT {
  1: i64 sessionContentCount
  2: string sessionContent
  }

struct XcalarApiSessionUploadInputT {
  1: string sessionName
  2: string pathToAdditionalFiles
  3: i64 sessionContentCount
  4: string sessionContent
}

struct XcalarApiGetQueryOutputT {
  1: string query
}

struct XcalarApiDagNodeNamePatternInputT {
  1: string namePattern
  2: SourceTypeEnum.SourceTypeT srcType
  3: bool deleteCompletely
}

struct XcalarApiArchiveTablesInputT {
  1: bool archive
  2: bool allTables
  3: list<string> tableNames
}

// ****** do not update DhtArgsT without also updating ******
// ****** DhtTypes.h                                   ******
struct DhtArgsT {
  1: double upperBound
  2: double lowerBound
  3: OrderingEnums.XcalarOrderingT ordering
}

struct XcalarApiCreateDhtInputT {
  1: string dhtName
  2: DhtArgsT dhtArgs
}

struct XcalarApiDeleteDhtInputT {
  1: i64 dhtNameLen
  2: string dhtName
}

struct XcalarApiSupportGenerateInputT {
  1: bool generateMiniBundle;
  2: i64 supportCaseId
}

struct XcalarApiSupportGenerateOutputT {
  1: string supportId
  2: bool supportBundleSent
  3: string bundlePath
}

// ****** do not update IndexErrorStatsT, LoadErrorStatsT, EvalErrorStatsT *****
// ****** OpErrorStatsT, OpDetailsT without also updating OperatorsTypes.h *****

struct IndexErrorStatsT {
  1: i64 numParseError
  2: i64 numFieldNoExist
  3: i64 numTypeMismatch
  4: i64 numOtherError
}

struct LoadErrorStatsT {
  1: i64 numFileOpenFailure
  2: i64 numDirOpenFailure
}

struct FailureDescT {
    1: i64 numRowsFailed
    2: string failureDesc
}

struct FailureSummaryT {
    1: string failureSummName
    2: list<FailureDescT> failureSummInfo
}

struct EvalUdfErrorStatsT {
  1: i64 numEvalUdfError
  2: list<FailureSummaryT> opFailureSummary
}

struct EvalXdfErrorStatsT {
  1: i64 numUnsubstituted
  2: i64 numUnspportedTypes
  3: i64 numMixedTypeNotSupported
  4: i64 numEvalCastError
  5: i64 numDivByZero
  6: i64 numMiscError
  7: i64 numTotal // grand total
}

struct EvalErrorStatsT {
  1: EvalXdfErrorStatsT evalXdfErrorStats
  2: EvalUdfErrorStatsT evalUdfErrorStats
}

union OpErrorStatsT {
  1: LoadErrorStatsT loadErrorStats
  2: IndexErrorStatsT indexErrorStats
  3: EvalErrorStatsT evalErrorStats
}

struct XcalarApiOpDetailsT {
  1: i64     numWorkCompleted
  2: i64     numWorkTotal
  3: bool         cancelled
  4: OpErrorStatsT errorStats
  5: i64     numRowsTotal
}

struct XcalarApiNodeOpStatsT {
  1: Status.StatusT status
  2: i64 nodeId
  3: XcalarApiOpDetailsT opDetails
}

struct XcalarApiPerNodeOpStatsT {
  1: i64 numNodes
  2: LibApisEnums.XcalarApisT api
  3: list<XcalarApiNodeOpStatsT> nodeOpStats
}

struct XcalarApiOpStatsOutT {
  1: LibApisEnums.XcalarApisT api
  2: XcalarApiOpDetailsT opDetails
}

struct XcalarApiImportRetinaInputT {
  1: string retinaName
  2: bool overwriteExistingUdf
  3: i64 retinaCount
  4: string retina
  5: bool loadRetinaJson
  6: string retinaJson
  7: string udfUserName
  8: string udfSessionName
}

struct XcalarApiExportRetinaInputT {
  1: string retinaName
}

struct XcalarApiStartFuncTestInputT {
  1: bool parallel
  2: bool runAllTests
  3: bool runOnAllNodes
  4: i32 numTestPatterns
  5: list<string> testNamePatterns
}

struct XcalarApiListFuncTestInputT {
  1: string namePattern
}

struct XcalarApiConfigParamT {
  1: string paramName
  2: string paramValue
  3: bool visible
  4: bool changeable
  5: bool restartRequired
  6: string defaultValue
}

struct XcalarApiGetConfigParamsOutputT {
  1: i64 numParams
  2: list<XcalarApiConfigParamT> parameter
  }

struct XcalarApiSetConfigParamInputT {
  1: string paramName
  2: string paramValue
}

struct XcalarApiGetTableMetaInputT {
  1: DagTypes.XcalarApiNamedInputT tableNameInput
  2: bool isPrecise
}

struct XcalarApiGetMemoryUsageInputT {
  1: string userName
  2: i64 userId
}

struct XcalarApiLogLevelSetInputT {
  1: i32 logLevel
  2: bool logFlush
  3: i32 logFlushLevel
  4: i32 logFlushPeriod
}

struct XcalarApiDagTableNameInputT {
  1: string tableInput
}

struct XcalarApiListParametersInRetinaInputT {
  1: string listRetInput
}

struct XcalarApiSessionListArrayInputT {
  1: string sesListInput
}

struct XcalarApiDeleteRetinaInputT {
  1: string delRetInput
}

struct XcalarApiShutdownInputT {
  1: bool doShutdown
}

struct XcalarApiGetIpAddrInputT {
  1: i64 nodeId
}

struct XcalarApiTagDagNodesInputT {
  1: list<DagTypes.XcalarApiNamedInputT> dagNodes
  2: string tag
}

struct XcalarApiCommentDagNodesInputT {
  1: i32 numDagNodes
  2: list<string> dagNodeNames
  3: string comment
}

struct XcalarApiGetDatasetsInfoInputT {
  1: string datasetsNamePattern
}

struct XcalarApiListDatasetUsersInputT {
  1: string datasetName
}

struct XcalarApiListUserDatasetsInputT {
  1: string userIdName
}

struct XcalarApiDatasetDeleteInputT {
  1: string datasetName
}

struct XcalarApiDatasetUnloadInputT {
  1: string datasetNamePattern
}

struct XcalarApiDatasetGetMetaInputT {
  1: string datasetName
}

struct XcalarApiSynthesizeInputT {
  1: string source
  2: string dest
  3: list<XcalarApiColumnT> columns
  4: bool sameSession
}

struct XcalarApiPublishInputT {
  1: string source
  2: string dest
  3: i64 unixTS
  4: bool dropSrc
}

struct XcalarApiUpdateTableInputT {
  1: string source
  2: string dest
  3: i64 unixTS
  4: bool dropSrc
}

struct XcalarApiUpdateInputT {
  1: list<XcalarApiUpdateTableInputT> updates
}

struct XcalarApiSelectInputT {
  1: string source
  2: string dest
  3: i64 minBatchId
  4: i64 maxBatchId
  5: string filterString
  6: list<XcalarApiColumnT> columns
  7: i64 limitRows
}

struct XcalarApiUnpublishInputT {
  1: string source
  2: bool inactivateOnly
}


struct XcalarApiRestoreTableInputT {
  1: string publishedTableName
}

struct XcalarApiCoalesceInputT {
  1: string source
}

struct XcalarApiListTablesInputT {
  1: string namePattern
  2: bool getUpdates
  3: bool getSelects
  4: i32 updateStartBatchId
}

struct XcalarApiListRetinasInputT {
  1: string namePattern
}

struct XcalarApiIndexRequestInputT {
  1: string tableName
  2: string keyName
}

union XcalarApiInputT {
  1: XcalarApiBulkLoadInputT      loadInput
  2: XcalarApiIndexInputT         indexInput
  3: XcalarApiStatInputT          statInput
  4: XcalarApiGetTableMetaInputT  getTableMetaInput
  5: XcalarApiResultSetNextInputT resultSetNextInput
  6: XcalarApiJoinInputT          joinInput
  7: XcalarApiFilterInputT        filterInput
  8: XcalarApiGroupByInputT       groupByInput
  9: XcalarApiResultSetAbsoluteInputT resultSetAbsoluteInput
  10: XcalarApiFreeResultSetInputT freeResultSetInput
  11: XcalarApiTableInputT             getTableRefCountInput
  12: XcalarApiDagNodeNamePatternInputT  listDagNodesInput
  13: XcalarApiDagNodeNamePatternInputT  deleteDagNodeInput
  14: XcalarApiQueryInputT        queryInput
  16: XcalarApiMakeResultSetInputT makeResultSetInput
  17: XcalarApiMapInputT          mapInput
  18: XcalarApiAggregateInputT    aggregateInput
  19: XcalarApiQueryNameInputT   queryStateInput
  20: XcalarApiExExportTargetInputT addTargetInput
  21: XcalarApiListExportTargetsInputT listTargetsInput
  22: XcalarApiExportInputT       exportInput
  23: XcalarApiDagTableNameInputT dagTableNameInput
  24: XcalarApiListFilesInputT    listFilesInput
  26: XcalarApiMakeRetinaInputT   makeRetinaInput
  27: XcalarApiGetRetinaInputT    getRetinaInput
  28: XcalarApiExecuteRetinaInputT executeRetinaInput
  29: XcalarApiUpdateRetinaInputT updateRetinaInput
  31: XcalarApiListParametersInRetinaInputT listParametersInRetinaInput
  32: XcalarApiKeyLookupInputT    keyLookupInput
  33: XcalarApiKeyAddOrReplaceInputT keyAddOrReplaceInput
  34: XcalarApiKeyDeleteInputT    keyDeleteInput
  35: XcalarApiTopInputT          topInput
  36: XcalarApiShutdownInputT     shutdownInput
  37: XcalarApiListXdfsInputT     listXdfsInput
  38: XcalarApiRenameNodeInputT   renameNodeInput
  40: XcalarApiSessionNewInputT    sessionNewInput
  41: XcalarApiSessionDeleteInputT sessionDeleteInput
  43: XcalarApiSessionListArrayInputT sessionListInput
  44: XcalarApiSessionRenameInputT sessionRenameInput
  45: XcalarApiCreateDhtInputT    createDhtInput
  46: XcalarApiKeyAppendInputT    keyAppendInput
  47: XcalarApiKeySetIfEqualInputT keySetIfEqualInput
  48: XcalarApiDeleteDhtInputT    deleteDhtInput
  49: XcalarApiDeleteRetinaInputT deleteRetinaInput
  53: XcalarApiProjectInputT       projectInput
  54: XcalarApiGetRowNumInputT     getRowNumInput
  55: XcalarApiUdfAddInputT udfAddUpdateInput
  56: XcalarApiUdfGetInputT udfGetInput
  57: XcalarApiUdfDeleteInputT udfDeleteInput
  58: XcalarApiPreviewInputT       previewInput
  59: XcalarApiImportRetinaInputT  importRetinaInput
  60: XcalarApiExportRetinaInputT  exportRetinaInput
  61: XcalarApiStartFuncTestInputT startFuncTestInput
  62: XcalarApiListFuncTestInputT listFuncTestInput
  64: XcalarApiSetConfigParamInputT setConfigParamInput
  65: XcalarApiExExportTargetHdrInputT removeTargetInput
  66: XcalarApiAppSetInputT appSetInput
  68: XcalarApiAppRunInputT appRunInput
  69: XcalarApiAppReapInputT appReapInput
  72: XcalarApiGetMemoryUsageInputT memoryUsageInput
  73: XcalarApiLogLevelSetInputT logLevelSetInput
  74: XcalarApiGetIpAddrInputT getIpAddrInput
  75: XcalarApiSupportGenerateInputT supportGenerateInput
  76: XcalarApiTagDagNodesInputT tagDagNodesInput
  77: XcalarApiCommentDagNodesInputT commentDagNodesInput
  78: XcalarApiListDatasetUsersInputT listDatasetUsersInput
  80: XcalarApiKeyListInputT keyListInput
  81: XcalarApiListUserDatasetsInputT listUserDatasetsInput
  82: XcalarApiUnionInputT unionInput
  83: XcalarApiTargetInputT targetInput
  84: XcalarApiSynthesizeInputT synthesizeInput
  85: XcalarApiGetRetinaJsonInputT getRetinaJsonInput
  86: XcalarApiGetDatasetsInfoInputT getDatasetsInfoInput
  87: XcalarApiArchiveTablesInputT archiveTablesInput
  88: XcalarApiSessionDownloadInputT sessionDownloadInput
  89: XcalarApiSessionUploadInputT sessionUploadInput
  90: XcalarApiPublishInputT publishInput
  91: XcalarApiUpdateInputT updateInput
  92: XcalarApiSelectInputT selectInput
  93: XcalarApiUnpublishInputT unpublishInput
  94: XcalarApiListTablesInputT listTablesInput
  95: XcalarApiRestoreTableInputT restoreTableInput
  96: XcalarApiCoalesceInputT coalesceInput
  98: XcalarApiSessionActivateInputT sessionActivateInput
  99: XcalarApiPtChangeOwnerInputT ptChangeOwnerInput
  100: XcalarApiDriverInputT driverInput
  101: XcalarApiRuntimeSetParamInputT runtimeSetParamInput
  // Create dataset uses same input as bulk load
  103: XcalarApiDatasetCreateInputT datasetCreateInput
  104: XcalarApiDatasetDeleteInputT datasetDeleteInput
  105: XcalarApiDatasetUnloadInputT datasetUnloadInput
  108: XcalarApiDatasetGetMetaInputT datasetGetMetaInput
  109: XcalarApiUdfGetResInputT udfGetResInput
  111: XcalarApiQueryListInputT queryListInput
  112: XcalarApiListRetinasInputT listRetinasInput
  113: XcalarApiIndexRequestInputT indexRequestInput
}

struct OpFailureInfoT {
    1: i64 numRowsFailedTotal
    2: list<FailureSummaryT> opFailureSummary
}

struct SlotInfoT {
    1: list<i32> numRowsPerSlot
    2: list<i32> numPagesPerSlot
}

struct XcalarApiDagNodeT {
    1: XcalarApiDagNameT name
    2: string tag
    3: string comment
    4: XcalarApiDagNodeIdT dagNodeId
    5: LibApisEnums.XcalarApisT api
    6: DagStateEnums.DgDagStateT state
    7: i64 xdbBytesRequired
    8: i64 xdbBytesConsumed
    9: i64 numTransPageSent
    10: i64 numTransPageRecv
    11: i64 numWorkCompleted
    12: i64 numWorkTotal
    13: XcalarApiTimeT elapsed
    14: i64 inputSize
    15: XcalarApiInputT input
    16: i64 numRowsTotal
    17: i32 numNodes
    18: list<i64> numRowsPerNode
    19: i64 sizeTotal
    20: list<i64> sizePerNode
    21: list<i64> numTransPagesReceivedPerNode
    22: i64 numParents
    23: list<XcalarApiDagNodeIdT> parents
    24: i64 numChildren
    25: list<XcalarApiDagNodeIdT> children
    26: string log
    27: Status.StatusT status
    28: bool pinned
    29: i64 startTime
    30: i64 endTime
    31: OpFailureInfoT opFailureInfo
    32: list<i8> hashSlotSkewPerNode
}

struct XcalarApiDagOutputT {
    1: i64 numNodes
    2: list<XcalarApiDagNodeT> node
}

struct DagRetinaDescT {
    1: string retinaName
}

struct XcalarApiRetinaT {
    1: DagRetinaDescT retinaDesc
    2: XcalarApiDagOutputT retinaDag
}

struct XcalarApiQueryStateOutputT {
  1: QueryStateEnums.QueryStateT queryState
  2: Status.StatusT queryStatus
  3: string query
  4: i64 numQueuedWorkItem
  5: i64 numCompletedWorkItem
  6: i64 numFailedWorkItem
  7: XcalarApiTimeT elapsed
  8: XcalarApiDagOutputT queryGraph
  9: i32 queryNodeId
}

struct XcalarApiListRetinasOutputT {
    1: i64 numRetinas
    2: list<DagRetinaDescT> retinaDescs
}

struct XcalarApiGetRetinaOutputT {
    1: XcalarApiRetinaT retina
}

struct XcalarApiGetRetinaJsonOutputT {
    1: string retinaJson
}

// if the state is not active, activeNode should not be used
struct XcalarApiSessionT {
  1: string name
  2: string state
  3: string info
  4: i32    activeNode
  5: string sessionId
  6: string description
}

struct XcalarApiSessionListOutputT {
  1: i64 numSessions
  2: list<XcalarApiSessionT> sessions
  3: XcalarApiSessionGenericOutputT sessionGenericOutput
}

struct XcalarApiImportRetinaOutputT {
  1: i64 numUdfModules
  2: list<XcalarApiUdfAddUpdateOutputT> udfModuleStatuses
}

struct XcalarApiExportRetinaOutputT {
  1: i64 retinaCount
  2: string retina
}

struct XcalarApiFuncTestOutputT {
  1: string testName
  2: Status.StatusT status
}

struct XcalarApiStartFuncTestOutputT {
  1: i32 numTests
  2: list<XcalarApiFuncTestOutputT> testOutputs
}

struct XcalarApiListFuncTestOutputT {
  1: i32 numTests
  2: list<string> testNames
}

struct XcalarApiDatasetMemoryUsageT {
  1: string datasetName
  2: string datsetId
  3: i64 totalBytes
  4: i32 numNodes
  5: list<i64> bytesPerNode
}

struct XcalarApiTableMemoryUsageT {
  1: string tableName
  2: string tableId
  3: i64 totalBytes
}

struct XcalarApiSessionMemoryUsageT {
  1: string sessionName
  2: i32 numTables
  3: list<XcalarApiTableMemoryUsageT> tableMemory
}

struct XcalarApiUserMemoryUsageT {
  1: string userName
  2: string userId
  3: i32 numSessions
  4: list<XcalarApiSessionMemoryUsageT> sessionMemory
}

struct XemClientConfigParamsT {
  1: bool enableStatsShipment
  2: bool isMultipleNodesPerHost
  3: i64 xemHostPortNumber
  4: i64 statsPushHeartBeat
  5: string xemHostAddress
  6: string clusterName
}

struct XcalarApiRuntimeGetParamOutputT {
  1: list<XcalarApiSchedParamT> schedParams
}

struct XcalarApiGetMemoryUsageOutputT {
  1: XcalarApiUserMemoryUsageT userMemory
}

struct XcalarApiGetIpAddrOutputT {
  1: string ipAddr
}

struct XcalarApiGetNumNodesOutputT {
  1: i64 numNodes
}

struct XcalarApiDatasetUserT {
  1: XcalarApiUserIdT userId;
  2: i64 referenceCount
}

struct XcalarApiListDatasetUsersOutputT {
  1: i64 usersCount
  2: list<XcalarApiDatasetUserT> user
}

struct XcalarApiUserDatasetT {
  1: string datasetName;
  2: bool isLocked;
}

struct XcalarApiListUserDatasetsOutputT {
  1: i64 numDatasets
  2: list<XcalarApiUserDatasetT> datasets
}

struct XcalarApiDatasetGetMetaOutputT {
  1: string datasetMeta
}

struct XcalarApiLogLevelGetOutputT {
  1: i32 logLevel
  2: i32 logFlushPeriod
}

struct XcalarApiUpdateInfoT {
  1: string source
  2: i64 startTS
  3: i64 batchId
  4: i64 size
  5: i64 numRows
  6: i64 numInserts
  7: i64 numUpdates
  8: i64 numDeletes
}

struct XcalarApiIndexInfoT {
  1: XcalarApiColumnInfoT key
  2: i64 sizeEstimate
  3: XcalarApiTimeT uptime
}

struct XcalarApiTableInfoT {
  1: string name
  2: i32 numPersistedUpdates
  3: XcalarApiPublishInputT source
  4: list<XcalarApiUpdateInfoT> updates
  5: list<XcalarApiSelectInputT> selects
  6: i64 oldestBatchId
  7: i64 nextBatchId
  8: list<XcalarApiColumnInfoT> keys
  9: list<XcalarApiColumnInfoT> values
  10: bool active
  11: i64 sizeTotal
  12: i64 numRowsTotal
  13: bool restoring
  14: string userIdName
  15: string sessionName
  16: list<XcalarApiIndexInfoT> indices
}

struct XcalarApiListTablesOutputT {
  1: i64 numTables
  2: list<XcalarApiTableInfoT> tables
}

struct XcalarApiUpdateOutputT {
  1: list<i64> batchIds
}

struct XcalarApiQueryInfoT {
  1: string name
  2: XcalarApiTimeT elapsed
  3: string state
}

struct XcalarApiQueryListOutputT {
  1: list<XcalarApiQueryInfoT> queries
}

struct XcalarApiRestoreTableOutputT {
  1: list<string> dependencies
}

union XcalarApiOutputResultT {
  1: XcalarApiGetVersionOutputT    getVersionOutput
  2: Status.StatusT                statusOutput
  3: XcalarApiGetStatOutputT       statOutput
  4: XcalarApiListDagNodesOutputT    listNodesOutput
  5: XcalarApiMakeResultSetOutputT makeResultSetOutput
  6: XcalarApiResultSetNextOutputT resultSetNextOutput
  7: XcalarApiGetTableMetaOutputT  getTableMetaOutput
  8: XcalarApiNewTableOutputT      indexOutput
  9: XcalarApiBulkLoadOutputT      loadOutput
  10: XcalarApiGetTableRefCountOutputT getTableRefCountOutput
  11: XcalarApiDeleteDagNodeOutputT deleteDagNodesOutput
  12: XcalarApiNewTableOutputT     joinOutput
  13: XcalarApiGetStatGroupIdMapOutputT statGroupIdMapOutput
  14: XcalarApiListDatasetsOutputT    listDatasetsOutput
  15: XcalarApiNewTableOutputT     mapOutput
  16: XcalarApiAggregateOutputT    aggregateOutput
  17: XcalarApiNewTableOutputT     filterOutput
  18: XcalarApiQueryOutputT        queryOutput
  19: XcalarApiQueryStateOutputT   queryStateOutput
  20: XcalarApiListExportTargetsOutputT listTargetsOutput
  21: XcalarApiDagOutputT          dagOutput
  22: XcalarApiListFilesOutputT    listFilesOutput
  23: XcalarApiNewTableOutputT     groupByOutput
  24: XcalarApiListRetinasOutputT  listRetinasOutput
  25: XcalarApiGetRetinaOutputT    getRetinaOutput
  26: XcalarApiListParametersInRetinaOutputT listParametersInRetinaOutput
  27: XcalarApiKeyLookupOutputT    keyLookupOutput
  28: XcalarApiTopOutputT          topOutput
  29: XcalarApiListXdfsOutputT     listXdfsOutput
  31: XcalarApiSessionListOutputT  sessionListOutput
  32: XcalarApiGetQueryOutputT     getQueryOutput
  34: XcalarApiSupportGenerateOutputT supportGenerateOutput
  35: XcalarApiNewTableOutputT     projectOutput
  36: XcalarApiNewTableOutputT     getRowNumOutput
  37: XcalarApiUdfAddUpdateOutputT udfAddUpdateOutput
  38: UdfTypes.UdfModuleSrcT udfGetOutput
  39: XcalarApiPerNodeOpStatsT     perNodeOpStatsOutput
  40: XcalarApiOpStatsOutT         opStatsOutput
  41: XcalarApiImportRetinaOutputT importRetinaOutput
  42: XcalarApiPreviewOutputT      previewOutput
  43: XcalarApiExportRetinaOutputT exportRetinaOutput
  44: XcalarApiStartFuncTestOutputT startFuncTestOutput
  45: XcalarApiListFuncTestOutputT listFuncTestOutput
  46: XcalarApiNewTableOutputT      executeRetinaOutput
  48: XcalarApiGetConfigParamsOutputT getConfigParamsOutput
  50: XcalarApiAppRunOutputT appRunOutput
  51: XcalarApiAppReapOutputT appReapOutput
  53: XcalarApiGetMemoryUsageOutputT memoryUsageOutput
  54: XcalarApiGetIpAddrOutputT getIpAddrOutput
  55: XcalarApiGetNumNodesOutputT getNumNodesOutput
  56: XcalarApiSessionGenericOutputT sessionGenericOutput
  57: XcalarApiSessionNewOutputT sessionNewOutput
  58: XcalarApiListDatasetUsersOutputT listDatasetUsersOutput
  59: XcalarApiLogLevelGetOutputT logLevelGetOutput
  60: XcalarApiKeyListOutputT keyListOutput
  61: XemClientConfigParamsT getCurrentXemConfigOutput
  62: XcalarApiListUserDatasetsOutputT listUserDatasetsOutput
  63: XcalarApiNewTableOutputT unionOutput
  64: XcalarApiTargetOutputT targetOutput
  65: XcalarApiNewTableOutputT synthesizeOutput
  66: XcalarApiGetRetinaJsonOutputT    getRetinaJsonOutput
  67: XcalarApiGetDatasetsInfoOutputT getDatasetsInfoOutput
  68: XcalarApiDeleteDagNodeOutputT archiveTablesOutput
  69: XcalarApiSessionDownloadOutputT sessionDownloadOutput
  70: XcalarApiListTablesOutputT listTablesOutput
  71: XcalarApiNewTableOutputT  selectOutput
  72: XcalarApiUpdateOutputT updateOutput
  73: XcalarApiDriverOutputT driverOutput
  74: XcalarApiRuntimeGetParamOutputT runtimeGetParamOutput
  76: XcalarApiDatasetUnloadOutputT datasetUnloadOutput
  77: XcalarApiDatasetGetMetaOutputT datasetGetMetaOutput
  78: XcalarApiUdfGetResOutputT udfGetResOutput
  80: XcalarApiQueryListOutputT queryListOutput
  81: XcalarApiRestoreTableOutputT restoreTableOutput
}

struct XcalarApiOutputHeaderT {
    1: Status.StatusT status
    2: XcalarApiTimeT elapsed
    3: string log
}

struct XcalarApiOutputT {
  1:XcalarApiOutputHeaderT hdr
  2:XcalarApiOutputResultT outputResult
}

struct XcalarApiWorkItemT {
  1: XcalarApiVersionSignature.XcalarApiVersionT apiVersionSignature
  2: LibApisEnums.XcalarApisT api
  3: XcalarApiInputT input
  4: string userId
  5: i64 userIdUnique
  6: LibApisEnums.XcalarApisT origApi
  7: string sessionName
}

struct XcalarApiWorkItemResult {
 # equivalent to libapis_common.h:XcalarWorkItem.status
  1: Status.StatusT jobStatus
  2: XcalarApiOutputT output
}

service XcalarApiService {
  XcalarApiWorkItemResult queueWork(1:XcalarApiWorkItemT workItem) throws (1:XcalarApiException err)
}
