// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//
// ******************************************************************
// ******** MUST BE KEPT IN SYNC WITH LibApisCommon.thrift *********
// ******************************************************************

#ifndef _LIBAPIS_COMMON_H_
#define _LIBAPIS_COMMON_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "df/DataFormatBaseTypes.h"
#include "export/DataTargetTypes.h"
#include "dag/DagTypes.h"
#include "dag/RetinaTypes.h"
#include "SocketTypes.h"
#include "operators/OperatorsTypes.h"
#include "JoinOpEnums.h"
#include "UnionOpEnums.h"
#include "LibApisEnums.h"
#include "stat/StatisticsTypes.h"
#include "libapis/LibApisConstants.h"
#include "QueryStateEnums.h"
#include "operators/XcalarEvalTypes.h"
#include "DagStateEnums.h"
#include "bits/posix1_lim.h"  // LOGIN_NAME_MAX
#include "SourceTypeEnum.h"
#include "operators/DhtTypes.h"
#include "udf/UdfTypes.h"
#include "app/App.h"
#include "test/FuncTests/FuncTestTypes.h"
#include "config/Config.h"
#include "xdb/Xdb.h"
#include "license/LicenseConstants.h"
#include "XcalarApiVersionSignature.h"
#include <arpa/inet.h>
#include "RuntimeEnums.h"

#define XcalarApiDefaultDestIP SOCK_LOCALHOST
#define XcalarApiDefaultDestPort 18552
#define XcalarApiTempTablePrefix "tempTable-"
#define XcalarApiTempDatasetPrefix "tempDataset-"
#define XcalarTempDagNodePrefix "temp-"
#define XcalarApiTempQueryPrefix "tempQuery-"
#define XcalarApiTempDhtPrefix "tempDht-"
#define XcalarApiDatasetPrefix ".XcalarDS."
#define XcalarApiRetinaPrefix ".XcalarRetina."
#define XcalarApiDatasetPrefixLen (sizeof(XcalarApiDatasetPrefix) - 1)
#define XcalarApiDeleteObjNodePrefix "deleteObj-"
#define XcalarApiArchiveTablePrefix "archiveTable-"
#define XcalarApiBroadcastTablePrefix "xcalarBroadcast"

#define XcalarOpCodeColumnName "XcalarOpCode"
#define XcalarRankOverColumnName "XcalarRankOver"
#define XcalarBatchIdColumnName "XcalarBatchId"

// Temporary until we have more granular dataset namespace
#define XcalarApiLrqPrefix ".XcalarLRQ."
#define XcalarApiLrqPrefixLen (sizeof(XcalarApiLrqPrefix) - 1)

#define XcalarApiLrqExportPrefix ".XcalarLRQExport."
#define XcalarApiLrqExportPrefixLen (sizeof(XcalarApiLrqPrefix) - 1)

#define XcalarApiSizeOfOutput(output) \
    (offsetof(XcalarApiOutput, outputResult) + sizeof(output))

typedef uint64_t XcalarApiXid;
typedef DagTypes::NodeId XcalarApiDagNodeId;

typedef ExExportTarget XcalarApiExExportTargetInput;
typedef UdfModuleSrc XcalarApiUdfAddInput;
typedef ExExportTargetHdr XcalarApiExExportTargetHdrInput;
typedef App::Packed XcalarApiPackedInput;
struct XcalarApiDagNodeNamePatternInput;
typedef XcalarApiDagNodeNamePatternInput XcalarApiDagNodeNamePatternDeleteInput;
// The dataset create uses the same arguments at bulk load
typedef XcalarApiBulkLoadInput XcalarApiDatasetCreateInput;

union XcalarApiInput;

enum {
    XcalarApiMaxKeyLen = 255,
    XcalarApiVersionSignatureBufLen = 100,
    XcalarApiVersionBufLen = 80,
    XcalarApiLicenseBufLen = 32,
    XcalarApiDgMaxChildNode = 128,
    XcalarApiMaxInputSize = 32 * KB,
    QrMaxNameLen = 255,
    XcalarApiMaxLicenseAttributesSize = 16 * KB,
    XcalarApiMaxLicenseeLen = 1 * KB,
};

struct XcalarApiTime {
    uint64_t milliseconds;
};

// XXX: NOTE: the following enum must be kept in sync with
// XcalarApiWorkbookScopeT in LibApisCommon.thrift.
// MgmtDaemon.cpp casts the thrift workbook scope enum
// to the C-equivalent below. We're not renaming the
// C-struct here due to ser/deser impact.
enum XcalarApiKeyScope {
    XcalarApiKeyInvalid = 0,
    XcalarApiKeyScopeGlobal = 1,
    XcalarApiKeyScopeUser = 2,
    XcalarApiKeyScopeSession = 3
};

struct XcalarApiUserId {
    char userIdName[LOGIN_NAME_MAX + 1];
    unsigned int userIdUnique;
};

struct XcalarApiKeyValuePair {
    char key[XcalarApiMaxKeyLen + 1];
    size_t valueSize;
    char value[0];
};

struct XcalarApiKeyAddOrReplaceInput {
    XcalarApiKeyScope scope;
    bool persist;
    XcalarApiKeyValuePair kvPair;
};

struct XcalarApiKeyAppendInput {
    XcalarApiKeyScope scope;
    char key[XcalarApiMaxKeyLen + 1];
    size_t suffixSize;
    char suffix[0];
};

struct XcalarApiKeySetIfEqualInput {
    XcalarApiKeyScope scope;
    bool persist;
    uint32_t countSecondaryPairs;
    char keyCompare[XcalarApiMaxKeyLen + 1];
    char keySecondary[XcalarApiMaxKeyLen + 1];

    size_t valueCompareSize;
    size_t valueReplaceSize;
    size_t valueSecondarySize;

    char values[0];
};

struct XcalarApiKeyListInput {
    XcalarApiKeyScope scope;
    char keyRegex[XcalarApiMaxKeyLen + 1];
};

struct XcalarApiKeyListOutput {
    int numKeys;
    char keys[0][XcalarApiMaxKeyLen + 1];
};

struct XcalarApiKeyDeleteInput {
    XcalarApiKeyScope scope;
    char key[XcalarApiMaxKeyLen + 1];
};

struct XcalarApiFileAttr {
    bool isDirectory;
    uint64_t size;
    time_t mtime;
    // XXX add user, group, inodeNum, atime, mode, etc.
};

struct XcalarApiFile {
    XcalarApiFileAttr attr;
    char name[XcalarApiMaxFileNameLen + 1];
};

struct XcalarApiListFilesInput {
    DataSourceArgs sourceArgs;
};

struct XcalarApiListFilesOutput {
    uint64_t numFiles;
    XcalarApiFile files[0];
};

struct XcalarApiListXdfsInput {
    char fnNamePattern[XcalarEvalMaxFnNameLen + 1];
    char categoryPattern[XcalarEvalMaxFnNameLen + 1];
};

struct XcalarApiListXdfsOutput {
    unsigned numXdfs;
    XcalarEvalFnDesc fnDescs[0];
};

struct XcalarApiTableInput {
    char tableName[XcalarApiMaxTableNameLen + 1];
    XcalarApiTableId tableId;
    XdbId xdbId;
};

typedef DsDatasetMeta XcalarApiDataset;

// **************************** Export **************************** //
struct XcalarApiListExportTargetsInput {
    char targetTypePattern[XcalarExportTargetMaxNameLen + 1];
    char targetNamePattern[XcalarExportTargetMaxNameLen + 1];
};

struct XcalarApiListExportTargetsOutput {
    int numTargets;
    ExExportTarget targets[0];
};

struct XcalarApiExportInput {
    XcalarApiTableInput srcTable;
    // This is used to name the dag node used for the export operation
    char exportName[XcalarApiMaxTableNameLen + 1];
    ExExportMeta meta;
};
// ************************** End Export ************************** //

// ***************************** Apps ***************************** //

struct XcalarApiAppRunInput {
    char name[XcalarApiMaxAppNameLen + 1];
    AppGroup::Scope scope;
    size_t inStrSize;
    char inStr[0];
};

struct XcalarApiAppRunOutput {
    uint64_t appGroupId;
};

struct XcalarApiAppReapInput {
    uint64_t appGroupId;
    bool cancel;
};

struct XcalarApiAppReapOutput {
    size_t outStrSize;
    size_t errStrSize;
    char str[0];
};

// *************************** End Apps *************************** //

struct XcalarApiTargetInput {
    uint64_t inputLen;
    // Contains JSON-ified target operation input
    char inputJson[0];
};

struct XcalarApiTargetOutput {
    // Target uses raw JSON to communicate
    uint64_t outputLen;
    char outputJson[0];
};

struct XcalarApiRenameMap {
    char oldName[DfMaxFieldNameLen + 1];
    char newName[DfMaxFieldNameLen + 1];
    bool isKey;
    DfFieldType type;
};

struct XcalarApiPreviewInput {
    uint64_t inputLen;
    // url, namePattern, recursive, offset, numBytesRequested
    char inputJson[0];
};

struct XcalarApiProjectInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    unsigned numColumns;
    char columnNames[0][DfMaxFieldNameLen + 1];
};

struct XcalarApiKeyInput {
    DfFieldType type;
    char keyName[DfMaxFieldNameLen + 1];
    char keyFieldName[DfMaxFieldNameLen + 1];
    Ordering ordering;
};

struct XcalarApiIndexInput {
    char dhtName[DhtMaxDhtNameLen + 1];  // Leave name blank for default
    DagTypes::NamedInput source;
    XcalarApiTableInput dstTable;

    // only for index from dataset
    char fatptrPrefixName[DfMaxFieldNameLen + 1];
    unsigned numKeys;
    XcalarApiKeyInput keys[TupleMaxNumValuesPerRecord];

    bool delaySort;
    bool broadcast;
};

struct XcalarApiStatInput {
    int64_t nodeId;
};

typedef struct ResetStatsInput {
    bool resetHwmStats;
    bool resetCumulativeStats;
} ResetStatsInput;

struct RetinaSrcTable {
    // filled in by execute retina
    Dag *srcGraph = NULL;

    DagTypes::NamedInput source;
    char dstName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiListRetinasInput {
    char namePattern[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiMakeRetinaInput {
    char retinaName[XcalarApiMaxTableNameLen + 1];
    uint64_t numTargetTables;
    uint64_t numSrcTables;
    uint64_t numColumnHints;

    // this is a pointer into the variable length array at the bottom
    Column *columnHints;
    RetinaSrcTable *srcTables;
    RetinaDst *tableArray[0];
};

struct XcalarApiGroupByInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    bool includeSrcTableSample;
    bool icvMode;
    bool groupAll;
    char newKeyName[DfMaxFieldNameLen + 1];  // deprecated

    // used to determine key renames
    uint32_t numKeys;
    XcalarApiKeyInput keys[TupleMaxNumValuesPerRecord];

    uint32_t numEvals;
    // These are pointers into the variable length "strings" buffer
    char *evalStrs[TupleMaxNumValuesPerRecord];
    char *newFieldNames[TupleMaxNumValuesPerRecord];

    ssize_t stringsCount;
    uint8_t strings[0];  // Must be last
};

struct XcalarApiAggregateInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    char evalStr[XcalarApiMaxEvalStringLen + 1];
};

struct XcalarApiRenameNodeInput {
    char oldName[DagTypes::MaxNameLen + 1];
    char newName[DagTypes::MaxNameLen + 1];
};

struct XcalarApiMakeResultSetInput {
    bool errorDs;
    DagTypes::NamedInput dagNode;
};

struct XcalarApiResultSetNextInput {
    uint64_t resultSetId;
    uint64_t numRecords;
};

struct XcalarApiFreeResultSetInput {
    uint64_t resultSetId;
};

struct XcalarApiStatOutput {
    char statName[StatStringLen];
    Stat statValue;
    StatType statType;
    uint64_t groupId;
};

struct XcalarApiJoinInput {
    XcalarApiTableInput leftTable;
    XcalarApiTableInput rightTable;
    XcalarApiTableInput joinTable;
    JoinOperator joinType;

    char filterString[XcalarApiMaxEvalStringLen + 1];

    bool collisionCheck;
    bool keepAllColumns;
    bool nullSafe;

    // these nums are for the renameMap
    unsigned numLeftColumns;
    unsigned numRightColumns;
    XcalarApiRenameMap renameMap[0];
};

struct XcalarApiUnionInput {
    // These are pointers into the buf array
    XcalarApiTableInput *srcTables;
    unsigned numSrcTables;
    XcalarApiTableInput dstTable;

    unsigned *renameMapSizes;
    XcalarApiRenameMap **renameMap;

    bool dedup;
    UnionOperator unionType;
    int64_t bufCount;
    uint8_t buf[0];
};

struct XcalarApiResultSetAbsoluteInput {
    uint64_t resultSetId;
    uint64_t position;
};

struct XcalarApiParameter {
    char parameterName[DfMaxFieldNameLen + 1];
    char parameterValue[XcalarApiMaxEvalStringLen + 1];
};

// Need to generalize this to be able to parameterize more than just
// strings
struct XcalarApiParamBulkLoad {
    char datasetUrl[XcalarApiMaxFileNameLen + 1];
    char namePattern[XcalarApiMaxFileNameLen + 1];
};

struct XcalarApiParamSynthesize {
    char source[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiParamFilter {
    char filterStr[XcalarApiMaxEvalStringLen + 1];
};

struct XcalarApiParamExport {
    char fileName[XcalarApiMaxFileNameLen + 1];
    // This is a deprecated field, do not use
    char udfTarget[XcalarApiMaxAppNameLen + 1];
    char targetName[XcalarApiMaxAppNameLen + 1];
    ExTargetType targetType;
};

union XcalarApiParamInputArgs {
    XcalarApiParamSynthesize paramSynthesize;
    XcalarApiParamBulkLoad paramLoad;
    XcalarApiParamFilter paramFilter;
    XcalarApiParamExport paramExport;
};

struct XcalarApiParamInput {
    XcalarApis paramType;
    XcalarApiParamInputArgs paramInputArgs;
};

struct XcalarApiUpdateRetinaInput {
    char retinaName[XcalarApiMaxTableNameLen + 1];
    XcalarApiDagNodeId dagNodeId;
    XcalarApiParamInput paramInput;
    size_t retinaJsonCount;
    char retinaJson[0];
};

struct XcalarApiListParametersInRetinaOutput {
    uint64_t numParameters;
    XcalarApiParameter parameters[0];
};

struct XcalarApiExecuteRetinaInput {
    char retinaName[XcalarApiMaxTableNameLen + 1];
    char queryName[XcalarApiMaxTableNameLen + 1];
    XcalarApiUserId userId;

    // session to extract udfs from, if left blank, uses udfs from the
    // retina container
    char udfUserName[LOGIN_NAME_MAX + 1];
    char udfSessionName[XcalarApiSessionNameLen + 1];

    bool exportToActiveSession;
    char schedName[XcalarApiMaxPathLen + 1];
    XcalarApiTableInput dstTable;

    // pin the results(tables) of dataflow execution, so that they cannot
    // be dropped unless unpinned
    bool pinResults;

    ssize_t numParameters;
    uint64_t exportRetinaBufSize;
    // this is a pointer to the end of variable sized parameters array
    char *exportRetinaBuf;
    XcalarApiParameter parameters[0];
};

struct XcalarApiGetStatOutput {
    uint64_t numStats;
    bool truncated;
    XcalarApiStatOutput stats[0];
};

struct XcalarApiMapInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    bool icvMode;
    uint32_t numEvals;
    // These are pointers into the variable length "strings" buffer
    char *evalStrs[TupleMaxNumValuesPerRecord];
    char *newFieldNames[TupleMaxNumValuesPerRecord];

    ssize_t stringsCount;
    uint8_t strings[0];  // Must be last
};

struct XcalarApiGetRowNumInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    char newFieldName[DfMaxFieldNameLen + 1];
    // XXX: this is filled in by us during operatorsGetRowNum
    // should be put into ApiWrapper
    uint64_t rowCountPerNode[MaxNodes];
};

//
// UDF management API.
//

struct XcalarApiUdfError {
    // data[0] thru data[messageSize - 1] contains message (including \0).
    size_t messageSize;
    // data[messageSize] thru data[messageSize + tracebackSize - 1] contains
    // traceback (including \0).
    size_t tracebackSize;
    char data[0];
};

struct XcalarApiUdfAddUpdateOutput {
    StatusCode status;
    char moduleName[XcalarApiMaxUdfModuleNameLen + 1];
    XcalarApiUdfError error;  // Variable-size. Must be last element
};

struct XcalarApiUdfGetInput {
    char moduleName[XcalarApiMaxUdfModuleNameLen + 1];
};

struct XcalarApiUdfDeleteInput {
    char moduleName[XcalarApiMaxUdfModuleNameLen + 1];
};

struct XcalarApiFilterInput {
    char filterStr[XcalarApiMaxEvalStringLen + 1];
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
};

// name should be XcalarApiQueryStateInput
// not able to change name due to IDL
// Not adding a new struct due to clients
struct XcalarApiQueryNameInput {
    char queryName[QrMaxNameLen + 1];
    bool detailedStats;
};

struct XcalarApiImportRetinaInput {
    char retinaName[XcalarApiMaxTableNameLen + 1];
    char udfUserName[LOGIN_NAME_MAX + 1];
    char udfSessionName[XcalarApiSessionNameLen + 1];
    bool loadRetinaJson;

    bool overwriteExistingUdf;
    ssize_t retinaCount;
    uint8_t retina[0];
};

// Must be serialized/unserialized to fixup pointers
struct XcalarApiImportRetinaOutput {
    uint64_t numUdfModules;
    // Array of pointers
    XcalarApiUdfAddUpdateOutput **udfModuleStatuses;
    size_t bufSize;
    uint8_t buf[0];
};

struct XcalarStatGroupInfo {
    uint64_t groupIdNum;
    uint64_t totalSingleStats;
    StatGroupName statsGroupName;
};

// Get Stats GroupId output
// the index in groupName represents the group id; the range is guaranteed to
// be monotonically increasing and contiguous beginning from 0
struct XcalarApiGetStatGroupIdMapOutput {
    uint64_t numGroupNames;
    bool truncated;
    XcalarStatGroupInfo groupNameInfo[0];
};

// XXX: make these arrays variable size
struct XcalarApiTableMeta {
    // per node
    StatusCode status;
    uint64_t numRows;
    uint64_t numPages;
    uint64_t numSlots;
    uint64_t size;
    uint64_t numRowsPerSlot[XdbMgr::XdbHashSlotsDefault];
    uint64_t numPagesPerSlot[XdbMgr::XdbHashSlotsDefault];
    uint64_t xdbPageConsumedInBytes;
    uint64_t xdbPageAllocatedInBytes;
    int64_t numTransPageSent;
    int64_t numTransPageRecv;
};

struct XcalarApiGetTableMetaOutput {
    // common across all nodes
    // XXX: datasets and result sets should be variable size
    unsigned numDatasets;
    char datasets[TupleMaxNumValuesPerRecord][XcalarApiDsDatasetNameLen];
    unsigned numResultSets;
    uint64_t resultSetIds[TupleMaxNumValuesPerRecord];
    unsigned numKeys;
    DfFieldAttrHeader keyAttr[TupleMaxNumValuesPerRecord];
    unsigned numValues;
    unsigned numImmediates;
    DfFieldAttrHeader valueAttrs[TupleMaxNumValuesPerRecord];
    Ordering ordering;
    XdbId xdbId;

    uint64_t numMetas;
    XcalarApiTableMeta metas[0];
};

struct XcalarApiMakeResultSetOutput {
    uint64_t resultSetId;
    uint64_t numEntries;
    XcalarApiGetTableMetaOutput metaOutput;
};

struct XcalarApiDagNodeInfo {
    char name[DagTypes::MaxNameLen + 1];
    XcalarApiDagNodeId dagNodeId;
    DgDagState state;
    uint64_t size;
    XcalarApis api;
    bool pinned;
};

struct XcalarApiResultSetNextOutput {
    StatusCode status;
    uint64_t numEntries;
    char entries[0];
};

struct XcalarApiSessionGenericOutput {
    char errorMessage[XcalarApiSessionNameLen + 1];
    char ipAddr[IpAddrStrLen];
    NodeId nodeId;
    bool outputAdded;
};

struct XcalarApiListDagNodesOutput {
    uint64_t numNodes;
    XcalarApiDagNodeInfo nodeInfo[0];
};

struct XcalarApiListDatasetsOutput {
    unsigned numDatasets;
    XcalarApiDataset datasets[0];
};

struct XcalarApiGetDatasetsInfoInput {
    char datasetsNamePattern[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiColumnInfo {
    char name[XcalarApiMaxFieldNameLen + 1];
    DfFieldType type;
};

struct XcalarApiDatasetsInfo {
    char datasetName[XcalarApiMaxTableNameLen + 1];
    bool downSampled;
    uint64_t totalNumErrors;
    uint64_t datasetSize;
    uint64_t numColumns;
    XcalarApiColumnInfo columns[TupleMaxNumValuesPerRecord];
};

// This can be slower performance than listing datasets.
struct XcalarApiGetDatasetsInfoOutput {
    unsigned numDatasets;
    XcalarApiDatasetsInfo datasets[0];
};

struct XcalarApiDatasetGetMetaOutput {
    size_t datasetMetaSize;
    char datasetMeta[0];
};

struct XcalarApiDatasetUnloadStatus {
    XcalarApiDataset dataset;
    StatusCode status;
};

struct XcalarApiDatasetUnloadOutput {
    uint64_t numDatasets;
    XcalarApiDatasetUnloadStatus statuses[0];
};

struct XcalarApiNewTableOutput {
    char tableName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiGetTableRefCountOutput {
    uint64_t refCount;
};

struct XcalarApiQueryOutput {
    char queryName[QrMaxNameLen];
};

struct XcalarApiPreviewOutput {
    // Preview uses raw json to communicate
    uint64_t outputLen;
    char outputJson[0];
};

struct XcalarApiBulkLoadOutput {
    // XXX: DatasetId is internal facing. Should not be exposed
    XcalarApiDataset dataset;
    uint64_t numFiles;
    uint64_t numBytes;
    char errorString[XcalarApiMaxInputSize];
    char errorFile[XcalarApiMaxPathLen];
};

struct XcalarApiGetVersionOutput {
    char version[XcalarApiVersionBufLen];
    char apiVersionSignatureFull[XcalarApiVersionSignatureBufLen];
    XcalarApiVersion apiVersionSignatureShort;
};

struct XcalarApiAggregateOutput {
    char tableName[XcalarApiMaxTableNameLen + 1];
    char jsonAnswer[0];
};

struct XcalarApiSingleQuery {
    char singleQuery[XcalarApiMaxSingleQuerySize];
    StatusCode status;
};

enum XcalarApiTopRequestType {
    XcalarApiTopRequestTypeInvalid = 0,
    GetCpuAndNetworkTopStats = 1,
    GetAllTopStats = 2,
};

enum XcalarApiTopType {
    XcalarApiTopInvalid = 0,
    XcalarApiTopAllNode = 1,
    XcalarApiTopLocalNode = 2,
    XcalarApiTopMax,
};

struct XcalarApiTopInput {
    uint64_t measureIntervalInMs;
    uint64_t cacheValidityInMs;
    XcalarApiTopRequestType topStatsRequestType;
};

struct XcalarApiTopOutputPerNode {
    int64_t nodeId;
    double cpuUsageInPercent;
    double memUsageInPercent;
    uint64_t memUsedInBytes;
    uint64_t totalAvailableMemInBytes;
    uint64_t networkRecvInBytesPerSec;
    uint64_t networkSendInBytesPerSec;
    uint64_t xdbUsedBytes;
    uint64_t xdbTotalBytes;
    double parentCpuUsageInPercent;
    double childrenCpuUsageInPercent;
    uint64_t numCores;
    uint64_t sysSwapUsedInBytes;
    uint64_t sysSwapTotalInBytes;
    uint64_t uptimeInSeconds;
    uint64_t datasetUsedBytes;
    uint64_t sysMemUsedInBytes;
    uint64_t publishedTableUsedBytes;
};

struct XcalarApiTopOutput {
    StatusCode status;
    uint64_t numNodes;
    XcalarApiTopOutputPerNode topOutputPerNode[0];
};

struct XcalarApiGetNumNodesOutput {
    uint64_t numNodes;
};

struct XcalarApiQueryInput {
    //
    // If sameSession is true, then the session to be used must be the one
    // supplied via the workItem which accompanies XcalarApiQueryInput.  If
    // sameSession is false, a new session is created with a synthetic name
    // generated by the API.
    //
    bool sameSession;
    bool bailOnError;

    // session to extract udfs from, if left blank, uses udfs from the
    // session the query is executing in
    char udfUserName[LOGIN_NAME_MAX + 1];
    char udfSessionName[XcalarApiSessionNameLen + 1];

    char schedName[XcalarApiMaxPathLen + 1];
    bool isAsync;
    bool pinResults;
    bool collectStats;
    char queryName[QrMaxNameLen + 1];
    char queryStr[0];
};

struct XcalarApiSessionNewInput {
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
    bool fork;
    size_t forkedSessionNameLength;
    char forkedSessionName[XcalarApiSessionNameLen + 1];
};

struct XcalarApiSessionNewOutput {
    XcalarApiSessionGenericOutput sessionGenericOutput;
    uint64_t sessionId;
    char error[XcalarApiSessionNameLen + 1];
};

struct XcalarApiSessionDeleteInput {
    uint64_t sessionId;
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
    bool noCleanup;  // Must be false
};

struct XcalarApiSessionInfoInput {
    uint64_t sessionId;
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
};

struct XcalarApiSessionActivateInput {
    uint64_t sessionId;
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
};

struct XcalarApiSession {
    uint64_t sessionId;
    char name[XcalarApiSessionNameLen + 1];
    char state[16];
    char info[16];
    NodeId activeNode;
    char description[XcalarApiSessionDescrLen + 1];
};

struct XcalarApiSessionListInput {
    char pattern[XcalarApiSessionNameLen + 1];
    size_t patternLength;
};

struct XcalarApiSessionListOutput {
    XcalarApiSessionGenericOutput sessionGenericOutput;
    unsigned numSessions;
    XcalarApiSession sessions[0];
};

struct XcalarApiSessionRenameInput {
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
    size_t origSessionNameLength;
    char origSessionName[XcalarApiSessionNameLen + 1];
};

struct XcalarApiSessionDownloadInput {
    uint64_t sessionId;
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
    char pathToAdditionalFiles[XcalarApiMaxPathLen + 1];
};

struct XcalarApiSessionDownloadOutput {
    XcalarApiSessionGenericOutput sessionGenericOutput;
    ssize_t sessionContentCount;
    uint8_t sessionContent[0];
};

// Model Upload after Create
struct XcalarApiSessionUploadInput {
    size_t sessionNameLength;
    char sessionName[XcalarApiSessionNameLen + 1];
    char pathToAdditionalFiles[XcalarApiMaxPathLen + 1];
    ssize_t sessionContentCount;
    uint8_t sessionContent[0];
};

struct XcalarApiSchedParam {
    char schedName[XcalarApiMaxPathLen + 1];
    RuntimeType runtimeType;
    uint32_t cpusReservedInPct;
};

struct XcalarApiRuntimeSetParamInput {
    uint32_t schedParamsCount;
    XcalarApiSchedParam schedParams[Runtime::TotalSdkScheds];
};

// Information for the UDF store
//     Store Type            How Specified
//     ----------            -------------
//     Old Style Global        NULL container (/udf/*) - may not be needed
//     New Style Global        non-NULL container, but all names empty
//                             (/globaludf/*): only used by default module
//                             and tests
//     Workbook                userId, sessionInfo set, retinaName NULL string
//     Dataflow                userId, sessionInfo clear, retinaName set
//     Dataflow in workbook    all specified (not supported currently)
//
//     See UserDefinedFunction::getUdfName() for more details

struct XcalarApiUdfContainer {
    // either these
    XcalarApiUserId userId;
    XcalarApiSessionInfoInput sessionInfo;

    // or this
    char retinaName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiDagNodeNamePatternInput {
    SourceType srcType;
    bool deleteCompletely;
    char namePattern[0];
};

struct XcalarApiArchiveTablesInput {
    bool archive;
    bool allTables;
    unsigned numTables;

    // tableNames seperated by null terminating char
    ssize_t tableNamesBufCount;
    char tableNamesBuf[0];
};

struct XcalarApiDeleteDagNodeStatus {
    XcalarApiDagNodeInfo nodeInfo;
    StatusCode status;
    uint64_t numRefs;
    DagTypes::DagRef refs[0];
};

struct XcalarApiDeleteDagNodesOutput {
    uint64_t numNodes;
    XcalarApiDeleteDagNodeStatus statuses[0];
};

struct XcalarApiListDatasetUsersInput {
    char datasetName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiDatasetUser {
    XcalarApiUserId userId;
    uint64_t referenceCount;
};

struct XcalarApiListDatasetUsersOutput {
    uint64_t usersCount;
    XcalarApiDatasetUser user[0];
};

struct XcalarApiListUserDatasetsInput {
    char userIdName[LOGIN_NAME_MAX + 1];
};

struct XcalarApiUserDataset {
    char datasetName[XcalarApiMaxTableNameLen];
    bool isLocked;
};

struct XcalarApiListUserDatasetsOutput {
    uint64_t numDatasets;
    XcalarApiUserDataset datasets[0];
};

struct XcalarApiDatasetDeleteInput {
    char datasetName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiDatasetUnloadInput {
    char datasetNamePattern[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiDatasetGetMetaInput {
    char datasetName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiCreateDhtInput {
    char dhtName[DhtMaxDhtNameLen + 1];
    DhtArgs dhtArgs;
};

struct XcalarApiDeleteDhtInput {
    size_t dhtNameLen;
    char dhtName[DhtMaxDhtNameLen + 1];
};

struct XcalarStressDagRaceInput {
    char firstCmd[256];
    char secondCmd[256];
};

struct XcalarApiSupportGenerateInput {
    bool generateMiniBundle;
    uint64_t supportCaseId;
};

struct XcalarApiSupportGenerateOutput {
    char supportId[XcalarApiUuidStrLen + 1];
    bool supportBundleSent;
    char bundlePath[0];
};

typedef OpDetails XcalarApiOpDetails;
typedef OpErrorStats XcalarApiOpErrorStats;

struct XcalarApiNodeOpStats {
    StatusCode status;
    int64_t nodeId;
    XcalarApiOpDetails opDetails;
};

struct XcalarApiPerNodeOpStats {
    uint64_t numNodes;
    XcalarApis api;
    XcalarApiNodeOpStats nodeOpStats[0];
};

struct XcalarApiOpStatsOut {
    XcalarApis api;
    XcalarApiOpDetails opDetails;
};

struct XcalarApiExportRetinaInput {
    char retinaName[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiExportRetinaOutput {
    ssize_t retinaCount;
    uint8_t retina[0];
};

struct XcalarApiStartFuncTestInput {
    bool parallel;
    bool runAllTests;
    bool runOnAllNodes;
    unsigned numTestPatterns;
    char testNamePatterns[0][FuncTestTypes::MaxTestNameLen + 1];
};

struct XcalarApiStartFuncTestOutput {
    unsigned numTests;
    FuncTestTypes::FuncTestOutput testOutputs[0];
};

struct XcalarApiListFuncTestInput {
    char namePattern[FuncTestTypes::MaxTestNameLen + 1];
};

struct XcalarApiListFuncTestOutput {
    unsigned numTests;
    char testName[0][FuncTestTypes::MaxTestNameLen + 1];
};

struct XcalarApiSetConfigParamInput {
    char paramName[Config::MaxKeyLength + 1];
    char paramValue[Config::MaxValueLength + 1];
};

union XcalarApiInput;
struct XcalarApiGetQueryInput {
    XcalarApis api;
    size_t inputSize;
    XcalarApiInput *input;
};

struct XcalarApiGetTableMetaInput {
    DagTypes::NamedInput tableNameInput;
    bool isPrecise;
};

struct XcalarApiDagTableNameInput {
    char tableInput[0];
};

struct XcalarApiGetRetinaInput {
    char retInput[0];
};

struct XcalarApiGetRetinaJsonInput {
    char retinaName[0];
};

struct XcalarApiListParametersInRetinaInput {
    char listRetInput[0];
};

struct XcalarApiSessionListArrayInput {
    char sesListInput[0];
};

struct XcalarApiDeleteRetinaInput {
    char delRetInput[0];
};

struct XcalarApiShutdownInput {
    bool doShutdown;
};

struct XcalarApiGetIpAddrInput {
    NodeId nodeId;
};

struct XcalarApiTagDagNodesInput {
    char tag[XcalarApiMaxDagNodeTagLen];
    unsigned nodeNamesCount;
    DagTypes::NamedInput nodeNames[0];
};

struct XcalarApiCommentDagNodesInput {
    char comment[XcalarApiMaxDagNodeCommentLen];
    unsigned nodeNamesCount;
    char nodeNames[0][XcalarApiMaxTableNameLen];
};

struct XcalarApiSynthesizeInput {
    DagTypes::NamedInput source;
    XcalarApiTableInput dstTable;
    bool sameSession;  // pull the source from the same session as dst
    unsigned columnsCount;
    Scalar *aggResult;  // only valid if source is an aggregate
    char filterString[XcalarApiMaxEvalStringLen + 1];
    XcalarApiRenameMap columns[0];
};

struct XcalarApiPublishInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    time_t unixTS;
    bool dropSrc;
};

struct XcalarApiUpdateTableInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    time_t unixTS;
    bool dropSrc;
};

struct XcalarApiUpdateInput {
    unsigned numUpdates;
    XcalarApiUpdateTableInput updates[0];
};

struct XcalarApiSelectInput {
    XcalarApiTableInput srcTable;
    XcalarApiTableInput dstTable;
    int64_t minBatchId;
    int64_t maxBatchId;
    bool createIndex;
    uint64_t limitRows;

    char filterString[XcalarApiMaxEvalStringLen + 1];
    XcalarApiTableInput joinTable;

    unsigned numColumns;
    XcalarApiRenameMap columns[TupleMaxNumValuesPerRecord];

    ssize_t evalInputCount;
    uint8_t evalInput[0];
};

struct XcalarApiUnpublishInput {
    XcalarApiTableInput srcTable;
    bool inactivateOnly;
};

struct XcalarApiRestoreTableInput {
    char name[XcalarApiMaxTableNameLen + 1];
};

struct XcalarApiCoalesceInput {
    XcalarApiTableInput srcTable;
};

struct XcalarApiListTablesInput {
    char namePattern[XcalarApiMaxTableNameLen + 1];
    bool getUpdates;
    int64_t updateStartBatchId;
    bool getSelects;
};

// All members of this union must have a unique typename of the following
// format:
//      XcalarApi<name>Input
// and also a corresponding entry in LibApisEnums.h::XcalarApis
//
// Types in this union must be compound types (ie structs), not fundamental
// types (int, char, etc.) or typedefs of fundamental types.
//
// Needs PageSize alignment for Sparse copy.
union  __attribute__((aligned(PageSize))) XcalarApiInput {
    XcalarApiBulkLoadInput loadInput;
    XcalarApiIndexInput indexInput;
    XcalarApiStatInput statInput;
    XcalarApiGetTableMetaInput getTableMetaInput;
    XcalarApiResultSetNextInput resultSetNextInput;
    XcalarApiJoinInput joinInput;
    XcalarApiUnionInput unionInput;
    XcalarApiProjectInput projectInput;
    XcalarApiFilterInput filterInput;
    XcalarApiGroupByInput groupByInput;
    XcalarApiResultSetAbsoluteInput resultSetAbsoluteInput;
    XcalarApiFreeResultSetInput freeResultSetInput;
    XcalarApiTableInput getTableRefCountInput;
    XcalarApiDagNodeNamePatternInput listDagNodesInput;
    XcalarApiDagNodeNamePatternDeleteInput deleteDagNodesInput;
    XcalarApiArchiveTablesInput archiveTablesInput;
    XcalarApiMakeResultSetInput makeResultSetInput;
    XcalarApiMapInput mapInput;
    XcalarApiGetRowNumInput getRowNumInput;
    XcalarApiSynthesizeInput synthesizeInput;
    XcalarApiAggregateInput aggregateInput;
    XcalarApiQueryNameInput queryStateInput;
    XcalarApiExExportTargetInput addTargetInput;
    XcalarApiListExportTargetsInput listTargetsInput;
    XcalarApiExportInput exportInput;
    XcalarApiDagTableNameInput dagTableNameInput;
    XcalarApiListFilesInput listFilesInput;
    XcalarApiMakeRetinaInput makeRetinaInput;
    XcalarApiGetRetinaInput getRetinaInput;
    XcalarApiGetRetinaJsonInput getRetinaJsonInput;
    XcalarApiExecuteRetinaInput executeRetinaInput;
    XcalarApiUpdateRetinaInput updateRetinaInput;
    XcalarApiListParametersInRetinaInput listParametersInRetinaInput;
    XcalarApiKeyAddOrReplaceInput keyAddOrReplaceInput;
    XcalarApiKeyAppendInput keyAppendInput;
    XcalarApiKeySetIfEqualInput keySetIfEqualInput;
    XcalarApiKeyDeleteInput keyDeleteInput;
    XcalarApiTopInput topInput;
    XcalarApiSessionNewInput sessionNewInput;
    XcalarApiSessionListInput listSessionInput;
    XcalarApiSessionDeleteInput sessionDeleteInput;
    XcalarApiSessionRenameInput sessionRenameInput;
    XcalarApiSessionListArrayInput sessionListInput;
    XcalarApiShutdownInput shutdownInput;
    XcalarApiListXdfsInput listXdfsInput;
    XcalarApiRenameNodeInput renameNodeInput;
    XcalarApiCreateDhtInput createDhtInput;
    XcalarApiDeleteDhtInput deleteDhtInput;
    XcalarApiDeleteRetinaInput deleteRetinaInput;
    XcalarApiUdfAddInput udfAddUpdateInput;
    XcalarApiUdfGetInput udfGetInput;
    XcalarApiUdfDeleteInput udfDeleteInput;
    XcalarApiImportRetinaInput importRetinaInput;
    XcalarApiPreviewInput previewInput;
    XcalarApiExportRetinaInput exportRetinaInput;
    XcalarApiStartFuncTestInput startFuncTestInput;
    XcalarApiListFuncTestInput listFuncTestInput;
    XcalarApiSessionInfoInput sessionInfoInput;
    XcalarApiGetQueryInput getQueryInput;
    XcalarApiSetConfigParamInput setConfigParamInput;
    XcalarApiExExportTargetHdrInput removeTargetInput;
    XcalarApiPackedInput appSetInput;
    XcalarApiAppRunInput appRunInput;
    XcalarApiAppReapInput appReapInput;
    XcalarApiGetIpAddrInput getIpAddrInput;
    XcalarApiSupportGenerateInput supportGenerateInput;
    XcalarApiTagDagNodesInput tagDagNodesInput;
    XcalarApiCommentDagNodesInput commentDagNodesInput;
    XcalarApiListDatasetUsersInput listDatasetUsersInput;
    XcalarApiKeyListInput keyListInput;
    XcalarApiListUserDatasetsInput listUserDatasetsInput;
    XcalarApiTargetInput targetInput;
    XcalarApiGetDatasetsInfoInput getDatasetsInfoInput;
    XcalarApiSessionDownloadInput sessionDownloadInput;
    XcalarApiSessionUploadInput sessionUploadInput;
    XcalarApiPublishInput publishInput;
    XcalarApiUpdateInput updateInput;
    XcalarApiSelectInput selectInput;
    XcalarApiUnpublishInput unpublishInput;
    XcalarApiCoalesceInput coalesceInput;
    XcalarApiRestoreTableInput restoreTableInput;
    XcalarApiSessionActivateInput sessionActivateInput;
    XcalarApiRuntimeSetParamInput runtimeSetParamInput;
    XcalarApiDatasetCreateInput datasetCreateInput;
    XcalarApiDatasetDeleteInput datasetDeleteInput;
    XcalarApiDatasetUnloadInput datasetUnloadInput;
    XcalarApiDatasetGetMetaInput datasetGetMetaInput;
    XcalarApiListRetinasInput listRetinasInput;
};

struct OpFailureInfo {
    uint64_t numRowsFailedTotal;
    FailureSummary opFailureSummary[XcalarApiMaxFailureEvals];
};

struct XcalarApiDagNodeLocalData {
    uint64_t xdbBytesRequired;
    uint64_t xdbBytesConsumed;
    uint64_t numTransPageSent;
    uint64_t numTransPageRecv;
    uint64_t numWorkCompleted;
    uint64_t numWorkTotal;
    unsigned numNodes;
    uint64_t numRowsTotal;
    uint64_t numTransPagesReceivedPerNode[MaxNodes];
    uint64_t numRowsPerNode[MaxNodes];
    int8_t hashSlotSkewPerNode[MaxNodes];
    size_t sizeTotal;
    size_t sizePerNode[MaxNodes];
    OpFailureInfo opFailureInfo;
};

struct XcalarApiDagNodeHdr {
    DagTypes::NodeName name;
    char tag[XcalarApiMaxDagNodeTagLen];
    char comment[XcalarApiMaxDagNodeCommentLen];
    bool pinned;
    XcalarApiDagNodeId dagNodeId;
    XcalarApis api;
    DgDagState state;
    size_t inputSize;
};

struct XcalarApiDagNode {
    XcalarApiDagNodeHdr hdr;
    XcalarApiDagNodeLocalData localData;
    XcalarApiTime elapsed;
    unsigned long long operatorStartTime;
    unsigned long long operatorEndTime;

    uint64_t numParents;
    uint64_t numChildren;

    XcalarApiDagNodeId *parents;
    XcalarApiDagNodeId *children;

    char log[MaxTxnLogSize];
    StatusCode status;

    // serialized buf containing
    // -input
    // -parents
    // -children
    XcalarApiInput input[0];  // Must be last element
};

struct XcalarApiDagOutput {
    uint64_t numNodes;
    size_t bufSize;
    // Array of node pointers to be fixed up in dst
    XcalarApiDagNode *node[0];
};

struct XcalarApiRetina {
    RetinaDesc retinaDesc;

    // This has to be the last field
    XcalarApiDagOutput retinaDag;
};

struct XcalarApiQueryStateOutput {
    QueryState queryState;
    StatusCode queryStatus;  // Status of the query
    uint64_t numQueuedWorkItem;
    uint64_t numCompletedWorkItem;
    uint64_t numFailedWorkItem;
    XcalarApiTime elapsed;
    NodeId queryNodeId;
    // must be last
    XcalarApiDagOutput queryGraph;
};

struct XcalarApiListRetinasOutput {
    uint64_t numRetinas;
    RetinaDesc retinaDescs[0];
};

struct XcalarApiGetRetinaOutput {
    // This has to be the last field
    XcalarApiRetina retina;
};

struct XcalarApiGetRetinaJsonOutput {
    uint64_t retinaJsonLen;
    char retinaJson[0];
};

struct XcalarApiGetQueryOutput {
    StatusCode status;
    char query[XcalarApiMaxQuerySize];
};

struct XcalarApiConfigParam {
    char paramName[Config::MaxKeyLength + 1];
    char paramValue[Config::MaxValueLength + 1];
    bool visible;
    bool changeable;
    bool restartRequired;
    char defaultValue[Config::MaxValueLength + 1];
};

struct XcalarApiGetConfigParamsOutput {
    uint64_t numParams;
    XcalarApiConfigParam parameter[0];
};

struct XcalarApiGetIpAddrOutput {
    char ipAddr[(uint32_t) SocketConstants::XcalarApiIpAddrSize];
};

struct XcalarApiUpdateInfo {
    XcalarApiTableInput srcTable;
    int64_t batchId;
    time_t startTS;
    size_t size;
    size_t numRows;
    size_t numInserts;
    size_t numUpdates;
    size_t numDeletes;
};

struct XcalarApiIndexInfo {
    XcalarApiColumnInfo key;
    size_t size;
    XcalarApiTime uptime;
};

struct XcalarApiTableInfo {
    char name[XcalarApiMaxTableNameLen + 1];
    unsigned numPersistedUpdates;
    bool active;
    bool restoring;

    int64_t oldestBatchId;
    int64_t nextBatchId;
    size_t sizeTotal;
    size_t numRowsTotal;

    XcalarApiPublishInput source;
    char userIdName[LOGIN_NAME_MAX + 1];
    char sessionName[XcalarApiSessionNameLen + 1];

    unsigned numUpdates;
    XcalarApiUpdateInfo updates[128];

    unsigned numSelects;
    XcalarApiSelectInput selects[128];

    unsigned numIndices;
    XcalarApiIndexInfo indices[TupleMaxNumValuesPerRecord];

    unsigned numKeys;
    XcalarApiColumnInfo keys[TupleMaxNumValuesPerRecord];
    unsigned numValues;
    XcalarApiColumnInfo values[TupleMaxNumValuesPerRecord];
};

struct XcalarApiListTablesOutput {
    uint64_t numTables;
    XcalarApiTableInfo tables[0];
};

struct XcalarApiUpdateOutput {
    unsigned numUpdates;
    int64_t batchIds[0];
};

struct XcalarApiRuntimeGetParamOutput {
    uint8_t schedParamsCount;
    XcalarApiSchedParam schedParams[Runtime::TotalSdkScheds];
};

struct XcalarApiRestoreTableOutput {
    unsigned numTables;
    char tableDependencies[128][XcalarApiMaxTableNameLen + 1];
};

union XcalarApiOutputResult {
    XcalarApiGetVersionOutput getVersionOutput;
    XcalarApiGetStatOutput statOutput;
    XcalarApiListDagNodesOutput listNodesOutput;
    XcalarApiListDatasetsOutput listDatasetsOutput;
    XcalarApiMakeResultSetOutput makeResultSetOutput;
    XcalarApiResultSetNextOutput resultSetNextOutput;
    XcalarApiGetTableMetaOutput getTableMetaOutput;
    XcalarApiNewTableOutput indexOutput;
    XcalarApiNewTableOutput synthesizeOutput;
    XcalarApiBulkLoadOutput loadOutput;
    XcalarApiGetTableRefCountOutput getTableRefCountOutput;
    XcalarApiDeleteDagNodesOutput deleteDagNodesOutput;
    XcalarApiDeleteDagNodesOutput archiveTablesOutput;
    XcalarApiNewTableOutput joinOutput;
    XcalarApiNewTableOutput unionOutput;
    XcalarApiGetStatGroupIdMapOutput statGroupIdMapOutput;
    XcalarApiNewTableOutput mapOutput;
    XcalarApiNewTableOutput getRowNumOutput;
    XcalarApiAggregateOutput aggregateOutput;
    XcalarApiNewTableOutput projectOutput;
    XcalarApiNewTableOutput filterOutput;
    XcalarApiQueryStateOutput queryStateOutput;
    XcalarApiListExportTargetsOutput listTargetsOutput;
    XcalarApiDagOutput dagOutput;
    XcalarApiListFilesOutput listFilesOutput;
    XcalarApiNewTableOutput groupByOutput;
    XcalarApiListRetinasOutput listRetinasOutput;
    XcalarApiGetRetinaOutput getRetinaOutput;
    XcalarApiGetRetinaJsonOutput getRetinaJsonOutput;
    XcalarApiListParametersInRetinaOutput listParametersInRetinaOutput;
    XcalarApiTopOutput topOutput;
    XcalarApiGetNumNodesOutput getNumNodesOutput;
    XcalarApiListXdfsOutput listXdfsOutput;
    XcalarApiSessionNewOutput sessionNewOutput;
    XcalarApiSessionListOutput sessionListOutput;
    XcalarApiSessionGenericOutput sessionGenericOutput;
    XcalarApiGetQueryOutput getQueryOutput;
    char noOutput[0];
    XcalarApiSupportGenerateOutput supportGenerateOutput;
    XcalarApiUdfAddUpdateOutput udfAddUpdateOutput;
    UdfModuleSrc udfGetOutput;
    XcalarApiPerNodeOpStats perNodeOpStatsOutput;
    XcalarApiOpStatsOut opStatsOutput;
    XcalarApiImportRetinaOutput importRetinaOutput;
    XcalarApiPreviewOutput previewOutput;
    XcalarApiExportRetinaOutput exportRetinaOutput;
    XcalarApiStartFuncTestOutput startFuncTestOutput;
    XcalarApiListFuncTestOutput listFuncTestOutput;
    XcalarApiNewTableOutput executeRetinaOutput;
    XcalarApiGetConfigParamsOutput getConfigParamsOutput;
    XcalarApiAppRunOutput appRunOutput;
    XcalarApiAppReapOutput appReapOutput;
    XcalarApiGetIpAddrOutput getIpAddrOutput;
    XcalarApiListDatasetUsersOutput listDatasetUsersOutput;
    XcalarApiKeyListOutput keyListOutput;
    XcalarApiListUserDatasetsOutput listUserDatasetsOutput;
    XcalarApiTargetOutput targetOutput;
    XcalarApiGetDatasetsInfoOutput getDatasetsInfoOutput;
    XcalarApiSessionDownloadOutput sessionDownloadOutput;
    XcalarApiNewTableOutput selectOutput;
    XcalarApiUpdateOutput updateOutput;
    XcalarApiRuntimeGetParamOutput runtimeGetParamOutput;
    XcalarApiDatasetGetMetaOutput datasetGetMetaOutput;
    XcalarApiDatasetUnloadOutput datasetUnloadOutput;
    XcalarApiRestoreTableOutput restoreTableOutput;
};

struct XcalarApiOutputHeader {
    StatusCode status;
    char log[MaxTxnLogSize];
    XcalarApiTime elapsed;
    char padding[4];
};

struct XcalarApiOutput {
    XcalarApiOutputHeader hdr;
    XcalarApiOutputResult outputResult;
};

struct XcalarWorkItem {
    // apiVersion and workItemLen must be the first 8 bytes,
    // just in case workItem grows in future
    XcalarApiVersion apiVersionSignature;
    unsigned int workItemLength;
    XcalarApis api;
    StatusCode status;
    XcalarApiInput *input;
    size_t inputSize;
    XcalarApiOutput *output;
    size_t outputSize;
    SocketHandle *sockFd;
    XcalarApiUserId *userId;
    XcalarApiSessionInfoInput *sessionInfo;
    size_t sessionInfoSize;
    size_t userIdSize;
    // This is from legacy/frozen code (e.g. xccli).
    bool legacyClient;
    // XXX May be add a contructor to this.
};

extern Status xcalarApiDeserializeOutput(XcalarApis api,
                                         XcalarApiOutputResult *output);

extern Status xcalarApiDeserializeRetinaInput(
    XcalarApiMakeRetinaInput *retinaInput, size_t inputSize);

extern Status xcalarApiSerializeGetQueryInput(
    XcalarApiGetQueryInput *getQueryInput, size_t inputSize);

extern Status xcalarApiDeserializeGetQueryInput(
    XcalarApiGetQueryInput *getQueryInput, size_t inputSize);

extern void xcalarApiSerializeMapInput(XcalarApiMapInput *mapInput);

extern size_t xcalarApiDeserializeMapInput(XcalarApiMapInput *mapInput);

extern size_t xcalarApiDeserializeUnionInput(XcalarApiUnionInput *unionInput);

extern void xcalarApiSerializeGroupByInput(XcalarApiGroupByInput *groupByInput);

extern size_t xcalarApiDeserializeGroupByInput(
    XcalarApiGroupByInput *groupByInput);

extern char *xcalarApiDeserializeUnionInputL1(XcalarApiUnionInput *unionInput);

extern char *xcalarApiDeserializeUnionInputL2(XcalarApiUnionInput *unionInput,
                                              char *cursor);

static inline const char *
xcalarApiGetEvalStringFromInput(XcalarApis api, XcalarApiInput *input)
{
    switch (api) {
    case XcalarApiMap:
        return input->mapInput.evalStrs[0];
    case XcalarApiGroupBy:
        return input->groupByInput.evalStrs[0];
    case XcalarApiAggregate:
        return input->aggregateInput.evalStr;
    case XcalarApiFilter:
        return input->filterInput.filterStr;
    default:
        return NULL;
    }
}

static inline size_t
xcalarApiSizeOfRetinaInputHdr(uint64_t numTables)
{
    XcalarApiMakeRetinaInput *retinaHdr;
    return sizeof(*retinaHdr) + (numTables * sizeof(retinaHdr->tableArray[0]));
}

static inline size_t
xcalarApiSizeOfRetinaColumnHints(uint64_t numColumns)
{
    XcalarApiMakeRetinaInput *retinaHdr;
    return (numColumns * sizeof(*retinaHdr->columnHints));
}

static inline size_t
xcalarApiSizeOfRetinaSrcTables(uint64_t numSrc)
{
    XcalarApiMakeRetinaInput *retinaHdr;
    return (numSrc * sizeof(*retinaHdr->srcTables));
}

static inline size_t
xcalarApiSizeOfRetinaDst(int numColumns)
{
    RetinaDst *retinaDst;
    return sizeof(*retinaDst) + (numColumns * sizeof(retinaDst->columns[0]));
}

static Unused Status
xcalarApiSerializeRetinaInput(XcalarApiMakeRetinaInput *serializedBuffer,
                              size_t inputSize,
                              const char *retinaName,
                              uint64_t numTargetTables,
                              RetinaDst *tableArray[],
                              uint64_t numSrcTables,
                              RetinaSrcTable *srcTableArray,
                              uint64_t numColumnHints,
                              Column *columnHints)
{
    uint64_t ii;
    size_t hdrSize = xcalarApiSizeOfRetinaInputHdr(numTargetTables);

    size_t ret = strlcpy(serializedBuffer->retinaName,
                         retinaName,
                         sizeof(serializedBuffer->retinaName));
    if (ret >= sizeof(serializedBuffer->retinaName)) {
        return StatusNoBufs;
    }

    serializedBuffer->numTargetTables = numTargetTables;

    for (ii = 0; ii < numTargetTables; ii++) {
        // To be fixed up at destination
        serializedBuffer->tableArray[ii] = (RetinaDst *) XcalarApiMagic;
    }

    // Populate the rest of retinaDst below the retinaInput hdr
    uintptr_t bufCursor = (uintptr_t) serializedBuffer + hdrSize;
    for (ii = 0; ii < numTargetTables; ++ii) {
        size_t retinaDstSize =
            xcalarApiSizeOfRetinaDst(tableArray[ii]->numColumns);
        if (bufCursor >= (uintptr_t) serializedBuffer + inputSize) {
            return StatusOverflow;
        }

        memcpy((void *) bufCursor, tableArray[ii], retinaDstSize);
        bufCursor += retinaDstSize;
    }

    // Copy the columnHints below the tableArray
    serializedBuffer->numColumnHints = numColumnHints;
    serializedBuffer->columnHints = (Column *) XcalarApiMagic;
    memcpy((void *) bufCursor,
           columnHints,
           numColumnHints * sizeof(*columnHints));
    bufCursor += numColumnHints * sizeof(*serializedBuffer->columnHints);

    // Copy srcTables below columnHints
    serializedBuffer->numSrcTables = numSrcTables;
    serializedBuffer->srcTables = (RetinaSrcTable *) XcalarApiMagic;
    memcpy((void *) bufCursor,
           srcTableArray,
           numSrcTables * sizeof(*srcTableArray));

    return StatusOk;
}

#endif  // _LIBAPIS_COMMON_H_
