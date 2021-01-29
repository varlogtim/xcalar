// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QUERY_PARSER_H_
#define _QUERY_PARSER_H_

#include "dag/DagTypes.h"
#include "QueryParserEnums.h"
#include "JoinOpEnums.h"
#include "libapis/LibApisCommon.h"
#include "operators/XcalarEval.h"
struct XcalarWorkItem;

class QueryCmdParser
{
  public:
    QueryCmdParser() { this->isValidCmdParser = false; };
    virtual ~QueryCmdParser() { this->isValidCmdParser = false; };

    MustCheck bool isValid() { return isValidCmdParser; };

    virtual MustCheck Status parse(int argc,
                                   char *argv[],
                                   XcalarWorkItem **workItemOut) = 0;
    virtual MustCheck Status parseJson(json_t *op,
                                       json_error_t *err,
                                       XcalarWorkItem **workItemOut) = 0;

    // appends a json object operation to a json array query
    virtual MustCheck Status reverseParse(XcalarApis api,
                                          const char *comment,
                                          const char *tag,
                                          DgDagState state,
                                          const XcalarApiInput *input,
                                          void *annotations,
                                          json_error_t *err,
                                          json_t *query);

    // specialized per api, emits arguments in a json object
    virtual MustCheck Status reverseParse(const XcalarApiInput *input,
                                          json_error_t *err,
                                          json_t **argsOut) = 0;

    static Status parseColumnsArray(json_t *columns,
                                    json_error_t *err,
                                    XcalarApiRenameMap *columnsOut,
                                    unsigned numKeys = 0,
                                    const char *keyNames[] = NULL);

    static constexpr const char *JsonOpFormatString =
        "{"
        "s:s"  // operation
        "s:s"  // comment
        "s:s"  // tag
        "s:s"  // state
        "s:o"  // args
        "s:o"  // annotations
        "}";

    static constexpr const char *JsonKeyPackFormatString =
        "{"
        "s:s"  // name
        "s:s"  // keyFieldName
        "s:s"  // type
        "s:s"  // ordering
        "}";

    static constexpr const char *JsonKeyUnpackFormatString =
        "{"
        "s:s"  // name
        "s?s"  // keyFieldName
        "s?s"  // type
        "s?s"  // ordering
        "}";

    static constexpr const char *JsonPackColumnFormatString =
        "{"
        "s:s"  // sourceColumn
        "s:s"  // destColumn
        "s:s"  // columnType
        "}";

    static constexpr const char *JsonUnpackColumnFormatString =
        "{"
        "s:s"  // sourceColumn
        "s?s"  // destColumn
        "s?s"  // columnType
        "}";

    static constexpr const char *JsonEvalFormatString =
        "{"
        "s:s"  // evalString
        "s:s"  // newField
        "}";

    const char *JsonAnnotationsFormatString =
        "{"
        "s:o"  // fieldsOfInterest
        "s:s"  // newField
        "}";

    // op
    static constexpr const char *OperationKey = "operation";
    static constexpr const char *CommentKey = "comment";
    static constexpr const char *TagKey = "tag";
    static constexpr const char *ArgsKey = "args";
    static constexpr const char *StateKey = "state";
    static constexpr const char *AnnotationsKey = "annotations";

    // key
    static constexpr const char *KeyFieldKey = "key";
    static constexpr const char *KeyNameKey = "name";
    static constexpr const char *KeyFieldNameKey = "keyFieldName";
    static constexpr const char *KeyTypeKey = "type";

    // column
    static constexpr const char *SourceColumnKey = "sourceColumn";
    static constexpr const char *DestColumnKey = "destColumn";
    static constexpr const char *ColumnTypeKey = "columnType";

    // eval
    static constexpr const char *EvalKey = "eval";
    static constexpr const char *EvalStringKey = "evalString";
    static constexpr const char *NewFieldKey = "newField";

    static constexpr const char *SourceKey = "source";
    static constexpr const char *DestKey = "dest";
    static constexpr const char *FileNameKey = "fileName";

    static constexpr const char *PrefixKey = "prefix";
    static constexpr const char *OrderingKey = "ordering";
    static constexpr const char *DhtNameKey = "dhtName";

    static constexpr const char *ColumnsKey = "columns";

    static constexpr const char *NewKeyFieldKey = "newKeyField";
    static constexpr const char *IncludeSampleKey = "includeSample";
    static constexpr const char *IcvKey = "icv";
    static constexpr const char *GroupAllKey = "groupAll";

    static constexpr const char *JoinTypeKey = "joinType";

    static constexpr const char *SameSessionKey = "sameSession";

    // Load query constants
    static constexpr const char *DatasetNameKey = "dest";
    static constexpr const char *LoadArgsKey = "loadArgs";

    static constexpr const char *SourceArgsListKey = "sourceArgsList";
    static constexpr const char *SourceArgsKey = "sourceArgs";
    static constexpr const char *PathKey = "path";
    static constexpr const char *FileNamePatternKey = "fileNamePattern";
    static constexpr const char *RecursiveKey = "recursive";

    static constexpr const char *ParseArgsKey = "parseArgs";
    static constexpr const char *ParserFnNameKey = "parserFnName";
    static constexpr const char *ParserArgJsonKey = "parserArgJson";
    static constexpr const char *SampleSizeKey = "size";
    static constexpr const char *FileNameFieldKey = "fileNameFieldName";
    static constexpr const char *RecordNumFieldKey = "recordNumFieldName";
    static constexpr const char *FileErrorsKey = "allowFileErrors";
    static constexpr const char *RecordErrorsKey = "allowRecordErrors";
    static constexpr const char *SchemaKey = "schema";
    static constexpr const char *XdbArgsKey = "xdbArgs";

    // Export query constants
    static constexpr const char *TargetNameKey = "targetName";
    static constexpr const char *TargetTypeKey = "targetType";
    static constexpr const char *SplitRuleKey = "splitRule";
    static constexpr const char *SplitSizeKey = "splitSize";
    static constexpr const char *SplitNumFilesKey = "splitNumFiles";
    static constexpr const char *FormatKey = "format";
    static constexpr const char *RecordDelimKey = "recordDelim";
    static constexpr const char *FieldDelimKey = "fieldDelim";
    static constexpr const char *QuoteDelimKey = "quoteDelim";
    static constexpr const char *DriverNameKey = "driverName";
    static constexpr const char *DriverParamsKey = "driverParams";

    static constexpr const char *HeaderTypeKey = "headerType";
    static constexpr const char *CreateRuleKey = "createRule";
    static constexpr const char *SortedKey = "sorted";

    static constexpr const char *ColumnNameKey = "columnName";
    static constexpr const char *HeaderNameKey = "headerName";

    static constexpr const char *RetinaNameKey = "retinaName";
    static constexpr const char *RetinaBufKey = "retinaBuf";
    static constexpr const char *RetinaBufSizeKey = "retinaBufSize";
    static constexpr const char *QueryNameKey = "queryName";
    static constexpr const char *SchedNameKey = "schedName";
    static constexpr const char *ParametersKey = "parameters";
    static constexpr const char *ParamNameKey = "paramName";
    static constexpr const char *ParamValueKey = "paramValue";
    static constexpr const char *LatencyOptimizedKey = "latencyOptimized";

    static constexpr const char *NameKey = "namePattern";
    static constexpr const char *SourceTypeKey = "srcType";
    static constexpr const char *DeleteCompletelyKey = "deleteCompletely";

    static constexpr const char *DedupKey = "dedup";
    static constexpr const char *UnionTypeKey = "unionType";
    static constexpr const char *DelaySortKey = "delaySort";
    static constexpr const char *BroadcastKey = "broadcast";

    static constexpr const char *MinBatchIdKey = "minBatchId";
    static constexpr const char *MaxBatchIdKey = "maxBatchId";

    bool isValidCmdParser = false;
};

class QueryParser
{
  public:
    static QueryParser *get();
    static Status init();
    void destroy();

    MustCheck QueryCmdParser *getCmdParser(QueryParserEnum qp);
    MustCheck QueryCmdParser *getCmdParser(XcalarApis api);
    MustCheck Status parse(const char *query,
                           XcalarApiUdfContainer *udfContainer,
                           Dag **queryGraphOut,
                           uint64_t *numQueryGraphNodesOut);

    // return queryStr must be freed using memFreeJson
    MustCheck Status reverseParse(Dag *queryGraph,
                                  char **queryStrOut,
                                  size_t *queryStrLenOut);

    // parses queryGraph
    MustCheck Status reverseParse(Dag *queryGraph, json_t **queryOut);

  private:
    QueryParser(){};
    ~QueryParser(){};

    MustCheck Status initInternal();
    MustCheck Status parseWorkItem(int argc,
                                   char *argv[],
                                   XcalarWorkItem **workItemOut);

    static QueryParser *instance;
    QueryCmdParser *cmdParsers[XcalarApisLen];
    QueryCmdParser *parserMap[QueryParserEnumLen];
    QueryCmdParser *reverseParserMap[XcalarApisLen];

    MustCheck Status cmdParserParse(const char *query, Dag *queryGraph);

    MustCheck Status jsonParse(const char *query, Dag *queryGraph);

    // Disallow
    QueryParser(const QueryParser &) = delete;
    QueryParser &operator=(const QueryParser &) = delete;
};

class QpInvalid final : public QueryCmdParser
{
  public:
    QpInvalid(){};
    virtual ~QpInvalid(){};
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override
    {
        assert(0);
        return StatusUnimpl;
    }

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override
    {
        assert(0);
        return StatusUnimpl;
    }

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override
    {
        return StatusUnimpl;
    }
};

class QpAggregate final : public QueryCmdParser
{
  public:
    QpAggregate();
    virtual ~QpAggregate();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonFormatString =
        "{"
        "s:s"        // source
        "s:s"        // dest
        "s:[{s:s}]"  // eval, evalString
        "}";

    struct AggregateArgs {
        const char *srcTableName;
        const char *dstTableName;
        const char *evalStr;
    };

    MustCheck Status parseArgs(int argc,
                               char *argv[],
                               AggregateArgs *aggregateArgs);
};

class QpLoad final : public QueryCmdParser
{
  public:
    QpLoad();
    virtual ~QpLoad();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

    // This only implements enough URL parsing to help ease the transition into
    // paths from URLs
    struct LoadUrl {
        LoadUrl() = default;
        ~LoadUrl() = default;
        Status parseFromString(const char *urlString);
        void populateSourceArgs(DataSourceArgs *sourceArgs);

        char protocol[XcalarApiMaxPathLen + 1] = "";
        char domainName[XcalarApiMaxPathLen + 1] = "";
        char fullPath[XcalarApiMaxPathLen + 1] = "";
    };

  private:
    const char *JsonSourceArgsFormatString =
        "{"
        "s:s"  // targetName
        "s:s"  // path
        "s:s"  // fileNamePattern
        "s:b"  // recursive
        "}";
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // dest / datasetName
        "s?o"  // xdbArgs
        "s:{"  // loadArgs
        "s?o"  // sourceArgs
        "s?o"  // sourceArgsList
        "s:{"  // parseArgs
        "s:s"  // parserFnName
        "s:s"  // parserArgJson
        "s?s"  // fileNameFieldName
        "s?s"  // recordNumFieldName
        "s?b"  // allowFileErrors
        "s?b"  // allowRecordErrors
        "s?o"  // schema
        "},"
        "s:I"  // sampleSize
        "}"
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // dest / datasetName
        "s:{"  // loadArgs
        "s:o"  // sourceArgList
        "s:{"  // parseArgs
        "s:s"  // parserFnName
        "s:s"  // parserArgJson
        "s:s"  // fileNameFieldName
        "s:s"  // recordNumFieldName
        "s:b"  // allowFileErrors
        "s:b"  // allowRecordErrors
        "s:o"  // schema
        "},"
        "s:I"  // sampleSize
        "}"
        "}";

    const char *JsonXdbArgsFormatString =
        "{"
        "s?o"  // key
        "s?s"  // filterString
        "}";

    const char *FilterStringKey = "filterString";

    struct LoadInput {
        const char *datasetName;
        DfLoadArgs args;
    };

    MustCheck Status parseArgs(int argc, char *argv[], LoadInput *loadInput);

    MustCheck Status extractSourceArgs(json_t *sourceArgsJson,
                                       json_error_t *err,
                                       DataSourceArgs *sourceArgs);
};

class QpIndex final : public QueryCmdParser
{
  public:
    QpIndex();
    ~QpIndex();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // key
        "s?s"  // prefix
        "s?s"  // dhtName
        "s?b"  // delaySort
        "s?b"  // broadcast
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // key
        "s:s"  // prefix
        "s:s"  // dhtName
        "s:b"  // delaySort
        "s:b"  // broadcast
        "}";

    struct IndexArgs {
        bool sync;
        const char *keyName;
        DfFieldType keyType;
        const char *srcDatasetName;
        const char *srcTableName;
        const char *dstTableName;
        const char *dhtName;
        const char *fatptrPrefixName;
        Ordering ordering;
    };

    Status MustCheck parseArgs(int argc, char *argv[], IndexArgs *indexArgs);
};

class QpProject final : public QueryCmdParser
{
  public:
    QpProject();
    ~QpProject();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // columns
        "}";

    struct ProjectArgs {
        const char *srcTableName;
        const char *dstTableName;
        int numColumns;
        char (*columns)[DfMaxFieldNameLen + 1];
    };

    MustCheck Status parseArgs(int argc,
                               char *argv[],
                               ProjectArgs *projectArgs);
};

class QpGetRowNum final : public QueryCmdParser
{
  public:
    QpGetRowNum();
    ~QpGetRowNum();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:s"  // newField
        "}";

    struct GetRowNumArgs {
        const char *srcTableName;
        const char *dstTableName;
        const char *newFieldName;
    };

    MustCheck Status parseArgs(int argc,
                               char *argv[],
                               GetRowNumArgs *getRowNumArgs);
};

class QpFilter final : public QueryCmdParser
{
  public:
    QpFilter();
    ~QpFilter();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonFormatString =
        "{"
        "s:s"        // source
        "s:s"        // dest
        "s:[{s:s}]"  // eval, evalString
        "}";

    struct FilterArgs {
        const char *evalStr;
        const char *srcTableName;
        const char *dstTableName;
    };

    MustCheck Status parseArgs(int argc, char *argv[], FilterArgs *filterArgs);
};

class QpGroupBy final : public QueryCmdParser
{
  public:
    QpGroupBy();
    ~QpGroupBy();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // eval
        "s?s"  // newKeyField (handles older versions with only 1 newKeyField)
        "s?o"  // key
        "s?b"  // includeSample
        "s?b"  // icv
        "s?b"  // groupAll
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // eval
        "s:o"  // key
        "s:b"  // includeSample
        "s:b"  // icv
        "s:b"  // groupAll
        "}";

    struct GroupByArgs {
        const char *srcTableName;
        const char *dstTableName;
        const char *newKeyFieldName;
        bool includeSrcTableSample;
        bool icvMode;

        unsigned numEvals;
        const char *evalStrs[TupleMaxNumValuesPerRecord];
        const char *newFieldNames[TupleMaxNumValuesPerRecord];
    };

    MustCheck Status parseArgs(int argc,
                               char *argv[],
                               GroupByArgs *groupByArgs);
};

class QpJoin final : public QueryCmdParser
{
  public:
    QpJoin();
    ~QpJoin();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *KeepAllColumnsKey = "keepAllColumns";
    const char *NullSafeKey = "nullSafe";

    const char *JsonUnpackFormatString =
        "{"
        "s:[s,s]"  // source
        "s:s"      // dest
        "s:s"      // joinType
        "s?[o,o]"  // key
        "s:[o,o]"  // columns
        "s?s"      // evalString
        "s?b"      // keepAllColumns
        "s?b"      // nullSafe
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:[s,s]"  // source
        "s:s"      // dest
        "s:s"      // joinType
        "s:[o,o]"  // key
        "s:[o,o]"  // columns
        "s:s"      // evalString
        "s:b"      // keepAllColumns
        "s:b"      // nullSafe
        "}";

    struct JoinArgs {
        const char *leftTableName;
        const char *rightTableName;
        const char *joinTableName;
        bool collisionCheck;
        JoinOperator joinType;
        unsigned numLeftColumns;
        unsigned numRightColumns;
        XcalarApiRenameMap renameMap[2 * TupleMaxNumValuesPerRecord];
    };

    MustCheck Status parseArgs(int argc, char *argv[], JoinArgs *joinArgs);
};

class QpUnion final : public QueryCmdParser
{
  public:
    QpUnion();
    ~QpUnion();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override
    {
        return StatusOk;
    };

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonPackFormatString =
        "{"
        "s:o"  // source
        "s:s"  // dest
        "s:o"  // key
        "s:b"  // dedup
        "s:s"  // unionType
        "s:o"  // columns
        "}";

    const char *JsonUnpackFormatString =
        "{"
        "s:o"  // source
        "s:s"  // dest
        "s?o"  // key
        "s:b"  // dedup
        "s?s"  // unionType (not required)
        "s:o"  // columns
        "}";
};

class QpMap final : public QueryCmdParser
{
  public:
    QpMap();
    ~QpMap();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // eval
        "s?b"  // icv
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // eval
        "s:b"  // icv
        "}";

    struct MapArgs {
        bool icvMode;
        const char *srcTableName;
        const char *dstTableName;

        unsigned numEvals;
        const char *evalStrs[TupleMaxNumValuesPerRecord];
        const char *newFieldNames[TupleMaxNumValuesPerRecord];
    };

    MustCheck Status parseArgs(int argc, char *argv[], MapArgs *mapArgs);
};

class QpSynthesize final : public QueryCmdParser
{
  public:
    QpSynthesize();
    ~QpSynthesize();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override
    {
        return StatusUnimpl;
    }

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s?b"  // sameSession
        "s:o"  // columns
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:b"  // sameSession
        "s:o"  // columns
        "}";
};

class QpExecuteRetina final : public QueryCmdParser
{
  public:
    QpExecuteRetina();
    ~QpExecuteRetina();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *UdfUserNameKey = "udfUserName";
    const char *UdfSessionNameKey = "udfSessionName";

    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // retinaName
        "s:s"  // retina (base64 encoded .tar.gz)
        "s:i"  // retinaSize (size of base64 encoded .tar.gz)
        "s:s"  // queryName
        "s:s"  // dest
        "s:o"  // parameters
        "s?b"  // latencyOptimized, deprecated
        "s?s"  // schedName
        "s?s"  // udfUserName
        "s?s"  // udfSessionName
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // retinaName
        "s:s"  // retina (base64 encoded .tar.gz)
        "s:i"  // retinaSize (size of base64 encoded .tar.gz)
        "s:s"  // queryName
        "s:s"  // dest
        "s:o"  // parameters
        "s:b"  // latencyOptimized
        "s:s"  // schedName
        "}";

    struct ExecuteRetinaArgs {
        const char *retinaName;
        const char *queryName;
        bool exportToActiveSession;
        const char *schedName;
        const char *dstTableName;
        uint64_t numParameters;
        XcalarApiParameter parameters[TupleMaxNumValuesPerRecord];
    };

    MustCheck Status parseArgs(int argc,
                               char *argv[],
                               ExecuteRetinaArgs *mapArgs);
};

class QpRename final : public QueryCmdParser
{
  public:
    QpRename();
    ~QpRename();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override
    {
        assert(0);
        return StatusUnimpl;
    };

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override
    {
        return StatusOk;
    };

  private:
    struct RenameArgs {
        const char *oldName;
        const char *newName;
    };

    MustCheck Status parseArgs(int argc, char *argv[], RenameArgs *renameArgs);
};

class QpExport final : public QueryCmdParser
{
  public:
    QpExport();
    ~QpExport();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // source
        "s?s"  // fileName
        "s?s"  // targetName
        "s?s"  // targetType
        "s:s"  // dest
        "s:o"  // columns
        "s?s"  // splitRule
        "s?I"  // splitSize
        "s?I"  // splitNumFiles
        "s?s"  // headerType
        "s?s"  // createRule
        "s?b"  // sorted
        "s?s"  // format
        "s?s"  // fieldDelim
        "s?s"  // recordDelim
        "s?s"  // quoteDelim
        // export 2.0 APIs
        "s?s"  // driver name
        "s?s"  // driver params
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:o"  // columns
        "s:s"  // driver name
        "s:s"  // driver params
        "}";

    struct ExportArgs {
        const char *tableName;
        const char *exportName;
        ExExportTarget target;
        ExInitExportSpecificInput specInput;
        ExExportCreateRule createRule;
        bool sorted;
        int numColumns;
        ExColumnName *columns;
    };

    MustCheck Status parseArgs(int argc, char *argv[], ExportArgs *exportArgs);
    MustCheck Status getLegacyFileTarget(const char *targetName,
                                         ExAddTargetSFInput *sfInput);
    MustCheck Status getUdfFilePathAndName(const char *targetName,
                                           ExAddTargetUDFInput *udfInput);
};

class QpDrop final : public QueryCmdParser
{
  public:
    QpDrop();
    ~QpDrop();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // namePattern
        "s:s"  // sourceType
        "s?b"  // deleteCompletely
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // namePattern
        "s:s"  // sourceType
        "s:b"  // deleteCompletely
        "}";
};

class QpSelect final : public QueryCmdParser
{
  public:
    QpSelect();
    ~QpSelect();

    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override
    {
        return StatusUnimpl;
    }

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override;

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override;

    static constexpr const char *LimitRowsKey = "limitRows";
    static constexpr const char *CreateIndexKey = "createIndex";
    static constexpr const char *FilterKey = "Filter";
    static constexpr const char *MapsKey = "Maps";
    static constexpr const char *GroupBysKey = "GroupBys";
    static constexpr const char *GroupByKeysKey = "GroupByKeys";
    static constexpr const char *JoinKey = "Join";

    static constexpr const char *FunctionKey = "func";
    static constexpr const char *ArgKey = "arg";

    static constexpr const char *GroupByFormatString =
        "{"
        "s:s"  // func
        "s:s"  // arg
        "s:s"  // newField
        "}";

    static constexpr const char *JoinFormatString =
        "{"
        "s:s"  // source
        "s?s"  // joinType
        "s?o"  // columns
        "}";

    static constexpr const char *EvalJsonFormatString =
        "{"
        "s?s"  // Filter
        "s?o"  // Maps
        "s?o"  // GroupBys
        "s?o"  // GroupByKeys
        "s?o"  // Join
        "}";

  private:
    const char *JsonUnpackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s?I"  // minBatchId
        "s?I"  // maxBatchId
        "s?s"  // evalString
        "s?o"  // columns
        "s?o"  // eval
        "s?b"  // createIndex
        "s?i"  // limitRows
        "}";

    const char *JsonPackFormatString =
        "{"
        "s:s"  // source
        "s:s"  // dest
        "s:I"  // minBatchId
        "s:I"  // maxBatchId
        "s:o"  // columns
        "s:o"  // eval
        "s:b"  // createIndex
        "}";
};

class QpDelist final : public QueryCmdParser
{
  public:
    QpDelist();
    ~QpDelist();
    MustCheck Status parse(int argc,
                           char *argv[],
                           XcalarWorkItem **workItemOut) override;

    MustCheck Status parseJson(json_t *op,
                               json_error_t *err,
                               XcalarWorkItem **workItemOut) override
    {
        assert(0);
        return StatusUnimpl;
    };

    MustCheck Status reverseParse(const XcalarApiInput *input,
                                  json_error_t *err,
                                  json_t **argsOut) override
    {
        return StatusOk;
    };
};

#endif  // _QUERY_PARSER_H_
