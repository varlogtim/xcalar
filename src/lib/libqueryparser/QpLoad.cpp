// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>
#include <stdio.h>
#include <assert.h>
#include <getopt.h>
#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "GetOpt.h"
#include "queryparser/QueryParser.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "udf/UserDefinedFunction.h"
#include "dataformat/DataFormatCsv.h"

static const char *moduleName = "QpLoad";

// This only implements enough URL parsing to help ease the transition into
// paths from URLs
Status
QpLoad::LoadUrl::parseFromString(const char *urlString)
{
    Status status = StatusOk;

    const char *protocolMatch = NULL;
    const char *domainNameMatch = NULL;
    const char *fullPathMatch = NULL;

    typedef enum { Protocol, DomainName, FullPath } MatchState;
    MatchState currState = Protocol;
    int ii = 0;
    size_t len = 0;
    int urlLen = 0;

    urlLen = strlen(urlString);
    protocolMatch = &urlString[ii];
    for (ii = 0; ii < urlLen; ii++) {
        switch (currState) {
        case Protocol:
            if (urlLen < (ii + 3)) {
                status = StatusCliParseError;
                goto CommonExit;
            }
            if (urlString[ii] == ':' && urlString[ii + 1] == '/' &&
                urlString[ii + 2] == '/') {
                len = (uintptr_t)(&urlString[ii]) - (uintptr_t) protocolMatch;
                strlcpy(protocol,
                        protocolMatch,
                        xcMin(len + 1, sizeof(protocol)));
                currState = DomainName;
                ii += 3;
                fullPathMatch = domainNameMatch = &urlString[ii];
                if (urlString[ii] == '/') {
                    domainName[0] = '\0';
                    currState = FullPath;
                }
            }
            break;
        case DomainName:
            if (urlString[ii] == '/') {
                len = (uintptr_t)(&urlString[ii]) - (uintptr_t) domainNameMatch;
                strlcpy(domainName,
                        domainNameMatch,
                        xcMin(len + 1, sizeof(domainName)));
                currState = FullPath;
            }
            break;
        default:
            break;
        }

        if (currState == FullPath) {
            break;
        }
    }

    switch (currState) {
    case Protocol:
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Error parsing %s: protocol not specified",
                      urlString);
        status = StatusCliParseError;
        goto CommonExit;
        break;
    case DomainName:
        len = (uintptr_t)(&urlString[urlLen]) - (uintptr_t) domainNameMatch;
        strlcpy(domainName,
                domainNameMatch,
                xcMin(len + 1, sizeof(domainName)));
        break;
    default:
        break;
    }

    len = (uintptr_t)(&urlString[urlLen]) - (uintptr_t) fullPathMatch;
    strlcpy(fullPath, fullPathMatch, xcMin(len + 1, sizeof(fullPath)));

CommonExit:
    return status;
}

void
QpLoad::LoadUrl::populateSourceArgs(DataSourceArgs *sourceArgs)
{
    // Extract a target name & path from the url
    if (strcmp(this->protocol, "nfs") == 0 ||
        strcmp(this->protocol, "shared") == 0 ||
        strcmp(this->protocol, "file") == 0) {
        strlcpy(sourceArgs->targetName,
                "Default Shared Root",
                sizeof(sourceArgs->targetName));
        strlcpy(sourceArgs->path, this->fullPath, sizeof(sourceArgs->path));
    } else if (strcmp(this->protocol, "s3") == 0) {
        strlcpy(sourceArgs->targetName,
                "Preconfigured S3 Account",
                sizeof(sourceArgs->targetName));
        strlcpy(sourceArgs->path, this->domainName, sizeof(sourceArgs->path));
        strlcat(sourceArgs->path, this->fullPath, sizeof(sourceArgs->path));
    } else if (strcmp(this->protocol, "gs") == 0) {
        strlcpy(sourceArgs->targetName,
                "Preconfigured Google Cloud Storage Account",
                sizeof(sourceArgs->targetName));
        strlcpy(sourceArgs->path, this->domainName, sizeof(sourceArgs->path));
        strlcat(sourceArgs->path, this->fullPath, sizeof(sourceArgs->path));
    } else if (strcmp(this->protocol, "memory") == 0) {
        strlcpy(sourceArgs->targetName,
                "QA memory",
                sizeof(sourceArgs->targetName));
        strlcpy(sourceArgs->path, this->domainName, sizeof(sourceArgs->path));
    } else if (strcmp(this->protocol, "azblob") == 0) {
        strlcpy(sourceArgs->targetName,
                "Preconfigured Azure Storage Account",
                sizeof(sourceArgs->targetName));
        strlcpy(sourceArgs->path, this->domainName, sizeof(sourceArgs->path));
        strlcat(sourceArgs->path, this->fullPath, sizeof(sourceArgs->path));
    }
}

QpLoad::QpLoad()
{
    this->isValidCmdParser = true;
}

QpLoad::~QpLoad()
{
    this->isValidCmdParser = false;
}

Status
QpLoad::parseArgs(int argc, char *argv[], LoadInput *loadInput)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    bool calculatedUnits = false;
    LoadUrl loadUrl;
    OptionThr longOptions[] = {
        {"url", required_argument, 0, 'u'},
        {"name", required_argument, 0, 'n'},
        {"size", required_argument, 0, 's'},
        {"format", required_argument, 0, 'f'},
        {"recorddelim", required_argument, 0, 'r'},
        {"fielddelim", required_argument, 0, 'e'},
        {"quotedelim", required_argument, 0, 'q'},
        {"recsToSkip", required_argument, 0, 'k'},
        {"apply", required_argument, 0, 'a'},
        {"crlf", no_argument, 0, 'l'},
        {"header", optional_argument, 0, 'h'},
        {"recursive", no_argument, 0, 'R'},
        {"fileNamePattern", required_argument, 0, 'p'},
        {0, 0, 0, 0},
    };
    char *endPtr;

    char url[XcalarApiMaxPathLen];
    DfFormatType format;
    char *defaultUnits = (char *) "MB";
    char sudfName[XcalarApiMaxPathLen + 1];

    CsvParser::Parameters *csvParams = NULL;
    json_t *csvJson = NULL;

    size_t bytesCopied = 0;
    assert(loadInput != NULL);

    csvParams = new (std::nothrow) CsvParser::Parameters();
    if (csvParams == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memZero(loadInput, sizeof(*loadInput));
    memZero(url, sizeof(url));
    memZero(sudfName, sizeof(sudfName));
    loadInput->args.sourceArgsListCount = 1;
    loadInput->args.sourceArgsList[0].recursive = false;
    loadInput->args.sourceArgsList[0].fileNamePattern[0] = '\0';

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "u:n:s:f:r:e:q:k:a:lh::Rp:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'u':
            strlcpy(url, optData.optarg, sizeof(url));
            break;
        case 'n':
            loadInput->datasetName = optData.optarg;
            break;
        case 'k':
            csvParams->linesToSkip =
                strtoull(optData.optarg, &endPtr, BaseCanonicalForm);
            if (endPtr == optData.optarg) {
                fprintf(stderr, "Error: [recsToSkip] is not an integer!\n");
                status = StatusCliParseError;
                goto CommonExit;
            }
            break;
        case 's':
            loadInput->args.maxSize =
                strtoull(optData.optarg, &endPtr, BaseCanonicalForm);
            if (endPtr == optData.optarg) {
                fprintf(stderr, "Error: [sizeToLoad] is not an integer!\n");
                status = StatusCliParseError;
                goto CommonExit;
            }

            assert(endPtr != NULL);
            if (strlen(endPtr) == 0) {
                printf("No units specified. Using default of %s\n",
                       defaultUnits);
                printf("To specify a unit, type %ld[Units] instead\n",
                       loadInput->args.maxSize);
                printf("[Units] can be \"B\", \"KB\", \"MB\" or \"GB\"\n");
                endPtr = defaultUnits;
            }
            assert(strlen(endPtr) > 0);

            calculatedUnits = false;
            do {
                calculatedUnits = true;
                if (strcasecmp(endPtr, "KB") == 0) {
                    loadInput->args.maxSize *= KB;
                } else if (strcasecmp(endPtr, "MB") == 0) {
                    loadInput->args.maxSize *= MB;
                } else if (strcasecmp(endPtr, "GB") == 0) {
                    loadInput->args.maxSize *= GB;
                } else if (strcasecmp(endPtr, "B") == 0 ||
                           strcasecmp(endPtr, "bytes") == 0 ||
                           strcasecmp(endPtr, "byte") == 0) {
                    loadInput->args.maxSize *= 1;
                } else {
                    printf("Unknown units. Using default of %s\n",
                           defaultUnits);
                    printf(
                        "Possible units are \"B\", \"KB\", \"MB\" or "
                        "\"GB\"");
                    endPtr = defaultUnits;
                    calculatedUnits = false;
                }
            } while (!calculatedUnits);
            break;

        case 'f':
            if (strcasecmp(optData.optarg, "json") == 0) {
                format = DfFormatJson;
            } else if (strcasecmp(optData.optarg, "csv") == 0) {
                format = DfFormatCsv;
            } else {
                fprintf(stderr,
                        "Error: Unsupported format type: %s\n",
                        optData.optarg);
                status = StatusCliParseError;
                goto CommonExit;
            }
            break;

        case 'r':
            csvParams->recordDelim[0] = optData.optarg[0];
            csvParams->recordDelim[1] = '\0';
            break;
        case 'e':
            if (strnlen(optData.optarg, DfCsvFieldDelimiterMaxLen + 1) >
                DfCsvFieldDelimiterMaxLen) {
                fprintf(stderr,
                        "Error: Field Delimiter %s longer than maximum"
                        "allowed length of %d\n",
                        optData.optarg,
                        (int) DfCsvFieldDelimiterMaxLen);
                status = StatusCliParseError;
                goto CommonExit;
            }
            bytesCopied = strlcpy(csvParams->fieldDelim,
                                  optData.optarg,
                                  sizeof(csvParams->fieldDelim));
            if (bytesCopied <
                strnlen(optData.optarg, DfCsvFieldDelimiterMaxLen + 1)) {
                fprintf(stderr, "Error: Strlcpy failed\n");
                assert(0);
                status = StatusCliParseError;
                goto CommonExit;
            }
            break;
        case 'q':
            csvParams->quoteDelim[0] = optData.optarg[0];
            csvParams->quoteDelim[1] = '\0';
            break;
        case 'a': {
            strlcpy(sudfName, optData.optarg, sizeof(sudfName));
            break;
        }
        case 'l':
            csvParams->isCRLF = true;
            break;
        case 'h':
            if (optData.optarg == NULL) {
                csvParams->schemaMode = CsvSchemaModeUseHeader;
            } else {
                csvParams->schemaMode = strToCsvSchemaMode(optData.optarg);
                if (!isValidCsvSchemaMode(csvParams->schemaMode)) {
                    fprintf(stderr,
                            "\"%s\" is not a valid schema mode",
                            optData.optarg);
                    status = StatusCliParseError;
                    goto CommonExit;
                }
            }
            break;
        case 'R':
            loadInput->args.sourceArgsList[0].recursive = true;
            break;
        case 'p':
            strlcpy(loadInput->args.sourceArgsList[0].fileNamePattern,
                    optData.optarg,
                    sizeof(loadInput->args.sourceArgsList[0].fileNamePattern));
            break;
        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (loadInput->datasetName == NULL) {
        loadInput->datasetName = "";
    }

    if (strlen(url) == 0) {
        fprintf(stderr, "--url <url>|memory required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }
    if (format == DfFormatUnknown) {
        fprintf(stderr, "--format <dataFormat> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }
    if (format == DfFormatCsv) {
        csvJson = csvParams->getJson();
        if (csvJson == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        char *csvArgs = json_dumps(csvJson, 0);
        if (csvArgs == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        strlcpy(loadInput->args.parseArgs.parserArgJson,
                csvArgs,
                sizeof(loadInput->args.parseArgs.parserArgJson));
        memFree(csvArgs);
        snprintf(loadInput->args.parseArgs.parserFnName,
                 sizeof(loadInput->args.parseArgs.parserFnName),
                 "%s%c%s",
                 UserDefinedFunction::DefaultModuleName,
                 UserDefinedFunction::ModuleDelim,
                 UserDefinedFunction::CsvParserName);
    } else if (format == DfFormatJson) {
        strlcpy(loadInput->args.parseArgs.parserArgJson,
                "{}",
                sizeof(loadInput->args.parseArgs.parserArgJson));
        snprintf(loadInput->args.parseArgs.parserFnName,
                 sizeof(loadInput->args.parseArgs.parserFnName),
                 "%s%c%s",
                 UserDefinedFunction::DefaultModuleName,
                 UserDefinedFunction::ModuleDelim,
                 UserDefinedFunction::JsonParserName);
    } else {
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (strlen(sudfName) > 0) {
        // A parser name will have already be set, since 'format' will always
        // be present; it'll probably be 'json', so let's overwrite that with
        // the actual UDF name
        strlcpy(loadInput->args.parseArgs.parserFnName,
                sudfName,
                sizeof(loadInput->args.parseArgs.parserFnName));
    }

    // Try our best to upgrade the url into a target
    status = loadUrl.parseFromString(url);
    if (status != StatusOk) {
        goto CommonExit;
    }

    loadUrl.populateSourceArgs(&loadInput->args.sourceArgsList[0]);

    status = StatusOk;
CommonExit:
    if (csvJson != NULL) {
        json_decref(csvJson);
        csvJson = NULL;
    }

    if (csvParams != NULL) {
        delete csvParams;
        csvParams = NULL;
    }

    return status;
}

Status
QpLoad::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    LoadInput *loadInput = NULL;
    Status status;
    XcalarWorkItem *workItem = NULL;

    loadInput = new (std::nothrow) LoadInput();
    BailIfNull(loadInput);
    memZero(loadInput, sizeof(*loadInput));

    status = parseArgs(argc, argv, loadInput);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    loadInput->args.parseArgs.fieldNamesCount = 0;
    workItem =
        xcalarApiMakeBulkLoadWorkItem(loadInput->datasetName, &loadInput->args);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (loadInput) {
        delete loadInput;
        loadInput = NULL;
    }
    *workItemOut = workItem;

    return status;
}

Status
QpLoad::extractSourceArgs(json_t *sourceArgsJson,
                          json_error_t *err,
                          DataSourceArgs *sourceArgs)
{
    Status status = StatusOk;
    const char *targetName = "";
    const char *path = "";
    const char *fileNamePattern = "*";
    int recursive = false;
    int ret;

    ret = json_unpack_ex(sourceArgsJson,
                         err,
                         0,
                         JsonSourceArgsFormatString,
                         TargetNameKey,
                         &targetName,
                         PathKey,
                         &path,
                         FileNamePatternKey,
                         &fileNamePattern,
                         RecursiveKey,
                         &recursive);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    sourceArgs->recursive = recursive;
    status = strStrlcpy(sourceArgs->targetName,
                        targetName,
                        sizeof(sourceArgs->targetName));
    BailIfFailedMsg(moduleName,
                    status,
                    "target name '%s' is too long, max size is %lu",
                    targetName,
                    sizeof(sourceArgs->targetName));

    status = strStrlcpy(sourceArgs->path, path, sizeof(sourceArgs->path));
    BailIfFailedMsg(moduleName,
                    status,
                    "path '%s' is too long, max size is %lu",
                    path,
                    sizeof(sourceArgs->path));

    status = strStrlcpy(sourceArgs->fileNamePattern,
                        fileNamePattern,
                        sizeof(sourceArgs->fileNamePattern));
    BailIfFailedMsg(moduleName,
                    status,
                    "fileNamePattern '%s' is too long, max size is %lu",
                    fileNamePattern,
                    sizeof(sourceArgs->fileNamePattern));

CommonExit:
    return status;
}

Status
QpLoad::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;

    const char *datasetName = "";

    // source args
    json_t *sourceArgsJson = NULL;
    json_t *sourceArgsListJson = NULL;

    // parse args
    const char *parserName = "";
    const char *parserArgs = "";
    const char *fileNameField = "";
    const char *recordNumField = "";
    int allowRecordErrors = false;
    int allowFileErrors = false;

    int64_t sampleSize = 0;

    int ret;
    XcalarWorkItem *workItem = NULL;
    DfLoadArgs *loadArgs = NULL;

    json_t *schema = NULL, *xdbArgsJson = NULL;

    loadArgs = new (std::nothrow) DfLoadArgs();
    BailIfNull(loadArgs);

    memZero(loadArgs, sizeof(*loadArgs));

    ret = json_unpack_ex(op,
                         err,
                         0,
                         JsonUnpackFormatString,
                         DatasetNameKey,
                         &datasetName,

                         XdbArgsKey,
                         &xdbArgsJson,

                         LoadArgsKey,

                         SourceArgsKey,
                         &sourceArgsJson,
                         SourceArgsListKey,
                         &sourceArgsListJson,

                         ParseArgsKey,
                         ParserFnNameKey,
                         &parserName,
                         ParserArgJsonKey,
                         &parserArgs,
                         FileNameFieldKey,
                         &fileNameField,
                         RecordNumFieldKey,
                         &recordNumField,
                         FileErrorsKey,
                         &allowFileErrors,
                         RecordErrorsKey,
                         &allowRecordErrors,
                         SchemaKey,
                         &schema,

                         SampleSizeKey,
                         &sampleSize);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    assert((sourceArgsListJson != NULL) != (sourceArgsJson != NULL) &&
           "sourceArgsList is valid XOR sourceArgs is valid");

    if (sourceArgsJson) {
        loadArgs->sourceArgsListCount = 1;
        status = extractSourceArgs(sourceArgsJson,
                                   err,
                                   &loadArgs->sourceArgsList[0]);
        BailIfFailed(status);
    } else {
        assert(sourceArgsListJson);
        json_t *sourceArgsElem;
        size_t ii;
        int sourcesType = json_typeof(sourceArgsListJson);
        if (sourcesType != JSON_ARRAY) {
            status = StatusJsonQueryParseError;
            BailIfFailedMsg(moduleName,
                            status,
                            "sourceArgsList is not a list, it is type '%d'",
                            sourcesType);
        }
        int numSources = json_array_size(sourceArgsListJson);
        if (numSources > (int) ArrayLen(loadArgs->sourceArgsList)) {
            status = StatusJsonQueryParseError;
            BailIfFailedMsg(moduleName,
                            status,
                            "sourceArgsList is length %d; max length is %d",
                            numSources,
                            (int) ArrayLen(loadArgs->sourceArgsList));
        }
        loadArgs->sourceArgsListCount = numSources;
        json_array_foreach (sourceArgsListJson, ii, sourceArgsElem) {
            assert((int) ii < numSources);
            status = extractSourceArgs(sourceArgsElem,
                                       err,
                                       &loadArgs->sourceArgsList[ii]);
            BailIfFailed(status);
        }
    }

    loadArgs->maxSize = sampleSize;
    status = strStrlcpy(loadArgs->parseArgs.parserFnName,
                        parserName,
                        sizeof(loadArgs->parseArgs.parserFnName));
    BailIfFailedMsg(moduleName,
                    status,
                    "parserFnName '%s' is too long, max size is %lu",
                    loadArgs->parseArgs.parserFnName,
                    sizeof(loadArgs->parseArgs.parserFnName));

    status = strStrlcpy(loadArgs->parseArgs.parserArgJson,
                        parserArgs,
                        sizeof(loadArgs->parseArgs.parserArgJson));
    BailIfFailedMsg(moduleName,
                    status,
                    "parserArgJson '%s' is too long, max size is %lu",
                    loadArgs->parseArgs.parserArgJson,
                    sizeof(loadArgs->parseArgs.parserArgJson));

    status = strStrlcpy(loadArgs->parseArgs.fileNameFieldName,
                        fileNameField,
                        sizeof(loadArgs->parseArgs.fileNameFieldName));
    BailIfFailedMsg(moduleName,
                    status,
                    "fileNameFieldName '%s' is too long, max size is %lu",
                    loadArgs->parseArgs.fileNameFieldName,
                    sizeof(loadArgs->parseArgs.fileNameFieldName));

    status = strStrlcpy(loadArgs->parseArgs.recordNumFieldName,
                        recordNumField,
                        sizeof(loadArgs->parseArgs.recordNumFieldName));
    BailIfFailedMsg(moduleName,
                    status,
                    "recordNumFieldName '%s' is too long, max size is %lu",
                    loadArgs->parseArgs.recordNumFieldName,
                    sizeof(loadArgs->parseArgs.recordNumFieldName));

    loadArgs->parseArgs.allowFileErrors = allowFileErrors;
    loadArgs->parseArgs.allowRecordErrors = allowRecordErrors;

    if (schema == NULL) {
        loadArgs->parseArgs.fieldNamesCount = 0;
    } else {
        loadArgs->parseArgs.fieldNamesCount = json_array_size(schema);
        if (loadArgs->parseArgs.fieldNamesCount > TupleMaxNumValuesPerRecord) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "num schema columns (%u) "
                          "exceeds max num columns (%u)",
                          loadArgs->parseArgs.fieldNamesCount,
                          TupleMaxNumValuesPerRecord);

            status = StatusInval;
            goto CommonExit;
        }

        unsigned ii;
        json_t *val;
        json_array_foreach (schema, ii, val) {
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

            loadArgs->parseArgs.types[ii] = strToDfFieldType(type);
            if (!isValidDfFieldType(loadArgs->parseArgs.types[ii])) {
                status = StatusInval;
                BailIfFailedMsg(moduleName,
                                status,
                                "%s is not a valid field type",
                                type);
            }

            status = strStrlcpy(loadArgs->parseArgs.oldNames[ii],
                                oldName,
                                sizeof(loadArgs->parseArgs.oldNames[ii]));
            BailIfFailed(status);

            if (strlen(newName) == 0) {
                newName = oldName;
            }

            strlcpy(loadArgs->parseArgs.fieldNames[ii],
                    newName,
                    sizeof(loadArgs->parseArgs.fieldNames[ii]));
            BailIfFailed(status);
        }
    }

    workItem = xcalarApiMakeBulkLoadWorkItem(datasetName, loadArgs);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // fill in xdbArgs after workItem has been created
    if (xdbArgsJson) {
        // copy over schema from parse args
        XdbLoadArgs *xdbLoadArgs =
            &workItem->input->loadInput.loadArgs.xdbLoadArgs;

        ParseArgs *parseArgs = &workItem->input->loadInput.loadArgs.parseArgs;

        xdbLoadArgs->valueDesc.numValuesPerTuple = parseArgs->fieldNamesCount;
        xdbLoadArgs->fieldNamesCount = parseArgs->fieldNamesCount;

        for (unsigned ii = 0; ii < parseArgs->fieldNamesCount; ii++) {
            xdbLoadArgs->valueDesc.valueType[ii] = parseArgs->types[ii];
            strlcpy(xdbLoadArgs->fieldNames[ii],
                    parseArgs->fieldNames[ii],
                    sizeof(xdbLoadArgs->fieldNames[ii]));
        }

        json_t *keyJson = NULL, *evalJson = NULL;
        ret = json_unpack_ex(xdbArgsJson,
                             err,
                             0,
                             JsonXdbArgsFormatString,
                             KeyFieldKey,
                             &keyJson,
                             EvalKey,
                             &evalJson);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        if (keyJson) {
            const char *name = "", *fieldName = NULL, *type = "DfUnknown",
                       *ordering = "Unordered";

            ret = json_unpack_ex(keyJson,
                                 err,
                                 0,
                                 JsonKeyUnpackFormatString,
                                 KeyNameKey,
                                 &name,
                                 KeyFieldNameKey,
                                 &fieldName,
                                 KeyTypeKey,
                                 &type,
                                 OrderingKey,
                                 &ordering);
            BailIfFailedWith(ret, StatusJsonQueryParseError);

            xdbLoadArgs->keyType = strToDfFieldType(type);
            strlcpy(xdbLoadArgs->keyName, name, sizeof(xdbLoadArgs->keyName));

            unsigned ii;
            for (ii = 0; ii < parseArgs->fieldNamesCount; ii++) {
                if (strcmp(xdbLoadArgs->fieldNames[ii], name) == 0) {
                    xdbLoadArgs->keyIndex = ii;
                    break;
                }
            }

            if (ii == parseArgs->fieldNamesCount) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Key %s not found in provided schema",
                              name);
                goto CommonExit;
            }
        } else {
            // use default key
            sprintf(xdbLoadArgs->keyName, DsDefaultDatasetKeyName);
            xdbLoadArgs->keyIndex = NewTupleMeta::DfInvalidIdx;
            xdbLoadArgs->keyType = DfInt64;
        }

        if (evalJson) {
            json_t *evalStringJson = json_object_get(evalJson, EvalStringKey);
            BailIfNullWith(evalStringJson, StatusJsonQueryParseError);

            const char *evalString = json_string_value(evalStringJson);

            strlcpy(xdbLoadArgs->evalString,
                    evalString,
                    sizeof(xdbLoadArgs->evalString));
        }
    }

CommonExit:
    *workItemOut = workItem;
    if (loadArgs) {
        delete loadArgs;
        loadArgs = NULL;
    }

    return status;
}

Status
QpLoad::reverseParse(const XcalarApiInput *input,
                     json_error_t *err,
                     json_t **argsOut)
{
    const XcalarApiBulkLoadInput *loadInput;
    Status status = StatusOk;
    json_t *args = NULL;
    json_t *sourceArgsListJson = NULL;
    json_t *sourceArgsElem = NULL;
    loadInput = &input->loadInput;
    const DfLoadArgs *loadArgs = &loadInput->loadArgs;

    assert(strlen(loadInput->datasetName) > 0);
    char datasetName[XcalarApiDsDatasetNameLen + 1];
    const char *tmp = loadInput->datasetName;
    json_t *schema = NULL, *val = NULL;
    int ret;

    schema = json_array();
    BailIfNull(schema);

    for (unsigned ii = 0; ii < loadArgs->parseArgs.fieldNamesCount; ii++) {
        val =
            json_pack_ex(err,
                         0,
                         JsonPackColumnFormatString,
                         SourceColumnKey,
                         loadArgs->parseArgs.oldNames[ii],
                         DestColumnKey,
                         loadArgs->parseArgs.fieldNames[ii],
                         ColumnTypeKey,
                         strGetFromDfFieldType(loadArgs->parseArgs.types[ii]));
        BailIfNullWith(val, StatusJsonQueryParseError);

        ret = json_array_append_new(schema, val);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        val = NULL;
    }

    if (strncmp(datasetName,
                XcalarApiDatasetPrefix,
                strlen(XcalarApiDatasetPrefix)) == 0) {
        tmp += strlen(XcalarApiDatasetPrefix);
    }

    snprintf(datasetName, sizeof(datasetName), "%s", tmp);

    sourceArgsListJson = json_array();
    BailIfNull(sourceArgsListJson);

    for (int ii = 0; ii < loadArgs->sourceArgsListCount; ii++) {
        const DataSourceArgs *sourceArg = &loadArgs->sourceArgsList[ii];
        sourceArgsElem = json_pack_ex(err,
                                      0,
                                      JsonSourceArgsFormatString,
                                      TargetNameKey,
                                      sourceArg->targetName,
                                      PathKey,
                                      sourceArg->path,
                                      FileNamePatternKey,
                                      sourceArg->fileNamePattern,
                                      RecursiveKey,
                                      (int) sourceArg->recursive);
        BailIfNull(sourceArgsElem);

        int ret = json_array_append_new(sourceArgsListJson, sourceArgsElem);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        sourceArgsElem = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        DatasetNameKey,
                        datasetName,
                        LoadArgsKey,

                        SourceArgsListKey,
                        sourceArgsListJson,

                        ParseArgsKey,
                        ParserFnNameKey,
                        loadArgs->parseArgs.parserFnName,
                        ParserArgJsonKey,
                        loadArgs->parseArgs.parserArgJson,
                        FileNameFieldKey,
                        loadArgs->parseArgs.fileNameFieldName,
                        RecordNumFieldKey,
                        loadArgs->parseArgs.recordNumFieldName,
                        FileErrorsKey,
                        loadArgs->parseArgs.allowFileErrors,
                        RecordErrorsKey,
                        loadArgs->parseArgs.allowRecordErrors,
                        SchemaKey,
                        schema,

                        SampleSizeKey,
                        loadArgs->maxSize);
    sourceArgsListJson = NULL;  // stolen by pack
    schema = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (sourceArgsListJson != NULL) {
        json_decref(sourceArgsListJson);
        sourceArgsListJson = NULL;
    }
    if (sourceArgsElem != NULL) {
        json_decref(sourceArgsElem);
        sourceArgsElem = NULL;
    }
    if (schema != NULL) {
        json_decref(schema);
        schema = NULL;
    }

    if (val != NULL) {
        json_decref(val);
        val = NULL;
    }

    *argsOut = args;

    return status;
}
