// Copyright 2014 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>
#include <stdio.h>
#include <assert.h>

#include "GetOpt.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "queryparser/QueryParser.h"

QpIndex::QpIndex()
{
    this->isValidCmdParser = true;
}

QpIndex::~QpIndex()
{
    this->isValidCmdParser = false;
}

Status
QpIndex::parseArgs(int argc, char *argv[], IndexArgs *indexArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"key", required_argument, 0, 'k'},
        {"keyType", required_argument, 0, 'y'},
        {"dsttable", required_argument, 0, 't'},
        {"srctable", required_argument, 0, 's'},
        {"dataset", required_argument, 0, 'd'},
        {"dhtname", required_argument, 0, 'h'},
        {"prefix", required_argument, 0, 'p'},
        {"sorted", optional_argument, 0, 'o'},
        {0, 0, 0, 0},
    };

    assert(indexArgs != NULL);

    indexArgs->keyName = NULL;
    indexArgs->keyType = DfUnknown;
    indexArgs->srcDatasetName = NULL;
    indexArgs->srcTableName = NULL;
    indexArgs->dstTableName = NULL;
    indexArgs->dhtName = NULL;
    indexArgs->fatptrPrefixName = NULL;
    indexArgs->ordering = Unordered;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "s:d:t:k:y:p:o::",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'k':
            indexArgs->keyName = optData.optarg;
            break;

        case 's':
            indexArgs->srcTableName = optData.optarg;
            break;

        case 't':
            indexArgs->dstTableName = optData.optarg;
            break;

        case 'd':
            indexArgs->srcDatasetName = optData.optarg;
            break;

        case 'h':
            indexArgs->dhtName = optData.optarg;
            break;

        case 'p':
            indexArgs->fatptrPrefixName = optData.optarg;
            break;

        case 'o':
            if (optData.optarg != NULL) {
                if (strcmp(optData.optarg, "desc") == 0) {
                    indexArgs->ordering = Descending;
                } else {
                    indexArgs->ordering = PartialAscending;
                }
            } else {
                indexArgs->ordering = Ascending;
            }
            break;

        case 'y':
            indexArgs->keyType = (DfFieldType) atoi(optData.optarg);
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (indexArgs->keyName == NULL) {
        fprintf(stderr, "--key <keyName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }
    if (indexArgs->srcDatasetName == NULL && indexArgs->srcTableName == NULL) {
        fprintf(stderr,
                "--dataset <datasetName> | --srctable <tableName> "
                "required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }
    if (indexArgs->srcDatasetName != NULL && indexArgs->srcTableName != NULL) {
        fprintf(stderr, "Only 1 of --dataset or --srctable allowed\n");
        status = StatusCliParseError;
        goto CommonExit;
    }
    if (indexArgs->dstTableName == NULL) {
        // Not an error!
        fprintf(stderr, "tableName not specified. Creating new table\n");
    }

    status = StatusOk;

CommonExit:

    return status;
}

Status
QpIndex::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status = StatusUnknown;
    IndexArgs indexArgs;
    XcalarWorkItem *workItem = NULL;

    status = parseArgs(argc, argv, &indexArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    workItem = xcalarApiMakeIndexWorkItem(indexArgs.srcDatasetName,
                                          indexArgs.srcTableName,
                                          1,
                                          &indexArgs.keyName,
                                          NULL,
                                          &indexArgs.keyType,
                                          &indexArgs.ordering,
                                          indexArgs.dstTableName,
                                          indexArgs.dhtName,
                                          indexArgs.fatptrPrefixName,
                                          false,
                                          false);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:

    *workItemOut = workItem;
    return status;
}

Status
QpIndex::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "", *prefix = "", *dhtName = "";
    int delaySort = false;
    int broadcast = false;
    json_t *keyJson;
    XcalarWorkItem *workItem = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             KeyFieldKey,
                             &keyJson,
                             PrefixKey,
                             &prefix,
                             DhtNameKey,
                             &dhtName,
                             DelaySortKey,
                             &delaySort,
                             BroadcastKey,
                             &broadcast);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    unsigned ii;
    json_t *key;
    unsigned numKeys;
    numKeys = json_array_size(keyJson);

    if (numKeys == 0) {
        status = StatusInval;
        goto CommonExit;
    }

    {
        const char *keyNames[numKeys];
        const char *keyFieldNames[numKeys];
        memZero(keyFieldNames, sizeof(keyFieldNames));

        DfFieldType keyTypes[numKeys];
        Ordering keyOrderings[numKeys];

        json_array_foreach (keyJson, ii, key) {
            const char *type = "DfUnknown", *ordering = "Unordered";

            ret = json_unpack_ex(key,
                                 err,
                                 0,
                                 JsonKeyUnpackFormatString,
                                 KeyNameKey,
                                 &keyNames[ii],
                                 KeyFieldNameKey,
                                 &keyFieldNames[ii],
                                 KeyTypeKey,
                                 &type,
                                 OrderingKey,
                                 &ordering);
            BailIfFailedWith(ret, StatusJsonQueryParseError);

            // have fieldName default to name
            if (keyFieldNames[ii] == NULL) {
                keyFieldNames[ii] = keyNames[ii];
            }

            keyTypes[ii] = strToDfFieldType(type);
            if (!isValidDfFieldType(keyTypes[ii])) {
                status = StatusJsonQueryParseError;
                goto CommonExit;
            }

            keyOrderings[ii] = strToOrdering(ordering);
        }

        // assume table, distinction between table and dataset will happen later
        workItem = xcalarApiMakeIndexWorkItem(NULL,
                                              source,
                                              numKeys,
                                              keyNames,
                                              keyFieldNames,
                                              keyTypes,
                                              keyOrderings,
                                              dest,
                                              dhtName,
                                              prefix,
                                              delaySort,
                                              broadcast);
        if (workItem == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpIndex::reverseParse(const XcalarApiInput *input,
                      json_error_t *err,
                      json_t **argsOut)
{
    const XcalarApiIndexInput *indexInput;
    int ret;
    Status status = StatusOk;
    json_t *args = NULL, *keys = NULL, *key = NULL;

    indexInput = &input->indexInput;

    keys = json_array();
    BailIfNull(keys);

    for (unsigned ii = 0; ii < indexInput->numKeys; ii++) {
        key = json_pack_ex(err,
                           0,
                           JsonKeyPackFormatString,
                           KeyNameKey,
                           indexInput->keys[ii].keyName,
                           KeyFieldNameKey,
                           indexInput->keys[ii].keyFieldName,
                           KeyTypeKey,
                           strGetFromDfFieldType(indexInput->keys[ii].type),
                           OrderingKey,
                           strGetFromOrdering(indexInput->keys[ii].ordering));
        BailIfNullWith(key, StatusJsonQueryParseError);

        ret = json_array_append_new(keys, key);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        key = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        indexInput->source.name,
                        DestKey,
                        indexInput->dstTable.tableName,
                        KeyFieldKey,
                        keys,
                        PrefixKey,
                        indexInput->fatptrPrefixName,
                        DhtNameKey,
                        indexInput->dhtName,
                        DelaySortKey,
                        indexInput->delaySort,
                        BroadcastKey,
                        indexInput->broadcast);
    keys = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    *argsOut = args;

    if (key) {
        json_decref(key);
        key = NULL;
    }

    if (keys) {
        json_decref(keys);
        keys = NULL;
    }

    return status;
}
