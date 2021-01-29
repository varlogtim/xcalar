// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/buffer.h>
#include <arrow/util/decimal.h>

#include <parquet/column_scanner.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "df/DataFormat.h"
#include "dataformat/DataFormatParquet.h"
#include "util/DFPUtils.h"
#include "sys/XLog.h"

ParquetFormatOps *ParquetFormatOps::instance;

static constexpr const char *moduleName = "libdf::parquet";

ParquetParser::~ParquetParser()
{
    int ii = 0;
    if (partitionKeyNames_ != NULL) {
        for (ii = 0; ii < numPartitionKeys_; ii++) {
            memFree(partitionKeyNames_[ii]);
            partitionKeyNames_[ii] = NULL;
        }
        memFree(partitionKeyNames_);
        partitionKeyNames_ = NULL;
    }

    if (partitionKeyValues_ != NULL) {
        for (ii = 0; ii < numPartitionKeys_; ii++) {
            memFree(partitionKeyValues_[ii]);
            partitionKeyValues_[ii] = NULL;
        }
        memFree(partitionKeyValues_);
        partitionKeyValues_ = NULL;
    }

    numPartitionKeys_ = 0;
}

//
// ParquetParser::Parameters
//

Status
ParquetParser::Parameters::setFromJson(ParseArgs *parseArgs,
                                       const json_t *paramJson,
                                       char *errorBuf,
                                       size_t errorBufLen)
{
    Status status = StatusUnknown;
    const json_t *treatNullAsFnfJson = NULL;
    const json_t *columnsJson = NULL;
    const json_t *columnJson = NULL;
    json_t *partitionsJson = NULL;
    const char *partitionKeyJson = NULL;
    const json_t *partitionValuesJson = NULL;
    unsigned ii;
    char *partitionKeyTmp = NULL;
    int numPartitionValuesTmp = 0;
    char **partitionValuesTmp = NULL;

    treatNullAsFnfJson = json_object_get(paramJson, "treatNullAsFnf");
    if (treatNullAsFnfJson != NULL) {
        treatNullAsFnf = json_is_true(treatNullAsFnfJson);
    }
    xSyslog(moduleName,
            XlogErr,
            "treatNullAsFnf: %s",
            treatNullAsFnf ? "True" : "False");

    columnsJson = json_object_get(paramJson, "columns");
    if (columnsJson != NULL) {
        if (!json_is_array(columnsJson)) {
            snprintf(errorBuf,
                     errorBufLen,
                     "columns must be an array of columns");
            status = StatusInval;
            goto CommonExit;
        }

        columns =
            (char **) memAlloc(sizeof(*columns) * json_array_size(columnsJson));
        BailIfNull(columns);

        json_array_foreach (columnsJson, ii, columnJson) {
            int stringSize = strlen(json_string_value(columnJson)) + 1;

            columns[ii] = (char *) memAlloc(stringSize);
            BailIfNull(columns[ii]);
            numColumns++;

            strlcpy(columns[ii], json_string_value(columnJson), stringSize);
        }
    }

    numPartitionKeys = 0;
    partitionsJson = json_object_get(paramJson, "partitionKeys");
    if (partitionsJson != NULL) {
        if (!json_is_object(partitionsJson)) {
            snprintf(errorBuf, errorBufLen, "partitionKeys must be an object");
            status = StatusInval;
            goto CommonExit;
        }

        int numKeys = json_object_size(partitionsJson);
        partitionKeys = (char **) memAlloc(sizeof(*partitionKeys) * numKeys);
        BailIfNull(partitionKeys);
        partitionValues =
            (char ***) memAlloc(sizeof(*partitionValues) * numKeys);
        BailIfNull(partitionValues);
        numPartitionValues =
            (int *) memAlloc(sizeof(*numPartitionValues) * numKeys);
        BailIfNull(numPartitionValues);

        json_object_foreach (partitionsJson,
                             partitionKeyJson,
                             partitionValuesJson) {
            int partitionKeySize = strlen(partitionKeyJson) + 1;
            int arraySize;
            unsigned jj = 0;
            const json_t *partitionValueJson;

            partitionKeyTmp = (char *) memAlloc(partitionKeySize);
            BailIfNull(partitionKeyTmp);
            strlcpy(partitionKeyTmp, partitionKeyJson, partitionKeySize);

            if (!json_is_array(partitionValuesJson)) {
                snprintf(errorBuf,
                         errorBufLen,
                         "Partition value of %s  must be an array",
                         partitionKeyTmp);
                status = StatusInval;
                goto CommonExit;
            }

            numPartitionValuesTmp = 0;
            arraySize = json_array_size(partitionValuesJson);
            partitionValuesTmp =
                (char **) memAlloc(sizeof(*partitionValuesTmp) * arraySize);
            BailIfNull(partitionValuesTmp);

            json_array_foreach (partitionValuesJson, jj, partitionValueJson) {
                int partitionValueSize =
                    strlen(json_string_value(partitionValueJson)) + 1;

                partitionValuesTmp[jj] = (char *) memAlloc(partitionValueSize);
                BailIfNull(partitionValuesTmp[jj]);
                numPartitionValuesTmp++;

                strlcpy(partitionValuesTmp[jj],
                        json_string_value(partitionValueJson),
                        partitionValueSize);
            }

            jj = numPartitionKeys;
            partitionKeys[jj] = partitionKeyTmp;
            partitionKeyTmp = NULL;
            partitionValues[jj] = partitionValuesTmp;
            partitionValuesTmp = NULL;
            numPartitionValues[jj] = numPartitionValuesTmp;
            numPartitionValuesTmp = 0;
            numPartitionKeys++;
        }
    }

    status = StatusOk;
CommonExit:
    if (partitionKeyTmp != NULL) {
        memFree(partitionKeyTmp);
        partitionKeyTmp = NULL;
    }

    if (partitionValuesTmp != NULL) {
        for (ii = 0; ii < (unsigned) numPartitionValuesTmp; ii++) {
            memFree(partitionValuesTmp[ii]);
            partitionValuesTmp[ii] = NULL;
        }
        memFree(partitionValuesTmp);
        partitionValuesTmp = NULL;
    }

    return status;
}

//
// ParquetParser
//

Status
ParquetParser::init(ParseArgs *parseArgs,
                    ParseOptimizerArgs *optimizerArgs,
                    DataPageWriter *writer,
                    google::protobuf::Arena *arena,
                    IRecordSink *pageCallback)
{
    Status status = StatusOk;
    json_t *parserJson = NULL;
    json_error_t jsonError;

    parseArgs_ = parseArgs;
    optimizerArgs_ = optimizerArgs;
    callback_ = pageCallback;
    writer_ = writer;
    arena_ = arena;

    parserJson = json_loads(parseArgs->parserArgJson, 0, &jsonError);
    if (parserJson == NULL) {
        status = callback_->err(-1,
                                "Parquet Parser argument parse error: '%s'",
                                jsonError.text);
        status = StatusInval;
        goto CommonExit;
    }

    status = params_.setFromJson(parseArgs,
                                 parserJson,
                                 errorStringBuf_,
                                 sizeof(errorStringBuf_));
    BailIfFailed(status);

CommonExit:
    if (parserJson) {
        json_decref(parserJson);
        parserJson = NULL;
    }

    return status;
}

Status
ParquetParser::writePage()
{
    Status status = StatusUnknown;

    status = callback_->writePage();
    BailIfFailed(status);

    fieldMetaCache_.clearCache();

CommonExit:
    return status;
}

Status
ParquetParser::addRecord(std::shared_ptr<parquet::Scanner> scanners[],
                         const char *colNames[],
                         parquet::Type::type colTypes[],
                         int numCols)
{
    int ii;
    bool hasRow = false;
    Status status = StatusUnknown;
    DataValue *dValues = NULL;
    bool *isNulls = NULL;
    DataPageWriter::PageStatus pageStatus;
    DfFieldValue val;
    std::string **tmpStrs = NULL;
    DFPUtils *dfp = DFPUtils::get();
    int numTmpStrs = 0;
    bool triedOnce = false;

    isNulls = (bool *) memAlloc(sizeof(*isNulls) * numCols);
    BailIfNull(isNulls);

    dValues = (DataValue *) memAlloc(sizeof(*dValues) * numCols);
    BailIfNull(dValues);

    tmpStrs = (std::string **) memAlloc(sizeof(*tmpStrs) * numCols);
    BailIfNull(tmpStrs);

    while (true) {
        DataPageWriter::Record *writeRecord;
        status = writer_->newRecord(&writeRecord);
        BailIfFailed(status);

        writeRecord->setFieldNameCache(&fieldMetaCache_);

        for (ii = 0; ii < numCols; ii++) {
            bool isNull = true;

            switch (colTypes[ii]) {
            case parquet::Type::BOOLEAN: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::BoolScanner *scanner =
                        (parquet::BoolScanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        scanner->NextValue(&val.boolVal, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            status =
                                dValues[ii].setFromFieldValue(&val, DfBoolean);
                            BailIfFailed(status);
                        }
                    }
                }
                break;
            }
            case parquet::Type::INT32: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::Int32Scanner *scanner =
                        (parquet::Int32Scanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        scanner->NextValue(&val.int32Val, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            status =
                                dValues[ii].setFromFieldValue(&val, DfInt32);
                            BailIfFailed(status);
                        }
                    }
                }
                break;
            }
            case parquet::Type::INT64: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::Int64Scanner *scanner =
                        (parquet::Int64Scanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        scanner->NextValue(&val.int64Val, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            status =
                                dValues[ii].setFromFieldValue(&val, DfInt64);
                            BailIfFailed(status);
                        }
                    }
                }
                break;
            }
            case parquet::Type::INT96: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::Int96Scanner *scanner =
                        (parquet::Int96Scanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        parquet::Int96 tmpVal;
                        scanner->NextValue(&tmpVal, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            val.int64Val = parquet::Int96GetNanoSeconds(tmpVal);
                            status =
                                dValues[ii].setFromFieldValue(&val, DfInt64);
                            BailIfFailed(status);
                        }
                    }
                }
                break;
            }
            case parquet::Type::FLOAT: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::FloatScanner *scanner =
                        (parquet::FloatScanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        scanner->NextValue(&val.float32Val, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            status =
                                dValues[ii].setFromFieldValue(&val, DfFloat32);
                            BailIfFailed(status);
                        }
                    }
                }
                break;
            }
            case parquet::Type::DOUBLE: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::DoubleScanner *scanner =
                        (parquet::DoubleScanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        scanner->NextValue(&val.float64Val, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            status =
                                dValues[ii].setFromFieldValue(&val, DfFloat64);
                            BailIfFailed(status);
                        }
                    }
                }
                break;
            }
            case parquet::Type::BYTE_ARRAY: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::ByteArrayScanner *scanner =
                        (parquet::ByteArrayScanner *) scanners[ii].get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        const parquet::ColumnDescriptor *colDesc =
                            scanner->descr();
                        parquet::ByteArray tmpVal;
                        scanner->NextValue(&tmpVal, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            if (colDesc->logical_type() ==
                                parquet::LogicalType::DECIMAL) {
                                arrow::Decimal128 decimalVal;
                                arrow::Status arrowStatus = arrow::Decimal128::
                                    FromBigEndian(tmpVal.ptr,
                                                  tmpVal.len,
                                                  &decimalVal);
                                if (!arrowStatus.ok()) {
                                    status = StatusUnSupportedDecimalType;
                                    goto CommonExit;
                                }

                                status = dfp->xlrNumericFromString(
                                    &val.numericVal,
                                    decimalVal.ToString(colDesc->type_scale())
                                        .c_str());
                                BailIfFailed(status);
                                status = dValues[ii].setFromFieldValue(&val,
                                                                       DfMoney);
                                BailIfFailed(status);
                            } else if (colDesc->logical_type() ==
                                           parquet::LogicalType::UTF8 ||
                                       colDesc->logical_type() ==
                                           parquet::LogicalType::NONE) {
                                std::string *tmpStr = new std::string(
                                    parquet::ByteArrayToString(tmpVal));
                                BailIfNull(tmpStr);
                                tmpStrs[numTmpStrs++] = tmpStr;

                                assert(strlen(tmpStr->c_str()) ==
                                       tmpStr->size());
                                dValues[ii].setString(tmpStr->c_str(),
                                                      tmpStr->size());
                            } else {
                                status = StatusUnSupportedLogicalType;
                                goto CommonExit;
                            }
                        }
                    }
                }
                break;
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                if (triedOnce) {
                    isNull = isNulls[ii];
                } else {
                    parquet::FixedLenByteArrayScanner *scanner =
                        (parquet::FixedLenByteArrayScanner *) scanners[ii]
                            .get();
                    if (scanner->HasNext()) {
                        hasRow = true;
                        const parquet::ColumnDescriptor *colDesc =
                            scanner->descr();
                        parquet::FixedLenByteArray tmpVal;
                        scanner->NextValue(&tmpVal, &isNull);
                        isNulls[ii] = isNull;
                        if (!isNull) {
                            if (colDesc->logical_type() ==
                                parquet::LogicalType::DECIMAL) {
                                arrow::Decimal128 decimalVal;
                                arrow::Status arrowStatus = arrow::Decimal128::
                                    FromBigEndian(tmpVal.ptr,
                                                  colDesc->type_length(),
                                                  &decimalVal);
                                if (!arrowStatus.ok()) {
                                    status = StatusUnSupportedDecimalType;
                                    goto CommonExit;
                                }
                                status = dfp->xlrNumericFromString(
                                    &val.numericVal,
                                    decimalVal.ToString(colDesc->type_scale())
                                        .c_str());
                                BailIfFailed(status);
                                status = dValues[ii].setFromFieldValue(&val,
                                                                       DfMoney);
                                BailIfFailed(status);
                            } else if (colDesc->logical_type() ==
                                           parquet::LogicalType::UTF8 ||
                                       colDesc->logical_type() ==
                                           parquet::LogicalType::NONE) {
                                std::string *tmpStr = new std::string(
                                    parquet::FixedLenByteArrayToString(
                                        tmpVal,
                                        sizeof(parquet::FixedLenByteArray)));
                                BailIfNull(tmpStr);
                                tmpStrs[numTmpStrs++] = tmpStr;

                                assert(strlen(tmpStr->c_str()) ==
                                       tmpStr->size());
                                dValues[ii].setString(tmpStr->c_str(),
                                                      tmpStr->size());
                            } else {
                                status = StatusUnSupportedLogicalType;
                                goto CommonExit;
                            }
                        }
                    }
                }
                break;
            }
            }

            if (isNull) {
                xSyslog(moduleName,
                        XlogDebug,
                        "%s: NULL, treatNullAsFnf: %s",
                        colNames[ii],
                        params_.treatNullAsFnf ? "True" : "False");
                if (!params_.treatNullAsFnf) {
                    TypedDataValue dValue;
                    dValue.setNull();
                    status = writeRecord->addFieldByName(colNames[ii], &dValue);
                    BailIfFailed(status);
                }
            } else {
                status = writeRecord->addFieldByIndex(ii, &dValues[ii]);
                BailIfFailed(status);
            }
        }

        for (int jj = 0; jj < numPartitionKeys_; jj++) {
            DataValue partitionVal;
            partitionVal.setString(partitionKeyValues_[jj],
                                   strlen(partitionKeyValues_[jj]));
            status = writeRecord->addFieldByIndex(ii + jj, &partitionVal);
            BailIfFailed(status);
            hasRow = true;
        }

        if (!hasRow) {
            break;
        }

        status =
            callback_->addRecord(numRecords_, writeRecord, &pageStatus, NULL);
        BailIfFailed(status);

        if (unlikely(pageStatus == DataPageWriter::PageStatus::Full)) {
            if (unlikely(triedOnce)) {
                status =
                    callback_->err(numRecords_, "max record size exceeded");
                goto CommonExit;
            }

            status = writePage();
            BailIfFailed(status);

            triedOnce = true;
        } else {
            break;
        }
    }

    status = StatusOk;
    if (hasRow) {
        ++numRecords_;
    }
CommonExit:
    if (isNulls != NULL) {
        memFree(isNulls);
        isNulls = NULL;
    }

    if (dValues != NULL) {
        memFree(dValues);
        dValues = NULL;
    }

    if (tmpStrs != NULL) {
        for (ii = 0; ii < numTmpStrs; ii++) {
            delete tmpStrs[ii];
            tmpStrs[ii] = NULL;
        }

        memFree(tmpStrs);
        tmpStrs = NULL;
    }

    return status;
}

Status
ParquetParser::getSchema(const uint8_t *dataPtr, int64_t dataSize)
{
    std::shared_ptr<arrow::io::BufferReader> bufferReader(
        new arrow::io::BufferReader(dataPtr, dataSize));
    std::unique_ptr<parquet::ParquetFileReader> pqReader =
        parquet::ParquetFileReader::Open(bufferReader);
    parquet::ParquetFileReader *pqReaderPtr = pqReader.get();
    const parquet::FileMetaData *fileMeta = pqReaderPtr->metadata().get();
    int numCols = fileMeta->num_columns();
    const parquet::SchemaDescriptor *schema = fileMeta->schema();
    int ii;

    for (ii = 0; ii < numCols; ii++) {
        const parquet::ColumnDescriptor *column = schema->Column(ii);
        printf(
            "ColPath: %s, physical_type: %s, logical_type: %s, is_primitive: "
            "%s, is_group: %s, is_optional: %s, is_repeated: %s, is_required: "
            "%s\n",
            column->path()->ToDotString().c_str(),
            parquet::TypeToString(column->physical_type()).c_str(),
            parquet::LogicalTypeToString(column->logical_type()).c_str(),
            column->schema_node()->is_primitive() ? "true" : "false",
            column->schema_node()->is_group() ? "true" : "false",
            column->schema_node()->is_optional() ? "true" : "false",
            column->schema_node()->is_repeated() ? "true" : "false",
            column->schema_node()->is_required() ? "true" : "false");
    }

    return StatusOk;
}

Status
ParquetParser::parsePartitionKeysFromFileName(const char *fileName)
{
    Status status = StatusUnknown;
    size_t fileNameLen = strlen(fileName);
    int ii, jj;
    int leftSlash = -1, rightSlash = -1, equalSign = -1;
    int numPartitionKeys = 0;
    char **partitionKeyNames = NULL;
    char **partitionKeyValues = NULL;
    char *partitionKeyName = NULL;
    char *partitionKeyValue = NULL;

    // This is a 2-pass algorithm. The first pass is to compute the size
    // of the buffers to allocate. Then we allocate it and the 2nd pass
    // is to actually perform the strcpy into the buffers.
    // Size of buffer is a function of the number of
    // partition keys.
    for (jj = 0; jj < 2; jj++) {
        numPartitionKeys = 0;

        for (ii = fileNameLen - 1; ii >= 0; ii--) {
            char c = fileName[ii];
            if (c == '/') {
                leftSlash = ii;
                if (leftSlash > 0 && rightSlash > 0 &&
                    (equalSign > (leftSlash + 1)) &&
                    equalSign < (rightSlash - 1)) {
                    if (jj == 1) {
                        // 2nd pass. We actually do the copy now.
                        partitionKeyName =
                            (char *) memAlloc(equalSign - leftSlash);
                        BailIfNull(partitionKeyName);
                        partitionKeyValue =
                            (char *) memAlloc(rightSlash - equalSign);
                        BailIfNull(partitionKeyValue);

                        strlcpy(partitionKeyName,
                                &fileName[leftSlash + 1],
                                equalSign - leftSlash);
                        strlcpy(partitionKeyValue,
                                &fileName[equalSign + 1],
                                rightSlash - equalSign);

                        partitionKeyNames[numPartitionKeys] = partitionKeyName;
                        partitionKeyValues[numPartitionKeys] =
                            partitionKeyValue;
                        partitionKeyName = partitionKeyValue = NULL;
                    }
                    numPartitionKeys++;
                }
                rightSlash = leftSlash;
            } else if (c == '=') {
                equalSign = ii;
            }
        }

        if (jj == 0) {
            // 1st pass. Now we know the number of partition keys and
            // hence size of the buffer to allocate.
            if (numPartitionKeys == 0) {
                break;
            }

            partitionKeyNames = (char **) memAlloc(sizeof(*partitionKeyNames) *
                                                   numPartitionKeys);
            BailIfNull(partitionKeyNames);
            partitionKeyValues = (char **) memAlloc(
                sizeof(*partitionKeyValues) * numPartitionKeys);
            BailIfNull(partitionKeyValues);
        }
    }

    numPartitionKeys_ = numPartitionKeys;
    partitionKeyNames_ = partitionKeyNames;
    partitionKeyNames = NULL;
    partitionKeyValues_ = partitionKeyValues;
    partitionKeyValues = NULL;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (partitionKeyName != NULL) {
            memFree(partitionKeyName);
            partitionKeyName = NULL;
        }

        if (partitionKeyValue != NULL) {
            memFree(partitionKeyValue);
            partitionKeyValue = NULL;
        }

        if (partitionKeyNames != NULL) {
            for (ii = 0; ii < numPartitionKeys; ii++) {
                memFree(partitionKeyNames[ii]);
                partitionKeyNames[ii] = NULL;
            }
            memFree(partitionKeyNames);
            partitionKeyNames = NULL;
        }

        if (partitionKeyValues != NULL) {
            for (ii = 0; ii < numPartitionKeys; ii++) {
                memFree(partitionKeyValues[ii]);
                partitionKeyValues[ii] = NULL;
            }
            memFree(partitionKeyValues);
            partitionKeyValues = NULL;
        }
    }

    return status;
}

// Verifies that params_.partitionKeys provided matches partition keys
// found in the path to the file
Status
ParquetParser::verifyParamsPartKeys()
{
    Status status = StatusUnknown;

    for (int ii = 0; ii < params_.numPartitionKeys; ii++) {
        bool found = false;
        for (int jj = 0; jj < numPartitionKeys_; jj++) {
            if (strcmp(params_.partitionKeys[ii], partitionKeyNames_[jj]) ==
                0) {
                found = true;
                break;
            }
        }

        if (!found) {
            Status tmpStatus;
            tmpStatus = callback_->err(-1,
                                       "Partition key %s not part of file path",
                                       params_.partitionKeys[ii]);
            status = StatusInval;
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}

// Verifies that params_.columns provided matches columns in the parquet file
Status
ParquetParser::verifyParamsCols(const parquet::SchemaDescriptor *schema,
                                int numCols)
{
    Status status = StatusUnknown;

    if (params_.columns != NULL) {
        for (int ii = 0; ii < params_.numColumns; ii++) {
            bool found = false;
            for (int jj = 0; jj < numCols; jj++) {
                const char *colName;
                const parquet::ColumnDescriptor *column;
                column = schema->Column(jj);
                colName = column->name().c_str();
                if (strcmp(colName, params_.columns[ii]) == 0) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                for (int jj = 0; jj < numPartitionKeys_; jj++) {
                    if (strcmp(partitionKeyNames_[jj], params_.columns[ii]) ==
                        0) {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                Status tmpStatus;
                tmpStatus =
                    callback_->err(-1,
                                   "Column name %s does not exist in parquet "
                                   "nor as part of partition keys",
                                   params_.columns[ii]);
                status = StatusInval;
                goto CommonExit;
            }
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
ParquetParser::parseParquet(parquet::ParquetFileReader *pqReader)
{
    const parquet::FileMetaData *fileMeta = pqReader->metadata().get();
    int numColsMax = fileMeta->num_columns();
    int numCols = 0;
    int numRowGroups = fileMeta->num_row_groups();
    const parquet::SchemaDescriptor *schema = fileMeta->schema();
    int ii, jj;
    std::string **tmpStrs = NULL;
    int numTmpStrs = 0;
    Status status = StatusUnknown;
    const char **colNames = NULL;
    parquet::Type::type *colTypes = NULL;
    int *colIdx = NULL;

    // Some sanity checks
    status = verifyParamsCols(schema, fileMeta->num_columns());
    BailIfFailed(status);

    colNames = (const char **) memAlloc(sizeof(*colNames) * numColsMax);
    BailIfNull(colNames);

    colTypes = (parquet::Type::type *) memAlloc(sizeof(*colTypes) * numColsMax);
    BailIfNull(colTypes);

    colIdx = (int *) memAlloc(sizeof(*colIdx) * numColsMax);
    BailIfNull(colIdx);

    tmpStrs = (std::string **) memAlloc(sizeof(*tmpStrs) * numColsMax);
    BailIfNull(tmpStrs);

    numRecords_ = 0;
    status = fieldMetaCache_.init(numColsMax + numPartitionKeys_, writer_);
    BailIfFailed(status);

    for (ii = 0; ii < numColsMax; ii++) {
        std::string *tmpStr;
        const char *colName;
        const parquet::ColumnDescriptor *column = schema->Column(ii);
        colName = column->name().c_str();
        if (params_.columns != NULL) {
            bool found = false;
            for (jj = 0; jj < params_.numColumns; jj++) {
                if (strcmp(colName, params_.columns[jj]) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                // Skip this column
                continue;
            }
        }

        colNames[numCols] = colName;
        colTypes[numCols] = column->physical_type();

        tmpStr = new std::string(column->path()->ToDotString());
        BailIfNull(tmpStr);
        tmpStrs[numTmpStrs++] = tmpStr;

        // XXX Implement support for complex types in future
        // Given a column of complex type, say "votes": { "funny": 5 },
        // "funny" is referred to as "funny" by colName, and as "votes.funny"
        // as the fully qualified path name tmpStr.
        // For arrays, like "friends": [a, b, c], colName will be "friends",
        // while, tmpStr will be friends.array.0
        // colName will only match tmpStr if the column is not of complex type
        if (strcmp(colName, tmpStr->c_str()) != 0) {
            status = StatusComplexTypeNotSupported;
            goto CommonExit;
        }

        fieldMetaCache_.setField(numCols,
                                 tmpStr->c_str(),
                                 parquetTypeToValueType(column));

        colIdx[numCols] = ii;
        numCols++;
    }

    for (ii = 0; ii < numPartitionKeys_; ii++) {
        fieldMetaCache_.setField(numCols + ii,
                                 partitionKeyNames_[ii],
                                 ValueType::String);
    }

    for (ii = 0; ii < numRowGroups; ii++) {
        std::shared_ptr<parquet::RowGroupReader> rowGroup =
            pqReader->RowGroup(ii);
        std::unique_ptr<parquet::RowGroupMetaData> rowGroupMeta =
            fileMeta->RowGroup(ii);

        std::shared_ptr<parquet::Scanner> scanners[numCols];
        for (jj = 0; jj < numCols; jj++) {
            std::shared_ptr<parquet::ColumnReader> colReader =
                rowGroup->Column(colIdx[jj]);
            scanners[jj] = parquet::Scanner::Make(colReader);
        }

        int64_t numRows = rowGroupMeta->num_rows();
        for (int64_t jj = 0; jj < numRows; jj++) {
            status = addRecord(scanners, colNames, colTypes, numCols);
            BailIfFailed(status);
        }
    }

    status = StatusOk;
CommonExit:
    if (colNames != NULL) {
        memFree(colNames);
        colNames = NULL;
    }

    if (colTypes != NULL) {
        memFree(colTypes);
        colTypes = NULL;
    }

    if (colIdx != NULL) {
        memFree(colIdx);
        colIdx = NULL;
    }

    if (tmpStrs != NULL) {
        for (ii = 0; ii < numTmpStrs; ii++) {
            delete tmpStrs[ii];
            tmpStrs[ii] = NULL;
        }
        memFree(tmpStrs);
        tmpStrs = NULL;
    }

    return status;
}

Status
ParquetParser::parseData(const uint8_t *dataPtr, int64_t dataSize)
{
    Status status = StatusUnknown;

    std::shared_ptr<arrow::io::BufferReader> bufferReader(
        new arrow::io::BufferReader(dataPtr, dataSize));
    std::unique_ptr<parquet::ParquetFileReader> pqReader;

    try {
        std::unique_ptr<parquet::ParquetFileReader> pqReader =
            parquet::ParquetFileReader::Open(bufferReader);

        parquet::ParquetFileReader *pqReaderPtr = pqReader.get();
        status = parseParquet(pqReaderPtr);
    } catch (std::exception &e) {
        Status tmpStatus;
        tmpStatus = callback_->err(-1, "Parquet Parse Error: %s", e.what());
        status = StatusParquetParserError;
    }

    return status;
}

ValueType
ParquetParser::parquetTypeToValueType(const parquet::ColumnDescriptor *colDesc)
{
    switch (colDesc->physical_type()) {
    case parquet::Type::BOOLEAN:
        return ValueType::Boolean;
    case parquet::Type::INT32:
        return ValueType::Int32;
    case parquet::Type::INT64:
        return ValueType::Int64;
    case parquet::Type::INT96:
        return ValueType::Int64;
    case parquet::Type::FLOAT:
        return ValueType::Float32;
    case parquet::Type::DOUBLE:
        return ValueType::Float64;
    case parquet::Type::BYTE_ARRAY:
        if (colDesc->logical_type() == parquet::LogicalType::DECIMAL) {
            return ValueType::Money;
        } else {
            return ValueType::String;
        }
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        if (colDesc->logical_type() == parquet::LogicalType::DECIMAL) {
            return ValueType::Money;
        } else {
            return ValueType::String;
        }
    }
    return ValueType::Invalid;
}

Status
ParquetParser::parseData(const char *fileName, IFileReader *fileReader)
{
    Status status = StatusUnknown;
    const uint8_t *dataPtr = NULL;
    int64_t dataSize;
    int ii, jj, kk;

    status = parsePartitionKeysFromFileName(fileName);
    BailIfFailed(status);

    status = verifyParamsPartKeys();
    BailIfFailed(status);

    for (ii = 0; ii < numPartitionKeys_; ii++) {
        for (jj = 0; jj < params_.numPartitionKeys; jj++) {
            if (params_.numPartitionValues[jj] == 0) {
                continue;
            }

            if (strcmp(partitionKeyNames_[ii], params_.partitionKeys[jj]) ==
                0) {
                bool found = false;
                for (kk = 0; kk < params_.numPartitionValues[jj]; kk++) {
                    if (strcmp(partitionKeyValues_[ii],
                               params_.partitionValues[jj][kk]) == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    status = StatusOk;
                    goto CommonExit;
                }
                break;
            }
        }
    }

    // Let's grab the entire file and create a BufferedReader from it
    // Please note that dataPtr comes from PyBytes_AsStringAndSize, and
    // according to https://docs.python.org/3/c-api/bytes.html, we should
    // not de-allocate it
    status = fileReader->readChunk(0, &dataPtr, &dataSize);
    BailIfFailed(status);

    status = parseData(dataPtr, dataSize);
    BailIfFailed(status);

CommonExit:
    return status;
}

//
// ParquetFormatOps
//

Status
ParquetFormatOps::init()
{
    void *ptr = NULL;
    Status status = StatusUnknown;

    ptr = memAllocExt(sizeof(ParquetFormatOps), __PRETTY_FUNCTION__);
    BailIfNull(ptr);

    ParquetFormatOps::instance = new (ptr) ParquetFormatOps();
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (ptr != NULL) {
            memFree(ptr);
            ptr = NULL;
        }
    }

    return status;
}

ParquetFormatOps *
ParquetFormatOps::get()
{
    assert(instance);
    return instance;
}

void
ParquetFormatOps::destroy()
{
    ParquetFormatOps *inst = ParquetFormatOps::get();

    inst->~ParquetFormatOps();
    memFree(inst);
    ParquetFormatOps::instance = NULL;
}

IRecordParser *
ParquetFormatOps::getParser()
{
    return new (std::nothrow) ParquetParser();
}
