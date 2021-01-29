// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "constants/XcalarConfig.h"
#include "SchemaProcessor.h"
#include "sys/XLog.h"

static const char *moduleName = "SchemaProcessor";

Status
SchemaProcessor::jsonifySchema(char **jsonOutStr)
{
    struct JsonHolder {
        JsonHolder(json_t *jsonIn) { json = jsonIn; }
        ~JsonHolder()
        {
            if (json != nullptr) {
                json_decref(json);
                json = nullptr;
            }
        }
        json_t *json;
    };

    JsonHolder columns(json_array());
    if (columns.json == nullptr) {
        xSyslog(moduleName, XlogErr, "Error creating JSON array for columns");
        return StatusJsonError;
    }

    for (uint64_t ii = 0; ii < schema_.numColumns; ii++) {
        JsonHolder singleColumn(
            json_pack("{"
                      "s:s,"
                      "s:s,"
                      "s:s,"
                      "s:s"
                      "}",
                      "name",
                      schema_.columns[ii]->columnName,
                      "mapping",
                      schema_.columns[ii]->jsonPathMapping,
                      "type",
                      schema_.columns[ii]->strDfFieldType,
                      "srcType",
                      schema_.columns[ii]->strSrcFieldType));
        if (singleColumn.json == nullptr) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error creating single column(%lu)",
                    ii);
            return StatusJsonError;
        }
        int ret = json_array_append_new(columns.json, singleColumn.json);
        if (ret != 0) {
            xSyslog(moduleName,
                    XlogErr,
                    "Unknown json error while packing schema column.");
            return StatusJsonError;
        }
        singleColumn.json = nullptr;
    }

    // TODO: If top level array is detected, we can set the rowpath
    // to something appropriate so that we can select the elements
    // of the array from the file. We would first need this to be
    // streaming ...
    JsonHolder schema(
        json_pack("{"
                  "s:s,"
                  "s:o,"
                  "s:s"
                  "}",
                  "rowpath",
                  "$",
                  "columns",
                  columns.json,
                  "status",
                  schema_.status));
    if (schema.json == nullptr) {
        xSyslog(moduleName,
                XlogErr,
                "Unknown json error while packing schema.");
        return StatusJsonError;
    }

    *jsonOutStr = json_dumps(schema.json, 0);
    if (*jsonOutStr == nullptr) {
        xSyslog(moduleName,
                XlogErr,
                "Error while dumping schema Json to string.");
        return StatusJsonError;
    }
    // ENG-10303, the docs say json_pack() returns a new reference.
    // It doesn't mention anything about json_dumps() decref'ing the
    // object, yet I get a heap-use-after-free when the dtor tries
    // to decref this.
    schema.json = nullptr;
    return StatusOk;
}

Status
SchemaProcessor::setStatus(Status status)
{
    const char *statusStr = strGetFromStatus(status);
    size_t statusStrLen = strlen(statusStr);
    if (statusStrLen < maxStatusStrLen_) {
        strncpy(schema_.status, statusStr, statusStrLen);
        schema_.status[statusStrLen] = '\0';
        return StatusOk;
    } else {
        xSyslog(moduleName, XlogErr, "Status str too long (%lu)", statusStrLen);
        return StatusFailed;
    }
}

inline Status
SchemaProcessor::checkNumColumns()
{
    if (schema_.numColumns < maxNumCols_) {
        return StatusOk;
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Could not add column. "
                "Reached max col count(%lu)",
                schema_.numColumns);
        return StatusMaxColumnsFound;
    }
}

inline Status
SchemaProcessor::addObjectElement(const char *name)
{
    Status status;
    status = checkAvailPathBuff(pathMapPtrPos_ + 1);
    if (status != StatusOk) {
        return status;
    }
    pathMapBuff_[pathMapPtrPos_++] = '.';
    pathMapBuff_[pathMapPtrPos_++] = '"';

    for (size_t ii = 0; ii < strlen(name); ii++) {
        status = checkAvailPathBuff(pathMapPtrPos_ + 1);
        if (status != StatusOk) {
            return status;
        }
        status = checkAvailColNameBuff(colNamePtrPos_);
        if (status != StatusOk) {
            return status;
        }
        colNameBuff_[colNamePtrPos_++] = cleanColNameChar(name[ii], ii);
        if (name[ii] == '"') {
            // Escape double quote with double quote.
            pathMapBuff_[pathMapPtrPos_++] = '"';
        }
        pathMapBuff_[pathMapPtrPos_++] = name[ii];
    }
    pathMapBuff_[pathMapPtrPos_++] = '"';

    return StatusOk;
}

inline Status
SchemaProcessor::addArrayElement(int64_t arrIdx)
{
    Status status;
    int64_t arrIdxStrLen = 1;

    if (arrIdx != 0) {
        arrIdxStrLen = log10(arrIdx) + 1;
    }
    status = checkAvailPathBuff(pathMapPtrPos_ + arrIdxStrLen + 2);
    if (status != StatusOk) {
        return status;
    }
    status = checkAvailColNameBuff(pathMapPtrPos_ + arrIdxStrLen + 1);
    if (status != StatusOk) {
        return status;
    }
    pathMapBuff_[pathMapPtrPos_++] = '[';
    colNameBuff_[colNamePtrPos_++] = '_';

    for (int offset, digit, digitIdx = 0; digitIdx < arrIdxStrLen; digitIdx++) {
        // Write digits from right to left
        digit = arrIdx % 10;
        offset = arrIdxStrLen - digitIdx - 1;
        pathMapBuff_[pathMapPtrPos_ + offset] = '0' + digit;
        colNameBuff_[colNamePtrPos_ + offset] = '0' + digit;
        arrIdx /= 10;
    }
    pathMapPtrPos_ += arrIdxStrLen;
    colNamePtrPos_ += arrIdxStrLen;
    pathMapBuff_[pathMapPtrPos_++] = ']';

    return StatusOk;
}

inline Status
SchemaProcessor::checkAvailColNameBuff(size_t bytesNeeded)
{
    if (bytesNeeded < maxColNameLen_) {
        return StatusOk;
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Out of buffer space for ColName buffer. "
                "Bytes needed: %lu (max: %lu).",
                bytesNeeded,
                maxColNameLen_);
        return StatusFailed;
    }
}

inline Status
SchemaProcessor::checkAvailPathBuff(size_t bytesNeeded)
{
    if (bytesNeeded < maxJsonPathLen_) {
        return StatusOk;
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Out of buffer space for PathMap buffer. "
                "Bytes needed: %lu (max: %lu).",
                bytesNeeded,
                maxJsonPathLen_);
        return StatusFailed;
    }
}

//
// JsonSchemaProcessor
//

Status
JsonSchemaProcessor::run(const char *data, int64_t dataSize)
{
    json_error_t jsonError;
    json_t *json = json_loads(data, 0, &jsonError);

    if (json == nullptr) {
        xSyslog(moduleName, XlogErr, "json error. msg='%s'", jsonError.text);
        return StatusJsonError;
    }

    schema_.numColumns = 0;
    pathMapBuff_[pathMapPtrPos_++] = '$';
    strncpy(colNameBuff_, "ROOT", strlen("ROOT"));
    colNamePtrPos_ = strlen("ROOT");

    Status findColumnsStatus = findColumns(json);
    Status status = setStatus(findColumnsStatus);
    if (status != StatusOk) {
        return status;
    } else {
        return findColumnsStatus;
    }
}

Status
JsonSchemaProcessor::findColumns(json_t *node)
{
    int nodeType = json_typeof(node);
    switch (nodeType) {
    case JSON_OBJECT:
        return walkObject(node);
    case JSON_ARRAY:
        return walkArray(node);
    case JSON_NULL:
    case JSON_STRING:
    case JSON_INTEGER:
    case JSON_REAL:
    case JSON_TRUE:
    case JSON_FALSE:
        return addColumn(node);
    default:
        xSyslog(moduleName,
                XlogErr,
                "Unknown JSON node type found(%d)",
                nodeType);
        return StatusFailed;
    }
}

Status
JsonSchemaProcessor::walkObject(json_t *node)
{
    Status status;
    const char *key;
    json_t *val = NULL;
    int64_t origPathMapPtrPos = pathMapPtrPos_;
    int64_t origColNamePtrPos = colNamePtrPos_;

    json_object_foreach (node, key, val) {
        colNamePtrStartPos_ = colNamePtrPos_;

        status = addObjectElement(key);
        if (status != StatusOk) {
            return status;
        }
        status = findColumns(val);
        if (status != StatusOk) {
            return status;
        }
        pathMapPtrPos_ = origPathMapPtrPos;
        colNamePtrPos_ = origColNamePtrPos;
    }
    return StatusOk;
}

Status
JsonSchemaProcessor::walkArray(json_t *node)
{
    Status status = StatusOk;
    size_t numElems = json_array_size(node);
    int64_t origPathMapPtrPos = pathMapPtrPos_;
    int64_t origColNamePtrPos = colNamePtrPos_;
    int64_t origColNamePtrStartPos = colNamePtrStartPos_;

    for (size_t ii = 0; ii < numElems; ii++) {
        status = addArrayElement(ii);
        if (status != StatusOk) {
            return status;
        }
        json_t *val = json_array_get(node, ii);
        status = findColumns(val);
        if (status != StatusOk) {
            return status;
        }
        pathMapPtrPos_ = origPathMapPtrPos;
        colNamePtrPos_ = origColNamePtrPos;
        colNamePtrStartPos_ = origColNamePtrStartPos;
    }
    return status;
}

Status
JsonSchemaProcessor::addColumn(json_t *node)
{
    Status status = checkNumColumns();
    if (status != StatusOk) {
        return status;
    }

    std::string dfFieldTypeStrBuff;
    std::string srcFieldTypeStrBuff;

    status = copyColumnTypes(node, dfFieldTypeStrBuff, srcFieldTypeStrBuff);
    if (status != StatusOk) {
        return status;
    }

    // XXX These could be shared functions in the parent class
    if (dfFieldTypeStrBuff.length() >= maxDfFieldTypeStrLen_) {
        xSyslog(moduleName,
                XlogErr,
                "dfFieldTypeStr too large(%lu): '%s'",
                dfFieldTypeStrBuff.length(),
                dfFieldTypeStrBuff.c_str());
        return StatusFailed;
    }
    if (srcFieldTypeStrBuff.length() >= maxSrcFieldTypeStrLen_) {
        xSyslog(moduleName,
                XlogErr,
                "srcTypeStr too large(%lu): '%s'",
                dfFieldTypeStrBuff.length(),
                dfFieldTypeStrBuff.c_str());
        return StatusFailed;
    }

    ColumnInfo *columnInfo = new (std::nothrow) ColumnInfo();
    if (columnInfo == nullptr) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create column(%lu)",
                schema_.numColumns);
        return StatusNoMem;
    }

    strcpy(columnInfo->strDfFieldType, dfFieldTypeStrBuff.c_str());
    strcpy(columnInfo->strSrcFieldType, srcFieldTypeStrBuff.c_str());
    strncpy(columnInfo->jsonPathMapping, pathMapBuff_, pathMapPtrPos_);
    columnInfo->columnName[pathMapPtrPos_] = '\0';

    size_t colNameLen = colNamePtrPos_ - colNamePtrStartPos_;
    if (colNameLen > maxColNameLen_) {
        colNameLen = maxColNameLen_;
    }
    for (size_t dest = 0, src = colNamePtrStartPos_; dest < colNameLen;
         dest++, src++) {
        columnInfo->columnName[dest] = colNameBuff_[src];
    }
    columnInfo->columnName[colNameLen] = '\0';

    schema_.columns[schema_.numColumns++] = columnInfo;

    return StatusOk;
}

Status
JsonSchemaProcessor::copyColumnTypes(json_t *node,
                                     std::string &dfFieldTypeStrBuff,
                                     std::string &srcFieldTypeStrBuff)
{
    // https://jansson.readthedocs.io/en/latest/apiref.html
    static const DfFieldType janssonToDfFieldType[] = {
        DfFieldType::DfObject,   // JSON_OBJECT
        DfFieldType::DfArray,    // JSON_ARRAY
        DfFieldType::DfString,   // JSON_STRING
        DfFieldType::DfInt64,    // JSON_INTEGER
        DfFieldType::DfFloat64,  // JSON_REAL
        DfFieldType::DfBoolean,  // JSON_TRUE
        DfFieldType::DfBoolean,  // JSON_FALSE
        DfFieldType::DfString    // JSON_NULL
    };
    static const char *janssonTypeStrs[] = {"JSON_OBJECT",
                                            "JSON_ARRAY",
                                            "JSON_STRING",
                                            "JSON_INTEGER",
                                            "JSON_REAL",
                                            "JSON_TRUE",
                                            "JSON_FALSE",
                                            "JSON_NULL"};
    // TODO: ^ make these a single struct/type

    static const uint8_t janssonToDfFieldTypeCount =
        sizeof(janssonToDfFieldType) / sizeof(janssonToDfFieldType[0]);
    int nodeType = json_typeof(node);

    if (nodeType > janssonToDfFieldTypeCount) {
        xSyslog(moduleName, XlogErr, "Unknown jansson type enum: %d", nodeType);
        return StatusFailed;
    }
    dfFieldTypeStrBuff = strGetFromDfFieldType(janssonToDfFieldType[nodeType]);
    srcFieldTypeStrBuff = janssonTypeStrs[nodeType];
    return StatusOk;
}

//
//  ParquetSchemaProcessor
//

Status
ParquetSchemaProcessor::run(const char *data, int64_t dataSize)
{
    try {
        std::shared_ptr<arrow::io::BufferReader> bufferReader(
            new arrow::io::BufferReader((uint8_t *) data, dataSize));

        std::unique_ptr<parquet::ParquetFileReader> pqReader =
            parquet::ParquetFileReader::Open(bufferReader);

        parquet::ParquetFileReader *pqReaderPtr = pqReader.get();

        const parquet::FileMetaData *fileMeta = pqReaderPtr->metadata().get();
        const parquet::SchemaDescriptor *schema = fileMeta->schema();
        const parquet::schema::NodePtr node = schema->schema_root();

        schema_.numColumns = 0;
        pathMapBuff_[pathMapPtrPos_++] = '$';
        strncpy(colNameBuff_, "ROOT", strlen("ROOT"));
        colNamePtrPos_ = strlen("ROOT");

        Status findColumnsStatus = findColumns(node);
        Status status = setStatus(findColumnsStatus);
        if (status != StatusOk) {
            return status;
        } else {
            return findColumnsStatus;
        }
    } catch (std::exception &e) {
        xSyslog(moduleName, XlogErr, "Parquet Parsing Error: %s", e.what());
        return StatusParquetParserError;
    }
}

Status
ParquetSchemaProcessor::findColumns(parquet::schema::NodePtr node)
{
    if (node->is_group()) {
        if (node->logical_type() == parquet::LogicalType::LIST) {
            return walkArray(node);
        } else {
            return walkObject(node);
        }
    } else {
        return addColumn(node);
    }
}

Status
ParquetSchemaProcessor::walkObject(parquet::schema::NodePtr node)
{
    Status status;
    const parquet::schema::GroupNode *group =
        static_cast<const parquet::schema::GroupNode *>(node.get());
    int64_t origPathMapPtrPos = pathMapPtrPos_;
    int64_t origColNamePtrPos = colNamePtrPos_;

    for (int ii = 0; ii < group->field_count(); ii++) {
        parquet::schema::NodePtr innerNode = group->field(ii);
        const char *nodeName = innerNode->name().c_str();
        colNamePtrStartPos_ = colNamePtrPos_;

        status = addObjectElement(nodeName);
        if (status != StatusOk) {
            return status;
        }
        status = findColumns(innerNode);
        if (status != StatusOk) {
            return status;
        }
        pathMapPtrPos_ = origPathMapPtrPos;
        colNamePtrPos_ = origColNamePtrPos;
    }
    return StatusOk;
}

Status
ParquetSchemaProcessor::walkArray(parquet::schema::NodePtr node)
{
    Status status;
    const parquet::schema::GroupNode *group =
        static_cast<const parquet::schema::GroupNode *>(node.get());
    int64_t origPathMapPtrPos = pathMapPtrPos_;
    int64_t origColNamePtrPos = colNamePtrPos_;
    int64_t origColNamePtrStartPos = colNamePtrStartPos_;

    for (int ii = 0; ii < group->field_count(); ii++) {
        parquet::schema::NodePtr innerNode = group->field(ii);

        status = addArrayElement(ii);
        if (status != StatusOk) {
            return status;
        }
        status = findColumns(innerNode);
        if (status != StatusOk) {
            return status;
        }
        pathMapPtrPos_ = origPathMapPtrPos;
        colNamePtrPos_ = origColNamePtrPos;
        colNamePtrStartPos_ = origColNamePtrStartPos;
    }
    return StatusOk;
}

Status
ParquetSchemaProcessor::addColumn(parquet::schema::NodePtr node)
{
    Status status;
    status = checkNumColumns();
    if (status != StatusOk) {
        return status;
    }

    std::string dfFieldTypeStrBuff;
    std::string srcFieldTypeStrBuff;

    status = copyColumnTypes(node, dfFieldTypeStrBuff, srcFieldTypeStrBuff);
    if (status != StatusOk) {
        return status;
    }
    // XXX These could be shared functions in the parent class
    if (dfFieldTypeStrBuff.length() >= maxDfFieldTypeStrLen_) {
        xSyslog(moduleName,
                XlogErr,
                "dfFieldTypeStr too large(%lu): '%s'",
                dfFieldTypeStrBuff.length(),
                dfFieldTypeStrBuff.c_str());
        return StatusFailed;
    }
    if (srcFieldTypeStrBuff.length() >= maxSrcFieldTypeStrLen_) {
        xSyslog(moduleName,
                XlogErr,
                "srcTypeStr too large(%lu): '%s'",
                dfFieldTypeStrBuff.length(),
                dfFieldTypeStrBuff.c_str());
        return StatusFailed;
    }

    ColumnInfo *columnInfo = new (std::nothrow) ColumnInfo();
    if (columnInfo == nullptr) {
        xSyslog(moduleName,
                XlogErr,
                "Could not add column(%lu)",
                schema_.numColumns);
        return StatusNoMem;
    }

    strcpy(columnInfo->strDfFieldType, dfFieldTypeStrBuff.c_str());
    strcpy(columnInfo->strSrcFieldType, srcFieldTypeStrBuff.c_str());
    strncpy(columnInfo->jsonPathMapping, pathMapBuff_, pathMapPtrPos_);
    columnInfo->columnName[pathMapPtrPos_] = '\0';

    size_t colNameLen = colNamePtrPos_ - colNamePtrStartPos_;
    if (colNameLen > maxColNameLen_) {
        colNameLen = maxColNameLen_;
    }
    for (size_t dest = 0, src = colNamePtrStartPos_; dest < colNameLen;
         dest++, src++) {
        columnInfo->columnName[dest] = colNameBuff_[src];
    }
    columnInfo->columnName[colNameLen] = '\0';

    schema_.columns[schema_.numColumns++] = columnInfo;

    return StatusOk;
}

// XXX Need to spend a serious amount of time reading the parquet
// code to figure out all the permutations of physical and logical
// types, make datasets with them, and verify how S3 select returns
// the data ... and determine the proper Xcalar DfFieldTypes we
// should use ... and document it.
Status
ParquetSchemaProcessor::copyColumnTypes(parquet::schema::NodePtr node,
                                        std::string &dfFieldTypeStrBuff,
                                        std::string &srcFieldTypeStrBuff)
{
    Status status = StatusOk;
    if (!node->is_primitive()) {
        xSyslog(moduleName,
                XlogErr,
                "populateColumnTypes() must be primitive node");
        return StatusFailed;
    }
    const parquet::schema::PrimitiveNode *primitiveNode =
        static_cast<const parquet::schema::PrimitiveNode *>(node.get());

    parquet::Type::type physicalType = primitiveNode->physical_type();
    parquet::LogicalType::type logicalType = primitiveNode->logical_type();

    // Example: BYTE_ARRAY|UTF8
    srcFieldTypeStrBuff = parquet::TypeToString(physicalType);
    srcFieldTypeStrBuff += typeSeparatorChar_;
    srcFieldTypeStrBuff += parquet::LogicalTypeToString(logicalType);

    // Maybe there should be some kind of map created?
    DfFieldType dfFieldType = DfFieldType::DfUnknown;
    switch (physicalType) {
    case parquet::Type::BOOLEAN:
        dfFieldType = DfFieldType::DfBoolean;
        break;
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
        dfFieldType = DfFieldType::DfFloat64;
        break;
    case parquet::Type::INT32:
    case parquet::Type::INT64:
        // Need to investigate this further.
        if (logicalType == parquet::LogicalType::TIMESTAMP_MILLIS) {
            dfFieldType = DfFieldType::DfString;
        } else if (logicalType == parquet::LogicalType::TIMESTAMP_MICROS) {
            dfFieldType = DfFieldType::DfString;
        } else {
            dfFieldType = DfFieldType::DfInt64;
        }
        break;
    case parquet::Type::INT96:
    case parquet::Type::BYTE_ARRAY:
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        dfFieldType = DfFieldType::DfString;
        break;
    }
    dfFieldTypeStrBuff = strGetFromDfFieldType(dfFieldType);

    return StatusOk;
}
