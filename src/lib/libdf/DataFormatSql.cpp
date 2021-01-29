// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "df/DataFormat.h"
#include "dataformat/DataFormatSql.h"
#include "util/MemTrack.h"
#include "common/Version.h"
#include "libapis/LibApisCommon.h"

SqlFormatOps *SqlFormatOps::instance;

Status
SqlFormatOps::init()
{
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(SqlFormatOps), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    SqlFormatOps::instance = new (ptr) SqlFormatOps();

    return StatusOk;
}

SqlFormatOps *
SqlFormatOps::get()
{
    assert(instance);
    return instance;
}

void
SqlFormatOps::destroy()
{
    SqlFormatOps *inst = SqlFormatOps::get();

    inst->~SqlFormatOps();
    memFree(inst);
    SqlFormatOps::instance = NULL;
}

IRowRenderer *
SqlFormatOps::getRowRenderer()
{
    SqlRowRenderer *renderer = new (std::nothrow) SqlRowRenderer();
    return renderer;
}

static Status
getTypeName(DfFieldType dfType, size_t destSize, char *dest, int varCharSize)
{
    Status status = StatusOk;
    size_t strLength;

    switch (dfType) {
    case DfInt32:
    case DfUInt32:
        strLength = snprintf(dest, destSize, "INTEGER");
        break;
    case DfInt64:
    case DfUInt64:
        strLength = snprintf(dest, destSize, "BIGINT");
        break;
    case DfFloat32:
        strLength = snprintf(dest, destSize, "REAL");
        break;
    case DfFloat64:
        strLength = snprintf(dest, destSize, "FLOAT");
        break;
    case DfBoolean:
        strLength = snprintf(dest, destSize, "BOOLEAN");
        break;
    case DfString:
        // strings are a very large nvarchar. This will be inefficient
        // on some databases that aren't lazy about nvarchar
        assert(varCharSize > 0);
        strLength = snprintf(dest, destSize, "NVARCHAR(%u)", varCharSize);
        break;
    case DfTimespec:
        strLength = snprintf(dest, destSize, "TIMESTAMP");
        break;
    case DfNull:
    case DfBlob:
    default:
        assert(0);
        status = StatusUnimpl;
    }
    if (status == StatusOk && strLength >= destSize) {
        status = StatusNoBufs;
    }
    return status;
}

static Status
renderHeader(const char *tableName,
             XcalarApiVersion version,
             size_t destSize,
             char *dest,
             size_t *bytesWritten)
{
    Status status;
    size_t strLength;
    time_t rawtime;
    struct tm *timeinfo;
    time(&rawtime);
    timeinfo = localtime(&rawtime);

    strLength = snprintf(dest,
                         destSize,
                         "-- Xcalar SQL Export of \"%s\"\n"
                         "-- %s"
                         "--\n"
                         "-- Xcalar Version: %s\n"
                         "-- API Version Signature: %s\n"
                         "\n"
                         "-- MySql Specific Settings\n"
                         "/*!SET SQL_MODE=ANSI_QUOTES */;\n"
                         "-- End MySql Specific Settings\n"
                         "\n",
                         tableName,
                         asctime(timeinfo),
                         versionGetStr(),
                         versionGetApiStr(version));
    if (strLength >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    status = StatusOk;
    *bytesWritten = strLength;

CommonExit:
    if (status != StatusOk) {
        *bytesWritten = 0;
    }
    return status;
}

static Status
renderSQLCreate(const char *tableName,
                int numColumns,
                const char **columnNames,
                const TupleValueDesc *valueDesc,
                size_t destSize,
                char *dest,
                size_t *bytesWritten)
{
    Status status;
    size_t strLength;
    int ii;
    bool firstCol = true;
    char typeBuf[64];
    // Right now this is constant.
    const int varCharSize = DfMaxFieldValueSize;

    // We want to build something like the following query string:
    // 'CREATE TABLE Persons(Name nvarchar(255), Age int, Height float)'

    // XXX - This rendering could be more efficient. As it stands it does
    // several full passes of the query string, but this is to make it
    // obvious what the code is doing, make safety easy, and performance
    // shouldn't matter since this is done once per export
    strLength = snprintf(dest, destSize, "CREATE TABLE %s (\n  ", tableName);
    if (strLength >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    // Now we have 'CREATE TABLE Persons('

    // Render each of the column names
    for (ii = 0; ii < numColumns; ii++) {
        if (!firstCol) {
            strLength = strlcat(dest, ",\n  ", destSize);
            if (strLength >= destSize) {
                status = StatusNoBufs;
                goto CommonExit;
            }
        }

        // Quotify the column names -- open
        strLength = strlcat(dest, "\"", destSize);
        if (strLength >= destSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        assert(columnNames[ii] != NULL);
        strLength = strlcat(dest, columnNames[ii], destSize);
        if (strLength >= destSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        // Now we have 'CREATE TABLE Persons(Name'
        strLength = strlcat(dest, "\" ", destSize);  // Close quoted column name
        if (strLength >= destSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        // Add on the type, storing in typeBuf first
        status = getTypeName(valueDesc->valueType[ii],
                             sizeof(typeBuf),
                             typeBuf,
                             varCharSize);
        BailIfFailed(status);

        strLength = strlcat(dest, typeBuf, destSize);
        if (strLength >= destSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        firstCol = false;
    }
    // Now we have
    // 'CREATE TABLE Persons(Name nvarchar(255), Age int, Height float'

    strLength = strlcat(dest, "\n);\n", destSize);
    if (strLength >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    // Now we have
    // 'CREATE TABLE Persons(
    //    Name nvarchar(255),
    //    Age int,
    //    Height float
    //  );'
    *bytesWritten = strLength;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        *bytesWritten = 0;
    }
    return status;
}

static Status
renderSQLDropIfExists(const char *tableName,
                      size_t destSize,
                      char *dest,
                      size_t *bytesWritten)
{
    Status status;
    size_t strLength;

    // XXX - THIS IS NOT PLATFORM INDEPENDENT
    // This syntax works in at least MySQL, PostgreSQL, RedShift, Hive
    // This DOES NOT WORK in Oracle
    strLength =
        snprintf(dest, destSize, "DROP TABLE IF EXISTS %s;\n\n", tableName);
    if (strLength >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    status = StatusOk;
    *bytesWritten = strLength;

CommonExit:
    if (status != StatusOk) {
        *bytesWritten = 0;
    }
    return status;
}

static Status
renderSQLInsertBegin(const char *tableName,
                     size_t destSize,
                     char *dest,
                     size_t *bytesWritten)
{
    Status status;
    size_t strLength;

    strLength = snprintf(dest, destSize, "INSERT INTO %s VALUES ", tableName);
    if (strLength >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    status = StatusOk;
    *bytesWritten = strLength;

CommonExit:
    if (status != StatusOk) {
        *bytesWritten = 0;
    }
    return status;
}

Status
SqlFormatOps::requiresFullSchema(
    const ExInitExportFormatSpecificArgs *formatArgs, bool *requiresFullSchema)
{
    *requiresFullSchema = true;
    return StatusOk;
}

Status
SqlFormatOps::renderColumnHeaders(
    const char *srcTableName,
    int numColumns,
    const char **headerColumns,
    const ExInitExportFormatSpecificArgs *formatArgs,
    const TupleValueDesc *valueDesc,
    char *buf,
    size_t bufSize,
    size_t *bytesWritten)
{
    Status status;
    const ExInitExportSQLArgs *args;
    size_t bufUsed = 0;
    size_t headBytesWritten;

    assert(numColumns > 0);

    args = &(formatArgs->sql);

    // Add a debugging/informational header
    status = renderHeader(srcTableName,
                          versionGetApiSig(),
                          bufSize - bufUsed,
                          buf + bufUsed,
                          &headBytesWritten);
    BailIfFailed(status);
    bufUsed += headBytesWritten;

    // Add a "DROP IF EXISTS"
    if (args->dropTable) {
        status = renderSQLDropIfExists(args->tableName,
                                       bufSize - bufUsed,
                                       buf + bufUsed,
                                       &headBytesWritten);
        BailIfFailed(status);
        bufUsed += headBytesWritten;
    }

    // Add a "CREATE TABLE table();"
    if (args->createTable) {
        status = renderSQLCreate(args->tableName,
                                 numColumns,
                                 headerColumns,
                                 valueDesc,
                                 bufSize - bufUsed,
                                 buf + bufUsed,
                                 &headBytesWritten);
        BailIfFailed(status);
        bufUsed += headBytesWritten;
    }

    headBytesWritten = strlcat(buf + bufUsed, "\n\n", bufSize - bufUsed);
    if (bufUsed + headBytesWritten >= bufSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    assert(bufUsed < bufSize);
    *bytesWritten = bufUsed;

    status = StatusOk;

CommonExit:
    return status;
}

// Invariant: Either bytesWritten = entire field || bytesWritten = 0
Status
SqlRowRenderer::formatFieldVal(char *buf,
                               int64_t bufSize,
                               DfFieldType dfType,
                               const DfFieldValue *fieldValue,
                               int64_t *bytesWritten) const
{
    unsigned ii;
    Status status = StatusOk;
    char c;
    DfFieldValue dst;

    const DfFieldValue *nonStringValue;

    const char *valueStr;
    int64_t valueStrLen;

    dst.stringVal.strActual = buf;
    dst.stringValTmp = (char *) dst.stringVal.strActual;

    // Here we have 2 main codepaths: strings and non-strings.
    // We have another set of branches: scalars and non-scalars.
    // For all strings: extract the actual string value and size
    // For all non-strings: extract the value, then convert
    if (dfType == DfScalarObj) {
        dfType = fieldValue->scalarVal->fieldType;
        if (dfType == DfString) {
            valueStr = fieldValue->scalarVal->fieldVals.strVals[0].strActual;
            valueStrLen = fieldValue->scalarVal->fieldVals.strVals[0].strSize;
        } else {
            nonStringValue = (DfFieldValue *) &fieldValue->scalarVal->fieldVals;
        }
    } else if (dfType == DfString) {
        valueStr = fieldValue->stringVal.strActual;
        valueStrLen = fieldValue->stringVal.strSize;
    } else {
        nonStringValue = fieldValue;
    }

    if (dfType != DfString) {
        // Formats the DfFieldValue src as a string and writes into dst
        status = DataFormat::convertValueType(DfString,
                                              dfType,
                                              nonStringValue,
                                              &dst,
                                              bufSize,
                                              BaseCanonicalForm);
        if (status != StatusOk) {
            *bytesWritten = 0;
            goto CommonExit;
        }
        // strSize contains null terminated character we want to overwrite
        *bytesWritten = dst.stringVal.strSize - 1;
    } else {
        // put strings in quotes
        buf[0] = '\'';
        *bytesWritten = 1;
        // strSize contains null terminated character
        for (ii = 0; ii < valueStrLen - 1; ii++) {
            c = valueStr[ii];
            switch (c) {
            case ('"'):
            case ('\\'):
                buf[*bytesWritten] = '\\';
                (*bytesWritten)++;
                if (unlikely(*bytesWritten >= bufSize)) {
                    *bytesWritten = 0;
                    status = StatusNoBufs;
                    goto CommonExit;
                }
                break;
            }
            buf[*bytesWritten] = c;
            (*bytesWritten)++;
            if (unlikely(*bytesWritten >= bufSize)) {
                *bytesWritten = 0;
                status = StatusNoBufs;
                goto CommonExit;
            }
        }
        buf[*bytesWritten] = '\'';
        (*bytesWritten)++;
        if (unlikely(*bytesWritten >= bufSize)) {
            *bytesWritten = 0;
            status = StatusNoBufs;
            goto CommonExit;
        }
        assert(*bytesWritten >= valueStrLen);
    }
CommonExit:

    return status;
}

Status
SqlRowRenderer::renderSQLValue(const NewKeyValueEntry *entry,
                               char *dest,
                               int64_t destSize,
                               int64_t *totalWritten) const
{
    Status status;
    int64_t bufUsed = 0;
    int64_t bytesWritten;

    // We want each of the column values like: ("Amy",23,3.5"),
    bytesWritten = strlcpy(dest + bufUsed, "(", destSize - bufUsed);

    if (bufUsed + bytesWritten >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    bufUsed += bytesWritten;

    size_t entryNumFields;
    entryNumFields = entry->kvMeta_->tupMeta_->getNumFields();

    // Put in all of the values for this record
    for (int ii = 0; ii < numFields_; ii++) {
        int ind = fields_[ii].entryIndex;
        DfFieldType typeTmp = entry->kvMeta_->tupMeta_->getFieldType(ind);
        bool retIsValid;
        DfFieldValue valueTmp =
            entry->tuple_.get(ind, entryNumFields, typeTmp, &retIsValid);
        if (!retIsValid) {
            bytesWritten = strlcpy(dest + bufUsed, "''", destSize - bufUsed);
            status = StatusOk;
            // We now have something like: (''
        } else {
            status = formatFieldVal(dest + bufUsed,
                                    destSize - bufUsed,
                                    fields_[ii].type,
                                    &valueTmp,
                                    &bytesWritten);
            BailIfFailed(status);
            // We now have something like: ("Amy"
        }
        if (bufUsed + bytesWritten >= destSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        bufUsed += bytesWritten;

        // last column doesn't get a comma
        if (ii != numFields_ - 1) {
            bytesWritten = strlcpy(dest + bufUsed, ",", destSize - bufUsed);
            if (bufUsed + bytesWritten >= destSize) {
                status = StatusNoBufs;
                goto CommonExit;
            }
            bufUsed += bytesWritten;
        }
    }

    // Add the ending parenthesis
    bytesWritten = strlcpy(dest + bufUsed, ")", destSize - bufUsed);
    if (bufUsed + bytesWritten >= destSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    bufUsed += bytesWritten;

    status = StatusOk;
    *totalWritten = bufUsed;

CommonExit:
    if (status != StatusOk) {
        *totalWritten = 0;
    }
    return status;
}

// This is like 'INSERT INTO MyTable VALUES '
Status
SqlFormatOps::renderPrelude(const TupleValueDesc *valueDesc,
                            const ExInitExportFormatSpecificArgs *formatArgs,
                            char *buf,
                            size_t bufSize,
                            size_t *bytesWritten)
{
    Status status;
    const ExInitExportSQLArgs *args;
    size_t bufUsed = 0;
    size_t headBytesWritten;

    args = &(formatArgs->sql);

    // We only want to have one INSERT, so add that here
    status = renderSQLInsertBegin(args->tableName,
                                  bufSize - bufUsed,
                                  buf + bufUsed,
                                  &headBytesWritten);
    BailIfFailed(status);
    bufUsed += headBytesWritten;

    assert(bufUsed < bufSize);
    *bytesWritten = bufUsed;

CommonExit:
    return status;
}

Status
SqlRowRenderer::init(const ExInitExportFormatSpecificArgs *formatArgs,
                     int numFields,
                     const Field *fields)
{
    Status status = StatusOk;
    // We only need sql, forget the formatArgs union
    args_ = &formatArgs->sql;
    numFields_ = numFields;
    fields_ = fields;

    return status;
}

Status
SqlRowRenderer::renderRow(const NewKeyValueEntry *entry,
                          bool first,
                          char *buf,
                          int64_t bufSize,
                          int64_t *bytesWritten)
{
    Status status;
    int64_t bufUsed = 0;
    int64_t rowBytesWritten;

    if (!first) {
        rowBytesWritten = strlcpy(buf + bufUsed, ",", bufSize - bufUsed);
        if (bufUsed + rowBytesWritten >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        bufUsed += rowBytesWritten;
    }

    status = renderSQLValue(entry,
                            buf + bufUsed,
                            bufSize - bufUsed,
                            &rowBytesWritten);
    BailIfFailed(status);
    if (bufUsed + rowBytesWritten >= bufSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    bufUsed += rowBytesWritten;

    assert(bufUsed < bufSize);
    *bytesWritten = bufUsed;

    status = StatusOk;
CommonExit:
    return status;
}

Status
SqlFormatOps::renderFooter(char *buf, size_t bufSize, size_t *bytesWritten)
{
    Status status;
    size_t bufUsed;

    bufUsed = strlcpy(buf, ";\n\n", bufSize);
    if (bufUsed >= bufSize) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    *bytesWritten = bufUsed;

    status = StatusOk;
CommonExit:
    return status;
}
