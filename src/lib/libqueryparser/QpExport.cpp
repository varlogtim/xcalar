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
#include <getopt.h>
#include <inttypes.h>
#include "GetOpt.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "StrlFunc.h"
#include "util/MemTrack.h"
#include "queryparser/QueryParser.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "export/DataTarget.h"
#include "udf/UserDefinedFunction.h"

static constexpr const char *moduleName = "QpExport";

QpExport::QpExport()
{
    this->isValidCmdParser = true;
}

QpExport::~QpExport()
{
    this->isValidCmdParser = false;
}

Status
QpExport::parseArgs(int argc, char *argv[], ExportArgs *exportArgs)
{
    assert(false && "old CLI style API is deprecated");
    return StatusFailed;
}

Status
QpExport::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    assert(false && "old CLI style API is deprecated");
    return StatusFailed;
}

Status
QpExport::getLegacyFileTarget(const char *targetName,
                              ExAddTargetSFInput *sfInput)
{
    Status status;
    XcalarApiOutput *apiOutput = NULL;
    XcalarApiListExportTargetsOutput *targetsOutput;
    const ExExportTarget *target;
    size_t ignored;

    // We know the user's fileName, we need to look up their target to construct
    // the fully qualified path, since the fileName is relative to the sftarget

    status = DataTargetManager::getRef().listTargets("*",
                                                     targetName,
                                                     &apiOutput,
                                                     &ignored);
    BailIfFailed(status);

    targetsOutput = &apiOutput->outputResult.listTargetsOutput;

    if (targetsOutput->numTargets == 0) {
        xSyslogTxnBuf("QPExport", XlogErr, "target '%s' not found", targetName);
        status = StatusLegacyTargetNotFound;
        goto CommonExit;
    } else if (targetsOutput->numTargets != 1) {
        xSyslogTxnBuf("QPExport",
                      XlogErr,
                      "unexpected %i targets named '%s' found",
                      targetsOutput->numTargets,
                      targetName);
        status = StatusInval;
        goto CommonExit;
    }
    assert(targetsOutput->numTargets == 1);
    target = &targetsOutput->targets[0];

    assert(strcmp(target->hdr.name, targetName) == 0);

    *sfInput = target->specificInput.sfInput;

CommonExit:
    if (apiOutput) {
        memFree(apiOutput);
        apiOutput = NULL;
    }
    return status;
}

MustCheck Status
QpExport::getUdfFilePathAndName(const char *targetName,
                                ExAddTargetUDFInput *udfInput)
{
    Status status;
    XcalarApiOutput *apiOutput = NULL;
    const XcalarApiListExportTargetsOutput *targetsOutput;
    const ExExportTarget *target;
    size_t ignored;
    char *newAppName = NULL;

    // We know the user's fileName, we need to look up their target to construct
    // the fully qualified path, since the fileName is relative to the sftarget

    status = DataTargetManager::getRef().listTargets("*",
                                                     targetName,
                                                     &apiOutput,
                                                     &ignored);
    BailIfFailed(status);

    targetsOutput = &apiOutput->outputResult.listTargetsOutput;

    if (targetsOutput->numTargets == 0) {
        xSyslogTxnBuf("QPExport", XlogErr, "target '%s' not found", targetName);
        status = StatusLegacyTargetNotFound;
        goto CommonExit;
    } else if (targetsOutput->numTargets != 1) {
        xSyslogTxnBuf("QPExport",
                      XlogErr,
                      "unexpected %i targets named '%s' found",
                      targetsOutput->numTargets,
                      targetName);
        status = StatusInval;
        goto CommonExit;
    }
    assert(targetsOutput->numTargets == 1);
    target = &targetsOutput->targets[0];

    assert(strcmp(target->hdr.name, targetName) == 0);

    *udfInput = target->specificInput.udfInput;

    // Generate a new module name from its full path. The module should be
    // already copied to the shared UDF space with the same generated name.
    // The export driver is going to use the copied UDF in shared space.
    status = UserDefinedFunction::generateUserWorkbookUdfName(udfInput->appName,
                                                              &newAppName,
                                                              true);
    BailIfFailed(status);

    status =
        strStrlcpy(udfInput->appName, newAppName, sizeof(udfInput->appName));
    BailIfFailed(status);

CommonExit:
    if (apiOutput) {
        memFree(apiOutput);
        apiOutput = NULL;
    }
    if (newAppName) {
        memFree(newAppName);
        newAppName = NULL;
    }
    return status;
}

Status
QpExport::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    char DefaultRecordDelim[2];
    DefaultRecordDelim[0] = DfCsvDefaultRecordDelimiter;
    DefaultRecordDelim[1] = '\0';

    char DefaultFieldDelim[2];
    DefaultFieldDelim[0] = DfCsvDefaultFieldDelimiter;
    DefaultFieldDelim[1] = '\0';

    char DefaultQuoteDelim[2];
    DefaultQuoteDelim[0] = DfCsvDefaultQuoteDelimiter;
    DefaultQuoteDelim[1] = '\0';

    Status status = StatusOk;
    const char *source = "", *fileName = "", *targetName = "", *targetType = "",
               *dest = "", *splitRule = "", *headerType = "", *createRule = "",
               *format = "", *recordDelim = DefaultRecordDelim,
               *fieldDelim = DefaultFieldDelim, *quoteDelim = DefaultQuoteDelim,
               *driverName = "", *driverParams = "";
    json_int_t splitSize = 0, splitNumFiles = 0;
    int sorted = true;
    json_t *columns = NULL;

    XcalarWorkItem *workItem = NULL;
    unsigned numColumns = 0;
    ExColumnName *exCols = NULL;

    // Stuff for rendering driver parameters
    json_t *driverParamJson = NULL;
    char *renderedDriverParams = NULL;
    char *renderedFilePath = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &source,
                             FileNameKey,
                             &fileName,
                             TargetNameKey,
                             &targetName,
                             TargetTypeKey,
                             &targetType,
                             DestKey,
                             &dest,
                             ColumnsKey,
                             &columns,
                             SplitRuleKey,
                             &splitRule,
                             SplitSizeKey,
                             &splitSize,
                             SplitNumFilesKey,
                             &splitNumFiles,
                             HeaderTypeKey,
                             &headerType,
                             CreateRuleKey,
                             &createRule,
                             SortedKey,
                             &sorted,
                             FormatKey,
                             &format,
                             FieldDelimKey,
                             &fieldDelim,
                             RecordDelimKey,
                             &recordDelim,
                             QuoteDelimKey,
                             &quoteDelim,
                             DriverNameKey,
                             &driverName,
                             DriverParamsKey,
                             &driverParams);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    json_t *col;
    unsigned ii;
    numColumns = json_array_size(columns);
    exCols = (ExColumnName *) memAlloc(numColumns * sizeof(*exCols));
    BailIfNull(exCols);

    json_array_foreach (columns, ii, col) {
        const char *colName, *headerName;

        ret = json_unpack_ex(col,
                             err,
                             0,
                             "{s:s,s:s}",
                             ColumnNameKey,
                             &colName,
                             HeaderNameKey,
                             &headerName);
        if (ret != 0) {
            status = StatusJsonQueryParseError;
            goto CommonExit;
        }

        status = strStrlcpy(exCols[ii].name, colName, sizeof(exCols[ii].name));
        BailIfFailed(status);

        status = strStrlcpy(exCols[ii].headerAlias,
                            headerName,
                            sizeof(exCols[ii].headerAlias));
        BailIfFailed(status);
    }

    // All old exports will have the targetType key; we can use this as a proxy
    // for whether we need to upgrade
    if (targetType[0] != '\0') {
        // We now know that we need to upgrade
        if (strcmp(targetType, "file") == 0) {
            ExAddTargetSFInput sfTarget;
            driverName = "multiple_csv";

            status = getLegacyFileTarget(targetName, &sfTarget);
            BailIfFailed(status);

            driverParamJson = json_pack("{s:s,s:s,s:s,s:s,s:s,s:s}",
                                        "target",
                                        "Default Shared Root",
                                        "directory_path",
                                        sfTarget.url,
                                        "file_base",
                                        fileName,
                                        "field_delim",
                                        fieldDelim,
                                        "record_delim",
                                        recordDelim,
                                        "quote_delim",
                                        quoteDelim);
            BailIfNull(driverParamJson);

            renderedDriverParams = json_dumps(driverParamJson, 0);
            BailIfNull(renderedDriverParams);

            driverParams = renderedDriverParams;
        } else if (strcmp(targetType, "udf") == 0) {
            ExAddTargetUDFInput udfTarget;
            driverName = "legacy_udf";

            status = getUdfFilePathAndName(targetName, &udfTarget);
            BailIfFailed(status);

            driverParamJson = json_pack("{s:s,s:s,s:s,s:s,s:s,s:s,s:s}",
                                        "directory_path",
                                        udfTarget.url,
                                        "export_udf",
                                        udfTarget.appName,
                                        "file_name",
                                        fileName,
                                        "header",
                                        headerType,
                                        "fieldDelim",
                                        fieldDelim,
                                        "recordDelim",
                                        recordDelim,
                                        "quoteDelim",
                                        quoteDelim);
            BailIfNull(driverParamJson);

            renderedDriverParams = json_dumps(driverParamJson, 0);
            BailIfNull(renderedDriverParams);

            driverParams = renderedDriverParams;
        } else {
            xSyslogTxnBuf("QPExport",
                          XlogErr,
                          "invalid target type %s",
                          targetType);
            status = StatusInval;
            goto CommonExit;
        }
    }

    workItem = xcalarApiMakeExportWorkItem(source,
                                           dest,
                                           numColumns,
                                           exCols,
                                           driverName,
                                           driverParams);
    BailIfNull(workItem);

CommonExit:
    if (exCols) {
        memFree(exCols);
        exCols = NULL;
    }
    if (driverParamJson) {
        json_decref(driverParamJson);
        driverParamJson = NULL;
    }
    if (renderedFilePath) {
        memFree(renderedFilePath);
        renderedFilePath = NULL;
    }
    if (renderedDriverParams) {
        memFree(renderedDriverParams);
        renderedDriverParams = NULL;
    }

    *workItemOut = workItem;

    return status;
}

Status
QpExport::reverseParse(const XcalarApiInput *input,
                       json_error_t *err,
                       json_t **argsOut)
{
    const XcalarApiExportInput *exInput;
    Status status = StatusOk;
    json_t *args = NULL, *columns = NULL;
    json_t *column = NULL;

    exInput = &input->exportInput;

    columns = json_array();
    BailIfNull(columns);

    for (int ii = 0; ii < exInput->meta.numColumns; ii++) {
        int ret;
        assert(column == NULL);
        column = json_pack_ex(err,
                              0,
                              "{s:s,s:s}",
                              ColumnNameKey,
                              exInput->meta.columns[ii].name,
                              HeaderNameKey,
                              exInput->meta.columns[ii].headerAlias);
        BailIfNullWith(column, StatusJsonQueryParseError);

        ret = json_array_append_new(columns, column);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        column = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        exInput->srcTable.tableName,
                        DestKey,
                        exInput->exportName,
                        ColumnsKey,
                        columns,
                        DriverNameKey,
                        exInput->meta.driverName,
                        DriverParamsKey,
                        exInput->meta.driverParams);
    columns = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (column != NULL) {
        json_decref(column);
        column = NULL;
    }

    if (columns != NULL) {
        json_decref(columns);
        columns = NULL;
    }

    *argsOut = args;

    return status;
}
