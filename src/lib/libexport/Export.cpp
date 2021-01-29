// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "export/Export.h"
#include "operators/Operators.h"
#include "app/AppMgr.h"
#include "sys/XLog.h"
#include "msg/Xid.h"

Status
ExportAppHost::exportTable(Dag *dag,
                           XdbId exportNodeId,
                           XdbId scalarTableId,
                           const XcalarApiExportInput *exportInput,
                           XcalarApiUserId *user)
{
    Status status;
    size_t metaSize, columnsSize;
    const ExExportMeta *meta;
    XdbId srcXdbId = exportInput->srcTable.xdbId;
    XdbId tableId = XdbIdInvalid;
    bool scalarTableMade = false;
    bool demystRequired;
    bool internalAppError = false;
    XdbMgr *xdbMgr = XdbMgr::get();

    assert(srcXdbId != XidInvalid);
    assert(scalarTableId != XidInvalid);

    meta = &exportInput->meta;

    columnsSize = meta->numColumns * sizeof(meta->columns[0]);
    metaSize = sizeof(*meta) + columnsSize;

    {
        int numVariables = meta->numColumns;
        char *variables[numVariables];
        XdbMeta *srcMeta;
        status = XdbMgr::get()->xdbGet(srcXdbId, NULL, &srcMeta);
        BailIfFailed(status);
        for (int ii = 0; ii < numVariables; ii++) {
            variables[ii] = (char *) meta->columns[ii].name;
        }
        demystRequired =
            Operators::get()->isDemystificationRequired(variables,
                                                        numVariables,
                                                        srcMeta);
    }

    if (demystRequired) {
        status = buildScalarTable(dag, user, meta, srcXdbId, scalarTableId);
        BailIfFailed(status);
        scalarTableMade = true;
        tableId = scalarTableId;
    } else {
        tableId = srcXdbId;
    }
    assert(tableId != XdbIdInvalid);

    status = runExportApp(tableId, exportInput, user, &internalAppError);
    BailIfFailed(status);

CommonExit:
    if (scalarTableMade) {
        assert(scalarTableId != XdbIdInvalid);
        if (!internalAppError) {
            xdbMgr->xdbDrop(scalarTableId);
        }
        scalarTableMade = false;
    }

    return status;
}

Status
ExportAppHost::buildScalarTable(Dag *dag,
                                XcalarApiUserId *user,
                                const ExExportMeta *meta,
                                XdbId srcXdbId,
                                XdbId scalarTableXdbId)
{
    Status status;
    size_t metaSize, columnsSize;
    ExExportBuildScalarTableInput *buildInput = NULL;
    size_t buildSize;

    assert(srcXdbId != XidInvalid);
    assert(scalarTableXdbId != XidInvalid);

    columnsSize = meta->numColumns * sizeof(meta->columns[0]);
    metaSize = sizeof(*meta) + columnsSize;

    buildSize =
        sizeof(*buildInput) + meta->numColumns * sizeof(buildInput->columns[0]);
    buildInput =
        (ExExportBuildScalarTableInput *) memAllocExt(buildSize, moduleName);
    BailIfNullWith(buildInput, StatusNoMem);

    buildInput->srcXdbId = srcXdbId;
    buildInput->dstXdbId = scalarTableXdbId;
    buildInput->sorted = true;  // Always sort the scalar table
    buildInput->numColumns = meta->numColumns;

    memcpy(buildInput->columns,
           meta->columns,
           meta->numColumns * sizeof(meta->columns[0]));

    status = Operators::get()->buildScalarTable(dag,
                                                buildInput,
                                                &scalarTableXdbId,
                                                user);
    BailIfFailed(status);

CommonExit:
    if (buildInput) {
        memFree(buildInput);
        buildInput = NULL;
    }

    return status;
}

Status
ExportAppHost::runExportApp(XdbId tableId,
                            const XcalarApiExportInput *exportInput,
                            XcalarApiUserId *user,
                            bool *retInternalAppError)
{
    Status status;
    App *exportApp = NULL;
    char *inBlob = NULL;
    char *outBlob = NULL;
    char *errBlob = NULL;
    LibNsTypes::NsHandle handle;

    exportApp =
        AppMgr::get()->openAppHandle(AppMgr::ExportHostAppName, &handle);
    if (exportApp == NULL) {
        status = StatusNoEnt;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Export host %s does not exist",
                      AppMgr::ExportHostAppName);
        goto CommonExit;
    }

    status = jsonifyExportInput(exportInput, tableId, &inBlob);
    BailIfFailed(status);

    // XXX TODO
    // App internal error cannot be ignored in the cases when attempt to
    // clean out fails due to failure to dispatch 2PC.
    // Even though this happens rarely, the calling code needs to be able to
    // leak references to objects in the event of cleanout failure.
    //
    // In Operators::loadDataset, Dataset object is leaked to specifically
    // handle the case where cleanout 2PC dispatch failured.
    //
    // Cleanest way to handle this is to also manage node local references
    // to active objects. For instance, if export is cursoring a Table, it
    // needs to grab local references to Table, such that the global drop on
    // Table from the source node of the export API just drops the ref instead
    // of just deleting the XDB.
    //
    status = AppMgr::get()->runMyApp(exportApp,
                                     AppGroup::Scope::Global,
                                     user ? user->userIdName : "",
                                     0,
                                     inBlob,
                                     0,
                                     &outBlob,
                                     &errBlob,
                                     retInternalAppError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Export host \"%s\" failed to start with error:\"%s\","
                      " output:\"%s\"",
                      exportApp->getName(),
                      errBlob ? errBlob : "",
                      outBlob ? outBlob : "");
        goto CommonExit;
    }

CommonExit:
    if (exportApp) {
        AppMgr::get()->closeAppHandle(exportApp, handle);
        exportApp = NULL;
    }
    if (inBlob) {
        memFree(inBlob);
        inBlob = NULL;
    }
    if (outBlob) {
        memFree(outBlob);
        outBlob = NULL;
    }
    if (errBlob) {
        memFree(errBlob);
        errBlob = NULL;
    }
    return status;
}

Status
ExportAppHost::jsonifyExportInput(const XcalarApiExportInput *exportInput,
                                  XdbId tableId,
                                  char **inBlob)
{
    Status status;
    json_t *json = NULL;
    json_t *columnsJson = NULL;
    json_t *colJson = NULL;
    *inBlob = NULL;

    columnsJson = json_array();
    BailIfNull(columnsJson);

    for (int ii = 0; ii < exportInput->meta.numColumns; ii++) {
        colJson = json_pack(
            "{"
            "s:s"  // columnName
            "s:s"  // columnHeader
            "}",
            "columnName",
            exportInput->meta.columns[ii].name,
            "headerAlias",
            exportInput->meta.columns[ii].headerAlias);
        BailIfNull(colJson);
        int ret = json_array_append_new(columnsJson, colJson);
        if (ret != 0) {
            status = StatusNoMem;
            goto CommonExit;
        }
        colJson = NULL;  // json_array_append_new steals the reference
    }

    json = json_pack(
        "{"
        "s:s"  // func
        "s:I"  // xdbId
        "s:o"  // columns
        "s:s"  // driverName
        "s:s"  // driverParams
        "}",
        "func",
        "launchDriver",
        "xdbId",
        tableId,
        "columns",
        columnsJson,
        "driverName",
        exportInput->meta.driverName,
        "driverParams",
        exportInput->meta.driverParams);
    if (json == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Unknown error json formatting export driver input");
        status = StatusInval;
        goto CommonExit;
    }
    columnsJson = NULL;  // json steals the reference

    *inBlob = json_dumps(json, 0);
    BailIfNull(*inBlob);

CommonExit:
    if (json != NULL) {
        json_decref(json);
        json = NULL;
    }
    if (columnsJson != NULL) {
        json_decref(columnsJson);
        columnsJson = NULL;
    }
    if (colJson != NULL) {
        json_decref(colJson);
        colJson = NULL;
    }
    if (status != StatusOk) {
        if (*inBlob) {
            memFree(*inBlob);
            *inBlob = NULL;
        }
    }
    return status;
}
