// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>
#include <stdio.h>
#include <errno.h>

#include "primitives/Primitives.h"
#include "util/System.h"
#include "util/Archive.h"
#include "util/MemTrack.h"
#include "stat/Statistics.h"
#include "dag/DagLib.h"
#include "dag/RetinaTypes.h"
#include "udf/UdfTypes.h"
#include "bc/BufferCache.h"

static void
displayRetinaInfo(RetinaInfo *retinaInfo)
{
    uint64_t ii;
    int jj;
    assert(retinaInfo != NULL);

    printf("Num tables: %lu\n\n", retinaInfo->numTables);
    for (ii = 0; ii < retinaInfo->numTables; ii++) {
        printf("Table %s\n", retinaInfo->tableArray[ii]->target.name);
        printf("Num Columns: %d\n", retinaInfo->tableArray[ii]->numColumns);
        for (jj = 0; jj < retinaInfo->tableArray[ii]->numColumns; jj++) {
            printf("\tColumn %d: %s (aka %s)\n", jj + 1,
                   retinaInfo->tableArray[ii]->columns[jj].name,
                   retinaInfo->tableArray[ii]->columns[jj].headerAlias);
        }
    }
    printf("\n\n");
    printf("Num UDF Modules: %lu\n\n", retinaInfo->numUdfModules);
    for (ii = 0; ii < retinaInfo->numUdfModules; ii++) {
        printf("Udf %s\n", retinaInfo->udfModulesArray[ii]->moduleName);
        printf("Udf Type: %s\n", strGetFromUdfType(
                                        retinaInfo->udfModulesArray[ii]->type));
        printf("Udf source len: %lu\n",
               retinaInfo->udfModulesArray[ii]->sourceSize);
        printf("\n=========================================================\n");
        printf("%s", retinaInfo->udfModulesArray[ii]->source);
        printf("\n=========================================================\n");
    }
}

static Status
loadRetina(char *retinaPath)
{
    Status status = StatusUnknown;
    FILE *fp = NULL;
    void *fileContents = NULL;
    long ret;
    size_t fileSize;
    ArchiveManifest *manifest = NULL;
    RetinaInfo *retinaInfo = NULL;
    DagLib *dagLib = DagLib::get();

    // @SymbolCheckIgnore
    fp = fopen(retinaPath, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Could not open file\n");
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    fseek(fp, 0L, SEEK_END);
    ret = ftell(fp);
    if (ret < 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    fileSize = ret;

    fseek(fp, 0L, SEEK_SET);

    fileContents = memAlloc(fileSize);
    if (fileContents == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    if (fread(fileContents, 1, fileSize, fp) != fileSize) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status = dagLib->parseRetinaFile(fileContents, fileSize, "testRetina",
                               &retinaInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }

    displayRetinaInfo(retinaInfo);
CommonExit:
    if (retinaInfo != NULL) {
        dagLib->destroyRetinaInfo(retinaInfo);
        retinaInfo = NULL;
    }

    if (manifest != NULL) {
        archiveFreeManifest(manifest);
        manifest = NULL;
    }

    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    if (fileContents != NULL) {
        memFree(fileContents);
        fileContents = NULL;
    }

    return status;
}

int
main(int argc, char *argv[])
{
    Status status = StatusUnknown;
    bool memTrackInited = false;
    bool bcCreated = false;
    bool statInited = false;
    bool archiveInited = false;
    Config *config = NULL;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <retinaSample.tar.gz>\n", argv[0]);
        status = StatusOk;
        goto CommonExit;
    }

    status = memTrackInit();
    if (status != StatusOk) {
        goto CommonExit;
    }
    memTrackInited = true;

    status = Config::init();
    if (status != StatusOk) {
        goto CommonExit;
    }
    config = Config::get();

    config->setMyNodeId(0);
    status = config->setNumActiveNodes(1);
    if (status != StatusOk) {
        goto CommonExit;
    }
    config->setNumActiveNodesOnMyPhysicalNode(1);

    status = BufferCacheMgr::get()->init(BufferCacheMgr::TypeNone, 0, 1);
    if (status != StatusOk) {
        goto CommonExit;
    }
    bcCreated = true;

    status = StatsLib::createSingleton();
    if (status != StatusOk) {
        goto CommonExit;
    }
    statInited = true;

    status = archiveInit();
    if (status != StatusOk) {
        goto CommonExit;
    }
    archiveInited = true;

    status = loadRetina(argv[1]);

CommonExit:
    if (archiveInited) {
        archiveDestroy();
        archiveInited = false;
    }

    if (statInited) {
        StatsLib::deleteSingleton();
        statInited = false;
    }

    if (bcCreated) {
        BufferCacheMgr::get()->destroy();
        bcCreated = false;
    }

    if (Config::get()) {
        Config::get()->destroy();
    }

    if (memTrackInited) {
        memTrackDestroy(true);
        memTrackInited = false;
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
        return 1;
    }

    return 0;
}
