// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"

void
cliListHelp(int argc, char *argv[])
{
    printf("Usage: %s <object> [pattern]\n", argv[0]);
    printf(
        "Possible <object> are \"tables\" , \"datasets\" ,\"export\" and "
        "\"constant\"\n");
}

static Status
listTablesAndConstants(const char *tableNamePattern,
                       bool prettyPrint,
                       SourceType srcType)
{
    XcalarWorkItem *workItem;
    Status status = StatusUnknown;
    XcalarApiListDagNodesOutput *listNodesOutput = NULL;
    unsigned ii;

    workItem = xcalarApiMakeListDagNodesWorkItem(tableNamePattern, srcType);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(workItem != NULL);
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    if (workItem->output->hdr.status != StatusOk.code()) {
        status.fromStatusCode(workItem->output->hdr.status);
        goto CommonExit;
    }

    listNodesOutput = &workItem->output->outputResult.listNodesOutput;

    if (prettyPrint) {
        printf("Table ID                 Table Name              State\n");
        printf("=======================================================\n");

        if (listNodesOutput->numNodes == 0) {
            printf("No tables in system found to match \"%s\"\n",
                   tableNamePattern);
        } else {
            for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
                printf("%-25lu%-25s%-30s\n",
                       listNodesOutput->nodeInfo[ii].dagNodeId,
                       listNodesOutput->nodeInfo[ii].name,
                       strGetFromDgDagState(
                           listNodesOutput->nodeInfo[ii].state));
            }
        }
        printf("=======================================================\n");
    } else {
        printf("Number of tables: %llu\n",
               (unsigned long long) listNodesOutput->numNodes);
        printf("Table ID\tTable name\n");
        for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
            printf("%lu\t\"%s\"\t%s\n",
                   listNodesOutput->nodeInfo[ii].dagNodeId,
                   listNodesOutput->nodeInfo[ii].name,
                   strGetFromDgDagState(listNodesOutput->nodeInfo[ii].state));
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        listNodesOutput = NULL;
    }

    return status;
}

Status
listDatasets(bool prettyPrint, const char *datasetName)
{
    XcalarWorkItem *workItem;
    Status status = StatusUnknown;
    XcalarApiListDatasetsOutput *listDatasetsOutput = NULL;
    unsigned ii;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasets, NULL, 0);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (workItem->output->hdr.status != StatusOk.code()) {
        status.fromStatusCode(workItem->output->hdr.status);
        goto CommonExit;
    }

    listDatasetsOutput = &workItem->output->outputResult.listDatasetsOutput;

    if (prettyPrint) {
        printf("%-20s%-50s%-40s%-20s%-20s%-20s\n",
               "Dataset ID",
               "Dataset Name",
               "URL",
               "Format",
               "Status",
               "RefCount");
        printf(
            "======================================================="
            "========================================\n");

        if (listDatasetsOutput->numDatasets == 0) {
            printf("No datasets created in system yet\n");
            goto CommonExit;
        }

        for (ii = 0; ii < listDatasetsOutput->numDatasets; ii++) {
            if (datasetName != NULL &&
                strcmp(datasetName, listDatasetsOutput->datasets[ii].name) !=
                    0) {
                continue;
            }
            printf("%-20lu%-50s%-20s\n",
                   listDatasetsOutput->datasets[ii].datasetId,
                   listDatasetsOutput->datasets[ii].name,
                   (listDatasetsOutput->datasets[ii].loadIsComplete)
                       ? "Ready"
                       : "Loading");
        }

        printf(
            "======================================================="
            "========================================\n");
    } else {
        if (datasetName == NULL) {
            printf("Number of datasets: %u\n", listDatasetsOutput->numDatasets);
        }
        printf("ID\tname\turl\tFormat\tLoadStatus\tRefCount\n");
        for (ii = 0; ii < listDatasetsOutput->numDatasets; ii++) {
            if (datasetName != NULL &&
                strcmp(datasetName, listDatasetsOutput->datasets[ii].name) !=
                    0) {
                continue;
            }
            printf("%lu\t\"%s\"\t\"%s\"\n",
                   listDatasetsOutput->datasets[ii].datasetId,
                   listDatasetsOutput->datasets[ii].name,
                   (listDatasetsOutput->datasets[ii].loadIsComplete)
                       ? "Ready"
                       : "Loading");
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        listDatasetsOutput = NULL;
    }

    return status;
}

void
cliListMain(int argc,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    Status status = StatusUnknown;
    const char *defaultPattern = "*";

    if (argc < 2 || argc > 3) {
        status = StatusCliParseError;
        cliListHelp(argc, argv);
        goto CommonExit;
    }

    if (strcasecmp(argv[1], "datasets") == 0 ||
        strcasecmp(argv[1], "dataset") == 0) {
        status = listDatasets(prettyPrint, NULL);
    } else {
        SourceType srcType;
        if (strcasecmp(argv[1], "tables") == 0 ||
            strcasecmp(argv[1], "table") == 0) {
            srcType = SrcTable;
        } else if (strcasecmp(argv[1], "constant") == 0 ||
                   strcasecmp(argv[1], "constants") == 0) {
            srcType = SrcConstant;
        } else if (strcasecmp(argv[1], "export") == 0 ||
                   strcasecmp(argv[1], "exports") == 0) {
            srcType = SrcExport;
        } else {
            status = StatusCliParseError;
            goto CommonExit;
        }

        status = listTablesAndConstants((argc > 2) ? argv[2] : defaultPattern,
                                        prettyPrint,
                                        srcType);
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
