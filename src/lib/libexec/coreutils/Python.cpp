// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>
#include <getopt.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "exec/ExecXcalarCmd.h"
#include "GetOpt.h"
#include "util/MemTrack.h"

static void uploadPythonHelp(int argc, char *argv[]);
static void uploadPythonMain(int argc,
                             char *argv[],
                             XcalarWorkItem *workItemIn,
                             bool prettyPrint,
                             bool interactive);
static void downloadPythonHelp(int argc, char *argv[]);
static void downloadPythonMain(int argc,
                               char *argv[],
                               XcalarWorkItem *workItemIn,
                               bool prettyPrint,
                               bool interactive);
static void listPythonHelp(int argc, char *argv[]);
static void listPythonMain(int argc,
                           char *argv[],
                           XcalarWorkItem *workItem,
                           bool prettyPrint,
                           bool interactive);

static const ExecCommandMapping commandMappings[] = {
    {
        .commandString = "upload",
        .commandDesc = "Upload a python file",
        .main = uploadPythonMain,
        .usage = uploadPythonHelp,
    },
    {
        .commandString = "download",
        .commandDesc = "Download a python file",
        .main = downloadPythonMain,
        .usage = downloadPythonHelp,
    },
    {
        .commandString = "list",
        .commandDesc = "List python functions registered on server",
        .main = listPythonMain,
        .usage = listPythonHelp,
    },
};

struct UploadPythonArgs {
    const char *localFile;
    const char *moduleName;
};

static Status
parseUploadPythonArgs(int argc,
                      char *argv[],
                      UploadPythonArgs *uploadPythonArgs)
{
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"localFile", required_argument, 0, 'f'},
        {"moduleName", required_argument, 0, 'm'},
        {0, 0, 0, 0},
    };
    GetOptDataThr optData;

    uploadPythonArgs->localFile = NULL;
    uploadPythonArgs->moduleName = NULL;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "f:m:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'f':
            uploadPythonArgs->localFile = optData.optarg;
            break;
        case 'm':
            uploadPythonArgs->moduleName = optData.optarg;
            break;
        default:
            return StatusCliParseError;
        }
    }

    if (uploadPythonArgs->localFile == NULL) {
        fprintf(stderr, "Error: localFile not specified!\n");
        return StatusCliParseError;
    }

    if (uploadPythonArgs->moduleName == NULL) {
        fprintf(stderr, "Error: moduleName not specified!\n");
        return StatusCliParseError;
    }

    return StatusOk;
}

static void
uploadPythonHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s %s --localFile <pathToPythonScript> "
        "--moduleName <nameOfPythonModule>\n",
        argv[0],
        argv[1]);
}

static void
uploadPythonMain(int argc,
                 char *argv[],
                 XcalarWorkItem *workItemIn,
                 bool prettyPrint,
                 bool interactive)
{
    Status status;
    UploadPythonArgs uploadPythonArgs;
    FILE *fp = NULL;
    size_t pythonSrcLen;
    char *pythonSrc = NULL;
    XcalarWorkItem *workItem = NULL;

    status = parseUploadPythonArgs(argc, argv, &uploadPythonArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    fp = fopen(uploadPythonArgs.localFile, "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    fseek(fp, 0L, SEEK_END);
    pythonSrcLen = ftell(fp);
    fseek(fp, 0L, SEEK_SET);

    if (pythonSrcLen > XcalarApiMaxUdfSourceLen) {
        status = StatusOverflow;
        goto CommonExit;
    }

    pythonSrc = (char *) memAlloc(pythonSrcLen + 1);
    if (pythonSrc == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    if (fread(pythonSrc, 1, pythonSrcLen, fp) != pythonSrcLen) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    pythonSrc[pythonSrcLen] = '\0';

    workItem = xcalarApiMakeUdfAddUpdateWorkItem(XcalarApiUdfAdd,
                                                 UdfTypePython,
                                                 uploadPythonArgs.moduleName,
                                                 pythonSrc);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    workItem->legacyClient = true;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("%s uploaded successfully\n", uploadPythonArgs.localFile);
    } else if (workItem->output->hdr.status ==
               StatusUdfModuleAlreadyExists.code()) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;

        workItem =
            xcalarApiMakeUdfAddUpdateWorkItem(XcalarApiUdfUpdate,
                                              UdfTypePython,
                                              uploadPythonArgs.moduleName,
                                              pythonSrc);
        BailIfNull(workItem);
        workItem->legacyClient = true;

        status = xcalarApiQueueWork(workItem,
                                    cliDestIp,
                                    cliDestPort,
                                    cliUsername,
                                    cliUserIdUnique);
        BailIfFailed(status);

        if (workItem->output->hdr.status == StatusOk.code()) {
            printf("%s uploaded successfully\n", uploadPythonArgs.localFile);
        }

        status.fromStatusCode(workItem->output->hdr.status);
    }

    if (status != StatusOk) {
        fprintf(stderr,
                "Error: %s\n",
                strGetFromStatusCode(workItem->output->hdr.status));

        XcalarApiUdfAddUpdateOutput *addOutput =
            &workItem->output->outputResult.udfAddUpdateOutput;
        if (addOutput->error.messageSize > 0) {
            fprintf(stderr, "%s\n", addOutput->error.data);
        }
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }

    if (pythonSrc != NULL) {
        memFree(pythonSrc);
        pythonSrc = NULL;
    }

    if (status == StatusCliParseError) {
        uploadPythonHelp(argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

static void
downloadPythonHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s %s --localFile <pathToPythonScript> "
        "--moduleName <nameOfPythonModule>\n",
        argv[0],
        argv[1]);
}

static void
downloadPythonMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive)
{
    UploadPythonArgs downloadPythonArgs;

    Status status;
    FILE *fp = NULL;
    XcalarWorkItem *workItem = NULL;
    UdfModuleSrc *downloadPythonOutput = NULL;
    int order = 0;
    double humanReadableSize;

    status = parseUploadPythonArgs(argc, argv, &downloadPythonArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    fp = fopen(downloadPythonArgs.localFile, "w");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    workItem = xcalarApiMakeUdfGetWorkItem(downloadPythonArgs.moduleName);
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

    downloadPythonOutput = &workItem->output->outputResult.udfGetOutput;
    // @SymbolCheckIgnore
    if (fwrite(downloadPythonOutput->source,
               1,
               downloadPythonOutput->sourceSize - 1,
               fp) != (downloadPythonOutput->sourceSize - 1)) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    humanReadableSize = (double) (downloadPythonOutput->sourceSize - 1);
    while (humanReadableSize >= 1024.0 && order < 4) {
        humanReadableSize /= 1024.0;
        order++;
    }

    const char *unit;
    switch (order) {
    case 0:
        unit = "bytes";
        break;
    case 1:
        unit = "KB";
        break;
    case 2:
        unit = "MB";
        break;
    case 3:
        unit = "GB";
        break;
    case 4:
        unit = "TB";
        break;
    }
    printf("Python module \"%s\" successfully downloaded to %s (%.2lf %s)\n",
           downloadPythonArgs.moduleName,
           downloadPythonArgs.localFile,
           humanReadableSize,
           unit);

CommonExit:
    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        downloadPythonOutput = NULL;
    }

    if (status == StatusCliParseError) {
        downloadPythonHelp(argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

struct ListPythonArgs {
    const char *fnNamePattern;
};

static Status
parseListPythonArgs(int argc, char *argv[], ListPythonArgs *listPythonArgs)
{
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"fnNamePattern", required_argument, 0, 'f'},
        {0, 0, 0, 0},
    };
    GetOptDataThr optData;

    listPythonArgs->fnNamePattern = "*";

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "f:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'f':
            listPythonArgs->fnNamePattern = optData.optarg;
            break;
        default:
            return StatusCliParseError;
        }
    }

    return StatusOk;
}

static void
listPythonHelp(int argc, char *argv[])
{
    printf("Usage: %s %s [--fnNamePattern <fnNamepattern>]\n",
           argv[0],
           argv[1]);
}

static void
listPythonMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiListXdfsOutput *listXdfsOutput = NULL;
    unsigned ii;
    char *savePtr;
    ListPythonArgs listPythonArgs;

    status = parseListPythonArgs(argc, argv, &listPythonArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeListXdfsWorkItem(listPythonArgs.fnNamePattern,
                                             strGetFromFunctionCategory(
                                                 FunctionCategoryUdf));
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

    listXdfsOutput = &workItem->output->outputResult.listXdfsOutput;

    if (prettyPrint) {
        printf("%-50s%-20s\n", "Python module", "Function name");
        printf("=======================================================\n");
        for (ii = 0; ii < listXdfsOutput->numXdfs; ii++) {
            char *moduleName =
                strtok_r(listXdfsOutput->fnDescs[ii].fnName, ":", &savePtr);
            char *fnName = strtok_r(NULL, ":", &savePtr);
            printf("%-50s%-20s\n", moduleName, fnName);
        }
        printf("=======================================================\n");
    } else {
        printf("Num UDFs: %u\n", listXdfsOutput->numXdfs);
        for (ii = 0; ii < listXdfsOutput->numXdfs; ii++) {
            char *moduleName =
                strtok_r(listXdfsOutput->fnDescs[ii].fnName, ":", &savePtr);
            char *fnName = strtok_r(NULL, ":", &savePtr);
            printf("\"%s\"\t\"%s\"\n", moduleName, fnName);
        }
    }

    status = StatusOk;

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status == StatusCliParseError) {
        listPythonHelp(argc, argv);
    } else if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

void
cliPythonHelp(int argc, char *argv[])
{
    unsigned ii;
    if (argc > 1) {
        for (ii = 0; ii < ArrayLen(commandMappings); ii++) {
            if (strcmp(argv[1], commandMappings[ii].commandString) == 0) {
                if (commandMappings[ii].usage != NULL) {
                    commandMappings[ii].usage(argc, argv);
                    return;
                } else {
                    printf("No help found for %s %s\n", argv[0], argv[1]);
                    goto CommonErrorExit;
                }
            }
        }

        printf("Unknown <action> %s\n", argv[1]);
    }

CommonErrorExit:
    printf("Usage: %s <action>\nPossible <action> are:\n", argv[0]);
    for (ii = 0; ii < ArrayLen(commandMappings); ii++) {
        printf("  %-12s - %s\n",
               commandMappings[ii].commandString,
               commandMappings[ii].commandDesc);
    }
}

void
cliPythonMain(int argc,
              char *argv[],
              XcalarWorkItem *workItemIn,
              bool prettyPrint,
              bool interactive)
{
    unsigned ii;

    if (argc > 1) {
        for (ii = 0; ii < ArrayLen(commandMappings); ii++) {
            if (strcmp(argv[1], commandMappings[ii].commandString) == 0) {
                assert(commandMappings[ii].main != NULL);
                commandMappings[ii].main(argc,
                                         argv,
                                         workItemIn,
                                         prettyPrint,
                                         interactive);
                return;
            }
        }

        fprintf(stderr, "Unknown <action> %s\n", argv[1]);
    }
    cliPythonHelp(1, argv);
}
