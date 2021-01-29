// Copyright 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "GetOpt.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "util/MemTrack.h"
#include "libapis/WorkItem.h"
#include "coreutils/CoreUtilsConstants.h"

using namespace cutil;

struct CreateDhtArgs {
    const char *dhtName;
    double upperBound;
    double lowerBound;
    Ordering ordering;
};

struct DeleteDhtArgs {
    size_t dhtNameLen;
    const char *dhtName;
};

struct ActionStringMapping {
    const char *string;
    DhtAction dhtAction;
};

static const ActionStringMapping actionStringMapping[] = {
    {"create", DhtCreate},
    {"delete", DhtDelete},
};

static Status
parseCreateDhtArgs(int argc, char *argv[], CreateDhtArgs *createDhtArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    char *endPtr;
    OptionThr longOptions[] = {
        {"dhtName", required_argument, 0, 'n'},
        {"upperBound", required_argument, 0, 'u'},
        {"lowerBound", required_argument, 0, 'l'},
        {"sorted", optional_argument, 0, 'o'},
        {0, 0, 0, 0},
    };

    assert(createDhtArgs != NULL);

    // The upper bound and lower bounds are set to - and + INT64_MAX
    // respectively to ensure that valid values are entered for both
    createDhtArgs->dhtName = NULL;
    createDhtArgs->lowerBound = (float64_t) INT64_MAX;
    createDhtArgs->upperBound = (float64_t) -INT64_MAX;
    createDhtArgs->ordering = Unordered;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "n:u:l:o::",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'n':
            createDhtArgs->dhtName = optData.optarg;
            if (strlen(createDhtArgs->dhtName) == 0) {
                fprintf(stderr, "Dht Name cannot be empty\n");
                status = StatusCliParseError;
                goto CommonExit;
            }

            if (strlen(createDhtArgs->dhtName) > DhtMaxDhtNameLen) {
                fprintf(stderr,
                        "Dht Name is too long (exceeded %u characters)"
                        "\n",
                        DhtMaxDhtNameLen);
                status = StatusCliParseError;
                goto CommonExit;
            }

            break;

        case 'u':
            createDhtArgs->upperBound = strtod(optData.optarg, &endPtr);
            if (optData.optarg == endPtr) {
                fprintf(stderr,
                        "%s is not a valid <upperBound>",
                        optData.optarg);
                status = StatusCliParseError;
                goto CommonExit;
            }
            break;

        case 'l':
            createDhtArgs->lowerBound = strtod(optData.optarg, &endPtr);
            if (optData.optarg == endPtr) {
                fprintf(stderr,
                        "%s is not a valid <lowerBound>",
                        optData.optarg);
                status = StatusCliParseError;
                goto CommonExit;
            }
            break;

        case 'o':
            if (optData.optarg != NULL) {
                createDhtArgs->ordering = Descending;
            } else {
                createDhtArgs->ordering = Ascending;
            }
            break;

        // Unrecognized parameter
        case '?':
            status = StatusCliParseError;
            goto CommonExit;
            break;
        }
    }

    if (createDhtArgs->dhtName == NULL) {
        fprintf(stderr, "--dhtName <dhtName> requried\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (createDhtArgs->upperBound < createDhtArgs->lowerBound) {
        fprintf(stderr, "upperBound is smaller than lower bound\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}

// Check that the required parameters are passed and are within bounds
// syntax: dht delete --dhtName <DHT name>

static Status
parseDeleteDht(int argc, char *argv[], DeleteDhtArgs *deleteDhtArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;

    OptionThr longOptions[] = {
        {"dhtName", required_argument, 0, 'n'},
        {0, 0, 0, 0},
    };

    assert(deleteDhtArgs != NULL);
    deleteDhtArgs->dhtName = NULL;
    if (argc != 4) {
        status = StatusCliParseError;
        fprintf(stderr, "Unrecognzed or missing parameter(s)\n");
        goto CommonExit;
    }

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "n",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        // --dhtName
        case 'n':
            deleteDhtArgs->dhtName = optData.optarg;
            deleteDhtArgs->dhtNameLen = strlen(deleteDhtArgs->dhtName);
            if (deleteDhtArgs->dhtNameLen == 0 ||
                deleteDhtArgs->dhtNameLen > DhtMaxDhtNameLen) {
                status = StatusCliParseError;
                fprintf(stderr,
                        "DHT name must be between 1 and %u characters\n",
                        DhtMaxDhtNameLen);
                goto CommonExit;
            }
            break;

        // Unrecognized parameter
        case '?':
            status = StatusCliParseError;
            fprintf(stderr, "Unrecognized DHT delete parameter\n");
            fprintf(stderr, "--dhtName expected\n");
            goto CommonExit;
            break;
        }
    }

    status = StatusOk;

CommonExit:
    return status;
}

void
cliDhtHelp(int argc, char *argv[])
{
    printf("Usage:\n");
    printf(
        "  %s create --dhtName <DHT Name> --upperBound <Upper bound>\n"
        "             --lowerBound <Lower bound> "
        "             [--sorted | --sorted=desc]\n",
        argv[0]);
    printf("  %s delete --dhtName <DHT Name>\n", argv[0]);
}

void
cliDhtMain(int argc,
           char *argv[],
           XcalarWorkItem *workItemIn,
           bool prettyPrint,
           bool interactive)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    DhtAction dhtAction;
    unsigned ii;

    if (argc < 2) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    assert(argc >= 2);

    for (ii = 0; ii < ArrayLen(actionStringMapping); ii++) {
        if (strcmp(argv[1], actionStringMapping[ii].string) == 0) {
            dhtAction = actionStringMapping[ii].dhtAction;
            break;
        }
    }

    if (ii == ArrayLen(actionStringMapping)) {
        fprintf(stderr, "Unknown action %s\n", argv[1]);
        status = StatusCliParseError;
        goto CommonExit;
    }

    switch (dhtAction) {
    case DhtCreate: {
        CreateDhtArgs createDhtArgs;
        status = parseCreateDhtArgs(argc - 1, &argv[1], &createDhtArgs);
        if (status != StatusOk) {
            goto CommonExit;
        }

        workItem = xcalarApiMakeCreateDhtWorkItem(createDhtArgs.dhtName,
                                                  createDhtArgs.upperBound,
                                                  createDhtArgs.lowerBound,
                                                  createDhtArgs.ordering);
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
            fprintf(stderr,
                    "Create DHT failed. Status: %s\n",
                    strGetFromStatusCode(workItem->output->hdr.status));
        } else {
            printf("Create DHT succeeded\n");
        }

        break;
    }

    case DhtDelete: {
        DeleteDhtArgs deleteDhtArgs = {0, NULL};

        status = parseDeleteDht(argc, argv, &deleteDhtArgs);
        if (status != StatusOk) {
            goto CommonExit;
        }

        workItem = xcalarApiMakeDeleteDhtWorkItem(deleteDhtArgs.dhtName,
                                                  deleteDhtArgs.dhtNameLen);
        if (workItem == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        status = xcalarApiQueueWork(workItem,
                                    cliDestIp,
                                    cliDestPort,
                                    cliUsername,
                                    cliUserIdUnique);

        if (workItem->output->hdr.status != StatusOk.code()) {
            fprintf(stderr,
                    "Delete DHT failed.  Status: %s\n",
                    strGetFromStatusCode(workItem->output->hdr.status));
        } else {
            printf("Successfully deleted DHT: %s\n", deleteDhtArgs.dhtName);
        }

        break;
    }

    default: {
        // This should NEVER happen
        fprintf(stderr, "Fatal error processing dht command\n");
        assert(0);
        break;
    }
    }

    assert(status == StatusOk);
CommonExit:
    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
        if (status == StatusCliParseError) {
            cliDhtHelp(argc, argv);
        }
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
}
