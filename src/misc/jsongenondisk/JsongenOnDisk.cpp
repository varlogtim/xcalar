// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <cstdlib>
#include <getopt.h>
#include <unistd.h>
#include "StrlFunc.h"
#include "util/System.h"
#include "primitives/Primitives.h"
#include "test/JsonGen.h"
#include "util/MemTrack.h"

enum {
    DefaultMaxField = 5,
    DefaultMaxLevel = 2,
    DefaultJsonKeyDupType = 2,
};

static const char *defaultSchemaFile = "schemaFile.cfg";
static uint64_t fileSizeThreshold = 1024 * MB;
static char dictFilePath[MaxPathSize];

static void
printHelp(void)
{
    printf("--outputDir (-o) <outputFileDirectory>\n");
    printf("[--dictionaryFile] (-x) <dictionaryFilePath>\n");
    printf("--numberOfRecord (-r) <totalNumberOfRecord>\n");
    printf("--fileSize (-z) <totalJsonFileSize>\n");
    printf("[--truncateSize] (-t) <sizeToTruncateInMB> (default 1GB)\n");
    printf("[--maxField] (-f) <maxNumberOfField> (default 5)\n");
    printf("--schemaFile (-s) <schemaFilePath>\n");
    printf("[--nestedLevel] (-l) <maxNestedLevel> (default 2)\n");
    printf("[--DuplicateKeyType] (-d) < 0 (Duplicated key) | "
           "1 (No duplicated key on the same level) | "
           "2 (default:No duplicated key)> \n");
    printf("[--randomString] (-g)\n");
    printf("[--mixPrimitiveInArray] (-p)\n");
    printf("--existingSchema (-c)\n");
    printf("[--mute] (-m)\n");
    printf("[--help] (-h)\n");
}

static Status
parseUnit(const char *optarg, int base, uint64_t *output) {
    uint64_t result;
    char *endPtr;
    Status status = StatusOk;
    const char *defaultUnits = "MB";
    bool calculatedUnits = false;

    result = strtoull(optarg, &endPtr, base);
    if (endPtr == optarg) {
        fprintf(stderr, "Error: [totalJsonFileSize] not an integer!\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    assert(endPtr != NULL);
    if (strlen(endPtr) == 0) {
        printf("No units specified. Using default of %s\n",
                defaultUnits);
        printf("To specify a unit, type %lu[Units] instead\n",
                result);
        printf("[Units] can be \"B\", \"KB\", \"MB\" or \"GB\"\n");
        endPtr = (char *)defaultUnits;
    }
    assert(strlen(endPtr) > 0);

    calculatedUnits = false;
    do {
        calculatedUnits = true;
        if (strcasecmp(endPtr, "KB") == 0) {
            result *= KB;
        } else if (strcasecmp(endPtr, "MB") == 0) {
            result *= MB;
        } else if (strcasecmp(endPtr, "GB") == 0) {
            result *= GB;
        } else if (strcasecmp(endPtr, "B") == 0 ||
                strcasecmp(endPtr, "bytes") == 0 ||
                strcasecmp(endPtr, "byte") == 0) {
            result *= 1;
        } else {
            printf("Unknown units. Using default of %s\n",
                    defaultUnits);
            printf("Possible units are \"B\", \"KB\", \"MB\" or "
                    "\"GB\"");
            endPtr = (char *)defaultUnits;
            calculatedUnits = false;
        }
    } while (!calculatedUnits);

    *output = result;

CommonExit:
    return status;
}
int
main(int argc, char *argv[])
{
    bool outputDirSet = false;
    bool byRecord = false;
    bool schemaFileSet = false;
    bool bySize = false;
    int flag;
    int optionIndex = 0;
    uint64_t maxField = DefaultMaxField;
    uint64_t maxNestedLevel = DefaultMaxLevel;
    uint64_t jsonGenKeyDupType = DefaultJsonKeyDupType;
    uint64_t numOfRecord = 0;
    uint64_t maxFileSize = 0;
    int fakeNodeId = 0;
    char schemaFile[MaxPathSize];
    bool usingExistingSchema = false;
    bool silenceMode = false;
    bool allowMixPrimitiveInArray = false;
    Status status;
    JsonGenHandle *jsonGenHandle = NULL;
    char outputFileDir[MaxPathSize];
    bool useDictForJsonGen = true;
    bool dictSet = false;

    status = memTrackInit();
    assert(status == StatusOk);

    static struct option long_options[] = {
        {"outputDir",             required_argument, 0, 'o'},
        {"randomString",          no_argument,       0, 'g'},
        {"dictionaryFile",        required_argument, 0, 'x'},
        {"numberOfRecord",        required_argument, 0, 'r'},
        {"fileSize",              required_argument, 0, 'z'},
        {"truncateSize",          required_argument, 0, 't'},
        {"maxField",              required_argument, 0, 'f'},
        {"schemaFile",            required_argument, 0, 's'},
        {"nestedLevel",           required_argument, 0, 'l'},
        {"DuplicateKeyType",      required_argument, 0, 'd'},
        {"mixPrimitiveInArray",   no_argument,       0, 'p'},
        {"existingSchema",        no_argument,       0, 'c'},
        {"mute",                  no_argument,       0, 'm'},
        {"help",                  no_argument,       0, 'h'},
        {0, 0, 0, 0},
    };

    // The ":" follows required args.
    while ((flag = getopt_long(argc, argv, "o:x:r:z:t:f:s:l:d:chmpg",
                               long_options, &optionIndex)) != -1) {
        switch (flag) {
        case 'o':
            snprintf(outputFileDir, sizeof(outputFileDir), "%s/", optarg);
            outputDirSet = true;
            break;

        case 'x':
            strlcpy(dictFilePath, optarg, sizeof(dictFilePath));
            dictSet = true;
            break;

        case 'r':
            numOfRecord = atoi(optarg);
            byRecord = true;
            break;

        case 't':
            // fileSizeThreshold = atoi(optarg) * MB;
            status = parseUnit(optarg, BaseCanonicalForm, &fileSizeThreshold);
            if (status != StatusOk) {
                printHelp();
                // @SymbolCheckIgnore
                exit(0);
            }
            break;

        case 'f':
            maxField = atoi(optarg);
            break;

        case 's':
            strlcpy(schemaFile, optarg, sizeof(schemaFile));
            schemaFileSet = true;
            break;

        case 'l':
            maxNestedLevel = atoi(optarg);
            break;

        case 'd':
            jsonGenKeyDupType = atoi(optarg);
            break;

         case 'c':
            usingExistingSchema = true;
            break;

         case 'p':
            allowMixPrimitiveInArray = true;
            break;

         case 'm':
            silenceMode = true;
            break;

         case 'g':
            useDictForJsonGen = false;
            break;

         case 'z':
            status = parseUnit(optarg, BaseCanonicalForm, &maxFileSize);
            if (status != StatusOk) {
                printHelp();
                // @SymbolCheckIgnore
                exit(0);
            }
            bySize = true;
            break;

         case 'h':
            printHelp();
            // @SymbolCheckIgnore
            exit(0);
            break;

         default:
            fprintf(stderr, "Unknown arg %c\n", flag);
            printHelp();
            // @SymbolCheckIgnore
            exit(1);
        }
    }

    if (!outputDirSet) {
        fprintf(stderr, "output file directory is missing \n");
        printHelp();
        // @SymbolCheckIgnore
        exit(1);
    }

    int ret = mkdir(outputFileDir, S_IRWXU | S_IRWXG | S_IRWXO);

    if (ret != 0) {
        if (EEXIST != errno) {
            fprintf(stderr, "Fail to create the output directory\n");
            printHelp();
            // @SymbolCheckIgnore
            exit(1);
        } else {
            printf("Use existing directory \n");
        }
    }

    if (!byRecord && !bySize) {
        fprintf(stderr, "Must specify the number of records or the max file"
                "size \n");
        printHelp();
        // @SymbolCheckIgnore
        exit(1);
    }

    if (!schemaFileSet) {
        if (usingExistingSchema) {
            printf("Please specify the schema file path or use -c flag to"
                    "generate schema file randomly\n");
            // @SymbolCheckIgnore
            exit(-1);
        }
        snprintf(schemaFile, sizeof(schemaFile), "%s%s", outputFileDir,
                 defaultSchemaFile);
    }

    if (!silenceMode) {
        if (usingExistingSchema) {
            printf("outputDir: %s, number of records: %lu, "
                    "maxFileSize: %lu, schema file: %s\n",
                    outputFileDir, numOfRecord, maxFileSize, schemaFile);
        } else {
            printf("outputDir: %s, number of records: %lu, "
                   "maxFileSize:%lu, schema file: %s\n, max field: %lu, "
                   "max nested level: %lu \n",
                   outputFileDir, numOfRecord, maxFileSize, schemaFile,
                   maxField, maxNestedLevel);
        }

        printf("Press [ENTER] to continue: ");
        int ret = getchar();
        assert(ret != EOF);
    }

    if (useDictForJsonGen) {
        if (dictSet) {
            jsonGenInitDict(dictFilePath);
        } else {
            jsonGenInitDict(NULL);
        }
    }

    if (usingExistingSchema) {
        status = jsonGenNewHandleWithSchema(schemaFile, &jsonGenHandle,
                                            fakeNodeId, useDictForJsonGen);
    } else {
        status = jsonGenNewHandleRand(schemaFile, maxField, maxNestedLevel,
                                      (JsonGenKeyDupType)jsonGenKeyDupType,
                                      allowMixPrimitiveInArray,
                                      &jsonGenHandle, fakeNodeId,
                                      useDictForJsonGen);
    }

    if (status != StatusOk) {
        fprintf(stderr, "failed to build the schema\n");
        // @SymbolCheckIgnore
        exit(-1);
    }

    // write recorde to file
    status = writeRecordToFile(jsonGenHandle, byRecord, numOfRecord, bySize,
                               maxFileSize, fileSizeThreshold, outputFileDir);
    jsonGenDestroyHandle(jsonGenHandle);
    memTrackDestroy(true);

    if (status != StatusOk) {
        fprintf(stderr, "failed to generate json files");
        // @SymbolCheckIgnore
        exit(-1);
    }
}
