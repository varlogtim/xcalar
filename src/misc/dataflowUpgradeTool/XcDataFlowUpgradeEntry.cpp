#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "config/Config.h"
#include "common/InitTeardown.h"
#include "log/Log.h"
#include "queryparser/QueryParser.h"
#include "dag/DagLib.h"
#include "common/Version.h"
#include "LibDagConstants.h"
#include "runtime/Runtime.h"
#include "Primitives.h"
#include "util/MemTrack.h"

#include "XcDataFlowUpgradeEntry.h"

static void
printUpgradeUsage(void)
{
    fprintf(stdout,
            "Usage: upgradeTool --cfg <absolute path to config file>"
            " --dataflowInputFile <absolute path to json format dataflow file>"
            " --dataflowOutputFile <absolute path to json format dataflow file>"
            " [--verbose]");
}

static Status
parseUpgradeOptions(int argc, char *argv[], UpgradeToolOptions *options)
{
    int optionIndex = 0;
    int flag;
    bool cfgSet = false;
    bool dataflowInputPathset = false;
    bool dataflowOutputPathset = false;

    struct option longOptions[] = {
        {"cfg", required_argument, 0, 'C'},
        {"verbose", no_argument, 0, 'V'},
        {"dataflowInputFile", required_argument, 0, 'I'},
        {"dataflowOutputFile", required_argument, 0, 'O'},
        {0, 0, 0, 0},
    };

    options->cfgFile[0] = '\0';
    options->dataflowInputFile[0] = '\0';
    options->dataflowOutputFile[0] = '\0';
    options->verbose = false;

    while (
        (flag =
             getopt_long(argc, argv, "C:V:I:O", longOptions, &optionIndex)) !=
        -1) {
        switch (flag) {
        case 'C':
            strlcpy(options->cfgFile, optarg, sizeof(options->cfgFile));
            cfgSet = true;
            break;
        case 'I':
            strlcpy(options->dataflowInputFile,
                    optarg,
                    sizeof(options->dataflowInputFile));
            dataflowInputPathset = true;
            break;
        case 'O':
            strlcpy(options->dataflowOutputFile,
                    optarg,
                    sizeof(options->dataflowOutputFile));
            dataflowOutputPathset = true;
            break;
        case 'V':
            options->verbose = true;
            break;
        default:
            fprintf(stderr, "Unknown arg %c\n", flag);
            break;
        }
    }

    if (!dataflowInputPathset || !dataflowOutputPathset || !cfgSet) {
        printUpgradeUsage();
        return StatusInval;
    }

    return StatusOk;
}

static Status
writeBufferToFile(const char *pathToFile, char *buffer, uint64_t bufferLength)
{
    Status status = StatusOk;
    FILE *fp = NULL;
    size_t numBytesWritten = 0;

    fp = fopen(pathToFile, "w");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    numBytesWritten = fwrite(buffer, bufferLength, 1, fp);
    if (numBytesWritten != bufferLength) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if (fflush(fp) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

CommonExit:

    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    return status;
}

// XXX:FIXME: dedupe this with retina's code
static Status
convertToJsonAndWriteToFile(const char *pathToFile, json_t **queryInJson)
{
    Status status = StatusOk;
    int ret;
    json_t *jsonXcalarVersion = NULL;
    json_t *jsonRetinaVersion = NULL;
    char *jsonStr = NULL;

    json_t *retinaInfoJson = NULL;

    retinaInfoJson = json_object();
    if (retinaInfoJson == NULL) {
        status = StatusNoMem;
        fprintf(stderr, "Could not create dataflowInfo");
        goto CommonExit;
    }

    json_object_set_new(retinaInfoJson, "query", *queryInJson);
    *queryInJson = NULL;

    jsonXcalarVersion = json_string(versionGetStr());
    if (jsonXcalarVersion == NULL) {
        status = StatusInval;
        fprintf(stderr,
                " Error exporting, could not convert version"
                "string \"%s\" to json string",
                versionGetStr());
        goto CommonExit;
    }

    ret = json_object_set(retinaInfoJson, "xcalarVersion", jsonXcalarVersion);
    if (ret != 0) {
        status = StatusInval;
        fprintf(stderr, "Error exporting xcalarVersion attribute");
        goto CommonExit;
    }
    json_decref(jsonXcalarVersion);
    jsonXcalarVersion = NULL;

    jsonRetinaVersion = json_integer(dagLib::RetinaVersion);
    if (jsonRetinaVersion == NULL) {
        status = StatusInval;
        fprintf(stderr,
                "Could not convert Dataflow version: %d"
                "to json integer",
                dagLib::RetinaVersion);
        goto CommonExit;
    }

    ret = json_object_set(retinaInfoJson, "dataflowVersion", jsonRetinaVersion);
    if (ret != 0) {
        status = StatusInval;
        fprintf(stderr,
                "failed to set dataflowVersion: %d attribute",
                (int) dagLib::RetinaVersion);
        goto CommonExit;
    }
    json_decref(jsonRetinaVersion);
    jsonRetinaVersion = NULL;

    jsonStr = json_dumps(retinaInfoJson, (JSON_INDENT(4) | JSON_ENSURE_ASCII));
    if (jsonStr == NULL) {
        status = StatusInval;
        fprintf(stderr, "Could not convert json_t to string");
        goto CommonExit;
    }

    status = writeBufferToFile(pathToFile, jsonStr, strlen(jsonStr));
    if (status != StatusOk) {
        fprintf(stderr, "failed to write to %s", pathToFile);
        goto CommonExit;
    }

CommonExit:

    if (retinaInfoJson != NULL) {
        json_decref(retinaInfoJson);
        retinaInfoJson = NULL;
    }

    if (jsonStr != NULL) {
        memFreeJson(jsonStr);
        jsonStr = NULL;
    }

    if (jsonRetinaVersion != NULL) {
        assert(status != StatusOk);
        json_decref(jsonRetinaVersion);
        jsonRetinaVersion = NULL;
    }

    if (jsonXcalarVersion != NULL) {
        assert(status != StatusOk);
        json_decref(jsonXcalarVersion);
        jsonXcalarVersion = NULL;
    }

    return status;
}

static Status
getFileContents(const char *pathToFile,
                char **contentsOut,
                uint64_t *contentLengthOut)
{
    Status status = StatusOk;
    FILE *fp = NULL;
    size_t contentLength = 0;
    char *content = NULL;

    *contentLengthOut = 0;
    assert(*contentsOut == NULL);
    *contentsOut = NULL;

    // @SymbolCheckIgnore
    fp = fopen(pathToFile, "rb");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if (fseek(fp, 0L, SEEK_END) == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    contentLength = ftell(fp);

    if (fseek(fp, 0L, SEEK_SET)) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    content = (char *) memAlloc(contentLength + 1);
    if (content == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    if (fread(content, 1, contentLength, fp) != contentLength) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    content[contentLength] = '\0';

    *contentLengthOut = contentLength;
    *contentsOut = content;

CommonExit:

    if (status != StatusOk) {
        assert(*contentLengthOut == 0);
        assert(*contentsOut == NULL);
        if (content != NULL) {
            memFree(content);
            content = NULL;
        }
    }

    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    return status;
}

static void *
parseQueryFnCb(void *args)
{
    Status status = StatusOk;
    UpgradeToolArgs *upgradeArgs = (UpgradeToolArgs *) args;

    Dag *queryGraph = NULL;
    uint64_t numQueryGraphNodes;
    QueryParser *qp = QueryParser::get();
    DagLib *daglib = DagLib::get();

    char *query = NULL;
    uint64_t queryLength = 0;

    json_t *queryInJson = NULL;

    status = getFileContents(upgradeArgs->options->dataflowInputFile,
                             &query,
                             &queryLength);
    if (status != StatusOk) {
        fprintf(stderr,
                "Unable to get contents from %s\n",
                upgradeArgs->options->dataflowInputFile);
        goto CommonExit;
    }

    status = qp->parse(query, NULL, &queryGraph, &numQueryGraphNodes);
    if (status != StatusOk) {
        fprintf(stderr, "Unable to parse the query '%s'\n", query);
        goto CommonExit;
    }
    assert(queryGraph != NULL);

    status = qp->reverseParse(queryGraph, &queryInJson);
    if (status != StatusOk) {
        fprintf(stderr, "Unable to reverse parse the query\n");
        goto CommonExit;
    }

    status =
        convertToJsonAndWriteToFile(upgradeArgs->options->dataflowOutputFile,
                                    &queryInJson);
    if (status != StatusOk) {
        fprintf(stderr,
                "Unable to write converted query to %s\n",
                upgradeArgs->options->dataflowOutputFile);
        goto CommonExit;
    }

CommonExit:

    if (queryGraph != NULL) {
        Status status2 =
            daglib->destroyDag(queryGraph,
                               DagTypes::DestroyDeleteAndCleanNodes);
        assert(status2 == StatusOk);
        queryGraph = NULL;
    }

    if (queryInJson != NULL) {
        memFreeJson(queryInJson);
        queryInJson = NULL;
    }

    if (query != NULL) {
        memFree(query);
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error %s\n", strGetFromStatus(status));
    }

    upgradeArgs->status = status;

    return NULL;
}

static Status
parseQuery(UpgradeToolOptions *options)
{
    Status status = StatusOk;
    pthread_t threadHandle;
    UpgradeToolArgs upgradeArgs;
    upgradeArgs.options = options;

    status = Runtime::get()->createBlockableThread(&threadHandle,
                                                   NULL,
                                                   parseQueryFnCb,
                                                   &upgradeArgs);
    if (status != StatusOk) {
        fprintf(stderr, "Unable to create thread");
        goto CommonExit;
    }

    sysThreadJoin(threadHandle, NULL);
    status = upgradeArgs.status;

CommonExit:

    return status;
}

int
main(int argc, char *argv[])
{
    Status status = StatusOk;
    UpgradeToolOptions upgradeOptions;

    status = parseUpgradeOptions(argc, argv, &upgradeOptions);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityUsrNode,
                                (char *) upgradeOptions.cfgFile,
                                NULL,
                                NULL,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = parseQuery(&upgradeOptions);
    BailIfFailed(status);

CommonExit:

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error %s\n", strGetFromStatus(status));
    }

    return status.code();
}
