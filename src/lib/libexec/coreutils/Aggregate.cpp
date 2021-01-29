// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>
#include <jansson.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"

void
cliAggregateHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --srctable <tableName> --eval <\"evalStr\"> "
        "[--dsttable <aggregateTableName>]\n",
        argv[0]);

    if (argc < 2) {
        printf("Use help %s operator for list of possible operators\n",
               argv[0]);
        return;
    }

    if (strcmp(argv[1], "operator") == 0) {
        printf("Possible operators are 'max', 'min', 'avg', 'count', 'sum'\n");
    }
}

void
cliAggregateMain(int argc,
                 char *argv[],
                 XcalarWorkItem *workItemIn,
                 bool prettyPrint,
                 bool interactive)
{
    Status status;
    XcalarApiAggregateOutput *aggregateOutput = NULL;
    XcalarWorkItem *workItem = workItemIn;

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    assert(workItem->outputSize > 0);
    aggregateOutput = &workItem->output->outputResult.aggregateOutput;

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("\"%s\" successfully created\n", aggregateOutput->tableName);

        json_t *jsonRecord, *jsonValue;
        json_error_t jsonError;
        char *prefix = (char *) "The answer is:";

        jsonRecord = json_loads(aggregateOutput->jsonAnswer, 0, &jsonError);
        assert(jsonRecord != NULL);

        jsonValue = json_object_get(jsonRecord, "Value");
        assert(jsonValue != NULL);

        switch (json_typeof(jsonValue)) {
        case JSON_INTEGER:
            printf("%s %lld\n", prefix, json_integer_value(jsonValue));
            break;
        case JSON_REAL:
            printf("%s %lf\n", prefix, json_real_value(jsonValue));
            break;
        default:
            printf("Server returned: %s\n", aggregateOutput->jsonAnswer);
            break;
        }

        json_decref(jsonRecord);
        jsonRecord = jsonValue = NULL;

    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
