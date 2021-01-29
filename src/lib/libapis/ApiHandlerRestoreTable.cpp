// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerRestoreTable.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "xdb/HashTree.h"
#include "sys/XLog.h"
#include "log/Log.h"
#include "strings/String.h"

ApiHandlerRestoreTable::ApiHandlerRestoreTable(XcalarApis api) : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerRestoreTable::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph);
}

Status
ApiHandlerRestoreTable::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusOk;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    Stopwatch stopwatch;
    HashTreeMgr *htreeMgr = HashTreeMgr::get();
    HashTreeMgr::RestoreInfo *restoreInfo = NULL;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.restoreTableOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Publish table %s restore failed to allocate output "
                "(Required size: %lu bytes): %s",
                input_->name,
                outputSize,
                strGetFromStatus(status));
        outputSize = 0;
        goto CommonExit;
    }
    output->outputResult.restoreTableOutput.numTables = 0;

    xSyslog(moduleName,
            XlogInfo,
            "Publish table %s restore started by user %s",
            input_->name,
            userId_->userIdName);
    stopwatch.restart();

    status = htreeMgr->restorePublishedTable(input_->name,
                                             userId_,
                                             &restoreInfo,
                                             dstGraph_);

    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Publish table %s restore started by user %s finished in "
                "%lu:%02lu:%02lu.%03lu",
                input_->name,
                userId_->userIdName,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Publish table %s restore started by user %s failed in "
                "%lu:%02lu:%02lu.%03lu: %s",
                input_->name,
                userId_->userIdName,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
    }

    if (restoreInfo) {
        unsigned numTables = restoreInfo->dependencies.getSize();
        unsigned maxTables =
            ArrayLen(output->outputResult.restoreTableOutput.tableDependencies);
        numTables = xcMin(numTables, maxTables);
        output->outputResult.restoreTableOutput.numTables = numTables;

        unsigned ii = 0;
        HashTreeMgr::RestoreInfo::DependentTable *table;
        for (auto iter = restoreInfo->dependencies.begin();
             (table = iter.get()) != NULL;
             iter.next()) {
            if (ii == numTables) {
                break;
            }

            strlcpy(output->outputResult.restoreTableOutput
                        .tableDependencies[ii],
                    table->name,
                    sizeof(output->outputResult.restoreTableOutput
                               .tableDependencies[ii]));
            ii++;
        }
    }

CommonExit:
    if (restoreInfo) {
        delete restoreInfo;
    }

    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerRestoreTable::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &apiInput_->restoreTableInput;
    return StatusOk;
}
