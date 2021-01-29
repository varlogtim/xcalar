// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <stdio.h>
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "sys/XLog.h"
#include "primitives/Macros.h"
#include "xcalar/compute/localtypes/log.pb.h"

static const char *logLevelStrings[] = {"Emerg",
                                        "Alert",
                                        "Crit",
                                        "Err",
                                        "Warn",
                                        "Note",
                                        "Info",
                                        "Debug",
                                        "Verbose",
                                        "Inval"};

void
cliLogLevelGetHelp(int argc, char *argv[])
{
    printf("Usage: %s\n", argv[0]);
}

void
cliLogLevelGetMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive)
{
    Status status;
    bool printUsage = true;
    ProtoRequestMsg requestMsg;
    ProtoResponseMsg responseMsg;
    ServiceSocket socket;
    google::protobuf::Empty empty;
    xcalar::compute::localtypes::log::GetLevelResponse response;

    if (argc > 1) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    printUsage = false;

    requestMsg.set_requestid(0);
    requestMsg.set_target(ProtoMsgTargetService);
    requestMsg.mutable_servic()->set_servicename("Log");
    requestMsg.mutable_servic()->set_methodname("GetLevel");
    requestMsg.mutable_servic()->mutable_body()->PackFrom(empty);

    status = socket.init();
    BailIfFailed(status);

    status = socket.sendRequest(&requestMsg, &responseMsg);
    BailIfFailed(status);

    status.fromStatusCode((StatusCode) responseMsg.status());
    if (status != StatusOk) {
        printf("hdr status failed\n");
        goto CommonExit;
    }

    if (!responseMsg.servic().body().UnpackTo(&response)) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

    printf("%s\n", logLevelStrings[response.log_level()]);
    if (response.log_flush_period_sec() <= 0) {
        printf("Periodic log flushing is off\n");
    } else {
        printf("Logs are being flushed every %d secs\n",
               response.log_flush_period_sec());
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
        if (printUsage) {
            cliLogLevelGetHelp(argc, argv);
        }
    }
}
