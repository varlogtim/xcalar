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

void
cliLogLevelSetHelp(int argc, char *argv[])
{
    printf(
        "\nUsage: %s Emerg|Alert|Crit|Err|Warn|Note|Info|Debug|NoChange\n"
        "\t[ FlushGlobal [ NUMBER ] | FlushLocal ]\n\n",
        argv[0]);
    printf("FlushGlobal: run on any ONE node to flush logs from all nodes\n");
    printf("NUMBER: >=%d secs flush period OR -1 to turn off periodic flush\n",
           xsyslogGetFlushPeriodMin());
    printf("FlushLocal: flush logs ONLY from node on which xccli is invoked\n");
}

void
cliLogLevelSetMain(int argc,
                   char *argv[],
                   XcalarWorkItem *workItemIn,
                   bool prettyPrint,
                   bool interactive)
{
    Status status;
    bool printUsage = true;
    uint32_t logLevel;
    uint32_t logFlushLevel = XlogFlushNone;
    int32_t logFlushPeriod = 0;
    ProtoRequestMsg requestMsg;
    ProtoResponseMsg responseMsg;
    ServiceSocket socket;
    google::protobuf::Empty empty;
    xcalar::compute::localtypes::log::SetLevelRequest req;

    //
    // In DEBUG, log isn't buffered so the Flush param isn't needed.  But
    // allow it to be specified for API simplicity - so callers like generate
    // support bundle can make a single API call without needing to know if the
    // installed version is debug or not. If Flush IS specified, it's not a
    // complete no-op, since the kernel buffers are fsync'ed (besides the
    // call to fflush(3)).
    //
    if (argc != 2 && argc != 3 && argc != 4) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    status = parseLogLevel(argv[1], &logLevel);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (argc == 3 || argc == 4) {
        if (strcasecmp(argv[2], "FlushGlobal") == 0) {
            logFlushLevel = XlogFlushGlobal;
            if (argc == 4) {
                if (strspn(argv[3], "0123456789-+") == strlen(argv[3])) {
                    logFlushPeriod = atoi(argv[3]);
                } else {
                    fprintf(stderr,
                            "Invalid arg (%s); FlushPeriod must be numeric\n",
                            argv[3]);
                    status = StatusCliParseError;
                    goto CommonExit;
                }
                if (logFlushPeriod != -1 && logFlushPeriod != 0 &&
                    logFlushPeriod < xsyslogGetFlushPeriodMin()) {
                    fprintf(stderr,
                            "Invalid arg (%d); FlushPeriod must be >= %d or "
                            "-1\n",
                            logFlushPeriod,
                            xsyslogGetFlushPeriodMin());
                    status = StatusInval;
                    goto CommonExit;
                }
            }
        } else if (strcasecmp(argv[2], "FlushLocal") == 0) {
            logFlushLevel = XlogFlushLocal;
            if (argc == 4) {
                fprintf(stderr,
                        "Invalid number of params: %s must be last param\n",
                        argv[2]);
                status = StatusCliParseError;
                goto CommonExit;
            }
        } else {
            fprintf(stderr, "Invalid params: %s\n", argv[2]);
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    printUsage = false;

    req.set_log_level(
        static_cast<::xcalar::compute::localtypes::XcalarEnumType::
                        XcalarSyslogMsgLevel>(logLevel));
    req.set_log_flush_level(
        static_cast<::xcalar::compute::localtypes::XcalarEnumType::
                        XcalarSyslogFlushLevel>(logFlushLevel));
    req.set_log_flush_period_sec(logFlushPeriod);

    requestMsg.set_requestid(0);
    requestMsg.set_target(ProtoMsgTargetService);
    requestMsg.mutable_servic()->set_servicename("Log");
    requestMsg.mutable_servic()->set_methodname("SetLevel");
    requestMsg.mutable_servic()->mutable_body()->PackFrom(req);

    status = socket.init();
    BailIfFailed(status);

    status = socket.sendRequest(&requestMsg, &responseMsg);
    BailIfFailed(status);

    status.fromStatusCode((StatusCode) responseMsg.status());

CommonExit:
    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
        if (status == StatusPerm &&
            (logFlushPeriod == -1 || logFlushPeriod > 0)) {
            fprintf(stderr,
                    "Log buffer flushing can't be modified: maybe"
                    " log buffering is not even on?\n");
        }
        if (printUsage) {
            cliLogLevelSetHelp(argc, argv);
        }
    }
}
