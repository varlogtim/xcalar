// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "msg/TwoPcFuncDefs.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "msg/Message.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libusrnodestubs";

// these variables are used to ensure that if we write a log message that the
// test routines were invoked in error, each message is only issused once
static bool loggedLclWorkFn = false;
static bool loggedLclCompFn = false;
static bool loggedRcvCompFn = false;

//
// Functions to receive the various 2PC calls that are primarily intended
// for test.  These functions will assert in the debug build and will
// issue a message (only once) in the production build.  LibMsg unit tests
// will replace these functions with its own.
//
// Immediate work
void usrNodeMsg2pcImmDummyTestFn(MsgEphemeral *eph, void *payload);
// Local work
void usrNodeMsg2pcRecvDummyTestLocalFn(MsgEphemeral *eph, void *payload);
// Local work complete
void usrNodeMsg2pcRecvDummyTestLocalFnComplete(MsgEphemeral *eph,
                                               void *payload);
// Remote work complete
void usrNodeMsg2pcRecvDummyTestFnDataComplete(MsgEphemeral *eph, void *payload);

//
// These test functions should never be called in actual code.  They exist
// to reserve slots in the various 2PC function tables that can be used by
// LibMsgTest in whatever way it wants to.  Assert if called in the debug
// build, log once if called by the prod build and return harmlessly.
void
TwoPcMsg2pcTestImmed1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    usrNodeMsg2pcRecvDummyTestLocalFn(ephemeral, payload);
}

void
TwoPcMsg2pcTestImmed1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    usrNodeMsg2pcRecvDummyTestLocalFnComplete(ephemeral, payload);
}

void
TwoPcMsg2pcTestImmed1::recvDataCompletion(MsgEphemeral *ephemeral,
                                          void *payload)
{
    usrNodeMsg2pcRecvDummyTestFnDataComplete(ephemeral, payload);
}

void
TwoPcMsg2pcTestRecv1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    usrNodeMsg2pcRecvDummyTestLocalFn(ephemeral, payload);
}

void
TwoPcMsg2pcTestRecv1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                           void *payload)
{
    usrNodeMsg2pcRecvDummyTestLocalFnComplete(ephemeral, payload);
}

void
TwoPcMsg2pcTestRecv1::recvDataCompletion(MsgEphemeral *ephemeral, void *payload)
{
    usrNodeMsg2pcRecvDummyTestFnDataComplete(ephemeral, payload);
}

void
usrNodeMsg2pcRecvDummyTestLocalFn(MsgEphemeral *eph, void *payload)
{
    assert(0);
    if (!loggedLclWorkFn) {
        xSyslog(moduleName,
                XlogInfo,
                "Unexpected function entry - test 2pc local work");
        loggedLclWorkFn = true;
    }
}

void
usrNodeMsg2pcRecvDummyTestLocalFnComplete(MsgEphemeral *eph, void *payload)
{
    assert(0);
    if (!loggedLclCompFn) {
        xSyslog(moduleName,
                XlogInfo,
                "Unexpected function entry - test 2pc local complete");
        loggedLclCompFn = true;
    }
}

void
usrNodeMsg2pcRecvDummyTestFnDataComplete(MsgEphemeral *eph, void *payload)
{
    assert(0);
    if (!loggedRcvCompFn) {
        xSyslog(moduleName,
                XlogInfo,
                "Unexpected function entry - test 2pc recv data complete");
        loggedRcvCompFn = true;
    }
}
