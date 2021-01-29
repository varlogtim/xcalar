// Copyright 2015, 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// logMsg.cpp: sets the logging message level for the cluster monitor
// XXX This is only temporary until all the log messages the monitor creates
//     are switched to using xSyslog for consistency with the rest of Xcalar.

// Print all messages received by the LOG macro
int logLevel = 1;
