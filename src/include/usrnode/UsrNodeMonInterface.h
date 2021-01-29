// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// Header file for Xcalar syslog functions

#ifndef _USRNODEMON_H_
#define _USRNODEMON_H_

// Initialize and terminate usrnode <=> monitor interface
extern Status usrNodeMonitorNew();
extern void usrNodeMonitorDestroy();

// Other functions (TBD)

#endif  // _USRNODEMON_H_
