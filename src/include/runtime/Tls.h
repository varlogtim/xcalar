// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// ------------------------------------------------------------------------
//
// Thread Level Storage (TLS)
//
// Thread level storage should be avoided unless absolutely necessary because
// the Xcalar runtime is free to change the thread of execution for most
// executing work. Blockable threads, aka dedicated threads, are one exception.
// Even if running on a blockable thread, using TLS requires appropriate
// justification which should be included here.
//
// Expect to defend your decision to use TLS in front of a non-sympathetic
// audience, possibly multiple times.
// -----------------------------------------------------------------------

#ifndef _TLS_H_
#define _TLS_H_

#include "runtime/Txn.h"

// Transaction ID (TXN). For most threads, the TXN is kept as the thread
// schedulable cookie. However, getRunningSchedulable() returns null for
// blockable threads, making it unusable in that case. Since the TXN must be
// available at all times, using TLS is a reasonable alternative even though it
// forces all the TXN code to be dual pathed (normal runtime threads vs.
// blockable threads).

extern __thread Txn __txn;

#endif  // _TLS_H_
