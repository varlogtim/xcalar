// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "runtime/Txn.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "runtime/Tls.h"
#include "msg/Xid.h"

bool
Txn::valid() const
{
    if (id_ == InvalidId || rtType_ == RuntimeTypeInvalid ||
        mode_ == Mode::Invalid) {
        return false;
    }

    if (rtType_ == RuntimeTypeImmediate) {
        if (rtSchedId_ != Runtime::SchedId::Immediate) {
            // Immediates ride on Immediate Scheduler only
            return false;
        }
    } else {
        if (rtSchedId_ == Runtime::SchedId::MaxSched) {
            // Needs to be a valid Scheduler
            return false;
        }
    }

    return true;
}

Txn
Txn::currentTxn()
{
    Schedulable *runSched = Runtime::get()->getRunningSchedulable();
    if (runSched != NULL) {
        return runSched->txn();
    } else {
        return __txn;
    }
}

Txn
Txn::newTxn(Mode mode)
{
    return newTxn(mode, Runtime::SchedId::Sched0);
}

Txn
Txn::newTxn(Mode mode, Runtime::SchedId rtSchedId)
{
    Txn::Id newId = XidMgr::get()->xidGetNext();
    RuntimeType rtType = Runtime::get()->getType(rtSchedId);
    return Txn(newId, mode, rtSchedId, rtType);
}

Txn
Txn::newImmediateTxn()
{
    Txn::Id newId = XidMgr::get()->xidGetNext();
    return Txn(newId,
               Mode::NonLRQ,
               Runtime::SchedId::Immediate,
               RuntimeTypeImmediate);
}

void
Txn::setTxn(Txn txn)
{
    // For the indefinite future, NULL will be returned for a blockable
    // thread. Since we must have our context, thread level storage will
    // be used for blockable threads.
    Schedulable *runSched = Runtime::get()->getRunningSchedulable();
    if (runSched == NULL) {
        __txn = txn;
    } else {
        runSched->setTxn(txn);
    }
}

bool
Txn::immediate()
{
    if (rtType_ == RuntimeTypeImmediate && mode_ == Mode::NonLRQ &&
        rtSchedId_ == Runtime::SchedId::Immediate) {
        return true;
    } else {
        return false;
    }
}

bool
Txn::currentTxnImmediate()
{
    return currentTxn().immediate();
}
