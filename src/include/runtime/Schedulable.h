// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SCHEDULABLE_H
#define SCHEDULABLE_H

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "runtime/Runtime.h"
#include "runtime/Stats.h"
#include "runtime/Txn.h"

//
// Abstract class representing some schedulable chunk of work that can be run by
// the runtime.
//

class Schedulable
{
  public:
    explicit Schedulable(const char *name);
    virtual ~Schedulable();

    // Override this to do actual work.
    virtual void run() = 0;

    // Override this to do cleanup.
    virtual void done() = 0;

    // false by default. Override this if there's a guarantee your work
    // never blocks.
    virtual bool isNonBlocking() const { return false; }

    // false by default. Override this if there's a guarantee your work
    // should be done ASAP.
    virtual bool isPriority() const { return false; }

    // Allows storage and retrieval of Txn. Not thread safe.
    void setTxn(Txn txn) { txn_ = txn; }

    Txn txn() const { return txn_; }

    void markToAbort() { aborted_ = true; }

    MustCheck bool isMarkedToAbort() { return aborted_; }

    std::shared_ptr<Stats> stats() { return stats_; }

  private:
    std::shared_ptr<Stats> stats_;
    Txn txn_;
    const char *name_;
    bool aborted_ = false;
};

#endif  // SCHEDULABLE_H
