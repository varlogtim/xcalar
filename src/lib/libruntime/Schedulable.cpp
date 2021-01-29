// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "FiberSchedThread.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"

Schedulable::Schedulable(const char *name) : name_(name)
{
    DCHECK(name_ != nullptr);
    DCHECK(strlen(name_) > 0);

    // Allocate and initialize the runtime stats fro this object. The STL
    // containers in this path with throw on OOM condition, so we can handle it
    // at here and below
    if (XcalarConfig::get()->runtimeStats_) {
        try {
            stats_ = std::make_shared<Stats>(name);
            DCHECK(stats_);
            stats_->start();
        } catch (std::exception &e) {
            xSyslog("libruntime",
                    XlogErr,
                    "Caught an exception while initializing stats: %s",
                    e.what());
        }
    }
}

Schedulable::~Schedulable()
{
    if (stats_ == nullptr) return;

    // Log the runtime threads with the owning thread.
    try {
        DCHECK(stats_->name() == name_);
        stats_->finish();

        // Wow, some of these don't have a thread (shimFunction).
        auto thread = FiberSchedThread::getRunningThread();
        if (thread == nullptr) return;

        thread->logFinish(stats_.get());
    } catch (std::exception &e) {
        xSyslog("libruntime",
                XlogErr,
                "Caught an exception while finishing stats: %s",
                e.what());
    }
}
