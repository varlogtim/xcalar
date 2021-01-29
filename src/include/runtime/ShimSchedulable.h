// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SHIMSCHEDOBJECT_H
#define SHIMSCHEDOBJECT_H

#include "primitives/Primitives.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "sys/XLog.h"

//
// DON'T USE THIS unless you really know what you're doing. This is easy to
// use incorrectly.
//

template <typename ArgType, typename ReturnType, ReturnType (*Func)(ArgType)>
class ShimSchedulable : public Schedulable
{
  public:
    explicit ShimSchedulable(ArgType arg)
        : Schedulable("ShimSchedulable"), argVal_(arg)
    {
        verify(sem_init(&semDone_, 0, 0) == 0);
    }

    ~ShimSchedulable() override { verify(sem_destroy(&semDone_) == 0); }

    void run() override { returnVal_ = Func(argVal_); }

    void done() override { verify(sem_post(&semDone_) == 0); }

    ReturnType returnVal_;
    sem_t semDone_;

  private:
    ArgType argVal_;
};

template <typename ArgType, typename ReturnType, ReturnType (*Func)(ArgType)>
ReturnType
shimFunction(ArgType arg)
{
    ShimSchedulable<ArgType, ReturnType, Func> shim(arg);
    if (Runtime::get() == NULL) {
        return (ReturnType) StatusFailed;
    }
    Status status = Runtime::get()->schedule(&shim);
    if (status != StatusOk) {
        return (ReturnType) StatusFailed;
    }
    while (true) {
        errno = 0;
        int ret = sem_wait(&shim.semDone_);
        if (ret != 0) {
            Status status = sysErrnoToStatus(errno);
            xSyslog("ShimSchedulable",
                    (errno == EINTR) ? XlogDebug : XlogErr,
                    "sem_wait failed. ret: %d. errno: %d (%s)",
                    ret,
                    errno,
                    strGetFromStatus(status));
            if (errno == EINTR) {
                // Benign error, go back to sleep
                continue;
            }
            xcalarAbort();
        }
        break;
    }
    return shim.returnVal_;
}

#endif  // SHIMSCHEDOBJECT_H
