// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef PYTHONAPP_H
#define PYTHONAPP_H

#include <pthread.h>
#include "primitives/Primitives.h"
#include "UdfPythonImpl.h"
#include "UdfPyFunction.h"

class ChildAppStartRequest;
class ProtoResponseMsg;

//
// XPU-only singleton that manages execution of Python2 Apps.
//

class PythonApp final
{
  public:
    PythonApp();
    ~PythonApp();

    void destroy();
    void startAsync(const ChildAppStartRequest &startArgs,
                    ProtoResponseMsg *response);

  private:
    static constexpr const char *ModuleName = "libapp";
    static constexpr uint64_t TimeoutUSecsDestroy = USecsPerSec;

    void *run();
    void done();

    // Disallow.
    PythonApp(const PythonApp &) = delete;
    PythonApp &operator=(const PythonApp &) = delete;

    // Things specific to Python host type. Applies to currently running App.
    UdfPyFunction *fn_;
    pthread_t thread_;
    char *inStr_;
    char *appName_;

    mutable Mutex lock_;
};

#endif  // PYTHONAPP_H
