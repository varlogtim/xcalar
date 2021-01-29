// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPHOST_H
#define APPHOST_H

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"

class PythonApp;

class AppHost final
{
  public:
    static Status init();
    void destroy();
    static AppHost *get() { return instance; }

    void dispatchStartRequest(const ChildAppStartRequest &startArgs,
                              ProtoResponseMsg *response);

  private:
    AppHost();
    ~AppHost();

    AppHost(const AppHost &) = delete;
    AppHost &operator=(const AppHost &) = delete;

    PythonApp *pythonApp_;

    static AppHost *instance;
};

#endif  // APPHOST_H
