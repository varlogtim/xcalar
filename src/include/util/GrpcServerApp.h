// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _GRPCSERVERAPP_H_
#define _GRPCSERVERAPP_H_

#include "app/SystemApp.h"

class GrpcServerApp : public SystemApp
{
  public:
    // Called during cluster boot
    static Status init();
    // Caller during cluster shutdown
    static void destroy();

    static SystemApp *get();

    const char *name() const override;
    bool isAppEnabled() override;

  private:
    static GrpcServerApp *instance_;
};

#endif  // _GRPCSERVERAPP_H_
