// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#if !defined(UTIL_SYSTEMPERFAPP_H)
#define UTIL_SYSTEMPERFAPP_H

#include "app/SystemApp.h"

class SystemPerfApp : public SystemApp
{
  public:
    // Called during cluster boot on every node to create the singleton.
    static Status init();

    // Called during cluster shutdown to destroy the singleton.
    static void destroy();

    static SystemApp *get();

    const char *name() const override;
    bool isAppEnabled() override;

  private:
    static SystemPerfApp *instance_;
};

#endif  // UTIL_SYSTEMPERFAPP_H
