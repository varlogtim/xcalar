// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APP_MGR_GVM_H_
#define _APP_MGR_GVM_H_

#include "gvm/GvmTarget.h"

class AppMgrGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        Create,
        Update,
        Remove,
    };

    static MustCheck AppMgrGvm *get();
    static Status init();
    void destroy();

    virtual MustCheck Status localHandler(uint32_t action,
                                          void *payload,
                                          size_t *outputSizeOut) override;
    virtual MustCheck GvmTarget::Index getGvmIndex() const override;

  private:
    static AppMgrGvm *instance;

    AppMgrGvm() {}
    ~AppMgrGvm() {}
    AppMgrGvm(const AppMgrGvm &) = delete;
    AppMgrGvm &operator=(const AppMgrGvm &) = delete;
};

#endif  // _APP_MGR_GVM_H_
