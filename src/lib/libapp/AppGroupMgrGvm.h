// Copyright 2017-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APP_GROUP_MGR_GVM_H_
#define _APP_GROUP_MGR_GVM_H_

#include "gvm/GvmTarget.h"
#include "app/AppGroup.h"

class AppGroupMgrGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        InitMetadata = 0,
        Start,
        NodeDone,
        Reap,
        Cleanup,
        Abort,
    };

    MustCheck static AppGroupMgrGvm *get();
    static MustCheck Status init();
    void destroy();

    virtual MustCheck Status localHandler(uint32_t action,
                                          void *payload,
                                          size_t *outputSizeOut) override;
    virtual MustCheck GvmTarget::Index getGvmIndex() const override;
    MustCheck const char *strGetFromAction(Action action);

  private:
    static AppGroupMgrGvm *instance;
    static constexpr const char *ModuleName = "AppGroupMgrGvm";

    MustCheck AppGroup *getAppGroup(AppGroup::Id appGroupId,
                                    AppGroupMgrGvm::Action,
                                    bool *retCanAbort);

    AppGroupMgrGvm() {}
    ~AppGroupMgrGvm() {}
    AppGroupMgrGvm(const AppGroupMgrGvm &) = delete;
    AppGroupMgrGvm &operator=(const AppGroupMgrGvm &) = delete;
};

#endif  // _APP_GROUP_MGR_GVM_H_
