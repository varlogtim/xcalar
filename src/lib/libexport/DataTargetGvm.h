// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATA_TARGET_GVM_H_
#define _DATA_TARGET_GVM_H_

#include "gvm/GvmTarget.h"

class DataTargetGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        Add = 0,
        Remove,
    };
    static MustCheck DataTargetGvm *get();
    static MustCheck Status init();
    void destroy();

    virtual MustCheck Status localHandler(uint32_t action,
                                          void *payload,
                                          size_t *outputSizeOut) override;
    virtual MustCheck GvmTarget::Index getGvmIndex() const override;

  private:
    static DataTargetGvm *instance;
    DataTargetGvm() {}
    ~DataTargetGvm() {}
    DataTargetGvm(const DataTargetGvm &) = delete;
    DataTargetGvm &operator=(const DataTargetGvm &) = delete;
};

#endif  // _DATA_TARGET_GVM_H_
