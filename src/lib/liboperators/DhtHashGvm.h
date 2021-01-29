// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DHTHASHGVM_H_
#define _DHTHASHGVM_H_

#include "gvm/GvmTarget.h"

class DhtHashGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        Add,
        Delete,
    };
    static DhtHashGvm *get();
    static Status init();
    void destroy();

    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) override;
    virtual GvmTarget::Index getGvmIndex() const override;

  private:
    static DhtHashGvm *instance;
    DhtHashGvm() {}
    ~DhtHashGvm() {}
    DhtHashGvm(const DhtHashGvm &) = delete;
    DhtHashGvm &operator=(const DhtHashGvm &) = delete;
};

#endif  // _DHTHASHGVM_H_
