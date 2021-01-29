// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DAG_GVM_H_
#define _DAG_GVM_H_

#include "gvm/GvmTarget.h"

class DagGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        Create = 1234,
        PreDelete,
        Delete,
    };

    static DagGvm *get();
    static Status init();
    void destroy();

    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) override;
    virtual GvmTarget::Index getGvmIndex() const override;

  private:
    static DagGvm *instance;
    DagGvm() {}
    ~DagGvm() {}
    DagGvm(const DagGvm &) = delete;
    DagGvm &operator=(const DagGvm &) = delete;
};

#endif  // _DAG_GVM_H_
