// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DAG_NODE_GVM_H_
#define _DAG_NODE_GVM_H_

#include "gvm/GvmTarget.h"

class DagNodeGvm final : public GvmTarget
{
  public:
    static DagNodeGvm *get();
    static Status init();
    void destroy();

    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) override;
    virtual GvmTarget::Index getGvmIndex() const override;

  private:
    static DagNodeGvm *instance;
    DagNodeGvm() {}
    ~DagNodeGvm() {}
    DagNodeGvm(const DagNodeGvm &) = delete;
    DagNodeGvm &operator=(const DagNodeGvm &) = delete;
};

#endif  // _DAG_NODE_GVM_H_
