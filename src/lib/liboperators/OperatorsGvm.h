// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORS_GVM_H_
#define _OPERATORS_GVM_H_

#include "gvm/GvmTarget.h"

class OperatorsGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        AddPerTxnInfo,
        UpdatePerTxnInfo,
        DeletePerTxnInfo,
    };
    static OperatorsGvm *get();
    static Status init();
    void destroy();

    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) override;
    virtual GvmTarget::Index getGvmIndex() const override;

  private:
    static OperatorsGvm *instance;
    OperatorsGvm() {}
    ~OperatorsGvm() {}
    OperatorsGvm(const OperatorsGvm &) = delete;
    OperatorsGvm &operator=(const OperatorsGvm &) = delete;
};

#endif  // _OPERATORS_GVM_H_
