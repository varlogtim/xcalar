// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBXDBGVM_H_
#define _LIBXDBGVM_H_

#include "gvm/GvmTarget.h"

class LibXdbGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        Create,
    };
    static LibXdbGvm *get();
    static Status init();
    void destroy();

    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) override;
    virtual GvmTarget::Index getGvmIndex() const override;

  private:
    static LibXdbGvm *instance;
    LibXdbGvm() {}
    ~LibXdbGvm() {}
    LibXdbGvm(const LibXdbGvm &) = delete;
    LibXdbGvm &operator=(const LibXdbGvm &) = delete;
};

#endif  // _LIBXDBGVM_H_
