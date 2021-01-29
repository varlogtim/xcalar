// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LICENSEGVM_H_
#define _LICENSEGVM_H_

#include "gvm/GvmTarget.h"

class LicenseGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        Update = 1234,
        Revert,
    };

    struct LicenseUpdateInfo {
        uint64_t licenseVersion;
        size_t licenseSize;
        char licenseString[0];
    };

    struct LicenseRevertInfo {
        uint64_t revertVersion;
    };

    static MustCheck LicenseGvm *get();
    static MustCheck Status init();
    void destroy();

    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) override;
    virtual GvmTarget::Index getGvmIndex() const override;

  private:
    static constexpr const char *moduleName = "licenseGvm";
    static LicenseGvm *instance;
    LicenseGvm() {}
    ~LicenseGvm() {}
    LicenseGvm(const LicenseGvm &) = delete;
    LicenseGvm &operator=(const LicenseGvm &) = delete;
};

#endif  // _LICENSEGVM_H_
