// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSESIGNATURE_H_
#define LICENSESIGNATURE_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"

const int XcalarLicMaxPasswordLen = 64;

class LicenseSignature
{
  public:
    enum class KeySource { NotProvided, FromKeyFile, FromEnvVar, FromBuffer };

    LicenseSignature(char *privateKeyFile,
                     char *privateKeyVar,
                     KeySource privateKeySrc,
                     char *publicKeyFile,
                     char *publicKeyVar,
                     const char *publicKey,
                     KeySource publicKeySrc,
                     const char *password);
    ~LicenseSignature() {}
    Status signSig(char *licenseString, char **buffer);
    Status verifySig(char *licenseString, char *signature);
    void setKeySize(int size);
    void setSaltSize(int size);

  private:
    char publicKeyFile_[XcalarApiMaxPathLen];
    char publicKeyVar_[XcalarApiMaxPathLen];
    const char *publicKey_ = NULL;

    KeySource publicKeySrc_;

    char privateKeyFile_[XcalarApiMaxPathLen];
    char privateKeyVar_[XcalarApiMaxPathLen];
    KeySource privateKeySrc_;

    char password_[XcalarLicMaxPasswordLen];
    int keySize_ = 32;
    int saltSize_ = 16;

    LicenseSignature &operator=(const LicenseSignature &) = delete;
    LicenseSignature(const LicenseSignature &) = delete;
};

#endif
