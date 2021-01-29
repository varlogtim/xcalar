// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEREADER2_H_
#define LICENSEREADER2_H_

#include "LicenseData2.h"

class LicenseReader2
{
  public:
    LicenseReader2(char *publicKeyFile,
                   char *publicKeyVar,
                   const char *publicKey,
                   const char *password)
        : publicKeyFile_(publicKeyFile),
          publicKeyVar_(publicKeyVar),
          publicKey_(publicKey),
          password_(password)
    {
    }
    virtual ~LicenseReader2();
    Status read(const char *key, LicenseData2 *result);

  private:
    char *publicKeyFile_;
    char *publicKeyVar_;
    const char *publicKey_;
    const char *password_;

    LicenseReader2 &operator=(const LicenseReader2 &) = delete;
    LicenseReader2 &operator=(const LicenseReader2 &&) = delete;
    LicenseReader2(const LicenseReader2 &) = delete;
    LicenseReader2(const LicenseReader2 &&) = delete;
};

#endif /* LICENSEREADER_H_ */
