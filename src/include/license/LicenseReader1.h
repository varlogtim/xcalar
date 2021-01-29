// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEREADER_H_
#define LICENSEREADER_H_

#include <cryptopp/eccrypto.h>
#include <cryptopp/pssr.h>
#include <cryptopp/osrng.h>
#include "license/LicenseData1.h"

using CryptoPP::ECDSA;
using CryptoPP::ECP;
using CryptoPP::SHA1;

class LicenseReader1
{
  public:
    LicenseReader1(ECDSA<ECP, SHA1>::PublicKey *keys, int count);
    virtual ~LicenseReader1();
    Status read(char *key, LicenseData1 **result);
    static int fileSize(char *type);

  private:
    static constexpr const size_t MaxDecodedSize = 10 * KB;

    ECDSA<ECP, SHA1>::PublicKey *publicKeys_;
    int keyCount_;

    LicenseReader1 &operator=(const LicenseReader1 &) = delete;
    LicenseReader1 &operator=(const LicenseReader1 &&) = delete;
    LicenseReader1(const LicenseReader1 &) = delete;
    LicenseReader1(const LicenseReader1 &&) = delete;
};

#endif /* LICENSEREADER_H_ */
