// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEKEYFILE_H_
#define LICENSEKEYFILE_H_

#include <cryptopp/eccrypto.h>
#include <cryptopp/pssr.h>
#include <cryptopp/osrng.h>
#include <cryptopp/oids.h>
#include "primitives/Primitives.h"

class LicenseKeyFile
{
  public:
    static Status loadData(const char *filename, char *data, int len);
    static Status loadPrivateKey(
        const char *filename,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PrivateKey *key);
    static Status loadPrivateKeyEnv(
        const char *envVar,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PrivateKey *key);
    static Status loadPublicKey(
        const char *filename,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PublicKey *key);
    static Status loadPublicKeyBuf(
        const char *keySource,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PublicKey *key);

    static Status loadPublicKeyEnv(
        const char *envVar,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PublicKey *key);

    static Status savePrivateKey(
        const char *filename,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PrivateKey *key);
    static Status savePublicKey(
        const char *filename,
        CryptoPP::ECDSA<CryptoPP::ECP, CryptoPP::SHA1>::PublicKey *key);
};

#endif /* LICENSEKEYFILE_H_ */
