// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>
#include <cryptopp/files.h>
#include <cryptopp/base64.h>
#include "primitives/Primitives.h"
#include "license/LicenseKeyFile.h"

using namespace CryptoPP;

Status
LicenseKeyFile::loadData(const char *filename, char *data, int len)
{
    Status status;
    std::string dataString = "";
    int dataLen = 0;

    try {
        FileSource fs1(filename, true, new StringSink(dataString));
    } catch (FileSource::OpenErr &e) {
        status = StatusLicFileOpen;
        goto CommonExit;
    } catch (FileSource::ReadErr &e) {
        status = StatusLicFileRead;
        goto CommonExit;
    }

    dataLen =
        (dataString.size() < (unsigned long) len) ? dataString.size() : len;
    if (dataLen == len) {
        status = StatusLicBufTooSmall;
        goto CommonExit;
    }

    memcpy(data, dataString.data(), dataLen);
    data[dataLen] = 0;

    status = StatusOk;

CommonExit:
    return status;
}

Status
LicenseKeyFile::loadPrivateKey(const char *filename,
                               ECDSA<ECP, SHA1>::PrivateKey *key)
{
    ByteQueue q1;

    try {
        FileSource fs1(filename, true);
        fs1.TransferTo(q1);
    } catch (FileSource::OpenErr &e) {
        return StatusLicFileOpen;
    } catch (FileSource::ReadErr &e) {
        return StatusLicFileRead;
    }

    try {
        key->Load(q1);
    } catch (BERDecodeErr &e) {
        return StatusLicInputInvalid;
    }
    return StatusOk;
}

Status
LicenseKeyFile::loadPrivateKeyEnv(const char *envVar,
                                  ECDSA<ECP, SHA1>::PrivateKey *key)
{
    std::string encoded;
    std::string decoded;
    ByteQueue q1;
    char *keySource = getenv(envVar);

    if (keySource == NULL) {
        return StatusLicFileOpen;
    }

    encoded = std::string(keySource);

    try {
        StringSource(encoded, true, new Base64Decoder(new StringSink(decoded)));
    } catch (Exception e) {
        return StatusLicFileRead;
    }

    try {
        StringSource dss(decoded, true);
        key->Load(dss);
    } catch (BERDecodeErr &e) {
        return StatusLicInputInvalid;
    }
    return StatusOk;
}

Status
LicenseKeyFile::loadPublicKey(const char *filename,
                              ECDSA<ECP, SHA1>::PublicKey *key)
{
    ByteQueue q1;

    try {
        FileSource fs1(filename, true);
        fs1.TransferTo(q1);
    } catch (FileSource::OpenErr &e) {
        return StatusLicFileOpen;
    } catch (FileSource::ReadErr &e) {
        return StatusLicFileRead;
    }

    try {
        key->Load(q1);
    } catch (BERDecodeErr &e) {
        return StatusLicInputInvalid;
    }
    return StatusOk;
}

Status
LicenseKeyFile::loadPublicKeyBuf(const char *keySource,
                                 ECDSA<ECP, SHA1>::PublicKey *key)
{
    std::string encoded;
    std::string decoded;
    ByteQueue q1;

    if (keySource == NULL) {
        return StatusLicPubKeyMissing;
    }

    encoded = std::string(keySource);

    try {
        StringSource(encoded, true, new Base64Decoder(new StringSink(decoded)));
    } catch (Exception e) {
        return StatusLicFileRead;
    }

    try {
        StringSource dss(decoded, true);
        key->Load(dss);
    } catch (BERDecodeErr &e) {
        return StatusLicInputInvalid;
    }
    return StatusOk;
}

Status
LicenseKeyFile::loadPublicKeyEnv(const char *envVar,
                                 ECDSA<ECP, SHA1>::PublicKey *key)
{
    return loadPublicKeyBuf(getenv(envVar), key);
}

Status
LicenseKeyFile::savePrivateKey(const char *filename,
                               ECDSA<ECP, SHA1>::PrivateKey *key)
{
    ByteQueue q1;
    key->Save(q1);

    try {
        FileSink fs1(filename, true);
        q1.TransferTo(fs1);
    } catch (FileSink::OpenErr &e) {
        return StatusLicFileOpen;
    } catch (FileSink::WriteErr &e) {
        return StatusLicFileWrite;
    }

    return StatusOk;
}

Status
LicenseKeyFile::savePublicKey(const char *filename,
                              ECDSA<ECP, SHA1>::PublicKey *key)
{
    ByteQueue q1;
    key->Save(q1);

    try {
        FileSink fs1(filename, true);
        q1.TransferTo(fs1);
    } catch (FileSink::OpenErr &e) {
        return StatusLicFileOpen;
    } catch (FileSink::WriteErr &e) {
        return StatusLicFileWrite;
    }

    return StatusOk;
}
