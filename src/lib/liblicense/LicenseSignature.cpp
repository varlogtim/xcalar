// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include <cryptopp/default.h>
#include <cryptopp/filters.h>
#include <cryptopp/files.h>
#include <cryptopp/eccrypto.h>
#include <cryptopp/hkdf.h>
#include <cryptopp/pssr.h>
#include <cryptopp/osrng.h>
#include <cryptopp/oids.h>
#include <cryptopp/base64.h>
#include <cryptopp/hex.h>
#include "StrlFunc.h"
#include "license/LicenseSignature.h"
#include "license/LicenseKeyFile.h"

using namespace CryptoPP;

void
generateSalt(SecByteBlock &salt1,
             SecByteBlock &salt2,
             SecByteBlock &salt3,
             std::string sigPassword)
{
    SHA256 hash256;
    time_t now = time(NULL);

    OS_GenerateRandomBlock(true, salt1, salt1.size());
    hash256.Update((const byte *) &now, sizeof(now));
    hash256.TruncatedFinal(salt2.data(), salt2.size());
    hash256.Restart();
    hash256.Update((const byte *) sigPassword.data(), sigPassword.size());
    hash256.Update((const byte *) salt1.data(), salt1.size());
    hash256.TruncatedFinal(salt3.data(), salt3.size());
}

void
extractSalt(std::string signature,
            SecByteBlock &salt1,
            SecByteBlock &salt2,
            SecByteBlock &salt3,
            std::string &signatureData,
            int saltSize)
{
    memcpy(salt1.data(), signature.data(), saltSize);
    memcpy(salt2.data(), signature.data() + saltSize, saltSize);
    memcpy(salt3.data(), signature.data() + 2 * saltSize, saltSize);
    signatureData = signature.substr(3 * saltSize);
}

void
deriveKeyAndIV(SecByteBlock &ekey,
               SecByteBlock &iv,
               SecByteBlock &akey,
               SecByteBlock salt1,
               SecByteBlock salt2,
               SecByteBlock salt3,
               std::string password)
{
    HKDF<SHA1> hkdf;
    hkdf.DeriveKey(ekey.data(),
                   ekey.size(),
                   (const byte *) password.data(),
                   password.size(),
                   salt1.data(),
                   salt1.size(),
                   NULL,
                   0);
    hkdf.DeriveKey(iv.data(),
                   iv.size(),
                   (const byte *) password.data(),
                   password.size(),
                   salt2.data(),
                   salt2.size(),
                   NULL,
                   0);
    hkdf.DeriveKey(akey.data(),
                   akey.size(),
                   (const byte *) password.data(),
                   password.size(),
                   salt3.data(),
                   salt3.size(),
                   NULL,
                   0);
}

void
buildSignature(SecByteBlock salt1,
               SecByteBlock salt2,
               SecByteBlock salt3,
               std::string result,
               std::string &signature)
{
    AlgorithmParameters params =
        MakeParameters(Name::InsertLineBreaks(), false);

    Base64Encoder importSink;

    importSink.IsolatedInitialize(params);
    importSink.Attach(new StringSink(signature));
    importSink.Put(salt1.data(), salt1.size());
    importSink.Put(salt2.data(), salt2.size());
    importSink.Put(salt3.data(), salt3.size());
    importSink.Put((const byte *) result.data(), result.size());
    importSink.MessageEnd();
}

LicenseSignature::LicenseSignature(char *privateKeyFile,
                                   char *privateKeyVar,
                                   LicenseSignature::KeySource privateKeySrc,
                                   char *publicKeyFile,
                                   char *publicKeyVar,
                                   const char *publicKey,
                                   LicenseSignature::KeySource publicKeySrc,
                                   const char *password)
{
    if (privateKeyFile != NULL) {
        strlcpy(privateKeyFile_, privateKeyFile, XcalarApiMaxPathLen);
    } else {
        strlcpy(privateKeyFile_, "", XcalarApiMaxPathLen);
    }

    if (privateKeyVar != NULL) {
        strlcpy(privateKeyVar_, privateKeyVar, XcalarApiMaxPathLen);
    } else {
        strlcpy(privateKeyVar_, "", XcalarApiMaxPathLen);
    }

    if (publicKeyFile != NULL) {
        strlcpy(publicKeyFile_, publicKeyFile, XcalarApiMaxPathLen);
    } else {
        strlcpy(publicKeyFile_, "", XcalarApiMaxPathLen);
    }

    if (publicKeyVar != NULL) {
        strlcpy(publicKeyVar_, publicKeyVar, XcalarApiMaxPathLen);
    } else {
        strlcpy(publicKeyVar_, "", XcalarApiMaxPathLen);
    }

    publicKey_ = publicKey;

    if (password != NULL) {
        strlcpy(password_, password, XcalarLicMaxPasswordLen);
    } else {
        strlcpy(password_, "", XcalarLicMaxPasswordLen);
    }

    publicKeySrc_ = publicKeySrc;
    privateKeySrc_ = privateKeySrc;
}

Status
LicenseSignature::signSig(char *licenseString, char **buffer)
{
    if ((strlen(privateKeyFile_) == 0) && (strlen(privateKeyVar_) == 0)) {
        return StatusLicPrivKeyMissing;
    }

    if (strlen(password_) == 0) {
        return StatusLicPasswdMissing;
    }

    if (strlen(licenseString) == 0) {
        return StatusLicLicenseMissing;
    }

    AutoSeededRandomPool rng;

    ECDSA<ECP, SHA1>::PrivateKey key;
    Status keyLoad;
    switch (privateKeySrc_) {
    case KeySource::FromKeyFile:
        keyLoad = LicenseKeyFile::loadPrivateKey(privateKeyFile_, &key);
        break;
    case KeySource::FromEnvVar:
        keyLoad = LicenseKeyFile::loadPrivateKeyEnv(privateKeyVar_, &key);
        break;
    default:
        assert(0);
        keyLoad = StatusLicUnsupportedOperation;
    }

    if (keyLoad != StatusOk) {
        return keyLoad;
    }
    ECDSA<ECP, SHA1>::Signer signer(key);

    std::string sigPassword = password_;
    std::string licenseBodyString = licenseString;
    std::string encryptedResult;
    SecByteBlock ekey(keySize_), iv(saltSize_), akey(keySize_);
    SecByteBlock salt1(saltSize_), salt2(saltSize_), salt3(saltSize_);

    generateSalt(salt1, salt2, salt3, sigPassword);
    deriveKeyAndIV(ekey, iv, akey, salt1, salt2, salt3, sigPassword);

    CBC_Mode<AES>::Encryption encryptor;
    encryptor.SetKeyWithIV(ekey, ekey.size(), iv, iv.size());
    HMAC<SHA256> hmac;
    hmac.SetKey(akey, akey.size());

    try {
        StringSource signBodySource(
            licenseBodyString,
            true,
            new SignerFilter(rng,
                             signer,
                             new StreamTransformationFilter(
                                 encryptor,
                                 new HashFilter(hmac,
                                                new StringSink(encryptedResult),
                                                true))));
    } catch (CryptoPP::CryptoMaterial::InvalidMaterial e) {
        return StatusLicPrivKeyErr;
    }

    std::string exportSignature;
    buildSignature(salt1, salt2, salt3, encryptedResult, exportSignature);

    strlcpy(*buffer, exportSignature.data(), exportSignature.size());

    return StatusOk;
}

Status
LicenseSignature::verifySig(char *licenseString, char *signature)
{
    if ((strlen(publicKeyFile_) == 0) && (strlen(publicKeyVar_) == 0) &&
        (publicKey_ == NULL)) {
        return StatusLicPubKeyMissing;
    }

    if (strlen(password_) == 0) {
        return StatusLicPasswdMissing;
    }

    if (strlen(licenseString) == 0) {
        return StatusLicLicenseMissing;
    }

    if (strlen(signature) == 0) {
        return StatusLicSignatureMissing;
    }

    std::string sigPassword = password_;
    std::string base64Signature = signature;
    std::string licenseBodyString = licenseString;
    std::string binarySignature;
    std::string binarySignatureData;
    std::string recoveredSignature;
    StringSource importSource(base64Signature,
                              true,
                              new Base64Decoder(
                                  new StringSink(binarySignature)));

    SecByteBlock ekey(keySize_), iv(saltSize_), akey(keySize_);
    SecByteBlock salt1(saltSize_), salt2(saltSize_), salt3(saltSize_);

    extractSalt(binarySignature,
                salt1,
                salt2,
                salt3,
                binarySignatureData,
                saltSize_);
    deriveKeyAndIV(ekey, iv, akey, salt1, salt2, salt3, sigPassword);

    CBC_Mode<AES>::Decryption decryptor;
    decryptor.SetKeyWithIV(ekey, ekey.size(), iv, iv.size());
    HMAC<SHA256> hmac;
    hmac.SetKey(akey, akey.size());

    static const word32 flags =
        CryptoPP::HashVerificationFilter::HASH_AT_END |
        CryptoPP::HashVerificationFilter::PUT_MESSAGE |
        CryptoPP::HashVerificationFilter::THROW_EXCEPTION;
    try {
        StringSource recoverSigSource(
            binarySignatureData,
            true,
            new HashVerificationFilter(hmac,
                                       new StreamTransformationFilter(
                                           decryptor,
                                           new StringSink(recoveredSignature)),
                                       flags));
    } catch (CryptoPP::HashVerificationFilter::HashVerificationFailed e) {
        return StatusLicPasswordError;
    }

    ECDSA<ECP, SHA1>::PublicKey key;
    Status keyLoad;

    switch (publicKeySrc_) {
    case KeySource::FromKeyFile:
        keyLoad = LicenseKeyFile::loadPublicKey(publicKeyFile_, &key);
        break;
    case KeySource::FromEnvVar:
        keyLoad = LicenseKeyFile::loadPublicKeyEnv(publicKeyVar_, &key);
        break;
    case KeySource::FromBuffer:
        keyLoad = LicenseKeyFile::loadPublicKeyBuf(publicKey_, &key);
        break;
    default:
        keyLoad = StatusLicUnsupportedOperation;
        break;
    }

    if (keyLoad != StatusOk) {
        return keyLoad;
    }

    ECDSA<ECP, SHA1>::Verifier verifier(key);
    bool finalResult = false;

    try {
        StringSource verifyBodySource(recoveredSignature + licenseBodyString,
                                      true,
                                      new SignatureVerificationFilter(
                                          verifier,
                                          new ArraySink((byte *) &finalResult,
                                                        sizeof(finalResult))));
    } catch (CryptoPP::CryptoMaterial::InvalidMaterial e) {
        return StatusLicPubKeyErr;
    }

    // Whew!
    if (!finalResult) {
        return StatusLicSignatureInvalid;
    }

    return StatusOk;
}
