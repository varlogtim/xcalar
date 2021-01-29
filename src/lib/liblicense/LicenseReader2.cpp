// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <iostream>
#include <iomanip>
#include <cryptopp/default.h>
#include <cryptopp/filters.h>
#include "StrlFunc.h"
#include "license/LicenseReader2.h"
#include "license/LicenseConstants.h"
#include "license/LicenseSignature.h"
#include "license/LicenseKeyFile.h"

LicenseReader2::~LicenseReader2() {}

Status
LicenseReader2::read(const char *data, LicenseData2 *result)
{
    Status status = StatusOk;
    LicenseSignature::KeySource publicKeySrc;

    if (publicKeyVar_ != NULL) {
        publicKeySrc = LicenseSignature::KeySource::FromEnvVar;
    } else if (publicKey_ != NULL) {
        publicKeySrc = LicenseSignature::KeySource::FromBuffer;
    } else {
        publicKeySrc = LicenseSignature::KeySource::FromKeyFile;
    }

    LicenseSignature signature(NULL,
                               NULL,
                               LicenseSignature::KeySource::NotProvided,
                               publicKeyFile_,
                               publicKeyVar_,
                               publicKey_,
                               publicKeySrc,
                               password_);
    char licenseData[LicenseKeyBufSize];

    status = result->parseSignature(data, false);

    if (status != StatusOk) {
        goto CommonExit;
    }

    result->createSignatureData(data, licenseData);

    status = signature.verifySig(licenseData, (char *) result->getSignature());

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = result->parse(data);

CommonExit:

    return status;
}
