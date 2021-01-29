// Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/LicenseService.h"

using namespace xcalar::compute::localtypes::License;

LicenseService::LicenseService()
{
    licenseMgr_ = LicenseMgr::get();
    assert(licenseMgr_);
}

ServiceAttributes
LicenseService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
LicenseService::create(const CreateRequest *createRequest,
                       google::protobuf::Empty *empty)
{
    Status status;

    // XXX: Need to make sure update can handle first time add
    status =
        licenseMgr_
            ->updateLicense(createRequest->licensevalue().value().c_str(),
                            createRequest->licensevalue().value().length());

    return status;
}

Status
LicenseService::destroy(const DestroyRequest *destroyRequest,
                        google::protobuf::Empty *empty)
{
    Status status;

    status = licenseMgr_->destroyLicense();
    BailIfFailed(status);

CommonExit:

    return status;
}

Status
LicenseService::get(const GetRequest *getRequest, GetResponse *getResponse)
{
    Status status = StatusOk;
    LicenseData2 *theLicense;
    int productVersion[4];
    char productVersionStr[XcalarApiLicenseBufLen];
    char expiration[XcalarApiLicenseBufLen];
    char *compressedLicense = NULL;
    size_t compressedLicenseSize = 0;

    // If the license isn't loaded there's not much info that can be returned.
    if (!licenseMgr_->licIsLoaded()) {
        status = licenseMgr_->licGetLoadStatus();
        assert(status != StatusOk);
        if (status == StatusOk) {
            status = StatusLicMissing;
        }
        goto CommonExit;
    }

    status = licenseMgr_->getCompressedLicense(&compressedLicense,
                                               &compressedLicenseSize);
    BailIfFailed(status);

    theLicense = licenseMgr_->getLicenseData();
    theLicense->getProductVersion(productVersion);

    try {
        getResponse->set_loaded(true);
        getResponse->set_expired(licenseMgr_->licIsExpired());
        getResponse->set_platform(
            strGetFromLicensePlatform(theLicense->getPlatform()));
        getResponse->set_product(
            strGetFromLicenseProduct(theLicense->getProduct()));
        getResponse->set_productfamily(
            strGetFromLicenseProductFamily(theLicense->getProductFamily()));
        snprintf(productVersionStr,
                 sizeof(productVersionStr),
                 "%d.%d.%d.%d",
                 productVersion[0],
                 productVersion[1],
                 productVersion[2],
                 productVersion[3]);
        getResponse->set_productversion(productVersionStr);
        snprintf(expiration,
                 sizeof(expiration),
                 "%ld",
                 theLicense->getExpiration());
        getResponse->set_expiration(expiration);
        getResponse->set_nodecount(theLicense->getNodeCount());
        getResponse->set_usercount(theLicense->getUserCount());
        getResponse->set_licensee(theLicense->getLicensee());
        getResponse->set_compressedlicense(compressedLicense);
        getResponse->set_compressedlicensesize(compressedLicenseSize);
        // "attributes" currently not returned
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:

    if (compressedLicense != NULL) {
        memFree(compressedLicense);
        compressedLicense = NULL;
    }

    return status;
}

Status
LicenseService::update(const UpdateRequest *updateRequest,
                       google::protobuf::Empty *empty)
{
    Status status;

    status =
        licenseMgr_
            ->updateLicense(updateRequest->licensevalue().value().c_str(),
                            updateRequest->licensevalue().value().length());
    return status;
}

Status
LicenseService::validate(const ValidateRequest *validateRequest,
                         ValidateResponse *validateResponse)
{
    Status status;
    char *compressedLicense = NULL;
    size_t compressedLicenseSize = 0;
    LicenseData2 *theLicense;
    Config *config = Config::get();

    validateResponse->set_islicensecompliant(false);

    if (!licenseMgr_->licIsLoaded()) {
        status = licenseMgr_->licGetLoadStatus();
        assert(status != StatusOk);
        if (status == StatusOk) {
            status = StatusLicMissing;
        }
        goto CommonExit;
    }

    status = licenseMgr_->getCompressedLicense(&compressedLicense,
                                               &compressedLicenseSize);
    BailIfFailed(status);

    theLicense = licenseMgr_->getLicenseData();

    if (licenseMgr_->licIsExpired()) {
        status = StatusLicExpired;
        goto CommonExit;
    }

    if (config->getActiveNodes() > theLicense->getNodeCount()) {
        status = StatusLicInsufficientNodes;
        goto CommonExit;
    }

    // All appears to be in compliance with the license
    validateResponse->set_islicensecompliant(true);

CommonExit:

    if (compressedLicense != NULL) {
        memFree(compressedLicense);
        compressedLicense = NULL;
    }

    return status;
}
