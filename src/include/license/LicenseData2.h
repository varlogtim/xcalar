// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEDATA2_H_
#define LICENSEDATA2_H_

#include <sys/types.h>
#include <time.h>
#include <string>
#include "LicenseConstants.h"
#include "Primitives.h"
#include "LicenseKeyMetadata.h"
#include "libapis/LibApisCommon.h"
#include "LicenseTypes.h"

class LicenseData2
{
  public:
    LicenseData2();
    virtual ~LicenseData2();

    Status parse(const char *data);
    Status parseSignature(const char *data, bool signKey);
    void dump(char **data, int len);
    Status createSignatureData(const char *data, char *sigData);
    bool prepSignature(char *data);

    float getIteration() { return iteration_; }
    LicenseProductFamily getProductFamily() { return productFamily_; }
    LicenseProduct getProduct() { return product_; }
    void getProductVersion(int *result)
    {
        for (int ii = 0; ii < 3; ii++) result[ii] = version_[ii];
    }
    LicensePlatform getPlatform() { return platform_; }
    time_t getExpiration() { return expirationDate_; }
    uint32_t getNodeCount() { return nodeCount_; }
    uint16_t getUserCount() { return userCount_; }
    unsigned char *getSignature() { return signature_; }
    long getSignaturePos() { return sigPos_; }
    const char *getLicensee() { return licensee_; }

    LicenseExpiryBehavior getLicenseExpiryBehavior()
    {
        return licenseExpiryBehavior_;
    }

    size_t getMaxInteractiveDataSize() { return maxInteractiveDataSize_; }

  private:
    void setupKeyValueMap();
    void markFound(unsigned char *bitmap, int idx);
    void markRequired(unsigned char *bitmap, int idx, float minIter);
    bool parseComplete(unsigned char *foundBitmap,
                       unsigned char *requiredBitmap,
                       int fieldCount);
    Status parseLine(char *data);
    Status parseKV(char *key, char *value);
    bool canIgnoreLine(char *line);
    char *strTrim(char *input);
    bool strIsWhiteSpace(char c);
    Status parseBoolean(char *input, bool *output);
    int parseMonth(char *input);
    bool parseSignatureLine(const char *data);
    void parseSignatureKeysLine(const char *data);
    bool parseSignatureDataLine(const char *data);
    void parseIterationLine(char *data);

    // buffer holding the complete license
    unsigned char *data_;

    // keyvalue metadata
    LicenseKeyMetadata *keyValueMap_;

    // parsing data structures
    int bitmapSize_ = (LicenseKeyMetadata::LicenseFieldCount / 8) + 1;
    unsigned char *parseBitmap_;
    unsigned char *requiredBitmap_;
    int parseFieldCount_ = LicenseKeyMetadata::LicenseFieldCount;

    // signature detection and calculation
    // data structures
    unsigned char *signature_;
    char **signatureKeys_;
    int signatureKeyCount_;
    bool useSignatureKeys_;
    unsigned char *sigParseBitmap_;
    long sigPos_;

    // data members
    float iteration_;

    int version_[3] = {0, 0, 0};

    LicensePlatform platform_;

    LicenseProduct product_;

    LicenseProductFamily productFamily_;

    LicenseExpiryBehavior licenseExpiryBehavior_;

    char licensee_[XcalarApiMaxLicenseeLen];

    time_t expirationDate_;

    int userCount_;

    int nodeCount_;

    size_t maxInteractiveDataSize_;

    bool jdbcEnabled_;
};

#endif /* LICENSEDATA2_H_ */
