// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LICENSEKEYMETADATA_H_
#define _LICENSEKEYMETADATA_H_

#include "LicenseConstants.h"

struct LicenseKeyMetadata {
    enum Type {
        Boolean,
        Integer,
        LongInteger,
        String,
        Timestamp,
        Float,
        VersionType,
        PlatformType,
        ProductType,
        ProductFamilyType,
        LicenseExpiryBehaviorType,
        SizeWithUnitsType,
    };

    enum : int {
        Iteration = 0,
        Version,
        Platform,
        Product,
        ProductFamily,
        ExpirationDate,
        UserCount,
        NodeCount,
        Licensee,
        OnExpiry,
        MaxInteractiveDataSize,
        JdbcEnabled,
        LicenseFieldCount  // Must be last
    };

    const char *keyName;
    void *variable;
    size_t bufSize;
    union {
        uint32_t defaultInteger;
        size_t defaultLongInteger;
        time_t defaultTimestamp;
        bool defaultBoolean;
        float defaultFloat;
        void *defaultString;
        int defaultVersion[3];
        LicensePlatform defaultPlatform;
        LicenseProduct defaultProduct;
        LicenseProductFamily defaultFamily;
        LicenseExpiryBehavior defaultLicenseExpiryBehavior;
    };
    Type type;
    bool doRangeCheck;
    int64_t minValue;
    int64_t maxValue;
    int maskPos;
    float minIteration;
};

#endif  // _LICENSEKEYMETADATA_H_
