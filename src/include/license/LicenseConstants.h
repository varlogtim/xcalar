// Copyright 2016-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSECONSTANTS_H_
#define LICENSECONSTANTS_H_

#include <string>
#include "primitives/Primitives.h"
#include "LicenseTypes.h"

/*
 * The master list of data type identifiers (must be 8 characters)
 */

const int LicenseTypeSize = 8;
const int LicenseByteCount = 62;
const int LicenseBufSize = 16384;
const int LicenseLineBufSize = 512;
const int LicenseKeyBufSize = 2048;
const int LicenseMaxKeySize = 64;
const int LicenseMaxFilePathLen = 256;
const int LicensePasswdLen = 64;
const int LicV1V2Size = 101;
const int LicV3Size = 181;

const char KNoType[] = "========";
const char KDBType[] = "abcdefgh";
const char KAppType[] = "zyxwvuts";

const unsigned char Base32Alphabet[] = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

/*
 * Important environment variables and default file names
 */

const char DefKeyFileName[64] = "EcdsaPub.key";
const char DefLicFileName[64] = "XcalarLic.key";

/*
 *  license expiration warning message interval
 */

const unsigned int ExpWarnIntvl = SecsPerWeek * 2;

const float IterationDefault = 2.0;
const float IterationMin = 0.0;
const float IterationMax = 1000000.0;
const int VersionDefault[3] = {0, 0, 0};
const LicensePlatform DefaultPlatform = LicPlatformLinuxX64;
const LicenseProduct DefaultProduct = LicProductXdp;
const LicenseProductFamily DefaultProductFamily = LicProductFamXdp;
const LicenseExpiryBehavior DefaultLicenseExpiryBehavior =
    LicExpiryBehaviorDisable;
const time_t ExpirationDateDefault = 1514793600;  // midnight 01-jan-2018
const int UserCountDefault = 8;
const int UserCountMin = 0;
const int UserCountMax = 1073741824;
const int NodeCountDefault = 16;
const int NodeCountMin = 0;
const int NodeCountMax = 1073741824;
const bool JdbcEnabledDefault = false;

#endif /* LICENSECONSTANTS_H_ */
