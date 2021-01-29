// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SRC_LIB_LIBLICENSE_LICENSEINPUT_H_
#define SRC_LIB_LIBLICENSE_LICENSEINPUT_H_

#include "license/LicenseConstants.h"

class LicenseInput
{
  public:
    static bool stringToPlatform(char *buf,
                                 int maxlen,
                                 LicensePlatform *platform);
    static bool stringToProduct(char *buf, int maxlen, LicenseProduct *product);
    static bool stringToProductFamily(char *buf,
                                      int maxlen,
                                      LicenseProductFamily *family);
    static bool stringToVersion(char *buf, int maxlen, LicenseVersion *version);
};

#endif /* SRC_LIB_LIBLICENSE_LICENSEINPUT_H_ */
