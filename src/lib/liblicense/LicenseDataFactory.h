// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEDATAFACTORY_H_
#define LICENSEDATAFACTORY_H_

#include "license/LicenseData1.h"

class LicenseDataFactory
{
  public:
    static LicenseData1 *create(char *type);
};

#endif /* LICENSEDATAFACTORY_H_ */
