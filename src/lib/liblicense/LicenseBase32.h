// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEBASE32_H_
#define LICENSEBASE32_H_

#include <string.h>
#include "primitives/Primitives.h"

class LicenseBase32
{
  public:
    static Status decode32(unsigned char *input,
                           int len,
                           unsigned char *output);
    static Status encode32(unsigned char *input,
                           int len,
                           unsigned char *output);

    static int getDecode32Length(int bytes);
    static int getEncode32Length(int bytes);

    static Status map32(unsigned char *data,
                        int data_len,
                        const unsigned char *alphabet);
    static Status unmap32(unsigned char *data,
                          int data_len,
                          const unsigned char *alphabet);
};

#endif /* LICENSEBASE32_H_ */
