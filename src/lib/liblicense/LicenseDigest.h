// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LICENSEDIGEST_H_
#define LICENSEDIGEST_H_

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>
#include <stdint.h>

class LicenseDigest
{
  public:
    static int createDigest(unsigned char *message,
                            unsigned int message_len,
                            uint16_t *result);
};

#endif /* LICENSEDIGEST_H_ */
