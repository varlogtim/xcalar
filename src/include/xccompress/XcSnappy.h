// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCSNAPPY_H_
#define _XCSNAPPY_H_

#include "primitives/Primitives.h"

// Enable this for tighter checks on Snappy compression.
// #define XcSnappyDebug

// Xcalar Snappy compression library. Note that there is no need for even a
// Singleton instance here, since this is purely stateless. So you cannot and
// should not instantiate this.
class XcSnappy
{
  public:
    static size_t getMaxCompressedLength(size_t deCompressedBufLen);
    static Status compress(void *deCompressedBuf,
                           size_t deCompressedBufLen,
                           void *compressedBuf,
                           size_t *compressedBufLen);
    static MustCheck Status deCompress(void *deCompressedBuf,
                                       void *compressedBuf,
                                       size_t compressedBufLen);
    static MustCheck Status getUncompressedLength(void *compressedBuf,
                                                  size_t compressedBufLen,
                                                  size_t *result);

  private:
    // Keep this private.
    XcSnappy() {}

    // Keep this private.
    ~XcSnappy() {}

    XcSnappy(const XcSnappy &) = delete;
    XcSnappy &operator=(const XcSnappy &) = delete;
};

#endif  // _XCSNAPPY_H_
