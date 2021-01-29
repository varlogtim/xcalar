// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <snappy.h>
#include "xccompress/XcSnappy.h"

size_t
XcSnappy::getMaxCompressedLength(size_t deCompressedBufLen)
{
    return snappy::MaxCompressedLength(deCompressedBufLen);
}

Status
XcSnappy::getUncompressedLength(void *compressedBuf,
                                size_t compressedBufLen,
                                size_t *result)
{
    // Runs in constant time
    const bool success = snappy::GetUncompressedLength((char *) compressedBuf,
                                                       compressedBufLen,
                                                       result);
    if (success) {
        return StatusOk;
    } else {
        return StatusDeCompressFailed;
    }
}

Status
XcSnappy::compress(void *deCompressedBuf,
                   size_t deCompressedBufLen,
                   void *compressedBuf,
                   size_t *compressedBufLen)
{
    Status status = StatusOk;
    size_t size = 0;

    try {
        snappy::RawCompress((char *) deCompressedBuf,
                            deCompressedBufLen,
                            (char *) compressedBuf,
                            &size);
    } catch (std::bad_alloc) {
        status = StatusNoMem;
        goto CommonExit;
    } catch (std::exception &e) {
        status = StatusCompressFailed;
        goto CommonExit;
    }
CommonExit:
    *compressedBufLen = size;
    return status;
}

Status
XcSnappy::deCompress(void *deCompressedBuf,
                     void *compressedBuf,
                     size_t compressedBufLen)
{
    Status status = StatusOk;

    try {
        if (snappy::RawUncompress((char *) compressedBuf,
                                  compressedBufLen,
                                  (char *) deCompressedBuf) != true) {
            status = StatusDeCompressFailed;
        }
    } catch (std::bad_alloc) {
        status = StatusNoMem;
        goto CommonExit;
    } catch (std::exception &e) {
        status = StatusDeCompressFailed;
        goto CommonExit;
    }
CommonExit:
    return status;
}
