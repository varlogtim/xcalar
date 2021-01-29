// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LOG_GEN_TEST_DATA_H_
#define _LOG_GEN_TEST_DATA_H_

#include "primitives/Primitives.h"
class LogGenTestData {
public:
    Status genFuncTestData(const char * const prefix, const bool verbose);

private:
    static constexpr size_t logBufSize = 256;

    Status genCleanLog(LogLib::Handle *handle, const char * const prefix,
                       const size_t logFileSize);

    Status dumpTestData(LogLib::Handle *handle);

    Status ffToRecord(LogLib::Handle *handle, const size_t pos);

    Status getSetFooter(LogLib::Handle *handle, LogLib::LogFooter *footer,
                        const bool isWrite);
};
#endif // _LOG_GEN_TEST_DATA_H_
