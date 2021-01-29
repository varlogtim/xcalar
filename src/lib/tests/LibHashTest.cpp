// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "FnvTest.h"
#include "primitives/Primitives.h"
#include "test/QA.h"
#include "hash/Hash.h"

static Status fnvTestsWrapper();
static Status crc32cTests();

static TestCase testCases[] = {
    {"FNV Tests", fnvTestsWrapper, TestCaseEnable, ""},
    // hashCrc32c() needs aligned memory
    {"CRC32c Tests", crc32cTests, TestCaseDisable, ""},
};

static Status
crc32cTests()
{
    typedef struct {
        const void *buf;
        size_t bufLen;
        uint32_t crc32c;
    } BufCrcMapping;

    unsigned ii;

    BufCrcMapping bufCrcMapping[] = {
        // Test vector taken from
        // http://reveng.sourceforge.net/crc-catalogue/
        // 17plus.htm#crc.cat.crc-32c
        {"123456789", strlen("123456789"), 0xe3069283},
    };

    for (ii = 0; ii < ArrayLen(bufCrcMapping); ii++) {
        uint32_t crc32c;
        crc32c = hashCrc32c(0, bufCrcMapping[ii].buf, bufCrcMapping[ii].bufLen);
        printf("%x\n", crc32c);
        assert(crc32c == bufCrcMapping[ii].crc32c);
    }

    return StatusOk;
}

static Status
fnvTestsWrapper()
{
    fnvTests();
    return StatusOk;
}

static TestCaseOptionMask testCaseOptionMask = TestCaseOptDisableIsPass;

int
main(int argc, char *argv[])
{
    Status status;
    int numTestsFailed;

    status = hashInit();
    assert(status == StatusOk);

    numTestsFailed =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    return numTestsFailed;
}
