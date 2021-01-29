// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/VarInt.h"
#include "util/Stopwatch.h"
#include <math.h>

#include "test/QA.h"  // Must be last

static Status varIntTest();
static Status zigZagTest();
static Status varIntPerf();

static TestCase testCases[] = {
    {"VarInt tests", varIntTest, TestCaseEnable, ""},
    {"ZigZag tests", zigZagTest, TestCaseEnable, ""},
    {"VarInt performance", varIntPerf, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass);

Status
varIntTest()
{
    Status status = StatusOk;

    uint8_t buf[10];

    struct VarIntCase {
        uint64_t value;
        size_t size;
    };
    constexpr const VarIntCase tests[] = {
        {0, 1},
        {1, 1},
        {0x7f, 1},
        {0x80, 2},
        {0x81, 2},
        {0x3fff, 2},
        {0x4000, 3},
        {0x4001, 3},
        {0x1fffff, 3},
        {0x200000, 4},
        {0x200001, 4},
        {0x0fffffff, 4},
        {0x10000000, 5},
        {0x10000001, 5},
        {0x7ffffffff, 5},
        {0x800000000, 6},
        {0x800000001, 6},
        {0x3ffffffffff, 6},
        {0x40000000000, 7},
        {0x40000000001, 7},
        {0x1ffffffffffff, 7},
        {0x2000000000000, 8},
        {0x2000000000001, 8},
        {0x0ffffffffffffff, 8},
        {0x100000000000000, 9},
        {0x100000000000001, 9},
        {0x7fffffffffffffffL, 9},
        {0x8000000000000000L, 10},
        {0x8000000000000000L, 10},
        {0xffffffffffffffffL, 10},
    };

    for (int ii = 0; ii < (int) ArrayLen(tests); ii++) {
        const VarIntCase *test = &tests[ii];
        const uint64_t value = test->value;
        const size_t size = test->size;

        size_t writeLength;
        bool success =
            encodeVarint<false, true>(value, buf, sizeof(buf), &writeLength);
        assert(success);
        assert(writeLength == size);

        size_t readLength;
        uint64_t readVal;
        success =
            decodeVarint<uint64_t>(buf, sizeof(buf), &readVal, &readLength);
        assert(success);
        assert(readVal == value);
        assert(readLength == size);
    }

    return status;
}

Status
zigZagTest()
{
    Status status = StatusOk;

    struct ZigZag32Case {
        int32_t intVal;
        uint32_t zigZagVal;
    };
    constexpr const ZigZag32Case tests32[] = {
        {0, 0},
        {-1, 1},
        {1, 2},
        {-2, 3},
        {2, 4},
        {2147483647, 0xfffffffe},       // INT MAX
        {-2147483647 - 1, 0xffffffff},  // INT MIN
    };

    for (int ii = 0; ii < (int) ArrayLen(tests32); ii++) {
        const ZigZag32Case *test = &tests32[ii];
        assert(zigZagEncode32(test->intVal) == test->zigZagVal);
        assert(zigZagDecode32(test->zigZagVal) == test->intVal);
        assert(zigZagEncode32(zigZagDecode32(test->zigZagVal)) ==
               test->zigZagVal);
    }

    struct ZigZag64Case {
        int64_t intVal;
        uint64_t zigZagVal;
    };
    constexpr const ZigZag64Case tests64[] = {
        {0, 0},
        {-1, 1},
        {1, 2},
        {-2, 3},
        {2, 4},
        {2147483647, 0xfffffffe},
        {-2147483648, 0xffffffff},
        {9223372036854775807, 0xfffffffffffffffe},  // INT MAX
        // The compiler won't let us have this value directly because the '-' is
        // treated as an operator on the literal, and -(INT_MIN) isn't
        // representable
        {-9223372036854775807 - 1, 0xffffffffffffffff},  // INT MIN
    };

    for (int ii = 0; ii < (int) ArrayLen(tests64); ii++) {
        const ZigZag64Case *test = &tests64[ii];
        assert(zigZagEncode64(test->intVal) == test->zigZagVal);
        assert(zigZagDecode64(test->zigZagVal) == test->intVal);
        assert(zigZagEncode64(zigZagDecode64(test->zigZagVal)) ==
               test->zigZagVal);
    }

    return status;
}

static uint64_t
expensiveFunc(uint64_t someNum)
{
    return (uint64_t)(cos(sin((double) someNum)) * 100);
}

Status
varIntPerf()
{
    uint64_t maxNum = 2000000;
    size_t bigBufSize = sizeof(uint64_t) * maxNum;
    uint8_t *bigBuf = (uint8_t *) memAlloc(bigBufSize);
    int numIter = 5;
    uint64_t *intArray = (uint64_t *) bigBuf;
    Stopwatch stopwatch;

    double intRead;
    double intWrite;
    double varintRead;
    double varintWrite;

    volatile uint64_t numSet;

    // Normal int array writes
    for (int iter = 0; iter < numIter; iter++) {
        for (uint64_t ii = 0; ii < maxNum; ii++) {
            intArray[ii] = expensiveFunc(ii);
        }
    }
    stopwatch.stop();
    {
        unsigned long long millis = stopwatch.getElapsedNSecs() / NSecsPerMSec;
        intWrite = ((double) millis) / 1000.0;
        fprintf(stderr,
                "wrote f(x) 0-%lu ints in %f secs; %lu bytes\n",
                maxNum,
                intWrite,
                bigBufSize);
    }

    // Normal int array reads
    stopwatch.restart();
    for (int iter = 0; iter < numIter; iter++) {
        for (uint64_t ii = 0; ii < maxNum; ii++) {
            numSet = expensiveFunc(intArray[ii]);
        }
    }
    stopwatch.stop();
    {
        unsigned long long millis = stopwatch.getElapsedNSecs() / NSecsPerMSec;
        intRead = ((double) millis) / 1000.0;
        fprintf(stderr, "read & f(x) 0-%lu ints in %f secs\n", maxNum, intRead);
    }

    size_t bufSpace;
    // Varint writes
    stopwatch.restart();
    for (int iter = 0; iter < numIter; iter++) {
        size_t bufUsed = 0;
        for (uint64_t ii = 0; ii < maxNum; ii++) {
            size_t sizeWritten;
            encodeVarint<false, false>(expensiveFunc(ii),
                                       bigBuf + bufUsed,
                                       0,
                                       &sizeWritten);
            bufUsed += sizeWritten;
        }
        bufSpace = bufUsed;
    }
    stopwatch.stop();
    {
        unsigned long long millis = stopwatch.getElapsedNSecs() / NSecsPerMSec;
        varintWrite = ((double) millis) / 1000.0;
        fprintf(stderr,
                "wrote f(x) 0-%lu varints in %f secs; %lu bytes\n",
                maxNum,
                varintWrite,
                bufSpace);
    }

    // Varint reads
    stopwatch.restart();
    for (int iter = 0; iter < numIter; iter++) {
        size_t bufUsed = 0;
        for (uint64_t ii = 0; ii < maxNum; ii++) {
            uint64_t val;
            size_t sizeRead;
            decodeVarint<uint64_t>(bigBuf + bufUsed,
                                   bigBufSize - bufUsed,
                                   &val,
                                   &sizeRead);
            numSet = expensiveFunc(val);
            bufUsed += sizeRead;
        }
    }

    (void) numSet;
    stopwatch.stop();
    {
        unsigned long long millis = stopwatch.getElapsedNSecs() / NSecsPerMSec;
        varintRead = ((double) millis) / 1000.0;
        fprintf(stderr,
                "read & f(x) 0-%lu ints in %f secs\n",
                maxNum,
                varintRead);
    }

    fprintf(stderr,
            "read speedup: %fx\nwrite speedup: %fx\nspace usage %fx\n",
            intRead / varintRead,
            intWrite / varintWrite,
            (double) bufSpace / (double) bigBufSize);

    memFree(bigBuf);
    return StatusOk;
}

int
main(int argc, char *argv[])
{
    int numFailedTests = 0;
    varIntTest();
    verifyOk(memTrackInit());
    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);
    memTrackDestroy(false);
    return numFailedTests;
}
