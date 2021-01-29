// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <libgen.h>
#include <time.h>
#include <unordered_map>
#include "StrlFunc.h"
#include "common/InitTeardown.h"
#include "util/IntHashTable.h"

#include "test/QA.h"

using std::unordered_map;

uint64_t
numberHash(unsigned number)
{
    return number;
}

class NumberedString
{
  public:
    NumberedString(unsigned num, const char *st) : number(num)
    {
        strlcpy(str, st, sizeof(str));
    }

    unsigned number;
    char str[256];
    IntHashTableHook hook;
    unsigned getNumber() const { return number; };

    void del() { delete this; }
};

// Make sure we perform as well as unordered_map on basic grinding. Will simply
// print comparisons (reasoning below). Uncomment asserts to verify comparisons.
static Status
hashtableTestCompare()
{
    static constexpr size_t numBuckets = 126271;
    static unsigned numElements = 100000;

    unsigned ii;

#ifndef __clang__
#pragma GCC diagnostic ignored "-Wlarger-than="  // Too big for GCC.
#endif
    IntHashTable<unsigned,
                 NumberedString,
                 &NumberedString::hook,
                 &NumberedString::getNumber,
                 numBuckets,
                 numberHash> *htCompare;
    unordered_map<unsigned, NumberedString *> m;

    htCompare = new IntHashTable<unsigned,
                                 NumberedString,
                                 &NumberedString::hook,
                                 &NumberedString::getNumber,
                                 numBuckets,
                                 numberHash>();

    struct timespec timeHtStart;
    struct timespec timeMapStart;
    struct timespec timeHtEnd;
    struct timespec timeMapEnd;

    struct timespec elapsedHt;
    struct timespec elapsedMap;

    printf("\n");

    // Inserts.
    clock_gettime(CLOCK_MONOTONIC, &timeMapStart);
    for (ii = 0; ii < numElements; ii++) {
        m.insert(std::make_pair(ii, new NumberedString(ii, "a number")));
    }
    clock_gettime(CLOCK_MONOTONIC, &timeMapEnd);

    clock_gettime(CLOCK_MONOTONIC, &timeHtStart);
    for (ii = 0; ii < numElements; ii++) {
        verifyOk(htCompare->insert(new NumberedString(ii, "a number")));
    }
    clock_gettime(CLOCK_MONOTONIC, &timeHtEnd);

    elapsedHt.tv_sec = timeHtEnd.tv_sec - timeHtStart.tv_sec;
    elapsedHt.tv_nsec = timeHtEnd.tv_nsec - timeHtStart.tv_nsec;
    elapsedMap.tv_sec = timeMapEnd.tv_sec - timeMapStart.tv_sec;
    elapsedMap.tv_nsec = timeMapEnd.tv_nsec - timeMapStart.tv_nsec;

    printf("Insert: IntHashTable %lu std::unordered_map %lu\n",
           elapsedHt.tv_sec * NSecsPerSec + elapsedHt.tv_nsec,
           elapsedMap.tv_sec * NSecsPerSec + elapsedMap.tv_nsec);

    // Removed because asserts that rely on timing make for brittle tests.
    // Uncomment to verify comparisons.
    // assert(elapsedHt.tv_sec < elapsedMap.tv_sec ||
    //        (elapsedHt.tv_sec == elapsedMap.tv_sec &&
    //        elapsedHt.tv_nsec < elapsedMap.tv_nsec));

    printf("Num Buckets: IntHashTable %lu std::unordered_map %lu\n",
           numBuckets,
           m.bucket_count());
    printf("Load Factor: IntHashTable %f std::unordered_map %f\n",
           (float) numElements / (float) numBuckets,
           m.load_factor());

    // Lookups.
    clock_gettime(CLOCK_MONOTONIC, &timeMapStart);
    for (ii = 0; ii < numElements; ii++) {
        m.find(ii);
    }
    clock_gettime(CLOCK_MONOTONIC, &timeMapEnd);

    clock_gettime(CLOCK_MONOTONIC, &timeHtStart);
    for (ii = 0; ii < numElements; ii++) {
        htCompare->find(ii);
    }
    clock_gettime(CLOCK_MONOTONIC, &timeHtEnd);

    elapsedHt.tv_sec = timeHtEnd.tv_sec - timeHtStart.tv_sec;
    elapsedHt.tv_nsec = timeHtEnd.tv_nsec - timeHtStart.tv_nsec;
    elapsedMap.tv_sec = timeMapEnd.tv_sec - timeMapStart.tv_sec;
    elapsedMap.tv_nsec = timeMapEnd.tv_nsec - timeMapStart.tv_nsec;

    printf("Lookup: IntHashTable %lu std::unordered_map %lu\n",
           elapsedHt.tv_sec * NSecsPerSec + elapsedHt.tv_nsec,
           elapsedMap.tv_sec * NSecsPerSec + elapsedMap.tv_nsec);

    // Reasoning for removal above.
    // assert(elapsedHt.tv_sec < elapsedMap.tv_sec ||
    //        (elapsedHt.tv_sec == elapsedMap.tv_sec &&
    //         elapsedHt.tv_nsec < elapsedMap.tv_nsec));

    // Delete all.
    clock_gettime(CLOCK_MONOTONIC, &timeMapStart);
    for (auto &mapEntry : m) {
        delete mapEntry.second;
    }
    m.clear();
    clock_gettime(CLOCK_MONOTONIC, &timeMapEnd);

    clock_gettime(CLOCK_MONOTONIC, &timeHtStart);
    htCompare->removeAll(&NumberedString::del);
    clock_gettime(CLOCK_MONOTONIC, &timeHtEnd);

    elapsedHt.tv_sec = timeHtEnd.tv_sec - timeHtStart.tv_sec;
    elapsedHt.tv_nsec = timeHtEnd.tv_nsec - timeHtStart.tv_nsec;
    elapsedMap.tv_sec = timeMapEnd.tv_sec - timeMapStart.tv_sec;
    elapsedMap.tv_nsec = timeMapEnd.tv_nsec - timeMapStart.tv_nsec;

    printf("Remove: IntHashTable %lu std::unordered_map %lu\n",
           elapsedHt.tv_sec * NSecsPerSec + elapsedHt.tv_nsec,
           elapsedMap.tv_sec * NSecsPerSec + elapsedMap.tv_nsec);

    // Don't assert on speed of removeAll. This isn't something we're
    // optimizing.

    printf("\n");

    delete htCompare;
    return StatusOk;
}

static TestCase testCases[] = {
    {"hashtable tests: IntHashTable comparisons",
     hashtableTestCompare,
     TestCaseEnable,
     ""},
};

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];

    char *dirTest = dirname(argv[0]);
    char dirConfig[1024];

    strlcpy(dirConfig, dirTest, sizeof(dirConfig));

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s%s",
             dirConfig,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::Basic,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Active on Physical */,
                                BufferCacheMgr::TypeNone));

    numTestsFailed = qaRunTestSuite(testCases,
                                    ArrayLen(testCases),
                                    TestCaseOptDisableIsPass);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
