// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef QUERYMANAGERTESTS_H
#define QUERYMANAGERTESTS_H

#include "primitives/Primitives.h"
#include "dataset/Dataset.h"
#include "runtime/Semaphore.h"

#include "test/QA.h"

class QueryManagerTests
{
  public:
    struct Params {
        unsigned numQueryCreationThread;
        unsigned numLoop;
        unsigned numStateRequestThread;
        unsigned numCancellationThread;
        unsigned numLoopCheckingState;
        unsigned numLoopCancelQuery;
        char loadPath[QaMaxQaDirLen + 1];
        char nonstopRetinaDir[QaMaxQaDirLen + 1];
        char errorRetinaDir[QaMaxQaDirLen + 1];
        bool queryLatencyOptimized;
    };

    static constexpr Params DefaultParams = {
        .numQueryCreationThread = 1,
        .numLoop = 1,
        .numStateRequestThread = 8,
        .numCancellationThread = 8,
        .numLoopCheckingState = 8,
        .numLoopCancelQuery = 8,
        .loadPath = "",
        .nonstopRetinaDir = "",
        .errorRetinaDir = "",
        .queryLatencyOptimized = false,
    };

    static constexpr const char *SessionUserName = "QueryManagerTestUser";
    static constexpr const char *SessionName = "QueryManagerTestSession";

    // Amount of time before retrying after a retryable error occurs
    // (e.g. StatusApiWouldBlock).
    static constexpr uint64_t RetryTimeUSecs = 1 * USecsPerMSec;
    // Maximum amount of time before retrying (using exponential backoff).
    static constexpr uint64_t MaxRetryTimeUSecs = 8 * USecsPerSec;

    static constexpr int MaxSleepTime = 120;
    static constexpr int MinSleepTime = 1;

    QueryManagerTests();
    virtual ~QueryManagerTests() {}

    static uint64_t retryTimeUSecs(uint64_t oldValue);
    static void *checkQueryState(void *arg);
    static void *cancelQuery(void *arg);

    static Status initTest();
    static Status createSession();
    static Status uploadUdfs();
    static Status loadDataset();
    static Status uploadRetina(const char *path,
                               const char *retinaName,
                               bool force);
    static Status uploadRetinas();

    static void deleteRetina(const char *retinaName);
    static void cleanoutTest();
    static Status stringQueryTest();
    static Status retinaQueryTest();

    static void initParams();
    static Status parseConfig(Config::Configuration *config,
                              char *key,
                              char *value,
                              bool stringentRules);
    static Params params;
    static bool paramsInited;
    static bool sessionCreated;
    static XcalarApiUserId sessionUser;
    static uint64_t sessionId;

    static bool datasetLoaded;
    static char datasetName[Dataset::MaxNameLen + 1];

    static Xid testId;

    static constexpr const char *LoadQuerySkel =
        "[\n"
        "{\n"
        "    \"operation\": \"XcalarApiBulkLoad\",\n"
        "    \"comment\": \"\",\n"
        "    \"args\": {\n"
        "        \"dest\": \".XcalarDS.%s\",\n"
        "        \"loadArgs\": {\n"
        "            \"sourceArgs\": {\n"
        "                \"targetName\": \"Default Shared Root\",\n"
        "                \"path\": \"%s\",\n"
        "                \"fileNamePattern\": \"\",\n"
        "                \"recursive\": false\n"
        "            },\n"
        "            \"parseArgs\": {\n"
        "                \"parserFnName\": \"default:parseJson\",\n"
        "                \"parserArgJson\": \"{}\"\n"
        "            },\n"
        "            \"size\": 10737418240\n"
        "        }\n"
        "    }\n"
        "}]";

    Status runTests();

  private:
    virtual Status executeRandomQuery(char *queryNameBuf,
                                      size_t queryNameBufSize) = 0;

    void *testThread();

    Semaphore testThreadsSem_;
};

class QueryManagerTestsQueryString final : public QueryManagerTests
{
  public:
    QueryManagerTestsQueryString() {}

    // "non-stop" query, making use of a UDF that sleeps for an hour
    static constexpr const char *NonStoppingUdfModule = "qmNonStoppingModule";
    static constexpr const char *NonStoppingUdfSource =
        "import time\n"
        "def sleep(dur):\n"
        "    time.sleep(dur)\n";

    static constexpr const char *NonStoppingQuerySkel =
        "[\n"
        "{\n"
        "    \"operation\": \"XcalarApiIndex\",\n"
        "    \"comment\": \"\",\n"
        "    \"tag\": \"\",\n"
        "    \"state\": \"Created\",\n"
        "    \"args\": {\n"
        "        \"source\": \"%s\",\n"
        "        \"dest\": \"QueryManagerTestsIndex_%lu\",\n"
        "        \"key\": [\n"
        "            {\n"
        "                \"name\": \"fans\",\n"
        "                \"keyFieldName\": \"fans\",\n"
        "                \"type\": \"DfUnknown\",\n"
        "                \"ordering\": \"Unordered\"\n"
        "            }\n"
        "        ],\n"
        "        \"prefix\": \"p\",\n"
        "        \"dhtName\": \"\",\n"
        "        \"delaySort\": false,\n"
        "        \"broadcast\": false\n"
        "    },\n"
        "    \"annotations\": {}\n"
        "},\n"
        "{\n"
        "    \"operation\": \"XcalarApiMap\",\n"
        "    \"comment\": \"\",\n"
        "    \"tag\": \"\",\n"
        "    \"state\": \"Created\",\n"
        "    \"args\": {\n"
        "        \"source\": \"QueryManagerTestsIndex_%lu\",\n"
        "        \"dest\": \"QueryManagerTestsMap_%lu\",\n"
        "        \"eval\": [\n"
        "            {\n"
        "                \"evalString\": \"%s:sleep(1000000)\",\n"
        "                \"newField\": \"tempField\"\n"
        "            }\n"
        "        ],\n"
        "        \"icv\": false\n"
        "    },\n"
        "    \"annotations\": {}\n"
        "}\n"
        "]";

    static constexpr const char *NonStoppingQueryName = "nonStoppingQuery";

    // query for an error index query due to none existed dataset
    static constexpr const char *ErrorQuery =
        "index --key fans --dataset .XcalarDS.None --dsttable indexT --prefix "
        "p;";
    static constexpr const char *ErrorQueryName = "errorQuery";

  private:
    Status executeRandomQuery(char *queryNameBuf,
                              size_t queryNameBufSize) override;
};

class QueryManagerTestsRetina final : public QueryManagerTests
{
  public:
    QueryManagerTestsRetina() {}
    ~QueryManagerTestsRetina() override {}

    static constexpr size_t MaxRetinaName = 256;

    // retina for non-stopping join
    static constexpr const char *NonStoppingRetinaFile = "nonstopRetina.tar.gz";
    static constexpr const char *NonStoppingRetinaName = "nonStoppingRetina";

    // retina for error query
    static constexpr const char *ErrorRetinaFile = "errorRetina2.tar.gz";
    static constexpr const char *ErrorRetinaName = "errorRetina";

    static constexpr const char *LoadPathParameter = "LibQmQaYelpUserPath";

  private:
    Status executeRandomQuery(char *queryNameBuf,
                              size_t queryNameBufSize) override;
    void *executeRetina();
};

#endif  // QUERYMANAGERTESTS_H
