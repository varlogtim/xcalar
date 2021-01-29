// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "StrlFunc.h"
#include "test/FuncTests/LibNsTests.h"
#include "util/Random.h"
#include "util/System.h"
#include "util/MemTrack.h"
#include "runtime/Runtime.h"
#include "sys/XLog.h"
#include "ns/LibNs.h"
#include "LibNsFuncTestConfig.h"
#include "test/QA.h"  // must be last

static constexpr const char *moduleName = "libNsFuncTest";

static constexpr uint64_t UsePhysicalCoreCount = 0;

// These parameters are changable via the default.cfg file.
static uint64_t numberOfCores = UsePhysicalCoreCount;
static uint64_t numThreadPerCore = 1;

static bool enableMultiOpTest = true;
static uint64_t deletionFreq = 4;
static uint64_t numObjPerThread = 256;
static uint64_t numLoop = 2;

static bool enableCreateObjTest = true;
static uint64_t numObjCreationTest = 1024;

static bool enablePathInfoTest = true;
static uint64_t numObjPathInfoTest = 256;
static uint64_t numPathInfoCalls = 32;

static bool enableHugeObjTest = true;
static uint64_t numHugeObjects = 256;

// The path names used by each thread are specific to that
// thread.  The thread can then append to the common name
// as needed.  This is useful to avoid having threads interfere
// with each other.
static constexpr const char *pathNameCommonFormatThread =
    "/libns/functest/testname-%s/node%d/thread%d/";

// Test object derived from the LibNs base object.
class TestObject : public NsObject
{
  public:
    TestObject(uint64_t cookie) : NsObject(sizeof(TestObject))
    {
        cookie_ = cookie;
    }
    uint64_t getCookie() { return cookie_; }

  private:
    uint64_t cookie_;
};

// A huge object is one larger than maximum 2pc payload size thus
// requiring message streaming to be used.
static constexpr uint64_t HugeObjectSize = 16 * MB;
class HugeObject : public NsObject
{
  public:
    uint64_t getCookie() { return cookie_; }
    HugeObject(uint64_t cookie) : NsObject(sizeof(HugeObject))
    {
        cookie_ = cookie;
    }

  private:
    uint64_t cookie_;
    uint8_t data[HugeObjectSize];
};

static void *
multiOpTest(void *arg)
{
    uint64_t numObj = numObjPerThread;
    RandHandle rndHandle;
    pid_t tid = sysGetTid();
    LibNs *libNs = LibNs::get();
    Config *config = Config::get();

    char pathNameCommon[LibNsTypes::MaxPathNameLen / 2];
    char pathName[LibNsTypes::MaxPathNameLen + 1];
    LibNsTypes::NsHandle nsHandle;
    LibNsTypes::NsId nsId;
    LibNsTypes::NsId publishedObjNsId[numObj];
    Status status;
    bool objDeleted;

    rndInitHandle(&rndHandle, tid);
    memZero(publishedObjNsId, sizeof(publishedObjNsId));

    uint64_t count = 0;
    size_t ret;

    for (unsigned ii = 0; ii < numObj; ++ii) {
        ret = snprintf(pathNameCommon,
                       sizeof(pathNameCommon),
                       pathNameCommonFormatThread,
                       "libnstest",
                       config->getMyNodeId(),
                       tid);
        assert(ret < sizeof(pathNameCommon));

        ret = snprintf(pathName,
                       sizeof(pathName),
                       "%spath-%d",
                       pathNameCommon,
                       ii);
        assert(ret < sizeof(pathName));

        // Publish the path name which uses a default, mimimal
        // object (refCnt => 1).

        nsId = libNs->publish(pathName, &status);
        assert(status == StatusOk);

#ifdef LIBNS_DEBUG
        xSyslog(moduleName, XlogDebug, "publish: %s", pathName);
#endif

        // Replace with a client-specific object.

        // Use the NsId as the "cookie" for the object.
        TestObject newObj(nsId);

        // refCnt => 2
        nsHandle = libNs->open(nsId, LibNsTypes::WriterExcl, &status);
        assert(status == StatusOk);

        // refCnt remains 2
        nsHandle = libNs->updateNsObject(nsHandle, &newObj, &status);
        assert(status == StatusOk);

        // refCnt => 1
        status = libNs->close(nsHandle, &objDeleted);
        assert(status == StatusOk);
        assert(objDeleted == false);

        // Add the NsId for the published path to our array.

        publishedObjNsId[ii] = nsId;
        count++;

        // Every deleteFrequency randomly delete one object

        if (count % deletionFreq == 0) {
            uint64_t toDeleteIdx;
            do {
                toDeleteIdx = rndGenerate32(&rndHandle) % (count);
            } while (publishedObjNsId[toDeleteIdx] == 0);

            nsId = publishedObjNsId[toDeleteIdx];

            // A ref count sanity check...
            LibNsTypes::NsHandle tempHandle;
            tempHandle = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
            assert(status == StatusOk);
            status = libNs->close(tempHandle, &objDeleted);
            assert(status == StatusOk);
            assert(objDeleted == false);

#ifdef LIBNS_DEBUG
            xSyslog(moduleName,
                    XlogDebug,
                    "markForRemovalAndDecRefCnt: '%lu'",
                    nsId);
#endif

            // The only ref count is from the publish so this should
            // be enough to get rid of it.

            status = libNs->remove(nsId, &objDeleted);
            assert(status == StatusOk);
            assert(objDeleted == true);

            // Ensure it is gone by attempting to open and getting an error.

            tempHandle = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
            assert(status == StatusNsNotFound);

            publishedObjNsId[toDeleteIdx] = 0;
        }

        // Randomly pick an object to test each feature.

        uint64_t randIdx;
        do {
            randIdx = rndGenerate32(&rndHandle) % count;
        } while (publishedObjNsId[randIdx] == 0);

        char *pathName;
        NsObject *nsObj;
        TestObject *testObj;

        nsId = publishedObjNsId[randIdx];

        // Get the path name associated with the NsId and open it.

        pathName = libNs->getPathName(nsId);
        assert(pathName != NULL);

        // refCnt++
        nsHandle = libNs->open(pathName, LibNsTypes::ReaderShared, &status);
        assert(status == StatusOk);

        // Ensure the NsId obtained via path name is as expected.

        assert(libNs->getNsId(pathName) == nsId);

        // Get the object via the NsId.

        nsObj = libNs->getNsObject(nsHandle);
        assert(nsObj != NULL);

        // The cookie in the object must match what was used when the object
        // was created.

        testObj = (TestObject *) nsObj;

        assert(testObj->getCookie() == nsId);

        // refCnt-- to release open()
        status = libNs->close(nsHandle, NULL);
        assert(status == StatusOk);

        memFree(pathName);
        memFree(nsObj);
    }

    // Get a list of the path names created by this thread and
    // ensure there's one for each one that we created (and didn't delete).

    LibNsTypes::PathInfoOut *pathInfo = NULL;
    char pattern[LibNsTypes::MaxPathNameLen];
    snprintf(pattern, sizeof(pattern), "%s*", pathNameCommon);
    size_t numReturned = libNs->getPathInfo(pattern, &pathInfo);
    assert(numReturned != 0);

    for (uint64_t ii = 0; ii < numObj; ++ii) {
        if (publishedObjNsId[ii] != 0) {
            bool found = false;
            for (uint64_t jj = 0; jj < numReturned; ++jj) {
                if (pathInfo[jj].nsId == publishedObjNsId[ii]) {
                    found = true;
                    break;
                }
            }
            assert(found);
        }
    }

    memFree(pathInfo);

    // Clean up.  Marking for removal and decrementing the ref count should
    // get rid of the path name and object.

    for (uint64_t ii = 0; ii < numObj; ++ii) {
        if (publishedObjNsId[ii] != 0) {
            status = libNs->remove(publishedObjNsId[ii], &objDeleted);
            assert(status == StatusOk);
            assert(objDeleted == true);

            // Check that it is gone
            char *path = libNs->getPathName(publishedObjNsId[ii]);
            assert(path == NULL);
        }
    }

    return NULL;
}

static void *
testPathInfo(void *arg)
{
    Status status;
    pid_t tid = sysGetTid();
    char pathNameCommon[LibNsTypes::MaxPathNameLen / 2];
    char pathName[LibNsTypes::MaxPathNameLen + 1];
    Config *config = Config::get();
    LibNs *libNs = LibNs::get();
    size_t ret;
    LibNsTypes::NsId nsId;
    bool objDeleted;

    // Create unique path names

    for (unsigned ii = 0; ii < numObjPathInfoTest; ii++) {
        ret = snprintf(pathNameCommon,
                       sizeof(pathNameCommon),
                       pathNameCommonFormatThread,
                       "pathInfoTest",
                       config->getMyNodeId(),
                       tid);
        assert(ret < sizeof(pathNameCommon));

        ret = snprintf(pathName,
                       sizeof(pathName),
                       "%spathInfo-%u",
                       pathNameCommon,
                       ii);
        assert(ret < sizeof(pathName));

        nsId = libNs->publish(pathName, &status);
        assert(status == StatusOk);
        assert(nsId != LibNsTypes::NsInvalidId);
    }

    // Get the path names that were just created

    LibNsTypes::PathInfoOut *pathInfo = NULL;
    char pattern[LibNsTypes::MaxPathNameLen];

    ret = snprintf(pattern, sizeof(pattern), "%s*", pathNameCommon);
    assert(ret < sizeof(pattern));

    size_t numReturned;
    for (unsigned ii = 0; ii < numPathInfoCalls; ii++) {
        numReturned = libNs->getPathInfo(pattern, &pathInfo);
        assert(numReturned == numObjPathInfoTest);
        assert(pathInfo != NULL);
        memFree(pathInfo);
    }

    numReturned = libNs->getPathInfo(pattern, &pathInfo);
    assert(numReturned == numObjPathInfoTest);
    assert(pathInfo != NULL);

    // Get rid of the path names

    for (unsigned ii = 0; ii < numObjPathInfoTest; ii++) {
        status = libNs->remove(pathInfo[ii].nsId, &objDeleted);
        assert(status == StatusOk);
        assert(objDeleted == true);
    }

    memFree(pathInfo);
    pathInfo = NULL;

    // Check that they are gone
    numReturned = libNs->getPathInfo(pattern, &pathInfo);
    assert(numReturned == 0);
    assert(pathInfo == NULL);

    return NULL;
}

// A number of threads are running this function and will attempt to create
// the specified number of objects.  The objects may already exist.
static void *
testCreateObj(void *arg)
{
    pid_t tid = sysGetTid();
    RandHandle rndHandle;
    char pathNameCommon[LibNsTypes::MaxPathNameLen / 2];
    char pathName[LibNsTypes::MaxPathNameLen + 1];
    rndInitHandle(&rndHandle, tid);
    Status status;
    LibNs *libNs = LibNs::get();
    Config *config = Config::get();
    LibNsTypes::NsId nsId;
    size_t ret;

    for (unsigned ii = 0; ii < numObjCreationTest * 2; ii++) {
        // Create a "random" path name within the creation range.  The
        // path name may already have been published.
        uint64_t idx = rndGenerate32(&rndHandle) % numObjCreationTest;

        ret = snprintf(pathNameCommon,
                       sizeof(pathNameCommon),
                       pathNameCommonFormatThread,
                       "testCreateObj",
                       config->getMyNodeId(),
                       tid);
        assert(ret < sizeof(pathNameCommon));

        ret = snprintf(pathName,
                       sizeof(pathName),
                       "%spath-%lu",
                       pathNameCommon,
                       idx);
        assert(ret < sizeof(pathName));

        nsId = libNs->publish(pathName, &status);

        if (status == StatusOk) {
            // Path name was published
            assert(nsId != LibNsTypes::NsInvalidId);

#ifdef LIBNS_DEBUG
            xSyslog(moduleName,
                    XlogInfo,
                    "testCreateObj: Created '%s' (%lu)",
                    pathName,
                    nsId);
#endif

        } else if (status == StatusExist) {
            // Path name already existed
            assert(nsId == LibNsTypes::NsInvalidId);
#ifdef LIBNS_DEBUG
            xSyslog(moduleName,
                    XlogInfo,
                    "testCreateObj: '%s' already exists",
                    pathName);
#endif

        } else {
            // Unexpected error.
            xSyslog(moduleName,
                    XlogErr,
                    "testCreateObj: Unexpected error when creating '%s': %s",
                    pathName,
                    strGetFromStatus(status));
            assert(0 && "testCreateObj: unexpected error");
        }
    }

    // Get rid of the above objects.

    ret = snprintf(pathName, sizeof(pathName), "%spath-*", pathNameCommon);
    assert(ret < sizeof(pathName));

#ifdef LIBNS_DEBUG
    xSyslog(moduleName, XlogInfo, "Marking '%s' for removal", pathName);
#endif

    status = libNs->removeMatching(pathName);
    assert(status == StatusOk);

    return NULL;
}

// A number of threads are running this function and will create huge objects.
static void *
hugeObjTest(void *arg)
{
    pid_t tid = sysGetTid();
    char pathNameCommon[LibNsTypes::MaxPathNameLen / 2];
    char pathName[LibNsTypes::MaxPathNameLen + 1];
    Status status;
    Config *config = Config::get();
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    LibNsTypes::NsHandle nsHandle;
    size_t ret;
    HugeObject *hugeObj = NULL;
    HugeObject *hugeObj2 = NULL;
    HugeObject *returnedObj = NULL;
    bool objDeleted;

    for (unsigned ii = 0; ii < numHugeObjects; ii++) {
        // Create a path name unique to this thread
        ret = snprintf(pathNameCommon,
                       sizeof(pathNameCommon),
                       pathNameCommonFormatThread,
                       "testHugeObj",
                       config->getMyNodeId(),
                       tid);
        assert(ret < sizeof(pathNameCommon));

        ret = snprintf(pathName,
                       sizeof(pathName),
                       "%sobject-%u",
                       pathNameCommon,
                       ii);
        assert(ret < sizeof(pathName));

        // Allocate a huge object
        hugeObj = (HugeObject *) memAllocExt(sizeof(*hugeObj), moduleName);
        assert(hugeObj != NULL);
        new (hugeObj) HugeObject(ii);

        nsId = libNs->publish(pathName, hugeObj, &status);
        assert(status == StatusOk);
        assert(nsId != LibNsTypes::NsInvalidId);

        // Once published we no longer need the local object
        memFree(hugeObj);
        hugeObj = NULL;

        // Ensure that open is able to return the object
        nsHandle = libNs->open(nsId,
                               LibNsTypes::WriterExcl,
                               (NsObject **) &returnedObj,
                               &status);
        assert(status == StatusOk);
        assert(returnedObj->getCookie() == ii);
        memFree(returnedObj);
        returnedObj = NULL;

        // Ensure that get object is also able to return the object
        returnedObj = (HugeObject *) libNs->getNsObject(nsHandle);
        assert(returnedObj != NULL);
        assert(returnedObj->getCookie() == ii);
        memFree(returnedObj);
        returnedObj = NULL;

        // Update the object with a different huge object
        hugeObj2 = (HugeObject *) memAllocExt(sizeof(*hugeObj2), moduleName);
        assert(hugeObj2 != NULL);
        new (hugeObj2) HugeObject(123456);

        nsHandle = libNs->updateNsObject(nsHandle, hugeObj2, &status);
        assert(status == StatusOk);
        memFree(hugeObj2);
        hugeObj2 = NULL;

        // Get the updated object
        returnedObj = (HugeObject *) libNs->getNsObject(nsHandle);
        assert(returnedObj != NULL);
        assert(returnedObj->getCookie() == 123456);
        memFree(returnedObj);
        returnedObj = NULL;

        // Do a pending removal
        status = libNs->remove(nsId, &objDeleted);
        assert(status == StatusOk);
        assert(objDeleted == false);

        // Close should remove it
        status = libNs->close(nsHandle, &objDeleted);
        assert(status == StatusOk);
        assert(objDeleted == true);
    }
    return NULL;
}

Status
nsTest()
{
    LibNs *libNs = LibNs::get();
    assert(libNs != NULL);
    Status status = StatusOk;
    pthread_t *threadHandle;
    uint64_t maxThreads;
    Runtime *runtime = Runtime::get();

    if (numberOfCores == 0) {
        // Wasn't specified as config parameter so use all the physical cores.
        numberOfCores = (uint64_t) XcSysHelper::get()->getNumOnlineCores();
    }

    maxThreads = numberOfCores * numThreadPerCore;

    if (enableMultiOpTest) {
        xSyslog(moduleName,
                XlogDebug,
                "Starting multiOpTest with %lu loops, %lu threads",
                numLoop,
                maxThreads);

        threadHandle = (pthread_t *) memAllocExt(sizeof(pthread_t) * maxThreads,
                                                 moduleName);
        assert(threadHandle != NULL);

        for (unsigned ii = 0; ii < numLoop; ++ii) {
            for (unsigned jj = 0; jj < maxThreads; jj++) {
                status = runtime->createBlockableThread(&threadHandle[jj],
                                                        NULL,
                                                        multiOpTest,
                                                        NULL);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogDebug,
                            "createBlockableThread failed: %s",
                            strGetFromStatus(status));
                }
                assert(status == StatusOk);
            }

            xSyslog(moduleName,
                    XlogDebug,
                    "%lu threads have been created.\n",
                    maxThreads);

            for (unsigned jj = 0; jj < maxThreads; jj++) {
                sysThreadJoin(threadHandle[jj], NULL);
            }

            xSyslog(moduleName,
                    XlogDebug,
                    "%lu threads have been joined.\n",
                    maxThreads);
        }

        memFree(threadHandle);
    } else {
        xSyslog(moduleName, XlogInfo, "MultiOpTest is not enabled");
    }

    if (enableCreateObjTest) {
        xSyslog(moduleName,
                XlogDebug,
                "Starting createObjTest with %lu threads",
                maxThreads);

        // Create a pool of threads to compete for creating new objects.

        threadHandle = (pthread_t *) memAllocExt(sizeof(pthread_t) * maxThreads,
                                                 moduleName);
        assert(threadHandle != NULL);

        for (unsigned ii = 0; ii < maxThreads; ii++) {
            status = runtime->createBlockableThread(&threadHandle[ii],
                                                    NULL,
                                                    testCreateObj,
                                                    NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "createBlockableThread failed: %s",
                        strGetFromStatus(status));
            }
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogInfo,
                "%lu creation threads have been created.",
                maxThreads);

        for (unsigned ii = 0; ii < maxThreads; ii++) {
            sysThreadJoin(threadHandle[ii], NULL);
        }

        memFree(threadHandle);
    } else {
        xSyslog(moduleName, XlogInfo, "CreateObjTest is not enabled");
    }

    if (enablePathInfoTest) {
        xSyslog(moduleName,
                XlogDebug,
                "Starting pathInfoTest with %lu threads",
                maxThreads);

        // Obtaining path info gets its own test as it uses the message stream
        // infrastructure.

        threadHandle = (pthread_t *) memAllocExt(sizeof(pthread_t) * maxThreads,
                                                 moduleName);
        assert(threadHandle != NULL);

        for (unsigned ii = 0; ii < maxThreads; ii++) {
            status = runtime->createBlockableThread(&threadHandle[ii],
                                                    NULL,
                                                    testPathInfo,
                                                    NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "createBlockableThread failed: %s",
                        strGetFromStatus(status));
            }
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogInfo,
                "%lu pathInfo threads have been created",
                maxThreads);

        for (unsigned ii = 0; ii < maxThreads; ii++) {
            sysThreadJoin(threadHandle[ii], NULL);
        }

        memFree(threadHandle);
    } else {
        xSyslog(moduleName, XlogInfo, "PathInfoTest is not enabled");
    }

    if (enableHugeObjTest) {
        xSyslog(moduleName,
                XlogDebug,
                "Starting hugeObjTest with %lu threads",
                maxThreads);

        threadHandle = (pthread_t *) memAllocExt(sizeof(pthread_t) * maxThreads,
                                                 moduleName);
        assert(threadHandle != NULL);

        for (unsigned ii = 0; ii < maxThreads; ii++) {
            status = runtime->createBlockableThread(&threadHandle[ii],
                                                    NULL,
                                                    hugeObjTest,
                                                    NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogDebug,
                        "createBlockableThread dfailed: %s",
                        strGetFromStatus(status));
            }
            assert(status == StatusOk);
        }

        xSyslog(moduleName,
                XlogInfo,
                "%lu hugeObj threads have been created",
                maxThreads);

        for (unsigned ii = 0; ii < maxThreads; ii++) {
            sysThreadJoin(threadHandle[ii], NULL);
        }

        memFree(threadHandle);
    } else {
        xSyslog(moduleName, XlogInfo, "hugeObjTest is not enabled");
    }

    return StatusOk;
}

Status
libNsFuncTestParseConfig(Config::Configuration *config,
                         char *key,
                         char *value,
                         bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibNsFuncTestConfig(LibNsFuncTestNumberOfCores)) ==
        0) {
        numberOfCores = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumThreadPerCore)) == 0) {
        numThreadPerCore = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestEnableMultiOpTest)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            enableMultiOpTest = true;
        } else if (strcasecmp(value, "false") == 0) {
            enableMultiOpTest = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestDeletionFreq)) == 0) {
        deletionFreq = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumObjPerThread)) == 0) {
        numObjPerThread = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumLoop)) == 0) {
        numLoop = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestEnableCreateObjTest)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            enableCreateObjTest = true;
        } else if (strcasecmp(value, "false") == 0) {
            enableCreateObjTest = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumObjCreationTest)) == 0) {
        numObjCreationTest = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestEnablePathInfoTest)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            enablePathInfoTest = true;
        } else if (strcasecmp(value, "false") == 0) {
            enablePathInfoTest = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumObjPathInfoTest)) == 0) {
        numObjPathInfoTest = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumPathInfoCalls)) == 0) {
        numPathInfoCalls = strtoll(value, NULL, 0);

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestEnableHugeObjTest)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            enableHugeObjTest = true;
        } else if (strcasecmp(value, "false") == 0) {
            enableHugeObjTest = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }

    } else if (strcasecmp(key,
                          strGetFromLibNsFuncTestConfig(
                              LibNsFuncTestNumHugeObjects)) == 0) {
        numHugeObjects = strtol(value, NULL, 0);

    } else {
        status = StatusUsrNodeIncorrectParams;
    }

    xSyslog(moduleName,
            XlogDebug,
            "%s changed %s to %s",
            (status == StatusOk ? "Successfully" : "Unsuccessfully"),
            key,
            value);

    return status;
}
