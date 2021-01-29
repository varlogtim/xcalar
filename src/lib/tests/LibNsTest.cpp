// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <semaphore.h>
#include <errno.h>
#include <stdio.h>
#include <libgen.h>

#include "StrlFunc.h"
#include "ns/LibNs.h"
#include "util/Random.h"
#include "util/System.h"
#include "config/Config.h"
#include "primitives/Primitives.h"
#include "msg/Message.h"
#include "util/MemTrack.h"
#include "common/InitTeardown.h"
#include "libapis/LibApisCommon.h"
#include "table/TableNs.h"
#include "sys/XLog.h"

#include "test/QA.h"  // Must be last

static constexpr const char *moduleName = "nstest";

static constexpr const char *genericPathName =
    "/dataset/test/generic/path/name";
static constexpr const char *removalPathName = "/dataset/test/pending/removal";
static constexpr const char *sharksPathName = "/dataset/sharks";
static constexpr const char *hugeObjPathName = "/libns/test/hughobj/path/name";

const char *cfgFile = "test-config.cfg";
char fullCfgFilePath[255];

static Status nsBasicChecks();
static Status setUp();
static Status tearDown();

static TestCase testCases[] = {
    {"set up", setUp, TestCaseEnable, ""},
    {"Namespace basic checks", nsBasicChecks, TestCaseEnable, ""},
    {"tear down", tearDown, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask = (TestCaseOptionMask)(
    TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime | TestCaseSetTxn);

class MyObject : public NsObject
{
  public:
    uint64_t getCookie() { return cookie_; }
    MyObject(uint64_t cookie) : NsObject(sizeof(MyObject)) { cookie_ = cookie; }

    void setCookie(uint64_t cookieIn) { cookie_ = cookieIn; }

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

void
basicTests()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    LibNsTypes::NsHandle nsHandle;
    LibNsTypes::NsHandle tmpHandle;
    static constexpr const unsigned NumArr = 16;
    LibNsTypes::NsHandle hArr[NumArr];
    bool objDeleted;

    // Publish (refCnt is 1)
    nsId = libNs->publish("/udf/cust3/parse", &status);
    assert(status == StatusOk);
    assert(nsId != LibNsTypes::NsInvalidId);

    //
    // Test Read only access
    //

    // Open for read-access (refCnt is 2)
    nsHandle = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusOk);

    // Open as writer for read shared write exclusive fails
    tmpHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclWriter, &status);
    assert(status == StatusAccess);

    // Open as reader for read shared write exclusive fails
    tmpHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclReader, &status);
    assert(status == StatusAccess);

    // Open for read write exclusive access fails
    tmpHandle = libNs->open(nsId, LibNsTypes::WriterExcl, &status);
    assert(status == StatusAccess);

    // Attempt to update, should fail as it's read-only.
    MyObject newObj(123321);
    tmpHandle = libNs->updateNsObject(nsHandle, &newObj, &status);
    assert(status == StatusAccess);

    // Open as read-access succeeds
    for (unsigned ii = 0; ii < NumArr; ii++) {
        hArr[ii] = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
        assert(status == StatusOk);
    }
    for (unsigned ii = 0; ii < NumArr; ii++) {
        status = libNs->close(hArr[ii], &objDeleted);
        assert(status == StatusOk);
        assert(objDeleted == false);
    }

    // Close it so that it can be reopened (refCnt is 1)
    status = libNs->close(nsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    //
    // Test read shared write exclusive access
    //

    // Open as writer for read shared write exclusive (refCnt is 2)
    nsHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclWriter, &status);
    assert(status == StatusOk);

    // Open for read-access fails
    tmpHandle = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusAccess);

    // Open for read-write exclusive access fails
    tmpHandle = libNs->open(nsId, LibNsTypes::WriterExcl, &status);
    assert(status == StatusAccess);

    // Open as writer for read shared write exclusive fails
    tmpHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclWriter, &status);
    assert(status == StatusAccess);

    // Attempt to update, should succeed.
    tmpHandle = libNs->updateNsObject(nsHandle, &newObj, &status);
    assert(status == StatusOk);

    // Open as reader for read shared write exclusive succeeds
    for (unsigned ii = 0; ii < NumArr; ii++) {
        hArr[ii] =
            libNs->open(nsId, LibNsTypes::ReadSharedWriteExclReader, &status);
        assert(status == StatusOk);
    }
    for (unsigned ii = 0; ii < NumArr; ii++) {
        status = libNs->close(hArr[ii], &objDeleted);
        assert(status == StatusOk);
        assert(objDeleted == false);
    }

    // Open as writer for read shared write exclusive fails
    tmpHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclWriter, &status);
    assert(status == StatusAccess);

    // Attempt to update, should succeed.
    tmpHandle = libNs->updateNsObject(nsHandle, &newObj, &status);
    assert(status == StatusOk);

    // Close it so that it can be reopened (refCnt is 1)
    status = libNs->close(nsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    //
    // Test read write exclusive access
    //

    // refCnt is 2
    nsHandle = libNs->open(nsId, LibNsTypes::WriterExcl, &status);
    assert(status == StatusOk);

    // Open for read-access fails
    tmpHandle = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusAccess);

    // Open as writer for read shared write exclusive fails
    tmpHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclWriter, &status);
    assert(status == StatusAccess);

    // Open as reader for read shared write exclusive fails
    tmpHandle =
        libNs->open(nsId, LibNsTypes::ReadSharedWriteExclReader, &status);
    assert(status == StatusAccess);

    // Update (refCnt remains 2)
    nsHandle = libNs->updateNsObject(nsHandle, &newObj, &status);
    assert(status == StatusOk);

    // Get the object (refCnt is 3)
    MyObject *myObj = (MyObject *) libNs->getNsObject(nsHandle);
    assert(myObj != NULL);
    assert(myObj->getCookie() == 123321);
    memFree(myObj);

    // mark for removal and release ref count for publish
    status = libNs->remove(nsId, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    // release ref count from open
    status = libNs->close(nsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == true);

    // Ensure handle is no longer valid.

    NsObject *tmpObj;

    tmpObj = libNs->getNsObject(nsHandle);
    assert(tmpObj == NULL);

    tmpHandle = libNs->updateNsObject(nsHandle, &newObj, &status);
    assert(status == StatusNsNotFound);

    status = libNs->close(nsHandle, NULL);
    assert(status == StatusNsNotFound);

    // Ensure nsId is no longer valid

    tmpHandle = libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusNsNotFound);

    char *tmpPath = libNs->getPathName(nsId);
    assert(tmpPath == NULL);

    status = libNs->remove(nsId, NULL);
    assert(status == StatusNsNotFound);
}

void
openGetObjTest()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    LibNsTypes::NsId nsId;
    MyObject myObj(5555);
    MyObject *returnedObj = NULL;
    bool objDeleted;

    // Publish path name and object
    nsId = libNs->publish(sharksPathName, &myObj, &status);
    assert(status == StatusOk);
    assert(nsId != LibNsTypes::NsInvalidId);

    // Ensure that open is able to also return the object
    nsHandle = libNs->open(nsId,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &returnedObj,
                           &status);
    assert(status == StatusOk);
    assert(returnedObj->getCookie() == 5555);
    memFree(returnedObj);

    status = libNs->close(nsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    status = libNs->remove(nsId, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == true);
}

void
getHugeObjTest()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    LibNsTypes::NsId nsId;
    HugeObject *hugeObj = NULL;
    HugeObject *returnedObj = NULL;
    bool objDeleted;

    // Allocate our huge object
    hugeObj = (HugeObject *) memAllocExt(sizeof(*hugeObj), moduleName);
    assert(hugeObj != NULL);
    new (hugeObj) HugeObject(777);

    // Publish path name and the huge object (huge
    nsId = libNs->publish(hugeObjPathName, hugeObj, &status);
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
    assert(returnedObj->getCookie() == 777);
    memFree(returnedObj);
    returnedObj = NULL;

    // Ensure that get object is also able to return the object
    returnedObj = (HugeObject *) libNs->getNsObject(nsHandle);
    assert(returnedObj != NULL);
    assert(returnedObj->getCookie() == 777);
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

void
pathNameValidation()
{
    // Path name validation checks
    LibNs *libNs = LibNs::get();
    NodeId nodeId;

    // Valid path
    assert(libNs->isValidPathName("/path/foo", &nodeId) == true);

    // Valid path with nodeId
    assert(libNs->isValidPathName("/nodeId-0/path/foo", &nodeId) == true);
    assert(nodeId == 0);

    // Fail NULL path names
    assert(libNs->isValidPathName(NULL, &nodeId) == false);

    // Fail '/' path names
    assert(libNs->isValidPathName("/", &nodeId) == false);

    // Fail non-fully qualified path names
    assert(libNs->isValidPathName("a", &nodeId) == false);

    // Fail "//" in pathnames
    assert(libNs->isValidPathName("/double/slash//not/valid", &nodeId) ==
           false);
    assert(libNs->isValidPathName("//", &nodeId) == false);

    // Fail trailing '/' path names
    assert(libNs->isValidPathName("/trailing/slash/", &nodeId) == false);

    // Path with invalid nodeId. Note that this is a 1-node cluster
    assert(libNs->isValidPathName("/nodeId-1/path/foo", &nodeId) == false);
}

void
markForRemovalCheck()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    LibNsTypes::NsHandle nsHandle;
    bool objDeleted;

    // Publish (refCnt is 1)
    nsId = libNs->publish("/xcalar/components/libns", &status);
    assert(status == StatusOk);

    // Open (refCnt is 2)
    nsHandle = libNs->open(nsId, LibNsTypes::WriterExcl, &status);
    assert(status == StatusOk);

    // Remove (refCnt is 1)
    status = libNs->remove(nsId, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    //
    // Disallowed with markForRemoval set
    //

    // Attempt open
    LibNsTypes::NsHandle h =
        libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusPendingRemoval);

    // Update is allowed even if pending removal.
    MyObject newObj(5555);
    h = libNs->updateNsObject(nsHandle, &newObj, &status);
    assert(status == StatusOk);

    // Attempt to get object
    MyObject *myObj = (MyObject *) libNs->getNsObject(nsHandle);
    assert(myObj == NULL);

    // Attempt to get nsid
    LibNsTypes::NsId n = libNs->getNsId("/xcalar/components/libns");
    assert(n == LibNsTypes::NsInvalidId);

    // Attempt to get path name
    char *path = libNs->getPathName(nsId);
    assert(path == NULL);

    //
    // Allowed with markForRemoval set
    //

    // close to offset open (refCnt is 1)
    status = libNs->close(h, NULL);
    assert(status == StatusOk);

    // Remove it... fails as the above close() deletes the path name
    status = libNs->remove(nsHandle.nsId, NULL);
    assert(status == StatusNsNotFound);
}

void
publishTests()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    NsObject nsObject(sizeof(NsObject));
    char *pathName;
    bool objDeleted;

    // Publish some path names
    LibNsTypes::NsId raidersNsId = libNs->publish("/dataset/raiders", &status);
    assert(raidersNsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    LibNsTypes::NsId ninersNsId = libNs->publish("/dataset/niners", &status);
    assert(ninersNsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    // Publish using a name that is already in use
    nsId = libNs->publish("/dataset/raiders", &status);
    assert(nsId == LibNsTypes::NsInvalidId);
    assert(status == StatusExist);

    // Publish with a base-class object
    LibNsTypes::NsId raidersObjNsId =
        libNs->publish("/dataset/raiders/withObj", &nsObject, &status);
    assert(raidersObjNsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    // Get the NsId for a published path...
    nsId = libNs->getNsId("/dataset/raiders");
    assert(nsId == raidersNsId);

    // ...and use it to get the path name
    pathName = libNs->getPathName(nsId);
    assert(pathName != NULL);
    assert(strcmp(pathName, "/dataset/raiders") == 0);
    memFree(pathName);

    // Check that publish with pending removal returns correct error.
    nsId = libNs->publish(removalPathName, &status);
    assert(nsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    LibNsTypes::NsHandle nsHandle =
        libNs->open(nsId, LibNsTypes::WriterExcl, &status);
    assert(status == StatusOk);

    // Guaranteed to have exclusive access.
    status = libNs->remove(nsId, NULL);
    assert(status == StatusOk);
    nsId = libNs->publish(removalPathName, &status);
    assert(status == StatusPendingRemoval);
    assert(nsId == LibNsTypes::NsInvalidId);

    status = libNs->close(nsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == true);

    // Ensure path name is gone
    nsId = libNs->getNsId(removalPathName);
    assert(nsId == LibNsTypes::NsInvalidId);

    status = libNs->remove(ninersNsId, NULL);
    assert(status == StatusOk);
    status = libNs->remove(raidersNsId, NULL);
    assert(status == StatusOk);
    status = libNs->remove(raidersObjNsId, NULL);
    assert(status == StatusOk);

    // To publish an object with auto-remove means the object is removed when
    // a close transitions the ref count from 2 to 1.  This means the
    // object doesn't require an explicit remove.
    LibNsTypes::NsId autoRemoveNsId =
        libNs->publishWithAutoRemove("/dataset/autoRemove/object",
                                     &nsObject,
                                     &status);
    assert(autoRemoveNsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    LibNsTypes::NsHandle autoRemoveNsHandle =
        libNs->open(autoRemoveNsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusOk);
    // Since this is a autoRemove object, this close will remove it.
    status = libNs->close(autoRemoveNsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == true);
    // Ensure it is gone
    status = libNs->remove(autoRemoveNsId, NULL);
    assert(status == StatusNsNotFound);

    // Create another autoRemove object and show existing usage pattern
    // (pending removal) still applies.
    autoRemoveNsId =
        libNs->publishWithAutoRemove("/dataset/another/autoRemove/object",
                                     &nsObject,
                                     &status);
    assert(autoRemoveNsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    autoRemoveNsHandle =
        libNs->open(autoRemoveNsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusOk);

    // Mark it as pending removal
    status = libNs->remove(autoRemoveNsId, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    // This should remove it
    status = libNs->close(autoRemoveNsHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == true);
}

void
updateTests()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;

    // Publish with a client-specific object (refCnt is 1)
    MyObject myObj(123456);
    LibNsTypes::NsId sharksNsId =
        libNs->publish(sharksPathName, &myObj, &status);
    assert(sharksNsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    // Open the object with read-write access (refCnt is 2)
    LibNsTypes::NsHandle sharksHandle =
        libNs->open(sharksNsId, LibNsTypes::WriterExcl, &status);
    assert(status == StatusOk);

    // Get the object and verify it's the one expected (refCnt is 3)
    MyObject *returnedObj;
    returnedObj = (MyObject *) libNs->getNsObject(sharksHandle);
    assert(returnedObj != NULL);
    assert(returnedObj->getCookie() == 123456);
    memFree(returnedObj);

    // Try to open the path name for a second read-write access
    nsHandle = libNs->open(sharksPathName, LibNsTypes::WriterExcl, &status);
    assert(status == StatusAccess);

    // Update the object
    MyObject newObj(777);
    sharksHandle = libNs->updateNsObject(sharksHandle, &newObj, &status);
    assert(status == StatusOk);

    // Attempt to open the path name for readonly access.  This is not
    // allowed as there's a read-write open (which is an exclusive open).
    LibNsTypes::NsHandle nsROHandle =
        libNs->open(sharksPathName, LibNsTypes::ReaderShared, &status);
    assert(status == StatusAccess);

    // Closing an invalid handle.
    status = libNs->close(nsROHandle, NULL);
    assert(status == StatusNsNotFound);

    // Use the handle to get the object.  It should be the updated object.
    // (refCnt is 4)
    returnedObj = (MyObject *) libNs->getNsObject(sharksHandle);
    assert(returnedObj != NULL);
    assert(returnedObj->getCookie() == 777);
    memFree(returnedObj);

    returnedObj = (MyObject *) libNs->getRefToObject(sharksHandle, &status);
    assert(status == StatusOk);

    returnedObj->setCookie(0xbaadbeef);
    returnedObj = (MyObject *) libNs->getNsObject(sharksHandle);
    assert(returnedObj != NULL);
    assert(returnedObj->getCookie() == 0xbaadbeef);
    memFree(returnedObj);

    status = libNs->close(sharksHandle, NULL);
    assert(status == StatusOk);

    sharksHandle =
        libNs->open(sharksPathName, LibNsTypes::ReaderShared, &status);
    assert(status == StatusOk);

    returnedObj = (MyObject *) libNs->getRefToObject(sharksHandle, &status);
    assert(status == StatusNsRefToObjectDenied);

    status = libNs->close(sharksHandle, NULL);
    assert(status == StatusOk);

    status = libNs->remove(sharksPathName, NULL);
    assert(status == StatusOk);
}

void
removeMatchingTests()
{
    Status status;
    static const int BufSize = 1 * KB;
    static const unsigned NumPaths = 1 * KB;
    char pathBuffer[BufSize];
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;

    for (unsigned ii = 0; ii < NumPaths; ii++) {
        // Create two different "types" of path names based on odd/even.
        if (ii % 2) {
            assert(snprintf(pathBuffer,
                            BufSize,
                            "/removeMatchingTests/path/odd%d",
                            ii) < BufSize);
        } else {
            assert(snprintf(pathBuffer,
                            BufSize,
                            "/removeMatchingTests/path/even%d",
                            ii) < BufSize);
        }
        // Publish the path name
        nsId = libNs->publish(pathBuffer, &status);
        assert(status == StatusOk);
        assert(nsId != LibNsTypes::NsInvalidId);
    }

    // Count the "odd" paths
    LibNsTypes::PathInfoOut *pathInfo = NULL;
    size_t oddCount = libNs->getPathInfo("*odd*", &pathInfo);
    assert(oddCount == NumPaths / 2);
    memFree(pathInfo);
    pathInfo = NULL;

    // Remove the "odd" paths
    status = libNs->removeMatching("*odd*");
    assert(status == StatusOk);

    // Count the remaining.
    size_t remaining = libNs->getPathInfo("*", &pathInfo);
    assert(remaining == NumPaths / 2);
    memFree(pathInfo);

    // Remove the "even" paths
    status = libNs->removeMatching("*even*");
    assert(status == StatusOk);
}

void
staleOpenTest()
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId;
    bool objDeleted;

    // Publish a path name
    nsId = libNs->publish("/dataset/raiders", &status);
    assert(nsId != LibNsTypes::NsInvalidId);
    assert(status == StatusOk);

    // Open it for the first time
    LibNsTypes::NsHandle firstHandle =
        libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusOk);

    // Open it a second time
    LibNsTypes::NsHandle secondHandle =
        libNs->open(nsId, LibNsTypes::ReaderShared, &status);
    assert(status == StatusOk);

    // Close the first handle
    status = libNs->close(firstHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    // Try to use the first handle again
    status = libNs->close(firstHandle, &objDeleted);
    assert(status == StatusNsStale);

    // Remove it.  Second open keeps the object around.
    status = libNs->remove(nsId, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == false);

    // Close it using the second handle.  The object should get deleted.
    status = libNs->close(secondHandle, &objDeleted);
    assert(status == StatusOk);
    assert(objDeleted == true);
}

void
tableNamespaceTest()
{
    XcalarApiUdfContainer sessionContainer;

    XcalarApiUserId userId;
    strlcpy(userId.userIdName, "user001", sizeof(userId.userIdName));
    userId.userIdUnique = 9999;

    XcalarApiSessionInfoInput sessInfo;
    strlcpy(sessInfo.sessionName, "session001", sizeof(sessInfo.sessionName));
    sessInfo.sessionNameLength = strlen(sessInfo.sessionName);
    sessInfo.sessionId = 9998;

    verifyOk(UserDefinedFunction::initUdfContainer(&sessionContainer,
                                                   &userId,
                                                   &sessInfo,
                                                   NULL));

    TableNsMgr *tableNsMgr = TableNsMgr::get();
    Status status = StatusOk;
    TableNsMgr::TableId table1, table2;
    char **tableNames = nullptr;
    size_t numTables;
    std::unordered_set<std::string> tableNamesSet;
    std::string tableNamesString;

    const char *tableName1 = "table001";
    const char *tableName2 = "table002";

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Unable to initialize TableNsMgr: %s",
                strGetFromStatus(status));

        return;
    }

    table1 = tableNsMgr->genTableId();
    table2 = tableNsMgr->genTableId();

    status = tableNsMgr->addToNs(&sessionContainer,
                                 table1,
                                 tableName1,
                                 DagTypes::DagIdInvalid,
                                 DagTypes::InvalidDagNodeId);
    assert(status == StatusOk);
    xSyslog(moduleName, XlogInfo, "Successfully added table001 to TableNsMgr");

    status = tableNsMgr->addToNs(&sessionContainer,
                                 table2,
                                 tableName2,
                                 DagTypes::DagIdInvalid,
                                 DagTypes::InvalidDagNodeId);
    assert(status == StatusOk);
    xSyslog(moduleName, XlogInfo, "Successfully added table002 to TableNsMgr");

    status = tableNsMgr->listGlobalTables("*", tableNames, numTables);
    assert(status == StatusOk);
    assert(numTables == 0);
    if (tableNames != nullptr) {
        for (size_t ii = 0; ii < numTables; ii++) {
            delete[] tableNames[ii];
        }
        delete[] tableNames;
        tableNames = nullptr;
    }

    status = tableNsMgr->listSessionTables("*",
                                           tableNames,
                                           numTables,
                                           &sessionContainer);
    assert(status == StatusOk);

    for (size_t ii = 0; ii < numTables; ++ii) {
        tableNamesSet.insert(tableNames[ii]);
        tableNamesString.append(tableNames[ii]);
        tableNamesString.append(" ");
    }
    assert(tableNamesSet.find(tableName1) != tableNamesSet.end());
    assert(tableNamesSet.find(tableName2) != tableNamesSet.end());

    xSyslog(moduleName,
            XlogInfo,
            "Found the following session tables: %s",
            tableNamesString.c_str());

    tableNamesString.clear();

    status = tableNsMgr->publishTable(&sessionContainer, tableName1);
    assert(status == StatusOk);

    status = tableNsMgr->publishTable(&sessionContainer, tableName2);
    assert(status == StatusOk);

    if (tableNames != nullptr) {
        for (size_t ii = 0; ii < numTables; ii++) {
            delete[] tableNames[ii];
        }
        delete[] tableNames;
        tableNames = nullptr;
    }
    tableNamesString.clear();
    status = tableNsMgr->listGlobalTables("*", tableNames, numTables);
    assert(status == StatusOk);

    for (size_t ii = 0; ii < numTables; ++ii) {
        tableNamesString.append(tableNames[ii]);
        tableNamesString.append(" ");
    }
    xSyslog(moduleName,
            XlogInfo,
            "Found the following global tables: %s",
            tableNamesString.c_str());
    assert(numTables == 2);

    status = tableNsMgr->unpublishTable(&sessionContainer, tableName1);
    assert(status == StatusOk);
    status = tableNsMgr->unpublishTable(&sessionContainer, tableName2);
    assert(status == StatusOk);

    if (tableNames != nullptr) {
        for (size_t ii = 0; ii < numTables; ii++) {
            delete[] tableNames[ii];
        }
        delete[] tableNames;
        tableNames = nullptr;
    }
    status = tableNsMgr->listGlobalTables("*", tableNames, numTables);
    assert(status == StatusOk);
    assert(numTables == 0);

    tableNsMgr->removeFromNs(&sessionContainer, table1, tableName1);
    tableNsMgr->removeFromNs(&sessionContainer, table2, tableName2);
    if (tableNames != nullptr) {
        for (size_t ii = 0; ii < numTables; ii++) {
            delete[] tableNames[ii];
        }
        delete[] tableNames;
        tableNames = nullptr;
    }
    status = tableNsMgr->listSessionTables("*",
                                           tableNames,
                                           numTables,
                                           &sessionContainer);
    assert(status == StatusOk);
    assert(numTables == 0);

    if (tableNames != nullptr) {
        for (size_t ii = 0; ii < numTables; ii++) {
            delete[] tableNames[ii];
        }
        delete[] tableNames;
        tableNames = nullptr;
    }
}

Status
doWork()
{
    // Compartmentalize tests.  There should not be any objects retained
    // across each individual test.  If a new test/function is added you
    // can run the test and look for any memory leaks.  If there are none
    // you should expect to see:
    //
    // Commencing test "tear down"
    // ok 3 - Test "tear down" passed in 0.000s
    // Xcalar Test: SessionUsr: Thr 26171: 0 total sessions, 0 persisted (0
    //              expected)
    // Xcalar Test: MemTrack: Thr 26151: Memory that was obtained but not freed:
    // Xcalar Test: MemTrack: Thr 26151: MemTrack.cpp:1026: 136 bytes
    // Xcalar Test: MemTrack: Thr 26151: MemTrack.cpp:1008: 56 bytes
    // Xcalar Test: MemTrack: Thr 26151: End unfreed memory

    basicTests();
    openGetObjTest();
    pathNameValidation();
    markForRemovalCheck();
    publishTests();
    updateTests();
    removeMatchingTests();
    staleOpenTest();
    getHugeObjTest();
    tableNamespaceTest();

    return StatusOk;
}

Status
nsBasicChecks()
{
    for (unsigned ii = 0; ii < 1; ii++) {
        doWork();
    }

    return StatusOk;
}

static Status
setUp()
{
    return StatusOk;
}

static Status
tearDown()
{
    return StatusOk;
}

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
