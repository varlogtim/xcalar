// Copyright 2016-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdarg.h>
#include <errno.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>
#include <libgen.h>
#include <time.h>
#include <string.h>
#include <cryptopp/oids.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "util/System.h"
#include "common/InitTeardown.h"
#include "test/QA.h"
#include "util/FileUtils.h"
#include <string>
#include "util/License.h"
#include "license/LicenseData2.h"
#include "license/LicenseReader2.h"
#include "license/LicenseKeyFile.h"
#include "license/LicenseConstants.h"

using namespace CryptoPP;

static Status badPubKeyReadTests();
static Status noPubKeyReadTests();
static Status badLicReadTests();
static Status noLicReadTests();
static Status badLicProductTests();
static Status badLicExpiredTests();
static Status badLicReadNoMatchTests();
static Status licMaxNodeTests();
static Status lic2BNodeTests();
static Status licSigKeyGoodTests();
static Status licSigKeyBadTests();

static TestCase testCases[] = {
    {"badPubKeyReadTests", badPubKeyReadTests, TestCaseEnable, ""},
    {"noPubKeyReadTests", noPubKeyReadTests, TestCaseEnable, ""},
    {"badLicReadTests", badLicReadTests, TestCaseEnable, ""},
    {"noLicReadTests", noLicReadTests, TestCaseEnable, ""},
    {"badLicProductTests", badLicProductTests, TestCaseEnable, ""},
    {"badLicExpiredTests", badLicExpiredTests, TestCaseEnable, ""},
    {"badLicReadNoMatchTests", badLicReadNoMatchTests, TestCaseEnable, ""},
    {"licMaxNodeTests", licMaxNodeTests, TestCaseEnable, ""},
    {"lic2BNodeTests", lic2BNodeTests, TestCaseEnable, ""},
    {"licSigKeyGoodTests", licSigKeyGoodTests, TestCaseEnable, ""},
    {"licSigKeyBadTests", licSigKeyBadTests, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask = TestCaseOptDisableIsPass;
static char *dirTest;
static const char *pubKeyFileName = "EcdsaPub.key";
static const char *pubKeyFileNameBad = "EcdsaPub.key.bad";
static const char *pubKeyFileNameNo = "EcdsaPub.key.bad.no";
static const char *pubKeyFileNameNoMatch = "EcdsaPub.key.nomatch";
static const char *licFileNameSt = "XcalarLic.key";
static const char *licFileNameSt20Node = "XcalarLic.key.20node";
static const char *licFileNameSt2BNode = "XcalarLic.key.2Bnode";
static const char *licFileNameBad = "XcalarLic.key.bad";
static const char *licFileNameNo = "XcalarLic.key.bad.no";
static const char *licFileNameProduct = "XcalarLic.key.product";
static const char *licFileNameExpired = "XcalarLic.key.expired";
static const char *licFileSigKeyGood = "XcalarLic.key.sigKey";
static const char *licFileSigKeyBad = "XcalarLic.key.sigKeyBad";

const int envBufSize = 1024;

char origLicEnv[envBufSize] = "";
char origKeyFileEnv[envBufSize] = "";
char origLicFileEnv[envBufSize] = "";

static void
createAbsPath(char *buf, const int buflen, const char *path, const char *file)
{
    snprintf(buf, buflen - 1, "%s/%s", path, file);
}

Status
badPubKeyReadTests()
{
    char pubKeyName[envBufSize];
    Status status = StatusOk;
    ECDSA<ECP, SHA1>::PublicKey public_keys[1];

    createAbsPath(pubKeyName, envBufSize, dirTest, pubKeyFileNameBad);

    status = LicenseKeyFile::loadPublicKey(pubKeyName, &public_keys[0]);
    assert(status == StatusLicInputInvalid);

    setenv("testKey", "Invalid license", 1);

    status = LicenseKeyFile::loadPublicKeyEnv("testKey", &public_keys[0]);
    assert(status == StatusLicInputInvalid);

    return StatusOk;
}

Status
noPubKeyReadTests()
{
    char pubKeyName[envBufSize];
    Status status = StatusOk;
    ECDSA<ECP, SHA1>::PublicKey public_keys[1];

    snprintf(pubKeyName, envBufSize - 1, "%s/%s", dirTest, pubKeyFileNameNo);

    status = LicenseKeyFile::loadPublicKey(pubKeyName, &public_keys[0]);
    assert(status == StatusLicFileOpen);

    status = LicenseKeyFile::loadPublicKeyEnv("noSuchKey", &public_keys[0]);
    assert(status == StatusLicPubKeyMissing);

    return StatusOk;
}

static Status
licReadTestsUtil(const char *testLicFileName,
                 const char *testPubKeyFileName,
                 Status readerStatus,
                 Status finalStatus)
{
    char pubKeyName[envBufSize];
    char licKeyName[envBufSize];
    char *licenseString = NULL;
    size_t fileSize = 0;
    size_t bytesRead;
    int fd = -1;
    const char *password = LicenseMgr::get()->getPassword();
    LicenseData2 loadedData;
    Status status = StatusOk;
    struct stat statBuf;
    int ret;
    LicenseMgr *licenseMgr = LicenseMgr::get();
    ECDSA<ECP, SHA1>::PublicKey public_keys[1];

    createAbsPath(pubKeyName, envBufSize, dirTest, testPubKeyFileName);

    createAbsPath(licKeyName, envBufSize, dirTest, testLicFileName);

    ret = stat(licKeyName, &statBuf);
    assert(ret == 0 || errno == ENOENT || errno == EACCES || errno == EBADF ||
           errno == EFAULT || errno == ELOOP || errno == ENAMETOOLONG ||
           errno == ENOMEM || errno == ENOTDIR || errno == EOVERFLOW);
    fileSize = statBuf.st_size;

    fd = open(licKeyName, O_CLOEXEC | O_RDONLY);
    if (fd <= 0) {
        return StatusLicFileOpen;
    }

    licenseString = (char *) memAlloc(fileSize + 1);
    memZero(licenseString, fileSize + 1);
    status = FileUtils::convergentRead(fd, licenseString, fileSize, &bytesRead);
    assert(status == StatusOk);

    {
        LicenseReader2 reader(pubKeyName, NULL, NULL, password);

        status = reader.read(licenseString, &loadedData);
        assert(status == readerStatus);
    }

    Status loadStatus;
    if (status == StatusOk) {
        loadStatus = licenseMgr->testLicense(&loadedData);
    } else {
        loadStatus = readerStatus;
    }
    assert(loadStatus == finalStatus);

    if (licenseString != NULL) {
        memFree(licenseString);
    }

    return (loadStatus == finalStatus) ? StatusOk : status;
}

Status
badLicReadTests()
{
    return licReadTestsUtil(licFileNameBad,
                            pubKeyFileName,
                            StatusLicPasswordError,
                            StatusLicPasswordError);
}

Status
badLicReadNoMatchTests()
{
    return licReadTestsUtil(licFileNameSt,
                            pubKeyFileNameNoMatch,
                            StatusLicSignatureInvalid,
                            StatusLicSignatureInvalid);
}

Status
noLicReadTests()
{
    Status status;
    status = licReadTestsUtil(licFileNameNo,
                              pubKeyFileName,
                              StatusLicErr,
                              StatusLicErr);
    assert(status == StatusLicFileOpen);
    return StatusOk;
}

Status
badLicProductTests()
{
    return licReadTestsUtil(licFileNameProduct,
                            pubKeyFileName,
                            StatusLicErr,
                            StatusLicErr);
}

Status
badLicExpiredTests()
{
    return licReadTestsUtil(licFileNameExpired,
                            pubKeyFileName,
                            StatusOk,
                            StatusLicExpired);
}

Status
licMaxNodeTests()
{
    Status status;
    unsigned numActiveNodes, numTotalNodes;
    Config *config = Config::get();

    numActiveNodes = config->getActiveNodes();
    numTotalNodes = config->getTotalNodes();
    status = config->setNumTotalNodes(23);
    assert(status == StatusOk);
    status = config->setNumActiveNodes(23);
    assert(status == StatusOk);
    status = licReadTestsUtil(licFileNameSt20Node,
                              pubKeyFileName,
                              StatusOk,
                              StatusLicInsufficientNodes);
    assert(config->setNumActiveNodes(numActiveNodes) == StatusOk);
    assert(config->setNumTotalNodes(numTotalNodes) == StatusOk);
    return status;
}

Status
lic2BNodeTests()
{
    return licReadTestsUtil(licFileNameSt2BNode,
                            pubKeyFileName,
                            StatusOk,
                            StatusOk);
}

Status
licSigKeyGoodTests()
{
    return licReadTestsUtil(licFileSigKeyGood,
                            pubKeyFileName,
                            StatusOk,
                            StatusOk);
}

Status
licSigKeyBadTests()
{
    return licReadTestsUtil(licFileSigKeyBad,
                            pubKeyFileName,
                            StatusLicSignatureInvalid,
                            StatusLicSignatureInvalid);
}

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    char absBinFilePath[1024];
    ssize_t pathSize = 0;

    memZero(absBinFilePath, sizeof(absBinFilePath));
    pathSize =
        readlink("/proc/self/exe", absBinFilePath, sizeof(absBinFilePath) - 1);
    assert(pathSize > 0);

    dirTest = dirname(absBinFilePath);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);

    assert(InitTeardown::init(InitLevel::UsrNode,
                              SyslogFacilityTest,
                              fullCfgFilePath,
                              NULL,
                              argv[0],
                              InitFlagsNone,
                              0 /* My node ID */,
                              1 /* Num active */,
                              1 /* Num on physical */,
                              BufferCacheMgr::TypeNone) == StatusOk);

    numTestsFailed =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
