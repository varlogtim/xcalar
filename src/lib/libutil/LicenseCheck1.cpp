// Copyright 2016-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <time.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <iostream>
#include <iomanip>
#include <time.h>
#include <string.h>
#include <cryptopp/oids.h>
#include "license/LicenseConstants.h"
#include "license/LicenseData1.h"
#include "license/LicenseReader1.h"
#include "license/LicenseKeyFile.h"

#include "StrlFunc.h"
#include "sys/XLog.h"
#include "LibUtilConstants.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "util/System.h"
#include "config/Config.h"
#include "util/License.h"
#include "util/MemTrack.h"

using namespace util;

bool
LicenseMgr::licIsLoaded() {
    return isLicLoaded_;
}

Status
LicenseMgr::licGetLoadStatus() {
    return licLoadStatus_;
}

void
LicenseMgr::TestUnloadLicense() {
    isLicExpired_ = false;
    isLicLoaded_ = false;
    licLoadStatus_ = StatusUnknown;

    if (initialLicenseString_ != NULL) {
        memFree(initialLicenseString_);
        initialLicenseString_ = NULL;
    }
}

void
LicenseMgr::licCheckData(LicenseData1 *license) {
    unsigned char *licData = theLicense_.Unload();
    if (licData == NULL) {
        return;
    }
    license->Load(licData, licenseByteCount);
    delete[] licData;
}


void
LicenseMgr::setLicenseKey(char *key, int keySize) {
    licUpdateLock();
    memZero(theLicenseKey, licenseBufSize);
    strlcpy(theLicenseKey, key,
            (keySize < licenseBufSize) ? keySize : licenseBufSize);
    licUpdateUnlock();
}

bool
LicenseMgr::checkDirExist(const char *dir) {
    struct stat statBuf;

    if (dir == NULL || strlen(dir) == 0) {
        return(false);
    }

    xSyslog(moduleName, XlogDebug,
            "Checking if path %s exists and is a directory.",
            dir);

    int ret = stat(dir, &statBuf);
    assert(ret == 0 || errno == ENOENT ||
            errno == EACCES || errno == EBADF ||
            errno == EFAULT || errno == ELOOP ||
            errno == ENAMETOOLONG || errno == ENOMEM ||
            errno == ENOTDIR || errno == EOVERFLOW);

    return ret == 0 && S_ISDIR(statBuf.st_mode);
}

Status
LicenseMgr::checkFileExist(char *file, Status errStatus,
        size_t *fileSize) {
    struct stat statBuf;
    Status status = StatusOk;
    int ret;

    if (file == NULL || strlen(file) == 0) {
        status = StatusIllegalFileName;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogDebug,
            "Checking if file %s exists.",
            file);

    ret = stat(file, &statBuf);
    assert(ret == 0 || errno == ENOENT ||
            errno == EACCES || errno == EBADF ||
            errno == EFAULT || errno == ELOOP ||
            errno == ENAMETOOLONG || errno == ENOMEM ||
            errno == ENOTDIR || errno == EOVERFLOW);
    if (ret != 0 && errno == ENOENT) {
        status = errStatus;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    } else if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }

    if (fileSize != NULL) {
        *fileSize = statBuf.st_size;
    }

CommonExit:
    return(status);
}

void
LicenseMgr::copyAtomToBuf(const char *src, char *dst,
                         unsigned int dstLen,
                         const char *type, const char *name) {
    size_t pathLen = strlen(src);
    if (pathLen >= (dstLen - 1)) {
        xSyslog(moduleName, XlogErr,
                "The %s %s is too long.", type, name);
    }
    assert(pathLen < dstLen);
    strlcpy(dst, src, dstLen);
}

void
LicenseMgr::findKeyDir(char *keyDirName, int dirLen) {
    memZero(keyDirName, dirLen);
    int noOp = 0;

    const char *keySources[3] = { getenv(xceLicenseEnv),
                            "/etc/xcalar",
                            XcalarConfig::get()->xcalarRootCompletePath_ };
    bool keySourcesExist[3] = { checkDirExist(keySources[0]),
                                checkDirExist(keySources[1]),
                                checkDirExist(keySources[2]) };

    if (keySourcesExist[0]) {
        copyAtomToBuf(keySources[0], keyDirName, dirLen,
                "environment variable", xceLicenseEnv);
        goto CommonExit;
    }

    if (keySourcesExist[1]) {
        copyAtomToBuf(keySources[1], keyDirName, dirLen,
                "path", keySources[1]);
        goto CommonExit;
    }

    xSyslog(moduleName, XlogWarn,
            "The environment variable %s is not set", xceLicenseEnv);
    xSyslog(moduleName, XlogWarn,
            "and the %s directory is not found", keySources[1]);
    xSyslog(moduleName, XlogWarn,
            "Please consider setting the former or creating the latter.");
    xSyslog(moduleName, XlogWarn,
            "Looking for license keys in %s instead.",
            keySources[2]);

    if (keySourcesExist[2]) {
        copyAtomToBuf(keySources[2], keyDirName, dirLen,
                "path", keySources[2]);
        goto CommonExit;
    }

    assert(0);

CommonExit:
    noOp++;  // cstyle.pl complains about a return here,
             // and the compiler complains if no return
             // so, this.
}

Status
LicenseMgr::findKeyName(char *path,
        char *keyFilePath, int keyPathLen,
        const char *keyFileEnv, const char *defFname,
        Status errStatus, size_t *fileSize) {
    char *fileSource = getenv(keyFileEnv);
    Status status = StatusOk;

    memZero(keyFilePath, keyPathLen);

    int createdLen = snprintf(keyFilePath, keyPathLen, "%s/%s",
            path, fileSource != NULL ? fileSource : defFname);
    if (createdLen >= keyPathLen) {
        xSyslog(moduleName, XlogErr,
                "The %s %s/%s is too long.", "key path", path, fileSource);
    }
    assert(createdLen < keyPathLen);

    status = checkFileExist(keyFilePath, errStatus, fileSize);

    return(status);
}

Status
LicenseMgr::testLicense(LicenseData1 *loadedData) {
    char tmpBuf[27]; // 26 mandated by ctime_r() manpage
    time_t expiry;
    time_t now;
    LicenseProductFamily family;
    LicenseProduct product;
    LicensePlatform platform;
    Status status = StatusOk;

    family = loadedData->getProductFamily();
    product = loadedData->getProduct();
    platform = loadedData->getPlatform();

    if ((family != XcalarX && family != ALL_FAMILIES) ||
        (product != Xce && product != XceMod &&
                product != XceOper && product != XceDemo &&
                product != ALL_PRODUCTS) ||
        (platform != LINUX_X64 && platform != ALL_PLATFORMS)) {
        status = StatusLicInvalid;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }


    /*
     * Check if we're expired
     */

    now = time(NULL);
    expiry = loadedData->getExpiration();
    assert(now >= 0);
    if (now == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }
    if (now >= expiry) {
        status = StatusLicExpired;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }
    if (now >= (expiry - (expWarnIntvl))) {
        xSyslog(moduleName, XlogWarn, "License will expire on %s",
                ctime_r(&timebombAlarm, tmpBuf));
    }

CommonExit:
    return(status);
}

Status
LicenseMgr::licVerify(char *licenseKey, int licKeySize) {
    char fileNameRoot[licenseDirLength];
    char fileNamePath[licenseDirLength +
                      maxFileNameLength];
    char licenseKeyCopy[licenseBufSize];
    Status pubKeyStatus;
    Status status = StatusOk;
    LicenseData1 *LoadedData = NULL;
    ECDSA<ECP, SHA1>::PublicKey publicKeys[1];

    /*
     * Copy the license key since the reader object
     * destroys it
     */
    strlcpy(licenseKeyCopy, licenseKey,
      (licKeySize < licenseBufSize) ? licKeySize : licenseBufSize);


    findKeyDir(fileNameRoot, licenseDirLength);

    status = findKeyName(fileNameRoot, fileNamePath,
                licenseDirLength + maxFileNameLength,
                xceKeyFileEnv, defKeyFileName,
                StatusLicPubKeyMissing, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    /*
     * Load the public key.  Fail if the load fails.
     */

    pubKeyStatus = LicenseKeyFile::loadPublicKey(fileNamePath, &publicKeys[0]);
    if (pubKeyStatus != StatusOk) {
        status = StatusLicPubKeyErr;
        xSyslog(moduleName, XlogCrit, "%s",
                strGetFromStatus(StatusLicPubKeyErr));
        xSyslog(moduleName, XlogCrit, "%s",
                strGetFromStatus(pubKeyStatus));
        goto CommonExit;
    }

    {
        LicenseReader1 reader(publicKeys, 1);

        status = reader.read(licenseKeyCopy, &LoadedData);
        if (status != StatusOk) {
            xSyslog(moduleName, XlogCrit, "%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = testLicense(LoadedData);

CommonExit:
    return(status);
}

Status
LicenseMgr::licCheckUpdate() {
    Status status = StatusOk;
#ifdef OLDWAY
    char fileNameRoot[licenseDirLength];
    char fileNamePath[licenseDirLength +
                      maxFileNameLength];
    Config *cfg = Config::get();

    findKeyDir(fileNameRoot, licenseDirLength);

    status = findKeyName(fileNameRoot, fileNamePath,
                licenseDirLength + maxFileNameLength,
                xceLicFileEnv, defLicFileName,
                StatusLicMissing, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    licUpdateLock();
    status = cfg->updateLicense(fileNamePath, theLicenseKey);
    licUpdateUnlock();
#else
    licUpdateLock();
    status = createNewLicenseFile(theLicenseKey, strlen(theLicenseKey));
    if (status != StatusOk) {
        goto CommonExit;
    }

#endif

CommonExit:

    return(status);
}

bool
LicenseMgr::isFatalError(Status status)
{
    // Wrong public key is fatal because we won't be
    // able to update the license
    return status != StatusOk &&
            status != StatusLicMissing &&
            status != StatusLicWrongSize &&
            status != StatusLicOldVersion &&
            status != StatusLicInsufficientNodes &&
            status != StatusLicInvalid &&
            status != StatusLicExpired;
}

Status LicenseMgr::readLicenseFile(char *fileNamePath,
                                   char *licenseString,
                                   size_t fileSize)
{
    int fd = -1;
    Status status = StatusOk;
    size_t bytesRead;

    fd = open(fileNamePath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = StatusLicMissing;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        return status;
    }

    // Attempt to read the license key
    status = FileUtils::convergentRead(fd,
                                       licenseString,
                                       fileSize,
                                       &bytesRead);
    licenseString[bytesRead] = 0;
    // We either got the key or we didn't, either way we're
    // not going to try again.
    FileUtils::close(fd);
    fd = -1;

    return status;
}

Status
LicenseMgr::licCheckActual()
{
    char fileNameRoot[licenseDirLength];
    char fileNamePath[licenseDirLength +
                      maxFileNameLength];
    char LicenseString[licenseStringSize + 1];
    char LicenseStringCopy[licenseStringSize + 1];
    int fd = -1;
    Status status = StatusOk;
    Status pubKeyStatus;
    size_t fileSize;
    size_t bytesRead;
    LicenseData1 *LoadedData = NULL;
    ECDSA<ECP, SHA1>::PublicKey publicKeys[1];
    Config *config = NULL;

    if (isLicLoaded_) {
        // Use the license info that we already have.
        // XXX: do the actual license check
        goto CommonExit;
    }

    // Only node 0 can find that it doesn't have a license loaded.  All other
    // nodes will have had it GVM'd to them.
    assert(Config::get()->getMyNodeId() == 0);

    findKeyDir(fileNameRoot, licenseDirLength);

    status = findKeyName(fileNameRoot, fileNamePath,
                licenseDirLength + maxFileNameLength,
                xceKeyFileEnv, defKeyFileName,
                StatusLicPubKeyMissing, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    /*
     * Load the public key.  Fail if the load fails.
     */
    pubKeyStatus = LicenseKeyFile::loadPublicKey(fileNamePath, &publicKeys[0]);
    if (pubKeyStatus != StatusOk) {
         status = StatusLicPubKeyErr;
         xSyslog(moduleName, XlogCrit, "%s",
                 strGetFromStatus(StatusLicPubKeyErr));
         xSyslog(moduleName, XlogCrit, "%s",
                 strGetFromStatus(pubKeyStatus));
         goto CommonExit;
    }

    /*
     * Stat the path of the license key. Fail if it isn't there.
     */
    status = findKeyName(fileNameRoot, fileNamePath,
                licenseDirLength + maxFileNameLength,
                xceLicFileEnv, defLicFileName,
                StatusLicMissing, &fileSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    /*
     * Try to read the whole file to get the license key.  Fail if it won't open
     * or the bytes read are greater than the size of the file.
     */

    if (fileSize > licV3Size + 8 ||
        fileSize < licV1V2Size) {
        status = StatusLicWrongSize;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }

    fd = open(fileNamePath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = StatusLicMissing;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }

    // Attempt to read the license key
    status = FileUtils::convergentRead(fd,
                                       LicenseString,
                                       fileSize,
                                       &bytesRead);
    LicenseString[bytesRead] = 0;
    // We either got the key or we didn't, either way we're
    // not going to try again.
    FileUtils::close(fd);
    fd = -1;

    if (status == StatusOk) {
        assert(bytesRead == fileSize);
    } else if (status == StatusEof) {
        // The file size changed in between our checking and now. Let's still
        // attempt to load it
        // If the file has changed to be -larger- then it will silently
        // miss the rest.
        // The other option would be to assume the results of stat are incorrect
        // and realloc as we go.
        assert(bytesRead <= fileSize);
    } else {
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        xSyslog(moduleName, XlogCrit, "%s",
                strGetFromStatus(StatusLicFileRead));
        goto CommonExit;
    }

    /*
     * Create the reader
     * Remove any trailing newline characters and load the license
     */

    // Save copy of the license string as reader.read munges it
    memcpy(LicenseStringCopy, LicenseString, licenseStringSize + 1);

    {
        LicenseReader1 reader(publicKeys, 1);

        LicenseString[strcspn((const char *) LicenseString, " \n\r\t")] = 0;
        status = reader.read(LicenseString, &LoadedData);
        if (status != StatusOk) {
            xSyslog(moduleName, XlogCrit, "%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        unsigned char *rawLoadedData = LoadedData->Unload();
        if (rawLoadedData == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
            goto CommonExit;
        }

        theLicense_.Load(rawLoadedData, licenseByteCount);
        delete [] rawLoadedData;
    }

    /*
     * Verify the license contents
     */

    status = testLicense(LoadedData);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogCrit,
                "License test fails: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    /*
     * Check if we have a 1.0.1 (or later) license and if we are licensed for
     * the number of nodes we are attempting to use.
     */

    if ((LoadedData->getLicenseVersion() == V1_0_0) ||
        (LoadedData->getLicenseVersion() == V1_0_1)) {
        status = StatusLicOldVersion;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }

    config = Config::get();
    if (LoadedData->getNodeCount() < config->getActiveNodes()) {
        status = StatusLicInsufficientNodes;
        xSyslog(moduleName, XlogCrit,
                "%s Licensed node count: %d Attempted node count: %d",
                strGetFromStatus(status),
                LoadedData->getNodeCount(), config->getActiveNodes());
        goto CommonExit;
    }

    // Set the initial license
    status = setInitialLicense(LicenseStringCopy, strlen(LicenseStringCopy));
    if (status != StatusOk) {
        xSyslog(moduleName, XlogCrit,
                "Unable to set the initial license: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    licLoadStatus_ = status;
    if (status == StatusOk || status == StatusLicInvalid ||
            status == StatusLicExpired) {
        isLicLoaded_ = true;
    } else {
        isLicLoaded_ = false;
    }

    if (status == StatusLicMissing || status == StatusLicExpired) {
        // if the license is missing or expired, we allow it to run
        // unlicensed by setting status to StatusOk
        xSyslog(moduleName, XlogCrit,
                "License is missing or has expired; running unlicensed");
        isLicExpired_ = true;
    } else if (status != StatusOk) {
        xSyslog(moduleName, XlogCrit,
                "An error getting the license occurred: %s",
                strGetFromStatus(status));
        isLicExpired_ = false;
    }

    if (LoadedData != NULL) {
        delete LoadedData;
    }

    if (fd != -1) {
        FileUtils::close(fd);
    }

    return (isFatalError(status)) ? status : StatusOk;
}

Status
LicenseMgr::licBoottimeVerification()
{
    Status status = StatusUnlicensedFeatureInUse;
    unsigned maxLicensedNodes = 256;
    Config *config = Config::get();

    // Verify number of nodes is compliant
    // XXX: maxLicensedNodes = getLicense("MAX_NODES");
    if (config->getTotalNodes() > maxLicensedNodes) {
        xSyslog(moduleName, XlogCrit,
                "Cluster is licensed for %d nodes but is configured for %d "
                "nodes", maxLicensedNodes, config->getTotalNodes());
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:

    return status;
}
