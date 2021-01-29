// Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <dirent.h>
#include <new>
#include <zlib.h>
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

#include "license/LicenseConstants.h"
#include "license/LicenseData2.h"
#include "license/LicenseReader2.h"
#include "license/LicenseKeyFile.h"
#include "primitives/Primitives.h"
#include "callout/Callout.h"
#include "runtime/Mutex.h"
#include "sys/XLog.h"
#include "util/License.h"
#include "constants/XcalarConfig.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "util/FileUtils.h"
#include "ns/LibNs.h"
#include "gvm/Gvm.h"
#include "LicenseGvm.h"
#include "util/License.h"
#include "util/Base64.h"
#include "StrlFunc.h"
#include "LibUtilConstants.h"
#include "LicenseEmbedded.h"
#include "dataset/Dataset.h"

using namespace util;

LicenseMgr *LicenseMgr::instance = NULL;

LicenseMgr *
LicenseMgr::get()
{
    return instance;
}

Status
LicenseMgr::initInternal(const char *pubSigningKeyFilePath)
{
    Status status = StatusOk;
    FILE *fp = NULL;
    char *keyContents = NULL;

    if (pubSigningKeyFilePath != NULL) {
        int ret;
        struct stat fileStat;
        long bytesRead;

        errno = 0;
        ret = stat(pubSigningKeyFilePath, &fileStat);
        if (ret != 0) {
            // Not a fatal error, we'll just default to using
            // the public key that's burned in
            xSyslog(moduleName,
                    XlogErr,
                    "Couldn't open %s: %s. Defaulting to "
                    "built-in public signing key",
                    pubSigningKeyFilePath,
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        keyContents = (char *) memAlloc(fileStat.st_size);
        if (keyContents == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate buffer for key contents. "
                    "(Bytes required: %lu). Defaulting to built-in public "
                    "signing key",
                    fileStat.st_size);
            goto CommonExit;
        }

        fp = fopen(pubSigningKeyFilePath, "rb");
        if (fp == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Couldn't open %s: %s. Defaulting to "
                    "built-in public signing key",
                    pubSigningKeyFilePath,
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        bytesRead = fread(keyContents, sizeof(char), fileStat.st_size, fp);
        if (bytesRead != fileStat.st_size) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error while reading %s: %s. Defaulting to built-in public "
                    "signing key",
                    pubSigningKeyFilePath,
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        Status status2;
        size_t ignored;
        status2 = base64Encode((const uint8_t *) keyContents,
                               fileStat.st_size,
                               &pubSigningKey_,
                               &ignored);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error occurred during base64 encoding of key contents: "
                    "%s. Defaulting to built-in public signing key",
                    strGetFromStatus(status2));
            goto CommonExit;
        }
    }

CommonExit:
    if (keyContents != NULL) {
        memFree(keyContents);
        keyContents = NULL;
    }

    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    return status;
}

Status
LicenseMgr::init(const char *pubSigningKeyFilePath)
{
    Status status = StatusOk;

    assert(instance == NULL);
    instance = new (std::nothrow) LicenseMgr;
    if (instance == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = LicenseEmbedded::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = LicenseGvm::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = instance->initInternal(pubSigningKeyFilePath);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    return status;
}

void
LicenseMgr::destroy()
{
    if (instance == NULL) {
        return;
    }

    if (pubSigningKey_ != NULL) {
        memFree(pubSigningKey_);
        pubSigningKey_ = NULL;
    }

    if (currentLicenseString_ != NULL) {
        memFree(currentLicenseString_);
        currentLicenseString_ = NULL;
    }

    LicenseEmbedded::destroy();

    delete instance;
    instance = NULL;
}

const char *
LicenseMgr::getPubSigningKey()
{
    if (pubSigningKey_ != NULL) {
        return pubSigningKey_;
    } else {
        return LicenseEmbedded::getPubSigningKey();
    }
}

bool
LicenseMgr::licIsExpiredInt(LicenseData2 *loadedData)
{
    time_t expiry;
    time_t now;

    now = time(NULL);
    expiry = loadedData->getExpiration();
    assert(now >= 0);

    if (now == -1) {
        xSyslog(moduleName,
                XlogCrit,
                "%s",
                strGetFromStatus(sysErrnoToStatus(errno)));
        return true;
    }

    if (now >= expiry) {
        return true;
    }

    return false;
}

bool
LicenseMgr::licIsExpired()
{
    if (!isLicLoaded_) {
        return true;
    }

    return licIsExpiredInt(getLicenseData());
}

void
LicenseMgr::licUpdateLock()
{
    updateLock_.lock();
}

void
LicenseMgr::licUpdateUnlock()
{
    updateLock_.unlock();
}

Status
LicenseMgr::boottimeInit()
{
    Status status = StatusOk;
    LogLib *logLib = LogLib::get();
    DIR *dirIter = NULL;
    int highestVersion = 0;
    char *licenseString = NULL;
    size_t licenseSize = 0;

    if (Config::get()->getMyNodeId() != 0) {
        // Node 0 does the initial work for the cluster.  The other nodes will
        // wait on a barrier until this (and other work) has completed.  They
        // get the license info from node 0 via GVM.
        goto CommonExit;
    }

    // Look in the license directory for the persisted license information.
    assert(logLib->licenseDirPath_[0] != '\0');
    dirIter = opendir(logLib->licenseDirPath_);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to open license directory '%s': %s",
                logLib->licenseDirPath_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    while (true) {
        errno = 0;
        int prefixLen = strlen(LicenseNamePrefix);
        int ver;
        struct dirent *currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error.
            assert(errno == 0);
            // End of directory
            break;
        }

        // Skip any files that don't start with the license file prefix
        if (strncmp(currentFile->d_name, LicenseNamePrefix, prefixLen) != 0) {
            continue;
        }

        // Skip if the delimiter between the file prefix and the version
        // number isn't correct.
        if (currentFile->d_name[prefixLen] != *LicenseVersionDelim) {
            continue;
        }

        ver = atoi(&currentFile->d_name[prefixLen + 1]);
        if (ver > highestVersion) {
            highestVersion = ver;
        }
    }

    if (highestVersion) {
        // Found a license file, validate its content.  If the contents of the
        // file are not valid then try prior versions (until we run out).
        int validVersion = highestVersion;

        do {
            status =
                validateLicenseFile(validVersion, &licenseString, &licenseSize);
            if (status != StatusOk) {
                // XXX: get rid of the bad version??
                validVersion--;
            }
        } while (status != StatusOk && validVersion > 0);

        if (validVersion == 0) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to find valid license file.  Will use the "
                    "installed version");
            highestVersion = 0;
        } else {
            currentVersionNum_ = validVersion;
        }
    }

    if (!highestVersion) {
        xSyslog(moduleName, XlogWarn, "Xcalar is starting unlicensed");
        status = StatusOk;
        goto CommonExit;
    }

    // We have the license string and the license info is persisted to durable
    // storage in the XLRROOT/license directory.

    // Broadcast the license via GVM it to all the nodes.
    status = publishAndBroadcastLicense(licenseString, licenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to broadcast license: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (dirIter != NULL) {
        verify(closedir(dirIter) == 0);
        dirIter = NULL;
    }

    return status;
}

Status
LicenseMgr::validateLicenseFile(int version,
                                char **licenseStringOut,
                                size_t *licenseSizeOut)
{
    Status status = StatusOk;
    char fullPath[XcalarApiMaxPathLen];
    int fd = -1;
    LicensePersistModule *licensePersistModule = NULL;
    int ret;
    uint64_t crc;
    struct stat fileStat;
    int moduleLen;
    size_t bytesRead;
    char *licenseString = NULL;
    size_t licenseSize = 0;
    LicenseData2 ignore;

    LogLib *logLib = LogLib::get();

    assert(*licenseStringOut == NULL);

    ret = snprintf(fullPath,
                   sizeof(fullPath),
                   "%s/%s%s%d",
                   logLib->licenseDirPath_,
                   LicenseNamePrefix,
                   LicenseVersionDelim,
                   version);
    assert(ret < (int) sizeof(fullPath));

    fd = open(fullPath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to open license file '%s': %s",
                fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = fstat(fd, &fileStat);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to fstat license file '%s': %s",
                fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Add 1 to ensure null termination
    moduleLen = fileStat.st_size + 1;
    licensePersistModule =
        (LicensePersistModule *) memAllocExt(moduleLen, moduleName);
    if (licensePersistModule == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate '%d' bytes for license file '%s' "
                "content: %s",
                moduleLen,
                fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = FileUtils::convergentRead(fd,
                                       licensePersistModule,
                                       fileStat.st_size,
                                       &bytesRead);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to read license file '%s': %s",
                fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert((int) bytesRead == fileStat.st_size);

    if ((int) licensePersistModule->ver != version) {
        xSyslog(moduleName,
                XlogErr,
                "License file version '%lu' inconsistent with expected "
                "version '%d' in '%s'",
                licensePersistModule->ver,
                version,
                fullPath);
        status = StatusLicInputInvalid;
        goto CommonExit;
    }

    if (licensePersistModule->size > (uint64_t) moduleLen) {
        xSyslog(moduleName,
                XlogErr,
                "licensePersistModule->size: %lu is invalid",
                licensePersistModule->size);
        status = StatusLicInputInvalid;
        goto CommonExit;
    }
    licensePersistModule->content[licensePersistModule->size] = '\0';

    crc = hashCrc32c(0,
                     &licensePersistModule->ver,
                     bytesRead - sizeof(licensePersistModule->crc));
    if (licensePersistModule->crc != crc) {
        xSyslog(moduleName,
                XlogErr,
                "License file crc '%lu' inconsistent with expected "
                "crc '%lu' in '%s'",
                crc,
                licensePersistModule->crc,
                fullPath);
        status = StatusLicInputInvalid;
        goto CommonExit;
    }

    // We have a valid file!
    licenseSize = licensePersistModule->size;
    licenseString = (char *) memAllocExt(licenseSize + 1, moduleName);
    if (licenseString == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memcpy(licenseString, licensePersistModule->content, licenseSize + 1);

    status = parseLicense(licenseString, &ignore);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *licenseStringOut = licenseString;
    licenseString = NULL;
    *licenseSizeOut = licenseSize;

CommonExit:
    if (licenseString != NULL) {
        memFree(licenseString);
        licenseString = NULL;
    }

    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }
    if (licensePersistModule != NULL) {
        memFree(licensePersistModule);
        licensePersistModule = NULL;
    }

    return status;
}

Status
LicenseMgr::createNewLicenseFile(const char *licenseString,
                                 uint64_t licenseSize)
{
    Status status = StatusOk;
    char fullPath[XcalarApiMaxPathLen];
    int ret;
    LogLib *logLib = LogLib::get();
    LicensePersistModule *licensePersistModule = NULL;
    uint64_t moduleLen;
    size_t moduleCrcSize;
    int fd = -1;
    uint32_t crc;
    bool fileCreated = false;
    int fileCreationRetries = 10;

    moduleLen = sizeof(LicensePersistModule) + licenseSize;

    // Round to a multiple of the alignment size.
    moduleLen = roundUp(moduleLen, HashCrc32Alignment);

    licensePersistModule =
        (LicensePersistModule *) memAllocExt(moduleLen, moduleName);
    if (licensePersistModule == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    do {
        currentVersionNum_++;

        ret = snprintf(fullPath,
                       sizeof(fullPath),
                       "%s/%s%s%d",
                       logLib->licenseDirPath_,
                       LicenseNamePrefix,
                       LicenseVersionDelim,
                       (int) currentVersionNum_);
        assert(ret < (int) sizeof(fullPath));

        // Ensure any padding added because of alignment rounding
        // is zero.
        memZero(licensePersistModule, moduleLen);

        licensePersistModule->ver = currentVersionNum_;
        licensePersistModule->size = licenseSize;
        memcpy(licensePersistModule->content, licenseString, licenseSize);

        moduleCrcSize = moduleLen - sizeof(licensePersistModule->crc);
        assert((moduleCrcSize % HashCrc32Alignment) == 0);
        crc = hashCrc32c(0, &licensePersistModule->ver, moduleCrcSize);
        licensePersistModule->crc = crc;

        // Create the file and write out the license info
        // XXX: this needs to handle EEXIST by retrying (for a limited number)
        // with currentVersionNum++
        fd = open(fullPath,
                  O_CREAT | O_EXCL | O_CLOEXEC | O_WRONLY,
                  S_IRWXU | S_IRWXG | S_IRWXO);
        if (fd == -1) {
            if (errno != EEXIST) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "Failed open create license file '%s': %s",
                        fullPath,
                        strGetFromStatus(status));
                goto CommonExit;
            } else {
                // File exists, try the next one in numerical order
                xSyslog(moduleName,
                        XlogErr,
                        "License file '%s' (ver %lu) exists, retrying with "
                        "incremented version",
                        fullPath,
                        currentVersionNum_);
                fileCreationRetries--;
            }
        } else {
            fileCreated = true;
        }
    } while (!fileCreated && fileCreationRetries);

    if (!fileCreated && !fileCreationRetries) {
        xSyslog(moduleName,
                XlogErr,
                "Unable to create license file '%s': %s",
                fullPath,
                strGetFromStatus(sysErrnoToStatus(EEXIST)));
        goto CommonExit;
    }

    status = FileUtils::convergentWrite(fd, licensePersistModule, moduleLen);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to write license file '%s': %s",
                fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogDebug,
            "License file '%s' successfully created",
            fullPath);

CommonExit:
    if (licensePersistModule != NULL) {
        memFree(licensePersistModule);
        licensePersistModule = NULL;
    }
    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }

    return status;
}

// Publish the license file to the global name space and broadcast to the rest
// of the cluster via GVM.
Status
LicenseMgr::publishAndBroadcastLicense(const char *licenseString,
                                       uint64_t licenseSize)
{
    Status status = StatusOk;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    LibNs *libNs = LibNs::get();
    LicenseRecord licenseRecord(currentVersionNum_);
    LicenseGvm::LicenseUpdateInfo *gvmInfo = NULL;
    size_t length;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   LicensePrefix,
                   LicenseFileName);
    assert(ret < (int) LibNsTypes::MaxPathNameLen);

    nsId = libNs->publish(fullyQualName, &licenseRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to publish '%s' into namespace: %s",
                fullyQualName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    handle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' in namespace: %s",
                fullyQualName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // GVM
    length = sizeof(LicenseGvm::LicenseUpdateInfo) + licenseSize;
    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + length);
    BailIfNull(gPayload);

    gvmInfo = (LicenseGvm::LicenseUpdateInfo *) gPayload->buf;
    gvmInfo->licenseVersion = licenseRecord.consistentVersion_;
    memcpy(&gvmInfo->licenseString, licenseString, licenseSize);
    gvmInfo->licenseSize = licenseSize;

    gPayload->init(LicenseGvm::get()->getGvmIndex(),
                   (uint32_t) LicenseGvm::Action::Update,
                   length);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    BailIfFailed(status);

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        if (nodeStatus[ii] != StatusOk) {
            status = nodeStatus[ii];
            break;
        }
    }
    BailIfFailed(status);

CommonExit:

    if (status != StatusOk) {
        if (nsId != LibNsTypes::NsInvalidId) {
            Status status2 = libNs->remove(nsId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to remove '%s' from namespace: %s",
                        fullyQualName,
                        strGetFromStatus(status2));
            }
        }
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s' from namespace: %s",
                    fullyQualName,
                    strGetFromStatus(status2));
        }
        handleValid = false;
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

Status
LicenseMgr::updateLicenseLocal(void *args)
{
    Status status = StatusOk;
    LicenseGvm::LicenseUpdateInfo *licenseInfo =
        (LicenseGvm::LicenseUpdateInfo *) args;
    bool isLocked = false;
    char *newLicenseString = NULL;

    updateLock_.lock();
    isLocked = true;

    newLicenseString =
        (char *) memAllocExt(licenseInfo->licenseSize + 1, moduleName);
    if (newLicenseString == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // Set the prior license info to what we currently have.
    if (priorLicenseString_ != NULL) {
        memFree(priorLicenseString_);
    }
    priorLicenseString_ = currentLicenseString_;
    priorLicenseSize_ = currentLicenseSize_;
    priorLicenseVersion_ = currentLicenseVersion_;

    // Set the current info to the new license info
    memcpy(newLicenseString,
           licenseInfo->licenseString,
           licenseInfo->licenseSize);
    newLicenseString[licenseInfo->licenseSize] = '\0';
    currentLicenseString_ = newLicenseString;
    newLicenseString = NULL;
    currentLicenseSize_ = licenseInfo->licenseSize;
    currentLicenseVersion_ = licenseInfo->licenseVersion;

    status = parseLicense(currentLicenseString_, &theLicense_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse license: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    isLicLoaded_ = true;
    licLoadStatus_ = testLicense(&theLicense_);

    if (licLoadStatus_ == StatusOk) {
        licLoadStatus_ = Dataset::get()->updateLicensedValues();
    }

CommonExit:
    if (!isLicLoaded_) {
        // Only update this if a license wasn't previously loaded.
        // Otherwise, we continue using the previous license
        licLoadStatus_ = status;
    }

    if (isLocked) {
        updateLock_.unlock();
        isLocked = false;
    }

    return status;
}

Status
LicenseMgr::revertLicenseLocal(void *args)
{
    Status status = StatusOk;
    LicenseGvm::LicenseRevertInfo *revertInfo =
        (LicenseGvm::LicenseRevertInfo *) args;

    updateLock_.lock();

    // Revert back to the older license if it's version matches the one we're
    // reverting to.  If we didn't get the update the prior version won't match
    // and there'll be nothing to do.
    if (revertInfo->revertVersion == priorLicenseVersion_) {
        memFree(currentLicenseString_);
        currentLicenseString_ = priorLicenseString_;
        currentLicenseSize_ = priorLicenseSize_;
        currentLicenseVersion_ = priorLicenseVersion_;
        priorLicenseString_ = NULL;
        priorLicenseSize_ = 0;
        priorLicenseVersion_ = 0;

        if (currentLicenseString_ != NULL) {
            Status status;
            status = parseLicense(currentLicenseString_, &theLicense_);
            isLicLoaded_ = (status == StatusOk);
            licLoadStatus_ = status;
        } else {
            isLicLoaded_ = false;
            licLoadStatus_ = StatusLicMissing;
        }
    }

    if (status == StatusOk) {
        status = Dataset::get()->updateLicensedValues();
    }

    updateLock_.unlock();

    return status;
}

// Update the global name space and broadcast to the rest of the cluster
// via GVM.
Status
LicenseMgr::updateLicense(const char *compressedLicenseString,
                          uint64_t compressedLicenseSize)
{
    Status status = StatusUnknown;
    char *uLicenseKey = NULL;
    size_t uLicenseSize;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    LicenseRecord licenseRecordNew(currentVersionNum_);
    LicenseRecord *licenseRecord = NULL;
    size_t length;
    LicenseGvm::LicenseUpdateInfo *gvmInfo = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    bool gvmIssued = false;
    uint64_t previousVersion = currentVersionNum_;
    Gvm::Payload *gPayload = NULL;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    status = uncompressLicense(compressedLicenseString,
                               compressedLicenseSize,
                               &uLicenseKey,
                               &uLicenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not uncompress license key: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // XXX: need to know the version number that was written
    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   LicensePrefix,
                   LicenseFileName);
    assert(ret < (int) LibNsTypes::MaxPathNameLen);

    handle = libNs->open(fullyQualName,
                         LibNsTypes::WriterExcl,
                         (NsObject **) &licenseRecord,
                         &status);
    if (status == StatusNsNotFound) {
        nsId = libNs->publish(fullyQualName, &licenseRecordNew, &status);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to publish '%s' into namespace: %s",
                    fullyQualName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        handle = libNs->open(fullyQualName,
                             LibNsTypes::WriterExcl,
                             (NsObject **) &licenseRecord,
                             &status);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' in namespace: %s",
                fullyQualName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // We now have the global libNS lock. So free to do global operations

    // Create a new license file in the XLRROOT/license/ directory.
    status = createNewLicenseFile(uLicenseKey, uLicenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create new license file: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // GVM
    length = sizeof(LicenseGvm::LicenseUpdateInfo) + uLicenseSize;
    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + length);
    BailIfNull(gPayload);

    gvmInfo = (LicenseGvm::LicenseUpdateInfo *) gPayload->buf;
    gvmInfo->licenseVersion = licenseRecord->consistentVersion_;
    memcpy(&gvmInfo->licenseString, uLicenseKey, uLicenseSize);
    gvmInfo->licenseSize = uLicenseSize;

    gvmIssued = true;
    gPayload->init(LicenseGvm::get()->getGvmIndex(),
                   (uint32_t) LicenseGvm::Action::Update,
                   length);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    BailIfFailed(status);

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        if (nodeStatus[ii] != StatusOk) {
            status = nodeStatus[ii];
            break;
        }
    }
    BailIfFailed(status);

    // Now update the record in the namespace
    licenseRecord->consistentVersion_ = currentVersionNum_;
    handle = libNs->updateNsObject(handle, licenseRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to update '%s' in namespace: %s",
                fullyQualName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    BailIfFailed(status);

CommonExit:

    if (status != StatusOk) {
        if (gvmIssued) {
            // Revert back to the prior version.  If node receives this but
            // didn't get the update this will be a noop for that node.
            // If a node received the update but doesn't get this revert then
            // they'll continue to use the updated license (nothing we can do
            // about this for now).
            LicenseGvm::LicenseRevertInfo *revertInfo = NULL;
            if (gPayload != NULL) {
                memFree(gPayload);
                gPayload = NULL;
            }
            gPayload = (Gvm::Payload *) memAlloc(
                sizeof(Gvm::Payload) + sizeof(LicenseGvm::LicenseRevertInfo));
            if (gPayload != NULL) {
                revertInfo = (LicenseGvm::LicenseRevertInfo *) gPayload->buf;
                revertInfo->revertVersion = previousVersion;

                gPayload->init(LicenseGvm::get()->getGvmIndex(),
                               (uint32_t) LicenseGvm::Action::Revert,
                               sizeof(LicenseGvm::LicenseRevertInfo));
                Status status2 = Gvm::get()->invoke(gPayload, nodeStatus);
                if (status2 == StatusOk) {
                    for (unsigned ii = 0; ii < nodeCount; ii++) {
                        if (nodeStatus[ii] != StatusOk) {
                            status2 = nodeStatus[ii];
                            break;
                        }
                    }
                }
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to complete GVM cleanout: %s",
                            strGetFromStatus(status2));
                }
            }
        }

        if (nsId != LibNsTypes::NsInvalidId) {
            Status status2 = libNs->remove(nsId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to remove '%s' from namespace: %s",
                        fullyQualName,
                        strGetFromStatus(status2));
            }
        }
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s' in namespace: %s",
                    fullyQualName,
                    strGetFromStatus(status2));
        }
        handleValid = false;
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (licenseRecord != NULL) {
        memFree(licenseRecord);
        licenseRecord = NULL;
    }

    if (uLicenseKey != NULL) {
        memFree(uLicenseKey);
        uLicenseKey = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

Status
LicenseMgr::destroyLicense()
{
    Status status = StatusOk;

    // XXX: How best to destroy a license and what it means.

    return status;
}

bool
LicenseMgr::licIsLoaded()
{
    return isLicLoaded_;
}

Status
LicenseMgr::licGetLoadStatus()
{
    return licLoadStatus_;
}

Status
LicenseMgr::testLicense(LicenseData2 *loadedData)
{
    LicenseProductFamily family;
    LicenseProduct product;
    LicensePlatform platform;
    Status status = StatusUnknown;

    family = loadedData->getProductFamily();
    product = loadedData->getProduct();
    platform = loadedData->getPlatform();

    if ((!isValidLicenseProductFamily(family)) ||
        (!isValidLicenseProduct(product)) ||
        (!isValidLicensePlatform(platform))) {
        status = StatusLicInvalid;
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }

    if (Config::get() != NULL &&
        Config::get()->getActiveNodes() > loadedData->getNodeCount()) {
        status = StatusLicInsufficientNodes;
        goto CommonExit;
    }

    /*
     * Check if we're expired
     */
    if (!XcalarConfig::get()->acceptExpiredLicense_ &&
        licIsExpiredInt(loadedData)) {
        status = StatusLicExpired;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
LicenseMgr::parseLicense(const char *uLicenseKey, LicenseData2 *licData)
{
    Status status = StatusUnknown;
    LicenseReader2 reader(NULL, NULL, getPubSigningKey(), getPassword());

    status = reader.read(uLicenseKey, licData);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
LicenseMgr::licVerify(char *licenseKey, int licKeySize)
{
    char *uLicenseKey = NULL;
    uint64_t uLicenseSize = -1;
    Status status = StatusOk;
    LicenseData2 loadedData;

    /*
     * Copy the license key since the reader object
     * destroys it
     */

    status =
        uncompressLicense(licenseKey, licKeySize, &uLicenseKey, &uLicenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogInfo, "License key: \"%s\"", licenseKey);
        xSyslog(moduleName,
                XlogErr,
                "Could not uncompress license key: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = parseLicense(uLicenseKey, &loadedData);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not parse license: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = testLicense(&loadedData);
CommonExit:
    if (uLicenseKey != NULL) {
        memFree(uLicenseKey);
    }

    return status;
}

Status
LicenseMgr::installLicense(const char *licenseFilePath)
{
    struct stat fileStat;
    char *licenseContents = NULL;
    int ret;
    FILE *fp = NULL;
    Status status = StatusUnknown;
    long bytesRead;
    LicenseData2 licData;
    char *compressedLicense = NULL;
    size_t compressedLicenseSize = 0;

    errno = 0;
    ret = stat(licenseFilePath, &fileStat);
    if (ret != 0) {
        status = (errno == ENOENT) ? StatusLicMissing : sysErrnoToStatus(ret);
        goto CommonExit;
    }

    licenseContents = (char *) memAlloc(fileStat.st_size + 1);
    if (licenseContents == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate buffer for license contents. "
                "Bytes required: %lu",
                fileStat.st_size + 1);
        goto CommonExit;
    }

    fp = fopen(licenseFilePath, "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    bytesRead = fread(licenseContents, sizeof(char), fileStat.st_size, fp);
    if (bytesRead != fileStat.st_size) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    licenseContents[bytesRead] = '\0';

    status = parseLicense(licenseContents, &licData);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = testLicense(&licData);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = compressLicense(licenseContents,
                             bytesRead + 1,
                             &compressedLicense,
                             &compressedLicenseSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = updateLicense(compressedLicense, compressedLicenseSize);

CommonExit:
    if (compressedLicense != NULL) {
        memFree(compressedLicense);
        compressedLicense = NULL;
        compressedLicenseSize = 0;
    }

    if (licenseContents != NULL) {
        memFree(licenseContents);
        licenseContents = NULL;
    }

    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    return status;
}

Status
LicenseMgr::compressLicense(const char *uncompressedLicense,
                            size_t uncompressedLicenseSize,
                            char **compressedLicenseOut,
                            size_t *compressedLicenseSizeOut)
{
    z_stream strm;
    Status status = StatusUnknown;
    unsigned char *in = NULL;
    char *compressedLicense = NULL;
    size_t compressedLicenseBufSize = 0;
    char *compressedLicenseTmp = NULL;
    int zlibStatus = -1;
    const int chunkSize = 0x4000;
    unsigned char compressedData[chunkSize];
    size_t compressedLicenseSize = 0, compressedDataSize = 0;
    bool strmInited = false;

    int windowBits = 15;
    int GZIP_ENCODING = 16;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;

    zlibStatus = deflateInit2(&strm,
                              Z_DEFAULT_COMPRESSION,
                              Z_DEFLATED,
                              windowBits | GZIP_ENCODING,
                              8,
                              Z_DEFAULT_STRATEGY);
    if (zlibStatus < 0) {
        status = StatusLicCompressInit;
        xSyslog(moduleName,
                XlogErr,
                "Error initializing zlib: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    strmInited = true;

    in = (unsigned char *) memAlloc(uncompressedLicenseSize);
    if (in == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memcpy(in, uncompressedLicense, uncompressedLicenseSize);

    compressedLicenseBufSize = uncompressedLicenseSize;
    compressedLicense = (char *) memAlloc(compressedLicenseBufSize);
    if (compressedLicense == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    compressedLicenseSize = 0;

    strm.next_in = in;
    strm.avail_in = uncompressedLicenseSize;
    do {
        strm.avail_out = chunkSize;
        strm.next_out = compressedData;
        zlibStatus = deflate(&strm, Z_FINISH);
        compressedDataSize = chunkSize - strm.avail_out;
        if (compressedDataSize + compressedLicenseSize >
            compressedLicenseBufSize) {
            compressedLicenseBufSize += chunkSize;
            compressedLicenseTmp =
                (char *) memRealloc(compressedLicense,
                                    compressedLicenseBufSize);
            if (compressedLicenseTmp == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to allocate compressedLicense buffer of size "
                        "%lu bytes",
                        compressedLicenseBufSize);
                status = StatusNoMem;
                goto CommonExit;
            }
            compressedLicense = compressedLicenseTmp;
            compressedLicenseTmp = NULL;
        }
        memcpy(compressedLicense + compressedLicenseSize,
               (const char *) compressedData,
               compressedDataSize);
        compressedLicenseSize += compressedDataSize;
    } while (strm.avail_out == 0);

    compressedLicenseTmp =
        (char *) memRealloc(compressedLicense, compressedLicenseSize);
    if (compressedLicenseTmp == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate compressedLicense buffer of size "
                "%lu bytes",
                compressedLicenseSize);
        status = StatusNoMem;
        goto CommonExit;
    }
    compressedLicense = compressedLicenseTmp;
    compressedLicenseTmp = NULL;

    status = base64Encode((uint8_t *) compressedLicense,
                          compressedLicenseSize,
                          compressedLicenseOut,
                          compressedLicenseSizeOut);
CommonExit:
    if (strmInited) {
        deflateEnd(&strm);
        strmInited = false;
    }

    if (in != NULL) {
        memFree(in);
        in = NULL;
    }

    if (compressedLicense != NULL) {
        memFree(compressedLicense);
        compressedLicense = NULL;
    }

    return status;
}

Status
LicenseMgr::uncompressLicense(const char *compressedLicense,
                              uint64_t compressedLicenseSize,
                              char **uncompressedLicense,
                              uint64_t *licenseSize)
{
    Status status = StatusUnknown;
    uint8_t *dataBuf = NULL;
    size_t binarySize;
    const int chunkSize = 1 * KB;
    const int windowBits = 15;
    const int enableZlibGzip = 32;
    z_stream strm;
    unsigned char decompressedData[chunkSize];
    size_t totalSize = 0;
    int zlibStatus = -1;
    size_t uLicenseSize = chunkSize;
    char *uLicenseTmp = NULL;
    bool strmInited = false;

    char *uLicense = (char *) memAllocExt(uLicenseSize, moduleName);
    if (uLicense == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate initial uLicense buffer of size %lu bytes",
                uLicenseSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    status = base64Decode(compressedLicense,
                          compressedLicenseSize,
                          &dataBuf,
                          &binarySize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to base64Decode license: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    if ((zlibStatus = inflateInit2(&strm, windowBits | enableZlibGzip)) < 0) {
        status = StatusLicDecompressInit;
        xSyslog(moduleName,
                XlogCrit,
                "Error initializing zLib: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    strmInited = true;
    strm.avail_in = binarySize;
    strm.next_in = (unsigned char *) dataBuf;
    do {
        unsigned decompressedDataSize;
        strm.avail_out = chunkSize;
        strm.next_out = decompressedData;
        zlibStatus = inflate(&strm, Z_NO_FLUSH);
        switch (zlibStatus) {
        case Z_STREAM_END:
        case Z_OK:
        case Z_BUF_ERROR:
            break;

        default:
            inflateEnd(&strm);
            status = StatusLicDecompressErr;
            xSyslog(moduleName, XlogCrit, "%s", strGetFromStatus(status));
            goto CommonExit;
        }
        decompressedDataSize = chunkSize - strm.avail_out;
        if (totalSize + decompressedDataSize + 1 > uLicenseSize) {
            uLicenseSize += chunkSize;
            uLicenseTmp =
                (char *) memReallocExt(uLicense, uLicenseSize, moduleName);
            if (uLicenseTmp == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to allocate uLicense struct of size %lu bytes",
                        uLicenseSize);
                status = StatusNoMem;
                goto CommonExit;
            }
            uLicense = uLicenseTmp;
            uLicenseTmp = NULL;
        }
        memcpy(uLicense + totalSize,
               (const char *) decompressedData,
               decompressedDataSize);
        totalSize += decompressedDataSize;
    } while (strm.avail_out == 0);
    uLicense[totalSize++] = '\0';

    // a final realloc to make the license the actual size
    uLicenseTmp = (char *) memReallocExt(uLicense, totalSize, moduleName);
    if (uLicenseTmp == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate uLicense struct of size %lu bytes",
                totalSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    *uncompressedLicense = uLicenseTmp;
    uLicenseTmp = uLicense = NULL;
    *licenseSize = totalSize;
    status = StatusOk;

CommonExit:
    if (strmInited) {
        inflateEnd(&strm);
        strmInited = false;
    }

    if (dataBuf != NULL) {
        memFree(dataBuf);
        dataBuf = NULL;
    }

    if (uLicense != NULL) {
        memFree(uLicense);
        uLicense = NULL;
    }

    return status;
}

Status
LicenseMgr::readLicenseFile(char *fileNamePath,
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
    status = FileUtils::convergentRead(fd, licenseString, fileSize, &bytesRead);
    licenseString[bytesRead] = 0;
    // We either got the key or we didn't, either way we're
    // not going to try again.
    FileUtils::close(fd);
    fd = -1;

    return status;
}
