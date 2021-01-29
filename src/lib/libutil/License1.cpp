// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <dirent.h>
#include <new>

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

LicenseMgr *LicenseMgr::instance = NULL;
static CalloutQueue::Handle licCheckCallout;

LicenseMgr *
LicenseMgr::get()
{
    return instance;
}

void
LicenseMgr::licCheckTimerCallback(void *unused)
{
    Status status;
    LicenseMgr *licenseMgr = LicenseMgr::get();

    (void) unused;

    status = licenseMgr->licCheckActual();
    if (status != StatusOk) {
        xSyslog(moduleName, XlogCrit,
                "Periodic license check failed: %s",
                strGetFromStatus(status));
    }
}

Status
LicenseMgr::init()
{
    Status status = StatusOk;

    assert(instance == NULL);
    instance = new (std::nothrow) LicenseMgr;
    if (instance == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = LicenseGvm::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
LicenseMgr::startPeriodicCheck()
{
    Status status;

    // Set system values based on what is licensed
    status = XcalarConfig::get()->setLicensedValues();
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (Config::get()->getMyNodeId() != 0) {
        // Node 0 does the heavy lifting for the cluster.
        goto CommonExit;
    }

    // Check boottime license compliance for things that are static
    // (e.g. number of nodes).
    status = licBoottimeVerification();
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Do additional license compliance and start a timer to have the checks
    // redone once each day.
    status = licCheckActual();
    if (status == StatusOk) {
        // XXX use periodic option
        status = CalloutQueue::get()->insert(&licCheckCallout,
                SecsPerDay, 0, 0, LicenseMgr::licCheckTimerCallback,
                NULL, "LicenseCheck", CalloutQueue::FlagsNone);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

CommonExit:

    return(status);
}

void
LicenseMgr::destroy()
{
    if (instance == NULL) {
        return;
    }

    delete instance;
    instance = NULL;
}

bool
LicenseMgr::licIsExpired()
{
    return isLicExpired_;
}

void
LicenseMgr::licUpdateLock() {
    updateLock_.lock();
}

void
LicenseMgr::licUpdateUnlock() {
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
        xSyslog(moduleName, XlogErr,
                "Failed to open license directory '%s': %s",
                logLib->licenseDirPath_, strGetFromStatus(status));
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

        ver = atoi(&currentFile->d_name[prefixLen+1]);
        if (ver > highestVersion) {
            highestVersion = ver;
        }
    }

    if (highestVersion) {
        // Found a license file, validate its content.  If the contents of the
        // file are not valid then try prior versions (until we run out).
        int  validVersion = highestVersion;

        do {
            status = validateLicenseFile(validVersion, &licenseString,
                    &licenseSize);
            if (status != StatusOk) {
                // XXX: get rid of the bad version??
                validVersion--;
            }
        } while (status != StatusOk && validVersion > 0);

        if (validVersion == 0) {
            xSyslog(moduleName, XlogErr,
                    "Failed to find valid license file.  Will use the "
                    "installed version");
            highestVersion = 0;
        } else {
            currentVersionNum_ = validVersion;
        }
    }

    if (!highestVersion) {
        int retries = 10;
        // Didn't find an existing valid license file.  Use the installation
        // licensed info read from /etc/xcalar/<license>.
        status = licCheckActual();
        if (status != StatusOk) {
            xSyslog(moduleName, XlogErr,
                    "Failed to get installed license info: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // Processing the license file by licCheckActual sets the initial
        // license.
        assert(initialLicenseString_ != NULL);
        licenseString = initialLicenseString_;
        licenseSize = initialLicenseSize_;

        // Write the golden copy to the XLRROOT/license directory
        currentVersionNum_ = LicenseRecord::StartVersion;
        status = StatusUnknown;
        while (status != StatusOk && retries) {
            status = createNewLicenseFile(licenseString, licenseSize);
            if (status != StatusOk) {
                xSyslog(moduleName, XlogErr,
                        "Failed to create new license file #%lu: %s",
                        currentVersionNum_, strGetFromStatus(status));
                currentVersionNum_++;
                retries--;
            }
        }
    }

    // We have the license string and the license info is persisted to durable
    // storage in the XLRROOT/license directory.

    // Broadcast the license via GVM it to all the nodes.
    status = publishAndBroadcastLicense(licenseString, licenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
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
LicenseMgr::validateLicenseFile(int version, char **licenseString,
        size_t *licenseSize)
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
    LogLib *logLib = LogLib::get();

    assert(*licenseString == NULL);

    ret = snprintf(fullPath, sizeof(fullPath), "%s/%s%s%d",
            logLib->licenseDirPath_, LicenseNamePrefix, LicenseVersionDelim,
            version);
    assert(ret < (int)sizeof(fullPath));

    fd = open(fullPath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogErr,
                "Failed to open license file '%s': %s",
                fullPath, strGetFromStatus(status));
        goto CommonExit;
    }

    ret = fstat(fd, &fileStat);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogErr,
                "Failed to fstat license file '%s': %s",
                fullPath, strGetFromStatus(status));
        goto CommonExit;
    }

    // Add 1 to ensure null termination
    moduleLen = fileStat.st_size + 1;
    licensePersistModule = (LicensePersistModule *)memAllocExt(moduleLen,
            moduleName);
    if (licensePersistModule == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr,
                "Failed to allocate '%d' bytes for license file '%s' "
                "content: %s", moduleLen, fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = FileUtils::convergentRead(fd, licensePersistModule,
            fileStat.st_size, &bytesRead);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to read license file '%s': %s",
                fullPath, strGetFromStatus(status));
        goto CommonExit;
    }
    assert((int)bytesRead == fileStat.st_size);

    if ((int)licensePersistModule->ver != version) {
        xSyslog(moduleName, XlogErr,
                "License file version '%lu' inconsistent with expected "
                "version '%d' in '%s'", licensePersistModule->ver,
                version, fullPath);
        status = StatusLicInputInvalid;
        goto CommonExit;
    }

    crc = hashCrc32c(0, &licensePersistModule->ver,
            bytesRead - sizeof(licensePersistModule->crc));
    if (licensePersistModule->crc != crc) {
        xSyslog(moduleName, XlogErr,
                "License file crc '%lu' inconsistent with expected "
                "crc '%lu' in '%s'", crc, licensePersistModule->crc,
                fullPath);
        goto CommonExit;
    }

    // We have a valid file!
    *licenseSize = licensePersistModule->size;
    *licenseString = (char *)memAllocExt(*licenseSize, moduleName);
    if (*licenseString == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memcpy(*licenseString, licensePersistModule->content, *licenseSize);

CommonExit:

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
LicenseMgr::setInitialLicense(const char *licenseString, uint64_t licenseSize)
{
    Status status = StatusOk;

    assert(initialLicenseString_ == NULL);

    initialLicenseString_ = (char *)memAllocExt(licenseSize, moduleName);
    if (initialLicenseString_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memcpy(initialLicenseString_, licenseString, licenseSize);
    initialLicenseSize_ = licenseSize;

CommonExit:

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
        (LicensePersistModule *)memAllocExt(moduleLen, moduleName);
    if (licensePersistModule == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    do {
        currentVersionNum_++;

        ret = snprintf(fullPath, sizeof(fullPath), "%s/%s%s%d",
                logLib->licenseDirPath_, LicenseNamePrefix, LicenseVersionDelim,
                (int)currentVersionNum_);
        assert(ret < (int)sizeof(fullPath));

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
        fd = open(fullPath, O_CREAT | O_EXCL | O_CLOEXEC | O_WRONLY,
                S_IRWXU | S_IRWXG | S_IRWXO);
        if (fd == -1) {
            if (errno != EEXIST) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName, XlogErr,
                        "Failed open create license file '%s': %s",
                        fullPath, strGetFromStatus(status));
                goto CommonExit;
            } else {
                // File exists, try the next one in numerical order
                xSyslog(moduleName, XlogErr,
                        "License file '%s' (ver %lu) exists, retrying with "
                        "incremented version", fullPath, currentVersionNum_);
                fileCreationRetries--;
            }
        } else {
            fileCreated = true;
        }
    } while (!fileCreated && fileCreationRetries);

    if (!fileCreated && !fileCreationRetries) {
        xSyslog(moduleName, XlogErr,
                "Unable to create license file '%s': %s",
                fullPath, strGetFromStatus(sysErrnoToStatus(EEXIST)));
        goto CommonExit;
    }

    status = FileUtils::convergentWrite(fd, licensePersistModule, moduleLen);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to write license file '%s': %s",
                fullPath, strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogDebug,
            "License file '%s' successfully created", fullPath);

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
    Status nodeStatus[nodeCount];

    ret = snprintf(fullyQualName, LibNsTypes::MaxPathNameLen, "%s%s",
            LicensePrefix, LicenseFileName);
    assert(ret < (int)LibNsTypes::MaxPathNameLen);

    nsId = libNs->publish(fullyQualName, &licenseRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to publish '%s' into namespace: %s",
                fullyQualName, strGetFromStatus(status));
        goto CommonExit;
    }

    handle = libNs->open(fullyQualName, LibNsTypes::ReadWrite, &status);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to open '%s' in namespace: %s",
                fullyQualName, strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // GVM
    length = sizeof(LicenseGvm::LicenseUpdateInfo) + licenseSize;
    gvmInfo = (LicenseGvm::LicenseUpdateInfo *)memAllocExt(
            length, moduleName);
    if (gvmInfo == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    gvmInfo->licenseVersion = licenseRecord.consistentVersion_;
    memcpy(&gvmInfo->licenseString, licenseString, licenseSize);
    gvmInfo->licenseSize = licenseSize;

    status = Gvm::get()->invoke(LicenseGvm::get()->getGvmIndex(),
            (uint32_t)LicenseGvm::Action::Update, gvmInfo, length, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

CommonExit:

    if (status != StatusOk) {
        if (nsId != LibNsTypes::NsInvalidId) {
            Status status2 = libNs->remove(nsId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName, XlogErr,
                        "Failed to remove '%s' from namespace: %s",
                        fullyQualName, strGetFromStatus(status2));
            }
        }
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName, XlogErr,
                    "Failed to close '%s' from namespace: %s",
                    fullyQualName, strGetFromStatus(status2));
        }
        handleValid = false;
    }
    if (gvmInfo != NULL) {
        memFree(gvmInfo);
        gvmInfo = NULL;
    }

    return status;
}

// Update the global name space and broadcast to the rest of the cluster
// via GVM.
Status
LicenseMgr::updateAndBroadcastLicense(const char *licenseString,
        uint64_t licenseSize)
{
    Status status = StatusOk;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    LibNs *libNs = LibNs::get();
    LicenseRecord *licenseRecord = NULL;
    size_t length;
    LicenseGvm::LicenseUpdateInfo *gvmInfo = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status nodeStatus[nodeCount];
    bool gvmIssued = false;

    // XXX: need to know the version number that was written

    ret = snprintf(fullyQualName, LibNsTypes::MaxPathNameLen, "%s%s",
            LicensePrefix, LicenseFileName);
    assert(ret < (int)LibNsTypes::MaxPathNameLen);

    handle = libNs->open(fullyQualName, LibNsTypes::ReadWrite,
            (NsObject **)&licenseRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to open '%s' in namespace: %s",
                fullyQualName, strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // GVM
    length = sizeof(LicenseGvm::LicenseUpdateInfo) + licenseSize;
    gvmInfo = (LicenseGvm::LicenseUpdateInfo *)memAllocExt(
            length, moduleName);
    if (gvmInfo == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    gvmInfo->licenseVersion = licenseRecord->consistentVersion_;
    memcpy(&gvmInfo->licenseString, licenseString, licenseSize);
    gvmInfo->licenseSize = licenseSize;

    gvmIssued = true;
    status = Gvm::get()->invoke(LicenseGvm::get()->getGvmIndex(),
            (uint32_t)LicenseGvm::Action::Update, gvmInfo, length, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    // Now update the record in the namespace
    handle = libNs->updateNsObject(handle, licenseRecord, &status);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to update '%s' in namespace: %s",
                fullyQualName, strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (status != StatusOk && gvmIssued) {
        // Revert back to the prior version.  If node receives this but didn't
        // get the update this will be a noop for that node.  If a node
        // received the update but doesn't get this revert then they'll
        // continue to use the updated license (nothing we can do about this
        // for now).
        LicenseGvm::LicenseRevertInfo revertInfo;
        // XXX TODO - fill in the revert version
        revertInfo.revertVersion = 777;
        Status status2 = Gvm::get()->invoke(LicenseGvm::get()->getGvmIndex(),
                (uint32_t)LicenseGvm::Action::Revert, gvmInfo, length,
                nodeStatus);
        if (status2 == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status2 = nodeStatus[ii];
                    break;
                }
            }
        }
        if (status2 != StatusOk) {
            xSyslog(moduleName, XlogErr,
                    "Failed to complete GVM cleanout: %s",
                    strGetFromStatus(status2));
        }
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName, XlogErr,
                    "Failed to close '%s' in namespace: %s",
                    fullyQualName, strGetFromStatus(status2));
        }
        handleValid = false;
    }

    if (gvmInfo != NULL) {
        memFree(gvmInfo);
        gvmInfo = NULL;
    }
    if (licenseRecord  != NULL) {
        memFree(licenseRecord);
        licenseRecord = NULL;
    }

    return status;
}

Status
LicenseMgr::updateLicenseLocal(void *args)
{
    Status status = StatusOk;
    LicenseGvm::LicenseUpdateInfo *licenseInfo =
        (LicenseGvm::LicenseUpdateInfo *)args;
    bool isLocked = false;
    char *newLicenseString = NULL;

    updateLock_.lock();
    isLocked = true;

    newLicenseString =
        (char *)memAllocExt(licenseInfo->licenseSize, moduleName);
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
    memcpy(newLicenseString, licenseInfo->licenseString,
            licenseInfo->licenseSize);
    currentLicenseString_ = newLicenseString;
    newLicenseString = NULL;
    currentLicenseSize_ = licenseInfo->licenseSize;
    currentLicenseVersion_ = licenseInfo->licenseVersion;

CommonExit:

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
        (LicenseGvm::LicenseRevertInfo *)args;

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
    }

    updateLock_.unlock();

    return status;
}

Status
LicenseMgr::getLicense(char *licenseString, uint64_t *licenseSize)
{
    Status status = StatusOk;

    updateLock_.lock();

    // Caller is responsible for freeing memory
    licenseString = (char *)memAllocExt(currentLicenseSize_, moduleName);
    if (licenseString == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memcpy(licenseString, currentLicenseString_, currentLicenseSize_);
    *licenseSize = currentLicenseSize_;

CommonExit:

    updateLock_.unlock();

    return status;
}

Status
LicenseMgr::updateLicense(const char *licenseString, uint64_t licenseSize)
{
    Status status = StatusOk;

    updateLock_.lock();

    // Create a new license file in the XLRROOT/license/ directory.
    status = createNewLicenseFile(licenseString, licenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to create new license file: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Tell the rest of the cluster about it.
    status = updateAndBroadcastLicense(licenseString, licenseSize);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr,
                "Failed to broadcast license: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    updateLock_.unlock();

    return status;
}
