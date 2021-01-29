// Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "SourceFormatConnector.h"
#include "UDFConnector.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "gvm/Gvm.h"
#include "DataTargetGvm.h"
#include "ns/LibNs.h"
#include "DurableVersions.h"
#include "subsys/DurableDataTarget.pb.h"
#include "durable/Durable.h"
#include "DataTargetDurable.h"

using namespace xcalar::internal;

static constexpr const char *moduleName = "libexport";

DataTargetManager *DataTargetManager::targetManager = NULL;

//
// Public methods
//
Status
DataTargetManager::init()
{
    Status status;
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(*DataTargetManager::targetManager),
                      __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Data Target Init Failed:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    DataTargetManager::targetManager = new (ptr) DataTargetManager();

    status = DataTargetManager::targetManager->initInternal();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Data Target Init Failed:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (ptr != NULL) {
            assert(DataTargetManager::targetManager != NULL);
            DataTargetManager::targetManager->~DataTargetManager();
            memFree(DataTargetManager::targetManager);
        }
    }
    return status;
}

void
DataTargetManager::destroy()
{
    assert(targetManager);
    if (DataTargetGvm::get() != NULL) {
        DataTargetGvm::get()->destroy();
    }
    targetManager->~DataTargetManager();
    memFree(targetManager);
    targetManager = NULL;
}

DataTargetManager &
DataTargetManager::getRef()
{
    assert(targetManager);
    return *targetManager;
}

Status
DataTargetManager::addLocalHandler(void *payload)
{
    Status status;
    ExportTargetMgrIface *targetMgr;
    const ExExportTarget *target = (const ExExportTarget *) payload;

    // Verify the payload
    if (!((isValidExTargetType(target->hdr.type)) &&
          (target->hdr.type != ExTargetUnknownType))) {
        status = StatusFailed;
        xSyslog(moduleName,
                XlogErr,
                "Add local handler failed:%s",
                strGetFromStatus(status));
        return status;
    }

    targetMgr = getTargetMgr(target->hdr.type);
    if (!targetMgr) {
        status = StatusFailed;
        xSyslog(moduleName,
                XlogErr,
                "Add %s local handler failed:%s",
                target->hdr.name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = targetMgr->addTargetLocal(target);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Add %s local handler failed:%s",
                target->hdr.name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

// This is to cleanup from a failed target add
Status
DataTargetManager::removeLocalHandler(void *payload)
{
    Status status;
    ExportTargetMgrIface *targetMgr;
    const ExExportTargetHdr *hdr = (const ExExportTargetHdr *) payload;

    // Verify the payload
    if (!((isValidExTargetType(hdr->type)) &&
          (hdr->type != ExTargetUnknownType))) {
        status = StatusFailed;
        xSyslog(moduleName,
                XlogErr,
                "remove local handler failed:%s",
                strGetFromStatus(status));
        return status;
    }

    targetMgr = getTargetMgr(hdr->type);
    assert(targetMgr);
    if (!targetMgr) {
        status = StatusFailed;
        xSyslog(moduleName,
                XlogErr,
                "remove %s local handler failed:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = targetMgr->removeTargetLocal(hdr);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "remove %s local handler failed:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

//
// Instance methods
//
Status
DataTargetManager::listTargets(const char *typePattern,
                               const char *namePattern,
                               XcalarApiOutput **outputOut,
                               size_t *listTargetsOutputSize)
{
    return listTargetsInternal(typePattern,
                               namePattern,
                               outputOut,
                               listTargetsOutputSize,
                               ListTargetAccess::Shared);
}

// This acts as a sort of dispatch table for getting the appropriate class
// based off of the type
ExportTargetMgrIface *
DataTargetManager::getTargetMgr(ExTargetType type)
{
    switch (type) {
    case ExTargetSFType:
        return &SourceFormatConnector::getRef();
    case ExTargetUDFType:
        return &UDFConnector::getRef();
    default:
        assert(0);
        return NULL;
    }
}

Status
DataTargetManager::addTarget(const ExExportTarget *target)
{
    Status status;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle nsHandle;
    bool nsHandleValid = false;
    LibNs *libNs = LibNs::get();
    ExportTargetMgrIface *targetMgr;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;

    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(*target));
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload->init(DataTargetGvm::get()->getGvmIndex(),
                   (uint32_t) DataTargetGvm::Action::Add,
                   sizeof(*target));
    memcpy(gPayload->buf, target, sizeof(*target));

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s",
                   DataTargetPrefix);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "Add Target %s failed:%s",
                target->hdr.name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    targetMgr = getTargetMgr(target->hdr.type);
    assert(targetMgr != NULL);

    status = targetMgr->verifyTarget(target);
    BailIfFailed(status);

    nsHandle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        if (status == StatusAccess) {
            status = StatusTargetInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Add Target %s failed on NS open:%s",
                target->hdr.name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsHandleValid = true;

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Add Target %s failed on GVM invoke:%s",
                target->hdr.name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = writeTargets();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Add Target %s failed on write Targets:%s",
                target->hdr.name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk && status != StatusExTargetAlreadyExists) {
        // GVM cleanout.
        if (gPayload != NULL) {
            memFree(gPayload);
            gPayload = NULL;
        }
        gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                             sizeof(target->hdr));
        if (gPayload != NULL) {
            gPayload->init(DataTargetGvm::get()->getGvmIndex(),
                           (uint32_t) DataTargetGvm::Action::Remove,
                           sizeof(target->hdr));
            memcpy(gPayload->buf, &target->hdr, sizeof(target->hdr));
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
                        "Add Target %s failed on GVM cleanout:%s",
                        target->hdr.name,
                        strGetFromStatus(status2));
            }
        }
    }

    if (nsHandleValid) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Add target %s failed on NS close:%s",
                    target->hdr.name,
                    strGetFromStatus(status2));
        }
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
DataTargetManager::removeTarget(const ExExportTargetHdr *hdr)
{
    Status status;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle nsHandle;
    bool nsHandleValid = false;
    LibNs *libNs = LibNs::get();
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;

    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(*hdr));
    BailIfNull(gPayload);
    gPayload->init(DataTargetGvm::get()->getGvmIndex(),
                   (uint32_t) DataTargetGvm::Action::Remove,
                   sizeof(*hdr));
    memcpy(gPayload->buf, hdr, sizeof(*hdr));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s",
                   DataTargetPrefix);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "Remove Target %s failed:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsHandle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        if (status == StatusAccess) {
            status = StatusTargetInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Remove Target %s failed on NS open:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsHandleValid = true;

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Remove Target failed %s on GVM invoke:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = writeTargets();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Remove Target failed %s on write Targets:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (nsHandleValid) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Remove target %s failed on NS close:%s",
                    hdr->name,
                    strGetFromStatus(status2));
        }
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

//
// Private functions
//
DataTargetManager::DataTargetManager() : logOpen_(false) {}

DataTargetManager::~DataTargetManager()
{
    if (exportTargets_[ExTargetSFType]) {
        SourceFormatConnector::destroy();
        exportTargets_[ExTargetSFType] = NULL;
    }
    if (exportTargets_[ExTargetUDFType]) {
        UDFConnector::destroy();
        exportTargets_[ExTargetUDFType] = NULL;
    }
}

Status
DataTargetManager::initInternal()
{
    Status status;
    ExExportTarget *defaultTargets = NULL;
    int numTargets;

    memZero(exportTargets_, ExTargetTypeLen * sizeof(exportTargets_[0]));

    status = DataTargetGvm::init();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Data Target Init failed on DataTargetGvm init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = SourceFormatConnector::init();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Data Target Init failed on SourceFormatConnector init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = UDFConnector::init();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Data Target Init failed on UdfConnector init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    exportTargets_[ExTargetSFType] = &SourceFormatConnector::getRef();
    exportTargets_[ExTargetUDFType] = &UDFConnector::getRef();

    // Add all the default targets
    for (ExTargetType type = (ExTargetType)(ExTargetUnknownType + 1);
         type < (ExTargetType) ExTargetTypeLen;
         type = (ExTargetType)(type + 1)) {
        ExportTargetMgrIface *targetMgr;
        targetMgr = exportTargets_[type];
        assert(targetMgr);

        status = targetMgr->getDefaultTargets(&numTargets, &defaultTargets);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Data Target Init failed on getDefaultTargets:%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (!numTargets) {
            // No defaults for this type
            assert(defaultTargets == NULL);
            continue;
        }
        assert(defaultTargets);
        for (int ii = 0; ii < numTargets; ii++) {
            // Add locally, and don't write to the log
            status = targetMgr->addTargetLocal(&defaultTargets[ii]);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Data Target %s Init failed on addTargetLocal:%s",
                        defaultTargets[ii].hdr.name,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
        memFree(defaultTargets);
        defaultTargets = NULL;
    }

    status = StatusOk;
CommonExit:
    if (defaultTargets) {
        memFree(defaultTargets);
        defaultTargets = NULL;
    }
    if (status != StatusOk) {
        if (exportTargets_[ExTargetSFType]) {
            SourceFormatConnector::destroy();
            exportTargets_[ExTargetSFType] = NULL;
        }
        if (exportTargets_[ExTargetUDFType]) {
            UDFConnector::destroy();
            exportTargets_[ExTargetUDFType] = NULL;
        }
    }
    return status;
}

Status
DataTargetManager::addPersistedTargets()
{
    Status status;
    bool logExisted;
    int numTargets;
    ExExportTarget *deserializedTargets = NULL;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsId nsId;
    bool published = false;
    LibNs *libNs = LibNs::get();

    if (DataTgtLogNodeId != Config::get()->getMyNodeId()) {
        // Only one node in the cluster shall scan and add persisted targets.
        return StatusOk;
    }

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s",
                   DataTargetPrefix);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "addPersistedTargets failed:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsId = libNs->publish(fullyQualName, &status);
    if (status != StatusOk) {
        if (status == StatusExist) {
            // Quite possible that some other node got ahead of us while
            // attempting to publish this name into the namespace during
            // bootstrapping. Never mind, we will now attempt to read all
            // persisted data targets.
            status = StatusOk;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "addPersistedTargets failed on NS publish:%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }
    published = true;

    status = this->openOrCreateLog(&logExisted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "addPersistedTargets failed on log creation:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(logOpen_);

    // If the log was there, we should read in whatever targets were written
    if (logExisted) {
        // Add all the persisted targets
        status = this->readTargets(&numTargets, &deserializedTargets);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "addPersistedTargets failed on read targets:%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        for (int ii = 0; ii < numTargets; ii++) {
            status = this->addTarget(&deserializedTargets[ii]);
            // Continue reading targets in the face of errors
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "addPersistedTargets read target %s from log failed:%s",
                        deserializedTargets[ii].hdr.name,
                        strGetFromStatus(status));
            }
        }
    }

CommonExit:
    if (logOpen_) {
        this->closeLog();
    }
    if (deserializedTargets) {
        memFree(deserializedTargets);
        deserializedTargets = NULL;
    }
    if (status != StatusOk && published) {
        Status status2 = libNs->remove(nsId, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "addPersistedTargets failed on NS close:%s",
                    strGetFromStatus(status2));
        }
    }
    return status;
}

void
DataTargetManager::removePersistedTargets()
{
    Status status = StatusUnknown;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    int ret;
    LibNs *libNs = LibNs::get();

    // Clean up target from libNs
    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s",
                   DataTargetPrefix);
    assert(ret < (int) sizeof(fullyQualName));

    status = libNs->remove(fullyQualName, NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to remove target '%s' from namespace: %s",
                DataTargetPrefix,
                strGetFromStatus(status));
        // continue...
    }
}

Status
DataTargetManager::openOrCreateLog(bool *existed)
{
    Status status;
    LogLib *log = LogLib::get();
    assert(!logOpen_);
    bool logExisted;

    status =
        log->create(&targetLog_,
                    LogLib::TargetDirIndex,
                    DataTargetManager::logFilePrefix,
                    LogLib::FileSeekToStart | LogLib::FileReturnErrorIfExists,
                    LogLib::LogDefaultFileCount,
                    LogLib::LogDefaultFileSize);
    if (status == StatusOk) {
        logOpen_ = true;
        logExisted = false;
    } else if (status == StatusExist) {
        logExisted = true;
        // Create in this case just opens and seeks to the end
        status = log->create(&targetLog_,
                             LogLib::TargetDirIndex,
                             DataTargetManager::logFilePrefix,
                             LogLib::FileSeekToLogicalEnd,
                             LogLib::LogDefaultFileCount,
                             LogLib::LogDefaultFileSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "openOrCreateLog failed on log create:%s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        logOpen_ = true;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "openOrCreateLog failed on log create:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (existed) {
        *existed = logExisted;
    }
CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open target log with status '%s'",
                strGetFromStatus(status));
    }
    return status;
}

void
DataTargetManager::closeLog()
{
    assert(logOpen_);
    LogLib::get()->close(&targetLog_);
    logOpen_ = false;
}

Status
DataTargetManager::readTargets(int *numTargets, ExExportTarget **targets)
{
    TargetListPersistedData *tgtListHdr = NULL;
    Status status;
    LibDurable *libDur = LibDurable::get();
    TargetListPersistedData *targetListPersistedData = NULL;
    durable::datatarget::TargetListPersistedData *pbDataTarget;
    char myCurrentSha[gitShaLen];
    LibDurable::Handle dh(&targetLog_, "dataTarget");

    uint8_t *serializedData = NULL;
    size_t totalLength;

    status = libDur->deserialize(&dh);
    if (status != StatusOk) {
        if (status == StatusEof) {
            // Nothing persisted yet
            *numTargets = 0;
            *targets = NULL;
            status = StatusOk;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to obtain deserialized protobuf: %s",
                    strGetFromStatus(status));
        }
        goto CommonExit;
    }

    status =
        dh.getMutable<durable::datatarget::DurableDataTarget,
                      durable::datatarget::TargetListPersistedData,
                      durable::datatarget::Ver>(durable::DurableDataTargetType,
                                                durable::datatarget::Ver::V1,
                                                &pbDataTarget);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialie DurableDataTarget: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(pbDataTarget != NULL);

    status =
        dh.getSha<durable::datatarget::DurableDataTarget>(myCurrentSha,
                                                          sizeof(myCurrentSha));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get durableSha: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Get the size of the info that was serialized out.
    totalLength = pbDataTarget->totallength().value();
    assert(totalLength >= sizeof(TargetListPersistedData));
    if (totalLength < sizeof(TargetListPersistedData)) {
        status = StatusUnderflow;
        xSyslog(moduleName,
                XlogErr,
                "Serialized data size '%lu' is smaller than mimimum "
                "expected size '%lu': %s",
                totalLength,
                sizeof(TargetListPersistedData),
                strGetFromStatus(status));
        goto CommonExit;
    }

    serializedData = (uint8_t *) memAllocExt(totalLength, moduleName);
    if (serializedData == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate '%lu' bytes for DataTarget data: %s",
                totalLength,
                strGetFromStatus(status));
        goto CommonExit;
    }

    targetListPersistedData = (TargetListPersistedData *) serializedData;

    status = DataTargetDurable::
        pbDesTargetListPersistedDataPub(NULL,
                                        targetListPersistedData,
                                        pbDataTarget,
                                        myCurrentSha);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to deserialize protobuf: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    tgtListHdr = (TargetListPersistedData *) serializedData;
    assert(tgtListHdr->version == currentTargetPersistVersion);

    status = this->deserializeTargets(serializedData,
                                      totalLength,
                                      numTargets,
                                      targets);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed read targets on deserialize targets:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (XcalarConfig::get()->durableUpdateOnLoad_) {
        status = writeTargets();
        BailIfFailed(status);
        xSyslog(moduleName,
                XlogInfo,
                "Wrote updated target %s",
                DataTargetManager::logFilePrefix);
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to read targets from log with status '%s'",
                strGetFromStatus(status));
    }

    if (serializedData) {
        memFree(serializedData);
        serializedData = NULL;
    }

    return status;
}

Status
DataTargetManager::writeTargets()
{
    Status status;
    uint8_t *serializedTargets = NULL;
    size_t serializedSize;
    LogLib *log = LogLib::get();
    LibDurable *libDur = LibDurable::get();
    TargetListPersistedData *targetListPersistedData = NULL;
    durable::datatarget::TargetListPersistedData *pbDataTarget;
    LibDurable::Handle *dh = NULL;
    // XXX: Generation of the correct sha will be made generic and moved
    // into libdurable, but it's here until then.
    const char *idlSha = DurableVersions::pbGetCurrSha_DurableDataTarget();

    // This will allocated the serialized buffer for us
    status = this->serializeTargets(&serializedTargets,
                                    &serializedSize,
                                    log->getMinLogicalBlockAlignment());
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "write targets failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Note that we keep the log open, since we are the dlm node
    if (!logOpen_) {
        status = this->openOrCreateLog(NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "write targets failed on log create: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Now that we have a log handle we can initialize the durable handle.
    dh = new (std::nothrow) LibDurable::Handle(&targetLog_, "dataTarget");
    if (dh == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate libdurable handle: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // We only allow serialization at the current version.
    status =
        dh->getMutable<durable::datatarget::DurableDataTarget,
                       durable::datatarget::TargetListPersistedData,
                       durable::datatarget::Ver>(durable::DurableDataTargetType,
                                                 durable::datatarget::Ver::V1,
                                                 &pbDataTarget,
                                                 idlSha);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Faileed to initialize DurableDataTarget: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(pbDataTarget != NULL);

    targetListPersistedData = (TargetListPersistedData *) serializedTargets;

    status = DataTargetDurable::
        pbSerTargetListPersistedDataPub(targetListPersistedData, pbDataTarget);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to serialize data to protobuf: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Write it out to persisted storage.
    status = libDur->serialize(dh);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to persist serialized protobuf: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName, XlogInfo, "Successfully persisted targets");

    LogLib::Cursor lastRecord;
    log->getLastWritten(&targetLog_, &lastRecord);

    status = log->resetHead(&targetLog_, &lastRecord);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "write targets failed on log reset head: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (logOpen_) {
        this->closeLog();
    }
    if (serializedTargets) {
        memAlignedFree(serializedTargets);
    }
    delete dh;
    dh = NULL;

    return status;
}

// Collect all the targets, allocate a buffer for them, and serialize into the
// buffer.
// Returns the allocated buffer in serializedData (aligned to blockAlign), and
// the size in size.
// DOES NOT serialize any default targets.
Status
DataTargetManager::serializeTargets(uint8_t **serializedData,
                                    size_t *size,
                                    size_t blockAlign)
{
    Status status;
    XcalarApiOutput *apiOutput = NULL;
    size_t apiOutSize;
    int numTotalTargets;
    int numPersistTargets = 0;
    int targetsSerialized;
    uint8_t *dataBuf = NULL;
    size_t dataSize;
    TargetListPersistedData *listHeader = NULL;

    status = this->listTargetsInternal("*",
                                       "*",
                                       &apiOutput,
                                       &apiOutSize,
                                       ListTargetAccess::None);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Serialize targets failed on list targets: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    numTotalTargets = apiOutput->outputResult.listTargetsOutput.numTargets;
    for (int ii = 0; ii < numTotalTargets; ii++) {
        ExExportTarget *target =
            &apiOutput->outputResult.listTargetsOutput.targets[ii];
        ExportTargetMgrIface *targetMgr = exportTargets_[target->hdr.type];
        bool persistable;
        assert(targetMgr);

        status = targetMgr->shouldPersist(target, &persistable);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Serialize targets failed on should persist %s target: %s",
                    target->hdr.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (persistable) {
            ++numPersistTargets;
        }
    }

    // When target specific structures are different sizes this will need
    // to be more complicated
    dataSize = sizeof(TargetListPersistedData) +
               numPersistTargets * sizeof(TargetPersistHeader);
    // Verify that the size is what we expect
    assert(dataSize ==
           (size_t)((char *) &listHeader->targets[numPersistTargets] -
                    (char *) listHeader));

    // Round up to the alignment
    dataSize = roundUp(dataSize, blockAlign);
    dataBuf = (uint8_t *) memAllocAlignedExt(blockAlign, dataSize, moduleName);
    if (dataBuf == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Serialize targets failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Zero the struct so we don't persist random data
    memZero(dataBuf, dataSize);

    listHeader = (TargetListPersistedData *) dataBuf;

    clock_gettime(CLOCK_REALTIME, &listHeader->persistTime);
    listHeader->totalLength = dataSize;
    listHeader->version = currentTargetPersistVersion;

    listHeader->targetsCount = numPersistTargets;

    targetsSerialized = 0;
    for (int ii = 0; ii < numTotalTargets; ii++) {
        ExExportTarget *target =
            &apiOutput->outputResult.listTargetsOutput.targets[ii];
        ExportTargetMgrIface *targetMgr = exportTargets_[target->hdr.type];
        bool persistable;
        assert(targetMgr);

        status = targetMgr->shouldPersist(target, &persistable);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Serialize targets failed on should persist %s target: %s",
                    target->hdr.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (!persistable) {
            continue;
        }
        assert(isValidExTargetType(target->hdr.type) &&
               target->hdr.type != ExTargetUnknownType);

        listHeader->targets[targetsSerialized].targetSize =
            sizeof(TargetPersistHeader);
        listHeader->targets[targetsSerialized].target = *target;
        targetsSerialized++;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Serialized %i targets for persistence",
            targetsSerialized);

    *serializedData = dataBuf;
    *size = dataSize;

CommonExit:
    if (apiOutput != NULL) {
        memFree(apiOutput);
    }

    return status;
}

// Deserializes and allocates a buffer for targets found
// numTargetsFound and deserializedTargets are valid when StatusOk
Status
DataTargetManager::deserializeTargets(const uint8_t *serializedData,
                                      size_t size,
                                      int *numTargetsFound,
                                      ExExportTarget **deserializedTargets)
{
    Status status = StatusOk;
    const TargetListPersistedData *listHeader =
        (const TargetListPersistedData *) serializedData;
    int numTargets = 0;
    int targetsLoaded = 0;
    struct timespec currentTime;
    int64_t persistTime;
    int64_t timeNow;
    ExExportTarget *targets = NULL;

    if (size < sizeof(*listHeader)) {
        status = StatusTargetCorrupted;
        xSyslog(moduleName,
                XlogErr,
                "Deserialize targets failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (listHeader->version != currentTargetPersistVersion) {
        status = StatusTargetCorrupted;
        xSyslog(moduleName,
                XlogErr,
                "Deserialize targets failed on invalid version %lu"
                " expected %lu: %s",
                listHeader->version,
                currentTargetPersistVersion,
                strGetFromStatus(status));
        goto CommonExit;
    }

    verify(clock_gettime(CLOCK_REALTIME, &currentTime) == 0);
    persistTime = listHeader->persistTime.tv_sec * NSecsPerSec +
                  listHeader->persistTime.tv_nsec;
    timeNow = currentTime.tv_sec * NSecsPerSec + currentTime.tv_nsec;
    // This is weird, but time can be weird, so let it be nonfatal
    if (timeNow < persistTime) {
        xSyslog(moduleName,
                XlogInfo,
                "Target persist time later than current time (%zu ns < %zu ns)",
                timeNow,
                persistTime);
    }

    numTargets = listHeader->targetsCount;

    xSyslog(moduleName, XlogInfo, "%d targets found in log", numTargets);

    targets = (ExExportTarget *) memAllocExt(numTargets * sizeof(targets[0]),
                                             moduleName);
    if (targets == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Deserialize targets failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // We read in all targets, or none
    for (int ii = 0; ii < numTargets; ii++) {
        const TargetPersistHeader *pHeader = &listHeader->targets[ii];
        const ExExportTarget *target;
        if (pHeader->targetSize != sizeof(*pHeader)) {
            status = StatusTargetCorrupted;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize targets failed on invalid size: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        target = &pHeader->target;

        if (!isValidExTargetType(target->hdr.type) ||
            target->hdr.type == ExTargetUnknownType) {
            status = StatusTargetCorrupted;
            xSyslog(moduleName,
                    XlogErr,
                    "Deserialize targets failed on target %s on invalid"
                    " type: %s",
                    target->hdr.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        targets[ii] = *target;
        ++targetsLoaded;
    }

    assert(targetsLoaded == numTargets);
    *numTargetsFound = targetsLoaded;
    *deserializedTargets = targets;
CommonExit:
    xSyslog(moduleName, XlogInfo, "%d targets loaded", targetsLoaded);
    if (status != StatusOk) {
        if (targets) {
            memFree(targets);
            targets = NULL;
        }
    }

    return status;
}

Status
DataTargetManager::listTargetsInternal(const char *typePattern,
                                       const char *namePattern,
                                       XcalarApiOutput **outputOut,
                                       size_t *listTargetsOutputSize,
                                       ListTargetAccess access)
{
    Status status = StatusOk;
    XcalarApiOutput *output;
    int numTargets = 0;
    int sourceTargets;
    int ii = 0;
    ExTargetType type;
    size_t outputSize;
    bool typeFilter = false;
    XcalarApiListExportTargetsOutput *listTargetsOutput = NULL;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle nsHandle;
    bool nsHandleValid = false;
    LibNs *libNs = LibNs::get();

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s",
                   DataTargetPrefix);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "List Targets %s failed:%s",
                typePattern,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (access == ListTargetAccess::Shared) {
        nsHandle =
            libNs->open(fullyQualName, LibNsTypes::ReaderShared, &status);
        if (status != StatusOk) {
            if (status == StatusAccess) {
                status = StatusTargetInUse;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "List Targets %s failed on NS open:%s",
                    typePattern,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        nsHandleValid = true;
    }

    if (typePattern != NULL && strlen(typePattern) > 0) {
        typeFilter = true;
    }

    // First get the total number of targets across all types
    for (type = (ExTargetType)(ExTargetUnknownType + 1);
         type < (ExTargetType) ExTargetTypeLen;
         type = (ExTargetType)(type + 1)) {
        ExportTargetMgrIface *targetMgr;
        // Check if the type name matches the pattern
        if (typeFilter &&
            !strMatch(typePattern, strGetFromExTargetType(type))) {
            continue;
        }
        targetMgr = exportTargets_[type];
        sourceTargets = targetMgr->countTargets(namePattern);
        numTargets += sourceTargets;
    }

    outputSize = XcalarApiSizeOfOutput(*listTargetsOutput) +
                 numTargets * sizeof(listTargetsOutput->targets[0]);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "List Targets %s failed:%s",
                typePattern,
                strGetFromStatus(status));
        goto CommonExit;
    }

    listTargetsOutput = &output->outputResult.listTargetsOutput;
    *listTargetsOutputSize = outputSize;
    listTargetsOutput->numTargets = numTargets;

    // If there are no sources we can stop here and return
    if (numTargets == 0) {
        *outputOut = output;
    } else {
        // We have the total number of sources, so we can make the workitem
        assert(numTargets > 0);

        ii = 0;
        // Copy in all of the target names
        for (type = (ExTargetType)(ExTargetUnknownType + 1);
             type < (ExTargetType) ExTargetTypeLen;
             type = (ExTargetType)(type + 1)) {
            ExportTargetMgrIface *targetMgr;
            // Check if the type name matches the pattern
            if (typeFilter &&
                !strMatch(typePattern, strGetFromExTargetType(type))) {
                continue;
            }
            targetMgr = exportTargets_[type];
            status = targetMgr->listTargets(listTargetsOutput->targets,
                                            &ii,
                                            numTargets,
                                            namePattern);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "List Targets %s failed:%s",
                        typePattern,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }

        assert(ii == numTargets);
        *outputOut = output;
        status = StatusOk;
    }

CommonExit:
    if (status != StatusOk) {
        if (listTargetsOutput != NULL) {
            memFree(output);
            *outputOut = NULL;
            output = NULL;
        }
    }
    if (nsHandleValid) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "List Targets %s failed on NS close:%s",
                    typePattern,
                    strGetFromStatus(status2));
        }
    }
    return status;
}

void
DataTargetManager::setDefaultExportArgs(ExTargetType type,
                                        LegacyExportMeta *meta)
{
    if (type == ExTargetSFType) {
        meta->specificInput.sfInput.format = DfFormatCsv;
        meta->specificInput.sfInput.formatArgs.csv.fieldDelim =
            DfCsvDefaultFieldDelimiter;
        meta->specificInput.sfInput.formatArgs.csv.recordDelim =
            DfCsvDefaultRecordDelimiter;
        meta->specificInput.sfInput.formatArgs.csv.quoteDelim =
            DfCsvDefaultQuoteDelimiter;
        meta->specificInput.sfInput.splitRule.type = ExSFFileSplitNone;
        meta->specificInput.sfInput.headerType = ExSFHeaderEveryFile;
    } else {
        assert(type == ExTargetUDFType);

        meta->specificInput.udfInput.format = DfFormatCsv;
        meta->specificInput.udfInput.formatArgs.csv.fieldDelim =
            DfCsvDefaultFieldDelimiter;
        meta->specificInput.udfInput.formatArgs.csv.recordDelim =
            DfCsvDefaultRecordDelimiter;
        meta->specificInput.udfInput.formatArgs.csv.quoteDelim =
            DfCsvDefaultQuoteDelimiter;
        meta->specificInput.udfInput.headerType = ExSFHeaderEveryFile;
    }

    meta->createRule = ExExportCreateOnly;
    meta->sorted = true;
}
