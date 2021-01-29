// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "StrlFunc.h"
#include "app/App.h"
#include "app/AppMgr.h"
#include "app/AppGroup.h"
#include "AppGroupMgr.h"
#include "app/AppInstance.h"
#include "strings/String.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "sys/XLog.h"
#include "util/FileUtils.h"
#include "util/MemTrack.h"

//
// Interface for managing Apps.
//

App::App()
    : hostType_(App::HostType::Invalid),
      flags_(App::FlagNone),
      execSize_(0),
      exec_(NULL)
{
}

App::~App()
{
    memFree(exec_);
    exec_ = NULL;
}

Status
App::init(const char *name,
          App::HostType hostType,
          uint32_t flags,
          const uint8_t *exec,
          size_t execSize)
{
    Status status = validate(name, hostType, flags, execSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s init failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }

    lock_.lock();

    exec_ = (uint8_t *) memAlloc(execSize);
    if (exec_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s init failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }
    execSize_ = execSize;
    memcpy(exec_, exec, execSize_);

    verify(strlcpy(name_, name, sizeof(name_)) < sizeof(name_));
    hostType_ = hostType;
    flags_ = flags;

    lock_.unlock();
    return StatusOk;
}

Status
App::pack(const char *name,
          App::HostType hostType,
          uint32_t flags,
          const uint8_t *exec,
          size_t execSize,
          App::Packed **packOut,
          size_t *packSizeOut)
{
    Status status = StatusOk;
    const size_t packSize =
        roundUp(sizeof(App::Packed) + execSize,
                LogLib::get()->getMinLogicalBlockAlignment());

    App::Packed *pack = (App::Packed *)
        memAllocAligned(LogLib::get()->getMinLogicalBlockAlignment(), packSize);
    if (pack == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s pack failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }
    memZero(pack, packSize);  // Clear residual data.

    if (strlcpy(pack->name, name, sizeof(pack->name)) >= sizeof(pack->name)) {
        memAlignedFree(pack);
        status = StatusAppNameInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "App %s pack failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }

    pack->hostType = hostType;
    pack->flags = flags;
    pack->execSize = execSize;
    pack->version = (uint32_t) Packed::Version::Current;

    memcpy(pack->exec, exec, execSize);

    *packSizeOut = packSize;
    *packOut = pack;
    return status;
}

// Performs logic that needs to be done every time an App::Packed comes in from
// somewhere untrusted.
Status
App::unpack(const void *payload,
            size_t payloadSize,
            const App::Packed **packOut)
{
    Status status = StatusOk;

    if (payloadSize < sizeof(App::Packed)) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "App unpack failed: %s",
                strGetFromStatus(status));
        return status;
    }

    const App::Packed *pack = (const App::Packed *) payload;
    if (roundUp(sizeof(App::Packed) + pack->execSize,
                LogLib::get()->getMinLogicalBlockAlignment()) != payloadSize) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "App unpack failed: %s",
                strGetFromStatus(status));
        return status;
    }

    if (pack->version != (uint32_t) Packed::Version::Current) {
        status = StatusVersionMismatch;
        xSyslog(ModuleName,
                XlogErr,
                "App unpack failed: %s",
                strGetFromStatus(status));
        return status;
    }

    *packOut = pack;
    return status;
}

App::HostType  // static
App::parseHostType(const char *str)
{
    if (strncmp("Python", str, strlen("Python") + 1) == 0) {
        return HostType::Python2;
    } else if (strncmp("Python2", str, strlen("Python2") + 1) == 0) {
        return HostType::Python2;
    }
    return HostType::Invalid;
}

uint32_t  // static
App::parseDuty(const char *str)
{
    if (strncmp("None", str, strlen("None") + 1) == 0) {
        return FlagNone;
    } else if (strncmp("", str, strlen("") + 1) == 0) {
        return FlagNone;
    } else if (strncmp("Import", str, strlen("Import") + 1) == 0) {
        return FlagImport;
    } else if (strncmp("Export", str, strlen("Export") + 1) == 0) {
        return FlagExport;
    } else {
        return FlagInvalid;
    }
}

Status
App::validate(const char *name,
              App::HostType hostType,
              uint32_t flags,
              size_t execSize)
{
    const size_t nameLen = strnlen(name, XcalarApiMaxAppNameLen + 1);
    Status status = StatusOk;

    if (nameLen == 0 || nameLen > XcalarApiMaxAppNameLen ||
        strspn(name, NameAllowedChars) < nameLen) {
        status = StatusAppNameInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "App %s validate failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }

    switch (hostType) {
    case HostType::Python2:
        break;
    default:
        status = StatusAppHostTypeInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "App %s validate failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }

    if (flags & ~FlagMask) {
        status = StatusAppFlagsInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "App %s validate failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }

    if (execSize > AppInstance::InputOutputLimit) {
        status = StatusAppExecTooBig;
        xSyslog(ModuleName,
                XlogErr,
                "App %s validate failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }

    return status;
}

Status
App::update(App::HostType hostType,
            uint32_t flags,
            const uint8_t *exec,
            size_t execSize)
{
    Status status = StatusOk;

    status = validate(name_, hostType, flags, execSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s update failed: %s",
                name_,
                strGetFromStatus(status));
        return status;
    }

    lock_.lock();

    if (execSize_ != execSize || memcmp(exec_, exec, execSize_) != 0) {
        uint8_t *tmp = (uint8_t *) memAlloc(execSize);
        if (tmp == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s update failed: %s",
                    name_,
                    strGetFromStatus(status));
            return status;
        }
        memcpy(tmp, exec, execSize);
        memFree(exec_);
        exec_ = tmp;
        execSize_ = execSize;
    }

    hostType_ = hostType;
    flags_ = flags;

    lock_.unlock();

    return status;
}

//
// Saving to persistent storage and restoring back into memory.
//

Status
App::save()
{
    if (flags_ & FlagBuiltIn) {
        // Built-in Apps not persisted.
        return StatusOk;
    }

    Status status;

    LogLib::Handle log;
    bool logOpened = false;

    // Get serialized representation of App.
    size_t packSize;
    Packed *pack;
    status =
        App::pack(name_, hostType_, flags_, exec_, execSize_, &pack, &packSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s save failed: %s",
                name_,
                strGetFromStatus(status));
        return status;
    }

    // Open or create log. Overwrite existing record.
    status = LogLib::get()->create(&log,
                                   LogLib::AppPersistDirIndex,
                                   name_,
                                   LogLib::FileSeekToStart |
                                       LogLib::FileDontPreallocate,
                                   1,   // Log file count.
                                   0);  // Log file size.
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s save failed: %s",
                name_,
                strGetFromStatus(status));
        goto CommonExit;
    }
    logOpened = true;

    // Log should have only a single record describing the latest version of the
    // App.
    status = LogLib::get()->writeRecord(&log,
                                        (void *) pack,
                                        packSize,
                                        LogLib::WriteOverwrite);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s save failed: %s",
                name_,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    memAlignedFree(pack);
    if (logOpened) {
        LogLib::get()->close(&log);
    }
    return status;
}

void
App::undoSave()
{
    Status status =
        LogLib::get()->fileDelete(LogLib::AppPersistDirIndex, name_);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to undo save of App '%s': %s",
                name_,
                strGetFromStatus(status));
    }
}

Status  // static
App::restore(const char *name)
{
    Status status;
    LogLib::Handle log;
    bool logOpen = false;

    status = LogLib::get()->create(&log,
                                   LogLib::AppPersistDirIndex,
                                   name,
                                   LogLib::FileReturnErrorIfExists |
                                       LogLib::FileSeekToStart,
                                   1,   // Log file count.
                                   0);  // Log file size.
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        return status;
    }
    logOpen = true;

    // Get needed size.
    size_t packSize;
    void *packBuf = NULL;
    const App::Packed *pack;
    status = LogLib::get()->readRecord(&log,
                                       NULL,
                                       0,
                                       &packSize,
                                       LogLib::ReadFromStart);
    assert(status != StatusOk);  // No zero-length log records.
    if (status != StatusOverflow) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    packBuf = memAlloc(packSize);
    if (packBuf == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = LogLib::get()->readRecord(&log,
                                       packBuf,
                                       packSize,
                                       &packSize,
                                       LogLib::ReadFromStart);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    LogLib::get()->close(&log);
    logOpen = false;

    status = App::unpack(packBuf, packSize, &pack);  // Does version check.
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (pack->flags & FlagBuiltIn) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = AppMgr::get()->createApp(pack->name,
                                      pack->hostType,
                                      pack->flags,
                                      pack->exec,
                                      pack->execSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s restore failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (logOpen) {
        LogLib::get()->close(&log);
    }
    if (packBuf != NULL) {
        memFree(packBuf);
    }
    return status;
}
