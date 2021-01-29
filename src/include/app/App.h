// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APP_H
#define APP_H

#include "primitives/Primitives.h"
#include "app/AppInstance.h"
#include "app/AppGroup.h"
#include "libapis/LibApisConstants.h"
#include "util/StringHashTable.h"
#include "hash/Hash.h"
#include "runtime/Semaphore.h"

//
// An App is user-provided code that runs on the Xcalar platform. This class
// simply describes an App that can be run.
//
class App final
{
    friend class AppMgr;

  public:
    // Type of XPU needed to execute this App.
    enum class HostType {
        Invalid = 0,
        Python2,
        // XXX Dead field but cannot delete it, since that would break
        // durability.
        Cpp,
    };

    // Bit set describing whether on App is a built-in App and what it
    // can be used for.
    static constexpr uint32_t FlagNone = 0;
    static constexpr uint32_t FlagBuiltIn = 1 << 0;
    static constexpr uint32_t FlagImport = 1 << 1;
    static constexpr uint32_t FlagExport = 1 << 2;
    static constexpr uint32_t FlagInstancePerNode = 1 << 3;
    static constexpr uint32_t FlagSysLevel = 1 << 4;
    static constexpr uint32_t FlagUsrLevel = 1 << 5;
    static constexpr uint32_t FlagMask = FlagBuiltIn | FlagImport | FlagExport |
                                         FlagInstancePerNode | FlagSysLevel |
                                         FlagUsrLevel;
    static constexpr uint32_t FlagInvalid = (uint32_t) -1;

    // On-disk/on-wire representation of an App.
    struct Packed {
        enum class Version : uint32_t {
            Current = 1,
        };
        uint32_t version;  // Must be first!
        HostType hostType;
        uint32_t flags;
        char name[XcalarApiMaxAppNameLen + 1];
        uint64_t reserved[16];
        ssize_t execSize;
        uint8_t exec[0];
    };

    App();
    ~App();

    static HostType parseHostType(const char *str);
    static uint32_t parseDuty(const char *str);

    // Accessors.
    const char *getName() const { return name_; }

    HostType getHostType() const { return hostType_; }

    uint32_t getFlags() const { return flags_; }

    void getExec(const uint8_t **exec, size_t *execSize) const
    {
        *exec = exec_;
        *execSize = execSize_;
    }

  private:
    static constexpr const char *ModuleName = "App";

    // Whitelist of characters allowed in App name.
    static constexpr const char *NameAllowedChars =
        "ABCDEFGHIKJLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789_-";

    // A guess at the number of Apps we're expecting to be registered. Guess
    // big, this doesn't take up much space.
    static constexpr unsigned SlotsApp = 11;

    Mutex lock_;
    StringHashTableHook hook_;  // Used for hashtable in AppMgr.

    char name_[XcalarApiMaxAppNameLen + 1];
    HostType hostType_;
    uint32_t flags_;

    // Blob of memory passed as-is to host to execute the App. Host-specific
    // format.
    size_t execSize_;
    uint8_t *exec_;

    Status init(const char *name,
                HostType hostType,
                uint32_t flags,
                const uint8_t *exec,
                size_t execSize);
    Status update(HostType hostType,
                  uint32_t flags,
                  const uint8_t *exec,
                  size_t execSize);
    Status save();
    void undoSave();
    Status validate(const char *name,
                    HostType hostType,
                    uint32_t flags,
                    size_t execSize);
    static Status restore(const char *name);
    static Status pack(const char *name,
                       App::HostType hostType,
                       uint32_t flags,
                       const uint8_t *exec,
                       size_t execSize,
                       App::Packed **packOut,
                       size_t *packSizeOut);
    static Status unpack(const void *payload,
                         size_t payloadSize,
                         const App::Packed **packOut);

    // Disallow.
    App(const App &) = delete;
    App &operator=(const App &) = delete;

  public:
    typedef StringHashTable<App, &App::hook_, &App::getName, SlotsApp>
        MgrHashTable;
};

#endif  // APP_H
