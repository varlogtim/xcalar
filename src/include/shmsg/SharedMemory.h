// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SHAREDMEMORY_H_
#define _SHAREDMEMORY_H_

#include <time.h>
#include "primitives/Primitives.h"
#include "stat/Statistics.h"
#include "common/InitLevel.h"
#include "constants/XcalarConfig.h"

//
// This module wraps the linux SHM API. It allows consumers to create new
// shared memory regions or attach to ones created by other processes. "Attach",
// here, means "map entirety of region into my address space".
//
class SharedMemoryMgr final
{
  public:
    static constexpr const char *ShmTypeDevShmDir = "/dev/shm";
    static constexpr const int ProjId = 'x';

    static MustCheck SharedMemoryMgr *get();
    static MustCheck Status init(InitLevel initLevel);
    static void removeSharedMemory();
    void destroy();

    MustCheck InitLevel getInitLevel() { return level_; }

    static MustCheck const char *getBufCachePath()
    {
        return (const char *) XcalarConfig::get()->bufferCachePath_;
    }

  private:
    static constexpr const char *ModuleName = "libshm";
    static SharedMemoryMgr *instance;
    InitLevel level_;

    static MustCheck Status cleanUpFiles(const char *dirName);
    static MustCheck Status cleanUpShmSegments(const char *dirName);
    MustCheck Status setupCoreDumpFilter(bool includeShm);

    SharedMemoryMgr() {}
    ~SharedMemoryMgr() {}

    // Disallow.
    SharedMemoryMgr(const SharedMemoryMgr &) = delete;
    SharedMemoryMgr &operator=(const SharedMemoryMgr &) = delete;
};

class SharedMemory final
{
  public:
    static constexpr const char *XcalarShmPrefix = "xcalar-shm";
    static constexpr const size_t NameLength = XcalarApiMaxPathLen + 1;

    enum class Type {
        Invalid,
        ShmTmpFs,
        ShmSegment,
        NonShm,
    };

    MustCheck bool validType(Type type)
    {
        switch (type) {
        case Type::ShmTmpFs:
        case Type::ShmSegment:
        case Type::NonShm:
            return true;
        default:
            return false;
        }
    }

    SharedMemory();
    ~SharedMemory();

    MustCheck Status create(const char *name, size_t bytes, Type type);
    MustCheck Status attach(const char *name, Type type);

    void detach();
    void del();

    void doneAttaching();

    MustCheck void *getAddr() { return mapAddr_; }

    MustCheck int getFd() { return fd_; }

    MustCheck size_t getSize() const { return bytes_; }

    MustCheck const char *getName() const { return name_; }

    MustCheck bool isBacked() const
    {
        return state_ == State::Created || state_ == State::Attached;
    }

    MustCheck bool isAttached() const { return state_ == State::Attached; }

  private:
    static constexpr const char *ModuleName = "libshm";

    enum class State {
        Invalid = 10,
        Created,
        Attached,
        Deleted,
        Detached,
        Destructed
    };

    void deleteShm();
    MustCheck Status createOrAttach(const char *name, size_t bytes);
    void deleteOrDetach();
    MustCheck const char *shmDirName();
    MustCheck Status createAttachWrap(const char *name,
                                      size_t bytes,
                                      Type type,
                                      State state);

    // Disallow.
    SharedMemory(const SharedMemory &) = delete;
    SharedMemory &operator=(const SharedMemory &) = delete;

    State state_ = State::Invalid;

    // File descriptor returned by shm_open to manage segment.
    int fd_ = -1;

    // Used to avoid unlinking twice.
    bool alreadyDeleted_ = false;

    // Size of segment in bytes. Assumes full segment is mapped.
    size_t bytes_ = 0;

    // Tells where segment is mapped in this process. Assumes all valid
    // SharedMem objects are mapped somewhere.
    void *mapAddr_ = NULL;

    // When was this object originally created or attached?
    struct timespec created_;

    // Identifies segment. Must be unique to machine.
    char name_[NameLength];

    Type type_ = Type::Invalid;

    // Used with shm segments
    key_t shmSegKey_;
    int shmSegId_;
};

#endif  // _SHAREDMEMORY_H_
