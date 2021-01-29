// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <new>
#include <dirent.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>

#include "StrlFunc.h"
#include "shmsg/SharedMemory.h"
#include "sys/XLog.h"
#include "stat/Statistics.h"
#include "strings/String.h"
#include "bc/BufferCache.h"
#include "bc/BufCacheMemMgr.h"

SharedMemoryMgr *SharedMemoryMgr::instance;

SharedMemoryMgr *
SharedMemoryMgr::get()
{
    return instance;
}

Status
SharedMemoryMgr::init(InitLevel level)
{
    Status status = StatusOk;
    bool includeShm = true;

    instance = (SharedMemoryMgr *) memAllocExt(sizeof(SharedMemoryMgr),
                                               SharedMemoryMgr::ModuleName);
    if (instance == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to SharedMemoryMgr::init level %u: %s",
                level,
                strGetFromStatus(status));
        goto CommonExit;
    }
    new (instance) SharedMemoryMgr();
    instance->level_ = level;

    if (level >= InitLevel::UsrNodeWithChildNode) {
        // Remove remnants of SHM artifacts from previous life.
        removeSharedMemory();

        const char *bcPath = getBufCachePath();
        SharedMemory::Type shmType = BufCacheMemMgr::shmType();
        if (shmType == SharedMemory::Type::NonShm ||
            shmType == SharedMemory::Type::ShmSegment) {
            int ret = mkdir(bcPath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
            if (ret < 0) {
                if (errno != EEXIST) {
                    status = sysErrnoToStatus(errno);
                    xSyslog(ModuleName,
                            XlogErr,
                            "Failed to mkdir \"%s\": %s",
                            bcPath,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
            }
        }
    } else if (level == InitLevel::ChildNode) {
        includeShm = false;
    }

    status = instance->setupCoreDumpFilter(includeShm);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to SharedMemoryMgr::init level %u: %s",
                    level,
                    strGetFromStatus(status));

CommonExit:
    if (status != StatusOk) {
        if (instance != NULL) {
            instance->destroy();
        }
    }

    return status;
}

void
SharedMemoryMgr::destroy()
{
    if (instance != NULL) {
        if (level_ >= InitLevel::UsrNodeWithChildNode) {
            // Remove remnants of SHM artifacts from this life.
            removeSharedMemory();
        }
        instance->~SharedMemoryMgr();
        memFree(instance);
        instance = NULL;
    }
}

void
SharedMemoryMgr::removeSharedMemory()
{
    const char *bcPath = getBufCachePath();

    // Always clean up old SHMs in TmpFs to free memory
    Status status = cleanUpFiles(ShmTypeDevShmDir);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogInfo,
                "Failed removeSharedMemory from \"%s\": %s",
                ShmTypeDevShmDir,
                strGetFromStatus(status));
    }

    if (!std::string(bcPath).empty()) {
        // Cleanout out lingering SHM segments. Note that SHM segments
        // require cleaning out the file in bcPath as well as the
        // SHM segment.
        status = cleanUpShmSegments(bcPath);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Failed removeSharedMemory from \"%s\": %s",
                    bcPath,
                    strGetFromStatus(status));
        }

        // Clean up old buffer cache files in file system
        status = cleanUpFiles(bcPath);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Failed removeSharedMemory from \"%s\": %s",
                    bcPath,
                    strGetFromStatus(status));
        }
    }
}

Status
SharedMemoryMgr::setupCoreDumpFilter(bool includeShm)
{
    Status status = StatusOk;
    FILE *fp = NULL;
    const char *fName = "/proc/self/coredump_filter";
    int val;
    int ret = 0;

    // bit 0  Dump anonymous private mappings.
    // bit 1  Dump anonymous shared mappings.
    // bit 2  Dump file-backed private mappings.
    // bit 3  Dump file-backed shared mappings.
    // bit 4  (since Linux 2.6.24)
    //        Dump ELF headers.
    // bit 5  (since Linux 2.6.28)
    //        Dump private huge pages.
    // bit 6  (since Linux 2.6.28)
    //        Dump shared huge pages.
    // Setting 0x3b to make sure we get core from SHM shared mappings.

    val = 0x33;
    if (includeShm) {
        val |= 0x4c;
    }

    // @SymbolCheckIgnore
    fp = fopen(fName, "w");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed to open \"%s\": %s",
                fName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = fprintf(fp, "%#x", val);
    if (ret != sizeof(val)) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed to write %d to \"%s\": %s",
                val,
                fName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
        fp = NULL;
    }
    return status;
}

Status
SharedMemoryMgr::cleanUpShmSegments(const char *dirName)
{
    Status status = StatusOk;
    char name[SharedMemory::NameLength];
    char fqName[SharedMemory::NameLength];
    key_t shmSegKey;
    int shmSegId;
    int shmFlag = 0;
    size_t size = 0;
    int ret = 0;
    bool delFile = false;

    verify((size_t) snprintf(name,
                             sizeof(name),
                             BufferCacheMgr::FileNamePattern,
                             SharedMemory::XcalarShmPrefix,
                             Config::get()->getMyNodeId()) < sizeof(name));

    verify((size_t) snprintf(fqName, sizeof(fqName), "%s/%s", dirName, name) <
           sizeof(fqName));

    xSyslog(ModuleName,
            XlogInfo,
            "SharedMemoryMgr::cleanUpShmSegments name \"%s\", dirName \"%s\"",
            name,
            dirName);

    // Hardcode proj_id.
    shmSegKey = ftok(fqName, SharedMemoryMgr::ProjId);
    if (shmSegKey == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogInfo,
                "Failed ftok for \"%s\": %s",
                fqName,
                strGetFromStatus(status));
        // Cannot find SHM file path. Not a hard error.
        status = StatusOk;
        goto CommonExit;
    }
    delFile = true;

    shmSegId = shmget(shmSegKey, size, shmFlag);
    if (shmSegId == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogInfo,
                "Failed shmget for \"%s\": %s",
                fqName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = shmctl(shmSegId, IPC_RMID, NULL);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogInfo,
                "Failed shmctl IPC_RMID \"%s\": %s",
                fqName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (delFile) {
        ret = unlink(fqName);
        if (ret == -1) {
            Status status2 = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogInfo,
                    "Failed to unlink SHM \"%s\": %s",
                    fqName,
                    strGetFromStatus(status2));
            goto CommonExit;
        }
    }
    return status;
}

// Cleanup any old SHM files created by this node ID on this machine.
Status
SharedMemoryMgr::cleanUpFiles(const char *dirName)
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    unsigned count = 0;
    char pattern[SharedMemory::NameLength];

    verify((size_t) snprintf(pattern,
                             sizeof(pattern),
                             "%s-%x-*",
                             SharedMemory::XcalarShmPrefix,
                             Config::get()->getMyNodeId()) < sizeof(pattern));

    xSyslog(ModuleName,
            XlogInfo,
            "SharedMemoryMgr::cleanUpFiles dirName \"%s\", pattern \"%s\"",
            dirName,
            pattern);

    dirIter = opendir(dirName);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogInfo,
                "Failed to opendir \"%s\" pattern \"%s\": %s",
                dirName,
                pattern,
                strGetFromStatus(status));
        goto CommonExit;
    }

    while (true) {
        int ret = 0;
        errno = 0;
        struct dirent *currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error
            assert(errno == 0);
            // End of directory.
            break;
        }

        if ((strcmp(currentFile->d_name, ".") == 0) ||
            (strcmp(currentFile->d_name, "..") == 0)) {
            // Ignore non-real directories
            continue;
        }

        if (!strMatch(pattern, currentFile->d_name)) {
            continue;
        }

        if (strcmp(dirName, ShmTypeDevShmDir) == 0) {
            ret = shm_unlink(currentFile->d_name);
        } else if (strcmp(dirName, XcalarConfig::get()->bufferCachePath_) ==
                   0) {
            char path[SharedMemory::NameLength];
            verify(snprintf(path,
                            sizeof(path),
                            "%s/%s",
                            XcalarConfig::get()->bufferCachePath_,
                            currentFile->d_name) < (int) sizeof(path));
            ret = unlink(path);
        }

        if (ret == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogInfo,
                    "Failed to unlink SHM \"%s\" dirName \"%s\", pattern "
                    "\"%s\": %s",
                    currentFile->d_name,
                    dirName,
                    pattern,
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }
        count++;
    }

CommonExit:
    if (dirIter != NULL) {
        verify(closedir(dirIter) == 0);
        dirIter = NULL;
    }
    if (status == StatusOk) {
        xSyslog(ModuleName,
                XlogInfo,
                "Unlinked old SHM files dirName \"%s\", pattern \"%s\", count "
                "%u",
                dirName,
                pattern,
                count);
    }
    return status;
}

void
SharedMemory::deleteShm()
{
    int ret = 0;

    if (alreadyDeleted_) {
        return;
    }

    switch (type_) {
    case Type::ShmTmpFs: {
        ret = shm_unlink(name_);
        break;
    }
    case Type::ShmSegment: {
        ret = shmctl(shmSegId_, IPC_RMID, NULL);
        if (ret == -1) {
            if (errno != EIDRM) {
                xSyslog(ModuleName,
                        XlogWarn,
                        "Failed to delete SHM \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
            }
        }
        ret = unlink(name_);
        break;
    }
    case Type::NonShm: {
        ret = unlink(name_);
        break;
    }
    default:
        assert(0 && "Invalid SharedMemory Type");
        return;
        break;
    }

    if (ret < 0) {
        if (errno != ENOENT) {
            xSyslog(ModuleName,
                    XlogWarn,
                    "Failed to delete SHM \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
        }
    } else {
        alreadyDeleted_ = true;
    }
}

void
SharedMemory::deleteOrDetach()
{
    int ret = 0;

    if (mapAddr_ == NULL) {
        return;
    }

    switch (type_) {
    case Type::NonShm:  // pass through
    case Type::ShmTmpFs: {
        if (state_ == State::Attached) {
            // Allow state to be completely flushed synchronously.
            ret = msync(mapAddr_, bytes_, MS_SYNC);
            if (ret == -1) {
                xSyslog(ModuleName,
                        XlogWarn,
                        "Failed to msync SHM region name \"%s\" dirName "
                        "\"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
            }
        }

        ret = memUnmap(mapAddr_, bytes_);
        if (ret != 0) {
            xSyslog(ModuleName,
                    XlogWarn,
                    "Failed to munmap SHM region name \"%s\" dirName \"%s\": "
                    "%s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
        }

        if (fd_ != -1) {
            close(fd_);
            fd_ = -1;
        }
        break;
    }
    case Type::ShmSegment: {
        ret = shmdt(mapAddr_);
        if (ret == -1) {
            xSyslog(ModuleName,
                    XlogWarn,
                    "Failed to shmdt SHM region name \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
        }
        if (fd_ != -1) {
            close(fd_);
            fd_ = -1;
        }
        break;
    }
    default: {
        assert(0 && "Invalid SharedMemory Type");
        break;
    }
    }

    if (state_ == State::Created) {
        deleteShm();
    }

    mapAddr_ = NULL;
}

void
SharedMemory::detach()
{
    assert(state_ == State::Attached);

    deleteOrDetach();
    state_ = State::Detached;
}

void
SharedMemory::del()
{
    assert(state_ == State::Created);

    deleteOrDetach();
    state_ = State::Deleted;
}

SharedMemory::SharedMemory()
{
    memZero(&name_, sizeof(name_));
    memZero(&created_, sizeof(created_));
    memZero(&shmSegKey_, sizeof(shmSegKey_));
    memZero(&shmSegId_, sizeof(shmSegId_));
}

SharedMemory::~SharedMemory()
{
    assert(state_ != State::Destructed);
    state_ = State::Destructed;
}

Status
SharedMemory::createOrAttach(const char *name, size_t bytes)
{
    Status status = StatusOk;
    int openFlags = O_RDWR | O_CLOEXEC;
    int openMode = 0;
    int ret;

    if (strlen(name) > NameLength) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "SHM open failed for \"%s\" dirName \"%s\": %s",
                name,
                shmDirName(),
                strGetFromStatus(status));
        return status;
    }

    bytes_ = bytes;

    if (state_ == State::Created) {
        openFlags |= O_CREAT | O_EXCL;  // Create. Fail if exists.

        // XXX Allow caller to specify custom permissions.
        openMode = S_IRUSR | S_IWUSR;  // Readable/writable by user only.
    } else {
        assert(state_ == State::Attached);
        assert(bytes_ == 0);
        // NOOP
    }

    switch (type_) {
    case Type::ShmTmpFs: {
        verify(snprintf(name_, sizeof(name_), "%s", name) <
               (int) sizeof(name_));

        // Create/open "shared memory object". See shm_open(3) and
        // shm_overview(7) for process followed to create shared section of
        // memory.
        fd_ = shm_open(name_, openFlags, openMode);
        if (fd_ == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "shm_open failed for \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        if (state_ == State::Created) {
            if (ftruncate(fd_, bytes_) != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "ftruncate failed for \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
                goto CommonExit;
            }
        } else {
            // Lookup size.
            struct stat statsBuf;
            if (fstat(fd_, &statsBuf) != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "fstat failed for \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
                goto CommonExit;
            }
            bytes_ = statsBuf.st_size;
        }

        mapAddr_ =
            memMap(NULL, bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mapAddr_ == MAP_FAILED) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "mmap failed for \"%s\" dirName\"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            mapAddr_ = NULL;
            goto CommonExit;
        }
        break;
    }

    case Type::NonShm: {
        verify(snprintf(name_, sizeof(name_), "%s/%s", shmDirName(), name) <
               (int) sizeof(name_));

        // create/open a regular file.
        fd_ = open(name_, openFlags, openMode);
        if (fd_ == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "open failed for \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        if (state_ == State::Created) {
            if (ftruncate(fd_, bytes_) != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "ftruncate failed for \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
                goto CommonExit;
            }

            // Thick allocate, so mmap does not fail with SIGBUS.
            ret = posix_fallocate(fd_, 0, bytes_);
            if (ret != 0) {
                xSyslog(ModuleName,
                        XlogErr,
                        "fallocate failed for \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(ret)));

                // fallocate may fail on some FSs, so let's try dd, which
                // is a lot slower
                try {
                    std::string ioStr;
                    ioStr.append("dd if=/dev/zero of=" + std::string(name_) +
                                 " bs=" + std::to_string(PageSize) +
                                 " count=" + std::to_string(bytes_ / PageSize));
                    status = runSystemCmd(ioStr);
                } catch (std::exception &e) {
                    status = StatusNoMem;
                }
                BailIfFailedMsg(ModuleName,
                                status,
                                "Failed to thick allocate \"%s\" dirName "
                                "\"%s\": %s",
                                name_,
                                shmDirName(),
                                strGetFromStatus(status));
            }
        } else {
            // Lookup size.
            struct stat statsBuf;
            if (fstat(fd_, &statsBuf) != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "fstat failed for \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
                goto CommonExit;
            }
            bytes_ = statsBuf.st_size;
        }

        mapAddr_ =
            memMap(NULL, bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mapAddr_ == MAP_FAILED) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "mmap failed for \"%s\" dirName\"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            mapAddr_ = NULL;
            goto CommonExit;
        }
        break;
    }

    case Type::ShmSegment: {
        int shmFlag = openMode;
        if (state_ == State::Created) {
            shmFlag |= (IPC_CREAT | IPC_EXCL);
        }

        verify(snprintf(name_, sizeof(name_), "%s/%s", shmDirName(), name) <
               (int) sizeof(name_));

        // create regular file.
        fd_ = open(name_, openFlags, openMode);
        if (fd_ == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "open failed for \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        // Hardcode proj_id.
        shmSegKey_ = ftok(name_, SharedMemoryMgr::ProjId);
        if (shmSegKey_ == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed ftok for \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        shmSegId_ = shmget(shmSegKey_, bytes_, shmFlag);
        if (shmSegId_ == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed shmget for \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        mapAddr_ = shmat(shmSegId_, NULL, 0);
        if ((uintptr_t) mapAddr_ == (uintptr_t) -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed shmat for \"%s\" dirName \"%s\": %s",
                    name_,
                    shmDirName(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            mapAddr_ = NULL;
            goto CommonExit;
        }

        // Lookup size
        if (state_ == State::Attached) {
            struct shmid_ds ds;
            ret = shmctl(shmSegId_, IPC_STAT, &ds);
            if (ret == -1) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed shmctl IPC_STATE for \"%s\" dirName \"%s\": %s",
                        name_,
                        shmDirName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
                goto CommonExit;
            }
            bytes_ = ds.shm_segsz;
        }
        break;
    }

    default: {
        assert(0 && "SharedMemory type not valid");
        status = StatusInval;
        goto CommonExit;
        break;
    }
    }

    // Expect pages to be referenced in random order. Hence, read ahead
    // may be less useful than normally.
    ret = madvise(mapAddr_, bytes_, MADV_RANDOM);
    if (ret < 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "SHM madvise failed for \"%s\" dirName \"%s\": %s",
                name_,
                shmDirName(),
                strGetFromStatus(sysErrnoToStatus(errno)));
        goto CommonExit;
    }

    verify(clock_gettime(CLOCK_REALTIME, &created_) == 0);
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        deleteOrDetach();
    }
    return status;
}

Status
SharedMemory::createAttachWrap(const char *name,
                               size_t bytes,
                               Type type,
                               State state)
{
    assert(state_ == State::Invalid || state_ == State::Deleted ||
           state_ == State::Detached);
    state_ = state;

    assert(validType(type));
    type_ = type;

    Status status = createOrAttach(name, bytes);
    if (status != StatusOk) {
        state_ = State::Invalid;
    }
    return status;
}

Status
SharedMemory::create(const char *name, size_t bytes, Type type)
{
    return createAttachWrap(name, bytes, type, State::Created);
}

Status
SharedMemory::attach(const char *name, Type type)
{
    return createAttachWrap(name, 0, type, State::Attached);
}

// Called when no other processes are expected/wanted to attach to this
// region. Will prevent further attaching and configure region to be deleted
// when all attached processes detach. Must only be called in creator
// process.
void
SharedMemory::doneAttaching()
{
    deleteShm();
}

const char *
SharedMemory::shmDirName()
{
    switch (type_) {
    case Type::ShmTmpFs: {
        return SharedMemoryMgr::ShmTypeDevShmDir;
    }
    case Type::ShmSegment: {
        return XcalarConfig::get()->bufferCachePath_;
    }
    case Type::NonShm: {
        return XcalarConfig::get()->bufferCachePath_;
    }
    default: {
        assert(0 && "SharedMemory Type invalid");
        return "";
    }
    }
}
