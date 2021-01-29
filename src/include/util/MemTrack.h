// Copyright 2015-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MEMTRACK_H_
#define _MEMTRACK_H_

#include <sys/mman.h>
#include <malloc.h>

#include "primitives/Primitives.h"
#include "msg/MessageTypes.h"
#include "constants/XcalarConfig.h"
#include "util/System.h"

static constexpr const uint64_t XcalarApiMaxMemTagNameLen = 30;
static constexpr const uint64_t XcalarApiMaxMemTagNum = 1000;

// From MessageTypes.h.
enum MsgCallReason : uint32_t;
struct MsgEphemeral;

MustCheck Status memTrackInit();

#ifdef MEMORY_TRACKING
MustCheck NonNull(3) void *_memAlloc(size_t size,
                                     const char *tag,
                                     const char *fileLocation);

MustCheck NonNull(4) void *_memAllocAligned(size_t alignment,
                                            size_t bytes,
                                            const char *tag,
                                            const char *fileLocation);

MustCheck NonNull(4) void *_memRealloc(void *p,
                                       size_t size,
                                       const char *tag,
                                       const char *fileLocation);

MustCheck NonNull(4) void *_memCalloc(size_t nmemb,
                                      size_t size,
                                      const char *tag,
                                      const char *fileLocation);

void *_memMap(void *addr,
              size_t length,
              int prot,
              int flags,
              int fd,
              off_t offset,
              const char *fileLocation);
#else
// XXX consider a lighter-weight tracking mechanism in production builds;
// until then, disable memory tracking.  currently hot allocs at the same
// location hitting across multiple threads will land on the same tag and
// thus cause contention on the same hash slot lock in tagHashTable
// @SymbolCheckIgnore
#define _memAlloc(size, tag, location) ::malloc((size))
#define _memAllocAligned(alignment, size, tag, location) \
    ::memalign((alignment), (size))
#define _memRealloc(ptr, size, tag, location) ::realloc((ptr), (size))
#define _memCalloc(nmemb, size, tag, location) ::calloc((nmemb), (size))
#define _memMap(addr, length, prot, flags, fd, offset, location) \
    ::mmap(addr, length, prot, flags, fd, offset)
#endif  // MEMORY_TRACKING

// This is used by passing in a (const char *) as tag to label the
// memory allocation. This is primarily a wrapper around malloc
#define memAllocExt(size, tag) _memAlloc(size, tag, FileLocationString)

// This is basically the same thing as memAlloc, except that it is
// not required to pass in a tag. In this case, less information will be
// provided, but it will still track the memory. This pattern is repeated
// for all memory allocation functions below
#define memAlloc(size) _memAlloc(size, NULL, FileLocationString)

#define memMap(addr, length, prot, flags, fd, offset) \
    _memMap(addr, length, prot, flags, fd, offset, FileLocationString)
// memAllocAligned
//!
//! \brief Allocate Aligned memory
//!
//! Same as malloc() except also ensure that memory is Aligned on the specified
//! boundary.
//!
//! \param[in] alignment alignment boundary; must be a power of 2 and a
//!            mulitple of sizeof(void *)
//! \param[in] bytes number of bytes to allocate
//!
//! \returns pointer to allocated memory or NULL
//!
#define memAllocAlignedExt(alignment, bytes, tag) \
    _memAllocAligned(alignment, bytes, tag, FileLocationString)
#define memAllocAligned(alignment, bytes) \
    _memAllocAligned(alignment, bytes, NULL, FileLocationString)

// memRealloc
#define memReallocExt(p, size, tag) \
    _memRealloc(p, size, tag, FileLocationString)
#define memRealloc(p, size) _memRealloc(p, size, NULL, FileLocationString)

// memCalloc
#define memCallocExt(nmemb, size, tag) \
    _memCalloc(nmemb, size, tag, FileLocationString)
#define memCalloc(nmemb, size) _memCalloc(nmemb, size, NULL, FileLocationString)

// memAlloca
// Do alloca but with an added check to catch large sizes
// @SymbolCheckIgnore
#define memAlloca(size) \
    alloca(size);       \
    assert(size < 128 * KB);

// This is the function to use to free any memory allocated by
// memAlloc, memRealloc, or memCalloc.
// memAllocAligned should call memAlignedFree to free memory
#ifdef MEMORY_TRACKING
void memFree(void *p);

NonNull(1) void pseudoFree(void *p);

int memUnmap(void *p, size_t length);

// This is only to be used to free aligned memory which was allocated
// using memAllocAligned
NonNull(1) void memAlignedFree(void *p);
#else
// XXX see above comments on disabling memory tracking in production builds
// @SymbolCheckIgnore
#define memFree(ptr) free((ptr))
// @SymbolCheckIgnore
#define memAlignedFree(ptr) free((ptr))
#define memUnmap(ptr, size) munmap(ptr, size)
#define pseudoFree(ptr)
#endif  // MEMORY_TRACKING

// Marks memory to not be written into a core dump, p will be page aligned
static inline Status
memMarkDontCoreDump(void *p, size_t size)
{
    int ret;
    // Sometimes this function will do nothing if the user wants a full core
    if (!XcalarConfig::get()->minidump_) {
        return StatusOk;
    }

    if (size < PageSize) {
        return StatusOk;
    }

    uintptr_t pStart = (uintptr_t) p;
    uintptr_t pEnd = (uintptr_t) p + size;

    // Go to the next page so we still dump the useful info on this page
    pStart = roundUp(pStart, PageSize);

    // Round down so we still dump the useful info on the last page
    pEnd = roundDown(pEnd, PageSize);

    if (pEnd <= pStart) {
        return StatusOk;
    }

    size_t finalSize = pEnd - pStart;
    assert(finalSize > 0 && finalSize <= size && (finalSize % PageSize) == 0);
    ret = madvise((void *) pStart, finalSize, MADV_DONTDUMP);
    if (ret != 0) {
        return sysErrnoToStatus(errno);
    }
    return StatusOk;
}

// Marks memory to be written into a core dump, p will be page aligned
static inline Status
memMarkDoCoreDump(void *p, size_t size)
{
    int ret;
    // Sometimes this function will do nothing if the user wants a full core
    if (!XcalarConfig::get()->minidump_) {
        return StatusOk;
    }

    if (size < PageSize) {
        return StatusOk;
    }

    uintptr_t pStart = (uintptr_t) p;
    uintptr_t pEnd = (uintptr_t) p + size;

    // Go to the next page so we still dump the useful info on this page
    pStart = roundUp(pStart, PageSize);

    // Round down so we still dump the useful info on the last page
    pEnd = roundDown(pEnd, PageSize);

    if (pEnd <= pStart) {
        return StatusOk;
    }

    size_t finalSize = pEnd - pStart;
    assert(finalSize > 0 && finalSize <= size && (finalSize % PageSize) == 0);
    ret = madvise((void *) pStart, finalSize, MADV_DODUMP);
    if (ret != 0) {
        return sysErrnoToStatus(errno);
    }
    return StatusOk;
}

void memTrackDestroy(bool reportLeaks);

// 2PC local function to return memory tracking info
void memCalculateUsage(MsgEphemeral *eph, void *payload);

void *memAllocJson(size_t size);
void memFreeJson(void *p);

#endif  // _MEMTRACK_H_
