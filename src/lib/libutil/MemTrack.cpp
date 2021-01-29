// Copyright 2015 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// -----
// This library allows the user to automatically track their memory usage
// and profile it by code location (File/line) as well as a symantic tag
// ("tableData","messageQueue", etc.). The goals for this are as follows
// 1.Track line-level memory usage
// 2.Track module memory usage
// 3.Minimize global-level need for modification
//     (i.e. enum{memSourceTable, memSourceMessages, ...})
// 4.Don't require a user to have to include the semantic tag
// 5.Integrate into existing statistics system
//     This allows for aggregation to be passed along to a user outside
//     this library, and can be queried and aggregated in another system
//
// To this end, the system can be used as follows:
// char *newString = memAlloc(10*sizeof(char), "User Name");
// ...// Use the string normally
// memFree(newString);
//
// In order to reduce memory and space utilization, each pointer is stored
// by adding on space for a header to each malloc call. This results in
// enough metadata for each allocation that at free-time it can be tracked
// to its user specified memory tag and update the memory usage appropriately.
//
// ========Issues ========
//
// The main issue with this approach is that it messes up memalign type calls.
// In order to do a header in memory, with the user pointer still being
// aligned, one of a few things needs to happen:
// 1. Existence of memAlignWithOffset(size_t alignment, size_t offset, ....)
//      This would perfectly fix the problem and let us store the header,
//      while keeping the user memory appropriately aligned.
//      Unfortunately, this doesn't exist and would require a custom malloc.
//      Writign a custom memory allocator is not completely unreasonable, but
//      is a significant cost that should be very carefully considered
//
// 2. Peek at the malloc internal structure.
//      Memory systems store this exact information, and we could peek above
//      the pointer at free time to get the information we need. However,
//      this is extremely platform dependent and could be changed if the
//      malloc implementation is updated. This is unacceptable.
//
// 3. Interface change, adding memAlignFree(void *p, size_t orig_size);
//      Changing the interface so that the user must keep track of the size
//      of memaligned memory would fix this problem, because it would mean
//      this library could use a footer instead of a header. This way, the
//      user's memory would be aligned, and we could use the provided size
//      at free time in order to figure out exactly where the footer is to
//      update memory metadata. This interface change shifts the burden to
//      the user, which is acceptable in some instances, but there are cases
//      where the user does not already have access to the size information,
//      potentially necessitating a code refactor, which is unacceptable.
//      The only current place where this code refactor would need to happen
//      would be Xdb, which seems to lose the size of XdbPage information.
//      POTENTIALLY THIS COULD BE FIXED. However, in future cases it might
//      not be reasonable to track this.
//
// 4.Separate handling of memalign freeing
//      memAlign could instead use something which is not a header at all,
//      and is a separately allocated struct which points to the aligned
//      memory. This separately allocated struct would then be stored in a
//      hash table at allocation time, and looked up at free time in order
//      to be able to update metadata. So this cost of this is as follows:
//      1 extra ~16 byte allocation at allocation time
//      1 extra hashtable lookup at free time
//      The benefit of this is that it perfectly follows the posix memalign
//      interface and places no burden on the user.
//      In order to achieve this separate handling, however, the memAlloc
//      system needs to know that this case must be handled differently
//      (in fact, this is true for option 3 as well), and there are two
//      ways of handling this.
//         a. Check every single free call to see if it is being stored
//            in the hashtable as a memAligned pointer. This hurts the
//            common case by adding a necessary hash lookup at free time.
//         b. Change the interface so memAlign must use a different function
//            to free memory. memAlignedFree(void *p); This could be an issue
//            if the user might have allocated this structure as aligned or
//            as unaligned. However, this is not a very common pattern.
//      Neither of these methods are great, but option b is preferrable, as
//      long as the performance critical components of the system tend to use
//      malloc instead of memalign.
//
// Of these options, 1 and 2 are not being considered here. 1 is far too great
// of a liability to consider presently. 2 would introduce an extremely large
// technical debt that would need to be balanced if a platform change/update
// was made.
//
// Options 3 and 4.b place some amount of burden on the user. 3 is the highest
// burden, requiring size tracking for all memaligned data, whereas 4.b only
// requires the user to know what memory has been memaligned, vs. malloced.
// 4.a retains the current interface, but hurts the common case for a normal
// free, which is conventially a low-cost constant time function.
//
// Given these options, 4.b has the best tradeoff of low-user burden, and
// completely protects the common case. At some point the interface could
// be changed to use option 3, and this would require a slight refactor
// of memAlignedFree to use a footer instead of a header
//
// ===
//
// Another important consideration is the mapping relationship between tags
// and file locations. If there is a library that allocates a lot of memory
// on behalf of another library, then that memory is attributed to the
// allocating library, which all occurs on a single line of code, but has
// several different possible logical origins. If this is allowed, then there
// is a one to many relationship between file locations and tags.
//
// Another use case is one where there are several different calls to memory
// but they all use the same tag. This is the common case, where all of the
// calls are using the module name as the tag, and this is done in several
// places in the code. From this perspective, there is a one to many
// relationship between tags and file locations.
//
// The obvious case is that each file location will be invoked multiple times
// so there are many MemAllocHeaders for each file location.
//
// In order to achieve the fastest performance at allocation and free time,
// the system uses the model that there is only a single tag for each
// file location, and leaves it up to something else to track more specific
// usage. (This means that we do not presently support the first use case).
//
// At allocation time, we lookup the tag for the specified allocation.
// At free time, we have a direct pointer to the tag from the memheader,
// so we can update it that way.
//
// In the future, a more complicated data model would tie each allocation
// header to a specific memLocation and a specific memTag, which allows
// for more specific tracking, but comes at the cost of a second lookup
// at allocation time.
//
// For the present model, in order to aggregate tag information, the table
// of MemTrackTags must be crawled and the tags which have the same
// fileLocation would need to be summed to get a per-tag value

#include <unistd.h>
#include <stdio.h>
#include <new>
#include <execinfo.h>
#include <stdlib.h>
#include <regex.h>
#include <dlfcn.h>
#include <cxxabi.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "MemTrackInt.h"
#include "util/System.h"
#include "hash/Hash.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "LibUtilConstants.h"
#include "libapis/LibApisCommon.h"

using namespace util;

static uint64_t
hashKey(void *key)
{
    return (uint64_t) key;
}

typedef IntHashTable<void *,
                     MemAlignMemPtr,
                     &MemAlignMemPtr::hook,
                     &MemAlignMemPtr::getUsrPtr,
                     NumAlignPtrHashSlots,
                     hashKey>
    AlignPtrHashTable;

typedef IntHashTable<uint64_t,
                     MemTrackTag,
                     &MemTrackTag::hook,
                     &MemTrackTag::getUniquifier,
                     NumTagHashSlots,
                     hashCastUint64<uint64_t>>
    TagHashTable;

static TagHashTable *tagHashTable = NULL;
static Spinlock *tagHashTableLock = NULL;
static bool memInitCalled = false;
static const char *internalMemTag = "memTrackMeta";
static MemTrackTag internalTag;
static MemTrackTag *intTag = &internalTag;
static constexpr const char *moduleName = "MemTrack";
static AlignPtrHashTable *alignPtrHashTable;
static Spinlock *alignPtrHashTableLock = NULL;

// Main Functions
MustCheck Status
memTrackInit()
{
    Status status = StatusOk;

    assert(!memInitCalled);

    void *ptr;

    ptr = memAllocExt(sizeof(TagHashTable), moduleName);
    if (ptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    tagHashTable = new (ptr) TagHashTable();
    ptr = NULL;

    ptr = memAllocExt(sizeof(Spinlock), moduleName);
    if (ptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    tagHashTableLock = new (ptr) Spinlock();
    ptr = NULL;

    ptr = memAllocExt(sizeof(AlignPtrHashTable), moduleName);
    if (ptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    alignPtrHashTable = new (ptr) AlignPtrHashTable();
    ptr = NULL;

    ptr = memAllocExt(sizeof(Spinlock), moduleName);
    if (ptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    alignPtrHashTableLock = new (ptr) Spinlock();
    ptr = NULL;

    // The hash table starts out with the element for memTrack's internal
    // memory tracking. Note that the tag does not have a header, since no
    // MeTrackTags or MemAllocHeaders have headers, but everything else does
    // internalTag is statically allocated to avoid a few edge cases of
    // self tracking memory at destroy time. The memory footprint is currently
    // the hashtable and this single tag, as well as the headers for both of
    // them.
    atomicWrite64(&intTag->memSize, sizeof(MemTrackTag));
    intTag->tag = internalMemTag;
    intTag->fileLocation = FileLocationString;

#ifdef MEMTRACKTRACE
    intTag->nFrames = getBacktrace(intTag->addressList, MemTrackBtFrames);
    if (intTag->nFrames > MemTrackBtFrames) {
        intTag->nFrames = MemTrackBtFrames;
    }
#endif  // MEMTRACKTRACE

    tagHashTable->insert(intTag);

    memInitCalled = true;

CommonExit:
    if (status != StatusOk) {
        if (tagHashTable != NULL) {
            tagHashTable->~TagHashTable();
            memFree(tagHashTable);
            tagHashTable = NULL;
        }

        if (tagHashTableLock != NULL) {
            tagHashTableLock->~Spinlock();
            memFree(tagHashTableLock);
            tagHashTableLock = NULL;
        }

        if (alignPtrHashTable != NULL) {
            alignPtrHashTable->~AlignPtrHashTable();
            memFree(alignPtrHashTable);
            alignPtrHashTable = NULL;
        }

        if (alignPtrHashTableLock != NULL) {
            alignPtrHashTableLock->~Spinlock();
            memFree(alignPtrHashTableLock);
            alignPtrHashTableLock = NULL;
        }
    }

    return status;
}

#ifdef MEMORY_TRACKING
static MustCheck MemAllocHeader *
internalMalloc(size_t size)
{
    size_t allocSize;
    MemAllocHeader *mhead;

    // We allocate enough room for the requested data but also include
    // space for the header info
    allocSize = size + sizeof(MemAllocHeader);

    // @SymbolCheckIgnore
    mhead = (MemAllocHeader *) malloc(allocSize);
    if (mhead == NULL) {
        return NULL;
    }
    mhead->size = size;

    // Atomically update this memory category's size
    // This is terrible for the cache, because this will evict from all
    // other L1s and L2s
    // Also note that we can still keep intTag up to date even if memTrackInit
    // hasn't been called, since intTag is statically allocated and initialized
    atomicAdd64(&intTag->memSize, sizeof(MemAllocHeader));

    return mhead;
}

// This function takes an additional parameter of size so that there doesn't
// need to be any overhead in tracking the initial allocation. This reduces
// the memory footprint of memory tracking and is quite low cost, since we
// aren't doing anything unusual with our memory here.
static void
internalFree(MemAllocHeader *mhead)
{
    // This must be valid, since this tag is inserted initially
    assert(intTag != NULL);

    // Update internal tracking since this header isn't being used now
    atomicSub64(&intTag->memSize, sizeof(MemAllocHeader));

    // Actually free the memory
    // @SymbolCheckIgnore
    free(mhead);
}

static MustCheck MemTrackTag *
getOrCreateTag(const char *tag, const char *fileLocation)
{
    MemTrackTag *mtag;
    MemTrackTag *lookupTag;
#ifdef MEMTRACKTRACE
    void *addressList[MemTrackBtFrames];
    int nFrames = getBacktrace(addressList, MemTrackBtFrames);
    if (nFrames > MemTrackBtFrames) {
        nFrames = MemTrackBtFrames;
    }
    uint64_t traceHash = MemTrackTag::hashBackTrace(addressList, nFrames);
#else
    uint64_t traceHash =
        hashBufFast((const uint8_t *) fileLocation, strlen(fileLocation));
#endif  // MEMTRACKTRACE

    tagHashTableLock->lock();
    mtag = tagHashTable->find(traceHash);
    tagHashTableLock->unlock();

    if (mtag == NULL) {
        // If the tag lookup failed, then we need to make a new one
        // @SymbolCheckIgnore
        mtag = (MemTrackTag *) malloc(sizeof(MemTrackTag));
        if (mtag == NULL) {
            return NULL;
        }

        // Set the internal MemTrackTag fields for this
        atomicWrite64(&mtag->memSize, 0);
        mtag->tag = tag;
        mtag->fileLocation = fileLocation;
#ifdef MEMTRACKTRACE
        mtag->nFrames = nFrames;
        for (int ii = 0; ii < nFrames; ii++) {
            mtag->addressList[ii] = addressList[ii];
        }
#endif  // MEMTRACKTRACE
        mtag->traceHash = traceHash;

        // Insert this tag into the table;
        // It is possible that another memAlloc with this addressListBuffer is
        // racing to be the first to make this mtag, so we do a
        // hashLookupOrInsert and respond accordingly, possibly deallocating
        // the tag, if we ended up losing the race

        tagHashTableLock->lock();
        lookupTag = tagHashTable->find(mtag->traceHash);

        if (lookupTag == NULL) {
            tagHashTable->insert(mtag);
        }
        tagHashTableLock->unlock();

        if (lookupTag != NULL) {
            // @SymbolCheckIgnore
            free(mtag);
            mtag = lookupTag;
        } else {
            // Add this new tag to the internal overhead, because we know
            // this tag must be new
            assert(intTag != NULL);
            atomicAdd64(&intTag->memSize, sizeof(MemTrackTag));
        }
    }
    return mtag;
}

void
MemTrackTag::freeTag()
{
    assert(memInitCalled);

    // Here we need to make sure we don't free the internal tag
    if (this == intTag) {
        return;
    }

    atomicSub64(&intTag->memSize, sizeof(MemTrackTag));

    // @SymbolCheckIgnore
    free(this);
}

void
MemAlignMemPtr::freeMemPtr()
{
    assert(memInitCalled);

    assert(intTag != NULL);
    atomicSub64(&intTag->memSize, sizeof(MemAlignMemPtr));

    // @SymbolCheckIgnore
    free(this);
}

extern void *
_memAlloc(size_t size, const char *tag, const char *fileLocation)
{
    MemAllocHeader *mhead = NULL;
    void *result = NULL;
    MemTrackTag *mtag;
    bool tagAdded = false;
    bool success = false;

    if (memInitCalled) {
        mhead = internalMalloc(size);

        // If malloc fails, we return NULL, as per malloc standards
        if (mhead == NULL) {
            goto CommonExit;
        }

        // We have successfully gotten the user's data, but now we need
        // to track in in the memory tag. If this is the first call
        // with this tag, it will need to be created as well
        mtag = getOrCreateTag(tag, fileLocation);
        if (mtag == NULL) {
            goto CommonExit;
        }

        atomicAdd64(&mtag->memSize, size);
        mhead->tag = mtag;
        tagAdded = true;
        // We aren't tracking. This will primarily occurs because the C/C++
        // runtime executes code before passing to main(). This code can call
        // new() before we have ever had a chance to memTrackInit
    } else {
        mhead = internalMalloc(size);
        if (mhead == NULL) {
            goto CommonExit;
        }
        mhead->tag = NULL;
    }

    success = true;

CommonExit:
    if (!success) {
        if (mhead != NULL) {
            internalFree(mhead);
        }

        if (tagAdded) {
            atomicSub64(&mtag->memSize, size);
        }

        result = NULL;
    } else {
        result = &mhead->usrPtr;
    }

    return result;
}

Status
trackAlloc(void *result,
           size_t bytes,
           const char *tag,
           const char *fileLocation)
{
    Status status = StatusUnknown;
    MemAlignMemPtr *memptr = NULL;
    MemTrackTag *mtag = NULL;
    bool memPtrAlloced = false;
    bool tagAdded = false;

    // We now need to allocate something to keep track of the used memory
    // @SymbolCheckIgnore
    memptr = (MemAlignMemPtr *) malloc(sizeof(MemAlignMemPtr));

    // If malloc fails, we return NULL, as per malloc standards
    if (memptr == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(intTag != NULL);
    // Update itnernal tracking
    atomicAdd64(&intTag->memSize, sizeof(MemAlignMemPtr));
    memPtrAlloced = true;

    // At this point we know that the allocation succeeded
    memptr->usrPtr = result;
    memptr->size = bytes;

    // We have successfully gotten the user's data, but now we need
    // to track in in the memory tag. If this is the first call
    // with this tag, it will need to be created as well
    mtag = getOrCreateTag(tag, fileLocation);
    if (mtag == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    memptr->tag = mtag;
    atomicAdd64(&mtag->memSize, bytes);
    tagAdded = true;

    alignPtrHashTableLock->lock();
    alignPtrHashTable->insert(memptr);
    alignPtrHashTableLock->unlock();

    alignPtrHashTableLock->lock();
    assert(memptr == alignPtrHashTable->find(result));
    alignPtrHashTableLock->unlock();

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (memPtrAlloced) {
            atomicSub64(&intTag->memSize, bytes);
            free(memptr);
        }

        if (tagAdded) {
            atomicSub64(&mtag->memSize, bytes);
        }
    }

    return status;
}

// Same logic as _memAllocAligned
extern void *
_memMap(void *addr,
        size_t length,
        int prot,
        int flags,
        int fd,
        off_t offset,
        const char *fileLocation)
{
    Status status;
    void *result = NULL;
    bool success = false;

    // @SymbolCheckIgnore
    result = mmap(addr, length, prot, flags, fd, offset);
    if (result == MAP_FAILED) {
        goto CommonExit;
    }

    status = trackAlloc(result, length, NULL, fileLocation);
    if (status != StatusOk) {
        goto CommonExit;
    }

    success = true;
CommonExit:
    if (!success) {
        if (result != MAP_FAILED) {
            // @SymbolCheckIgnore
            munmap(result, length);
        }
        result = MAP_FAILED;
    }

    return result;
}

extern MustCheck NonNull(4) void *_memAllocAligned(size_t alignment,
                                                   size_t bytes,
                                                   const char *tag,
                                                   const char *fileLocation)
{
    void *result = NULL;
    int ret;
    bool success = false;
    Status status;
    bool dataAlloced = false;

    assert(memInitCalled);
    assert(mathIsPowerOfTwo(alignment));

    // @SymbolCheckIgnore
    ret = posix_memalign(&result, alignment, bytes);
    assert(ret != EINVAL);
    if (ret) {
        goto CommonExit;
    }
    dataAlloced = true;

    assert(mathIsAligned(result, (uint32_t) alignment));

    status = trackAlloc(result, bytes, tag, fileLocation);
    if (status != StatusOk) {
        goto CommonExit;
    }

    success = true;
CommonExit:
    if (!success) {
        if (dataAlloced) {
            free(result);
        }
        result = NULL;
    }

    return result;
}

extern MustCheck NonNull(4) void *_memRealloc(void *p,
                                              size_t size,
                                              const char *tag,
                                              const char *fileLocation)
{
    void *result = NULL;
    MemAllocHeader *oldHead;
    MemAllocHeader *newHead;
    MemTrackTag *oldTag;
    MemTrackTag *newTag;
    size_t newSize;
    size_t oldSize;

    assert(memInitCalled);

    // Take care of the special cases for realloc
    if (p == NULL) {
        return _memAlloc(size, tag, fileLocation);
    }

    if (size == 0) {
        memFree(p);
        return NULL;
    }

    // Get a pointer to the expected MemAllocHeader
    oldHead = ContainerOf((const char(*)[0]) p, MemAllocHeader, usrPtr);
    oldTag = oldHead->tag;
    oldSize = oldHead->size;

    newSize = size + sizeof(MemAllocHeader);

    // @SymbolCheckIgnore
    newHead = (MemAllocHeader *) realloc(oldHead, newSize);
    if (newHead == NULL) {
        return NULL;
    }

    // Update the old mtag
    // We make sure to do this after the realloc call, because if realloc
    // returns NULL, then the block is still allocated, and thus the memSize
    // shouldn't change
    atomicSub64(&oldTag->memSize, oldSize);

    // At this point we know that the allocation succeeded
    newTag = getOrCreateTag(tag, fileLocation);
    // If the user attempts to realloc something which was memAllocAligned
    // then this assertion will fail, because the keyStr is invalid.
    // It might also segfault in the process of looking up
    assert(newTag != NULL);

    atomicAdd64(&newTag->memSize, size);

    newHead->size = size;
    newHead->tag = newTag;

    result = newHead->usrPtr;

    return result;
}

// Calloc is just a malloc with a memset
extern void *
_memCalloc(size_t nmemb, size_t size, const char *tag, const char *fileLocation)
{
    void *result;
    size_t total_size;

    assert(memInitCalled);

    // calloc symantics require multiplications of nmemb and size to get
    // the total number of bytes
    total_size = nmemb * size;
    result = _memAlloc(total_size, tag, fileLocation);

    if (result != NULL) {
        memZero(result, total_size);
    }
    return result;
}

void
pseudoFree(void *p)
{
    MemAllocHeader *mhead;
    MemTrackTag *mtag;

    mhead = ContainerOf((const char(*)[0]) p, MemAllocHeader, usrPtr);
    if (mhead->tag) {
        mtag = mhead->tag;
        assert(mtag != NULL);
        assert((size_t) mtag->memSize.val >= mhead->size);
        atomicSub64(&mtag->memSize, mhead->size);
    }

    atomicSub64(&intTag->memSize, sizeof(MemAllocHeader));
}

void
memFree(void *p)
{
    // Freeing NULL must be ignored, as per standard. This is done in 3rd
    // party code.
    if (p == NULL) {
        return;
    }
    // Get a pointer to the expected MemAllocHeader
    MemAllocHeader *mhead =
        ContainerOf((const char(*)[0]) p, MemAllocHeader, usrPtr);
    MemTrackTag *mtag = mhead->tag;

    // If this was a tracked allocation
    if (mtag != NULL) {
        if (mtag->tag != NULL) {
            assert(atomicRead64(&mtag->memSize) >= (int64_t) mhead->size);
        }

        // Update this tag's memsize
        atomicSub64(&mtag->memSize, mhead->size);
    }

    // Update internal tracking
    // Note that this works even if the allocation itself isn't tracked,
    // because intTag is statically allocated
    atomicSub64(&intTag->memSize, sizeof(MemAllocHeader));

    // Metadata has all been updated, so we can free the data
    // Note that we are freeing the header, which includes the user data
    free(mhead);
}

void
untrackAlloc(void *p, size_t *bytesFreed)
{
    MemAlignMemPtr *mptr = NULL;
    MemTrackTag *mtag = NULL;

    alignPtrHashTableLock->lock();
    mptr = alignPtrHashTable->remove(p);
    alignPtrHashTableLock->unlock();

    if (memInitCalled == true) {
        // If this is null, that means that the user has probably called
        // memAlignedFree on something which was allocated using one of the
        // unaligned allocators, and should call memFree instead
        assert(mptr != NULL);
    }

    if (mptr != NULL) {
        assert(mptr->usrPtr == p);

        mtag = mptr->tag;

        // This should never be null, because this tag should have been
        // inserted when the memory was allocated. If this assertion fails
        // it means that the user is attempting to free something which was
        // not allocated or is trying to use memFree on something which was
        // memAllocAligned, in which case it should be memAlignedFree'ed
        assert(mtag != NULL);

        assert(*bytesFreed <= mptr->size);
        if (*bytesFreed == 0) {
            atomicSub64(&mtag->memSize, mptr->size);
            *bytesFreed = mptr->size;
        } else if (*bytesFreed <= mptr->size) {
            atomicSub64(&mtag->memSize, *bytesFreed);
        }

        // Now we need to deal with the overhead
        mptr->freeMemPtr();
    }
}

void
memAlignedFree(void *p)
{
    size_t bytes = 0;

    untrackAlloc(p, &bytes);
    free(p);
}

int
memUnmap(void *p, size_t length)
{
    size_t bytes = length;

    untrackAlloc(p, &bytes);
    // @SymbolCheckIgnore
    return munmap(p, length);
}
#endif  // MEMORY_TRACKING

void
printUsedTags(TagHashTable *hashTable)
{
    MemTrackTag *mtag;
    bool headerPrinted = false;

    // Check for any tags that have a non-zero memory usage
    for (TagHashTable::iterator it = hashTable->begin();
         (mtag = it.get()) != NULL;
         it.next()) {
        if (mtag == intTag) {
            continue;
        }

        if (mtag->memSize.val > 0) {
            // Only print this if there is actual unfreed memory
            if (!headerPrinted) {
                xSyslog(moduleName,
                        XlogErr,
                        "Memory that was obtained but not freed:");
                headerPrinted = true;
            }
#ifdef MEMTRACKTRACE
            char btBuffer[MemTrackBtBuffer];
            printBackTrace(btBuffer,
                           sizeof(btBuffer),
                           mtag->nFrames,
                           mtag->addressList);
            xSyslog(moduleName,
                    XlogErr,
                    "%s: %zu bytes:\n%s",
                    mtag->fileLocation,
                    mtag->memSize.val,
                    btBuffer);
#else
            xSyslog(moduleName,
                    XlogErr,
                    "%s: %zu bytes",
                    mtag->fileLocation,
                    mtag->memSize.val);
#endif  // MEMTRACKTRACE
        }
    }

    if (headerPrinted) {
        xSyslog(moduleName, XlogErr, "End unfreed memory");
    }
}

void
memTrackDestroy(bool reportLeaks)
{
    assert(memInitCalled);

    if (reportLeaks) {
        printUsedTags(tagHashTable);
    }

#ifdef UNDEF
    // XXX For certain global datastructures, destructors get called after
    // function main exits. So we cannot empty this table.
    tagHashTableLock->lock();
#ifdef MEMORY_TRACKING  // This will be empty if memory tracking is off
    tagHashTable->removeAll(&MemTrackTag::freeTag);
#endif  // MEMORY_TRACKING
    tagHashTableLock->unlock();
#endif  // UNDEF

    tagHashTableLock->~Spinlock();
    memFree(tagHashTableLock);
    tagHashTableLock = NULL;

    tagHashTable->~TagHashTable();
    memFree(tagHashTable);
    tagHashTable = NULL;

#ifdef UNDEF
    // XXX For certain global datastructures, destructors get called after
    // function main exits. So we cannot empty this table.
    alignPtrHashTableLock->lock();
#ifdef MEMORY_TRACKING  // This will be empty if memory tracking is off
    alignPtrHashTable->removeAll(&MemAlignMemPtr::freeMemPtr);
#endif  // MEMORY_TRACKING
    alignPtrHashTableLock->unlock();
#endif  // UNDEF

    alignPtrHashTableLock->~Spinlock();
    memFree(alignPtrHashTableLock);
    alignPtrHashTableLock = NULL;

    alignPtrHashTable->~AlignPtrHashTable();
    memFree(alignPtrHashTable);
    alignPtrHashTable = NULL;

    memInitCalled = false;
}

void *
memAllocJson(size_t size)
{
    return _memAlloc(size, "jansson", FileLocationString);
}

void
memFreeJson(void *p)
{
    memFree(p);
}

#ifdef MEMORY_TRACKING

//
// Overload new and delete
//

// memAlloc doesn't throw, but this function's semantics gaurantee that it will
// either succeed or throw an exception. For this reason, we must have it throw
// the exception. Note that this is called by uncontrolled c++ runtime code,
// before main() is called.
void *
operator new(std::size_t count) throw(std::bad_alloc)
{
    void *p = memAlloc((size_t) count);
    if (p == NULL) {
        std::bad_alloc exception;
        throw exception;
    }
    return p;
}

// XXX - This shouldn't exist. The C++ specification says that overloading
// the throwing version is all that is necessary to use a custom new function.
// This does not seem to work, however. To test this, simply remove the below
// override of 'nothrow new' and run some code. The program will encounter
// errors on delete, because delete has been successfully overridden, but
// 'nothrow' new has not. For more, please see:
// http://en.cppreference.com/w/cpp/memory/new/operator_new
void *
operator new[](std::size_t count) throw(std::bad_alloc)
{
    void *p = memAlloc((size_t) count);
    if (p == NULL) {
        std::bad_alloc exception;
        throw exception;
    }
    return p;
}

void *
operator new(std::size_t count, const std::nothrow_t &tag) noexcept
{
    return memAlloc((size_t) count);
}
void *
operator new[](std::size_t count, const std::nothrow_t &tag) noexcept
{
    return memAlloc((size_t) count);
}

// This doesn't actually throw anything, but it should match the
// definition of delete
void
operator delete(void *p) throw()
{
    memFree(p);
}

void
operator delete(void *p, const std::nothrow_t &tag) throw()
{
    memFree(p);
}

void
operator delete[](void *p) throw()
{
    memFree(p);
}

void
operator delete[](void *p, const std::nothrow_t &tag) throw()
{
    memFree(p);
}

#endif  // MEMORY_TRACKING
