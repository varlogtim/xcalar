// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "cursor/Cursor.h"
#include "util/MemTrack.h"
#include "runtime/Semaphore.h"
#include "util/Atomics.h"
#include "xdb/Xdb.h"
#include "dataset/Dataset.h"

CursorManager *CursorManager::instance = NULL;

Status
CursorManager::init()
{
    Status status = StatusOk;

    instance = new (std::nothrow) CursorManager();
    BailIfNull(instance);

CommonExit:
    return status;
}

CursorManager *
CursorManager::get()
{
    assert(instance);
    return instance;
}

void
CursorManager::tearDown()
{
    CursorManager *instance = CursorManager::get();

    delete instance;
    instance = NULL;
}

Status
CursorManager::incUnsortedRef(XdbId xdbId)
{
    Status status = StatusOk;
    bool locked = false;
    CursorTracker *tracker;
    hashTableLock_.lock();
    locked = true;

    tracker = hashTable_.find(xdbId);
    if (!tracker) {
        // this is the first tracker for this table
        tracker = (CursorTracker *) memAllocExt(sizeof(*tracker), moduleName);
        BailIfNull(tracker);

        tracker = new (tracker) CursorTracker();

        tracker->xdbId = xdbId;
        verifyOk(status = hashTable_.insert(tracker));
    }
    hashTableLock_.unlock();
    locked = false;

    tracker->lock.lock();
    tracker->unsortedRefs++;
    tracker->lock.unlock();

CommonExit:
    if (locked) {
        hashTableLock_.unlock();
    }

    return status;
}

void
CursorManager::decUnsortedRef(XdbId xdbId)
{
    CursorTracker *tracker;
    hashTableLock_.lock();
    tracker = hashTable_.find(xdbId);
    assert(tracker);
    hashTableLock_.unlock();

    tracker->lock.lock();
    tracker->unsortedRefs--;
    tracker->lock.unlock();
}

Status
CursorManager::trackCursor(Cursor *cur, Cursor::TrackOption trackOption)
{
    CursorTracker *tracker;
    Status status = StatusOk;
    bool locked = false;
    Xid backingId = cur->getBackingId();
    hashTableLock_.lock();
    locked = true;

    tracker = hashTable_.find(backingId);
    if (!tracker) {
        // this is the first cursor for this table
        tracker = (CursorTracker *) memAllocExt(sizeof(*tracker), moduleName);
        BailIfNull(tracker);

        tracker = new (tracker) CursorTracker();

        tracker->xdbId = backingId;
        verifyOk(status = hashTable_.insert(tracker));
    }
    hashTableLock_.unlock();
    locked = false;

    tracker->lock.lock();
    switch (trackOption) {
    case Cursor::InsertIntoHashTable:
        cur->cursorListNext_ = tracker->listHead;
        cur->cursorListPrev_ = NULL;

        if (tracker->listHead != NULL) {
            tracker->listHead->cursorListPrev_ = cur;
        }

        tracker->listHead = cur;
        tracker->numCursors++;
        // fall through
    case Cursor::IncrementRefOnly:
        if (cur->ordering_ == Unordered) {
            tracker->unsortedRefs++;
        }
        tracker->totalRefs++;
        break;
    default:
        assert(0);
        status = StatusUnimpl;
        break;
    }
    tracker->lock.unlock();

CommonExit:
    if (locked) {
        hashTableLock_.unlock();
        locked = false;
    }

    return status;
}

void
CursorManager::untrackCursor(Cursor *cur)
{
    CursorTracker *tracker;
    Xid backingId = cur->getBackingId();
    hashTableLock_.lock();

    tracker = hashTable_.find(backingId);
    assert(tracker);

    hashTableLock_.unlock();

    tracker->lock.lock();
    switch (cur->trackOption_) {
    case Cursor::InsertIntoHashTable:
        if (tracker->listHead == cur) {
            tracker->listHead = cur->cursorListNext_;
        } else {
            cur->cursorListPrev_->cursorListNext_ = cur->cursorListNext_;
            if (cur->cursorListNext_ != NULL) {
                cur->cursorListNext_->cursorListPrev_ = cur->cursorListPrev_;
            }
        }

        cur->cursorListNext_ = NULL;
        cur->cursorListPrev_ = NULL;

        tracker->numCursors--;
        // fall through
    case Cursor::IncrementRefOnly:
        if (cur->ordering_ == Unordered) {
            tracker->unsortedRefs--;
        }
        tracker->totalRefs--;
        break;

    default:
        assert(0);
        break;
    }
    tracker->lock.unlock();
}

void
CursorManager::removeTableTracking(XdbId xdbId)
{
    CursorTracker *tracker;
    bool locked = false;
    hashTableLock_.lock();
    locked = true;

    tracker = hashTable_.remove(xdbId);
    if (!tracker) {
        goto CommonExit;
    }
    tracker->~CursorTracker();
    memFree(tracker);

CommonExit:
    if (locked) {
        hashTableLock_.unlock();
    }
}

void
CursorManager::backingTableInitializeHelper(TableCursor *cur,
                                            uint64_t startRecord,
                                            XdbId xdbId)
{
    XdbMeta *xdbMeta;
    Status status;
    XdbMgr *xdbMgr = XdbMgr::get();

    new (&cur->xdbPgTupCursor) NewTuplesCursor(cur->xdbPgTupBuf);
    cur->startRecordNum = startRecord;
    cur->eofWasReached = false;
    cur->needDestroy_ = true;
    status = xdbMgr->xdbGet(xdbId, NULL, &xdbMeta);
    assert(status == StatusOk);

    cur->kvMeta_ = (NewKeyValueMeta *) &xdbMeta->kvNamedMeta.kvMeta_;
}

Status
CursorManager::createOnTable(XdbId xdbId,
                             Ordering ordering,
                             Cursor::TrackOption trackOption,
                             TableCursor *cur)
{
    Status status = StatusOk;
    bool tracked = false;

    cur->needDestroy_ = false;
    cur->xdbId = xdbId;
    cur->ordering_ = ordering;
    cur->trackOption_ = trackOption;
    cur->backing_ = Cursor::BackingTable;

    if (trackOption != Cursor::Untracked) {
        status = trackCursor(cur, trackOption);
        BailIfFailed(status);
        tracked = true;
    }

    status = cur->xdbPgCursor.init(xdbId, ordering);
    BailIfFailed(status);

    cur->xdbPgTupBuf = cur->xdbPgCursor.xdbPage_->tupBuf;

    backingTableInitializeHelper(cur, 0, xdbId);
    cur->tupleMeta_ = cur->kvMeta_->tupMeta_;

CommonExit:
    if (status != StatusOk) {
        if (tracked) {
            untrackCursor(cur);
        }
    }

    return status;
}

Status
CursorManager::createOnTable(Xdb *xdb,
                             Ordering ordering,
                             Cursor::TrackOption trackOption,
                             TableCursor *cur)
{
    Status status = StatusOk;
    bool tracked = false;
    XdbId xdbId = XdbMgr::xdbGetXdbId(xdb);
    XdbMeta *xdbMeta = XdbMgr::xdbGetMeta(xdb);

    cur->needDestroy_ = false;
    cur->xdbId = xdbId;
    cur->ordering_ = ordering;
    cur->trackOption_ = trackOption;
    cur->backing_ = Cursor::BackingTable;

    if (trackOption != Cursor::Untracked) {
        status = trackCursor(cur, trackOption);
        BailIfFailed(status);
        tracked = true;
    }

    status = cur->xdbPgCursor.init(xdb, ordering);
    BailIfFailed(status);

    cur->xdbPgTupBuf = cur->xdbPgCursor.xdbPage_->tupBuf;

    new (&cur->xdbPgTupCursor) NewTuplesCursor(cur->xdbPgTupBuf);

    cur->startRecordNum = 0;
    cur->eofWasReached = false;
    cur->needDestroy_ = true;
    cur->kvMeta_ = (NewKeyValueMeta *) &xdbMeta->kvNamedMeta.kvMeta_;
    cur->tupleMeta_ = cur->kvMeta_->tupMeta_;

CommonExit:
    if (status != StatusOk) {
        if (tracked) {
            untrackCursor(cur);
        }
    }

    return status;
}

Status
CursorManager::createOnDataset(DsDatasetId datasetId,
                               Cursor::TrackOption trackOption,
                               bool errorDs,
                               DatasetCursor *cur)
{
    Status status = StatusOk;

    cur->recordNum = 0;
    cur->datasetId = datasetId;
    cur->ordering_ = Unordered;
    cur->tupleMeta_ = cur->kvMeta_->tupMeta_;
    cur->backing_ = Cursor::BackingDataset;
    cur->trackOption_ = trackOption;
    cur->needDestroy_ = true;
    cur->errorDs = errorDs;

    if (trackOption != Cursor::Untracked) {
        status = trackCursor(cur, trackOption);
    }

    return status;
}

// Note: this function only works on tables
Status
CursorManager::createOnSlot(XdbId xdbId,
                            uint64_t slotId,
                            uint64_t startRecord,
                            Ordering ordering,
                            TableCursor *cur)
{
    Status status = StatusUnknown;
    bool tracked = false;

    cur->needDestroy_ = false;
    cur->xdbId = xdbId;
    cur->backing_ = Cursor::BackingTable;
    cur->ordering_ = ordering;
    cur->trackOption_ = Cursor::InsertIntoHashTable;

    status = trackCursor(cur, Cursor::InsertIntoHashTable);
    BailIfFailed(status);
    tracked = true;

    status = cur->xdbPgCursor.init(xdbId, ordering, slotId);
    BailIfFailed(status);

    cur->xdbPgTupBuf = cur->xdbPgCursor.xdbPage_->tupBuf;

    backingTableInitializeHelper(cur, startRecord, xdbId);
    cur->tupleMeta_ = cur->kvMeta_->tupMeta_;

    // Don't bother setting up ValueDesc since it's only used by resultSets.

CommonExit:
    if (status != StatusOk) {
        if (tracked) {
            untrackCursor(cur);
        }
    }

    return status;
}

void
CursorManager::destroy(Cursor *cur)
{
    if (cur->trackOption_ != Cursor::Untracked) {
        untrackCursor(cur);
    }

    if (cur->backing_ == Cursor::BackingTable) {
        Status status = StatusOk;
        TableCursor *tcur = dynamic_cast<TableCursor *>(cur);

        if (tcur->xdbPgCursor.xdbPage_ != NULL) {
            XdbMgr::get()->pagePutRef(tcur->xdbPgCursor.xdb_,
                                      0,
                                      tcur->xdbPgCursor.xdbPage_);
            tcur->xdbPgCursor.xdbPage_ = NULL;
        }

        if (cur->needDestroy_) {
            status = tcur->xdbPgCursor.cleanupUnorderedPages();
            // The only bad status returned is XDB not found
            assert(status == StatusXdbSlotHasActiveCursor ||
                   (status == StatusOk));
        }
    }
    cur->kvMeta_ = NULL;
    cur->tupleMeta_ = NULL;
}

uint64_t
CursorManager::getUnsortedRefs(XdbId xdbId)
{
    CursorTracker *tracker;
    uint64_t refs = 0;

    hashTableLock_.lock();

    tracker = hashTable_.find(xdbId);
    if (!tracker) {
        refs = 0;
    } else {
        tracker->lock.lock();
        refs = tracker->unsortedRefs;
        tracker->lock.unlock();
    }

    hashTableLock_.unlock();

    return refs;
}

uint64_t
CursorManager::getTotalRefs(XdbId xdbId)
{
    CursorTracker *tracker;
    uint64_t refs = 0;

    hashTableLock_.lock();

    tracker = hashTable_.find(xdbId);
    if (!tracker) {
        refs = 0;
    } else {
        tracker->lock.lock();
        refs = tracker->totalRefs;
        tracker->lock.unlock();
    }

    hashTableLock_.unlock();

    return refs;
}

Status
CursorManager::getCursorRefs(XdbId xdbId, Xid **refsOut, unsigned *numRefsOut)
{
    Status status = StatusOk;
    CursorTracker *tracker = NULL;
    Xid *refs = NULL;
    unsigned numRefs = 0;

    bool trackerLocked = false;
    bool hashTableLocked = false;

    hashTableLock_.lock();
    hashTableLocked = true;

    tracker = hashTable_.find(xdbId);
    if (tracker) {
        tracker->lock.lock();
        trackerLocked = true;

        numRefs = tracker->numCursors;
        refs = (Xid *) memAlloc(numRefs * sizeof(*refs));
        BailIfNull(refs);

        Cursor *cursorList = tracker->listHead;
        unsigned ii = 0;
        while (cursorList) {
            refs[ii++] = cursorList->ownerId_;
            cursorList = cursorList->cursorListNext_;
        }

        assert(ii == numRefs);
    }

CommonExit:
    if (trackerLocked) {
        tracker->lock.unlock();
        trackerLocked = false;
    }

    if (hashTableLocked) {
        hashTableLock_.unlock();
        hashTableLocked = false;
    }

    if (status != StatusOk) {
        if (refs) {
            memFree(refs);
            refs = NULL;
        }

        numRefs = 0;
    }

    *refsOut = refs;
    *numRefsOut = numRefs;

    return status;
}
