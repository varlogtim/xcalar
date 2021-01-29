// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
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
#include "newtupbuf/NewTuplesCursor.h"
#include "datapage/DataPageIndex.h"

Status
TableCursor::getNext(NewKeyValueEntry *kvEntry)
{
    Status status = StatusUnknown;

    if (unlikely(this->eofWasReached || this->xdbPgTupBuf == NULL)) {
        return StatusNoData;
    }

    while (true) {
        status = this->xdbPgTupCursor.getNext(this->kvMeta_->tupMeta_,
                                              &kvEntry->tuple_);
        if (unlikely(status == StatusNoData)) {
            this->startRecordNum += this->xdbPgTupBuf->getNumTuples();
            status = this->xdbPgCursor.getNextTupBuf(&this->xdbPgTupBuf);
            if (likely(status == StatusOk)) {
                new (&this->xdbPgTupCursor) NewTuplesCursor(this->xdbPgTupBuf);
                continue;
            }

            new (&this->xdbPgTupCursor) NewTuplesCursor(this->xdbPgTupBuf);
            this->eofWasReached = true;
        }
        break;
    }

    return status;
}

Status
TableCursor::invalidate(bool gotoNext)
{
    Status status;

    if (unlikely(this->eofWasReached || this->xdbPgTupBuf == NULL)) {
        return StatusNoData;
    }

    while (true) {
        status =
            this->xdbPgTupCursor.invalidate(this->kvMeta_->tupMeta_, gotoNext);
        if (unlikely(status == StatusNoData)) {
            this->startRecordNum += this->xdbPgTupBuf->getNumTuples();
            status = this->xdbPgCursor.getNextTupBuf(&this->xdbPgTupBuf);
            if (likely(status == StatusOk)) {
                new (&this->xdbPgTupCursor) NewTuplesCursor(this->xdbPgTupBuf);
                continue;
            }

            new (&this->xdbPgTupCursor) NewTuplesCursor(this->xdbPgTupBuf);
            this->eofWasReached = true;
        }
        break;
    }

    return status;
}

Status
TableCursor::seek(int64_t offset, Cursor::SeekOption whence)
{
    Status status = StatusOk;
    uint64_t newOffset;

    NewTuplesCursor::Position pos = this->xdbPgTupCursor.getPosition();

    if (whence == Cursor::SeekRelative) {
        newOffset = this->startRecordNum + pos.nextTupleIdx + offset;
    } else {
        assert(whence == Cursor::SeekToAbsRow);
        newOffset = offset;
    }

    status = this->xdbPgCursor.seekToRow(&this->xdbPgTupBuf,
                                         &this->startRecordNum,
                                         newOffset);
    if (status != StatusOk) {
        this->eofWasReached = true;
        this->xdbPgTupCursor.reset();
    } else {
        new (&this->xdbPgTupCursor) NewTuplesCursor(this->xdbPgTupBuf);

        this->eofWasReached = false;
        assert(newOffset >= this->startRecordNum);
        status = this->xdbPgTupCursor.seek(this->kvMeta_->tupMeta_,
                                           newOffset - this->startRecordNum,
                                           NewTuplesCursor::SeekOpt::Begin);
        if (status != StatusOk) {
            this->eofWasReached = true;
            this->xdbPgTupCursor.reset();
        }
    }

    if (status == StatusNoData) {
        assert(this->eofWasReached == true);
        status = StatusOk;
    }

    return status;
}

Status
TableCursor::seekPosition(int64_t offset,
                          Cursor::SeekOption whence,
                          NewTuplesCursor::Position position)
{
    Status status = StatusOk;
    uint64_t newOffset;

    if (whence == Cursor::SeekRelative) {
        NewTuplesCursor::Position pos = this->xdbPgTupCursor.getPosition();
        newOffset = this->startRecordNum + pos.nextTupleIdx + offset;
    } else {
        assert(whence == Cursor::SeekToAbsRow);
        newOffset = offset;
    }

    status = this->xdbPgCursor.seekToRow(&this->xdbPgTupBuf,
                                         &this->startRecordNum,
                                         newOffset);
    if (status != StatusOk) {
        this->eofWasReached = true;
        this->xdbPgTupCursor.reset();
    } else {
        new (&this->xdbPgTupCursor) NewTuplesCursor(this->xdbPgTupBuf);
        this->xdbPgTupCursor.setPosition(position);
        this->eofWasReached = false;
    }

    if (status == StatusNoData) {
        assert(this->eofWasReached == true);
        status = StatusOk;
    }

    return status;
}

Status
DatasetCursor::seek(int64_t offset, Cursor::SeekOption whence)
{
    Status status = StatusOk;

    if (whence == Cursor::SeekRelative) {
        this->recordNum += offset;
    } else {
        assert(whence == Cursor::SeekToAbsRow);
        this->recordNum = offset;
    }

    return status;
}

void
TableCursor::savePosition(TableCursor::Position *pos)
{
    pos->xdbId = this->xdbId;
    pos->slotIndex = this->xdbPgCursor.slotId_;
    pos->xdbPage = this->xdbPgCursor.xdbPage_;
    pos->xdbPgTupBuf = this->xdbPgTupBuf;
    pos->xdbPgTupCursor = this->xdbPgTupCursor;
    pos->startRecordNum = this->startRecordNum;
    pos->eofWasReached = this->eofWasReached;
}

Status
TableCursor::restorePosition(TableCursor::Position *pos)
{
    Status status = StatusOk;

    assert(this->xdbId == pos->xdbId);

    this->xdbPgCursor.slotId_ = pos->slotIndex;
    if (this->xdbPgCursor.xdbPage_ != pos->xdbPage) {
        // If we're still on the same page we already have a ref to it,
        // otherwise its ref would have been decremented by getNext, so
        // resurrect the reference here
        status = pos->xdbPage->getRef(this->xdbPgCursor.xdb_);
        BailIfFailed(status);

        // tupBuf can change across pageout/pagein cycle, so reset the copies
        // here
        this->xdbPgTupBuf = pos->xdbPage->tupBuf;

        // Copy over the offsets from pos
        this->xdbPgTupCursor = pos->xdbPgTupCursor;
        // But reseat the tuple buffer itself in case a pageout/pagein cycle
        // occurred
        this->xdbPgTupCursor.deserialize(pos->xdbPage->tupBuf);
        // XXX: Should we also decref the old position???
    } else {
        this->xdbPgTupBuf = pos->xdbPgTupBuf;
        this->xdbPgTupCursor = pos->xdbPgTupCursor;
    }
    assert(atomicRead32(&pos->xdbPage->hdr.pageState) == XdbPage::Resident);
    assert(!pos->xdbPage->hdr.isCompressed);
    this->xdbPgCursor.xdbPage_ = pos->xdbPage;
    this->startRecordNum = pos->startRecordNum;
    this->eofWasReached = pos->eofWasReached;

CommonExit:
    return status;
}

// New cursor is untracked, this is for performance reasons as we
// duplicate cursors in the fast path
MustCheck Status
TableCursor::duplicate(TableCursor *curOut)
{
    Status status;
    bool needDecRef = false;

    if (this->xdbPgCursor.xdbPage_ != NULL) {
        // Ref count is stored in the xdbpage, not the cursor, so when
        // duplicating the cursor we need to inc the xdb refcount
        status = this->xdbPgCursor.xdbPage_->getRef(this->xdbPgCursor.xdb_);
        BailIfFailed(status);
        needDecRef = true;
        assert(atomicRead32(&this->xdbPgCursor.xdbPage_->hdr.pageState) ==
               XdbPage::Resident);
        assert(!this->xdbPgCursor.xdbPage_->hdr.isCompressed);
    }

    if (curOut->xdbPgCursor.xdbPage_ != NULL) {
        // curOut is about to be overwritten
        XdbMgr::get()->pagePutRef(curOut->xdbPgCursor.xdb_,
                                  0,
                                  curOut->xdbPgCursor.xdbPage_);
        curOut->xdbPgCursor.xdbPage_ = NULL;
    }

    *curOut = *this;  // This will steal the reference to the xdbPage_
    needDecRef = false;
    curOut->trackOption_ = Cursor::Untracked;
    curOut->needDestroy_ = false;

CommonExit:
    if (status != StatusOk) {
        if (needDecRef) {
            XdbMgr::get()->pagePutRef(this->xdbPgCursor.xdb_,
                                      0,
                                      this->xdbPgCursor.xdbPage_);
            needDecRef = false;
        }
    }

    assert(!needDecRef);
    return status;
}

Xid
TableCursor::getBackingId()
{
    return this->xdbId;
}

Xid
DatasetCursor::getBackingId()
{
    return this->datasetId;
}
