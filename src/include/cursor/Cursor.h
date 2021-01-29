// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CURSOR_H
#define _CURSOR_H

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "runtime/Semaphore.h"
#include "runtime/Mutex.h"
#include "util/AtomicTypes.h"
#include "dataset/DatasetTypes.h"
#include "xdb/TableTypes.h"
#include "xdb/DataModelTypes.h"
#include "util/IntHashTable.h"
#include "util/IntHashTableHook.h"
#include "hash/Hash.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "newtupbuf/NewTupleTypes.h"
#include "newtupbuf/NewTuplesCursor.h"

class Cursor
{
  public:
    enum TrackOption {
        Untracked,
        InsertIntoHashTable,
        IncrementRefOnly,
    };

    enum Backing {
        BackingDataset,
        BackingTable,
    };

    enum SeekOption {
        SeekToAbsRow,
        SeekRelative,
    };

    const NewTupleMeta *tupleMeta_;
    const NewKeyValueMeta *kvMeta_;

    Ordering ordering_;
    Backing backing_;
    TrackOption trackOption_;
    bool needDestroy_;
    Cursor *cursorListPrev_;
    Cursor *cursorListNext_;

    // identifier for the structure using this cursor
    uint64_t ownerId_ = 0;

    // these don't do anything, please use CursorManager create and destroy
    Cursor() {}
    virtual ~Cursor() {}

    virtual MustCheck Status seek(int64_t offset, SeekOption whence) = 0;
    virtual MustCheck Xid getBackingId() = 0;
};

class TableCursor : public Cursor
{
  public:
    struct Position {
        XdbId xdbId;
        size_t slotIndex;
        XdbPage *xdbPage;
        NewTuplesBuffer *xdbPgTupBuf;
        NewTuplesCursor xdbPgTupCursor;
        uint64_t startRecordNum;
        bool eofWasReached;
    };

    // start of member variables
    XdbId xdbId;
    XdbPgCursor xdbPgCursor;
    NewTuplesBuffer *xdbPgTupBuf;
    NewTuplesCursor xdbPgTupCursor;
    uint64_t startRecordNum;
    bool eofWasReached;

    TableCursor() : xdbPgTupCursor((NewTuplesBuffer *) NULL) {}
    virtual ~TableCursor() {}

    virtual MustCheck Status seek(int64_t offset, SeekOption whence);
    virtual MustCheck Status seekPosition(int64_t offset,
                                          SeekOption whence,
                                          NewTuplesCursor::Position position);
    virtual MustCheck Xid getBackingId();

    MustCheck Status getNext(NewKeyValueEntry *kvEntryOut);

    void savePosition(Position *pos);

    MustCheck Status restorePosition(Position *pos);

    // New cursor is untracked
    MustCheck Status duplicate(TableCursor *curOut);

    MustCheck Status invalidate(bool gotoNext);
};

class DatasetCursor : public Cursor
{
  public:
    DsDatasetId datasetId;
    uint64_t recordNum;
    bool errorDs;

    DatasetCursor() {}
    virtual ~DatasetCursor() {}

    virtual MustCheck Status seek(int64_t offset, SeekOption whence);

    virtual MustCheck Xid getBackingId();
};

class CursorManager
{
  public:
    static Status init();
    static CursorManager *get();
    void tearDown();

    enum {
        NumCursorTrackerSlots = 127,
    };

    // Creates a cursor pointing to the first recor
    MustCheck Status createOnTable(XdbId xdbId,
                                   Ordering ordering,
                                   Cursor::TrackOption trackOption,
                                   TableCursor *cur);
    MustCheck Status createOnTable(Xdb *xdb,
                                   Ordering ordering,
                                   Cursor::TrackOption trackOption,
                                   TableCursor *cur);

    // Creates an unordered cursor pointint to the first record
    // Returns records in a stringified json format
    MustCheck Status createOnDataset(DsDatasetId datasetId,
                                     Cursor::TrackOption trackOption,
                                     bool errorDs,
                                     DatasetCursor *cur);

    // This is highly specific to the Operators/Xdb code path
    // Creates a refTrackedOnly cursor on a specific xdb slot. This
    // cursor cannot leave this slot
    MustCheck Status createOnSlot(XdbId xdbId,
                                  uint64_t slotId,
                                  uint64_t startRecord,
                                  Ordering ordering,
                                  TableCursor *cur);

    void destroy(Cursor *cur);
    void removeTableTracking(XdbId id);

    // Manage unsortedRefs only
    Status incUnsortedRef(XdbId xdbId);
    void decUnsortedRef(XdbId xdbId);

    uint64_t getUnsortedRefs(XdbId xdbId);
    uint64_t getTotalRefs(XdbId xdbId);
    Status getCursorRefs(XdbId xdbId, Xid **refsOut, unsigned *numRefsOut);

  private:
    static CursorManager *instance;
    static constexpr const char *moduleName = "Cursor";

    struct CursorTracker {
        IntHashTableHook hook;
        XdbId xdbId;

        XdbId getXdbId() const { return xdbId; };

        Mutex lock;
        uint64_t totalRefs = 0;
        uint64_t unsortedRefs = 0;
        uint64_t numCursors = 0;
        Cursor *listHead = NULL;
    };

    // member variables
    IntHashTable<XdbId,
                 CursorTracker,
                 &CursorTracker::hook,
                 &CursorTracker::getXdbId,
                 NumCursorTrackerSlots,
                 hashIdentity>
        hashTable_;
    Mutex hashTableLock_;

    Status trackCursor(Cursor *cur, Cursor::TrackOption trackOption);
    void untrackCursor(Cursor *cur);

    void backingTableInitializeHelper(TableCursor *cur,
                                      uint64_t startRecord,
                                      XdbId xdbId);

    // Keep this private, use init instead
    CursorManager() {}

    // Keep this private, use destroy instead
    ~CursorManager() {}

    CursorManager(const CursorManager &) = delete;
    CursorManager &operator=(const CursorManager &) = delete;
};

#endif  // _CURSOR_H
