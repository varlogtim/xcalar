// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// ----- Overview -----
// This class allows for efficient access of DataPages, which require
// sequential scans by themselves.
//
// DataPageIndex := [RecordIndex]
// RecordIndex := (Record *) (RecordNum)
//
// DataPageIndex is ordered by recordNum and may be binary searched in order to
// find the corresponding Record pointer.
//
// The number of RecordIndexes per page must be at least 1 so that the page
// doesn't get lost. The exact number can be dynamically determined, but is
// constant for the time being.
//
// In order to decode the record, it is usually necessary to use the top of the
// DataPage. This can be gotten using the fact that the DataPage is aligned.
//
// When walking from a RecordIndex to the desired record, the desired record is
// guaranteed to be on the same page. (It is possible to break this assumption
// by exploiting knowledge of the page size).
//
// At least 1 RecordIndex is guaranteed to be on each page.
//
// In order to find all the pages in a DataPageIndex, all RecordIndexes must be
// traversed; again, this exploits the alignment of the data pages.

#ifndef _DATAPAGEINDEX_H_
#define _DATAPAGEINDEX_H_

#include "primitives/Primitives.h"
#include "DataPage.h"
#include "runtime/Spinlock.h"

class ReaderRecord;

class DataPageIndex final
{
  public:
    typedef void (*FreeFunc)(void *);

    struct IndexEntry {
        int64_t recordNum;
        uint8_t *recPointer;
    };

    struct RangeReservation {
        int64_t startRecordNum = -1;
        int32_t numRecords = -1;
        int32_t numIndexEntries = -1;
        IndexEntry *pageEntries = NULL;
    };

    DataPageIndex() = default;
    ~DataPageIndex();

    // The freeFunc provided is used to clean up all provided pages
    MustCheck Status init(int32_t pageSize, FreeFunc freeFunc);

    // Reserve a range of record numbers which will later be
    // populated with data via addPage
    MustCheck Status reserveRecordRange(uint8_t *dataPage,
                                        RangeReservation *res);

    // Build the index page by page
    // This steals the reference to dataPage
    // This function is threadsafe
    // dataPage MUST be aligned according to pageSize
    void addPage(uint8_t *dataPage, const RangeReservation *reservation);

    // Create an array on top of the entry list to enable fast record retrieval
    MustCheck Status finalize();

    int32_t getNumRecords() const;
    int32_t getNumPages() const;
    int32_t getPageSize() const;

    // Retrieves the desired record
    void getRecordByNum(int64_t recordIdx, ReaderRecord *record) const;

    // Number of bytes used by this index
    // sizeof(IndexEntry) * maxEntriesPerPage_ / pageSize
    int64_t getIndexOverhead() const;

    // Number of bytes used by all the pages in this index
    // numPages * pageSize
    int64_t getPageMemUsed() const;

  private:
    // Size of DIMM row
    static constexpr const int32_t MaxMemScanPerRec = 512;
    static constexpr const int32_t InitialNumIndexEntries = 127;

    struct EntryList {
        EntryList *next;
        int32_t numEntries;
        IndexEntry entries[0];
    };

    struct EntryListElem {
        int64_t startRec;
        IndexEntry *entries;
    };

    DataPageIndex(const DataPageIndex &) = delete;
    DataPageIndex &operator=(const DataPageIndex &) = delete;

    IndexEntry *findEntry(int64_t recordIdx) const;
    IndexEntry *findEntryInEntryList(int64_t recordIdx,
                                     int32_t numEntries,
                                     IndexEntry *entryList) const;

    int32_t pageSize_ = -1;
    FreeFunc freeFunc_ = NULL;

    Mutex lock_;

    // linked list of entries in ascending recordNum order
    EntryList *head_ = NULL;
    EntryList *tail_ = NULL;

    // upper level index on top of the linked list of EntryLists
    // Enables binary search and hash based lookup
    EntryListElem *entryLists_ = NULL;

    int64_t numRecords_ = 0;
    int64_t maxEntriesPerPage_ = -1;
    int64_t numPages_ = 0;

  public:
    class RecordIterator final
    {
      public:
        RecordIterator(const DataPageIndex *index);
        ~RecordIterator() = default;

        void seek(int64_t recordIdx);

        // Returns false when another record was not found
        bool getNext(ReaderRecord *record);

      private:
        RecordIterator &operator=(const RecordIterator &) = delete;

        const DataPageIndex *index_ = NULL;
        int32_t curRecord_ = -1;
    };

    class PageIterator final
    {
      public:
        PageIterator(const DataPageIndex *index)
        {
            index_ = index;
            curPage_ = index->head_;
            curPageIdx_ = 0;
        }

        ~PageIterator() = default;

        void seek(int64_t pageIdx);

        // Returns false when another page was not found
        bool getNext(uint8_t **pageOut);

      private:
        PageIterator &operator=(const PageIterator &) = delete;

        const DataPageIndex *index_ = NULL;
        EntryList *curPage_ = NULL;
        int64_t curPageIdx_ = -1;
    };
};

#endif  // _DATAPAGEINDEX_H_
