// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "datapage/DataPageIndex.h"
#include "df/DataFormat.h"

static constexpr const char *moduleName = "dataPageIndex";

DataPageIndex::~DataPageIndex()
{
    int64_t foundPages = 0;
    EntryList *entryList = head_;
    EntryList *entryListTmp = entryList;
    // Go through all of the index entries and free any pages we find;
    // There will be at least 1 entry per page so will find all pages.
    // There may be duplicate entries per page, so we have to dedupe
    while (entryList != NULL) {
        entryListTmp = entryList->next;

        IndexEntry *entry = &entryList->entries[0];

        uint8_t *dataPage = reinterpret_cast<uint8_t *>(
            roundDown((uintptr_t) entry->recPointer, pageSize_));
        freeFunc_(dataPage);

        ++foundPages;

        memFree(entryList);
        entryList = entryListTmp;
    }

    if (entryLists_) {
        memFree(entryLists_);
    }
    assert(foundPages == numPages_);
}

Status
DataPageIndex::init(int32_t pageSize, FreeFunc freeFunc)
{
    pageSize_ = pageSize;
    freeFunc_ = freeFunc;
    maxEntriesPerPage_ = xcMax(pageSize / MaxMemScanPerRec, 1);
    return StatusOk;
}

Status
DataPageIndex::reserveRecordRange(uint8_t *dataPage, RangeReservation *res)
{
    Status status = StatusOk;
    DataPageReader reader;
    EntryList *entryList = NULL;

    assert(dataPage != NULL);

    reader.init(dataPage, pageSize_);

    // dataPage must be aligned according to the pageSize
    assert(mathIsAligned(dataPage, pageSize_));

    res->numRecords = reader.getNumRecords();
    assert(res->numRecords > 0);

    res->numIndexEntries = xcMin(maxEntriesPerPage_, (int64_t) res->numRecords);
    entryList = (EntryList *) memAlloc(
        sizeof(*entryList) + res->numIndexEntries * sizeof(IndexEntry));
    BailIfNull(entryList);

    entryList->numEntries = res->numIndexEntries;
    entryList->next = NULL;
    res->pageEntries = entryList->entries;

    //
    // WARNING
    // All of the below must succeed or the entryList will be corrupted
    // We have made a reservation within the index; if we cannot fulfill the
    // reservation, then the load must be aborted and the index thrown away.
    // WARNING
    //

    lock_.lock();
    // add ourselves to the linked list of EntryLists, update the shared record
    // counter according to the number of records in this EntryList
    res->startRecordNum = numRecords_;
    numRecords_ += res->numRecords;
    ++numPages_;

    if (unlikely(head_ == NULL)) {
        assert(tail_ == NULL);
        head_ = entryList;
        tail_ = entryList;
    } else {
        tail_->next = entryList;
        tail_ = entryList;
    }
    lock_.unlock();

CommonExit:
    return status;
}

void
DataPageIndex::addPage(uint8_t *dataPage, const RangeReservation *res)
{
    DataPageReader reader;
    reader.init(dataPage, pageSize_);

    assert(dataPage != NULL);
    assert(mathIsAligned(dataPage, pageSize_));
    assert(reader.getNumRecords() == res->numRecords);
    assert(res->numRecords > 0);

    // We can now build our index
    {
        DataPageReader::RecordIterator recIter(&reader);
        ReaderRecord record;
        IndexEntry *entry = &res->pageEntries[0];
        int64_t curRecordNum;
        verify(recIter.getNext(&record));

        // There will be `minRecsPerSpan + 1` indexEntries for the first
        // `numBonusSpans` indexEntries, and `minRecsPerSpan` for the remainder
        int minRecsPerSpan = res->numRecords / res->numIndexEntries;
        int numBonusSpans = res->numRecords % res->numIndexEntries;

        // This loop is unrolled slightly to avoid traversing the end of the
        // page unnecessarily
        curRecordNum = res->startRecordNum;
        entry->recordNum = curRecordNum;
        entry->recPointer = dataPage + record.getOffset();
        if (res->numIndexEntries > 1) {
            for (int ii = 1; ii < res->numIndexEntries; ii++) {
                entry = &res->pageEntries[ii];
                // Skip over all the records which are indexed
                // by the previous entry
                int skippedRecs =
                    minRecsPerSpan + ((ii - 1) < numBonusSpans ? 1 : 0);
                for (int jj = 0; jj < skippedRecs; jj++) {
                    verify(recIter.getNext(&record));
                }
                curRecordNum += skippedRecs;
                entry->recordNum = curRecordNum;
                entry->recPointer = dataPage + record.getOffset();
            }
        }
    }
}

Status
DataPageIndex::finalize()
{
    Status status = StatusOk;
    EntryList *entryList = head_;
    unsigned index = 0;

    entryLists_ =
        (EntryListElem *) memAllocExt(sizeof(*entryLists_) * numPages_,
                                      moduleName);
    BailIfNull(entryLists_);

    while (entryList != NULL) {
        entryLists_[index].startRec = entryList->entries[0].recordNum;
        entryLists_[index].entries = entryList->entries;

        index++;
        entryList = entryList->next;
    }

    assert(index == numPages_);

CommonExit:
    return status;
}

int32_t
DataPageIndex::getNumRecords() const
{
    return numRecords_;
}

int32_t
DataPageIndex::getNumPages() const
{
    return numPages_;
}

int32_t
DataPageIndex::getPageSize() const
{
    return pageSize_;
}

void
DataPageIndex::getRecordByNum(int64_t recordIdx, ReaderRecord *record) const
{
    const IndexEntry *entry;

    assert(entryLists_ != NULL);
    assert(recordIdx < numRecords_);

    entry = findEntry(recordIdx);
    if (!entry) {
        assert(false);
    }
    assert(entry->recordNum <= recordIdx);
    assert(entry->recPointer != NULL);

    // Now we need to walk to the desired record
    {
        // The dataPage for this record is guaranteed to be aligned, so we can
        // grab that by rounding down
        const uint8_t *dataPage = reinterpret_cast<const uint8_t *>(
            roundDown((uintptr_t) entry->recPointer, pageSize_));
        int32_t offset = (uintptr_t) entry->recPointer - (uintptr_t) dataPage;
        assert(dataPage != NULL);
        DataPageReader reader;
        reader.init(dataPage, pageSize_);

        int32_t numWalk = recordIdx - entry->recordNum;
        reader.getRecord(offset, numWalk, record);
    }
}

DataPageIndex::IndexEntry *
DataPageIndex::findEntry(int64_t recordIdx) const
{
    assert(recordIdx < numRecords_);
    assert(entryLists_ != NULL);

    unsigned index;
    EntryList *entryList;
    EntryListElem *entryListElem;

    // do a binary search across entryLists_
    // Short circuit the last page so we don't have to check for it below
    entryListElem = &entryLists_[numPages_ - 1];
    if (recordIdx >= entryListElem->startRec) {
        entryList = ContainerOf(entryListElem->entries, EntryList, entries);

        return findEntryInEntryList(recordIdx,
                                    entryList->numEntries,
                                    entryList->entries);
    }

    unsigned lowerBound = 0;
    unsigned upperBound = numPages_ - 1;

    index = (upperBound - lowerBound) / 2;
    while (true) {
        assert(index != numPages_ - 1);
        entryListElem = &entryLists_[index];
        if (lowerBound >= upperBound) {
            assert(false);
            // We didn't find it, quit
            return NULL;
        }
        if (recordIdx < entryListElem->startRec) {
            upperBound = index;
        } else {
            // We know that entryListElem isn't the last one so this is safe
            if (unlikely(recordIdx < (entryListElem + 1)->startRec)) {
                // We found it; we're done
                entryList =
                    ContainerOf(entryListElem->entries, EntryList, entries);
                return findEntryInEntryList(recordIdx,
                                            entryList->numEntries,
                                            entryList->entries);
            } else {
                lowerBound = index + 1;
            }
        }
        index = (upperBound + lowerBound) / 2;
    }
}

DataPageIndex::IndexEntry *
DataPageIndex::findEntryInEntryList(int64_t recordIdx,
                                    int32_t numEntries,
                                    IndexEntry *entries) const
{
    assert(recordIdx < numRecords_);

    // Short circuit the last page so we don't have to check for it below
    if (recordIdx >= entries[numEntries - 1].recordNum) {
        return &entries[numEntries - 1];
    }

    int32_t lowerBound = 0;
    int32_t upperBound = numEntries - 1;
    int32_t pos = (upperBound - lowerBound) / 2;
    while (true) {
        assert(pos != numEntries - 1);
        IndexEntry *entry = &entries[pos];
        if (lowerBound >= upperBound) {
            assert(false);
            // We didn't find it, quit
            return NULL;
        }
        if (recordIdx < entry->recordNum) {
            upperBound = pos;
        } else {
            // We know that entry isn't the last one so this is safe
            if (recordIdx < (entry + 1)->recordNum) {
                // We found it; we're done
                assert(entry->recPointer != NULL);
                return entry;
            } else {
                lowerBound = pos + 1;
            }
        }
        pos = (upperBound + lowerBound) / 2;
    }
}

int64_t
DataPageIndex::getIndexOverhead() const
{
    return sizeof(this) + sizeof(IndexEntry) * (numPages_ * maxEntriesPerPage_);
}

int64_t
DataPageIndex::getPageMemUsed() const
{
    return pageSize_ * numPages_;
}

DataPageIndex::RecordIterator::RecordIterator(const DataPageIndex *index)
{
    index_ = index;
    curRecord_ = 0;
}

void
DataPageIndex::RecordIterator::seek(int64_t recordIdx)
{
    curRecord_ = recordIdx;
}

bool
DataPageIndex::RecordIterator::getNext(ReaderRecord *record)
{
    if (curRecord_ >= index_->getNumRecords()) {
        return false;
    }
    // XXX make this more efficient; crawling the pages/records instead of
    // traversing the index every time
    index_->getRecordByNum(curRecord_, record);
    ++curRecord_;
    return true;
}

void
DataPageIndex::PageIterator::seek(int64_t pageIdx)
{
    EntryList *entryList = index_->head_;

    for (unsigned ii = 0; ii < pageIdx; ii++) {
        entryList = entryList->next;
    }

    curPage_ = entryList;
    curPageIdx_ = pageIdx;
}

bool
DataPageIndex::PageIterator::getNext(uint8_t **pageOut)
{
    if (curPage_ == NULL) {
        return false;
    }

    IndexEntry *entry = &curPage_->entries[0];

    uint8_t *dataPage = reinterpret_cast<uint8_t *>(
        roundDown((uintptr_t) entry->recPointer, index_->pageSize_));
    *pageOut = dataPage;
    curPage_ = curPage_->next;

    return true;
}
