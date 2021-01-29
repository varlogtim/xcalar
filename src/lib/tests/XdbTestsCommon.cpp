// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>
#include <string>
#include <new>

#include "config/Config.h"
#include "xdb/Xdb.h"
#include "operators/Dht.h"
#include "df/DataFormat.h"
#include "operators/OperatorsHash.h"
#include "operators/Operators.h"
#include "util/MemTrack.h"
#include "common/InitTeardown.h"
#include "runtime/Runtime.h"
#include "msg/Xid.h"
#include "util/Random.h"
#include "test/QA.h"
#include "cursor/Cursor.h"
#include "util/System.h"
#include "xdb/HashMergeSort.h"
#include "app/AppMgr.h"
#include "parent/Parent.h"

static constexpr const char *moduleName = "xdbFuncTest";
static constexpr XdbId XdbInvalidXdbId = 0xdeadbeef;

// Configuration for cursor tests.
static uint64_t numRowsCursorTestAscending = 1 << 20;
static uint64_t numRowsCursorTestDescending = 1 << 20;
static uint64_t numRowsCursorTestInsertAndGetSingle = 1 << 20;
static uint64_t numRowsCursorTestInsertAndSeekThenVerifyOne = 1 << 20;
static uint64_t numRowsCursorTestInsertAndSeekThenGetNext = 1 << 20;
static uint64_t numRowsCursorTestPartiallyOrderedVsUnordered = 1 << 20;

// Configuration for page cursor tests.
static uint64_t xdbPgCursorStressThreads = 7;
static uint64_t numRowsXdbPgCursorStress = 1 << 20;

// Configuration for OrderedVsUnordered tests.
static uint64_t xdbOrderedVsUnorderedStressThreads = 7;
static uint64_t numRowsXdbOrderedVsUnorderedStress = 1 << 20;
static uint64_t numItersXdbOrderedVsUnorderedStress = 1 << 15;

// Configuration for Page Cursor Perf tests.
static uint64_t numRowsXdbPgCursorPerfTest = 1 << 20;

// Configuration for Xdb create, load, drop test.
static uint64_t numRowsPerXdbForCreateLoadDropTest = 1 << 20;
static uint64_t numXdbsForCreateLoadDropTest = 1 << 4;
static uint64_t xdbCreateLoadDropTestThreads = 7;
static uint64_t xdbParallelKvInsertsThreads = 7;
static uint64_t xdbParallelPageInsertsThreads = 7;
static uint64_t xdbParallelCursorThreads = 7;

struct ArgsXdbPgCursorTests {
    pthread_t threadId;
    Status retStatus;
};

void
setNumRowsCursorTestAscending(uint64_t val)
{
    numRowsCursorTestAscending = val;
}

void
setNumRowsCursorTestDescending(uint64_t val)
{
    numRowsCursorTestDescending = val;
}

void
setNumRowsCursorTestInsertAndGetSingle(uint64_t val)
{
    numRowsCursorTestInsertAndGetSingle = val;
}

void
setNumRowsCursorTestInsertAndSeekThenVerifyOne(uint64_t val)
{
    numRowsCursorTestInsertAndSeekThenVerifyOne = val;
}

void
setNumRowsCursorTestInsertAndSeekThenGetNext(uint64_t val)
{
    numRowsCursorTestInsertAndSeekThenGetNext = val;
}

void
setNumRowsCursorTestPartiallyOrderedVsUnordered(uint64_t val)
{
    numRowsCursorTestPartiallyOrderedVsUnordered = val;
}

void
setXdbPgCursorStressThreads(uint64_t val)
{
    xdbPgCursorStressThreads = val;
}

void
setNumRowsXdbPgCursorStress(uint64_t val)
{
    numRowsXdbPgCursorStress = val;
}

void
setXdbOrderedVsUnorderedStressThreads(uint64_t val)
{
    xdbOrderedVsUnorderedStressThreads = val;
}

void
setNumRowsXdbOrderedVsUnorderedStress(uint64_t val)
{
    numRowsXdbOrderedVsUnorderedStress = val;
}

void
setNumItersXdbOrderedVsUnorderedStress(uint64_t val)
{
    numItersXdbOrderedVsUnorderedStress = val;
}

void
setNumRowsXdbPgCursorPerfTest(uint64_t val)
{
    numRowsXdbPgCursorPerfTest = val;
}

void
setNumRowsPerXdbForCreateLoadDropTest(uint64_t val)
{
    numRowsPerXdbForCreateLoadDropTest = val;
}

void
setNumXdbsForCreateLoadDropTest(uint64_t val)
{
    numXdbsForCreateLoadDropTest = val;
}

void
setXdbCreateLoadDropTestThreads(uint64_t val)
{
    numXdbsForCreateLoadDropTest = val;
}

void
setXdbParallelKvInsertsThreads(uint64_t val)
{
    xdbParallelKvInsertsThreads = val;
}

void
setXdbParallelPageInsertsThreads(uint64_t val)
{
    xdbParallelPageInsertsThreads = val;
}

void
setXdbParallelCursorThreads(uint64_t val)
{
    xdbParallelCursorThreads = val;
}

void
unitTestConfig()
{
    // Configuration for cursor tests.
    numRowsCursorTestAscending = 1 << 15;
    numRowsCursorTestDescending = 1 << 15;
    numRowsCursorTestInsertAndGetSingle = 1 << 15;
    numRowsCursorTestInsertAndSeekThenVerifyOne = 1 << 15;
    numRowsCursorTestInsertAndSeekThenGetNext = 1 << 15;
    numRowsCursorTestPartiallyOrderedVsUnordered = 1 << 15;

    // Configuration for page cursor tests.
    xdbPgCursorStressThreads = 7;
    numRowsXdbPgCursorStress = 1 << 15;

    // Configuration for OrderedVsUnordered tests.
    xdbOrderedVsUnorderedStressThreads = 7;
    numRowsXdbOrderedVsUnorderedStress = 1 << 15;
    numItersXdbOrderedVsUnorderedStress = 1 << 7;

// Configuration for Page Cursor Perf tests.
#ifdef RUST
#ifdef ADDRESS_SANITIZER
    numRowsXdbPgCursorPerfTest = 1 << 20;
#else
    numRowsXdbPgCursorPerfTest = 1 << 30;
#endif  // ADDRESS_SANITIZER
#else
    numRowsXdbPgCursorPerfTest = 1 << 15;
#endif  // RUST
}

// Serialize the deserialize entire table.  Piggybacks on caller's subsequent
// tests to verify data integrity.
Status
xdbCheckSerDes(const XdbId xdbId, bool serOnly)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    TableCursor xdbCursor;
    Xdb *xdb;

    status = xdbMgr->xdbGet(xdbId, &xdb, NULL);
    assert(status == StatusOk);

    if (XcalarConfig::get()->xdbSerDesMode_ !=
        (uint32_t) XcalarConfig::SerDesMode::Disabled) {
        status = xdbMgr->xdbSerializeLocal(xdbId);
        verify(status == StatusOk || status == StatusNoXdbPageBcMem);

        if (serOnly) {
            return status;
        }

        status =
            CursorManager::get()->createOnTable(xdbId,
                                                Ordered,
                                                Cursor::InsertIntoHashTable,
                                                &xdbCursor);
        verify(status == StatusXdbNotResident);

        status = xdbMgr->xdbDeserializeLocal(xdbId);
        verify(status == StatusOk);
    }

    return status;
}

Status
xdbCheckSerDes(const XdbId xdbId)
{
    return xdbCheckSerDes(xdbId, false);
}

Status
xdbSetupRuntime()
{
    Status status;
    status = AppMgr::get()->addBuildInApps();
    verify(status == StatusOk);
    status = DhtMgr::get()->dhtStateRestore();
    verify(status == StatusOk);
    return StatusOk;
}

Status
xdbTeardownRuntime()
{
    Status status = StatusOk;

    AppMgr::get()->removeBuiltInApps();

    return status;
}

// Typical Unit test.
// - Xdb Creation and Failure handling.
// - Xdb Cursoring and Failure handling.
Status
xdbBasicTests()
{
    Status status;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    XdbId xdbIdDesc = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    XdbMeta *xdbMeta;
    Xdb *xdbDesc;
    XdbMeta *xdbMetaDesc;
    XdbMgr *xdbMgr = XdbMgr::get();
    const char *keyName = "foo";
    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    NewKeyValueEntry *kvEntry = NULL;
    TableCursor xdbCursor;

    size_t numFields = 2;
    tupMeta.setNumFields(numFields);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfInt64, 1);

    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);
    kvEntry = new (std::nothrow) NewKeyValueEntry(&kvMeta);

    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               "foo",
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    verify(status == StatusOk);

    keyName = "fooDesc";
    status = xdbMgr->xdbCreate(xdbIdDesc,
                               "fooDesc",
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Descending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    verify(status == StatusOk);

    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    verify(status == StatusOk);
    status = xdbMgr->xdbGet(xdbIdDesc, &xdbDesc, &xdbMetaDesc);
    verify(status == StatusOk);

    status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
    verify(status == StatusXdbNotFound);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    verify(status == StatusOk);
    status = xdbMgr->xdbSetKeyType(xdbMetaDesc, DfInt64, 0);
    verify(status == StatusOk);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    verify(status == StatusOk);
    status = xdbMgr->xdbSetKeyType(xdbMeta, DfString, 0);

    DfFieldValue key;
    NewTupleValues valueArray;

    // validate insert
    for (uint64_t ii = 3; ii > 0; ii--) {
        key.int64Val = ii;

        DfFieldValue valTmp;
        valTmp.fatptrVal = ii * 2;
        valueArray.set(0, valTmp, DfFatptr);
        valueArray.set(1, key, DfInt64);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        verify(status == StatusOk);

        status =
            xdbMgr->xdbInsertKv(xdbDesc, &key, &valueArray, XdbInsertCrcHash);
        verify(status == StatusOk);

        printf("key.int64Val %d data %ld\n", (int) key.int64Val, ii * 2);

        valTmp.fatptrVal = ii * 2 + 1;
        valueArray.set(0, valTmp, DfFatptr);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        verify(status == StatusOk);
        status =
            xdbMgr->xdbInsertKv(xdbDesc, &key, &valueArray, XdbInsertCrcHash);
        verify(status == StatusOk);

        printf("key.int64Val %d data %ld\n", (int) key.int64Val, ii * 2 + 1);

        valTmp.fatptrVal = ii * 2 + 2;
        valueArray.set(0, valTmp, DfFatptr);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        verify(status == StatusOk);
        status =
            xdbMgr->xdbInsertKv(xdbDesc, &key, &valueArray, XdbInsertCrcHash);
        verify(status == StatusOk);

        printf("key.int64Val %d data %ld\n", (int) key.int64Val, ii * 2 + 2);
    }

    // test case for bug 2461
    uint64_t numRows;
    verify(xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows) == StatusBusy);

    status = xdbMgr->xdbLoadDone(xdbId);
    verify(status == StatusOk);

    status = xdbMgr->xdbLoadDone(xdbIdDesc);
    verify(status == StatusOk);

    status = xdbCheckSerDes(xdbId);
    verify(status == StatusOk);

    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
    verify(status == StatusOk);
    verify(numRows == 9);

    status = CursorManager::get()->createOnTable(xdbId,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    verify(status == StatusOk);

    // validate sort
    int64_t last = -1;
    bool retIsValid;
    while ((status = xdbCursor.getNext(kvEntry)) == StatusOk) {
        key = kvEntry->getKey(&retIsValid);
        assert(retIsValid);
        verify(last <= key.int64Val);

        printf("Sort key.int64Val %d\n", (int) key.int64Val);

        last = key.int64Val;
        DfFieldType tmpType = kvEntry->kvMeta_->tupMeta_->getFieldType(0);
        bool retIsValid;
        DfFieldValue tmpValue =
            kvEntry->tuple_.get(0, numFields, tmpType, &retIsValid);
        verify(tmpValue.fatptrVal >= (uint64_t) key.int64Val * 2);
        verify(tmpValue.fatptrVal <= (uint64_t) key.int64Val * 2 + 2);
    }
    verify(status == StatusNoData);

    CursorManager::get()->destroy(&xdbCursor);

    // validate desc sort
    status = CursorManager::get()->createOnTable(xdbIdDesc,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    verify(status == StatusOk);

    last = INT64_MAX;
    while ((status = xdbCursor.getNext(kvEntry)) == StatusOk) {
        key = kvEntry->getKey(&retIsValid);
        assert(retIsValid);
        verify(last >= key.int64Val);

        printf("Sort Desc key.int64Val %d\n", (int) key.int64Val);

        last = key.int64Val;
        DfFieldType tmpType = kvEntry->kvMeta_->tupMeta_->getFieldType(0);
        bool retIsValid;
        DfFieldValue tmpValue =
            kvEntry->tuple_.get(0, numFields, tmpType, &retIsValid);
        verify(tmpValue.fatptrVal >= (uint64_t) key.int64Val * 2);
        verify(tmpValue.fatptrVal <= (uint64_t) key.int64Val * 2 + 2);
    }
    verify(status == StatusNoData);

    CursorManager::get()->destroy(&xdbCursor);

    status = CursorManager::get()->createOnTable(xdbId,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    verify(status == StatusOk);

    // validate seek
    status = xdbCursor.seek(9, Cursor::SeekToAbsRow);
    verify(status == StatusOk);
    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusNoData);

    status = xdbCursor.seek(8, Cursor::SeekToAbsRow);
    verify(status == StatusOk);
    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);

    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusNoData);

    status = xdbCursor.seek(-3, Cursor::SeekRelative);
    verify(status == StatusOk);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);

    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);

    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);

    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusNoData);

    status = xdbCursor.seek(0, Cursor::SeekToAbsRow);
    verify(status == StatusOk);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 1);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 1);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 1);

    status = xdbCursor.seek(3, Cursor::SeekToAbsRow);
    verify(status == StatusOk);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 2);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 2);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 2);

    status = xdbCursor.seek(-2, Cursor::SeekRelative);
    verify(status == StatusOk);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 2);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 2);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    // validate dup
    status = xdbCursor.seek(7, Cursor::SeekToAbsRow);
    verify(status == StatusOk);
    TableCursor dupCursor;
    status = xdbCursor.duplicate(&dupCursor);
    verify(status == StatusOk);

    status = dupCursor.seek(-2, Cursor::SeekRelative);
    verify(status == StatusOk);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = dupCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 2);

    status = dupCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = dupCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = dupCursor.getNext(kvEntry);
    verify(status == StatusOk);
    key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    verify(key.int64Val == 3);

    status = dupCursor.getNext(kvEntry);
    verify(status == StatusNoData);

    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusNoData);
    status = dupCursor.getNext(kvEntry);
    verify(status == StatusNoData);
    status = xdbCursor.getNext(kvEntry);
    verify(status == StatusNoData);

    CursorManager::get()->destroy(&xdbCursor);

    CursorManager::get()->destroy(&dupCursor);

    xdbMgr->xdbDrop(xdbId);
    xdbMgr->xdbDrop(xdbIdDesc);

    keyName = "bar";
    status = xdbMgr->xdbCreate(xdbId,
                               "bar",
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    verify(status == StatusOk);

    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    verify(status == StatusOk);
    status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
    verify(status == StatusXdbNotFound);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    verify(status == StatusOk);

    DfFieldValue valTmp;
    valTmp.fatptrVal = 0;
    valueArray.set(0, valTmp, DfFatptr);
    key.int64Val = -1;
    status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
    verify(status == StatusOk);
    key.int64Val = INT64_MAX;
    status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
    verify(status == StatusOk);
    key.int64Val = INT64_MAX / 2;
    status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
    verify(status == StatusOk);
    key.int64Val = INT64_MAX / 8;
    status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
    verify(status == StatusOk);
    key.int64Val = INT64_MAX / 128;
    status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
    verify(status == StatusOk);

    status = xdbMgr->xdbLoadDone(xdbId);
    verify(status == StatusOk);

    xdbMgr->xdbDrop(xdbId);

    delete kvEntry;
    kvEntry = NULL;

    return StatusOk;
}

Status
xdbSortTest(uint64_t numRows)
{
    Status status;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbId xdbId = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    uint64_t numPages;
    size_t xdbSize;

    Stopwatch stopwatch;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;

    const char *immediateName[2];
    immediateName[0] = "column0";
    immediateName[1] = "key";
    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    bool retIsValid;

    tupMeta.setNumFields(2);
    tupMeta.setFieldType(DfUInt64, 0);
    tupMeta.setFieldType(DfUInt64, 1);
    tupMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);

    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);
    NewKeyValueEntry kvEntryIns;
    new (&kvEntryIns) NewKeyValueEntry(&kvMeta);
    NewKeyValueEntry kvEntryCur;
    new (&kvEntryCur) NewKeyValueEntry(&kvMeta);

    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               "key",
                               DfUInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               immediateName,
                               2,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    verify(status == StatusOk);

    status = xdbMgr->xdbGet(xdbId, &xdb, NULL);
    assert(status == StatusOk);

    HashMergeSort hashMergeSort;

    XdbPage *firstPage = NULL;
    XdbPage *xdbPage = xdbMgr->xdbAllocXdbPage(XidInvalid, xdb);
    BailIfNull(xdbPage);

    firstPage = xdbPage;
    numPages = 1;

    xdbMgr->xdbInitXdbPage(xdb,
                           xdbPage,
                           NULL,
                           xdbMgr->bcSize(),
                           XdbSortedNormal);

    for (int64_t ii = numRows - 1; ii >= 0; ii--) {
        DfFieldValue valTmp;
        valTmp.uint64Val = ii;
        kvEntryIns.tuple_.set(0, valTmp, DfUInt64);
        valTmp.uint64Val = ii;
        kvEntryIns.tuple_.set(1, valTmp, DfUInt64);

        status = xdbPage->insertKv(&kvMeta, &kvEntryIns.tuple_);
        if (status != StatusOk) {
            XdbPage *newPage = xdbMgr->xdbAllocXdbPage(XidInvalid, xdb);
            BailIfNullWith(newPage, StatusNoXdbPageBcMem);
            numPages++;

            xdbMgr->xdbInitXdbPage(xdb,
                                   newPage,
                                   NULL,
                                   xdbMgr->bcSize(),
                                   XdbSortedNormal);

            xdbPage->hdr.nextPage = newPage;
            newPage->hdr.prevPage = xdbPage;

            xdbPage = newPage;

            status = xdbPage->insertKv(&kvMeta, &kvEntryIns.tuple_);
            assert(status == StatusOk);
        }
    }
    xdbPage->hdr.nextPage = NULL;

    status = xdbMgr->xdbLoadDone(xdbId);
    verify(status == StatusOk);

    status = xdbCheckSerDes(xdbId);
    verify(status == StatusOk);

    xdbSize = numPages * xdbMgr->bcSize();

    {
        DfFieldValue min;
        DfFieldValue max;
        XdbPage *sortedPage;
        uint64_t numSortedPages;

        min.uint64Val = 0;
        max.uint64Val = numRows - 1;

        stopwatch.restart();
        status = hashMergeSort.sort(firstPage,
                                    &kvMeta,
                                    min,
                                    max,
                                    numRows,
                                    0,
                                    xdb,
                                    &sortedPage,
                                    &numSortedPages);
        BailIfFailed(status);
        stopwatch.stop();
        stopwatch.getPrintableTime(hours,
                                   minutesLeftOver,
                                   secondsLeftOver,
                                   millisecondsLeftOver);
        xSyslog(moduleName,
                XlogInfo,
                "hashMergeSort of xdb Id %lu"
                " of size %lu and rows %lu"
                " finished in %lu:%02lu:%02lu.%03lu",
                xdbId,
                xdbSize,
                numRows,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);

        XdbPage *tmp = sortedPage;
        uint64_t counter = 0;

        while (sortedPage != NULL) {
            NewTuplesCursor tupCursor;
            new (&tupCursor) NewTuplesCursor(sortedPage->tupBuf);

            while ((status = tupCursor.getNext(kvMeta.tupMeta_,
                                               &kvEntryCur.tuple_)) ==
                   StatusOk) {
                DfFieldValue key = kvEntryCur.getKey(&retIsValid);
                assert(retIsValid);
                verify(key.uint64Val == counter);
                counter++;
            }

            sortedPage = (XdbPage *) sortedPage->hdr.nextPage;
        }

        while (tmp != NULL) {
            sortedPage = (XdbPage *) tmp->hdr.nextPage;
            xdbMgr->xdbFreeXdbPage(tmp);
            tmp = sortedPage;
        }
    }

    status = StatusOk;

CommonExit:
    while (firstPage != NULL) {
        xdbPage = (XdbPage *) firstPage->hdr.nextPage;
        xdbMgr->xdbFreeXdbPage(firstPage);
        firstPage = xdbPage;
    }
    xdbMgr->xdbDrop(xdbId);
    return status;
}

Status
xdbSortSanityTest()
{
    uint64_t numRows = 1 << 15;
    return xdbSortTest(numRows);
}

// Cursoring test based on ordering for N keys in Xdb.
Status
cursorTest(bool sorted, Ordering ordering, uint64_t numInserts)
{
    Status status;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    XdbMeta *xdbMeta;
    uint64_t ii;
    RandHandle rndHandle;
    DfFieldValue key;
    NewTupleValues *valueArray = NULL;
    TableCursor xdbCursor;
    XdbMgr *xdbMgr = XdbMgr::get();
    uint64_t numGets = 0;
    bool xdbCreated = false, cursorCreated = false;
    int64_t myMin = INT64_MAX, myMax = INT64_MIN;
    bool retIsValid;

    NewTupleMeta tupMeta;

    size_t numFields = 2;
    tupMeta.setNumFields(numFields);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfInt64, 1);

    NewKeyValueMeta kvMeta(&tupMeta, 1);

    NewKeyValueEntry *kvEntry1 = NULL;
    NewKeyValueEntry *kvEntry2 = NULL;
    kvEntry1 = new (std::nothrow) NewKeyValueEntry(&kvMeta);
    BailIfNull(kvEntry1);

    kvEntry2 = new (std::nothrow) NewKeyValueEntry(&kvMeta);
    BailIfNull(kvEntry2);

    valueArray = new (std::nothrow) NewTupleValues();
    BailIfNull(valueArray);

    rndInitHandle(&rndHandle, 0);

    const char *keyName;
    keyName = "bar";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               keyName,
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               ordering,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    xdbCreated = true;

    // Pretty straightforward call and should not fail
    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);
    status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
    verify(status == StatusXdbNotFound);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    DfFieldValue fieldVal;
    valueArray->set(0, fieldVal, DfFatptr);

    for (ii = 0; ii < numInserts; ii++) {
        fieldVal.fatptrVal = ii;
        valueArray->set(0, fieldVal, DfFatptr);
        key.int64Val = rndGenerate64(&rndHandle);

        if (ii % 2 == 1) {
            key.int64Val = -key.int64Val;
        }

        if (key.int64Val < myMin) {
            myMin = key.int64Val;
        }

        if (key.int64Val > myMax) {
            myMax = key.int64Val;
        }

        valueArray->set(1, key, DfInt64);

        status = xdbMgr->xdbInsertKv(xdb, &key, valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(xdbId);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail
    uint64_t numRows;
    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
    verify(status == StatusOk);
    verify(numRows == numInserts);

    status = CursorManager::get()->createOnTable(xdbId,
                                                 sorted ? Ordered : Unordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    cursorCreated = true;

    while ((status = xdbCursor.getNext(kvEntry1)) == StatusOk) {
        DfFieldValue field1, field2;
        field1 = kvEntry1->tuple_.get(0,
                                      numFields,
                                      tupMeta.getFieldType(0),
                                      &retIsValid);
        assert(retIsValid);
        verify(field1.fatptrVal < numInserts);
        DfFieldValue key1 = kvEntry1->getKey(&retIsValid);
        assert(retIsValid);
        DfFieldValue key2;

        if (numGets == 0) {
            if (ordering == Ascending) {
                verify(myMin == key1.int64Val);
            } else {
                verify(myMax == key1.int64Val);
            }
        } else if (numGets == numInserts - 1) {
            if (ordering == Ascending) {
                verify(myMax == key1.int64Val);
            } else {
                verify(myMin == key1.int64Val);
            }
        }

        TableCursor dupCursor;

        status = xdbCursor.duplicate(&dupCursor);
        BailIfFailed(status);

        status = dupCursor.seek(-1, Cursor::SeekRelative);
        BailIfFailed(status);

        status = dupCursor.getNext(kvEntry2);
        BailIfFailed(status);

        key2 = kvEntry2->getKey(&retIsValid);
        assert(retIsValid);
        verify(DataFormat::fieldIsEqual(DfInt64, key1, key2));

        field2 = kvEntry2->tuple_.get(0,
                                      numFields,
                                      tupMeta.getFieldType(0),
                                      &retIsValid);
        assert(retIsValid);
        verify(field1.fatptrVal == field2.fatptrVal);

        CursorManager::get()->destroy(&dupCursor);

        numGets++;
    }

    if (status != StatusNoData) {
        goto CommonExit;
    }
    verify(numGets == numInserts);
    status = StatusOk;

CommonExit:
    if (cursorCreated) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    if (xdbCreated) {
        xdbMgr->xdbDrop(xdbId);
    }
    if (kvEntry1) {
        delete kvEntry1;
        kvEntry1 = NULL;
    }
    if (kvEntry2) {
        delete kvEntry2;
        kvEntry2 = NULL;
    }
    if (valueArray) {
        delete valueArray;
        valueArray = NULL;
    }

    return status;
}

// Create table, insert and then getNext() each of the rows
// starting from row 0.
Status
cursorTestInsertAndGetSingle(bool sorted, uint64_t numInserts)
{
    Status status = StatusOk;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    XdbMeta *xdbMeta;
    uint64_t ii;
    DfFieldValue key;
    NewTupleValues valueArray;
    TableCursor opCursor;
    XdbMgr *xdbMgr = XdbMgr::get();
    uint64_t numGets = 0;
    int64_t jj = 0;
    bool xdbCreated = false;
    bool cursorCreated = false;
    bool retIsValid;

    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    size_t numFields = 2;
    tupMeta.setNumFields(numFields);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfInt64, 1);
    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);

    NewKeyValueEntry kvEntry(&kvMeta);

    const char *keyName = "bar";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               keyName,
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    xdbCreated = true;

    // Pretty straightforward call and should not fail
    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);
    status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
    verify(status == StatusXdbNotFound);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    // Do sequential inserts.
    for (ii = 0; ii < numInserts; ii++) {
        DfFieldValue field;
        field.fatptrVal = ii;
        valueArray.set(0, field, DfFatptr);

        key.int64Val = jj++;
        valueArray.set(1, key, DfInt64);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(xdbId);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail
    uint64_t numRows;
    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
    verify(status == StatusOk);
    verify(numRows == numInserts);

    status = CursorManager::get()->createOnTable(xdbId,
                                                 sorted ? Ordered : Unordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &opCursor);
    BailIfFailed(status);
    cursorCreated = true;

    jj = 0;
    key.int64Val = 0;

    // Fetch each key starting from row 0 (because operatorsCursorCreateLocal()
    // sets xdbCursor to row 0). Since cursor is sorted we expect
    // sequential keys.
    while ((status = opCursor.getNext(&kvEntry)) == StatusOk) {
        DfFieldValue key = kvEntry.getKey(&retIsValid);
        verify(retIsValid);
        verify(key.int64Val == jj);

        DfFieldValue field;
        field = kvEntry.tuple_.get(0,
                                   numFields,
                                   tupMeta.getFieldType(0),
                                   &retIsValid);
        assert(retIsValid);
        verify(field.fatptrVal == (uint64_t) jj);
        ++jj;
        ++numGets;
    }

    if (status != StatusNoData) {
        goto CommonExit;
    }
    verify(numGets == numInserts);
    status = StatusOk;

CommonExit:
    if (cursorCreated) {
        CursorManager::get()->destroy(&opCursor);
    }

    if (xdbCreated) {
        xdbMgr->xdbDrop(xdbId);
    }

    return status;
}

// Create table, insert, seek() to each row starting
// from row 0, then verify kv at the seek position.
Status
cursorTestInsertAndSeekThenVerifyOne(bool sorted, uint64_t numInserts)
{
    Status status = StatusOk;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    XdbMeta *xdbMeta;
    uint64_t ii;

    NewTupleValues valueArray;
    TableCursor xdbCursor;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool xdbCreated = false;
    bool cursorCreated = false;
    int64_t jj = 0;
    int64_t rowNumIn = 0;

    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    size_t numFields = 2;
    tupMeta.setNumFields(numFields);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfInt64, 1);
    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);

    NewKeyValueEntry kvEntry(&kvMeta);

    const char *keyName = "bar";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               keyName,
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    xdbCreated = true;

    // Pretty straightforward call and should not fail
    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);
    status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
    verify(status == StatusXdbNotFound);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    DfFieldValue key;

    // Do sequential inserts.
    for (ii = 0; ii < numInserts; ii++) {
        DfFieldValue field;
        field.fatptrVal = ii;
        valueArray.set(0, field, DfFatptr);
        key.int64Val = jj++;
        valueArray.set(1, key, DfInt64);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(xdbId);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail
    uint64_t numRows;
    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
    verify(status == StatusOk);
    verify(numRows == numInserts);

    // Init cursor
    status = CursorManager::get()->createOnTable(xdbId,
                                                 sorted ? Ordered : Unordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    cursorCreated = true;

    // Start from row 0.
    while (true) {
        DfFieldValue field;

        status = xdbCursor.seek(rowNumIn, Cursor::SeekToAbsRow);
        BailIfFailed(status);
        status = xdbCursor.getNext(&kvEntry);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        bool retIsValid;
        field = kvEntry.getKey(&retIsValid);
        verify(retIsValid);
        verify(field.int64Val == rowNumIn);
        field = kvEntry.tuple_.get(0,
                                   numFields,
                                   kvEntry.kvMeta_->tupMeta_->getFieldType(0),
                                   &retIsValid);
        verify(retIsValid);
        verify(field.fatptrVal == (unsigned) rowNumIn);

        ++rowNumIn;
    }
    if (status != StatusNoData) {
        goto CommonExit;
    }
    status = StatusOk;
    verify(rowNumIn == (int64_t) numInserts);

    // Start from last row and go backwards.
    rowNumIn = numRows;
    status = xdbCursor.seek(rowNumIn, Cursor::SeekToAbsRow);
    BailIfFailed(status);

    status = xdbCursor.getNext(&kvEntry);
    if (status != StatusNoData) {
        goto CommonExit;
    }
    status = StatusOk;

    --rowNumIn;
    while (true) {
        DfFieldValue field;

        status = xdbCursor.seek(rowNumIn, Cursor::SeekToAbsRow);
        BailIfFailed(status);
        status = xdbCursor.getNext(&kvEntry);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        bool retIsValid;
        field = kvEntry.getKey(&retIsValid);
        verify(retIsValid);
        verify(field.int64Val == rowNumIn);
        field = kvEntry.tuple_.get(0,
                                   numFields,
                                   kvEntry.kvMeta_->tupMeta_->getFieldType(0),
                                   &retIsValid);
        verify(retIsValid);
        verify(field.fatptrVal == (unsigned) rowNumIn);

        --rowNumIn;
    }

    if (status != StatusNoData) {
        goto CommonExit;
    }
    status = StatusOk;
    verify(rowNumIn == -1);

CommonExit:
    if (cursorCreated == true) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    if (xdbCreated == true) {
        xdbMgr->xdbDrop(xdbId);
    }

    return status;
}

Status
xdbCursorTests()
{
    Status status = StatusFailed;

    status = cursorTest(true, Ascending, numRowsCursorTestAscending);
    if (status != StatusOk) {
        return status;
    }

    status = cursorTest(true, Descending, numRowsCursorTestDescending);
    if (status != StatusOk) {
        return status;
    }

    status =
        cursorTestInsertAndGetSingle(true, numRowsCursorTestInsertAndGetSingle);
    if (status != StatusOk) {
        return status;
    }

    status = cursorTestInsertAndSeekThenVerifyOne(
        true, numRowsCursorTestInsertAndSeekThenVerifyOne);
    if (status != StatusOk) {
        return status;
    }

    return status;
}

// Read from a Dictionary, populate a table and cursor it in sorted order.
Status
xdbStringTests()
{
    FILE *fp = NULL;
    char tmpString[100];
    const XdbId xdbId = XidMgr::get()->xidGetNext();
    DfFieldValue key;
    NewTupleValues valueArray;
    Status status = StatusOk;
    Xdb *xdb;
    XdbMeta *xdbMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool xdbTableCreated = false;
    bool cursorCreated = false;
    uint64_t lastHash = 0;
    uint64_t curHash;
    char *newLine = NULL;
    const char *dictFile = "/usr/share/dict/words";
    TableCursor xdbCursor;

    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    tupMeta.setNumFields(2);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfString, 1);
    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);

    NewKeyValueEntry kvEntry(&kvMeta);
    NewKeyValueEntry lastKvEntry(&kvMeta);

    const char *keyName;
    keyName = "bar";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               keyName,
                               DfString,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    xdbTableCreated = true;

    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfString, 0);
    BailIfFailed(status);

    // @SymbolCheckIgnore
    fp = fopen(dictFile, "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogInfo,
                "xdbStringTests: File %s open failed with %s",
                dictFile,
                strGetFromStatus(status));
        goto CommonExit;
    }

    DfFieldValue field;
    field.fatptrVal = 0;
    valueArray.set(0, field, DfFatptr);

    while (fgets(tmpString, sizeof(tmpString), fp) != NULL) {
        newLine = strrchr(tmpString, '\n');
        verify(newLine != NULL);
        newLine[0] = '\0';

        key.stringVal.strActual = tmpString;
        key.stringVal.strSize = strlen(tmpString) + 1;

        curHash = operatorsHashByString(tmpString);
        verify(curHash >= lastHash);
        valueArray.set(1, key, DfString);

        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(xdbId);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail
    uint64_t numRows;
    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
    verify(status == StatusOk);

    status = xdbCheckSerDes(xdbId);
    verify(status == StatusOk);

    status = CursorManager::get()->createOnTable(xdbId,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    cursorCreated = true;

    // validate sort
    field.stringVal.strActual = "";
    lastKvEntry.tuple_.set(1, field, DfString);
    while ((status = xdbCursor.getNext(&kvEntry)) == StatusOk) {
        DfFieldValue lastKey, key;
        bool retIsValid;
        lastKey = lastKvEntry.getKey(&retIsValid);
        verify(retIsValid);
        key = kvEntry.getKey(&retIsValid);
        verify(retIsValid);

        verify(DataFormat::fieldCompare(DfString, lastKey, key) < 0);
        kvEntry.tuple_.cloneTo(&tupMeta, &lastKvEntry.tuple_);
    }

    if (status == StatusNoData) {
        status = StatusOk;
    }

CommonExit:
    if (fp != NULL) {
        // @SymbolCheckIgnore
        fclose(fp);
    }

    if (cursorCreated) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    if (xdbTableCreated) {
        xdbMgr->xdbDrop(xdbId);
    }

    return status;
}

// Cursor Xdb pages using multiple threads.
void *
xdbPgCursorTests(void *arg)
{
    ArgsXdbPgCursorTests *argInfo = (ArgsXdbPgCursorTests *) arg;
    Status status = StatusOk;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    XdbMeta *xdbMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool xdbTableCreated = false;
    bool xdbPgCursorCreated = false;
    XdbPgCursor xdbPgCursor;

    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    size_t numFields = 2;
    tupMeta.setNumFields(numFields);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfUInt64, 1);
    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);

    DfFieldValue key;
    NewTupleValues valueArray;
    DfFieldValue value;

    const char *keyName = "foo";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               keyName,
                               DfUInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    xdbTableCreated = true;

    // Pretty straightforward call and should not fail
    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
    verify(status == StatusXdbNotFound);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail. This is because
    // the key type is already set here.
    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfString, 0);

    // Insert keys
    for (uint64_t ii = 0; ii < numRowsXdbPgCursorStress; ++ii) {
        key.uint64Val = ii;
        value.uint64Val = ii;
        valueArray.set(0, value, DfUInt64);
        valueArray.set(1, key, DfUInt64);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(xdbId);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail
    uint64_t numRows;
    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
    verify(status == StatusOk);
    verify(numRows == numRowsXdbPgCursorStress);

    NewTuplesBuffer *tupBufOut;
    status = xdbPgCursor.init(xdbId, Ordered);
    BailIfFailed(status);
    xdbPgCursorCreated = true;

    // xdbPgCursor already points to the first page. Hence ii = 1.
    int32_t ii;
    ii = 1;

    do {
        status = xdbPgCursor.getNextTupBuf(&tupBufOut);
        if (status != StatusOk) {
            if (status == StatusNoData) {
                verify(!tupBufOut);
            } else {
                goto CommonExit;
            }
        } else {
            ++ii;
            verify(tupBufOut);
        }
    } while (status == StatusOk);

    if (status != StatusNoData) {
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    // Pretty straightforward call and should not fail
    if (xdbPgCursorCreated == true) {
        Status retStatus = xdbPgCursor.cleanupUnorderedPages();
        verify(retStatus == StatusOk);
        if (xdbPgCursor.xdbPage_ != NULL) {
            xdbMgr->pagePutRef(xdbPgCursor.xdb_, 0, xdbPgCursor.xdbPage_);
            xdbPgCursor.xdbPage_ = NULL;
        }
    }

    if (xdbTableCreated == true) {
        xdbMgr->xdbDrop(xdbId);
    }

    argInfo->retStatus = status;
    return NULL;
}

Status
xdbPgCursorThreadStress()
{
    Status status = StatusOk;
    ArgsXdbPgCursorTests *args = (ArgsXdbPgCursorTests *) memAlloc(
        sizeof(ArgsXdbPgCursorTests) * xdbPgCursorStressThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbPgCursorStressThreads; ++ii) {
        args[ii].retStatus = StatusOk;
        verifyOk(Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       xdbPgCursorTests,
                                                       (void *) &args[ii]));
    }

    for (uint64_t ii = 0; ii < xdbPgCursorStressThreads; ++ii) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

struct ArgXdbOrderedvsUnorderedTests {
    XdbId stressXdbId;
    uint64_t threadId;
    uint64_t numIters;
    Status retStatus;
};

// Cursor ordered/unordered seek by key.
void *
xdbOrderedvsUnorderedTests(void *arg)
{
    Status status = StatusOk;
    ArgXdbOrderedvsUnorderedTests *argInfo =
        (ArgXdbOrderedvsUnorderedTests *) arg;
    uint64_t threadId = argInfo->threadId;
    bool sorted = (threadId == 0);
    uint64_t numIters = argInfo->numIters;
    XdbId stressXdbId = argInfo->stressXdbId;
    RandHandle rndHandle;
    bool operatorsCursorCreated = false;
    rndInitHandle(&rndHandle, (uint32_t) threadId);
    TableCursor xdbCursor;

    NewKeyValueEntry kvEntry;

    status = CursorManager::get()->createOnTable(stressXdbId,
                                                 sorted ? Ordered : Unordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    operatorsCursorCreated = true;
    new (&kvEntry) NewKeyValueEntry(xdbCursor.kvMeta_);
    size_t numFields;
    numFields = kvEntry.kvMeta_->tupMeta_->getNumFields();

    for (uint64_t ii = 0; ii < numIters; ii++) {
        DfFieldValue key;
        key.int64Val =
            rndGenerate64(&rndHandle) % numRowsXdbOrderedVsUnorderedStress;

        status = xdbCursor.seek(key.int64Val, Cursor::SeekToAbsRow);
        BailIfFailed(status);

        status = xdbCursor.getNext(&kvEntry);
        assert(status == StatusOk);

        if (sorted) {
            bool retIsValid;
            DfFieldValue value1 =
                kvEntry.tuple_.get(0,
                                   numFields,
                                   kvEntry.kvMeta_->tupMeta_->getFieldType(0),
                                   &retIsValid);
            verify(retIsValid);
            DfFieldValue value2 =
                kvEntry.tuple_.get(1,
                                   numFields,
                                   kvEntry.kvMeta_->tupMeta_->getFieldType(1),
                                   &retIsValid);
            verify(retIsValid);
            verify(value1.fatptrVal == (unsigned) value2.int64Val);
        }
    }

CommonExit:
    if (operatorsCursorCreated == true) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    argInfo->retStatus = status;
    return NULL;
}

Status
xdbOrderedVsUnorderedStress()
{
    Status status = StatusOk;
    Xdb *xdb;
    XdbMeta *xdbMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbId stressXdbId = XidMgr::get()->xidGetNext();
    bool stressTableCreated = false;
    ArgXdbOrderedvsUnorderedTests *args = NULL;

    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    tupMeta.setNumFields(2);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfUInt64, 1);
    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);

    DfFieldValue key;
    NewTupleValues valueArray;

    const char *keyName = "foo";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(stressXdbId,
                               keyName,
                               DfUInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    stressTableCreated = true;

    // Pretty straightforward call and should not fail
    status = xdbMgr->xdbGet(stressXdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    // Insert keys
    for (uint64_t ii = 0; ii < numRowsXdbOrderedVsUnorderedStress; ++ii) {
        key.uint64Val = ii;
        DfFieldValue field;
        field.uint64Val = ii;

        valueArray.set(0, field, DfUInt64);
        valueArray.set(1, key, DfUInt64);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(stressXdbId);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail
    uint64_t numRows;
    status = xdbMgr->xdbGetNumLocalRowsFromXdbId(stressXdbId, &numRows);
    verify(status == StatusOk);
    verify(numRows == numRowsXdbOrderedVsUnorderedStress);

    args = (ArgXdbOrderedvsUnorderedTests *) memAlloc(
        sizeof(ArgXdbOrderedvsUnorderedTests) *
        xdbOrderedVsUnorderedStressThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbOrderedVsUnorderedStressThreads; ++ii) {
        args[ii].stressXdbId = stressXdbId;
        args[ii].numIters = numItersXdbOrderedVsUnorderedStress;
        args[ii].retStatus = StatusOk;
        verifyOk(
            Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                  NULL,
                                                  xdbOrderedvsUnorderedTests,
                                                  (void *) &args[ii]));
    }

    for (uint64_t ii = 0; ii < xdbOrderedVsUnorderedStressThreads; ++ii) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

CommonExit:
    if (stressTableCreated == true) {
        xdbMgr->xdbDrop(stressXdbId);
    }

    if (args != NULL) {
        memFree(args);
        args = NULL;
    }

    return status;
}

// Measure time taken for Xdb Loads, sort, page iteration, scan.
Status
xdbPgCursorPerfTest()
{
    Status status = StatusOk;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    Xdb *xdb;
    XdbMeta *xdbMeta;
    uint64_t ii;
    XdbMgr *xdbMgr = XdbMgr::get();
    DfFieldValue key;
    NewTupleValues valueArray;
    TableCursor xdbCursor;
    bool xdbPerfTableCreated = false;
    bool xdbCursorCreated = false;

    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    tupMeta.setNumFields(2);
    tupMeta.setFieldType(DfFatptr, 0);
    tupMeta.setFieldType(DfInt64, 1);
    new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);
    NewKeyValueEntry kvEntry(&kvMeta);

    const char *keyName = "xdbPerf";
    const char *fatPtrName[1];
    fatPtrName[0] = "p";
    status = xdbMgr->xdbCreate(xdbId,
                               keyName,
                               DfInt64,
                               1,
                               &tupMeta,
                               NULL,
                               0,
                               &keyName,
                               1,
                               fatPtrName,
                               1,
                               Ascending,
                               XdbGlobal,
                               DhtInvalidDhtId);
    BailIfFailed(status);
    xdbPerfTableCreated = true;

    // Pretty straightforward call and should not fail
    status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    int64_t myMin, myMax;
    myMin = INT64_MAX;
    myMax = INT64_MIN;

    xSyslog(moduleName, XlogInfo, "xdbPgCursorPerfTest: Testing xdb load");

    int ret;
    struct timespec startTime, endTime;

    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    verify(ret == 0);

    for (ii = 0; ii < numRowsXdbPgCursorPerfTest; ii++) {
        DfFieldValue field;
        field.fatptrVal = ii;
        valueArray.set(0, field, DfFatptr);
        key.int64Val = (ii % 2) ? ii : -ii;

        if (ii % 2 == 1) {
            key.int64Val = -key.int64Val;
        }

        if (key.int64Val < myMin) {
            myMin = key.int64Val;
        }

        if (key.int64Val > myMax) {
            myMax = key.int64Val;
        }
        valueArray.set(1, key, DfInt64);
        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }

    status = xdbMgr->xdbLoadDone(xdbId);
    BailIfFailed(status);

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    verify(ret == 0);

    uint64_t durationInMSec;
    durationInMSec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerMSec;

    xSyslog(moduleName,
            XlogInfo,
            "xdbPgCursorPerfTest: xdb load of %llu kvPairs took %llu msec rate "
            "%Lg "
            "records/msec",
            (unsigned long long) numRowsXdbPgCursorPerfTest,
            (unsigned long long) durationInMSec,
            (long double) numRowsXdbPgCursorPerfTest / durationInMSec);

    xSyslog(moduleName, XlogInfo, "xdbPgCursorPerfTest: Testing xdb sort");

    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    verify(ret == 0);

    status = CursorManager::get()->createOnTable(xdbId,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    xdbCursorCreated = true;

    NewTuplesBuffer *tupBuf;
    while ((status = xdbCursor.xdbPgCursor.getNextTupBuf(&tupBuf)) ==
           StatusOk) {
        // Empty loop body intentional
    }
    if (status != StatusNoData) {
        goto CommonExit;
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    verify(ret == 0);

    durationInMSec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerMSec;

    xSyslog(moduleName,
            XlogInfo,
            "xdbPgCursorPerfTest: xdb sort of %llu kvPairs took %llu msec rate "
            "%Lg "
            "records/msec",
            (unsigned long long) numRowsXdbPgCursorPerfTest,
            (unsigned long long) durationInMSec,
            (long double) numRowsXdbPgCursorPerfTest / durationInMSec);

    CursorManager::get()->destroy(&xdbCursor);
    xdbCursorCreated = false;

    xSyslog(moduleName,
            XlogInfo,
            "xdbPgCursorPerfTest: Testing xdb page iteration");

    status = CursorManager::get()->createOnTable(xdbId,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    xdbCursorCreated = true;

    while ((status = xdbCursor.xdbPgCursor.getNextTupBuf(&tupBuf)) ==
           StatusOk) {
        // Empty loop body intentional
    }
    if (status != StatusNoData) {
        goto CommonExit;
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    verify(ret == 0);

    durationInMSec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerMSec;

    xSyslog(moduleName,
            XlogInfo,
            "xdbPgCursorPerfTest: xdb page iter of %llu kvPairs took %llu msec "
            "rate %Lg records/msec",
            (unsigned long long) numRowsXdbPgCursorPerfTest,
            (unsigned long long) durationInMSec,
            (long double) numRowsXdbPgCursorPerfTest / durationInMSec);

    CursorManager::get()->destroy(&xdbCursor);
    xdbCursorCreated = false;

    xSyslog(moduleName, XlogInfo, "xdbPgCursorPerfTest: Testing xdb scan");

    status = CursorManager::get()->createOnTable(xdbId,
                                                 Ordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &xdbCursor);
    BailIfFailed(status);
    xdbCursorCreated = true;

    while (xdbCursor.getNext(&kvEntry) == StatusOk) {
        // Empty loop body intentional
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    verify(ret == 0);

    durationInMSec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerMSec;

    xSyslog(moduleName,
            XlogInfo,
            "xdbPgCursorPerfTest: xdb scan of %llu kvPairs took %llu msec rate "
            "%Lg "
            "records/msec",
            (unsigned long long) numRowsXdbPgCursorPerfTest,
            (unsigned long long) durationInMSec,
            (long double) numRowsXdbPgCursorPerfTest / durationInMSec);

CommonExit:

    if (xdbCursorCreated) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    if (xdbPerfTableCreated == true) {
        xdbMgr->xdbDrop(xdbId);
    }
    return status;
}

struct ArgsXdbParallelPageInsertsPerThread {
    pthread_t threadId;
    XdbId xdbId;
    uint64_t startVal;
    uint64_t endVal;
    Status retStatus;
};

static void *
xdbParallelPageInsertsPerThread(void *arg)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    ArgsXdbParallelPageInsertsPerThread *testArg =
        (ArgsXdbParallelPageInsertsPerThread *) arg;
    XdbPage *xdbPage = NULL;
    bool keyRangePreset = false;
    DfFieldValue minKey;
    DfFieldValue maxKey;
    bool keyRangeValid = false;
    NewTupleValues valueArray;
    DfFieldValue key;

    // Pretty straightforward call and should not fail
    Xdb *xdb;
    XdbMeta *xdbMeta;
    status = xdbMgr->xdbGet(testArg->xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail. This is because
    // the key type is already set here.
    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfString, 0);

    // Insert keys
    for (uint64_t jj = testArg->startVal; jj <= testArg->endVal; jj++) {
        DfFieldValue field;
        field.uint64Val = jj;
        valueArray.set(0, field, DfUInt64);
        key.uint64Val = jj;
        valueArray.set(1, key, DfUInt64);

    RetryInsert:
        if (xdbPage == NULL) {
            keyRangeValid = false;
            xdbPage = xdbMgr->xdbAllocXdbPage(XidInvalid, xdb);
            if (xdbPage == NULL) {
                status = StatusNoXdbPageBcMem;
                goto CommonExit;
            }

            xdbMgr->xdbInitXdbPage(xdb,
                                   xdbPage,
                                   NULL,
                                   XdbMgr::bcSize(),
                                   XdbUnsortedNormal);

            status =
                xdbMgr->xdbInsertKvIntoXdbPage(xdbPage,
                                               keyRangePreset,
                                               &keyRangeValid,
                                               &minKey,
                                               &maxKey,
                                               &key,
                                               NULL,
                                               &valueArray,
                                               &xdbMeta->kvNamedMeta.kvMeta_,
                                               xdbMeta->keyAttr[0].type);
            if (status != StatusOk) {
                // Row is pretty small to fit in a page.
                xdbMgr->xdbFreeXdbPage(xdbPage);
                goto CommonExit;
            }
        } else {
            verify(keyRangeValid);
            status =
                xdbMgr->xdbInsertKvIntoXdbPage(xdbPage,
                                               keyRangePreset,
                                               &keyRangeValid,
                                               &minKey,
                                               &maxKey,
                                               &key,
                                               NULL,
                                               &valueArray,
                                               &xdbMeta->kvNamedMeta.kvMeta_,
                                               xdbMeta->keyAttr[0].type);
            if (status == StatusOk) {
                continue;
            } else if (status == StatusNoData) {
                status = xdbMgr->xdbInsertOnePage(xdb,
                                                  xdbPage,
                                                  true,
                                                  minKey,
                                                  maxKey);
                verifyOk(status);
                xdbPage = NULL;
                goto RetryInsert;
            } else {
                goto CommonExit;
            }
        }
    }

    if (xdbPage) {
        status = xdbMgr->xdbInsertOnePage(xdb, xdbPage, true, minKey, maxKey);
        verifyOk(status);
        xdbPage = NULL;
    }
    status = StatusOk;

CommonExit:
    testArg->retStatus = status;
    return NULL;
}

static Status
xdbParallelPageInserts(XdbId xdbId, uint64_t numInserts)
{
    Status status = StatusOk;
    ArgsXdbParallelPageInsertsPerThread *args =
        (ArgsXdbParallelPageInsertsPerThread *) memAlloc(
            sizeof(ArgsXdbParallelPageInsertsPerThread) *
            xdbParallelPageInsertsThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbParallelPageInsertsThreads; ++ii) {
        args[ii].xdbId = xdbId;
        args[ii].startVal = (numInserts / xdbParallelPageInsertsThreads) * ii;
        args[ii].endVal = args[ii].startVal +
                          (numInserts / xdbParallelPageInsertsThreads) - 1;
        if (ii == xdbParallelPageInsertsThreads - 1 &&
            numInserts % xdbParallelPageInsertsThreads) {
            args[ii].endVal += numInserts % xdbParallelPageInsertsThreads;
        }
        args[ii].retStatus = StatusOk;
        verifyOk(Runtime::get()
                     ->createBlockableThread(&args[ii].threadId,
                                             NULL,
                                             xdbParallelPageInsertsPerThread,
                                             &args[ii]));
    }

    for (uint64_t ii = 0; ii < xdbParallelPageInsertsThreads; ++ii) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

struct ArgsXdbParallelKvInsertsPerThread {
    pthread_t threadId;
    XdbId xdbId;
    uint64_t startVal;
    uint64_t endVal;
    Status retStatus;
};

static void *
xdbParallelKvInsertsPerThread(void *arg)
{
    Status status = StatusOk;
    ArgsXdbParallelKvInsertsPerThread *testArg =
        (ArgsXdbParallelKvInsertsPerThread *) arg;
    NewTupleValues valueArray;
    DfFieldValue key;

    // Pretty straightforward call and should not fail
    Xdb *xdb;
    XdbMeta *xdbMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    status = xdbMgr->xdbGet(testArg->xdbId, &xdb, &xdbMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    // Pretty straightforward call and should not fail. This is because
    // the key type is already set here.
    status = xdbMgr->xdbSetKeyType(xdbMeta, DfInt64, 0);
    BailIfFailed(status);

    status = xdbMgr->xdbSetKeyType(xdbMeta, DfString, 0);

    // Insert keys
    for (uint64_t jj = testArg->startVal; jj <= testArg->endVal; jj++) {
        DfFieldValue field;
        field.uint64Val = jj;
        valueArray.set(0, field, DfUInt64);
        key.uint64Val = jj;
        valueArray.set(1, key, DfUInt64);

        status = xdbMgr->xdbInsertKv(xdb, &key, &valueArray, XdbInsertCrcHash);
        if (status != StatusOk) {
            // No other failure expected here
            verify(status == StatusNoXdbPageBcMem);
            goto CommonExit;
        }
    }
CommonExit:
    testArg->retStatus = status;
    return NULL;
}

static Status
xdbParallelCursor(XdbId xdbId, uint64_t numIters)
{
    Status status = StatusOk;
    ArgXdbOrderedvsUnorderedTests *args = NULL;
    args = (ArgXdbOrderedvsUnorderedTests *) memAlloc(
        sizeof(ArgXdbOrderedvsUnorderedTests) *
        xdbOrderedVsUnorderedStressThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbParallelCursorThreads; ++ii) {
        args[ii].stressXdbId = xdbId;
        args[ii].numIters = numIters;
        args[ii].retStatus = StatusOk;
        verifyOk(
            Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                  NULL,
                                                  xdbOrderedvsUnorderedTests,
                                                  (void *) &args[ii]));
    }

    for (uint64_t ii = 0; ii < xdbParallelCursorThreads; ++ii) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

static Status
xdbParallelKvInserts(XdbId xdbId, uint64_t numInserts)
{
    Status status = StatusOk;
    ArgsXdbParallelKvInsertsPerThread *args =
        (ArgsXdbParallelKvInsertsPerThread *) memAlloc(
            sizeof(ArgsXdbParallelKvInsertsPerThread) *
            xdbParallelKvInsertsThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbParallelKvInsertsThreads; ++ii) {
        args[ii].xdbId = xdbId;
        args[ii].startVal = (numInserts / xdbParallelKvInsertsThreads) * ii;
        args[ii].endVal =
            args[ii].startVal + (numInserts / xdbParallelKvInsertsThreads) - 1;
        if (ii == xdbParallelKvInsertsThreads - 1 &&
            numInserts % xdbParallelKvInsertsThreads) {
            args[ii].endVal += numInserts % xdbParallelKvInsertsThreads;
        }
        args[ii].retStatus = StatusOk;
        verifyOk(
            Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                  NULL,
                                                  xdbParallelKvInsertsPerThread,
                                                  &args[ii]));
    }

    for (uint64_t ii = 0; ii < xdbParallelKvInsertsThreads; ++ii) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }
CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

struct ArgXdbCreateLoadDropTestPerThread {
    pthread_t threadId;
    Status retStatus;
};

class TestInfo
{
  public:
    XdbId xdbId;
    TableCursor cursor;
    bool cursorValid;
    TestInfo()
    {
        xdbId = XdbInvalidXdbId;
        cursorValid = false;
    }
};

// Exercises Xdb create, setKeyType, load, drop. Also the key inserts here are
// page/entry level.
static void *
xdbCreateLoadDropTestPerThread(void *arg)
{
    ArgXdbCreateLoadDropTestPerThread *argInfo =
        (ArgXdbCreateLoadDropTestPerThread *) arg;
    // Prevent optimizing away for bug 9679
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    RandHandle rndHandle;

    rndInitHandle(&rndHandle, 0);
    TestInfo *testInfo =
        (TestInfo *) memAllocExt(sizeof(TestInfo) *
                                     numXdbsForCreateLoadDropTest,
                                 moduleName);
    if (testInfo == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    for (uint64_t ii = 0; ii < numXdbsForCreateLoadDropTest; ii++) {
        new (&testInfo[ii]) TestInfo();
    }

    for (uint64_t ii = 0; ii < numXdbsForCreateLoadDropTest; ii++) {
        uint64_t numGets = 0;
        Xdb *xdb;
        XdbMeta *xdbMeta;
        testInfo[ii].xdbId = XidMgr::get()->xidGetNext();
        XdbId xdbId = testInfo[ii].xdbId;
        char tableName[16];
        const char *keyName = tableName;
        // @SymbolCheckIgnore
        sprintf(tableName, "foo%lu", ii);

        NewTupleMeta tupMeta;
        NewKeyValueMeta kvMeta;
        size_t numFields = 2;
        tupMeta.setNumFields(numFields);
        tupMeta.setFieldType(DfFatptr, 0);
        tupMeta.setFieldType(DfInt64, 1);
        new (&kvMeta) NewKeyValueMeta(&tupMeta, 1);

        NewKeyValueEntry kvEntry(&kvMeta);

        const char *fatPtrName[1];
        fatPtrName[0] = "p";
        status = xdbMgr->xdbCreate(xdbId,
                                   keyName,
                                   DfInt64,
                                   1,
                                   &tupMeta,
                                   NULL,
                                   0,
                                   &keyName,
                                   1,
                                   fatPtrName,
                                   1,
                                   Ascending,
                                   XdbGlobal,
                                   DhtInvalidDhtId);
        BailIfFailed(status);

        // Pretty straightforward call and should not fail
        status = xdbMgr->xdbGet(xdbId, &xdb, &xdbMeta);
        if (status != StatusOk) {
            assert(false);
            goto CommonExit;
        }

        status = xdbMgr->xdbGet(XdbInvalidXdbId, NULL, NULL);
        if (status != StatusXdbNotFound) {
            assert(false);
            goto CommonExit;
        }

        if (rndGenerate64(&rndHandle) % 2) {
            status =
                xdbParallelKvInserts(xdbId, numRowsPerXdbForCreateLoadDropTest);
            verifyOk(status);
        } else {
            status = xdbParallelPageInserts(xdbId,
                                            numRowsPerXdbForCreateLoadDropTest);
            verifyOk(status);
        }
        BailIfFailed(status);

        status = xdbMgr->xdbLoadDone(xdbId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "xdbLoadDone failed, status %s",
                    strGetFromStatus(status));
            assert(false);
            goto CommonExit;
        }

        status = xdbCheckSerDes(xdbId);
        verify(status == StatusOk);

        // Pretty straightforward call and should not fail
        uint64_t numRows;
        status = xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, &numRows);
        if (status != StatusOk) {
            assert(false);
            goto CommonExit;
        }
        verify(numRows == numRowsPerXdbForCreateLoadDropTest);

        status =
            CursorManager::get()->createOnTable(xdbId,
                                                Ordered,
                                                Cursor::InsertIntoHashTable,
                                                &testInfo[ii].cursor);
        if (status != StatusOk) {
            assert(false);
            goto CommonExit;
        }
        testInfo[ii].cursorValid = true;

        // Fetch each key starting from row 0 (because
        // operatorsCursorCreateLocal() sets xdbCursor to row 0). Since cursor
        // is sorted we expect sequential keys.
        uint64_t jj;
        jj = 0;
        while ((status = testInfo[ii].cursor.getNext(&kvEntry)) == StatusOk) {
            bool retIsValid;
            DfFieldValue key = kvEntry.getKey(&retIsValid);
            verify(retIsValid);
            verify(key.int64Val == (int64_t) jj);

            DfFieldValue field =
                kvEntry.tuple_.get(0,
                                   numFields,
                                   kvEntry.kvMeta_->tupMeta_->getFieldType(0),
                                   &retIsValid);
            verify(retIsValid);
            verify(field.fatptrVal == (uint64_t) jj);
            ++jj;
            ++numGets;
        }
        if (status != StatusNoData) {
            assert(false);
            goto CommonExit;
        }
        verify(numGets == numRowsPerXdbForCreateLoadDropTest);
        status = StatusOk;

        // Do some insane cursoring in parallel.
        status = xdbParallelCursor(xdbId, 1ULL << 15);
        if (status != StatusNoData) {
            goto CommonExit;
        }
    }

CommonExit:
    if (testInfo != NULL) {
        for (uint64_t jj = 0; jj < numXdbsForCreateLoadDropTest; jj++) {
            if (testInfo[jj].cursorValid) {
                CursorManager::get()->destroy(&testInfo[jj].cursor);
            }
            if (testInfo[jj].xdbId != XdbInvalidXdbId) {
                // Test that we can drop XDBs with serialized buckets
                status = xdbCheckSerDes(testInfo[jj].xdbId, true);
                verify(status == StatusOk);
                xdbMgr->xdbDrop(testInfo[jj].xdbId);
            }
        }
        memFree(testInfo);
        testInfo = NULL;
    }

    argInfo->retStatus = status;
    return NULL;
}

Status
xdbCreateLoadDropTest()
{
    Status status = StatusOk;
    ArgXdbCreateLoadDropTestPerThread *args =
        (ArgXdbCreateLoadDropTestPerThread *) memAlloc(
            sizeof(ArgXdbCreateLoadDropTestPerThread) *
            xdbCreateLoadDropTestThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbCreateLoadDropTestThreads; ++ii) {
        verifyOk(Runtime::get()
                     ->createBlockableThread(&args[ii].threadId,
                                             NULL,
                                             xdbCreateLoadDropTestPerThread,
                                             &args[ii]));
    }

    for (uint64_t ii = 0; ii < xdbCreateLoadDropTestThreads; ++ii) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }
CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}
