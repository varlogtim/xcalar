// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//
//
#include <libgen.h>

#include "datapage/DataPage.h"
#include "datapage/DataPageIndex.h"
#include "datapage/MemPool.h"
#include "util/Random.h"
#include "util/Stopwatch.h"
#include "newtupbuf/NewTuplesCursor.h"

#include <google/protobuf/arena.h>
#include <google/protobuf/util/message_differencer.h>
#include <cmath>
#include "common/InitTeardown.h"
#include "test/QA.h"  // Must be last

using namespace google::protobuf;

struct TestField {
    TestField(char *fieldName, TypedDataValue *fieldValue) : name_(fieldName)
    {
        value_ = *fieldValue;
    }
    ~TestField() { memFree(name_); }
    char *name_;
    TypedDataValue value_;
};

class TestRecord
{
  public:
    TestRecord(int fieldCapacity) : fieldCapacity_(fieldCapacity)
    {
        fields_ = (TestField **) memAlloc(fieldCapacity * sizeof(TestField *));
        numFields_ = 0;
    }

    ~TestRecord()
    {
        for (int ii = 0; ii < numFields_; ii++) {
            delete fields_[ii];
        }
        memFree(fields_);
    }

    void addField(TestField *newField)
    {
        fields_[numFields_] = newField;
        ++numFields_;
        assert(numFields_ <= fieldCapacity_);
    }

    int fieldCapacity_;
    int numFields_;
    TestField **fields_;
};

class TestDataset
{
  public:
    struct Stats {
        int64_t totalNumFields = 0;
        int64_t totalDataSize = 0;
        int64_t totalFieldNameSize = 0;
        unsigned long hours = 0;
        unsigned long minutes = 0;
        unsigned long seconds = 0;
        unsigned long millis = 0;
        // These are derived from the above
        float avgFieldSize = 0;
        float avgFieldPerRec = 0;
        float avgRecSize = 0;
    };
    TestDataset() = default;

    ~TestDataset()
    {
        for (int ii = 0; ii < numRecords_; ii++) {
            delete records_[ii];
        }
        memFree(records_);
        records_ = NULL;
    }

    int64_t numRecords_;
    TestRecord **records_;
    Stats stats_;
};

struct TestPage {
    uint8_t *data;
    int32_t numRecs;
};

static Status memPoolTest();
static Status sizeTest();
static Status newKvBufVsDataPageTest();
static Status readWriteTest();
static Status randomRecsTest();
static Status hugeRecsTest();
static Status pageIndexTest();
static Status parallelPageIndexTest();

static TestDataset *genRandomDataset(uint64_t seed,
                                     int maxNumRecords,
                                     int maxNumFields,
                                     bool intsOnly = false);
static TestRecord *genNestedRecord(RandWeakHandle *rng, int numFields);
static TestRecord *genIntRecord(RandWeakHandle *rng, int numFields);
static void genRandomDataValue(RandWeakHandle *rng,
                               int nesting,
                               TypedDataValue *value);
static void genRandomProtoValue(RandWeakHandle *rng,
                                int nesting,
                                ProtoFieldValue *value);

static void writeDatasetToPages(const TestDataset *dataset,
                                int32_t pageSize,
                                ElemBuf<TestPage> *pages);

static void writeDatasetToPagesNewTuple(const TestDataset *dataset,
                                        int32_t pageSize,
                                        ElemBuf<TestPage> *pages);

static void verifySerializedPage(const uint8_t *page,
                                 int32_t pageSize,
                                 int numRecords,
                                 TestRecord **testRecords);

static void verifyRecordsEqual(const TestRecord *testRecord,
                               ReaderRecord *actualRecord);

static TestCase testCases[] = {
    {"MemPool test", memPoolTest, TestCaseEnable, ""},
    {"size test", sizeTest, TestCaseEnable, ""},
    {"Benchmark test", newKvBufVsDataPageTest, TestCaseEnable, ""},
    {"read/write test", readWriteTest, TestCaseEnable, ""},
    {"random record test", randomRecsTest, TestCaseEnable, ""},
    {"huge record test", hugeRecsTest, TestCaseEnable, ""},
    {"page index test", pageIndexTest, TestCaseEnable, ""},
    {"parallel page index test", parallelPageIndexTest, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass);

// Keep this dataset global so it can be reused by multiple tests
static google::protobuf::Arena arena;
static TestDataset *testDatasets[5];

void
genRandomDataValue(RandWeakHandle *rng, int nesting, TypedDataValue *value)
{
    int maxNumArrayVals = 5;
    int maxNumObjectValues = 5;
    int numOptions = nesting == 0 ? 8 : 10;
    int type = rndWeakGenerate32(rng) % numOptions;
    uint64_t val = rndWeakGenerate64(rng);

    // bitcast val to a bunch of different types
    union {
        uint32_t uint32;
        int32_t int32;
        uint64_t uint64;
        int64_t int64;
        float32_t float32;
        float64_t float64;
    } tmp;

    tmp.uint64 = val;

    switch (type) {
    case 0: {
        char buf[100];
        snprintf(buf, sizeof(buf), "String value %lu !!!!!", val);
        ProtoFieldValue *pValue = Arena::CreateMessage<ProtoFieldValue>(&arena);
        // This creates a std::string, which we can then reference immediately
        // We do this to avoid having to allocate and manage strings separately
        // from proto values
        pValue->set_stringval(buf);
        value->setString(pValue->stringval().c_str(),
                         pValue->stringval().length());
        break;
    }
    case 1:
        value->setBool(val % 2);
        break;
    case 2:
        value->setInt32(tmp.int32);
        break;
    case 3:
        value->setInt64(tmp.int64);
        break;
    case 4:
        value->setUInt32(tmp.uint32);
        break;
    case 5:
        value->setUInt64(tmp.uint64);
        break;
    case 6:
        value->setFloat32(tmp.float32);
        break;
    case 7:
        value->setFloat64(tmp.float64);
        break;
    case 8: {
        ProtoFieldValue_ArrayValue *arrayVal =
            Arena::CreateMessage<ProtoFieldValue_ArrayValue>(&arena);
        int arrLength = rndWeakGenerate32(rng) % maxNumArrayVals;
        for (int ii = 0; ii < arrLength; ii++) {
            ProtoFieldValue *arrElm = arrayVal->add_elements();
            genRandomProtoValue(rng, nesting - 1, arrElm);
        }
        ProtoFieldValue *pValue = Arena::CreateMessage<ProtoFieldValue>(&arena);
        pValue->set_allocated_arrayvalue(arrayVal);
        value->setArray(pValue);
        break;
    }
    case 9: {
        ProtoFieldValue_ObjectValue *objectValue =
            Arena::CreateMessage<ProtoFieldValue_ObjectValue>(&arena);
        int numKeys = rndWeakGenerate32(rng) % maxNumObjectValues;
        for (int ii = 0; ii < numKeys; ii++) {
            char buf[50];
            snprintf(buf, sizeof(buf), "Object key %d", ii);
            ProtoFieldValue objVal;
            genRandomProtoValue(rng, nesting - 1, &objVal);
            (*objectValue->mutable_values())[buf] = objVal;
        }
        ProtoFieldValue *pValue = Arena::CreateMessage<ProtoFieldValue>(&arena);
        pValue->set_allocated_objectvalue(objectValue);
        value->setObject(pValue);
        break;
    }
    }
    assert(valueTypeValid(value->type_));
}

void
genRandomProtoValue(RandWeakHandle *rng, int nesting, ProtoFieldValue *value)
{
    int maxNumArrayVals = 5;
    int maxNumObjectValues = 5;
    int numOptions = nesting == 0 ? 9 : 11;
    int type = rndWeakGenerate32(rng) % numOptions;
    uint64_t val = rndWeakGenerate64(rng);

    // bitcast val to a bunch of different types
    union {
        uint32_t uint32;
        int32_t int32;
        uint64_t uint64;
        int64_t int64;
        float32_t float32;
        float64_t float64;
    } tmp;

    tmp.uint64 = val;

    switch (type) {
    case 0: {
        char buf[100];
        snprintf(buf, sizeof(buf), "String value %lu !!!!!", val);
        value->set_stringval(buf);
        break;
    }
    case 1:
        value->set_boolval(val % 2);
        break;
    case 2:
        value->set_uint32val(tmp.uint32);
        break;
    case 3:
        value->set_int32val(tmp.int32);
        break;
    case 4:
        value->set_uint64val(tmp.uint64);
        break;
    case 5:
        value->set_int64val(tmp.int64);
        break;
    case 6:
        value->set_float32val(tmp.float32);
        break;
    case 7:
        value->set_float64val(tmp.float64);
        break;
    case 8:
        value->set_byteval(&val, sizeof(val));
        break;
    case 9: {
        ProtoFieldValue_ArrayValue *arrayVal =
            Arena::CreateMessage<ProtoFieldValue_ArrayValue>(&arena);
        int arrLength = rndWeakGenerate32(rng) % maxNumArrayVals;
        for (int ii = 0; ii < arrLength; ii++) {
            ProtoFieldValue *arrElm = arrayVal->add_elements();
            genRandomProtoValue(rng, nesting - 1, arrElm);
        }
        value->set_allocated_arrayvalue(arrayVal);
        break;
    }
    case 10: {
        ProtoFieldValue_ObjectValue *objectValue =
            Arena::CreateMessage<ProtoFieldValue_ObjectValue>(&arena);
        int numKeys = rndWeakGenerate32(rng) % maxNumObjectValues;
        for (int ii = 0; ii < numKeys; ii++) {
            char buf[50];
            snprintf(buf, sizeof(buf), "Object key %d", ii);
            ProtoFieldValue objVal;
            genRandomProtoValue(rng, nesting - 1, &objVal);
            (*objectValue->mutable_values())[buf] = objVal;
        }
        value->set_allocated_objectvalue(objectValue);
        break;
    }
    }
}

TestRecord *
genIntRecord(RandWeakHandle *rng, int numFields)
{
    TestRecord *rec = new TestRecord(numFields);

    for (int ii = 0; ii < numFields; ii++) {
        TypedDataValue value;
        char *fieldName = static_cast<char *>(memAlloc(50));
        snprintf(fieldName, 50, "field name %i", ii);

        int64_t val = rndWeakGenerate64(rng) % 1000;
        value.setInt64(val);
        TestField *field = new TestField(fieldName, &value);
        rec->addField(field);
    }
    return rec;
}

TestRecord *
genNestedRecord(RandWeakHandle *rng, int numFields)
{
    int maxNesting = 15;
    int percentNested = 0;  // most data isn't nested; lets account for that
    int nesting;
    if ((int) rndWeakGenerate32(rng) % 100 < percentNested) {
        nesting = rndWeakGenerate32(rng) % maxNesting;
    } else {
        nesting = 0;
    }

    TestRecord *rec = new TestRecord(numFields);

    // Shuffle the field orderings
    bool fieldSet[numFields];
    memZero(fieldSet, sizeof(fieldSet));

    for (int ii = 0; ii < numFields; ii++) {
        int fieldNum = rndWeakGenerate32(rng) % numFields;
        if (fieldSet[fieldNum]) {
            continue;
        }
        fieldSet[fieldNum] = true;
        TypedDataValue value;
        char *fieldName = static_cast<char *>(memAlloc(50));
        snprintf(fieldName, 50, "field name %i", fieldNum);
        genRandomDataValue(rng, nesting, &value);
        assert(valueTypeValid(value.type_));
        TestField *field = new TestField(fieldName, &value);
        rec->addField(field);
    }

    return rec;
}

struct GenArgs {
    TestDataset *dataset;
    int startRec;
    int numRecs;
    uint64_t seed;
    int maxNumFields;

    bool intsOnly;
};

void *
genDatasetHelper(void *in)
{
    const GenArgs *args = (const GenArgs *) in;
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, args->seed);

    // Generate the records
    for (int ii = args->startRec; ii < args->startRec + args->numRecs; ii++) {
        int numFields = rndWeakGenerate32(&rng) % args->maxNumFields + 1;
        if (args->intsOnly) {
            numFields = 2;
            args->dataset->records_[ii] = genIntRecord(&rng, numFields);
        } else {
            args->dataset->records_[ii] = genNestedRecord(&rng, numFields);
        }
    }
    return NULL;
}

TestDataset *
genRandomDataset(uint64_t seed,
                 int maxNumRecords,
                 int maxNumFields,
                 bool intsOnly)
{
    TestDataset *dataset = new TestDataset();
    TestDataset::Stats *stats = &dataset->stats_;
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, seed);

    dataset->numRecords_ = 1 + rndWeakGenerate32(&rng) % (maxNumRecords - 1);
    dataset->records_ = static_cast<TestRecord **>(
        memAlloc(sizeof(TestRecord *) * dataset->numRecords_));

    Stopwatch stopwatch;
    long numThreads = (unsigned) XcSysHelper::get()->getNumOnlineCores();
    assert(numThreads > 0);
    pthread_t datasetThreads[numThreads];
    GenArgs threadArgs[numThreads];

    int minRecsPerThread = dataset->numRecords_ / numThreads;
    int numBonusThreads = dataset->numRecords_ % numThreads;

    int startRec = 0;
    for (int ii = 0; ii < numThreads; ii++) {
        GenArgs *args = &threadArgs[ii];
        int size = minRecsPerThread + (ii < numBonusThreads ? 1 : 0);
        args->startRec = startRec;
        args->dataset = dataset;
        args->seed = rndWeakGenerate64(&rng);
        args->numRecs = size;
        args->maxNumFields = maxNumFields;
        args->intsOnly = intsOnly;
        if (ii != 0) {
            // @SymbolCheckIgnore
            int ret = pthread_create(&datasetThreads[ii],
                                     NULL,
                                     genDatasetHelper,
                                     args);
            assert(ret == 0);
        }

        startRec += size;
    }

    genDatasetHelper(&threadArgs[0]);

    for (int ii = 1; ii < numThreads; ii++) {
        // @SymbolCheckIgnore
        verify(pthread_join(datasetThreads[ii], NULL) == 0);
    }
    stopwatch.stop();

    for (int ii = 0; ii < dataset->numRecords_; ii++) {
        TestRecord *srcRecord = dataset->records_[ii];
        dataset->stats_.totalNumFields += srcRecord->numFields_;
        for (int jj = 0; jj < srcRecord->numFields_; jj++) {
            TestField *srcField = srcRecord->fields_[jj];
            dataset->stats_.totalDataSize += srcField->value_.getSize();
            dataset->stats_.totalFieldNameSize += strlen(srcField->name_);
        }
    }
    stopwatch.getPrintableTime(dataset->stats_.hours,
                               dataset->stats_.minutes,
                               dataset->stats_.seconds,
                               dataset->stats_.millis);

    stats->avgFieldSize =
        (float) stats->totalDataSize / (float) stats->totalNumFields;
    stats->avgFieldPerRec =
        (float) stats->totalNumFields / (float) dataset->numRecords_;
    stats->avgRecSize =
        (float) stats->totalDataSize / (float) dataset->numRecords_;

    fprintf(stderr,
            "Generated %li records in %02lu:%02lu.%03lu\n"
            "  Average field size   : %f bytes\n"
            "  Average fields/record: %f fields\n"
            "  Average record size  : %f bytes\n",
            dataset->numRecords_,
            stats->minutes,
            stats->seconds,
            stats->millis,
            stats->avgFieldSize,
            stats->avgFieldPerRec,
            stats->avgRecSize);
    return dataset;
}

void
writeDatasetToPages(const TestDataset *dataset,
                    int32_t pageSize,
                    ElemBuf<TestPage> *pages)
{
    const TestDataset::Stats *stats = &dataset->stats_;
    DataPageWriter writer;
    verifyOk(writer.init(pageSize));

    assert(pages->bufSize == 0);
    assert(pages->bufCapacity > 0);

    Stopwatch stopwatch;
    ++pages->bufSize;
    TestPage *curPage = &pages->buf[0];
    int recordNum = 0;
    int pageRecords = 0;
    while (recordNum < dataset->numRecords_) {
        curPage = &pages->buf[pages->bufSize - 1];
        TestRecord *srcRecord = dataset->records_[recordNum];
        DataPageWriter::Record *writeRecord;
        verifyOk(writer.newRecord(&writeRecord));

        for (int jj = 0; jj < srcRecord->numFields_; jj++) {
            TestField *srcField = srcRecord->fields_[jj];
            verifyOk(writeRecord->addFieldByName(srcField->name_,
                                                 &srcField->value_));
        }
        DataPageWriter::PageStatus pageStatus;
        Status status = writer.commit(writeRecord, &pageStatus, NULL);
        assert(status == StatusOk);

        if (pageStatus == DataPageWriter::PageStatus::Full) {
            assert(recordNum > 0);
            assert(pageRecords > 0 && "page is too small for records");
            curPage->data =
                static_cast<uint8_t *>(memAllocAligned(pageSize, pageSize));
            curPage->numRecs = pageRecords;
            writer.serializeToPage(curPage->data);

            verifyOk(pages->growBuffer(1));
            ++pages->bufSize;

            writer.clear();
            pageRecords = 0;
        } else {
            ++recordNum;
            ++pageRecords;
        }
    }
    // Note that the 'retry' scheme means there will always be records leftover,
    // unless there were no records to begin with
    assert(pageRecords);
    if (pageRecords > 0) {
        curPage->data =
            static_cast<uint8_t *>(memAllocAligned(pageSize, pageSize));
        curPage->numRecs = pageRecords;
        writer.serializeToPage(curPage->data);
    }

    stopwatch.stop();
    unsigned long hoursW, minutesW, secondsW, millisW;
    stopwatch.getPrintableTime(hoursW, minutesW, secondsW, millisW);
    float dataPerPage = stats->totalDataSize / (float) pages->bufSize;

    // Cursor here.
    Stopwatch stopwatchReads;
    DataPageReader reader;
    for (int32_t ii = 0; ii < pages->bufSize; ii++) {
        curPage = &pages->buf[ii];
        reader.init(curPage->data, pageSize);
        DataPageReader::RecordIterator iter(&reader);
        ReaderRecord readRec;
        while (iter.getNext(&readRec)) {
            // NOOP
        }
    }
    stopwatchReads.stop();
    unsigned long hoursR, minutesR, secondsR, millisR;
    stopwatchReads.getPrintableTime(hoursR, minutesR, secondsR, millisR);

    fprintf(stderr,
            "Fit %li records into %li pages in %02lu:%02lu.%03lu"
            " and cursor in %02lu:%02lu.%03lu\n"
            "  Records / page         : %f records\n"
            "  Serialized record size : %f bytes\n"
            "  Raw Data / page        : %f bytes\n"
            "  Packing overhead(includes field names) : %f%%\n",
            dataset->numRecords_,
            pages->bufSize,
            minutesW,
            secondsW,
            millisW,
            minutesR,
            secondsR,
            millisR,
            (float) dataset->numRecords_ / (float) pages->bufSize,
            (float) pages->bufSize * (float) pageSize /
                (float) dataset->numRecords_,
            dataPerPage,
            100 * ((float) pageSize - dataPerPage) / (float) pageSize);
}

void
writeDatasetToPagesNewTuple(const TestDataset *dataset,
                            int32_t pageSize,
                            ElemBuf<TestPage> *pages)
{
    Status status;
    const TestDataset::Stats *stats = &dataset->stats_;

    assert(pages->bufSize == 0);
    assert(pages->bufCapacity > 0);

    NewTupleMeta tupleMeta;
    size_t numFields = 2;
    tupleMeta.setNumFields(numFields);
    for (size_t ii = 0; ii < numFields; ii++) {
        tupleMeta.setFieldType(DfInt64, ii);
    }
    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);

    Stopwatch stopwatch;
    ++pages->bufSize;
    TestPage *curPage = &pages->buf[0];
    curPage->data = static_cast<uint8_t *>(memAllocAligned(pageSize, pageSize));
    int recordNum = 0;
    int pageRecords = 0;
    NewTupleValues tupleValues;
    DfFieldValue valueTmp;

    // Init first page
    new (curPage->data) NewTuplesBuffer((void *) curPage->data, pageSize);

    while (recordNum < dataset->numRecords_) {
        TestRecord *srcRecord = dataset->records_[recordNum];

        for (int jj = 0; jj < srcRecord->numFields_; jj++) {
            TestField *srcField = srcRecord->fields_[jj];

            // Always assume int64_t
            valueTmp.int64Val = srcField->value_.value_.int64_;

#ifdef UNDEF
            // XXX TODO Need to add other types
            valueTmp.stringVal.strActual =
                srcField->value_->stringval().c_str();
            valueTmp.stringVal.strSize =
                srcField->value_->stringval().size() + 1;  // XXX maybe?
#endif                                                     // UNDEF
            tupleValues.set(jj, valueTmp, DfInt64);
        }

        do {
            NewTuplesBuffer *fTbuf = (NewTuplesBuffer *) curPage->data;
            status = fTbuf->append(&tupleMeta, &tupleValues);
            if (status == StatusNoData) {
                assert(recordNum > 0);
                assert(pageRecords > 0 && "page is too small for records");
                curPage->numRecs = pageRecords;

                // init next page
                verifyOk(pages->growBuffer(1));
                ++pages->bufSize;
                pageRecords = 0;
                curPage = &pages->buf[pages->bufSize - 1];

                curPage->data =
                    static_cast<uint8_t *>(memAllocAligned(pageSize, pageSize));
                new (curPage->data)
                    NewTuplesBuffer((void *) curPage->data, pageSize);
                continue;
            } else {
                ++recordNum;
                ++pageRecords;
            }
            break;
        } while (true);
    }

    stopwatch.stop();
    unsigned long hoursW, minutesW, secondsW, millisW;
    stopwatch.getPrintableTime(hoursW, minutesW, secondsW, millisW);
    float dataPerPage = stats->totalDataSize / (float) pages->bufSize;

    // Cursor here.
    int64_t sum = 0;
    Stopwatch stopwatchReads;
    for (int32_t ii = 0; ii < pages->bufSize; ii++) {
        curPage = &pages->buf[ii];
        NewTuplesBuffer *fTbuf = (NewTuplesBuffer *) curPage->data;
        NewTuplesCursor fCursor(fTbuf);
        NewTupleValues fTuplePtr;
        DfFieldValue value, value2;

        do {
            status = fCursor.getNext(&tupleMeta, &fTuplePtr);
            if (status == StatusOk) {
                bool retIsValid;
                // Force memory read here for all the fields
                value = fTuplePtr.get(0, numFields, DfInt64, &retIsValid);
                assert(retIsValid);

                value2 = fTuplePtr.get(1, numFields, DfInt64, &retIsValid);
                assert(retIsValid);
                sum += value.int64Val + value2.int64Val;
            }
        } while (status == StatusOk);
    }
    stopwatchReads.stop();
    unsigned long hoursR, minutesR, secondsR, millisR;
    stopwatchReads.getPrintableTime(hoursR, minutesR, secondsR, millisR);
    verify(sum);  // prevents compiler from optimizing this out.

    fprintf(stderr,
            "Fit %li records into %li pages in %02lu:%02lu.%03lu"
            " and cursor in %02lu:%02lu.%03lu\n"
            "  Records / page         : %f records\n"
            "  Serialized record size : %f bytes\n"
            "  Raw Data / page        : %f bytes\n"
            "  Packing overhead(includes field names) : %f%%\n",
            dataset->numRecords_,
            pages->bufSize,
            minutesW,
            secondsW,
            millisW,
            minutesR,
            secondsR,
            millisR,
            (float) dataset->numRecords_ / (float) pages->bufSize,
            (float) pages->bufSize * (float) pageSize /
                (float) dataset->numRecords_,
            dataPerPage,
            100 * ((float) pageSize - dataPerPage) / (float) pageSize);
}

Status
memPoolTest()
{
    MallocAllocator alloc;
    MemPool mpool;
    alloc.init(1023);
    mpool.init(&alloc);
    int32_t numAllocations = 100000;

    char **storedStrs = (char **) memAlloc(sizeof(char *) * numAllocations);
    assert(storedStrs);

    // Perform the test twice to test the 'cached pages' code path
    for (int testIter = 0; testIter < 2; testIter++) {
        // Allocate a bunch of strings
        for (int ii = 0; ii < numAllocations; ii++) {
            int strLen = snprintf(NULL, 0, "%i", ii);
            char *str = (char *) mpool.alloc(strLen + 1);
            assert(str);
            snprintf(str, strLen + 1, "%i", ii);
            storedStrs[ii] = str;
        }

        // Verify the strings we allocated above
        for (int ii = 0; ii < numAllocations; ii++) {
            char tmp[20];
            char *str = storedStrs[ii];
            snprintf(tmp, sizeof(tmp), "%i", ii);
            assert(strcmp(str, tmp) == 0);
        }

        mpool.resetPool();
    }

    memFree(storedStrs);
    storedStrs = NULL;

    return StatusOk;
}

Status
sizeTest()
{
    struct TestVal {
        ProtoFieldValue val;
        const char *strVal = "";
    };

    TestVal testVals[11];
    testVals[0].val.set_boolval(true);
    testVals[0].strVal = "true";

    testVals[1].val.set_boolval(false);
    testVals[1].strVal = "false";

    testVals[2].val.set_stringval("hello");
    testVals[2].strVal = "\"hello\"";

    testVals[3].val.set_int32val(0);
    testVals[3].strVal = "0";

    testVals[4].val.set_int32val(0x12341234);
    testVals[4].strVal = "0x12341234)";

    testVals[5].val.set_int64val(0);
    testVals[5].strVal = "0";

    testVals[6].val.set_int64val(0x1234123412341234);
    testVals[6].strVal = "0x1234123412341234";

    testVals[7].val.set_float32val(0.0);
    testVals[7].strVal = "0.0";

    testVals[8].val.set_float32val(9999999.12341234);
    testVals[8].strVal = "9999999.12341234";

    testVals[9].val.set_float64val(9999999.12341234);
    testVals[9].strVal = "9999999.12341234";

    testVals[10].val.set_byteval("hel\0lo", 6);
    testVals[10].strVal = "\"hel\\0lo\"";

    for (unsigned long ii = 0; ii < ArrayLen(testVals); ii++) {
        TestVal *tv = &testVals[ii];
        auto desc = tv->val.descriptor();
        int fieldIndex = tv->val.dataValue_case() - 1;
        fprintf(stderr,
                "%s(%s) has size %ld\n",
                desc->oneof_decl(0)->field(fieldIndex)->full_name().c_str(),
                tv->strVal,
                tv->val.ByteSizeLong());
    }
    return StatusOk;
}

Status
readWriteTest()
{
    Status status;
    const char *fieldName = "testField";
    int64_t testVal = 1234567;
    uint8_t buf[100];

    // First we write the record out
    DataPageWriter writer;
    verifyOk(writer.init(sizeof(buf)));

    DataPageWriter::PageStatus pageStatus = DataPageWriter::PageStatus::NotFull;
    int numRecords = 0;
    while (pageStatus == DataPageWriter::PageStatus::NotFull) {
        ProtoFieldValue protoWriteVal;
        TypedDataValue value;
        DataPageWriter::Record *writeRecord;
        verifyOk(writer.newRecord(&writeRecord));

        value.setInt64(testVal * numRecords);
        verifyOk(writeRecord->addFieldByName(fieldName, &value));

        status = writer.commit(writeRecord, &pageStatus, NULL);
        assert(status == StatusOk);
        if (pageStatus == DataPageWriter::PageStatus::NotFull) {
            ++numRecords;
        }
    }
    assert(numRecords == 16);

    writer.serializeToPage(buf);

    // Now we read the record back
    DataPageReader reader;
    reader.init(buf, sizeof(buf));
    assert(reader.validatePage());
    assert(reader.getNumRecords() == numRecords);

    for (int ii = 0; ii < numRecords; ii++) {
        ReaderRecord readRecord;
        ProtoFieldValue *protoReadVal =
            Arena::CreateMessage<ProtoFieldValue>(&arena);
        DataValueReader dValue;
        reader.getRecord(0, ii, &readRecord);
        assert(readRecord.getNumFields() == 1);

        verifyOk(readRecord.getFieldByName(fieldName, &dValue, NULL));
        verifyOk(dValue.getAsProto(protoReadVal));

        assert(protoReadVal->int64val() == testVal * ii);
    }
    return StatusOk;
}

void
verifySerializedPage(const uint8_t *page,
                     int32_t pageSize,
                     int numRecords,
                     TestRecord **testRecords)
{
    DataPageReader reader;
    reader.init(page, pageSize);

    verify(reader.validatePage());
    assert(reader.getNumRecords() == numRecords);

    DataPageReader::RecordIterator iter(&reader);
    ReaderRecord readRec;
    int ii = 0;
    while (iter.getNext(&readRec)) {
        const TestRecord *testRecord = testRecords[ii];
        verifyRecordsEqual(testRecord, &readRec);

        ++ii;
    }
    assert(ii == numRecords);
}

void
verifyProtosEqual(const ProtoFieldValue *v1, const ProtoFieldValue *v2)
{
    assert(v1->dataValue_case() == v2->dataValue_case());
    switch (v1->dataValue_case()) {
    case ProtoFieldValue::kStringVal:
        assert(strcmp(v1->stringval().c_str(), v2->stringval().c_str()) == 0);
        assert(v1->stringval().length() == v2->stringval().length());
        break;
    case ProtoFieldValue::kBoolVal:
        assert(v1->boolval() == v2->boolval());
        break;
    case ProtoFieldValue::kUint32Val:
        assert(v1->uint32val() == v2->uint32val());
        break;
    case ProtoFieldValue::kInt32Val:
        assert(v1->int32val() == v2->int32val());
        break;
    case ProtoFieldValue::kUint64Val:
        assert(v1->uint64val() == v2->uint64val());
        break;
    case ProtoFieldValue::kInt64Val:
        assert(v1->int64val() == v2->int64val());
        break;
    case ProtoFieldValue::kFloat32Val: {
        auto f1 = v1->float32val();
        auto f2 = v2->float32val();
        assert(((::isnanf(f1) && ::isnanf(f2)) || f1 == f2));
        break;
    }
    case ProtoFieldValue::kFloat64Val: {
        auto f1 = v1->float64val();
        auto f2 = v2->float64val();
        assert(((::isnanl(f1) && ::isnanl(f2)) || f1 == f2));
        break;
    }
    case ProtoFieldValue::kByteVal: {
        assert(v1->byteval().length() == v2->byteval().length());
        assert(memcmp(v1->byteval().c_str(),
                      v2->byteval().c_str(),
                      v1->byteval().length()) == 0);
        break;
    }
    case ProtoFieldValue::kArrayValue:
        assert(v1->arrayvalue().elements_size() ==
               v2->arrayvalue().elements_size());
        for (int ii = 0; ii < v1->arrayvalue().elements_size(); ii++) {
            verifyProtosEqual(&v1->arrayvalue().elements(ii),
                              &v2->arrayvalue().elements(ii));
        }
        break;
    case ProtoFieldValue::kObjectValue: {
        assert(v1->objectvalue().values_size() ==
               v2->objectvalue().values_size());

        auto iter1 = v1->objectvalue().values().begin();
        for (; iter1 != v1->objectvalue().values().end(); iter1++) {
            auto value2 = v2->objectvalue().values().at(iter1->first);
            verifyProtosEqual(&iter1->second, &value2);
        }
        break;
    }
    case ProtoFieldValue::DATAVALUE_NOT_SET:
        break;
    default:
        assert(false);
    }
}

void
verifyFieldsEqual(const TypedDataValue *v1, const TypedDataValue *v2)
{
    assert(v1->type_ == v2->type_);
    switch (v1->type_) {
    case ValueType::String:
        assert(v1->value_.string_.length == v2->value_.string_.length);
        assert(strncmp(v1->value_.string_.str,
                       v2->value_.string_.str,
                       v1->value_.string_.length) == 0);
        break;
    case ValueType::Boolean:
        assert(v1->value_.bool_ == v2->value_.bool_);
        break;
    case ValueType::Int32:
        assert(v1->value_.int32_ == v2->value_.int32_);
        break;
    case ValueType::Int64:
        assert(v1->value_.int64_ == v2->value_.int64_);
        break;
    case ValueType::UInt32:
        assert(v1->value_.uint32_ == v2->value_.uint32_);
        break;
    case ValueType::UInt64:
        assert(v1->value_.uint64_ == v2->value_.uint64_);
        break;
    case ValueType::Float32: {
        auto f1 = v1->value_.float32_;
        auto f2 = v2->value_.float32_;
        assert(((::isnanf(f1) && ::isnanf(f2)) || f1 == f2));
        break;
    }
    case ValueType::Float64: {
        auto f1 = v1->value_.float64_;
        auto f2 = v2->value_.float64_;
        assert(((::isnanl(f1) && ::isnanl(f2)) || f1 == f2));
        break;
    }
    case ValueType::Array:
        assert(v1->value_.proto_->arrayvalue().elements_size() ==
               v2->value_.proto_->arrayvalue().elements_size());
        for (int ii = 0; ii < v1->value_.proto_->arrayvalue().elements_size();
             ii++) {
            verifyProtosEqual(&v1->value_.proto_->arrayvalue().elements(ii),
                              &v2->value_.proto_->arrayvalue().elements(ii));
        }
        break;
    case ValueType::Object: {
        assert(v1->value_.proto_->objectvalue().values_size() ==
               v2->value_.proto_->objectvalue().values_size());
        auto iter1 = v1->value_.proto_->objectvalue().values().begin();
        for (; iter1 != v1->value_.proto_->objectvalue().values().end();
             iter1++) {
            auto value2 =
                v2->value_.proto_->objectvalue().values().at(iter1->first);
            verifyProtosEqual(&iter1->second, &value2);
        }
        break;
    }
    case ValueType::Null:
        break;
    default:
        assert(false);
    }
}

void
verifyRecordsEqual(const TestRecord *testRecord, ReaderRecord *actualRecord)
{
    TypedDataValue actualValue;

    assert(actualRecord->getNumFields() == testRecord->numFields_);
    for (int jj = 0; jj < testRecord->numFields_; jj++) {
        const TestField *testField = testRecord->fields_[jj];
        int32_t fieldIndex;
        int expectedValSize = testField->value_.getSize();
        DataValueReader dValue;

        // Check by name
        {
            verifyOk(actualRecord->getFieldByName(testField->name_,
                                                  &dValue,
                                                  &fieldIndex));
            verifyOk(dValue.getAsTypedDataValue(&actualValue));
            assert(actualValue.getSize() == expectedValSize);
            verifyFieldsEqual(&actualValue, &testField->value_);
        }

        // Check by index
        {
            const char *fieldName;
            verifyOk(
                actualRecord->getFieldByIdx(fieldIndex, &fieldName, &dValue));
            verifyOk(dValue.getAsTypedDataValue(&actualValue));
            assert(actualValue.getSize() == expectedValSize);
            assert(strcmp(fieldName, testField->name_) == 0);

            verifyFieldsEqual(&actualValue, &testField->value_);
        }
    }
}

Status
randomRecsTest()
{
    TestDataset *testDataset = testDatasets[0];
    int pageSize = 16 * KB;

    ElemBuf<TestPage> pages;
    verifyOk(pages.init(5));
    writeDatasetToPages(testDataset, pageSize, &pages);

    int recordNum = 0;
    for (int ii = 0; ii < pages.bufSize; ii++) {
        TestPage *tp = &pages.buf[ii];
        verifySerializedPage(tp->data,
                             pageSize,
                             tp->numRecs,
                             testDataset->records_ + recordNum);
        recordNum += tp->numRecs;
    }

    for (int ii = 0; ii < pages.bufSize; ii++) {
        TestPage *tp = &pages.buf[ii];
        memAlignedFree(tp->data);
    }

    return StatusOk;
}

Status
benchmarkNewKvBufVsDatapage(const TestDataset *dataset)
{
    int pageSize = 16 * KB;

    fprintf(stderr, " * * * * DATAPAGE * * * *\n");
    {
        ElemBuf<TestPage> pages;
        verifyOk(pages.init(5));
        writeDatasetToPages(dataset, pageSize, &pages);

        for (int ii = 0; ii < pages.bufSize; ii++) {
            TestPage *tp = &pages.buf[ii];
            memAlignedFree(tp->data);
        }
    }

    fprintf(stderr, " * * * * NEW TUPLE * * * *\n");
    {
        ElemBuf<TestPage> pages;
        verifyOk(pages.init(5));
        writeDatasetToPagesNewTuple(dataset, pageSize, &pages);

        for (int ii = 0; ii < pages.bufSize; ii++) {
            TestPage *tp = &pages.buf[ii];
            memAlignedFree(tp->data);
        }
    }

    return StatusOk;
}

// XXX TODO
// - Pages which are packed and cursored needs to be backed by XDB pages. Since
// mlock() of XDB pages kick in the first time it's allocated, we need some init
// code to prime the XDB pages before beginning benchmarking.
// - Add variable length objects like Strings, Scalar objects here.
Status
newKvBufVsDataPageTest()
{
    uint64_t seed = 0xbadc0ffee;
    int maxNumRecords = 16 * MB;
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, seed);

    seed = rndWeakGenerate64(&rng);
    TestDataset *testDataset = genRandomDataset(seed, maxNumRecords, 2, true);
    assert(testDataset != NULL);

    verifyOk(benchmarkNewKvBufVsDatapage(testDataset));

    delete testDataset;

    return StatusOk;
}

Status
hugeRecsTest()
{
    uint64_t seed = 0xbadc0ffee;
    int numFields = 1000;
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, seed);
    char buf[1000];
    TestRecord *hugeRec = genNestedRecord(&rng, numFields);

    DataPageWriter writer;
    verifyOk(writer.init(sizeof(buf)));

    DataPageWriter::Record *writeRecord;
    verifyOk(writer.newRecord(&writeRecord));

    for (int ii = 0; ii < hugeRec->numFields_; ii++) {
        TestField *srcField = hugeRec->fields_[ii];
        verifyOk(
            writeRecord->addFieldByName(srcField->name_, &srcField->value_));
    }
    DataPageWriter::PageStatus pageStatus;
    Status status = writer.commit(writeRecord, &pageStatus, NULL);
    assert(status == StatusOk);
    assert(pageStatus == DataPageWriter::PageStatus::Full);
    delete hugeRec;
    return StatusOk;
}

static void
freeWrapper(void *p)
{
    memAlignedFree(p);
}

//
// Data Page Index Test
// ---
// This tests along 2 dimensions: page sizes and datasets
// In order to optimize testing / sec, this is multithreaded, but each
// individual test case is single threaded. Each thread runs test cases
// untili there are no more left to run
//

static int pageSizes[] = {
    8 * KB,
    16 * KB,
    32 * KB,
    64 * KB,
    128 * KB,
    256 * KB,
};

static int NumIndexTestCases = ArrayLen(testDatasets) * ArrayLen(pageSizes);
static Atomic32 curTestCase;

// This performs all of the test cases for the index test. It uses the Atomic
// above to schedule test cases onto the threads.
void *
pageIndexTestHelper(void *p)
{
    int testCase = atomicInc32(&curTestCase) - 1;
    while (testCase < NumIndexTestCases) {
        int datasetIndex = testCase / ArrayLen(pageSizes);
        int pageSizeIndex = testCase % ArrayLen(pageSizes);

        int pageSize = pageSizes[pageSizeIndex];
        TestDataset *testDataset = testDatasets[datasetIndex];

        // Skip the big record dataset if pages aren't big enough
        if (datasetIndex == 1 && pageSizeIndex < 2) {
            testCase = atomicInc32(&curTestCase) - 1;
            continue;
        }
        ElemBuf<TestPage> pages;
        verifyOk(pages.init(5));
        writeDatasetToPages(testDataset, pageSize, &pages);

        DataPageIndex index;

        verifyOk(index.init(pageSize, freeWrapper));

        for (int pageNum = 0; pageNum < pages.bufSize; pageNum++) {
            TestPage *tp = &pages.buf[pageNum];
            DataPageIndex::RangeReservation res;
            verifyOk(index.reserveRecordRange(tp->data, &res));
            index.addPage(tp->data, &res);
        }
        assert(testDataset->numRecords_ == index.getNumRecords());
        verifyOk(index.finalize());

        ReaderRecord actualRecord;
        int curPage = 0;
        int curPageRec = 0;
        for (int rec = 0; rec < testDataset->numRecords_; rec++) {
            const TestRecord *testRecord = testDataset->records_[rec];
            index.getRecordByNum(rec, &actualRecord);

            // Verify that the index gave us the same
            // as using a reader on the page directly
            DataPageReader reader;
            ReaderRecord actualRecord2;
            reader.init(pages.buf[curPage].data, pageSize);
            reader.getRecord(0, curPageRec, &actualRecord2);
            assert(actualRecord.getOffset() == actualRecord2.getOffset());
            assert(actualRecord.getNumFields() == actualRecord2.getNumFields());

            verifyRecordsEqual(testRecord, &actualRecord);
            if (pages.buf[curPage].numRecs - 1 == curPageRec) {
                ++curPage;
                curPageRec = 0;
            } else {
                ++curPageRec;
            }
        }

        // Do a second pass, timing this one
        Stopwatch stopwatch;
        for (int rec = 0; rec < testDataset->numRecords_; rec++) {
            index.getRecordByNum(rec, &actualRecord);
        }
        stopwatch.stop();

        unsigned long hours, minutes, seconds, millis;
        float duration;
        stopwatch.getPrintableTime(hours, minutes, seconds, millis);
        duration =
            (float) (stopwatch.getElapsedNSecs() / NSecsPerMSec) / 1000.0;

        fprintf(stderr,
                "Read %li records from index on dataset %i\n"
                "  Total duration: %02lu:%02lu.%03lu\n"
                "  Records / sec : %f Mrec/sec\n"
                "  Index overhead: %ld bytes\n",
                testDataset->numRecords_,
                datasetIndex,
                minutes,
                seconds,
                millis,
                (float) testDataset->numRecords_ / duration / 1000000.0,
                index.getIndexOverhead());

        // We're done with this test case; move to the next test case
        testCase = atomicInc32(&curTestCase) - 1;
    }
    return NULL;
}

Status
pageIndexTest()
{
    atomicWrite32(&curTestCase, 0);

    long numThreads = (unsigned) XcSysHelper::get()->getNumOnlineCores();
    assert(numThreads > 0);
    pthread_t workers[numThreads];

    // Spawn threads to run all of the testcases
    for (int ii = 0; ii < numThreads; ii++) {
        if (ii != 0) {
            // @SymbolCheckIgnore
            int ret =
                pthread_create(&workers[ii], NULL, pageIndexTestHelper, NULL);
            assert(ret == 0);
        }
    }
    pageIndexTestHelper(NULL);

    for (int ii = 1; ii < numThreads; ii++) {
        // @SymbolCheckIgnore
        verify(pthread_join(workers[ii], NULL) == 0);
    }

    return StatusOk;
}

struct ParallelTestArgs {
    TestDataset *testDataset;
    DataPageIndex *index;
    uint64_t seed;
    int32_t numRecChecks;
};

void *
parallelPageTestHelper(void *p)
{
    const ParallelTestArgs *args = (const ParallelTestArgs *) p;
    uint64_t seed = args->seed;
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, seed);

    ReaderRecord actualRecord;
    for (int ii = 0; ii < args->numRecChecks; ii++) {
        int recNum = rndWeakGenerate32(&rng) % args->testDataset->numRecords_;
        const TestRecord *testRecord = args->testDataset->records_[recNum];
        args->index->getRecordByNum(recNum, &actualRecord);
        verifyRecordsEqual(testRecord, &actualRecord);
    }

    return NULL;
}

Status
parallelPageIndexTest()
{
    TestDataset *testDataset = testDatasets[0];
    int pageSize = 16 * KB;
    uint64_t seed = 0x24681357;
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, seed);

    ElemBuf<TestPage> pages;
    verifyOk(pages.init(5));
    writeDatasetToPages(testDataset, pageSize, &pages);

    DataPageIndex index;

    verifyOk(index.init(pageSize, freeWrapper));

    for (int pageNum = 0; pageNum < pages.bufSize; pageNum++) {
        TestPage *tp = &pages.buf[pageNum];
        DataPageIndex::RangeReservation res;
        verifyOk(index.reserveRecordRange(tp->data, &res));
        index.addPage(tp->data, &res);
    }
    verifyOk(index.finalize());
    assert(testDataset->numRecords_ == index.getNumRecords());

    long numThreads = (unsigned) XcSysHelper::get()->getNumOnlineCores();
    assert(numThreads > 0);
    pthread_t workers[numThreads];
    ParallelTestArgs args[numThreads];

    // Spawn threads to run all of the testcases
    for (int ii = 0; ii < numThreads; ii++) {
        args[ii].testDataset = testDataset;
        args[ii].index = &index;
        args[ii].seed = rndWeakGenerate32(&rng);
        args[ii].numRecChecks = testDataset->numRecords_ / 100;
        if (ii != 0) {
            // @SymbolCheckIgnore
            int ret = pthread_create(&workers[ii],
                                     NULL,
                                     parallelPageTestHelper,
                                     &args[ii]);
            assert(ret == 0);
        }
    }
    pageIndexTestHelper((void *) &args[0]);

    for (int ii = 1; ii < numThreads; ii++) {
        // @SymbolCheckIgnore
        verify(pthread_join(workers[ii], NULL) == 0);
    }

    return StatusOk;
}

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);
    Status status = StatusOk;

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    //
    // Generate test datasets
    //
    RandWeakHandle rng;
    rndInitWeakHandle(&rng, 0xdeadb335);

    // Dataset of small records
    uint64_t seed = rndWeakGenerate64(&rng);
    testDatasets[0] = genRandomDataset(seed, 10000, 1);

    // Dataset of large records
    seed = rndWeakGenerate64(&rng);
    testDatasets[1] = genRandomDataset(seed, 100, 500);

    // The rest can be random
    for (int ii = 2; ii < (int) ArrayLen(testDatasets); ii++) {
        TestDataset *testDataset;
        seed = rndWeakGenerate64(&rng);
        int maxNumRecords = 100000;
        int maxNumFields = 100;
        testDataset = genRandomDataset(seed, maxNumRecords, maxNumFields);
        testDatasets[ii] = testDataset;
    }

    //
    // Run the actual tests
    //

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    //
    // Clean up our generated test datasets
    //
    for (int ii = 0; ii < (int) ArrayLen(testDatasets); ii++) {
        TestDataset *testDataset = testDatasets[ii];
        delete testDataset;
        testDatasets[ii] = NULL;
    }

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
