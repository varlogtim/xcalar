// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <string.h>

#include "StrlFunc.h"
#include "datapage/DataPage.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "util/RemoteBitmap.h"
#include "xdb/Xdb.h"

constexpr char DataPageFieldMeta::BannedChars[];

/* ***************************** DataPageWriter ***************************** */

Status
DataPageWriter::init(int32_t pageCapacity)
{
    Status status = StatusOk;
    pageCapacity_ = pageCapacity;

    status = recordBuf_.init(InitialNumRecords);
    BailIfFailed(status);

    status = fieldMetaIndex_.init(InitialNumFields);
    BailIfFailed(status);

    memAllocator_.init(XdbMgr::bcSize());
    memPool_.init(&memAllocator_);

    // 1 for numFields, 1 for numRecords
    pageSize_ = sizeof(int32_t) + sizeof(int32_t);
CommonExit:
    return status;
}

DataPageWriter::~DataPageWriter()
{
    clear();
}

void
DataPageWriter::clear()
{
    discoveredFields_.removeAll(&DataPageFieldMeta::del);
    fieldMetaIndex_.bufSize = 0;

    for (int ii = 0; ii < recordBuf_.bufSize; ii++) {
        Record *record = &recordBuf_.buf[ii];
        record->clear();
    }
    numRecords_ = 0;
    recordBuf_.bufSize = 0;
    // 1 for numFields, 1 for numRecords
    pageSize_ = sizeof(int32_t) + sizeof(int32_t);

    memPool_.resetPool();
}

Status
DataPageWriter::newRecord(Record **record)
{
    Status status = StatusOk;

    // Now let's make sure our record buffer has space for this one
    status = recordBuf_.growBuffer(1);
    BailIfFailed(status);

    *record = &recordBuf_.buf[recordBuf_.bufSize];
    assert(recordBuf_.bufSize < recordBuf_.bufCapacity);
    ++recordBuf_.bufSize;

    if ((*record)->writer_ == NULL) {
        status = (*record)->init(this);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

// This adds 'record' to internal metadata such that when serializePage is
// called this record will be included.
// This record already has all of its field values serialized.
//
// We store a complete list of a page's field names in one location in a
// specific but arbitrary order. Within a serialized record, a bitmap determines
// which fields are present; the field values are then stored directly.
//
// The above schema relies upon a page-wide consistent ordering of fields.
Status
DataPageWriter::commit(Record *record,
                       PageStatus *pageStatus,
                       int32_t *bytesOverflow)
{
    Status status = StatusOk;
    int32_t incrementalSpace = 0;
    int numNewFields = 0;
    assert(pageStatus != NULL);
    *pageStatus = PageStatus::NotFull;

    if (bytesOverflow != NULL) {
        *bytesOverflow = 0;
    }

    // First we need to check if this record is going to fit on the page.
    // We can determine this by checking this record's serialized length as
    // well as the changes due to new fields being present.

    for (int ii = 0; ii < record->fieldBuf_.bufSize; ii++) {
        Record::Field *recField = &record->fieldBuf_.buf[ii];
        assert((int) strlen(recField->meta->fieldName) ==
               recField->meta->fieldNameLen);
        assert(recField->meta->fieldNameLen > 0);
        if (recField->meta->index == -1) {
            // This is a field yet unseen in this page
            // Space from the type
            incrementalSpace += sizeof(recField->meta->fieldType);
            // Space from the field name itself
            incrementalSpace += recField->meta->fieldNameLen + 1;
            numNewFields++;
        }
    }

    // More fields means a larger bitmap
    // Subtract the size for the 'numbits' field of the bitmap; we don't need it
    int32_t newBitmapSize = RemoteBitmap<uint8_t>::sizeInWords(
        fieldMetaIndex_.bufSize + numNewFields);
    int32_t incrementalBitmap =
        newBitmapSize -
        RemoteBitmap<uint8_t>::sizeInWords(fieldMetaIndex_.bufSize);

    // The bitmap cost is paid for every previous record
    incrementalSpace += (incrementalBitmap) * (numRecords_);

    // The bitmap cost is also paid for this record
    incrementalSpace += newBitmapSize;

    // Account for all fields
    incrementalSpace += record->totFieldValSize_;

    if (pageSize_ + incrementalSpace > pageCapacity_) {
        *pageStatus = PageStatus::Full;
        if (bytesOverflow != NULL) {
            *bytesOverflow = pageSize_ + incrementalSpace - pageCapacity_;
        }
        goto CommonExit;
    }

    // Now let's make sure our fieldName index buffer has space for this one
    status = fieldMetaIndex_.growBuffer(numNewFields);
    BailIfFailed(status);

    // The rest of this operation is now guaranteed to succeed

    // We now know that we have space for this record; let's account for it.
    for (int ii = 0; ii < record->fieldBuf_.bufSize; ii++) {
        Record::Field *recField = &record->fieldBuf_.buf[ii];
        if (recField->meta->index == -1) {
            int32_t fieldIndex = fieldMetaIndex_.bufSize;
            fieldMetaIndex_.buf[fieldIndex] = recField->meta;
            recField->meta->index = fieldIndex;
            ++fieldMetaIndex_.bufSize;
        }
    }

    pageSize_ += incrementalSpace;
    assert(pageSize_ <= pageCapacity_);

    ++numRecords_;
    record->valid_ = true;

CommonExit:
    return status;
}

void
DataPageWriter::serializeToPage(uint8_t *page) const
{
    int32_t nameListSize, recListSize;
    serializePageHeader(page, pageCapacity_, &nameListSize);
    assert(nameListSize <= pageCapacity_);

    serializeRecords(page + nameListSize,
                     pageCapacity_ - nameListSize,
                     &recListSize);
    assert(nameListSize + recListSize <= pageCapacity_);
    assert(nameListSize + recListSize == pageSize_);
}

void
DataPageWriter::serializePageHeader(uint8_t *start,
                                    int32_t capacity,
                                    int32_t *bytesUsed) const
{
    *bytesUsed = 0;
    // First add the number of fields
    int32_t numFields = fieldMetaIndex_.bufSize;
    memcpy(start + *bytesUsed, &numFields, sizeof(numFields));
    *bytesUsed += sizeof(numFields);

    for (int ii = 0; ii < numFields; ii++) {
        // Copy in the actual field header
        DataPageFieldMeta *dpFieldMeta = fieldMetaIndex_.buf[ii];
        // Copy in the field type
        memcpy(start + *bytesUsed,
               &dpFieldMeta->fieldType,
               sizeof(dpFieldMeta->fieldType));
        *bytesUsed += sizeof(dpFieldMeta->fieldType);

        int thisLen = strlcpy((char *) start + *bytesUsed,
                              dpFieldMeta->fieldName,
                              capacity - *bytesUsed);
        assert(thisLen != 0);
        assert(thisLen == dpFieldMeta->fieldNameLen);
        *bytesUsed += thisLen + 1;  // 1 for \0
    }
}

void
DataPageWriter::serializeRecords(uint8_t *start,
                                 int32_t capacity,
                                 int32_t *bytesUsed) const
{
    *bytesUsed = 0;
    // First add the number of fields
    int32_t numTotalFields = fieldMetaIndex_.bufSize;
    memcpy(start + *bytesUsed, &numRecords_, sizeof(numRecords_));
    *bytesUsed += sizeof(numRecords_);

    // This aligned bitmap can be reused for all records
    int32_t bitmapSize = RemoteBitmap<uint8_t>::sizeInWords(numTotalFields);
    assert(*bytesUsed + bitmapSize <= capacity);

    int32_t numFoundRecords = 0;
    for (int ii = 0; ii < recordBuf_.bufSize; ii++) {
        Record *record = &recordBuf_.buf[ii];
        if (!record->valid_) {
            continue;
        }
        ++numFoundRecords;
        int numRecordFields = record->fieldBuf_.bufSize;

        record->sortFields();

        // Write the bitmap for this record
        uint8_t *bitmapBytes = start + *bytesUsed;
        RemoteBitmap<uint8_t> bitmap(bitmapBytes, numTotalFields);
        // Increase the bytesUsed even though we don't set the bitmap value
        // until later
        // Subtract the size for the 'numbits' field of the bitmap
        *bytesUsed += bitmap.sizeInBytes();

        // We need to clear our bitmap
        bitmap.clearAll();

        int32_t fieldValSize = 0;
        for (int jj = 0; jj < numRecordFields; jj++) {
            const Record::Field *thisField = &record->fieldBuf_.buf[jj];
            assert(thisField->valueSize + *bytesUsed <= capacity);

            // Set the bitmap for this field; note that the index is not
            // necessarily the previous field + 1
            bitmap.set(thisField->meta->index);

            memcpy(start + *bytesUsed, thisField->value, thisField->valueSize);
            *bytesUsed += thisField->valueSize;
            fieldValSize += thisField->valueSize;
        }
        assert(fieldValSize == record->totFieldValSize_);
        assert(bitmap.numSet() == numRecordFields);
    }
    assert(numFoundRecords == numRecords_);
}

int
DataPageWriter::compareRecordFields(const void *obj1, const void *obj2)
{
    const Record::Field *field1;
    const Record::Field *field2;
    field1 = static_cast<const Record::Field *>(obj1);
    field2 = static_cast<const Record::Field *>(obj2);

    assert(field1->meta->index != -1);
    assert(field2->meta->index != -1);
    assert(field1->meta->index != field2->meta->index);

    return field1->meta->index > field2->meta->index;
}

Status
DataPageWriter::getFieldMeta(const char *fieldName,
                             ValueType fieldType,
                             DataPageFieldMeta **fieldMeta)
{
    Status status = StatusOk;
    bool allocatedNewDp = false;
    bool insertedNewDp = false;
    *fieldMeta = NULL;
    assert(valueTypeValid(fieldType));

    uint64_t hashedIdentity =
        DataPageFieldMeta::hashNameAndType(fieldName, fieldType);

    // XXX This might have a bug if we have a hash collision. We probably want
    // the equivalent of a key type which is a tuple of hash name and type, but
    // I don't think this is possible with our existing hash table types.
    *fieldMeta = discoveredFields_.find(hashedIdentity);
    if (*fieldMeta == NULL) {
        char *fieldNameCopy;
        int32_t fieldNameLen;

        fieldNameLen = strlen(fieldName);
        fieldNameCopy = (char *) memPool_.alloc(fieldNameLen + 1);
        BailIfNull(fieldNameCopy);

        strlcpy(fieldNameCopy, fieldName, fieldNameLen + 1);
        *fieldMeta = new (std::nothrow) DataPageFieldMeta();
        BailIfNull(*fieldMeta);
        allocatedNewDp = true;

        (*fieldMeta)->fieldName = fieldNameCopy;
        fieldNameCopy = NULL;
        (*fieldMeta)->fieldNameLen = fieldNameLen;
        (*fieldMeta)->fieldType = fieldType;

        status = discoveredFields_.insert(*fieldMeta);
        BailIfFailed(status);
        insertedNewDp = true;
    }
    assert(strcmp((*fieldMeta)->fieldName, fieldName) == 0);
    assert((*fieldMeta)->fieldType == fieldType);

CommonExit:
    if (status != StatusOk) {
        // fieldNameCopy will be freed when memPool is destructed
        if (insertedNewDp) {
            verify(discoveredFields_.remove(hashedIdentity) == *fieldMeta);
        }
        if (allocatedNewDp) {
            delete *fieldMeta;
            *fieldMeta = NULL;
        }
    }
    return status;
}

/* ********************* DataPageWriter::FieldNameCache ********************* */

DataPageWriter::FieldMetaCache::~FieldMetaCache()
{
    reset();
}

Status
DataPageWriter::FieldMetaCache::init(int numFields, DataPageWriter *writer)
{
    Status status = StatusOk;
    reset();

    cacheLen_ = numFields;
    metaCache_ = new (std::nothrow) CacheEntry[cacheLen_];
    BailIfNull(metaCache_);

    writer_ = writer;

CommonExit:
    return status;
}

void
DataPageWriter::FieldMetaCache::reset()
{
    if (metaCache_) {
        delete[] metaCache_;
        metaCache_ = NULL;
    }
}

void
DataPageWriter::FieldMetaCache::setField(int index,
                                         const char *fieldName,
                                         ValueType type)
{
    assert(strlen(fieldName) > 0);
    assert(metaCache_ != NULL);
    assert(index < cacheLen_);

    metaCache_[index].fieldName = fieldName;
    metaCache_[index].fieldType = type;
}

void
DataPageWriter::FieldMetaCache::clearCache()
{
    assert(metaCache_);
    for (int ii = 0; ii < cacheLen_; ii++) {
        metaCache_[ii].dataPageFieldMeta = NULL;
    }
}

Status
DataPageWriter::FieldMetaCache::getFieldByIndex(int fieldIndex,
                                                DataPageFieldMeta **dpfnOut)
{
    Status status = StatusOk;
    DataPageFieldMeta *dpfn;

    assert(metaCache_ && "must be inited");
    assert(fieldIndex < cacheLen_);
    assert(metaCache_[fieldIndex].fieldName != NULL &&
           "fieldName must have been added");
    assert(valueTypeValid(metaCache_[fieldIndex].fieldType) &&
           "fieldType must be set correctly");

    dpfn = metaCache_[fieldIndex].dataPageFieldMeta;

    if (dpfn == NULL) {
        // we need to borrow a reference to this from the DataPageWriter
        status = writer_->getFieldMeta(metaCache_[fieldIndex].fieldName,
                                       metaCache_[fieldIndex].fieldType,
                                       &dpfn);
        BailIfFailed(status);

        metaCache_[fieldIndex].dataPageFieldMeta = dpfn;
    }

    *dpfnOut = dpfn;

CommonExit:
    return status;
}

/* ************************ DataPageWriter::Record ************************** */

DataPageWriter::Record::~Record()
{
    clear();
}

void
DataPageWriter::Record::clear()
{
    // Record::Field values get cleared in the DataPageWriter's memPool dtor
    fieldBuf_.bufSize = 0;
    valid_ = false;
    totFieldValSize_ = 0;
}

Status
DataPageWriter::Record::init(DataPageWriter *pageWriter)
{
    Status status = StatusOk;
    status = fieldBuf_.init(InitialNumFields);
    BailIfFailed(status);
    writer_ = pageWriter;
    totFieldValSize_ = 0;
CommonExit:
    return status;
}

void
DataPageWriter::Record::setFieldNameCache(FieldMetaCache *fieldCache)
{
    fieldNameCache_ = fieldCache;
}

Status
DataPageWriter::Record::addFieldByName(const char *fieldName,
                                       const TypedDataValue *fieldValue)
{
    Status status;
    DataPageFieldMeta *fieldMeta;

    status = writer_->getFieldMeta(fieldName, fieldValue->type_, &fieldMeta);
    BailIfFailed(status);

    status = addFieldValue(fieldMeta, &fieldValue->value_);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
DataPageWriter::Record::addFieldByIndex(int fieldIndex,
                                        const DataValue *fieldValue)
{
    Status status = StatusOk;
    DataPageFieldMeta *fieldMeta;

    assert(fieldNameCache_ != NULL &&
           "fieldNameCache_ is required to use addByIndex");

    status = fieldNameCache_->getFieldByIndex(fieldIndex, &fieldMeta);
    BailIfFailed(status);
    assert(fieldMeta);

    status = addFieldValue(fieldMeta, fieldValue);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
DataPageWriter::Record::addFieldValue(DataPageFieldMeta *fieldMeta,
                                      const DataValue *fieldValue)
{
    Status status = StatusOk;
    uint8_t *fieldVal = NULL;
    int32_t fieldValSize;
    int32_t writtenSize;
    Field *newField;
    bool success;
    assert(fieldMeta);
    assert(valueTypeValid(fieldMeta->fieldType));
    fieldValSize = fieldValue->getSize(fieldMeta->fieldType);

    fieldVal = static_cast<uint8_t *>(writer_->memPool_.alloc(fieldValSize));
    BailIfNull(fieldVal);

    success = fieldValue->serialize<false, false>(fieldMeta->fieldType,
                                                  fieldVal,
                                                  fieldValSize,
                                                  &writtenSize);
    assert(success && "we checked the size, so this must work");
    assert(writtenSize == fieldValSize && "the size shouldn't have changed");

    status = fieldBuf_.growBuffer(1);
    BailIfFailed(status);

    assert(fieldBuf_.buf != NULL);
    assert(fieldBuf_.bufSize < fieldBuf_.bufCapacity && "we just grew");
    newField = &fieldBuf_.buf[fieldBuf_.bufSize];
    // Treat the buf as being an array of (const char *)s

    newField->meta = fieldMeta;
    newField->valueSize = fieldValSize;
    newField->value = fieldVal;
    fieldVal = NULL;  // Steals the reference
    ++fieldBuf_.bufSize;
    totFieldValSize_ += fieldValSize;

CommonExit:
    if (fieldVal != NULL) {
        // fieldVal is owned by the memPool
        fieldVal = NULL;
    }
    return status;
}

bool
DataPageWriter::Record::containsField(const char *fieldName) const
{
    for (int ii = 0; ii < fieldBuf_.bufSize; ii++) {
        if (strcmp(fieldBuf_.buf[ii].meta->fieldName, fieldName) == 0) {
            return true;
        }
    }
    return false;
}

void
DataPageWriter::Record::sortFields() const
{
    // Sort this record's fields by the field index
    int numRecordFields = fieldBuf_.bufSize;
    bool sorted = true;

    // Fields are often already sorted; check if this is the case.
    // XXX This is unnecessary if we use a better sorting algorithm
    for (int jj = 1; jj < numRecordFields; jj++) {
        if (fieldBuf_.buf[jj].meta->index < fieldBuf_.buf[jj - 1].meta->index) {
            sorted = false;
            break;
        }
    }

    if (!sorted) {
        // This is only allowed because fieldBuf_ is a mutable field here
        qsort(fieldBuf_.buf,
              numRecordFields,
              sizeof(fieldBuf_.buf[0]),
              DataPageWriter::compareRecordFields);
    }
}

int32_t
DataPageWriter::Record::recordSize() const
{
    return totFieldValSize_;
}

/* **************************** DataPageReader ****************************** */

void
DataPageReader::init(const uint8_t *page, int32_t pageCapacity)
{
    page_ = page;
    pageCapacity_ = pageCapacity;
    numFields_ = -1;
    numRecords_ = -1;
    if (metas_) {
        delete[] metas_;
        metas_ = NULL;
    }
}

bool
DataPageReader::validatePage()
{
    bool ret;
    int32_t nameListLen;
    int32_t recordListLen;
    int32_t numFields;
    ret = validateNameList(0, &numFields, &nameListLen);
    if (ret == false) {
        return false;
    }

    ret = validateRecordList(nameListLen, numFields, &recordListLen);
    if (ret == false) {
        return false;
    }
    return true;
}

bool
DataPageReader::validateNameList(int32_t initialPos,
                                 int32_t *numFields,
                                 int32_t *length)
{
    bool ret;
    *length = 0;
    Cursor cursor(this);
    cursor.setPos(initialPos);

    ret = cursor.getNumFieldNames(true, numFields);
    if (unlikely(!ret)) {
        return false;
    }

    for (int ii = 0; ii < *numFields; ii++) {
        ret = cursor.getFieldMeta(true, NULL, NULL, NULL);
        if (unlikely(!ret)) {
            return false;
        }
    }

    *length = cursor.getPos() - initialPos;

    return true;
}

bool
DataPageReader::validateRecordList(int32_t initialPos,
                                   int32_t numFields,
                                   int32_t *length)
{
    bool ret;
    int32_t numRecords;
    *length = 0;
    Cursor cursor(this);
    cursor.setPos(initialPos);

    ret = cursor.getNumRecords(true, &numRecords);
    if (unlikely(!ret)) {
        return false;
    }

    for (int ii = 0; ii < numRecords; ii++) {
        ret = cursor.walkRecord(numFields);
        if (unlikely(!ret)) {
            return false;
        }
    }

    return true;
}

void
DataPageReader::getRecord(int32_t offsetStart,
                          int32_t recordIdxDiff,
                          ReaderRecord *record)
{
    // 1. Walk along the record list, starting at page+offsetStart,
    // using recordSize for stride step
    // 2. Repeat recordIdxDiff times
    // 3. Create Record with a pointer to this new offset

    // 1. Base case is offsetStart < pageCapacity_.
    // 2. Edge case is when last record in the page has all FNFs and so
    // numFields is 0. In this case, offsetStart_ == pageCapacity_, but
    // iterator continues to iterate this page.
    // XXX TODO Need to add more metadata to tell apart case 1 & 2 here.
    assert(offsetStart <= pageCapacity_);

    int32_t numFields = -1;
    Cursor cursor(this);
    cursor.setPos(0);

    verify(cursor.getNumFieldNames(true, &numFields));

    if (offsetStart == 0) {
        // We are starting at the very top of a page, so we need to walk over
        // all of the field metadatas
        for (int ii = 0; ii < numFields; ii++) {
            cursor.getFieldMeta(true, NULL, NULL, NULL);
        }
        int32_t numRecordsPresent = 0;
        verify(cursor.getNumRecords(true, &numRecordsPresent));
        assert(numRecordsPresent > recordIdxDiff);
    } else {
        cursor.setPos(offsetStart);
    }

    for (int32_t ii = 0; ii < recordIdxDiff; ii++) {
        verify(cursor.walkRecord(numFields));
    }
    record->init(page_, pageCapacity_, cursor.getPos());
}

int32_t
DataPageReader::getNumFields() const
{
    if (numFields_ == -1) {
        Cursor cursor(this);
        cursor.setPos(0);
        verify(cursor.getNumFieldNames(false, &numFields_));
    }
    return numFields_;
}

int32_t
DataPageReader::getNumRecords() const
{
    if (numRecords_ == -1) {
        bool ret;
        int32_t numFields = getNumFields();
        Cursor cursor(this);
        cursor.setPos(0);

        verify(cursor.getNumFieldNames(true, NULL));

        for (int ii = 0; ii < numFields; ii++) {
            ret = cursor.getFieldMeta(true, NULL, NULL, NULL);
            if (unlikely(!ret)) {
                return false;
            }
        }
        verify(cursor.getNumRecords(false, &numRecords_));
    }
    return numRecords_;
}

/* ***************************** ReaderRecord ******************************* */

void
ReaderRecord::init(const uint8_t *page, int32_t pageCapacity, int32_t offset)
{
    reader_.init(page, pageCapacity);
    offset_ = offset;
}

int32_t
ReaderRecord::getNumFields() const
{
    int32_t numPageFields = reader_.getNumFields();

    DataPageReader::Cursor cursor(&reader_);
    cursor.setPos(offset_);

    RemoteBitmap<const uint8_t> bitmap;
    // This function will set bitmap to be valid
    verify(cursor.getBitmap(false, numPageFields, &bitmap));

    int32_t recFields = bitmap.numSet();
    assert(recFields <= numPageFields);
    return recFields;
}

Status
ReaderRecord::getFieldByName(const char *fieldName,
                             DataValueReader *fieldValue,
                             int32_t *index) const
{
    Status status = StatusOk;
    int32_t numFields = reader_.getNumFields();
    int fieldIndex = -1;
    FieldValueIterator valueIterator(this);

    // Look through all the fields until we find one with a matching name that
    // exists. We have to potentially try multiple fields because this DataPage
    // might have multiple fields with the same name, but different types.
    // However, this shouldn't occur for any single record, so if we find a
    // field whose name matches, we know that it is the old field whose name
    // matches for this record.
    for (int ii = 0; ii < numFields; ii++) {
        bool done;
        bool exists;
        const char *thisFieldName;
        DataValueReader *iteratorReader;
        bool matchingField;

        status = reader_.getFieldMeta(ii, &thisFieldName, NULL);
        BailIfFailed(status);
        assert(thisFieldName);

        matchingField = strcmp(fieldName, thisFieldName) == 0;

        // If this field matches, then we should try to get its value, which
        // means passing in a valid fieldValue pointer. If this is a NULL
        // pointer, we don't need to extract the value, improving performance.
        iteratorReader = matchingField ? fieldValue : NULL;

        status =
            valueIterator.getNext(&done, &exists, NULL, NULL, iteratorReader);
        BailIfFailed(status);
        assert(!done);

        if (matchingField && exists) {
            // We know that we have a field whose name matches and is present.
            // There may be multiple fields with this specific name, but only
            // 1 will be present for a given record.
            fieldIndex = ii;
            break;
        }
    }

    if (fieldIndex == -1) {
        status = StatusDfFieldNoExist;
        goto CommonExit;
    }

    if (index) {
        *index = fieldIndex;
    }

CommonExit:
    return status;
}

Status
ReaderRecord::getFieldByIdx(int32_t index,
                            const char **fieldNameOut,
                            DataValueReader *fieldValueOut) const
{
    Status status;
    FieldValueIterator valueIterator(this);
    assert(index < reader_.getNumFields());
    bool done;
    bool exists;

    // Walk over all the fields we don't care about
    for (int ii = 0; ii < index; ii++) {
        status = valueIterator.getNext(&done, NULL, NULL, NULL, NULL);
        BailIfFailed(status);
        assert(!done && "we know that there are enough fields here");
    }

    // Get the value we care about. Some of these arguments may be NULL, but
    // that is allowed by the function
    status = valueIterator.getNext(&done,
                                   &exists,
                                   fieldNameOut,
                                   NULL,
                                   fieldValueOut);
    BailIfFailed(status);

    assert(!done && "we know that there are enough fields here");

    if (!exists) {
        status = StatusDfFieldNoExist;
        goto CommonExit;
    }

CommonExit:
    return status;
}

/* *********** DataPageReader::ReaderRecord::FieldValueIterator ************* */

ReaderRecord::FieldValueIterator::FieldValueIterator(const ReaderRecord *record)
    : record_(record),
      metaCursor_(&record->reader_),
      dataCursor_(&record->reader_)
{
    numFields_ = record_->reader_.getNumFields();

    // Walk the metaCursor to the start of the metas
    metaCursor_.setPos(0);
    verify(metaCursor_.getNumFieldNames(true, NULL));

    // seek dataCursor to our record and prep the bitmap
    dataCursor_.setPos(record_->getOffset());
    dataCursor_.getBitmap(true, numFields_, &bitmap_);
}

Status
ReaderRecord::FieldValueIterator::getNext(bool *done,
                                          bool *existsOut,
                                          const char **fieldNameOut,
                                          ValueType *typeOut,
                                          DataValueReader *fieldValueOut)
{
    Status status;
    ValueType type = ValueType::Invalid;
    bool exists = false;

    *done = false;

    if (curField_ >= numFields_) {
        *done = true;
        goto CommonExit;
    }

    // Walk the metaCursor forward and grab this field's metadata
    // We can directly pass through the fieldNameOut; if it's NULL that's fine
    // because we do not need it.
    // We get type because we need it, regardless of whether the caller does
    verify(metaCursor_.getFieldMeta(true, fieldNameOut, NULL, &type));

    exists = bitmap_.test(curField_);

    if (exists) {
        status = dataCursor_.getField(true, type, fieldValueOut);
        BailIfFailed(status);
    }

    ++curField_;

CommonExit:
    if (existsOut) {
        *existsOut = exists;
    }

    if (typeOut) {
        *typeOut = type;
    }
    return status;
}

/* ******************** DataPageReader::RecordIterator ********************** */

DataPageReader::RecordIterator::RecordIterator(DataPageReader *reader)
    : reader_(reader), cursor_(reader_)
{
    cursor_.setPos(0);
    verify(cursor_.getNumFieldNames(true, &numFields_));

    for (int ii = 0; ii < numFields_; ii++) {
        verify(cursor_.getFieldMeta(true, NULL, NULL, NULL));
    }

    verify(cursor_.getNumRecords(true, &numRecords_));

    curRecord_ = 0;
}

bool
DataPageReader::RecordIterator::getNext(ReaderRecord *record)
{
    if (curRecord_ >= numRecords_) {
        return false;
    }

    if (curRecord_ != 0) {
        verify(cursor_.walkRecord(numFields_));
    }

    if (record) {
        reader_->getRecord(cursor_.getPos(), 0, record);
    }
    ++curRecord_;

    return true;
}

/* ******************* DataPageReader::FieldNameIterator ******************** */

DataPageReader::FieldMetaIterator::FieldMetaIterator(
    const DataPageReader *reader)
    : reader_(reader), cursor_(reader_)
{
    cursor_.setPos(0);
    verify(cursor_.getNumFieldNames(true, &numFields_));
    curField_ = 0;
}

bool
DataPageReader::FieldMetaIterator::getNext(const char **fieldName,
                                           int32_t *fieldNameLen,
                                           ValueType *type)
{
    if (curField_ >= numFields_) {
        return false;
    }

    verify(cursor_.getFieldMeta(true, fieldName, fieldNameLen, type));
    ++curField_;

    return true;
}

/* ************************ DataPageReader::Cursor ************************** */

DataPageReader::Cursor::Cursor(const DataPageReader *reader) : reader_(reader)
{
}

bool
DataPageReader::Cursor::getNumFieldNames(bool walk, int32_t *numFieldNamesOut)
{
    if (unlikely(pos_ + (int32_t) sizeof(*numFieldNamesOut) >
                 reader_->pageCapacity_)) {
        return false;
    }
    if (numFieldNamesOut) {
        memcpy(numFieldNamesOut,
               reader_->page_ + pos_,
               sizeof(*numFieldNamesOut));
    }

    if (walk) {
        pos_ += sizeof(*numFieldNamesOut);
    }
    return true;
}

bool
DataPageReader::Cursor::getFieldMeta(bool walk,
                                     const char **fieldNameOut,
                                     int32_t *fieldNameLenOut,
                                     ValueType *typeOut)
{
    // Make sure we have room for the field type
    if (unlikely(pos_ + (int32_t) sizeof(ValueType) > reader_->pageCapacity_)) {
        return false;
    }

    ValueType type;
    memcpy(&type, reader_->page_ + pos_, sizeof(type));

    // Check the field name and get its size
    const char *fieldNameStart =
        (const char *) reader_->page_ + pos_ + sizeof(ValueType);
    int32_t fieldNameLen =
        strnlen(fieldNameStart, reader_->pageCapacity_ - pos_);
    if (unlikely(fieldNameLen == 0)) {
        return false;
    }
    if (unlikely(pos_ + (int32_t) sizeof(ValueType) + fieldNameLen + 1 >
                 reader_->pageCapacity_)) {
        return false;
    }

    if (fieldNameLenOut) {
        *fieldNameLenOut = fieldNameLen;
    }

    if (fieldNameOut) {
        *fieldNameOut = fieldNameStart;
    }

    if (typeOut) {
        *typeOut = type;
    }

    if (walk) {
        pos_ += sizeof(ValueType) + fieldNameLen + 1;
    }

    return true;
}

bool
DataPageReader::Cursor::getNumRecords(bool walk, int32_t *numRecordsOut)
{
    if (unlikely(pos_ + (int32_t) sizeof(*numRecordsOut) >
                 reader_->pageCapacity_)) {
        return false;
    }
    if (numRecordsOut) {
        memcpy(numRecordsOut, reader_->page_ + pos_, sizeof(*numRecordsOut));
    }

    if (walk) {
        pos_ += sizeof(*numRecordsOut);
    }
    return true;
}

bool
DataPageReader::Cursor::walkRecord(int32_t numFields)
{
    Status status;
    RemoteBitmap<const uint8_t> bitmap;
    bool ret = getBitmap(true, numFields, &bitmap);
    if (unlikely(!ret)) {
        return false;
    }

    for (int32_t ii = 0; ii < numFields; ii++) {
        if (bitmap.test(ii)) {
            // XXX this treats parse errors the same as memory allocation
            // errors. This could be bad for diagnosability if we expect either
            // of these to be common, which they aren't here.
            ValueType type;
            status = reader_->getFieldMeta(ii, NULL, &type);
            if (unlikely(status != StatusOk)) {
                return false;
            }
            Status status = getField(true, type, NULL);
            if (unlikely(status != StatusOk)) {
                return false;
            }
        }
    }

    return true;
}

Status
DataPageReader::Cursor::getField(bool walk,
                                 ValueType type,
                                 DataValueReader *valueReaderOut)
{
    Status status;
    DataValueReader stackReader;
    // We still need a reader even if the caller doesn't want the value
    DataValueReader *reader = valueReaderOut ? valueReaderOut : &stackReader;
    int32_t fieldSize;
    bool dryRun;

    // If the caller doesn't need the value, then we don't need to even parse it
    // out and can just do a dry parse, which will get us the value size.
    dryRun = valueReaderOut == NULL;

    if (dryRun) {
        status = reader->parseFromBuffer<true>(reader_->page_ + pos_,
                                               reader_->pageCapacity_ - pos_,
                                               &fieldSize,
                                               type);
    } else {
        status = reader->parseFromBuffer<false>(reader_->page_ + pos_,
                                                reader_->pageCapacity_ - pos_,
                                                &fieldSize,
                                                type);
    }
    BailIfFailed(status);

    if (walk) {
        pos_ += fieldSize;
    }

CommonExit:
    return status;
}

bool
DataPageReader::Cursor::getBitmap(bool walk,
                                  int32_t numFields,
                                  RemoteBitmap<const uint8_t> *bitmap)
{
    // This is the size needed by the caller to allocate the bitmap
    int32_t bitmapSize = RemoteBitmap<const uint8_t>::sizeInWords(numFields);
    if (unlikely(pos_ + bitmapSize > reader_->pageCapacity_)) {
        return false;
    }
    const RemoteBitmap<const uint8_t> bm(reader_->page_, numFields);
    if (bitmap) {
        // Note that this is using the assignment constructor
        *bitmap = RemoteBitmap<const uint8_t>(reader_->page_ + pos_, numFields);
    }

    if (walk) {
        pos_ += bitmapSize;
    }
    return true;
}
