// Copyright 2017-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// ----- Overview -----
// This set of classes provides functionality for packing/unpacking data
// into/from fixed size pages. This is primary to allow for datasets to be
// backed by pages from the BufferCache.
//
// ----- Page Format Specification -----
// data_page    ::= field_list record_list
// num_fields   ::= int32
// field_list   ::= num_fields [field_meta]
// field_type   ::= Int32 | Int64 | UInt32 | UInt64 | Float32 | Float64 |
//                  Boolean | String | Timestamp | Null | Object | Array
// field_type   ::= int8
// field_name   ::= string
// field_meta   ::= field_type field_name
// num_records  ::= int32
// record_list  ::= num_records [record]
// record       ::= bitmap [field_value]    # bitmap size is from numNames
// field_value  ::= [byte]                  # see field_type for interpreting
// field_value  ::= varint        |         # For Int32, Int64, UInt32, UInt64
//                  [byte]*4      |         # For Float32
//                  [byte]*8      |         # For Float64
//                  [byte]*9      |         # For Timestamp
//                  byte          |         # For Bool
//                  varint [byte] |         # For String, Object, Array
//                  []                      # For Null
// bitmap       ::= [byte]                  # length = ceil(numNames / 8)
// string       ::= [byte] '\0'
//
// ----- Page Format Discussion -----
//
// Page is intended to be implemented with an external index on `record_list`,
// allowing quick access to any given record.

#ifndef _DATAPAGE_H_
#define _DATAPAGE_H_

#include "primitives/Primitives.h"
#include "util/StringHashTable.h"
#include "util/ElemBuf.h"
#include "datapage/MemPool.h"
#include "datapage/DataValue.h"
#include "util/RemoteBitmap.h"

class DataPageWriter;
class ReaderRecord;
class DataValueReader;

struct DataPageFieldMeta {
    static constexpr char BannedChars[] = {
        '"',
        '^',
        ',',
        '(',
        ')',
        '[',
        ']',
        '{',
        '}',
        '\'',
        // '.',
        '\\',
        ':',
        '\0'  // null terminate this list so it can be used with strpbrk
    };

    static constexpr uint64_t BigPrime = 179432087;

    static bool fieldNameAllowed(const char *rawName, int rawLen)
    {
        assert(rawName != NULL);
        assert(rawLen > 0);
        assert((int) strlen(rawName) == rawLen);

        // Check leading spaces
        if (unlikely(rawName[0] == ' ')) {
            return false;
        }

        // Check for invalid characters
        if (unlikely(strpbrk(rawName, BannedChars))) {
            return false;
        }

        // Check for trailing spaces
        if (unlikely(rawName[rawLen - 1] == ' ')) {
            return false;
        }

        return true;
    }

    inline uint64_t hashIdentifier() const
    {
        return hashNameAndType(this->fieldName, this->fieldType);
    }

    static inline uint64_t hashNameAndType(const char *fieldName,
                                           ValueType fieldType)
    {
        uint64_t initial = hashStringFast(fieldName);
        uint64_t typeVal = (uint64_t) fieldType * BigPrime;
        return initial ^ typeVal;
    }

    ~DataPageFieldMeta()
    {
        if (this->fieldName) {
            // We free all fieldNames together with the freeing of the
            // memPool_ memory
            this->fieldName = NULL;
        }
    }

    void del() { delete this; }

    IntHashTableHook hook;
    int32_t fieldNameLen = -1;
    char *fieldName = NULL;
    ValueType fieldType = ValueType::Invalid;
    int32_t index = -1;
};

// Allows for records to be packed into a fixed-size page. User should
// continually addRecords until the page is full, at which point the writer
// can serialize the page.
class DataPageWriter final
{
  public:
    // Data Structures
    enum PageStatus { NotFull = 10, Full };

    class FieldMetaCache final
    {
      public:
        FieldMetaCache() = default;
        ~FieldMetaCache();

        MustCheck Status init(int numFields, DataPageWriter *writer);

        // fieldName is shallow-copeid. fieldName must stay valid as long as
        // this cache is used
        void setField(int index, const char *fieldName, ValueType type);

        // Cache must be cleared when DataPageWriter writes out
        // XXX this could be a callback on the DataPageWriter
        void clearCache();

        MustCheck Status getFieldByIndex(int fieldIndex,
                                         DataPageFieldMeta **dpfnOut);

      private:
        struct CacheEntry {
            // Borrowed reference from the DataPageWriter
            DataPageFieldMeta *dataPageFieldMeta = NULL;
            // Borrowed memory from caller
            const char *fieldName = NULL;
            ValueType fieldType = ValueType::Invalid;
        };

        // Resets internal metadata and storage to 'init' state
        void reset();

        DataPageWriter *writer_ = NULL;
        int cacheLen_ = -1;
        CacheEntry *metaCache_ = NULL;
    };

    // Allows a caller to construct records to be added to the writer.
    // The usage of this class is generally:
    // Record record;
    // record.init(writer);
    // record.addField(); # likely multiples times
    // writer.addRecord(record, &pageStatus)
    // if (pageStatus == Full) {
    //     writer.serialize()
    //     writer.clear()
    //     record.clear(); # must be called before record.init()
    //     record.init(writer);
    //     record.addField()
    // etc.
    // If DataPageWriter::addRecord reports a full page, this record must be
    // cleared and re-inited with the cleared writer
    class Record final
    {
        friend class DataPageWriter;

      public:
        struct Field {
            DataPageFieldMeta *meta;
            uint8_t *value;
            int32_t valueSize;
        };

        // These should only be invoked from within the DataPageWriter
        Record() = default;
        Record(Record &&other) = default;

        ~Record();

        // This is used in cases where our fields are consistently ordered
        // and thus the names can be cached, rather than looked up each time
        void setFieldNameCache(FieldMetaCache *fieldCache);

        // Check if the record contains a given field name
        // XXX this is O(n) n=numFieldsInRecord right now
        bool containsField(const char *fieldName) const;

        // For use by callers constructing records
        MustCheck Status addFieldByName(const char *fieldName,
                                        const TypedDataValue *fieldValue);

        // Requires a fieldNameCache be set
        MustCheck Status addFieldByIndex(int fieldIndex,
                                         const DataValue *fieldValue);

        void sortFields() const;

        // This object can be reset if it needs to be reconstructed with a new
        // page, because the original was full
        void clear();

        int32_t recordSize() const;

      private:
        static constexpr int InitialNumFields = 31;

        Record(const Record &) = delete;
        Record &operator=(const Record &) = delete;

        // Ties this record to the given writer. The attached writer cannot
        // be changed
        MustCheck Status init(DataPageWriter *writer);

        Status addFieldValue(DataPageFieldMeta *fieldName,
                             const DataValue *fieldValue);

        bool valid_ = false;
        DataPageWriter *writer_ = NULL;
        int32_t totFieldValSize_ = 0;
        // This contains the `Field`s for this record
        // This is mutable to allow sorting under const
        mutable ElemBuf<Field> fieldBuf_;
        FieldMetaCache *fieldNameCache_ = NULL;
    };

    // Methods
    DataPageWriter() = default;
    ~DataPageWriter();

    MustCheck Status init(int32_t pageCapacity);

    MustCheck Status newRecord(Record **record);

    // When returned Status == StatusOk, reference to record is stolen
    // When pageStatus == Full, this record should be cleared discarded
    // When pageStatus == Full, *bytesOverflow will be set with how much larger
    // the page would need to be to fit this record.
    MustCheck Status commit(Record *record,
                            PageStatus *pageStatus,
                            int32_t *bytesOverflow);

    int32_t pageSize() const { return pageSize_; };

    int32_t pageCapacity() const { return pageCapacity_; };

    void serializeToPage(uint8_t *page) const;

    // Clears all added records, allowing this object to be reused for
    // a new page.
    void clear();

  private:
    static constexpr int NumHashSlots = 63;
    static constexpr int InitialNumRecords = 101;
    static constexpr int InitialNumFields = 31;
    static constexpr const char *ModuleName = "libdatapage";
    typedef IntHashTable<uint64_t,
                         DataPageFieldMeta,
                         &DataPageFieldMeta::hook,
                         &DataPageFieldMeta::hashIdentifier,
                         NumHashSlots,
                         hashIdentity>
        FieldHashTable;

    DataPageWriter(const DataPageWriter &) = delete;
    DataPageWriter &operator=(const DataPageWriter &) = delete;

    MustCheck Status getFieldMeta(const char *fieldName,
                                  ValueType fieldType,
                                  DataPageFieldMeta **fieldMeta);

    void serializePageHeader(uint8_t *start,
                             int32_t capacity,
                             int32_t *bytesUsed) const;
    void serializeRecords(uint8_t *start,
                          int32_t capacity,
                          int32_t *bytesUsed) const;
    // Used in the qsort function
    static int compareRecordFields(const void *obj1, const void *obj2);

    int32_t pageCapacity_ = -1;
    int32_t pageSize_;

    // Number of valid records in recordBuf_
    int32_t numRecords_ = 0;
    ElemBuf<Record> recordBuf_;
    // This allows for constant time lookup of existing fields
    FieldHashTable discoveredFields_;
    // This provides consistent ordering of all fields within this page
    ElemBuf<DataPageFieldMeta *> fieldMetaIndex_;
    MallocAllocator memAllocator_;
    MemPool memPool_;
};

//
// Reader classes
//

// Allows for records to be retrieved from a fixed-size page. User should get
// a record by index into the page, then retrieve the field by name.
class DataPageReader final
{
    friend ReaderRecord;

  public:
    DataPageReader() = default;
    ~DataPageReader()
    {
        if (metas_) {
            delete[] metas_;
            metas_ = NULL;
        }
    }

    void init(const uint8_t *page, int32_t pageCapacity);

    bool validatePage();
    int32_t getNumFields() const;
    int32_t getNumRecords() const;
    inline Status getFieldMeta(int index,
                               const char **fieldName,
                               ValueType *type) const
    {
        Status status;
        if (unlikely(metas_ == NULL)) {
            metas_ = new (std::nothrow) MetaCacheEntry[getNumFields()];
            BailIfNull(metas_);
            // XXX do this lazily; only building enough field names to do what
            // we need
            DataPageReader::FieldMetaIterator cursor(this);
            for (int ii = 0; ii < getNumFields(); ii++) {
                verify(cursor.getNext(&metas_[ii].fieldName,
                                      NULL,
                                      &metas_[ii].type));
            }
        }
        if (type) {
            *type = metas_[index].type;
        }
        if (fieldName) {
            *fieldName = metas_[index].fieldName;
        }
    CommonExit:
        return status;
    }
    // walks recordIdxDiff from offsetStart, which must refer to a record
    void getRecord(int32_t offsetStart,
                   int32_t recordIdxDiff,
                   ReaderRecord *record);

  private:
    struct MetaCacheEntry {
        const char *fieldName;
        ValueType type;
    };
    static constexpr const char *ModuleName = "libdatapage";

    class Cursor
    {
      public:
        Cursor(const DataPageReader *reader);
        ~Cursor() = default;

        void setPos(int32_t pos) { pos_ = pos; }

        int32_t getPos() const { return pos_; }

        // These are 'safe' functions which return false if the walk would fail.
        // it's possible to have 'unsafe' versions which assume a valid page
        // These functions take a 'walk' argument that determines whether the
        // cursor's position is updated (only on success).
        // Any output args may be set to NULL if not desired
        inline bool getNumFieldNames(bool walk, int32_t *numFieldNamesOut);

        inline bool getFieldMeta(bool walk,
                                 const char **fieldNameOut,
                                 int32_t *fieldNameLenOut,
                                 ValueType *typeOut);

        inline bool getNumRecords(bool walk, int32_t *numRecordsOut);

        inline bool walkRecord(int32_t numFields);

        inline MustCheck Status getField(bool walk,
                                         ValueType type,
                                         DataValueReader *valueReaderOut);
        // returns bitmap into 'bitmap' if not NULL
        inline bool getBitmap(bool walk,
                              int32_t numFields,
                              RemoteBitmap<const uint8_t> *bitmap);

      private:
        const DataPageReader *reader_;
        int32_t pos_ = -1;
    };

    DataPageReader(const DataPageReader &) = delete;
    DataPageReader &operator=(const DataPageReader &) = delete;

    bool validateNameList(int32_t initialPos,
                          int32_t *numFields,
                          int32_t *length);
    bool validateRecordList(int32_t initialPos,
                            int32_t numFields,
                            int32_t *length);

    const uint8_t *page_ = NULL;
    int32_t pageCapacity_ = -1;
    mutable int32_t numFields_ = -1;
    mutable int32_t numRecords_ = -1;

    // We need to keep track of our field types for easy lookup
    // We want to be able to allocate this on demand, so we make it mutable,
    // allowing it to be allocated under const
    mutable MetaCacheEntry *metas_ = NULL;

  public:
    // Data Structures
    class RecordIterator final
    {
      public:
        RecordIterator(DataPageReader *reader);
        ~RecordIterator() = default;

        // Returns false when another record was not found
        bool getNext(ReaderRecord *record);

      private:
        RecordIterator &operator=(const RecordIterator &) = delete;

        DataPageReader *reader_;
        int32_t numRecords_;
        int32_t numFields_;
        int32_t curRecord_;
        Cursor cursor_;
    };

    class FieldMetaIterator final
    {
      public:
        FieldMetaIterator(const DataPageReader *reader);
        ~FieldMetaIterator() = default;

        // Returns false when another record was not found
        bool getNext(const char **fieldName,
                     int32_t *fieldNameLen,
                     ValueType *type);

      private:
        FieldMetaIterator &operator=(const FieldMetaIterator &) = delete;

        const DataPageReader *reader_;
        int32_t numFields_;
        int32_t curField_;
        Cursor cursor_;
    };
};

class ReaderRecord final
{
  public:
    class FieldValueIterator final
    {
      public:
        FieldValueIterator(const ReaderRecord *record);
        ~FieldValueIterator() = default;

        // Returns false when another record was not found
        // Sets 'done' when the iterator is depleted
        Status getNext(bool *done,
                       bool *existsOut,
                       const char **fieldNameOut,
                       ValueType *typeOut,
                       DataValueReader *fieldValueOut);

      private:
        FieldValueIterator &operator=(const FieldValueIterator &) = delete;

        const ReaderRecord *record_ = NULL;
        int32_t numFields_ = -1;
        int32_t curField_ = 0;
        RemoteBitmap<const uint8_t> bitmap_;
        DataPageReader::Cursor metaCursor_;
        DataPageReader::Cursor dataCursor_;
    };

    ReaderRecord() = default;
    ~ReaderRecord() = default;

    void init(const uint8_t *page, int32_t pageCapacity, int32_t offset);

    int32_t getNumFields() const;
    // *fieldValue is NULL if field does not exist
    MustCheck Status getFieldByName(const char *fieldNameOut,
                                    DataValueReader *fieldValueOut,
                                    int32_t *indexOut) const;
    MustCheck Status getFieldByIdx(int32_t index,
                                   const char **fieldNameOut,
                                   DataValueReader *fieldValueOut) const;
    uint32_t getOffset() const { return offset_; }

  private:
    ReaderRecord(const ReaderRecord &) = delete;
    ReaderRecord &operator=(const ReaderRecord &) = delete;

    DataPageReader reader_;
    int32_t offset_;
};

#endif  // _DATAPAGE_H_
