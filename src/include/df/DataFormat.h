// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMAT_H_
#define _DATAFORMAT_H_

#include <endian.h>

#include <jansson.h>
#include <google/protobuf/arena.h>

#include "XlrDecimal64.h"

#include "config/Config.h"
#include "operators/XcalarEvalTypes.h"
#include "df/DataFormatTypes.h"
#include "dataset/DatasetTypes.h"
#include "msg/MessageTypes.h"
#include "operators/Dht.h"
#include "bc/BufferCache.h"
#include "util/MemTrack.h"
#include "StrlFunc.h"
#include "df/Accessor.h"
#include "df/Exportable.h"
#include "datapage/DataValue.h"
#include "runtime/Mutex.h"

#define DfRecordFatptrMagic (0x746146ULL)
#define DfRecordMagicShift (32)
#define FatptrTypeMatch(t1, t2) \
    ((t1 == DfFatptr && t2 == DfFatptr) || (t1 != DfFatptr && t2 != DfFatptr))

const DfFieldValue DfFieldValueNull = {
    .stringVal = {.strActual = NULL, .strSize = 0}};

enum DfFieldValueRangeState : uint8_t {
    DfFieldValueRangeValid,
    DfFieldValueRangeInvalid,
};
enum HashString : uint8_t {
    DoHashString,
    DontHashString,
};

static constexpr int InvalidIdx = -1;

class DataPageIndex;
class DataPageWriter;
class ReaderRecord;
class DataValueReader;
class BackingDataMem;
class Importable;
class Exportable;
class IRecordParser;
class IRowRenderer;
union ExInitExportFormatSpecificArgs;

struct DemystifyVariable {
    DemystifyVariable() = default;
    ~DemystifyVariable() = default;

    MustCheck Status init(const char *name, DfFieldType type);

    // XXX - this can be made into an int once all tests use fatptr prefixes
    bool *possibleEntries = NULL;
    // This is the 'optimization context' for this field across records
    DfFieldType type = DfUnknown;
    int valueArrayIdx = -1;
    bool isImmediate = false;
    Accessor accessor;
};

struct Column {
    char columnName[XcalarApiMaxFieldNameLen];
    DfFieldType type;
};

class DataFormat final
{
  public:
    static Status init();
    void destroy();
    static DataFormat *get();

    Importable *importables_[DfFormatTypeLen];
    Exportable *exportables_[DfFormatTypeLen];

    unsigned recordFatptrNodeIdShift_;
    uint64_t recordFatptrNodeIdMask_;
    unsigned recordFatptrRecIdShift_;
    uint64_t recordFatptrRecIdMask_;
    uint64_t recordFatptrMask_;
    unsigned recordFatptrNumBits_;

    static constexpr const char *DefaultDateFormatIn =
        "%d-%d-%d%c%d:%d:%f%c%d:00";

    static constexpr const char *DefaultDateFormatOutUTC =
        "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ";

    static constexpr const char *DefaultDateFormatOutTZ =
        "%04d-%02d-%02dT%02d:%02d:%02d.%03d%c%02d:00";

    static void freeXdbPage(void *p);

    static Status convertValueType(DfFieldType outputType,
                                   DfFieldType inputType,
                                   const DfFieldValue *inputFieldValue,
                                   DfFieldValue *outputFieldValue,
                                   size_t outputStrBufSize,
                                   Base inputBase);

    static Status jsonifyValue(DfFieldType inputType,
                               const DfFieldValue *fieldValue,
                               json_t **jsonVal);

    void fatptrInit(uint64_t maxNodes, uint64_t maxRecordsPerNode);
    Status fatptrToJson(DfRecordFatptr recFatptr,
                        const char *fatptrPrefix,
                        json_t **fatptrJson);

    // XXX FIXME this is temporary; see createIndexFromTableWork1()
    void getFieldValuesFatptrCompletion(MsgEphemeral *eph, void *payload);
    Status getFieldValuesFatptr(DfRecordFatptr recFatptr,
                                const char *fieldName,
                                unsigned startValueNum,
                                DfFieldValueArray *value,
                                size_t valueBufSize,
                                DfFieldType *recordFieldType);

    Status msgFatptrBatchOp(MsgEphemeral *eph, void *payload);

    static NodeId hashValueToNodeId(Dht *dht,
                                    DfFieldValue fieldVal,
                                    DfFieldType fieldType);

    static Status addJsonFieldImm(json_t *fieldObject,
                                  const char *fatptrPrefix,
                                  const char *fieldName,
                                  DfFieldType fieldType,
                                  const DfFieldValue *fieldValue);

    MustCheck Status extractFields(const NewKeyValueEntry *srcKvEntry,
                                   const NewTupleMeta *tupleMeta,
                                   int numVariables,
                                   DemystifyVariable *variables,
                                   Scalar **scratchPadScalars,
                                   NewKeyValueEntry *dstKvEntry);

    static Status escapeNestedDelim(char *fieldName,
                                    const size_t bufSize,
                                    size_t *outSize);
    static void unescapeNestedDelim(char *fieldName);

    static void updateFieldValueRange(DfFieldType type,
                                      DfFieldValue curValue,
                                      DfFieldValue *minValue,
                                      DfFieldValue *maxValue,
                                      HashString hashString,
                                      DfFieldValueRangeState range);
    static void replaceFatptrPrefixDelims(char *string);
    static void revertFatptrPrefixDelims(char *string);

    static Status createValidIndicesMap(const XdbMeta *xdbMeta,
                                        unsigned numVariables,
                                        char **variableNames,
                                        bool **validIndicesMap);

    // Miscellaneous functions
    static size_t fieldGetStringFieldSize(const void *srcIn);
    static size_t fieldGetStringFieldSize2(DfFieldValueStringPtr srcIn);
    static Status fieldToUInt64(DfFieldValue fieldVal,
                                DfFieldType fieldType,
                                uint64_t *intOut);
    static Status fieldToUInt64BitCast(DfFieldValue fieldVal,
                                       DfFieldType fieldType,
                                       uint64_t *intOut);
    static Status fieldToProto(DfFieldValue value,
                               DfFieldType type,
                               ProtoFieldValue *protoField);

    MustCheck MustInline static bool fieldTypeIsFixed(DfFieldType fieldType)
    {
        bool ret;

        switch (fieldType) {
        case DfInt32:
        case DfUInt32:
        case DfInt64:
        case DfUInt64:
        case DfFloat32:
        case DfFloat64:
        case DfBoolean:
        case DfNull:
        case DfFatptr:
        case DfOpRowMetaPtr:
            ret = true;
            break;
        case DfBlob:
        case DfString:
        case DfScalarObj:
        case DfMixed:
        case DfUnknown:
        case DfMoney:
        case DfTimespec:
            ret = false;
            break;
        default:
            assert(0);
            ret = false;
        }

        return ret;
    }

    static Status fieldToUIntptr(DfFieldValue fieldVal,
                                 DfFieldType fieldType,
                                 uintptr_t *intOut);
    static Status fieldToInt64(DfFieldValue fieldVal,
                               DfFieldType fieldType,
                               int64_t *intOut);
    static Status fieldToFloat64(DfFieldValue fieldVal,
                                 DfFieldType fieldType,
                                 float64_t *floatOut);
    static Status fieldToNumeric(DfFieldValue fieldVal,
                                 DfFieldType fieldType,
                                 XlrDfp *numericOut);
    MustCheck MustInline static size_t fieldGetSize(DfFieldType fieldType)
    {
        switch (fieldType) {
        case DfInt32:
        case DfUInt32:
        case DfFloat32:
            return sizeof(uint32_t);
            break;
        case DfInt64:
        case DfUInt64:
        case DfFloat64:
        case DfFatptr:
            return sizeof(uint64_t);
            break;
        case DfScalarPtr:
            return sizeof(Scalar *);
            break;
        case DfOpRowMetaPtr:
            return sizeof(uintptr_t);
            break;
        case DfBoolean:
            return sizeof(bool);
            break;
        case DfUnknown:
        case DfNull:
            return 0;
            break;
        default:
            assert(0);
        // fall through
        case DfTimespec:
        case DfBlob:
        case DfString:
        case DfMoney:
        case DfScalarObj:
        case DfMixed:
            break;
        }

        return DfVariableFieldSize;
    }

    static inline size_t fieldGetSize(DfFieldType fieldType, DfFieldValue field)
    {
        size_t cur = fieldGetSize(fieldType);

        if (cur == DfVariableFieldSize) {
            switch (fieldType) {
            case DfString:
                cur = field.stringVal.strSize + sizeof(DfFieldValueString);
                break;
            case DfTimespec:
                cur = sizeof(DfTimeval);
                break;
            case DfMoney:
                cur = sizeof(XlrDfp);
                break;
            case DfScalarObj:
                cur = field.scalarVal->getUsedSize();
                break;
            default:
                assert(0);
                break;
            }
        }

        return cur;
    }
    MustCheck MustInline static Status getFieldValueFromArray(
        const DfFieldValueArray *fields,
        size_t sizeInBytes,
        DfFieldType fieldType,
        unsigned elementNum,
        DfFieldValue *fieldVal)
    {
        Status status;
        unsigned strCount;
        const DfFieldValueString *tmp;

        switch (fieldType) {
        case DfInt32:
            if ((1 + elementNum) * sizeof(fieldVal->int32Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const int32_t *val = fields->int32Val;
                fieldVal->int32Val = val[elementNum];
            }
            break;
        case DfUInt32:
            if ((1 + elementNum) * sizeof(fieldVal->uint32Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const uint32_t *val = fields->uint32Val;
                fieldVal->uint32Val = val[elementNum];
            }
            break;
        case DfInt64:
            if ((1 + elementNum) * sizeof(fieldVal->int64Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const int64_t *val = fields->int64Val;
                fieldVal->int64Val = val[elementNum];
            }
            break;
        case DfUInt64:
        case DfOpRowMetaPtr:
            if ((1 + elementNum) * sizeof(fieldVal->uint64Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const uint64_t *val = fields->uint64Val;
                fieldVal->uint64Val = val[elementNum];
            }
            break;
        case DfMoney:
            if ((1 + elementNum) * sizeof(fieldVal->numericVal) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const XlrDfp *val = fields->numericVal;
                fieldVal->numericVal = val[elementNum];
            }
            break;
        case DfFloat32:
            if ((1 + elementNum) * sizeof(fieldVal->float32Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const float32_t *val = fields->float32Val;
                fieldVal->float32Val = val[elementNum];
            }
            break;
        case DfFloat64:
            if ((1 + elementNum) * sizeof(fieldVal->float64Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const float64_t *val = fields->float64Val;
                fieldVal->float64Val = val[elementNum];
            }
            break;
        case DfBoolean:
            if ((1 + elementNum) * sizeof(fieldVal->boolVal) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const bool *val = fields->boolVal;
                fieldVal->boolVal = val[elementNum];
            }
            break;
        case DfString:
            tmp = &fields->strVals[0];
            strCount = 0;
            for (; (char *) tmp <
                       &(((char *) fields)[sizeInBytes - sizeof(*tmp)]) &&
                   strCount < elementNum;
                 tmp = (DfFieldValueString *) &tmp->strActual[tmp->strSize],
                 strCount++) {
                // empty body
            }

            if ((char *) tmp >=
                &(((char *) fields)[sizeInBytes - sizeof(*tmp)])) {
                assert(0);
                status = StatusOverflow;
            } else {
                fieldVal->stringVal.strSize = tmp->strSize;
                fieldVal->stringVal.strActual = &tmp->strActual[0];
            }
            break;
        case DfTimespec:
            if ((1 + elementNum) * sizeof(fieldVal->timeVal) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                const DfTimeval *val = fields->timeVal;
                fieldVal->timeVal = val[elementNum];
            }
            break;
        case DfUnknown:
        case DfNull:
            fieldVal->boolVal = false;
            break;
        case DfBlob:
        default:
            assert(0);
            status = StatusUnimpl;
        }

        return status;
    }

    MustCheck MustInline static Status setFieldValueInArray(
        DfFieldValueArray *fields,
        size_t sizeInBytes,
        DfFieldType fieldType,
        unsigned elementNum,
        DfFieldValue fieldVal)
    {
        Status status;
        unsigned strCount;
        DfFieldValueString *tmp;
        size_t bytesLeftInBuffer;

        switch (fieldType) {
        case DfInt32:
            if ((1 + elementNum) * sizeof(fieldVal.int32Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->int32Val[elementNum] = fieldVal.int32Val;
            }
            break;
        case DfUInt32:
            if ((1 + elementNum) * sizeof(fieldVal.uint32Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->uint32Val[elementNum] = fieldVal.uint32Val;
            }
            break;
        case DfInt64:
            if ((1 + elementNum) * sizeof(fieldVal.int64Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->int64Val[elementNum] = fieldVal.int64Val;
            }
            break;
        case DfUInt64:
        case DfOpRowMetaPtr:
            if ((1 + elementNum) * sizeof(fieldVal.uint64Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->uint64Val[elementNum] = fieldVal.uint64Val;
            }
            break;
        case DfFloat32:
            if ((1 + elementNum) * sizeof(fieldVal.float32Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->float32Val[elementNum] = fieldVal.float32Val;
            }
            break;
        case DfFloat64:
            if ((1 + elementNum) * sizeof(fieldVal.float64Val) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->float64Val[elementNum] = fieldVal.float64Val;
            }
            break;
        case DfMoney:
            if ((1 + elementNum) * sizeof(fieldVal.numericVal) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->numericVal[elementNum] = fieldVal.numericVal;
            }
            break;
        case DfBoolean:
            if ((1 + elementNum) * sizeof(fieldVal.boolVal) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->boolVal[elementNum] = fieldVal.boolVal;
            }
            break;
        case DfString:
            tmp = &fields->strVals[0];
            strCount = 0;
            for (; (char *) tmp <
                       &(((char *) fields)[sizeInBytes - sizeof(*tmp)]) &&
                   strCount < elementNum;
                 tmp = (DfFieldValueString *) &tmp->strActual[tmp->strSize],
                 strCount++) {
                // empty body
            }

            if ((char *) tmp >=
                &(((char *) fields)[sizeInBytes - sizeof(*tmp)])) {
                status = StatusOverflow;
            } else {
                bytesLeftInBuffer =
                    (size_t)(&(((char *) fields)[sizeInBytes]) - (char *) tmp);
                assert(bytesLeftInBuffer > sizeof(tmp->strSize));
                assert(fieldVal.stringVal.strSize > 0);
                bytesLeftInBuffer -= sizeof(tmp->strSize);
                if (bytesLeftInBuffer < fieldVal.stringVal.strSize) {
                    status = StatusOverflow;
                } else {
                    tmp->strSize = fieldVal.stringVal.strSize;
                    memcpy(&tmp->strActual[0],
                           fieldVal.stringVal.strActual,
                           tmp->strSize - 1);
                    tmp->strActual[tmp->strSize - 1] = '\0';
                }
            }
            break;
        case DfTimespec:
            if ((1 + elementNum) * sizeof(fieldVal.timeVal) > sizeInBytes) {
                status = StatusOverflow;
            } else {
                fields->timeVal[elementNum] = fieldVal.timeVal;
            }
            break;
        case DfNull:
            // Don't actually need to set any value.
            break;
        case DfBlob:
        default:
            assert(0);
            status = StatusUnimpl;
        }

        return status;
    }

    bool fieldAttrHdrsAreEqual(const DfFieldAttrHeader *h1,
                               const DfFieldAttrHeader *h2);
    bool fieldAttrsAreEqual(const DfFieldAttr *f1, const DfFieldAttr *f2);
    static bool fieldIsEqual(const DfFieldType type,
                             const DfFieldValue val1,
                             const DfFieldValue val2);
    static int fieldCompare(const DfFieldType type,
                            const DfFieldValue val1,
                            const DfFieldValue val2);

    static int fieldArrayCompare(unsigned numFields,
                                 Ordering *orderings,
                                 const int *fieldOrderArray1,
                                 const NewTupleMeta *tupMeta1,
                                 const NewTupleValues *t1,
                                 const int *fieldOrderArray2,
                                 const NewTupleMeta *tupMeta2,
                                 const NewTupleValues *t2,
                                 bool nullSafe = true);

    static uint64_t fieldHash(const DfFieldType type, const DfFieldValue val);
    static uint64_t fieldArrayHash(unsigned numFields,
                                   const int *fieldOrderArray,
                                   const NewTupleMeta *tupMeta,
                                   const NewTupleValues *t);

    bool fatptrIsValid(DfRecordFatptr recFatptr);
    NodeId fatptrGetNodeId(DfRecordFatptr recFatptr);
    DfRecordId fatptrGetRecordId(DfRecordFatptr recFatptr);
    bool fatptrIsLocal(DfRecordFatptr recFatptr);
    void fatptrSet(DfRecordFatptr *recFatptr,
                   NodeId nodeId,
                   DfRecordId recordId);
    unsigned fatptrGetNumBits();

    bool isValidAccessor(const Accessor *accessor)
    {
        return (accessor->nameDepth >= 1 &&
                accessor->names[0].type == AccessorName::Type::Field &&
                accessor->names[0].value.field != NULL);
    }

    Status accessField(ReaderRecord *record,
                       const Accessor *accessor,
                       DataValueReader *outValue);

    MustCheck Status getFieldValueFromRecord(DfRecordFatptr fatptr,
                                             DemystifyVariable *variable,
                                             DfFieldValueArray *value,
                                             size_t valueBufSize,
                                             bool *extractedField);

    // XXX fieldPresent will be false on errors like if the value is too
    // large for the value array. The user will see this as an ICV and can
    // correct it then. Note that this isn't a fatal error
    MustCheck Status getFieldValueFromImmediate(const NewTupleValues *tuple,
                                                const NewTupleMeta *tupleMeta,
                                                unsigned valueArrayIdx,
                                                DemystifyVariable *variable,
                                                Scalar *scalar,
                                                bool *extractedField);

    MustCheck Status getFieldValue(const NewKeyValueEntry *srcKvEntry,
                                   const NewTupleMeta *tupleMeta,
                                   DemystifyVariable *variable,
                                   Scalar *outScalar,
                                   bool *extractedField);

    // Canonizes the records in this index to have valid fat pointers
    MustCheck Status addPageIndex(DataPageIndex *pageIndex,
                                  DfRecordId *startRecord);

    MustCheck Status loadDataPagesIntoXdb(XdbId xdbId,
                                          int64_t numPages,
                                          uint8_t **pages,
                                          int64_t pageSize,
                                          const XdbLoadArgs *xdbLoadArgs,
                                          OpStatus *opStatus,
                                          uint64_t *numRecordsInserted);

    MustCheck Status loadFixedSchemaPageIntoXdb(XdbId xdbId,
                                                uint8_t *page,
                                                int64_t pageSize,
                                                const XdbLoadArgs *xdbLoadArgs);

    MustCheck Status populateScalar(Scalar *scalar,
                                    DfFieldValue *fieldVal,
                                    const DataValueReader *dValue,
                                    DfFieldType expectedFieldType,
                                    bool *extractedField,
                                    bool isKey,
                                    XdbMeta *dstMeta);

    void initFieldAttr(DfFieldAttr *fieldAttr);

    // Export Functions
    MustCheck Status
    requiresFullSchema(DfFormatType formatType,
                       const ExInitExportFormatSpecificArgs *formatArgs,
                       bool *requiresFullSchema);
    MustCheck Status
    renderColumnHeaders(DfFormatType formatType,
                        const char *srcTableName,
                        int numColumns,
                        const char **headerColumns,
                        const ExInitExportFormatSpecificArgs *formatArgs,
                        const TupleValueDesc *valueDesc,
                        char *buf,
                        size_t bufSize,
                        size_t *bytesWritten);
    MustCheck Status
    renderPrelude(DfFormatType formatType,
                  const TupleValueDesc *valueDesc,
                  const ExInitExportFormatSpecificArgs *formatArgs,
                  char *buf,
                  size_t bufSize,
                  size_t *bytesWritten);
    MustCheck Status renderFooter(DfFormatType formatType,
                                  char *buf,
                                  size_t bufSize,
                                  size_t *bytesWritten);

    MustCheck IRecordParser *getParser(DfFormatType format);
    MustCheck IRowRenderer *getRowRenderer(DfFormatType format);

  private:
    enum : int32_t {
        RecordNumIdx = -2,
    };

    static constexpr int32_t InitialNumPages = 1023;

    class GlobalIndexStore final
    {
      public:
        GlobalIndexStore();
        ~GlobalIndexStore();

        Status init(int numRangePages, int pageSize);

        Status insert(DataPageIndex *index,
                      uint64_t numRecords,
                      DfRecordId *startRecId);

        const DataPageIndex *lookup(const DfRecordId recordId,
                                    DfRecordId *accessRec);

      private:
        struct RecordIdRange {
            DfRecordId start;
            uint64_t numRecords;
            DataPageIndex *index;
        };

        struct RecordIdRangePage {
            DfRecordId start;
            uint64_t numRecords;

            int size;
            int capacity;

            RecordIdRange *recordIdRanges;
        };

        Mutex lock_;
        uint64_t nextRecordId_;
        int curPage_;
        int maxPages_;
        int pageSize_;

        RecordIdRangePage *rangePages_;

        GlobalIndexStore(const GlobalIndexStore &) = delete;
        GlobalIndexStore &operator=(const GlobalIndexStore &) = delete;
    };

    static DataFormat *instance;
    DataFormat(){};
    ~DataFormat(){};

    DataFormat(const DataFormat &) = delete;
    DataFormat &operator=(const DataFormat &) = delete;

    // recordId is the recordId in the global namespace of records
    // indexRecordId is the record id in the index namespace
    const DataPageIndex *lookupIndex(const DfRecordId recordId,
                                     DfRecordId *indexRecordId);

    int jsonEmitString(char *buf,
                       size_t bufSize,
                       const char *fieldValue,
                       size_t fieldValueSize);
    int jsonEmitField(char *buf,
                      size_t bufSize,
                      const char *fatptrPrefix,
                      const char *fieldNameIn,
                      DfFieldType fieldType,
                      const DfFieldValue *fieldVal);

    Status fatptrToJsonRemote(NodeId nodeId,
                              DfRecordFatptr recFatptr,
                              const char *fatptrPrefix,
                              json_t **fatptrJson);

    GlobalIndexStore segmentStore_;
};

#define TIMESTAMP_STR_LEN 255

Status convertTimestampToStr(const DfFieldValue *inputFieldValue,
                             DfFieldValue *outputFieldValue,
                             size_t outputStrBufSize);

#endif  // _DATAFORMAT_H_
