// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <jansson.h>
#include <new>

#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "hash/Hash.h"
#include "config/Config.h"
#include "operators/OperatorsHash.h"
#include "operators/Dht.h"
#include "operators/Operators.h"
#include "xdb/Xdb.h"
#include "operators/OperatorsXdbPageOps.h"
#include "sys/XLog.h"
#include "dataformat/DataFormatCsv.h"
#include "dataformat/DataFormatSql.h"
#include "dataformat/DataFormatJson.h"
#include "dataformat/DataFormatParquet.h"
#include "util/MemTrack.h"
#include "stat/Statistics.h"
#include "df/DataFormatBaseTypes.h"
#include "DataFormatConstants.h"
#include "dataset/BackingData.h"
#include "datapage/DataPageIndex.h"
#include "operators/XcalarEval.h"
#include "msg/Xid.h"
#include "util/DFPUtils.h"

using namespace df;

static constexpr const char *moduleName = "libdf";
const char *DfFatptrPrefixDelimiter = "::";
const char *DfFatptrPrefixDelimiterReplaced = "--";
DataFormat *DataFormat::instance;

Status
DemystifyVariable::init(const char *name, DfFieldType type)
{
    Status status = StatusOk;
    AccessorNameParser parser;

    status = parser.parseAccessor(name, &this->accessor);
    BailIfFailed(status);

    this->type = type;

CommonExit:
    return status;
}

Status
AccessorNameParser::parseAccessor(const char *name, Accessor *accessorOut)
{
    Status status = StatusOk;
    int nameDepth;
    inName_ = name;
    nameLen_ = strlen(name);

    accessorOut->nameDepth = 0;
    accessorOut->names = NULL;

    if (nameLen_ > (int) DfMaxFieldNameLen) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    status = parsePass(true);
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Error parsing field %s: %s",
                       name,
                       strGetFromStatus(status));

    nameDepth = curNumNames_;

    assert(names_ == NULL);
    names_ = new (std::nothrow) AccessorName[nameDepth];
    BailIfNull(this->names_);

    status = parsePass(false);
    BailIfFailed(status);

    accessorOut->nameDepth = nameDepth;
    accessorOut->names = names_;
    names_ = NULL;

CommonExit:
    if (status != StatusOk) {
        // Don't leave names in an inconsistent state
        if (names_) {
            delete[] names_;
            names_ = NULL;
        }
    }
    return status;
}

Status
AccessorNameParser::parsePass(bool dryRun)
{
    Status status = StatusOk;
    curNumNames_ = 0;
    curLen_ = 0;
    state_ = ParserState::Normal;

    if (nameLen_ == 0) {
        status = StatusAstMalformedEvalString;
        goto CommonExit;
    }

    // Traverse the name, parsing as we go
    for (int ii = 0; ii < nameLen_; ii++) {
        assert(curLen_ <= (int) sizeof(tmpBuf_));
        ParserState nextState;
        char c = inName_[ii];
        switch (state_) {
        case ParserState::Normal:
            switch (c) {
            case DfNestedDelimiter:
                if (curLen_ == 0) {
                    status = StatusAstMalformedEvalString;
                    goto CommonExit;
                }
                status = addFieldAccessor(dryRun);
                BailIfFailed(status);

                nextState = ParserState::Normal;
                break;
            case DfArrayIndexStartDelimiter:
                if (curLen_ == 0) {
                    status = StatusAstMalformedEvalString;
                    goto CommonExit;
                }
                status = addFieldAccessor(dryRun);
                BailIfFailed(status);

                nextState = ParserState::InSubscript;
                break;
            case EscapeChar:
                nextState = ParserState::Escaping;
                break;
            case DfArrayIndexEndDelimiter:
                // This is an invalid character to have here
                status = StatusAstMalformedEvalString;
                goto CommonExit;
                break;
            default:
                tmpBuf_[curLen_] = c;
                ++curLen_;
                nextState = ParserState::Normal;
                break;
            }
            break;
        case ParserState::Escaping:
            switch (c) {
            case DfNestedDelimiter:
            case DfArrayIndexStartDelimiter:
            case DfArrayIndexEndDelimiter:
            case EscapeChar:
                // Escaped values
                tmpBuf_[curLen_] = c;
                ++curLen_;
                nextState = ParserState::Normal;
                break;
            default:
                // Trying to escape anything else is a parse error
                status = StatusAstMalformedEvalString;
                goto CommonExit;
                break;
            }
            break;
        case ParserState::InSubscript:
            switch (c) {
            case DfArrayIndexEndDelimiter:
                status = addSubscriptAccessor(dryRun);
                BailIfFailed(status);

                nextState = ParserState::FinishedSubscript;
                break;
            default:
                if (!isdigit(c)) {
                    status = StatusAstMalformedEvalString;
                    goto CommonExit;
                }
                tmpBuf_[curLen_] = c;
                ++curLen_;
                nextState = ParserState::InSubscript;
            }
            break;
        case ParserState::FinishedSubscript:
            switch (c) {
            case DfNestedDelimiter:
                nextState = ParserState::Normal;
                break;
            case DfArrayIndexStartDelimiter:
                nextState = ParserState::InSubscript;
                break;
            default:
                // nester must follow like someArray[0].thing
                // someArray[0]thing would be an error
                status = StatusAstMalformedEvalString;
                goto CommonExit;
                break;
            }
            break;
        case ParserState::Invalid:  // Fallthrough
        default:
            assert(false);
            status = StatusFailed;
            goto CommonExit;
        }
        state_ = nextState;
    }

    switch (state_) {
    case ParserState::Normal:
        if (curLen_ == 0) {
            status = StatusAstMalformedEvalString;
            goto CommonExit;
        }
        status = addFieldAccessor(dryRun);
        BailIfFailed(status);
        break;
    case ParserState::Escaping:
        status = StatusAstMalformedEvalString;
        goto CommonExit;
        break;
    case ParserState::InSubscript:
        status = StatusAstMalformedEvalString;
        goto CommonExit;
    case ParserState::FinishedSubscript:
        // Accept
        break;
    case ParserState::Invalid:  // Fallthrough
    default:
        assert(false);
        status = StatusFailed;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
AccessorNameParser::addFieldAccessor(bool dryRun)
{
    Status status = StatusOk;
    assert(curLen_ < (int) sizeof(tmpBuf_));
    if (!dryRun) {
        assert(names_ != NULL);
        AccessorName *thisName = &names_[curNumNames_];

        tmpBuf_[curLen_] = '\0';
        thisName->value.field = static_cast<char *>(memAlloc(curLen_ + 1));
        BailIfNull(thisName->value.field);
        verify(strlcpy(thisName->value.field, tmpBuf_, curLen_ + 1) ==
               (size_t) curLen_);
        thisName->type = AccessorName::Type::Field;
    }
    ++curNumNames_;
    curLen_ = 0;

CommonExit:
    return status;
}

Status
AccessorNameParser::addSubscriptAccessor(bool dryRun)
{
    Status status = StatusOk;
    assert(curLen_ < (int) sizeof(tmpBuf_));
    if (!dryRun) {
        assert(names_ != NULL);
        AccessorName *thisName = &names_[curNumNames_];

        tmpBuf_[curLen_] = '\0';

        char *endptr;
        errno = 0;
        int parsedSubscript = strtol(tmpBuf_, &endptr, 10);
        if (endptr == tmpBuf_ || *endptr != '\0' || errno == ERANGE) {
            status = StatusAstMalformedEvalString;
            goto CommonExit;
        }
        assert(endptr != NULL && endptr != tmpBuf_ && *endptr == '\0');
        thisName->value.subscript = parsedSubscript;
        thisName->type = AccessorName::Type::Subscript;
    }
    ++curNumNames_;
    curLen_ = 0;
CommonExit:

    return status;
}

DataFormat::GlobalIndexStore::GlobalIndexStore()
    : nextRecordId_(0), curPage_(0), maxPages_(0), pageSize_(0), rangePages_(0)
{
}

DataFormat::GlobalIndexStore::~GlobalIndexStore()
{
    if (rangePages_) {
        for (int ii = 0; ii <= curPage_; ii++) {
            memFree(rangePages_[ii].recordIdRanges);
            rangePages_[ii].recordIdRanges = NULL;
        }

        memFree(rangePages_);
        rangePages_ = NULL;
    }
}

Status
DataFormat::GlobalIndexStore::init(int numRangePages, int pageSize)
{
    Status status = StatusOk;

    rangePages_ = (RecordIdRangePage *) memAlloc(numRangePages *
                                                 sizeof(RecordIdRangePage));
    BailIfNullWith(rangePages_, StatusNoMem);
    memZero(rangePages_, numRangePages * sizeof(RecordIdRangePage));

    maxPages_ = numRangePages;
    pageSize_ = pageSize;

    rangePages_[0].recordIdRanges =
        (RecordIdRange *) memAlloc(pageSize * sizeof(RecordIdRange));
    BailIfNullWith(rangePages_[0].recordIdRanges, StatusNoMem);

    rangePages_[0].size = 0;
    rangePages_[0].capacity = pageSize;
    rangePages_[0].start = 0;
    rangePages_[0].numRecords = 0;

CommonExit:
    return status;
}

Status
DataFormat::GlobalIndexStore::insert(DataPageIndex *index,
                                     uint64_t numRecords,
                                     DfRecordId *startRecId)
{
    Status status = StatusOk;
    RecordIdRange *thisRange = NULL;
    RecordIdRangePage *curPagePtr = NULL;
    bool allocedNewPage = false;
    lock_.lock();

    assert(curPage_ <= maxPages_);
    assert(rangePages_);

    if (unlikely(curPage_ == maxPages_)) {
        status = StatusMaxFileLimitReached;
        goto CommonExit;
    }

    // Actually perform the insertion
    curPagePtr = &rangePages_[curPage_];

    if (unlikely(curPagePtr->size == curPagePtr->capacity)) {
        curPagePtr = &rangePages_[curPage_ + 1];

        curPagePtr->recordIdRanges =
            (RecordIdRange *) memAlloc(pageSize_ * sizeof(RecordIdRange));
        BailIfNullWith(curPagePtr->recordIdRanges, StatusNoMem);

        curPagePtr->size = 0;
        curPagePtr->capacity = pageSize_;
        curPagePtr->start = nextRecordId_;
        curPagePtr->numRecords = 0;
        allocedNewPage = true;
    }

    thisRange = &curPagePtr->recordIdRanges[curPagePtr->size];

    thisRange->index = index;
    thisRange->numRecords = numRecords;
    thisRange->start = nextRecordId_;

    nextRecordId_ += numRecords;
    curPagePtr->numRecords += numRecords;
    // ensure struct is initialized before we increment our size, allowing
    // readers to access this struct
    memBarrier();

    curPagePtr->size++;
    if (unlikely(allocedNewPage)) {
        curPage_++;
    }

    *startRecId = thisRange->start;

CommonExit:
    lock_.unlock();

    return status;
}

const DataPageIndex *
DataFormat::GlobalIndexStore::lookup(const DfRecordId recordId,
                                     DfRecordId *accessRec)
{
    const RecordIdRange *foundRange = NULL;
    RecordIdRangePage *page = NULL;

    for (int ii = 0; ii <= curPage_; ii++) {
        if (recordId >= rangePages_[ii].start &&
            recordId < rangePages_[ii].start + rangePages_[ii].numRecords) {
            page = &rangePages_[ii];
            break;
        }
    }

    assert(page != NULL);
    if (unlikely(page == NULL)) {
        return NULL;
    }

    int lowerBound = 0;
    int upperBound = page->size;
    int pos = (upperBound - lowerBound) / 2;

    while (true) {
        const RecordIdRange *thisRange = &page->recordIdRanges[pos];
        if (lowerBound >= upperBound) {
            // We didn't find it, quit
            break;
        }
        if (recordId >= thisRange->start &&
            recordId < thisRange->start + thisRange->numRecords) {
            // We found it; we're done
            foundRange = thisRange;
            break;
        }

        if (recordId < thisRange->start) {
            upperBound = pos;
        } else {
            lowerBound = pos + 1;
        }
        pos = (upperBound + lowerBound) / 2;
    }

    *accessRec = recordId - foundRange->start;

    return foundRange->index;
}

Status
DataFormat::init()
{
    Status status;

    void *ptr = NULL;

    ptr = memAllocExt(sizeof(DataFormat), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    DataFormat::instance = new (ptr) DataFormat();

    status = JsonFormatOps::init();
    BailIfFailed(status);
    status = CsvFormatOps::init();
    BailIfFailed(status);
    status = SqlFormatOps::init();
    BailIfFailed(status);
    status = ParquetFormatOps::init();
    BailIfFailed(status);

    DataFormat::instance->importables_[DfFormatUnknown] = NULL;
    DataFormat::instance->importables_[DfFormatJson] = NULL;
    DataFormat::instance->importables_[DfFormatCsv] = CsvFormatOps::get();
    DataFormat::instance->importables_[DfFormatSql] = NULL;
    DataFormat::instance->importables_[DfFormatParquet] =
        ParquetFormatOps::get();

    DataFormat::instance->exportables_[DfFormatUnknown] = NULL;
    DataFormat::instance->exportables_[DfFormatJson] = NULL;
    DataFormat::instance->exportables_[DfFormatCsv] = CsvFormatOps::get();
    DataFormat::instance->exportables_[DfFormatSql] = SqlFormatOps::get();
    DataFormat::instance->exportables_[DfFormatParquet] = NULL;

    DataFormat::instance->fatptrInit(Config::get()->getActiveNodes(),
                                     DfMaxRecords);

    json_set_alloc_funcs(memAllocJson, memFreeJson);

    status =
        DataFormat::instance->segmentStore_.init(NumRangePages, RangePageSize);
    BailIfFailed(status);

CommonExit:
    return status;
}

DataFormat *
DataFormat::get()
{
    assert(instance);
    return instance;
}

void
DataFormat::destroy()
{
    DataFormat *df = DataFormat::get();

    df->~DataFormat();
    memFree(df);

    DataFormat::instance = NULL;
    JsonFormatOps::get()->destroy();
    CsvFormatOps::get()->destroy();
    SqlFormatOps::get()->destroy();
    ParquetFormatOps::get()->destroy();
}

void
DataFormat::freeXdbPage(void *p)
{
    return XdbMgr::get()->bcFree(p);
}

size_t
DataFormat::fieldGetStringFieldSize(const void *srcIn)
{
    const char *src = (const char *) srcIn;

    return strlen(src) + 1;
}

size_t
DataFormat::fieldGetStringFieldSize2(DfFieldValueStringPtr srcIn)
{
    return sizeof(srcIn.strSize) + srcIn.strSize;
}

MustCheck Status
DataFormat::fieldToUInt64(DfFieldValue fieldVal,
                          DfFieldType fieldType,
                          uint64_t *intOut)
{
    Status status;

    switch (fieldType) {
    case DfFloat32:
        *intOut = (uint64_t) fieldVal.float32Val;
        break;
    case DfInt32:
        *intOut = fieldVal.int32Val;
        break;
    case DfUInt32:
        *intOut = fieldVal.uint32Val;
        break;
    case DfFloat64:
        *intOut = (uint64_t) fieldVal.float64Val;
        break;
    case DfMoney: {
        char buf[XLR_DFP_STRLEN];
        DFPUtils *dfp = DFPUtils::get();

        dfp->xlrNumericToString(buf, &fieldVal.numericVal);
        *intOut = (uint64_t) strtoul(buf, NULL, 10);
    } break;
    case DfInt64:
        *intOut = fieldVal.int64Val;
        break;
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        *intOut = fieldVal.uint64Val;
        break;
    case DfBoolean:
        *intOut = fieldVal.boolVal;
        break;
    case DfNull:
        *intOut = false;
        break;
    case DfTimespec:
        *intOut = fieldVal.timeVal.ms;
        break;
    case DfUnknown:
    default:
        assert(0);
    // fall through
    case DfBlob:
    case DfString:
    case DfScalarObj:
    case DfMixed:
        status = StatusOverflow;
    }

    return status;
}

MustCheck Status
DataFormat::fieldToUInt64BitCast(DfFieldValue fieldVal,
                                 DfFieldType fieldType,
                                 uint64_t *intOut)
{
    Status status;

    // std::bit_cast
    switch (fieldType) {
    case DfFloat32:
        memcpy(intOut, &fieldVal.float32Val, sizeof fieldVal.float32Val);
        break;
    case DfInt32:
        memcpy(intOut, &fieldVal.int32Val, sizeof fieldVal.int32Val);
        break;
    case DfUInt32:
        memcpy(intOut, &fieldVal.uint32Val, sizeof fieldVal.uint32Val);
        break;
    case DfFloat64:
        memcpy(intOut, &fieldVal.float64Val, sizeof fieldVal.float64Val);
        break;
    case DfMoney:
        memcpy(intOut,
               &fieldVal.numericVal.ieee[0],
               sizeof fieldVal.numericVal.ieee[0]);
        break;
    case DfInt64:
        memcpy(intOut, &fieldVal.int64Val, sizeof fieldVal.int64Val);
        break;
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        memcpy(intOut, &fieldVal.uint64Val, sizeof fieldVal.uint64Val);
        break;
    case DfBoolean:
        memcpy(intOut, &fieldVal.boolVal, sizeof fieldVal.boolVal);
        break;
    case DfNull:
        *intOut = 0;
        break;
    case DfTimespec:
        memcpy(intOut, &fieldVal.timeVal.ms, sizeof fieldVal.timeVal.ms);
        break;
    case DfUnknown:
    default:
        assert(0);
    // fall through
    case DfBlob:
    case DfString:
    case DfScalarObj:
    case DfMixed:
        status = StatusOverflow;
    }

    return status;
}

MustCheck Status
DataFormat::fieldToUIntptr(DfFieldValue fieldVal,
                           DfFieldType fieldType,
                           uintptr_t *intOut)
{
    Status status;

    switch (fieldType) {
    case DfFloat32:
    case DfInt32:
    case DfUInt32:
        *intOut = fieldVal.uint32Val;
        break;
    case DfFloat64:
    case DfInt64:
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        *intOut = fieldVal.uint64Val;
        break;
    case DfBoolean:
        *intOut = fieldVal.boolVal;
        break;
    case DfNull:
        *intOut = false;
        break;
    case DfTimespec:
        *intOut = fieldVal.timeVal.ms;
        break;
    case DfUnknown:
    default:
        assert(0);
    // fall through
    case DfBlob:
    case DfMoney:
    case DfString:
    case DfScalarObj:
    case DfMixed:
        status = StatusOverflow;
    }

    return status;
}

MustCheck Status
DataFormat::fieldToInt64(DfFieldValue fieldVal,
                         DfFieldType fieldType,
                         int64_t *intOut)
{
    Status status;

    switch (fieldType) {
    case DfFloat32:
        *intOut = (int64_t) fieldVal.float32Val;
        break;
    case DfInt32:
        *intOut = fieldVal.int32Val;
        break;
    case DfUInt32:
        *intOut = fieldVal.uint32Val;
        break;
    case DfFloat64:
        *intOut = (int64_t) fieldVal.float64Val;
        break;
    case DfMoney: {
        char buf[XLR_DFP_STRLEN];
        DFPUtils *dfp = DFPUtils::get();

        dfp->xlrNumericToString(buf, &fieldVal.numericVal);
        *intOut = (int64_t) strtol(buf, NULL, 10);
    } break;
    case DfInt64:
        *intOut = fieldVal.int64Val;
        break;
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        *intOut = fieldVal.uint64Val;
        break;
    case DfBoolean:
        *intOut = fieldVal.boolVal;
        break;
    case DfNull:
        *intOut = false;
        break;
    case DfTimespec:
        *intOut = fieldVal.timeVal.ms;
        break;
    case DfUnknown:
    default:
        assert(0);
    // fall through
    case DfBlob:
    case DfString:
    case DfScalarObj:
    case DfMixed:
        status = StatusOverflow;
    }

    return status;
}

MustCheck Status
DataFormat::fieldToFloat64(DfFieldValue fieldVal,
                           DfFieldType fieldType,
                           float64_t *floatOut)
{
    Status status;

    switch (fieldType) {
    case DfFloat32:
        *floatOut = fieldVal.float32Val;
        break;
    case DfInt32:
        *floatOut = fieldVal.int32Val;
        break;
    case DfUInt32:
        *floatOut = fieldVal.uint32Val;
        break;
    case DfFloat64:
        *floatOut = fieldVal.float64Val;
        break;
    case DfMoney:
        status = DFPUtils::get()->xlrDfpNumericToFloat64(floatOut,
                                                         &fieldVal.numericVal);
        assert(status == StatusOk);
        break;
    case DfInt64:
        *floatOut = (float64_t) fieldVal.int64Val;
        break;
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        *floatOut = (float64_t) fieldVal.uint64Val;
        break;
    case DfBoolean:
        *floatOut = fieldVal.boolVal;
        break;
    case DfNull:
        *floatOut = false;
        break;
    case DfTimespec:
        *floatOut = (float64_t) fieldVal.timeVal.ms;
        break;
    case DfUnknown:
    default:
        assert(0);
        // fall through
    case DfBlob:
    case DfString:
    case DfScalarObj:
    case DfMixed:
        status = StatusOverflow;
    }

    return status;
}

MustCheck Status
DataFormat::fieldToNumeric(DfFieldValue fieldVal,
                           DfFieldType fieldType,
                           XlrDfp *numericOut)
{
    Status status;
    DFPUtils *dfp = DFPUtils::get();

    switch (fieldType) {
    case DfMoney:
        *numericOut = fieldVal.numericVal;
        break;
    case DfInt32:
        dfp->xlrDfpInt32ToNumeric(numericOut, fieldVal.int32Val);
        break;
    case DfUInt32:
        dfp->xlrDfpUInt32ToNumeric(numericOut, fieldVal.uint32Val);
        break;
    case DfInt64:
        status = dfp->xlrDfpInt64ToNumeric(numericOut, fieldVal.int64Val);
        BailIfFailed(status);
        break;
    case DfUInt64:
        status = dfp->xlrDfpUInt64ToNumeric(numericOut, fieldVal.uint64Val);
        BailIfFailed(status);
        break;
    case DfBoolean:
        dfp->xlrDfpUInt32ToNumeric(numericOut, fieldVal.boolVal ? 1 : 0);
        break;
    case DfNull:
        dfp->xlrDfpUInt32ToNumeric(numericOut, 0);
        break;
    case DfString:
        status =
            dfp->xlrNumericFromString(numericOut, fieldVal.stringVal.strActual);
        BailIfFailed(status);
        break;
    case DfUnknown:
    default:
        assert(0);
        // fall through
    case DfTimespec:
    case DfFatptr:
    case DfOpRowMetaPtr:
    case DfFloat64:
    case DfFloat32:
    case DfBlob:
    case DfScalarObj:
    case DfMixed:
        status = StatusOverflow;
    }

CommonExit:
    return status;
}

// XXX FIXME should fields be DfFieldValueArray *?
bool
DataFormat::fieldAttrHdrsAreEqual(const DfFieldAttrHeader *h1,
                                  const DfFieldAttrHeader *h2)
{
    // can't simply use memcmp() since junk may be present after the '\0'
    // terminator in the name strings
    return strcmp(h1->name, h2->name) == 0 && h1->type == h2->type;
}

bool
DataFormat::fieldAttrsAreEqual(const DfFieldAttr *f1, const DfFieldAttr *f2)
{
    // can't simply use memcmp() since junk may be present after the nul
    // terminator in the origName and userName strings
    return fieldAttrHdrsAreEqual(&f1->header, &f2->header) &&
           f1->fieldSize == f2->fieldSize && f1->valueSize == f2->valueSize &&
           f1->numValues == f2->numValues;
}

bool
DataFormat::fieldIsEqual(const DfFieldType type,
                         const DfFieldValue val1,
                         const DfFieldValue val2)
{
    switch (type) {
    case DfFloat32:
        return val1.float32Val == val2.float32Val;
        break;
    case DfInt32:
        return val1.int32Val == val2.int32Val;
        break;
    case DfUInt32:
        return val1.uint32Val == val2.uint32Val;
        break;
    case DfFloat64:
        return val1.float64Val == val2.float64Val;
        break;
    case DfInt64:
        return val1.int64Val == val2.int64Val;
        break;
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        return val1.uint64Val == val2.uint64Val;
        break;
    case DfMoney:
        return DFPUtils::get()->xlrDfp64Cmp(&val1.numericVal,
                                            &val2.numericVal) == 0;
    case DfBoolean:
        return val1.boolVal == val2.boolVal;
        break;
    case DfNull:
        return true;
        break;
    case DfTimespec:
        return val1.timeVal.ms == val2.timeVal.ms;
        break;
    case DfScalarObj:
        return (val1.scalarVal->fieldAllocedSize ==
                val2.scalarVal->fieldAllocedSize) &&
               (memcmp(val1.scalarVal,
                       val2.scalarVal,
                       val1.scalarVal->fieldAllocedSize) == 0);
        break;
    case DfString:
        return val1.stringVal.strSize == val2.stringVal.strSize &&
               strcmp(val1.stringVal.strActual, val2.stringVal.strActual) == 0;
        break;
    case DfBlob:
    case DfMixed:
    case DfUnknown:
    default:
        assert(0);
    }

    return false;
}

// returns -1 if val1 < val2
// returns 0 if val1 == val2
// returns 1 if val1 > val2
int
DataFormat::fieldCompare(const DfFieldType type,
                         const DfFieldValue val1,
                         const DfFieldValue val2)
{
    switch (type) {
    case DfFloat32:
        if (val1.float32Val < val2.float32Val) {
            return -1;
        } else if (val1.float32Val > val2.float32Val) {
            return 1;
        }
        break;
    case DfInt32:
        if (val1.int32Val < val2.int32Val) {
            return -1;
        } else if (val1.int32Val > val2.int32Val) {
            return 1;
        }
        break;
    case DfUInt32:
        if (val1.uint32Val < val2.uint32Val) {
            return -1;
        } else if (val1.uint32Val > val2.uint32Val) {
            return 1;
        }
        break;
    case DfFloat64:
        if (val1.float64Val < val2.float64Val) {
            return -1;
        } else if (val1.float64Val > val2.float64Val) {
            return 1;
        }
        break;
    case DfInt64:
        if (val1.int64Val < val2.int64Val) {
            return -1;
        } else if (val1.int64Val > val2.int64Val) {
            return 1;
        }
        break;
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
        if (val1.uint64Val < val2.uint64Val) {
            return -1;
        } else if (val1.uint64Val > val2.uint64Val) {
            return 1;
        }
        break;
    case DfBoolean:
        if (val1.boolVal < val2.boolVal) {
            return -1;
        } else if (val1.boolVal > val2.boolVal) {
            return 1;
        }
        break;
    case DfNull:
        break;
    case DfTimespec:
        if (val1.timeVal.ms < val2.timeVal.ms) {
            return -1;
        } else if (val1.timeVal.ms > val2.timeVal.ms) {
            return 1;
        }
        break;
    case DfMoney:
        return DFPUtils::get()->xlrDfp64Cmp(&val1.numericVal, &val2.numericVal);
    case DfScalarPtr:
    case DfScalarObj:
        return val1.scalarVal->compare(val2.scalarVal);
        break;
    case DfString:
        return strcmp(val1.stringVal.strActual, val2.stringVal.strActual);
        break;
    case DfUnknown:
        break;
    case DfBlob:
    case DfMixed:
    default:
        assert(0);
    }

    return 0;
}

int
DataFormat::fieldArrayCompare(unsigned numFields,
                              Ordering *orderings,
                              const int *fieldOrderArray1,
                              const NewTupleMeta *tupMeta1,
                              const NewTupleValues *t1,
                              const int *fieldOrderArray2,
                              const NewTupleMeta *tupMeta2,
                              const NewTupleValues *t2,
                              bool nullSafe)
{
    int ret = 0;
    int idx1, idx2;
    size_t numFields1 = tupMeta1->getNumFields();
    size_t numFields2 = tupMeta2->getNumFields();

    for (unsigned ii = 0; ii < numFields; ii++) {
        if (fieldOrderArray1) {
            idx1 = fieldOrderArray1[ii];
        } else {
            idx1 = ii;
        }

        if (fieldOrderArray2) {
            idx2 = fieldOrderArray2[ii];
        } else {
            idx2 = ii;
        }

        if (idx1 == NewTupleMeta::DfInvalidIdx ||
            idx2 == NewTupleMeta::DfInvalidIdx) {
            continue;
        }

        bool retIsValid1, retIsValid2;
        DfFieldType type1, type2;
        DfFieldValue value1, value2;

        assert((size_t) idx1 < numFields1);
        type1 = tupMeta1->getFieldType(idx1);
        value1 = t1->get(idx1, numFields1, type1, &retIsValid1);

        assert((size_t) idx2 < numFields2);
        type2 = tupMeta2->getFieldType(idx2);
        value2 = t2->get(idx2, numFields2, type2, &retIsValid2);

        if (!retIsValid1 && !retIsValid2) {
            if (!nullSafe) {
                ret = -1;
            } else {
                ret = 0;
            }
        } else if (!retIsValid1) {
            ret = -1;
        } else if (!retIsValid2) {
            ret = 1;
        } else {
            assert(type1 == type2);
            ret = DataFormat::fieldCompare(type1, value1, value2);
        }

        if (orderings != NULL && orderings[ii] & DescendingFlag) {
            // invert the result
            ret = -ret;
        }

        if (ret != 0) {
            break;
        }
    }

    return ret;
}

uint64_t
DataFormat::fieldHash(const DfFieldType type, const DfFieldValue val)
{
    Status status;
    uint64_t ret = 0;
    DfFieldValue fieldVal;

    switch (type) {
    case DfFloat32:
    case DfInt32:
    case DfUInt32:
    case DfFloat64:
    case DfInt64:
    case DfUInt64:
    case DfFatptr:
    case DfOpRowMetaPtr:
    case DfBoolean:
    case DfNull:
    case DfTimespec:
    case DfMoney:
        status = fieldToUInt64BitCast(val, type, &ret);
        assert(status == StatusOk);
        break;
    case DfScalarPtr:
    case DfScalarObj:
        status = val.scalarVal->getValue(&fieldVal);
        assert(status == StatusOk);
        ret = fieldHash(val.scalarVal->fieldType, fieldVal);
        break;
    case DfString:
        ret = hashStringFast(val.stringVal.strActual);
        break;
    case DfUnknown:
        break;
    case DfBlob:
    case DfMixed:
    default:
        assert(0);
    }

    return ret;
}

uint64_t
DataFormat::fieldArrayHash(unsigned numFields,
                           const int *fieldOrderArray,
                           const NewTupleMeta *tupMeta,
                           const NewTupleValues *t)
{
    uint64_t ret = 0;

    for (size_t ii = 0; ii < numFields; ++ii) {
        int idx = fieldOrderArray ? fieldOrderArray[ii] : ii;
        if (idx == NewTupleMeta::DfInvalidIdx) {
            continue;
        }
        assert((size_t) idx < tupMeta->getNumFields());

        DfFieldType type = tupMeta->getFieldType(idx);
        bool retIsValid = false;
        DfFieldValue value = t->get(idx, numFields, type, &retIsValid);

        if (retIsValid) {
            hashCombine(ret, DataFormat::fieldHash(type, value));
        } else {
            hashCombine(ret, 0);
        }
    }

    return ret;
}

bool
DataFormat::fatptrIsValid(DfRecordFatptr recFatptr)
{
    uint64_t magic;

    if (recordFatptrNumBits_ <= DfRecordMagicShift) {
        magic = recFatptr >> DfRecordMagicShift;
        return magic == DfRecordFatptrMagic;
    }

    return true;
}

NodeId
DataFormat::fatptrGetNodeId(DfRecordFatptr recFatptr)
{
    assert(fatptrIsValid(recFatptr));

    return (NodeId)(recFatptr & recordFatptrNodeIdMask_) >>
           recordFatptrNodeIdShift_;
}

DfRecordId
DataFormat::fatptrGetRecordId(DfRecordFatptr recFatptr)
{
    assert(fatptrIsValid(recFatptr));

    return (DfRecordId)((recFatptr & recordFatptrRecIdMask_) >>
                        recordFatptrRecIdShift_);
}

bool
DataFormat::fatptrIsLocal(DfRecordFatptr recFatptr)
{
    assert(fatptrIsValid(recFatptr));

    return fatptrGetNodeId(recFatptr) == (NodeId) Config::get()->getMyNodeId();
}

void NonNull(2) DataFormat::fatptrSet(DfRecordFatptr *recFatptr,
                                      NodeId nodeId,
                                      DfRecordId recordId)
{
    assert(recordId < DfMaxRecords);

    *recFatptr = (((DfRecordFatptr) nodeId) << recordFatptrNodeIdShift_);
    *recFatptr |= (((DfRecordFatptr) recordId) << recordFatptrRecIdShift_);

#ifndef NDEBUG
    if (recordFatptrNumBits_ <= DfRecordMagicShift) {
        assert(recordFatptrNumBits_ <= DfRecordMagicShift);
        *recFatptr |= DfRecordFatptrMagic << DfRecordMagicShift;
    }
#endif

    assert(fatptrGetNodeId(*recFatptr) == nodeId);
    assert(fatptrGetRecordId(*recFatptr) == recordId);
    assert(fatptrIsValid(*recFatptr));
}

unsigned
DataFormat::fatptrGetNumBits()
{
    return recordFatptrNumBits_;
}

Status
DataFormat::getFieldValue(const NewKeyValueEntry *srcKvEntry,
                          const NewTupleMeta *tupleMeta,
                          DemystifyVariable *variable,
                          Scalar *outScalar,
                          bool *extractedField)
{
    Status status = StatusOk;
    *extractedField = false;
    int ii;
    size_t numFields = tupleMeta->getNumFields();

    for (ii = 0; ii < (int) numFields; ii++) {
        if (!variable->possibleEntries[ii]) {
            continue;
        }

        DfFieldValue valueTmp;
        bool retIsValid;
        DfFieldType typeTmp = tupleMeta->getFieldType(ii);
        valueTmp = srcKvEntry->tuple_.get((size_t) ii,
                                          numFields,
                                          typeTmp,
                                          &retIsValid);
        if (!retIsValid) {
            continue;
        }

        if (typeTmp == DfFatptr) {
            // Fatptr case
            status = getFieldValueFromRecord(valueTmp.fatptrVal,
                                             variable,
                                             &outScalar->fieldVals,
                                             outScalar->fieldAllocedSize,
                                             extractedField);
            BailIfFailed(status);
            if (*extractedField) {
                assert(variable->type != DfUnknown);
                outScalar->fieldNumValues = 1;
                outScalar->fieldType = variable->type;
                variable->isImmediate = false;
                // TODO getFieldValueFromRecord should be changed to return
                // fieldValue size as well
                outScalar->updateUsedSize();
                break;
            }
        } else {
            // Immediate case
            status = getFieldValueFromImmediate(&srcKvEntry->tuple_,
                                                tupleMeta,
                                                ii,
                                                variable,
                                                outScalar,
                                                extractedField);
            BailIfFailed(status);
            if (*extractedField) {
                variable->isImmediate = true;
                break;
            }
        }
    }

    if (*extractedField) {
        variable->valueArrayIdx = ii;
    }
CommonExit:
    // If we weren't successfuly in extracting the field from any fatptrs or
    // immediates or we encountered a fatal error
    if (!*extractedField || status != StatusOk) {
        outScalar->fieldUsedSize = 0;
        outScalar->fieldNumValues = 0;
    }
    return status;
}

Status
DataFormat::getFieldValueFromRecord(DfRecordFatptr fatptr,
                                    DemystifyVariable *variable,
                                    DfFieldValueArray *value,
                                    size_t valueBufSize,
                                    bool *extractedField)
{
    Status status = StatusOk;
    DfRecordId recordId;
    const DataPageIndex *index;
    ReaderRecord record;
    DfRecordId indexRecId = 0;
    DataValueReader dFieldValue;
    DfFieldType fieldType;

    assert(value != NULL);

    if (fatptrGetNodeId(fatptr) != (NodeId) Config::get()->getMyNodeId()) {
        *extractedField = false;
        return StatusOk;
    }

    recordId = fatptrGetRecordId(fatptr);
    index = lookupIndex(recordId, &indexRecId);
    assert(index != NULL);

    index->getRecordByNum(indexRecId, &record);

    // XXX - get field by index instead; nontrivial since index differs per page
    status = accessField(&record, &variable->accessor, &dFieldValue);
    if (unlikely(status == StatusDfFieldNoExist)) {
        *extractedField = false;
        status = StatusOk;
        goto CommonExit;
    } else if (unlikely(status != StatusOk)) {
        *extractedField = false;
        goto CommonExit;
    }
    *extractedField = true;

    status =
        dFieldValue.getAsFieldValueInArray(valueBufSize, value, 0, &fieldType);
    if (unlikely(status == StatusDfFieldNoExist)) {
        *extractedField = false;
        status = StatusOk;
        goto CommonExit;
    } else if (unlikely(status != StatusOk)) {
        *extractedField = false;
        goto CommonExit;
    }

    variable->type = fieldType;

CommonExit:
    return status;
}

Status
DataFormat::accessField(ReaderRecord *record,
                        const Accessor *accessor,
                        DataValueReader *outValue)
{
    Status status = StatusOk;

    // Reject invalid names
    if (!isValidAccessor(accessor)) {
        status = StatusDfFieldNoExist;
        goto CommonExit;
    }

    if (accessor->nameDepth == 1) {
        status = record->getFieldByName(accessor->names[0].value.field,
                                        outValue,
                                        NULL);
        BailIfFailed(status);
    } else {
        DataValueReader tmp;
        status = tmp.initProtoValue();
        BailIfFailed(status);

        // Get the top level value into a temporary value
        status =
            record->getFieldByName(accessor->names[0].value.field, &tmp, NULL);
        BailIfFailed(status);
        status = tmp.accessNestedField(accessor, outValue);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

// Please note that this function will only getFieldValues from local
// fatptr. Please use dfFatptrBatchIssue if there could be non-local
// fatptrs in your value array
Status
DataFormat::getFieldValueFromImmediate(const NewTupleValues *tuple,
                                       const NewTupleMeta *tupleMeta,
                                       unsigned valueArrayIdx,
                                       DemystifyVariable *variable,
                                       Scalar *scalar,
                                       bool *extractedField)
{
    Status status;
    size_t numFields = tupleMeta->getNumFields();

    *extractedField = true;

    assert(tuple != NULL);
    assert(tupleMeta != NULL);
    assert(scalar != NULL);
    assert(valueArrayIdx < numFields);

    DfFieldType fieldType = tupleMeta->getFieldType(valueArrayIdx);
    bool retIsValid;
    DfFieldValue fieldVal =
        tuple->get((size_t) valueArrayIdx, numFields, fieldType, &retIsValid);
    assert(retIsValid);

    status = scalar->setValue(fieldVal, fieldType);
    if (!status.ok()) {
        // Note that this isn't exactly true; the field is present, but
        // we are unable to access it because it overflows the value
        *extractedField = false;
        variable->type = fieldType;
        return StatusOk;
    } else {
        variable->type = scalar->fieldType;
    }

    return status;
}

// XPUs pack the pages with schema ordering as present in
// XdbLoadArgs as XdbLoadArgs is pushed into xpus to maintain
// column order and type.
Status
DataFormat::loadFixedSchemaPageIntoXdb(XdbId xdbId,
                                       uint8_t *page,
                                       int64_t pageSize,
                                       const XdbLoadArgs *xdbLoadArgs)
{
    Status status;
    NewKeyValueEntry kvEntry;

    NewTupleMeta *dstTupMeta;
    XdbMeta *dstMeta = NULL;
    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    XdbMgr *xdbMgr = XdbMgr::get();
    Operators *op = Operators::get();
    Dht *dht;
    size_t numFields;

    NewTuplesBuffer *tupBuf = (NewTuplesBuffer *) page;
    tupBuf->deserialize();
    NewTuplesCursor tupCursor(tupBuf);

    status = xdbMgr->xdbGet(xdbId, NULL, &dstMeta);
    BailIfFailed(status);
    dstTupMeta = (NewTupleMeta *) dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    new (&kvEntry) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);
    numFields = kvEntry.kvMeta_->tupMeta_->getNumFields();

    dht = DhtMgr::get()->dhtGetDht(dstMeta->dhtId);
    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               XdbInsertRandomHash);
    BailIfFailed(status);
    insertHandleInited = true;

    while ((status = tupCursor.getNext(dstTupMeta, &kvEntry.tuple_)) ==
           StatusOk) {
        DfFieldValue key;
        bool keyValid;
        if (xdbLoadArgs->keyIndex == NewTupleMeta::DfInvalidIdx) {
            keyValid = false;
        } else {
            key = kvEntry.tuple_.get(xdbLoadArgs->keyIndex,
                                     numFields,
                                     kvEntry.kvMeta_->tupMeta_->getFieldType(
                                         xdbLoadArgs->keyIndex),
                                     &keyValid);
            if (unlikely(dstMeta->keyAttr[0].type == DfUnknown)) {
                // This should not be valid in optimized execution,
                // so failing in debug build to catch optimizer bugs if any
                assert(false);
                DfFieldType keyType = kvEntry.kvMeta_->tupMeta_->getFieldType(
                    xdbLoadArgs->keyIndex);
                xSyslog(moduleName,
                        XlogInfo,
                        "Setting type of key %s to %s",
                        xdbLoadArgs->keyName,
                        strGetFromDfFieldType(keyType));
                status = xdbMgr->xdbSetKeyType(dstMeta, keyType, 0);
                BailIfFailed(status);
            }
        }

        status =
            op->hashAndSendKv(insertHandle.loadInfo->transPageHandle,
                              (TransportPage **) insertHandle.node->transPages,
                              NULL,
                              NULL,
                              1,
                              &dstMeta->keyAttr[0].type,
                              &key,
                              &keyValid,
                              &kvEntry,
                              &insertHandle,
                              dht,
                              TransportPageType::SendSourcePage,
                              DemystifyMgr::Op::Index);
        BailIfFailed(status);
    }
    if (status == StatusNoData) {
        // we are done cursoring
        status = StatusOk;
    }

CommonExit:
    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }
    return status;
}

// This is a sub-optimal case where we dealing with modeling
// env where we deal with FatPtrs along with Objects/Array types.
// which is why we deal with data pages which supports unstructured
// data packing.
// XXX Need a way to assign an identifier for field names in DataPage,
// so each DataPage need not waste space for keeping the fieldNames.
Status
DataFormat::loadDataPagesIntoXdb(XdbId xdbId,
                                 int64_t numPages,
                                 uint8_t **pages,
                                 int64_t pageSize,
                                 const XdbLoadArgs *xdbLoadArgs,
                                 OpStatus *opStatus,
                                 uint64_t *numRecordsInserted)
{
    uint64_t recordsRead = 0;
    NewKeyValueEntry kvEntry;
    Status status;
    Scalar **scratchPadScalars = NULL;
    DemystifyVariable *variables = NULL;
    unsigned numScalarsAlloced = 0;

    typedef struct {
        int recordFieldIdx;
        int foiIdx;
    } RecordToFOIMapEntry;
    size_t numRecordToFOIMapping = 0;
    RecordToFOIMapEntry *recordToFOIMapping = NULL;

    // Which fields are the special "RecordNum" and not an actual FOI from
    // dataset
    int64_t *recordFieldIndices = NULL;

    NewTupleMeta *dstTupMeta;
    Xdb *dstXdb = NULL;
    XdbMeta *dstMeta = NULL;
    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    XdbMgr *xdbMgr = XdbMgr::get();
    Operators *op = Operators::get();
    ReaderRecord record;
    DataPageReader reader;
    DfRecordId startRec = 0;

    Dht *dht;
    XcalarEval *eval = XcalarEval::get();
    XcalarEvalClass1Ast ast;
    int *evalArgIndices = NULL;
    bool astInited = false;
    unsigned jj, kk;

    DfFieldValue fieldVal = DfFieldValueNull;

    if (xdbLoadArgs->evalString[0] != '\0') {
        status = eval->generateClass1Ast(xdbLoadArgs->evalString, &ast);
        BailIfFailed(status);
        astInited = true;

        evalArgIndices = (int *) memAlloc(ast.astCommon.numScalarVariables *
                                          sizeof(*evalArgIndices));
        BailIfNull(evalArgIndices);

        for (jj = 0; jj < ast.astCommon.numScalarVariables; jj++) {
            char *varName = ast.astCommon.scalarVariables[jj].variableName;

            for (kk = 0; kk < xdbLoadArgs->fieldNamesCount; kk++) {
                if (strcmp(xdbLoadArgs->fieldNames[kk], varName) == 0) {
                    break;
                }
            }

            evalArgIndices[jj] = kk;
        }
    }

    scratchPadScalars = (Scalar **) memAlloc(sizeof(*scratchPadScalars) *
                                             xdbLoadArgs->fieldNamesCount);
    if (unlikely(scratchPadScalars == NULL)) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate scratchPadScalars (numFields: "
                "%u)",
                xdbLoadArgs->fieldNamesCount);
        status = StatusNoMem;
        goto CommonExit;
    }

    variables =
        new (std::nothrow) DemystifyVariable[xdbLoadArgs->fieldNamesCount];
    if (unlikely(variables == NULL)) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate variables (numFields: %u)",
                xdbLoadArgs->fieldNamesCount);
        status = StatusNoMem;
        goto CommonExit;
    }

    recordFieldIndices = (int64_t *) memAlloc(sizeof(*recordFieldIndices) *
                                              xdbLoadArgs->fieldNamesCount);
    if (unlikely(recordFieldIndices == NULL)) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate recordFieldIndices (numFields:"
                " %u",
                xdbLoadArgs->fieldNamesCount);
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xdbMgr->xdbGet(xdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);

    dht = DhtMgr::get()->dhtGetDht(dstMeta->dhtId);

    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               XdbInsertRandomHash);
    BailIfFailed(status);
    insertHandleInited = true;

    dstTupMeta = (NewTupleMeta *) dstMeta->kvNamedMeta.kvMeta_.tupMeta_;

    numScalarsAlloced = 0;
    for (int64_t ii = 0; ii < xdbLoadArgs->fieldNamesCount; ii++) {
        scratchPadScalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        if (scratchPadScalars[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numScalarsAlloced++;

        recordFieldIndices[ii] = InvalidIdx;

        // Check to see if we want the special field 'recordNum'
        if (xdbLoadArgs->fieldNames[ii][0] == '\0') {
            // XXX this is sometimes the empty string; ignore it
            continue;
        }
        const char *prefixPtr, *fieldName;

        prefixPtr = strstr(xdbLoadArgs->fieldNames[ii],
                           DfFatptrPrefixDelimiterReplaced);
        if (prefixPtr != NULL) {
            fieldName = prefixPtr + strlen(DfFatptrPrefixDelimiterReplaced);
        } else {
            fieldName = xdbLoadArgs->fieldNames[ii];
        }

        if (strstr(fieldName, DsDefaultDatasetKeyName)) {
            recordFieldIndices[ii] = RecordNumIdx;
        }

        status =
            variables[ii].init(fieldName, xdbLoadArgs->valueDesc.valueType[ii]);
        BailIfFailed(status);
    }

    new (&kvEntry) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);
    int64_t workCounter;
    size_t numFields;
    numFields = kvEntry.kvMeta_->tupMeta_->getNumFields();

    for (int64_t pageNum = 0; pageNum < numPages; pageNum++) {
        int32_t numFieldsInRecord;
        workCounter = 0;

        reader.init(pages[pageNum], pageSize);

        if (recordToFOIMapping != NULL) {
            // Every page has a potentially different set of columns
            memFree(recordToFOIMapping);
            recordToFOIMapping = NULL;
        }

        numFieldsInRecord = reader.getNumFields();

        size_t maxNumRecordToFOIMapping;
        maxNumRecordToFOIMapping =
            xcMin(xdbLoadArgs->fieldNamesCount * numFields,
                  xdbLoadArgs->fieldNamesCount *
                      (size_t) ValueType::NumValueTypes);

        recordToFOIMapping = (typeof(recordToFOIMapping)) memAlloc(
            sizeof(*recordToFOIMapping) * maxNumRecordToFOIMapping);
        if (unlikely(recordToFOIMapping == NULL)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate FOI mapping "
                    "(numFOIinXdbLoadArgs: %u, numFields: %lu, "
                    "maxNumRecordToFOIMapping: %lu)",
                    xdbLoadArgs->fieldNamesCount,
                    numFields,
                    maxNumRecordToFOIMapping);
            status = StatusNoBufs;
            goto CommonExit;
        }

        int curMappingIdx = 0;
        for (int32_t ii = 0; ii < numFieldsInRecord; ii++) {
            // The invariant is that every FOI maps to at most 1 field in a
            // record.
            if (curMappingIdx >= (int) maxNumRecordToFOIMapping) {
                // And thus we've found all the FOIs we're looking for
                break;
            }

            const char *fieldName;
            ValueType fieldType;
            status = reader.getFieldMeta(ii, &fieldName, &fieldType);
            recordToFOIMapping[curMappingIdx].recordFieldIdx = ii;
            recordToFOIMapping[curMappingIdx].foiIdx = InvalidIdx;
            for (int64_t jj = 0; jj < xdbLoadArgs->fieldNamesCount; jj++) {
                const Accessor *accessor = &variables[jj].accessor;
                if (isValidAccessor(accessor) &&
                    strcmp(accessor->names[0].value.field, fieldName) == 0) {
                    // We don't support extracting complex types right
                    // now
                    if ((fieldType == ValueType::Array ||
                         fieldType == ValueType::Object) &&
                        accessor->nameDepth == 1) {
                        continue;
                    }

                    recordToFOIMapping[curMappingIdx].recordFieldIdx = ii;
                    recordToFOIMapping[curMappingIdx].foiIdx = jj;
                    curMappingIdx++;
                }
            }
        }
        numRecordToFOIMapping = curMappingIdx;

        // Here's an example of using the recordsToFOIMapping
        // Suppose we have a record schema that looks like this
        // {
        //  "b": ["string", "null"]
        //  "cols": {
        //     "array": ["int", "int", "int"]
        //  },
        //  "a": {
        //      "b": ["string", "null", "int"]
        //  },
        //  "a.b": ["string", "null", "int"]
        // }
        // And we have the FOI [ "cols.array[0]", "a\.b" ]
        // Then the recordsToFOIMappingEntry will look like this:
        // [ { "recordFieldIdx": 2, "foiIdx": 0 },
        //   { "recordFieldIdx": 8, "foiIdx": 1 },
        //   { "recordFieldIdx": 9, "foiIdx": 1 },
        //   { "recordFieldIdx": 10, "foiIdx": 1 }
        // ]
        // This is because each (fieldName, fieldType) in a record is treated as
        // a unique recordField. Notice how the foi "a\.b" corresponds to 3
        // possible ("a.b", "string"), ("a.b", "null"), ("a.b", "int")
        // recordFields, and that's why we have 3 entries for that foi "a.b".
        // However, given a fieldName, at most one of the (fieldName, fieldType)
        // entry is valid in a given record. This is because we don't allow
        // duplicate fieldNames from within a record, but it is possible that a
        // given fieldName may have different fieldTypes across records. And
        // thus the below cursoring algorithm works as follows:
        // 1) Start with the first recordsToFOIMappingEntry
        // 2) Advance the FieldValueIterator until it matches the recordFieldIdx
        //    in the recordsToFOIMappingEntry
        // 3) Extract value, since this is a FOI, and advance
        //    recordsToFOIMappingEntry. If value is complex, (i.e. contains
        //    nested values), keep advancing recordsToFOIMappingEntry until
        //    we've extracted all nested values. We know when we're done when
        //    the recordFieldIdx of the recordToFOIMapping no longer matches the
        //    current record. Repeat step 2 with this new recordToFOIMapping
        //    entry.
        DataPageReader::RecordIterator recIter(&reader);
        while (recIter.getNext(&record)) {
            DfFieldValue field;
            field.int64Val = startRec + recordsRead;
            recordsRead++;
            assert(xdbLoadArgs->fieldNamesCount <= TupleMaxNumValuesPerRecord);

            ReaderRecord::FieldValueIterator fieldIter(&record);

            kvEntry.init();

            for (int64_t jj = 0; jj < xdbLoadArgs->fieldNamesCount; jj++) {
                if (recordFieldIndices[jj] == RecordNumIdx) {
                    fieldVal.int64Val = field.int64Val;
                    scratchPadScalars[jj]->setValue(fieldVal, DfInt64);
                    kvEntry.tuple_.set(jj,
                                       fieldVal,
                                       kvEntry.kvMeta_->tupMeta_->getFieldType(
                                           jj));
                } else {
                    // Invalid unless otherwise stated
                    assert(!kvEntry.tuple_.isValid(jj));
                }
            }

            int curMappingIdx = 0;
            for (int32_t ii = 0; ii < numFieldsInRecord; ii++) {
                DataValueReader dValueComplex, dValue;
                const Accessor *accessor;
                int64_t jj;
                bool extractedField = false;
                bool done, exists = false;

                assert(curMappingIdx <= (int) numRecordToFOIMapping);

                if (curMappingIdx >= (int) numRecordToFOIMapping ||
                    recordToFOIMapping[curMappingIdx].foiIdx == InvalidIdx) {
                    // We've found all the fields we're interested in
                    break;
                }

                if (recordToFOIMapping[curMappingIdx].recordFieldIdx != ii) {
                    assert(ii <
                           recordToFOIMapping[curMappingIdx].recordFieldIdx);
                    // This field is not a field of interest. Skip
                    status =
                        fieldIter.getNext(&done, &exists, NULL, NULL, NULL);
                    BailIfFailed(status);
                    continue;
                }

                assert(recordToFOIMapping[curMappingIdx].recordFieldIdx == ii);
                jj = recordToFOIMapping[curMappingIdx].foiIdx;
                accessor = &variables[jj].accessor;

                // A subtle assumption we're making here is that we do not
                // support object/array typed FOIs Otherwise, just because the
                // first FOI is "col", doesn't mean the next FOI might not be
                // "col.nested", which is what this is implicitly assuming
                if (accessor->nameDepth == 1) {
                    status =
                        fieldIter.getNext(&done, &exists, NULL, NULL, &dValue);
                    BailIfFailedTxnMsg(moduleName,
                                       status,
                                       "Error retrieving column \"%s\"",
                                       xdbLoadArgs->fieldNames[jj]);
                } else {
                    // This is a complex type and hence we'll have to save the
                    // value as a protobuf-encoded value
                    assert(accessor->nameDepth > 1);
                    status = dValueComplex.initProtoValue();
                    BailIfFailedTxnMsg(moduleName,
                                       status,
                                       "Failed to initialize protobuf dValue");
                    status = fieldIter.getNext(&done,
                                               &exists,
                                               NULL,
                                               NULL,
                                               &dValueComplex);
                    BailIfFailedTxnMsg(moduleName,
                                       status,
                                       "Error retrieving column \"%s\"",
                                       xdbLoadArgs->fieldNames[jj]);
                }

                while (curMappingIdx < (int) numRecordToFOIMapping &&
                       recordToFOIMapping[curMappingIdx].recordFieldIdx == ii) {
                    jj = recordToFOIMapping[curMappingIdx].foiIdx;
                    curMappingIdx++;

                    if (!exists) {
                        // The parent field doesn't exist. So all nested value
                        // of this field doesn't exist either
                        continue;
                    }

                    accessor = &variables[jj].accessor;

                    if (accessor->nameDepth > 1) {
                        status =
                            dValueComplex.accessNestedField(accessor, &dValue);
                        if (status == StatusDfFieldNoExist) {
                            // The nested field does not exist. Skip
                            continue;
                        }
                        BailIfFailed(status);
                    }

                    extractedField = false;
                    status = populateScalar(scratchPadScalars[jj],
                                            &fieldVal,
                                            &dValue,
                                            dstTupMeta->getFieldType(jj),
                                            &extractedField,
                                            (jj == xdbLoadArgs->keyIndex),
                                            dstMeta);
                    if (status == StatusOk && extractedField) {
                        kvEntry.tuple_
                            .set(jj,
                                 fieldVal,
                                 kvEntry.kvMeta_->tupMeta_->getFieldType(jj));
                    }
                }
            }

            if (astInited) {
                for (jj = 0; jj < ast.astCommon.numScalarVariables; jj++) {
                    int idx = evalArgIndices[jj];

                    if (scratchPadScalars[idx] == NULL ||
                        scratchPadScalars[idx]->fieldUsedSize == 0) {
                        ast.astCommon.scalarVariables[jj].content = NULL;
                    } else {
                        ast.astCommon.scalarVariables[jj].content =
                            scratchPadScalars[idx];
                    }
                }

                DfFieldValue evalResult;
                DfFieldType resultType;
                status = eval->eval(&ast, &evalResult, &resultType, false);
                if (eval->isFatalError(status)) {
                    goto CommonExit;
                }

                if (status != StatusOk ||
                    !XcalarEval::isEvalResultTrue(evalResult, resultType)) {
                    // filter evaluated to false, skip this row
                    continue;
                }
            }

            DfFieldValue key;
            bool keyValid;
            if (xdbLoadArgs->keyIndex == NewTupleMeta::DfInvalidIdx) {
                keyValid = false;
            } else {
                key =
                    kvEntry.tuple_.get(xdbLoadArgs->keyIndex,
                                       numFields,
                                       kvEntry.kvMeta_->tupMeta_->getFieldType(
                                           xdbLoadArgs->keyIndex),
                                       &keyValid);
            }

            status = op->hashAndSendKv(insertHandle.loadInfo->transPageHandle,
                                       (TransportPage **)
                                           insertHandle.node->transPages,
                                       NULL,
                                       NULL,
                                       1,
                                       &dstMeta->keyAttr[0].type,
                                       &key,
                                       &keyValid,
                                       &kvEntry,
                                       &insertHandle,
                                       dht,
                                       TransportPageType::SendSourcePage,
                                       DemystifyMgr::Op::Index);
            BailIfFailed(status);

            ++recordsRead;
            ++workCounter;
        }

        if (opStatus != NULL) {
            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
        }
    }

CommonExit:
    if (scratchPadScalars != NULL) {
        for (int64_t ii = 0; ii < numScalarsAlloced; ii++) {
            assert(scratchPadScalars[ii] != NULL);
            Scalar::freeScalar(scratchPadScalars[ii]);
            scratchPadScalars[ii] = NULL;
        }
        memFree(scratchPadScalars);
        scratchPadScalars = NULL;
    }

    if (variables != NULL) {
        delete[] variables;
        variables = NULL;
    }

    if (recordFieldIndices != NULL) {
        memFree(recordFieldIndices);
        recordFieldIndices = NULL;
    }

    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    memFree(evalArgIndices);
    if (astInited) {
        // scratchPadScalars have already been freed
        eval->dropScalarVarRef(&ast.astCommon);
        eval->destroyClass1Ast(&ast);
        astInited = false;
    }

    if (numRecordsInserted != NULL) {
        *numRecordsInserted = recordsRead;
    }

    if (recordToFOIMapping != NULL) {
        memFree(recordToFOIMapping);
        recordToFOIMapping = NULL;
    }

    return status;
}

Status
DataFormat::populateScalar(Scalar *scalar,
                           DfFieldValue *fieldVal,
                           const DataValueReader *dValue,
                           DfFieldType expectedFieldType,
                           bool *extractedField,
                           bool isKey,
                           XdbMeta *dstMeta)
{
    Status status = StatusOk;
    DfFieldType fieldType = DfUnknown;
    *extractedField = true;

    status = dValue->getAsFieldValueInArray(scalar->fieldAllocedSize,
                                            &scalar->fieldVals,
                                            0,
                                            &fieldType);
    if (unlikely(status == StatusDfFieldNoExist)) {
        *extractedField = false;
        status = StatusOk;
        goto CommonExit;
    } else if (unlikely(status != StatusOk)) {
        goto CommonExit;
    }
    BailIfFailed(status);
    assert(fieldType != DfUnknown);

    if (isKey) {
        if (unlikely(dstMeta->keyAttr[0].type == DfUnknown)) {
            status = XdbMgr::get()->xdbSetKeyType(dstMeta, fieldType, 0);
            BailIfFailed(status);
        }

        expectedFieldType = dstMeta->keyAttr[0].type;
        assert(expectedFieldType != DfScalarObj);
    }

    scalar->fieldNumValues = 1;
    scalar->fieldType = fieldType;
    scalar->updateUsedSize();

    assert(isValidDfFieldType(scalar->fieldType));

    if (expectedFieldType == DfFatptr) {
        // Don't have to do anything
    } else if (expectedFieldType == DfScalarObj) {
        fieldVal->scalarVal = scalar;
    } else {
        status = scalar->getValue(fieldVal);
        BailIfFailed(status);

        if (expectedFieldType != scalar->fieldType) {
            if (scalar->fieldType == DfNull) {
                *extractedField = false;
                goto CommonExit;
            }

            status = scalar->convertType(expectedFieldType);
            BailIfFailed(status);

            status = scalar->getValue(fieldVal);
            BailIfFailed(status);
        }
    }
CommonExit:
    return status;
}

Status
DataFormat::addPageIndex(DataPageIndex *pageIndex, DfRecordId *startRecord)
{
    Status status = StatusOk;

    status = segmentStore_.insert(pageIndex,
                                  pageIndex->getNumRecords(),
                                  startRecord);
    BailIfFailed(status);

CommonExit:
    return status;
}

IRecordParser *
DataFormat::getParser(DfFormatType format)
{
    return importables_[format]->getParser();
}

IRowRenderer *
DataFormat::getRowRenderer(DfFormatType format)
{
    return exportables_[format]->getRowRenderer();
}

// Please make sure you own a reference to dht before passing it to us
NodeId
DataFormat::hashValueToNodeId(Dht *dht,
                              DfFieldValue fieldVal,
                              DfFieldType fieldType)
{
    const unsigned numNodes = Config::get()->getActiveNodes();
    NodeId nodeId;
    DhtMgr *dhtMgr = DhtMgr::get();

    switch (fieldType) {
    case DfString:
        nodeId = dhtMgr->dhtHashString(dht, fieldVal.stringVal.strActual);
        break;
    case DfInt32:
        nodeId = dhtMgr->dhtHashInt64(dht, fieldVal.int32Val);
        break;
    case DfUInt32:
        nodeId = dhtMgr->dhtHashUInt64(dht, fieldVal.uint32Val);
        break;
    case DfInt64:
        nodeId = dhtMgr->dhtHashInt64(dht, fieldVal.int64Val);
        break;
    case DfUInt64:
    case DfOpRowMetaPtr:
        nodeId = dhtMgr->dhtHashUInt64(dht, fieldVal.uint64Val);
        break;
    case DfFloat32:
        nodeId = dhtMgr->dhtHashFloat64(dht, fieldVal.float32Val, true);
        break;
    case DfFloat64:
        nodeId = dhtMgr->dhtHashFloat64(dht, fieldVal.float64Val, true);
        break;
    case DfBoolean:
        nodeId = dhtMgr->dhtHashUInt64(dht, fieldVal.boolVal);
        break;
    case DfTimespec:
        nodeId = dhtMgr->dhtHashInt64(dht, fieldVal.timeVal.ms);
        break;
    case DfMoney: {
        Status status;
        DFPUtils *dfp = DFPUtils::get();
        float64_t fVal;

        // TODO: native numeric format
        status = dfp->xlrDfpNumericToFloat64(&fVal, &fieldVal.numericVal);
        assert(status == StatusOk);
        if (status == StatusOk) {
            nodeId = dhtMgr->dhtHashFloat64(dht, fVal, true);
        } else {
            nodeId = 0;
        }
    } break;
    case DfBlob:
        nodeId = dhtMgr->dhtHashString(dht, fieldVal.stringVal.strActual);
        assert(0);
        break;
    case DfNull:
        nodeId = 0;
        break;
    case DfMixed:
    default:
        assert(0);
        nodeId = 0;
        break;
    }

    assert(nodeId < numNodes);

    return nodeId;
}

Status
DataFormat::addJsonFieldImm(json_t *fieldObject,
                            const char *fatptrPrefix,
                            const char *fieldName,
                            DfFieldType fieldType,
                            const DfFieldValue *fieldValue)
{
    Status status;
    int ret;
    json_t *jsonValue = NULL;
    char fullFieldName[DfMaxFieldNameLen + 1];

    if (fatptrPrefix != NULL && fatptrPrefix[0] != '\0') {
        ret = snprintf(fullFieldName,
                       sizeof(fullFieldName),
                       "%s%s%s",
                       fatptrPrefix,
                       DfFatptrPrefixDelimiter,
                       fieldName);
        if (ret >= (int) sizeof(fullFieldName)) {
            status = StatusNoBufs;
            goto CommonExit;
        }
    } else {
        ret = snprintf(fullFieldName, sizeof(fullFieldName), "%s", fieldName);
        if (ret >= (int) sizeof(fullFieldName)) {
            status = StatusNoBufs;
            goto CommonExit;
        }
    }

    status = DataFormat::jsonifyValue(fieldType, fieldValue, &jsonValue);
    BailIfFailed(status);

    // This steals the jsonValue ref
    ret = json_object_set_new(fieldObject, fullFieldName, jsonValue);
    if (ret == -1) {
        // This is probably the only real case this will happen
        status = StatusNoMem;
        goto CommonExit;
    }
    jsonValue = NULL;

CommonExit:
    if (jsonValue != NULL) {
        json_decref(jsonValue);
        jsonValue = NULL;
    }
    return status;
}

Status
DataFormat::requiresFullSchema(DfFormatType formatType,
                               const ExInitExportFormatSpecificArgs *formatArgs,
                               bool *requiresFullSchema)
{
    if (formatType == DfFormatUnknown) {
        return StatusUnimpl;
    }
    return exportables_[formatType]->requiresFullSchema(formatArgs,
                                                        requiresFullSchema);
}

Status
DataFormat::renderColumnHeaders(
    DfFormatType formatType,
    const char *srcTableName,
    int numColumns,
    const char **headerColumns,
    const ExInitExportFormatSpecificArgs *formatArgs,
    const TupleValueDesc *valueDesc,
    char *buf,
    size_t bufSize,
    size_t *bytesWritten)
{
    return exportables_[formatType]->renderColumnHeaders(srcTableName,
                                                         numColumns,
                                                         headerColumns,
                                                         formatArgs,
                                                         valueDesc,
                                                         buf,
                                                         bufSize,
                                                         bytesWritten);
}

Status
DataFormat::renderPrelude(DfFormatType formatType,
                          const TupleValueDesc *valueDesc,
                          const ExInitExportFormatSpecificArgs *formatArgs,
                          char *buf,
                          size_t bufSize,
                          size_t *bytesWritten)
{
    return exportables_[formatType]->renderPrelude(valueDesc,
                                                   formatArgs,
                                                   buf,
                                                   bufSize,
                                                   bytesWritten);
}

Status
DataFormat::renderFooter(DfFormatType formatType,
                         char *buf,
                         size_t bufSize,
                         size_t *bytesWritten)
{
    return exportables_[formatType]->renderFooter(buf, bufSize, bytesWritten);
}

const DataPageIndex *
DataFormat::lookupIndex(const DfRecordId recordId, DfRecordId *indexRecId)
{
    return this->segmentStore_.lookup(recordId, indexRecId);
}

Status
DataFormat::escapeNestedDelim(char *fieldName,
                              const size_t bufSize,
                              size_t *outSize)
{
    Status status = StatusOk;
    char tmpFieldName[strlen(fieldName) + 1];
    strlcpy(tmpFieldName, fieldName, sizeof(tmpFieldName));
    unsigned jj = 0;
    for (unsigned ii = 0; ii < strlen(tmpFieldName); ii++) {
        if (tmpFieldName[ii] == DfNestedDelimiter ||
            tmpFieldName[ii] == DfArrayIndexStartDelimiter ||
            tmpFieldName[ii] == DfArrayIndexEndDelimiter ||
            tmpFieldName[ii] == '\\') {
            if (jj >= bufSize - 1) {
                status = StatusNameTooLong;
                goto CommonExit;
            }

            fieldName[jj] = '\\';
            jj++;
        }

        if (jj >= bufSize - 1) {
            status = StatusNameTooLong;
            goto CommonExit;
        }

        fieldName[jj] = tmpFieldName[ii];
        jj++;
    }
    fieldName[jj] = '\0';

CommonExit:
    if (outSize) {
        *outSize = jj;
    }

    return status;
}

void
DataFormat::unescapeNestedDelim(char *fieldName)
{
    char tmpFieldName[strlen(fieldName) + 1];
    strlcpy(tmpFieldName, fieldName, sizeof(tmpFieldName));
    unsigned jj = 0;
    unsigned ii = 0;
    bool escaping = false;
    for (ii = 0; ii < strlen(tmpFieldName) - 1; ii++) {
        if (!escaping && tmpFieldName[ii] == '\\' &&
            (tmpFieldName[ii + 1] == DfNestedDelimiter ||
             tmpFieldName[ii + 1] == DfArrayIndexStartDelimiter ||
             tmpFieldName[ii + 1] == DfArrayIndexEndDelimiter ||
             tmpFieldName[ii + 1] == '\\')) {
            escaping = true;
            continue;
        }

        fieldName[jj] = tmpFieldName[ii];
        jj++;

        if (escaping) {
            escaping = false;
        }
    }
    fieldName[jj] = tmpFieldName[ii];
    fieldName[jj + 1] = '\0';
}

void
DataFormat::updateFieldValueRange(DfFieldType type,
                                  DfFieldValue curValue,
                                  DfFieldValue *minValue,
                                  DfFieldValue *maxValue,
                                  HashString hashString,
                                  DfFieldValueRangeState range)
{
    uint64_t curHash = 0;

    if (range == DfFieldValueRangeValid) {
        if (type == DfString) {
            // XXX hack for strings to avoid copy; store hash instead
            if (hashString == DoHashString) {
                curHash = operatorsHashByString(curValue.stringVal.strActual);
            } else {
                assert(hashString == DontHashString);
                curHash = curValue.uint64Val;
            }
            if (curHash > maxValue->uint64Val) {
                maxValue->uint64Val = curHash;
            }
            if (curHash < minValue->uint64Val) {
                minValue->uint64Val = curHash;
            }
        } else {
            if (fieldCompare(type, (*maxValue), curValue) < 0) {
                (*maxValue) = curValue;
            }
            if (fieldCompare(type, (*minValue), curValue) > 0) {
                (*minValue) = curValue;
            }
        }
    } else {
        assert(range == DfFieldValueRangeInvalid);
        if (type == DfString) {
            // XXX hack for strings to avoid copy; store hash instead
            if (hashString == DoHashString) {
                curHash = operatorsHashByString(curValue.stringVal.strActual);
            } else {
                assert(hashString == DontHashString);
                curHash = curValue.uint64Val;
            }
            maxValue->uint64Val = curHash;
            minValue->uint64Val = curHash;
        } else {
            (*maxValue) = curValue;
            (*minValue) = curValue;
        }
    }
}

void
DataFormat::replaceFatptrPrefixDelims(char *string)
{
    char *prefixPtr;
    while ((prefixPtr = strstr(string, DfFatptrPrefixDelimiter))) {
        assert(strlen(DfFatptrPrefixDelimiter) == 2);
        assert(strlen(DfFatptrPrefixDelimiterReplaced) ==
               strlen(DfFatptrPrefixDelimiter));

        prefixPtr[0] = DfFatptrPrefixDelimiterReplaced[0];
        prefixPtr[1] = DfFatptrPrefixDelimiterReplaced[1];
        string = prefixPtr + strlen(DfFatptrPrefixDelimiter);
    }
}

void
DataFormat::revertFatptrPrefixDelims(char *string)
{
    char *prefixPtr;
    while ((prefixPtr = strstr(string, DfFatptrPrefixDelimiterReplaced))) {
        assert(strlen(DfFatptrPrefixDelimiter) == 2);
        assert(strlen(DfFatptrPrefixDelimiterReplaced) ==
               strlen(DfFatptrPrefixDelimiter));

        prefixPtr[0] = DfFatptrPrefixDelimiter[0];
        prefixPtr[1] = DfFatptrPrefixDelimiter[1];
        string = prefixPtr + strlen(DfFatptrPrefixDelimiter);
    }
}

// validIndicesMap should be initialized to all false
//
// Assigns a boolean map of possible indices in the valueArray
// for each variable. Also parses out fatptr prefixes
//
// 1. Immediates and prefixed fatptr fields will have one index set.
// 2. Aggregate results will have no indices set.
// 3. Everything else will have all fatptr indices set.
//
// This will be optimized once we get rid of non-prefixed fatptrs
Status
DataFormat::createValidIndicesMap(const XdbMeta *xdbMeta,
                                  unsigned numVariables,
                                  char **variableNames,
                                  bool **validIndicesMap)
{
    Status status = StatusOk;
    const NewTupleMeta *tupMeta = xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t numFields = tupMeta->getNumFields();

    for (unsigned ii = 0; ii < numVariables; ii++) {
        // this variable is an aggregate result and won't be in valueArray
        if (variableNames[ii][0] == OperatorsAggregateTag) {
            continue;
        }

        Accessor accessor;
        bool isPrefixedFp = false;
        char *prefixName;
        char *variableName;
        int nameDepth;

        char *prefixPtr = strstr(variableNames[ii], DfFatptrPrefixDelimiter);
        if (prefixPtr != NULL) {
            *prefixPtr = '\0';
            prefixName = variableNames[ii];
            variableNames[ii] = prefixPtr + strlen(DfFatptrPrefixDelimiter);
            isPrefixedFp = true;
        }

        AccessorNameParser accessorParser;
        status = accessorParser.parseAccessor(variableNames[ii], &accessor);
        BailIfFailed(status);

        variableName = accessor.names[0].value.field;
        nameDepth = accessor.nameDepth;

        for (size_t jj = 0; jj < numFields; jj++) {
            assert(validIndicesMap[ii][jj] == false);

            if (tupMeta->getFieldType(jj) == DfFatptr) {
                // assume all fatptr indices are valid unless proven otherwise
                if (isPrefixedFp) {
                    if (strcmp(prefixName,
                               xdbMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                        // we found an index for our prefixed fatptr,
                        // but there could be more with the same prefix
                        validIndicesMap[ii][jj] = true;
                    }
                }
            } else {
                if (!isPrefixedFp && nameDepth == 1 &&
                    strcmp(variableName,
                           xdbMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                    // we found an index for our immediate,
                    // but there could be more with the same name
                    validIndicesMap[ii][jj] = true;
                }
            }
        }
    }

CommonExit:
    return status;
}

Status
DataFormat::fieldToProto(DfFieldValue value,
                         DfFieldType type,
                         ProtoFieldValue *protoOut)
{
    Status status = StatusOk;

    try {
        switch (type) {
        case DfInt32:
            protoOut->set_int32val(value.int32Val);
            break;
        case DfUInt32:
            protoOut->set_uint32val(value.uint32Val);
            break;
        case DfFloat32:
            protoOut->set_float32val(value.float32Val);
            break;
        case DfInt64:
            protoOut->set_int64val(value.int64Val);
            break;
        case DfUInt64:
            protoOut->set_uint64val(value.uint64Val);
            break;
        case DfFloat64:
            protoOut->set_float64val(value.float64Val);
            break;
        case DfMoney:
            for (size_t i = 0; i < ArrayLen(value.numericVal.ieee); i++) {
                protoOut->mutable_numericval()->add_val(
                    value.numericVal.ieee[i]);
            }
            break;
        case DfBoolean:
            protoOut->set_boolval(value.boolVal);
            break;
        case DfString:
            protoOut->set_stringval(value.stringVal.strActual);
            break;

        case DfScalarPtr:
        case DfScalarObj:
            DfFieldValue scalarVal;
            status = value.scalarVal->getValue(&scalarVal);
            BailIfFailed(status);

            status =
                fieldToProto(scalarVal, value.scalarVal->fieldType, protoOut);
            BailIfFailed(status);
            break;

        case DfTimespec: {
            google::protobuf::Timestamp *t = protoOut->mutable_timeval();
            t->set_seconds(value.timeVal.ms / 1000);
            t->set_nanos((value.timeVal.ms % 1000) * 1000000);
            break;
        }
        case DfUnknown:
        case DfNull:
            // Leave this as DATAVALUE_NOT_SET
            break;

        default:
            assert(0);
            return StatusUnimpl;
            break;
        }
    } catch (std::exception) {
        return StatusNoMem;
    }

CommonExit:
    return status;
}
