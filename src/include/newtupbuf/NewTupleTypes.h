// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _NEW_TUPLE_TYPES_H
#define _NEW_TUPLE_TYPES_H

#include <sys/types.h>
#include <assert.h>
#include <algorithm>

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"

// #define NewTupleDebug

//
// Avoid virtual methods here to save the vtable lookup cost.
// All the Impls are in the header file are inlined deliberately.
//

// Describes the fields type in a Tuple.
class NewTupleMeta
{
    friend class NewTuplesBuffer;
    friend class NewTuplesCursor;
    friend class NewTupleValues;

  public:
    enum class FieldsPacking : uint8_t {
        Unknown,
        Fixed,     // fixed field packing
        Variable,  // variable field packing
    };

    static constexpr const DfFieldValue InvalidField = {
        .uint64Val = 0xbaadbeefbaadbeef,
    };
    static constexpr const DfFieldValue InvalidKey = {
        .uint64Val = 0xc001c001cafecafe,
    };

    static constexpr const ssize_t DfInvalidIdx = -1;

  private:
    // 1 << 6 = 64 bits = 8 bytes = word
    static constexpr const size_t MaxWordSizeInBitsShft = 6;

    // 8 bytes Field
    static constexpr const size_t EightBytesFsMask = (uint64_t)(-1);
    static constexpr const size_t EightBytesFsClearMask = ~EightBytesFsMask;
    static constexpr const size_t EightBytesFsShft = 3;
    static constexpr const size_t EightBytesFieldSize = 1 << EightBytesFsShft;

    // 6 Bytes Field
    static constexpr const size_t SixBytesFsMask =
        (uint64_t)(((uint64_t)(-1)) >> (BitsPerUInt8 * 2));
    static constexpr const size_t SixBytesFsClearMask =
        (uint64_t)(~((uint64_t) SixBytesFsMask));
    static constexpr const size_t SixBytesFieldSize = 6;

    // 5 Bytes Field
    static constexpr const size_t FiveBytesFsMask =
        (uint64_t)(((uint64_t)(-1)) >> (BitsPerUInt8 * 3));
    static constexpr const size_t FiveBytesFsClearMask =
        (uint64_t)(~((uint64_t) FiveBytesFsMask));
    static constexpr const size_t FiveBytesFieldSize = 5;

    // 4 Bytes Field
    static constexpr const size_t FourBytesFsMask = (uint32_t)(-1);
    static constexpr const size_t FourBytesFsClearMask = ~FourBytesFsMask;
    static constexpr const size_t FourBytesFsShft = EightBytesFsShft - 1;
    static constexpr const size_t FourBytesFieldSize = 1 << FourBytesFsShft;

    // 3 Bytes Field
    static constexpr const size_t ThreeBytesFsMask =
        (uint32_t)(((uint32_t)(-1)) >> BitsPerUInt8);
    static constexpr const size_t ThreeBytesFsClearMask =
        (uint32_t)(~((uint32_t) ThreeBytesFsMask));
    static constexpr const size_t ThreeBytesFieldSize = 3;

    // 2 Bytes Field
    static constexpr const size_t TwoBytesFsMask = (uint16_t)(-1);
    static constexpr const size_t TwoBytesFsClearMask = ~TwoBytesFsMask;
    static constexpr const size_t TwoBytesFsShft = FourBytesFsShft - 1;
    static constexpr const size_t TwoBytesFieldSize = 1 << TwoBytesFsShft;

    // 1 Byte Field
    static constexpr const size_t OneByteFsMask = (uint8_t)(-1);
    static constexpr const size_t OneByteFsClearMask = ~OneByteFsMask;
    static constexpr const size_t OneByteFsShft = TwoBytesFsShft - 1;
    static constexpr const size_t OneByteFieldSize = 1 << OneByteFsShft;

    // Invalid Field
    static constexpr const size_t InvalidFieldSize = 0;

    // Max field size
    static constexpr const size_t MaxFieldSize = EightBytesFieldSize;

    // Mix field size
    static constexpr const size_t MinFieldSize = OneByteFieldSize;

    // Bmap states
    static constexpr const size_t BmapNumBitsPerFieldShft = 2;
    static constexpr const size_t BmapNumBitsPerField =
        1 << BmapNumBitsPerFieldShft;
    static constexpr const size_t BmapInvalid = 0;
    static constexpr const size_t BmapOneByteField = 1;
    static constexpr const size_t BmapTwoBytesField = 2;
    static constexpr const size_t BmapThreeBytesField = 3;
    static constexpr const size_t BmapFourBytesField = 4;
    static constexpr const size_t BmapFiveBytesField = 5;
    static constexpr const size_t BmapSixBytesField = 6;
    static constexpr const size_t BmapEightBytesField = 7;
    static constexpr const size_t BmapMarkInvalid = 8;
    static constexpr const size_t BmapOneByteFieldMarkInval = 9;
    static constexpr const size_t BmapTwoBytesFieldMarkInval = 10;
    static constexpr const size_t BmapThreeBytesFieldMarkInval = 11;
    static constexpr const size_t BmapFourBytesFieldMarkInval = 12;
    static constexpr const size_t BmapFiveBytesFieldMarkInval = 13;
    static constexpr const size_t BmapSixBytesFieldMarkInval = 14;
    static constexpr const size_t BmapEightBytesFieldMarkInval = 15;
    static constexpr const size_t BmapValidMask = 0x7;
    static constexpr const size_t BmapMask = 0xf;
    static constexpr const size_t BmapMax = BmapMask + 1;
    static constexpr const size_t BmapPoison = 0xff;

    static constexpr const size_t BmapNumEntriesPerByte =
        BitsPerUInt8 / BmapNumBitsPerField;
    static constexpr const size_t BmapNumEntriesPerByteShft = 1;

    //
    // BMAP values to field size look up table.
    //
    //--------------------------------------------------------------------------
    // Bmap     State                   Field size
    //--------------------------------------------------------------------------
    // 0000     Invalid                 InvalidFieldSize
    // 0001     1 Byte                  OneBytesFieldSize
    // 0010     2 Bytes                 TwoBytesFieldSize
    // 0011     3 Bytes                 ThreeBytesFieldSize
    // 0100     4 Bytes                 FourBytesFieldSize
    // 0101     5 Bytes                 FiveBytesFieldSize
    // 0110     6 Bytes                 SixBytesFieldSize
    // 0111     8 Bytes                 EightBytesFieldSize
    // 1000     Maked Invalid           InvalidFieldSize
    // 1001     1 Byte Marked Invalid   OneBytesFieldSize
    // 1010     2 Byte Marked Invalid   TwoBytesFieldSize
    // 1011     3 Byte Marked Invalid   ThreeBytesFieldSize
    // 1100     4 Byte Marked Invalid   FourBytesFieldSize
    // 1101     5 Byte Marked Invalid   FiveBytesFieldSize
    // 1110     6 Byte Marked Invalid   SizeBytesFieldSize
    // 1111     8 Byte Marked Invalid   EightBytesFieldSize
    //--------------------------------------------------------------------------
    //
    static constexpr uint8_t BmapValueValidFieldSizeLUT[] = {
        InvalidFieldSize,     // 0x0
        OneByteFieldSize,     // 0x1
        TwoBytesFieldSize,    // 0x2
        ThreeBytesFieldSize,  // 0x3
        FourBytesFieldSize,   // 0x4
        FiveBytesFieldSize,   // 0x5
        SixBytesFieldSize,    // 0x6
        EightBytesFieldSize,  // 0x7
    };

    // XXX Xc-12247 Needs to dynamic instead.
    DfFieldType fieldType_[TupleMaxNumValuesPerRecord];
    FieldsPacking fieldsPacking_;
    uint16_t numFields_ = 0;

  public:
    NewTupleMeta()
    {
        assertStatic(1 << EightBytesFsShft == WordSizeInBytes &&
                     "Invalid EightBytesFsShft");
        assertStatic(1 << MaxWordSizeInBitsShft == WordSizeInBits &&
                     "Invalid MaxWordSizeInBitsShft");
        assertStatic(1 << BmapNumEntriesPerByteShft == BmapNumEntriesPerByte &&
                     "Invalid BmapNumEntriesPerByteShft");
    }
    ~NewTupleMeta() = default;

    NewTupleMeta &operator=(NewTupleMeta other)
    {
        std::swap(this->numFields_, other.numFields_);
        std::swap(this->fieldsPacking_, other.fieldsPacking_);
        for (uint16_t ii = 0; ii < this->numFields_; ii++) {
            this->fieldType_[ii] = other.fieldType_[ii];
        }
        return *this;
    }

    MustInline MustCheck size_t getNumFields() const { return numFields_; }

    MustInline void setNumFields(size_t numFields) { numFields_ = numFields; }

    MustInline MustCheck FieldsPacking getFieldsPacking() const
    {
        return fieldsPacking_;
    }

    MustInline void setFieldsPacking(FieldsPacking fieldsPacking)
    {
        fieldsPacking_ = fieldsPacking;
    }

    MustInline MustCheck DfFieldType getFieldType(size_t fieldsIdx) const
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        return fieldType_[fieldsIdx];
    }

    MustInline void setFieldType(DfFieldType fieldType, size_t fieldsIdx)
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        fieldType_[fieldsIdx] = fieldType;
    }

    void addField(const char **srcFieldNames,
                  const char *fieldName,
                  DfFieldType fieldType)
    {
        // check if we are replacing an existing field
        unsigned ii;
        for (ii = 0; ii < numFields_; ii++) {
            if (strcmp(fieldName, srcFieldNames[ii]) == 0) {
                setFieldType(fieldType, ii);
                return;
            }
        }

        // append field
        setFieldType(fieldType, numFields_);
        srcFieldNames[numFields_] = fieldName;
        numFields_++;
    }

    MustInline void addOrReplaceField(int idx, DfFieldType fieldType)
    {
        assert(idx <= numFields_);
        if (unlikely(idx == numFields_)) {
            // insert a field
            numFields_++;
        }

        setFieldType(fieldType, idx);
    }

    MustInline MustCheck static bool isFieldBmapZeroByteInvalid(uint8_t bmap)
    {
        return bmap == BmapInvalid || bmap == BmapMarkInvalid;
    }

    MustInline MustCheck static bool isFieldBmapInvalid(uint8_t bmap)
    {
        return bmap == BmapInvalid;
    }

    MustInline MustCheck static bool isFieldBmapMarkedInvalid(uint8_t bmap)
    {
        return bmap == BmapMarkInvalid || bmap == BmapOneByteFieldMarkInval ||
               bmap == BmapTwoBytesFieldMarkInval ||
               bmap == BmapThreeBytesFieldMarkInval ||
               bmap == BmapFourBytesFieldMarkInval ||
               bmap == BmapFiveBytesFieldMarkInval ||
               bmap == BmapSixBytesFieldMarkInval ||
               bmap == BmapEightBytesFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapOneByteSize(uint8_t bmap)
    {
        return bmap == BmapOneByteField || bmap == BmapOneByteFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapTwoBytesSize(uint8_t bmap)
    {
        return bmap == BmapTwoBytesField || bmap == BmapTwoBytesFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapThreeBytesSize(uint8_t bmap)
    {
        return bmap == BmapThreeBytesField ||
               bmap == BmapThreeBytesFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapFourBytesSize(uint8_t bmap)
    {
        return bmap == BmapFourBytesField ||
               bmap == BmapFourBytesFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapFiveBytesSize(uint8_t bmap)
    {
        return bmap == BmapFiveBytesField ||
               bmap == BmapFiveBytesFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapSixBytesSize(uint8_t bmap)
    {
        return bmap == BmapSixBytesField || bmap == BmapSixBytesFieldMarkInval;
    }

    MustInline MustCheck static bool isFieldBmapEightBytesSize(uint8_t bmap)
    {
        return bmap == BmapEightBytesField ||
               bmap == BmapEightBytesFieldMarkInval;
    }

    MustInline MustCheck static size_t getBmapFieldSize(uint8_t bmap)
    {
        assert(bmap <= BmapMask && "Unknown bmap");
        return BmapValueValidFieldSizeLUT[bmap & BmapValidMask];
    }

    MustInline MustCheck static size_t getPerRecordBmapSize(size_t numFields)
    {
        // (roundUp((NewTupleMeta::BmapNumBitsPerField * numFields),
        // BitsPerUInt8) >> BitsPerUInt8Shift);
        // Below logic is same as above avoiding unnecessay modulo operations in
        // roundUp().
        return ((((NewTupleMeta::BmapNumBitsPerField * numFields) + 7) &
                 (-BitsPerUInt8)) >>
                BitsPerUInt8Shift);
    }

    MustInline MustCheck static uint8_t getBmapFieldMarkInvalid(uint8_t bmap)
    {
        return (bmap | BmapMarkInvalid);
    }

    MustInline MustCheck static uint8_t getBmapFieldUnMarkInvalid(uint8_t bmap)
    {
        return (bmap & BmapValidMask);
    }
};

// Userdata or the field values in a Tuple.
class NewTupleValues
{
    friend class NewTuplesBuffer;
    friend class NewTuplesCursor;

  private:
    static constexpr const size_t FieldPoison = 0xae;
    DfFieldValue fields_[TupleMaxNumValuesPerRecord];
    uint8_t bitMaps_[TupleMaxNumValuesPerRecord];

  public:
    MustInline MustCheck bool isValid(size_t fieldsIdx) const
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        assert(bitMaps_[fieldsIdx] <= NewTupleMeta::BmapMask &&
               "Unknown fieldBmap");
        return !NewTupleMeta::isFieldBmapInvalid(bitMaps_[fieldsIdx]) &&
               !NewTupleMeta::isFieldBmapMarkedInvalid(bitMaps_[fieldsIdx]);
    }

    MustInline MustCheck DfFieldValue get(size_t fieldsIdx,
                                          size_t numFields,
                                          DfFieldType fieldType,
                                          bool *retIsValid) const
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        *retIsValid = isValid(fieldsIdx);
        return fields_[fieldsIdx];
    }

    MustInline MustCheck DfFieldValue get(size_t fieldsIdx,
                                          bool *retIsValid) const
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        *retIsValid = isValid(fieldsIdx);
        return fields_[fieldsIdx];
    }

    MustInline void setInvalid(size_t fieldsIdx)
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        bitMaps_[fieldsIdx] = NewTupleMeta::BmapInvalid;
#ifdef TUPLE_BUFFER_POISON
        memset(&fields_[fieldsIdx], FieldPoison, sizeof(DfFieldValue));
#endif  // TUPLE_BUFFER_POISON
    }

    MustInline void setInvalid(size_t fieldsIdx, size_t numFields)
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        assert(fieldsIdx + numFields <= TupleMaxNumValuesPerRecord &&
               "fieldsIdx + numFields out of bounds");
        memset(&bitMaps_[fieldsIdx], NewTupleMeta::BmapInvalid, numFields);
#ifdef TUPLE_BUFFER_POISON
        memset(&fields_[fieldsIdx],
               FieldPoison,
               sizeof(DfFieldValue) * numFields);
#endif  // TUPLE_BUFFER_POISON
    }

    void set(size_t fieldsIdx, DfFieldValue value, DfFieldType type);

    MustInline MustCheck bool isKeyFNF(size_t keyIdx)
    {
        return keyIdx != (size_t) NewTupleMeta::DfInvalidIdx &&
               !isValid(keyIdx);
    }

    void cloneTo(const NewTupleMeta *tupMeta, NewTupleValues *dstTuple) const
    {
        size_t numFields = tupMeta->getNumFields();
        for (size_t ii = 0; ii < numFields; ii++) {
            dstTuple->fields_[ii] = fields_[ii];
            assert(bitMaps_[ii] <= NewTupleMeta::BmapMask &&
                   "Unknown fieldBmap");
#ifdef DEBUG
            DfFieldType fieldType = tupMeta->getFieldType(ii);
            uint8_t fieldBmap = bitMaps_[ii];
            switch (fieldType) {
            case DfTimespec:
            case DfMoney:
            case DfString:
            case DfScalarObj:
                assert(fieldBmap <= NewTupleMeta::BmapMask &&
                       "Unknown fieldBmap");
                break;
            case DfUnknown:
                // If field type is DfUnknown, bitmap value would have indicated
                // an invalid field and we will not reached here.
                assert((NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
                        NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap)) &&
                       "Unknown fieldBmap");
                break;
            default:
                break;
            }
#endif  // DEBUG
            dstTuple->bitMaps_[ii] = bitMaps_[ii];
        }
    }

    void cloneTo(const NewTupleMeta *tupMeta,
                 DfFieldValue *fieldsOut,
                 uint8_t *bitMapsOut) const
    {
        size_t numFields = tupMeta->getNumFields();
        for (size_t ii = 0; ii < numFields; ii++) {
            fieldsOut[ii] = fields_[ii];
            assert(bitMaps_[ii] <= NewTupleMeta::BmapMask &&
                   "Unknown fieldBmap");
            bitMapsOut[ii] = bitMaps_[ii];
        }
    }

    void cloneFrom(const NewTupleMeta *tupMeta,
                   DfFieldValue *fieldsIn,
                   uint8_t *bitMapsIn)
    {
        size_t numFields = tupMeta->getNumFields();
        for (size_t ii = 0; ii < numFields; ii++) {
            fields_[ii] = fieldsIn[ii];
            assert(bitMapsIn[ii] <= NewTupleMeta::BmapMask &&
                   "Unknown fieldBmap");
            bitMaps_[ii] = bitMapsIn[ii];
        }
    }

    NewTupleValues()
    {
#ifdef TUPLE_BUFFER_POISON
        poison();
#endif  // TUPLE_BUFFER_POISON
    }
    ~NewTupleValues() = default;

  protected:
    MustInline MustCheck DfFieldValue get(size_t fieldsIdx,
                                          size_t numFields,
                                          DfFieldType fieldType) const
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        return fields_[fieldsIdx];
    }

    MustInline MustCheck DfFieldValue get(size_t fieldsIdx, size_t numFields)
    {
        assert(fieldsIdx < TupleMaxNumValuesPerRecord &&
               "fieldsIdx out of bounds");
        return fields_[fieldsIdx];
    }

    MustInline MustCheck uintptr_t getBitMap(size_t fieldsIdx)
    {
        return bitMaps_[fieldsIdx];
    }

    void poison()
    {
        memset(bitMaps_, NewTupleMeta::BmapPoison, TupleMaxNumValuesPerRecord);
        memset(fields_,
               FieldPoison,
               TupleMaxNumValuesPerRecord * sizeof(DfFieldValue));
    }
};

#endif  // _NEW_TUPLE_TYPES_H
