// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "newtupbuf/NewTupleTypes.h"
#include "df/DataFormat.h"

constexpr DfFieldValue NewTupleMeta::InvalidKey;
constexpr DfFieldValue NewTupleMeta::InvalidField;
constexpr uint8_t NewTupleMeta::BmapValueValidFieldSizeLUT[];

void
NewTupleValues::set(size_t fieldsIdx, DfFieldValue value, DfFieldType type)
{
    assert(fieldsIdx < TupleMaxNumValuesPerRecord && "fieldsIdx out of bounds");
    fields_[fieldsIdx] = value;
    uint8_t fieldBmap = NewTupleMeta::BmapInvalid;

    if (DataFormat::fieldTypeIsFixed(type)) {
        if (value.uint64Val & NewTupleMeta::SixBytesFsClearMask) {
            fieldBmap = NewTupleMeta::BmapEightBytesField;
        } else if (value.uint64Val & NewTupleMeta::FiveBytesFsClearMask) {
            fieldBmap = NewTupleMeta::BmapSixBytesField;
        } else if (value.uint64Val & NewTupleMeta::FourBytesFsClearMask) {
            fieldBmap = NewTupleMeta::BmapFiveBytesField;
        } else if (value.uint64Val & NewTupleMeta::ThreeBytesFsClearMask) {
            fieldBmap = NewTupleMeta::BmapFourBytesField;
        } else if (value.uint64Val & NewTupleMeta::TwoBytesFsClearMask) {
            fieldBmap = NewTupleMeta::BmapThreeBytesField;
        } else if (value.uint64Val & NewTupleMeta::OneByteFsClearMask) {
            fieldBmap = NewTupleMeta::BmapTwoBytesField;
        } else {
            fieldBmap = NewTupleMeta::BmapOneByteField;
        }
    } else {
        switch (type) {
        case DfString: {
            size_t srcSize = value.stringVal.strSize;
            if (srcSize & NewTupleMeta::ThreeBytesFsClearMask) {
                fieldBmap = NewTupleMeta::BmapFourBytesField;
            } else if (srcSize & NewTupleMeta::TwoBytesFsClearMask) {
                fieldBmap = NewTupleMeta::BmapThreeBytesField;
            } else if (srcSize & NewTupleMeta::OneByteFsClearMask) {
                fieldBmap = NewTupleMeta::BmapTwoBytesField;
            } else {
                fieldBmap = NewTupleMeta::BmapOneByteField;
            }
            break;
        }
        case DfScalarObj: {
            size_t srcSize = value.scalarVal->getUsedSize();
            if (srcSize & NewTupleMeta::ThreeBytesFsClearMask) {
                fieldBmap = NewTupleMeta::BmapFourBytesField;
            } else if (srcSize & NewTupleMeta::TwoBytesFsClearMask) {
                fieldBmap = NewTupleMeta::BmapThreeBytesField;
            } else if (srcSize & NewTupleMeta::OneByteFsClearMask) {
                fieldBmap = NewTupleMeta::BmapTwoBytesField;
            } else {
                fieldBmap = NewTupleMeta::BmapOneByteField;
            }
            break;
        }
        case DfMoney:
        case DfTimespec:
            fieldBmap = NewTupleMeta::BmapOneByteField;
            break;
        case DfUnknown:
            // If field type is DfUnknown, bitmap value would have indicated
            // an invalid field and we will not reached here.
            assert(0 && "Valid field cannot have field type Dfunknown");
            break;
        default:
            assert(0 && "Uknown variable length field type");
            break;
        }
        assert((fieldBmap == NewTupleMeta::BmapOneByteField ||
                fieldBmap == NewTupleMeta::BmapTwoBytesField ||
                fieldBmap == NewTupleMeta::BmapThreeBytesField ||
                fieldBmap == NewTupleMeta::BmapFourBytesField) &&
               "Unknown fieldBmap");
    }
    assert(fieldBmap <= NewTupleMeta::BmapMask && "Unknown fieldBmap");
    bitMaps_[fieldsIdx] = fieldBmap;
}
