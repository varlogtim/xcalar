// Copyright 2017-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <string.h>
#include <math.h>

#include "StrlFunc.h"
#include "datapage/DataValue.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "dataformat/DataFormatJson.h"
#include "util/ProtoWrap.h"
#include "util/DFPUtils.h"

//
// DataValueReader
//

int32_t
DataValueReader::getSize() const
{
    assert(type_ != ValueType::Invalid);
    return value_.getSize(type_);
}

Status
DataValueReader::initProtoValue()
{
    Status status = StatusOk;
    try {
        if (!arena_) {
            arena_ = new google::protobuf::Arena();
            BailIfNull(arena_);
        }
        protoValue_ =
            google::protobuf::Arena::CreateMessage<ProtoFieldValue>(arena_);
        BailIfNull(protoValue_);
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
DataValueReader::getAsFieldValue(size_t valueBufSize,
                                 DfFieldValue *value,
                                 DfFieldType *type) const
{
    Status status = StatusOk;
    switch (type_) {
    case ValueType::String:
        *type = DfString;
        if ((int32_t) valueBufSize <= value_.string_.length) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        // use memcpy here as data value might contain null in between
        if (value_.string_.length) {
            memcpy(value->stringValTmp,
                   value_.string_.str,
                   value_.string_.length);
        }
        value->stringValTmp[value_.string_.length - 1] = '\0';
        value->stringVal.strSize = value_.string_.length;
        break;
    case ValueType::Boolean:
        value->boolVal = value_.bool_;
        *type = DfBoolean;
        break;
    case ValueType::Int32:
        value->int32Val = value_.int32_;
        *type = DfInt32;
        break;
    case ValueType::Int64:
        value->int64Val = value_.int64_;
        *type = DfInt64;
        break;
    case ValueType::UInt32:
        value->uint32Val = value_.uint32_;
        *type = DfUInt32;
        break;
    case ValueType::UInt64:
        value->uint64Val = value_.uint64_;
        *type = DfUInt64;
        break;
    case ValueType::Float32:
        value->float32Val = value_.float32_;
        *type = DfFloat32;
        break;
    case ValueType::Float64:
        value->float64Val = value_.float64_;
        *type = DfFloat64;
        break;
    case ValueType::Money:
        value->numericVal = value_.numeric_;
        *type = DfMoney;
        break;
    case ValueType::Timestamp:
        value->timeVal.ms = value_.timestamp_.ms;
        value->timeVal.tzoffset = value_.timestamp_.tzoffset;
        *type = DfTimespec;
        break;
    case ValueType::Null:
        *type = DfNull;
        break;
    case ValueType::Object:
        // this needs an appropriate DfFieldType
        assert(false);
        break;
    case ValueType::Array:
        // this needs an appropriate DfFieldType
        assert(false);
        break;
    default:
        assert(false);
        status = StatusFailed;
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
DataValueReader::getAsFieldValueInArray(size_t valueBufSize,
                                        DfFieldValueArray *valueArray,
                                        unsigned arrayIndex,
                                        DfFieldType *type) const
{
    Status status = StatusOk;
    assert(arrayIndex == 0 && "figure this out when we support arrays");
    switch (type_) {
    case ValueType::String:
        if ((int32_t) valueBufSize <= value_.string_.length) {
            status = StatusOverflow;
            goto CommonExit;
        }
        if (value_.string_.length) {
            memcpy(valueArray->strVals[arrayIndex].strActual,
                   value_.string_.str,
                   value_.string_.length);
        }
        valueArray->strVals[arrayIndex].strActual[value_.string_.length] = '\0';
        valueArray->strVals[arrayIndex].strSize = value_.string_.length + 1;
        *type = DfString;
        break;
    case ValueType::Boolean:
        valueArray->boolVal[arrayIndex] = value_.bool_;
        *type = DfBoolean;
        break;
    case ValueType::Int32:
        valueArray->int32Val[arrayIndex] = value_.int32_;
        *type = DfInt32;
        break;
    case ValueType::Int64:
        valueArray->int64Val[arrayIndex] = value_.int64_;
        *type = DfInt64;
        break;
    case ValueType::UInt32:
        valueArray->uint32Val[arrayIndex] = value_.uint32_;
        *type = DfUInt32;
        break;
    case ValueType::UInt64:
        valueArray->uint64Val[arrayIndex] = value_.uint64_;
        *type = DfUInt64;
        break;
    case ValueType::Float32:
        valueArray->float32Val[arrayIndex] = value_.float32_;
        *type = DfFloat32;
        break;
    case ValueType::Float64:
        valueArray->float64Val[arrayIndex] = value_.float64_;
        *type = DfFloat64;
        break;
    case ValueType::Money:
        valueArray->numericVal[arrayIndex] = value_.numeric_;
        *type = DfMoney;
        break;
    case ValueType::Timestamp:
        valueArray->timeVal[arrayIndex].ms = value_.timestamp_.ms;
        valueArray->timeVal[arrayIndex].tzoffset = value_.timestamp_.tzoffset;
        *type = DfTimespec;
        break;
    case ValueType::Null:
        *type = DfNull;
        break;
    case ValueType::Array:
    case ValueType::Object:
        // these need an appropriate DfFieldTypes
    default:
        status = StatusDfFieldNoExist;
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
DataValueReader::getAsJsonField(json_t *recordObject,
                                const char *fatptrPrefix,
                                const char *fieldName) const
{
    Status status = StatusOk;
    int ret;
    char fullFieldName[DfMaxFieldNameLen + 1];
    json_t *fieldJson = NULL;

    if (fatptrPrefix != NULL && fatptrPrefix[0] != '\0') {
        ret = snprintf(fullFieldName,
                       sizeof(fullFieldName),
                       "%s%s%s",
                       fatptrPrefix,
                       DfFatptrPrefixDelimiter,
                       fieldName);
        if (ret >= (int) sizeof(fullFieldName)) {
            fullFieldName[xcMin(80, (int) DfMaxFieldNameLen)] = '\0';
            status = StatusNoBufs;
            goto CommonExit;
        }
    } else {
        ret = snprintf(fullFieldName, sizeof(fullFieldName), "%s", fieldName);
        if (ret >= (int) sizeof(fullFieldName)) {
            fullFieldName[xcMin(80, (int) DfMaxFieldNameLen)] = '\0';
            status = StatusNoBufs;
            goto CommonExit;
        }
    }
    // Set the appropriate json type; allow memory errors to be handled after
    // the switch
    switch (type_) {
    case ValueType::String:
        fieldJson = json_stringn(value_.string_.str, value_.string_.length);
        break;
    case ValueType::Boolean:
        fieldJson = json_boolean(value_.bool_);
        break;
    case ValueType::Int32:
        fieldJson = json_integer(value_.int32_);
        break;
    case ValueType::Int64:
        fieldJson = json_integer(value_.int64_);
        break;
    case ValueType::UInt32:
        fieldJson = json_integer(value_.uint32_);
        break;
    case ValueType::UInt64:
        fieldJson = json_integer(value_.uint64_);
        break;
    case ValueType::Float32:
        if (isnan(value_.float32_) || isinf(value_.float32_)) {
            // longest potential string is '-Infinity'
            char tmpBuf[10];
            verify(snprintf(tmpBuf, sizeof(tmpBuf), "%f", value_.float32_) <
                   static_cast<int>(sizeof(tmpBuf)));
            fieldJson = json_string(tmpBuf);
            BailIfNull(fieldJson);
        } else {
            fieldJson = json_real(value_.float32_);
            BailIfNull(fieldJson);
        }
        break;
    case ValueType::Float64:
        if (isnan(value_.float64_) || isinf(value_.float64_)) {
            // longest potential string is '-Infinity'
            char tmpBuf[10];
            verify(snprintf(tmpBuf, sizeof(tmpBuf), "%f", value_.float64_) <
                   static_cast<int>(sizeof(tmpBuf)));
            fieldJson = json_string(tmpBuf);
            BailIfNull(fieldJson);
        } else {
            fieldJson = json_real(value_.float64_);
            BailIfNull(fieldJson);
        }
        break;
    case ValueType::Money: {
        char buf[XLR_DFP_STRLEN];

        DFPUtils::get()->xlrNumericToString(buf, &value_.numeric_);
        fieldJson = json_string(buf);
        break;
    }
    case ValueType::Timestamp: {
        DfFieldValue strVal, fieldVal;
        char buf[TIMESTAMP_STR_LEN];  // buf to hold timestamp string

        memZero(&strVal, sizeof(DfFieldValue));
        memZero(&fieldVal, sizeof(DfFieldValue));
        strVal.stringValTmp = buf;
        fieldVal.timeVal.ms = value_.timestamp_.ms;
        fieldVal.timeVal.tzoffset = value_.timestamp_.tzoffset;
        status = convertTimestampToStr(&fieldVal, &strVal, sizeof(buf));
        assert(status == StatusOk);  // buf should be big enough to hold ts
        fieldJson = json_string(buf);
        break;
    }
    case ValueType::Null:
        fieldJson = json_null();
        break;
    // Fallthrough; objects and arrays are handled the same
    case ValueType::Object:
    case ValueType::Array:
        status = JsonFormatOps::convertProtoToJson(protoValue_, &fieldJson);
        BailIfFailed(status);
        assert(fieldJson != NULL);
        break;
    default:
        assert(false);
        status = StatusFailed;
        goto CommonExit;
    }
    BailIfNull(fieldJson);

    // This steals the jsonValue ref
    ret = json_object_set_new(recordObject, fullFieldName, fieldJson);
    if (ret == -1) {
        // This is probably the only real case this will happen
        status = StatusNoMem;
        goto CommonExit;
    }
    fieldJson = NULL;

CommonExit:
    if (fieldJson) {
        json_decref(fieldJson);
        fieldJson = NULL;
    }
    return status;
}

Status
DataValueReader::getAsProto(ProtoFieldValue *outVal) const
{
    Status status = StatusOk;
    // This is only used with arenas, which we need
    google::protobuf::Arena *arena = outVal->GetArena();
    assert(arena);
    try {
        switch (type_) {
        case ValueType::String:
            outVal->set_stringval(value_.string_.str, value_.string_.length);
            break;
        case ValueType::Boolean:
            outVal->set_boolval(value_.bool_);
            break;
        case ValueType::Int32:
            outVal->set_int32val(value_.int32_);
            break;
        case ValueType::Int64:
            outVal->set_int64val(value_.int64_);
            break;
        case ValueType::UInt32:
            outVal->set_uint32val(value_.uint32_);
            break;
        case ValueType::UInt64:
            outVal->set_uint64val(value_.uint64_);
            break;
        case ValueType::Float32:
            outVal->set_float32val(value_.float32_);
            break;
        case ValueType::Float64:
            outVal->set_float64val(value_.float64_);
            break;
        case ValueType::Money:
            for (size_t i = 0; i < ArrayLen(value_.numeric_.ieee); i++) {
                outVal->mutable_numericval()->add_val(value_.numeric_.ieee[i]);
            }
            break;
        case ValueType::Timestamp: {
            google::protobuf::Timestamp *protoTimeStamp =
                google::protobuf::Arena::CreateMessage<
                    google::protobuf::Timestamp>(arena);
            BailIfNull(protoTimeStamp);
            int64_t seconds = value_.timestamp_.ms / MSecsPerSec;
            int32_t nanos = (value_.timestamp_.ms % MSecsPerSec) * NSecsPerMSec;
            protoTimeStamp->set_seconds(seconds);
            protoTimeStamp->set_nanos(nanos);
            outVal->set_allocated_timeval(protoTimeStamp);
            break;
        }
        case ValueType::Null: {
            // Keep as not set
            break;
        }
        case ValueType::Object:  // Fallthrough
        case ValueType::Array: {
            outVal->CopyFrom(*protoValue_);
            break;
        }
        default:
            status = StatusInval;
            goto CommonExit;
        }
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
DataValueReader::getAsTypedDataValue(TypedDataValue *outVal) const
{
    Status status;
    outVal->type_ = type_;
    outVal->value_ = value_;
    return status;
}

Status
DataValueReader::accessNestedField(const Accessor *accessor,
                                   DataValueReader *outValue) const
{
    Status status = StatusOk;
    if (unlikely(accessor->nameDepth <= 1)) {
        return StatusInval;
    }

    const ProtoFieldValue *prevValue = protoValue_;

    for (int ii = 1; ii < accessor->nameDepth; ii++) {
        const AccessorName *thisName = &accessor->names[ii];
        switch (thisName->type) {
        case AccessorName::Type::Field: {
            if (prevValue->dataValue_case() != ProtoFieldValue::kObjectValue) {
                status = StatusDfFieldNoExist;
                goto CommonExit;
            }

            const ProtoFieldValue_ObjectValue *objValue;
            objValue = &prevValue->objectvalue();

            auto valueIter = objValue->values().find(thisName->value.field);
            if (valueIter == objValue->values().end()) {
                status = StatusDfFieldNoExist;
                goto CommonExit;
            }
            prevValue = &valueIter->second;

            break;
        }
        case AccessorName::Type::Subscript: {
            if (prevValue->dataValue_case() != ProtoFieldValue::kArrayValue) {
                status = StatusDfFieldNoExist;
                goto CommonExit;
            }
            const ProtoFieldValue_ArrayValue *arrayValue;
            arrayValue = &prevValue->arrayvalue();
            if (thisName->value.subscript >= arrayValue->elements().size()) {
                status = StatusDfFieldNoExist;
                goto CommonExit;
            }
            prevValue =
                &prevValue->arrayvalue().elements(thisName->value.subscript);
            break;
        }
        case AccessorName::Type::Invalid:
            // Fallthrough
        default:
            assert(false);
            status = StatusFailed;
            goto CommonExit;
        }
    }

    // We have the correct nested value; we can set it now
    try {
        status = outValue->setFromProtoValue(prevValue);
        BailIfFailed(status);
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
DataValueReader::setFromProtoValue(const ProtoFieldValue *protoValue)
{
    Status status;
    switch (protoValue->dataValue_case()) {
    case ProtoFieldValue::kStringVal: {
        auto stringVal = protoValue->stringval();
        status = storeStringInBuf(stringVal.c_str(), stringVal.size());
        if (unlikely(status != StatusOk)) {
            return status;
        }
        value_.setString(buf_, stringVal.size());
        type_ = ValueType::String;
        break;
    }
    case ProtoFieldValue::kBoolVal:
        value_.setBool(protoValue->boolval());
        type_ = ValueType::Boolean;
        break;
    case ProtoFieldValue::kUint32Val:
        value_.setUInt32(protoValue->uint32val());
        type_ = ValueType::UInt32;
        break;
    case ProtoFieldValue::kInt32Val:
        value_.setInt32(protoValue->int32val());
        type_ = ValueType::Int32;
        break;
    case ProtoFieldValue::kUint64Val:
        value_.setUInt64(protoValue->uint64val());
        type_ = ValueType::UInt64;
        break;
    case ProtoFieldValue::kInt64Val:
        value_.setInt64(protoValue->int64val());
        type_ = ValueType::Int64;
        break;
    case ProtoFieldValue::kFloat32Val:
        value_.setFloat32(protoValue->float32val());
        type_ = ValueType::Float32;
        break;
    case ProtoFieldValue::kFloat64Val:
        value_.setFloat64(protoValue->float64val());
        type_ = ValueType::Float64;
        break;
    case ProtoFieldValue::kTimeValFieldNumber: {
        auto tval = protoValue->timeval();
        int64_t ms = tval.seconds() * MSecsPerSec + tval.nanos() / NSecsPerMSec;
        value_.setTimestamp(ms, 0);
        type_ = ValueType::Timestamp;
        break;
    }
    case ProtoFieldValue::kByteVal:
    case ProtoFieldValue::kArrayValue:
    case ProtoFieldValue::kObjectValue:
    case ProtoFieldValue::DATAVALUE_NOT_SET:
        return StatusDfFieldNoExist;
    default:
        assert(false);
        return StatusFailed;
    }
    return status;
}

Status
DataValueReader::storeStringInBuf(const char *str, int32_t strLen)
{
    Status status;

    if (buf_ != NULL) {
        delete[] buf_;
        buf_ = NULL;
    }

    if (!strLen) {
        goto CommonExit;
    }

    buf_ = new (std::nothrow) char[strLen];
    BailIfNull(buf_);

    memcpy(buf_, str, strLen);

CommonExit:
    return status;
}
