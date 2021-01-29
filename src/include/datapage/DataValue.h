// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAVALUE_H_
#define _DATAVALUE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/ProtoFieldValue.pb.h"
#include "df/DataFormat.h"
#include "util/VarInt.h"
#include "util/ProtoWrap.h"

struct Accessor;

enum class ValueType : uint8_t {
    Object,
    Array,
    String,

    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,

    Boolean,
    Timestamp,
    Null,
    Money,

    NumValueTypes,  // must be last 2
    Invalid,
};

inline ValueType
dfTypeToValueType(DfFieldType dfType)
{
    switch (dfType) {
    case DfString:
        return ValueType::String;
    case DfBoolean:
        return ValueType::Boolean;
    case DfInt32:
        return ValueType::Int32;
    case DfInt64:
        return ValueType::Int64;
    case DfUInt32:
        return ValueType::UInt32;
    case DfUInt64:
        return ValueType::UInt64;
    case DfFloat32:
        return ValueType::Float32;
    case DfFloat64:
        return ValueType::Float64;
    case DfTimespec:
        return ValueType::Timestamp;
    case DfMoney:
        return ValueType::Money;
    default:
        assert(false && "this should never happen");
        return ValueType::Invalid;
    }
}

inline DfFieldType
valueTypeToDfType(ValueType type)
{
    switch (type) {
    case ValueType::String:
        return DfString;
    case ValueType::Boolean:
        return DfBoolean;
    case ValueType::Int32:
        return DfInt32;
    case ValueType::Int64:
        return DfInt64;
    case ValueType::UInt32:
        return DfUInt32;
    case ValueType::UInt64:
        return DfUInt64;
    case ValueType::Float32:
        return DfFloat32;
    case ValueType::Float64:
        return DfFloat64;
    case ValueType::Object:
        return DfObject;
    case ValueType::Array:
        return DfArray;
    case ValueType::Timestamp:
        return DfTimespec;
    case ValueType::Null:
        return DfNull;
    case ValueType::Money:
        return DfMoney;
    case ValueType::Invalid:
    case ValueType::NumValueTypes:
        assert(false && "this should never happen");
        return DfUnknown;
    }
}

// This is primarily useful for assertions about the type
inline bool
valueTypeValid(ValueType type)
{
    return type >= ValueType::Object && type < ValueType::Invalid;
}

class DataValue
{
  public:
    struct String {
        int64_t length;
        const char *str;
    };
    struct Timestamp {
        static constexpr const int32_t ByteSize = 9;
        int64_t ms;       // milliseconds since epoch in UTC
        int8_t tzoffset;  // offset in hours to reach desired tz
    };

    DataValue() = default;

    ~DataValue() = default;

    inline void setString(const char *str, int64_t length)
    {
        string_.str = str;
        string_.length = length;
    }

    inline void setBool(bool boolVal) { bool_ = boolVal; }

    inline void setInt32(int32_t intVal) { int32_ = intVal; }

    inline void setInt64(int64_t intVal) { int64_ = intVal; }

    inline void setUInt32(uint32_t intVal) { uint32_ = intVal; }

    inline void setUInt64(uint64_t intVal) { uint64_ = intVal; }

    inline void setFloat32(float floatVal) { float32_ = floatVal; }

    inline void setFloat64(double doubleVal) { float64_ = doubleVal; }

    inline void setTimestamp(int64_t ms, int8_t tzoffset)
    {
        timestamp_.ms = ms;
        timestamp_.tzoffset = tzoffset;
    }

    inline void setNumeric(XlrDfp numericVal) { numeric_ = numericVal; }

    inline void setProto(ProtoFieldValue *protoValue) { proto_ = protoValue; }

    inline MustCheck Status setFromFieldValue(const DfFieldValue *dfValue,
                                              const DfFieldType type)
    {
        Status status;
        switch (type) {
        case DfString:
            setString(dfValue->stringVal.strActual, dfValue->stringVal.strSize);
            break;
        case DfBoolean:
            setBool(dfValue->boolVal);
            break;
        case DfInt32:
            setInt32(dfValue->int32Val);
            break;
        case DfInt64:
            setInt64(dfValue->int64Val);
            break;
        case DfUInt32:
            setUInt32(dfValue->uint32Val);
            break;
        case DfUInt64:
            setUInt64(dfValue->uint64Val);
            break;
        case DfFloat32:
            setFloat32(dfValue->float32Val);
            break;
        case DfFloat64:
            setFloat64(dfValue->float64Val);
            break;
        case DfTimespec:
            setTimestamp(dfValue->timeVal.ms, dfValue->timeVal.tzoffset);
            break;
        case DfMoney:
            setNumeric(dfValue->numericVal);
            break;
        case DfScalarObj: {
            DfFieldValue scalarVal;
            status = dfValue->scalarVal->getValue(&scalarVal);
            BailIfFailed(status);

            status =
                setFromFieldValue(&scalarVal, dfValue->scalarVal->fieldType);
            BailIfFailed(status);
            break;
        }
        default:
            assert(false && "this should never happen");
            status = StatusFailed;
        }
    CommonExit:
        return status;
    }

    inline int32_t getSize(ValueType type) const
    {
        int32_t bytesWritten;
        serialize<true, false>(type, NULL, 0, &bytesWritten);
        return bytesWritten;
    }

    // Returns the number of bytes to serialize this value.
    // The dryRun template argument prevents any actual data from being written.
    // This prevents us from needing a distinct 'getSize' function separate from
    // serialize itself. If it is a dryRun, we don't respect the bufferSize
    template <bool dryRun, bool checkSpace>
    inline bool serialize(ValueType type,
                          uint8_t *buffer,
                          int32_t bufferSize,
                          int32_t *serializedSize) const
    {
        if (!dryRun) {
            assert(buffer);
        }
        if (checkSpace) {
            assert(bufferSize >= 0 && "we allow 0 buffer for things like NULL");
        }
        // Leave this unitialized so that failure to set will cause compile fail
        int32_t fieldSize;
        switch (type) {
        case ValueType::String: {
            // We first have the length of the string, then the entire
            // serialized string with no null terminator (if we want to get
            // fancy, this code could use the Small String Optimization)
            size_t varintLength;
            bool success = encodeVarint<dryRun, checkSpace>(string_.length,
                                                            buffer,
                                                            bufferSize,
                                                            &varintLength);
            if (unlikely(!success)) {
                assert(checkSpace);
                return false;
            }
            fieldSize = varintLength + string_.length;
            if (checkSpace && unlikely(bufferSize < fieldSize)) {
                return false;
            }
            if (!dryRun) {
                memcpy(buffer + varintLength, string_.str, string_.length);
            }
            break;
        }
        case ValueType::Boolean:
            if (checkSpace && unlikely(bufferSize < (int32_t) sizeof(bool_))) {
                return false;
            }
            if (!dryRun) {
                *((bool *) buffer) = bool_;
            }
            fieldSize = sizeof(bool_);
            break;
        case ValueType::Int32: {
            size_t varintLength;
            uint32_t zigZagInt = zigZagEncode32(int32_);
            bool success = encodeVarint<dryRun, checkSpace>(zigZagInt,
                                                            buffer,
                                                            bufferSize,
                                                            &varintLength);
            if (unlikely(!success)) {
                assert(checkSpace && "this should be the only reason we fail");
                return false;
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::Int64: {
            size_t varintLength;
            uint64_t zigZagInt = zigZagEncode64(int64_);
            bool success = encodeVarint<dryRun, checkSpace>(zigZagInt,
                                                            buffer,
                                                            bufferSize,
                                                            &varintLength);
            if (unlikely(!success)) {
                assert(checkSpace && "this should be the only reason we fail");
                return false;
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::UInt32: {
            size_t varintLength;
            bool success = encodeVarint<dryRun, checkSpace>(uint32_,
                                                            buffer,
                                                            bufferSize,
                                                            &varintLength);
            if (unlikely(!success)) {
                assert(checkSpace && "this should be the only reason we fail");
                return false;
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::UInt64: {
            size_t varintLength;
            bool success = encodeVarint<dryRun, checkSpace>(uint64_,
                                                            buffer,
                                                            bufferSize,
                                                            &varintLength);
            if (unlikely(!success)) {
                assert(checkSpace && "this should be the only reason we fail");
                return false;
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::Float32:
            static_assert(sizeof(float32_) == 4, "floats are 4 bytes");
            if (checkSpace &&
                unlikely(bufferSize < (int32_t) sizeof(float32_))) {
                return false;
            }
            if (!dryRun) {
                memcpy(buffer, &float32_, sizeof(float32_));
            }
            fieldSize = sizeof(float32_);
            break;
        case ValueType::Float64:
            static_assert(sizeof(float64_) == 8, "doubles are 8 bytes");
            if (checkSpace &&
                unlikely(bufferSize < (int32_t) sizeof(float64_))) {
                return false;
            }
            if (!dryRun) {
                memcpy(buffer, &float64_, sizeof(float64_));
            }
            fieldSize = sizeof(float64_);
            break;
        case ValueType::Timestamp:
            static_assert(sizeof(timestamp_.ms) + sizeof(timestamp_.tzoffset) ==
                              Timestamp::ByteSize,
                          "without padding, timestamp should be 9 bytes");
            static_assert(offsetof(Timestamp, tzoffset) +
                                  sizeof(timestamp_.tzoffset) ==
                              Timestamp::ByteSize,
                          "the last bytes of this should be padding");
            if (checkSpace &&
                unlikely(bufferSize < (int32_t) sizeof(float64_))) {
                return false;
            }
            if (!dryRun) {
                memcpy(buffer, &timestamp_, Timestamp::ByteSize);
            }
            fieldSize = Timestamp::ByteSize;
            break;
        case ValueType::Money:
            static_assert(sizeof(numeric_) == 16, "numerics are 16 bytes");
            if (checkSpace &&
                unlikely(bufferSize < (int32_t) sizeof(numeric_))) {
                return false;
            }
            if (!dryRun) {
                memcpy(buffer, &numeric_, sizeof(numeric_));
            }
            fieldSize = sizeof(numeric_);
            break;
        case ValueType::Object:
        case ValueType::Array: {
            // We first have the length of the protobuf blob as a varint, then
            // the blob itself
            int32_t fieldBlobSize = proto_->ByteSizeLong();
            size_t varintLength;
            bool success = encodeVarint<dryRun, checkSpace>(fieldBlobSize,
                                                            buffer,
                                                            bufferSize,
                                                            &varintLength);
            if (unlikely(!success)) {
                assert(checkSpace && "we only fail for space reasons");
                return false;
            }
            fieldSize = varintLength + fieldBlobSize;
            if (checkSpace && unlikely(bufferSize < fieldSize)) {
                return false;
            }
            if (!dryRun) {
                Status status = pbSerializeToArray(proto_,
                                                   buffer + varintLength,
                                                   bufferSize - varintLength);
                assert(status == StatusOk &&
                       "caller should provide an appropriate buffer");
            }
            break;
        }
        case ValueType::Null:
            fieldSize = 0;
            break;
        case ValueType::NumValueTypes:
        case ValueType::Invalid:
            assert(false);
            break;
        }
        if (serializedSize) {
            *serializedSize = fieldSize;
        }
        return true;
    }

    // Deserialization functionality
    // XXX we might want to refactor this and pull pValue out
    // pValue is provided so that if we need to deserialize a proto value, we
    // have a place to put it
    template <bool dryRun>
    inline Status deserializeFromBuffer(ValueType type,
                                        const uint8_t *buffer,
                                        int32_t bufferSize,
                                        int32_t *serializedSize,
                                        ProtoFieldValue *pValue)
    {
        Status status;
        int32_t fieldSize;
        switch (type) {
        case ValueType::String: {
            size_t varintLength;
            int64_t length;
            bool success =
                decodeVarint(buffer, bufferSize, &length, &varintLength);
            if (unlikely(!success)) {
                return StatusFailedParseStrFieldLen;
            }

            if (!dryRun) {
                string_.length = length;
                string_.str = (const char *) buffer + varintLength;
            }

            fieldSize = varintLength + length;
            break;
        }
        case ValueType::Boolean:
            if (unlikely(bufferSize < (int32_t) sizeof(bool_))) {
                return StatusFailedParseBoolField;
            }
            if (!dryRun) {
                bool_ = *((bool *) buffer);
            }
            fieldSize = sizeof(bool_);
            break;
        case ValueType::Int32: {
            size_t varintLength;
            uint32_t rawInt;
            bool success = decodeVarint(buffer,
                                        bufferSize,
                                        dryRun ? NULL : &rawInt,
                                        &varintLength);
            if (unlikely(!success)) {
                return StatusFailedParseInt32Field;
            }
            if (!dryRun) {
                int32_ = zigZagDecode32(rawInt);
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::Int64: {
            size_t varintLength;
            uint64_t rawInt;
            bool success = decodeVarint(buffer,
                                        bufferSize,
                                        dryRun ? NULL : &rawInt,
                                        &varintLength);
            if (unlikely(!success)) {
                return StatusFailedParseInt64Field;
            }
            if (!dryRun) {
                int64_ = zigZagDecode64(rawInt);
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::UInt32: {
            size_t varintLength;
            bool success = decodeVarint(buffer,
                                        bufferSize,
                                        dryRun ? NULL : &uint32_,
                                        &varintLength);
            if (unlikely(!success)) {
                return StatusFailedParseUint32Field;
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::UInt64: {
            size_t varintLength;
            bool success = decodeVarint(buffer,
                                        bufferSize,
                                        dryRun ? NULL : &uint64_,
                                        &varintLength);
            if (unlikely(!success)) {
                return StatusFailedParseUint64Field;
            }
            fieldSize = varintLength;
            break;
        }
        case ValueType::Float32:
            if (unlikely(bufferSize < (int32_t) sizeof(float32_))) {
                return StatusFailedParseFloat32Field;
            }
            if (!dryRun) {
                memcpy(&float32_, buffer, (int32_t) sizeof(float32_));
            }
            fieldSize = sizeof(float32_);
            break;
        case ValueType::Float64:
            if (unlikely(bufferSize < (int32_t) sizeof(float64_))) {
                return StatusFailedParseFloat64Field;
            }
            if (!dryRun) {
                memcpy(&float64_, buffer, (int32_t) sizeof(float64_));
            }
            fieldSize = sizeof(float64_);
            break;
        case ValueType::Timestamp:
            if (unlikely(bufferSize < Timestamp::ByteSize)) {
                return StatusFailedParseTimestampField;
            }
            if (!dryRun) {
                memcpy(&timestamp_, buffer, Timestamp::ByteSize);
            }
            fieldSize = Timestamp::ByteSize;
            break;
        case ValueType::Money:
            if (unlikely(bufferSize < (int32_t) sizeof(numeric_))) {
                return StatusFailedParseNumericField;
            }
            if (!dryRun) {
                memcpy(&numeric_, buffer, (int32_t) sizeof(numeric_));
            }
            fieldSize = sizeof(numeric_);
            break;
        case ValueType::Object:
        case ValueType::Array: {
            // The caller gave us a proto value to unpack into
            proto_ = pValue;
            size_t varintLength;
            int32_t fieldBlobSize;
            bool success =
                decodeVarint(buffer, bufferSize, &fieldBlobSize, &varintLength);
            if (unlikely(!success)) {
                return StatusFailedParseProtoValFieldLen;
            }
            fieldSize = varintLength + fieldBlobSize;

            if (unlikely(bufferSize < (int32_t) fieldSize)) {
                return StatusFailedParseProtoValField;
            }

            // Initialize the proto field
            if (!dryRun) {
                status = pbParseFromArray(proto_,
                                          buffer + varintLength,
                                          fieldBlobSize);
                if (unlikely(status != StatusOk)) {
                    return status;
                }
                // Check that the value is what we expect
                assert(proto_->dataValue_case() ==
                       (type == ValueType::Array
                            ? ProtoFieldValue::kArrayValue
                            : ProtoFieldValue::kObjectValue));
                assert(fieldBlobSize = proto_->ByteSizeLong());
            }
            break;
        }
        case ValueType::Null:
            fieldSize = 0;
            break;
        case ValueType::NumValueTypes:
        case ValueType::Invalid:
            assert(false);
            return StatusInvalidFieldType;
        }

        if (serializedSize) {
            *serializedSize = fieldSize;
        }
        return status;
    }

    // Members
    union {
        ProtoFieldValue *proto_;
        String string_;
        Timestamp timestamp_;
        bool bool_;
        int32_t int32_;
        int64_t int64_;
        uint32_t uint32_;
        uint64_t uint64_;
        float float32_;
        double float64_;
        XlrDfp numeric_;  // Storage for IEEE754 DFP type
    };
};

struct TypedDataValue {
    inline void setString(const char *str, int64_t length)
    {
        type_ = ValueType::String;
        value_.setString(str, length);
    }

    inline void setBool(bool boolVal)
    {
        type_ = ValueType::Boolean;
        value_.setBool(boolVal);
    }

    inline void setInt32(int32_t intVal)
    {
        type_ = ValueType::Int32;
        value_.setInt32(intVal);
    }

    inline void setInt64(int64_t intVal)
    {
        type_ = ValueType::Int64;
        value_.setInt64(intVal);
    }

    inline void setUInt32(uint32_t intVal)
    {
        type_ = ValueType::UInt32;
        value_.setUInt32(intVal);
    }

    inline void setUInt64(uint64_t intVal)
    {
        type_ = ValueType::UInt64;
        value_.setUInt64(intVal);
    }

    inline void setFloat32(float floatVal)
    {
        type_ = ValueType::Float32;
        value_.setFloat32(floatVal);
    }

    inline void setFloat64(double doubleVal)
    {
        type_ = ValueType::Float64;
        value_.setFloat64(doubleVal);
    }

    inline void setTimestamp(int64_t ms, int8_t tzoffset)
    {
        type_ = ValueType::Timestamp;
        value_.setTimestamp(ms, tzoffset);
    }

    inline void setNumeric(XlrDfp numericVal)
    {
        type_ = ValueType::Money;
        value_.setNumeric(numericVal);
    }

    inline void setObject(ProtoFieldValue *objValue)
    {
        type_ = ValueType::Object;
        assert(objValue->dataValue_case() == ProtoFieldValue::kObjectValue);
        value_.setProto(objValue);
    }

    inline void setArray(ProtoFieldValue *arrValue)
    {
        type_ = ValueType::Array;
        assert(arrValue->dataValue_case() == ProtoFieldValue::kArrayValue);
        value_.setProto(arrValue);
    }

    inline void setNull() { type_ = ValueType::Null; }

    int32_t getSize() const { return value_.getSize(type_); }

    // Members
    ValueType type_;
    DataValue value_;
};

// See above for class description
class DataValueReader final
{
  public:
    DataValueReader() = default;
    ~DataValueReader()
    {
        if (buf_) {
            delete[] buf_;
            buf_ = NULL;
        }
        if (protoValue_) {
            // protoValue_ is allocated in protobuf Arena and freeing it
            // requires destructing the Arena itself.
            protoValue_ = NULL;
        }
        if (arena_) {
            delete arena_;
            arena_ = NULL;
        }
    }

    // This is to use Reader function below
    // using pre-populated dValue and ValType
    void setFromTypedDataValue(DataValue dValue, ValueType valType)
    {
        value_ = dValue;
        type_ = valType;
        if (type_ == ValueType::Object || type_ == ValueType::Array) {
            protoValue_ = dValue.proto_;
        }
    }
    //
    // Serialization/Deserialization functions
    //
    int32_t getSize() const;

    MustCheck Status initProtoValue();

    // Caller must guarantee that this is a valid buffer
    template <bool dryRun>
    MustCheck Status parseFromBuffer(const uint8_t *buffer,
                                     int32_t bufferSize,
                                     int32_t *valueSize,
                                     ValueType type)
    {
        Status status = StatusOk;

        // Only Array and Object types need to use Arenas to optimize lots of
        // small heap allocations to deserialize protobuf.
        if (type == ValueType::Object || type == ValueType::Array) {
            status = initProtoValue();
            BailIfFailed(status);
        }

        status = value_.deserializeFromBuffer<dryRun>(type,
                                                      buffer,
                                                      bufferSize,
                                                      valueSize,
                                                      protoValue_);
        BailIfFailed(status);

        type_ = type;

    CommonExit:
        return status;
    }

    //
    // Reading functions
    //
    MustCheck Status getAsFieldValue(size_t valueBufSize,
                                     DfFieldValue *value,
                                     DfFieldType *type) const;

    MustCheck Status getAsFieldValueInArray(size_t valueBufSize,
                                            DfFieldValueArray *valueArray,
                                            unsigned arrayIndex,
                                            DfFieldType *type) const;

    MustCheck Status getAsJsonField(json_t *fieldObject,
                                    const char *fatptrPrefix,
                                    const char *fieldName) const;

    MustCheck Status getAsProto(ProtoFieldValue *outVal) const;
    MustCheck Status getAsTypedDataValue(TypedDataValue *outVal) const;

    MustCheck Status accessNestedField(const Accessor *accessor,
                                       DataValueReader *outValue) const;

  private:
    MustCheck Status setFromProtoValue(const ProtoFieldValue *protoValue);

    MustCheck Status storeStringInBuf(const char *str, int32_t strLen);

    // We use this to hold any protobuf values that we happen to need
    ProtoFieldValue *protoValue_ = NULL;
    // We use this to hold temporary data needed to read our data.
    // Specifically, we copy strings into this buffer if they were originally
    // in a nested protobuf object, since  we don't have a way to swap a
    // protobuf object A with a protobuf object A.B nested inside of it.
    char *buf_ = NULL;

    DataValue value_;
    ValueType type_;
    google::protobuf::Arena *arena_ = NULL;
};

#endif  // _DATAVALUE_H_
