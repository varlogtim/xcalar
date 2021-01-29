// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <execinfo.h>
#include <math.h>
#include <float.h>
#include <new>

#include <jansson.h>

#include "hash/Hash.h"
#include "df/DataFormat.h"
#include "dataformat/DataFormatJson.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "DataFormatConstants.h"
#include "dataset/BackingData.h"
#include "util/DFPUtils.h"

using namespace df;
JsonFormatOps *JsonFormatOps::instance;
static constexpr const char *moduleName = "libdf";

Status
JsonFormatOps::init()
{
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(JsonFormatOps), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    JsonFormatOps::instance = new (ptr) JsonFormatOps();

    return StatusOk;
}

JsonFormatOps *
JsonFormatOps::get()
{
    assert(instance);
    return instance;
}

void
JsonFormatOps::destroy()
{
    JsonFormatOps *inst = JsonFormatOps::get();

    inst->~JsonFormatOps();
    memFree(inst);
    JsonFormatOps::instance = NULL;
}

Status
JsonFormatOps::convertProtoToJson(const ProtoFieldValue *protoValue,
                                  json_t **jsonOut)
{
    Status status = StatusOk;
    *jsonOut = NULL;

    switch (protoValue->dataValue_case()) {
    case ProtoFieldValue::kStringVal:
        *jsonOut = json_string(protoValue->stringval().c_str());
        break;
    case ProtoFieldValue::kBoolVal:
        *jsonOut = json_boolean(protoValue->boolval());
        break;
    case ProtoFieldValue::kUint32Val:
        *jsonOut = json_integer(protoValue->uint32val());
        break;
    case ProtoFieldValue::kInt32Val:
        *jsonOut = json_integer(protoValue->int32val());
        break;
    case ProtoFieldValue::kUint64Val:
        *jsonOut = json_integer(protoValue->uint64val());
        break;
    case ProtoFieldValue::kInt64Val:
        *jsonOut = json_integer(protoValue->int64val());
        break;
    case ProtoFieldValue::kFloat32Val:
        *jsonOut = json_real(protoValue->float32val());
        break;
    case ProtoFieldValue::kFloat64Val:
        *jsonOut = json_real(protoValue->float64val());
        break;
    case ProtoFieldValue::kTimeValFieldNumber: {
        google::protobuf::Timestamp ts = protoValue->timeval();
        DfFieldValue strVal, fieldVal;
        char buf[TIMESTAMP_STR_LEN];  // buf to hold timestamp string

        memZero(&strVal, sizeof(DfFieldValue));
        memZero(&fieldVal, sizeof(DfFieldValue));
        strVal.stringValTmp = buf;
        fieldVal.timeVal.ms =
            (ts.seconds() * MSecsPerSec) + (ts.nanos() / NSecsPerMSec);
        status = convertTimestampToStr(&fieldVal, &strVal, sizeof(buf));
        assert(status == StatusOk);  // buf should be big enough to hold ts
        *jsonOut = json_string(buf);
        break;
    }
    case ProtoFieldValue::kNumericVal: {
        Status status;
        char buf[XLR_DFP_STRLEN];
        DFPUtils *dfp = DFPUtils::get();
        XlrDfp dfpVal;

        if (protoValue->numericval().val_size() == ArrayLen(dfpVal.ieee)) {
            for (size_t i = 0; i < ArrayLen(dfpVal.ieee); i++) {
                dfpVal.ieee[i] = protoValue->numericval().val(i);
            }
        } else {
            dfp->xlrDfpNan(&dfpVal);
        }
        dfp->xlrNumericToStringInternal(buf, &dfpVal, false);
        *jsonOut = json_string(buf);
    } break;
    case ProtoFieldValue::kByteVal:
        // XXX - there is no json byte type; make this base64 or something
        assert(false);
        break;
    case ProtoFieldValue::kArrayValue: {
        const ProtoFieldValue_ArrayValue *arrayVal = &protoValue->arrayvalue();
        *jsonOut = json_array();
        if (*jsonOut == NULL) {
            break;
        }
        for (int ii = 0; ii < arrayVal->elements_size(); ii++) {
            // Recursively create json elements from this array
            json_t *thisJson;
            status = convertProtoToJson(&arrayVal->elements(ii), &thisJson);
            BailIfFailed(status);
            assert(thisJson != NULL);
            // This steals the reference to thisJson
            int ret = json_array_append_new(*jsonOut, thisJson);
            if (ret != 0) {
                json_decref(thisJson);
                thisJson = NULL;
                status = StatusNoMem;
                goto CommonExit;
            }
            thisJson = NULL;
        }
        break;
    }
    case ProtoFieldValue::kObjectValue: {
        const ProtoFieldValue_ObjectValue *objVal = &protoValue->objectvalue();
        *jsonOut = json_object();
        if (*jsonOut == NULL) {
            break;
        }
        google::protobuf::Map<std::basic_string<char>,
                              ProtoFieldValue>::const_iterator iter;
        for (iter = objVal->values().begin(); iter != objVal->values().end();
             iter++) {
            // Recursively create json elements from this object
            const char *fieldName = iter->first.c_str();
            const ProtoFieldValue *value = &iter->second;
            json_t *thisJson;

            status = convertProtoToJson(value, &thisJson);
            BailIfFailed(status);
            assert(thisJson != NULL);
            // This steals the reference to thisJson
            int ret = json_object_set_new(*jsonOut, fieldName, thisJson);
            if (ret != 0) {
                json_decref(thisJson);
                thisJson = NULL;
                status = StatusNoMem;
                goto CommonExit;
            }
            thisJson = NULL;
        }
        break;
    }
    case ProtoFieldValue::DATAVALUE_NOT_SET:
        *jsonOut = json_null();
        break;
    default:
        assert(false);
        return StatusFailed;
    }
    if (*jsonOut == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (*jsonOut) {
            json_decref(*jsonOut);
            *jsonOut = NULL;
        }
    }
    return status;
}

Status
JsonFormatOps::convertJsonToProto(json_t *jsonValue, ProtoFieldValue *protoOut)
{
    Status status = StatusOk;
    ProtoFieldValue_ObjectValue *objectVal = NULL;
    ProtoFieldValue_ArrayValue *arrayVal = NULL;
    google::protobuf::Arena *arena = protoOut->GetArena();

    try {
        switch (json_typeof(jsonValue)) {
        case JSON_STRING:
            protoOut->set_stringval(json_string_value(jsonValue));
            break;
        case JSON_INTEGER:
            protoOut->set_int64val(json_integer_value(jsonValue));
            break;
        case JSON_REAL:
            protoOut->set_float64val(json_real_value(jsonValue));
            break;
        case JSON_TRUE:
            // fallthrough
        case JSON_FALSE:
            protoOut->set_boolval(json_is_true(jsonValue));
            break;
        case JSON_NULL:
            // Leave this as DATAVALUE_NOT_SET
            break;
        case JSON_OBJECT: {
            const char *fName;
            json_t *fValue;
            objectVal = google::protobuf::Arena::CreateMessage<
                ProtoFieldValue_ObjectValue>(arena);
            BailIfNull(objectVal);
            json_object_foreach (jsonValue, fName, fValue) {
                ProtoFieldValue *pField =
                    google::protobuf::Arena::CreateMessage<ProtoFieldValue>(
                        arena);
                BailIfNull(pField);
                status = convertJsonToProto(fValue, pField);
                BailIfFailed(status);

                (*objectVal->mutable_values())[fName] = *pField;
            }
            protoOut->set_allocated_objectvalue(objectVal);
            objectVal = NULL;
            break;
        }
        case JSON_ARRAY: {
            arrayVal = google::protobuf::Arena::CreateMessage<
                ProtoFieldValue_ArrayValue>(arena);
            BailIfNull(arrayVal);
            for (int64_t ii = 0; ii < (int64_t) json_array_size(jsonValue);
                 ii++) {
                ProtoFieldValue *arrElm = arrayVal->add_elements();
                json_t *arrVal = json_array_get(jsonValue, ii);
                BailIfNull(arrVal);
                status = convertJsonToProto(arrVal, arrElm);
                BailIfFailed(status);
            }
            protoOut->set_allocated_arrayvalue(arrayVal);
            arrayVal = NULL;
            break;
        }
        default:
            status = StatusDfParseError;
            goto CommonExit;
            break;
        }
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (arena == NULL) {
        // When arena != NULL, this will be cleaned up at arena destruction time
        if (unlikely(objectVal)) {
            delete objectVal;
            objectVal = NULL;
        }
        if (unlikely(arrayVal)) {
            delete arrayVal;
            arrayVal = NULL;
        }
    }
    return status;
}
