// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _NEW_KEY_VALUE_TYPES_H
#define _NEW_KEY_VALUE_TYPES_H

#include "util/System.h"
#include "operators/GenericTypes.h"
#include "primitives/Primitives.h"
#include "newtupbuf/NewTupleTypes.h"

//
// Avoid virtual methods here to save the vtable lookup cost.
// All the Impls are in the header file are inlined deliberately.
//

class NewKeyValueMeta
{
  public:
    const NewTupleMeta *tupMeta_;
    ssize_t keyIdx_;

    NewKeyValueMeta()
    {
        tupMeta_ = NULL;
        keyIdx_ = NewTupleMeta::DfInvalidIdx;
    }

    NewKeyValueMeta(const NewTupleMeta *tupMeta, ssize_t keyIdx)
    {
        tupMeta_ = tupMeta;
        keyIdx_ = keyIdx;
    }

    DfFieldType getKeyType() const
    {
        if (keyIdx_ == NewTupleMeta::DfInvalidIdx) {
            return DfUnknown;
        } else {
            return tupMeta_->getFieldType(keyIdx_);
        }
    }

    unsigned getNumFields() const { return tupMeta_->getNumFields(); }

    ~NewKeyValueMeta() = default;
};

class NewKeyValueNamedMeta
{
  public:
    NewKeyValueMeta kvMeta_;
    char valueNames_[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];

    NewKeyValueNamedMeta() { memZero(valueNames_, sizeof(valueNames_)); }

    NewKeyValueNamedMeta(const NewTupleMeta *tupMeta, size_t keyIdx)
        : kvMeta_(tupMeta, keyIdx)
    {
        memZero(valueNames_, sizeof(valueNames_));
    }

    unsigned getNumFields() const { return kvMeta_.getNumFields(); }

    ~NewKeyValueNamedMeta() = default;
};

class NewKeyValueEntry
{
  public:
    const NewKeyValueMeta *kvMeta_;
    NewTupleValues tuple_;
    NewKeyValueEntry() {}

    NewKeyValueEntry(const NewKeyValueMeta *kvMeta)
    {
        kvMeta_ = kvMeta;
        init();
    }

    void init() { tuple_.setInvalid(0, kvMeta_->tupMeta_->getNumFields()); }

    ~NewKeyValueEntry() = default;

    MustInline MustCheck DfFieldValue getKey(bool *retKeyValid)
    {
        ssize_t fieldsIdx = kvMeta_->keyIdx_;
        const NewTupleMeta *tupMeta = kvMeta_->tupMeta_;
        size_t numFields = tupMeta->getNumFields();
        DfFieldValue key = NewTupleMeta::InvalidKey;

        if (fieldsIdx == NewTupleMeta::DfInvalidIdx) {
            *retKeyValid = false;
            return key;
        }
        DfFieldType type = tupMeta->getFieldType(fieldsIdx);
        key = tuple_.get(fieldsIdx, numFields, type, retKeyValid);
        if (*retKeyValid) {
            return key;
        }
        return NewTupleMeta::InvalidKey;
    }
};

#endif  // _NEW_KEY_VALUE_TYPES_H
