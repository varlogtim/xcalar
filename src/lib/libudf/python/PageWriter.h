// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef PAGEWRITER_H
#define PAGEWRITER_H

#include <Python.h>

#include "primitives/Primitives.h"
#include "util/StringHashTable.h"
#include "util/ElemBuf.h"
#include "datapage/DataValue.h"
#include "util/RemoteBitmap.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "operators/XcalarEval.h"

class PageWriter final
{
  public:
    struct FieldOfInterest {
        StringHashTableHook hook;
        Accessor accessor;
        ValueType fieldType;

        int idx = -1;
        // sub field indexes having same first level key
        ElemBuf<int> subFieldIdxs;

        Status init(const char *fieldName, const DfFieldType fieldType);
        const char *getFieldName() const;
    };

    class Record final
    {
        friend class PageWriter;

      public:
        Record() = default;
        ~Record();

        MustCheck Status init(PageWriter *pageWriter);
        MustCheck Status setFieldByIndex(int fieldIndex,
                                         const TypedDataValue *fieldValType);
        MustCheck Scalar *getFieldByIndex(int fieldIndex);
        MustCheck int32_t recordSize() const;
        void clearAll();
        void destroy();

      private:
        // this is total record size updated once we write to xdb page
        int32_t totalFieldSizeUsed_ = 0;
        Scalar **scratchPadScalars_ = NULL;
        int numScalarsAlloced_ = 0;
        uint8_t *bitmapBytes_ = NULL;
        RemoteBitmap<uint8_t> *bitmap_ = NULL;

        PageWriter *writer_ = NULL;

        MustCheck Status populateScalar(Scalar *scalar,
                                        const DataValueReader *dValue);
    };

    PageWriter() = default;
    ~PageWriter();

    MustCheck Status init(PyObject *xceDataset,
                          ParseOptimizerArgs &optimizerArgs);
    MustCheck PageWriter::Record *newRecord();
    MustCheck Status writeRecord(Record *rec);
    MustCheck bool isFixedSchema() const;
    MustCheck FieldOfInterest *getFieldOfInterest(const char *fieldName);
    MustCheck int getNumFields() const;
    MustCheck Status shipPage();
    void destroy();

  private:
    static constexpr unsigned FieldOfInterestHashTableSlot = 499;
    typedef StringHashTable<FieldOfInterest,
                            &FieldOfInterest::hook,
                            &FieldOfInterest::getFieldName,
                            FieldOfInterestHashTableSlot,
                            hashStringFast>
        FieldOfInterestHashTable;

    // Optimizer args
    ParseOptimizerArgs *optimizerArgs_ = NULL;
    int numFieldsRequired_ = 0;
    FieldOfInterest *fieldsOfInterest_ = NULL;
    // this just holds the references of the above elements.
    // Key of this HT is unflattended field name.
    FieldOfInterestHashTable fieldsOfInterestHT_;
    int *evalArgIndices_ = NULL;
    XcalarEvalClass1Ast ast_;
    bool astInited_ = false;
    bool isFixedSchema_ = false;

    // data values holder
    Record record_;
    uint64_t recordNum_ = -1;
    // Source for the input stream of empty pages
    PyObject *xceDataset_ = NULL;

    NewTupleValues *tupleValues_ = NULL;
    NewTupleMeta tupleMeta_;
    NewTuplesBuffer *tupBuf_ = NULL;  // xdb page

    // This dumps out any page if exists and loads new one.
    MustCheck Status getNewPage();
};

#endif  // PAGEWRITER_H
