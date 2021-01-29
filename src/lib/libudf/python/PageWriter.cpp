// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "PageWriter.h"
#include "sys/XLog.h"
#include "UdfPyXcalar.h"

static const char *moduleName = "PageWriter";

// PageWriter::Record

Status
PageWriter::Record::init(PageWriter *pageWriter)
{
    Status status;
    assert(scratchPadScalars_ == NULL);
    writer_ = pageWriter;

    int numFields = writer_->numFieldsRequired_;
    int32_t bitmapSize = RemoteBitmap<uint8_t>::sizeInWords(numFields);
    bitmapBytes_ = new (std::nothrow) uint8_t[bitmapSize];
    BailIfNullMsg(bitmapBytes_,
                  StatusNoMem,
                  moduleName,
                  "Failed to allocate bitmapBytes_: %s",
                  strGetFromStatus(status));

    bitmap_ = new (std::nothrow) RemoteBitmap<uint8_t>(bitmapBytes_, numFields);
    BailIfNullMsg(bitmap_,
                  StatusNoMem,
                  moduleName,
                  "Failed to allocate bitmap_: %s",
                  strGetFromStatus(status));
    bitmap_->clearAll();

    scratchPadScalars_ = new (std::nothrow) Scalar *[numFields];
    BailIfNullMsg(scratchPadScalars_,
                  StatusNoMem,
                  moduleName,
                  "Insufficient memory to allocate scratchPadScalars "
                  "(numFields: %u): %s",
                  numFields,
                  strGetFromStatus(status));

    for (int ii = 0; ii < numFields; ii++) {
        scratchPadScalars_[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        if (scratchPadScalars_[ii] == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to allocate scalar at index %u, numFields %u: %s",
                    ii,
                    numFields,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        numScalarsAlloced_++;
    }

CommonExit:
    if (!status.ok()) {
        destroy();
    }
    return status;
}

// XXX TODO
// convert from pyObject instead from TypedDataValue to Scalar
Status
PageWriter::Record::setFieldByIndex(int fieldIndex,
                                    const TypedDataValue *fieldValType)
{
    assert(fieldIndex >= 0 && fieldIndex < writer_->numFieldsRequired_);
    Status status;
    DataValueReader dValReader;
    int currFieldIndex = fieldIndex;
    auto *foi = &writer_->fieldsOfInterest_[currFieldIndex];
    auto *scalar = scratchPadScalars_[currFieldIndex];

    dValReader.setFromTypedDataValue(fieldValType->value_, fieldValType->type_);

    // this handles object and array cases
    auto *subFieldIdxes = &foi->subFieldIdxs;
    int ii = 0;
    bool isComplexType = foi->accessor.nameDepth > 1;

    while (true) {
        DataValueReader dValReaderTmp;
        if (isComplexType) {
            status =
                dValReader.accessNestedField(&foi->accessor, &dValReaderTmp);
        } else {
            dValReaderTmp = dValReader;
            status = StatusOk;
        }
        if (status.ok()) {
            status = populateScalar(scalar, &dValReaderTmp);
            if (status.ok()) {
                bitmap_->set(currFieldIndex);
            } else if (status != StatusDfFieldNoExist) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to populate scalar for "
                        "field name %s and index %i: %s",
                        foi->accessor.names[0].value.field,
                        currFieldIndex,
                        strGetFromStatus(status));
                return status;
            }
        } else if (status != StatusDfFieldNoExist) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to access nested field for "
                    "field name %s and index %i: %s",
                    foi->accessor.names[0].value.field,
                    currFieldIndex,
                    strGetFromStatus(status));
            return status;
        }
        status = StatusOk;
        totalFieldSizeUsed_ += scalar->fieldUsedSize;
        if (ii < subFieldIdxes->bufSize) {
            currFieldIndex = subFieldIdxes->buf[ii++];
            foi = &writer_->fieldsOfInterest_[currFieldIndex];
            scalar = scratchPadScalars_[currFieldIndex];
        } else {
            break;
        }
    }

    return status;
}

Status
PageWriter::Record::populateScalar(Scalar *scalar,
                                   const DataValueReader *dValue)
{
    Status status;
    DfFieldType fieldType = DfUnknown;

    status = dValue->getAsFieldValueInArray(scalar->fieldAllocedSize,
                                            &scalar->fieldVals,
                                            0,
                                            &fieldType);
    if (unlikely(status != StatusOk)) {
        return status;
    }
    assert(fieldType != DfUnknown);

    scalar->fieldNumValues = 1;
    scalar->fieldType = fieldType;
    scalar->updateUsedSize();

    assert(isValidDfFieldType(scalar->fieldType));
    return status;
}

Scalar *
PageWriter::Record::getFieldByIndex(int fieldIndex)
{
    assert(fieldIndex >= 0 && fieldIndex < writer_->numFieldsRequired_);
    if (bitmap_->test(fieldIndex)) {
        return scratchPadScalars_[fieldIndex];
    } else {
        return NULL;
    }
}

PageWriter::Record::~Record()
{
    destroy();
}

void
PageWriter::Record::clearAll()
{
    assert(bitmap_);
    bitmap_->clearAll();
    totalFieldSizeUsed_ = 0;
}

void
PageWriter::Record::destroy()
{
    delete[] bitmapBytes_;
    bitmapBytes_ = NULL;
    delete bitmap_;
    bitmap_ = NULL;

    for (int64_t ii = 0; ii < numScalarsAlloced_; ii++) {
        assert(scratchPadScalars_[ii] != NULL);
        Scalar::freeScalar(scratchPadScalars_[ii]);
        scratchPadScalars_[ii] = NULL;
    }
    delete[] scratchPadScalars_;
    scratchPadScalars_ = NULL;
    numScalarsAlloced_ = 0;
}

int32_t
PageWriter::Record::recordSize() const
{
    return totalFieldSizeUsed_;
}

// PageWriter

Status
PageWriter::init(PyObject *xceDataset, ParseOptimizerArgs &optimizerArgs)
{
    Status status;
    XcalarEval *eval = XcalarEval::get();

    optimizerArgs_ = &optimizerArgs;
    if (optimizerArgs.numFieldsRequired == 0) {
        numFieldsRequired_ = 0;
        isFixedSchema_ = false;
        return StatusOk;
    }
    fieldsOfInterest_ =
        new (std::nothrow) FieldOfInterest[optimizerArgs.numFieldsRequired];
    BailIfNullMsg(fieldsOfInterest_,
                  StatusNoMem,
                  moduleName,
                  "Failed to allocate fieldsOfInterest_: %s",
                  strGetFromStatus(status));

    isFixedSchema_ = true;
    numFieldsRequired_ = optimizerArgs.numFieldsRequired;
    tupleMeta_.setNumFields(numFieldsRequired_);
    tupleMeta_.setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

    for (int ii = 0; ii < numFieldsRequired_; ii++) {
        auto *foi = &fieldsOfInterest_[ii];

        Status status = foi->init(optimizerArgs.fieldNames[ii],
                                  optimizerArgs.valueTypes[ii]);
        if (!status.ok()) {
            goto CommonExit;
        }
        tupleMeta_.setFieldType(optimizerArgs.valueTypes[ii], ii);

        status = fieldsOfInterestHT_.insert(foi);
        if (status == StatusExist) {
            auto *foiWithSameParent =
                fieldsOfInterestHT_.find(foi->getFieldName());
            status = foiWithSameParent->subFieldIdxs.growBuffer(1);
            BailIfFailed(status);
            auto *subFields = &foiWithSameParent->subFieldIdxs;
            subFields->buf[subFields->bufSize++] = ii;
            continue;
        } else if (status != StatusOk) {
            goto CommonExit;
        }
        foi->idx = ii;

        xSyslog(moduleName,
                XlogDebug,
                "FOI name %s and type %s.",
                foi->getFieldName(),
                strGetFromDfFieldType(optimizerArgs.valueTypes[ii]));
    }

    if (optimizerArgs_->evalString[0] != '\0') {
        status = eval->generateClass1Ast(optimizerArgs_->evalString, &ast_);
        BailIfFailed(status);
        astInited_ = true;

        evalArgIndices_ =
            new (std::nothrow) int[ast_.astCommon.numScalarVariables];
        BailIfNull(evalArgIndices_);

        int jj = 0;
        for (unsigned ii = 0; ii < ast_.astCommon.numScalarVariables; ii++) {
            char *varName = ast_.astCommon.scalarVariables[ii].variableName;

            for (jj = 0; jj < numFieldsRequired_; jj++) {
                if (strcmp(optimizerArgs_->fieldNamesPrefixed[jj], varName) ==
                    0) {
                    break;
                }
            }
            evalArgIndices_[ii] = jj;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "Load with fixedSchema=%s, Total fieldsOfInterestCount=%d, "
            "Unique fieldsOfInterestCount=%d, "
            "eval string: %s",
            isFixedSchema_ ? "true" : "false",
            numFieldsRequired_,
            fieldsOfInterestHT_.getSize(),
            optimizerArgs_->evalString[0] != '\0' ? optimizerArgs_->evalString
                                                  : NULL);

    xceDataset_ = xceDataset;

    tupleValues_ = new (std::nothrow) NewTupleValues();
    BailIfNull(tupleValues_);

    // init the record
    status = record_.init(this);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to initialize record: %s",
                    strGetFromStatus(status));

CommonExit:
    if (!status.ok()) {
        destroy();
    }
    return status;
}

PageWriter::Record *
PageWriter::newRecord()
{
    record_.clearAll();
    return &record_;
}

PageWriter::FieldOfInterest *
PageWriter::getFieldOfInterest(const char *fieldName)
{
    return fieldsOfInterestHT_.find(fieldName);
}

Status
PageWriter::writeRecord(Record *rec)
{
    Status status;
    bool retryOnce = true;
    XcalarEval *eval = XcalarEval::get();
    DfFieldValue fieldVal = DfFieldValueNull;

    tupleValues_->setInvalid(0, numFieldsRequired_);

    for (int ii = 0; ii < numFieldsRequired_; ii++) {
        auto *scalar = rec->getFieldByIndex(ii);
        auto expectedType = optimizerArgs_->valueTypes[ii];
        if (scalar == NULL || scalar->fieldType == DfNull) {
            // field not present
            // XXX also treating null as FnF
            continue;
        } else if (expectedType == DfScalarObj) {
            fieldVal.scalarVal = scalar;
        } else {
            Status status2;
            if (expectedType != scalar->fieldType) {
                status2 = scalar->convertType(expectedType);
                if (!status2.ok()) {
                    continue;
                }
            }
            status2 = scalar->getValue(&fieldVal);
            if (!status2.ok()) {
                continue;
            }
            assert(scalar->fieldType == expectedType);
        }
        tupleValues_->set(ii, fieldVal, expectedType);
    }
    assert(status.ok());

    // run eval to see if this record should be filtered out
    if (astInited_) {
        for (unsigned jj = 0; jj < ast_.astCommon.numScalarVariables; jj++) {
            int idx = evalArgIndices_[jj];
            auto *scalar = rec->getFieldByIndex(idx);

            if (scalar == NULL || scalar->fieldUsedSize == 0) {
                ast_.astCommon.scalarVariables[jj].content = NULL;
            } else {
                ast_.astCommon.scalarVariables[jj].content = scalar;
            }
        }

        DfFieldValue evalResult;
        DfFieldType resultType;
        status = eval->eval(&ast_, &evalResult, &resultType, false);
        if (eval->isFatalError(status)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to run evalString %s: %s",
                    optimizerArgs_->evalString,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (status != StatusOk ||
            !XcalarEval::isEvalResultTrue(evalResult, resultType)) {
            // filter evaluated to false, skip this row
            status = StatusOk;
            goto CommonExit;
        }
    }

    while (true) {
        if (status == StatusNoData || tupBuf_ == NULL) {
            status = getNewPage();
            BailIfFailed(status);
            assert(tupBuf_);
        }
        status = tupBuf_->append(&tupleMeta_, tupleValues_);
        if (status == StatusNoData) {
            if (tupBuf_->getNumTuples() == 0) {
                status = StatusMaxRowSizeExceeded;
                BailIfFailedMsg(moduleName,
                                status,
                                "record size is too large, cannot fit "
                                "xdb page, record at %lu : %s",
                                recordNum_,
                                strGetFromStatus(status));
            } else if (retryOnce) {
                retryOnce = false;
                continue;
            }
        }
        break;
    }
    recordNum_++;

CommonExit:
    if (!status.ok()) {
        xSyslog(moduleName,
                XlogErr,
                "Failed in writeRecord at record %lu: %s",
                recordNum_,
                strGetFromStatus(status));
    }
    return status;
}

Status
PageWriter::getNewPage()
{
    Status status;
    PyObject *ret = NULL;
    PyObject *pageObject = NULL;
    int64_t pageSize;

    if (tupBuf_ != NULL) {
        status = shipPage();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to ship page: %s",
                        strGetFromStatus(status));
    }
    assert(tupBuf_ == NULL);

    // Get a new shared memory page
    pageObject = PyObject_CallMethod(xceDataset_, "get_buffer", NULL);
    tupBuf_ =
        (NewTuplesBuffer *) PyRecordStreamer::getPageFromObject(pageObject);
    if (tupBuf_ == NULL) {
        // PyErr must have already been set
        status = StatusFailed;
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to get new page: %s",
                        strGetFromStatus(status));
    }
    Py_DECREF(pageObject);
    pageObject = NULL;

    {
        ret = PyObject_CallMethod(xceDataset_, "page_size", NULL);
        if (ret == NULL) {
            assert(PyErr_Occurred() && "callMethod sets the exception");
            goto CommonExit;
        }

        if (!PyLong_Check(ret)) {
            PyErr_Format(PyExc_ValueError, "page size is not an int");
            goto CommonExit;
        }

        PyErr_Clear();  // This must be set so we can detect errors in
                        // PyLong_As...
        pageSize = PyLong_AsLongLong(ret);
        if (PyErr_Occurred()) {
            goto CommonExit;
        }

        if (pageSize <= 0) {
            PyErr_Format(PyExc_ValueError,
                         "pageSize(%lu) must be greater than 0",
                         pageSize);
            goto CommonExit;
        }
    }

    new (tupBuf_) NewTuplesBuffer((void *) tupBuf_, pageSize);

CommonExit:
    if (ret != NULL) {
        Py_DECREF(ret);
        ret = NULL;
    }

    if (pageObject != NULL) {
        Py_DECREF(pageObject);
        pageObject = NULL;
    }

    return status;
}

Status
PageWriter::shipPage()
{
    Status status;
    PyObject *ret = NULL;

    if (tupBuf_ == NULL) {
        // nothing to dump
        return status;
    }

    tupBuf_->serialize();

    // Inform XCE that this page is ready to be processed
    ret = PyObject_CallMethod(xceDataset_,
                              "finish_buffer",
                              "lO",
                              (uint8_t *) tupBuf_,
                              Py_False);
    if (ret == NULL) {
        status = StatusFailed;
        goto CommonExit;
    }
    tupBuf_ = NULL;

CommonExit:
    if (ret != NULL) {
        Py_DECREF(ret);
        ret = NULL;
    }
    return status;
}

bool
PageWriter::isFixedSchema() const
{
    return isFixedSchema_;
}

int
PageWriter::getNumFields() const
{
    return fieldsOfInterestHT_.getSize();
}

void
PageWriter::destroy()
{
    XcalarEval *eval = XcalarEval::get();

    delete[] fieldsOfInterest_;
    fieldsOfInterest_ = NULL;
    delete tupleValues_;
    tupleValues_ = NULL;

    delete[] evalArgIndices_;
    evalArgIndices_ = NULL;
    if (astInited_) {
        // scratchPadScalars have already been freed
        eval->dropScalarVarRef(&ast_.astCommon);
        eval->destroyClass1Ast(&ast_);
        astInited_ = false;
    }
}

PageWriter::~PageWriter()
{
    destroy();
}

// PageWriter::FieldOfInterest

Status
PageWriter::FieldOfInterest::init(const char *fieldName,
                                  const DfFieldType fieldType)
{
    AccessorNameParser accessorParser;
    Status status = accessorParser.parseAccessor(fieldName, &this->accessor);
    if (!status.ok()) {
        return status;
    }
    if (fieldType == DfUnknown || fieldType == DfScalarObj) {
        // We don't know the exact type, let's infer from the data value later
        this->fieldType = ValueType::Invalid;
    } else if (accessor.nameDepth == 1) {
        this->fieldType = dfTypeToValueType(fieldType);
    } else {
        assert(accessor.nameDepth > 1);
        if (accessor.names[1].type == AccessorName::Subscript) {
            this->fieldType = ValueType::Array;
        } else {
            this->fieldType = ValueType::Object;
        }
    }
    return status;
}

const char *
PageWriter::FieldOfInterest::getFieldName() const
{
    assert(accessor.names != NULL);
    if (unlikely(this->accessor.names == NULL)) {
        return NULL;
    } else {
        return this->accessor.names[0].value.field;
    }
}
