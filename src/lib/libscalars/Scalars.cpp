// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <cmath>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "bc/BufferCache.h"
#include "scalars/Scalars.h"
#include "operators/XcalarEvalTypes.h"
#include "util/MemTrack.h"
#include "df/DataFormat.h"
#include "util/DFPUtils.h"

Scalar *
Scalar::allocScalar(size_t fieldSize)
{
    size_t totalScalarSize;
    Scalar *scalar;

    totalScalarSize = sizeof(*scalar) + fieldSize;
    scalar = (Scalar *) memAllocExt(totalScalarSize, moduleName);
    if (scalar == NULL) {
        return NULL;
    }

    // Silence many valgrind conditional jump errors
    memZero(scalar, totalScalarSize);

    scalar->fieldUsedSize = 0;
    scalar->fieldAllocedSize = fieldSize;
    return scalar;
}

void
Scalar::freeScalar(Scalar *scalar)
{
    memFree(scalar);
}

Status
Scalar::copyFrom(Scalar *srcScalar)
{
    assert(srcScalar != NULL);

    if (fieldAllocedSize < srcScalar->fieldUsedSize) {
        return StatusOverflow;
    }

    fieldUsedSize = srcScalar->fieldUsedSize;
    fieldType = srcScalar->fieldType;
    fieldNumValues = srcScalar->fieldNumValues;

    memcpy(&fieldVals, &srcScalar->fieldVals, srcScalar->fieldUsedSize);

    return StatusOk;
}

void
Scalar::updateUsedSize()
{
    Status status;
    if (DataFormat::fieldGetSize(fieldType) == DfVariableFieldSize) {
        if (fieldType == DfString) {
            unsigned jj;
            fieldUsedSize = 0;
            DfFieldValue fieldVal;
            for (jj = 0; jj < fieldNumValues; jj++) {
                status = getValue(&fieldVal, jj);
                assert(status == StatusOk);
                fieldUsedSize +=
                    DataFormat::fieldGetStringFieldSize2(fieldVal.stringVal);
            }
        } else {
            switch (fieldType) {
            case DfTimespec:
                fieldUsedSize = fieldNumValues * sizeof(DfTimeval);
                break;
            case DfMoney:
                fieldUsedSize = fieldNumValues * sizeof(XlrDfp);
                break;
            default:
                assert(0);
            }
        }
    } else {
        // This means we are a fixed size scalar
        fieldUsedSize = fieldNumValues * DataFormat::fieldGetSize(fieldType);
    }
}

size_t
Scalar::getTotalSize() const
{
    return sizeof(*this) + fieldAllocedSize;
}

size_t
Scalar::getUsedSize() const
{
    return sizeof(*this) + fieldUsedSize;
}

int
Scalar::compare(Scalar *scalar) const
{
    DfFieldValue val1, val2;
    Status status;
    assert(fieldType == scalar->fieldType);

    // XXX: Don't support arrays
    assert(fieldNumValues == 1);
    assert(scalar->fieldNumValues == 1);

    status = getValue(&val1);
    assert(status == StatusOk);

    status = scalar->getValue(&val2);
    assert(status == StatusOk);

    return DataFormat::fieldCompare(fieldType, val1, val2);
}

Status
Scalar::getValue(DfFieldValue *fieldVal, unsigned arrayIdx) const
{
    return DataFormat::getFieldValueFromArray(&fieldVals,
                                              fieldAllocedSize,
                                              fieldType,
                                              arrayIdx,
                                              fieldVal);
}

Status
Scalar::setValue(DfFieldValue fieldVal, DfFieldType fieldTypeIn)
{
    Status status;

    switch (fieldTypeIn) {
    case DfScalarObj:
        status = copyFrom(fieldVal.scalarVal);
        break;
    case DfNull:
        fieldType = fieldTypeIn;
        fieldNumValues = 0;
        fieldUsedSize = 0;
        break;
    default:
        status = DataFormat::setFieldValueInArray(&fieldVals,
                                                  fieldAllocedSize,
                                                  fieldTypeIn,
                                                  0,
                                                  fieldVal);
        if (!status.ok()) {
            return status;
        }
        fieldType = fieldTypeIn;
        fieldNumValues = 1;
        // TODO change the method  DataFormat::setFieldValueInArray to return
        // fieldSize instead of calculating here again
        fieldUsedSize = DataFormat::fieldGetSize(fieldTypeIn, fieldVal);
    }

    return status;
}

Status
Scalar::convertType(DfFieldType outType)
{
    Status status = StatusOk;
    size_t bufSize = fieldAllocedSize;
    if (fieldType == outType) {
        goto CommonExit;
    }

    DfFieldValue fieldVal;

    status = getValue(&fieldVal);
    BailIfFailed(status);

    DfFieldValue fieldValTmp;
    fieldValTmp.stringValTmp = fieldVals.strVals->strActual;

    if (outType == DfString) {
        // take away bytes for the strSize;
        if (bufSize < sizeof(fieldValTmp.stringVal.strSize)) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        bufSize -= sizeof(fieldValTmp.stringVal.strSize);
    }

    status = DataFormat::convertValueType(outType,
                                          fieldType,
                                          &fieldVal,
                                          &fieldValTmp,
                                          bufSize,
                                          BaseCanonicalForm);
    BailIfFailed(status);

    status = setValue(fieldValTmp, outType);
    BailIfFailed(status);

CommonExit:
    return status;
}
