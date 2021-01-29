// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SCALARS_H_
#define _SCALARS_H_

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "xdb/DataModelTypes.h"
#include "operators/XcalarEvalTypes.h"

// A Scalar object is like a container for a field extracted from a table.
// Since the field exists outside a table, it must be wrapped with the field
// type, since the table schema isn't available. This is especially true for
// intermediate nodes in ASTs (Abstract Syntax Tree) - the data values held
// in an intermediate node while executing the AST on a table must have
// type information describing the data which is hard to get from the table
// since the node doesn't directly relate to any field in the table. The
// Scalar object offers a convenient container holding the data plus type
// information for such cases. The rest of the fields in this object describe
// the memory layout in this object, to hold the field value(s). This object
// is widely used in XcalarEval and the operators code to hold values
// temporarily, in intermediate nodes of ASTs, extracted from tables for
// export to other modules, or used for ingest into a table, to create or
// update an existing field in the table.
// In this case, a Scalar is anything that isn't a Fat Pointer, this includes
// arrays, which is why numValues is a part of this struct.
//
// Typical use case can be concluded as:
//  1. For aggregate and synthesize (if the source of synthesize is an
//     aggregate value), a scalar member variable is in dstNode for
//     keeping result value.
//  2. In aggregate calculation, scalar objects are created as containers of
//     intermediate values. For example in min(col), scalar object min will be
//     created and updated for each tuple to hold the current min value, then
//     Scalar::getValue extracts the value and passes it to other functions
//     like tuple.set.
//  3. XcalarEval builds eval ASTs which use scalar objects to hold arguments
//     and result of each node (intermediate values). Especially for
//     intermediate values, metadata of the value, such as type is needed,
//     Hence the need of a Scalar object. Also temporary scratchPadScalars are
//     created for evals. The args and output passed into xdfs are all scalar
//     objects. See detailed comments about ASTs in XcalarEval.h
//  4. Scalars are used during load to create xcalarRecordNum and hold data
//     for resolving evalString and tuple.set later.
class Scalar
{
  public:
    static constexpr uint64_t DefaultFieldValsBufSize = 8;

    DfFieldType fieldType;      // the type of data in this object
    uint32_t fieldAllocedSize;  // bytes allocated for fieldVals buffer
    uint32_t fieldUsedSize;     // bytes used in fieldVals buffer
    // for array type this is the length, otherwise 1 or 0 (if it's null)
    uint16_t fieldNumValues;
    // Ptr to start of var length array of bytes, size of fieldVals field is 0
    DfFieldValueArray fieldVals;

    Scalar() = default;
    ~Scalar() = default;

    // allocate memory for a Scalar object with bytes for the fieldVals
    static Scalar *allocScalar(size_t fieldSize);
    static void freeScalar(Scalar *scalar);  // free the Scalar object

    MustCheck Status copyFrom(Scalar *srcScalar);  // copy from srcScalar
    void updateUsedSize();        // calculate and update fieldUsedSize
    size_t getTotalSize() const;  // return bytes allocated plus sizeof(Scalar)
    size_t getUsedSize() const;   // return field's usage plus sizeof(Scalar)
    // compare value of two Scalar objects and return -1, 0 or 1 as result
    // return -1 if this < val
    // return 0 if this == val
    // return 1 if this > val
    int compare(Scalar *val) const;
    Status getValue(DfFieldValue *fieldVal,
                    unsigned arrayIdx = 0) const;  // extract value at index
    Status setValue(DfFieldValue fieldVal,
                    DfFieldType fieldType);   // set the value of Scalar object
    Status convertType(DfFieldType outType);  // convert Scalar to target type
  private:
    Scalar(const Scalar &) = delete;
    Scalar &operator=(const Scalar &) = delete;
};

#endif  // _SCALARS_H_
