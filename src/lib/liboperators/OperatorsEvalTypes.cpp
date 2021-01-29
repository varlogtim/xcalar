// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "operators/OperatorsEvalTypes.h"
#include "operators/OperatorsHash.h"

DfFieldType
GroupEvalContext::getOutputType(AccumulatorType accType, DfFieldType argType)
{
    switch (accType) {
    case AccumulatorCount:
    case AccumulatorSumInteger:
        return DfInt64;
    case AccumulatorSumFloat:
    case AccumulatorAvg:
        return DfFloat64;
    case AccumulatorAvgNumeric:
    case AccumulatorSumNumeric:
        return DfMoney;
    case AccumulatorMin:
    case AccumulatorMax:
        return argType;
    }
}

bool
GroupEvalContext::isValidInputType(AccumulatorType accType, DfFieldType argType)
{
    switch (accType) {
    case AccumulatorSumInteger:
        return argType == DfInt64 || argType == DfBoolean;
    case AccumulatorSumFloat:
        return argType == DfInt64 || argType == DfBoolean ||
               argType == DfFloat64;
    case AccumulatorAvg:
        return argType == DfInt64 || argType == DfBoolean ||
               argType == DfFloat64 || argType == DfMoney;
    case AccumulatorAvgNumeric:
    case AccumulatorSumNumeric:
        return argType == DfMoney;
    case AccumulatorCount:
    case AccumulatorMin:
    case AccumulatorMax:
        return true;
    }
}

ValAccumulator *
GroupEvalContext::allocAccumulator(AccumulatorType accType)
{
    ValAccumulator *acc = NULL;

    switch (accType) {
    case AccumulatorMax:
        acc = new (std::nothrow) MaxAccumulator();
        break;
    case AccumulatorMin:
        acc = new (std::nothrow) MinAccumulator();
        break;
    case AccumulatorSumFloat:
        acc = new (std::nothrow) SumFloatAccumulator();
        break;
    case AccumulatorSumInteger:
        acc = new (std::nothrow) SumIntegerAccumulator();
        break;
    case AccumulatorSumNumeric:
        acc = new (std::nothrow) SumNumericAccumulator();
        break;
    case AccumulatorCount:
        acc = new (std::nothrow) CountAccumulator();
        break;
    case AccumulatorAvg:
        acc = new (std::nothrow) AvgAccumulator();
        break;
    case AccumulatorAvgNumeric:
        acc = new (std::nothrow) AvgNumericAccumulator();
        break;
    }

    return acc;
}

ValAccumulator *
GroupEvalContext::allocAccumulator(AccumulatorType accType, MemoryPool *memPool)
{
    Status status;
    ValAccumulator *acc = NULL;

    switch (accType) {
    case AccumulatorMax:
        acc = (ValAccumulator *) memPool->getElem(sizeof(MaxAccumulator));
        BailIfNull(acc);

        new (acc) MaxAccumulator();
        break;
    case AccumulatorMin:
        acc = (ValAccumulator *) memPool->getElem(sizeof(MinAccumulator));
        BailIfNull(acc);

        new (acc) MinAccumulator();
        break;
    case AccumulatorSumFloat:
        acc = (ValAccumulator *) memPool->getElem(sizeof(SumFloatAccumulator));
        BailIfNull(acc);

        new (acc) SumFloatAccumulator();
        break;
    case AccumulatorSumInteger:
        acc =
            (ValAccumulator *) memPool->getElem(sizeof(SumIntegerAccumulator));
        BailIfNull(acc);

        new (acc) SumIntegerAccumulator();
        break;

    case AccumulatorSumNumeric:
        acc =
            (ValAccumulator *) memPool->getElem(sizeof(SumNumericAccumulator));
        BailIfNull(acc);

        new (acc) SumNumericAccumulator();
        break;
    case AccumulatorCount:
        acc = (ValAccumulator *) memPool->getElem(sizeof(CountAccumulator));
        BailIfNull(acc);

        new (acc) CountAccumulator();
        break;
    case AccumulatorAvg:
        acc = (ValAccumulator *) memPool->getElem(sizeof(AvgAccumulator));
        BailIfNull(acc);

        new (acc) AvgAccumulator();
        break;
    case AccumulatorAvgNumeric:
        acc =
            (ValAccumulator *) memPool->getElem(sizeof(AvgNumericAccumulator));
        BailIfNull(acc);

        new (acc) AvgNumericAccumulator();
        break;
    }

CommonExit:
    return acc;
}

AccumulatorType
GroupEvalContext::fnNameToAccumulatorType(const char *fnName)
{
    if (strncmp(fnName, "max", 3) == 0) {
        return AccumulatorMax;
    } else if (strncmp(fnName, "min", 3) == 0) {
        return AccumulatorMin;
    } else {
        return strToAccumulatorType(fnName);
    }
}

bool
GroupEvalContext::isValidAst(XcalarEvalAstCommon *ast)
{
    AccumulatorType accType =
        fnNameToAccumulatorType(ast->rootNode->registeredFn->getFnName());

    if (!XcalarEval::get()->isComplex(ast->rootNode) &&
        isValidAccumulatorType(accType) && ast->rootNode->numArgs == 1) {
        return true;
    } else {
        return false;
    }
}
bool
GroupEvalContext::initFromAst(XcalarEvalClass2Ast *ast,
                              XdbMeta *srcMeta,
                              int resultIdxIn)
{
    return initFromAst(&ast->astCommon, srcMeta, resultIdxIn);
}

bool
GroupEvalContext::initFromAst(XcalarEvalAstCommon *ast,
                              XdbMeta *srcMeta,
                              int resultIdxIn)
{
    if (!isValidAst(ast)) {
        return false;
    }

    resultIdx = resultIdxIn;
    accType = fnNameToAccumulatorType(ast->rootNode->registeredFn->getFnName());

    if (XcalarEval::isConstant(ast->rootNode->arguments[0]->nodeType)) {
        Scalar *constant = NULL;
        if (ast->rootNode->arguments[0]->varContent == NULL) {
            // constant has not been set up yet
            unsigned idx = 0;
            assert(ast->numConstants == 1);
            assert(ast->constants);
            memZero(ast->constants, sizeof(*ast->constants) * 1);

            Status status =
                XcalarEval::get()
                    ->fixupConstant(ast->rootNode->arguments[0]->nodeType,
                                    ast->rootNode->arguments[0],
                                    ast->constants,
                                    &idx);
            if (status != StatusOk) {
                return false;
            }
        }

        constant = *ast->rootNode->arguments[0]->varContent;
        argType = constant->fieldType;

        if (argType != DfNull) {
            constant->getValue(&constantVal);
            constantValid = true;
        }

        return true;
    }

    const char *varName = ast->rootNode->arguments[0]->token;

    for (unsigned ii = 0;
         ii < srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
         ii++) {
        DfFieldType type =
            srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii);

        if (strcmp(varName, srcMeta->kvNamedMeta.valueNames_[ii]) == 0 &&
            type != DfFatptr) {
            argType = type;
            argIdx = ii;

            return isValidInputType(accType, argType);
        }
    }

    // arg was not found
    return false;
}

EvalContext::~EvalContext()
{
    if (argIndices_) {
        memFree(argIndices_);
        argIndices_ = NULL;
    }

    if (scalars_) {
        for (unsigned ii = 0; ii < ast_.astCommon.numScalarVariables; ii++) {
            if (scalars_[ii] != NULL) {
                Scalar::freeScalar(scalars_[ii]);
                scalars_[ii] = NULL;
            }
        }

        memFree(scalars_);
        scalars_ = NULL;
    }

    if (astInit_) {
        // scratchPadScalars have already been freed
        XcalarEval::dropScalarVarRef(&ast_.astCommon);
        XcalarEval::destroyClass1Ast(&ast_);
        astInit_ = false;
    }
}

void
EvalContext::setupResultIdx(const char *newFieldName,
                            unsigned numFields,
                            const char *fieldNames[])
{
    // find our result entry in dstMeta
    unsigned ii;
    for (ii = 0; ii < numFields; ii++) {
        if (strcmp(fieldNames[ii], newFieldName) == 0) {
            resultIdx_ = ii;
            return;
        }
    }

    resultIdx_ = InvalidIdx;
}

Status
EvalContext::setupAst(const char *evalString,
                      unsigned numFields,
                      const char **fieldNames,
                      DagTypes::DagId dagId)
{
    unsigned numVariables;
    Status status = XcalarEval::get()->setupEvalAst(evalString,
                                                    numFields,
                                                    fieldNames,
                                                    ast_,
                                                    numVariables,
                                                    argIndices_,
                                                    scalars_,
                                                    dagId);
    if (status == StatusOk) {
        astInit_ = true;
        resultType_ = XcalarEval::getOutputType(&ast_.astCommon);
    }

    return status;
}

Status
EvalContext::setupAst(const char *evalString,
                      XdbMeta *xdbMeta,
                      DagTypes::DagId dagId)
{
    unsigned numVariables;
    Status status = XcalarEval::get()->setupEvalAst(evalString,
                                                    xdbMeta,
                                                    ast_,
                                                    numVariables,
                                                    argIndices_,
                                                    scalars_,
                                                    dagId);
    if (status == StatusOk) {
        astInit_ = true;
        resultType_ = XcalarEval::getOutputType(&ast_.astCommon);
    }

    return status;
}

Status
EvalContext::setupMultiAst(const char *evalString,
                           XdbMeta *leftMeta,
                           XdbMeta *rightMeta,
                           XcalarApiRenameMap *renameMap,
                           unsigned numRenameEntriesLeft,
                           unsigned numRenameEntriesRight,
                           DagTypes::DagId dagId)
{
    unsigned numVariables;
    Status status = XcalarEval::get()->setupEvalMultiAst(evalString,
                                                         leftMeta,
                                                         rightMeta,
                                                         ast_,
                                                         numVariables,
                                                         argIndices_,
                                                         scalars_,
                                                         renameMap,
                                                         numRenameEntriesLeft,
                                                         numRenameEntriesRight,
                                                         dagId);
    if (status == StatusOk) {
        astInit_ = true;
        resultType_ = XcalarEval::getOutputType(&ast_.astCommon);
    }

    return status;
}

Status
EvalContext::filterAst(NewKeyValueEntry *dstKvEntry,
                       unsigned dstNumFields,
                       bool *result)
{
    return XcalarEval::get()->executeFilterAst(dstKvEntry,
                                               dstNumFields,
                                               &ast_,
                                               argIndices_,
                                               scalars_,
                                               result);
}

Status
EvalContext::filterMultiAst(NewKeyValueEntry *leftKvEntry,
                            unsigned leftNumFields,
                            NewKeyValueEntry *rightKvEntry,
                            bool *result)
{
    return XcalarEval::get()->executeFilterMultiAst(leftKvEntry,
                                                    leftNumFields,
                                                    rightKvEntry,
                                                    &ast_,
                                                    argIndices_,
                                                    scalars_,
                                                    result);
}

bool
FilterRange::init(EvalContext *filterCtxIn, XdbMeta *xdbMeta)
{
    filterCtx_ = filterCtxIn;

    XcalarEvalAstOperatorNode *rootNode = filterCtx_->ast_.astCommon.rootNode;

    const char *fnName = rootNode->registeredFn->getFnName();
    const char *varName = rootNode->arguments[0]->token;

    if (filterCtx_->ast_.astCommon.numScalarVariables == 1 &&
        !XcalarEval::get()->isComplex(rootNode) &&
        (strcmp(fnName, "eq") == 0 || strcmp(fnName, "lt") == 0 ||
         strcmp(fnName, "le") == 0 || strcmp(fnName, "gt") == 0 ||
         strcmp(fnName, "ge") == 0 || strcmp(fnName, "between") == 0)) {
        keyName_ = varName;
        valType_ = xdbMeta->getFieldType(varName);
        fnName_ =
            filterCtx_->ast_.astCommon.rootNode->registeredFn->getFnName();

        if (setupRange() == StatusOk) {
            return true;
        }
    }

    return false;
}

Status
FilterRange::setupRange()
{
    Status status;
    XcalarEvalAstOperatorNode *rootNode = filterCtx_->ast_.astCommon.rootNode;

    if (strcmp(fnName_, "eq") == 0) {
        Scalar *s = *rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != valType_) {
            // mismatched types
            return StatusInval;
        }

        status = s->getValue(&minVal_);
        assert(status == StatusOk);

        minValSet_ = true;

        status = s->getValue(&maxVal_);
        assert(status == StatusOk);

        maxValSet_ = true;
    } else if (strcmp(fnName_, "lt") == 0 || strcmp(fnName_, "le") == 0) {
        Scalar *s = *rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != valType_) {
            // mismatched types
            return StatusInval;
        }

        status = s->getValue(&maxVal_);
        assert(status == StatusOk);

        maxValSet_ = true;
    } else if (strcmp(fnName_, "gt") == 0 || strcmp(fnName_, "ge") == 0) {
        Scalar *s = *rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != valType_) {
            // mismatched types
            return StatusInval;
        }

        status = s->getValue(&minVal_);
        assert(status == StatusOk);

        minValSet_ = true;
    } else if (strcmp(fnName_, "between") == 0) {
        Scalar *s = *rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != valType_) {
            // mismatched types
            return StatusInval;
        }

        status = s->getValue(&minVal_);
        assert(status == StatusOk);

        minValSet_ = true;

        s = *rootNode->arguments[2]->varContent;
        assert(s != NULL);
        if (s->fieldType != valType_) {
            // mismatched types
            return StatusInval;
        }

        status = s->getValue(&maxVal_);
        assert(status == StatusOk);

        maxValSet_ = true;
    }

    if (valType_ == DfString) {
        // need to hash the value to compare it
        if (minValSet_) {
            minVal_.uint64Val =
                operatorsHashByString(minVal_.stringVal.strActual);
        }

        if (maxValSet_) {
            maxVal_.uint64Val =
                operatorsHashByString(maxVal_.stringVal.strActual);
        }

        valType_ = DfUInt64;
    }

    return StatusOk;
}

bool
FilterRange::inRange(DfFieldValue minIn, DfFieldValue maxIn)
{
    // if this max is below minVal, skip it
    if (minValSet_ && DataFormat::fieldCompare(valType_, maxIn, minVal_) < 0) {
        return false;
    }

    // if this min is above maxVal, skip it
    if (maxValSet_ && DataFormat::fieldCompare(valType_, minIn, maxVal_) > 0) {
        return false;
    }

    return true;
}

Status
GroupEvalContext::getArgType(const char *fieldName,
                             const char **srcFields,
                             NewTupleMeta *tupMeta,
                             DfFieldType &argType)
{
    argType = DfUnknown;

    if (isConstant(fieldName)) {
        if (fieldName[0] == '"') {
            // field is a string
            argType = DfString;
        } else if (isDigit(fieldName[0])) {
            if (strstr(fieldName, ".")) {
                argType = DfFloat64;
            } else {
                argType = DfInt64;
            }
        }
    } else {
        // lookup argtype from existing columns
        for (unsigned jj = 0; jj < tupMeta->getNumFields(); jj++) {
            if (strcmp(fieldName, srcFields[jj]) == 0) {
                argType = tupMeta->getFieldType(jj);
            }
        }

        if (argType == DfUnknown) {
            // field was not a source column
            xSyslogTxnBuf("GroupEval",
                          XlogErr,
                          "could not find column %s in source",
                          fieldName);
            return StatusInval;
        }
    }

    return StatusOk;
}

Status
GroupEvalContext::initArgVal(const char *fieldName,
                             const char **srcFields,
                             NewTupleMeta *tupMeta)
{
    Status status = StatusOk;
    assert(argType != DfUnknown && "arg type should be initialized before");

    if (isConstant(fieldName)) {
        // field is a constant
        constantValid = true;

        if (argType == DfString) {
            // allocate scalar to hold contents of string
            scalarVal = Scalar::allocScalar(DfMaxFieldValueSize);
            BailIfNull(scalarVal);

            DfFieldValue tmp;
            // trim leading and trailing quotes
            tmp.stringVal.strSize = strlen(fieldName) - 1;
            tmp.stringVal.strActual = &fieldName[1];

            status = scalarVal->setValue(tmp, DfString);
            BailIfFailed(status);

            status = scalarVal->getValue(&constantVal);
            BailIfFailed(status);
        } else if (argType == DfFloat64) {
            constantVal.float64Val = std::stod(fieldName, NULL);
        } else {
            constantVal.int64Val = atol(fieldName);
        }
    } else {
        unsigned jj;
        // find field in scratch xdb and populate argIdx
        for (jj = 0; jj < tupMeta->getNumFields(); jj++) {
            if (strcmp(fieldName, srcFields[jj]) == 0) {
                argIdx = jj;
                break;
            }
        }

        // first time seeing this field, add it to tup meta
        if (jj == tupMeta->getNumFields()) {
            argIdx = jj;
            tupMeta->addField(srcFields, fieldName, argType);
        }
    }

CommonExit:
    return status;
}
