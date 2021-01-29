// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "operators/XcalarEval.h"
#include "util/MemTrack.h"
#include "bc/BufferCache.h"
#include "scalars/Scalars.h"
#include "stat/Statistics.h"
#include "dataformat/DataFormatCsv.h"
#include "dataformat/DataFormatSql.h"
#include "dataformat/DataFormatJson.h"
#include "common/InitTeardown.h"
#include <libgen.h>
#include "dataset/BackingData.h"

#include "test/QA.h"  // Must be last

/*
static void
xcalarEvalVariableSubstFromRecord(XcalarEvalClass1Ast *ast)
{
    Status status;
    unsigned ii;
    unsigned numValsRemaining;
    XcalarEvalScalarVariable *scalarVariable;
    DfFieldAttr fieldAttr;
    DataFormat::get()->initFieldAttr(&fieldAttr);
    Scalar *scalar;
    ScalarLib *scl = ScalarLib::get();

    for (ii = 0; ii < ast->astCommon.numScalarVariables; ii++) {
        scalarVariable = &ast->astCommon.scalarVariables[ii];
        assert(scalarVariable->content == NULL);

        DemystifyVariable variable;

        verifyOk(variable.setName(scalarVariable->variableName));

        status = DataFormat::get()->getFieldValue()
        status = DataFormat::get()->
            getFieldFromRecord(record, scalarVariable->variableName,
                                      &fieldAttr, true);
        if (status == StatusOk) {
            if (fieldAttr.numValues > 1 ||
                DataFormat::
                fieldGetSize(fieldAttr.header.type) == DfVariableFieldSize) {
                scalar = scl->newVariableSizeScalar(fieldAttr.fieldSize,
                                                    true);
            } else {
                scalar = scl->newFixedSizeScalar();
            }
            assert(scalar != NULL);

            status = DataFormat::get()->
                getFieldValuesFromRecord2(record, &fieldAttr,
                                                 0, &scalar->fieldVals,
                                                 scalar->fieldValsBufSize,
                                                 &scalar->numValues,
                                                 &numValsRemaining);
            scalar->argType = fieldAttr.header.type;
            if (status == StatusOk) {
                assert(numValsRemaining == 0);
                assert(scalar->numValues == fieldAttr.numValues);
                scalarVariable->content = scalar;
            } else {
                assert(scalarVariable->content == NULL);
                scl->freeScalar(scalar);
                scalar = NULL;
            }
        } else {
            assert(scalarVariable->content == NULL);
        }
    }
}
*/

/*
static void
freeWrapper(void *p) {
    memAlignedFree(p);
}
*/

static Status
evalTestSanity()
{
    /*
    XcalarEvalClass1Ast ast;
    Status status;
    const char *evalStr;
    XcalarEvalAstOperatorNode *operatorNode;
    double result;
    int64_t intResult;
    DfFieldAttr fieldAttr;
    DataFormat::get()->initFieldAttr(&fieldAttr);
    DfFieldType fieldType;
    DfFieldValue fieldVal;

    evalStr = "add(1, 2)";
    status = XcalarEval::get()->generateClass1Ast(evalStr, &ast);
    assert(status == StatusOk);
    assert(strcmp(ast.astCommon.rootNode->common.token, "add") == 0);
    assert(ast.astCommon.rootNode->numArgs == 2);
    assert(strcmp(ast.astCommon.rootNode->arguments[0]->token, "1") == 0);
    assert(strcmp(ast.astCommon.rootNode->arguments[1]->token, "2") == 0);
    status = XcalarEval::get()->eval(&ast, &fieldVal, &fieldType);
    assert(status == StatusOk);
    assert(fieldType == DfFloat64);
    result = fieldVal.float64Val;
    assert(result == (1.0 + 2.0));
    XcalarEval::get()->destroyClass1Ast(&ast);

    verifyOk(XcalarEval::get()->generateClass1Ast("add(0x1A, 0xf)", &ast));
    assert(strcmp(ast.astCommon.rootNode->common.token, "add") == 0);
    assert(ast.astCommon.rootNode->numArgs == 2);
    assert(strcmp(ast.astCommon.rootNode->arguments[0]->token, "0x1A") == 0);
    assert(strcmp(ast.astCommon.rootNode->arguments[1]->token, "0xf") == 0);
    verifyOk(XcalarEval::get()->eval(&ast, &fieldVal, &fieldType));
    assert(fieldType == DfFloat64);
    result = fieldVal.float64Val;
    assert(result == (float) (0x1A + 0xf));
    XcalarEval::get()->destroyClass1Ast(&ast);

    evalStr = "sub(3, add(1, 2))";
    status = XcalarEval::get()->generateClass1Ast(evalStr, &ast);
    assert(status == StatusOk);
    assert(strcmp(ast.astCommon.rootNode->common.token, "sub") == 0);
    assert(ast.astCommon.rootNode->numArgs == 2);
    assert(strcmp(ast.astCommon.rootNode->arguments[0]->token, "3") == 0);
    assert(ast.astCommon.rootNode->arguments[1]->nodeType ==
           XcalarEvalAstOperator);
    operatorNode = (XcalarEvalAstOperatorNode *)
                   ast.astCommon.rootNode->arguments[1];
    assert(strcmp(operatorNode->common.token, "add") == 0);
    assert(operatorNode->numArgs == 2);
    assert(strcmp(operatorNode->arguments[0]->token, "1") == 0);
    assert(strcmp(operatorNode->arguments[1]->token, "2") == 0);
    operatorNode = NULL;
    status = XcalarEval::get()->eval(&ast, &fieldVal, &fieldType);
    assert(status == StatusOk);
    assert(fieldType == DfFloat64);
    result = fieldVal.float64Val;
    assert(result == (3.0 - (1.0 + 2.0)));
    XcalarEval::get()->destroyClass1Ast(&ast);

    evalStr = "sub(add(1, 2), 3)";
    status = XcalarEval::get()->generateClass1Ast(evalStr, &ast);
    assert(status == StatusOk);
    assert(strcmp(ast.astCommon.rootNode->common.token, "sub") == 0);
    assert(ast.astCommon.rootNode->numArgs == 2);
    assert(ast.astCommon.rootNode->arguments[0]->nodeType ==
           XcalarEvalAstOperator);
    operatorNode = (XcalarEvalAstOperatorNode *)
                   ast.astCommon.rootNode->arguments[0];
    assert(strcmp(operatorNode->common.token, "add") == 0);
    assert(operatorNode->numArgs == 2);
    assert(strcmp(operatorNode->arguments[0]->token, "1") == 0);
    assert(strcmp(operatorNode->arguments[1]->token, "2") == 0);
    operatorNode = NULL;
    assert(strcmp(ast.astCommon.rootNode->arguments[1]->token, "3") == 0);
    status = XcalarEval::get()->eval(&ast, &fieldVal, &fieldType);
    assert(status == StatusOk);
    assert(fieldType == DfFloat64);
    result = fieldVal.float64Val;
    assert(result == ((1.0 + 2.0) - 3.0));
    XcalarEval::get()->destroyClass1Ast(&ast);

    evalStr = "sub(add(3, 4), add(1, 2))";
    status = XcalarEval::get()->generateClass1Ast(evalStr, &ast);
    assert(status == StatusOk);
    assert(strcmp(ast.astCommon.rootNode->common.token, "sub") == 0);
    assert(ast.astCommon.rootNode->numArgs == 2);
    assert(ast.astCommon.rootNode->arguments[0]->nodeType ==
           XcalarEvalAstOperator);
    operatorNode = (XcalarEvalAstOperatorNode *)
                   ast.astCommon.rootNode->arguments[0];
    assert(strcmp(operatorNode->common.token, "add") == 0);
    assert(operatorNode->numArgs == 2);
    assert(strcmp(operatorNode->arguments[0]->token, "3") == 0);
    assert(strcmp(operatorNode->arguments[1]->token, "4") == 0);
    operatorNode = NULL;
    assert(ast.astCommon.rootNode->arguments[1]->nodeType ==
           XcalarEvalAstOperator);
    operatorNode = (XcalarEvalAstOperatorNode *)
                   ast.astCommon.rootNode->arguments[1];
    assert(strcmp(operatorNode->common.token, "add") == 0);
    assert(operatorNode->numArgs == 2);
    assert(strcmp(operatorNode->arguments[0]->token, "1") == 0);
    assert(strcmp(operatorNode->arguments[1]->token, "2") == 0);
    operatorNode = NULL;
    status = XcalarEval::get()->eval(&ast, &fieldVal, &fieldType);
    assert(status == StatusOk);
    assert(fieldType == DfFloat64);
    result = fieldVal.float64Val;
    assert(result == ((3.0 + 4.0) - (1.0 + 2.0)));
    XcalarEval::get()->destroyClass1Ast(&ast);


    evalStr = "wordCount(\"The quick  red\tfox\njumps\t"
              "\tover\n\nthe\t\nlazy brown \t\ndog\")";
    status = XcalarEval::get()->generateClass1Ast(evalStr, &ast);
    assert(status == StatusOk);
    status = XcalarEval::get()->eval(&ast, &fieldVal, &fieldType);
    assert(status == StatusOk);
    assert(fieldType == DfInt64);
    intResult = fieldVal.int64Val;
    printf("%s = %lu\n", evalStr, intResult);
    assert(intResult == 10);
    XcalarEval::get()->destroyClass1Ast(&ast);

    // Ok to cast to char* here - in the real code we do allocate the char *,
    // buffer properly. In this particular test we(=Brent) KNOW that we do
    // NOT modify this piece of memory.
    char *jsonObjects[] = {
        (char *)"{\"funny\": 10, \"useful\": 20, \"expectedResult\": 14.0}",
        (char *)"{\"funny\": 20, \"useful\": 10, \"expectedResult\": 16.0}",
    };

    evalStr = "div(add(mult(funny, 6), mult(useful, 4)), 10)";
    status = XcalarEval::get()->generateClass1Ast(evalStr, &ast);
    assert(status == StatusOk);
    assert(ast.astCommon.numScalarVariables == 2);
    assert(ast.astCommon.numConstants == 3);
    assert(strcmp(ast.astCommon.scalarVariables[0].variableName, "funny") == 0);
    assert(strcmp(ast.astCommon.scalarVariables[1].variableName, "useful")
                                                                          == 0);
    DataPageIndex index;
    Status iterStatus;
    unsigned ii;
    char errStr[100];


    DfLoadArgs loadArgs;
    memZero(&loadArgs, sizeof(loadArgs));
    DfDictionarySegment *dictSegment;
    for (ii = 0; ii < ArrayLen(jsonObjects); ii++) {
        size_t bufLen = strlen(jsonObjects[ii]) + 1;
        void *data = memAlloc(bufLen);
        assert(data);
        memcpy(data, jsonObjects[ii], bufLen);
        BackingDataMem *bd = new BackingDataMem(NULL, data, bufLen, bufLen);
        status = DataFormat::get()->
            parseDataIntoIndex(&index,
                               bd,
                               DfFormatJson,
                               &loadArgs,
                               errStr,
                               sizeof(errStr));
        assert(status == StatusOk);
    }

    for (int ii = 0; ii < index.getNumRecords(); ii++) {
        double expectedResult;
        unsigned numValsReturned;
        unsigned numValsRemaining;

        DfDeclFieldValueArray(64, Temp);
        DfFieldValueArrayTemp tmpFieldVals;

        xcalarEvalVariableSubstFromRecord(&ast, &recordIter.curRecord);
        status = XcalarEval::get()->eval(&ast, &fieldVal, &fieldType);
        assert(status == StatusOk);
        assert(fieldType == DfFloat64);
        result = fieldVal.float64Val;
        XcalarEval::get()->deleteVariables(&ast);

        status = DataFormat::get()->getFieldFromRecord(&recordIter.curRecord,
                                      "expectedResult", &fieldAttr, false);

        assert(status == StatusOk);
        assert(strcmp(fieldAttr.header.name, "expectedResult") == 0);
        status = DataFormat::get()->
            getFieldValuesFromRecord2(&recordIter.curRecord,
                                             &fieldAttr, 0,
                                             &tmpFieldVals.fields,
                                             sizeof(tmpFieldVals),
                                             &numValsReturned,
                                             &numValsRemaining);
        assert(status == StatusOk);
        assert(numValsReturned == 1);
        assert(numValsRemaining == 0);

        status = DataFormat::getFieldValueFromArray(&tmpFieldVals.fields,
                                          sizeof(tmpFieldVals), DfFloat64,
                                          0, &fieldVal, NULL);
        assert(status == StatusOk);
        expectedResult = fieldVal.float64Val;

        assert(result == expectedResult);
        printf("Computed Result %lf Expected Result %lf\n", result,
               expectedResult);
    }

    XcalarEval::get()->destroyClass1Ast(&ast);
    */

    return StatusOk;
}

static Status
evalTestInvalid()
{
    XcalarEvalClass1Ast ast;

    verify(XcalarEval::get()->generateClass1Ast("add(1, 2)    ", &ast) ==
           StatusOk);
    XcalarEval::get()->destroyClass1Ast(&ast);

    verify(XcalarEval::get()->generateClass1Ast("add(1, 2) weird trailing",
                                                &ast) ==
           StatusAstMalformedEvalString);
    XcalarEval::get()->destroyClass1Ast(&ast);
    verify(XcalarEval::get()->generateClass1Ast("add(1, 2)add(1, 2)", &ast) ==
           StatusAstMalformedEvalString);
    XcalarEval::get()->destroyClass1Ast(&ast);
    verify(XcalarEval::get()->generateClass1Ast("add(1, 2q)", &ast) ==
           StatusOk);
    XcalarEval::get()->destroyClass1Ast(&ast);
    verify(XcalarEval::get()->generateClass1Ast("add(1.0, 1.0.6)", &ast) ==
           StatusOk);
    XcalarEval::get()->destroyClass1Ast(&ast);

    // Would seg fault without Xc-3355 fix.
    Status status = XcalarEval::get()->generateClass1Ast("god:dog", &ast);
    assert(status == StatusAstMalformedEvalString);
    XcalarEval::get()->destroyClass1Ast(&ast);

    return StatusOk;
}

static TestCase testCases[] = {
    {"libeval: sanity", evalTestSanity, TestCaseEnable, ""},
    {"libeval: invalid eval strings", evalTestInvalid, TestCaseEnable, "3355"},
};

int
main(int argc, char *argv[])
{
    char fullCfgFilePath[255];
    const char *cfgFile = "test-config.cfg";
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Active on Physical */,
                                BufferCacheMgr::TypeNone));

    int numTestsFailed = qaRunTestSuite(testCases,
                                        ArrayLen(testCases),
                                        TestCaseScheduleOnRuntime);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
