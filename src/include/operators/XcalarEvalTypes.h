// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCALAR_EVAL_TYPES_H
#define _XCALAR_EVAL_TYPES_H

#include "xdb/TableTypes.h"
#include "xdb/DataModelTypes.h"
#include "df/DataFormatTypes.h"
#include "FunctionCategory.h"
#include "util/RefCount.h"
#include "util/StringHashTable.h"
#include "cursor/Cursor.h"
#include "XcalarEvalEnums.h"

enum {
    XcalarEvalMaxNumArgs = 1024,
    XcalarEvalVariableNumArgs = -1,
    XcalarEvalMaxTokenLen = 1024,
    XcalarEvalMaxNumVariables = 1024,
    XcalarEvalMaxFnNameLen = LibNsTypes::MaxPathNameLen,
    XcalarEvalMaxFnDescLen = 1024,
    XcalarEvalMaxArgDescLen = 256,
    EvalUdfModuleSetSlotCount = 5,
};

struct XcalarEvalScalarVariable {
    Scalar *content;
    char *variableName;
};

// Scalar values can be stored in varContent in the astNode itself
// Group values are stored in the tempXdb created
// Constants are guaranteed to be scalar for now
// A class 2 fn will return a scalar value
// When run in aggregate mode, a class 1 fn will return a group of values
// Scalar operator are operators whose return value is s scalar
// (could be a class 1 fn acting on a scalar, or a class 2 fn acting on a group)
// Group operators are operators whose return value is a group
// (right now only a class 1 fn acting on a group)
// XcalarEvalAstOperator is indeterminate (we haven't determine its return
// value yet)
typedef enum {
    XcalarEvalAstOperator,
    XcalarEvalAstGroupOperator,
    XcalarEvalAstScalarOperator,
    XcalarEvalAstVariable,
    XcalarEvalAstConstantNumber,
    XcalarEvalAstConstantStringLiteral,
    XcalarEvalAstConstantBool,
    XcalarEvalAstConstantNull,
} XcalarEvalAstNodeType;

enum { XcalarEvalAstNodeInvalidIdx = -1 };
struct XcalarEvalAstNode {
    XcalarEvalAstNodeType nodeType;
    char token[XcalarEvalMaxTokenLen];
    union {
        // If node contains a scalar value, can be found here
        Scalar **varContent;

        // If node contains a group value, then valueArrayIdx
        // is the entry in valueArray returned in the tempXdb
        // corresponding to this node's value
        int valueArrayIdx;
    };
};

typedef Status (*XcalarEvalFn)(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
typedef Status (*XcalarAggFn)(struct ScalarGroupIter *groupIter, Scalar *out);
typedef void (*XcalarCleanupFn)(void *context);
typedef void *(*XcalarMakeContextFn)(void *context);
typedef void (*XcalarDelContextFn)(void *context);

typedef enum {
    XdfAcceptNone = 0,

    // primitive types
    XdfAcceptInt32 = 1 << DfInt32,
    XdfAcceptInt64 = 1 << DfInt64,
    XdfAcceptFloat32 = 1 << DfFloat32,
    XdfAcceptFloat64 = 1 << DfFloat64,
    XdfAcceptString = 1 << DfString,
    XdfAcceptBoolean = 1 << DfBoolean,
    XdfAcceptUInt32 = 1 << DfUInt32,
    XdfAcceptUInt64 = 1 << DfUInt64,
    XdfAcceptNull = 1 << DfNull,
    XdfAcceptTimestamp = 1 << DfTimespec,
    XdfAcceptMoney = 1 << DfMoney,
    XdfAcceptInt = XdfAcceptInt32 | XdfAcceptInt64 | XdfAcceptUInt32 |
                   XdfAcceptUInt64 | XdfAcceptBoolean,

    XdfAcceptFloat = XdfAcceptFloat32 | XdfAcceptFloat64,

    XdfAcceptAll = ~0 ^ XdfAcceptNull,
} XdfTypesAccepted;

enum XcalarFnType {
    XcalarFnTypeEval,
    XcalarFnTypeAgg,
};

struct XcalarEvalFnDesc {
    char fnName[XcalarEvalMaxFnNameLen + 1];
    char fnDesc[XcalarEvalMaxFnDescLen + 1];
    FunctionCategory category;
    bool isSingletonOutput;
    DfFieldType outputType;

    // Opaque context given when registered that's passed to every invocation.
    void *context;

    // XXX should change numArgs to minArgs && maxArgs and use
    // a thrift-exposed symbolic XcalarEvalVariableNumArgs instead of -1
    // to represent variable#
    //
    // Set to -1 if you have variable number of arguments
    // If variable number of arguments, enforceArgCheck must be false
    int numArgs;
    struct ArgDesc {
        char argDesc[XcalarEvalMaxArgDescLen + 1];
        XdfTypesAccepted typesAccepted;

        // Until we have higher-order types, such as sum-type (union),
        // multiplicative-type (tuples), recursive types (to allow arrays and
        // objects) (Xc-3271)
        // we'll just have to treat array as a primitive type with numValues
        bool isSingletonValue;
        XcalarEvalArgType argType;

        // only valid for optional args
        DfFieldValue defaultValue;
        DfFieldType defaultType;

        // only valid for variable args
        unsigned minArgs;
        unsigned maxArgs;
    } argDescs[XcalarEvalMaxNumArgs];
};

// Needs PageSize alignment for Sparse copy.
struct __attribute__((aligned(PageSize))) XcalarEvalRegisteredFn {
    XcalarEvalFnDesc fnDesc;
    struct {
        union {
            XcalarEvalFn evalFn;
            XcalarAggFn aggFn;
        };
        XcalarFnType fnType;
    };
    bool enforceArgCheck;
    bool isBuiltIn;
    StringHashTableHook hook;

    const char *getFnName() const { return fnDesc.fnName; };

    void del();

    // Called once this XcalarEvalRegisteredFn's ref count reaches 0. Passed
    // fnDesc.context.
    XcalarCleanupFn cleanupFn;
    RefCount refCount;
    // dynamic context function
    XcalarMakeContextFn makeContextFn;
    XcalarDelContextFn delContextFn;

    static MustCheck XcalarEvalRegisteredFn *allocXcEvalRefFn();
    static void freeXcEvalRegisteredFn(XcalarEvalRegisteredFn *regFn);
};

struct XcalarEvalAstOperatorNode {
    XcalarEvalAstNode common;
    XcalarEvalRegisteredFn *registeredFn;
    LibNsTypes::NsHandle nsHandle;
    unsigned numArgs;
    unsigned maxNumArgs;
    void *dynamicContext = NULL;
    int *argMapping = NULL;
    XcalarEvalAstNode *arguments[XcalarEvalMaxNumArgs];
};

struct XcalarEvalAstCommon {
    XcalarEvalAstOperatorNode *rootNode;
    unsigned numConstants;
    Scalar **constants;
    unsigned numScalarIntermediates;
    Scalar **scalarIntermediates;
    unsigned numScalarVariables;
    XcalarEvalScalarVariable *scalarVariables;
};

// Used to communicate a hashtable of needed modules to UDF infrastructure.
struct EvalUdfModule {
    char moduleName[XcalarApiMaxUdfModuleNameLen + 1];
    StringHashTableHook hook;

    const char *getName() const { return moduleName; }

    void del();
};

enum ScalarArgumentType {
    ScalarArgFromTable,
    ScalarArgFromScalarArray,
};

enum ScalarCursorType {
    ScalarSameKeyCursor,
    ScalarLocalCursor,
    ScalarGlobalCursor,
};

struct ScalarRecordArgument {
    ScalarArgumentType argType;
    XdfTypesAccepted typesAccepted;
    bool isSingletonValue;
    union {
        // If argument to be retrieved from tempXdbId
        unsigned valueArrayIdx;
        // If argument to be retrieved from scalarValues
        unsigned scalarValueIdx;
    };
};

struct ScalarGroupIter {
    XdbId srcXdbId;
    ScalarCursorType cursorType;
    XdbMeta *srcMeta = NULL;
    Xdb *srcXdb = NULL;
    struct OpStatus *opStatus = NULL;

    bool scalarFieldType;
    bool parallelize;
    bool enforceArgCheck;
    unsigned numArgs = 0;
    unsigned maxArgs = 0;
    ScalarRecordArgument *args = NULL;
    unsigned numScalarValues = 0;
    unsigned maxScalarValues = 0;
    Scalar **scalarValues = NULL;
    XdbId dstXdbId;
    DagTypes::DagId dagId;

    int *argMapping = NULL;

    void setInput(ScalarCursorType cursorType,
                  XdbId srcXdbId,
                  bool parallelize,
                  Xdb *srcXdb,
                  XdbMeta *srcMeta,
                  int *argMapping,
                  OpStatus *opStatus,
                  bool scalarFieldType)
    {
        this->srcXdbId = srcXdbId;
        this->cursorType = cursorType;
        this->opStatus = opStatus;
        this->parallelize = parallelize;
        this->srcXdb = srcXdb;
        this->srcMeta = srcMeta;
        this->argMapping = argMapping;
        this->scalarFieldType = scalarFieldType;
    }
};

struct XcalarEvalClass1Ast {
    XcalarEvalAstCommon astCommon;
};

struct XcalarEvalClass2Ast {
    XcalarEvalAstCommon astCommon;
    unsigned numUniqueVariables;
    char **variableNames;
    XdbId scalarXdbId;  // To store output of class 1 fn applied to group
    XdbGlobalStateMask scalarTableGlobalState;
    struct ScalarGroupIter groupIter;
};

typedef StringHashTable<EvalUdfModule,
                        &EvalUdfModule::hook,
                        &EvalUdfModule::getName,
                        EvalUdfModuleSetSlotCount>
    EvalUdfModuleSet;

#endif  // _XCALAR_EVAL_H
