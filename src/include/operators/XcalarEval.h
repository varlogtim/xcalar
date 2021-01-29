// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCALAR_EVAL_H
#define _XCALAR_EVAL_H

#include "primitives/Primitives.h"
#include "bc/BufferCache.h"
#include "util/StringHashTable.h"
#include "runtime/Spinlock.h"
#include "operators/XcalarEvalTypes.h"
#include "df/DataFormat.h"
#include "df/DataFormatTypes.h"
#include "scalars/Scalars.h"
#include "operators/XcalarEvalTypes.h"
#include "msg/MessageTypes.h"

struct OpEvalErrorStats;
struct XcalarApiUdfContainer;

class XcalarEval final
{
  public:
    enum ConvertEvalStringFlags {
        NoFlags = 0,
        ToImmediate = 1,
        FromImmediate = 1 << 1,
        UDFToRelative = 1 << 2,
    };

    static Status init();
    static void destroy();
    static XcalarEval *get();

    static XdfTypesAccepted convertToTypesAccepted(DfFieldType type);
    static DfFieldType getBestFieldType(XdfTypesAccepted typesAccepted);

    static void putFunction(XcalarEvalRegisteredFn *fn,
                            LibNsTypes::NsHandle *nsHandle);

    static bool isConstant(XcalarEvalAstNodeType type)
    {
        return type == XcalarEvalAstConstantNumber ||
               type == XcalarEvalAstConstantStringLiteral ||
               type == XcalarEvalAstConstantBool ||
               type == XcalarEvalAstConstantNull;
    }

    Status addUdf(XcalarEvalFnDesc *fnDesc,
                  XcalarEvalFn evalFn,
                  XcalarCleanupFn cleanupFn);
    unsigned deleteUdfsWithPrefix(const char *prefix);
    Status getFnList(const char *fnNamePattern,
                     const char *categoryPattern,
                     XcalarApiUdfContainer *udfContainer,
                     XcalarApiOutput **outputOut,
                     size_t *outputSizeOut);

    Status parseIdentifier(XcalarEvalAstNode *regularNode,
                           const char *evalStr,
                           const char *tokenStart,
                           unsigned *numConstants,
                           unsigned *numVariables);

    Status parseEvalStr(const char *evalStr,
                        XcalarFnType evalStrType,
                        XcalarApiUdfContainer *udfContainer,
                        XcalarEvalAstCommon *ast,
                        unsigned *numIntermediatesOut,
                        unsigned *numVariablesOut,
                        unsigned *numConstantsOut,
                        uint64_t *numFuncOut);

    Status reverseParseAst(XcalarEvalAstNodeType nodeType,
                           void *nodeIn,
                           char *buf,
                           unsigned &cur,
                           size_t &bytesRemaining,
                           ConvertEvalStringFlags flags);

    Status convertEvalString(char *evalString,
                             XcalarApiUdfContainer *udfContainer,
                             size_t bufSize,
                             XcalarFnType evalStrType,
                             unsigned numRenameCols,
                             struct XcalarApiRenameMap *renameMap,
                             ConvertEvalStringFlags flags);

    Status makeUDFRelativeInEvalString(char *evalString,
                                       size_t bufSize,
                                       XcalarFnType evalStrType,
                                       XcalarApiUdfContainer *udfContainer,
                                       ConvertEvalStringFlags flags);

    Status convertAstNames(XcalarEvalAstNodeType nodeType,
                           void *nodeIn,
                           unsigned numRenameCols,
                           struct XcalarApiRenameMap *renameMap,
                           ConvertEvalStringFlags flags);

    // detect types and uses the right function for that type if possible
    Status fixupTypeSensitiveFns(XcalarEvalAstNodeType nodeType,
                                 void *nodeIn,
                                 XdbMeta *xdbMeta);

    static Status escapeToken(const char *token,
                              char *tokenEscaped,
                              size_t size);
    static void freeCommonAst(XcalarEvalAstCommon *ast);
    void findMaxArgs(XcalarEvalAstOperatorNode *node,
                     unsigned *numScalarArgsOut,
                     unsigned *numArgsOut);
    // A groupIter is used to carry parameters and args info around.
    // It is updated for each node.
    Status initGroupIter(ScalarGroupIter *groupIter,
                         DagTypes::DagId dagId,
                         XdbId dstXdbId,
                         XcalarEvalAstOperatorNode *rootNode);
    void destroyGroupIter(ScalarGroupIter *groupIter);

    Status getVariablesList(XcalarEvalAstCommon *ast,
                            const char *variableNames[],
                            XdfTypesAccepted *variableTypes,
                            unsigned maxNumVariables,
                            unsigned *numVariablesOut);
    // Class1Ast is built for functions like map, and filter
    // (i.e. non-aggregate). The eval string for such functions is first
    // parsed to get the number of variables, constants, and intermediates
    // needed for the eval, and then memory is allocated for arrays of
    // pointers to them, and Scalar objects for intermediate nodes.
    // After that it traverses the AST to link scalar pointers on each
    // node to the pointer array on root, meanwhile scalar objects are
    // allocated for constants with token. Finally scalar objects for
    // variables are created.
    Status generateClass1Ast(const char *evalStr, XcalarEvalClass1Ast *ast);
    // The AST for group by and aggregate is slightly different from class1.
    // Data on class2AST is either scalar or group type. The same parse
    // happens and a first pass is applied on AST to get number of scalar
    // variables (agg result), group variables, scalar intermediates, group
    // intermediates and constants. Then the second pass acts like the pass of
    // class1AST but we only allocate scalar objects for scalar values.
    Status generateClass2Ast(const char *evalStr,
                             XcalarEvalClass2Ast *ast,
                             uint64_t *numFunc,
                             DagTypes::DagId dagId,
                             XdbId dstXdbId);
    static void destroyClass1Ast(XcalarEvalClass1Ast *ast);
    void destroyClass2Ast(XcalarEvalClass2Ast *ast);
    void aggregateDeleteVariables(XcalarEvalClass2Ast *ast);
    void deleteVariables(XcalarEvalClass1Ast *ast);

    Status eval(XcalarEvalClass1Ast *ast,
                DfFieldValue *fieldVal,
                DfFieldType *fieldType,
                bool icvMode);
    static bool isEvalResultTrue(DfFieldValue fieldVal, DfFieldType fieldType);
    // Validate names from the point of view of Xcalar Eval which is a context
    // free grammar.
    bool isValidDatasetName(const char *datasetName);
    bool isValidTableName(const char *tableName);
    bool isValidFieldName(const char *fieldName);

    static DfFieldType getOutputType(XcalarEvalAstCommon *ast);
    DfFieldType getOutputTypeXdf(const char *evalString);
    static bool isSingletonOutputType(XcalarEvalAstCommon *ast);

    Status distributeEvalFn(ScalarGroupIter *groupIter,
                            XcalarEvalRegisteredFn *registeredFn,
                            XdbId *scalarXdbIdOut,
                            Xdb **scalarXdbOut,
                            XdbGlobalStateMask scalarTableGlobalState);

    void aggregateMsgDistributeEvalFn(MsgEphemeral *ephemeral, void *payload);

    void aggregateMsgPopulateStatus(MsgEphemeral *ephemeral, void *payload);

    bool isComplex(XcalarEvalAstOperatorNode *operatorNode);

    Status aggregateEval(XcalarEvalClass2Ast *ast,
                         Scalar *scalarOut,
                         OpStatus *opStatus,
                         Xdb *scratchXdb);

    Status getArgs(Scalar *argv[],
                   Scalar **scratchPadScalars,
                   NewKeyValueEntry *kvEntry,
                   ScalarGroupIter *groupIter);

    size_t computeAuxiliaryPacketSize(ScalarGroupIter *groupIter);
    void packAuxiliaryPacket(ScalarGroupIter *groupIter,
                             uint8_t *auxiliaryPacket,
                             size_t auxiliaryPacketSize,
                             size_t *bytesCopiedOut);
    void unpackAuxiliaryPacket(ScalarGroupIter *groupIter,
                               uint8_t *auxiliaryPacket,
                               size_t *bytesProcessedOut,
                               Scalar *scalarValues[],
                               ScalarRecordArgument **argsOut,
                               int **argMappingOut);

    Status getUniqueVariablesList(XcalarEvalAstCommon *ast,
                                  const char *variableNames[],
                                  unsigned maxNumVariables,
                                  unsigned *numUniqueVariablesOut);

    bool containsUdf(XcalarEvalAstOperatorNode *node);
    bool containsUdf(char *evalString);
    bool containsAggregateVar(char *evalString);
    Status getUdfModuleFromLoad(const char *fqUdfName,
                                EvalUdfModuleSet *modules,
                                uint64_t *numUdfModules);
    Status getUdfModules(XcalarEvalAstOperatorNode *node,
                         EvalUdfModuleSet *modules,
                         uint64_t *numUdfModules);
    static void freeUdfModule(EvalUdfModule *module);

    static bool isFatalError(Status status);
    static void dropScalarVarRef(XcalarEvalAstCommon *ast);
    Status evalInternal(XcalarEvalAstOperatorNode *operatorNode, bool icvMode);

    static void aggregateMsgDistributeEvalFnWrapper(MsgEphemeral *ephemeral,
                                                    void *payload);
    static void aggregateMsgPopulateStatusWrapper(MsgEphemeral *ephemeral,
                                                  void *payload);

    Status evalAndInsert(int argc,
                         Scalar **argv,
                         Scalar **scratchPadScalars,
                         NewKeyValueEntry *kvEntry,
                         ScalarGroupIter *groupIter,
                         XcalarEvalRegisteredFn *registeredFn,
                         XdbMeta *prevXdbMeta,
                         XdbMeta *outputXdbMeta,
                         Xdb *outputXdb,
                         Scalar *outputScalar,
                         int64_t slotId,
                         OpEvalErrorStats *errorStats);

    // The fnName may be a builtIn or a UDF. The caller context typically knows
    // whether the fnName could be a UDF - if so, pass a non-NULL udfContainer
    // into the call - leave it NULL otherwise. If it's not a UDF (udfContainer
    // is NULL), the call returns the function as normal. If it could be a UDF,
    // (udfContainer is non-NULL - typically during parsing), the function
    // resolves the fnName by searching the paths specified in
    // UserDefinedFunction::UdfPath[] and returns the function if found in one
    // of the paths.
    XcalarEvalRegisteredFn *getOrResolveRegisteredFn(
        const char *fnName,
        XcalarApiUdfContainer *udfContainer,
        LibNsTypes::NsHandle *nsHandle);

    Status updateArgMappings(XcalarEvalAstOperatorNode *operatorNode,
                             XdbMeta *xdbMeta);

    Status setupEvalAst(const char *filterString,
                        XdbMeta *dstMeta,
                        XcalarEvalClass1Ast &ast,
                        unsigned &numVariables,
                        int *&argIndices,
                        Scalar **&scalars,
                        DagTypes::DagId dagId);

    Status setupEvalAst(const char *filterString,
                        unsigned numFields,
                        const char **valueNames,
                        XcalarEvalClass1Ast &ast,
                        unsigned &numVariables,
                        int *&argIndices,
                        Scalar **&scalars,
                        DagTypes::DagId dagId);

    Status setupEvalMultiAst(const char *filterString,
                             XdbMeta *leftMeta,
                             XdbMeta *rightMeta,
                             XcalarEvalClass1Ast &ast,
                             unsigned &numVariables,
                             int *&argIndices,
                             Scalar **&scalars,
                             XcalarApiRenameMap *renameMap,
                             unsigned numRenameEntriesLeft,
                             unsigned numRenameEntriesRight,
                             DagTypes::DagId dagId);

    Status executeEvalAst(NewKeyValueEntry *dstKvEntry,
                          unsigned dstNumFields,
                          XcalarEvalClass1Ast *ast,
                          int *argIndices,
                          Scalar **scalars,
                          DfFieldValue *resultOut,
                          DfFieldType *resultTypeOut);

    Status executeEvalMultiAst(NewKeyValueEntry *leftKvEntry,
                               unsigned leftNumFields,
                               NewKeyValueEntry *rightKvEntry,
                               XcalarEvalClass1Ast *ast,
                               int *argIndices,
                               Scalar **scalars,
                               DfFieldValue *resultOut,
                               DfFieldType *resultTypeOut);

    Status executeFilterMultiAst(NewKeyValueEntry *leftKvEntry,
                                 unsigned leftNumFields,
                                 NewKeyValueEntry *rightKvEntry,
                                 XcalarEvalClass1Ast *ast,
                                 int *argIndices,
                                 Scalar **scalars,
                                 bool *result);

    Status executeFilterAst(NewKeyValueEntry *dstKvEntry,
                            unsigned dstNumFields,
                            XcalarEvalClass1Ast *ast,
                            int *argIndices,
                            Scalar **scalars,
                            bool *result);

    static void replaceAggregateVariableName(const char *oldAggName,
                                             const char *newAggName,
                                             char *evalString);

    bool isRangeFilter(XcalarEvalClass1Ast *ast, const char *keyName);

    bool inFilterRange(XcalarEvalClass1Ast *ast,
                       DfFieldValue min,
                       DfFieldValue max,
                       bool valid,
                       const char *keyName,
                       DfFieldType keyType);
    Status fixupConstant(XcalarEvalAstNodeType nodeType,
                         XcalarEvalAstNode *regularNode,
                         Scalar *constants[],
                         unsigned *constantIdx);

  private:
    static constexpr const char *ModuleName = "liboperators";
    static constexpr unsigned NumRegisteredFnsHashSlots = 89;
    static constexpr const char *KeywordFalse = "false";
    static constexpr const char *KeywordTrue = "true";
    static constexpr const char *KeywordCapFalse = "False";
    static constexpr const char *KeywordCapTrue = "True";
    static constexpr const char *KeywordNone = "None";
    static constexpr const char *KeywordNull = "Null";
    static constexpr const int ArgMappingInvalid = -1;
    static constexpr const char *truncatedMsg = "(truncated..) ";
    static constexpr const char *failedErrorMsg = "<error unknown>";

    typedef StringHashTable<XcalarEvalRegisteredFn,
                            &XcalarEvalRegisteredFn::hook,
                            &XcalarEvalRegisteredFn::getFnName,
                            NumRegisteredFnsHashSlots,
                            hashStringFast>
        FnHashTable;

    XcalarEval();
    ~XcalarEval();

    XcalarEval(const XcalarEval &) = delete;
    XcalarEval &operator=(const XcalarEval &) = delete;

    Status initInternal();
    void destroyInternal();

    void deleteRegisteredFn(RefCount *refCount);
    static void deleteRegisteredFnWrapper(RefCount *refCount);
    XcalarEvalRegisteredFn *getRegisteredFn(const char *fnName,
                                            LibNsTypes::NsHandle *nsHandle);

    static bool isOperatorNode(XcalarEvalAstNodeType nodeType);
    bool isValidIdentifierChar(char c);
    bool isGroupArg(XcalarEvalAstNodeType nodeType, const char *token);
    Status fixupClass2AstFirstPass(
        XcalarEvalAstOperatorNode *operatorNodeParent,
        unsigned *numUniqueVariables,
        char *variableNames[],
        unsigned *numScalarIntermediates,
        unsigned *numGroupIntermediates,
        unsigned *numScalarVariables);
    Status fixupArgumentPointersClass1(
        XcalarEvalAstOperatorNode *operatorNodeParent,
        unsigned *scalarVariableIdx,
        unsigned *constantIdx,
        unsigned *scalarIntermediateIdx,
        XcalarEvalScalarVariable *scalarVariables,
        Scalar *constants[],
        Scalar *scalarIntermediates[]);
    Status fixupClass2AstSecondPass(
        XcalarEvalAstOperatorNode *operatorNodeParent,
        unsigned *valueArrayIdx,
        unsigned *constantIdx,
        unsigned *scalarIntermediateIdx,
        unsigned *scalarVariableIdx,
        Scalar *constants[],
        Scalar *scalarIntermediates[],
        XcalarEvalScalarVariable scalarVariables[]);
    Status allocScalarVariables(XcalarEvalAstCommon *ast,
                                unsigned numVariables);
    static void freeScalarVariables(XcalarEvalAstCommon *ast);
    Status allocScalarIntermediates(XcalarEvalAstCommon *ast,
                                    unsigned numIntermediates);
    static void freeScalarIntermediates(XcalarEvalAstCommon *ast);
    static void freeScalarConstants(XcalarEvalAstCommon *ast);
    Status getVariablesListInternal(
        XcalarEvalAstOperatorNode *operatorNodeParent,
        unsigned *variableIdx,
        const char *variableNames[],
        XdfTypesAccepted *variableTypes,
        unsigned maxNumVariables);

    void makeArgMapping(XdbMeta *xdbMeta,
                        XcalarEvalAstNode **args,
                        unsigned numArgs,
                        int *argMapping);

    static void freeAstNode(XcalarEvalAstOperatorNode *operatorNode);

    Status evalLocalInt(ScalarGroupIter *groupIter,
                        XdbId outputScalarXdbId,
                        XcalarEvalRegisteredFn *registeredFn);
    Status aggregateEvalInt(XdbId *scalarXdbId,
                            Xdb **scalarXdb,
                            XdbGlobalStateMask scalarTableGlobalState,
                            XcalarEvalAstOperatorNode *operatorNode,
                            ScalarGroupIter *groupIter,
                            OpStatus *opStatus);
    Status insertIntoEvalUdfModuleSet(EvalUdfModuleSet *modules,
                                      const char *fullyQualifiedFnName,
                                      uint64_t *numUdfModules);
    Status getUniqueVariablesList(XcalarEvalAstOperatorNode *operatorNodeParent,
                                  const char *variableNames[],
                                  unsigned maxNumVariables,
                                  unsigned *variableIdx);
    Status parseOperatorArgs(char *evalStr,
                             XcalarFnType evalStrType,
                             XcalarApiUdfContainer *udfContainer,
                             char **evalStrOut,
                             XcalarEvalAstOperatorNode *operatorNodeParent,
                             unsigned *numVariables,
                             unsigned *numConstants,
                             unsigned *numIntermediates,
                             uint64_t *numFunc);
    XcalarEvalRegisteredFn *getRegisteredFnInternal(const char *fnName);

    FnHashTable registeredFns_;
    Mutex registeredFnsLock_;

    Status convertIfConditions(XcalarEvalAstNodeType nodeType,
                               void *nodeIn,
                               XdbMeta *xdbMeta);

    Status convertMinMaxFns(XcalarEvalAstNodeType nodeType,
                            void *nodeIn,
                            XdbMeta *xdbMeta);

    static XcalarEval *instance;
};

#endif  // _XCALAR_EVAL_H
