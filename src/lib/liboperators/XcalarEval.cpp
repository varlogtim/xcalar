// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <cstdlib>
#include <stddef.h>
#include <math.h>

#include "StrlFunc.h"
#include "operators/XcalarEval.h"
#include "XcalarEvalInt.h"
#include "df/DataFormat.h"
#include "operators/Xdf.h"
#include "scalars/Scalars.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisRecv.h"
#include "xdb/Xdb.h"
#include "operators/Operators.h"
#include "util/MemTrack.h"
#include "hash/Hash.h"
#include "util/RefCount.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "dag/DagLib.h"
#include "msg/Xid.h"
#include "udf/UserDefinedFunction.h"
#include "common/InitTeardown.h"
#include "ns/LibNs.h"
#include "operators/OperatorsHash.h"

bool supportedMatrix[DfFieldTypeLen][DfFieldTypeLen];
XcalarEval *XcalarEval::instance = NULL;
static constexpr const char *ModuleName = "XcalarEval";

struct FreeScalarValuesPacket {
    XdbId scalarXdbId;
    OperatorsFreeScalarValuesMode mode;
};

static_assert(DfFieldTypeLen <
                  BitsPerUInt8 * sizeof(((XcalarEvalRegisteredFn *) 0)
                                            ->fnDesc.argDescs[0]
                                            .typesAccepted),
              "typesAccepted fits in DfFieldTypeLen");

////////////////////////////////////////////////////////////////////////////////

//
// Init/teardown logic.
//

XcalarEval::XcalarEval() {}

XcalarEval::~XcalarEval() {}

Status  // static
XcalarEval::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) XcalarEval;
    if (instance == NULL) {
        return StatusNoMem;
    }
    Status status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }
    return status;
}

void  // static
XcalarEval::destroy()
{
    if (instance == NULL) {
        return;
    }
    instance->destroyInternal();
    delete instance;
    instance = NULL;
}

XcalarEval *
XcalarEval::get()
{
    return instance;
}

Status
XcalarEval::initInternal()
{
    Status status = StatusOk;

    for (unsigned ii = 0; ii < builtInFnsCount; ii++) {
        builtInFns[ii].isBuiltIn = true;
        refInit(&builtInFns[ii].refCount,
                XcalarEval::deleteRegisteredFnWrapper);
        for (int jj = 0; jj < builtInFns[ii].fnDesc.numArgs; jj++) {
            if (builtInFns[ii].fnDesc.argDescs[jj].argType == OptionalArg) {
                assert(builtInFns[ii].fnDesc.argDescs[jj].defaultType !=
                       DfUnknown);
            }
        }
        registeredFns_.insert(&builtInFns[ii]);
    }

    for (int ii = 0; ii < DfFieldTypeLen - 1; ++ii) {
        for (int jj = 0; jj < DfFieldTypeLen - 1; ++jj) {
            supportedMatrix[ii][jj] = false;
        }
    }

    supportedMatrix[DfString][DfString] = true;
    supportedMatrix[DfTimespec][DfTimespec] = true;
    supportedMatrix[DfMoney][DfMoney] = true;

    supportedMatrix[DfInt32][DfInt32] = true;
    supportedMatrix[DfInt32][DfUInt32] = true;
    supportedMatrix[DfInt32][DfInt64] = true,
    supportedMatrix[DfInt32][DfUInt64] = true;
    supportedMatrix[DfInt32][DfFloat32] = true;
    supportedMatrix[DfInt32][DfFloat64] = true;
    supportedMatrix[DfInt32][DfMoney] = true;

    supportedMatrix[DfUInt32][DfInt32] = true;
    supportedMatrix[DfUInt32][DfUInt32] = true;
    supportedMatrix[DfUInt32][DfInt64] = true,
    supportedMatrix[DfUInt32][DfUInt64] = true;
    supportedMatrix[DfUInt32][DfFloat32] = true,
    supportedMatrix[DfUInt32][DfFloat64] = true;
    supportedMatrix[DfUInt32][DfMoney] = true;

    supportedMatrix[DfInt64][DfInt32] = true;
    supportedMatrix[DfInt64][DfUInt32] = true;
    supportedMatrix[DfInt64][DfInt64] = true,
    supportedMatrix[DfInt64][DfUInt64] = true;
    supportedMatrix[DfInt64][DfFloat32] = true,
    supportedMatrix[DfInt64][DfFloat64] = true;
    supportedMatrix[DfInt64][DfMoney] = true;

    supportedMatrix[DfUInt64][DfInt32] = true;
    supportedMatrix[DfUInt64][DfUInt32] = true;
    supportedMatrix[DfUInt64][DfInt64] = true,
    supportedMatrix[DfUInt64][DfUInt64] = true;
    supportedMatrix[DfUInt64][DfFloat32] = true;
    supportedMatrix[DfUInt64][DfFloat64] = true;
    supportedMatrix[DfUInt64][DfMoney] = true;

    supportedMatrix[DfFloat32][DfInt32] = true;
    supportedMatrix[DfFloat32][DfUInt32] = true;
    supportedMatrix[DfFloat32][DfInt64] = true;
    supportedMatrix[DfFloat32][DfUInt64] = true;
    supportedMatrix[DfFloat32][DfFloat32] = true;
    supportedMatrix[DfFloat32][DfFloat64] = true;

    supportedMatrix[DfFloat64][DfInt32] = true;
    supportedMatrix[DfFloat64][DfUInt32] = true;
    supportedMatrix[DfFloat64][DfInt64] = true;
    supportedMatrix[DfFloat64][DfUInt64] = true;
    supportedMatrix[DfFloat64][DfFloat32] = true;
    supportedMatrix[DfFloat64][DfFloat64] = true;

    supportedMatrix[DfBoolean][DfBoolean] = true;

    if (status != StatusOk) {
        destroyInternal();
    }
    return status;
}

void
XcalarEval::destroyInternal()
{
    registeredFns_.removeAll(&XcalarEvalRegisteredFn::del);
}

////////////////////////////////////////////////////////////////////////////////

//
// Management of registered functions.
//

void  // static
XcalarEval::deleteRegisteredFnWrapper(RefCount *refCount)
{
    get()->deleteRegisteredFn(refCount);
}

void
XcalarEvalRegisteredFn::del()
{
    refPut(&refCount);
}

void
XcalarEval::deleteRegisteredFn(RefCount *refCount)
{
    XcalarEvalRegisteredFn *registeredFn;

    registeredFn = RefEntry(refCount, typeof(*registeredFn), refCount);

    if (registeredFn->cleanupFn != NULL) {
        registeredFn->cleanupFn(registeredFn->fnDesc.context);
    }

    if (!registeredFn->isBuiltIn) {
        xSyslog(ModuleName,
                XlogDebug,
                "Freeing udf %s",
                registeredFn->fnDesc.fnName);
        XcalarEvalRegisteredFn::freeXcEvalRegisteredFn(registeredFn);
        registeredFn = NULL;
    } else {
        assert(registeredFn->fnDesc.category != FunctionCategoryUdf);
    }
}

XcalarEvalRegisteredFn *
XcalarEval::getRegisteredFnInternal(const char *fnName)
{
    XcalarEvalRegisteredFn *registeredFn = NULL;

    registeredFnsLock_.lock();
    if (InitTeardown::get()->getInitLevel() == InitLevel::ChildNode) {
        // childnodes don't have versioned udfs, strip out the version
        // name if there are any
        const char *delim;

        xSyslog(ModuleName,
                XlogDebug,
                "Xcalar Eval get registered function Internal fnName %s",
                fnName);

        if ((delim = rindex(fnName, UserDefinedFunction::UdfVersionDelim[0]))) {
            char newFnName[strlen(fnName)];
            unsigned jj = 0;
            bool inVersion = false;
            for (unsigned ii = 0; ii < strlen(fnName); ii++) {
                if (&fnName[ii] == delim) {
                    inVersion = true;
                } else if (fnName[ii] == ':') {
                    assert(inVersion);
                    inVersion = false;
                }

                if (!inVersion) {
                    newFnName[jj] = fnName[ii];
                    jj++;
                }
            }
            newFnName[jj] = '\0';

            xSyslog(ModuleName,
                    XlogDebug,
                    "Xcalar Eval get registered function Internal newFnName %s",
                    newFnName);

            registeredFn = registeredFns_.find(newFnName);
            assert(registeredFn != NULL);
        } else {
            registeredFn = registeredFns_.find(fnName);
        }
    } else {
        registeredFn = registeredFns_.find(fnName);
    }

    if (registeredFn != NULL) {
        // Critical to increment the refCount before releasing the slotLock.
        // This is because in the time between hashLookup and incrementing the
        // refCount, someone could have deleted this fn.
        refGet(&registeredFn->refCount);
    }

    registeredFnsLock_.unlock();
    return registeredFn;
}

XcalarEvalRegisteredFn *
XcalarEval::getRegisteredFn(const char *fnName, LibNsTypes::NsHandle *nsHandle)
{
    XcalarEvalRegisteredFn *registeredFn = NULL;
    UserDefinedFunction *udf = UserDefinedFunction::get();

    if (nsHandle != NULL) {
        // Init Handle.
        nsHandle->nsId = LibNsTypes::NsInvalidId;
    }

    registeredFn = getRegisteredFnInternal(fnName);
    if (registeredFn != NULL) {
        return registeredFn;
    }

    // This function could be an unversioned UDF in which case we would need
    // to get the version. Try getting a handle
    if (nsHandle == NULL) {
        // Caller does not think it is UDF since they have not provided with
        // return handle. So declare that the function cannot be found.
        return NULL;
    }

    // Get the function name versioned here.
    // Note that the fnName must always be absolute path, since XcalarEval's
    // registered functions are all absolute path - so no need to pass
    // a udfContainer to openUdfHandle().
    char *fnNameVersioned = NULL;
    Status status =
        udf->openUdfHandle((char *) fnName, NULL, nsHandle, &fnNameVersioned);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Xcalar Eval get registered function %s open handle failed:%s",
                fnName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Used the function name versioned to find the Xcalar Eval registration
    // function.
    registeredFn = getRegisteredFnInternal(fnNameVersioned);
    if (registeredFn == NULL) {
        // Didn't find the function.  Close the udf module.
        status = udf->closeUdfHandle((char *) fnName, *nsHandle);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xcalar Eval get registered function %s close handle "
                    "failed: %s",
                    fnName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        nsHandle->nsId = LibNsTypes::NsInvalidId;
    }

CommonExit:
    if (fnNameVersioned != NULL) {
        memFree(fnNameVersioned);
    }
    return registeredFn;
}

// The fnName passed to this routine may be a builtIn or a UDF. The caller
// of this routine must pass a NULL udfContainer, if the fnName can't be a
// UDF. If the fnName could be a UDF (but also maybe a built-in), a non-NULL
// udfContainer must be passed in so the UDF could be resolved.
//
// For a UDF, this routine resolves the function by searching a list of paths in
// which the function's definition / UDF module may reside.
//
// The policy is hard-coded in UserDefinedFunction::UdfPath which is an array of
// strings, with each element in the array being the dir-path in which the
// module name is searched - the search order is of course the same as the order
// of elements in the array. NOTE that for builtIn functions, the UdfPath is
// meaningless.
//

XcalarEvalRegisteredFn *
XcalarEval::getOrResolveRegisteredFn(const char *fnName,
                                     XcalarApiUdfContainer *udfContainer,
                                     LibNsTypes::NsHandle *nsHandle)
{
    Status status = StatusUnknown;
    UserDefinedFunction *udf = UserDefinedFunction::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen + 1];
    char prefixedSym[LibNsTypes::MaxPathNameLen + 1];
    XcalarEvalRegisteredFn *regFn = NULL;
    int udfPathNpaths = 0;
    int ii;

    //
    // XXX: in future, get UdfPath[] from the session instead of the global
    // UserDefinedFunction::UdfPath[]. Since there's no mechanism currently for
    // the user to modify the UdfPath[], a per-session UdfPath[] is overkill -
    // if the path is unmodifiable and the same for all sessions, might as well
    // put it in global state. So that's what we do.
    //

    if ((strchr(fnName, '/') != NULL) || udfContainer == NULL) {
        // If fnName is a fully resolved absolute name, this must be executing
        // in a XPU (since this routine is shared between XCE and XPU nodes,
        // this could occur). So just retrieve the function from the XPU's hash
        // table which must've been populated by XCE when dispatching the eval
        // string to XPU.  Alternatively, the caller knows the fnName couldn't
        // be a UDF, so udfContainer is NULL, and no need to search through
        // UdfPath[].
        regFn = getRegisteredFn(fnName, nsHandle);
    } else {
        udfPathNpaths = UserDefinedFunction::UdfPaths::Npaths;
        // relative name - resolve it by searching through UdfPath[]. Note this
        // is expected to work only in XCE node, not XPU
        assert(udfPathNpaths == (sizeof(udf->UdfPath) / sizeof(char *)));

        for (ii = 0; ii < udfPathNpaths; ii++) {
            switch (ii) {
            case UserDefinedFunction::UdfPaths::CurDir:
                assert(
                    strncmp(udf->UdfPath[ii],
                            UserDefinedFunction::UdfCurrentDirPath,
                            strlen(UserDefinedFunction::UdfCurrentDirPath)) ==
                    0);
                verify(
                    snprintf(prefixedSym, sizeof(prefixedSym), "%s", fnName) <
                    (int) sizeof(prefixedSym));
                break;
            default:
                // NOT current workbook; prefix the current path to fnName so
                // full path can be searched
                verify(snprintf(prefixedSym,
                                sizeof(prefixedSym),
                                "%s/%s",
                                udf->UdfPath[ii],
                                fnName) < (int) sizeof(prefixedSym));
                break;
            }

            status = udf->getUdfName(fullyQualName,
                                     sizeof(fullyQualName),
                                     prefixedSym,
                                     udfContainer,
                                     true);
            BailIfFailed(status);
            regFn = getRegisteredFn(fullyQualName, nsHandle);
            if (regFn != NULL) {
                break;
            }
        }
    }
    if (regFn != NULL) {
        status = StatusOk;  // found the function !
    }
CommonExit:
    assert(status == StatusOk || regFn == NULL);  // fail => regFn must be NULL
    return regFn;
}

void
XcalarEval::putFunction(XcalarEvalRegisteredFn *fn,
                        LibNsTypes::NsHandle *nsHandle)
{
    if (fn->isBuiltIn == true) {
        assert(nsHandle == NULL);
    } else {
        assert(nsHandle != NULL);
        if (nsHandle->nsId != LibNsTypes::NsInvalidId) {
            Status status =
                UserDefinedFunction::get()
                    ->closeUdfHandle((char *) fn->getFnName(), *nsHandle);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Xcalar Eval put function %s close handle failed:%s",
                        fn->getFnName(),
                        strGetFromStatus(status));
            }
        }
    }
    refPut(&fn->refCount);
}

XcalarEvalRegisteredFn *
XcalarEvalRegisteredFn::allocXcEvalRefFn()
{
    XcalarEvalRegisteredFn *regFn = NULL;
    void *ptr = mmap(NULL,
                     sizeof(XcalarEvalRegisteredFn),
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS,
                     -1,
                     0);
    if (ptr != MAP_FAILED) {
        regFn = new (ptr) XcalarEvalRegisteredFn;
    }
    return regFn;
}

void
XcalarEvalRegisteredFn::freeXcEvalRegisteredFn(XcalarEvalRegisteredFn *regFn)
{
    regFn->~XcalarEvalRegisteredFn();
    int ret = munmap(regFn, sizeof(XcalarEvalRegisteredFn));
    if (ret) {
        // In theory munmap can fail, almost always due to an invalid
        // address.  In malloc/free land this would be heap corruption, so
        // we treat this just like free and keep going (and log an error
        // since we can).
        xSyslog(ModuleName,
                XlogErr,
                "munmap failed with %s",
                strGetFromStatus(sysErrnoToStatus(errno)));
        assert(false && "Failed to unmap XdbMeta");
    }
}

// Tell eval module about the availability of a UDF function. Adding/removing
// registered functions is protected by the hash table's slot locks.
Status
XcalarEval::addUdf(XcalarEvalFnDesc *fnDesc,
                   XcalarEvalFn evalFn,
                   XcalarCleanupFn cleanupFn)
{
    Status status = StatusUnknown;
    XcalarEvalRegisteredFn *udfFn = NULL;
    XcalarEvalRegisteredFn *tmpUdfFn = NULL;

    assert(fnDesc != NULL);

    const size_t fnLen = strlen(fnDesc->fnName);
    for (unsigned ii = 0; ii < fnLen; ii++) {
        if (!isValidIdentifierChar(fnDesc->fnName[ii])) {
            // The function isn't likely to have an invalid name since it was
            // pulled from a compiled version of the UDF. Thus, blame the
            // module.
            return StatusUdfModuleInvalidName;
        }
    }

    udfFn = XcalarEvalRegisteredFn::allocXcEvalRefFn();
    if (udfFn == NULL) {
        return StatusNoMem;
    }

    verifyOk(::sparseMemCpy(&udfFn->fnDesc,
                            fnDesc,
                            sizeof(XcalarEvalFnDesc),
                            PageSize,
                            NULL));
    udfFn->fnType = XcalarFnTypeEval;
    udfFn->evalFn = evalFn;
    udfFn->cleanupFn = cleanupFn;
    udfFn->enforceArgCheck = false;
    udfFn->isBuiltIn = false;
    // adding default functions to make and delete context
    // we return the static context as is;
    // static context is set in UdfPyFunction::init
    udfFn->makeContextFn = [](void *a) { return a; };
    udfFn->delContextFn = [](void *a) {};

    status = StatusOk;
    refInit(&udfFn->refCount, XcalarEval::deleteRegisteredFnWrapper);
    registeredFnsLock_.lock();
    tmpUdfFn = registeredFns_.find(fnDesc->fnName);

    assert(tmpUdfFn == NULL);
    if (tmpUdfFn == NULL) {
        registeredFns_.insert(udfFn);
        udfFn = NULL;
    } else {
        // This should never ever happen.
        xSyslog(ModuleName,
                XlogErr,
                "Udf \"%s\" already exists",
                fnDesc->fnName);
        XcalarEvalRegisteredFn::freeXcEvalRegisteredFn(udfFn);
        udfFn = NULL;
        status = StatusUdfAlreadyExists;
    }
    registeredFnsLock_.unlock();
    return status;
}

// Deletes all registered functions that start with "<prefix>:". For deleting
// all UDF functions in a module. Returns number of deleted UDFs.
unsigned
XcalarEval::deleteUdfsWithPrefix(const char *prefix)
{
    unsigned count = 0;

    size_t prefixLen = strlen(prefix);
    char fullPrefix[prefixLen + 2];  // 2 for : and \0.

    verify(strlcpy(fullPrefix, prefix, sizeof(fullPrefix)) <
           sizeof(fullPrefix));
    fullPrefix[prefixLen] = ':';
    fullPrefix[prefixLen + 1] = '\0';

    registeredFnsLock_.lock();
    FnHashTable::iterator it = registeredFns_.begin();
    XcalarEvalRegisteredFn *udfFn = it.get();

    while (udfFn != NULL) {
        bool cont = false;
        it.next();

        if (strncmp(fullPrefix, udfFn->fnDesc.fnName, prefixLen + 1) != 0) {
            cont = true;
        } else if (udfFn->isBuiltIn) {
            assert(false);
            cont = true;
        }

        if (!cont) {
            verify(registeredFns_.remove(udfFn->getFnName()) != NULL);
            xSyslog(ModuleName,
                    XlogDebug,
                    "Deleting udf %s",
                    udfFn->fnDesc.fnName);

            refPut(&udfFn->refCount);
            count++;
        }

        udfFn = it.get();
    }

    registeredFnsLock_.unlock();

    return count;
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods dealing with generating and destroying ASTs. ASTs combine a
// traditional abstract syntax tree parsed from the eval string with a means of
// storing intermediate results.
//

// Parses an "identifier": an integer literal, constant (such as one
// representing a bool value), or variable.
Status
XcalarEval::parseIdentifier(XcalarEvalAstNode *regularNode,
                            const char *evalStr,
                            const char *tokenStart,
                            unsigned *numConstants,
                            unsigned *numVariables)
{
    Status status = StatusUnknown;

    if ((size_t)(evalStr - tokenStart + 1) >= sizeof(regularNode->token)) {
        status = StatusXcalarEvalTokenNameTooLong;
        goto CommonExit;
    }

    strlcpy(regularNode->token, tokenStart, evalStr - tokenStart + 1);
    if (isDigit(*tokenStart) || *tokenStart == '-') {
        regularNode->nodeType = XcalarEvalAstConstantNumber;
        (*numConstants)++;
    } else if (strcmp(regularNode->token, KeywordTrue) == 0 ||
               strcmp(regularNode->token, KeywordFalse) == 0 ||
               strcmp(regularNode->token, KeywordCapTrue) == 0 ||
               strcmp(regularNode->token, KeywordCapFalse) == 0) {
        regularNode->nodeType = XcalarEvalAstConstantBool;
        (*numConstants)++;
    } else if (strcasecmp(regularNode->token, KeywordNone) == 0 ||
               strcasecmp(regularNode->token, KeywordNull) == 0) {
        regularNode->nodeType = XcalarEvalAstConstantNull;
        (*numConstants)++;
    } else {
        // XXX(kkochis) Make sure variable identifiers are validated somewhere.
        regularNode->nodeType = XcalarEvalAstVariable;
        (*numVariables)++;
    }

    status = StatusOk;
CommonExit:
    return status;
}

bool
XcalarEval::isOperatorNode(XcalarEvalAstNodeType nodeType)
{
    return nodeType == XcalarEvalAstOperator ||
           nodeType == XcalarEvalAstGroupOperator ||
           nodeType == XcalarEvalAstScalarOperator;
}

// We must allow 'slashes' in the name since fully qualified UDF func name
// has slashes in the name
bool
XcalarEval::isValidIdentifierChar(char c)
{
    return isalnum(c) || c == ' ' || c == ':' || c == '-' || c == '_' ||
           c == OperatorsAggregateTag || c == '#' || c == '$' || c == '.' ||
           c == '<' || c == '>' || c == '/' || c == '@';
}

bool
XcalarEval::isGroupArg(XcalarEvalAstNodeType nodeType, const char *token)
{
    return nodeType == XcalarEvalAstGroupOperator ||
           (nodeType == XcalarEvalAstVariable &&
            *token != OperatorsAggregateTag);
}

Status
XcalarEval::parseOperatorArgs(char *evalStr,
                              XcalarFnType evalStrType,
                              XcalarApiUdfContainer *udfContainer,
                              char **evalStrOut,
                              XcalarEvalAstOperatorNode *operatorNodeParent,
                              unsigned *numVariables,
                              unsigned *numConstants,
                              unsigned *numIntermediates,
                              uint64_t *numFunc)
{
    const char *origEvalStr = evalStr;

    // A "token" is one of: integer literal, string literal, constant, variable,
    // or subexpression.

    typedef enum {
        Start,            // Just began.
        DoneWithToken,    // Just finished parsing complete token.
        LookingForToken,  // Looking for start of next token.
        PopulateToken,
        DblQuoteOpened,    // Parsing string literal ending in ".
        SingleQuoteOpened  // Parsing string literal ending in '.
    } State;

    State currentState = Start;
    uint64_t strTokenIdx = 0;
    const char *tokenStart = NULL;
    unsigned argc = 0;
    XcalarEvalAstOperatorNode *operatorNode = NULL;
    XcalarEvalAstNode *regularNode = NULL;
    Status status = StatusUnknown;

    while (*evalStr != '\0') {
        switch (currentState) {
        case Start:
            if (*evalStr == ')') {
                // Empty arg list.
                assert(argc == 0);
                status = StatusOk;
                goto CommonExit;
            }

        // Fall through
        case LookingForToken:
            switch (*evalStr) {
            case ' ':  // Ignore spaces
                break;
            case ',':
            case '(':
            case ')':
                status = StatusAstMalformedEvalString;
                goto CommonExit;
            case '"':  // Fall through
            case '\'':
                if (*evalStr == '"') {
                    currentState = DblQuoteOpened;
                } else {
                    assert(*evalStr == '\'');
                    currentState = SingleQuoteOpened;
                }
                strTokenIdx = 0;
                assert(regularNode == NULL);
                regularNode = new (std::nothrow) XcalarEvalAstNode();
                if (regularNode == NULL) {
                    status = StatusNoMem;
                    goto CommonExit;
                }

                break;
            default:
                if (!isValidIdentifierChar(*evalStr)) {
                    status = StatusAstMalformedEvalString;
                    goto CommonExit;
                }

                currentState = PopulateToken;
                tokenStart = evalStr;
                break;
            }
            break;

        case SingleQuoteOpened:
        // Fall through
        case DblQuoteOpened:
            assert(regularNode != NULL);
            if (strTokenIdx >= sizeof(regularNode->token)) {
                status = StatusXcalarEvalTokenNameTooLong;
                goto CommonExit;
            }

            // Only allow escapes in double quotes
            if (currentState == DblQuoteOpened && *evalStr == '\\') {
                char c = *(evalStr + 1);
                switch (c) {
                case '\\':
                case '"':
                    regularNode->token[strTokenIdx++] = c;
                    break;
                case 't':
                    regularNode->token[strTokenIdx++] = '\t';
                    break;
                case 'n':
                    regularNode->token[strTokenIdx++] = '\n';
                    break;
                case 'r':
                    regularNode->token[strTokenIdx++] = '\r';
                    break;
                default:
                    status = StatusAstMalformedEvalString;
                    goto CommonExit;
                }

                evalStr++;
                break;
            }

            if ((currentState == SingleQuoteOpened && *evalStr == '\'') ||
                (currentState == DblQuoteOpened && *evalStr == '"')) {
                // End of string.
                regularNode->token[strTokenIdx++] = '\0';

                regularNode->nodeType = XcalarEvalAstConstantStringLiteral;
                (*numConstants)++;
                // XXX: only a python function taking in an array can trigger.
                // all other cases are caught on creation
                if (unlikely(argc >= XcalarEvalMaxNumArgs)) {
                    status = StatusUdfFunctionTooManyParams;
                    goto CommonExit;
                }
                operatorNodeParent->arguments[argc] = regularNode;
                argc++;
                regularNode = NULL;  // Handed off to operatorNodeParent
                currentState = DoneWithToken;
            } else {
                regularNode->token[strTokenIdx++] = *evalStr;
            }
            break;

        case PopulateToken:
            switch (*evalStr) {
            case '(':
                assert(tokenStart != NULL);
                operatorNode = new (std::nothrow) XcalarEvalAstOperatorNode();
                if (operatorNode == NULL) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
                operatorNode->common.nodeType = XcalarEvalAstOperator;
                operatorNode->numArgs = 0;
                operatorNode->registeredFn = NULL;

                if ((size_t)(evalStr - tokenStart + 1) >=
                    sizeof(operatorNode->common.token)) {
                    status = StatusXcalarEvalTokenNameTooLong;
                    goto CommonExit;
                }

                (*numIntermediates)++;
                strlcpy(operatorNode->common.token,
                        tokenStart,
                        evalStr - tokenStart + 1);

                operatorNode->registeredFn =
                    getOrResolveRegisteredFn(operatorNode->common.token,
                                             udfContainer,
                                             &operatorNode->nsHandle);

                if (operatorNode->registeredFn == NULL) {
                    status = StatusAstNoSuchFunction;
                    xSyslogTxnBuf(ModuleName,
                                  XlogErr,
                                  "Could not find function %s",
                                  operatorNode->common.token);
                    goto CommonExit;
                }

                if (operatorNode->registeredFn->fnType == XcalarFnTypeAgg &&
                    evalStrType == XcalarFnTypeEval) {
                    status = StatusAggFnInClass1Ast;
                    goto CommonExit;
                }

                // check if parent expects singleton argument
                if (!operatorNode->registeredFn->fnDesc.isSingletonOutput &&
                    operatorNodeParent->registeredFn->fnDesc.argDescs[argc]
                        .isSingletonValue) {
                    status = StatusXdfInvalidArrayInput;
                    goto CommonExit;
                }

                if (operatorNode->registeredFn->makeContextFn != NULL) {
                    operatorNode->dynamicContext =
                        operatorNode->registeredFn->makeContextFn(
                            operatorNode->registeredFn->fnDesc.context);
                    BailIfNull(operatorNode->dynamicContext);
                }

                (*numFunc)++;

                status = parseOperatorArgs((char *) evalStr + 1,
                                           evalStrType,
                                           udfContainer,
                                           (char **) &evalStr,
                                           operatorNode,
                                           numVariables,
                                           numConstants,
                                           numIntermediates,
                                           numFunc);
                if (status != StatusOk) {
                    goto CommonExit;
                }
                // XXX: only a python function taking in an array can trigger.
                // all other cases are caught on creation
                if (unlikely(argc >= XcalarEvalMaxNumArgs)) {
                    status = StatusUdfFunctionTooManyParams;
                    goto CommonExit;
                }
                operatorNodeParent->arguments[argc] =
                    (XcalarEvalAstNode *) operatorNode;
                argc++;
                operatorNode = NULL;  // Handed off to operatorNodeParent
                currentState = DoneWithToken;
                tokenStart = NULL;
                continue;

            case ',':
                assert(tokenStart != NULL);
                regularNode = new (std::nothrow) XcalarEvalAstNode();
                if (regularNode == NULL) {
                    status = StatusNoMem;
                    goto CommonExit;
                }

                status = parseIdentifier(regularNode,
                                         evalStr,
                                         tokenStart,
                                         numConstants,
                                         numVariables);
                if (status != StatusOk) {
                    goto CommonExit;
                }
                // XXX: only a python function taking in an array can trigger.
                // all other cases are caught on creation
                if (unlikely(argc >= XcalarEvalMaxNumArgs)) {
                    status = StatusUdfFunctionTooManyParams;
                    goto CommonExit;
                }
                operatorNodeParent->arguments[argc] = regularNode;
                argc++;
                regularNode = NULL;  // Handed off to operatorParent
                currentState = LookingForToken;
                break;

            case ')':
                assert(tokenStart != NULL);
                regularNode = new (std::nothrow) XcalarEvalAstNode();
                if (regularNode == NULL) {
                    status = StatusNoMem;
                    goto CommonExit;
                }

                status = parseIdentifier(regularNode,
                                         evalStr,
                                         tokenStart,
                                         numConstants,
                                         numVariables);
                if (status != StatusOk) {
                    goto CommonExit;
                }
                // XXX: only a python function taking in an array can trigger.
                // all other cases are caught on creation
                if (unlikely(argc >= XcalarEvalMaxNumArgs)) {
                    status = StatusUdfFunctionTooManyParams;
                    goto CommonExit;
                }
                operatorNodeParent->arguments[argc] = regularNode;
                argc++;
                regularNode = NULL;  // Handed off to operatorNodeParent
                status = StatusOk;
                goto CommonExit;

            default:
                if (tokenStart == NULL) {
                    tokenStart = evalStr;
                }
                break;
            }
            break;

        case DoneWithToken:
            switch (*evalStr) {
            case ',':
                currentState = LookingForToken;
                break;
            case ')':
                status = StatusOk;
                goto CommonExit;
            case ' ':  // Ignore spaces
                break;
            default:
                status = StatusAstMalformedEvalString;
                goto CommonExit;
            }
            break;
        }
        evalStr++;
    }

    *evalStrOut = evalStr;
    status = StatusAstMalformedEvalString;

CommonExit:
    if (status == StatusOk) {
        assert(operatorNodeParent->registeredFn != NULL);
        if (operatorNodeParent->registeredFn->enforceArgCheck) {
            // The above arg count check can fail if there are optional
            // or variable arguments.
            unsigned minArgCount = 0, maxArgCount = 0;
            for (int ii = 0;
                 ii < operatorNodeParent->registeredFn->fnDesc.numArgs;
                 ii++) {
                XcalarEvalFnDesc::ArgDesc *argDesc =
                    &operatorNodeParent->registeredFn->fnDesc.argDescs[ii];

                switch (argDesc->argType) {
                case RequiredArg:
                    minArgCount++;
                    maxArgCount++;
                    break;
                case OptionalArg:
                    maxArgCount++;
                    break;
                case VariableArg:
                    minArgCount += argDesc->minArgs;
                    maxArgCount += argDesc->maxArgs;
                    break;
                default:
                    assert(0);
                }
            }
            if (argc < minArgCount || argc > maxArgCount) {
                status = StatusAstWrongNumberOfArgs;
                goto CommonExit;
            }
        }

        *evalStrOut = evalStr + 1;
        operatorNodeParent->numArgs = argc;
    } else {
        if (regularNode != NULL) {
            delete regularNode;
            regularNode = NULL;
        }

        if (operatorNode != NULL) {
            assert(operatorNode->numArgs == 0);
            freeAstNode(operatorNode);
            operatorNode = NULL;
        }

        // On failure, this function must free any additional nodes it
        // allocated. Don't leave that up to the caller.
        if (argc > 0) {
            for (unsigned ii = 0; ii < argc; ii++) {
                if (operatorNodeParent->arguments[ii]->nodeType ==
                    XcalarEvalAstOperator) {
                    freeAstNode((XcalarEvalAstOperatorNode *)
                                    operatorNodeParent->arguments[ii]);
                } else {
                    delete operatorNodeParent->arguments[ii];
                }
                operatorNodeParent->arguments[ii] = NULL;
            }
            argc = 0;
        }

        if (status == StatusXcalarEvalTokenNameTooLong) {
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "Failed to parse %s since %s Max: %d",
                          origEvalStr,
                          strGetFromStatus(status),
                          XcalarEvalMaxTokenLen);
        } else {
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "Failed to parse %s since %s",
                          origEvalStr,
                          strGetFromStatus(status));
        }
    }

    assert(regularNode == NULL);
    return status;
}

Status
XcalarEval::fixupConstant(XcalarEvalAstNodeType nodeType,
                          XcalarEvalAstNode *regularNode,
                          Scalar *constants[],
                          unsigned *constantIdx)
{
    Status status = StatusUnknown;
    Scalar *scalar;

    assert(constants != NULL);
    assert(constantIdx != NULL);

    switch (nodeType) {
    case XcalarEvalAstConstantStringLiteral: {
        DfFieldValue fieldVal;

        regularNode->varContent = &constants[(*constantIdx)++];
        assert(*(regularNode->varContent) == NULL);

        fieldVal.stringVal.strActual = regularNode->token;
        fieldVal.stringVal.strSize = strlen(regularNode->token) + 1;
        scalar = Scalar::allocScalar(DfMaxFieldValueSize);
        if (scalar == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        *(regularNode->varContent) = scalar;

        status = scalar->setValue(fieldVal, DfString);
        assert(status == StatusOk);

        break;
    }

    case XcalarEvalAstConstantBool: {
        DfFieldValue fieldVal;

        regularNode->varContent = &constants[(*constantIdx)++];
        assert(*(regularNode->varContent) == NULL);

        scalar = Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
        if (scalar == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        *(regularNode->varContent) = scalar;

        if (strcmp(regularNode->token, KeywordTrue) == 0 ||
            strcmp(regularNode->token, KeywordCapTrue) == 0) {
            fieldVal.boolVal = true;
        } else {
            assert(strcmp(regularNode->token, KeywordFalse) == 0 ||
                   strcmp(regularNode->token, KeywordCapFalse) == 0);
            fieldVal.boolVal = false;
        }

        status = scalar->setValue(fieldVal, DfBoolean);
        assert(status == StatusOk);

        break;
    }

    case XcalarEvalAstConstantNull: {
        DfFieldValue fieldVal;

        regularNode->varContent = &constants[(*constantIdx)++];
        assert(*(regularNode->varContent) == NULL);

        scalar = Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
        if (scalar == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        *(regularNode->varContent) = scalar;

        status = scalar->setValue(fieldVal, DfNull);
        assert(status == StatusOk);

        break;
    }

    case XcalarEvalAstConstantNumber: {
        DfFieldValue fieldVal;
        DfFieldType argType;

        regularNode->varContent = &constants[(*constantIdx)++];
        assert(*(regularNode->varContent) == NULL);

        scalar = Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
        if (scalar == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        scalar->fieldNumValues = 1;
        *(regularNode->varContent) = scalar;

        DfFieldValue tokenFieldVal;
        tokenFieldVal.stringVal.strActual = regularNode->token;
        tokenFieldVal.stringVal.strSize = strlen(regularNode->token) + 1;

        // Must resolve which type of 'number' this is,
        // if it has any decimals then it's a float, otherwise treat as int
        if (strstr(regularNode->token, ".")) {
            status = DataFormat::convertValueType(DfFloat64,
                                                  DfString,
                                                  &tokenFieldVal,
                                                  &fieldVal,
                                                  0,
                                                  BaseCanonicalForm);
            BailIfFailed(status);

            argType = DfFloat64;
        } else {
            status = DataFormat::convertValueType(DfInt64,
                                                  DfString,
                                                  &tokenFieldVal,
                                                  &fieldVal,
                                                  0,
                                                  BaseCanonicalForm);
            BailIfFailed(status);

            argType = DfInt64;
        }

        status = scalar->setValue(fieldVal, argType);
        BailIfFailed(status);

        break;
    }

    default:
        assert(0);
    }

CommonExit:
    return status;
}

// In first pass, we do the following:
// 1) find unique variables
// 2) distinguish between scalarOperators and groupOperators (scalarOperators
// are operators whose return value is scalar, and groupOperators are
// operators whose return value is a group)
// 3) set valueArrayIdx for uniqueVariables
Status
XcalarEval::fixupClass2AstFirstPass(
    XcalarEvalAstOperatorNode *operatorNodeParent,
    unsigned *numUniqueVariables,
    char *variableNames[],
    unsigned *numScalarIntermediates,
    unsigned *numGroupIntermediates,
    unsigned *numScalarVariables)
{
    Status status = StatusUnknown;
    unsigned ii, jj;
    XcalarEvalAstNode *regularNode;
    XcalarEvalAstOperatorNode *operatorNode;
    bool hasGroupArg = false;

    for (ii = 0; ii < operatorNodeParent->numArgs; ii++) {
        switch (operatorNodeParent->arguments[ii]->nodeType) {
        case XcalarEvalAstOperator:
            operatorNode =
                (XcalarEvalAstOperatorNode *) operatorNodeParent->arguments[ii];
            status = fixupClass2AstFirstPass(operatorNode,
                                             numUniqueVariables,
                                             variableNames,
                                             numScalarIntermediates,
                                             numGroupIntermediates,
                                             numScalarVariables);
            if (status != StatusOk) {
                goto CommonExit;
            }

            break;

        case XcalarEvalAstVariable: {
            bool variableNameFound = false;

            assert(variableNames != NULL);

            regularNode = operatorNodeParent->arguments[ii];

            if (strlen(regularNode->token) > DfMaxFieldNameLen) {
                status = StatusVariableNameTooLong;
                goto CommonExit;
            }

            for (jj = 0; jj < *numUniqueVariables; jj++) {
                if (strcmp(variableNames[jj], regularNode->token) == 0) {
                    variableNameFound = true;
                    regularNode->valueArrayIdx = jj;
                    break;
                }
            }

            if (!variableNameFound) {
                variableNames[*numUniqueVariables] = regularNode->token;
                regularNode->valueArrayIdx = *numUniqueVariables;
                (*numUniqueVariables)++;
                if (*regularNode->token == OperatorsAggregateTag) {
                    (*numScalarVariables)++;
                }
            }

            break;
        }

        // We should not be seeing either one of the 2 yet
        case XcalarEvalAstGroupOperator:
        case XcalarEvalAstScalarOperator:
            assert(0);

        default:
            // Do nothing
            break;
        }
    }

    status = StatusOk;

    assert(operatorNodeParent->common.nodeType == XcalarEvalAstOperator);
    hasGroupArg = false;
    // There are 2 kinds of functions:
    // f: scalar -> scalar
    // g: group -> scalar
    // and 4 possible cases, f(scalar), f(group), g(scalar), g(group).
    // g(scalar) can be converted to g(group) by treating the scalar
    // as a group of size 1. Hence, in the 3 cases, g(scalar), g(group),
    // and f(scalar), the output is guaranteed to be a scalar.
    // Only when f(group) do we have to distribute f to each record in group.
    // More formally, we introduce h: (scalar -> scalar) x group -> group,
    // where h(f, group) applies f to each record in the group to get another
    // group
    if (operatorNodeParent->registeredFn->fnType == XcalarFnTypeEval) {
        for (ii = 0; ii < operatorNodeParent->numArgs; ii++) {
            assert(operatorNodeParent->arguments[ii]->nodeType !=
                   XcalarEvalAstOperator);
            hasGroupArg =
                isGroupArg(operatorNodeParent->arguments[ii]->nodeType,
                           operatorNodeParent->arguments[ii]->token);
            if (hasGroupArg) {
                break;
            }
        }
    }

    if (hasGroupArg) {
        (*numGroupIntermediates)++;
        operatorNodeParent->common.nodeType = XcalarEvalAstGroupOperator;
    } else {
        (*numScalarIntermediates)++;
        operatorNodeParent->common.nodeType = XcalarEvalAstScalarOperator;
    }

CommonExit:
    return status;
}

Status
XcalarEval::fixupArgumentPointersClass1(
    XcalarEvalAstOperatorNode *operatorNodeParent,  // current node
    unsigned *scalarVariableIdx,
    unsigned *constantIdx,
    unsigned *scalarIntermediateIdx,
    XcalarEvalScalarVariable *scalarVariables,
    Scalar *constants[],
    Scalar *scalarIntermediates[])
{
    unsigned ii;
    Status status = StatusUnknown;
    XcalarEvalAstNode *regularNode;  // astVariableNode
    XcalarEvalAstOperatorNode *operatorNode;
    operatorNodeParent->common.varContent =
        &scalarIntermediates[(*scalarIntermediateIdx)++];

    for (ii = 0; ii < operatorNodeParent->numArgs; ii++) {
        switch (operatorNodeParent->arguments[ii]->nodeType) {
        case XcalarEvalAstOperator:
            operatorNode =
                (XcalarEvalAstOperatorNode *) operatorNodeParent->arguments[ii];
            status = fixupArgumentPointersClass1(operatorNode,
                                                 scalarVariableIdx,
                                                 constantIdx,
                                                 scalarIntermediateIdx,
                                                 scalarVariables,
                                                 constants,
                                                 scalarIntermediates);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;

        case XcalarEvalAstVariable:
            regularNode = operatorNodeParent->arguments[ii];

            if (strlen(regularNode->token) > DfMaxFieldNameLen) {
                status = StatusVariableNameTooLong;
                goto CommonExit;
            }

            scalarVariables[(*scalarVariableIdx)].content = NULL;
            scalarVariables[(*scalarVariableIdx)].variableName =
                regularNode->token;
            regularNode->varContent =
                &(scalarVariables[(*scalarVariableIdx)].content);
            (*scalarVariableIdx)++;
            break;

        case XcalarEvalAstConstantStringLiteral:
        case XcalarEvalAstConstantBool:
        case XcalarEvalAstConstantNumber:
        case XcalarEvalAstConstantNull:
            status = fixupConstant(operatorNodeParent->arguments[ii]->nodeType,
                                   operatorNodeParent->arguments[ii],
                                   constants,
                                   constantIdx);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;

        case XcalarEvalAstGroupOperator:
        case XcalarEvalAstScalarOperator:
            // Should not see class 2 AST nodes here
            assert(0);
            break;
        }
    }

    status = StatusOk;
CommonExit:
    // Don't have to free up scalar because if fixupArgumentPointersClass1 fail,
    // then our parent function will walk through the constants and variables
    // array to clean up
    return status;
}

Status
XcalarEval::escapeToken(const char *token, char *tokenEscaped, size_t size)
{
    unsigned jj = 0;
    for (unsigned ii = 0; ii < strlen(token); ii++) {
        switch (token[ii]) {
        case '\\':
        case '"':
            if (jj + 2 >= size) {
                return StatusOverflow;
            }

            tokenEscaped[jj] = '\\';
            jj++;
            tokenEscaped[jj] = token[ii];
            jj++;

            break;
        case '\t':
            if (jj + 2 >= size) {
                return StatusOverflow;
            }

            tokenEscaped[jj] = '\\';
            jj++;

            tokenEscaped[jj] = 't';
            jj++;

            break;
        case '\n':
            if (jj + 2 >= size) {
                return StatusOverflow;
            }

            tokenEscaped[jj] = '\\';
            jj++;

            tokenEscaped[jj] = 'n';
            jj++;

            break;

        default:
            if (jj + 1 >= size) {
                return StatusOverflow;
            }

            tokenEscaped[jj] = token[ii];
            jj++;
            break;
        }
    }

    if (jj == size) {
        return StatusOverflow;
    } else {
        tokenEscaped[jj] = '\0';
    }

    return StatusOk;
}

Status
XcalarEval::convertAstNames(XcalarEvalAstNodeType nodeType,
                            void *nodeIn,
                            unsigned numRenameCols,
                            struct XcalarApiRenameMap *renameMap,
                            ConvertEvalStringFlags flags)
{
    Status status = StatusOk;

    switch (nodeType) {
    case XcalarEvalAstOperator:
    case XcalarEvalAstGroupOperator:
    case XcalarEvalAstScalarOperator: {
        XcalarEvalAstOperatorNode *node = (XcalarEvalAstOperatorNode *) nodeIn;

        for (unsigned ii = 0; ii < node->numArgs; ii++) {
            status = convertAstNames(node->arguments[ii]->nodeType,
                                     (void *) node->arguments[ii],
                                     numRenameCols,
                                     renameMap,
                                     flags);
            BailIfFailed(status);
        }
        break;
    }
    case XcalarEvalAstVariable: {
        XcalarEvalAstNode *node = (XcalarEvalAstNode *) nodeIn;

        // reverse any renames that were applied to this eval
        for (unsigned ii = 0; ii < numRenameCols; ii++) {
            if (strcmp(node->token, renameMap[ii].newName) == 0) {
                strlcpy(node->token,
                        renameMap[ii].oldName,
                        sizeof(node->token));
                break;
            }
        }

        if (flags & ToImmediate) {
            if (strstr(node->token, DfFatptrPrefixDelimiter)) {
                DataFormat::replaceFatptrPrefixDelims(node->token);
                status = DataFormat::escapeNestedDelim(node->token,
                                                       sizeof(node->token),
                                                       NULL);
                BailIfFailed(status);
            }
        } else if (flags & FromImmediate) {
            if (strstr(node->token, DfFatptrPrefixDelimiterReplaced)) {
                DataFormat::revertFatptrPrefixDelims(node->token);
                DataFormat::unescapeNestedDelim(node->token);
            }
        }
        break;
    }
    default:
        break;
    }
CommonExit:
    return status;
}

Status
XcalarEval::fixupTypeSensitiveFns(XcalarEvalAstNodeType nodeType,
                                  void *nodeIn,
                                  XdbMeta *xdbMeta)
{
    Status status;

    status = convertIfConditions(nodeType, nodeIn, xdbMeta);
    BailIfFailed(status);

    status = convertMinMaxFns(nodeType, nodeIn, xdbMeta);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
XcalarEval::convertMinMaxFns(XcalarEvalAstNodeType nodeType,
                             void *nodeIn,
                             XdbMeta *xdbMeta)

{
    Status status = StatusOk;

    switch (nodeType) {
    case XcalarEvalAstScalarOperator:
    case XcalarEvalAstGroupOperator:
    case XcalarEvalAstOperator: {
        XcalarEvalAstOperatorNode *node = (XcalarEvalAstOperatorNode *) nodeIn;
        char fnName[XcalarEvalMaxFnNameLen];
        status =
            strStrlcpy(fnName, node->registeredFn->getFnName(), sizeof(fnName));
        BailIfFailed(status);

        for (unsigned ii = 0; ii < node->numArgs; ii++) {
            status = convertMinMaxFns(node->arguments[ii]->nodeType,
                                      (void *) node->arguments[ii],
                                      xdbMeta);
            BailIfFailed(status);
        }

        if (strcmp(fnName, "max") == 0 || strcmp(fnName, "min") == 0) {
            DfFieldType type = DfScalarObj;

            // check the first argument for the type
            switch (node->arguments[0]->nodeType) {
            case XcalarEvalAstVariable: {
                for (unsigned jj = 0;
                     jj < xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
                     jj++) {
                    if (strcmp(node->arguments[0]->token,
                               xdbMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                        type =
                            xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(
                                jj);
                        break;
                    }
                }
                break;
            }
            case XcalarEvalAstScalarOperator:
            case XcalarEvalAstGroupOperator:
            case XcalarEvalAstOperator: {
                XcalarEvalAstOperatorNode *tmpNode =
                    (XcalarEvalAstOperatorNode *) node->arguments[0];

                type = tmpNode->registeredFn->fnDesc.outputType;
                break;
            }
            case XcalarEvalAstConstantStringLiteral: {
                type = DfString;
                break;
            }
            case XcalarEvalAstConstantBool: {
                type = DfBoolean;
                break;
            }
            case XcalarEvalAstConstantNumber: {
                if (strchr(node->arguments[0]->token, '.') != NULL) {
                    type = DfFloat64;
                } else {
                    type = DfInt64;
                }
                break;
            }
            default:
                break;
            }

            if (type == DfString || type == DfInt64 || type == DfFloat64 ||
                type == DfTimespec || type == DfBoolean || type == DfMoney) {
                switch (type) {
                case DfString:
                    strcat(fnName, "String");
                    break;
                case DfTimespec:
                    strcat(fnName, "Timestamp");
                    break;
                case DfBoolean:
                case DfInt64:
                    strcat(fnName, "Integer");
                    break;
                case DfFloat64:
                    strcat(fnName, "Float");
                    break;
                case DfMoney:
                    strcat(fnName, "Numeric");
                    break;
                default:
                    break;
                }

                putFunction(node->registeredFn, NULL);
                node->registeredFn =
                    getOrResolveRegisteredFn(fnName, NULL, &node->nsHandle);
            }
        }
        break;
    }
    default:
        break;
    }
CommonExit:
    return status;
}

Status
XcalarEval::convertIfConditions(XcalarEvalAstNodeType nodeType,
                                void *nodeIn,
                                XdbMeta *xdbMeta)

{
    Status status = StatusOk;

    switch (nodeType) {
    case XcalarEvalAstScalarOperator:
    case XcalarEvalAstGroupOperator:
    case XcalarEvalAstOperator: {
        XcalarEvalAstOperatorNode *node = (XcalarEvalAstOperatorNode *) nodeIn;

        for (unsigned ii = 0; ii < node->numArgs; ii++) {
            status = convertIfConditions(node->arguments[ii]->nodeType,
                                         (void *) node->arguments[ii],
                                         xdbMeta);
            BailIfFailed(status);
        }

        if (strcmp(node->registeredFn->getFnName(), "if") == 0) {
            DfFieldType type = DfFloat64;
            DfFieldType elseType = DfFloat64;

            // check the first argument for the type
            switch (node->arguments[1]->nodeType) {
            case XcalarEvalAstVariable: {
                for (unsigned jj = 0;
                     jj < xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
                     jj++) {
                    if (strcmp(node->arguments[1]->token,
                               xdbMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                        type =
                            xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(
                                jj);
                        break;
                    }
                }
                break;
            }
            case XcalarEvalAstScalarOperator:
            case XcalarEvalAstGroupOperator:
            case XcalarEvalAstOperator: {
                XcalarEvalAstOperatorNode *tmpNode =
                    (XcalarEvalAstOperatorNode *) node->arguments[1];

                type = tmpNode->registeredFn->fnDesc.outputType;
                break;
            }
            case XcalarEvalAstConstantStringLiteral: {
                type = DfString;
                break;
            }
            case XcalarEvalAstConstantBool: {
                type = DfBoolean;
                break;
            }
            case XcalarEvalAstConstantNumber: {
                if (strchr(node->arguments[1]->token, '.') == NULL) {
                    type = DfInt64;
                }
                break;
            }
            default:
                break;
            }

            // check the second argument for the type
            switch (node->arguments[2]->nodeType) {
            case XcalarEvalAstVariable: {
                for (unsigned jj = 0;
                     jj < xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
                     jj++) {
                    if (strcmp(node->arguments[2]->token,
                               xdbMeta->kvNamedMeta.valueNames_[jj]) == 0) {
                        elseType =
                            xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(
                                jj);
                        break;
                    }
                }
                break;
            }
            case XcalarEvalAstScalarOperator:
            case XcalarEvalAstGroupOperator:
            case XcalarEvalAstOperator: {
                XcalarEvalAstOperatorNode *tmpNode =
                    (XcalarEvalAstOperatorNode *) node->arguments[2];

                elseType = tmpNode->registeredFn->fnDesc.outputType;
                break;
            }
            case XcalarEvalAstConstantStringLiteral: {
                elseType = DfString;
                break;
            }
            case XcalarEvalAstConstantBool: {
                elseType = DfBoolean;
                break;
            }
            case XcalarEvalAstConstantNumber: {
                if (strchr(node->arguments[2]->token, '.') == NULL) {
                    elseType = DfInt64;
                }
                break;
            }
            default:
                break;
            }

            // If it is String or Int, change the function accordingly
            if (type == DfString || elseType == DfString) {
                putFunction(node->registeredFn, NULL);
                node->registeredFn =
                    getOrResolveRegisteredFn("ifStr", NULL, &node->nsHandle);
            } else if ((type == DfInt64 || type == DfBoolean) &&
                       (elseType == DfInt64 || elseType == DfBoolean)) {
                putFunction(node->registeredFn, NULL);
                node->registeredFn =
                    getOrResolveRegisteredFn("ifInt", NULL, &node->nsHandle);
            } else if (type == DfTimespec || elseType == DfTimespec) {
                putFunction(node->registeredFn, NULL);
                node->registeredFn = getOrResolveRegisteredFn("ifTimestamp",
                                                              NULL,
                                                              &node->nsHandle);
            } else if (type == DfMoney || elseType == DfMoney) {
                putFunction(node->registeredFn, NULL);
                node->registeredFn = getOrResolveRegisteredFn("ifNumeric",
                                                              NULL,
                                                              &node->nsHandle);
            }
        }
        break;
    }
    default:
        break;
    }
CommonExit:
    return status;
}

Status
XcalarEval::convertEvalString(char *evalString,
                              XcalarApiUdfContainer *udfContainer,
                              size_t bufSize,
                              XcalarFnType evalStrType,
                              unsigned numRenameCols,
                              struct XcalarApiRenameMap *renameMap,
                              ConvertEvalStringFlags flags)
{
    Status status;
    XcalarEvalAstCommon ast;
    unsigned numVariables;
    unsigned cur = 0;
    bool astCreated = false;

    status = parseEvalStr(evalString,
                          evalStrType,
                          udfContainer,
                          &ast,
                          NULL,
                          &numVariables,
                          NULL,
                          NULL);
    BailIfFailed(status);
    astCreated = true;

    status = convertAstNames(XcalarEvalAstOperator,
                             ast.rootNode,
                             numRenameCols,
                             renameMap,
                             flags);
    BailIfFailed(status);

    status = reverseParseAst(XcalarEvalAstOperator,
                             ast.rootNode,
                             evalString,
                             cur,
                             bufSize,
                             XcalarEval::NoFlags);
    BailIfFailed(status);

CommonExit:
    if (astCreated) {
        freeCommonAst(&ast);
        astCreated = false;
    }

    return status;
}

Status
XcalarEval::makeUDFRelativeInEvalString(char *evalString,
                                        size_t bufSize,
                                        XcalarFnType evalStrType,
                                        XcalarApiUdfContainer *udfContainer,
                                        ConvertEvalStringFlags flags)
{
    Status status;
    XcalarEvalAstCommon ast;
    unsigned numVariables;
    unsigned cur = 0;
    bool astCreated = false;

    status = parseEvalStr(evalString,
                          evalStrType,
                          udfContainer,
                          &ast,
                          NULL,
                          &numVariables,
                          NULL,
                          NULL);
    BailIfFailed(status);
    astCreated = true;

    status = reverseParseAst(XcalarEvalAstOperator,
                             ast.rootNode,
                             evalString,
                             cur,
                             bufSize,
                             XcalarEval::UDFToRelative);
    BailIfFailed(status);

CommonExit:
    if (astCreated) {
        freeCommonAst(&ast);
        astCreated = false;
    }

    return status;
}

// Parses ast into eval string
Status
XcalarEval::reverseParseAst(XcalarEvalAstNodeType nodeType,
                            void *nodeIn,
                            char *buf,
                            unsigned &cur,
                            size_t &bytesRemaining,
                            ConvertEvalStringFlags flags)
{
    Status status = StatusOk;
    size_t ret;
    char *udfModuleNameDup = NULL;
    const char *udfModuleName;
    const char *udfModuleVersion;
    const char *udfFunctionName;
    char udfModuleFunctionName[XcalarApiMaxUdfModuleNameLen + 1];
    char parserFnNameCopy[UdfVersionedFQFname];

    switch (nodeType) {
    case XcalarEvalAstOperator:
    case XcalarEvalAstGroupOperator:
    case XcalarEvalAstScalarOperator: {
        XcalarEvalAstOperatorNode *node = (XcalarEvalAstOperatorNode *) nodeIn;
        assert(node->registeredFn);

        char *fnName = node->registeredFn->fnDesc.fnName;

        // we are a function
        if ((flags & UDFToRelative) && fnName[0] != '\0' &&
            strchr(fnName, '/') != NULL) {
            // Copy function name as parseFunctionName modifies what is
            // passed to it.
            strlcpy(parserFnNameCopy, fnName, sizeof(parserFnNameCopy));

            status = UserDefinedFunction::parseFunctionName(parserFnNameCopy,
                                                            &udfModuleName,
                                                            &udfModuleVersion,
                                                            &udfFunctionName);
            BailIfFailed(status);

            udfModuleNameDup = strAllocAndCopy(udfModuleName);
            BailIfNull(udfModuleNameDup);

            snprintf(udfModuleFunctionName,
                     sizeof(udfModuleFunctionName),
                     "%s:%s",
                     basename(udfModuleNameDup),
                     udfFunctionName);

            ret = snprintf(&buf[cur],
                           bytesRemaining,
                           "%s(",
                           udfModuleFunctionName);
        } else {
            ret = snprintf(&buf[cur],
                           bytesRemaining,
                           "%s(",
                           node->registeredFn->fnDesc.fnName);
        }
        if (ret >= bytesRemaining) {
            return StatusEvalStringTooLong;
        }
        cur += ret;
        bytesRemaining -= ret;

        if (node->numArgs == 0) {
            ret = snprintf(&buf[cur], bytesRemaining, ")");
            cur += ret;
            bytesRemaining -= ret;
        }

        for (unsigned ii = 0; ii < node->numArgs; ii++) {
            status = reverseParseAst(node->arguments[ii]->nodeType,
                                     (void *) node->arguments[ii],
                                     buf,
                                     cur,
                                     bytesRemaining,
                                     flags);
            BailIfFailed(status);

            if (ii < node->numArgs - 1) {
                ret = snprintf(&buf[cur], bytesRemaining, ",");
            } else {
                ret = snprintf(&buf[cur], bytesRemaining, ")");
            }

            if (ret >= bytesRemaining) {
                return StatusEvalStringTooLong;
            }
            cur += ret;
            bytesRemaining -= ret;
        }
        break;
    }
    case XcalarEvalAstConstantNumber:
    case XcalarEvalAstVariable:
    case XcalarEvalAstConstantBool:
    case XcalarEvalAstConstantNull: {
        XcalarEvalAstNode *node = (XcalarEvalAstNode *) nodeIn;
        ret = snprintf(&buf[cur], bytesRemaining, "%s", node->token);
        if (ret >= bytesRemaining) {
            return StatusEvalStringTooLong;
        }

        cur += ret;
        bytesRemaining -= ret;
        break;
    }
    case XcalarEvalAstConstantStringLiteral: {
        XcalarEvalAstNode *node = (XcalarEvalAstNode *) nodeIn;

        // we need to do escaping
        char tokenEscaped[XcalarEvalMaxTokenLen];
        status = escapeToken(node->token, tokenEscaped, sizeof(tokenEscaped));
        BailIfFailed(status);

        ret = snprintf(&buf[cur], bytesRemaining, "\"%s\"", tokenEscaped);
        if (ret >= bytesRemaining) {
            return StatusEvalStringTooLong;
        }

        cur += ret;
        bytesRemaining -= ret;
        break;
    }
    }

CommonExit:
    if (udfModuleNameDup) {
        memFree(udfModuleNameDup);
        udfModuleNameDup = NULL;
    }
    return status;
}

// Parses eval string into AST.
Status
XcalarEval::parseEvalStr(const char *evalStr,
                         XcalarFnType evalStrType,
                         XcalarApiUdfContainer *udfContainer,
                         XcalarEvalAstCommon *ast,
                         unsigned *numIntermediatesOut,
                         unsigned *numVariablesOut,
                         unsigned *numConstantsOut,
                         uint64_t *numFuncOut)
{
    const char *origEvalStr = evalStr;
    unsigned numIntermediates, numVariables, numConstants, ii;
    XcalarEvalAstOperatorNode *operatorNode = NULL;
    const char *tokenStart = NULL;
    Status status = StatusUnknown;
    bool allocedConstants = false;
    uint64_t numFunc = 0;
    bool parseCalled = false;

    assert(ast != NULL);
    assert(evalStr != NULL);

    if (strlen(evalStr) >= XcalarApiMaxEvalStringLen) {
        status = StatusEvalStringTooLong;
        goto CommonExit;
    }

    operatorNode = new (std::nothrow) XcalarEvalAstOperatorNode();
    if (operatorNode == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(operatorNode, sizeof(*operatorNode));
    operatorNode->common.nodeType = XcalarEvalAstOperator;
    operatorNode->registeredFn = NULL;

    numVariables = numConstants = 0;
    numIntermediates = 1;

    while (*evalStr != '\0') {
        switch (*evalStr) {
        case ' ':
            // Ignore spaces
            break;
        case '(':
            if (operatorNode->registeredFn != NULL) {
                status = StatusAstMalformedEvalString;
                goto CommonExit;
            }

            if (tokenStart == NULL) {
                // Open parens must be preceded by identifier.
                status = StatusAstMalformedEvalString;
                goto CommonExit;
            }
            if ((size_t)(evalStr - tokenStart + 1) >=
                sizeof(operatorNode->common.token)) {
                status = StatusXcalarEvalTokenNameTooLong;
                goto CommonExit;
            }
            strlcpy(operatorNode->common.token,
                    tokenStart,
                    evalStr - tokenStart + 1);

            operatorNode->registeredFn =
                getOrResolveRegisteredFn(operatorNode->common.token,
                                         udfContainer,
                                         &operatorNode->nsHandle);

            if (operatorNode->registeredFn == NULL) {
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Could not find function %s",
                              operatorNode->common.token);
                status = StatusAstNoSuchFunction;
                goto CommonExit;
            }

            if (operatorNode->registeredFn->fnType == XcalarFnTypeAgg &&
                evalStrType == XcalarFnTypeEval) {
                status = StatusAggFnInClass1Ast;
                goto CommonExit;
            }
            numFunc++;
            status = parseOperatorArgs((char *) evalStr + 1,
                                       evalStrType,
                                       udfContainer,
                                       (char **) &evalStr,
                                       operatorNode,
                                       &numVariables,
                                       &numConstants,
                                       &numIntermediates,
                                       &numFunc);
            if (status != StatusOk) {
                goto CommonExit;
            }
            parseCalled = true;
            // parseOperatorArgs will leave evalStr pointing after args.
            continue;

        case ',':  // Fall through
        case ')':
            status = StatusAstMalformedEvalString;
            goto CommonExit;

        default:
            if (!isValidIdentifierChar(*evalStr)) {
                status = StatusAstMalformedEvalString;
                goto CommonExit;
            }

            if (tokenStart == NULL) {
                tokenStart = evalStr;
            } else if (parseCalled) {
                status = StatusAstMalformedEvalString;
                goto CommonExit;
            }

            break;
        }
        evalStr++;
    }

    if (operatorNode->registeredFn == NULL) {
        status = StatusAstMalformedEvalString;
        goto CommonExit;
    }

    if (operatorNode->registeredFn->makeContextFn != NULL) {
        operatorNode->dynamicContext =
            operatorNode->registeredFn->makeContextFn(
                operatorNode->registeredFn->fnDesc.context);
        BailIfNull(operatorNode->dynamicContext);
    }

    ast->rootNode = operatorNode;

    if (numConstants > 0) {
        ast->constants =
            (Scalar **) memAllocExt(numConstants * sizeof(*(ast->constants)),
                                    ModuleName);
        if (ast->constants == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        allocedConstants = true;
    } else {
        ast->constants = NULL;
    }
    for (ii = 0; ii < numConstants; ii++) {
        ast->constants[ii] = NULL;
    }
    ast->numConstants = numConstants;

    // To be filled in by other functions
    ast->numScalarIntermediates = 0;
    ast->scalarIntermediates = NULL;

    // To be filled in by other functions
    ast->numScalarVariables = 0;
    ast->scalarVariables = NULL;

    if (numVariablesOut != NULL) {
        *numVariablesOut = numVariables;
    }

    if (numIntermediatesOut != NULL) {
        *numIntermediatesOut = numIntermediates;
    }

    if (numConstantsOut != NULL) {
        *numConstantsOut = numConstants;
    }

    assert(status == StatusOk);

CommonExit:
    if (status != StatusOk) {
        if (allocedConstants) {
            assert(ast->constants != NULL);
            memFree(ast->constants);
            ast->constants = NULL;
            ast->numConstants = 0;
        }

        if (operatorNode != NULL) {
            freeAstNode(operatorNode);
            operatorNode = NULL;
            ast->rootNode = NULL;
        }
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failure: %s %s",
                      origEvalStr,
                      strGetFromStatus(status));
    }

    if (numFuncOut != NULL) {
        *numFuncOut = numFunc;
    }
    return status;
}

void
XcalarEval::freeCommonAst(XcalarEvalAstCommon *ast)
{
    assert(ast != NULL);
    if (ast->scalarIntermediates != NULL) {
        freeScalarIntermediates(ast);
    }

    if (ast->rootNode != NULL) {
        freeAstNode(ast->rootNode);
        ast->rootNode = NULL;
    }

    if (ast->constants != NULL) {
        freeScalarConstants(ast);
    }

    if (ast->scalarVariables != NULL) {
        freeScalarVariables(ast);
    }

    assert(ast->numConstants == 0);
    assert(ast->constants == NULL);
    assert(ast->numScalarIntermediates == 0);
    assert(ast->scalarIntermediates == NULL);
}

// It is critical that we process the AST in the same order
// as fixupClass2AstFirstPass
Status
XcalarEval::fixupClass2AstSecondPass(
    XcalarEvalAstOperatorNode *operatorNodeParent,
    unsigned *valueArrayIdx,
    unsigned *constantIdx,
    unsigned *scalarIntermediateIdx,
    unsigned *scalarVariableIdx,
    Scalar *constants[],
    Scalar *scalarIntermediates[],
    XcalarEvalScalarVariable scalarVariables[])
{
    unsigned ii, jj;
    Status status = StatusUnknown;
    XcalarEvalAstOperatorNode *operatorNode;
    XcalarEvalAstNode *regularNode;

    assert(operatorNodeParent != NULL);
    assert(valueArrayIdx != NULL);
    assert(constantIdx != NULL);
    assert(scalarIntermediateIdx != NULL);

    for (ii = 0; ii < operatorNodeParent->numArgs; ii++) {
        switch (operatorNodeParent->arguments[ii]->nodeType) {
        case XcalarEvalAstGroupOperator:  // Fall through
        case XcalarEvalAstScalarOperator:
            operatorNode =
                (XcalarEvalAstOperatorNode *) operatorNodeParent->arguments[ii];
            status = fixupClass2AstSecondPass(operatorNode,
                                              valueArrayIdx,
                                              constantIdx,
                                              scalarIntermediateIdx,
                                              scalarVariableIdx,
                                              constants,
                                              scalarIntermediates,
                                              scalarVariables);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;

        case XcalarEvalAstVariable:
            regularNode = operatorNodeParent->arguments[ii];

            if (*(regularNode->token) == OperatorsAggregateTag) {
                bool variableFound = false;
                for (jj = 0; jj < *scalarVariableIdx; jj++) {
                    if (strcmp(scalarVariables[jj].variableName,
                               regularNode->token) == 0) {
                        variableFound = true;
                        regularNode->varContent =
                            &(scalarVariables[jj].content);
                        break;
                    }
                }

                if (!variableFound) {
                    assert(jj == *scalarVariableIdx);
                    scalarVariables[jj].content = NULL;
                    scalarVariables[jj].variableName = regularNode->token;
                    regularNode->varContent = &(scalarVariables[jj].content);
                    (*scalarVariableIdx)++;
                }
            }

            break;

        case XcalarEvalAstConstantStringLiteral:
        case XcalarEvalAstConstantBool:
        case XcalarEvalAstConstantNumber:
        case XcalarEvalAstConstantNull:
            status = fixupConstant(operatorNodeParent->arguments[ii]->nodeType,
                                   operatorNodeParent->arguments[ii],
                                   constants,
                                   constantIdx);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;

        case XcalarEvalAstOperator:
            assert(0);
            break;
        }
    }

    switch (operatorNodeParent->common.nodeType) {
    case XcalarEvalAstGroupOperator:
        operatorNodeParent->common.valueArrayIdx = (*valueArrayIdx)++;
        break;
    case XcalarEvalAstScalarOperator:
        operatorNodeParent->common.varContent =
            &scalarIntermediates[(*scalarIntermediateIdx)++];
        break;
    default:
        assert(0);
    }

    status = StatusOk;
CommonExit:
    return status;
}

void
XcalarEval::findMaxArgs(XcalarEvalAstOperatorNode *node,
                        unsigned *numScalarArgsOut,
                        unsigned *numArgsOut)
{
    unsigned myScalarArgs = 0;
    unsigned maxScalarArgs = 0, maxArgs = 0;
    unsigned childScalarArgs = 0, childArgs = 0;

    for (unsigned ii = 0; ii < node->numArgs; ii++) {
        if (isOperatorNode(node->arguments[ii]->nodeType)) {
            findMaxArgs((XcalarEvalAstOperatorNode *) node->arguments[ii],
                        &childScalarArgs,
                        &childArgs);
            maxArgs = fmax(maxArgs, childArgs);
            maxScalarArgs = fmax(maxScalarArgs, childScalarArgs);
        }

        if (!isGroupArg(node->arguments[ii]->nodeType,
                        node->arguments[ii]->token)) {
            myScalarArgs++;
        }
    }

    maxArgs = fmax(maxArgs, node->numArgs);
    maxScalarArgs = fmax(maxScalarArgs, myScalarArgs);

    *numArgsOut = maxArgs;
    *numScalarArgsOut = maxScalarArgs;
}

Status
XcalarEval::initGroupIter(ScalarGroupIter *groupIter,
                          DagTypes::DagId dagId,
                          DagTypes::NodeId dstXdbId,
                          XcalarEvalAstOperatorNode *rootNode)
{
    Status status = StatusOk;
    groupIter->dagId = dagId;
    groupIter->dstXdbId = dstXdbId;

    unsigned maxScalarValues, maxArgs;

    findMaxArgs(rootNode, &maxScalarValues, &maxArgs);

    groupIter->maxScalarValues = maxScalarValues;
    groupIter->maxArgs = maxArgs;

    if (maxScalarValues > 0) {
        groupIter->scalarValues =
            (Scalar **) memAllocExt(sizeof(*groupIter->scalarValues) *
                                        maxScalarValues,
                                    ModuleName);
        BailIfNull(groupIter->scalarValues);
    }

    if (maxArgs > 0) {
        groupIter->args = (ScalarRecordArgument *)
            memAllocExt(sizeof(*groupIter->args) * maxArgs, ModuleName);
        BailIfNull(groupIter->args);
    }

CommonExit:
    if (status != StatusOk) {
        if (groupIter->scalarValues != NULL) {
            memFree(groupIter->scalarValues);
            groupIter->scalarValues = NULL;
        }

        if (groupIter->args != NULL) {
            memFree(groupIter->args);
            groupIter->args = NULL;
        }
    }

    return status;
}

void
XcalarEval::destroyGroupIter(ScalarGroupIter *groupIter)
{
    if (groupIter->scalarValues != NULL) {
        memFree(groupIter->scalarValues);
        groupIter->scalarValues = NULL;
        groupIter->numScalarValues = 0;
    }

    if (groupIter->args != NULL) {
        memFree(groupIter->args);
        groupIter->args = NULL;
        groupIter->numArgs = 0;
    }
}

Status
XcalarEval::generateClass2Ast(const char *evalStr,
                              XcalarEvalClass2Ast *ast,
                              uint64_t *numFunc,
                              DagTypes::DagId dagId,
                              XdbId dstXdbId)
{
    Status status = StatusUnknown;
    unsigned numIntermediates, numConstants, numUniqueVariables;
    unsigned numVariables = 0;
    unsigned numScalarIntermediates, numGroupIntermediates, numScalarVariables;
    bool astGenerated = false, allocedVariableNames = false;
    unsigned ii;
    unsigned valueArrayIdx;

    assert(evalStr != NULL);
    assert(ast != NULL);
    memZero(ast, sizeof(*ast));

    ast->variableNames = NULL;

    status = parseEvalStr(evalStr,
                          XcalarFnTypeAgg,
                          NULL,  // XXX: container?
                          &ast->astCommon,
                          &numIntermediates,
                          &numVariables,
                          &numConstants,
                          numFunc);
    if (status != StatusOk) {
        goto CommonExit;
    }
    astGenerated = true;

    // numUniqueVariabes <= numVariables. So allocate for worse case
    if (numVariables > 0) {
        ast->variableNames =
            (char **) memAllocExt(numVariables * sizeof(*(ast->variableNames)),
                                  ModuleName);
        if (ast->variableNames == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        allocedVariableNames = true;
    } else {
        ast->variableNames = NULL;
    }

    for (ii = 0; ii < numVariables; ii++) {
        ast->variableNames[ii] = NULL;
    }

    numScalarVariables = 0;
    numUniqueVariables = numScalarIntermediates = numGroupIntermediates = 0;
    status = fixupClass2AstFirstPass(ast->astCommon.rootNode,
                                     &numUniqueVariables,
                                     ast->variableNames,
                                     &numScalarIntermediates,
                                     &numGroupIntermediates,
                                     &numScalarVariables);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(numGroupIntermediates + numScalarIntermediates == numIntermediates);

    assert(ast->astCommon.rootNode->common.nodeType != XcalarEvalAstOperator);
    if (ast->astCommon.rootNode->common.nodeType ==
        XcalarEvalAstGroupOperator) {
        status = StatusAggregateReturnValueNotScalar;
        goto CommonExit;
    }

    ast->numUniqueVariables = numUniqueVariables;

    status = allocScalarIntermediates(&ast->astCommon, numScalarIntermediates);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = allocScalarVariables(&ast->astCommon, numScalarVariables);
    if (status != StatusOk) {
        goto CommonExit;
    }

    valueArrayIdx = numUniqueVariables;
    unsigned scalarIntermediateIdx, constantIdx, scalarVariableIdx;

    scalarIntermediateIdx = constantIdx = scalarVariableIdx = 0;

    status = fixupClass2AstSecondPass(ast->astCommon.rootNode,
                                      &valueArrayIdx,
                                      &constantIdx,
                                      &scalarIntermediateIdx,
                                      &scalarVariableIdx,
                                      ast->astCommon.constants,
                                      ast->astCommon.scalarIntermediates,
                                      ast->astCommon.scalarVariables);
    if (status != StatusOk) {
        goto CommonExit;
    }

    ast->scalarXdbId = XdbIdInvalid;
    ast->scalarTableGlobalState = (XdbGlobalStateMask) 0;

    status = initGroupIter(&ast->groupIter,
                           dagId,
                           dstXdbId,
                           ast->astCommon.rootNode);
    BailIfFailed(status);

    assert(valueArrayIdx == numUniqueVariables + numGroupIntermediates);
    assert(constantIdx == numConstants);
    assert(scalarIntermediateIdx == numScalarIntermediates);
    assert(scalarVariableIdx == numScalarVariables);

    assert(status == StatusOk);
CommonExit:
    if (status != StatusOk) {
        if (allocedVariableNames) {
            assert(astGenerated);
            assert(ast->variableNames != NULL);
            memFree(ast->variableNames);
            ast->variableNames = NULL;
            ast->numUniqueVariables = 0;
        }

        if (astGenerated) {
            XcalarEval::get()->freeCommonAst(&ast->astCommon);
            astGenerated = false;
        }
    }

    return status;
}

Status
XcalarEval::allocScalarVariables(XcalarEvalAstCommon *ast,
                                 unsigned numVariables)
{
    Status status = StatusUnknown;
    XcalarEvalScalarVariable *scalarVariables = NULL;

    assert(ast != NULL);
    if (numVariables > 0) {
        scalarVariables = (XcalarEvalScalarVariable *)
            memAllocExt(numVariables * sizeof(*scalarVariables), ModuleName);
        if (scalarVariables == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        for (unsigned ii = 0; ii < numVariables; ii++) {
            scalarVariables[ii].content = NULL;
        }
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (scalarVariables != NULL) {
            assert(numVariables > 0);
            memFree(scalarVariables);
            scalarVariables = NULL;
            numVariables = 0;
        }
    }

    ast->scalarVariables = scalarVariables;
    ast->numScalarVariables = numVariables;

    return status;
}

void
XcalarEval::freeScalarVariables(XcalarEvalAstCommon *ast)
{
    if (ast->scalarVariables != NULL) {
        for (unsigned ii = 0; ii < ast->numScalarVariables; ii++) {
            if (ast->scalarVariables[ii].content != NULL) {
                Scalar::freeScalar(ast->scalarVariables[ii].content);
                ast->scalarVariables[ii].content = NULL;
            }
        }
        memFree(ast->scalarVariables);
        ast->scalarVariables = NULL;
        ast->numScalarVariables = 0;
    }
}

Status
XcalarEval::allocScalarIntermediates(XcalarEvalAstCommon *ast,
                                     unsigned numIntermediates)
{
    bool allocedScalarIntermediates = false;
    Status status = StatusUnknown;
    unsigned numScalarIntermediatesAlloced = 0;
    unsigned ii;

    assert(ast != NULL);
    assert(numIntermediates > 0);

    ast->scalarIntermediates =
        (Scalar **) memAllocExt(numIntermediates *
                                    sizeof(*(ast->scalarIntermediates)),
                                ModuleName);
    if (ast->scalarIntermediates == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    allocedScalarIntermediates = true;

    for (numScalarIntermediatesAlloced = 0;
         numScalarIntermediatesAlloced < numIntermediates;
         numScalarIntermediatesAlloced++) {
        // XXX ENG-9968 when scalars are copied or transferred over the
        // network, we need to think through how to avoid copying
        // DfMaxFieldValueSize bytes once the size of the field is known.
        ast->scalarIntermediates[numScalarIntermediatesAlloced] =
            Scalar::allocScalar(DfMaxFieldValueSize);
        if (ast->scalarIntermediates[numScalarIntermediatesAlloced] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
    }

    assert(numScalarIntermediatesAlloced == numIntermediates);
    ast->numScalarIntermediates = numIntermediates;

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (numScalarIntermediatesAlloced > 0) {
            assert(allocedScalarIntermediates);
            assert(numScalarIntermediatesAlloced <= numIntermediates);
            assert(ast->scalarIntermediates != NULL);
            for (ii = 0; ii < numScalarIntermediatesAlloced; ii++) {
                assert(ast->scalarIntermediates[ii] != NULL);
                Scalar::freeScalar(ast->scalarIntermediates[ii]);
                ast->scalarIntermediates[ii] = NULL;
            }
            numScalarIntermediatesAlloced = 0;
        }

        if (allocedScalarIntermediates) {
            assert(numScalarIntermediatesAlloced == 0);
            assert(ast->scalarIntermediates != NULL);
            assert(numIntermediates > 0);
            memFree(ast->scalarIntermediates);
            ast->scalarIntermediates = NULL;
            numIntermediates = 0;
        }
    }

    return status;
}

void
XcalarEval::freeScalarIntermediates(XcalarEvalAstCommon *ast)
{
    unsigned ii;

    assert(ast != NULL);

    for (ii = 0; ii < ast->numScalarIntermediates; ii++) {
        if (ast->scalarIntermediates[ii] != NULL) {
            Scalar::freeScalar(ast->scalarIntermediates[ii]);
            ast->scalarIntermediates[ii] = NULL;
        }
    }
    ast->numScalarIntermediates = 0;

    if (ast->scalarIntermediates != NULL) {
        memFree(ast->scalarIntermediates);
        ast->scalarIntermediates = NULL;
    }
}

void
XcalarEval::freeScalarConstants(XcalarEvalAstCommon *ast)
{
    unsigned ii;

    assert(ast != NULL);
    assert(ast->numConstants > 0);

    for (ii = 0; ii < ast->numConstants; ii++) {
        if (ast->constants[ii] != NULL) {
            Scalar::freeScalar(ast->constants[ii]);
            ast->constants[ii] = NULL;
        }
    }
    memFree(ast->constants);
    ast->constants = NULL;
    ast->numConstants = 0;
}

Status
XcalarEval::getVariablesListInternal(
    XcalarEvalAstOperatorNode *operatorNodeParent,
    unsigned *variableIdx,
    const char *variableNames[],
    XdfTypesAccepted *variableTypes,
    unsigned maxNumVariables)
{
    unsigned ii;
    Status status = StatusUnknown;
    XcalarEvalAstNode *regularNode;
    XcalarEvalAstOperatorNode *operatorNode;

    for (ii = 0; ii < operatorNodeParent->numArgs; ii++) {
        switch (operatorNodeParent->arguments[ii]->nodeType) {
        case XcalarEvalAstOperator:
            operatorNode =
                (XcalarEvalAstOperatorNode *) operatorNodeParent->arguments[ii];
            status = getVariablesListInternal(operatorNode,
                                              variableIdx,
                                              variableNames,
                                              variableTypes,
                                              maxNumVariables);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;

        case XcalarEvalAstVariable: {
            regularNode = operatorNodeParent->arguments[ii];
            unsigned idx = *variableIdx;

            if (idx >= maxNumVariables) {
                status = StatusOverflow;
                goto CommonExit;
            }
            variableNames[idx] = regularNode->token;
            variableTypes[idx] =
                operatorNodeParent->registeredFn->fnDesc.argDescs[ii]
                    .typesAccepted;

            (*variableIdx)++;
            break;
        }
        case XcalarEvalAstConstantStringLiteral:
        case XcalarEvalAstConstantBool:
        case XcalarEvalAstConstantNumber:
        case XcalarEvalAstConstantNull:
        case XcalarEvalAstGroupOperator:
        case XcalarEvalAstScalarOperator:
            break;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
XcalarEval::getVariablesList(XcalarEvalAstCommon *ast,
                             const char *variableNames[],
                             XdfTypesAccepted *variableTypes,
                             unsigned maxNumVariables,
                             unsigned *numVariablesOut)
{
    Status status = StatusUnknown;
    unsigned variableIdx = 0;

    assert(ast != NULL);
    assert(variableNames != NULL);

    status = getVariablesListInternal(ast->rootNode,
                                      &variableIdx,
                                      variableNames,
                                      variableTypes,
                                      maxNumVariables);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (numVariablesOut != NULL) {
        *numVariablesOut = variableIdx;
    }
CommonExit:
    return status;
}

Status
XcalarEval::generateClass1Ast(const char *evalStr, XcalarEvalClass1Ast *ast)
{
    Status status = StatusUnknown;
    unsigned numIntermediates, numVariables, numConstants;
    bool allocedScalarIntermediates, allocedScalarVariables;
    bool astGenerated = false;

    assert(evalStr != NULL);
    assert(ast != NULL);

    allocedScalarIntermediates = allocedScalarVariables = false;

    status = XcalarEval::get()->parseEvalStr(evalStr,
                                             XcalarFnTypeEval,
                                             NULL,  // XXX: container?
                                             &ast->astCommon,
                                             &numIntermediates,
                                             &numVariables,
                                             &numConstants,
                                             NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    astGenerated = true;
    assert(ast->astCommon.rootNode != NULL);

    status = allocScalarIntermediates(&ast->astCommon, numIntermediates);
    if (status != StatusOk) {
        goto CommonExit;
    }
    allocedScalarIntermediates = true;

    status = allocScalarVariables(&ast->astCommon, numVariables);
    if (status != StatusOk) {
        goto CommonExit;
    }
    allocedScalarVariables = true;
    // XXX why not allocate constants here

    unsigned constantIdx, scalarVariableIdx, scalarIntermediateIdx;
    constantIdx = scalarVariableIdx = scalarIntermediateIdx = 0;
    status = fixupArgumentPointersClass1(ast->astCommon.rootNode,
                                         &scalarVariableIdx,
                                         &constantIdx,
                                         &scalarIntermediateIdx,
                                         ast->astCommon.scalarVariables,
                                         ast->astCommon.constants,
                                         ast->astCommon.scalarIntermediates);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(scalarVariableIdx == numVariables);
    assert(constantIdx == numConstants);
    assert(scalarIntermediateIdx == numIntermediates);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (allocedScalarVariables) {
            freeScalarVariables(&ast->astCommon);
            allocedScalarVariables = false;
        }

        if (allocedScalarIntermediates) {
            freeScalarIntermediates(&ast->astCommon);
            allocedScalarIntermediates = false;
        }

        if (astGenerated) {
            XcalarEval::get()->freeCommonAst(&ast->astCommon);
            astGenerated = false;
        }
    }

    return status;
}

void
XcalarEval::freeAstNode(XcalarEvalAstOperatorNode *operatorNode)
{
    XcalarEvalAstNode *astNode;
    assert(isOperatorNode(operatorNode->common.nodeType));
    unsigned ii;

    for (ii = 0; ii < operatorNode->numArgs; ii++) {
        astNode = operatorNode->arguments[ii];
        if (isOperatorNode(astNode->nodeType)) {
            freeAstNode((XcalarEvalAstOperatorNode *) astNode);
        } else {
            delete astNode;
        }
        operatorNode->arguments[ii] = NULL;
    }

    if (operatorNode->dynamicContext != NULL) {
        DCHECK(operatorNode->registeredFn->delContextFn != NULL);
        operatorNode->registeredFn->delContextFn(operatorNode->dynamicContext);
        operatorNode->dynamicContext = NULL;
    }

    if (operatorNode->registeredFn != NULL) {
        putFunction(operatorNode->registeredFn,
                    (operatorNode->registeredFn->isBuiltIn
                         ? NULL
                         : &operatorNode->nsHandle));
        operatorNode->registeredFn = NULL;
    }

    if (operatorNode->argMapping != NULL) {
        memFree(operatorNode->argMapping);
        operatorNode->argMapping = NULL;
    }

    delete operatorNode;
    operatorNode = NULL;
}

void
XcalarEval::destroyClass2Ast(XcalarEvalClass2Ast *ast)
{
    if (ast->variableNames != NULL) {
        assert(ast->numUniqueVariables > 0);
        memFree(ast->variableNames);
        ast->variableNames = NULL;
        ast->numUniqueVariables = 0;
    }

    aggregateDeleteVariables(ast);
    freeCommonAst(&ast->astCommon);

    destroyGroupIter(&ast->groupIter);
    assert(ast->scalarXdbId == XdbIdInvalid);
    assert(!ast->scalarTableGlobalState);
    assert(ast->variableNames == NULL);
    assert(ast->numUniqueVariables == 0);
}

void
XcalarEval::destroyClass1Ast(XcalarEvalClass1Ast *ast)
{
    freeCommonAst(&ast->astCommon);
}

////////////////////////////////////////////////////////////////////////////////

Status
XcalarEval::evalInternal(XcalarEvalAstOperatorNode *operatorNode, bool icvMode)
{
    unsigned ii;
    Status status = StatusOk;
    Status statusCopy = StatusOk;
    Scalar *argv[operatorNode->numArgs];
    DfFieldValue valueTmp;
    char argErrStr[XcalarApiMaxFailureDescLen + 1];
    argErrStr[0] = '\0';

    if (operatorNode->registeredFn->fnType == XcalarFnTypeAgg) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Agg function %s is not supported in map eval",
                      operatorNode->registeredFn->getFnName());
        status = StatusInval;
        goto CommonExit;
    }

    for (ii = 0; ii < operatorNode->numArgs; ii++) {
        if (operatorNode->arguments[ii]->nodeType == XcalarEvalAstOperator) {
            status = evalInternal(
                (XcalarEvalAstOperatorNode *) operatorNode->arguments[ii], icvMode);
            if (isFatalError(status)) {
                goto CommonExit;
            } else if (status != StatusOk) {
                // We only care about failure strings in icvMode
                if (icvMode) {
                    argv[ii] = *(operatorNode->arguments[ii]->varContent);
                    //f the evalInternal() fails, this means that *(operatorNode->arguments[ii]->varContent)
                    // must be of type DfString (since the failure string should be stored in it)
                    assert(argv[ii]->fieldType == DfString);
                    statusCopy = argv[ii]->getValue(&valueTmp);
                    if (statusCopy == StatusOk) {
                        // Append this arguments failure to previous argument
                        // failures.
                        size_t argErrStrLen = strlen(argErrStr);
                        snprintf(argErrStr + argErrStrLen,
                                 sizeof(argErrStr) - argErrStrLen,
                                 "Argument %d failed with the following "
                                 "error:\n%s\n",
                                 ii,
                                 valueTmp.stringVal.strActual);
                    }
                }
                // If there is a failure that is not a fatal error, we want to
                // let the nested eval continue, but set current argument to
                // None.
                argv[ii] = NULL;
            } else {
                argv[ii] = *(operatorNode->arguments[ii]->varContent);
            }
            // Continue as usual in the case of a non-fatal error
            status = StatusOk;
        } else {
            argv[ii] = *(operatorNode->arguments[ii]->varContent);
        }
    }

    if (operatorNode->registeredFn->enforceArgCheck) {
        // Enforcing that operatorNode->registeredFn.numArgs ==
        // operatorNode->numArgs is the responsibility of GenerateAst
        assert(operatorNode->numArgs < INT32_MAX);
        unsigned argDescIndex = 0;
        for (ii = 0; ii < operatorNode->numArgs; ii++) {
            XcalarEvalFnDesc::ArgDesc *argDesc =
                &operatorNode->registeredFn->fnDesc.argDescs[argDescIndex];

            if (argDesc->argType != VariableArg) {
                argDescIndex++;
            }

            if (argv[ii] == NULL || argv[ii]->fieldNumValues == 0) {
                if (argDesc->typesAccepted & XdfAcceptNull) {
                    continue;
                } else if (argDesc->argType == OptionalArg) {
                    if (argv[ii] == NULL) {
                        assert(argDesc->defaultType != DfString);
                        // @SymbolCheckIgnore
                        argv[ii] = (Scalar *) memAlloca(sizeof(Scalar) +
                                                        sizeof(DfFieldValue));
                        argv[ii]->fieldUsedSize = 0;
                        argv[ii]->fieldAllocedSize = sizeof(DfFieldValue);
                    }

                    status = argv[ii]->setValue(argDesc->defaultValue,
                                                argDesc->defaultType);
                    BailIfFailed(status);
                    continue;
                } else {
                    // Bubble the error up
                    status = StatusEvalUnsubstitutedVariables;
                    goto CommonExit;
                }
            }

            assert(argv[ii] != NULL);

            if ((!(argDesc->typesAccepted & (1 << argv[ii]->fieldType))) ||
                (argDesc->isSingletonValue && argv[ii]->fieldNumValues > 1)) {
                // Bubble the error up
                status = StatusXdfTypeUnsupported;
                goto CommonExit;
            }
        }
    }

    if (status == StatusOk) {
        status = operatorNode->registeredFn
                     ->evalFn(operatorNode->dynamicContext,
                              operatorNode->numArgs,
                              argv,
                              *(operatorNode->common.varContent));
    }
CommonExit:
    if (status != StatusOk && !isFatalError(status)) {
        if (icvMode) {
            //if non-fatal error in icvMode, we copy error string to output
            char finalErrStr[XcalarApiMaxFailureDescLen + 1];
            // argErrStr will only be non-empty if there were failures in one or
            // more arguments.
            if (argErrStr[0] != '\0') {
                const char *errorStr;
                // The following code displays the error found in a nested
                // eval string in a way that makes it clear exactly what
                // functions failed in that nested eval string.

                // First, we extract the current error so that it can be appended
                // to the nested errors.
                if (status != StatusUdfExecuteFailed) {
                    // This is an XDF failure. The error can be
                    // extracted from the status string.
                    errorStr = strGetFromStatus(status);
                } else {
                    // The error string is in the Scalar out object in the case of
                    // UDF failures.
                    Scalar *out = *(operatorNode->common.varContent);
                    statusCopy = out->getValue(&valueTmp);
                    if (statusCopy != StatusOk) {
                        errorStr = failedErrorMsg;
                    } else {
                        errorStr = valueTmp.stringVal.strActual;
                    }
                }
                // Now we can build the full nested error string..
                unsigned long n = snprintf(finalErrStr,
                        sizeof(finalErrStr),
                        "%sWhile processing the next outer nested level, the "
                        "following error occurred:\n\n%s",
                        argErrStr,
                        errorStr);
                if (n >= sizeof(finalErrStr)) {
                    // The error has been truncated, so add truncated msg so user is
                    // aware that error has been cut off.
                    snprintf(finalErrStr,
                        sizeof(finalErrStr),
                        "%s%sWhile processing the next outer nested level, the "
                        "following error occurred:\n\n%s",
                        truncatedMsg,
                        argErrStr,
                        errorStr);
                }

                valueTmp.stringVal.strActual = finalErrStr;
                valueTmp.stringVal.strSize = strlen(finalErrStr) + 1;
                Scalar *out = *(operatorNode->common.varContent);
                statusCopy = out->setValue(valueTmp, DfString);
                if (statusCopy != StatusOk) {
                    status = statusCopy;
                }
            } else if (status != StatusUdfExecuteFailed) {
                // Xdfs don't return error strings, but we can
                // infer something about the error from the status,
                // so we extract that here
                Scalar *out = *(operatorNode->common.varContent);
                snprintf(finalErrStr,
                        sizeof(finalErrStr),
                        "%s\n",
                        strGetFromStatus(status));
                valueTmp.stringVal.strSize = strlen(finalErrStr) + 1;
                valueTmp.stringVal.strActual = finalErrStr;
                out->setValue(valueTmp, DfString);
            }
        } else {
            // If non-fatal error when not in ICV mode, we can set output to NULL
            Scalar *out = *(operatorNode->common.varContent);
            out->setValue(DfFieldValueNull, DfNull);;
        }
    }
    return status;
}

void
XcalarEval::aggregateDeleteVariables(XcalarEvalClass2Ast *ast)
{
    XdbMgr *xdbMgr = XdbMgr::get();
    assert(ast != NULL);

    if (ast->scalarXdbId != XdbIdInvalid) {
        XdbMeta *xdbMeta;
        // XXX Remove unnecessary xdbGet and use perTxnInfo.
        Status status = xdbMgr->xdbGet(ast->scalarXdbId, NULL, &xdbMeta);
        if (status == StatusOk && !xdbMeta->isScratchPadXdb) {
            xdbMgr->xdbDrop(ast->scalarXdbId);
        }

        ast->scalarXdbId = XdbIdInvalid;
    }
    ast->scalarTableGlobalState = (XdbGlobalStateMask) 0;
}

void
XcalarEval::deleteVariables(XcalarEvalClass1Ast *ast)
{
    unsigned ii;
    for (ii = 0; ii < ast->astCommon.numScalarVariables; ii++) {
        if (ast->astCommon.scalarVariables[ii].content != NULL) {
            Scalar::freeScalar(ast->astCommon.scalarVariables[ii].content);
            ast->astCommon.scalarVariables[ii].content = NULL;
        }
    }
}

Status
XcalarEval::eval(XcalarEvalClass1Ast *ast,
                 DfFieldValue *fieldVal,
                 DfFieldType *fieldType,
                 bool icvMode)
{
    Scalar *scalar;
    Status status;
    Status status2;

    assert(ast != NULL);
    assert(fieldVal != NULL);
    assert(fieldType != NULL);
    bool evalFailed = false;

    status = evalInternal(ast->astCommon.rootNode, icvMode);
    if (unlikely(isFatalError(status))) {
        return status;
    }
    if (status != StatusOk) {
        evalFailed = true;
    }

    scalar = *(ast->astCommon.rootNode->common.varContent);

    // assert that output is singleton for UDFs since return type is always
    // string for UDFs, this works (not for XDFs like explodeString which has
    // multi-output)
    assert(ast->astCommon.rootNode->registeredFn->isBuiltIn == true ||
           isSingletonOutputType(&ast->astCommon));


    if (icvMode) {
        if (!evalFailed) {
            // When in ICV mode, we do not care about successful results. So we can
            // set output to NULL
            *fieldType = DfNull;
            *fieldVal = DfFieldValueNull;
        } else {
            // When in ICV mode, failed results should always be a string.
            assert(scalar->fieldType == DfString);
            *fieldType = scalar->fieldType;
            status2 = scalar->getValue(fieldVal);
            if (status2 != StatusOk) {
                // getValue failure trumps the original eval failure status
                status = status2;
            }
        }
    } else {
        if (isSingletonOutputType(&ast->astCommon)) {
            *fieldType = scalar->fieldType;
            status2 = scalar->getValue(fieldVal);
            if (!evalFailed || status2 != StatusOk) {
                // getValue failure trumps the original eval failure status
                status = status2;
            }
        } else {
            *fieldType = DfScalarObj;
            fieldVal->scalarVal = scalar;
        }
    }
    return status;
}

bool
XcalarEval::isEvalResultTrue(DfFieldValue fieldVal, DfFieldType fieldType)
{
    switch (fieldType) {
    case DfBoolean:
        return fieldVal.boolVal;
    case DfInt32:
        return fieldVal.int32Val != 0;
    case DfInt64:
        return fieldVal.int64Val != 0;
    case DfFloat32:
        return fieldVal.float32Val != 0.0;
    case DfFloat64:
        return fieldVal.float64Val != 0.0;
    case DfString:
        return fieldVal.stringVal.strSize != 0;
    case DfNull:
        // Same as PostgreSQL and MySQL;
        return false;
    default:
        assert(0);
        NotReached();
        return false;
    }
}

bool
XcalarEval::isValidDatasetName(const char *datasetName)
{
    if (datasetName == NULL) {
        return false;
    }

    if (*datasetName == '\0') {
        // Empty dataset name is valid
        return true;
    }
#ifdef ENFORCE_NAMING
    // First character must be alpha (or '.' as this is a Xcalar test
    // convention).
    if (!isalpha(*datasetName) && *datasetName != '.') {
        return false;
    }
#endif

    // XXX Add more extensive validation given the TBD dataset naming
    // rules.

    return true;
}

bool
XcalarEval::isValidTableName(const char *tableName)
{
    if (tableName == NULL) {
        return false;
    }

    if (*tableName == '\0') {
        // Empty table name is valid
        return true;
    }

#ifdef ENFORCE_NAMING
    // First character must be alpha (or '.' as this is a Xcalar test
    // convention.
    if (!isalpha(*tableName) && *tableName != '.') {
        return false;
    }
#endif

    // XXX Add more extensive validation given the TBD table naming
    // rules.

    return true;
}

bool
XcalarEval::isValidFieldName(const char *fieldName)
{
    if (fieldName == NULL) {
        return false;
    }

    if (*fieldName == '\0') {
        // Empty field name is invalid
        return false;
    }

#ifdef ENFORCE_NAMING
    // First character must be alpha (or '.' as this is a Xcalar test
    // convention.
    if (!isalpha(*fieldName) && *fieldName != '.') {
        return false;
    }
#endif

    // XXX Add more extensive validation given the TBD field naming
    // rules.

    if (strstr(fieldName, DfFatptrPrefixDelimiter)) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Field %s cannot have prefix delimiter %s",
                      fieldName,
                      DfFatptrPrefixDelimiter);
        return false;
    }

    if (Txn::currentTxn().mode_ == Txn::Mode::NonLRQ &&
        strstr(fieldName, DfFatptrPrefixDelimiterReplaced)) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Field %s cannot have replaced prefix delimiter %s in"
                      " modeling mode",
                      fieldName,
                      DfFatptrPrefixDelimiterReplaced);
        return false;
    }

    return true;
}

DfFieldType
XcalarEval::getOutputType(XcalarEvalAstCommon *ast)
{
    assert(ast != NULL);
    assert(ast->rootNode != NULL);
    assert(ast->rootNode->registeredFn != NULL);
    return ast->rootNode->registeredFn->fnDesc.outputType;
}

DfFieldType
XcalarEval::getOutputTypeXdf(const char *evalString)
{
    const char *tok = strstr(evalString, "(");
    size_t fnNameLen = tok - evalString;
    char fnName[fnNameLen + 1];

    strlcpy(fnName, evalString, sizeof(fnName));

    XcalarEvalRegisteredFn *fn = getOrResolveRegisteredFn(fnName, NULL, NULL);

    if (fn == NULL) {
        return DfUnknown;
    } else {
        return fn->fnDesc.outputType;
    }
}

bool
XcalarEval::isSingletonOutputType(XcalarEvalAstCommon *ast)
{
    return ast->rootNode->registeredFn->fnDesc.isSingletonOutput;
}

Status
XcalarEval::evalAndInsert(int argc,
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
                          OpEvalErrorStats *errorStats)
{
    Status status;

    status =
        XcalarEval::get()->getArgs(argv, scratchPadScalars, kvEntry, groupIter);
    if (status == StatusOk) {
        status = registeredFn->evalFn(registeredFn->fnDesc.context,
                                      argc,
                                      argv,
                                      outputScalar);
        if (status == StatusOk) {
            outputScalar->updateUsedSize();
        }
    }

    Operators::get()->updateEvalXdfErrorStats(status, errorStats);

    NewKeyValueEntry dstKvEntry(&outputXdbMeta->kvNamedMeta.kvMeta_);
    const NewTupleMeta *srcTupMeta = kvEntry->kvMeta_->tupMeta_;
    const NewTupleMeta *dstTupMeta = dstKvEntry.kvMeta_->tupMeta_;
    size_t srcNumFields = srcTupMeta->getNumFields();

    for (size_t ii = 0; ii < srcNumFields; ii++) {
        bool retIsValid;
        DfFieldType typeTmp = srcTupMeta->getFieldType(ii);
        DfFieldValue valueTmp =
            kvEntry->tuple_.get(ii, srcNumFields, typeTmp, &retIsValid);
        if (retIsValid) {
            dstKvEntry.tuple_.set(ii, valueTmp, dstTupMeta->getFieldType(ii));
        }
    }

    if (status == StatusOk) {
        DfFieldValue valueTmp;
        valueTmp.scalarVal = outputScalar;
        dstKvEntry.tuple_.set(srcNumFields, valueTmp, DfScalarObj);
    }

    bool retIsValid;
    DfFieldValue key = kvEntry->getKey(&retIsValid);
    status = XdbMgr::get()->xdbInsertKvNoLock(outputXdb,
                                              slotId,
                                              &key,
                                              &dstKvEntry.tuple_,
                                              XdbInsertSlotHash);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
XcalarEval::evalLocalInt(ScalarGroupIter *groupIter,
                         XdbId outputScalarXdbId,
                         XcalarEvalRegisteredFn *registeredFn)
{
    int argc;
    Status status = StatusUnknown;
    Scalar *argv[groupIter->numArgs];
    TableCursor xdbCursor;
    bool xdbCursorInit = false;
    NewKeyValueEntry kvEntry;
    Xdb *srcXdb, *outputXdb;
    XdbMeta *prevXdbMeta, *outputXdbMeta;
    Scalar *outputScalar = NULL;
    DagLib *dagLib = DagLib::get();
    Operators *operators = Operators::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    uint64_t count = 0;
    OpEvalErrorStats *evalErrorStats = NULL;

    evalErrorStats =  // XXX use buffer cache?
        (OpEvalErrorStats *) memAllocExt(sizeof(OpEvalErrorStats), ModuleName);
    BailIfNull(evalErrorStats);

    memset(evalErrorStats, 0, sizeof(OpEvalErrorStats));

    assert(groupIter != NULL);
    assert(outputScalarXdbId != XdbIdInvalid);
    assert(groupIter->cursorType == ScalarLocalCursor);

    OpStatus *opStatus;
    status = (dagLib->getDagLocal(groupIter->dagId))
                 ->getOpStatusFromXdbId(groupIter->dstXdbId, &opStatus);

    if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    // XXX Remove unnecessary xdbGet and use perTxnInfo.
    status = xdbMgr->xdbGet(groupIter->srcXdbId, &srcXdb, &prevXdbMeta);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(prevXdbMeta != NULL);

    // XXX Remove unnecessary xdbGet and use perTxnInfo.
    status = xdbMgr->xdbGet(outputScalarXdbId, &outputXdb, &outputXdbMeta);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(outputXdb != NULL);
    assert(outputXdbMeta != NULL);

    argc = groupIter->numArgs;
    outputScalar = Scalar::allocScalar(DfMaxFieldValueSize);
    if (outputScalar == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xdbMgr->createCursorFast(srcXdb, 0, &xdbCursor);
    if (status != StatusOk && status != StatusNoData) {
        goto CommonExit;
    }
    new (&kvEntry) NewKeyValueEntry(xdbCursor.kvMeta_);
    status = StatusOk;
    xdbCursorInit = true;

    while ((status = xdbCursor.getNext(&kvEntry)) == StatusOk) {
        status = evalAndInsert(argc,
                               argv,
                               NULL,
                               &kvEntry,
                               groupIter,
                               registeredFn,
                               prevXdbMeta,
                               outputXdbMeta,
                               outputXdb,
                               outputScalar,
                               0,
                               evalErrorStats);
        BailIfFailed(status);

        count++;

        if (unlikely(count % XcalarConfig::GlobalStateCheckInterval == 0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }
        }
    }

    operators->updateOpStatusForEval(evalErrorStats,
                                     &opStatus->atomicOpDetails.errorStats
                                          .evalErrorStats);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, count);

    assert(status != StatusOk);
    if (status != StatusNoData) {
        goto CommonExit;
    }
    status = StatusOk;

CommonExit:
    if (evalErrorStats != NULL) {
        memFree(evalErrorStats);
        evalErrorStats = NULL;
    }
    if (xdbCursorInit) {
        CursorManager::get()->destroy(&xdbCursor);
    }

    if (outputScalar != NULL) {
        Scalar::freeScalar(outputScalar);
    }

    return status;
}

Status
XcalarEval::distributeEvalFn(ScalarGroupIter *groupIter,
                             XcalarEvalRegisteredFn *registeredFn,
                             XdbId *scalarXdbIdOut,
                             Xdb **scalarXdbOut,
                             XdbGlobalStateMask scalarTableGlobalState)
{
    XdbId prevScalarXdbId;
    XdbId outputScalarXdbId = XdbIdInvalid;
    Status status = StatusUnknown;
    XdbMeta *prevXdbMeta;
    NewTupleMeta tupMeta;
    unsigned ii;
    MsgXcalarAggregateEvalDistributeEvalFn *msgInput = NULL;
    XdbMgr *xdbMgr = XdbMgr::get();

    assert(groupIter != NULL);
    assert(registeredFn != NULL);
    assert(scalarXdbIdOut != NULL);

    prevScalarXdbId = *scalarXdbIdOut;
    assert(groupIter->srcXdbId == prevScalarXdbId);

    // XXX Remove unnecessary xdbGet and use perTxnInfo.
    status = xdbMgr->xdbGet(prevScalarXdbId, NULL, &prevXdbMeta);
    if (status != StatusOk) {
        goto CommonExit;
    }

    const NewTupleMeta *prevTupMeta;
    prevTupMeta = prevXdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t prevNumFields;
    prevNumFields = prevTupMeta->getNumFields();

    // +1 for the output of evalFn
    tupMeta.setNumFields(prevNumFields + 1);
    for (ii = 0; ii < prevNumFields; ii++) {
        tupMeta.setFieldType(prevTupMeta->getFieldType(ii), ii);
        assert(tupMeta.getFieldType(ii) == DfScalarObj);
    }
    tupMeta.setFieldType(DfScalarObj,
                         ii);  // This is for the output to come

    {
        const char *immediateNames[prevXdbMeta->numImmediates + 1];
        char tempImmediateName[DfMaxFieldNameLen + 1];
        unsigned immIndex = 0;

        for (ii = 0; ii < prevNumFields; ii++) {
            if (prevTupMeta->getFieldType(ii) != DfFatptr) {
                immediateNames[immIndex] =
                    prevXdbMeta->kvNamedMeta.valueNames_[ii];
                immIndex++;
            }
        }

        snprintf(tempImmediateName,
                 sizeof(tempImmediateName),
                 "temporary-%d",
                 immIndex);
        immediateNames[immIndex] = tempImmediateName;

        assert(ArrayLen(immediateNames) == prevXdbMeta->numImmediates + 1);
        outputScalarXdbId = XidMgr::get()->xidGetNext();
        status = xdbMgr->xdbCreate(outputScalarXdbId,
                                   DsDefaultDatasetKeyName,
                                   DfInt64,
                                   NewTupleMeta::DfInvalidIdx,
                                   &tupMeta,
                                   NULL,
                                   0,
                                   immediateNames,
                                   (unsigned) ArrayLen(immediateNames),
                                   NULL,
                                   0,
                                   Random,
                                   scalarTableGlobalState,
                                   DhtInvalidDhtId);
        if (status != StatusOk) {
            // We normally wouldn't do this, but we don't want to leak
            // memory if we were hit by fault injection and the XDB was
            // in fact created.
            if (status == StatusFaultInjection2PC) {
                goto CommonExit;
            }
            outputScalarXdbId = XdbIdInvalid;
            goto CommonExit;
        }
    }

    switch (groupIter->cursorType) {
    case ScalarLocalCursor: {
        assert(registeredFn->fnType == XcalarFnTypeEval);
        if (groupIter->parallelize) {
            status =
                parallelEvalLocal(groupIter, outputScalarXdbId, registeredFn);
        } else {
            status = evalLocalInt(groupIter, outputScalarXdbId, registeredFn);
        }

        if (status != StatusOk) {
            goto CommonExit;
        }

        break;
    }

    case ScalarGlobalCursor: {
        size_t auxiliaryPacketSize;
        size_t msgInputSize;

        auxiliaryPacketSize =
            XcalarEval::get()->computeAuxiliaryPacketSize(groupIter);
        msgInputSize = sizeof(*msgInput) + auxiliaryPacketSize;
        assert(msgInputSize < XdbMgr::bcSize());

        msgInput =
            (MsgXcalarAggregateEvalDistributeEvalFn *) memAllocExt(msgInputSize,
                                                                   ModuleName);
        if (msgInput == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        size_t bytesCopied = 0;
        XcalarEval::get()->packAuxiliaryPacket(groupIter,
                                               msgInput->auxiliaryPacket,
                                               auxiliaryPacketSize,
                                               &bytesCopied);
        assert(bytesCopied == auxiliaryPacketSize);

        msgInput->groupIter = *groupIter;
        msgInput->groupIter.args = NULL;
        msgInput->groupIter.scalarValues = NULL;

        assert(strlen(registeredFn->fnDesc.fnName) < sizeof(msgInput->fnName));
        strlcpy(msgInput->fnName,
                registeredFn->fnDesc.fnName,
                sizeof(msgInput->fnName));

        msgInput->outputScalarXdbId = outputScalarXdbId;
        msgInput->auxiliaryPacketSize = auxiliaryPacketSize;

        Status nodeStatuses[Config::get()->getActiveNodes()];
        MsgEphemeral eph;
        MsgMgr::get()
            ->twoPcEphemeralInit(&eph,
                                 msgInput,
                                 msgInputSize,
                                 0,
                                 TwoPcSlowPath,
                                 TwoPcCallId::
                                     Msg2pcXcalarAggregateEvalDistributeEvalFn1,
                                 nodeStatuses,
                                 TwoPcZeroCopyInput);

        TwoPcHandle twoPcHandle;
        status =
            MsgMgr::get()
                ->twoPc(&twoPcHandle,
                        MsgTypeId::Msg2pcXcalarAggregateEvalDistributeEvalFn,
                        TwoPcDoNotReturnHandle,
                        &eph,
                        (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                           MsgRecvHdrOnly),
                        TwoPcSyncCmd,
                        TwoPcAllNodes,
                        TwoPcIgnoreNodeId,
                        TwoPcClassNonNested);
        BailIfFailed(status);
        assert(!twoPcHandle.twoPcHandle);

        for (ii = 0; ii < ArrayLen(nodeStatuses); ii++) {
            if (nodeStatuses[ii] != StatusOk) {
                status = nodeStatuses[ii];
                goto CommonExit;
            }
        }
        break;
    }
    case ScalarSameKeyCursor:  // Fall through. Should not see SameKeyCursor
                               // here
    default:
        assert(0);
        break;
    }

    assert(status == StatusOk);
    assert(prevScalarXdbId != XdbIdInvalid);
    assert(outputScalarXdbId != XdbIdInvalid);

    if (!prevXdbMeta->isScratchPadXdb) {
        // don't drop scratch xdbs
        xdbMgr->xdbDrop(prevScalarXdbId);
    }
    *scalarXdbIdOut = outputScalarXdbId;
    // XXX Remove unnecessary xdbGet and use perTxnInfo.
    status = xdbMgr->xdbGet(outputScalarXdbId, scalarXdbOut, NULL);
    assert(status == StatusOk);

CommonExit:
    if (outputScalarXdbId != XdbIdInvalid) {
        Status loadDoneStatus = xdbMgr->xdbLoadDone(outputScalarXdbId);
        if (status == StatusOk && loadDoneStatus != StatusOk) {
            status = loadDoneStatus;
        }
    }

    if (msgInput != NULL) {
        memFree(msgInput);
        msgInput = NULL;
    }

    if (status != StatusOk) {
        if (outputScalarXdbId != XdbIdInvalid) {
            xdbMgr->xdbDrop(outputScalarXdbId);
        }
    }

    return status;
}

void
XcalarEval::aggregateMsgDistributeEvalFn(MsgEphemeral *eph, void *payload)
{
    MsgXcalarAggregateEvalDistributeEvalFn *msgInput;
    Scalar **scalarValues = NULL;
    ScalarRecordArgument *args;
    int *argMapping;
    Status status = StatusUnknown;
    ScalarGroupIter groupIter;
    XcalarEvalRegisteredFn *registeredFn = NULL;
    XdbMeta *srcMeta;
    Xdb *srcXdb;

    msgInput = (MsgXcalarAggregateEvalDistributeEvalFn *) payload;

    registeredFn = getOrResolveRegisteredFn(msgInput->fnName, NULL, NULL);
    assert(registeredFn != NULL);
    assert(registeredFn->fnType == XcalarFnTypeEval);

    size_t bytesProcessed = 0;
    scalarValues =
        (Scalar **) memAllocExt(sizeof(*scalarValues) *
                                    msgInput->groupIter.numScalarValues,
                                ModuleName);
    if (scalarValues == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // XXX Remove unnecessary xdbGet and use perTxnInfo.
    status =
        XdbMgr::get()->xdbGet(msgInput->groupIter.srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    XcalarEval::get()->unpackAuxiliaryPacket(&msgInput->groupIter,
                                             msgInput->auxiliaryPacket,
                                             &bytesProcessed,
                                             scalarValues,
                                             &args,
                                             &argMapping);

    groupIter.setInput(ScalarLocalCursor,
                       msgInput->groupIter.srcXdbId,
                       true,
                       srcXdb,
                       srcMeta,
                       argMapping,
                       NULL,
                       false);

    groupIter.numScalarValues = msgInput->groupIter.numScalarValues;
    groupIter.scalarValues = scalarValues;
    groupIter.numArgs = msgInput->groupIter.numArgs;
    groupIter.args = args;
    groupIter.enforceArgCheck = msgInput->groupIter.enforceArgCheck;
    groupIter.dstXdbId = msgInput->groupIter.dstXdbId;
    groupIter.dagId = msgInput->groupIter.dagId;

    status = parallelEvalLocal(&groupIter,
                               msgInput->outputScalarXdbId,
                               registeredFn);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (registeredFn != NULL) {
        refPut(&registeredFn->refCount);
        registeredFn = NULL;
    }

    if (scalarValues != NULL) {
        memFree(scalarValues);
        scalarValues = NULL;
    }

    eph->setAckInfo(status, 0);
}

Status
XcalarEval::aggregateEvalInt(XdbId *scalarXdbId,
                             Xdb **scalarXdb,
                             XdbGlobalStateMask scalarTableGlobalState,
                             XcalarEvalAstOperatorNode *operatorNode,
                             ScalarGroupIter *groupIter,
                             OpStatus *opStatus)
{
    unsigned ii;
    Status status = StatusUnknown;
    XcalarEvalRegisteredFn *registeredFn;
    bool takesInGroupArg;
    XdbMgr *xdbMgr = XdbMgr::get();
    unsigned numScalarArgs, numGroupArgs;
    int *argMapping = operatorNode->argMapping;
    assert(argMapping != NULL);

    numScalarArgs = numGroupArgs = 0;

    assert(scalarXdbId != NULL);
    assert(operatorNode != NULL);

    // Start by evaluating the inner-most and left-most operator and figure
    // out if the operatorNode takes in groupArgs
    takesInGroupArg = false;
    for (ii = 0; ii < operatorNode->numArgs; ii++) {
        if (isOperatorNode(operatorNode->arguments[ii]->nodeType)) {
            status = aggregateEvalInt(scalarXdbId,
                                      scalarXdb,
                                      scalarTableGlobalState,
                                      (XcalarEvalAstOperatorNode *)
                                          operatorNode->arguments[ii],
                                      groupIter,
                                      opStatus);
            if (status != StatusOk) {
                goto CommonExit;
            }
        }

        if (isGroupArg(operatorNode->arguments[ii]->nodeType,
                       operatorNode->arguments[ii]->token)) {
            numGroupArgs++;
            takesInGroupArg = true;
        } else {
            numScalarArgs++;
        }
    }

    groupIter->numScalarValues = numScalarArgs;
    groupIter->numArgs = operatorNode->numArgs;

    assert(groupIter->numScalarValues <= groupIter->maxScalarValues);
    assert(groupIter->numArgs <= groupIter->maxArgs);

    registeredFn = operatorNode->registeredFn;
    assert(registeredFn != NULL);

    if (!takesInGroupArg && registeredFn->fnType == XcalarFnTypeEval) {
        // The only case where it's just a simple eval
        status = evalInternal(operatorNode, false);
        if (status != StatusOk) {
            goto CommonExit;
        }

    } else {
        assert(*scalarXdbId != XdbIdInvalid);
        unsigned scalarValueIdx;
        registeredFn = operatorNode->registeredFn;

        XdbMeta *scalarMeta;
        if (unlikely(*scalarXdb == NULL)) {
            // XXX Remove unnecessary xdbGet and use perTxnInfo.
            status = xdbMgr->xdbGet(*scalarXdbId, scalarXdb, &scalarMeta);
            BailIfFailed(status);
        } else {
            scalarMeta = xdbMgr->xdbGetMeta(*scalarXdb);
        }

        if (scalarTableGlobalState & XdbGlobal) {
            groupIter->setInput(ScalarGlobalCursor,
                                *scalarXdbId,
                                true,
                                *scalarXdb,
                                scalarMeta,
                                argMapping,
                                NULL,
                                true);
        } else {
            groupIter->setInput(ScalarLocalCursor,
                                *scalarXdbId,
                                false,
                                *scalarXdb,
                                scalarMeta,
                                argMapping,
                                opStatus,
                                true);
        }

        groupIter->enforceArgCheck = registeredFn->enforceArgCheck;
        assert(operatorNode->numArgs == groupIter->numArgs);
        scalarValueIdx = 0;
        for (ii = 0; ii < operatorNode->numArgs; ii++) {
            groupIter->args[ii].typesAccepted =
                registeredFn->fnDesc.argDescs[ii].typesAccepted;
            groupIter->args[ii].isSingletonValue =
                registeredFn->fnDesc.argDescs[ii].isSingletonValue;
            if (isGroupArg(operatorNode->arguments[ii]->nodeType,
                           operatorNode->arguments[ii]->token)) {
                groupIter->args[ii].argType = ScalarArgFromTable;
                groupIter->args[ii].valueArrayIdx =
                    operatorNode->arguments[ii]->valueArrayIdx;
            } else {
                Scalar *scalarValue;

                assert(groupIter->scalarValues != NULL);
                assert(scalarValueIdx < groupIter->numScalarValues);
                scalarValue = *(operatorNode->arguments[ii]->varContent);
                groupIter->scalarValues[scalarValueIdx] = scalarValue;
                groupIter->args[ii].argType = ScalarArgFromScalarArray;
                groupIter->args[ii].scalarValueIdx = scalarValueIdx;
                scalarValueIdx++;

                if (!(groupIter->args[ii].typesAccepted &
                      (1 << scalarValue->fieldType))) {
                    status = StatusXdfTypeUnsupported;
                    goto CommonExit;
                }
            }
        }

        assert(scalarValueIdx == groupIter->numScalarValues);

        switch (registeredFn->fnType) {
        case XcalarFnTypeEval: {
            // Output is another group and saved in new updated scalarXdbId
            status = distributeEvalFn(groupIter,
                                      registeredFn,
                                      scalarXdbId,
                                      scalarXdb,
                                      scalarTableGlobalState);
            if (status != StatusOk) {
                goto CommonExit;
            }

            break;
        }
        case XcalarFnTypeAgg: {
            // Output is s scalar
            Scalar *scalarOut;

            assert(operatorNode->common.nodeType ==
                   XcalarEvalAstScalarOperator);
            scalarOut = *operatorNode->common.varContent;
            status = registeredFn->aggFn(groupIter, scalarOut);
            if (status != StatusOk) {
                goto CommonExit;
            }

            break;
        }
        }
    }

    assert(status == StatusOk);

CommonExit:
    return status;
}

void
XcalarEval::aggregateMsgPopulateStatus(MsgEphemeral *ephemeral, void *payload)
{
    Status *nodeStatuses;
    nodeStatuses = (Status *) ephemeral->ephemeral;
    nodeStatuses[MsgMgr::get()->getMsgDstNodeId(ephemeral)] = ephemeral->status;
}

bool
XcalarEval::isComplex(XcalarEvalAstOperatorNode *operatorNode)
{
    for (unsigned ii = 0; ii < operatorNode->numArgs; ii++) {
        if (isOperatorNode(operatorNode->arguments[ii]->nodeType)) {
            return true;
        }
    }
    return false;
}

Status
XcalarEval::aggregateEval(XcalarEvalClass2Ast *ast,
                          Scalar *scalarOut,
                          OpStatus *opStatus,
                          Xdb *scratchXdb)
{
    Status status = StatusUnknown;
    Scalar *scalar;

    assert(ast != NULL);
    assert(scalarOut != NULL);

    for (unsigned ii = 0; ii < ast->astCommon.numScalarVariables; ii++) {
        if (ast->astCommon.scalarVariables[ii].content == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "\"%s\" is not defined while doing xcalarAggregateEval",
                    ast->astCommon.scalarVariables[ii].variableName);
            status = StatusEvalUnsubstitutedVariables;
            goto CommonExit;
        }
    }

    status = aggregateEvalInt(&ast->scalarXdbId,
                              &scratchXdb,
                              ast->scalarTableGlobalState,
                              ast->astCommon.rootNode,
                              &ast->groupIter,
                              opStatus);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(ast->astCommon.rootNode->common.nodeType ==
           XcalarEvalAstScalarOperator);
    scalar = *(ast->astCommon.rootNode->common.varContent);

    status = scalarOut->copyFrom(scalar);
    BailIfFailed(status);

    if (getOutputType(&ast->astCommon) != DfScalarObj &&
        scalar->fieldType != getOutputType(&ast->astCommon)) {
        status = scalarOut->convertType(getOutputType(&ast->astCommon));
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

const char *
XcalarEvalFuncListInfo::getFnName() const
{
    return fnDesc.fnName;
}

void
XcalarEvalFuncListInfo::del()
{
    memFree(this);
}

Status
XcalarEval::getFnList(const char *fnNamePattern,
                      const char *categoryPattern,
                      XcalarApiUdfContainer *udfContainer,
                      XcalarApiOutput **outputOut,
                      size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiListXdfsOutput *listXdfsOutput = NULL;
    unsigned numFnsMatched = 0, ii = 0;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    XcalarEvalRegisteredFn *registeredFn;
    XcalarEvalFuncListHashTable funcListHt;
    XcalarEvalFuncListInfo *funcListInfo = NULL;
    int ret;
    char expandedNamePattern[XcalarEvalMaxFnNameLen + 1];
    bool locked = false;

    assert(fnNamePattern != NULL);
    assert(categoryPattern != NULL);
    assert(outputSizeOut != NULL);

    if (udfContainer != NULL) {
        // Need to expand name pattern to include user/sessionId
        status =
            UserDefinedFunction::get()->getUdfName(expandedNamePattern,
                                                   sizeof(expandedNamePattern),
                                                   (char *) fnNamePattern,
                                                   udfContainer,
                                                   false);
        BailIfFailed(status);
    } else {
        // XXX: is this ok? probably just call getUdfName() directly for
        // both cases: udfContainer NULL or not...shouldn't matter. If it is
        // NULL, we should be looking in Global UDF namespace, and following
        // will not have the Global UDF prefix... or assert that
        // udfContainer must be non-NULL.
        strlcpy(expandedNamePattern,
                fnNamePattern,
                sizeof(expandedNamePattern));
    }

    registeredFnsLock_.lock();
    locked = true;
    for (FnHashTable::iterator it = registeredFns_.begin();
         (registeredFn = it.get()) != NULL;
         it.next()) {
        if (funcListInfo == NULL) {
            funcListInfo = (XcalarEvalFuncListInfo *)
                memAllocExt(sizeof(XcalarEvalFuncListInfo), ModuleName);
            if (funcListInfo == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
        }
        memcpy(&funcListInfo->fnDesc,
               &registeredFn->fnDesc,
               sizeof(XcalarEvalFnDesc));

        // Now a UDF is expected to be versioned to handle Transaction
        // recovery of updates. So we need to strip out the version before
        // presenting to user. Also naturally, once the version is stripped,
        // there may be multiple copies and this needs to be uniquiefied.
        if (!registeredFn->isBuiltIn) {
            char fnNameTmp[XcalarEvalMaxFnNameLen + 1];
            const char *moduleNameTmp;
            const char *moduleVersionTmp;
            const char *funcNameTmp;

            memcpy(fnNameTmp, &registeredFn->fnDesc.fnName, sizeof(fnNameTmp));

            status = UserDefinedFunction::parseFunctionName(fnNameTmp,
                                                            &moduleNameTmp,
                                                            &moduleVersionTmp,
                                                            &funcNameTmp);
            if (status != StatusOk) {
                goto CommonExit;
            }

            ret = snprintf(funcListInfo->fnDesc.fnName,
                           sizeof(registeredFn->fnDesc.fnName),
                           "%s:%s",
                           moduleNameTmp,
                           funcNameTmp);
            if (ret >= (int) sizeof(registeredFn->fnDesc.fnName)) {
                status = StatusNameTooLong;
                goto CommonExit;
            }
        }

        if (!strMatch(expandedNamePattern, funcListInfo->fnDesc.fnName) ||
            !strMatch(categoryPattern,
                      strGetFromFunctionCategory(
                          funcListInfo->fnDesc.category))) {
            continue;
        }

        status = funcListHt.insert(funcListInfo);
        if (status == StatusExist) {
            // Function already exists because with UDFs we expect multiple
            // versions. So move on. Reuse funcListInfo memory allocated
            // above in the next iteration.
            assert(!registeredFn->isBuiltIn);
            status = StatusOk;
        } else {
            assert(status == StatusOk);
            numFnsMatched++;
            funcListInfo = NULL;
        }
    }
    assert(locked);
    registeredFnsLock_.unlock();
    locked = false;

    if (funcListInfo != NULL) {
        memFree(funcListInfo);
        funcListInfo = NULL;
    }

    outputSize = XcalarApiSizeOfOutput(*listXdfsOutput) +
                 (sizeof(listXdfsOutput->fnDescs[0]) * numFnsMatched);
    output = (XcalarApiOutput *) memAllocExt(outputSize, ModuleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    listXdfsOutput = &output->outputResult.listXdfsOutput;
    for (XcalarEvalFuncListHashTable::iterator it = funcListHt.begin();
         (funcListInfo = it.get()) != NULL;
         it.next(), ii++) {
        assert(ii < numFnsMatched);
        listXdfsOutput->fnDescs[ii] = funcListInfo->fnDesc;
    }
    listXdfsOutput->numXdfs = numFnsMatched;

    status = StatusOk;
    *outputOut = output;
    *outputSizeOut = outputSize;

CommonExit:

    if (locked) {
        registeredFnsLock_.unlock();
        locked = false;
    }

    if (funcListInfo != NULL) {
        memFree(funcListInfo);
    }
    funcListHt.removeAll(&XcalarEvalFuncListInfo::del);

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
    }

    return status;
}

Status
XcalarEval::getArgs(Scalar *argv[],
                    Scalar **scratchPadScalars,
                    NewKeyValueEntry *kvEntry,
                    ScalarGroupIter *groupIter)
{
    Status status = StatusOk;
    unsigned ii;
    const NewTupleMeta *srcTupMeta =
        groupIter->srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t srcNumFields = srcTupMeta->getNumFields();

    assert(groupIter != NULL);
    assert(argv != NULL);
    assert(kvEntry != NULL);

    for (ii = 0; ii < groupIter->numArgs; ii++) {
        switch (groupIter->args[ii].argType) {
        case ScalarArgFromTable: {
            unsigned valueArrayIdx;
            if (groupIter->argMapping[ii] == ArgMappingInvalid) {
                valueArrayIdx = groupIter->args[ii].valueArrayIdx;
            } else {
                valueArrayIdx = groupIter->argMapping[ii];
            }

            bool retIsValid;
            DfFieldType typeTmp;
            if (groupIter->scalarFieldType) {
                typeTmp = DfScalarObj;
            } else {
                typeTmp = srcTupMeta->getFieldType(valueArrayIdx);
            }
            DfFieldValue valueTmp = kvEntry->tuple_.get(valueArrayIdx,
                                                        srcNumFields,
                                                        typeTmp,
                                                        &retIsValid);
            if (retIsValid) {
                if (typeTmp == DfScalarObj) {
                    argv[ii] = valueTmp.scalarVal;
                } else {
                    status = scratchPadScalars[ii]->setValue(valueTmp, typeTmp);
                    BailIfFailed(status);

                    argv[ii] = scratchPadScalars[ii];
                }
            } else {
                argv[ii] = NULL;

                if (groupIter->enforceArgCheck) {
                    status = StatusEvalUnsubstitutedVariables;
                    break;
                }
            }
            break;
        }
        case ScalarArgFromScalarArray: {
            unsigned scalarValueIdx;
            scalarValueIdx = groupIter->args[ii].scalarValueIdx;
            assert(groupIter->numScalarValues > scalarValueIdx);
            argv[ii] = groupIter->scalarValues[scalarValueIdx];
            break;
        }
        }

        if (argv[ii] != NULL && groupIter->enforceArgCheck &&
            ((!(groupIter->args[ii].typesAccepted &
                (1 << argv[ii]->fieldType))) ||
             (groupIter->args[ii].isSingletonValue &&
              argv[ii]->fieldNumValues > 1))) {
            status = StatusXdfTypeUnsupported;
        }
    }

CommonExit:
    return status;
}

size_t
XcalarEval::computeAuxiliaryPacketSize(ScalarGroupIter *groupIter)
{
    size_t scalarValuesSize;
    unsigned ii;

    scalarValuesSize = 0;
    for (ii = 0; ii < groupIter->numScalarValues; ii++) {
        scalarValuesSize += sizeof(*groupIter->scalarValues[ii]) +
                            groupIter->scalarValues[ii]->fieldAllocedSize;
    }

    return scalarValuesSize + (sizeof(*groupIter->args) * groupIter->numArgs) +
           (sizeof(*groupIter->argMapping) * groupIter->numArgs);
}

void
XcalarEval::packAuxiliaryPacket(ScalarGroupIter *groupIter,
                                uint8_t *auxiliaryPacket,
                                size_t auxiliaryPacketSize,
                                size_t *bytesCopiedOut)
{
    unsigned ii;
    size_t bytesCopied = 0;

    assert(groupIter != NULL);
    assert(auxiliaryPacket != NULL);
    assert(bytesCopiedOut != NULL);

    // 1. We populate scalarValuesArray
    for (ii = 0; ii < groupIter->numScalarValues; ii++) {
        size_t scalarValueSize;
        scalarValueSize = groupIter->scalarValues[ii]->getTotalSize();
        assert(auxiliaryPacketSize >= scalarValueSize + bytesCopied);
        memcpy(&auxiliaryPacket[bytesCopied],
               groupIter->scalarValues[ii],
               scalarValueSize);
        bytesCopied += scalarValueSize;
    }

    // 2. We populate args
    size_t argSize = sizeof(*groupIter->args) * groupIter->numArgs;
    assert(auxiliaryPacketSize >= bytesCopied + argSize);
    memcpy(&auxiliaryPacket[bytesCopied], groupIter->args, argSize);
    bytesCopied += argSize;

    // 3. populate argMapping
    size_t argMappingSize = sizeof(*groupIter->argMapping) * groupIter->numArgs;
    assert(auxiliaryPacketSize >= bytesCopied + argMappingSize);
    memcpy(&auxiliaryPacket[bytesCopied],
           groupIter->argMapping,
           argMappingSize);
    bytesCopied += argMappingSize;

    *bytesCopiedOut = bytesCopied;
}

void
XcalarEval::unpackAuxiliaryPacket(ScalarGroupIter *groupIter,
                                  uint8_t *auxiliaryPacket,
                                  size_t *bytesProcessedOut,
                                  Scalar *scalarValues[],
                                  ScalarRecordArgument **argsOut,
                                  int **argMappingOut)
{
    unsigned ii;
    size_t scalarValueSize;
    size_t bytesProcessed = 0;
    ScalarRecordArgument *args;
    int *argMapping;

    // 1. Get scalarValues from auxiliary packet
    for (ii = 0; ii < groupIter->numScalarValues; ii++) {
        Scalar *scalarValue;
        scalarValue = (Scalar *) &auxiliaryPacket[bytesProcessed];
        scalarValueSize = scalarValue->getTotalSize();

        xSyslog(ModuleName, XlogDebug, "ScalarValueSize: %lu", scalarValueSize);
        scalarValues[ii] = scalarValue;
        bytesProcessed += scalarValueSize;
    }

    // 2. Get args
    args = (ScalarRecordArgument *) &auxiliaryPacket[bytesProcessed];
    bytesProcessed += sizeof(*args) * (groupIter->numArgs);
    *argsOut = args;

    // 3. Get argMapping
    argMapping = (int *) &auxiliaryPacket[bytesProcessed];
    bytesProcessed += sizeof(*argMapping) * (groupIter->numArgs);

    *bytesProcessedOut = bytesProcessed;
    *argMappingOut = argMapping;
}

bool
XcalarEval::containsUdf(XcalarEvalAstOperatorNode *node)
{
    if (!node->registeredFn->isBuiltIn) {
        return true;
    }

    for (unsigned ii = 0; ii < node->numArgs; ii++) {
        if (node->arguments[ii]->nodeType != XcalarEvalAstOperator) {
            continue;
        }

        if (XcalarEval::get()->containsUdf(
                (XcalarEvalAstOperatorNode *) node->arguments[ii])) {
            return true;
        }
    }

    return false;
}

bool
XcalarEval::containsUdf(char *evalString)
{
    XcalarEvalClass1Ast ast;
    bool astInit = false, containsUdfBool = false;

    // check that evalstring is not NULL and not an empty string
    if (evalString != NULL && evalString[0] != '\0') {
        Status status = generateClass1Ast(evalString, &ast);
        BailIfFailed(status);
        astInit = true;
        containsUdfBool = containsUdf(ast.astCommon.rootNode);
    }

CommonExit:
    if (astInit) {
        destroyClass1Ast(&ast);
        astInit = false;
    }

    return containsUdfBool;
}

bool
XcalarEval::containsAggregateVar(char *evalString)
{
    XcalarEvalClass1Ast ast;
    bool astInit = false, containsAgg = false;
    Status status = generateClass1Ast(evalString, &ast);
    if (status != StatusOk) {
        return false;
    }
    astInit = true;

    for (unsigned ii = 0; ii < ast.astCommon.numScalarVariables; ii++) {
        if (*ast.astCommon.scalarVariables[ii].variableName ==
            OperatorsAggregateTag) {
            containsAgg = true;
            goto CommonExit;
        }
    }

CommonExit:
    if (astInit) {
        destroyClass1Ast(&ast);
        astInit = false;
    }

    return containsAgg;
}

Status
XcalarEval::insertIntoEvalUdfModuleSet(EvalUdfModuleSet *modules,
                                       const char *fullyQualifiedFnName,
                                       uint64_t *numUdfModules)
{
    Status status;
    const char *moduleName;
    const char *moduleVersion;
    const char *functionName;

    EvalUdfModule *module =
        (EvalUdfModule *) memAlloc(sizeof(*module));  // XXX use buffer cache?
    BailIfNull(module);

    if (strlcpy(module->moduleName,
                fullyQualifiedFnName,
                sizeof(module->moduleName)) >= sizeof(module->moduleName)) {
        assert(false);
        status = StatusOverflow;
        goto CommonExit;
    }

    status = UserDefinedFunction::parseFunctionName(module->moduleName,
                                                    &moduleName,
                                                    &moduleVersion,
                                                    &functionName);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (modules->find(moduleName) != NULL) {
        // Already inserted. Free in CommonExit.
        status = StatusOk;
        goto CommonExit;
    }

    verifyOk(modules->insert(module));
    if (numUdfModules != NULL) {
        *numUdfModules += 1;
    }
    module = NULL;
    status = StatusOk;

CommonExit:
    if (module != NULL) {
        memFree(module);
    }
    return status;
}

// Seems jarring initially that udfLoadArgs is processed by XcalarEval.
// But in future, even load should be able to take in a XcalarEval statement
Status
XcalarEval::getUdfModuleFromLoad(const char *fqUdfName,
                                 EvalUdfModuleSet *modules,
                                 uint64_t *numUdfModules)
{
    return insertIntoEvalUdfModuleSet(modules, fqUdfName, numUdfModules);
}

// Get a collection of all the UDF modules invoked by this AST. A hashtable
// is needed here for deduplication. This is not fast path, it happens once
// per map/filter per childnode created.
// Uses of this data structure assumed to be single threaded.
Status
XcalarEval::getUdfModules(XcalarEvalAstOperatorNode *node,
                          EvalUdfModuleSet *modules,
                          uint64_t *numUdfModules)
{
    Status status = StatusUnknown;

    if (!node->registeredFn->isBuiltIn) {
        status = insertIntoEvalUdfModuleSet(modules,
                                            node->registeredFn->fnDesc.fnName,
                                            numUdfModules);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    for (unsigned ii = 0; ii < node->numArgs; ii++) {
        if (node->arguments[ii]->nodeType != XcalarEvalAstOperator) {
            continue;
        }

        status = XcalarEval::get()->getUdfModules((XcalarEvalAstOperatorNode *)
                                                      node->arguments[ii],
                                                  modules,
                                                  numUdfModules);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    status = StatusOk;

CommonExit:
    return status;
}

void
EvalUdfModule::del()
{
    memFree(this);
}

Status
XcalarEval::getUniqueVariablesList(
    XcalarEvalAstOperatorNode *operatorNodeParent,
    const char *variableNames[],
    unsigned maxNumVariables,
    unsigned *variableIdx)
{
    unsigned ii, jj;
    XcalarEvalAstNode *regularNode;
    XcalarEvalAstOperatorNode *operatorNode;
    Status status = StatusUnknown;
    bool variableExists = false;

    for (ii = 0; ii < operatorNodeParent->numArgs; ii++) {
        switch (operatorNodeParent->arguments[ii]->nodeType) {
        case XcalarEvalAstGroupOperator:   // Fall through
        case XcalarEvalAstScalarOperator:  // Fall through
        case XcalarEvalAstOperator:
            operatorNode =
                (XcalarEvalAstOperatorNode *) operatorNodeParent->arguments[ii];
            status = getUniqueVariablesList(operatorNode,
                                            variableNames,
                                            maxNumVariables,
                                            variableIdx);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        case XcalarEvalAstVariable:
            regularNode = operatorNodeParent->arguments[ii];
            variableExists = false;
            for (jj = 0; jj < *variableIdx; jj++) {
                // strcmp is slow. If numVariables > 1024, consider using
                // hashStringFast
                if (strcmp(variableNames[jj], regularNode->token) == 0) {
                    variableExists = true;
                    break;
                }
            }

            if (variableExists) {
                break;
            }

            if (*variableIdx >= maxNumVariables) {
                status = StatusNoBufs;
                goto CommonExit;
            }
            variableNames[*variableIdx] = regularNode->token;
            (*variableIdx)++;
            break;
        default:
            // Do nothing
            break;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
XcalarEval::getUniqueVariablesList(XcalarEvalAstCommon *ast,
                                   const char *variableNames[],
                                   unsigned maxNumVariables,
                                   unsigned *numUniqueVariablesOut)
{
    unsigned numUniqueVariables = 0;
    Status status = StatusUnknown;

    status = getUniqueVariablesList(ast->rootNode,
                                    variableNames,
                                    maxNumVariables,
                                    &numUniqueVariables);
    if (status != StatusOk) {
        numUniqueVariables = 0;
    }
    *numUniqueVariablesOut = numUniqueVariables;
    return status;
}

// A fatal xcalarEvalError is one where the entire operation
// must be aborted (e.g. failure to even parse the xcalarEval statement)
// A nonfatal xcalarEvalERror is one where only the row is bad
// and we can proceed by ignoring the row
bool
XcalarEval::isFatalError(Status status)
{
    // Maybe we can collect stats per error we receive here?
    return !(
        status == StatusOk || status == StatusInval ||
        status == StatusDfTypeMismatch || status == StatusXdfTypeUnsupported ||
        status == StatusEvalUnsubstitutedVariables ||
        status == StatusXdfMixedTypeNotSupported ||
        status == StatusEvalCastError || status == StatusXdfDivByZero ||
        status == StatusXdfFloatNan || status == StatusDfFieldTypeUnsupported ||
        status == StatusStrEncodingNotSupported ||
        status == StatusUdfExecuteFailed || status == StatusUdfInval ||
        status == StatusUdfUnsupportedType || status == StatusUdfPyConvert ||
        status == StatusUdfFunctionTooFewParams ||
        status == StatusAggregateNoSuchField || status == StatusOverflow ||
        status == StatusScalarFunctionFieldOverflow ||
        status == StatusDfpError);
}

// Sometimes, we give scalar references (as opposed to scalar copies)
// to the AST. We just want to drop these references, instead of actually
// performing a freeScalar
void
XcalarEval::dropScalarVarRef(XcalarEvalAstCommon *ast)
{
    for (unsigned ii = 0; ii < ast->numScalarVariables; ii++) {
        ast->scalarVariables[ii].content = NULL;
    }
    ast->numScalarVariables = 0;
}

void  // static
XcalarEval::aggregateMsgDistributeEvalFnWrapper(MsgEphemeral *ephemeral,
                                                void *payload)
{
    get()->aggregateMsgDistributeEvalFn(ephemeral, payload);
}

void  // static
XcalarEval::aggregateMsgPopulateStatusWrapper(MsgEphemeral *ephemeral,
                                              void *payload)
{
    get()->aggregateMsgPopulateStatus(ephemeral, payload);
}

Status
XcalarEval::updateArgMappings(XcalarEvalAstOperatorNode *operatorNode,
                              XdbMeta *xdbMeta)
{
    Status status = StatusOk;

    if (operatorNode->argMapping != NULL) {
        memFree(operatorNode->argMapping);
        operatorNode->argMapping = NULL;
    }

    operatorNode->argMapping =
        (int *) memAlloc(operatorNode->numArgs * sizeof(int));
    BailIfNull(operatorNode->argMapping);

    makeArgMapping(xdbMeta,
                   (XcalarEvalAstNode **) operatorNode->arguments,
                   operatorNode->numArgs,
                   operatorNode->argMapping);

    for (unsigned ii = 0; ii < operatorNode->numArgs; ii++) {
        if (isOperatorNode(operatorNode->arguments[ii]->nodeType)) {
            status = updateArgMappings((XcalarEvalAstOperatorNode *)
                                           operatorNode->arguments[ii],
                                       xdbMeta);
            if (status != StatusOk) {
                goto CommonExit;
            }
        }
    }

CommonExit:
    return status;
}

void
XcalarEval::makeArgMapping(XdbMeta *xdbMeta,
                           XcalarEvalAstNode **args,
                           unsigned numArgs,
                           int *argMapping)
{
    const NewTupleMeta *tupMeta = xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t numFields = tupMeta->getNumFields();

    for (unsigned ii = 0; ii < numArgs; ii++) {
        argMapping[ii] = ArgMappingInvalid;

        if (args[ii]->nodeType != XcalarEvalAstVariable) {
            continue;
        }

        for (unsigned jj = 0; jj < numFields; jj++) {
            // skip fatptr type as derived field can have the
            // same name as that of fatptr
            if ((tupMeta->getFieldType(jj) != DfFatptr) &&
                (strcmp(args[ii]->token,
                        xdbMeta->kvNamedMeta.valueNames_[jj]) == 0)) {
                argMapping[ii] = jj;
                break;
            }
        }
        assert(argMapping[ii] != ArgMappingInvalid);
    }
}

XdfTypesAccepted
XcalarEval::convertToTypesAccepted(DfFieldType type)
{
    if (type == DfUnknown || type == DfScalarObj) {
        return XdfAcceptAll;
    } else {
        return (XdfTypesAccepted)(1 << type);
    }
}

// parses the possible types accepted and outputs the best field type that
// matches the criteria
DfFieldType
XcalarEval::getBestFieldType(XdfTypesAccepted typesAccepted)
{
    DfFieldType type;
    switch (typesAccepted) {
    case XdfAcceptString:
        type = DfString;
        break;
    case XdfAcceptFloat:
    case XdfAcceptFloat64:
        type = DfFloat64;
        break;
    case XdfAcceptInt:
    case XdfAcceptInt64:
        type = DfInt64;
        break;
    case XdfAcceptBoolean:
        type = DfBoolean;
        break;
    default:
        type = DfScalarObj;
        break;
    }

    return type;
}

Status
XcalarEval::setupEvalAst(const char *evalString,
                         XdbMeta *dstMeta,
                         XcalarEvalClass1Ast &ast,
                         unsigned &numVariables,
                         int *&argIndices,
                         Scalar **&scalars,
                         DagTypes::DagId dagId)
{
    Status status;
    const char *valueNames[TupleMaxNumValuesPerRecord];
    const NewTupleMeta *tupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    unsigned numFields = tupMeta->getNumFields();

    for (unsigned ii = 0; ii < numFields; ii++) {
        if (tupMeta->getFieldType(ii) == DfFatptr) {
            valueNames[ii] = "";
        } else {
            valueNames[ii] = dstMeta->kvNamedMeta.valueNames_[ii];
        }
    }

    status = setupEvalAst(evalString,
                          numFields,
                          valueNames,
                          ast,
                          numVariables,
                          argIndices,
                          scalars,
                          dagId);

    return status;
}

Status
XcalarEval::setupEvalMultiAst(const char *evalString,
                              XdbMeta *leftMeta,
                              XdbMeta *rightMeta,
                              XcalarEvalClass1Ast &ast,
                              unsigned &numVariables,
                              int *&argIndices,
                              Scalar **&scalars,
                              XcalarApiRenameMap *renameMap,
                              unsigned numRenameEntriesLeft,
                              unsigned numRenameEntriesRight,
                              DagTypes::DagId dagId)
{
    Status status;
    const char *valueNames[TupleMaxNumValuesPerRecord * 2];
    unsigned jj = 0;

    const NewTupleMeta *tupMeta = leftMeta->kvNamedMeta.kvMeta_.tupMeta_;
    unsigned numFields = tupMeta->getNumFields();
    for (unsigned ii = 0; ii < tupMeta->getNumFields(); ii++) {
        if (tupMeta->getFieldType(ii) == DfFatptr) {
            valueNames[jj] = "";
        } else {
            valueNames[jj] =
                Operators::columnNewName(renameMap,
                                         numRenameEntriesLeft,
                                         leftMeta->kvNamedMeta.valueNames_[ii],
                                         tupMeta->getFieldType(ii));
        }
        ++jj;
    }

    tupMeta = rightMeta->kvNamedMeta.kvMeta_.tupMeta_;
    numFields += tupMeta->getNumFields();
    for (unsigned ii = 0; ii < tupMeta->getNumFields(); ii++) {
        if (tupMeta->getFieldType(ii) == DfFatptr) {
            valueNames[jj] = "";
        } else {
            valueNames[jj] =
                Operators::columnNewName(renameMap + numRenameEntriesLeft,
                                         numRenameEntriesRight,
                                         rightMeta->kvNamedMeta.valueNames_[ii],
                                         tupMeta->getFieldType(ii));
        }
        ++jj;
    }

    status = setupEvalAst(evalString,
                          numFields,
                          valueNames,
                          ast,
                          numVariables,
                          argIndices,
                          scalars,
                          dagId);

    return status;
}

Status
XcalarEval::setupEvalAst(const char *evalString,
                         unsigned numFields,
                         const char **fieldNames,
                         XcalarEvalClass1Ast &ast,
                         unsigned &numVariables,
                         int *&argIndices,
                         Scalar **&scalars,
                         DagTypes::DagId dagId)
{
    Status status;
    bool astInit = false;
    Dag *dag = NULL;

    status = generateClass1Ast(evalString, &ast);
    // now scalar objects allocated for intermediates and constants in the AST
    // memory for variables will be allocated below
    BailIfFailed(status);
    astInit = true;

    numVariables = ast.astCommon.numScalarVariables;
    argIndices = (int *) memAlloc(numVariables * sizeof(*argIndices));
    BailIfNull(argIndices);

    scalars = (Scalar **) memAlloc(numVariables * sizeof(*scalars));
    BailIfNull(scalars);
    memZero(scalars, numVariables * sizeof(*scalars));

    if (dagId != XidInvalid) {
        dag = DagLib::get()->getDagLocal(dagId);
    }

    for (unsigned ii = 0; ii < numVariables; ii++) {
        char *variableName = ast.astCommon.scalarVariables[ii].variableName;
        argIndices[ii] = NewTupleMeta::DfInvalidIdx;
        bool isAgg = false;

        if (strstr(variableName, DfFatptrPrefixDelimiter)) {
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "Cannot perform eval on prefixed "
                          "field %s",
                          variableName);
            status = StatusInval;
            goto CommonExit;
        }

        DataFormat::unescapeNestedDelim(variableName);

        if (variableName[0] != OperatorsAggregateTag) {
            scalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
            BailIfNull(scalars[ii]);

            for (size_t jj = 0; jj < numFields; jj++) {
                if (strcmp(fieldNames[jj], variableName) == 0) {
                    argIndices[ii] = jj;
                    break;
                }
            }
        } else if (dagId != XidInvalid) {
            XdbId aggId;
            // XXX TODO May need to support fully qualified table names?
            status = dag->getDagNodeId(&variableName[1],
                                       Dag::TableScope::LocalOnly,
                                       &aggId);
            BailIfFailed(status);

            status = dag->getScalarResult(aggId, &scalars[ii]);
            BailIfFailed(status);
            isAgg = true;
        }

        if (argIndices[ii] == NewTupleMeta::DfInvalidIdx && !isAgg) {
            // field was not found
            status = StatusFieldNotFound;
            goto CommonExit;
        }
    }

CommonExit:
    if (status != StatusOk) {
        if (astInit) {
            // scratchPadScalars have already been freed
            dropScalarVarRef(&ast.astCommon);
            destroyClass1Ast(&ast);
            astInit = false;
        }

        if (argIndices) {
            memFree(argIndices);
            argIndices = NULL;
        }

        if (scalars) {
            for (unsigned ii = 0; ii < numVariables; ii++) {
                if (scalars[ii] != NULL) {
                    Scalar::freeScalar(scalars[ii]);
                    scalars[ii] = NULL;
                }
            }

            memFree(scalars);
            scalars = NULL;
        }
    }

    return status;
}

Status
XcalarEval::executeEvalAst(NewKeyValueEntry *dstKvEntry,
                           unsigned dstNumFields,
                           XcalarEvalClass1Ast *ast,
                           int *argIndices,
                           Scalar **scalars,
                           DfFieldValue *evalResultOut,
                           DfFieldType *resultTypeOut)
{
    Status status = StatusOk;

    for (unsigned ii = 0; ii < ast->astCommon.numScalarVariables; ii++) {
        int dstIdx = argIndices[ii];

        if (dstIdx != NewTupleMeta::DfInvalidIdx) {
            DfFieldType typeTmp =
                dstKvEntry->kvMeta_->tupMeta_->getFieldType(dstIdx);
            bool retIsValid;
            DfFieldValue valueTmp = dstKvEntry->tuple_.get(dstIdx,
                                                           dstNumFields,
                                                           typeTmp,
                                                           &retIsValid);
            if (retIsValid) {
                status = scalars[ii]->setValue(valueTmp, typeTmp);
                BailIfFailed(status);

                ast->astCommon.scalarVariables[ii].content = scalars[ii];
            } else {
                ast->astCommon.scalarVariables[ii].content = NULL;
            }
        } else {
            // This is an aggregate value and we already set it in setup.
            assert(scalars[ii]);
            ast->astCommon.scalarVariables[ii].content = scalars[ii];
        }
    }

    status = eval(ast, evalResultOut, resultTypeOut, false);

CommonExit:
    return status;
}

Status
XcalarEval::executeEvalMultiAst(NewKeyValueEntry *leftKvEntry,
                                unsigned leftNumFields,
                                NewKeyValueEntry *rightKvEntry,
                                XcalarEvalClass1Ast *ast,
                                int *argIndices,
                                Scalar **scalars,
                                DfFieldValue *evalResultOut,
                                DfFieldType *resultTypeOut)
{
    Status status = StatusOk;

    for (unsigned ii = 0; ii < ast->astCommon.numScalarVariables; ii++) {
        if (argIndices[ii] != NewTupleMeta::DfInvalidIdx) {
            unsigned dstIdx = (unsigned) argIndices[ii];
            DfFieldType typeTmp =
                dstIdx < leftNumFields
                    ? leftKvEntry->kvMeta_->tupMeta_->getFieldType(dstIdx)
                    : rightKvEntry->kvMeta_->tupMeta_->getFieldType(
                          dstIdx - leftNumFields);
            bool retIsValid;
            DfFieldValue valueTmp =
                dstIdx < leftNumFields
                    ? leftKvEntry->tuple_.get(dstIdx, &retIsValid)
                    : rightKvEntry->tuple_.get(dstIdx - leftNumFields,
                                               &retIsValid);
            if (retIsValid) {
                status = scalars[ii]->setValue(valueTmp, typeTmp);
                BailIfFailed(status);

                ast->astCommon.scalarVariables[ii].content = scalars[ii];
            } else {
                ast->astCommon.scalarVariables[ii].content = NULL;
            }
        } else {
            assert(scalars[ii]);
            ast->astCommon.scalarVariables[ii].content = scalars[ii];
        }
    }

    status = eval(ast, evalResultOut, resultTypeOut, false);

CommonExit:
    return status;
}

Status
XcalarEval::executeFilterAst(NewKeyValueEntry *dstKvEntry,
                             unsigned dstNumFields,
                             XcalarEvalClass1Ast *ast,
                             int *argIndices,
                             Scalar **scalars,
                             bool *result)
{
    DfFieldValue evalResult;
    DfFieldType resultType;

    Status status = executeEvalAst(dstKvEntry,
                                   dstNumFields,
                                   ast,
                                   argIndices,
                                   scalars,
                                   &evalResult,
                                   &resultType);
    if (isFatalError(status)) {
        goto CommonExit;
    }

    // treat non-fatal errors as false
    if (status != StatusOk ||
        !XcalarEval::isEvalResultTrue(evalResult, resultType)) {
        *result = false;
    } else {
        *result = true;
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
XcalarEval::executeFilterMultiAst(NewKeyValueEntry *leftKvEntry,
                                  unsigned leftNumFields,
                                  NewKeyValueEntry *rightKvEntry,
                                  XcalarEvalClass1Ast *ast,
                                  int *argIndices,
                                  Scalar **scalars,
                                  bool *result)
{
    DfFieldValue evalResult;
    DfFieldType resultType;

    Status status = executeEvalMultiAst(leftKvEntry,
                                        leftNumFields,
                                        rightKvEntry,
                                        ast,
                                        argIndices,
                                        scalars,
                                        &evalResult,
                                        &resultType);
    if (isFatalError(status)) {
        goto CommonExit;
    }

    // treat non-fatal errors as false
    if (status != StatusOk ||
        !XcalarEval::isEvalResultTrue(evalResult, resultType)) {
        *result = false;
    } else {
        *result = true;
    }

    status = StatusOk;
CommonExit:
    return status;
}

// assumes evalString is big enough to hold new name
void
XcalarEval::replaceAggregateVariableName(const char *oldAggName,
                                         const char *newAggName,
                                         char *evalString)
{
    char aggVar[XcalarApiMaxTableNameLen + 2];

    snprintf(aggVar, sizeof(aggVar), "%c%s", OperatorsAggregateTag, oldAggName);

    char *tokStart = NULL;
    do {
        tokStart = strstr(evalString, aggVar);

        if (tokStart != NULL) {
            tokStart += 1;

            // shift characters after var name
            char *newTokEnd = tokStart + strlen(newAggName);
            char *oldTokEnd = tokStart + strlen(oldAggName);

            memmove(newTokEnd, oldTokEnd, strlen(oldTokEnd));

            // copy over new var name
            for (unsigned ii = 0; ii < strlen(newAggName); ii++) {
                tokStart[ii] = newAggName[ii];
            }
        }
    } while (tokStart != NULL);
}

bool
XcalarEval::isRangeFilter(XcalarEvalClass1Ast *ast, const char *keyName)
{
    const char *fnName = ast->astCommon.rootNode->registeredFn->getFnName();
    const char *varName = ast->astCommon.rootNode->arguments[0]->token;

    if (ast->astCommon.numScalarVariables == 1 &&
        !isComplex(ast->astCommon.rootNode) && strcmp(varName, keyName) == 0 &&
        (strcmp(fnName, "eq") == 0 || strcmp(fnName, "lt") == 0 ||
         strcmp(fnName, "le") == 0 || strcmp(fnName, "gt") == 0 ||
         strcmp(fnName, "ge") == 0 || strcmp(fnName, "between") == 0)) {
        return true;
    }

    return false;
}

bool
XcalarEval::inFilterRange(XcalarEvalClass1Ast *ast,
                          DfFieldValue min,
                          DfFieldValue max,
                          bool valid,
                          const char *keyName,
                          DfFieldType keyType)
{
    if (!isRangeFilter(ast, keyName)) {
        // if we aren't a range filter, look at all slots
        return true;
    }

    // min/max is not valid
    if (!valid) {
        return false;
    }

    Status status = StatusOk;
    const char *fnName = ast->astCommon.rootNode->registeredFn->getFnName();
    DfFieldType cmpType = keyType;

    DfFieldValue minVal;
    bool minValSet = false;

    DfFieldValue maxVal;
    bool maxValSet = false;

    if (strcmp(fnName, "eq") == 0) {
        Scalar *s = *ast->astCommon.rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != cmpType) {
            // mismatched types
            return true;
        }

        status = s->getValue(&minVal);
        assert(status == StatusOk);

        minValSet = true;

        status = s->getValue(&maxVal);
        assert(status == StatusOk);

        maxValSet = true;
    } else if (strcmp(fnName, "lt") == 0 || strcmp(fnName, "le") == 0) {
        Scalar *s = *ast->astCommon.rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != cmpType) {
            // mismatched types
            return true;
        }

        status = s->getValue(&maxVal);
        assert(status == StatusOk);

        maxValSet = true;
    } else if (strcmp(fnName, "gt") == 0 || strcmp(fnName, "ge") == 0) {
        Scalar *s = *ast->astCommon.rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != cmpType) {
            // mismatched types
            return true;
        }

        status = s->getValue(&minVal);
        assert(status == StatusOk);

        minValSet = true;
    } else if (strcmp(fnName, "between") == 0) {
        Scalar *s = *ast->astCommon.rootNode->arguments[1]->varContent;
        assert(s != NULL);
        if (s->fieldType != cmpType) {
            // mismatched types
            return true;
        }

        status = s->getValue(&minVal);
        assert(status == StatusOk);

        minValSet = true;

        s = *ast->astCommon.rootNode->arguments[2]->varContent;
        assert(s != NULL);
        if (s->fieldType != cmpType) {
            // mismatched types
            return true;
        }

        status = s->getValue(&maxVal);
        assert(status == StatusOk);

        maxValSet = true;
    }

    if (cmpType == DfString) {
        // need to hash the value to compare it
        if (minValSet) {
            minVal.uint64Val =
                operatorsHashByString(minVal.stringVal.strActual);
        }

        if (maxValSet) {
            maxVal.uint64Val =
                operatorsHashByString(maxVal.stringVal.strActual);
        }

        cmpType = DfUInt64;
    }

    // if this max is below minVal, skip it
    if (minValSet && DataFormat::fieldCompare(cmpType, max, minVal) < 0) {
        return false;
    }

    // if this min is above maxVal, skip it
    if (maxValSet && DataFormat::fieldCompare(cmpType, min, maxVal) > 0) {
        return false;
    }

    return true;
}
