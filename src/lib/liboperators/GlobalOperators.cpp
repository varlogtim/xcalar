// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "msg/Message.h"
#include "bc/BufferCache.h"
#include "xdb/TableTypes.h"
#include "dataset/Dataset.h"
#include "scalars/Scalars.h"
#include "config/Config.h"
#include "operators/Operators.h"
#include "operators/Xdf.h"
#include "querymanager/QueryManager.h"
#include "xdb/Xdb.h"
#include "operators/XcalarEval.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "operators/Dht.h"
#include "sys/XLog.h"
#include "optimizer/Optimizer.h"
#include "dag/DagTypes.h"
#include "operators/OperatorsApiWrappers.h"
#include "export/DataTarget.h"
#include "operators/OperatorsHash.h"
#include "util/Uuid.h"
#include "dataset/AppLoader.h"
#include "ns/LibNs.h"
#include "msg/Xid.h"
#include "WorkItem.h"
#include "OperatorHandler.h"
#include "LibApisRecv.h"
#include "udf/UserDefinedFunction.h"
#include "OperatorsGvm.h"
#include "gvm/Gvm.h"
#include "util/DFPUtils.h"
#include "strings/String.h"
#include "usr/Users.h"

Operators *Operators::instance = NULL;

Status  // static
Operators::init()
{
    Status status = StatusUnknown;

    assert(instance == NULL);

    instance = (Operators *) memAllocExt(sizeof(Operators), moduleName);
    if (instance == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    instance = new (instance) Operators();

    status = StatusOk;

CommonExit:

    if (status != StatusOk) {
        if (instance != NULL) {
            memFree(instance);
            instance = NULL;
        }
    }

    return status;
}

Operators *  // static
Operators::get()
{
    assert(instance);
    return instance;
}

void
Operators::destroy()
{
    instance->~Operators();
    memFree(instance);
    instance = NULL;
}

Status
Operators::fixupKeyIndexes(unsigned numKeys,
                           int *keyIndexes,
                           const char **keyNames,
                           DfFieldType *keyTypes,
                           NewTupleMeta *tupMeta,
                           unsigned &numImmediates,
                           const char *immediateNames[])
{
    Status status = StatusOk;

    for (unsigned ii = 0; ii < numKeys; ii++) {
        if (keyIndexes[ii] == NewTupleMeta::DfInvalidIdx &&
            strcmp(keyNames[ii], DsDefaultDatasetKeyName) != 0) {
            // update tupMeta and add key as a new immediate
            keyIndexes[ii] = tupMeta->getNumFields();
            if (tupMeta->getNumFields() >= TupleMaxNumValuesPerRecord - 1) {
                xSyslog(moduleName,
                        XlogErr,
                        "Number of columns exceeds max num cols: %u",
                        TupleMaxNumValuesPerRecord);
                return StatusFieldLimitExceeded;
            }
            tupMeta->setFieldType(keyTypes[ii], keyIndexes[ii]);
            tupMeta->setNumFields(tupMeta->getNumFields() + 1);
            immediateNames[numImmediates++] = keyNames[ii];
        }
    }

    return status;
}

Status
Operators::populateJoinTupMeta(bool useLeftKey,
                               const XdbMeta *leftXdbMeta,
                               const XdbMeta *rightXdbMeta,
                               unsigned numLeftColumns,
                               XcalarApiRenameMap *leftRenameMap,
                               unsigned numRightColumns,
                               XcalarApiRenameMap *rightRenameMap,
                               const char *immediateNames[],
                               const char *fatptrPrefixNames[],
                               JoinOperator joinType,
                               unsigned numKeys,
                               const char **keyNames,
                               int *keyIndexes,
                               DfFieldType *keyTypes,
                               bool collisionCheck,
                               NewTupleMeta *joinTupMeta,
                               unsigned *numImmediatesOut,
                               unsigned *numFatptrsOut)
{
    Status status = StatusOk;
    unsigned leftFpCount = 0, leftImmCount = 0, rightFpCount = 0,
             rightImmCount = 0;
    const char *leftName, *rightName;
    joinTupMeta->setNumFields(0);

    for (unsigned ii = 0; ii < numKeys; ii++) {
        keyIndexes[ii] = NewTupleMeta::DfInvalidIdx;
    }

    // process left meta
    const NewTupleMeta *leftTupMeta = leftXdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
    for (unsigned ii = 0; ii < leftTupMeta->getNumFields(); ii++) {
        bool fieldSelected = false;
        leftName = leftXdbMeta->kvNamedMeta.valueNames_[ii];
        DfFieldType typeTmp = leftTupMeta->getFieldType(ii);

        for (unsigned jj = 0; jj < numLeftColumns; jj++) {
            // check whether the rename map type matches the NewTupleMeta type
            // If they are both fatptrs or both immediates, do the rename
            if (FatptrTypeMatch(leftRenameMap[jj].type, typeTmp) &&
                strcmp(leftRenameMap[jj].oldName, leftName) == 0) {
                // this column has been renamed
                leftName = leftRenameMap[jj].newName;
                fieldSelected = true;

                if (useLeftKey) {
                    for (unsigned kk = 0; kk < numKeys; kk++) {
                        if (typeTmp != DfFatptr &&
                            strcmp(leftRenameMap[jj].oldName, keyNames[kk]) ==
                                0) {
                            // key has been renamed
                            keyNames[kk] = leftName;
                            keyIndexes[kk] = joinTupMeta->getNumFields();
                        }
                    }
                }
            }
        }

        if (!fieldSelected) {
            continue;
        }

        if (typeTmp == DfFatptr) {
            fatptrPrefixNames[leftFpCount] = leftName;
            leftFpCount++;
        } else {
            immediateNames[leftImmCount] = leftName;
            leftImmCount++;
        }

        if (joinTupMeta->getNumFields() >= TupleMaxNumValuesPerRecord - 1) {
            xSyslog(moduleName,
                    XlogErr,
                    "Number of columns exceeds max num cols: %u",
                    TupleMaxNumValuesPerRecord);
            return StatusFieldLimitExceeded;
        }

        joinTupMeta->setFieldType(typeTmp, joinTupMeta->getNumFields());
        joinTupMeta->setNumFields(joinTupMeta->getNumFields() + 1);
    }

    if (useLeftKey) {
        status = fixupKeyIndexes(numKeys,
                                 keyIndexes,
                                 keyNames,
                                 keyTypes,
                                 joinTupMeta,
                                 leftImmCount,
                                 immediateNames);
        if (status != StatusOk) {
            return status;
        }
    }

    if (joinType == LeftSemiJoin || joinType == LeftAntiJoin) {
        // don't need to add right table's meta
        goto CommonExit;
    }

    // process right meta
    const NewTupleMeta *rightTupMeta;
    rightTupMeta = rightXdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
    for (unsigned ii = 0; ii < rightTupMeta->getNumFields(); ii++) {
        bool fieldSelected = false;
        DfFieldType typeTmp = rightTupMeta->getFieldType(ii);
        rightName = rightXdbMeta->kvNamedMeta.valueNames_[ii];

        for (unsigned jj = 0; jj < numRightColumns; jj++) {
            // check whether the rename map type matches the NewTupleMeta type
            // If they are both fatptrs or both immediates, do the rename
            if (FatptrTypeMatch(rightRenameMap[jj].type, typeTmp) &&
                strcmp(rightRenameMap[jj].oldName, rightName) == 0) {
                // this column has been renamed
                rightName = rightRenameMap[jj].newName;
                fieldSelected = true;

                if (!useLeftKey) {
                    for (unsigned kk = 0; kk < numKeys; kk++) {
                        if (typeTmp != DfFatptr &&
                            strcmp(rightRenameMap[jj].oldName, keyNames[kk]) ==
                                0) {
                            // key has been renamed
                            keyNames[kk] = rightName;
                            keyIndexes[kk] = joinTupMeta->getNumFields();
                        }
                    }
                }
            }
        }

        if (!fieldSelected) {
            continue;
        }

        if (typeTmp == DfFatptr) {
            fatptrPrefixNames[leftFpCount + rightFpCount] = rightName;
            rightFpCount++;
        } else {
            immediateNames[leftImmCount + rightImmCount] = rightName;
            rightImmCount++;
        }

        if (joinTupMeta->getNumFields() >= TupleMaxNumValuesPerRecord - 1) {
            xSyslog(moduleName,
                    XlogErr,
                    "Number of columns exceeds max num cols: %u",
                    TupleMaxNumValuesPerRecord);
            return StatusFieldLimitExceeded;
        }

        joinTupMeta->setFieldType(typeTmp, joinTupMeta->getNumFields());
        joinTupMeta->setNumFields(joinTupMeta->getNumFields() + 1);
    }

    if (!useLeftKey) {
        unsigned totalImm = leftImmCount + rightImmCount;
        status = fixupKeyIndexes(numKeys,
                                 keyIndexes,
                                 keyNames,
                                 keyTypes,
                                 joinTupMeta,
                                 totalImm,
                                 immediateNames);
        if (status != StatusOk) {
            return status;
        }
        // rightImm might have increased, need to recalculate
        rightImmCount = totalImm - leftImmCount;
    }

    if (!collisionCheck) {
        goto CommonExit;
    }

    // check for immediate name collisions
    for (unsigned ii = 0; ii < leftImmCount; ii++) {
        leftName = immediateNames[ii];
        for (unsigned jj = leftImmCount; jj < leftImmCount + rightImmCount;
             jj++) {
            rightName = immediateNames[jj];

            if (leftName[0] != '\0' && strcmp(leftName, rightName) == 0) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Name collision leftTable (%lu) and "
                              "rightTable (%lu): %s",
                              leftXdbMeta->xdbId,
                              rightXdbMeta->xdbId,
                              leftName);

                return StatusImmediateNameCollision;
            }
        }
    }

    // check for fatptr name collisions
    for (unsigned ii = 0; ii < leftFpCount; ii++) {
        leftName = fatptrPrefixNames[ii];
        for (unsigned jj = leftFpCount; jj < leftFpCount + rightFpCount; jj++) {
            rightName = fatptrPrefixNames[jj];

            // allow multiple prefix-less fatptrs
            if (leftName[0] != '\0' && strcmp(leftName, rightName) == 0) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Name collision leftTable (%lu) and "
                              "rightTable (%lu): %s",
                              leftXdbMeta->xdbId,
                              rightXdbMeta->xdbId,
                              leftName);

                return StatusFatptrPrefixCollision;
            }
        }
    }

CommonExit:
    *numImmediatesOut = leftImmCount + rightImmCount;
    *numFatptrsOut = leftFpCount + rightFpCount;
    return status;
}

Status
Operators::populateValuesDescWithProjectedFields(
    unsigned numKeys,
    int *keyIndexes,
    const NewTupleMeta *srcTupMeta,
    NewTupleMeta *dstTupMeta,
    XdbMeta *srcMeta,
    char (*projectedFieldNames)[DfMaxFieldNameLen + 1],
    unsigned numProjectedFieldNames,
    const char *immediateNames[],
    unsigned *numImmediatesOut,
    const char *fatptrPrefixNames[],
    unsigned *numFatptrsOut,
    bool keepKey,
    bool *keyKept)
{
    unsigned ii, jj;
    unsigned numValues = 0;
    unsigned numImmediates = 0, numFatptrs = 0;

    Accessor accessors[numProjectedFieldNames];
    char prefixes[numProjectedFieldNames][XcalarApiMaxFieldNameLen + 1];
    Status status = StatusOk;
    int keyNumMapping[srcTupMeta->getNumFields()];
    if (keyKept) {
        *keyKept = true;
    }

    assert(srcTupMeta != NULL);
    assert(dstTupMeta != NULL);
    assert(srcMeta != NULL);
    assert(projectedFieldNames != NULL);
    assert(immediateNames != NULL);
    assert(numImmediatesOut != NULL);

    for (ii = 0; ii < srcTupMeta->getNumFields(); ii++) {
        keyNumMapping[ii] = NewTupleMeta::DfInvalidIdx;
    }

    if (keyIndexes != NULL) {
        for (ii = 0; ii < numKeys; ii++) {
            if (keyIndexes[ii] != NewTupleMeta::DfInvalidIdx) {
                keyNumMapping[keyIndexes[ii]] = ii;
            }
        }
    }

    for (ii = 0; ii < numProjectedFieldNames; ii++) {
        char *prefixPtr =
            strstr(projectedFieldNames[ii], DfFatptrPrefixDelimiter);
        if (prefixPtr != NULL) {
            strlcpy(prefixes[ii],
                    projectedFieldNames[ii],
                    prefixPtr - projectedFieldNames[ii] + 1);

            if (Txn::currentTxn().mode_ == Txn::Mode::LRQ) {
                DataFormat::replaceFatptrPrefixDelims(projectedFieldNames[ii]);
                status = DataFormat::escapeNestedDelim(projectedFieldNames[ii],
                                                       DfMaxFieldNameLen + 1,
                                                       NULL);
                BailIfFailed(status);
            }
        } else {
            prefixes[ii][0] = '\0';
        }

        AccessorNameParser ap;
        status = ap.parseAccessor(projectedFieldNames[ii], &accessors[ii]);
        BailIfFailed(status);

        if (prefixPtr == NULL && accessors[ii].nameDepth > 1) {
            // We cannot have a nested field accessor for not prefixed fields
            // there must have been an error with the string
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Field %s cannot have nested delimiters",
                          projectedFieldNames[ii]);
            goto CommonExit;
        }
    }

    // copy all fatPtrs and keys over. Immediate types in tupMeta are shown in
    // the order they appear in immediateNames. copy desired immediates
    for (ii = 0; ii < srcTupMeta->getNumFields(); ii++) {
        DfFieldType srcTypeTmp = srcTupMeta->getFieldType(ii);
        if (srcTypeTmp != DfFatptr) {
            bool found = false;

            if (keepKey && keyNumMapping[ii] != NewTupleMeta::DfInvalidIdx) {
                found = true;
            } else {
                // search through desired immediates
                for (jj = 0; jj < numProjectedFieldNames; jj++) {
                    if (accessors[jj].nameDepth == 1 &&
                        strcmp(accessors[jj].names[0].value.field,
                               srcMeta->kvNamedMeta.valueNames_[ii]) == 0) {
                        found = true;
                        break;
                    }
                }
            }

            if (found) {
                immediateNames[numImmediates] =
                    srcMeta->kvNamedMeta.valueNames_[ii];
                numImmediates++;
                dstTupMeta->setFieldType(srcTypeTmp, numValues);

                if (keyNumMapping[ii] != NewTupleMeta::DfInvalidIdx) {
                    keyIndexes[keyNumMapping[ii]] = numValues;
                }

                numValues++;
            } else {
                if (keyNumMapping[ii] != NewTupleMeta::DfInvalidIdx) {
                    // this field was a key and it wasn't kept
                    assert(keepKey == false && keyKept != NULL);
                    if (*keyKept) {
                        *keyKept = false;
                    }
                }
            }
        } else {
            // search through desired fatptrs
            for (jj = 0; jj < numProjectedFieldNames; jj++) {
                if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                           prefixes[jj]) == 0) {
                    dstTupMeta->setFieldType(srcTypeTmp, numValues);
                    fatptrPrefixNames[numFatptrs] =
                        srcMeta->kvNamedMeta.valueNames_[ii];
                    numFatptrs++;
                    numValues++;
                    break;
                }
            }
        }
    }

    dstTupMeta->setNumFields(numValues);

    *numImmediatesOut = numImmediates;
    *numFatptrsOut = numFatptrs;

CommonExit:
    return status;
}

bool
Operators::orderingIsJoinCompatible(Ordering leftOrdering,
                                    Ordering rightOrdering)
{
    return leftOrdering == rightOrdering ||
           // Unordered may be upgraded to PartiallyAscending
           (leftOrdering == Unordered && rightOrdering == PartialAscending) ||
           (leftOrdering == PartialAscending && rightOrdering == Unordered);
}

// payloadToDistribute will be copied into OperatorsApiInput as a
// XcalarApiInput
Status
Operators::issueTwoPcForOp(void *payloadToDistribute,
                           size_t payloadLength,
                           MsgTypeId msgTypeId,
                           TwoPcCallId twoPcCallId,
                           Dag *dag,
                           XcalarApiUserId *user,
                           OperatorFlag opFlag)
{
    Status status = StatusOk;
    XcalarApiUdfContainer *udfContainer = NULL;
    unsigned numNodes = Config::get()->getActiveNodes();
    TwoPcHandle twoPcHandle;
    size_t userIdNameSize;
    MsgEphemeral eph;
    Status nodeStatus[numNodes];
    for (unsigned ii = 0; ii < numNodes; ii++) {
        nodeStatus[ii] = StatusOk;
    }
    size_t opInputSize = sizeof(OperatorsApiInput) + payloadLength;

    OperatorsApiInput *opInput =
        (OperatorsApiInput *) memAllocExt(opInputSize, moduleName);
    BailIfNull(opInput);

    opInput->dagId = dag->getId();
    opInput->flags = opFlag;
    udfContainer = dag->getUdfContainer();
    userIdNameSize = sizeof(opInput->userIdName);
    // if user is present, then udfContainer must also be
    // XXX:
    // eventually, remove "user" param from function signature and use
    // dag->getudfContainer() to get user and sessionId ...maybe just stuff
    // a udfContainer into opInput eventually?
    assert(user == NULL || udfContainer != NULL);
    if (user != NULL) {
        assert(strcmp(udfContainer->userId.userIdName, user->userIdName) == 0 ||
               (udfContainer->userId.userIdName[0] == '\0' &&
                udfContainer->retinaName[0] != '\0') ||
               (strcmp(udfContainer->userId.userIdName,
                       UserDefinedFunction::PublishedDfUserName) == 0));
        // copy username from user if it exists (for retinaName, and the new
        // PublishedDfUserName, this is useful)
        if (strcmp(udfContainer->userId.userIdName,
                   UserDefinedFunction::PublishedDfUserName) == 0) {
            verify(strlcpy(opInput->userIdName,
                           udfContainer->userId.userIdName,
                           userIdNameSize) < userIdNameSize);
        } else {
            verify(strlcpy(opInput->userIdName,
                           user->userIdName,
                           userIdNameSize) < userIdNameSize);
        }
        opInput->sessionId = udfContainer->sessionInfo.sessionId;
    } else if (udfContainer != NULL) {
        verify(strlcpy(opInput->userIdName,
                       udfContainer->userId.userIdName,
                       userIdNameSize) < userIdNameSize);
        opInput->sessionId = udfContainer->sessionInfo.sessionId;
    } else {
        opInput->userIdName[0] = '\0';
        opInput->sessionId = 0;
    }
    opInput->apiInputSize = payloadLength;
    memcpy(opInput->buf, payloadToDistribute, payloadLength);

    switch (msgTypeId) {
    case MsgTypeId::Msg2pcXcalarApiMap:
        xcalarApiSerializeMapInput((XcalarApiMapInput *) opInput->buf);
        break;
    case MsgTypeId::Msg2pcXcalarApiGroupBy:
        xcalarApiSerializeGroupByInput((XcalarApiGroupByInput *) &opInput->buf);
        break;
    default:
        break;
    }

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      opInput,
                                      opInputSize,
                                      0,
                                      TwoPcSlowPath,
                                      twoPcCallId,
                                      nodeStatus,
                                      TwoPcMemCopyInput);
    MsgSendRecvFlags flag;
    flag = (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrOnly);
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  msgTypeId,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  flag,
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);

    for (unsigned ii = 0; ii < numNodes; ++ii) {
        if (status == StatusOk && nodeStatus[ii] != StatusOk) {
            status = nodeStatus[ii];
            break;
        }
    }

CommonExit:
    if (opInput) {
        memFree(opInput);
        opInput = NULL;
    }
    return status;
}

// routines to invoke API style operators but for internal use only

// Use case for internal APIs: if an operation needs to do a group-by on an
// existing table, it can now invoke the following internal API for it, instead
// of doing this raw using the xcalarApi*Workitem directly.
//
// The destination table name is returned in dstTableNameOut[]

Status
Operators::groupByApiInternal(
    XcalarApiUserId *userId,
    const char *sessionName,
    Dag *sessionGraph,
    const char *srcTableName,
    unsigned numKeys,
    const char **keyNames,
    unsigned numEvals,
    const char *evalStrs[TupleMaxNumValuesPerRecord],
    const char *newFieldNames[TupleMaxNumValuesPerRecord],
    bool includeSample,
    bool icvMode,
    bool groupAll,  // if true, the operation is like an aggregate operation
    bool keepTxn,   // if true, continue using caller's transaction
    char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    const char *evalStrsTmp[numEvals];
    const char *fieldNamesTmp[numEvals];
    Txn prevTxn = Txn();
    XcalarApiKeyInput *keys = NULL;

    for (unsigned ii = 0; ii < numEvals; ii++) {
        evalStrsTmp[ii] = evalStrs[ii];
        fieldNamesTmp[ii] = newFieldNames[ii];
    }

    if (numKeys) {
        keys = new (std::nothrow) XcalarApiKeyInput[numKeys];
        BailIfNull(keys);
    }

    for (unsigned ii = 0; ii < numKeys; ii++) {
        status = strStrlcpy(keys[ii].keyName,
                            keyNames[ii],
                            sizeof(keys[ii].keyName));
        BailIfFailedMsg(moduleName,
                        status,
                        "groupByApiInternal(%s, %p, %s, %s) failed: key %d, "
                        "keyName \"%s\" too long",
                        userId->userIdName,
                        sessionGraph,
                        srcTableName,
                        (includeSample ? "true" : "false"),
                        ii,
                        keyNames[ii]);

        status = strStrlcpy(keys[ii].keyFieldName,
                            keyNames[ii],
                            sizeof(keys[ii].keyFieldName));
        BailIfFailedMsg(moduleName,
                        status,
                        "groupByApiInternal(%s, %p, %s, %s) failed: key %d, "
                        "keyName \"%s\" too long",
                        userId->userIdName,
                        sessionGraph,
                        srcTableName,
                        (includeSample ? "true" : "false"),
                        ii,
                        keyNames[ii]);
        keys[ii].ordering = Unordered;
        keys[ii].type = DfUnknown;
    }

    xSyslog(moduleName,
            XlogInfo,
            "groupByApiInternal(%s, %p, %s, %s) starting",
            userId->userIdName,
            sessionGraph,
            srcTableName,
            (includeSample ? "true" : "false"));

    workItem = xcalarApiMakeGroupByWorkItem(srcTableName,
                                            NULL,  // use temp table name as dst
                                            numKeys,
                                            keys,
                                            includeSample,
                                            icvMode,
                                            groupAll,
                                            numEvals,
                                            evalStrsTmp,
                                            fieldNamesTmp,
                                            sessionName);

    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           keepTxn ? NULL : &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Could not create dag node for map");
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "groupByApiInternal(%s, %p, %s, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                srcTableName,
                (includeSample ? "true" : "false"),
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dstTableNameOut) {
        status =
            strStrlcpy(dstTableNameOut,
                       workItem->output->outputResult.groupByOutput.tableName,
                       XcalarApiMaxTableNameLen + 1);
        BailIfFailedMsg(moduleName,
                        status,
                        "groupByApiInternal(%s, %p, %s, %s) failed: output "
                        "tableName \"%s\" too long",
                        userId->userIdName,
                        sessionGraph,
                        srcTableName,
                        (includeSample ? "true" : "false"),
                        workItem->output->outputResult.groupByOutput.tableName);
    }

    xSyslog(moduleName,
            XlogInfo,
            "groupByApiInternal(%s, %p, %s, %s) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            srcTableName,
            (includeSample ? "true" : "false"),
            workItem->output->outputResult.groupByOutput.tableName);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    }

    return status;
}

Status
Operators::mapApiInternal(
    XcalarApiUserId *userId,
    const char *sessionName,
    Dag *sessionGraph,
    const char *srcTableName,
    unsigned numEvals,
    const char *evalStrs[TupleMaxNumValuesPerRecord],
    const char *newFieldNames[TupleMaxNumValuesPerRecord],
    bool icvMode,  // include only rows with failures
    bool keepTxn,  // if true, continue using caller's transaction
    DagTypes::NodeId *dstNodeIdOut,
    char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    const char *evalStrsTmp[numEvals];
    const char *fieldNamesTmp[numEvals];
    Txn prevTxn = Txn();

    for (unsigned ii = 0; ii < numEvals; ii++) {
        evalStrsTmp[ii] = evalStrs[ii];
        fieldNamesTmp[ii] = newFieldNames[ii];
    }

    xSyslog(moduleName,
            XlogInfo,
            "mapApiInternal(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            srcTableName);

    workItem = xcalarApiMakeMapWorkItem(srcTableName,
                                        NULL,
                                        icvMode,
                                        numEvals,
                                        evalStrsTmp,
                                        fieldNamesTmp,
                                        sessionName);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           keepTxn ? NULL : &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::InvalidGraph);
    if (status != StatusOk) {
        xSyslog(moduleName, XlogErr, "Could not create dag node for map");
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "mapApiInternal(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                srcTableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dstTableNameOut) {
        status = strStrlcpy(dstTableNameOut,
                            workItem->output->outputResult.mapOutput.tableName,
                            XcalarApiMaxTableNameLen + 1);
        BailIfFailedMsg(moduleName,
                        status,
                        "mapApiInternal(%s, %p, %s) failed: output tableName "
                        "\"%s\" too long",
                        userId->userIdName,
                        sessionGraph,
                        srcTableName,
                        workItem->output->outputResult.mapOutput.tableName);
    }

    xSyslog(moduleName,
            XlogInfo,
            "mapApiInternal(%s, %p, %s) succeeded. Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            srcTableName,
            workItem->output->outputResult.mapOutput.tableName);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    } else if (dstNodeIdOut) {
        *dstNodeIdOut = workspaceGraphNodeId;
    }

    return status;
}

Status
Operators::indexTableApiInternal(
    XcalarApiUserId *userId,
    const char *sessionName,
    Dag *sessionGraph,
    const char *srcTableName,
    const char *dstTableName,
    bool broadcastDstTable,
    DagTypes::NodeId *dstNodeIdOut,
    unsigned numKeys,
    const char **keyNames,
    Ordering *orderings,
    bool delaySort,
    bool keepTxn,  // if true, continue using caller's transaction
    char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    if (dstNodeIdOut) {
        *dstNodeIdOut = DagTypes::InvalidDagNodeId;
    }

    xSyslog(moduleName,
            XlogInfo,
            "indexTableApiInternal(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            srcTableName);

    workItem =
        xcalarApiMakeIndexWorkItem(NULL,  // dataset must be NULL
                                   srcTableName,
                                   numKeys,
                                   keyNames,
                                   NULL,  // keyFieldName
                                   NULL,  // keyType
                                   orderings,
                                   dstTableName,
                                   NULL,  // dhtName
                                   NULL,  // fatPtrPrefixName
                                   delaySort,
                                   broadcastDstTable,  // broadcast or not
                                   sessionName);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error creating indexTableApiInternal workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           keepTxn ? NULL : &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::WorkspaceGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for indexTableApiInternal: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "indexTableApiInternal(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                srcTableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dstTableNameOut) {
        status =
            strStrlcpy(dstTableNameOut,
                       workItem->output->outputResult.indexOutput.tableName,
                       XcalarApiMaxTableNameLen + 1);
        BailIfFailedMsg(moduleName,
                        status,
                        "indexTableApiInternal(%s, %p, %s) failed: tableName "
                        "\"%s\" too long",
                        userId->userIdName,
                        sessionGraph,
                        srcTableName,
                        workItem->output->outputResult.indexOutput.tableName);
    }

    xSyslog(moduleName,
            XlogInfo,
            "indexTableApiInternal(%s, %p, %s) succeeded. "
            "Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            srcTableName,
            workItem->output->outputResult.indexOutput.tableName);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    } else if (dstNodeIdOut) {
        *dstNodeIdOut = workspaceGraphNodeId;
    }

    return status;
}

Status
Operators::filterTableApiInternal(
    XcalarApiUserId *userId,
    const char *sessionName,
    Dag *sessionGraph,
    const char *srcTableName,
    const char *evalStr,
    bool keepTxn,  // if true, continue using caller's transaction
    DagTypes::NodeId *dstNodeIdOut,
    char dstTableNameOut[XcalarApiMaxTableNameLen + 1])
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    OperatorHandler *operatorHandler = NULL;
    Txn prevTxn = Txn();

    xSyslog(moduleName,
            XlogInfo,
            "filterTableApiInternal(%s, %p, %s) starting",
            userId->userIdName,
            sessionGraph,
            srcTableName);

    workItem = xcalarApiMakeFilterWorkItem(evalStr, srcTableName, NULL);
    if (workItem == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error creating filterTableApiInternal workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId,
                                           sessionGraph,
                                           false,
                                           keepTxn ? NULL : &prevTxn);
    BailIfFailed(status);

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::WorkspaceGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not create dag node for filterTableApiInternal: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->run(&workItem->output, &workItem->outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "filterTableApiInternal(%s, %p, %s) failed: %s",
                userId->userIdName,
                sessionGraph,
                srcTableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dstTableNameOut) {
        status = strStrlcpy(dstTableNameOut,
                            workItem->output->outputResult.mapOutput.tableName,
                            XcalarApiMaxTableNameLen + 1);
        BailIfFailedMsg(moduleName,
                        status,
                        "filterTableApiInternal(%s, %p, %s) failed: tableName "
                        "\"%s\" too long",
                        userId->userIdName,
                        sessionGraph,
                        srcTableName,
                        workItem->output->outputResult.mapOutput.tableName);
    }

    xSyslog(moduleName,
            XlogInfo,
            "filterTableApiInternal(%s, %p, %s) succeeded. "
            "Dst Table: %s",
            userId->userIdName,
            sessionGraph,
            srcTableName,
            workItem->output->outputResult.mapOutput.tableName);

CommonExit:
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }
    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
            Status status2 =
                sessionGraph->dropAndChangeState(workspaceGraphNodeId,
                                                 DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        workspaceGraphNodeId,
                        strGetFromStatus(status2));
            }
        }
    } else if (dstNodeIdOut) {
        *dstNodeIdOut = workspaceGraphNodeId;
    }

    return status;
}

// Per-eval utility function for genSummaryFailureTabs. Given the name of a
// table generated by a map (in ICV mode) with multiple eval strings, generate
// the ONE failure table for the eval string specified by evalNum, and return
// the ID of this failure table in failureTableIdOut
Status
Operators::genSummaryFailureOneTab(char *failMapTableName,
                                   uint32_t evalNum,
                                   DagTypes::NodeId *failureTableIdOut,
                                   char *newFieldColName,
                                   char *sessionName,
                                   XcalarApiUserId *userId,
                                   Dag *dstGraph)
{
    Status status;
    Status status2;
    Ordering ordering;
    size_t ret;
    char failIndexedTableNameOut[XcalarApiMaxTableNameLen + 1];
    char failGroupByTableNameOut[XcalarApiMaxTableNameLen + 1];
    char failSortedGroupByTableNameOut[XcalarApiMaxTableNameLen + 1];
    char failFilteredTableNameOut[XcalarApiMaxTableNameLen + 1];
    char *gbEvalString = NULL;
    char *filterEvalString = NULL;
    char *failCountFieldName = NULL;
    bool dropIndexedTable = false;
    bool dropGroupByTable = false;
    bool dropSortedGbTable = false;

    // Step 1 -> Index failMapTableName on failure-description column as
    //           index, producing table failIndexedTableNameOut to prep for
    //           the subsequent group-by operation

    assert(dropIndexedTable == false);

    ordering = Unordered;
    status = indexTableApiInternal(userId,
                                   sessionName,
                                   dstGraph,
                                   failMapTableName,
                                   NULL,   // use system table out name
                                   false,  // broadcast
                                   NULL,   // don't need dst node id
                                   1,
                                   (const char **) &newFieldColName,
                                   &ordering,
                                   false,  // delaysort
                                   false,  // keep current txn
                                   failIndexedTableNameOut);
    BailIfFailed(status);

    dropIndexedTable = true;

    // Step 2 - Group-by
    //
    // For group-by, need ONE eval string to do the count of failure desc
    //
    failCountFieldName =
        (char *) memAllocExt((XcalarApiMaxFieldNameLen + 1), moduleName);
    BailIfNull(failCountFieldName);

    ret = snprintf(failCountFieldName,
                   XcalarApiMaxFieldNameLen + 1,
                   "%s-%s",
                   newFieldColName,
                   failureCountColName);

    if (ret >= (XcalarApiMaxFieldNameLen + 1)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Failure count column name \"%s-%s\" is too long",
                newFieldColName,
                failureCountColName);
        goto CommonExit;
    }

    gbEvalString =
        (char *) memAllocExt((XcalarApiMaxFieldNameLen + 1), moduleName);
    BailIfNull(gbEvalString);

    ret = snprintf(gbEvalString,
                   XcalarApiMaxFieldNameLen + 1,
                   "count(%s)",
                   newFieldColName);
    if (ret >= (XcalarApiMaxFieldNameLen + 1)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Eval string \"count(%s)\" is too long",
                newFieldColName);
        goto CommonExit;
    }

    status = groupByApiInternal(userId,
                                sessionName,
                                dstGraph,
                                failIndexedTableNameOut,
                                1,  // numKeys will always be 1 even for
                                    // multi-eval map, since only the ONE
                                    // failure-desc column is the key for
                                    // the GB
                                (const char **) &newFieldColName,
                                1,
                                (const char **) &gbEvalString,
                                (const char **) &failCountFieldName,
                                false,  // includeSample
                                false,  // icvMode
                                false,  // groupAll
                                false,  // keep current txn
                                failGroupByTableNameOut);
    BailIfFailed(status);

    dropGroupByTable = true;

    // indexed table can now be removed
    status =
        dstGraph->dropNode(failIndexedTableNameOut, SrcTable, NULL, NULL, true);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to drop node %s during map failure processing",
                failIndexedTableNameOut);
        status = StatusOk;  // non-fatal
    }

    dropIndexedTable = false;

    // Step 3 -> Sort "failure-count" column in descending order

    ordering = Descending;

    status = indexTableApiInternal(userId,
                                   sessionName,
                                   dstGraph,
                                   failGroupByTableNameOut,
                                   NULL,   // use system table out name
                                   false,  // broadcast
                                   failureTableIdOut,
                                   1,
                                   (const char **) &failCountFieldName,
                                   &ordering,
                                   true,   // delaysort = true -sort this!
                                   false,  // keep current txn
                                   failSortedGroupByTableNameOut);
    BailIfFailed(status);

    dropSortedGbTable = true;

    // can now remove group-by table
    status =
        dstGraph->dropNode(failGroupByTableNameOut, SrcTable, NULL, NULL, true);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to drop node %s during map/filter failure processing",
                failGroupByTableNameOut);
        status = StatusOk;  // non-fatal
    }

    dropGroupByTable = false;

    // Step 4 -> Filter out NULLs (any rows which passed) - will yield empty
    // table (numRows == 0), if the eval passed on ALL rows

    filterEvalString =
        (char *) memAllocExt((XcalarApiMaxFieldNameLen + 1), moduleName);
    BailIfNull(filterEvalString);

    ret = snprintf(filterEvalString,
                   XcalarApiMaxFieldNameLen + 1,
                   "exists(%s)",
                   newFieldColName);

    if (ret >= (XcalarApiMaxFieldNameLen + 1)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Eval string \"count(%s)\" is too long",
                newFieldColName);
        goto CommonExit;
    }

    status = filterTableApiInternal(userId,
                                    sessionName,
                                    dstGraph,
                                    failSortedGroupByTableNameOut,
                                    (const char *) filterEvalString,
                                    false,  // keep current txn
                                    failureTableIdOut,
                                    failFilteredTableNameOut);
    BailIfFailed(status);

    // can now remove the sorted, group-by table
    status = dstGraph->dropNode(failSortedGroupByTableNameOut,
                                SrcTable,
                                NULL,
                                NULL,
                                true);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to drop node %s during map/filter failure processing",
                failSortedGroupByTableNameOut);
        status = StatusOk;  // non-fatal
    }

    dropSortedGbTable = false;

CommonExit:
    if (dropIndexedTable == true) {
        status2 = dstGraph->dropNode(failIndexedTableNameOut,
                                     SrcTable,
                                     NULL,
                                     NULL,
                                     true);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to drop node %s during map/filter failure "
                    "processing",
                    failIndexedTableNameOut);
        }
    }
    if (dropGroupByTable == true) {
        status2 = dstGraph->dropNode(failGroupByTableNameOut,
                                     SrcTable,
                                     NULL,
                                     NULL,
                                     true);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to drop node %s during map failure "
                    "processing",
                    failGroupByTableNameOut);
        }
    }

    if (dropSortedGbTable == true) {
        status2 = dstGraph->dropNode(failSortedGroupByTableNameOut,
                                     SrcTable,
                                     NULL,
                                     NULL,
                                     true);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to drop node %s during map/filter failure "
                    "processing",
                    failSortedGroupByTableNameOut);
        }
    }

    if (failCountFieldName != NULL) {
        memFree(failCountFieldName);
    }
    if (gbEvalString != NULL) {
        memFree(gbEvalString);
    }
    if (filterEvalString != NULL) {
        memFree(filterEvalString);
    }
    return (status);
}

Status
Operators::broadcastTable(Dag *dag,
                          XcalarApiUserId *userId,
                          const char *srcTableName,
                          const char *dstTableName,
                          DagTypes::NodeId *dstNodeIdOut,
                          void *optimizerContext)
{
    Status status;
    *dstNodeIdOut = DagTypes::InvalidDagNodeId;
    const char *keyName = DsDefaultDatasetKeyName;
    Ordering ordering = Random;

    status = indexTableApiInternal(userId,
                                   NULL,  // no session name
                                   dag,
                                   srcTableName,
                                   dstTableName,
                                   true,          // broadcast the dst table
                                   dstNodeIdOut,  // get dst table node id
                                   1,
                                   &keyName,
                                   &ordering,
                                   false,  // no sort
                                   true,   // keep current txn
                                   NULL);

    return status;
}

Status
Operators::createRangeHashDht(Dag *dag,
                              DagTypes::NodeId srcNodeId,
                              XdbId srcXdbId,
                              DagTypes::NodeId dstNodeId,
                              XdbId dstXdbId,
                              const char *keyName,
                              const char *dhtName,
                              Ordering ordering,
                              XcalarApiUserId *user,
                              DhtId *dhtId)
{
    Status status;
    XcalarApiAggregateInput aggInput;
    Scalar *min = NULL, *max = NULL;
    DfFieldValue minVal, maxVal;
    DfFieldValue minValFloat, maxValFloat;

    DfFieldType keyType;

    aggInput.srcTable.tableId = srcNodeId;
    aggInput.srcTable.xdbId = srcXdbId;
    // re-use the opStatus
    aggInput.dstTable.tableId = dstNodeId;
    aggInput.dstTable.xdbId = dstXdbId;

    min = Scalar::allocScalar(DfMaxFieldValueSize);
    BailIfNullWith(min, StatusNoMem);

    max = Scalar::allocScalar(DfMaxFieldValueSize);
    BailIfNullWith(max, StatusNoMem);

    snprintf(aggInput.evalStr, sizeof(aggInput.evalStr), "min(%s)", keyName);
    status = aggregate(dag, &aggInput, min, false, user);
    if (status == StatusAggregateNoSuchField) {
        // Fake a min value so that processing continues (e.g. workbook
        // activation).
        xdfMakeInt64IntoScalar(min, INT64_MIN);
        status = StatusOk;
    }
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed to perform aggregate(%s) "
                       "on table %lu, xdb %lu: %s",
                       aggInput.evalStr,
                       srcNodeId,
                       srcXdbId,
                       strGetFromStatus(status));

    snprintf(aggInput.evalStr, sizeof(aggInput.evalStr), "max(%s)", keyName);
    status = aggregate(dag, &aggInput, max, false, user);
    if (status == StatusAggregateNoSuchField) {
        // Fake a max value so that processing continues (e.g. workbook
        // activation).
        xdfMakeInt64IntoScalar(max, INT64_MAX);
        status = StatusOk;
    }
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed to perform aggregate(%s) "
                       "on table %lu, xdb %lu: %s",
                       aggInput.evalStr,
                       srcNodeId,
                       srcXdbId,
                       strGetFromStatus(status));

    // If the min and max are of different types, then the column is of
    // mixed type. In such cases, fail the operation(index/sort)
    if (min->fieldType != max->fieldType) {
        status = StatusDfTypeMismatch;
        goto CommonExit;
    }

    keyType = min->fieldType;

    status = min->getValue(&minVal);
    assert(status == StatusOk);

    status = max->getValue(&maxVal);
    assert(status == StatusOk);

    if (keyType == DfString) {
        minVal.uint64Val = operatorsHashByString(minVal.stringVal.strActual);
        maxVal.uint64Val = operatorsHashByString(maxVal.stringVal.strActual);
        keyType = DfUInt64;
    }

    status = DataFormat::convertValueType(DfFloat64,
                                          keyType,
                                          &minVal,
                                          &minValFloat,
                                          DfMaxFieldValueSize,
                                          BaseCanonicalForm);
    assert(status == StatusOk);

    status = DataFormat::convertValueType(DfFloat64,
                                          keyType,
                                          &maxVal,
                                          &maxValFloat,
                                          DfMaxFieldValueSize,
                                          BaseCanonicalForm);
    assert(status == StatusOk);

    status = DhtMgr::get()->dhtCreate(dhtName,
                                      minValFloat.float64Val,
                                      maxValFloat.float64Val,
                                      ordering,
                                      DoNotBroadcast,
                                      DoAutoRemove,
                                      dhtId);
CommonExit:
    if (min) {
        Scalar::freeScalar(min);
    }

    if (max) {
        Scalar::freeScalar(max);
    }

    return status;
}

Status
Operators::issueGvm(XdbId srcXdbId,
                    XdbId dstXdbId,
                    unsigned numVariables,
                    char **variableNames,
                    DagTypes::DagId dagId,
                    OperatorFlag opFlag)
{
    size_t inputSize = 0;
    size_t maxOutputSize = 0;
    size_t varLenTotal = 0;
    DsDemystifyTableInput *demystifyInput = NULL;
    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;
    Config *config = Config::get();
    NodeId myNode = config->getMyNodeId();
    unsigned nodeCount = config->getActiveNodes();
    Status *nodeStatus = NULL;
    void *outputPerNode[MaxNodes];
    uint64_t sizePerNode[MaxNodes];
    bool doCleanout = false;
    OpTxnCookie perNodeCookies[MaxNodes];
    PerOpGlobalInput *perOpGlobIp = NULL;

    memZero(outputPerNode, sizeof(outputPerNode));
    memZero(sizePerNode, sizeof(sizePerNode));
    memZero(perNodeCookies, sizeof(perNodeCookies));

    for (unsigned ii = 0; ii < numVariables; ii++) {
        size_t varLen = strnlen(variableNames[ii], DfMaxFieldNameLen);
        if (varLen >= DfMaxFieldNameLen) {
            status = StatusOverflow;
            xSyslog(moduleName,
                    XlogErr,
                    "Operators::issueGvm srcXdbId %lu dstXdbId %lu "
                    "numVariables %u"
                    " dagId %lu flags %u failed: %s",
                    srcXdbId,
                    dstXdbId,
                    numVariables,
                    dagId,
                    opFlag,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        varLenTotal += varLen + 1;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    // GVM input payload size =
    // (Payload length + Per operators global input + demystification input)
    inputSize = sizeof(size_t) + sizeof(PerOpGlobalInput) +
                sizeof(*demystifyInput) + varLenTotal;

    // GVM output payload size =
    // (GVM input payload size + size of remote node cookie)
    maxOutputSize = inputSize + (sizeof(OpTxnCookie));

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + inputSize);
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Operators::issueGvm srcXdbId %lu dstXdbId %lu numVariables %u"
                " dagId %lu flags %u failed: %s",
                srcXdbId,
                dstXdbId,
                numVariables,
                dagId,
                opFlag,
                strGetFromStatus(status));
        goto CommonExit;
    }

    gPayload->init(OperatorsGvm::get()->getGvmIndex(),
                   (uint32_t)(OperatorsGvm::Action::AddPerTxnInfo),
                   inputSize);

    *((size_t *) gPayload->buf) = inputSize;
    perOpGlobIp =
        (PerOpGlobalInput *) ((uintptr_t) gPayload->buf + sizeof(size_t));
    perOpGlobIp->srcXdbId = srcXdbId;
    perOpGlobIp->dstXdbId = dstXdbId;
    perOpGlobIp->dagId = dagId;
    perOpGlobIp->srcNodeId = myNode;
    perOpGlobIp->opFlag = opFlag;

    demystifyInput =
        (DsDemystifyTableInput *) ((uintptr_t) gPayload->buf + sizeof(size_t) +
                                   sizeof(PerOpGlobalInput));
    demystifyInput->opMeta = NULL;
    demystifyInput->numVariables = numVariables;
    demystifyInput->includeSrcTableSample =
        (opFlag & OperatorFlagIncludeSrcTableInSample ? true : false);
    demystifyInput->inputStringsSize = varLenTotal;

    {
        size_t cursor = 0;
        for (unsigned ii = 0; ii < numVariables; ii++) {
            size_t ret;
            ret = strlcpy(&demystifyInput->inputStrings[cursor],
                          variableNames[ii],
                          DfMaxFieldNameLen + 1);
            assert(ret <= DfMaxFieldNameLen);
            cursor += ret + 1;
        }
        assert(cursor == varLenTotal);
    }

    doCleanout = true;
    status = Gvm::get()->invokeWithOutput(gPayload,
                                          maxOutputSize,
                                          (void **) &outputPerNode,
                                          sizePerNode,
                                          nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Operators::issueGvm srcXdbId %lu dstXdbId %lu numVariables %u"
                " dagId %lu flags %u failed: %s",
                srcXdbId,
                dstXdbId,
                numVariables,
                dagId,
                opFlag,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        perNodeCookies[ii] =
            *(OpTxnCookie *) ((uintptr_t) outputPerNode[ii] + inputSize);
    }

    inputSize = sizeof(OpTxnCookie) * nodeCount;
    assert(gPayload != NULL);
    memFree(gPayload);
    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + inputSize);
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Operators::issueGvm srcXdbId %lu dstXdbId %lu numVariables %u"
                " dagId %lu flags %u failed: %s",
                srcXdbId,
                dstXdbId,
                numVariables,
                dagId,
                opFlag,
                strGetFromStatus(status));
        goto CommonExit;
    }

    gPayload->init(OperatorsGvm::get()->getGvmIndex(),
                   (uint32_t)(OperatorsGvm::Action::UpdatePerTxnInfo),
                   inputSize);

    memcpy(gPayload->buf, perNodeCookies, inputSize);

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Operators::issueGvm srcXdbId %lu dstXdbId %lu numVariables %u"
                " dagId %lu flags %u failed: %s",
                srcXdbId,
                dstXdbId,
                numVariables,
                dagId,
                opFlag,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    for (unsigned ii = 0; ii < nodeCount; ii++) {
        if (outputPerNode[ii] != NULL) {
            memFree(outputPerNode[ii]);
            outputPerNode[ii] = NULL;
        }
    }
    if (status != StatusOk && doCleanout) {
        cleanoutGvm(XidInvalid, dstXdbId, dagId, status);
    }
    return status;
}

void
Operators::cleanoutGvm(XdbId srcXdbId,
                       XdbId dstXdbId,
                       DagTypes::DagId dagId,
                       Status cleanoutStatus)
{
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;
    size_t inputSize = sizeof(Status);

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + inputSize);
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    *(Status *) ((uintptr_t) gPayload + sizeof(Gvm::Payload)) = cleanoutStatus;
    gPayload->init(OperatorsGvm::get()->getGvmIndex(),
                   (uint32_t)(OperatorsGvm::Action::DeletePerTxnInfo),
                   inputSize);

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Operators::cleanoutGvm srcXdbId %lu dstXdbId %lu dagId %lu"
                " failed: %s",
                srcXdbId,
                dstXdbId,
                dagId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
}

// XXX:should refactor globalOperators for index/map/groupby/filter/load/join
Status
Operators::index(Dag *dag,
                 XcalarApiIndexInput *indexInput,
                 void *optimizerContext,
                 XcalarApiUserId *user)
{
    Status status = StatusUnknown;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool gvmIssued = false;
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    DagTypes::DagId dagId = dag->getId();

    gvmIssued = true;
    if (indexInput->source.isTable) {
        char *variableNames[indexInput->numKeys];
        for (unsigned ii = 0; ii < indexInput->numKeys; ii++) {
            variableNames[ii] = indexInput->keys[ii].keyName;
        }
        status = issueGvm(indexInput->source.xid,
                          indexInput->dstTable.xdbId,
                          indexInput->numKeys,
                          variableNames,
                          dagId,
                          opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Index(%lu) failed issueGvm %s",
                        indexInput->dstTable.tableId,
                        strGetFromStatus(status));
    } else {
        status = issueGvm(XidInvalid,
                          indexInput->dstTable.xdbId,
                          0,
                          NULL,
                          dagId,
                          opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Index(%lu) failed issueGvm %s",
                        indexInput->dstTable.tableId,
                        strGetFromStatus(status));
    }

    status = issueTwoPcForOp(indexInput,
                             sizeof(*indexInput),
                             MsgTypeId::Msg2pcXcalarApiIndex,
                             TwoPcCallId::Msg2pcXcalarApiIndex1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Index(%lu) failed issueTwoPcForOp %s",
                    indexInput->dstTable.tableId,
                    strGetFromStatus(status));

    if ((indexInput->keys[0].ordering & SortedFlag) &&
        !(indexInput->keys[0].ordering & PartiallySortedFlag)) {
        status = xdbMgr->xdbLoadDoneSort(indexInput->dstTable.xdbId,
                                         dag,
                                         indexInput->delaySort);
    } else {
        status = xdbMgr->xdbLoadDone(indexInput->dstTable.xdbId);
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Index(%lu) failed xdbLoadDone %s",
                    indexInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (gvmIssued) {
        cleanoutGvm(indexInput->source.xid,
                    indexInput->dstTable.xdbId,
                    dagId,
                    status);
    }
    return status;
}

Status
Operators::synthesize(Dag *dag,
                      XcalarApiSynthesizeInput *synthesizeInput,
                      size_t inputSize,
                      void *optimizerContext)
{
    Status status = StatusUnknown;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool gvmIssued = false;
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    DagTypes::DagId dagId = dag->getId();

    if (synthesizeInput->source.isTable) {
        XdbMeta *dstMeta;
        Xdb *dstXdb;

        status = XdbMgr::get()->xdbGet(synthesizeInput->dstTable.xdbId,
                                       &dstXdb,
                                       &dstMeta);
        assert(status == StatusOk);

        unsigned numVariables = dstMeta->numImmediates;
        char variableNames[dstMeta->numImmediates]
                          [XcalarApiMaxFieldNameLen + 1];
        char *varNameTmp[dstMeta->numImmediates];

        // obtain the source variables in the same order as dstMeta
        for (unsigned ii = 0; ii < numVariables; ii++) {
            status = strStrlcpy(variableNames[ii],
                                dstMeta->kvNamedMeta.valueNames_[ii],
                                sizeof(variableNames[ii]));
            BailIfFailed(status);
            varNameTmp[ii] = variableNames[ii];

            unsigned jj;
            for (jj = 0; jj < synthesizeInput->columnsCount; jj++) {
                if (strcmp(variableNames[ii],
                           synthesizeInput->columns[jj].newName) == 0) {
                    status = strStrlcpy(variableNames[ii],
                                        synthesizeInput->columns[jj].oldName,
                                        sizeof(variableNames[ii]));
                    BailIfFailed(status);
                    break;
                }
            }

            if (jj == synthesizeInput->columnsCount) {
                // this column was not specified in the input, which means
                // it's the original source raw key name. Convert it into
                // an accessor name here
                status =
                    DataFormat::escapeNestedDelim(variableNames[ii],
                                                  sizeof(variableNames[ii]),
                                                  NULL);
                BailIfFailed(status);
            }
        }

        gvmIssued = true;
        status = issueGvm(synthesizeInput->source.xid,
                          synthesizeInput->dstTable.xdbId,
                          numVariables,
                          varNameTmp,
                          dagId,
                          opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Synthesize(%lu) failed issueGvm %s",
                        synthesizeInput->dstTable.xdbId,
                        strGetFromStatus(status));
    } else {
        gvmIssued = true;
        status = issueGvm(XidInvalid,
                          synthesizeInput->dstTable.xdbId,
                          0,
                          NULL,
                          dagId,
                          opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Synthesize(%lu) failed issueGvm %s",
                        synthesizeInput->dstTable.xdbId,
                        strGetFromStatus(status));
    }

    status = issueTwoPcForOp(synthesizeInput,
                             inputSize,
                             MsgTypeId::Msg2pcXcalarApiSynthesize,
                             TwoPcCallId::Msg2pcXcalarApiSynthesize1,
                             dag,
                             NULL,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Synthesize(%lu) failed issueTwoPcForOp %s",
                    synthesizeInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(synthesizeInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Synthesize(%lu) failed xdbLoadDone %s",
                    synthesizeInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (gvmIssued) {
        cleanoutGvm(synthesizeInput->source.xid,
                    synthesizeInput->dstTable.tableId,
                    dagId,
                    status);
    }
    return status;
}

Status
Operators::loadDataset(Dag *dag,
                       XcalarApiBulkLoadInput *bulkLoadInput,
                       void *optimizerContext,
                       XcalarApiBulkLoadOutput *bulkLoadOutput,
                       DsDataset **datasetOut,
                       DatasetRefHandle *dsRefHandleOut,
                       XdbId *xdbIdOut,
                       XcalarApiUserId *user)
{
    Status status = StatusUnknown;
    DsDataset *dataset = NULL;
    OpStatus *opStatus;
    bool datasetLoaded = false;
    bool dsHandleValid = false;
    bool loadInProgress = false;
    Dataset *ds = Dataset::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbId xdbId = bulkLoadInput->dstXdbId;
    DsDatasetId datasetId;
    AppLoader appLoader;
    LoadDatasetInput *loadDatasetInput = NULL;
    DatasetRefHandle dsRefHandle;
    bool appInternalError = false;
    bool dagRef = false;
    XcalarApiUserId *userInContainer = NULL;
    char *parserFnName = NULL;

    assert(bulkLoadOutput);

    // Be prepared to leak the DagNode reference if the AppLoader comes back
    // with appInternalError set.
    status = dag->getDagNodeRefById(bulkLoadInput->dagNodeId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not get refCount to dagNode %lu for dataset %s: %s",
                bulkLoadInput->dagNodeId,
                bulkLoadInput->datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    dagRef = true;

    // Open Handle to Dataset in Read Only mode first. If the object already
    // exists,
    // - If the UUIDs match, it's the same Dataset. So return the Read only
    // handle to the caller.
    // - Else, Dataset namespace collision has occured. So we will close
    // the Read only handle.
    status = ds->openHandleToDatasetByName(bulkLoadInput->datasetName,
                                           user,
                                           &dataset,
                                           LibNsTypes::ReaderShared,
                                           &dsRefHandle);
    if (status == StatusOk) {
        dsHandleValid = true;
        xSyslog(moduleName,
                XlogErr,
                "Dataset %s already exists. Comparing datasetUuid: %lu-%lu "
                " with input's uuid: %lu-%lu",
                bulkLoadInput->datasetName,
                dataset->loadArgs_.datasetUuid.buf[0],
                dataset->loadArgs_.datasetUuid.buf[1],
                bulkLoadInput->loadArgs.datasetUuid.buf[0],
                bulkLoadInput->loadArgs.datasetUuid.buf[1]);
        // This dataset name already exists. But is it the same dataset?
        if (strncmp(bulkLoadInput->datasetName,
                    XcalarApiDatasetPrefix,
                    XcalarApiDatasetPrefixLen) == 0) {
            // XD generated names that are exact are allowed to have a dataset
            // UUID mismatch (Xc-12616).
            status = StatusDsDatasetLoaded;
        } else if (uuidCmp(&dataset->loadArgs_.datasetUuid,
                           &bulkLoadInput->loadArgs.datasetUuid)) {
            // They're the same as the name and dataset UUID matches
            status = StatusDsDatasetLoaded;
        } else {
            // They're not the same. Dataset name collision
            status = StatusDatasetNameAlreadyExists;
        }
        goto CommonExit;
    }

    status = ds->validateLoadInput(bulkLoadInput);
    BailIfFailed(status);

    loadDatasetInput = new (std::nothrow) LoadDatasetInput;
    BailIfNull(loadDatasetInput);

    loadDatasetInput->bulkLoadInput = *bulkLoadInput;

    parserFnName = bulkLoadInput->loadArgs.parseArgs.parserFnName;
    if (parserFnName[0] != '\0' && parserFnName[0] != '/' &&
        strncmp(parserFnName,
                UserDefinedFunction::DefaultModuleName,
                strlen(UserDefinedFunction::DefaultModuleName)) != 0) {
        // Convert relative name to fully qualified name (except for the
        // "default" module).  The name will be saved in the dataset structure
        // so that other users "attaching" to a shared dataset will be able to
        // pull the UDF into their workbook.  This is needed so that the
        // session "attaching" to the shared dataset can replay the session
        // (e.g. after a cluster restart and the original dataset loader
        // has not yet activated their session).  It is also needed to allow
        // the workbook be downloaded.  This is because UDF names in the
        // workbook's qgraph are relative, and need to be prefixed with the
        // workbook container path. So any references to UDFs in a workbook's
        // qgraph, must be to UDFs contained in the workbook. Attaching to a
        // shared dataset which uses some other workbook's UDF forces this UDF
        // to be copied into the workbook so that the workbook's relative
        // references to the UDF can be resolved inside this workbook itself

        status = convertLoadUdfToAbsolutePath(&loadDatasetInput->bulkLoadInput
                                                   .loadArgs,
                                              dag->getUdfContainer());
        BailIfFailed(status);
    }
    // Verify that we have a good parser name.
    parserFnName =
        loadDatasetInput->bulkLoadInput.loadArgs.parseArgs.parserFnName;
    assert(parserFnName[0] == '\0' || parserFnName[0] == '/' ||
           strncmp(parserFnName,
                   UserDefinedFunction::DefaultModuleName,
                   strlen(UserDefinedFunction::DefaultModuleName)) == 0);

    loadDatasetInput->dagId = dag->getId();

    status = ds->loadDataset(loadDatasetInput, user, &dataset, &dsRefHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to load dataset %s and open Read Write mode: %s",
                bulkLoadInput->datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    datasetLoaded = true;
    dsHandleValid = true;

    assert(dataset != NULL);
    datasetId = dataset->getDatasetId();

    status = appLoader.init(datasetId, &dsRefHandle);
    BailIfFailed(status);

    status = dag->getOpStatus(bulkLoadInput->dagNodeId, &opStatus);
    BailIfFailed(status);

    loadInProgress = true;
    assert(dag->getUdfContainer() != NULL);
    userInContainer = &dag->getUdfContainer()->userId;

    if (strlen(userInContainer->userIdName) == 0) {
        memcpy(userInContainer, user, sizeof(*userInContainer));
    } else if (strcmp(userInContainer->userIdName, user->userIdName) != 0 &&
               strcmp(userInContainer->userIdName,
                      UserDefinedFunction::PublishedDfUserName) != 0) {
        assert(false && "udfContainer user not same as passed in user");
        status = StatusInval;
        xSyslog(moduleName,
                XlogErr,
                "Failed to load %s; user in Dag %s != user %s: %s",
                bulkLoadInput->datasetName,
                userInContainer->userIdName,
                user->userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = appLoader.loadDataset(&loadDatasetInput->bulkLoadInput.loadArgs,
                                   dag->getUdfContainer(),
                                   opStatus,
                                   bulkLoadOutput,
                                   &appInternalError);
    if (status != StatusOk) {
        if (appInternalError == false) {
            loadInProgress = false;
        }
        goto CommonExit;
    }

    // XXX TODO
    // App internal error is emitted because there is a failure to dispatch 2PC
    // for cleanout.
    // Instead of leaking the Dataset object in the event of App internal error,
    // Dataset object need to manage node local reference counts, such that
    // global drop on the Dataset from the source node of the BulkLoad API just
    // drops the ref intead of deleting the node local Dataset managed state.
    loadInProgress = false;

    status = ds->finalizeDataset(datasetId, dsRefHandle.nsHandle);
    BailIfFailed(status);

    // Close the Dataset handle in Read Write mode.
    status = ds->closeHandleToDataset(&dsRefHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close dataset %s: %s",
                bulkLoadInput->datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    dsHandleValid = false;

    // Open Handle to the Dataset in Read Only mode. Note that at this point
    // Dataset can be shared!
    status = ds->openHandleToDatasetByName(bulkLoadInput->datasetName,
                                           user,
                                           NULL,
                                           LibNsTypes::ReaderShared,
                                           &dsRefHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open dataset %s in Read only mode: %s",
                bulkLoadInput->datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    dsHandleValid = true;

CommonExit:
    if (xdbId != XdbIdInvalid) {
        Status tmpStatus;
        tmpStatus = xdbMgr->xdbLoadDone(xdbId);
        if (status == StatusOk && tmpStatus != StatusOk) {
            status = tmpStatus;
        }
    }

    if (loadDatasetInput) {
        delete loadDatasetInput;
        loadDatasetInput = NULL;
    }

    if (status != StatusOk) {
        if (datasetLoaded) {
            if (loadInProgress == false) {
                Status status2 = ds->unloadDatasetById(datasetId);
                if (status2 != StatusOk &&
                    status2 != StatusDatasetAlreadyDeleted) {
                    xSyslog(moduleName,
                            XlogErr,
                            "failed to delete dataset %s: %s",
                            bulkLoadInput->datasetName,
                            strGetFromStatus(status2));
                }
            }
            dataset = NULL;
        }

        if (xdbId != XdbIdInvalid) {
            if (loadInProgress == false) {
                xdbMgr->xdbDrop(xdbId);
            }
            xdbId = XdbIdInvalid;
        }

        if (dsHandleValid && status != StatusDsDatasetLoaded) {
            if (loadInProgress == false) {
                Status status2 = ds->closeHandleToDataset(&dsRefHandle);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to close dataset %s: %s",
                            bulkLoadInput->datasetName,
                            strGetFromStatus(status2));
                }
            }
            dataset = NULL;
        }
    }

    // Be prepared to leak the DagNode reference if the AppLoader comes back
    // with appInternalError set.
    if (appInternalError == false && dagRef == true) {
        dag->putDagNodeRefById(bulkLoadInput->dagNodeId);
    }

    if (xdbIdOut != NULL) {
        *xdbIdOut = xdbId;
    }

    if (datasetOut != NULL) {
        *datasetOut = dataset;
        *dsRefHandleOut = dsRefHandle;
    }

    return status;
}

Status
Operators::project(Dag *dag,
                   XcalarApiProjectInput *projectInput,
                   void *optimizerContext,
                   XcalarApiUserId *user)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    size_t inputSize =
        sizeof(*projectInput) +
        projectInput->numColumns * sizeof(*projectInput->columnNames);
    OperatorFlag opFlag = OperatorFlagNone;
    bool gvmIssued = false;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    DagTypes::DagId dagId = dag->getId();

    gvmIssued = true;
    status = issueGvm(projectInput->srcTable.xdbId,
                      projectInput->dstTable.xdbId,
                      0,
                      NULL,
                      dagId,
                      opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Project(%lu) failed issueGvm %s",
                    projectInput->dstTable.xdbId,
                    strGetFromStatus(status));

    status = issueTwoPcForOp(projectInput,
                             inputSize,
                             MsgTypeId::Msg2pcXcalarApiProject,
                             TwoPcCallId::Msg2pcXcalarApiProject1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Project(%lu) failed issueTwoPcForOp %s",
                    projectInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(projectInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Project(%lu) failed xdbLoadDone %s",
                    projectInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (gvmIssued) {
        cleanoutGvm(projectInput->srcTable.xdbId,
                    projectInput->dstTable.xdbId,
                    dagId,
                    status);
    }
    return status;
}

Status
Operators::filter(Dag *dag,
                  XcalarApiFilterInput *filterInput,
                  void *optimizerContext,
                  XcalarApiUserId *user)
{
    Status status = StatusUnknown;
    XdbMgr *xdbMgr = XdbMgr::get();
    char *evalString = filterInput->filterStr;
    bool issuedGvm = false;
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    DagTypes::DagId dagId = dag->getId();

    issuedGvm = true;
    status = filterAndMapGlobal(dag,
                                filterInput->srcTable.xdbId,
                                filterInput->dstTable.xdbId,
                                1,
                                &evalString,
                                opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Filter(%lu) failed filterAndMapGlobal %s",
                    filterInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = issueTwoPcForOp(filterInput,
                             sizeof(*filterInput),
                             MsgTypeId::Msg2pcXcalarApiFilter,
                             TwoPcCallId::Msg2pcXcalarApiFilter1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Filter(%lu) failed issueTwoPcForOp %s",
                    filterInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(filterInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Filter(%lu) failed xdbLoadDone %s",
                    filterInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (issuedGvm) {
        cleanoutGvm(filterInput->srcTable.xdbId,
                    filterInput->dstTable.xdbId,
                    dagId,
                    status);
    }
    return status;
}

Status
Operators::aggregate(Dag *dag,
                     XcalarApiAggregateInput *aggregateInput,
                     Scalar *scalarOut,
                     bool shouldDestroyOp,
                     XcalarApiUserId *user)
{
    Status status = StatusOk;
    XcalarEvalClass2Ast ast;
    bool astGenerated = false;
    bool gvmIssued = false;
    XdbId scalarXdbId = XdbIdInvalid;
    bool scalarTableCreated = false;
    XdbMgr *xdbMgr = XdbMgr::get();
    const XdbId srcXdbId = aggregateInput->srcTable.xdbId;
    const XdbId dstXdbId = aggregateInput->dstTable.xdbId;
    XdbMeta *srcMeta;
    OperatorFlag opFlag = OperatorFlagNone;
    DagTypes::DagId dagId = dag->getId();

    status = xdbMgr->xdbGet(srcXdbId, NULL, &srcMeta);
    BailIfFailed(status);

    status = XcalarEval::get()->generateClass2Ast(aggregateInput->evalStr,
                                                  &ast,
                                                  NULL,
                                                  dagId,
                                                  dstXdbId);
    BailIfFailed(status);
    astGenerated = true;

    for (unsigned ii = 0; ii < ast.astCommon.numScalarVariables; ii++) {
        DagTypes::NodeId aggId;
        assert(ast.astCommon.scalarVariables[ii].variableName[0] ==
               OperatorsAggregateTag);
        status = dag->getDagNodeId(&ast.astCommon.scalarVariables[ii]
                                        .variableName[1],
                                   Dag::TableScope::LocalOnly,
                                   &aggId);
        if (status == StatusOk) {
            status = dag->getScalarResult(aggId,
                                          &ast.astCommon.scalarVariables[ii]
                                               .content);
            if (status != StatusOk) {
                goto CommonExit;
            }
        } else if (status == StatusDagNodeNotFound) {
            ast.astCommon.scalarVariables[ii].content = NULL;
        } else {
            goto CommonExit;
        }
    }

    if (isDemystificationRequired(ast.variableNames,
                                  ast.numUniqueVariables,
                                  srcMeta) ||
        XcalarEval::get()->isComplex(ast.astCommon.rootNode)) {
        NewTupleMeta tupMeta;
        tupMeta.setNumFields(ast.numUniqueVariables);
        const char *immediateNames[tupMeta.getNumFields()];
        unsigned numVariables = ast.numUniqueVariables;
        char **variableNames = ast.variableNames;

        for (unsigned ii = 0; ii < ast.numUniqueVariables; ii++) {
            tupMeta.setFieldType(DfScalarObj, ii);
            immediateNames[ii] = ast.variableNames[ii];
        }

        scalarXdbId = dstXdbId;

        status = xdbMgr->xdbCreate(scalarXdbId,
                                   DsDefaultDatasetKeyName,
                                   DfInt64,
                                   NewTupleMeta::DfInvalidIdx,
                                   &tupMeta,
                                   NULL,
                                   0,
                                   immediateNames,
                                   tupMeta.getNumFields(),
                                   NULL,
                                   0,
                                   Random,
                                   XdbGlobal,
                                   DhtInvalidDhtId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Aggregate(%lu) failed xdbCreate %s",
                        aggregateInput->dstTable.tableId,
                        strGetFromStatus(status));

        scalarTableCreated = true;

        ast.scalarXdbId = scalarXdbId;
        ast.scalarTableGlobalState = XdbGlobal;

        gvmIssued = true;
        status = issueGvm(aggregateInput->srcTable.xdbId,
                          aggregateInput->dstTable.xdbId,
                          numVariables,
                          variableNames,
                          dagId,
                          opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Aggregate(%lu) failed issueGvm %s",
                        aggregateInput->dstTable.tableId,
                        strGetFromStatus(status));

        status = issueTwoPcForOp(aggregateInput,
                                 sizeof(*aggregateInput),
                                 MsgTypeId::Msg2pcXcalarApiAggregate,
                                 TwoPcCallId::Msg2pcXcalarApiAggregate1,
                                 dag,
                                 user,
                                 opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Aggregate(%lu) failed issueTwoPcForOp %s",
                        aggregateInput->dstTable.tableId,
                        strGetFromStatus(status));

        status = xdbMgr->xdbLoadDone(scalarXdbId);
        BailIfFailed(status);
    } else {
        ast.scalarXdbId = srcXdbId;
        ast.scalarTableGlobalState = XdbGlobal;

        gvmIssued = true;
        status = issueGvm(srcXdbId, XidInvalid, 0, NULL, dagId, opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Aggregate(%lu) failed issueGvm %s",
                        aggregateInput->dstTable.tableId,
                        strGetFromStatus(status));
    }

    XdbMeta *scalarMeta;
    status = xdbMgr->xdbGet(ast.scalarXdbId, NULL, &scalarMeta);
    assert(status == StatusOk);

    status = XcalarEval::get()->updateArgMappings(ast.astCommon.rootNode,
                                                  scalarMeta);
    BailIfFailed(status);

    status = XcalarEval::get()->aggregateEval(&ast, scalarOut, NULL, NULL);
    // scalar table freed by xcalarAggregateEval
    scalarTableCreated = false;
    BailIfFailedMsg(moduleName,
                    status,
                    "Aggregate(%lu) failed aggregateEval %s",
                    aggregateInput->dstTable.tableId,
                    strGetFromStatus(status));
    // Update the scalar size; if a UDF returned an invalid scalar
    scalarOut->updateUsedSize();

CommonExit:
    if (scalarTableCreated) {
        assert(status != StatusOk);
        xdbMgr->xdbDrop(scalarXdbId);
    }

    if (astGenerated) {
        // don't free table if we used the srcTable as the scalarTable
        if (ast.scalarXdbId == srcXdbId) {
            ast.scalarXdbId = XdbIdInvalid;
        }

        XcalarEval::get()->destroyClass2Ast(&ast);
        astGenerated = false;
    }

    if (gvmIssued) {
        cleanoutGvm(aggregateInput->srcTable.tableId,
                    aggregateInput->dstTable.tableId,
                    dagId,
                    status);
    }
    return status;
}

Status
Operators::buildScalarTable(Dag *dag,
                            ExExportBuildScalarTableInput *buildInput,
                            XdbId *tempXdbId,
                            XcalarApiUserId *user)
{
    Status status = StatusUnknown;
    NewTupleMeta tupMeta;
    ExColumnName *columnNames = buildInput->columns;
    bool xdbCreated = false;
    bool gvmIssued = false;
    unsigned numColumns = buildInput->numColumns;
    DhtId dhtId = DhtInvalidDhtId;
    const char *variableNames[buildInput->numColumns + 1];
    XdbMgr *xdbMgr = XdbMgr::get();
    DhtMgr *dhtMgr = DhtMgr::get();
    const char *keyName = DsDefaultDatasetKeyName;
    OperatorFlag opFlag = OperatorFlagNone;
    DagTypes::DagId dagId = dag->getId();

    assert(numColumns <= TupleMaxNumValuesPerRecord);

    // +1 for the rowNum key
    tupMeta.setNumFields(numColumns + 1);
    for (unsigned ii = 0; ii < numColumns; ii++) {
        tupMeta.setFieldType(DfScalarObj, ii);
    }
    tupMeta.setFieldType(DfInt64, numColumns);

    // Build column header alias pointer tables
    for (unsigned ii = 0; ii < numColumns; ii++) {
        // Verify that all of our name lengths are appropriate
        assert(strnlen(columnNames[ii].headerAlias,
                       sizeof(columnNames[ii].headerAlias)) <
               sizeof(columnNames[0].headerAlias));
        assert(strnlen(columnNames[ii].name, sizeof(columnNames[ii].name)) <
               sizeof(columnNames[0].name));
        variableNames[ii] = columnNames[ii].name;
    }
    variableNames[numColumns] = keyName;

    status = buildInput->sorted
                 ? dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemAscendingDht, &dhtId)
                 : dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemUnorderedDht, &dhtId);
    // Get for system DHT should always succeed
    assert(status == StatusOk);

    Ordering order = buildInput->sorted ? Ascending : Random;

    status = xdbMgr->xdbCreate(buildInput->dstXdbId,
                               keyName,
                               DfInt64,
                               numColumns,
                               &tupMeta,
                               NULL,
                               0,
                               variableNames,
                               numColumns + 1,
                               NULL,
                               0,
                               order,
                               XdbGlobal,
                               dhtId);
    dhtId = DhtInvalidDhtId;
    BailIfFailedMsg(moduleName,
                    status,
                    "BuildScalarTable(%lu) failed xdbCreate %s",
                    buildInput->dstXdbId,
                    strGetFromStatus(status));
    xdbCreated = true;

    gvmIssued = true;
    status = issueGvm(buildInput->srcXdbId,
                      buildInput->dstXdbId,
                      numColumns,
                      (char **) variableNames,
                      dagId,
                      opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "BuildScalarTable(%lu) failed issueGvm %s",
                    buildInput->dstXdbId,
                    strGetFromStatus(status));

    size_t buildInputSize;
    buildInputSize =
        sizeof(*buildInput) + numColumns * sizeof(buildInput->columns[0]);

    status = issueTwoPcForOp(buildInput,
                             buildInputSize,
                             MsgTypeId::Msg2pcExportMakeScalarTable,
                             TwoPcCallId::Msg2pcExportMakeScalarTable1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "BuildScalarTable(%lu) failed issueTwoPc %s",
                    buildInput->dstXdbId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(buildInput->dstXdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "BuildScalarTable(%lu) failed xdbLoadDone %s",
                    buildInput->dstXdbId,
                    strGetFromStatus(status));

    *tempXdbId = buildInput->dstXdbId;

    // We now have a scalar table that we can export
CommonExit:
    if (status != StatusOk && xdbCreated) {
        xdbMgr->xdbDrop(buildInput->dstXdbId);
        xdbCreated = false;
        *tempXdbId = 0;
    }

    if (gvmIssued) {
        cleanoutGvm(buildInput->srcXdbId, buildInput->dstXdbId, dagId, status);
    }
    return status;
}

Status
Operators::groupBy(Dag *dag,
                   XcalarApiGroupByInput *groupByInput,
                   size_t inputSize,
                   void *optimizerContext,
                   XcalarApiUserId *user)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool gvmIssued = false;
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    if (groupByInput->includeSrcTableSample) {
        opFlag = (OperatorFlag)(opFlag | OperatorFlagIncludeSrcTableInSample);
    }
    if (groupByInput->icvMode) {
        opFlag = (OperatorFlag)(opFlag | OperatorFlagIcvMode);
    }
    XcalarEvalClass2Ast *asts = NULL;
    char **variableNames = NULL;
    XcalarEval *xcalarEval = XcalarEval::get();
    unsigned numEvals = groupByInput->numEvals;
    uint64_t numFuncs = 0;
    bool astsCreated[numEvals];
    memZero(astsCreated, sizeof(astsCreated));
    int *evalArgIndices[numEvals];
    memZero(evalArgIndices, sizeof(evalArgIndices));
    DagTypes::DagId dagId = dag->getId();

    if (groupByInput->groupAll) {
        // XXX: the work for groupAll was done in createXdb, do nothing here
        status = StatusOk;
    } else {
        unsigned numVariables = 0;

        asts = (XcalarEvalClass2Ast *) memAlloc(numEvals * sizeof(*asts));
        BailIfNull(asts);

        for (unsigned ii = 0; ii < numEvals; ii++) {
            uint64_t numFunc;
            status =
                xcalarEval->generateClass2Ast(groupByInput->evalStrs[ii],
                                              &asts[ii],
                                              &numFunc,
                                              dagId,
                                              groupByInput->dstTable.xdbId);
            BailIfFailed(status);
            astsCreated[ii] = true;

            evalArgIndices[ii] = (int *) memAlloc(asts[ii].numUniqueVariables *
                                                  sizeof(*evalArgIndices[ii]));
            BailIfNull(evalArgIndices[ii]);

            numVariables += asts[ii].numUniqueVariables;
            numFuncs += numFunc;
        }

        // allocate for worst case numVariables
        variableNames =
            (char **) memAlloc(numVariables * sizeof(*variableNames));
        BailIfNull(variableNames);

        // redo count, this time removing duplicates
        numVariables = 0;
        for (unsigned ii = 0; ii < numEvals; ii++) {
            for (unsigned jj = 0; jj < asts[ii].numUniqueVariables; jj++) {
                char *varName = asts[ii].variableNames[jj];
                unsigned kk;
                for (kk = 0; kk < numVariables; kk++) {
                    if (strcmp(variableNames[kk], varName) == 0) {
                        break;
                    }
                }

                evalArgIndices[ii][jj] = kk;

                if (kk == numVariables) {
                    // first use of this variable
                    variableNames[numVariables++] = varName;
                }
            }
        }

        gvmIssued = true;
        status = issueGvm(groupByInput->srcTable.xdbId,
                          groupByInput->dstTable.xdbId,
                          numVariables,
                          variableNames,
                          dagId,
                          opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "GroupBy(%lu) failed issueGvm %s",
                        groupByInput->dstTable.tableId,
                        strGetFromStatus(status));

        status = issueTwoPcForOp(groupByInput,
                                 inputSize,
                                 MsgTypeId::Msg2pcXcalarApiGroupBy,
                                 TwoPcCallId::Msg2pcXcalarApiGroupBy1,
                                 dag,
                                 user,
                                 opFlag);
        BailIfFailedMsg(moduleName,
                        status,
                        "GroupBy(%lu) failed issueTwoPcForOp %s",
                        groupByInput->dstTable.tableId,
                        strGetFromStatus(status));
    }

    status = xdbMgr->xdbLoadDone(groupByInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "GroupBy(%lu) failed xdbLoadDone %s",
                    groupByInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (gvmIssued) {
        cleanoutGvm(groupByInput->srcTable.xdbId,
                    groupByInput->dstTable.xdbId,
                    dagId,
                    status);
    }

    if (variableNames != NULL) {
        memFree(variableNames);
        variableNames = NULL;
    }

    if (asts != NULL) {
        for (unsigned ii = 0; ii < numEvals; ii++) {
            if (astsCreated[ii]) {
                xcalarEval->destroyClass2Ast(&asts[ii]);
                astsCreated[ii] = false;
            }
        }
    }

    for (unsigned ii = 0; ii < numEvals; ii++) {
        memFree(evalArgIndices[ii]);
        evalArgIndices[ii] = NULL;
    }

    if (asts != NULL) {
        memFree(asts);
        asts = NULL;
    }

    return status;
}

Status
Operators::join(Dag *dag,
                XcalarApiJoinInput *joinInput,
                void *optimizerContext,
                XcalarApiUserId *user)
{
    Status status = StatusUnknown;
    XdbMgr *xdbMgr = XdbMgr::get();
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    bool gvmIssued = true;
    DagTypes::DagId dagId = dag->getId();

    status = issueGvm(XidInvalid,
                      joinInput->joinTable.xdbId,
                      0,
                      NULL,
                      dagId,
                      opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Join(%lu) failed issueGvm %s",
                    joinInput->joinTable.xdbId,
                    strGetFromStatus(status));

    size_t joinInputSize;
    joinInputSize = sizeof(*joinInput) +
                    (sizeof(joinInput->renameMap[0]) *
                     (joinInput->numLeftColumns + joinInput->numRightColumns));
    status = issueTwoPcForOp(joinInput,
                             joinInputSize,
                             MsgTypeId::Msg2pcXcalarApiJoin,
                             TwoPcCallId::Msg2pcXcalarApiJoin1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Join(%lu) failed issueTwoPcForOp %s",
                    joinInput->joinTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(joinInput->joinTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Join(%lu) failed xdbLoadDone %s",
                    joinInput->joinTable.tableId,
                    strGetFromStatus(status));
CommonExit:
    if (gvmIssued) {
        cleanoutGvm(XidInvalid, joinInput->joinTable.xdbId, dagId, status);
    }
    return status;
}

Status
Operators::filterAndMapGlobal(Dag *dag,
                              const XdbId srcXdbId,
                              const XdbId dstXdbId,
                              unsigned numEvals,
                              char **evalStrs,
                              OperatorFlag opFlag)
{
    Status status = StatusOk;
    char **variableNames = NULL;
    unsigned numVariables = 0;
    bool astsCreated[numEvals];
    memZero(astsCreated, sizeof(astsCreated));
    XcalarEval *xcalarEval = XcalarEval::get();
    XcalarEvalClass1Ast *asts = NULL;
    DagTypes::DagId dagId = dag->getId();

    asts = (XcalarEvalClass1Ast *) memAlloc(numEvals * sizeof(*asts));
    BailIfNull(asts);

    for (unsigned ii = 0; ii < numEvals; ii++) {
        status = xcalarEval->generateClass1Ast(evalStrs[ii], &asts[ii]);
        BailIfFailed(status);
        astsCreated[ii] = true;
        numVariables += asts[ii].astCommon.numScalarVariables;

        if (xcalarEval->containsUdf(asts[ii].astCommon.rootNode)) {
            // Will require childnodes for UDF processing.
            opFlag = (OperatorFlag)(opFlag | OperatorFlagChildProcessing);
        }
    }

    // allocate for worst case numVariables
    variableNames = (char **) memAlloc(numVariables * sizeof(*variableNames));
    BailIfNull(variableNames);

    // redo count, this time removing duplicates
    numVariables = 0;
    for (unsigned ii = 0; ii < numEvals; ii++) {
        for (unsigned jj = 0; jj < asts[ii].astCommon.numScalarVariables;
             jj++) {
            char *varName = asts[ii].astCommon.scalarVariables[jj].variableName;
            unsigned kk = 0;
            for (kk = 0; kk < numVariables; kk++) {
                if (strcmp(variableNames[kk], varName) == 0) {
                    break;
                }
            }
            if (kk == numVariables) {
                // first use of this variable
                variableNames[numVariables++] = varName;
            }
        }
    }

    status = issueGvm(srcXdbId,
                      dstXdbId,
                      numVariables,
                      variableNames,
                      dagId,
                      opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "filterAndMapGlobal(%lu) failed issueGvm %s",
                    dstXdbId,
                    strGetFromStatus(status));

CommonExit:
    if (variableNames != NULL) {
        memFree(variableNames);
        variableNames = NULL;
    }

    if (asts != NULL) {
        for (unsigned ii = 0; ii < numEvals; ii++) {
            if (astsCreated[ii]) {
                xcalarEval->destroyClass1Ast(&asts[ii]);
                astsCreated[ii] = false;
            }
        }
        memFree(asts);
        asts = NULL;
    }

    return status;
}

Status
Operators::map(Dag *dag,
               XcalarApiMapInput *mapInput,
               size_t inputSize,
               void *optimizerContext,
               XcalarApiUserId *user)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool issuedGvm = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    uintptr_t retPerNodeCookies[nodeCount];
    memZero(retPerNodeCookies, sizeof(retPerNodeCookies));
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    if (mapInput->icvMode) {
        opFlag = (OperatorFlag)(opFlag | OperatorFlagIcvMode);
    }
    DagTypes::DagId dagId = dag->getId();

    issuedGvm = true;
    status = filterAndMapGlobal(dag,
                                mapInput->srcTable.xdbId,
                                mapInput->dstTable.xdbId,
                                mapInput->numEvals,
                                mapInput->evalStrs,
                                opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Map(%lu) failed filterAndMapGlobal %s",
                    mapInput->dstTable.xdbId,
                    strGetFromStatus(status));

    status = issueTwoPcForOp(mapInput,
                             inputSize,
                             MsgTypeId::Msg2pcXcalarApiMap,
                             TwoPcCallId::Msg2pcXcalarApiMap1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Map(%lu) failed issueTwoPcForOp %s",
                    mapInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(mapInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Map(%lu) failed xdbLoadDone %s",
                    mapInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (issuedGvm) {
        cleanoutGvm(mapInput->srcTable.xdbId,
                    mapInput->dstTable.xdbId,
                    dagId,
                    status);
    }

    return status;
}

Status
Operators::unionOp(Dag *dag,
                   XcalarApiUnionInput *unionInput,
                   size_t inputSize,
                   void *optimizerContext,
                   XcalarApiUserId *user)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    DagTypes::DagId dagId = dag->getId();
    bool issuedGvm = true;

    status = issueGvm(XidInvalid,
                      unionInput->dstTable.xdbId,
                      0,
                      NULL,
                      dagId,
                      opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Union(%lu) failed issueGvm %s",
                    unionInput->dstTable.xdbId,
                    strGetFromStatus(status));

    status = issueTwoPcForOp(unionInput,
                             inputSize,
                             MsgTypeId::Msg2pcXcalarApiUnion,
                             TwoPcCallId::Msg2pcXcalarApiUnion1,
                             dag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Union(%lu) failed issueTwoPcForOp %s",
                    unionInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(unionInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Union(%lu) failed xdbLoadDone %s",
                    unionInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (issuedGvm) {
        cleanoutGvm(XidInvalid, unionInput->dstTable.xdbId, dagId, status);
    }
    return status;
}

Status
Operators::getTableMeta(XcalarApiGetTableMetaInput *getTableMetaInputIn,
                        XcalarApiOutput **output,
                        size_t *outputSizeOut,
                        DagTypes::DagId dagId,
                        XcalarApiUserId *user)
{
    Status status = StatusOk;
    XcalarApiGetTableMetaOutput *getTableMetaOutput = NULL;
    size_t outputSize;
    XcalarApiOutput *out = NULL;
    Config *config = Config::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    bool nodeRefObtained = false;
    DgDagState state;
    Dataset *ds = Dataset::get();
    DsDataset *dataset = NULL;
    DatasetRefHandle dsRefHandle;
    Dag *dstGraph = NULL;
    ResultSetId *cursorRefs = NULL;
    unsigned numCursorRefs = 0;
    DagTypes::NodeId nodeId = getTableMetaInputIn->tableNameInput.nodeId;
    Xid xid = getTableMetaInputIn->tableNameInput.xid;
    GetTableMetaInput getTableMetaInput;
    unsigned ii = 0;
    MsgEphemeral eph;
    TableNsMgr::TableHandleTrack handleTrack;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    XdbId xdbId = XdbIdInvalid;

    dstGraph = DagLib::get()->getDagLocal(dagId);
    assert(dstGraph != NULL);

    XcalarApiUdfContainer *sessionContainer;
    sessionContainer = dstGraph->getSessionContainer();

    if (getTableMetaInputIn->tableNameInput.isTable) {
        status = dstGraph->getDagNodeStateAndRef(nodeId, &state);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error getting Dag node state and ref for table %ld: %s",
                    nodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        nodeRefObtained = true;

        if (state == DgDagStateError) {
            status = StatusDgDagNodeError;
            goto CommonExit;
        }

        status = dstGraph->getTableIdFromNodeId(nodeId, &handleTrack.tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getTableIdFromNodeId for dagNode %lu: %s",
                        nodeId,
                        strGetFromStatus(status));

        status = tnsMgr->openHandleToNs(sessionContainer,
                                        handleTrack.tableId,
                                        LibNsTypes::ReaderShared,
                                        &handleTrack.tableHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %ld: %s",
                        handleTrack.tableId,
                        strGetFromStatus(status));
        handleTrack.tableHandleValid = true;
    } else {
        status = ds->openHandleToDatasetById(xid,
                                             user,
                                             &dataset,
                                             LibNsTypes::ReaderShared,
                                             &dsRefHandle);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retriving dataset for %ld: %s",
                    xid,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    getTableMetaInput.isTable = getTableMetaInputIn->tableNameInput.isTable;
    getTableMetaInput.xid = xid;
    getTableMetaInput.isPrecise = getTableMetaInputIn->isPrecise;
    getTableMetaInput.nodeId = nodeId;

    outputSize =
        XcalarApiSizeOfOutput(*getTableMetaOutput) +
        (sizeof(getTableMetaOutput->metas[0]) * config->getActiveNodes());
    out = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (out == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(out, outputSize);

    getTableMetaOutput = &out->outputResult.getTableMetaOutput;

    if (getTableMetaInput.isTable) {
        xdbId = dstGraph->getXdbIdFromNodeId(getTableMetaInput.nodeId);

        getTableMetaInput.xid = xdbId;

        status =
            xdbMgr->xdbGetXcalarApiTableMetaGlobal(xdbId, getTableMetaOutput);
        assert(status == StatusOk || status == StatusXdbNotFound);
        BailIfFailed(status);
    } else {
        getTableMetaOutput->numMetas = config->getActiveNodes();
    }

    if (UserDefinedFunction::containerWithWbScope(sessionContainer)) {
        status = UserMgr::get()
                     ->getResultSetIdsByTableId(handleTrack.tableId,
                                                &sessionContainer->userId,
                                                &sessionContainer->sessionInfo,
                                                &cursorRefs,
                                                &numCursorRefs);
        BailIfFailed(status);
    }

    if (numCursorRefs > ArrayLen(getTableMetaOutput->resultSetIds)) {
        status = StatusTooManyResultSets;
        goto CommonExit;
    }
    getTableMetaOutput->numResultSets = numCursorRefs;
    for (unsigned ii = 0; ii < numCursorRefs; ii++) {
        getTableMetaOutput->resultSetIds[ii] = cursorRefs[ii];
    }

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &getTableMetaInput,
                                      sizeof(getTableMetaInput),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcXcalarApiGetTableMeta1,
                                      out,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    TwoPcHandle twoPcHandle;
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcXcalarApiGetTableMeta,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);

    getTableMetaOutput->xdbId = xdbId;
    for (ii = 0; ii < getTableMetaOutput->numMetas; ii++) {
        if (getTableMetaOutput->metas[ii].status != StatusOk.code()) {
            status.fromStatusCode(getTableMetaOutput->metas[ii].status);
            goto CommonExit;
        }
    }

    assert(outputSize > 0);
    if (outputSizeOut != NULL) {
        *outputSizeOut = outputSize;
    }

CommonExit:
    if (status != StatusOk) {
        if (out != NULL) {
            memFree(out);
            out = NULL;
        }
    }

    if (cursorRefs) {
        delete[] cursorRefs;
        cursorRefs = NULL;
    }

    if (nodeRefObtained) {
        dstGraph->putDagNodeRefById(nodeId);
    }

    if (handleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
        handleTrack.tableHandleValid = false;
    }

    if (dataset != NULL) {
        Status status2 = ds->closeHandleToDataset(&dsRefHandle);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error closing dataset for %ld: %s",
                    nodeId,
                    strGetFromStatus(status2));
            if (status == StatusOk) {
                status = status2;
            }
        }
    }

    *output = out;
    return status;
}

Status
Operators::getRowCount(DagTypes::NamedInput *tableNameInput,
                       DagTypes::DagId dagId,
                       uint64_t *rowCountOut,
                       XcalarApiUserId *user)
{
    Status status;
    size_t outputSize;
    XcalarApiOutput *apiOut = NULL;
    uint64_t rowCount = 0;
    XcalarApiGetTableMetaInput getTableMetaInput;
    memcpy(&getTableMetaInput.tableNameInput,
           tableNameInput,
           sizeof(DagTypes::NamedInput));
    getTableMetaInput.isPrecise = false;

    status =
        getTableMeta(&getTableMetaInput, &apiOut, &outputSize, dagId, user);
    BailIfFailed(status);

    for (unsigned ii = 0; ii < Config::get()->getActiveNodes(); ii++) {
        rowCount += apiOut->outputResult.getTableMetaOutput.metas[ii].numRows;
    }

CommonExit:
    if (apiOut) {
        memFree(apiOut);
        apiOut = NULL;
    }

    *rowCountOut = rowCount;
    return status;
}

Status
Operators::getRowNum(Dag *srcDag,
                     Dag *dstDag,
                     XcalarApiGetRowNumInput *getRowNumInput,
                     void *optimizerContext,
                     XcalarApiUserId *user)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    Config *config = Config::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    OperatorFlag opFlag = OperatorFlagNone;
    Optimizer::DagNodeAnnotations *annotations =
        (Optimizer::DagNodeAnnotations *) optimizerContext;
    if (annotations != NULL) {
        opFlag = (OperatorFlag)(opFlag | annotations->flags);
    }
    bool issuedGvm = false;
    XcalarApiGetTableMetaInput getTableMetaInput;
    DagTypes::DagId dstDagId = dstDag->getId();

    getTableMetaInput.tableNameInput.isTable = true;
    getTableMetaInput.tableNameInput.nodeId = getRowNumInput->srcTable.tableId;
    getTableMetaInput.tableNameInput.xid = getRowNumInput->srcTable.xdbId;
    getTableMetaInput.isPrecise = false;

    status = Operators::get()->getTableMeta(&getTableMetaInput,
                                            &output,
                                            NULL,
                                            srcDag->getId(),
                                            user);
    BailIfFailedMsg(moduleName,
                    status,
                    "GetRowNum(%lu) failed getTableMeta %s",
                    getRowNumInput->dstTable.tableId,
                    strGetFromStatus(status));

    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        getRowNumInput->rowCountPerNode[ii] =
            output->outputResult.getTableMetaOutput.metas[ii].numRows;
    }

    issuedGvm = true;
    status = issueGvm(getRowNumInput->srcTable.xdbId,
                      getRowNumInput->dstTable.xdbId,
                      0,
                      NULL,
                      dstDagId,
                      opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "GetRowNum(%lu) failed issueGvm %s",
                    getRowNumInput->dstTable.xdbId,
                    strGetFromStatus(status));

    status = issueTwoPcForOp(getRowNumInput,
                             sizeof(*getRowNumInput),
                             MsgTypeId::Msg2pcXcalarApiGetRowNum,
                             TwoPcCallId::Msg2pcXcalarApiGetRowNum1,
                             dstDag,
                             user,
                             opFlag);
    BailIfFailedMsg(moduleName,
                    status,
                    "GetRowNum(%lu) failed issueTwoPcForOp %s",
                    getRowNumInput->dstTable.tableId,
                    strGetFromStatus(status));

    status = xdbMgr->xdbLoadDone(getRowNumInput->dstTable.xdbId);
    BailIfFailedMsg(moduleName,
                    status,
                    "GetRowNum(%lu) failed xdbLoadDone %s",
                    getRowNumInput->dstTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
    if (issuedGvm) {
        cleanoutGvm(getRowNumInput->srcTable.xdbId,
                    getRowNumInput->dstTable.xdbId,
                    dstDagId,
                    status);
    }

    return status;
}

bool
Operators::isDemystificationRequired(char **variableNames,
                                     unsigned numVariables,
                                     const XdbMeta *srcMeta)
{
    const NewTupleMeta *tupMeta;
    unsigned ii, jj;
    bool found;

    tupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    for (ii = 0; ii < numVariables; ii++) {
        found = false;
        for (jj = 0; jj < tupMeta->getNumFields(); jj++) {
            // skip fatptr type as derived field can have the
            // same name as that of fatptr
            if ((tupMeta->getFieldType(jj) != DfFatptr) &&
                (strcmp(variableNames[ii],
                        srcMeta->kvNamedMeta.valueNames_[jj]) == 0)) {
                found = true;
                break;
            }
        }

        if (!found) {
            return true;
        }
    }

    return false;
}

bool
Operators::isDemystificationRequired(const XdbMeta *srcMeta)
{
    const NewTupleMeta *tupMeta;
    tupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;

    for (unsigned ii = 0; ii < tupMeta->getNumFields(); ii++) {
        if (tupMeta->getFieldType(ii) == DfFatptr) {
            return true;
        }
    }

    return false;
}

// XXX: Refactor this code to leverage getOrResolveRegisteredFn
Status
Operators::convertLoadUdfToAbsolutePath(DfLoadArgs *loadArgs,
                                        XcalarApiUdfContainer *udfContainer)
{
    Status status;
    const char *udfModuleName;
    const char *udfModuleVersion;
    const char *udfFunctionName;
    char udfModuleFunctionName[XcalarApiMaxUdfModuleNameLen + 1];
    char parserFnNameCopy[UdfVersionedFQFname];
    char parserFnNameToUse[UdfVersionedFQFname];
    UserDefinedFunction *udfLib = UserDefinedFunction::get();
    XcalarApiUdfContainer sharedUdfContainer;

    // Copy parser name as parseFunctionName modifies what is
    // passed to it.
    strlcpy(parserFnNameCopy,
            loadArgs->parseArgs.parserFnName,
            sizeof(parserFnNameCopy));

    status = UserDefinedFunction::parseFunctionName(parserFnNameCopy,
                                                    &udfModuleName,
                                                    &udfModuleVersion,
                                                    &udfFunctionName);
    BailIfFailed(status);

    snprintf(udfModuleFunctionName,
             sizeof(udfModuleFunctionName),
             "%s:%s",
             udfModuleName,
             udfFunctionName);

    // Replace the relative parser name with the fully qualified name
    // which may be within the workbook (if present) or in the sharedUDFs
    // space.
    status = udfLib->getUdfName(parserFnNameToUse,
                                sizeof(parserFnNameToUse),
                                udfModuleFunctionName,
                                udfContainer,
                                false);
    BailIfFailed(status);

    if (!udfLib->udfModuleExistsInNamespace(parserFnNameToUse)) {
        // Try using sharedUDFs
        memset(&sharedUdfContainer, 0, sizeof(sharedUdfContainer));
        status = udfLib->getUdfName(parserFnNameToUse,
                                    sizeof(parserFnNameToUse),
                                    udfModuleFunctionName,
                                    &sharedUdfContainer,
                                    false);
        BailIfFailed(status);
        if (!udfLib->udfModuleExistsInNamespace(parserFnNameToUse)) {
            status = StatusUdfModuleNotFound;
            goto CommonExit;
        }
    }

    status = strStrlcpy(loadArgs->parseArgs.parserFnName,
                        parserFnNameToUse,
                        sizeof(loadArgs->parseArgs.parserFnName));
    BailIfFailedMsg(moduleName,
                    status,
                    "convertLoadUdfToAbsolutePath: size (%lu) of loadArgs "
                    "parserFnName insufficient to store %s: %s",
                    sizeof(loadArgs->parseArgs.parserFnName),
                    parserFnNameToUse,
                    strGetFromStatus(status));

CommonExit:
    return status;
}
