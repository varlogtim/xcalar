// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "bc/BufferCache.h"
#include "util/System.h"
#include "util/Random.h"
#include "libapis/LibApisRecv.h"
#include "util/MemTrack.h"
#include "util/IntHashTable.h"

// With libgraph, the query library will define its own query nodes,
// rather than have to rely on a common DagNode
#include "optimizer/Optimizer.h"
#include "xdb/Xdb.h"
#include "msg/Message.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "dataset/Dataset.h"
#include "operators/XcalarEval.h"
#include "libapis/WorkItem.h"
#include "runtime/Semaphore.h"
#include "udf/UserDefinedFunction.h"
#include "dataformat/DataFormatCsv.h"
#include "session/Sessions.h"
#include "queryparser/QueryParser.h"
#include "table/Table.h"

Optimizer *Optimizer::instance = NULL;
static constexpr const char *moduleName = "liboptimize";

Optimizer *
Optimizer::get()
{
    return Optimizer::instance;
}

Status
Optimizer::init()
{
    Status status;
    assert(instance == NULL);
    instance = new (std::nothrow) Optimizer;
    if (instance == NULL) {
        return StatusNoMem;
    }

    status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }
    return status;
}

void
Optimizer::destroy()
{
    if (instance == NULL) {
        return;
    }

    instance->destroyInternal();
    delete instance;
    instance = NULL;
}

void
Optimizer::destroyInternal()
{
    if (fieldsBcHandle_ != NULL) {
        BcHandle::destroy(&fieldsBcHandle_);
        fieldsBcHandle_ = NULL;
    }

    if (annotationsBcHandle_ != NULL) {
        BcHandle::destroy(&annotationsBcHandle_);
        annotationsBcHandle_ = NULL;
    }
}

Status
Optimizer::initInternal()
{
    Status status = StatusUnknown;

    annotationsBcHandle_ =
        BcHandle::create(BufferCacheObjects::OptimizerQueryAnnotations);
    if (annotationsBcHandle_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    fieldsBcHandle_ =
        BcHandle::create(BufferCacheObjects::OptimizerQueryFieldsElt);
    if (fieldsBcHandle_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        destroyInternal();
    }

    return status;
}

Status
Optimizer::getProjectedFieldNames(
    void *optimizerContext,
    char (**projectedFieldNamesOut)[DfMaxFieldNameLen + 1],
    unsigned *numProjectedFieldNamesOut)
{
    Status status = StatusUnknown;
    DagNodeAnnotations *annotations = (DagNodeAnnotations *) optimizerContext;
    FieldsRequiredSet *fieldsRequiredSet = &annotations->fieldsRequiredSet;
    char(*projectedFieldNames)[DfMaxFieldNameLen + 1];
    unsigned ii = 0;
    size_t ret;

    projectedFieldNames = (typeof(projectedFieldNames))
        memAllocExt(sizeof(*projectedFieldNames) *
                        fieldsRequiredSet->numFieldsRequired,
                    moduleName);
    if (projectedFieldNames == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    Field *field;
    for (FieldSetHashTable::iterator it = fieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        assert(ii < fieldsRequiredSet->numFieldsRequired);
        ret = strlcpy(projectedFieldNames[ii],
                      field->fieldName,
                      sizeof(projectedFieldNames[ii]));
        assert(ret <= sizeof(projectedFieldNames[ii]));
        ii++;
    }

    assert(ii == fieldsRequiredSet->numFieldsRequired);

    *projectedFieldNamesOut = projectedFieldNames;
    *numProjectedFieldNamesOut = ii;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (projectedFieldNames != NULL) {
            memFree(projectedFieldNames);
            projectedFieldNames = NULL;
        }
    }

    return status;
}

Status
Optimizer::populateValuesDescWithFieldsRequired(void *optimizerContext,
                                                unsigned numKeys,
                                                int *keyIndexes,
                                                const char *keyNames[],
                                                const NewTupleMeta *srcTupMeta,
                                                NewTupleMeta *dstTupMeta,
                                                XdbMeta *srcMeta,
                                                const char *immediateNames[],
                                                unsigned &numImmediates,
                                                const char *fatptrPrefixNames[],
                                                unsigned &numFatptrs)
{
    Status status = StatusOk;
    DagNodeAnnotations *annotations = (DagNodeAnnotations *) optimizerContext;
    FieldsRequiredSet *fieldsRequiredSet = &annotations->fieldsRequiredSet;
    unsigned ii = 0;
    unsigned numValues = 0;
    int keyNumMapping[srcTupMeta->getNumFields()];

    Field **fieldsRequired = (Field **) memAlloc(
        sizeof(*fieldsRequired) * fieldsRequiredSet->numFieldsRequired);
    BailIfNull(fieldsRequired);

    Field *field;
    for (FieldSetHashTable::iterator it = fieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        fieldsRequired[ii] = field;

        DataFormat::replaceFatptrPrefixDelims(field->fieldName);
        ii++;
    }

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

    for (ii = 0; ii < srcTupMeta->getNumFields(); ii++) {
        DfFieldType srcTypeTmp = srcTupMeta->getFieldType(ii);
        bool srcIsImmediate = srcTypeTmp != DfFatptr;

        bool keepColumn = false;
        const char *newFieldName = srcMeta->kvNamedMeta.valueNames_[ii];

        // search through required columns for renames
        for (unsigned jj = 0; jj < fieldsRequiredSet->numFieldsRequired; jj++) {
            if (fieldsRequired[jj]->isImmediate != srcIsImmediate) {
                continue;
            }

            if (strcmp(fieldsRequired[jj]->fieldName,
                       srcMeta->kvNamedMeta.valueNames_[ii]) == 0) {
                keepColumn = true;

                if (strlen(fieldsRequired[jj]->newFieldName) > 0) {
                    newFieldName = fieldsRequired[jj]->newFieldName;
                }
                break;
            }
        }

        if (keyNumMapping[ii] != NewTupleMeta::DfInvalidIdx) {
            // always carry over keys
            keepColumn = true;
            keyNames[keyNumMapping[ii]] = newFieldName;
        }

        if (keepColumn) {
            if (srcIsImmediate) {
                immediateNames[numImmediates] = newFieldName;
                numImmediates++;
            } else {
                fatptrPrefixNames[numFatptrs] = newFieldName;
                numFatptrs++;
            }
            dstTupMeta->setFieldType(srcTypeTmp, numValues);

            if (keyNumMapping[ii] != NewTupleMeta::DfInvalidIdx) {
                keyIndexes[keyNumMapping[ii]] = numValues;
            }

            numValues++;
        }
    }

    dstTupMeta->setNumFields(numValues);
CommonExit:
    if (fieldsRequired) {
        memFree(fieldsRequired);
        fieldsRequired = NULL;
    }

    return status;
}

void
Optimizer::Field::del()
{
    Optimizer::get()->freeField(this);
}

void
Optimizer::freeField(Field *field)
{
    fieldsBcHandle_->freeBuf(field);
    field = NULL;
}

Status
Optimizer::newField(const char *fieldName,
                    Field **fieldOut,
                    XdfTypesAccepted typesAccepted)
{
    Status status = StatusUnknown;
    Field *field;
    size_t ret;

    assert(fieldName != NULL);
    assert(fieldOut != NULL);

    field = (Field *) fieldsBcHandle_->allocBuf(XidInvalid, &status);
    if (field == NULL) {
        goto CommonExit;
    }
    new (field) Field();

    ret = strlcpy(field->fieldName, fieldName, sizeof(field->fieldName));
    if (ret > sizeof(field->fieldName)) {
        status = StatusOverflow;
        goto CommonExit;
    }

    field->typesAccepted = typesAccepted;

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (field != NULL) {
            fieldsBcHandle_->freeBuf(field);
            field = NULL;
        }
    }
    *fieldOut = field;

    return status;
}

void
Optimizer::removeAnnotations(DagNodeAnnotations *dagNodeAnnotations)
{
    assert(dagNodeAnnotations != NULL);
    if (dagNodeAnnotations->fieldsRequiredSet.hashTable != NULL) {
        dagNodeAnnotations->fieldsRequiredSet.hashTable->removeAll(&Field::del);
        delete dagNodeAnnotations->fieldsRequiredSet.hashTable;
        dagNodeAnnotations->fieldsRequiredSet.hashTable = NULL;
        dagNodeAnnotations->fieldsRequiredSet.numFieldsRequired = 0;
    }
    annotationsBcHandle_->freeBuf(dagNodeAnnotations);
    dagNodeAnnotations = NULL;
}

Optimizer::DagNodeAnnotations *
Optimizer::newDagNodeAnnotations(uint64_t numChildren)
{
    Status status = StatusUnknown;
    DagNodeAnnotations *dagNodeAnnotations = NULL;

    dagNodeAnnotations =
        (DagNodeAnnotations *) annotationsBcHandle_->allocBuf(
            XidInvalid, &status);
    if (dagNodeAnnotations == NULL) {
        goto CommonExit;
    }

    dagNodeAnnotations->fieldsRequiredSet.numFieldsRequired = 0;

    dagNodeAnnotations->fieldsRequiredSet.hashTable =
        new (std::nothrow) FieldSetHashTable();
    if (dagNodeAnnotations->fieldsRequiredSet.hashTable == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    dagNodeAnnotations->numChildrenLeftToProcess = numChildren;
    dagNodeAnnotations->flags = OperatorFlagNone;

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (dagNodeAnnotations != NULL) {
            removeAnnotations(dagNodeAnnotations);
            dagNodeAnnotations = NULL;
        }
    }

    return dagNodeAnnotations;
}

void
Optimizer::insertField(FieldsRequiredSet *fieldsRequiredSet, Field *field)
{
    Field *oldField;
    if ((oldField = fieldsRequiredSet->hashTable->find(field->fieldName)) !=
        NULL) {
        oldField->typesAccepted =
            (XdfTypesAccepted)(oldField->typesAccepted & field->typesAccepted);

        field->del();
        field = NULL;
        return;
    }

    fieldsRequiredSet->hashTable->insert(field);
    fieldsRequiredSet->numFieldsRequired++;
    // XXX We need to do 1 more round of pruning.
    //    assert(fieldsRequiredSet->numFieldsRequired <=
    //           TupleMaxNumValuesPerRecord);
}

Status
Optimizer::initDagNodeAnnotations(Dag *dag,
                                  DagNodeTypes::Node **leafNodesOut[],
                                  uint64_t *numLeafNodesOut)
{
    Status status = StatusUnknown;
    uint64_t numLeafNodes = 0, numNodes, ii;
    DagTypes::NodeId nextNodeId;
    DagNodeTypes::Node *currNode;
    DagNodeTypes::Node **leafNodes = NULL, **leafNodesRealloc = NULL;
    DagNodeAnnotations *dagNodeAnnotations = NULL;

    assert(dag != NULL);
    assert(leafNodesOut != NULL);
    assert(numLeafNodesOut != NULL);

    numNodes = dag->getNumNode();

    // Allocate for worst case
    leafNodes =
        (DagNodeTypes::Node **) memAllocExt(sizeof(*leafNodes) * numNodes,
                                            moduleName);
    if (leafNodes == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    nextNodeId = dag->getFirstNodeIdInOrder();
    for (ii = 0; ii < numNodes; ii++) {
        assert(nextNodeId != DagTypes::InvalidDagNodeId);
        status = dag->lookupNodeById(nextNodeId, &currNode);
        assert(status == StatusOk);
        assert(currNode != NULL);

        dagNodeAnnotations = newDagNodeAnnotations(currNode->numChild);
        if (dagNodeAnnotations == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        currNode->annotations = dagNodeAnnotations;
        dagNodeAnnotations = NULL;

        if (currNode->numChild == 0) {
            leafNodes[numLeafNodes++] = currNode;
        }

        nextNodeId = currNode->dagNodeHdr.dagNodeOrder.next;
    }
    assert(nextNodeId == DagTypes::InvalidDagNodeId);
    currNode = NULL;

    assert(numLeafNodes > 0);
    leafNodesRealloc =
        (DagNodeTypes::Node **) memReallocExt(leafNodes,
                                              sizeof(*leafNodes) * numLeafNodes,
                                              moduleName);
    if (leafNodesRealloc == NULL) {
        status = StatusReallocShrinkFailed;
        goto CommonExit;
    }
    leafNodes = leafNodesRealloc;
    leafNodesRealloc = NULL;

    *leafNodesOut = leafNodes;
    *numLeafNodesOut = numLeafNodes;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (leafNodesRealloc != NULL) {
            assert(leafNodes == NULL);
            memFree(leafNodesRealloc);
            leafNodesRealloc = NULL;
        }

        if (leafNodes != NULL) {
            assert(leafNodesRealloc == NULL);
            memFree(leafNodes);
            leafNodes = NULL;
        }

        if (dagNodeAnnotations != NULL) {
            removeAnnotations(dagNodeAnnotations);
            dagNodeAnnotations = NULL;
        }
    }

    assert(dagNodeAnnotations == NULL);

    return status;
}

Status
Optimizer::getFieldsProduced(DagNodeTypes::Node *node,
                             const char ***fieldsOut,
                             unsigned *numFieldsOut)
{
    Status status = StatusOk;
    const char **fieldsProduced = NULL;
    unsigned numFieldsProduced = 0;

    switch (node->dagNodeHdr.apiDagNodeHdr.api) {
    case XcalarApiMap:
        numFieldsProduced = node->dagNodeHdr.apiInput->mapInput.numEvals;
        fieldsProduced = (const char **) memAllocExt(sizeof(*fieldsProduced) *
                                                         numFieldsProduced,
                                                     moduleName);
        BailIfNullWith(fieldsProduced, StatusNoMem);

        for (unsigned ii = 0; ii < numFieldsProduced; ii++) {
            fieldsProduced[ii] =
                node->dagNodeHdr.apiInput->mapInput.newFieldNames[ii];
        }
        break;
    case XcalarApiSynthesize:
        numFieldsProduced =
            node->dagNodeHdr.apiInput->synthesizeInput.columnsCount;
        fieldsProduced = (const char **) memAllocExt(sizeof(*fieldsProduced) *
                                                         numFieldsProduced,
                                                     moduleName);
        BailIfNullWith(fieldsProduced, StatusNoMem);

        for (unsigned ii = 0; ii < numFieldsProduced; ii++) {
            fieldsProduced[ii] =
                node->dagNodeHdr.apiInput->synthesizeInput.columns[ii].newName;
        }
        break;
    case XcalarApiGroupBy: {
        // if srcTable sample wasn't included, we added the key as an immediate
        XcalarApiGroupByInput *groupByInput =
            &node->dagNodeHdr.apiInput->groupByInput;

        numFieldsProduced = groupByInput->numEvals + groupByInput->numKeys;

        fieldsProduced = (const char **) memAllocExt(sizeof(*fieldsProduced) *
                                                         numFieldsProduced,
                                                     moduleName);
        BailIfNullWith(fieldsProduced, StatusNoMem);

        for (unsigned ii = 0; ii < groupByInput->numEvals; ii++) {
            fieldsProduced[ii] = groupByInput->newFieldNames[ii];
        }

        for (unsigned ii = groupByInput->numEvals;
             ii < groupByInput->numKeys + groupByInput->numEvals;
             ii++) {
            int keyIdx = ii - groupByInput->numEvals;
            fieldsProduced[ii] = groupByInput->keys[keyIdx].keyFieldName;
        }

        break;
    }
    case XcalarApiGetRowNum:
        numFieldsProduced = 1;
        fieldsProduced = (const char **) memAllocExt(sizeof(*fieldsProduced) *
                                                         numFieldsProduced,
                                                     moduleName);
        BailIfNullWith(fieldsProduced, StatusNoMem);

        fieldsProduced[0] =
            node->dagNodeHdr.apiInput->getRowNumInput.newFieldName;
        break;
    case XcalarApiIndex: {
        XcalarApiIndexInput *indexInput =
            &node->dagNodeHdr.apiInput->indexInput;

        fieldsProduced = (const char **) memAllocExt(sizeof(*fieldsProduced) *
                                                         indexInput->numKeys,
                                                     moduleName);
        BailIfNullWith(fieldsProduced, StatusNoMem);

        for (unsigned ii = 0; ii < indexInput->numKeys; ii++) {
            // only prefixed indexes produce new fields
            if (!indexInput->source.isTable ||
                strstr(indexInput->keys[ii].keyName, DfFatptrPrefixDelimiter)) {
                fieldsProduced[numFieldsProduced++] =
                    indexInput->keys[ii].keyFieldName;
            }
        }
    }

    // The following doesn't produce any fields
    case XcalarApiJoin:
    case XcalarApiUnion:
    case XcalarApiFilter:
    case XcalarApiAggregate:
    case XcalarApiProject:
    case XcalarApiBulkLoad:
    case XcalarApiExport:
    case XcalarApiDeleteObjects:
    case XcalarApiSelect:
        break;
    default:
        status = StatusUnimpl;
        break;
    }

CommonExit:
    *numFieldsOut = numFieldsProduced;
    *fieldsOut = fieldsProduced;

    return status;
}

Status
Optimizer::updateParentsFieldsRequiredSet(DagNodeTypes::Node *parentNode,
                                          unsigned parentIdx,
                                          DagNodeTypes::Node *currNode,
                                          FieldsRequiredSet *fieldsConsumed)
{
    const char **fieldsProduced = NULL;
    unsigned numFieldsProduced = 0;
    Status status = StatusUnknown;
    DagNodeAnnotations *parentsAnnotations, *currAnnotations;
    FieldsRequiredSet *parentsFieldsRequiredSet, *currFieldsRequiredSet;
    Field *fieldCopy = NULL;
    char fieldNameBuf[DfMaxFieldNameLen + 1];
    bool isFromOtherParent = false;

    assert(parentNode != NULL);
    assert(currNode != NULL);

    parentsAnnotations = (DagNodeAnnotations *) parentNode->annotations;
    parentsFieldsRequiredSet = &parentsAnnotations->fieldsRequiredSet;
    currAnnotations = (DagNodeAnnotations *) currNode->annotations;
    currFieldsRequiredSet = &currAnnotations->fieldsRequiredSet;

    // Get fields produced by currNode
    status = getFieldsProduced(currNode, &fieldsProduced, &numFieldsProduced);
    if (status != StatusOk) {
        return status;
    }

    char fieldsProducedConverted[numFieldsProduced][DfMaxFieldNameLen + 1];

    // convert fields produced into accessor names since we will be comparing
    // with the accesor names in fieldsRequiredSet
    for (unsigned ii = 0; ii < numFieldsProduced; ii++) {
        char *prefixPtr = NULL;
        status = strStrlcpy(fieldsProducedConverted[ii],
                            fieldsProduced[ii],
                            sizeof(fieldsProducedConverted[ii]));
        BailIfFailed(status);
        prefixPtr =
            strstr(fieldsProducedConverted[ii], DfFatptrPrefixDelimiter);
        if (prefixPtr == NULL) {
            DataFormat::escapeNestedDelim(fieldsProducedConverted[ii],
                                          sizeof(fieldsProducedConverted[ii]),
                                          NULL);
        }
    }

    Field *field;

    // Let our parents know what we consume
    for (FieldSetHashTable::iterator it = fieldsConsumed->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        const char *fieldName = field->fieldName;

        if (!fieldInColumnsInput(fieldName,
                                 fieldNameBuf,
                                 sizeof(fieldNameBuf),
                                 currNode->dagNodeHdr.apiDagNodeHdr.api,
                                 currNode->dagNodeHdr.apiInput,
                                 parentIdx,
                                 &isFromOtherParent)) {
            // skip this field
            continue;
        }

        status = newField(fieldNameBuf, &fieldCopy, field->typesAccepted);

        if (status != StatusOk) {
            goto CommonExit;
        }

        insertField(parentsFieldsRequiredSet, fieldCopy);
        fieldCopy = NULL;
    }

    // Let our parents know what our children require
    for (FieldSetHashTable::iterator it =
             currFieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        // Except the field we produce. We don't need our parents to provide us
        // that
        bool fieldProduced = false;
        for (unsigned ii = 0; ii < numFieldsProduced; ii++) {
            if (strcmp(fieldsProducedConverted[ii], field->fieldName) == 0) {
                fieldProduced = true;
                break;
            }
        }

        if (fieldProduced) {
            continue;
        }

        const char *fieldName = field->fieldName;
        isFromOtherParent = false;

        if (!fieldInColumnsInput(fieldName,
                                 fieldNameBuf,
                                 sizeof(fieldNameBuf),
                                 currNode->dagNodeHdr.apiDagNodeHdr.api,
                                 currNode->dagNodeHdr.apiInput,
                                 parentIdx,
                                 &isFromOtherParent) &&
            !(currNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiJoin &&
              currNode->dagNodeHdr.apiInput->joinInput.keepAllColumns &&
              !isFromOtherParent)) {
            // skip this field
            continue;
        }

        status = newField(fieldNameBuf, &fieldCopy, XdfAcceptAll);
        if (status != StatusOk) {
            goto CommonExit;
        }

        insertField(parentsFieldsRequiredSet, fieldCopy);
        fieldCopy = NULL;
    }

    status = StatusOk;
CommonExit:
    if (fieldsProduced != NULL) {
        memFree(fieldsProduced);
        fieldsProduced = NULL;
    }

    if (fieldCopy != NULL) {
        assert(status != StatusOk);
        fieldCopy->del();
        fieldCopy = NULL;
    }

    return status;
}

Status
Optimizer::getFieldsConsumed(Dag *dag,
                             FieldsRequiredSet *fieldsRequiredSet,
                             char *evalStr)
{
    Status status = StatusUnknown;
    XcalarEvalAstCommon ast;
    bool astCreated = false;
    unsigned numVariables, ii;
    const char **fields = NULL;
    XdfTypesAccepted *fieldTypes = NULL;
    Field *field = NULL;

    assert(fieldsRequiredSet != NULL);
    assert(evalStr != NULL);

    status = XcalarEval::get()->parseEvalStr(evalStr,
                                             XcalarFnTypeAgg,
                                             dag->getUdfContainer(),
                                             &ast,
                                             NULL,
                                             &numVariables,
                                             NULL,
                                             NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    astCreated = true;

    fields = (const char **) memAlloc(sizeof(*fields) * numVariables);
    BailIfNull(fields);
    fieldTypes = (XdfTypesAccepted *) memAlloc(sizeof(*fields) * numVariables);
    BailIfNull(fieldTypes);

    unsigned numVariablesFound;
    status = XcalarEval::get()->getVariablesList(&ast,
                                                 fields,
                                                 fieldTypes,
                                                 numVariables,
                                                 &numVariablesFound);
    assert(status == StatusOk);
    assert(numVariablesFound == numVariables);

    for (ii = 0; ii < numVariables; ii++) {
        if (fields[ii][0] == OperatorsAggregateTag) {
            continue;
        }

        status = newField(fields[ii], &field, fieldTypes[ii]);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(field != NULL);

        const char *prefixPtr = strstr(fields[ii], DfFatptrPrefixDelimiter);
        if (prefixPtr == NULL) {
            // variable names will escape certain characters used for specifying
            // a nested field ex: "a.b" will be referenced as "a\.b"
            // unescape
            DataFormat::unescapeNestedDelim(field->fieldName);
        }

        insertField(fieldsRequiredSet, field);
        field = NULL;
    }

    status = StatusOk;
CommonExit:
    if (fields != NULL) {
        memFree(fields);
        fields = NULL;
    }
    if (fieldTypes != NULL) {
        memFree(fieldTypes);
        fieldTypes = NULL;
    }
    if (field != NULL) {
        assert(status != StatusOk);
        field->del();
        field = NULL;
    }

    if (astCreated) {
        XcalarEval::get()->freeCommonAst(&ast);
        astCreated = false;
    }

    return status;
}

Status
Optimizer::getFieldsConsumedSet(Dag *dag,
                                DagNodeTypes::Node *currNode,
                                FieldsRequiredSet *fieldsConsumed)
{
    Status status = StatusOk;
    DagNodeAnnotations *annotations;
    Field *field = NULL;

    assert(currNode != NULL);

    annotations = (DagNodeAnnotations *) currNode->annotations;
    assert(annotations != NULL);

    // Get fields consumed by currNode
    switch (currNode->dagNodeHdr.apiDagNodeHdr.api) {
    case XcalarApiIndex: {
        XcalarApiIndexInput *indexInput =
            &currNode->dagNodeHdr.apiInput->indexInput;

        for (unsigned ii = 0; ii < indexInput->numKeys; ii++) {
            XdfTypesAccepted typesAccepted =
                XcalarEval::convertToTypesAccepted(indexInput->keys[ii].type);

            if (currNode->dagNodeHdr.apiInput->indexInput.fatptrPrefixName[0] !=
                    '\0' &&
                !currNode->dagNodeHdr.apiInput->indexInput.source.isTable) {
                char newFieldName[DfMaxFieldNameLen * 2 +
                                  sizeof(DfFatptrPrefixDelimiter) + 1];
                int ret;
                ret = snprintf(newFieldName,
                               sizeof(newFieldName),
                               "%s%s%s",
                               indexInput->fatptrPrefixName,
                               DfFatptrPrefixDelimiter,
                               indexInput->keys[ii].keyName);
                if (ret >= (int) sizeof(newFieldName)) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Fieldname %s%s%s is too long (%d chars). "
                            "Max is %lu chars",
                            indexInput->fatptrPrefixName,
                            DfFatptrPrefixDelimiter,
                            indexInput->keys[ii].keyName,
                            ret,
                            sizeof(newFieldName));
                    status = StatusNoBufs;
                    goto CommonExit;
                }
                status = newField(newFieldName, &field, typesAccepted);
            } else {
                status = newField(indexInput->keys[ii].keyName,
                                  &field,
                                  typesAccepted);
            }
            if (status != StatusOk) {
                goto CommonExit;
            }

            assert(field != NULL);
            insertField(fieldsConsumed, field);
            field = NULL;
        }
        break;
    }
    case XcalarApiMap:
        for (unsigned ii = 0;
             ii < currNode->dagNodeHdr.apiInput->mapInput.numEvals;
             ii++) {
            status = getFieldsConsumed(dag,
                                       fieldsConsumed,
                                       currNode->dagNodeHdr.apiInput->mapInput
                                           .evalStrs[ii]);
            if (status != StatusOk) {
                goto CommonExit;
            }
        }
        break;
    case XcalarApiFilter:
        status = getFieldsConsumed(dag,
                                   fieldsConsumed,
                                   currNode->dagNodeHdr.apiInput->filterInput
                                       .filterStr);
        if (status != StatusOk) {
            goto CommonExit;
        }
        break;
    case XcalarApiGroupBy:
        for (unsigned ii = 0;
             ii < currNode->dagNodeHdr.apiInput->groupByInput.numEvals;
             ii++) {
            status = getFieldsConsumed(dag,
                                       fieldsConsumed,
                                       currNode->dagNodeHdr.apiInput
                                           ->groupByInput.evalStrs[ii]);
            if (status != StatusOk) {
                goto CommonExit;
            }
        }
        break;
    case XcalarApiAggregate:
        status = getFieldsConsumed(dag,
                                   fieldsConsumed,
                                   currNode->dagNodeHdr.apiInput->aggregateInput
                                       .evalStr);
        if (status != StatusOk) {
            goto CommonExit;
        }
        break;
    case XcalarApiExport: {
        int ii;
        XcalarApiExportInput *exportInput;

        exportInput = &currNode->dagNodeHdr.apiInput->exportInput;
        // XXX: Todo. Add support for *
        assert(exportInput->meta.numColumns > 0);
        for (ii = 0; ii < exportInput->meta.numColumns; ii++) {
            status = newField(exportInput->meta.columns[ii].name,
                              &field,
                              XdfAcceptAll);
            if (status != StatusOk) {
                goto CommonExit;
            }
            assert(field != NULL);
            insertField(fieldsConsumed, field);
            field = NULL;
        }
        break;
    }
    case XcalarApiSynthesize: {
        XcalarApiSynthesizeInput *synthesizeInput =
            &currNode->dagNodeHdr.apiInput->synthesizeInput;
        for (unsigned ii = 0; ii < synthesizeInput->columnsCount; ii++) {
            status = newField(synthesizeInput->columns[ii].oldName,
                              &field,
                              XdfAcceptAll);
            if (status != StatusOk) {
                goto CommonExit;
            }
            assert(field != NULL);
            insertField(fieldsConsumed, field);
            field = NULL;
        }
        break;
    }
    case XcalarApiJoin: {
        XcalarApiJoinInput *joinInput =
            &currNode->dagNodeHdr.apiInput->joinInput;
        if (joinInput->filterString[0] != '\0') {
            Field *curField = NULL;
            status =
                getFieldsConsumed(dag, fieldsConsumed, joinInput->filterString);
            if (status != StatusOk) {
                goto CommonExit;
            }

            // Need to add the filter fields to fieldsRequired HT, so Join
            // rewrite includes these in the renameMap.
            for (FieldSetHashTable::iterator it =
                     fieldsConsumed->hashTable->begin();
                 (curField = it.get()) != NULL;
                 it.next()) {
                status =
                    newField(curField->getFieldName(), &field, XdfAcceptAll);
                if (status != StatusOk) {
                    goto CommonExit;
                }
                assert(field != NULL);
                insertField(&annotations->fieldsRequiredSet, field);
                field = NULL;
            }
        }
        break;
    }

    // The following doesn't consume any fields
    case XcalarApiProject:
    case XcalarApiUnion:
    case XcalarApiBulkLoad:
    case XcalarApiSelect:
    case XcalarApiGetRowNum:
    case XcalarApiDeleteObjects:
        break;
    default:
        status = StatusUnimpl;
    }

CommonExit:
    if (field != NULL) {
        assert(status != StatusOk);
        field->del();
        field = NULL;
    }

    return status;
}

Status
Optimizer::insertNodeToList(Node *nodeList, DagNodeTypes::Node *dagNode)
{
    Node *node = NULL;
    node = new (std::nothrow) Node();
    if (node == NULL) {
        return StatusNoMem;
    }

    node->dagNode = dagNode;
    node->prev = nodeList->prev;
    node->next = nodeList;

    nodeList->prev->next = node;
    nodeList->prev = node;
    return StatusOk;
}

void
Optimizer::freeNodeList(Node *nodeList)
{
    Node *node;
    node = nodeList->next;
    while (node != nodeList) {
        // remove from head
        node->prev->next = node->next;
        node->next->prev = node->prev;

        delete node;

        node = nodeList->next;
    }
}

Status
Optimizer::fieldsLivenessAnalysis(Dag *dag,
                                  Node *loadNodesList,
                                  Node *projectNodesList,
                                  Node *joinNodesList,
                                  Node *synthesizeNodesList,
                                  Node *selectNodesList,
                                  Node *unionNodesList,
                                  FieldSetHashTable *userDefinedFields)
{
    uint64_t ii, searchStatesAlloced;
    DagNodeTypes::Node **leafNodes = NULL;
    uint64_t numLeafNodes, numNodes = 0ULL;
    Status status = StatusUnknown;
    SearchState searchQueueAnchor;
    SearchState *searchStateBase = NULL;
    Field *userDefinedField = NULL;
    const char **fieldsProduced = NULL;
    FieldsRequiredSet fieldsConsumed;

    fieldsConsumed.hashTable = new (std::nothrow) FieldSetHashTable();
    BailIfNull(fieldsConsumed.hashTable);

    fieldsConsumed.numFieldsRequired = 0;

    assert(dag != NULL);
    assert(loadNodesList != NULL);
    assert(projectNodesList != NULL);

    // @SymbolCheckIgnore
    searchQueueAnchor.next = searchQueueAnchor.prev = &searchQueueAnchor;

    numNodes = dag->getNumNode();
    if (numNodes == 0) {
        // Empty Dag...nothing to do.
        status = StatusDgDagEmpty;
        goto CommonExit;
    }

    searchStateBase =
        (SearchState *) memAlloc(sizeof(*searchStateBase) * numNodes);
    if (searchStateBase == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    searchStatesAlloced = 0;

    numLeafNodes = 0;
    status = initDagNodeAnnotations(dag, &leafNodes, &numLeafNodes);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(leafNodes != NULL);

    for (ii = 0; ii < numLeafNodes; ii++) {
        SearchState *searchState;
        searchState = &searchStateBase[searchStatesAlloced++];
        searchState->dagNode = leafNodes[ii];
        // append
        searchState->prev = searchQueueAnchor.prev;
        searchState->next = &searchQueueAnchor;
        searchQueueAnchor.prev->next = searchState;
        searchQueueAnchor.prev = searchState;
    }

    // Have to do BFS so that when we get to a parent,
    // we're sure we're done with all its chlidren
    SearchState *currState;
    currState = searchQueueAnchor.next;
    while (currState != &searchQueueAnchor) {
        // remove head
        currState->prev->next = currState->next;
        currState->next->prev = currState->prev;

        DagNodeTypes::NodeIdListElt *parentElt;
        DagNodeTypes::Node *currNode;
        DagNodeAnnotations *currAnnotations;
        unsigned numFieldsProduced = 0;

        currNode = currState->dagNode;
        currAnnotations = (DagNodeAnnotations *) currNode->annotations;

        switch (currNode->dagNodeHdr.apiDagNodeHdr.api) {
        case XcalarApiBulkLoad:
            status = insertNodeToList(loadNodesList, currNode);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        case XcalarApiProject:
            status = insertNodeToList(projectNodesList, currNode);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        case XcalarApiJoin:
            status = insertNodeToList(joinNodesList, currNode);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        case XcalarApiUnion:
            status = insertNodeToList(unionNodesList, currNode);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        case XcalarApiSynthesize:
            status = insertNodeToList(synthesizeNodesList, currNode);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        case XcalarApiSelect:
            status = insertNodeToList(selectNodesList, currNode);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;
        default:
            break;
        }

        // The invariant of this algorithm is that we only process
        // leaf nodes. After processing, we remove this node
        // from consideration and go on to process the new leaf nodes
        // In this fashion, when we inform our parents the set of fields
        // we and our descendants need, we can be sure that we don't
        // have to change our minds because of some laggard descendants
        assert(currAnnotations->numChildrenLeftToProcess == 0);

        status = getFieldsConsumedSet(dag, currNode, &fieldsConsumed);
        BailIfFailed(status);

        // returns malloced memory, needs to be freed
        status =
            getFieldsProduced(currNode, &fieldsProduced, &numFieldsProduced);
        BailIfFailed(status);

        for (ii = 0; ii < numFieldsProduced; ii++) {
            // a field that we produce without consuming is a userDefinedField
            if (fieldsConsumed.hashTable->find(fieldsProduced[ii]) == NULL) {
                status = newField(fieldsProduced[ii],
                                  &userDefinedField,
                                  XdfAcceptAll);
                if (status != StatusOk) {
                    goto CommonExit;
                }

                if (userDefinedFields->find(userDefinedField->fieldName) !=
                    NULL) {
                    // XXX Is this an error?
                    userDefinedField->del();
                    userDefinedField = NULL;
                } else {
                    userDefinedFields->insert(userDefinedField);
                    userDefinedField = NULL;
                }
            }
        }

        parentElt = currNode->parentsList;
        for (ii = 0; ii < currNode->numParent; ii++) {
            DagNodeTypes::Node *parentNode;
            DagNodeAnnotations *parentAnnotations;

            assert(parentElt != NULL);
            assert(parentElt->nodeId != DagTypes::InvalidDagNodeId);
            status = dag->lookupNodeById(parentElt->nodeId, &parentNode);
            assert(status == StatusOk);
            assert(parentNode != NULL);
            parentAnnotations = (DagNodeAnnotations *) parentNode->annotations;

            // Let our parent know what fields we require; except Aggregate
            // since it doesn't give us any fields
            if (parentNode->dagNodeHdr.apiDagNodeHdr.api !=
                XcalarApiAggregate) {
                status = updateParentsFieldsRequiredSet(parentNode,
                                                        ii,
                                                        currNode,
                                                        &fieldsConsumed);
                if (status != StatusOk) {
                    goto CommonExit;
                }
            }

            assert(parentAnnotations->numChildrenLeftToProcess >= 1);
            parentAnnotations->numChildrenLeftToProcess--;
            if (parentAnnotations->numChildrenLeftToProcess == 0) {
                // All my parent's children have informed her of their
                // requirements. No more surprises

                SearchState *searchState;
                searchState = &searchStateBase[searchStatesAlloced++];
                searchState->dagNode = parentNode;

                searchState->prev = searchQueueAnchor.prev;
                searchState->next = &searchQueueAnchor;
                searchQueueAnchor.prev->next = searchState;
                searchQueueAnchor.prev = searchState;
            }

            parentElt = parentElt->next;
        }

        currState = searchQueueAnchor.next;

        if (fieldsProduced != NULL) {
            memFree(fieldsProduced);
            fieldsProduced = NULL;
        }

        fieldsConsumed.hashTable->removeAll(&Field::del);
        fieldsConsumed.numFieldsRequired = 0;
    }

    assert(searchStatesAlloced == numNodes);
    status = StatusOk;
CommonExit:
    if (fieldsProduced != NULL) {
        memFree(fieldsProduced);
        fieldsProduced = NULL;
    }

    if (fieldsConsumed.hashTable != NULL) {
        delete fieldsConsumed.hashTable;
        fieldsConsumed.hashTable = NULL;
    }

    if (userDefinedField != NULL) {
        assert(status != StatusOk);
        userDefinedField->del();
        userDefinedField = NULL;
    }

    if (leafNodes != NULL) {
        memFree(leafNodes);
        leafNodes = NULL;
    }

    if (searchStateBase != NULL) {
        memFree(searchStateBase);
        searchStateBase = NULL;
    }

    return status;
}

Status
Optimizer::updateLoadNode(Dag *dag,
                          DagNodeTypes::Node *dagNode,
                          QueryHints *queryHints,
                          FieldSetHashTable *userDefinedFields)
{
    Status status = StatusUnknown;
    XcalarApiBulkLoadInput *bulkLoadInput;
    size_t ret;
    DagNodeAnnotations *annotations;
    FieldsRequiredSet *fieldsRequiredSet;
    TupleValueDesc *valueDesc = NULL;
    char(*fieldNames)[DfMaxFieldNameLen + 1] = NULL;
    unsigned ii = 0, jj = 0;
    json_t *parserJson = NULL;
    DfLoadArgs *loadArgs;
    bool dataIsCsv = false;
    uint64_t numColumnHints;
    Column *columnHints;
    char csvParserName[UdfVersionedFQFname];

    // Prepare the name of the default csv parser so we can compare against it
    // later.
    snprintf(csvParserName,
             sizeof(csvParserName),
             "%s:%s",
             UserDefinedFunction::DefaultModuleName,
             UserDefinedFunction::CsvParserName);

    if (queryHints != NULL) {
        numColumnHints = queryHints->retina->numColumnHints;
        columnHints = queryHints->retina->columnHints;
    } else {
        numColumnHints = 0;
        columnHints = NULL;
    }

    assert(dagNode != NULL);

    valueDesc = (TupleValueDesc *) memAlloc(sizeof(*valueDesc));
    if (valueDesc == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // We don't have to change the name once we have more granular dataset
    // namespace. The datasets created during LRQ should not even be
    // externally visible
    char newDatasetName[sizeof(bulkLoadInput->datasetName)];
    assertStatic(sizeof(newDatasetName) > sizeof(char *));
    bulkLoadInput = &dagNode->dagNodeHdr.apiInput->loadInput;
    ret = snprintf(newDatasetName,
                   sizeof(newDatasetName),
                   XcalarApiLrqPrefix "%lu%s",
                   dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId,
                   bulkLoadInput->datasetName);
    if (ret >= sizeof(newDatasetName)) {
        status = StatusOverflow;
        goto CommonExit;
    }
    assertStatic(sizeof(newDatasetName) == sizeof(bulkLoadInput->datasetName));

    status =
        dag->renameDagNodeLocalEx(bulkLoadInput->datasetName, newDatasetName);
    BailIfFailed(status);

    // XXX: if the cluster is not operationally licensed then the load must
    // adhere to MaxInteractiveDataSize.  We can't disallow running BDFs as
    // they could be used for modeling (e.g. cascading BDFs) but we will limit
    // the amount of data that can be loaded.
    bulkLoadInput->loadArgs.maxSize = INT64_MAX;
    strlcpy(bulkLoadInput->datasetName,
            newDatasetName,
            sizeof(bulkLoadInput->datasetName));

    // Populate load with fields of interest args
    annotations = (DagNodeAnnotations *) dagNode->annotations;
    fieldsRequiredSet = &annotations->fieldsRequiredSet;
    if (fieldsRequiredSet->numFieldsRequired > TupleMaxNumValuesPerRecord) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Number of columns (%u) exceeeds"
                      " maximum allowed (%u) when loading the dataset (%s)",
                      fieldsRequiredSet->numFieldsRequired,
                      TupleMaxNumValuesPerRecord,
                      bulkLoadInput->datasetName);
        status = StatusFieldLimitExceeded;
        goto CommonExit;
    }

    fieldNames = (char(*)[DfMaxFieldNameLen + 1])
        memAlloc(sizeof(*fieldNames) * fieldsRequiredSet->numFieldsRequired);
    if (fieldNames == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    loadArgs = (DfLoadArgs *) &bulkLoadInput->loadArgs;
    dataIsCsv = (strcmp(loadArgs->parseArgs.parserFnName, csvParserName) == 0);

    ii = 0;
    Field *field;
    for (FieldSetHashTable::iterator it = fieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        bool foundInLoadInput = false;
        bool foundInSchemaHints = false;
        char *prefixPtr, *fieldName;
        assertStatic(sizeof(fieldNames[ii]) == sizeof(field->fieldName));

        // prefixes can either be delimited by the normal delimiter or
        // the replaced delimiter in the case of nested dataflows
        prefixPtr = strstr(field->fieldName, DfFatptrPrefixDelimiter);
        if (prefixPtr == NULL) {
            prefixPtr =
                strstr(field->fieldName, DfFatptrPrefixDelimiterReplaced);
        }

        if (prefixPtr != NULL) {
            fieldName = prefixPtr + strlen(DfFatptrPrefixDelimiter);
        } else {
            fieldName = field->fieldName;
        }

        if (strstr(fieldName, DsDefaultDatasetKeyName)) {
            continue;
        }

        strlcpy(&fieldNames[ii][0], field->fieldName, sizeof(fieldNames[ii]));
        DataFormat::replaceFatptrPrefixDelims(fieldNames[ii]);

        for (jj = 0; jj < ii; jj++) {
            if (strcmp(fieldNames[jj], fieldNames[ii]) == 0) {
                // we've already added this field to the dataset
                break;
            }
        }

        if (jj != ii) {
            continue;
        }

        for (jj = 0; jj < loadArgs->parseArgs.fieldNamesCount; jj++) {
            if (strcmp(fieldName, loadArgs->parseArgs.fieldNames[jj]) == 0) {
                valueDesc->valueType[ii] = loadArgs->parseArgs.types[jj];
                foundInLoadInput = true;
                break;
            }
        }

        if (!foundInLoadInput) {
            // Use queryHints if provided to figure out the type
            // Otherwise, default to DfScalarObj
            for (jj = 0; jj < numColumnHints; jj++) {
                // prefixed comparison
                if (strcmp(field->fieldName, columnHints[jj].columnName) == 0) {
                    valueDesc->valueType[ii] = columnHints[jj].type;
                    foundInSchemaHints = true;
                    break;
                }
            }
        }

        if (!foundInLoadInput && !foundInSchemaHints) {
            if (dataIsCsv) {
                valueDesc->valueType[ii] = DfString;
            } else {
                valueDesc->valueType[ii] =
                    XcalarEval::getBestFieldType(field->typesAccepted);
            }
        }
        ii++;
    }

#ifdef DEBUG
    // make sure there are no duplicates
    for (jj = 0; jj < ii; jj++) {
        for (unsigned kk = jj + 1; kk < ii; kk++) {
            assert(strcmp(fieldNames[jj], fieldNames[kk]) != 0);
        }
    }
#endif  // DEBUG

    valueDesc->numValuesPerTuple = ii;

    Dataset::initXdbLoadArgs(loadArgs, valueDesc, ii, fieldNames);
    memFree(valueDesc);
    valueDesc = NULL;  // Ref given to loadArgs
    status = StatusOk;
CommonExit:
    if (parserJson != NULL) {
        json_decref(parserJson);
        parserJson = NULL;
    }

    if (fieldNames != NULL) {
        memFree(fieldNames);
        fieldNames = NULL;
    }

    if (status != StatusOk) {
        if (valueDesc != NULL) {
            memFree(valueDesc);
            valueDesc = NULL;
        }
    }
    assert(valueDesc == NULL);

    return status;
}

Status
Optimizer::updateSelectNode(Dag *dag,
                            Dag *srcDag,
                            DagNodeTypes::Node *dagNode,
                            QueryHints *queryHints,
                            FieldSetHashTable *userDefinedFields)
{
    Status status = StatusOk;
    unsigned newNumCols = 0;
    XcalarApiRenameMap *newCols = NULL;

    XcalarApiSelectInput *selectInput =
        &dagNode->dagNodeHdr.apiInput->selectInput;

    // reset columns based on fields of interest info stored in dagNode
    status = updateRenameMap(dagNode,
                             selectInput->numColumns,
                             selectInput->columns,
                             &newNumCols,
                             &newCols);
    BailIfFailed(status);

    selectInput->numColumns = newNumCols;
    memcpy(selectInput->columns, newCols, sizeof(*newCols) * newNumCols);

    // XXX We cannot pull in the filter into Select nodes because Select
    // operator has implicit dependency in the order in which it will evaluate
    // it's filter, map, groupBy and Join. This just makes it not amenable to
    // doing optimizations here. Plan is to remove Select Operator anyway and so
    // we won't exploit any optimization opportunities with Select going
    // forward.

CommonExit:
    if (newCols) {
        memFree(newCols);
        newCols = NULL;
    }

    return status;
}

Status
Optimizer::updateSourceNode(Dag *dag,
                            DagNodeTypes::Node *dagNode,
                            QueryHints *queryHints,
                            FieldSetHashTable *userDefinedFields)
{
    Status status = StatusOk;
    DagNodeAnnotations *annotations;
    FieldsRequiredSet *fieldsRequiredSet;
    unsigned jj = 0;
    uint64_t numColumnHints;
    Column *columnHints;
    Dag *sessionDag = queryHints->retina->sessionDag;
    XcalarApiSynthesizeInput *synthesizeInput =
        &dagNode->dagNodeHdr.apiInput->synthesizeInput;
    XcalarApiSynthesizeInput *newInput = NULL;
    DagNodeTypes::Node *srcNode;
    size_t inputSize;
    TableMgr::FqTableState fqTabState;
    bool refAcquired = false;

    if (dagNode->numParent) {
        // Nothing to fix up, if synthesize refers to source that belongs
        // to the same session.
        synthesizeInput->sameSession = true;
        goto CommonExit;
    }
    synthesizeInput->sameSession = false;

    status = fqTabState.setUp(synthesizeInput->source.name, sessionDag);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get source node %s: %s",
                      synthesizeInput->source.name,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    status = fqTabState.graph->getDagNodeRefById(fqTabState.nodeId);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get source node %s: %s",
                      synthesizeInput->source.name,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    refAcquired = true;

    status =
        fqTabState.graph->lookupNodeByName(synthesizeInput->source.name,
                                           &srcNode,
                                           Dag::TableScope::FullyQualOrLocal,
                                           true);  // sessionDag needs locking
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get source node %s: %s",
                      synthesizeInput->source.name,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    if (srcNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiAggregate) {
        if (srcNode->scalarResult == NULL) {
            status = StatusAggregateResultNotFound;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to get aggregate from source node %s: %s",
                          synthesizeInput->source.name,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        synthesizeInput->aggResult = srcNode->scalarResult;

        // nothing more to do for aggregates
        goto CommonExit;
    }

    synthesizeInput->source.isTable =
        (srcNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiBulkLoad);

    if (synthesizeInput->source.isTable) {
        synthesizeInput->source.xid = srcNode->xdbId;
    } else {
        synthesizeInput->source.xid =
            Dataset::get()->getDatasetIdFromName(synthesizeInput->source.name,
                                                 &status);
        BailIfFailed(status);
    }

    if (queryHints != NULL) {
        numColumnHints = queryHints->retina->numColumnHints;
        columnHints = queryHints->retina->columnHints;
    } else {
        numColumnHints = 0;
        columnHints = NULL;
    }

    assert(dagNode != NULL);

    annotations = (DagNodeAnnotations *) dagNode->annotations;
    fieldsRequiredSet = &annotations->fieldsRequiredSet;

    inputSize =
        sizeof(*synthesizeInput) + sizeof(*synthesizeInput->columns) *
                                       fieldsRequiredSet->numFieldsRequired;

    newInput = (XcalarApiSynthesizeInput *) memAlloc(inputSize);
    BailIfNull(newInput);

    memZero(newInput, inputSize);

    newInput->source = synthesizeInput->source;
    newInput->dstTable = synthesizeInput->dstTable;
    newInput->aggResult = NULL;
    newInput->sameSession = synthesizeInput->sameSession;
    newInput->columnsCount = 0;

    // For Synthesize nodes where source tables don't have Fully Qualified
    // names, overwrite the session scoped Table names to Full Qualified
    // names.
    if (newInput->source.isTable &&
        !TableNsMgr::get()->isValidFullyQualTableName(newInput->source.name)) {
        status = TableNsMgr::get()->getFQN(newInput->source.name,
                                           sizeof(newInput->source.name),
                                           &fqTabState.sessionContainer,
                                           synthesizeInput->source.name);
        BailIfFailed(status);
    }

    Field *field;
    for (FieldSetHashTable::iterator it = fieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        char *prefixPtr, *fieldName;

        prefixPtr = strstr(field->fieldName, DfFatptrPrefixDelimiter);
        if (prefixPtr == NULL) {
            fieldName = field->fieldName;
        } else {
            fieldName = prefixPtr + strlen(DfFatptrPrefixDelimiter);
        }

        // fields coming from dataset should be prefixed
        if (prefixPtr == NULL && !newInput->source.isTable) {
            continue;
        }

        // populate src col name
        strlcpy(newInput->columns[newInput->columnsCount].oldName,
                field->fieldName,
                sizeof(field->fieldName));

        if (!newInput->source.isTable) {
            // For datasets, replace the prefixes
            DataFormat::replaceFatptrPrefixDelims(
                newInput->columns[newInput->columnsCount].oldName);

            strlcpy(newInput->columns[newInput->columnsCount].newName,
                    newInput->columns[newInput->columnsCount].oldName,
                    sizeof(newInput->columns[newInput->columnsCount].oldName));
        } else {
            strlcpy(newInput->columns[newInput->columnsCount].newName,
                    newInput->columns[newInput->columnsCount].oldName,
                    sizeof(newInput->columns[newInput->columnsCount].oldName));

            DataFormat::replaceFatptrPrefixDelims(
                newInput->columns[newInput->columnsCount].newName);
            if (prefixPtr == NULL) {
                // do a layer of unescaping for immediates
                DataFormat::unescapeNestedDelim(
                    newInput->columns[newInput->columnsCount].newName);
            }
        }

        // make sure there are no duplicates
        int kk;
        for (kk = 0; kk < (int) newInput->columnsCount; kk++) {
            if (strcmp(newInput->columns[kk].newName,
                       newInput->columns[newInput->columnsCount].newName) ==
                0) {
                break;
            }
        }

        if (kk != (int) newInput->columnsCount) {
            // found a duplicate, continue
            continue;
        }

        // Use queryHints if provided to figure out the type
        // Otherwise, default to DfScalarObj
        for (jj = 0; jj < numColumnHints; jj++) {
            // prefixed comparison
            if (strcmp(field->fieldName, columnHints[jj].columnName) == 0) {
                newInput->columns[newInput->columnsCount].type =
                    columnHints[jj].type;
                break;
            }
        }

        if (jj == numColumnHints) {
            if (strstr(fieldName, DsDefaultDatasetKeyName)) {
                newInput->columns[newInput->columnsCount].type = DfInt64;
            } else {
                newInput->columns[newInput->columnsCount].type =
                    XcalarEval::getBestFieldType(field->typesAccepted);
            }
        }
        newInput->columnsCount++;
    }

#ifdef DEBUG
    // make sure there are no duplicates
    for (jj = 0; jj < newInput->columnsCount; jj++) {
        for (unsigned kk = jj + 1; kk < newInput->columnsCount; kk++) {
            assert(strcmp(newInput->columns[jj].newName,
                          newInput->columns[kk].newName) != 0);
        }
    }
#endif  // DEBUG

    if (!newInput->source.isTable) {
        // we will be synthesizing this dataset into a table, strip away
        // the dataset prefix
        char *newDatasetName =
            newInput->source.name + XcalarApiDatasetPrefixLen;
        status =
            dag->renameDagNodeLocalEx(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                      newDatasetName);
        BailIfFailed(status);

        // If we synthesized a dataset, need to update the child indexes
        DagNodeTypes::NodeIdListElt *childElt;
        DagNodeTypes::Node *childNode;
        childElt = dagNode->childrenList;
        while (childElt != NULL) {
            status = dag->lookupNodeById(childElt->nodeId, &childNode);
            assert(status == StatusOk);  // Of course the node exists
            if (childNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiIndex) {
                status = updateIndexNode(childNode, newDatasetName);
                if (status != StatusOk) {
                    goto CommonExit;
                }
            }
            childElt = childElt->next;
        }
    }

    // check if we can pull in a filter
    if (dagNode->numChild == 1) {
        DagNodeTypes::Node *filterNode;
        if (getFilterStringFromChild(dag,
                                     dagNode,
                                     true,
                                     false,
                                     false,
                                     &filterNode,
                                     newInput->filterString)) {
            dag->lock();
            status = dag->deleteQueryOperationNode(filterNode);
            dag->unlock();
            BailIfFailed(status);
        }
    }

    // recalculate inputSize, we might not have taken all the columms
    inputSize = sizeof(*synthesizeInput) +
                sizeof(*synthesizeInput->columns) * newInput->columnsCount;

    // replace the old node with a synthesize node
    Dag::dagApiFree(dagNode);
    dagNode->dagNodeHdr.apiInput = Dag::dagApiAlloc(inputSize);
    BailIfNull(dagNode->dagNodeHdr.apiInput);
    dagNode->dagNodeHdr.apiDagNodeHdr.inputSize = inputSize;
    status =
        Dag::sparseMemCpy(dagNode->dagNodeHdr.apiInput, newInput, inputSize);
    BailIfFailed(status);

CommonExit:
    if (refAcquired) {
        fqTabState.graph->putDagNodeRefById(fqTabState.nodeId);
        refAcquired = false;
    }

    if (newInput != NULL) {
        memFree(newInput);
        newInput = NULL;
    }

    return status;
}

Status
Optimizer::updateIndexNode(DagNodeTypes::Node *indexNode,
                           const char *newDatasetName)
{
    XcalarApiIndexInput *indexInput;
    char newFieldName[sizeof(indexInput->keys[0].keyName)];
    assert(indexNode != NULL);

    indexInput = &indexNode->dagNodeHdr.apiInput->indexInput;

    indexInput->source.isTable = true;
    strlcpy(indexInput->source.name,
            newDatasetName,
            sizeof(indexInput->source.name));

    // if we are changing the index source to be a table, if it was a dataset
    // previously, we need to prepend the prefixName to all the keys
    if (indexInput->fatptrPrefixName[0] != '\0') {
        for (unsigned ii = 0; ii < indexInput->numKeys; ii++) {
            int ret;
            if (strstr(indexInput->keys[ii].keyName, DsDefaultDatasetKeyName)) {
                ret = snprintf(newFieldName,
                               sizeof(newFieldName),
                               "%s",
                               indexInput->keys[ii].keyName);
            } else {
                ret = snprintf(newFieldName,
                               sizeof(newFieldName),
                               "%s%s%s",
                               indexInput->fatptrPrefixName,
                               DfFatptrPrefixDelimiter,
                               indexInput->keys[ii].keyName);
                if (ret >= (int) sizeof(newFieldName)) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Fieldname %s%s%s is too long (%d chars). "
                            "Max is %lu chars",
                            indexInput->fatptrPrefixName,
                            DfFatptrPrefixDelimiter,
                            indexInput->keys[ii].keyName,
                            ret,
                            sizeof(newFieldName));
                    return StatusNoBufs;
                }
            }

            ret = strlcpy(indexInput->keys[ii].keyName,
                          newFieldName,
                          sizeof(indexInput->keys[ii].keyName));
        }
    }

    return StatusOk;
}

bool
Optimizer::getFilterStringFromChild(Dag *dag,
                                    DagNodeTypes::Node *dagNode,
                                    bool convertNames,
                                    bool aggVarsAllowed,
                                    bool udfsAllowed,
                                    DagNodeTypes::Node **filterNodeOut,
                                    char *evalStringOut)
{
    Status status;
    DagNodeTypes::Node *childNode;
    DagNodeTypes::NodeIdListElt *childElt;
    char *filterString;

    childElt = dagNode->childrenList;
    while (childElt != NULL) {
        status = dag->lookupNodeById(childElt->nodeId, &childNode);
        assert(status == StatusOk);  // Of course the node exists
        if (childNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiFilter) {
            status =
                DagLib::convertNamesToImmediate(dag,
                                                XcalarApiFilter,
                                                childNode->dagNodeHdr.apiInput);
            if (status != StatusOk) {
                return false;
            }

            filterString =
                childNode->dagNodeHdr.apiInput->filterInput.filterStr;

            if (!aggVarsAllowed &&
                XcalarEval::get()->containsAggregateVar(filterString)) {
                return false;
            }

            if (!udfsAllowed && XcalarEval::get()->containsUdf(filterString)) {
                return false;
            }

            strlcpy(evalStringOut,
                    filterString,
                    sizeof(
                        childNode->dagNodeHdr.apiInput->filterInput.filterStr));

            *filterNodeOut = childNode;
            return true;
        }

        childElt = childElt->next;
    }

    *filterNodeOut = NULL;
    return false;
}

Status
Optimizer::rewriteLoadNodes(Dag *dag,
                            Node *loadNodesList,
                            QueryHints *queryHints,
                            FieldSetHashTable *userDefinedFields)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode, *childNode;
    DagNodeTypes::NodeIdListElt *childElt;
    Node *loadNode = loadNodesList->next;

    while (loadNode != loadNodesList) {
        dagNode = loadNode->dagNode;

        if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiBulkLoad) {
            // this load has been optimized away
            loadNode = loadNode->next;
            continue;
        }

        status = updateLoadNode(dag, dagNode, queryHints, userDefinedFields);
        if (status != StatusOk) {
            goto CommonExit;
        }

        DfLoadArgs *loadArgs =
            &dagNode->dagNodeHdr.apiInput->loadInput.loadArgs;

        Dataset::updateXdbLoadArgKey(loadArgs,
                                     DsDefaultDatasetKeyName,
                                     NewTupleMeta::DfInvalidIdx,
                                     DfInt64);

        childElt = dagNode->childrenList;
        while (childElt != NULL) {
            status = dag->lookupNodeById(childElt->nodeId, &childNode);
            assert(status == StatusOk);  // Of course the node exists
            if (childNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiIndex) {
                status =
                    updateIndexNode(childNode,
                                    dagNode->dagNodeHdr.apiDagNodeHdr.name);
                if (status != StatusOk) {
                    goto CommonExit;
                }

                DagLib::renameSrcApiInput(NULL,
                                          dagNode->dagNodeHdr.apiDagNodeHdr
                                              .name,
                                          childNode);
            }

            childElt = childElt->next;
        }

        loadNode = loadNode->next;
    }

CommonExit:
    return status;
}

Status
Optimizer::rewriteSourceNodes(Dag *dag,
                              Node *synthesizeNodesList,
                              QueryHints *queryHints,
                              FieldSetHashTable *userDefinedFields)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    Node *node = synthesizeNodesList->next;

    while (node != synthesizeNodesList) {
        dagNode = node->dagNode;

        status = updateSourceNode(dag, dagNode, queryHints, userDefinedFields);
        BailIfFailed(status);

        node = node->next;
    }

CommonExit:
    return status;
}

Status
Optimizer::rewriteSelectNodes(Dag *dag,
                              Node *selectNodesList,
                              QueryHints *queryHints,
                              FieldSetHashTable *userDefinedFields)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    Node *node = selectNodesList->next;

    while (node != selectNodesList) {
        Dag *srcGraph = queryHints->retina->sessionDag;

        dagNode = node->dagNode;

        status = updateSelectNode(dag,
                                  srcGraph,
                                  dagNode,
                                  queryHints,
                                  userDefinedFields);
        BailIfFailed(status);

        node = node->next;
    }

CommonExit:
    return status;
}

// rewrite the projected columns with the columns from the fieldRequiredSet
Status
Optimizer::updateProjectNode(DagNodeTypes::Node *dagNode)
{
    Status status = StatusOk;

    XcalarApiProjectInput *input;
    input = &dagNode->dagNodeHdr.apiInput->projectInput;
    XcalarApiProjectInput *newInput = NULL;

    XcalarApiInput *newApiInput = NULL;
    DagNodeAnnotations *annotations =
        (DagNodeAnnotations *) dagNode->annotations;
    FieldsRequiredSet *fieldsRequiredSet = &annotations->fieldsRequiredSet;
    bool *fieldsRequiredAdded = NULL;

    unsigned ii, jj, kk;
    unsigned numColumnsRequired = 0;
    Field *field;
    size_t newInputSize = 0;
    for (jj = 0; jj < input->numColumns; jj++) {
        // keep all immediates specified by project
        char *prefixPtr =
            strstr(input->columnNames[jj], DfFatptrPrefixDelimiter);
        Field *field;
        if (prefixPtr == NULL) {
            status = newField(input->columnNames[jj], &field, XdfAcceptAll);
            if (status != StatusOk) {
                goto CommonExit;
            }
            insertField(fieldsRequiredSet, field);
        }
    }

    fieldsRequiredAdded =
        (bool *) memAlloc(fieldsRequiredSet->numFieldsRequired * sizeof(bool));
    BailIfNull(fieldsRequiredAdded);
    memZero(fieldsRequiredAdded,
            fieldsRequiredSet->numFieldsRequired * sizeof(bool));

    for (jj = 0; jj < input->numColumns; jj++) {
        char fatptrPrefix[DfMaxFieldNameLen + 1 +
                          sizeof(DfFatptrPrefixDelimiter)];
        char *prefixPtr = NULL;
        size_t prefixLen = 0;

        // Note that we don't use the DelimiterReplace
        // here, but Delimiter, because we don't
        // replace Delimiter with DelimiterReplaced in
        // convertNamesToImmediate, for otherwise
        // we can't distinguish immediates containing the
        // delimiterReplaced chars, VS fatptr colName with the
        // fatptrDelimiter replaced
        prefixPtr = strstr(input->columnNames[jj], DfFatptrPrefixDelimiter);
        if (prefixPtr != NULL) {
            prefixLen =
                (uintptr_t) prefixPtr - (uintptr_t) input->columnNames[jj];
            assert(prefixLen <
                   sizeof(fatptrPrefix) - sizeof(DfFatptrPrefixDelimiter));

            strlcpy(fatptrPrefix, input->columnNames[jj], prefixLen + 1);
            fatptrPrefix[prefixLen] = DfFatptrPrefixDelimiter[0];
            fatptrPrefix[prefixLen + 1] = DfFatptrPrefixDelimiter[1];
            fatptrPrefix[prefixLen + 2] = '\0';
        }

        kk = 0;
        for (FieldSetHashTable::iterator it =
                 fieldsRequiredSet->hashTable->begin();
             (field = it.get()) != NULL;
             it.next()) {
            if (fieldsRequiredAdded[kk]) {
                kk++;
                continue;
            }

            if (prefixPtr != NULL) {
                if (strncmp(field->fieldName, fatptrPrefix, prefixLen + 2) ==
                    0) {
                    fieldsRequiredAdded[kk] = true;
                    numColumnsRequired++;
                    // There could be more than 1 field with the same
                    // fatptrPrefix, so don't break
                }
            } else if (strcmp(field->fieldName, input->columnNames[jj]) == 0) {
                fieldsRequiredAdded[kk] = true;
                numColumnsRequired++;
                break;
            }

            kk++;
        }
    }

    newInputSize =
        sizeof(*input) + (sizeof(input->columnNames[0]) * numColumnsRequired);
    newApiInput = (XcalarApiInput *) memAllocExt(newInputSize, moduleName);
    BailIfNull(newApiInput);
    newInput = &newApiInput->projectInput;
    newInput->srcTable = input->srcTable;
    newInput->dstTable = input->dstTable;
    newInput->numColumns = numColumnsRequired;

    ii = 0;
    jj = 0;
    for (FieldSetHashTable::iterator it = fieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        if (fieldsRequiredAdded[jj]) {
            strlcpy(newInput->columnNames[ii],
                    field->fieldName,
                    sizeof(newInput->columnNames[ii]));
            ii++;
        }
        jj++;
    }
    assert(ii == numColumnsRequired);

    Dag::dagApiFree(dagNode);
    dagNode->dagNodeHdr.apiInput = Dag::dagApiAlloc(newInputSize);
    dagNode->dagNodeHdr.apiDagNodeHdr.inputSize = newInputSize;
    status =
        Dag::sparseMemCpy(dagNode->dagNodeHdr.apiInput, newInput, newInputSize);
    BailIfFailed(status);

CommonExit:
    if (fieldsRequiredAdded != NULL) {
        memFree(fieldsRequiredAdded);
        fieldsRequiredAdded = NULL;
    }
    if (newApiInput) {
        memFree(newApiInput);
        newApiInput = NULL;
    }

    return status;
}

Status
Optimizer::rewriteProjectNodes(Dag *dag, Node *projectNodesList)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    Node *projectNode = projectNodesList->next;

    while (projectNode != projectNodesList) {
        dagNode = projectNode->dagNode;
        if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiProject) {
            // this project has been optimized away
            projectNode = projectNode->next;
            continue;
        }

        dag->lock();
        status = dag->deleteQueryOperationNode(dagNode);
        dag->unlock();

        BailIfFailed(status);

        projectNode = projectNode->next;
    }

CommonExit:

    return status;
}

Status
Optimizer::updateJoinNode(Dag *dag, DagNodeTypes::Node *dagNode)
{
    Status status = StatusUnknown;
    XcalarApiJoinInput *joinInput;
    XcalarWorkItem *workItem = NULL;
    XcalarApiRenameMap *finalCols = NULL;
    unsigned numSrc;
    bool needKeepAllColumn = false;

    joinInput = &dagNode->dagNodeHdr.apiInput->joinInput;
    numSrc = 2;

    unsigned numCols[numSrc];
    XcalarApiRenameMap *cols[numSrc];
    memZero(cols, sizeof(cols));

    for (unsigned ii = 0; ii < numSrc; ii++) {
        unsigned srcNumCols;
        XcalarApiRenameMap *srcCols;
        if (ii == 0) {
            srcNumCols = joinInput->numLeftColumns;
            srcCols = joinInput->renameMap;
        } else {
            srcNumCols = joinInput->numRightColumns;
            srcCols = &joinInput->renameMap[joinInput->numLeftColumns];
        }

        status = updateRenameMap(dagNode,
                                 srcNumCols,
                                 srcCols,
                                 &numCols[ii],
                                 &cols[ii],
                                 ii,
                                 &needKeepAllColumn);
        BailIfFailed(status);

        if (needKeepAllColumn) {
            break;
        }
    }

    // Join wants the renameMaps concattenated together
    if (needKeepAllColumn) {
        // keepAllColumns = true, use original rename map
        finalCols = joinInput->renameMap;
        numCols[0] = joinInput->numLeftColumns;
        numCols[1] = joinInput->numRightColumns;
    } else {
        finalCols = (XcalarApiRenameMap *) memAlloc((numCols[0] + numCols[1]) *
                                                    sizeof(*finalCols));
        BailIfNull(finalCols);

        memcpy(finalCols, cols[0], numCols[0] * sizeof(*finalCols));
        memcpy(&finalCols[numCols[0]],
               cols[1],
               numCols[1] * sizeof(*finalCols));
    }

    workItem = xcalarApiMakeJoinWorkItem(joinInput->leftTable.tableName,
                                         joinInput->rightTable.tableName,
                                         joinInput->joinTable.tableName,
                                         joinInput->joinType,
                                         joinInput->collisionCheck,
                                         needKeepAllColumn,
                                         joinInput->nullSafe,
                                         numCols[0],
                                         numCols[1],
                                         finalCols,
                                         joinInput->filterString);
    BailIfNull(workItem);

    // replace old input
    Dag::dagApiFree(dagNode);
    joinInput = NULL;

    dagNode->dagNodeHdr.apiInput = Dag::dagApiAlloc(workItem->inputSize);
    dagNode->dagNodeHdr.apiDagNodeHdr.inputSize = workItem->inputSize;
    status = Dag::sparseMemCpy(dagNode->dagNodeHdr.apiInput,
                               workItem->input,
                               workItem->inputSize);
    BailIfFailed(status);

CommonExit:
    for (unsigned ii = 0; ii < numSrc; ii++) {
        if (cols[ii]) {
            memFree(cols[ii]);
            cols[ii] = NULL;
        }
    }

    if (finalCols && !needKeepAllColumn) {
        memFree(finalCols);
        finalCols = NULL;
    }

    if (workItem) {
        if (workItem->input) {
            memFree(workItem->input);
            workItem->input = NULL;
        }

        memFree(workItem);
        workItem = NULL;
    }

    return status;
}

Status
Optimizer::rewriteJoinNodes(Dag *dag, Node *joinNodesList)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    Node *joinNode = joinNodesList->next;

    while (joinNode != joinNodesList) {
        dagNode = joinNode->dagNode;
        if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiJoin) {
            // this join has been optimized away
            joinNode = joinNode->next;
            continue;
        }

        status = updateJoinNode(dag, dagNode);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to update join node: %s",
                    strGetFromStatus(status));
            return status;
        }
        joinNode = joinNode->next;
    }

    return status;
}

Status
Optimizer::updateRenameMap(DagNodeTypes::Node *dagNode,
                           unsigned numColsIn,
                           XcalarApiRenameMap *renameMapIn,
                           unsigned *numColsOut,
                           XcalarApiRenameMap **renameMapOut,
                           int numSrc,
                           bool *needKeepAllColumn)
{
    Status status = StatusOk;
    int ret;
    unsigned ii, numColsFromOtherParent, cur = 0;
    DagNodeAnnotations *annotation;
    FieldsRequiredSet *fields;
    Field *field;
    char fieldName[DfMaxFieldNameLen + 1];
    char prefix[DfMaxFieldNameLen + 1];
    XcalarApiRenameMap *cols, *renameMapFromOtherParent;

    annotation = (DagNodeAnnotations *) dagNode->annotations;
    fields = &annotation->fieldsRequiredSet;

    if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiJoin &&
        dagNode->dagNodeHdr.apiInput->joinInput.keepAllColumns) {
        if (numSrc == 0) {
            numColsFromOtherParent =
                dagNode->dagNodeHdr.apiInput->joinInput.numRightColumns;
            renameMapFromOtherParent =
                &dagNode->dagNodeHdr.apiInput->joinInput.renameMap[numColsIn];
        } else {
            numColsFromOtherParent =
                dagNode->dagNodeHdr.apiInput->joinInput.numLeftColumns;
            renameMapFromOtherParent =
                dagNode->dagNodeHdr.apiInput->joinInput.renameMap;
        }
    }

    // add all keys to our fields required set
    for (ii = 0; ii < numColsIn; ii++) {
        if (renameMapIn[ii].isKey) {
            Field *keyField;
            status = newField(renameMapIn[ii].newName, &keyField, XdfAcceptAll);
            BailIfFailed(status);

            insertField(fields, keyField);
            keyField = NULL;
        }
    }

    cols = (XcalarApiRenameMap *) memAlloc(fields->numFieldsRequired *
                                           sizeof(*cols));
    BailIfNull(cols);

    memZero(cols, fields->numFieldsRequired * sizeof(*cols));

    // iterate across the fields required from this node,
    // selecting the corresponding fields from the renameMap
    for (FieldSetHashTable::iterator it = fields->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        bool isOldColName = false, isFromOtherParent = false;
        status = strStrlcpy(fieldName, field->fieldName, sizeof(fieldName));
        BailIfFailed(status);

        // the fieldName in the fieldsRequiredSet is an accessor name,
        // while the name in our rename map is a raw name. Do the
        // conversion by unescaping nested delims in immediate names
        char *prefixPtr = strstr(fieldName, DfFatptrPrefixDelimiter);
        if (prefixPtr == NULL) {
            DataFormat::unescapeNestedDelim(fieldName);
        }

        // try to find this field in the supplied renameMap
        for (ii = 0; ii < numColsIn; ii++) {
            if (renameMapIn[ii].type != DfFatptr) {
                // For Immediates do a direct comparison with the newName
                if (strcmp(fieldName, renameMapIn[ii].newName) == 0) {
                    status = strStrlcpy(cols[cur].oldName,
                                        renameMapIn[ii].oldName,
                                        sizeof(cols[cur].oldName));
                    BailIfFailed(status);
                    cols[cur].isKey = renameMapIn[ii].isKey;
                    break;
                } else if (strcmp(fieldName, renameMapIn[ii].oldName) == 0) {
                    isOldColName = true;
                }
            } else {
                // For Fatptrs compare and rename only the prefixes
                ret = snprintf(prefix,
                               sizeof(prefix),
                               "%s%s",
                               renameMapIn[ii].newName,
                               DfFatptrPrefixDelimiter);
                assert(ret <= (int) sizeof(prefix) - 1);

                if (strncmp(fieldName, prefix, ret) == 0) {
                    status = strSnprintf(cols[cur].oldName,
                                         sizeof(cols[cur].oldName),
                                         "%s%s%s",
                                         renameMapIn[ii].oldName,
                                         DfFatptrPrefixDelimiterReplaced,
                                         &fieldName[ret]);
                    cols[cur].isKey = renameMapIn[ii].isKey;
                    BailIfFailed(status);
                    break;
                }

                ret = snprintf(prefix,
                               sizeof(prefix),
                               "%s%s",
                               renameMapIn[ii].oldName,
                               DfFatptrPrefixDelimiter);
                assert(ret <= (int) sizeof(prefix) - 1);

                if (strncmp(fieldName, prefix, ret) == 0) {
                    isOldColName = true;
                }
            }
        }

        if (ii < numColsIn) {
            // found a match, initialize rest of map and increment counter
            status = strStrlcpy(cols[cur].newName,
                                fieldName,
                                sizeof(cols[cur].newName));
            BailIfFailed(status);
            DataFormat::replaceFatptrPrefixDelims(cols[cur].newName);

            cols[cur].type = DfUnknown;
            cur++;
        } else if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiJoin &&
                   dagNode->dagNodeHdr.apiInput->joinInput.keepAllColumns &&
                   !isOldColName) {
            // If keepAllColumns is true, and colName not in old names
            // check if it appear in the other rename map
            for (ii = 0; ii < numColsFromOtherParent; ii++) {
                if (renameMapFromOtherParent[ii].type != DfFatptr) {
                    // If created by the other side, continue
                    if (strcmp(fieldName,
                               renameMapFromOtherParent[ii].newName) == 0) {
                        isFromOtherParent = true;
                        break;
                    } else if (strcmp(fieldName,
                                      renameMapFromOtherParent[ii].oldName) ==
                               0) {
                        // Found in old name of other parent
                        // This column should be from current parent
                        break;
                    }
                } else {
                    // For Fatptrs compare only the prefixes
                    ret = snprintf(prefix,
                                   sizeof(prefix),
                                   "%s%s",
                                   renameMapFromOtherParent[ii].newName,
                                   DfFatptrPrefixDelimiter);
                    assert(ret <= (int) sizeof(prefix) - 1);

                    if (strncmp(fieldName, prefix, ret) == 0) {
                        isFromOtherParent = true;
                        break;
                    }

                    ret = snprintf(prefix,
                                   sizeof(prefix),
                                   "%s%s",
                                   renameMapFromOtherParent[ii].oldName,
                                   DfFatptrPrefixDelimiter);
                    assert(ret <= (int) sizeof(prefix) - 1);

                    if (strncmp(fieldName, prefix, ret) == 0) {
                        break;
                    }
                }
            }
            if (ii == numColsFromOtherParent) {
                // ColName not exist in any rename map
                // have to make keepAllColumns = true
                *needKeepAllColumn = true;
                break;
            } else if (!isFromOtherParent) {
                // ColName found in old name of the other parent
                // push add this column into rename map
                status = strStrlcpy(cols[cur].oldName,
                                    fieldName,
                                    sizeof(cols[cur].oldName));
                BailIfFailed(status);
                status = strStrlcpy(cols[cur].newName,
                                    fieldName,
                                    sizeof(cols[cur].newName));
                BailIfFailed(status);
                DataFormat::replaceFatptrPrefixDelims(cols[cur].oldName);
                DataFormat::replaceFatptrPrefixDelims(cols[cur].newName);
                cols[cur].type = DfUnknown;
                cur++;
            }
        }
    }

CommonExit:
    if (status != StatusOk) {
        if (cols) {
            memFree(cols);
            cols = NULL;
        }
    }

    *numColsOut = cur;
    *renameMapOut = cols;

    return status;
}

Status
Optimizer::updateUnionNode(Dag *dag, DagNodeTypes::Node *dagNode)
{
    Status status = StatusUnknown;
    XcalarApiUnionInput *unionInput;
    XcalarWorkItem *workItem = NULL;
    unsigned numSrc;

    unionInput = &dagNode->dagNodeHdr.apiInput->unionInput;
    numSrc = unionInput->numSrcTables;

    const char *srcTables[numSrc];
    unsigned numCols[numSrc];
    XcalarApiRenameMap *cols[numSrc];
    memZero(cols, sizeof(cols));

    for (unsigned ii = 0; ii < numSrc; ii++) {
        srcTables[ii] = unionInput->srcTables[ii].tableName;

        status = updateRenameMap(dagNode,
                                 unionInput->renameMapSizes[ii],
                                 unionInput->renameMap[ii],
                                 &numCols[ii],
                                 &cols[ii]);
        BailIfFailed(status);
    }

    workItem = xcalarApiMakeUnionWorkItem(numSrc,
                                          srcTables,
                                          unionInput->dstTable.tableName,
                                          numCols,
                                          cols,
                                          unionInput->dedup,
                                          unionInput->unionType);
    BailIfNull(workItem);

    // replace old input
    Dag::dagApiFree(dagNode);
    unionInput = NULL;

    dagNode->dagNodeHdr.apiInput = Dag::dagApiAlloc(workItem->inputSize);
    BailIfNull(dagNode->dagNodeHdr.apiInput);
    status = Dag::sparseMemCpy(dagNode->dagNodeHdr.apiInput,
                               workItem->input,
                               workItem->inputSize);
    BailIfFailed(status);
    dagNode->dagNodeHdr.apiDagNodeHdr.inputSize = workItem->inputSize;

CommonExit:
    for (unsigned ii = 0; ii < numSrc; ii++) {
        if (cols[ii]) {
            memFree(cols[ii]);
            cols[ii] = NULL;
        }
    }

    if (workItem) {
        if (workItem->input) {
            memFree(workItem->input);
            workItem->input = NULL;
        }

        memFree(workItem);
        workItem = NULL;
    }

    return status;
}

Status
Optimizer::rewriteUnionNodes(Dag *dag, Node *unionNodesList)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    Node *unionNode = unionNodesList->next;

    while (unionNode != unionNodesList) {
        dagNode = unionNode->dagNode;
        if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiUnion) {
            // this union has been optimized away
            unionNode = unionNode->next;
            continue;
        }

        status = updateUnionNode(dag, dagNode);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to update union node: %s",
                    strGetFromStatus(status));
            return status;
        }
        unionNode = unionNode->next;
    }

    return status;
}

// Use more optimal operators. E.g. instead of usual load,
// use load with fields of interest
Status
Optimizer::queryRewrite(Dag *dag,
                        Node *loadNodesList,
                        Node *projectNodesList,
                        Node *joinNodesList,
                        Node *synthesizeNodesList,
                        Node *selectNodesList,
                        Node *unionNodesList,
                        QueryHints *queryHints,
                        FieldSetHashTable *userDefinedFields)
{
    Status status;

    // this converts some nodes to synthesize nodes, must be first
    status = rewriteSourceNodes(dag,
                                synthesizeNodesList,
                                queryHints,
                                userDefinedFields);
    BailIfFailed(status);

    status =
        rewriteSelectNodes(dag, selectNodesList, queryHints, userDefinedFields);
    BailIfFailed(status);

    status =
        rewriteLoadNodes(dag, loadNodesList, queryHints, userDefinedFields);
    BailIfFailed(status);

    status = rewriteProjectNodes(dag, projectNodesList);
    BailIfFailed(status);

    status = rewriteJoinNodes(dag, joinNodesList);
    BailIfFailed(status);

    status = rewriteUnionNodes(dag, unionNodesList);
    BailIfFailed(status);

CommonExit:
    return status;
}

void
Optimizer::hintsInit(QueryHints *queryHintsOut, DagLib::DgRetina *retina)
{
    queryHintsOut->retina = retina;
}

void
Optimizer::hintsDestroy(QueryHints *queryHintsOut)
{
    // XXX On the next episode of query optimization, queryHints discovers that
}

Status
Optimizer::hintsAddDatasetColTypeInfo(QueryHints *queryHints,
                                      const char *datasetName,
                                      uint64_t numCols,
                                      const char *colNames[],
                                      DfFieldType colTypes[])
{
    // XXX Will queryHints ever figure out the true fieldType? Don't miss the
    // next chapter of query optimization!
    return StatusUnimpl;
}

// Warning! This takes your existing DAG and modifies it. The following
// modification might be made:
// 1) re-ordering of dagNodes
// 2) annotation of dagNodes
// 3) insertion of additional dagNodes
// 4) Dropping of some dagNodes
// This function also assumes that no one else is looking/modifying
// the dag
Status
Optimizer::optimize(Dag *dag, QueryHints *queryHints)
{
    Status status = StatusUnknown;
    Node loadNodesListAnchor;
    Node projectNodesListAnchor;
    Node joinNodesListAnchor;
    Node unionNodesListAnchor;
    Node synthesizeNodesListAnchor;
    Node selectNodesListAnchor;

    FieldSetHashTable userDefinedFields;

    loadNodesListAnchor.next = loadNodesListAnchor.prev = &loadNodesListAnchor;

    projectNodesListAnchor.next = projectNodesListAnchor.prev =
        &projectNodesListAnchor;

    joinNodesListAnchor.next = joinNodesListAnchor.prev = &joinNodesListAnchor;
    unionNodesListAnchor.next = unionNodesListAnchor.prev =
        &unionNodesListAnchor;

    synthesizeNodesListAnchor.next = synthesizeNodesListAnchor.prev =
        &synthesizeNodesListAnchor;

    selectNodesListAnchor.next = selectNodesListAnchor.prev =
        &selectNodesListAnchor;

    status = fieldsLivenessAnalysis(dag,
                                    &loadNodesListAnchor,
                                    &projectNodesListAnchor,
                                    &joinNodesListAnchor,
                                    &synthesizeNodesListAnchor,
                                    &selectNodesListAnchor,
                                    &unionNodesListAnchor,
                                    &userDefinedFields);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = queryRewrite(dag,
                          &loadNodesListAnchor,
                          &projectNodesListAnchor,
                          &joinNodesListAnchor,
                          &synthesizeNodesListAnchor,
                          &selectNodesListAnchor,
                          &unionNodesListAnchor,
                          queryHints,
                          &userDefinedFields);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    userDefinedFields.removeAll(&Field::del);
    freeNodeList(&loadNodesListAnchor);
    freeNodeList(&projectNodesListAnchor);
    freeNodeList(&joinNodesListAnchor);
    freeNodeList(&unionNodesListAnchor);
    freeNodeList(&synthesizeNodesListAnchor);
    freeNodeList(&selectNodesListAnchor);

    return status;
}

Status
Optimizer::addField(DagNodeAnnotations *annotations,
                    const char *oldName,
                    const char *newName,
                    bool isImmediate)
{
    Status status = StatusOk;
    Field *field = NULL;

    status = newField(oldName, &field, XdfAcceptAll);
    BailIfFailed(status);

    strlcpy(field->newFieldName, newName, sizeof(field->newFieldName));
    field->isImmediate = isImmediate;

    insertField(&annotations->fieldsRequiredSet, field);
    field = NULL;

CommonExit:
    return status;
}

Status
Optimizer::applyRenameMap(const char *fieldName,
                          XcalarApiRenameMap *renameMap,
                          char *newFieldName,
                          size_t newFieldNameSize,
                          bool *fieldRenamed,
                          bool *isOldColName)
{
    Status status = StatusOk;
    size_t ret, retOld;
    char fatptrPrefix[DfMaxFieldNameLen + 1];
    char fatptrPrefixOld[DfMaxFieldNameLen + 1];
    *fieldRenamed = false;

    if (renameMap->type == DfFatptr) {
        ret = snprintf(fatptrPrefix,
                       sizeof(fatptrPrefix),
                       "%s%s",
                       renameMap->newName,
                       DfFatptrPrefixDelimiter);
        if (ret >= sizeof(fatptrPrefix)) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Field name %s%s is too long (%lu chars). "
                          "Max is %lu chars",
                          renameMap->newName,
                          DfFatptrPrefixDelimiter,
                          ret,
                          sizeof(fatptrPrefix) - 1);
            status = StatusNoBufs;
            goto CommonExit;
        }
        retOld = snprintf(fatptrPrefixOld,
                          sizeof(fatptrPrefixOld),
                          "%s%s",
                          renameMap->oldName,
                          DfFatptrPrefixDelimiter);
        if (retOld >= sizeof(fatptrPrefixOld)) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Field name %s%s is too long (%lu chars). "
                          "Max is %lu chars",
                          renameMap->oldName,
                          DfFatptrPrefixDelimiter,
                          retOld,
                          sizeof(fatptrPrefixOld) - 1);
            status = StatusNoBufs;
            goto CommonExit;
        }
        if (strncmp(fatptrPrefix, fieldName, ret) == 0) {
            ret = snprintf(newFieldName,
                           DfMaxFieldNameLen,
                           "%s%s%s",
                           renameMap->oldName,
                           DfFatptrPrefixDelimiter,
                           &(fieldName)[ret]);
            if (ret >= sizeof(fatptrPrefix)) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Field name %s%s%s is too long (%lu "
                              "chars). Max is %lu chars",
                              renameMap->oldName,
                              DfFatptrPrefixDelimiter,
                              &(fieldName)[ret],
                              ret,
                              sizeof(fatptrPrefix) - 1);
                status = StatusNoBufs;
                goto CommonExit;
            }
            *fieldRenamed = true;
        } else if (strncmp(fatptrPrefixOld, fieldName, retOld) == 0) {
            *isOldColName = true;
        }
    } else {
        // fields coming in are accessor field names, need to convert to raw
        // field names to compare with the raw field names in renameMaps
        char fieldNameTmp[DfMaxFieldNameLen + 1];
        strlcpy(fieldNameTmp, fieldName, sizeof(fieldNameTmp));

        DataFormat::unescapeNestedDelim(fieldNameTmp);

        if (strcmp(renameMap->newName, fieldNameTmp) == 0) {
            // old field name was an immediate.
            // Need to convert the raw field name
            // in the renameMap into an accessor field name by escaping delims
            status =
                strStrlcpy(newFieldName, renameMap->oldName, newFieldNameSize);
            BailIfFailed(status);
            status = DataFormat::escapeNestedDelim(newFieldName,
                                                   sizeof(renameMap->oldName),
                                                   NULL);

            if (status == StatusNameTooLong) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Cannot escape nested delimiters, "
                              "Field name %s is too long",
                              renameMap->oldName);
                goto CommonExit;
            }
            *fieldRenamed = true;
        } else if (strcmp(renameMap->oldName, fieldNameTmp) == 0) {
            *isOldColName = true;
        }
    }

CommonExit:
    return status;
}

bool
Optimizer::fieldInColumnsInput(const char *fieldNameInOut,
                               char *fieldNameBuf,
                               size_t fieldNameBufSize,
                               XcalarApis api,
                               XcalarApiInput *apiInput,
                               int parentIdx,
                               bool *isFromOtherParent)
{
    Status status;
    bool fieldRenamed = false;
    bool isOldColName = false;

    if (api == XcalarApiJoin) {
        XcalarApiJoinInput *joinInput;
        joinInput = &apiInput->joinInput;

        unsigned start, end;

        if (parentIdx == 0) {
            start = 0;
            end = joinInput->numLeftColumns;
        } else {
            start = joinInput->numLeftColumns;
            end = joinInput->numLeftColumns + joinInput->numRightColumns;
        }

        status = strStrlcpy(fieldNameBuf, fieldNameInOut, fieldNameBufSize);
        BailIfFailed(status);

        for (unsigned ii = start; ii < end; ii++) {
            status = applyRenameMap(fieldNameInOut,
                                    &joinInput->renameMap[ii],
                                    fieldNameBuf,
                                    fieldNameBufSize,
                                    &fieldRenamed,
                                    &isOldColName);
            BailIfFailed(status);

            if (fieldRenamed) {
                break;
            }
        }

        if (fieldRenamed) {
            // Find in new name of rename map, should be from this parent
            *isFromOtherParent = false;
        } else if (isOldColName) {
            // Not in new name but in old name of rename map
            // this column can't be from current parent
            *isFromOtherParent = true;
        } else {
            // name not exist in this rename map at all
            // need to check rename map of the other parent
            // if it's created there, it's from the other parent
            char fieldNameBufTmp[DfMaxFieldNameLen + 1];
            bool fieldRenamedTmp = false;
            if (parentIdx == 1) {
                start = 0;
                end = joinInput->numLeftColumns;
            } else {
                start = joinInput->numLeftColumns;
                end = joinInput->numLeftColumns + joinInput->numRightColumns;
            }
            for (unsigned ii = start; ii < end; ii++) {
                status = applyRenameMap(fieldNameInOut,
                                        &joinInput->renameMap[ii],
                                        fieldNameBufTmp,
                                        sizeof(fieldNameBufTmp),
                                        &fieldRenamedTmp,
                                        &isOldColName);
                BailIfFailed(status);

                // Find in new name of the other parent's rename map
                if (fieldRenamedTmp) {
                    *isFromOtherParent = true;
                    break;
                }
            }
        }
    } else if (api == XcalarApiUnion) {
        XcalarApiUnionInput *unionInput;
        unionInput = &apiInput->unionInput;
        for (unsigned ii = 0; ii < unionInput->renameMapSizes[parentIdx];
             ii++) {
            status = applyRenameMap(fieldNameInOut,
                                    &unionInput->renameMap[parentIdx][ii],
                                    fieldNameBuf,
                                    fieldNameBufSize,
                                    &fieldRenamed,
                                    &isOldColName);
            BailIfFailed(status);

            if (fieldRenamed) {
                break;
            }
        }
    } else if (api == XcalarApiIndex && !apiInput->indexInput.source.isTable) {
        // Keep only columns with correct prefix
        // flag name may not be accurate, here it mean it's valid or not
        int ret;
        char prefix[DfMaxFieldNameLen + 1];
        ret = snprintf(prefix,
                       sizeof(prefix),
                       "%s%s",
                       apiInput->indexInput.fatptrPrefixName,
                       DfFatptrPrefixDelimiter);
        assert(ret <= (int) sizeof(prefix) - 1);
        if (strncmp(fieldNameInOut, prefix, ret) == 0) {
            status = strStrlcpy(fieldNameBuf, fieldNameInOut, fieldNameBufSize);
            BailIfFailed(status);
            fieldRenamed = true;
        } else {
            ret = snprintf(prefix,
                           sizeof(prefix),
                           "%s%s",
                           apiInput->indexInput.fatptrPrefixName,
                           DfFatptrPrefixDelimiterReplaced);
            assert(ret <= (int) sizeof(prefix) - 1);
            if (strncmp(fieldNameInOut, prefix, ret) == 0) {
                status =
                    strStrlcpy(fieldNameBuf, fieldNameInOut, fieldNameBufSize);
                BailIfFailed(status);
                fieldRenamed = true;
            }
        }
    } else {
        // all other apis don't rely on column input
        status = strStrlcpy(fieldNameBuf, fieldNameInOut, fieldNameBufSize);
        BailIfFailed(status);
        fieldRenamed = true;
    }

CommonExit:
    return fieldRenamed;
}

Status
Optimizer::reverseParseAnnotations(void *annotations, json_t *annotationsJson)
{
    int ret;
    Status status = StatusOk;
    DagNodeAnnotations *optimizerContext = (DagNodeAnnotations *) annotations;

    FieldsRequiredSet *fieldsRequiredSet = &optimizerContext->fieldsRequiredSet;

    json_t *fields = json_array();
    BailIfNull(fields);

    Field *field;
    for (FieldSetHashTable::iterator it = fieldsRequiredSet->hashTable->begin();
         (field = it.get()) != NULL;
         it.next()) {
        json_error_t err;
        const char *typeStr;
        if (field->isImmediate) {
            // figure out actual type later and fill in
            typeStr = "DfUnknown";
        } else {
            typeStr = "DfFatptr";
        }

        json_t *fieldJson =
            json_pack_ex(&err,
                         0,
                         QueryCmdParser::JsonPackColumnFormatString,
                         QueryCmdParser::SourceColumnKey,
                         field->fieldName,
                         QueryCmdParser::DestColumnKey,
                         field->newFieldName,
                         QueryCmdParser::ColumnTypeKey,
                         typeStr);
        BailIfNull(fieldJson);

        ret = json_array_append_new(fields, fieldJson);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        fieldJson = NULL;
    }

    ret = json_object_set_new(annotationsJson,
                              QueryCmdParser::ColumnsKey,
                              fields);
    BailIfFailedWith(ret, StatusJsonQueryParseError);
    fields = NULL;

CommonExit:
    if (fields) {
        json_decref(fields);
        fields = NULL;
    }

    return status;
}

Status
Optimizer::parseAnnotations(json_t *annotationsJson, void **annotationsOut)
{
    Status status = StatusOk;
    DagNodeAnnotations *optimizerContext = NULL;

    json_t *fields =
        json_object_get(annotationsJson, QueryCmdParser::ColumnsKey);
    if (fields == NULL) {
        goto CommonExit;
    }

    optimizerContext = newDagNodeAnnotations(0);
    BailIfNull(optimizerContext);

    unsigned ii;
    json_t *val;

    json_array_foreach (fields, ii, val) {
        const char *type = "DfUnknown", *oldName = "", *newName = "";
        json_error_t err;

        int ret = json_unpack_ex(val,
                                 &err,
                                 0,
                                 QueryCmdParser::JsonUnpackColumnFormatString,
                                 QueryCmdParser::SourceColumnKey,
                                 &oldName,
                                 QueryCmdParser::DestColumnKey,
                                 &newName,
                                 QueryCmdParser::ColumnTypeKey,
                                 &type);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        bool isImmediate;
        if (strcmp(type, "DfFatptr") == 0) {
            isImmediate = false;
        } else {
            isImmediate = true;
        }

        if (strlen(newName) != 0) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Column rename is not supported");
            goto CommonExit;
        }

        status = addField(optimizerContext, oldName, newName, isImmediate);
        BailIfFailed(status);
    }

CommonExit:
    if (status != StatusOk) {
        if (optimizerContext) {
            removeAnnotations(optimizerContext);
        }
    } else {
        *annotationsOut = optimizerContext;
    }

    return status;
}
