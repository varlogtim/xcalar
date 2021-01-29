// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPTIMIZER_H_
#define _OPTIMIZER_H_

#include "dag/DagTypes.h"
#include "runtime/Semaphore.h"
#include "util/StringHashTable.h"
#include "dag/DagNodeTypes.h"
#include "bc/BufferCache.h"
#include "dag/DagLib.h"

class Optimizer final
{
  public:
    struct Field {
        Field() { memZero(newFieldName, sizeof(newFieldName)); }

        char fieldName[DfMaxFieldNameLen + 1];
        char newFieldName[DfMaxFieldNameLen + 1];
        bool isImmediate = true;
        XdfTypesAccepted typesAccepted;
        StringHashTableHook hook;
        const char *getFieldName() const { return fieldName; };
        void del();
    };

    enum : uint64_t { FieldHashTableSlot = 7 };

    typedef StringHashTable<Field,
                            &Field::hook,
                            &Field::getFieldName,
                            FieldHashTableSlot,
                            hashStringFast>
        FieldSetHashTable;

    static MustCheck Optimizer *get();
    static MustCheck Status init();
    void destroy();

    enum : uint64_t {
        AnnotationsBcNumElems = 128,
        FieldsEltBcNumElems = 128,
    };

    struct FieldsRequiredSet {
        FieldSetHashTable *hashTable = NULL;
        unsigned numFieldsRequired = 0;
    };

    struct DagNodeAnnotations {
        FieldsRequiredSet fieldsRequiredSet;
        uint64_t numChildrenLeftToProcess = 0;
        OperatorFlag flags = OperatorFlagNone;
    };

    struct QueryHints {
        DagLib::DgRetina *retina = NULL;
    };

    static constexpr size_t AnnotationsBcBufSize = sizeof(DagNodeAnnotations);
    static constexpr size_t FieldsEltBcBufSize = sizeof(Field);

    bool fieldInColumnsInput(const char *fieldNameInOut,
                             char *fieldNameBuf,
                             size_t fieldNameBufSize,
                             XcalarApis api,
                             XcalarApiInput *apiInput,
                             int parentIdx,
                             bool *isFromOtherParent);

    MustCheck Status parseAnnotations(json_t *annotationsJson,
                                      void **annotationsOut);

    MustCheck static Status reverseParseAnnotations(void *annotations,
                                                    json_t *annotationsJson);

    void removeAnnotations(DagNodeAnnotations *annotations);
    MustCheck Status optimize(Dag *dag, QueryHints *queryHints);
    MustCheck Status
    getProjectedFieldNames(void *optimizerContext,
                           char (**projectedFieldNames)[DfMaxFieldNameLen + 1],
                           unsigned *numProjectedFieldNamesOut);
    void hintsInit(QueryHints *queryHintsOut, DagLib::DgRetina *retina);
    void hintsDestroy(QueryHints *queryHints);
    MustCheck Status hintsAddDatasetColTypeInfo(QueryHints *queryHints,
                                                const char *datasetName,
                                                uint64_t numCols,
                                                const char *colNames[],
                                                DfFieldType colTypes[]);
    MustCheck Status addField(DagNodeAnnotations *annotations,
                              const char *oldName,
                              const char *newName,
                              bool isImmediate);
    MustCheck Status applyRenameMap(const char *fieldName,
                                    XcalarApiRenameMap *renameMap,
                                    char *newFieldName,
                                    size_t newFieldNameSize,
                                    bool *fieldRenamed,
                                    bool *isOldColName);

    MustCheck static Status populateValuesDescWithFieldsRequired(
        void *optimizerContext,
        unsigned numKeys,
        int *keyIndexes,
        const char *keyNames[],
        const NewTupleMeta *srcTupMeta,
        NewTupleMeta *dstTupMeta,
        XdbMeta *srcMeta,
        const char *immediateNames[],
        unsigned &numImmediates,
        const char *fatptrPrefixNames[],
        unsigned &numFatptrs);

  private:
    Optimizer(){};
    ~Optimizer(){};

    struct SearchState {
        DagNodeTypes::Node *dagNode = NULL;
        struct SearchState *prev = NULL;
        struct SearchState *next = NULL;
    };

    struct Node {
        DagNodeTypes::Node *dagNode = NULL;
        struct Node *prev = NULL;
        struct Node *next = NULL;
    };

    BcHandle *annotationsBcHandle_ = NULL;
    BcHandle *fieldsBcHandle_ = NULL;

    static Optimizer *instance;

    MustCheck Status initInternal();
    void destroyInternal();

    void freeField(Field *field);

    MustCheck Status newField(const char *fieldName,
                              Field **fieldOut,
                              XdfTypesAccepted typesAccepted);

    void insertField(FieldsRequiredSet *fieldsRequiredSet, Field *field);
    MustCheck Status getFieldsProduced(DagNodeTypes::Node *node,
                                       const char ***fieldsOut,
                                       unsigned *numFieldsOut);
    MustCheck Status
    updateParentsFieldsRequiredSet(DagNodeTypes::Node *parentNode,
                                   unsigned parentIdx,
                                   DagNodeTypes::Node *currNode,
                                   FieldsRequiredSet *fieldsConsumed);
    MustCheck Status getFieldsConsumed(Dag *dag,
                                       FieldsRequiredSet *fieldsRequiredSet,
                                       char *evalStr);
    MustCheck Status getFieldsConsumedSet(Dag *dag,
                                          DagNodeTypes::Node *currNode,
                                          FieldsRequiredSet *fieldsConsumed);

    MustCheck Status initDagNodeAnnotations(Dag *dag,
                                            DagNodeTypes::Node **leafNodesOut[],
                                            uint64_t *numLeafNodesOut);
    MustCheck DagNodeAnnotations *newDagNodeAnnotations(uint64_t numChildren);

    MustCheck Status insertNodeToList(Node *nodeList,
                                      DagNodeTypes::Node *dagNode);
    void freeNodeList(Node *nodeList);

    MustCheck Status
    fieldsLivenessAnalysis(Dag *dag,
                           Node *loadNodesList,
                           Node *projectNodesList,
                           Node *joinNodesList,
                           Node *synthesizeNodesList,
                           Node *selectNodesList,
                           Node *unionNoesList,
                           FieldSetHashTable *userDefinedFields);

    MustCheck Status updateLoadNode(Dag *dag,
                                    DagNodeTypes::Node *dagNode,
                                    QueryHints *queryHints,
                                    FieldSetHashTable *userDefinedFields);
    MustCheck Status updateSourceNode(Dag *dag,
                                      DagNodeTypes::Node *dagNode,
                                      QueryHints *queryHints,
                                      FieldSetHashTable *userDefinedFields);
    MustCheck Status updateSelectNode(Dag *dag,
                                      Dag *srcGraph,
                                      DagNodeTypes::Node *dagNode,
                                      QueryHints *queryHints,
                                      FieldSetHashTable *userDefinedFields);
    MustCheck Status updateIndexNode(DagNodeTypes::Node *indexNode,
                                     const char *newDatasetName);
    MustCheck Status updateProjectNode(DagNodeTypes::Node *dagNode);
    MustCheck Status updateJoinNode(Dag *dag, DagNodeTypes::Node *dagNode);
    MustCheck Status updateUnionNode(Dag *dag, DagNodeTypes::Node *dagNode);

    MustCheck Status rewriteLoadNodes(Dag *dag,
                                      Node *loadNodesList,
                                      QueryHints *queryHints,
                                      FieldSetHashTable *userDefinedFields);
    MustCheck Status rewriteProjectNodes(Dag *dag, Node *projectNodesList);
    MustCheck Status rewriteJoinNodes(Dag *dag, Node *joinNodesList);
    MustCheck Status rewriteUnionNodes(Dag *dag, Node *unionNodesList);
    MustCheck Status rewriteSourceNodes(Dag *dag,
                                        Node *synthesizeNodesList,
                                        QueryHints *queryHints,
                                        FieldSetHashTable *userDefinedFields);

    MustCheck Status rewriteSelectNodes(Dag *dag,
                                        Node *selectNodesList,
                                        QueryHints *queryHints,
                                        FieldSetHashTable *userDefinedFields);

    MustCheck Status queryRewrite(Dag *dag,
                                  Node *loadNodesList,
                                  Node *projectNodesList,
                                  Node *joinNodesList,
                                  Node *synthesizeNodesList,
                                  Node *selectNodesList,
                                  Node *unionNodesList,
                                  QueryHints *queryHints,
                                  FieldSetHashTable *userDefinedFields);
    MustCheck bool getFilterStringFromChild(Dag *dag,
                                            DagNodeTypes::Node *dagNode,
                                            bool convertNames,
                                            bool aggVarsAllowed,
                                            bool udfsAllowed,
                                            DagNodeTypes::Node **filterNodeOut,
                                            char *evalStringOut);
    MustCheck Status updateRenameMap(DagNodeTypes::Node *sourceNode,
                                     unsigned numColsIn,
                                     XcalarApiRenameMap *renameMapIn,
                                     unsigned *numColsOut,
                                     XcalarApiRenameMap **renameMapOut,
                                     int numSrc = 0,
                                     bool *needKeepAllColumn = NULL);

    // Disallow
    Optimizer(const Optimizer &) = delete;
    Optimizer &operator=(const Optimizer &) = delete;
};

#endif  // _OPTIMIZER_H_
