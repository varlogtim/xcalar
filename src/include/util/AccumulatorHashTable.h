// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef ACCHASHTABLE_H
#define ACCHASHTABLE_H

#include "xdb/DataModelTypes.h"
#include "operators/OperatorsEvalTypes.h"
#include "util/MemoryPool.h"
#include "df/DataFormat.h"
#include "sys/XLog.h"

class AccumulatorHashTableEntry final
{
    friend class AccumulatorHashTable;

  public:
    DfFieldValue *tupleFields = NULL;
    uint8_t *tupleBitMaps = NULL;
    const XdbMeta *xdbMeta = NULL;
    ValAccumulator **accs = NULL;

  private:
    // Left node and right node in the binary search tree structure.
    AccumulatorHashTableEntry *left = NULL;
    AccumulatorHashTableEntry *right = NULL;
    // This is purely for quick traversal (unordered). Nodes are linked when
    // they are inserted into the tree. The ordered iterator doesn't need this.
    AccumulatorHashTableEntry *next = NULL;
};

class AccumulatorHashTable final
{
  public:
    AccumulatorHashTable() {}
    ~AccumulatorHashTable();
    AccumulatorHashTableEntry *insert(const NewTupleValues *tupleIn,
                                      const XdbMeta *xdbMetaIn);
    AccumulatorHashTableEntry *&find(const NewTupleValues *tupleIn,
                                     const XdbMeta *xdbMetaIn);
    Status init(AccumulatorType *accTypesIn,
                DfFieldType *argTypesIn,
                size_t numAccs,
                bool ignoreKeys = false);
    void clear();

    class StackNode final
    {
      public:
        AccumulatorHashTableEntry *entry;
        StackNode *prev;

        StackNode(AccumulatorHashTableEntry *entryIn, StackNode *prevIn)
            : entry(entryIn), prev(prevIn){};
    };

    class OrderedIterator final
    {
      public:
        OrderedIterator(AccumulatorHashTable *ht)
            : entry(NULL),
              hashTable(ht),
              topNode(NULL),
              curEntry(NULL),
              curSlot(0){};
        ~OrderedIterator();
        AccumulatorHashTableEntry *entry;
        void next(Status *statusOut);
        void next();

      private:
        AccumulatorHashTable *hashTable;
        StackNode *topNode;
        AccumulatorHashTableEntry *curEntry;
        size_t curSlot;
    };

    class Iterator final
    {
      public:
        Iterator(AccumulatorHashTable *ht) : entry(ht->head), hashTable(ht){};
        AccumulatorHashTableEntry *entry;
        void next();

      private:
        AccumulatorHashTable *hashTable;
    };

    std::unique_ptr<OrderedIterator> orderedBegin();
    std::unique_ptr<Iterator> begin();

  private:
    size_t numSlots;
    AccumulatorHashTableEntry **slots = NULL;
    MemoryPool memPool;
    AccumulatorType accTypes[TupleMaxNumValuesPerRecord];
    DfFieldType argTypes[TupleMaxNumValuesPerRecord];
    size_t numAccs;
    AccumulatorHashTableEntry *head = NULL;
    AccumulatorHashTableEntry *tail = NULL;
    NewTupleValues tmpTuple;
    bool ignoreKeys;

    size_t getSlot(const NewTupleValues *tupleIn, const XdbMeta *xdbMetaIn);
    int compare(const NewTupleValues *tupleIn1,
                const XdbMeta *xdbMetaIn1,
                const NewTupleValues *tupleIn2,
                const XdbMeta *xdbMetaIn2);
    MustCheck Status insertEntry(const NewTupleValues *tupleIn,
                                 const XdbMeta *xdbMetaIn,
                                 AccumulatorHashTableEntry *&entry);
    ValAccumulator **getAccs();
};

#endif
