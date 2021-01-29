// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/AccumulatorHashTable.h"

Status
AccumulatorHashTable::init(AccumulatorType *accTypesIn,
                           DfFieldType *argTypesIn,
                           size_t numAccs,
                           bool ignoreKeys)
{
    for (size_t i = 0; i < numAccs; ++i) {
        this->accTypes[i] = accTypesIn[i];
        this->argTypes[i] = argTypesIn[i];
    }
    this->numAccs = numAccs;
    this->ignoreKeys = ignoreKeys;

    this->numSlots = XcalarConfig::get()->accumulatorHashTableSlotCount_;
    this->slots =
        new (std::nothrow) AccumulatorHashTableEntry *[this->numSlots];
    if (this->slots == NULL) {
        return StatusNoMem;
    }
    memset(this->slots, 0, (sizeof *this->slots) * this->numSlots);
    return StatusOk;
}

AccumulatorHashTable::~AccumulatorHashTable()
{
    delete[] this->slots;
    this->slots = NULL;
}

// Return NULL if out of memory when inserting a new element.
AccumulatorHashTableEntry *
AccumulatorHashTable::insert(const NewTupleValues *tupleIn,
                             const XdbMeta *xdbMetaIn)
{
    AccumulatorHashTableEntry *&entry = this->find(tupleIn, xdbMetaIn);

    if (!entry) {
        Status status = this->insertEntry(tupleIn, xdbMetaIn, entry);
        if (status != StatusOk) {
            entry = NULL;
        }
    }

    return entry;
}

// Return the element. If doesn't exist, return the position it should be
// inserted.
AccumulatorHashTableEntry *&
AccumulatorHashTable::find(const NewTupleValues *tupleIn,
                           const XdbMeta *xdbMetaIn)
{
    AccumulatorHashTableEntry **node =
        &this->slots[this->getSlot(tupleIn, xdbMetaIn)];

    while (*node) {
        const NewTupleMeta *tupMeta =
            (*node)->xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
        // XXX This can be optimized. But somehow need to change
        // DataFormat::fieldArrayCompare.
        this->tmpTuple.cloneFrom(tupMeta,
                                 (*node)->tupleFields,
                                 (*node)->tupleBitMaps);
        int comp = this->compare(tupleIn,
                                 xdbMetaIn,
                                 &this->tmpTuple,
                                 (*node)->xdbMeta);

        if (comp == 0) {
            break;
        } else if (comp < 0) {
            node = &(*node)->left;
        } else {
            node = &(*node)->right;
        }
    }

    return *node;
}

void
AccumulatorHashTable::clear()
{
    this->memPool.free();
    memset(this->slots, 0, (sizeof *this->slots) * this->numSlots);
}

uint64_t
AccumulatorHashTable::getSlot(const NewTupleValues *tupleIn,
                              const XdbMeta *xdbMetaIn)
{
    return (size_t) DataFormat::fieldArrayHash(xdbMetaIn->numKeys,
                                               xdbMetaIn->keyIdxOrder,
                                               xdbMetaIn->kvNamedMeta.kvMeta_
                                                   .tupMeta_,
                                               tupleIn) %
           this->numSlots;
}

int
AccumulatorHashTable::compare(const NewTupleValues *tupleIn1,
                              const XdbMeta *xdbMetaIn1,
                              const NewTupleValues *tupleIn2,
                              const XdbMeta *xdbMetaIn2)
{
    unsigned numFields;
    const int *fieldOrderArray1;
    const int *fieldOrderArray2;

    if (this->ignoreKeys) {
        numFields = xdbMetaIn1->getNumFields();
        fieldOrderArray1 = NULL;
        fieldOrderArray2 = NULL;
    } else {
        numFields = xdbMetaIn1->numKeys;
        fieldOrderArray1 = xdbMetaIn1->keyIdxOrder;
        fieldOrderArray2 = xdbMetaIn2->keyIdxOrder;
    }

    return DataFormat::fieldArrayCompare(numFields,
                                         NULL,
                                         fieldOrderArray1,
                                         xdbMetaIn1->kvNamedMeta.kvMeta_
                                             .tupMeta_,
                                         tupleIn1,
                                         fieldOrderArray2,
                                         xdbMetaIn2->kvNamedMeta.kvMeta_
                                             .tupMeta_,
                                         tupleIn2,
                                         true);
}

// entry == NULL if out of memory.
Status
AccumulatorHashTable::insertEntry(const NewTupleValues *tupleIn,
                                  const XdbMeta *xdbMetaIn,
                                  AccumulatorHashTableEntry *&entry)
{
    Status status = StatusOk;
    size_t numFields = 0;

    AccumulatorHashTableEntry *newEntry =
        (AccumulatorHashTableEntry *) this->memPool.getElem(sizeof *newEntry);
    BailIfNullXdb(newEntry);
    entry = new (newEntry) AccumulatorHashTableEntry();

    entry->xdbMeta = xdbMetaIn;

    numFields = xdbMetaIn->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
    entry->tupleFields = (DfFieldValue *) this->memPool.getElem(
        (sizeof *entry->tupleFields) * numFields);
    BailIfNullXdb(entry->tupleFields);

    entry->tupleBitMaps = (uint8_t *) this->memPool.getElem(
        (sizeof *entry->tupleBitMaps) * numFields);
    BailIfNullXdb(entry->tupleBitMaps);

    tupleIn->cloneTo(xdbMetaIn->kvNamedMeta.kvMeta_.tupMeta_,
                     entry->tupleFields,
                     entry->tupleBitMaps);

    entry->accs = this->getAccs();
    BailIfNullXdb(entry->accs);

    entry->left = NULL;
    entry->right = NULL;

    if (!this->head) {
        this->head = entry;
    }
    if (this->tail) {
        this->tail->next = entry;
    }
    this->tail = entry;

CommonExit:
    if (status != StatusOk) {
        if (entry) {
            if (entry->accs) {
                for (size_t i = 0; i < this->numAccs; ++i) {
                    if (entry->accs[i]) {
                        this->memPool.putElem(entry->accs[i]);
                    }
                }
                this->memPool.putElem(entry->accs);
            }

            if (entry->tupleBitMaps) {
                this->memPool.putElem(entry->tupleBitMaps);
            }

            if (entry->tupleFields) {
                this->memPool.putElem(entry->tupleFields);
            }

            this->memPool.putElem(entry);
            entry = NULL;
        }
    }
    return status;
}

// Get a new copy of initialized accumulators. Return NULL if out of memory.
ValAccumulator **
AccumulatorHashTable::getAccs()
{
    Status status = StatusOk;
    size_t ii = 0;
    ValAccumulator **accs = (ValAccumulator **) this->memPool.getElem(
        (sizeof *accs) * this->numAccs);
    BailIfNullXdb(accs);

    for (ii = 0; ii < this->numAccs; ++ii) {
        accs[ii] = GroupEvalContext::allocAccumulator(this->accTypes[ii],
                                                      &this->memPool);
        BailIfNullXdb(accs[ii]);
        accs[ii]->init(this->argTypes[ii]);
    }

CommonExit:
    if (status != StatusOk) {
        if (accs) {
            for (size_t jj = 0; jj < ii; ++jj) {
                if (accs[jj]) {
                    this->memPool.putElem(accs[jj]);
                }
            }
            this->memPool.putElem(accs);
        }
        return NULL;
    }

    return accs;
}

AccumulatorHashTable::OrderedIterator::~OrderedIterator()
{
    while (this->topNode) {
        StackNode *tmpNode = this->topNode;
        this->topNode = this->topNode->prev;
        this->hashTable->memPool.putElem(tmpNode);
    }
}

void
AccumulatorHashTable::OrderedIterator::next()
{
    Status status = StatusOk;
    this->next(&status);

    if (status != StatusOk) {
        this->entry = NULL;
    }
}

// Inorder travesal
// Space complexity: Average case O(log(n)). Worst case O(n).
// But this doesn't require additional links (* next) between nodes.
// Morris traversal is slower than using a stack.
void
AccumulatorHashTable::OrderedIterator::next(Status *statusOut)
{
    Status status = StatusOk;
    StackNode *tmpNode = NULL;

    // curSlot could be empty.
    while (!this->curEntry && !this->topNode) {
        if (this->curSlot >= this->hashTable->numSlots) {
            this->entry = NULL;
            return;
        }

        this->curEntry = this->hashTable->slots[this->curSlot];
        ++this->curSlot;
    }

    while (this->curEntry) {
        // push current entry to stack.
        StackNode *newNode =
            (StackNode *) this->hashTable->memPool.getElem(sizeof *newNode);
        BailIfNullXdb(newNode);
        new (newNode) StackNode(this->curEntry, this->topNode);
        this->topNode = newNode;

        // go to the left node.
        this->curEntry = this->curEntry->left;
    }

    // curEntry is NULL, the top item in the stack is the next item for the
    // iterator.
    this->curEntry = this->topNode->entry;
    this->entry = this->curEntry;

    // pop from stack.
    tmpNode = this->topNode;
    this->topNode = this->topNode->prev;
    this->hashTable->memPool.putElem(tmpNode);

    // go to the right node.
    this->curEntry = this->curEntry->right;

CommonExit:
    *statusOut = status;
}

// This isn't really ordered. Only each slot is ordered.
std::unique_ptr<AccumulatorHashTable::OrderedIterator>
AccumulatorHashTable::orderedBegin()
{
    Status status = StatusOk;
    std::unique_ptr<OrderedIterator> it(new (std::nothrow)
                                            OrderedIterator(this));
    BailIfNull(it);
    it->next(&status);
    BailIfFailed(status);

CommonExit:
    return status == StatusOk ? std::move(it) : NULL;
}

void
AccumulatorHashTable::Iterator::next()
{
    this->entry = this->entry->next;
}

std::unique_ptr<AccumulatorHashTable::Iterator>
AccumulatorHashTable::begin()
{
    std::unique_ptr<Iterator> it(new (std::nothrow) Iterator(this));
    return it;
}
