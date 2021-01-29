// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _JSONGEN_H_
#define _JSONGEN_H_

#include "util/Random.h"
#include "util/Atomics.h"
#include "JsonGenEnums.h"

typedef enum {
    DuplicatedKey = 0,
    NoDuplicatedKeySameLevel = 1,
    NoDuplicatedKey = 2
} JsonGenKeyDupType;

// Longest word in a major dictionary is 45
typedef char DictWord[256];

enum {
    JsonGenMaxNestedLevel = 32,
    JsonGenMaxNumDictWord = 500000,  // linux.words has 479,828 entries
    MaxPathSize = 256,
    MaxSingleFieldLen = 32,
};

struct JsonEle {
    char *fieldName;
    JgFieldType type;
    unsigned listSize;
    struct JsonEle *listHead;
    unsigned emptyRatio;
    struct JsonEle *prev;
    struct JsonEle *next;
};

struct JsonGenHandle {
    JsonEle *topLevelList;
    RandHandle rndHandle;
    uint64_t uniqueFieldIndex;
    Atomic64 primaryKeyIndex;
    bool useDict;
};

void jsonGenDestroyHandle(JsonGenHandle *handle);

Status jsonGenNewHandleWithSchema(char *fileName,
                                  JsonGenHandle **handle,
                                  int nodeId,
                                  bool useDict);

Status jsonGenNewHandleRand(char *fileName,
                            uint64_t maxField,
                            uint64_t maxNestedLevel,
                            JsonGenKeyDupType jsonGenKeyDupType,
                            bool allowMixPrimitiveFieldInArray,
                            JsonGenHandle **handle,
                            int nodeId,
                            bool useDict);

Status jsonGenGetRecord(char *buffer,
                        size_t bufferSize,
                        size_t *outputSize,
                        JsonGenHandle *handle);

Status jsonGenGetRecords(char *buffer,
                         size_t bufferSize,
                         size_t *outputSize,
                         unsigned numRecords,
                         JsonGenHandle *handle);

Status jsonGenInitDict(char *path);
void jsonGenDestroyDict();

Status writeRecordToFile(JsonGenHandle *handle,
                         bool byRecord,
                         uint64_t numOfRecord,
                         bool bySize,
                         uint64_t fileSize,
                         uint64_t fileSizeThreshold,
                         char *outputFileDir);

Status generateRandJsonContent(RandHandle *rndWeakHandle,
                               char *buffer,
                               size_t bufLenIn,
                               size_t *writtenLen,
                               JgFieldType type,
                               Atomic64 *primaryKeyIndexIn,
                               bool useDict);

#endif  // _JSONGEN_H_
