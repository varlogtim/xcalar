// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include "StrlFunc.h"
#include "util/System.h"
#include "primitives/Primitives.h"
#include "util/Random.h"
#include "config/Config.h"
#include "test/JsonGen.h"
#include "math.h"
#include "JsonGenInt.h"
#include "util/MemTrack.h"
#include "msg/Xid.h"

static const char *commonFieldPrefix = "f";
static const char *commonArrayElePrefix = "e";
static const char *trueString = "true";
static const char *falseString = "false";
static uint64_t wordCount;
static DictWord *dictWordArray;
static bool dictInitCalled = false;
static const char *defaultDictFilePath = "/usr/share/dict/words";
#ifdef MEMORY_TRACKING  // This avoids an 'unused-variable'
static const char *moduleName = "libjsongen";
#endif  // MEMORY_TRACKING

static const char *outputFileName = "outputFile";
static const char *outputFileExtention = ".json";

static void
appendJsonEleList(JsonEle **listHead, JsonEle *element)
{
    assert(element != NULL);

    if (*listHead == NULL) {
        element->prev = element->next = element;
        *listHead = element;
    } else {
        element->prev = (*listHead)->prev;
        element->next = (*listHead);
        (*listHead)->prev->next = element;
        (*listHead)->prev = element;
    }
}

static JsonEle *
removeHeadJsonEleList(JsonEle **listHead)
{
    JsonEle *ret = NULL;

    if (*listHead != NULL) {
        ret = (*listHead);

        if ((*listHead)->next == (*listHead)) {
            // last element
            *listHead = NULL;
        } else {
            *listHead = (*listHead)->next;
            ret->next->prev = ret->prev;
            ret->prev->next = ret->next;
        }
    }

    return ret;
}

static void
rndGeneratedString(char *buffer, uint64_t bufferLength, RandHandle *rndHandle)
{
    char charset[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    uint64_t index;
    uint64_t inputLen;
    if (bufferLength == 0) {
        return;
    }

    assert(buffer != NULL);

    inputLen = bufferLength - 1;
    while (inputLen > 0) {
        index = rndGenerate64Range(rndHandle, sizeof(charset) - 1);
        *buffer++ = charset[index];
        inputLen--;
    }

    *buffer = '\0';
}

static Status
generateJsonSchemaFile(FILE *fp,
                       char *prefix,
                       bool isArrayElement,
                       unsigned nestedLevel,
                       uint64_t maxField,
                       uint64_t maxNestedLevel,
                       JsonGenKeyDupType jsonGenKeyDupType,
                       bool allowMixPrimitiveFieldInArray,
                       JgFieldType arrayElePrimitiveType,
                       JsonGenHandle *jsonGenHandle)
{
    char *newPrefix = NULL;
    JgFieldType typeToGen;
    unsigned emptyRatio;
    uint64_t numOfFiled;
    char *commonPrefix;
    bool isArray;
    uint64_t fieldIndex = 0;
    int ret;
    bool nested;
    Status status = StatusOk;
    uint64_t maxFieldLen;
    // The nestedRatio will gurantee there will be 0.5% chance that
    // the max level will appear
    uint64_t nestedRatio = 0;

    maxFieldLen = MaxSingleFieldLen * maxNestedLevel;
    nestedRatio = (uint64_t)(pow(0.005, 1.0 / (double) maxNestedLevel) * 100);

    assert(nestedLevel <= maxNestedLevel);
    if (nestedLevel == 0) {
        numOfFiled = maxField;
    } else {
        numOfFiled = rndGenerate64Range(&jsonGenHandle->rndHandle, maxField);
    }

    ret = fprintf(fp, " %llu\n", (unsigned long long) numOfFiled);
    if (ret < -1) {
        status = StatusIO;
        goto CommonExit;
    }

    newPrefix = (char *) memAllocExt(sizeof(char) * maxFieldLen, moduleName);
    if (newPrefix == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < numOfFiled; ++ii) {
        isArray = false;
        if ((isArrayElement) && (!allowMixPrimitiveFieldInArray)) {
            assert(arrayElePrimitiveType != JgTypeUnknown);
            typeToGen = arrayElePrimitiveType;
        } else {
            if ((rndGenerate64Range(&jsonGenHandle->rndHandle, 99) <
                 nestedRatio) &&
                (nestedLevel != maxNestedLevel)) {
                nested = true;
            } else {
                nested = false;
            }

            if (nested) {
                typeToGen = (JgFieldType)(
                    rndGenerate64Range(&jsonGenHandle->rndHandle, 1) + JgArray);
            } else {
                typeToGen =
                    (JgFieldType) rndGenerate64Range(&jsonGenHandle->rndHandle,
                                                     (uint64_t)(JgArray - 1));
            }
        }

        emptyRatio = (unsigned) rndGenerate64Range(&jsonGenHandle->rndHandle,
                                                   EmptyRatioLimit);

        // print last level prefix
        if (prefix != NULL) {
            ret = fprintf(fp, "%s ", prefix);
            if (ret < -1) {
                status = StatusIO;
                goto CommonExit;
            }
        }

        // print current level prefix
        if (isArrayElement) {
            commonPrefix = (char *) commonArrayElePrefix;
        } else {
            commonPrefix = (char *) commonFieldPrefix;
        }
        assert(strlen(commonPrefix) < maxFieldLen);

        switch (jsonGenKeyDupType) {
        case DuplicatedKey:
            fieldIndex =
                rndGenerate64Range(&jsonGenHandle->rndHandle, maxField);
            break;
        case NoDuplicatedKeySameLevel:
            fieldIndex = ii;
            break;
        case NoDuplicatedKey:
            fieldIndex = jsonGenHandle->uniqueFieldIndex++;
        }

        switch (typeToGen) {
        case JgString:
        case JgUnsignedInteger:
        case JgSignedInteger:
        case JgFloat:
        case JgUnsignedInteger32:
        case JgSignedInteger32:
        case JgFloat32:
        case JgBool:
        case JgStringInt:
        case JgStringFloat:
        case JgPrimaryKey:
            ret = fprintf(fp,
                          "%s%lu %u %u\n",
                          commonPrefix,
                          fieldIndex,
                          typeToGen,
                          emptyRatio);
            if (ret < -1) {
                status = StatusIO;
                goto CommonExit;
            }

            break;
        case JgArray:
            isArray = true;
            if ((!allowMixPrimitiveFieldInArray)) {
                if ((rndGenerate64Range(&jsonGenHandle->rndHandle, 99) <
                     nestedRatio) &&
                    (nestedLevel != maxNestedLevel - 1)) {
                    nested = true;
                } else {
                    nested = false;
                }

                if (nested) {
                    arrayElePrimitiveType = (JgFieldType)(
                        rndGenerate64Range(&jsonGenHandle->rndHandle, 1) +
                        JgArray);
                } else {
                    arrayElePrimitiveType = (JgFieldType)
                        rndGenerate64Range(&jsonGenHandle->rndHandle,
                                           (uint64_t)(JgArray - 1));
                }
            }
            // fall through
        case JgObject:
            if (prefix != NULL) {
                ret = snprintf(newPrefix,
                               maxFieldLen,
                               "%s %s%lu",
                               prefix,
                               commonPrefix,
                               fieldIndex);
            } else {
                ret = snprintf(newPrefix,
                               maxFieldLen,
                               "%s%lu",
                               commonPrefix,
                               fieldIndex);
            }
            if (ret < 0 || ret >= (int) maxFieldLen) {
                status = StatusNoBufs;
                goto CommonExit;
            }

            ret = fprintf(fp,
                          "%s%lu %u %u",
                          commonPrefix,
                          fieldIndex,
                          typeToGen,
                          emptyRatio);
            if (ret < -1) {
                status = StatusIO;
                goto CommonExit;
            }

            status = generateJsonSchemaFile(fp,
                                            newPrefix,
                                            isArray,
                                            nestedLevel + 1,
                                            maxField,
                                            maxNestedLevel,
                                            jsonGenKeyDupType,
                                            allowMixPrimitiveFieldInArray,
                                            arrayElePrimitiveType,
                                            jsonGenHandle);
            if (status != StatusOk) {
                goto CommonExit;
            }
            break;

        default:
            fprintf(stderr, "Wrong Jsontype %d\n", typeToGen);
            assert(0);
        }
    }

CommonExit:
    if (newPrefix != NULL) {
        memFree(newPrefix);
    }
    return status;
}

static void
freeJsonEle(JsonEle *jsonEle)
{
    JsonEle *jsonEleTmp = NULL;
    if (jsonEle->fieldName != NULL) {
        memFree(jsonEle->fieldName);
    }

    while ((jsonEleTmp = removeHeadJsonEleList(&jsonEle->listHead)) != NULL) {
        freeJsonEle(jsonEleTmp);
    }
    assert(jsonEle->listHead == NULL);
    memFree(jsonEle);
}

static Status
generateSchema(FILE *fp,
               char *inputLine,
               JsonEle **jsonList,
               unsigned eleCount,
               bool isArray,
               int nestedLevel,
               uint64_t maxNestedLevel)
{
    JsonEle *jsonObj;
    ssize_t ret;
    char *token;
    bool isNestedArray;
    Status status = StatusOk;
    uint64_t maxFieldLen;
    uint64_t maxInputLineLen;
    char *saveptr = NULL;

    maxInputLineLen = maxNestedLevel * MaxSingleFieldLen + JsonSchemaContentLen;
    maxFieldLen = MaxSingleFieldLen * maxNestedLevel;
    for (unsigned ii = 0; ii < eleCount; ++ii) {
        // Init jsonObj
        jsonObj = NULL;
        jsonObj = (JsonEle *) memAllocExt(sizeof(JsonEle), moduleName);
        if (jsonObj == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        jsonObj->fieldName = NULL;
        jsonObj->listSize = 0;
        jsonObj->listHead = NULL;

        ret = getline(&inputLine, &maxInputLineLen, fp);
        if (ret < 0) {
            status = StatusIO;
            goto CommonExit;
        }

        token = strtok_r(inputLine, " ", &saveptr);
        if (token == NULL) {
            status = StatusInval;
            assert(0);
            goto CommonExit;
        }

        // skip fieldname
        for (int jj = 0; jj < nestedLevel; ++jj) {
            token = strtok_r(NULL, " ", &saveptr);
        }

        if (!isArray) {
            jsonObj->fieldName =
                (char *) memAllocExt(sizeof(char) * maxFieldLen, moduleName);
            if (jsonObj->fieldName == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            strlcpy(jsonObj->fieldName, token, maxFieldLen);
        }

        token = strtok_r(NULL, " ", &saveptr);
        if (token == NULL) {
            status = StatusInval;
            assert(0);
            goto CommonExit;
        }

        jsonObj->type = (JgFieldType) atoi(token);

        token = strtok_r(NULL, " ", &saveptr);
        if (token == NULL) {
            status = StatusInval;
            assert(0);
            goto CommonExit;
        }

        jsonObj->emptyRatio = atoi(token);

        if (jsonObj->type == JgArray || jsonObj->type == JgObject) {
            isNestedArray = (jsonObj->type == JgArray);
            token = strtok_r(NULL, " ", &saveptr);
            if (token == NULL) {
                status = StatusInval;
                assert(0);
                goto CommonExit;
            }

            jsonObj->listSize = atoi(token);
            if (jsonObj->listSize != 0) {
                jsonObj->listHead = NULL;
                // @SymbolCheckIgnore

                status = generateSchema(fp,
                                        inputLine,
                                        &jsonObj->listHead,
                                        jsonObj->listSize,
                                        isNestedArray,
                                        nestedLevel + 1,
                                        maxNestedLevel);
                if (status != StatusOk) {
                    goto CommonExit;
                }
            }
        }

        appendJsonEleList(jsonList, jsonObj);
    }
CommonExit:
    if (status != StatusOk) {
        if (jsonObj != NULL) {
            freeJsonEle(jsonObj);
        }

        JsonEle *jsonEleTmp = NULL;
        while ((jsonEleTmp = removeHeadJsonEleList(jsonList)) != NULL) {
            freeJsonEle(jsonEleTmp);
        }
        assert((*jsonList) == NULL);
    }
    return status;
}

static Status
buildSchema(FILE *schemaFp, uint64_t maxNestedLevel, JsonEle **topLevelList)
{
    char *inputLine;
    ssize_t ret;
    int topLevelFieldNum;
    Status status;
    int currentNestedLevel = 0;
    uint64_t maxInputLineLen;

    maxInputLineLen = maxNestedLevel * MaxSingleFieldLen + JsonSchemaContentLen;

    inputLine =
        (char *) memAllocExt(sizeof(char) * maxInputLineLen, moduleName);
    if (inputLine == NULL) {
        assert(0);
        status = StatusNoMem;
        goto CommonExit;
    }

    ret = fseek(schemaFp, 0, SEEK_SET);
    if (ret < 0) {
        status = StatusIO;
        goto CommonExit;
    }

    ret = getline(&inputLine, &maxInputLineLen, schemaFp);
    if (ret < 0) {
        status = StatusIO;
        goto CommonExit;
    }

    topLevelFieldNum = atoi(inputLine);

    status = generateSchema(schemaFp,
                            inputLine,
                            topLevelList,
                            topLevelFieldNum,
                            NotArray,
                            currentNestedLevel,
                            maxNestedLevel);

CommonExit:
    if (inputLine != NULL) {
        memFree(inputLine);
    }

    return status;
}

// Json Array or Json Object will not be generated, those two need to be handled
// separately
Status
generateRandJsonContent(RandHandle *rndHandle,
                        char *buffer,
                        size_t bufLenIn,
                        size_t *writtenLen,
                        JgFieldType type,
                        Atomic64 *primaryKeyIndexIn,
                        bool useDict)
{
    uint64_t strLen;
    char *jsonStr;
    int64_t primaryKeyIndex;
    Status status = StatusOk;
    int ret = -1;

    switch (type) {
    case JgString:
        if (useDict) {
            uint64_t idx;
            idx = rndGenerate64Range(rndHandle, wordCount - 1);

            ret = snprintf(buffer, bufLenIn, "\"%s\"", dictWordArray[idx]);
        } else {
            strLen = rndGenerate64Range(rndHandle, MaxJsonStringLen);
            if (strLen != 0) {
                jsonStr =
                    (char *) memAllocExt(sizeof(char) * strLen, moduleName);
                if (jsonStr == NULL) {
                    status = StatusNoMem;
                    goto CommonExit;
                }

                rndGeneratedString(jsonStr, strLen, rndHandle);
                ret = snprintf(buffer, bufLenIn, "\"%s\"", jsonStr);
                memFree(jsonStr);
            } else {
                ret = snprintf(buffer, bufLenIn, "\"\"");
            }
        }
        break;

    case JgUnsignedInteger:
        ret = snprintf(buffer,
                       bufLenIn,
                       "%lu",
                       rndGenerate64Range(rndHandle, LONG_MAX));

        break;

    case JgUnsignedInteger32:
        ret = snprintf(buffer,
                       bufLenIn,
                       "%u",
                       (uint32_t) rndGenerate64Range(rndHandle, LONG_MAX));

        break;

    case JgPrimaryKey:
        primaryKeyIndex = atomicInc64(primaryKeyIndexIn);
        ret = snprintf(buffer, bufLenIn, "%lu", primaryKeyIndex);
        break;

    case JgSignedInteger:
        ret = snprintf(buffer, bufLenIn, "%ld", rndGenerate64(rndHandle));
        break;

    case JgSignedInteger32:
        ret = snprintf(buffer,
                       bufLenIn,
                       "%d",
                       (int32_t) rndGenerate64(rndHandle));

        break;

    case JgStringInt:
        ret = snprintf(buffer, bufLenIn, "\"%ld\"", rndGenerate64(rndHandle));
        break;

    case JgFloat:
        ret =
            snprintf(buffer, bufLenIn, "%f", (double) rndGenerate64(rndHandle));
        break;

    case JgFloat32:
        ret =
            snprintf(buffer, bufLenIn, "%f", (float) rndGenerate64(rndHandle));

        break;

    case JgStringFloat:
        ret = snprintf(buffer,
                       bufLenIn,
                       "\"%f\"",
                       (double) rndGenerate64(rndHandle));
        break;

    case JgBool:
        if (rndGenerate64Range(rndHandle, 1)) {
            ret = snprintf(buffer, bufLenIn, "%s", trueString);
        } else {
            ret = snprintf(buffer, bufLenIn, "%s", falseString);
        }
        break;

    default:
        assert(0);
    }

    if (ret < 0 || (size_t) ret >= bufLenIn) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    *writtenLen = ret;

CommonExit:
    return status;
}

static Status
generateSingleRandJson(char *buffer,
                       size_t bufferSize,
                       JsonEle **list,
                       JsonGenHandle *jsonGenHandle,
                       size_t *outputSize,
                       bool isArray)
{
    JsonEle *jsonEle;
    bool firstEle = true;
    unsigned emptyRatio;
    int ret;
    size_t written;
    size_t copySize = 0;
    Status status = StatusOk;
    char leftBracket = isArray ? '[' : '{';
    char rightBracket = isArray ? ']' : '}';

    ret = snprintf(buffer + copySize, bufferSize - copySize, "%c", leftBracket);
    if ((size_t) ret >= bufferSize - copySize) {
        status = StatusIO;
        goto CommonExit;
    } else {
        copySize += ret;
    }

    jsonEle = *list;

    while (jsonEle != NULL) {
        emptyRatio = (unsigned) rndGenerate64Range(&jsonGenHandle->rndHandle,
                                                   EmptyRatioLimit);
        if (jsonEle->emptyRatio > emptyRatio) {
            continue;
        }

        if (firstEle) {
            firstEle = false;
        } else {
            // print previous comma
            ret = snprintf(buffer + copySize, bufferSize - copySize, ", ");
            if ((size_t) ret >= bufferSize - copySize) {
                status = StatusIO;
                goto CommonExit;
            } else {
                copySize += ret;
            }
        }

        if (!isArray) {
            ret = snprintf(buffer + copySize,
                           bufferSize - copySize,
                           "\"%s\": ",
                           jsonEle->fieldName);
            if ((size_t) ret >= bufferSize - copySize) {
                status = StatusIO;
                goto CommonExit;
            } else {
                copySize += ret;
            }
        }

        JsonEle **elementList = &jsonEle->listHead;

        switch (jsonEle->type) {
        case JgObject:
            status = generateSingleRandJson(buffer + copySize,
                                            bufferSize - copySize,
                                            elementList,
                                            jsonGenHandle,
                                            &written,
                                            NotArray);
            break;

        case JgArray:
            status = generateSingleRandJson(buffer + copySize,
                                            bufferSize - copySize,
                                            elementList,
                                            jsonGenHandle,
                                            &written,
                                            IsArray);

            break;

        default:
            status = generateRandJsonContent(&jsonGenHandle->rndHandle,
                                             buffer + copySize,
                                             bufferSize - copySize,
                                             &written,
                                             jsonEle->type,
                                             &jsonGenHandle->primaryKeyIndex,
                                             jsonGenHandle->useDict);
        }

        if (status != StatusOk) {
            goto CommonExit;
        }

        copySize += written;

        jsonEle = jsonEle->next;
        if (jsonEle == *list) {
            // reaches the end
            break;
        }
    }

    ret =
        snprintf(buffer + copySize, bufferSize - copySize, "%c", rightBracket);
    if (ret < 0 || (size_t) ret >= bufferSize - copySize) {
        status = StatusIO;
        goto CommonExit;
    } else {
        copySize += ret;
        assert(copySize <= bufferSize);
    }

CommonExit:

    *outputSize = copySize;
    return status;
}

Status
jsonGenGetRecord(char *buffer,
                 size_t bufferSize,
                 size_t *outputSize,
                 JsonGenHandle *handle)
{
    Status status = StatusUnknown;
    status = generateSingleRandJson(buffer,
                                    bufferSize,
                                    &handle->topLevelList,
                                    handle,
                                    outputSize,
                                    NotArray);
    return status;
}

Status
jsonGenGetRecords(char *buffer,
                  size_t bufferSize,
                  size_t *outputSize,
                  unsigned numRecords,
                  JsonGenHandle *handle)
{
    unsigned ii;
    size_t copySize = 0;
    size_t tmpCopySize = 0;
    int ret;
    Status status;
    const size_t TrailingJsonSize = 6;

    ret = snprintf(buffer, bufferSize, "[ ");
    if ((size_t) ret >= bufferSize) {
        return StatusIO;
    } else {
        copySize += ret;
    }

    for (ii = 0; ii < numRecords && bufferSize - copySize > TrailingJsonSize;
         ii++) {
        status = jsonGenGetRecord(&buffer[copySize],
                                  // allow space for closing the record
                                  bufferSize - copySize - TrailingJsonSize,
                                  &tmpCopySize,
                                  handle);
        if (status == StatusOk) {
            assert(buffer[copySize] == '{');
            assert(buffer[copySize + tmpCopySize - 1] == '}');

            copySize += tmpCopySize;

            ret = snprintf(&buffer[copySize], bufferSize - copySize, ", ");
            if ((size_t) ret >= bufferSize - copySize) {
                return StatusIO;
            } else {
                copySize += ret;
            }
        } else {
            status = StatusOk;  // just truncate
            break;
        }
    }

    ret = snprintf(&buffer[copySize], bufferSize - copySize, "{} ]");
    if ((size_t) ret >= bufferSize - copySize) {
        return StatusIO;
    } else {
        copySize += ret;
    }

    *outputSize = copySize;

    return StatusOk;
}

void
jsonGenDestroyHandle(JsonGenHandle *handle)
{
    JsonEle *jsonEleTmp = NULL;
    while ((jsonEleTmp = removeHeadJsonEleList(&handle->topLevelList)) !=
           NULL) {
        freeJsonEle(jsonEleTmp);
    }
    assert(handle->topLevelList == NULL);
    memFree(handle);
    handle = NULL;
}

static Status
loadDictFile(char *dictFilePath)
{
    FILE *fp;
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    Status status = StatusOk;
    int ii = 0;
    size_t ret;
    bool fileOpened = false;

    // @SymbolCheckIgnore
    fp = fopen(dictFilePath, "r");
    if (fp == NULL) {
        status = StatusNoEnt;
        goto CommonExit;
    }
    fileOpened = true;

    // Here we let the getline implementation allocate a memory buffer for us.
    // There is always a chance that this can happen anyway, so this creates
    // well defined behavior. A consequence of this is that getline is not
    // going to be using the memAlloc implementation, and instead uses the
    // stock malloc, which is untracked.
    // This type of invocation of getline doesn't need to initialize len
    while ((read = getline(&line, &len, fp)) != -1) {
        assert(len <= sizeof(DictWord));
        assert((ii + 1) < JsonGenMaxNumDictWord);
        ret = strlcpy(dictWordArray[ii++], line, sizeof(DictWord));
        assert(dictWordArray[ii - 1][ret - 1] == '\n');
        dictWordArray[ii - 1][ret - 1] = '\0';
    }
    wordCount = ii;

CommonExit:
    if (line != NULL) {
        // @SymbolCheckIgnore
        free(line);
    }

    if (fileOpened) {
        // @SymbolCheckIgnore
        fclose(fp);
    }

    return status;
}

static Status
createJsonGenHandle(JsonGenHandle **handle)
{
    Status status = StatusOk;
    JsonGenHandle *jsonGenHandle;
    struct timespec ts;
    int ret;

    jsonGenHandle =
        (JsonGenHandle *) memAllocExt(sizeof(*jsonGenHandle), moduleName);
    if (jsonGenHandle == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    jsonGenHandle->useDict = true;

    ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    if (ret != 0) {
        status = sysErrnoToStatus(ret);
        goto CommonExit;
    }

    rndInitHandle(&jsonGenHandle->rndHandle,
                  (uint32_t) clkTimespecToNanos(&ts));

    jsonGenHandle->topLevelList = NULL;

CommonExit:
    if (status != StatusOk) {
        if (jsonGenHandle != NULL) {
            memFree(jsonGenHandle);
        }
        jsonGenHandle = NULL;
    }

    *handle = jsonGenHandle;
    return status;
}

Status
jsonGenInitDict(char *path)
{
    Status status = StatusOk;
    if (dictInitCalled) {
        goto CommonExit;
    }
    // init dictionary
    dictWordArray =
        (DictWord *) memAllocExt(sizeof(DictWord) * JsonGenMaxNumDictWord,
                                 moduleName);
    if (dictWordArray == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    if (path == NULL) {
        path = (char *) defaultDictFilePath;
    }

    status = loadDictFile(path);
    if (status != StatusOk) {
        goto CommonExit;
    }

    dictInitCalled = true;
CommonExit:
    return status;
}

void
jsonGenDestroyDict()
{
    if (dictInitCalled) {
        memFree(dictWordArray);
        dictInitCalled = false;
    }
}

Status
jsonGenNewHandleWithSchema(char *fileName,
                           JsonGenHandle **handle,
                           int nodeId,
                           bool useDict)
{
    FILE *schemaFp = NULL;
    Status status = StatusUnknown;
    JsonGenHandle *jsonGenHandle = NULL;

    assert(fileName != NULL);

    if (useDict && !dictInitCalled) {
        status = StatusModuleNotInit;
        goto CommonExit;
    }

    status = createJsonGenHandle(&jsonGenHandle);
    if (status != StatusOk) {
        goto CommonExit;
    }

    jsonGenHandle->useDict = useDict;
    // @SymbolCheckIgnore
    schemaFp = fopen(fileName, "r");
    if (schemaFp == NULL) {
        status = StatusIO;
        goto CommonExit;
    }

    jsonGenHandle->uniqueFieldIndex = 0;
    atomicWrite64(&jsonGenHandle->primaryKeyIndex,
                  (((int64_t) nodeId) << XidMgr::XidLeftShift));
    status = buildSchema(schemaFp,
                         JsonGenMaxNestedLevel,
                         &jsonGenHandle->topLevelList);

CommonExit:
    if (schemaFp != NULL) {
        // @SymbolCheckIgnore
        fclose(schemaFp);
    }
    if (status != StatusOk) {
        if (jsonGenHandle != NULL) {
            jsonGenDestroyHandle(jsonGenHandle);
            jsonGenHandle = NULL;
        }
    }
    *handle = jsonGenHandle;
    return status;
}

Status
jsonGenNewHandleRand(char *fileName,
                     uint64_t maxField,
                     uint64_t maxNestedLevel,
                     JsonGenKeyDupType jsonGenKeyDupType,
                     bool allowMixPrimitiveFieldInArray,
                     JsonGenHandle **handle,
                     int nodeId,
                     bool useDict)
{
    FILE *schemaFp = NULL;
    char *fieldNamePrefix = NULL;
    int currentNestedLevel = 0;
    Status status = StatusUnknown;
    bool fileCreated = false;
    JsonGenHandle *jsonGenHandle = NULL;

    assert(fileName != NULL);
    assert(maxNestedLevel < JsonGenMaxNestedLevel);

    if (useDict && !dictInitCalled) {
        status = StatusModuleNotInit;
        goto CommonExit;
    }

    status = createJsonGenHandle(&jsonGenHandle);
    if (status != StatusOk) {
        goto CommonExit;
    }

    jsonGenHandle->useDict = useDict;
    // @SymbolCheckIgnore
    schemaFp = fopen(fileName, "w+");
    if (schemaFp == NULL) {
        status = StatusIO;
        goto CommonExit;
    }

    status = generateJsonSchemaFile(schemaFp,
                                    fieldNamePrefix,
                                    NotArray,
                                    currentNestedLevel,
                                    maxField,
                                    maxNestedLevel,
                                    jsonGenKeyDupType,
                                    allowMixPrimitiveFieldInArray,
                                    JgTypeUnknown,
                                    jsonGenHandle);
    if (status != StatusOk) {
        goto CommonExit;
    }
    fileCreated = true;

    jsonGenHandle->uniqueFieldIndex = 0;
    atomicWrite64(&jsonGenHandle->primaryKeyIndex,
                  ((int64_t) nodeId << XidMgr::XidLeftShift));
    status =
        buildSchema(schemaFp, maxNestedLevel, &jsonGenHandle->topLevelList);

CommonExit:
    if (schemaFp != NULL) {
        // @SymbolCheckIgnore
        fclose(schemaFp);
    }

    if (status != StatusOk) {
        if (fileCreated) {
            int ret = remove(fileName);
            assert(ret == 0);
        }

        if (jsonGenHandle != NULL) {
            jsonGenDestroyHandle(jsonGenHandle);
            jsonGenHandle = NULL;
        }
    }
    *handle = jsonGenHandle;

    return status;
}

Status
writeRecordToFile(JsonGenHandle *handle,
                  bool byRecord,
                  uint64_t numOfRecord,
                  bool bySize,
                  uint64_t fileSize,
                  uint64_t fileSizeThreshold,
                  char *outputFileDir)
{
    Status status;
    char *buffer = (char *) memAlloc(BufLen);
    assert(buffer);
    size_t outputSize;
    uint64_t curFileSize = 0;
    uint64_t recordIdx = 0;
    uint64_t totalFileSize = 0;
    int ret;
    bool newfile = false;
    uint64_t outputFileIdx = 0;
    char curOutputFile[MaxPathSize];
    FILE *outputFp = NULL;

    ret = snprintf(curOutputFile,
                   MaxPathSize,
                   "%s/%s%llu%s",
                   outputFileDir,
                   outputFileName,
                   (unsigned long long) outputFileIdx,
                   outputFileExtention);

    printf("open file %s\n", curOutputFile);

    if (ret < 0 || ret >= MaxPathSize) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    outputFp = fopen(curOutputFile, "w");
    if (outputFp == NULL) {
        status = StatusFailOpenFile;
        goto CommonExit;
    }

    ret = fprintf(outputFp, "[\n");
    if (ret < 0) {
        status = StatusIO;
        goto CommonExit;
    }

    while (true) {
        status = jsonGenGetRecord(buffer, BufLen, &outputSize, handle);
        if (status != StatusOk) {
            goto CommonExit;
        }

        if ((recordIdx != 0) && (!newfile)) {
            ret = fprintf(outputFp, ",\n");
            if (ret < 0) {
                status = StatusIO;
                goto CommonExit;
            }
        }

        if (newfile) {
            ret = fprintf(outputFp, "[\n");
            if (ret < 0) {
                status = StatusIO;
                goto CommonExit;
            }
            newfile = false;
        }

        ret = fprintf(outputFp, "%s", buffer);
        if (ret < 0) {
            status = StatusIO;
            goto CommonExit;
        }
        assert((size_t) ret == outputSize);

        totalFileSize += ret;
        curFileSize += ret;

        if (curFileSize > fileSizeThreshold) {
            ret = fprintf(outputFp, "\n]");
            if (ret < 0) {
                status = StatusIO;
                goto CommonExit;
            }

            curFileSize = 0;
            outputFileIdx++;
            // @SymbolCheckIgnore
            fclose(outputFp);
            outputFp = NULL;
            // truncate the output file
            ret = snprintf(curOutputFile,
                           MaxPathSize,
                           "%s%s%llu%s",
                           outputFileDir,
                           outputFileName,
                           (unsigned long long) outputFileIdx,
                           outputFileExtention);

            if (ret < 0 || ret >= MaxPathSize) {
                status = StatusNameTooLong;
                goto CommonExit;
            }

            // @SymbolCheckIgnore
            outputFp = fopen(curOutputFile, "w");
            if (outputFp == NULL) {
                status = StatusFailOpenFile;
                goto CommonExit;
            }
            newfile = true;
        }
        recordIdx++;
        if (byRecord && (recordIdx > numOfRecord)) {
            break;
        }

        if (bySize && (totalFileSize > fileSize)) {
            break;
        }
    }
    ret = fprintf(outputFp, "\n]");
    if (ret < 0) {
        status = StatusIO;
        goto CommonExit;
    }
    printf("total file size :%lu \n", totalFileSize);

CommonExit:
    if (status != StatusOk) {
        fprintf(stderr, "fail to write json record to file");
    }
    if (outputFp != NULL) {
        // @SymbolCheckIgnore
        fclose(outputFp);
    }
    memFree(buffer);

    return status;
}
