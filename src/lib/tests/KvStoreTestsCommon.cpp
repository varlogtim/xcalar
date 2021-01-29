// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/MemTrack.h"
#include "kvstore/KvStore.h"
#include "KvStoreTestsCommon.h"
#include "msg/Xid.h"

uint64_t randomLoopIterations = RandomLoopIterationsDefault;

// This allows selectively disabling assert() verifications.
//
// The sanity test does straight forward operations that are deterministic and
// verifiable and thus assert checking is done.
//
// The FuncTest creates many threads all "spewing" operations at the
// library non-deterministically.  Because the interactions of the operations
// occur in a random fashion the only "pass" is to not crash.
//
// Note: All files, except one, are opened as KvStoreSession to keep the size
//       small and expand if needed.
//       KvStoreGlobal creates much larger log files.
static void
myAssert(bool condition, bool doVerify)
{
    if (!doVerify) {
        // In the future we might want to do some checking
        return;
    }
    assert(condition);
}

Status
kvStoreTests(const char *kvStoreName, bool doVerify)
{
    Status status;
    size_t valueBufSize = 0;
    char *valueBuf = NULL;
    char *kvKey;
    char *kvValue;
    KvStoreLib *kvs = KvStoreLib::get();
    char storeName[XcalarApiMaxPathLen];
    // Get unique, unused IDs.
    Xid kvs_id0 = XidMgr::get()->xidGetNext();
    Xid kvs_id1 = XidMgr::get()->xidGetNext();
    Xid kvs_id2 = XidMgr::get()->xidGetNext();
    // Multikeys update
    char **keys = NULL;
    char **values = NULL;
    size_t *keysSize = NULL;
    size_t *valuesSize = NULL;
    int numEnts = 0;

    // Open KvStore 0 and make sure it's empty.
    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    // Insert something. Make sure it can be looked up.
    kvKey = (char *) "foo";
    kvValue = (char *) "bar";
    status = kvs->addOrReplace(kvs_id0,
                               kvKey,
                               strlen(kvKey) + 1,
                               kvValue,
                               strlen(kvValue) + 1,
                               NonPersist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);
    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "bar") == 0, doVerify);
    myAssert(strlen("bar") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // Open KvStore 1. Make sure there's no cross-contamination.
    snprintf(storeName, XcalarApiMaxPathLen, "anotherKvStore%s", kvStoreName);
    status = kvs->open(kvs_id1, storeName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id1, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    kvValue = (char *) "cross-contamination";
    status = kvs->addOrReplace(kvs_id1,
                               kvKey,
                               strlen(kvKey) + 1,
                               kvValue,
                               strlen(kvValue) + 1,
                               NonPersist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id1, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "cross-contamination") == 0, doVerify);
    myAssert(strlen("cross-contamination") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "bar") == 0, doVerify);
    myAssert(strlen("bar") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    kvs->close(kvs_id1, KvStoreCloseDeletePersisted);

    status = kvs->append(kvs_id0, "apple", "banana", KvStoreOptNone);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    status = kvs->append(kvs_id0, "foo", "baz", KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "barbaz") == 0, doVerify);
    myAssert(strlen("barbaz") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // If "apple" has the value "banana" set "apple" to the value "orange" and
    // also set "x" to the value "y"
    status = kvs->setIfEqual(kvs_id0,
                             1,
                             "apple",
                             "banana",
                             "orange",
                             "x",
                             "y",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    status = kvs->setIfEqual(kvs_id0,
                             1,
                             "foo",
                             "bar",
                             "orange",
                             "x",
                             "y",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusKvEntryNotEqual, doVerify);

    status = kvs->setIfEqual(kvs_id0,
                             1,
                             "foo",
                             "barbaz",
                             "orange",
                             "x",
                             "z",
                             NonPersist,
                             KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "orange") == 0, doVerify);
    myAssert(strlen("orange") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    status = kvs->lookup(kvs_id0, "x", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "z") == 0, doVerify);
    myAssert(strlen("z") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // If key "x" has value "z", replace value of "x" with "w"
    status = kvs->setIfEqual(kvs_id0,
                             0,
                             "x",
                             "z",
                             "w",
                             "ignored",
                             "ignored",
                             NonPersist,
                             KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);

    // Ensure the above replacement happened
    status = kvs->lookup(kvs_id0, "x", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "w") == 0, doVerify);
    myAssert(strlen("w") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // ...and that the secondary pair was ignored
    status = kvs->lookup(kvs_id0, "ignored", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    kvs->close(kvs_id0, KvStoreCloseOnly);

    // Ensure we're prevented from lookup on closed store
    status = kvs->lookup(kvs_id0, "x", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvStoreNotFound, doVerify);

    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);
    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    // we chose not to persist earlier
    myAssert(status == StatusKvEntryNotFound, doVerify);

    // use an = in the value since we use that as a delimiter when persisting
    kvValue = (char *) "bar = junk";
    status = kvs->addOrReplace(kvs_id0,
                               kvKey,
                               strlen(kvKey) + 1,
                               kvValue,
                               strlen(kvValue) + 1,
                               Persist,
                               KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);
    kvs->close(kvs_id0, KvStoreCloseOnly);

    // Verify persistence of foo was updated.
    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);
    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "bar = junk") == 0, doVerify);
    myAssert(strlen("bar = junk") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // Verify multiAddOrReplace
    numEnts = 2;
    keys = (char **) memAlloc(numEnts * sizeof(*keys));
    BailIfNull(keys);
    keysSize = (size_t *) memAlloc(numEnts * sizeof(*keysSize));
    BailIfNull(keysSize);
    values = (char **) memAlloc(numEnts * sizeof(*values));
    BailIfNull(values);
    valuesSize = (size_t *) memAlloc(numEnts * sizeof(*valuesSize));
    BailIfNull(valuesSize);
    // Verify persistence of multiAddOrReplace
    keys[0] = (char *) "foo_multi";
    keysSize[0] = strlen(keys[0]) + 1;
    keys[1] = (char *) "bar_multi";
    keysSize[1] = strlen(keys[1]) + 1;
    values[0] = (char *) "foo_values_multi";
    valuesSize[0] = strlen(values[0]) + 1;
    values[1] = (char *) "bar_values_multi";
    valuesSize[1] = strlen(values[1]) + 1;
    status = kvs->multiAddOrReplace(kvs_id0,
                                    (const char **) keys,
                                    (const char **) values,
                                    (const size_t *) keysSize,
                                    (const size_t *) valuesSize,
                                    true,
                                    KvStoreOptSync,
                                    numEnts);
    myAssert(status == StatusOk, doVerify);
    // verify they are updated in persist way
    kvs->close(kvs_id0, KvStoreCloseOnly);
    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo_multi", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "foo_values_multi") == 0, doVerify);
    myAssert(strlen("foo_values_multi") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;
    status = kvs->lookup(kvs_id0, "bar_multi", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "bar_values_multi") == 0, doVerify);
    myAssert(strlen("bar_values_multi") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;
    // verify non-persist of multiAddOrReplace
    keys[0] = (char *) "foo_multi_non_persist";
    keysSize[0] = strlen(keys[0]) + 1;
    keys[1] = (char *) "bar_multi_non_persist";
    keysSize[1] = strlen(keys[1]) + 1;
    values[0] = (char *) "foo_values_multi_non_persist";
    valuesSize[0] = strlen(values[0]) + 1;
    values[1] = (char *) "bar_values_multi_non_persist";
    valuesSize[1] = strlen(values[1]) + 1;
    status = kvs->multiAddOrReplace(kvs_id0,
                                    (const char **) keys,
                                    (const char **) values,
                                    (const size_t *) keysSize,
                                    (const size_t *) valuesSize,
                                    NonPersist,
                                    KvStoreOptSync,
                                    numEnts);
    myAssert(status == StatusOk, doVerify);
    // verify they are updated first
    status =
        kvs->lookup(kvs_id0, "foo_multi_non_persist", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "foo_values_multi_non_persist") == 0, doVerify);
    myAssert(strlen("foo_values_multi_non_persist") == valueBufSize - 1,
             doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;
    status =
        kvs->lookup(kvs_id0, "bar_multi_non_persist", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "bar_values_multi_non_persist") == 0, doVerify);
    myAssert(strlen("bar_values_multi_non_persist") == valueBufSize - 1,
             doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;
    // verify the non-persistance
    kvs->close(kvs_id0, KvStoreCloseOnly);
    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);
    status =
        kvs->lookup(kvs_id0, "foo_multi_non_persist", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);
    status =
        kvs->lookup(kvs_id0, "bar_multi_non_persist", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);
    // Cleanup
    memFree(keys);
    keys = NULL;
    memFree(keysSize);
    keysSize = NULL;
    memFree(values);
    values = NULL;
    memFree(valuesSize);
    valuesSize = NULL;
    numEnts = 0;

    // Verify persist of append.
    status = kvs->append(kvs_id0, "foo", "...", KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    kvs->close(kvs_id0, KvStoreCloseOnly);

    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "bar = junk...") == 0, doVerify);
    myAssert(strlen("bar = junk...") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // Verify SetIfEqual persist.
    kvKey = (char *) "foo2";
    kvValue = (char *) "bar";
    status = kvs->addOrReplace(kvs_id0,
                               kvKey,
                               strlen(kvKey) + 1,
                               kvValue,
                               strlen(kvValue) + 1,
                               NonPersist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    // Since key "foo2" has the value "bar" it will be replaced with
    // "grapefruit" and, secondary key "a" will be assigned value "b"
    status = kvs->setIfEqual(kvs_id0,
                             1,
                             "foo2",
                             "bar",
                             "grapefruit",
                             "a",
                             "b",
                             Persist,
                             KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);

    kvs->close(kvs_id0, KvStoreCloseOnly);

    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo2", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "grapefruit") == 0, doVerify);
    myAssert(strlen("grapefruit") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // Verify the secondary assignment done by setIfEqual persisted
    status = kvs->lookup(kvs_id0, "a", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "b") == 0, doVerify);
    myAssert(strlen("b") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // Because "1" has no value, this should take effect.
    status = kvs->setIfEqual(kvs_id0,
                             1,
                             "1",
                             "",
                             "2",
                             "11",
                             "12",
                             NonPersist,
                             KvStoreOptNone);

    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "1", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "2") == 0, doVerify);
    myAssert(strlen("2") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    status = kvs->lookup(kvs_id0, "11", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "12") == 0, doVerify);
    myAssert(strlen("12") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    // Because "1" already has a value, this should have no effect.
    status = kvs->setIfEqual(kvs_id0,
                             1,
                             "1",
                             "",
                             "3",
                             "11",
                             "13",
                             NonPersist,
                             KvStoreOptNone);
    myAssert(status == StatusKvEntryNotEqual, doVerify);

    status = kvs->lookup(kvs_id0, "1", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "2") == 0, doVerify);
    myAssert(strlen("2") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    status = kvs->lookup(kvs_id0, "11", &valueBuf, &valueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(strcmp(valueBuf, "12") == 0, doVerify);
    myAssert(strlen("12") == valueBufSize - 1, doVerify);
    memFree(valueBuf);
    valueBuf = NULL;
    valueBufSize = 0;

    status = kvs->del(kvs_id0, "junk", KvStoreOptNone);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    status = kvs->del(kvs_id0, "foo", KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);

    status = kvs->del(kvs_id0, "foo2", KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    kvs->close(kvs_id0, KvStoreCloseOnly);

    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    kvs->close(kvs_id0, KvStoreCloseDeletePersisted);

    snprintf(storeName, XcalarApiMaxPathLen, "KvStoreToDelete%s", kvStoreName);
    status = kvs->open(kvs_id1, storeName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    kvKey = (char *) "foo";
    kvValue = (char *) "bar";
    status = kvs->addOrReplace(kvs_id1,
                               kvKey,
                               strlen(kvKey) + 1,
                               kvValue,
                               strlen(kvValue) + 1,
                               Persist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    kvs->close(kvs_id1, KvStoreCloseDeletePersisted);

    // Ensure store content was deleted
    status = kvs->open(kvs_id1, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id1, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    kvs->close(kvs_id1, KvStoreCloseDeletePersisted);

    // Open KvStore 2 using the global attribute and make sure it's empty.
    // The rest of the operations should be insensitive to size, so we will
    // just close and delete the files when done.
    status = kvs->open(kvs_id2, kvStoreName, KvStoreGlobal);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id2, "foo", &valueBuf, &valueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    kvs->close(kvs_id2, KvStoreCloseDeletePersisted);

    status = StatusOk;

CommonExit:
    if (valueBuf != NULL) {
        memFree(valueBuf);
        valueBuf = NULL;
    }
    if (keys != NULL) {
        memFree(keys);
        keys = NULL;
    }
    if (keysSize != NULL) {
        memFree(keys);
        keys = NULL;
    }
    if (values != NULL) {
        memFree(keys);
        values = NULL;
    }
    if (valuesSize != NULL) {
        memFree(keys);
        valuesSize = NULL;
    }
    return status;
}

Status
kvStoreBigMessageTest(const char *kvStoreName, bool doVerify)
{
    Status status;
    const size_t ValueBufSize = 63 * MB;
    KvStoreLib *kvs = KvStoreLib::get();

    char *valueBuf = (char *) memAlloc(ValueBufSize);
    if (valueBuf == NULL) {
        return StatusNoMem;
    }
    char *retValueBuf = NULL;
    size_t retValueBufSize = 0;

    // Get unique, unused IDs.
    Xid kvs_id0 = XidMgr::get()->xidGetNext();

    for (unsigned ii = 0; ii < ValueBufSize - 1; ii++) {
        valueBuf[ii] = 'b';
    }
    valueBuf[ValueBufSize - 1] = '\0';

    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->addOrReplace(kvs_id0,
                               "foo",
                               4,
                               valueBuf,
                               ValueBufSize,
                               Persist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    kvs->close(kvs_id0, KvStoreCloseOnly);

    // validate we can re-read what we just persisted
    status = kvs->open(kvs_id0, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    status = kvs->lookup(kvs_id0, "foo", &retValueBuf, &retValueBufSize);
    myAssert(status == StatusOk, doVerify);
    myAssert(retValueBufSize == ValueBufSize, doVerify);
    myAssert((memcmp(retValueBuf, valueBuf, retValueBufSize) == 0), doVerify);

    status = kvs->del(kvs_id0, "foo", KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);
    kvs->close(kvs_id0, KvStoreCloseDeletePersisted);

    memFree(valueBuf);
    memFree(retValueBuf);

    return StatusOk;
}

Status
kvStoreBadUserTest(const char *kvStoreName, bool doVerify)
{
    Status status;
    static const size_t ValueBufSize = 4 * KB;
    char valueBuf[ValueBufSize];
    KvStoreLib *kvs = KvStoreLib::get();
    // Get unique, unused IDs.
    Xid kvs_id = XidMgr::get()->xidGetNext();
    char *retValueBuf = NULL;
    size_t retValueBufSize = 0;

    // This is a malicious user doing bad things...

    // Open the kvstore
    status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);
    // Open it again
    status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
    myAssert(status == StatusExist, doVerify);
    // Open a different store using the same ID
    snprintf(valueBuf, ValueBufSize, "%s-%s", kvStoreName, "partDeux");
    status = kvs->open(kvs_id, valueBuf, KvStoreSession);
    myAssert(status == StatusExist, doVerify);

    // Close the kvstore (for real)
    kvs->close(kvs_id, KvStoreCloseDeletePersisted);
    // Close already closed kvstore (no errors are returned)
    kvs->close(kvs_id, KvStoreCloseOnly);
    kvs->close(kvs_id, KvStoreCloseDeletePersisted);
    // Close bogus ID.  Don't want to close random IDs as they may be legitimate
    // users.
    kvs->close((Xid) -1, KvStoreCloseDeletePersisted);
    kvs->close((Xid) -1, KvStoreCloseOnly);
    // Open the kvstore
    status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);
    // Close but keep it around
    kvs->close(kvs_id, KvStoreCloseOnly);
    // Open it again and close/delete
    status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);
    kvs->close(kvs_id, KvStoreCloseDeletePersisted);

    // del - all bogus
    status = kvs->del(kvs_id, "NotHome", KvStoreOptNone);
    myAssert(status == StatusKvStoreNotFound, doVerify);
    status = kvs->del(kvs_id, "OutSide", KvStoreOptSync);
    myAssert(status == StatusKvStoreNotFound, doVerify);
    status = kvs->del((Xid) -1, "Vacation", KvStoreOptNone);
    myAssert(status == StatusKvStoreNotFound, doVerify);
    status = kvs->del((Xid) -1, "Rental", KvStoreOptSync);
    myAssert(status == StatusKvStoreNotFound, doVerify);

    // legitimate open
    status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
    myAssert(status == StatusOk, doVerify);

    // Bogus ops on open kvstore

    status = kvs->del(kvs_id, "", KvStoreOptSync);
    myAssert(status == StatusKvEntryNotFound, doVerify);
    status = kvs->lookup((Xid) -1, "Ace", &retValueBuf, &retValueBufSize);
    myAssert(status == StatusKvStoreNotFound, doVerify);
    status = kvs->lookup(kvs_id, "King", &retValueBuf, &retValueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);
    status = kvs->lookup(kvs_id, "Queen", &retValueBuf, &retValueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);
    status = kvs->lookup(kvs_id, "Jack", &retValueBuf, &retValueBufSize);
    myAssert(status == StatusKvEntryNotFound, doVerify);

    // Add something legit
    status = kvs->addOrReplace(kvs_id,
                               "Royal",
                               6,
                               "Flush",
                               6,
                               Persist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    // Replace with same thing
    status = kvs->addOrReplace(kvs_id,
                               "Royal",
                               6,
                               "Flush",
                               6,
                               NonPersist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);
    status = kvs->addOrReplace(kvs_id,
                               "Royal",
                               6,
                               "Palace",
                               7,
                               NonPersist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    // Append
    status = kvs->append(kvs_id, "Royal", "Straight", KvStoreOptNone);
    myAssert(status == StatusOk, doVerify);
    status = kvs->append((Xid) -1, "Foo", "Bar", KvStoreOptNone);
    myAssert(status == StatusKvStoreNotFound, doVerify);
    for (int i = 0; i < 1000; i++) {
        snprintf(valueBuf, ValueBufSize, "%s%d", kvStoreName, i);
        status = kvs->append(kvs_id, "Royal", valueBuf, KvStoreOptNone);
        myAssert(status == StatusOk, doVerify);
    }

    // setIfEqual
    status = kvs->setIfEqual((Xid) -1,
                             1,
                             "Toby",
                             "Jack",
                             "Sierra",
                             "Chester",
                             "Dogs",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusKvStoreNotFound, doVerify);
    status = kvs->setIfEqual(kvs_id,
                             1,
                             "Toby",
                             "Jack",
                             "Sierra",
                             "Chester",
                             "Dogs",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusKvEntryNotFound, doVerify);
    status = kvs->setIfEqual(kvs_id,
                             10,
                             "Toby",
                             "Jack",
                             "Sierra",
                             "Chester",
                             "Dogs",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusInval, doVerify);
    // Add a legit entry
    status = kvs->addOrReplace(kvs_id,
                               "Trout",
                               6,
                               "Fishing",
                               8,
                               NonPersist,
                               KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);
    // If "Trout" has the value "Food" change "Trout" to the value "Drinks",
    status = kvs->setIfEqual(kvs_id,
                             1,
                             "Trout",
                             "Food",
                             "Drinks",
                             "",
                             "",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusKvEntryNotEqual, doVerify);
    status = kvs->setIfEqual(kvs_id,
                             1,
                             "Trout",
                             "Fishing",
                             "Drinks",
                             "",
                             "",
                             NonPersist,
                             KvStoreOptSync);
    myAssert(status == StatusOk, doVerify);

    // Clean up the file
    kvs->close(kvs_id, KvStoreCloseDeletePersisted);

    return StatusOk;
}

Status
kvStoreRandomTest(const char *kvStoreName, bool doVerify)
{
    // This needs to be set to the number of possible operations in the
    // switch statement.
    const unsigned PossibleOps = 20;
    Status status;
    KvStoreLib *kvs = KvStoreLib::get();
    // Get unique, unused IDs.
    Xid kvs_id = XidMgr::get()->xidGetNext();
    // Help when debugging an issue.  Keep track of the last random ops
    static const size_t RandomOpsLogBufSize = 4 * KB;
    unsigned randomOps[RandomOpsLogBufSize];
    char *retValueBuf = NULL;
    size_t retValueBufSize = 0;
    char *kvKey = (char *) "TestParameter";
    char *kvValue = (char *) "TestValue";

    // Randomly do operations to the specified kvstore.  As the ops are random
    // there's no expectation that it'll be successful.
    // The "open" operation has more chances of being hit so that the other
    // ops have a better chance of being tried on an open kvstore
    for (unsigned ii = 0; ii < randomLoopIterations; ii++) {
        unsigned op = random() % PossibleOps;
        // Save the op for debugging help
        randomOps[ii % RandomOpsLogBufSize] = op;
        switch (op) {
        case 0 ... 4:
            status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
            break;
        case 5:
            kvs->close(kvs_id, KvStoreCloseOnly);
            break;
        case 6:
            kvs->close(kvs_id, KvStoreCloseDeletePersisted);
            break;
        case 7:
            status = kvs->del(kvs_id, "TestParameter", KvStoreOptNone);
            break;
        case 8:
            status = kvs->del(kvs_id, "TestParameter", KvStoreOptSync);
            break;
        case 9:
            status = kvs->lookup(kvs_id,
                                 "TestParameter",
                                 &retValueBuf,
                                 &retValueBufSize);
            if (status == StatusOk) {
                memFree(retValueBuf);
                retValueBuf = NULL;
                retValueBufSize = 0;
            }
            break;
        case 10:
            status = kvs->addOrReplace(kvs_id,
                                       kvKey,
                                       strlen(kvKey) + 1,
                                       kvValue,
                                       strlen(kvValue) + 1,
                                       Persist,
                                       KvStoreOptSync);
            break;
        case 11:
            status = kvs->addOrReplace(kvs_id,
                                       kvKey,
                                       strlen(kvKey) + 1,
                                       kvValue,
                                       strlen(kvValue) + 1,
                                       NonPersist,
                                       KvStoreOptSync);
            break;
        case 12:
            status = kvs->addOrReplace(kvs_id,
                                       kvKey,
                                       strlen(kvKey) + 1,
                                       kvValue,
                                       strlen(kvValue) + 1,
                                       Persist,
                                       KvStoreOptNone);
            break;
        case 13:
            status = kvs->addOrReplace(kvs_id,
                                       kvKey,
                                       strlen(kvKey) + 1,
                                       kvValue,
                                       strlen(kvValue) + 1,
                                       NonPersist,
                                       KvStoreOptNone);
            break;
        case 14:
            status = kvs->append(kvs_id,
                                 "TestParameter",
                                 "AppendValule",
                                 KvStoreOptNone);
            break;
        case 15:
            status = kvs->append(kvs_id,
                                 "TestParameter",
                                 "AppendValule",
                                 KvStoreOptSync);
            break;
        case 16:
            status = kvs->setIfEqual(kvs_id,
                                     1,
                                     "TestParameter",
                                     "TestValue",
                                     "ReplaceValue",
                                     "",
                                     "",
                                     NonPersist,
                                     KvStoreOptSync);
            break;
        case 17:
            status = kvs->setIfEqual(kvs_id,
                                     1,
                                     "TestParameter",
                                     "TestValue",
                                     "ReplaceValue",
                                     "",
                                     "",
                                     Persist,
                                     KvStoreOptSync);
            break;
        case 18:
            status = kvs->setIfEqual(kvs_id,
                                     1,
                                     "TestParameter",
                                     "TestValue",
                                     "ReplaceValue",
                                     "",
                                     "",
                                     NonPersist,
                                     KvStoreOptNone);
            break;
        case 19:
            status = kvs->setIfEqual(kvs_id,
                                     1,
                                     "TestParameter",
                                     "TestValue",
                                     "ReplaceValue",
                                     "",
                                     "",
                                     Persist,
                                     KvStoreOptNone);
            break;
        default:
            // PossibleOps is too big
            assert(0);
            break;
        }
    }

    // Clean up the file if it's still around.  It has to be open in order
    // to do a close/delete on it.
    status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
    if (status == StatusOk || status == StatusExist) {
        kvs->close(kvs_id, KvStoreCloseDeletePersisted);
    }

    // Avoid compiler warning about unused variable
    (void) randomOps;

    status = StatusOk;

    return status;
}

// Wrapper functions so the sanity and func tests can call the same
// routines via different infrastructures.
Status
kvStoreTestsSanity()
{
    return kvStoreTests(commonKvStoreName, DoVerification);
}

Status
kvStoreBigMessageTestSanity()
{
    return kvStoreBigMessageTest(commonKvStoreName, DoVerification);
}

Status
kvStoreBadUserTestSanity()
{
    return kvStoreBadUserTest(commonKvStoreName, DoVerification);
}

Status
kvStoreRandomTestSanity()
{
    return kvStoreRandomTest(commonKvStoreName, DoVerification);
}
