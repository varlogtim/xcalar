// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>
#include <jansson.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "primitives/Primitives.h"
#include "runtime/Spinlock.h"
#include "util/MemTrack.h"
#include "util/System.h"
#include "util/SaveTrace.h"
#include "util/IntHashTable.h"
#include "strings/String.h"
#include "common/Version.h"
#include "sys/XLog.h"

typedef uint32_t ctxIdx_t;
#define MAX_CTX (((ctxIdx_t) ~(ctxIdx_t) 0))

constexpr size_t XdbRefTraceSize = 10;
constexpr ssize_t MaxDbgRefCount = 4;
constexpr ssize_t MaxDbgRefCtxCount = 256;

static constexpr const char *ModuleName = "SaveTrace";

static bool EnabledOnStartup = false;

struct PtrToCtxItem {
    IntHashTableHook hook;

    void *getKey() const { return keyPtr; }

    static uint64_t hashKey(void *k) { return (uint64_t) k; }

    void del() { delete this; }

    void *keyPtr;
    ctxIdx_t val;
};

typedef IntHashTable<void *,
                     PtrToCtxItem,
                     &PtrToCtxItem::hook,
                     &PtrToCtxItem::getKey,
                     2049,
                     PtrToCtxItem::hashKey>
    PtrToCtxMap;

struct PtrToCtxArrItem {
    IntHashTableHook hook;

    void *getKey() const { return keyPtr; }

    static uint64_t hashKey(void *k) { return (uint64_t) k; }

    void del() { delete this; }

    void *keyPtr;
    ctxIdx_t incs[MaxDbgRefCtxCount];
    ctxIdx_t decs[MaxDbgRefCtxCount];
};

typedef IntHashTable<void *,
                     PtrToCtxArrItem,
                     &PtrToCtxArrItem::hook,
                     &PtrToCtxArrItem::getKey,
                     2049,
                     PtrToCtxArrItem::hashKey>
    PtrToCtxArrMap;

struct Trace {
    void *trace[XdbRefTraceSize];
    size_t count;  // Number of occurances of trace
    int traceSize;
};

struct Traces {
    // Circular window of the last MaxDbgRefCount inc/dec (or alloc/free) traces
    Trace lastRefs[MaxDbgRefCount];
    // Used to track dump histogram of inc/dec (or alloc/free) contexts
    Trace refCtx[MaxDbgRefCtxCount];
    size_t lastRefsIdx;
    size_t numRefCtxOverflow;     // Overflows regarding MaxDbgRefCtxCount
    size_t numRefCtxElmOverflow;  // Overflows regarding sizeof(ctxIdx_t)
};

struct GlobalTraces {
    Traces tracesInc;     // RefCount increment
    Traces tracesAllocs;  // XDB page alloc
    Traces tracesDec;     // RefCount decrement
    Traces tracesFrees;   // XDB page free
    PtrToCtxMap *tracesMemInFlight;
    PtrToCtxArrMap *tracesRefInFlight;
    // number of items that aren't inserted into tracesMemInFlight due to error
    size_t errorCount;
};

GlobalTraces *globalTraces_ = NULL;

// TODO: Make finer grained by moving into Traces
// TODO: Construction init needs to be ordered.
Spinlock dbgXdbTraceLock;

// XXX: Due to jansson vagaries on the error path these all intentionally leak
// the json ref on error
json_t *
dumpTrace(Trace *trace)
{
    Status status = StatusOk;
    json_t *root = json_object();

    if (root == NULL) {
        return NULL;
    }
    json_t *arrObj = json_array();
    if (arrObj == NULL) {
        return NULL;
    }

    int ret = json_object_set_new(root, "count", json_integer(trace->count));
    if (ret) {
        return NULL;
    }
    ret =
        json_object_set_new(root, "traceSize", json_integer(trace->traceSize));
    if (ret) {
        return NULL;
    }
    ret = json_object_set_new(root, "trace", arrObj);
    if (ret) {
        return NULL;
    }

    for (size_t ii = 0; ii < XdbRefTraceSize; ii++) {
        ret = json_array_append_new(arrObj,
                                    json_integer((size_t) trace->trace[ii]));
        if (ret) {
            return NULL;
        }
    }

    return root;
}

json_t *
dumpTraces(Traces *traces)
{
    json_t *root = json_object();
    if (root == NULL) {
        return NULL;
    }
    json_t *arrObj;

    arrObj = json_array();
    if (arrObj == NULL) {
        return NULL;
    }
    int ret = json_object_set_new(root, "lastRefs", arrObj);
    if (ret) {
        return NULL;
    }
    for (size_t ii = 0; ii < MaxDbgRefCount; ii++) {
        json_t *traceObj = dumpTrace(&traces->lastRefs[ii]);
        if (traceObj == NULL) {
            return NULL;
        }
        ret = json_array_append_new(arrObj, traceObj);
        if (ret) {
            return NULL;
        }
    }

    arrObj = json_array();
    if (arrObj == NULL) {
        return NULL;
    }
    ret = json_object_set_new(root, "refCtx", arrObj);
    if (ret) {
        return NULL;
    }
    for (size_t ii = 0; ii < MaxDbgRefCtxCount; ii++) {
        json_t *traceObj = dumpTrace(&traces->refCtx[ii]);
        if (traceObj == NULL) {
            return NULL;
        }
        ret = json_array_append_new(arrObj, traceObj);
        if (ret) {
            return NULL;
        }
    }

    ret = json_object_set_new(root,
                              "lastRefsIdx",
                              json_integer(traces->lastRefsIdx));
    if (ret) {
        return NULL;
    }

    ret = json_object_set_new(root,
                              "numRefCtxOverflow",
                              json_integer(traces->numRefCtxOverflow));
    if (ret) {
        return NULL;
    }
    ret = json_object_set_new(root,
                              "numRefCtxElmOverflow",
                              json_integer(traces->numRefCtxElmOverflow));
    if (ret) {
        return NULL;
    }

    return root;
}

json_t *
dumpMemInFlight()
{
    int ret;
    PtrToCtxItem *item = NULL;
    json_t *arrObj = json_array();
    if (arrObj == NULL) {
        return NULL;
    }
    // These would require much less heap using csv vs json
    for (PtrToCtxMap::iterator it = globalTraces_->tracesMemInFlight->begin();
         (item = it.get()) != NULL;
         it.next()) {
        json_t *hashEnt = json_object();
        if (hashEnt == NULL) {
            return NULL;
        }
        ret = json_object_set_new(hashEnt,
                                  "keyPtr",
                                  json_integer((size_t) item->keyPtr));
        if (ret) {
            return NULL;
        }
        ret = json_object_set_new(hashEnt, "val", json_integer(item->val));
        if (ret) {
            return NULL;
        }
        ret = json_array_append_new(arrObj, hashEnt);
        if (ret) {
            return NULL;
        }
    }

    return arrObj;
}

json_t *
dumpRefInFlight()
{
    int ret;
    PtrToCtxArrItem *item = NULL;
    json_t *arrObj = json_array();
    if (arrObj == NULL) {
        return NULL;
    }
    for (PtrToCtxArrMap::iterator it =
             globalTraces_->tracesRefInFlight->begin();
         (item = it.get()) != NULL;
         it.next()) {
        json_t *hashEnt = json_object();
        ret = json_object_set_new(hashEnt,
                                  "keyPtr",
                                  json_integer((size_t) item->keyPtr));
        if (ret) {
            return NULL;
        }

        json_t *incsArrObj;
        json_t *decsArrObj;
        incsArrObj = json_array();
        if (incsArrObj == NULL) {
            return NULL;
        }
        decsArrObj = json_array();
        if (decsArrObj == NULL) {
            return NULL;
        }
        for (size_t ii = 0; ii < MaxDbgRefCtxCount; ii++) {
            ret =
                json_array_append_new(incsArrObj, json_integer(item->incs[ii]));
            if (ret) {
                return NULL;
            }
            ret =
                json_array_append_new(decsArrObj, json_integer(item->decs[ii]));
            if (ret) {
                return NULL;
            }
        }

        ret = json_object_set_new(hashEnt, "incs", incsArrObj);
        if (ret) {
            return NULL;
        }
        ret = json_object_set_new(hashEnt, "decs", decsArrObj);
        if (ret) {
            return NULL;
        }
        ret = json_array_append_new(arrObj, hashEnt);
        if (ret) {
            return NULL;
        }
    }

    return arrObj;
}

Status
writeJsonFile(json_t *root, const char *fname)
{
    Status status = StatusOk;
    FILE *fh = NULL;
    char fqpn[XcalarApiMaxFileNameLen + 1];
    time_t secs = time(NULL);
    static size_t fileNum = 0;
    const char *xlrRoot = XcalarConfig::get()->xcalarRootCompletePath_;
    char *outStr = json_dumps(root, 0);
    char oomTracesDirPath[XcalarApiMaxFileNameLen + 1];
    int ret;
    bool traceDirExists = false;

    BailIfNull(outStr);

    status = strSnprintf(oomTracesDirPath,
                         sizeof(oomTracesDirPath),
                         "%s/%s",
                         xlrRoot,
                         oomTracesDirName);
    if (status == StatusOk) {
        ret = mkdir(oomTracesDirPath, S_IRWXU);
        if (ret == -1 && errno != EEXIST) {
            // If dir can't be created, that's ok; just dump the file in rootdir
            // better to have this file somewhere, than skip dumping it; but log
            // the error; it it's an EEXIST error, the dir already exists, no
            // issue
            xSyslog(ModuleName,
                    XlogErr,
                    "Cannot create '%s' directory in Xcalar root: %s",
                    oomTracesDirPath,
                    strGetFromStatus(sysErrnoToStatus(errno)));
        } else {
            traceDirExists = true;
        }
    } else {
        xSyslog(ModuleName,
                XlogErr,
                "Cannot create path in Xcalar Root for '%s': %s",
                oomTracesDirName,
                strGetFromStatus(status));
    }

    if (traceDirExists) {
        status = strSnprintf(fqpn,
                             sizeof(fqpn),
                             "%s/%s-%03u-%05lu-%ld.json",
                             oomTracesDirPath,
                             fname,
                             Config::get()->getMyNodeId(),
                             fileNum++,
                             secs);
        BailIfFailed(status);
    } else {
        status = strSnprintf(fqpn,
                             sizeof(fqpn),
                             "%s/%s-%03u-%05lu-%ld.json",
                             xlrRoot,
                             fname,
                             Config::get()->getMyNodeId(),
                             fileNum++,
                             secs);
        BailIfFailed(status);
    }

    xSyslog(ModuleName,
            XlogInfo,
            "Dump trace logs '%s'",
            fqpn);

    fh = fopen(fqpn, "w");
    BailIfNull(fh);
    fprintf(fh, "%s", outStr);

CommonExit:
    if (outStr) {
        free(outStr);
        outStr = NULL;
    }
    if (fh) {
        fclose(fh);
        fh = NULL;
    }

    return status;
}

Status
dumpAllTraces(const char *fname)
{
    json_t *root = json_object();
    json_t *arrObj;
    json_t *tracesObj;
    Status status;

    dbgXdbTraceLock.lock();
    if (!globalTraces_) {
        dbgXdbTraceLock.unlock();
        return StatusInval;
    }

    int ret = json_object_set_new(root,
                                  "MaxDbgRefCount",
                                  json_integer(MaxDbgRefCount));
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    ret = json_object_set_new(root,
                              "MaxDbgRefCtxCount",
                              json_integer(MaxDbgRefCtxCount));
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    ret =
        json_object_set_new(root,
                            "XcalarConfig::instance->ctxTracesMode_",
                            json_integer(XcalarConfig::get()->ctxTracesMode_));
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    ret = json_object_set_new(root,
                              "EnabledOnStartup",
                              json_boolean(EnabledOnStartup));
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    ret = json_object_set_new(root,
                              "fullVersionString",
                              json_string(versionGetFullStr()));
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    ret = json_object_set_new(root,
                              "ErrorCount",
                              json_integer(globalTraces_->errorCount));
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }

    tracesObj = dumpTraces(&globalTraces_->tracesInc);
    BailIfNull(tracesObj);
    ret = json_object_set_new(root, "tracesInc", tracesObj);
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    tracesObj = dumpTraces(&globalTraces_->tracesAllocs);
    BailIfNull(tracesObj);
    ret = json_object_set_new(root, "tracesAllocs", tracesObj);
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    tracesObj = dumpTraces(&globalTraces_->tracesDec);
    BailIfNull(tracesObj);
    ret = json_object_set_new(root, "tracesDec", tracesObj);
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }
    tracesObj = dumpTraces(&globalTraces_->tracesFrees);
    BailIfNull(tracesObj);
    ret = json_object_set_new(root, "tracesFrees", tracesObj);
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }

    arrObj = dumpMemInFlight();
    BailIfNull(arrObj);
    ret = json_object_set_new(root, "tracesMemInFlight", arrObj);
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }

    arrObj = dumpRefInFlight();
    ret = json_object_set_new(root, "tracesRefInFlight", arrObj);
    if (ret) {
        status = StatusInval;
        goto CommonExit;
    }

    status = writeJsonFile(root, fname);
    BailIfFailed(status);

    // Do not dec on error paths due to jansson bugs
    json_decref(root);

CommonExit:
    dbgXdbTraceLock.unlock();
    return status;
}

Status
saveTraceInit(bool isStartup)
{
    Status status = StatusOk;
    int32_t mode = XcalarConfig::get()->ctxTracesMode_;

    assert(!globalTraces_);
    EnabledOnStartup = isStartup;

    if (mode) {
        globalTraces_ = (GlobalTraces *) memAlloc(sizeof(*globalTraces_));
        BailIfNull(globalTraces_);
        memZero(globalTraces_, sizeof(*globalTraces_));

        // Always initialize this to support dynamic enabling of in-flights
        // without restart
        globalTraces_->tracesMemInFlight =
            (PtrToCtxMap *) memAlloc(sizeof(PtrToCtxMap));
        BailIfNull(globalTraces_->tracesMemInFlight);
        globalTraces_->tracesMemInFlight =
            new (globalTraces_->tracesMemInFlight) PtrToCtxMap();

        globalTraces_->tracesRefInFlight =
            (PtrToCtxArrMap *) memAlloc(sizeof(PtrToCtxArrMap));
        BailIfNull(globalTraces_->tracesRefInFlight);
        globalTraces_->tracesRefInFlight =
            new (globalTraces_->tracesRefInFlight) PtrToCtxArrMap();
        globalTraces_->errorCount = 0;
    }

CommonExit:
    if (status.ok()) {
        xSyslog(ModuleName,
                XlogWarn,
                "Initialized Xcalar memory tracking %s startup",
                isStartup ? "during" : "after");
    } else {
        if (globalTraces_) {
            if (globalTraces_->tracesRefInFlight) {
                memFree(globalTraces_->tracesRefInFlight);
                globalTraces_->tracesRefInFlight = NULL;
            }

            if (globalTraces_->tracesMemInFlight) {
                memFree(globalTraces_->tracesMemInFlight);
                globalTraces_->tracesMemInFlight = NULL;
            }

            memFree(globalTraces_);
            globalTraces_ = NULL;
        }
        xSyslog(ModuleName,
                XlogWarn,
                "Failed to initialize Xcalar memory tracking (%s)",
                strGetFromStatus(status));
    }

    return StatusOk;
}

void
saveTraceDestroy()
{
    if (globalTraces_) {
        if (globalTraces_->tracesRefInFlight) {
            globalTraces_->tracesRefInFlight->removeAll(&PtrToCtxArrItem::del);
            globalTraces_->tracesRefInFlight->~PtrToCtxArrMap();
            memFree(globalTraces_->tracesRefInFlight);
            globalTraces_->tracesRefInFlight = NULL;
        }

        if (globalTraces_->tracesMemInFlight) {
            globalTraces_->tracesMemInFlight->removeAll(&PtrToCtxItem::del);
            globalTraces_->tracesMemInFlight->~PtrToCtxMap();
            memFree(globalTraces_->tracesMemInFlight);
            globalTraces_->tracesMemInFlight = NULL;
        }

        memFree(globalTraces_);
        globalTraces_ = NULL;
    }
}

static void
saveInflightCtx(
    TraceOpts opts, void *buf, int32_t slab, ctxIdx_t ctxNum, size_t count)
{
    PtrToCtxItem *addrItem = NULL;
    PtrToCtxArrItem *addrArrItem = NULL;
    int32_t mode = XcalarConfig::get()->ctxTracesMode_;

    switch (opts) {
    case TraceOpts::MemInc:
        if (!(mode & (int32_t) XcalarConfig::CtxTraces::BufCacheInFlight)) {
            return;
        }
        assert(count == 1);
        // TODO: Preallocate these on startup
        addrItem = (PtrToCtxItem *) memAlloc(sizeof(*addrItem));
        if (addrItem == NULL) {
            globalTraces_->errorCount++;
            return;
        }
        addrItem->keyPtr = buf;
        addrItem->val = ctxNum;
        globalTraces_->tracesMemInFlight->insert(addrItem);
        break;
    case TraceOpts::MemDec:
        if (!(mode & (int32_t) XcalarConfig::CtxTraces::BufCacheInFlight)) {
            return;
        }
        assert(count == 1);
        addrItem = globalTraces_->tracesMemInFlight->remove(buf);
        if (!addrItem && globalTraces_->errorCount) {
            return;
        }
        assert(addrItem);
        memFree(addrItem);
        break;
    case TraceOpts::RefInc:
        if (!(mode & (int32_t) XcalarConfig::CtxTraces::RefsInFlight)) {
            return;
        }
        assert(count == 1);
        addrArrItem = globalTraces_->tracesRefInFlight->find(buf);
        if (!addrArrItem) {
            // First ref count increment so add entry
            addrArrItem = (PtrToCtxArrItem *) memAlloc(sizeof(*addrArrItem));
            if (addrArrItem == NULL) {
                globalTraces_->errorCount++;
                return;
            }
            memZero(addrArrItem, sizeof(*addrArrItem));
            addrArrItem->keyPtr = buf;
            globalTraces_->tracesRefInFlight->insert(addrArrItem);
        }
        if (ctxNum >= MaxDbgRefCtxCount) {
            // This case is tracked by numRefCtxOverflow
            return;
        }
        assert(ctxNum < MaxDbgRefCtxCount);
        if (addrArrItem->incs[ctxNum] == MAX_CTX) {
            globalTraces_->tracesInc.numRefCtxElmOverflow++;
        } else {
            addrArrItem->incs[ctxNum]++;
        }
        break;
    case TraceOpts::RefDec:
        if (!(mode & (int32_t) XcalarConfig::CtxTraces::RefsInFlight)) {
            return;
        }
        assert(count == 1);
        addrArrItem = globalTraces_->tracesRefInFlight->find(buf);
        if (!addrArrItem && globalTraces_->errorCount) {
            return;
        }
        assert(addrArrItem);
        if (ctxNum >= MaxDbgRefCtxCount) {
            // This case is tracked by numRefCtxOverflow
            return;
        }
        assert(ctxNum < MaxDbgRefCtxCount);
        if (addrArrItem->decs[ctxNum] == MAX_CTX) {
            globalTraces_->tracesDec.numRefCtxElmOverflow++;
        } else {
            addrArrItem->decs[ctxNum]++;
        }
        break;
    case TraceOpts::RefDel:
        if (!(mode & (int32_t) XcalarConfig::CtxTraces::RefsInFlight)) {
            return;
        }
        // Refcounts are only used to track if an item is eligible for
        // eviction, not to free an item.  So when we do finally free a page
        // (and refcount need not be zero to do so) we need to explicitly
        // remove it from the table.
        addrArrItem = globalTraces_->tracesRefInFlight->remove(buf);
        if (!addrArrItem && globalTraces_->errorCount) {
            return;
        }
        assert(addrArrItem);
#ifdef DEBUG
        {
            size_t totalIncs = 0;
            size_t totalDecs = 0;
            for (size_t ctx = 0; ctx < MaxDbgRefCtxCount; ctx++) {
                totalIncs += addrArrItem->incs[ctx];
                totalDecs += addrArrItem->decs[ctx];
            }

            assert(totalIncs >= totalDecs);
            // Hitting this means something aside from the refcounting
            // functions manipulated the header's refcount, or there was a
            // double init of the page.
            assert(totalIncs == totalDecs + count);
        }
#endif

        memFree(addrArrItem);
        break;
    default:
        assert(false && "Invalid XDB debug trace option");
        return;
    }
}

void
saveTraceHelper(TraceOpts opts, void *buf, size_t count, int32_t slab)
{
    size_t refCtxIdx;  // Index into lastRefs trace bins
    bool found;
    Traces *traces = NULL;

    Trace currTrace;

    dbgXdbTraceLock.lock();
    // Allow enable via config without requiring restart
    if (unlikely(!globalTraces_)) {
        Status status = saveTraceInit(false);
        if (!status.ok()) {
            return;
        }
    }

    switch (opts) {
    case TraceOpts::MemInc:
        traces = &globalTraces_->tracesAllocs;
        break;
    case TraceOpts::MemDec:
        traces = &globalTraces_->tracesFrees;
        break;
    case TraceOpts::RefInc:
        traces = &globalTraces_->tracesInc;
        break;
    case TraceOpts::RefDec:
        traces = &globalTraces_->tracesDec;
        break;
    case TraceOpts::RefDel:
        traces = &globalTraces_->tracesDec;
        break;
    default:
        assert(false && "Invalid XDB debug trace option");
        dbgXdbTraceLock.unlock();
        return;
    }

    currTrace.traceSize = getBacktrace(currTrace.trace, XdbRefTraceSize);

    if (MaxDbgRefCount > 0) {
        size_t lastRefsIdx;  // Circular index into lastRefs trace history
        lastRefsIdx = traces->lastRefsIdx;
        memcpy(traces->lastRefs[lastRefsIdx].trace,
               currTrace.trace,
               sizeof(traces->lastRefs[lastRefsIdx].trace));
        traces->lastRefs[lastRefsIdx].traceSize = currTrace.traceSize;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdivision-by-zero"
        traces->lastRefsIdx = (traces->lastRefsIdx + 1) % MaxDbgRefCount;
#pragma GCC diagnostic pop
    }

    for (refCtxIdx = 0; refCtxIdx < MaxDbgRefCtxCount; refCtxIdx++) {
        if (traces->refCtx[refCtxIdx].traceSize == 0) {
            break;  // No more contexts to search
        }

        found = true;
        if (currTrace.traceSize == traces->refCtx[refCtxIdx].traceSize) {
            for (size_t frame = 0; frame < (size_t)currTrace.traceSize; frame++) {
                if (currTrace.trace[frame] !=
                    traces->refCtx[refCtxIdx].trace[frame]) {
                    found = false;
                    break;
                }
            }

            if (found) {
                traces->refCtx[refCtxIdx].count += count;
                saveInflightCtx(opts, buf, slab, refCtxIdx, count);
                dbgXdbTraceLock.unlock();
                return;
            }
        }
    }

    if (refCtxIdx >= MaxDbgRefCtxCount) {
        // Increase MaxDbgRefCtxCount and rebuild
        traces->numRefCtxOverflow++;
        // We can still track the entry even with a context overflow
        saveInflightCtx(opts, buf, slab, refCtxIdx, count);
        dbgXdbTraceLock.unlock();
        return;
    }

    assert(refCtxIdx < MaxDbgRefCtxCount);
    assert(traces->refCtx[refCtxIdx].traceSize == 0);
    assert(traces->refCtx[refCtxIdx].count == 0);

    // Didn't find this context in above loop, so add it in the next free slot
    memcpy(traces->refCtx[refCtxIdx].trace,
           currTrace.trace,
           sizeof(traces->refCtx[refCtxIdx].trace));
    traces->refCtx[refCtxIdx].traceSize = currTrace.traceSize;
    traces->refCtx[refCtxIdx].count += count;
    saveInflightCtx(opts, buf, slab, refCtxIdx, count);
    dbgXdbTraceLock.unlock();
}
