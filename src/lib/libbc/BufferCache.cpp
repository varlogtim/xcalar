// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "bc/BufCacheMemMgr.h"
#include "bc/BufCacheObjectMgr.h"
#include "bc/BufferCache.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "callout/Callout.h"
#include "dataset/Dataset.h"
#include "operators/XcalarEval.h"
#include "constants/XcalarConfig.h"
#include "transport/TransportPage.h"
#include "xdb/Xdb.h"
#include "queryeval/QueryEvaluate.h"
#include "optimizer/Optimizer.h"
#include "querymanager/QueryManager.h"
#include "runtime/Runtime.h"
#include "localmsg/LocalMsg.h"
#include "localmsg/LocalConnection.h"

static constexpr const char *moduleName = "libbc";
constexpr BufferCacheObjects BufferCacheMgr::bcUsrNode[];
constexpr BufferCacheObjects BufferCacheMgr::bcChildNode[];
constexpr BufferCacheObjects BufferCacheMgr::bcXccli[];

BufferCacheMgr *BufferCacheMgr::instance = NULL;

class BcScanCleanout : public Schedulable
{
  public:
    BcScanCleanout() : Schedulable("BcScanCleanout") {}
    virtual ~BcScanCleanout() {}

    virtual void run();
    virtual void done();

    Xid txnId_;
    BcHandle *bc_;
};

BufferCacheMgr *
BufferCacheMgr::get()
{
    return instance;
}

Status
BufferCacheMgr::init(Type type, uint64_t bufCacheSize)
{
    Status status = StatusOk;

    if (bufCacheSize == InvalidSize) {
        status = StatusInval;
        goto CommonExit;
    }

    instance =
        (BufferCacheMgr *) memAllocExt(sizeof(BufferCacheMgr), moduleName);
    if (instance == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::init failed, type %u bufCacheSize %lu: %s",
                type,
                bufCacheSize,
                strGetFromStatus(status));
        goto CommonExit;
    }
    instance = new (instance) BufferCacheMgr();

    instance->type_ = type;

    switch (instance->type_) {
    case TypeUsrnode:
        // Only Buffer Cache type usrnode uses the partition system.
        instance->objectCount_ = ArrayLen(bcUsrNode);
        instance->bufferCacheObjectIds_ = bcUsrNode;
        break;

    case TypeChildnode:
        instance->objectCount_ = ArrayLen(bcChildNode);
        instance->bufferCacheObjectIds_ = bcChildNode;
        break;

    case TypeXccli:
        instance->objectCount_ = ArrayLen(bcXccli);
        instance->bufferCacheObjectIds_ = bcXccli;
        break;

    case TypeNone:
        // XXX This can be removed once all consumers of Buffer Cache have
        // a object table for their type.
        instance->objectCount_ = ArrayLen(bcUsrNode);
        instance->bufferCacheObjectIds_ = bcUsrNode;
        break;

    default:
        assert(0);
        break;  // Never reached.
    }

    instance->bcHandle_ =
        (BcHandle **) memAllocExt(sizeof(BcHandle *) * instance->objectCount_,
                                  moduleName);
    if (instance->bcHandle_ == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::init failed, type %u bufCacheSize %lu: %s",
                type,
                bufCacheSize,
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(instance->bcHandle_, sizeof(BcHandle *) * instance->objectCount_);

    status = BufCacheMemMgr::init(type, bufCacheSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::init failed, type %u bufCacheSize %lu: %s",
                type,
                bufCacheSize,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = instance->initObjects(instance->objectCount_,
                                   instance->bufferCacheObjectIds_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::init failed, type %u bufCacheSize %lu: %s",
                type,
                bufCacheSize,
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance->init_ = true;

CommonExit:
    if (status != StatusOk && instance != NULL) {
        instance->destroy();
    }

    return status;
}

void
BufferCacheMgr::destroy()
{
    this->destroyObjects();

    if (BufCacheMemMgr::get()) {
        BufCacheMemMgr::get()->destroy();
    }

    if (bcHandle_ != NULL) {
        memFree(bcHandle_);
        bcHandle_ = NULL;
    }

    instance->~BufferCacheMgr();
    memFree(instance);
    instance = NULL;
}

BufferCacheMgr::Type
BufferCacheMgr::getType()
{
    return type_;
}

Status
BufferCacheMgr::initObject(BufferCacheObjects objectId)
{
    assert((uint32_t) objectId < (uint32_t) BufferCacheObjects::ObjectMax);
    BcObject *object = this->getObject(objectId);
    XcalarConfig *xcalarConfig = XcalarConfig::get();
    Status status = StatusOk;
    unsigned numNodes = Config::get()->getActiveNodes();

    switch (objectId) {
    case BufferCacheObjects::XcalarApis:
        status = object->setUp("xcalarApis.bc",
                               xcalarConfig->maxOutstandApisWork_,
                               1,
                               getBcXcalarApisBufSize(),
                               BcObject::InitFastAllocsOnly,
                               objectId);
        break;

    case BufferCacheObjects::CalloutDispatch:
        status = object->setUp("callout.dispatch.bc",
                               CalloutQueue::BcCalloutDispatchNumElems,
                               1,
                               CalloutQueue::BcCalloutDispatchBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::TransPageSource:
        status =
            object->setUp("transPage.source.bc",
                          TransportPageMgr::transportSourcePagesCount(numNodes),
                          1,
                          TransportPageMgr::transportPageSize(),
                          (BcObject::Init)(
                              (uint32_t) BcObject::InitFastAllocsOnly |
                              (uint32_t) BcObject::InitDontCoreDump),
                          objectId);
        break;

    case BufferCacheObjects::TransPageSerializationBuf:
        status = object->setUp("transPage.serializationBuffers.bc",
                               TransportPageMgr::getMaxSerializationBuffers(),
                               1,
                               TransportPageMgr::getSerializationBufferSize(),
                               (BcObject::Init)(
                                   (uint32_t) BcObject::InitSlowAllocsOk |
                                   (uint32_t) BcObject::InitDontCoreDump),
                               objectId);
        break;

    case BufferCacheObjects::UkMsgNormal:
        // Limits the number of outstanding messages in the system and
        // hence has to be only fast allocs (axiomatic).
        status = object->setUp("uk.msg.normal.bc",
                               MsgMgr::getNormalMsgCount(numNodes),
                               1,
                               MsgMgr::bcMsgMessageBufSize(),
                               BcObject::InitFastAllocsOnly,
                               objectId);
        break;

    case BufferCacheObjects::UkMsgAlt:
        // Limits the number of outstanding messages in the system and
        // hence has to be only fast allocs (axiomatic).
        status = object->setUp("uk.msg.alt.bc",
                               MsgMgr::getAltMsgCount(numNodes),
                               1,
                               MsgMgr::bcMsgMessageBufSize(),
                               BcObject::InitFastAllocsOnly,
                               objectId);
        break;

    case BufferCacheObjects::UkMsgAtomic64:
        // Used to manage twoPcEphemeralInitAsync and hence can allow
        // slow allocs.
        status = object->setUp("uk.msg.atomic64.bc",
                               MsgMgr::getUkMsgAtomic64(numNodes),
                               1,
                               sizeof(Atomic64),
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::UkMsgTwoPcPages:
        // Limits the number of outstanding messages in the system and
        // hence has to be only fast allocs (axiomatic).
        status = object->setUp("uk.msg.twoPcPages.bc",
                               MsgMgr::getTwoPcPages(numNodes),
                               1,
                               XdbMgr::bcSize(),
                               BcObject::InitFastAllocsOnly,
                               objectId);
        break;

    case BufferCacheObjects::UkSchedObject:
        // XXX allow slow allocs until we implement flow control for uK
        status = object->setUp("uk.sched.object.bc",
                               MsgMgr::getSchedQSize(),
                               1,
                               MsgMgr::getUkSchedObjectBufSize(),
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::Dht:
        status = object->setUp("dht.bc",
                               DhtMgr::BcDhtNumElems,
                               1,
                               DhtMgr::BcDhtBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::QueryEvalRunningElems:
        status = object->setUp("queryEvalRunning.bc",
                               QueryEvaluate::RunningBcNumElems,
                               1,
                               QueryEvaluate::RunningBcBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::QueryEvalRunnableElems:
        status = object->setUp("queryEvalRunnable.bc",
                               QueryEvaluate::RunnableBcNumElems,
                               1,
                               QueryEvaluate::RunnableBcBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::QueryEvalCompletedElems:
        status = object->setUp("queryEvalCompleted.bc",
                               QueryEvaluate::CompletedBcNumElems,
                               1,
                               QueryEvaluate::CompletedBcBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::OptimizerQueryAnnotations:
        status = object->setUp("optimizerQueryAnnotations.bc",
                               Optimizer::AnnotationsBcNumElems,
                               1,
                               Optimizer::AnnotationsBcBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::OptimizerQueryFieldsElt:
        status = object->setUp("optimizerQueryFieldsElt.bc",
                               Optimizer::FieldsEltBcNumElems,
                               1,
                               Optimizer::FieldsEltBcBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::ArchiveManifest:
        status = object->setUp("archiveManifest.bc",
                               getBcArchiveManifestNumElems(),
                               1,
                               BcArchiveManifestBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::ArchiveFile:
        status = object->setUp("archiveFile.bc",
                               getBcArchiveFileNumElems(),
                               1,
                               BcArchiveFileBufSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::LocalMsgSmallMsg:
        status = object->setUp("localmsg.bc.smallmsg",
                               LocalMsg::BcSmallMsgCount,
                               1,
                               LocalMsg::BcSmallMsgSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::LocalMsgRequestHandler:
        status = object->setUp("localmsg.bc.requestHandler",
                               LocalMsg::BcRequestHandlerCount,
                               1,
                               LocalMsg::BcRequestHandlerSize,
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::LocalMsgResponse:
        status = object->setUp("localmsg.bc.response",
                               LocalConnection::BcResponseCount,
                               1,
                               sizeof(LocalConnection::Response),
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::XdbLocal:
        status = object->setUp("xdb.local.bc",
                               XdbMgr::MaxXdbs,
                               1,
                               XdbMgr::getBcXdbLocalBufSize(),
                               BcObject::InitSlowAllocsOk,
                               objectId);
        break;

    case BufferCacheObjects::XdbPage: {
        uint64_t numElements = 0;

        if (BufCacheMemMgr::get()->getType() ==
            BufCacheMemMgr::TypeContigAlloc) {
            uintptr_t startAddr =
                (uintptr_t) BufCacheMemMgr::get()->getStartAddr();

            // XdbMgr::bcSize() > sizeof(XdbPage) so we
            // round up to the larger size for alignment
            uintptr_t curAddr =
                roundUp(startAddr + BufCacheMemMgr::get()->getNextOffset(),
                        XdbMgr::bcSize());

            // XdbPage slab configuration can be any of the below.
            // 1. XdbPage slab is mlocked (lazily or just completely during
            // boot). With this config, XdbPageOsPageable slab will have 0
            // elements.
            //
            // 2. XdbPage slab is not mlocked at all, since the entire
            // BufferCache is not mlocked. We just set up BufferCache with
            // BufferCachePercentOfMemory > 100. In this case, XdbPageOsPageable
            // slab will have 0 elements, since the entire BufferCache is OS
            // pageable.
            //
            // 3. BufferCache is partially mlocked, so getMlockedSize() API
            // returns the Bytes mlocked in the BufferCache. It is possible that
            // Xdbpage slab ends up with 0 elements. Then XdbPageOsPageable slab
            // will serve the XdbPages for Tables and Datasets.
            //
            uint64_t size = BufCacheMemMgr::get()->getMlockedSize();
            if (!size) {
                size = BufCacheMemMgr::get()->getSize();
            }

            uintptr_t endAddr = roundDown(startAddr + size,
                                          XdbMgr::bcSize());  // BufSize aligned

            // Subtract one element to account for worst case
            // aligment of the sum of the sizes.
            if (endAddr > curAddr) {
                numElements = ((endAddr - curAddr) / XdbMgr::bcSize());
                if (numElements > 0) {
                    numElements -= 1;
                }
            }
        } else {
            assert(BufCacheMemMgr::get()->getType() ==
                   BufCacheMemMgr::TypeMalloc);
            // XXX For now, just choose a default for XdbPage count.
            numElements = 128 * KB;
        }
#ifdef XLR_VALGRIND
        numElements /= 3;
#endif
        status = object->setUp("xdb.pagekvbuf.bc",
                               numElements,
                               XdbMgr::bcSize(),
                               XdbMgr::bcSize(),
                               (BcObject::Init)(
                                   (uint32_t) BcObject::InitFastAllocsOnly |
                                   (uint32_t) BcObject::InitDontCoreDump),
                               objectId);
    } break;

    case BufferCacheObjects::XdbPageOsPageable: {
        uint64_t numElements = 0;

        if (xcalarConfig->bufCacheMlock_ &&
            xcalarConfig->bufferCachePercentOfTotalMem_ > 100) {
            if (BufCacheMemMgr::get()->getType() ==
                BufCacheMemMgr::TypeContigAlloc) {
                uintptr_t startAddr =
                    (uintptr_t) BufCacheMemMgr::get()->getStartAddr();

                // It is quite possible that XdbPageOsPageable slab has 0
                // elements.

                // XdbMgr::bcSize() > sizeof(XdbPage) so we
                // round up to the larger size for alignment
                uintptr_t curAddr =
                    roundUp(startAddr + BufCacheMemMgr::get()->getNextOffset(),
                            XdbMgr::bcSize());

                // Figure out XdbPage count based on the buffer cache size
                uintptr_t endAddr =
                    roundDown(startAddr + BufCacheMemMgr::get()->getSize(),
                              XdbMgr::bcSize());  // BufSize aligned

                // Subtract one element to account for worst case
                // aligment of the sum of the sizes.
                if (endAddr > curAddr) {
                    numElements = ((endAddr - curAddr) / XdbMgr::bcSize());
                    if (numElements > 0) {
                        numElements -= 1;
                    }
                }
            } else {
                assert(BufCacheMemMgr::get()->getType() ==
                       BufCacheMemMgr::TypeMalloc);
                // XXX For now, just choose a default for XdbPage count.
                numElements = 128 * KB;
            }
        }

#ifdef XLR_VALGRIND
        numElements /= 3;
#endif

        status = object->setUp("xdb.pagekvbufPageable.bc",
                               numElements,
                               XdbMgr::bcSize(),
                               XdbMgr::bcSize(),
                               (BcObject::Init)(
                                   (uint32_t) BcObject::InitFastAllocsOnly |
                                   (uint32_t) BcObject::InitDontCoreDump),
                               objectId);
    } break;

    default:
        assert(0 && "Invalid Buffer Cache object");
        status = StatusInval;
        break;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::initObject failed, Object Id %u: %s",
                objectId,
                strGetFromStatus(status));
    }

    return status;
}

size_t
BufferCacheMgr::getObjectIdx(BufferCacheObjects objectId)
{
    size_t ii;
    for (ii = 0; ii < objectCount_; ii++) {
        if (bufferCacheObjectIds_[ii] == objectId) {
            return ii;
        }
    }
    return ii;
}

void
BufferCacheMgr::addHandle(BcHandle *handle, BufferCacheObjects objectId)
{
    assert(this->getObjectIdx(objectId) < objectCount_);
    assert(bcHandle_[this->getObjectIdx(objectId)] == NULL);
    bcHandle_[this->getObjectIdx(objectId)] = handle;
}

void
BufferCacheMgr::removeHandle(BufferCacheObjects objectId)
{
    assert(this->getObjectIdx(objectId) < objectCount_);
    assert(bcHandle_[this->getObjectIdx(objectId)] != NULL);
    bcHandle_[this->getObjectIdx(objectId)] = NULL;
}

uint64_t
BufferCacheMgr::getOffsetFromAddr(void *addr)
{
    BufCacheMemMgr *bcMemMgr = BufCacheMemMgr::get();

    if ((uintptr_t) addr >= (uintptr_t) bcMemMgr->getStartAddr() &&
        (uintptr_t) addr <
            (uintptr_t) bcMemMgr->getStartAddr() + bcMemMgr->getSize()) {
        return (uintptr_t) addr - (uintptr_t) bcMemMgr->getStartAddr();
    } else {
        return InvalidAddr;
    }
}

size_t
BufferCacheMgr::getBufCachePct()
{
    XcalarConfig *xc = XcalarConfig::get();

    if (xc->bufCacheDbgMemLimitBytes_ == 0) {
        return xc->bufferCachePercentOfTotalMem_;
    } else {
        size_t pct = 100 * xc->bufCacheDbgMemLimitBytes_ /
                     XcSysHelper::get()->getPhysicalMemorySizeInBytes();

        xSyslog(moduleName,
                XlogInfo,
                "Overriding BufCache setting to %lu",
                pct);
        return pct;
    }
}

// numLocalNodes: Number of instances of usrnode processes on this host
// (physical hardware).
// return Buffer cache memory size in Bytes (PageSize aligned).
uint64_t
BufferCacheMgr::computeSize(unsigned numLocalNodes)
{
    uint64_t totMemSize =
        XcSysHelper::get()->getPhysicalMemorySizeInBytes() / numLocalNodes;
    uint64_t bcSize;

    if (mlockedPercentOfMemory() > 100) {
        // Cannot Mlock more than available memory
        bcSize = InvalidSize;
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::computeSize failed, since Mlocked percent of "
                "memory %u cannt exceed 100",
                mlockedPercentOfMemory());
    } else {
        bcSize = roundDown(((totMemSize * getBufCachePct()) / 100), PageSize);
    }
    return bcSize;
}

unsigned
BufferCacheMgr::mlockedPercentOfMemory()
{
    XcalarConfig *xc = XcalarConfig::get();
    if (xc->bufCacheMlock_) {
        return (getBufCachePct() * xc->bufferCacheMlockPercent_) / 100;
    } else {
        return 0;
    }
}

BcObject *
BufferCacheMgr::getObject(BufferCacheObjects objectId)
{
    uint32_t ii;
    for (ii = 0; ii < objectCount_; ii++) {
        if (bufferCacheObjectIds_[ii] == objectId) {
            return &objects_[ii];
        }
    }
    return NULL;
}

size_t
BufferCacheMgr::getObjectCount()
{
    return objectCount_;
}

Status
BufferCacheMgr::initObjects(size_t objectCount,
                            const BufferCacheObjects *bufferCacheObjectIds)
{
    Status status = StatusOk;
    size_t ii = 0;

    objectCount_ = objectCount;
    bufferCacheObjectIds_ = bufferCacheObjectIds;
    objects_ = new (std::nothrow) BcObject[objectCount];
    if (objects_ == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "BufferCacheMgr::initObjects failed, objectCount %lu: %s",
                objectCount,
                strGetFromStatus(status));
        goto CommonExit;
    }
    for (ii = 0; ii < objectCount_; ii++) {
        status = this->initObject(bufferCacheObjectIds_[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "BufferCacheMgr::initObjects failed, Object Id %lu: %s",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (status != StatusOk) {
        for (size_t jj = 0; jj < ii; jj++) {
            BufferCacheObjects objectId =
                (BufferCacheObjects) bufferCacheObjectIds_[jj];
            this->getObject(objectId)->tearDown();
        }
    }
    return status;
}

void
BufferCacheMgr::destroyObjects()
{
    if (objects_) {
        for (uint32_t ii = 0; ii < objectCount_; ii++) {
            objects_[ii].tearDown();
        }
        delete[] objects_;
        objects_ = NULL;
    }
    objectCount_ = 0;
}
