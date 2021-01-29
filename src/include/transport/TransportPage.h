// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_

#include "dataset/DatasetTypes.h"
#include "bc/BufferCache.h"
#include "util/SchedulableFsm.h"
#include "xccompress/XcSnappy.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "operators/GenericTypes.h"
#include "xdb/DataModelTypes.h"
#include "stat/Statistics.h"
#include "xdb/Xdb.h"
#include "util/MemoryPool.h"

class SchedFsmTransportPage;

enum class TransportPageType : uint8_t {
    SendSourcePage,  // these pages are used for requests, thrashed
    RecvPage,        // these pages are used for responses
};

struct DsDemystifyTableInput {
    void *opMeta;
    unsigned numVariables;
    bool includeSrcTableSample;
    size_t inputStringsSize;

    // variableNames
    char inputStrings[0];
};

struct TransportPage;

class TransportPageHandle;

class TransportPageMgr final
{
    friend class TransportPageHandle;

    friend struct TransportPage;

  public:
    static bool doNetworkCompression;

    static constexpr bool DoNetworkCompressionDefault = true;

    static Status init();

    static void destroy();

    MustCheck Status setupShipper();
    void teardownShipper();

    static TransportPageMgr *get();

    struct UsrEph {
        NodeId srcNodeId;
        NodeId dstNodeId;
        XdbId tableId;
        TransportPage *transPage;

        TransportPageHandle *transPageHandle;
    };

    // return Fatptr Transport page count.
    static uint32_t transportSourcePagesCount(uint32_t numNodes);
    static uint32_t getMaxSerializationBuffers();

    static size_t getSerializationBufferSize()
    {
        return XcSnappy::getMaxCompressedLength(transportPageSize());
    }
    static size_t transportPageSize() { return XdbMgr::bcSize(); }

    void transportPageShipComplete(MsgEphemeral *eph, void *payload);

    MustCheck TransportPage *allocRecvTransportPage(Status *statusOut);
    MustCheck Status sendAllPages(TransportPage **pages,
                                  unsigned numPages,
                                  TransportPageHandle *tpHandle);

    Status allocMissingSourcePages(TransportPage **pages,
                                   unsigned numPages,
                                   TransportPageHandle *tpHandle,
                                   TransportPage **additionalTps,
                                   TransportPageHandle *additionalTph);

    void freeTransportPage(TransportPage *transPage);
    void processPageForShipping(TransportPage *transPage);
    void incTransPagesReceived()
    {
        StatsLib::statAtomicIncr64(cumlTransPagesReceived_);
    }

  private:
    bool inited_ = false;
    Atomic64 statusAgainCounter_[MaxNodes];

    BcHandle *bcHandleSourcePages_ = NULL;
    BcHandle *bcHandleSerializationBuf_ = NULL;

    MemoryPool *usrEphPool_ = NULL;
    Mutex usrEphPoolLock_;

    // This is used to batch allocate pages
    TransportPage **tmpSourcePages_ = NULL;
    Mutex sourcePageAllocationLock_;
    // tracks freed pages.
    Semaphore sourcePageSem_;

    Spinlock shipStackLock_;

    StatHandle sourcePagesEnqueued_;
    // This stack of transport pages is staged to be shipped.
    TransportPage *shipStackOnHold_[MaxNodes] = {NULL};
    Atomic32 shipperActive_[MaxNodes];

    Atomic32 *outstandingPagesCounter_ = NULL;

    static TransportPageMgr *instance;

    TransportPageMgr();
    ~TransportPageMgr();

    TransportPageMgr(const TransportPageMgr &) = delete;
    TransportPageMgr &operator=(const TransportPageMgr &) = delete;

    Status initInternal();
    void destroyInternal();

    void shipDriver();
    void ship(TransportPage *shipStackIn);
    // this allocates missing pages all or nothing;
    // returns true if all allocations succeeds
    bool allocSourcePagesInternal(TransportPage **pages, unsigned numPages);

    MustCheck TransportPageMgr::UsrEph *allocUsrEph();
    void freeUsrEph(TransportPageMgr::UsrEph *usrEph);

    StatGroupId statsGrpId_;
    StatHandle numFailedToAllocSerialBufferStat_;
    StatHandle cumlTransPagesShipped_;
    StatHandle cumlTransPagesReceived_;
    StatHandle shipCompletionFailure_;
    StatHandle cumShipperCleanout_;
    StatHandle numSourceTransPagesAllocRetries_;
    StatHandle numTransPagesAllocsXdbPool_;
    StatHandle numTransPagesFreesXdbPool_;
    StatHandle numUsrEphAllocs_;
    StatHandle numUsrEphFrees_;

    class TpShipStart : public FsmState
    {
      public:
        TpShipStart(SchedulableFsm *schedFsm)
            : FsmState("TpShipStart", schedFsm)
        {
        }

        virtual TraverseState doWork() override;
    };

    class SchedFsmShipTransPage : public SchedulableFsm
    {
        friend class TpShipStart;

      public:
        SchedFsmShipTransPage(TransportPageMgr *tpMgr)
            : SchedulableFsm("SchedFsmShipTransPage", NULL),
              start_(this),
              tpMgr_(tpMgr)
        {
            setNextState(&start_);
            waitSem_.init(0);
            doneSem_.init(0);
            atomicWrite32(&activeShippers_, 0);
            atomicWrite32(&shippersDone_, 0);
        }
        ~SchedFsmShipTransPage() {}
        virtual void done();

        void wake() { waitSem_.post(); }

        void waitUntilDone()
        {
            unsigned active = atomicRead32(&activeShippers_);
            for (uint64_t ii = 0; ii < active; ii++) {
                wake();
            }
            doneSem_.semWait();
        }

        void incActiveShippers() { atomicInc32(&activeShippers_); }

        MustCheck unsigned getActiveShippers()
        {
            return atomicRead32(&activeShippers_);
        }

      private:
        TpShipStart start_;
        Semaphore waitSem_;
        Semaphore doneSem_;
        Atomic32 activeShippers_;
        Atomic32 shippersDone_;
        TransportPageMgr *tpMgr_ = NULL;
    };

    SchedFsmShipTransPage tpShipFsm_;
};

struct TransportPage {
    // XXX TODO All per TXN states here seem redundant and can be removed.
    // Instead use operators per txn cookie to lookup these states.
    XdbId dstXdbId = XidInvalid;  // destination table
    NewTupleMeta tupMeta;
    NewKeyValueMeta kvMeta;
    TransportPageMgr::UsrEph *usrEph;
    struct TransportPage *next;  // next pointer shared by various lists
    Txn txn;
    NodeId srcNodeId;
    NodeId dstNodeId;
    OpTxnCookie srcNodeTxnCookie;
    OpTxnCookie dstNodeTxnCookie;
    bool compressed;
    uint8_t demystifyOp;  // used for demystification op
    TransportPageType pageType;
    uint32_t headerSize;
    uint32_t payloadSize;
    NewTuplesBuffer *tupBuf;  // To be fixed up in the destination

    // variable size buf shared by input and tupBuf.bufferStart_
    uint8_t buf[0];

    TransportPage(TransportPageType pageTypeIn) { pageType = pageTypeIn; }

    void serialize();
    MustCheck Status deserialize();
};

class TransportPageHandle
{
  public:
    static constexpr const char *moduleName = "TransportPage";
    enum ShipType {
        Async,
        Sync,
    };

    MsgTypeId msgTypeId_;
    TwoPcCallId msgCallId_;
    OpTxnCookie *perNodeTxnCookies_;
    Atomic32 outstandingTransportPageMsgs_;
    Spinlock lock_;
    CondVar wakeupCv_;
    XdbId dstXdbId_;
    Status cmpStatus_;
    SchedFsmTransportPage *schedFsm_;
    ShipType type_;

    MustCheck Status initHandle(MsgTypeId msgTypeId,
                                TwoPcCallId msgCallId,
                                OpTxnCookie *perNodeTxnCookies,
                                XdbId dstXdbId,
                                SchedFsmTransportPage *schedFsm);
    void destroy();

    void initPage(TransportPage *transPage,
                  NodeId dstNodeId,
                  const NewTupleMeta *tupleMeta,
                  uint8_t op);

    MustCheck Status insertKv(TransportPage **transPageOut,
                              const NewTupleMeta *tupleMeta,
                              NewTupleValues *tuple);

    // XXX: additional pages is a hack for index from table. It should
    // be the same length as transPages. If we are about to go to sleep,
    // select a tranport page from either transPages or additionalPages to
    // send out
    MustCheck Status allocQuotaTransportPage(TransportPageType type,
                                             TransportPage **transPages,
                                             TransportPage **additionalTps,
                                             TransportPageHandle *additionalTph,
                                             unsigned numTransPages,
                                             unsigned dstId);

    MustCheck Status enqueuePage(TransportPage *transPage);

    // waitForCompletions blocks until all acks are received
    MustCheck Status waitForCompletions();
    MustCheck Status shipPage(TransportPage *transPage);

  private:
    TransportPageMgr *tpMgr_;
};

#endif  // _TRANSPORT_H_
