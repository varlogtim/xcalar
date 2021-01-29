// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// Use this module for batched demystification.
// To customize, you must create 3 inherited objects:
// DemystifySend:
//   Used for issuing rows to be demystifyed
//
// DemystifyRecvFatptr:
//  Used for demystyfying an incoming transPage of fatptrs
//
// DemystifyRecvScalar:
//  Used for processing incoming transPage of demystified values (scalars)
//
// The flow is as follows
// 1. Initialize a DemystifySend obj with appropriate params
// 2. Call DemystifySend.demystifyRow for each row
// 3. This will start packing transPages until one gets filled
// 4. The filled transPage gets send to a remote node by the transPageHandle
// 5. The remote node calls DemystifyRecvFatptr.demystifySourcePage
// 6. This fills up scalar pages which will be sent to a destination node
// 7. The destination calls DemystifyRecvScalar.processScalarPage
//
// The caller must manage the accounting of rows outside of this module

#ifndef _DEMYSTIFY_H_
#define _DEMYSTIFY_H_

#include "df/DataFormatTypes.h"
#include "dataset/DatasetTypes.h"
#include "msg/MessageTypes.h"
#include "transport/TransportPage.h"
#include "msg/Message.h"
#include "df/DataFormat.h"
#include "newtupbuf/NewKeyValueTypes.h"

class DemystifyMgr
{
  public:
    enum Op : uint8_t {
        Invalid,
        // These share a common demystifySourcePage function
        // defined in DemystifyMgr::OperatorsRecv
        FilterAndMap,
        GroupBy,
        CreateScalarTable,
        Index,
        ResultSet,

        Last,
    };

    static Status init();
    static void destroy();
    static DemystifyMgr *get();

    // For now, the incoming source TPs that need to be demystified are always
    // packed with fixed style fields. So we can make a hard assumption here
    // about the type of TP.
    void demystifySourcePageWork(MsgEphemeral *eph, TransportPage *transPage);

    // For now, the incoming Scalar Page TPs are always packed with variable
    // field type DfScalar. So we can make a hard assumption here about
    // the type of TP.

    // returns true if page was enqueued or false if it wasn't
    // this also sets up the demystifyContext for the transaction
    bool enqueueScalarPage(MsgEphemeral *eph, TransportPage *transPage);

    // returns dequeued page or NULL
    TransportPage *dequeueScalarPage(Txn txn);

    void addScalarPageWorker(Txn txn);
    void removeScalarPageWorker(Txn txn);

    void removeDemystifyContext(Txn txn);

  private:
    static DemystifyMgr *instance;
    const char *moduleName_ = "Demystify";
    static constexpr unsigned ContextHashSlotNum = 7;

    class DemystifyContext
    {
      public:
        DemystifyContext(Txn txn) {
            txn_ = txn;
            maxScalarPageWorkers_ = Runtime::get()->getThreadsCount(txn.rtSchedId_);
        }

        IntHashTableHook hook;
        Xid getTxnXid() const { return txn_.id_; }

        Txn txn_;
        unsigned maxScalarPageWorkers_ = 0;
        unsigned numScalarPageWorkers_ = 0;
        unsigned listSize_ = 0;

        TransportPage *listHead_ = NULL;
    };

    IntHashTable<Xid,
                 DemystifyContext,
                 &DemystifyContext::hook,
                 &DemystifyContext::getTxnXid,
                 DemystifyMgr::ContextHashSlotNum,
                 hashIdentity>
        contextTable_;
    Mutex lock_;

    DemystifyMgr();
    ~DemystifyMgr();

    DemystifyMgr(const DemystifyMgr &) = delete;
    DemystifyMgr &operator=(const DemystifyMgr &) = delete;
};

class TransportPageHandle;

template <typename OpDemystifySend>
class DemystifySend
{
  public:
    enum SendOption {
        SendFatptrsOnly,
        SendEntireRow,
    };

    TransportPage **demystifyPages_;

    XdbId dstXdbId_;
    DfFieldType keyType_;
    int keyIndex_;
    bool *validIndices_;
    // stream handle for sending all transport page
    TransportPageHandle *demystifyPagesHandle_;

    struct PerNodeState {
        bool issuedToNode;
        NewTupleMeta dstTupleMeta;
        NewTupleValues dstTuple;
        PerNodeState() = default;
        ~PerNodeState() = default;
    };
    PerNodeState *perNodeState_ = NULL;

    // these are here for the index case. Transport page allocations are
    // based off of a quota. When the quota is reached, we need to send out
    // partially filled pages. These can either come from the demystifyPages_
    // or the additionalPages_ array,
    TransportPage **additionalPages_;
    TransportPageHandle *additionalPagesHandle_;

    void *localCookie_;
    SendOption sendOption_;
    DemystifyMgr::Op op_;

    DemystifySend(DemystifyMgr::Op op,
                  void *localCookie,
                  SendOption sendOption,
                  TransportPage **demystifyPages,
                  XdbId dstXdbId,
                  DfFieldType keyType,
                  int keyIndex,
                  bool *validIndices,
                  TransportPageHandle *demystifyPagesHandle,
                  TransportPage **additionalPages,
                  TransportPageHandle *additionalPagesHandle);

    ~DemystifySend() = default;

    // request
    MustCheck Status demystifyRow(NewKeyValueEntry *srcKvEntry,
                                  DfFieldValue rowMetaField,
                                  Atomic32 *countToIncrement);

    MustInline MustCheck Status
    processDemystifiedRow(void *localCookie,
                          NewKeyValueEntry *srcKvEntry,
                          DfFieldValue rowMetaField,
                          Atomic32 *countToDecrement)
    {
        return static_cast<OpDemystifySend *>(this)
            ->processDemystifiedRow(localCookie,
                                    srcKvEntry,
                                    rowMetaField,
                                    countToDecrement);
    }

    MustCheck Status init()
    {
        Status status = StatusOk;
        perNodeState_ = new (std::nothrow) PerNodeState[activeNodes_];
        BailIfNull(perNodeState_);
    CommonExit:
        return status;
    }

    void destroy()
    {
        if (perNodeState_ != NULL) {
            delete[] perNodeState_;
            perNodeState_ = NULL;
        }
        delete this;
    }

  private:
    NodeId myNode_;
    unsigned activeNodes_;
    TransportPageMgr *tpMgr_;
    DataFormat *df_;

    MustCheck Status sendValueArray(unsigned dstNodeId,
                                    const NewTupleMeta *tupleMeta,
                                    NewTupleValues *tupleValues);

    DemystifySend(const DemystifySend &) = delete;
    DemystifySend &operator=(const DemystifySend &) = delete;
};

class DemystifyRecv
{
  public:
    DemystifyRecv(){};
    virtual ~DemystifyRecv() {}

    // For now, the incoming source TPs that need to be demystified are always
    // packed with fixed style fields. So we can make a hard assumption here
    // about the type of TP.
    virtual MustCheck Status demystifySourcePage(
        TransportPage *transPage, TransportPageHandle *scalarHandle) = 0;

    // For now, the incoming Scalar Page TPs are always packed with variable
    // field type DfScalar. So we can make a hard assumption here about
    // the type of TP.
    virtual MustCheck Status processScalarPage(TransportPage *transPage,
                                               void *context) = 0;
};

extern DemystifyRecv *demystifyRecvHandlers[DemystifyMgr::Last];

#endif  // _DEMYSTIFY_H_
