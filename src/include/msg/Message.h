// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include "primitives/Primitives.h"
#include "config/Config.h"
#include "SocketTypes.h"
#include "msg/MessageTypes.h"
#include "runtime/Spinlock.h"
#include "runtime/Semaphore.h"
#include "runtime/CondVar.h"
#include "sys/SgTypes.h"
#include "stat/StatisticsTypes.h"
#include "util/SchedulableFsm.h"

struct MsgMessage;
class BcHandle;
class SchedMessageObject;
class XcProcFsHelper;

class MsgMgr final
{
    friend class SchedMgr;
    friend class TwoPcMgr;
    friend class SchedMessageObject;

  public:
    enum BarrierType : uint64_t {
        BarrierIsNetworkReady = 0,
        BarrierSystemAppAdded,
        BarrierStreamsAdded,
        BarrierUsrnodeInitDone,
        BarrierUsrnodeApisRecvReady,
        BarrierWorkbookPersistence,
        BarrierShutdown,
        BarrierIsSessionShutdown,
        BarrierGlobalObjectsDeleted,
        NumBarriers,
        BarrierUnknown,
    };

    static MustCheck uint64_t getMsgMaxPayloadSize();

    static MustCheck uint64_t getNormalMsgCount(unsigned nodes);

    static MustCheck uint64_t getAltMsgCount(unsigned nodes);

    static MustCheck uint64_t getTwoPcPages(unsigned nodes);

    static MustCheck uint64_t getUkMsgAtomic64(unsigned nodes);

    static MustCheck uint64_t getSchedQSize();

    static MustCheck uint64_t getUkSchedObjectBufSize();

    static MsgMgr *get();

    static MustCheck Status init();

    void destroy();

    // 0. twoPc() is a very heavily loaded and fairly complex interface because
    // it incorporates a large number of requirements.
    //
    // 1. Use ephemeral for anything you wish to be passed in an opaque
    // fashion along with the message. It also contains some meta-data for the
    // lifetime of the cmd, and certain fields (payloadToDistribute,
    // payloadLength, payloadLengthTwoPcToAllocate, and twoPcFastOrSlowPath)
    // need to be setup by the caller prior to the call as they describe what
    // the caller wants to achieve. The caller must know what to do with
    // ephemeral.ephemeral which is the "use as whatever field," that can
    // be viewed as a cookie. Look up the comments for setting ephemeral
    // where typedef struct MsgEphemeral {} is defined.
    //
    // 2. twoPc() returns a handle to the cmd that the user
    // can use to reference their distributed cmd during the lifetime of the
    // command.
    //
    // 3. If twoPcCmdType is async then the caller must ask for a
    // returnHandle. Sync cmds mamy or may not ask for a handle.
    //
    // 4 The user needs to check for cmd completions within TwoPcCmdTimeout.
    // Else the twoPcStateCompletions[][] status will be cleaned out which means
    // that the returnHandle is useless.
    //
    // 5. Use twoPcHandleProperty to suggest if the cmd should be sent to all
    // nodes or a specific node (with the nodeId that follows).
    //
    // 6. Look up the definitions of MsgSendRecvFlags and the valid combinations
    // that are permitted. They control if the twoPc() cmd sends and/or receives
    // payloads.
    //
    // Note: Do not use twoPc() to do business on local node even though
    // this technically works. Just do the work yourself.
    // Caller must ensure non-zero TwoPcHandle is returned.
    MustCheck Status twoPc(TwoPcHandle *twoPcHandleOut,
                           MsgTypeId msgTypeId,
                           TwoPcHandleEnum returnHandle,
                           MsgEphemeral *ephemeral,
                           MsgSendRecvFlags msgSendRecvFlags,
                           TwoPcCmdType twoPcCmdType,
                           TwoPcNodeProperty twoPcHandleProperty,
                           NodeId remoteNodeId,
                           TwoPcClass twoPcClass);

    // If you think you know how to use twoPc() then you may be ready for the
    // alternate verson who's reputation preceeds its interface description.
    // It is a bit like going to war...no amount of training with twoPc() will
    // prepare you for what lies ahead with twoPcAlt(). The formerly void return
    // value captures the spirit of this war. ROFL. (It was changed to return
    // status because the caller needs to be able to tell if the message
    // failed to be sent, NOTFL.)
    MustCheck Status twoPcAlt(MsgTypeId msgTypeId,
                              MsgEphemeral *ephemeral,
                              MsgSendRecvFlags msgSendRecvFlags,
                              TwoPcCmdType twoPcCmdType,
                              PayloadBufCacheType payloadBufCacheType,
                              TwoPcNodeProperty twoPcHandleProperty,
                              NodeId remoteNodeId,
                              Txn txn);

    MustCheck Status twoPcBarrier(BarrierType type);

    // The caller of twoPc() is responsible for freeing up twoPcHandle if they
    // acquired one in the first place.
    void twoPcFreeCmdHandle(TwoPcHandle twoPcHandle);

    MustCheck bool twoPcIsCmdComplete(TwoPcHandle twoPcHandle,
                                      uint64_t numNodes);

    MustCheck Status initSendConnections();

    MustCheck Status wait();

    MustCheck Status getRecvThrStatus();

    MustCheck int spinUntilRecvThreadReady();

    MustCheck Status spinUntilSendThreadsReady();

    void spinUntilSendThreadsExit();

    void spinUntilRecvThreadExit();

    MustCheck NodeId getMsgSrcNodeId(MsgEphemeral *eph);

    static MustCheck NodeId getMsgDstNodeId(MsgEphemeral *eph);

    void twoPcEphemeralInit(MsgEphemeral *ephIn,
                            void *payloadToDistribute,
                            size_t payloadLength,
                            size_t payloadLengthTwoPcToAllocate,
                            TwoPcFastOrSlowPath twoPcFastOrSlowPath,
                            TwoPcCallId twoPcFuncIndex,
                            void *ephemeral,
                            TwoPcBufLife twoPcBufLife);

    void twoPcEphemeralInitAsync(MsgEphemeral *eph,
                                 void *payloadToDistribute,
                                 size_t payloadLength,
                                 size_t payloadLengthTwoPcToAllocate,
                                 TwoPcFastOrSlowPath twoPcFastOrSlowPath,
                                 TwoPcCallId twoPcFuncIndex,
                                 void *ephemeral,
                                 TwoPcBufLife twoPcBufLife,
                                 Semaphore *sem,
                                 uint64_t numNodes);

    static MustCheck uint64_t bcMsgMessageBufSize();

    void setMsgNodeState(MsgEphemeral *ephemeral, void *payload);

    void ackAndFree(MsgMessage *msg);

    MustCheck bool transactionAbortSched(Schedulable *sched,
                                         Status errorStatus);

    struct TxnLog {
        static constexpr unsigned MaxMessages = 1;

        TxnLog(Txn txn)
        {
            txn_ = txn;
            memZero(messages, MaxMessages * MaxTxnLogSize);
        }

        void del() { delete this; }

        IntHashTableHook hook;
        Txn txn_;

        Xid getTxnXid() const { return txn_.id_; }

        unsigned numMessages = 0;
        char messages[MaxMessages][MaxTxnLogSize];
    };

    void logTxnBuf(Txn txn, char *msgHdr, char *buf);

    MustCheck Status addTxnLog(Txn txn);
    void deleteTxnLog(Txn txn);

    MustCheck TxnLog *getTxnLog(Txn txn);

    void transferTxnLog(TxnLog *srcLog, TxnLog *dstLog);

    void restoreTxnAndTransferTxnLog(Txn parentTxn);

    MustCheck unsigned numCoresOnNode(NodeId nodeId);

    MustCheck bool symmetricalNodes() { return symmetricalNodes_; }

    // Calling this means only local cleanup can occur as the listener thread
    // will exit.
    Status setClusterStateShutdown();

    XcProcFsHelper *procFsHelper_ = NULL;

  private:
    static constexpr uint64_t MsgMaxPayloadSizeAbsolute = 128 * KB;

    // Abstractions on top of libmsg could require nesting of 2PCs. For instance
    // GVMs may require going to DLM nodes and then broadcasting. So it's
    // concurrency should be chosen appropriately.
    static constexpr uint64_t MaxNesting = 3;

    // Minimum number of messages for completing a twoPc on the source node of
    // twoPc, i.e. this is the number of messages needed for a 2PC going to all
    // nodes.
    static constexpr uint64_t MinMessages = 2;

    // Note: TwoPcMaxOutstandingCmds and TwoPcNumCmdIndexBitsInHandle
    // must be maintained together as the number of bits must be able to index
    // the highest cmd without overflow in TwoPcHandle which maintains cmd
    // index.
    static constexpr uint32_t TwoPcMaxOutstandingCmds = 256;
    static constexpr uint32_t TwoPcNumCmdIndexBitsInHandle = 16;

    // Concurrency of twoPcs in steady state.
    static constexpr uint64_t SteadyStateConcurrency = 32;
    static constexpr uint64_t SteadyStateNestingConcurrency =
        SteadyStateConcurrency * MaxNesting;
    static constexpr uint64_t SteadyStateMsgCount =
        MinMessages * (SteadyStateConcurrency + SteadyStateNestingConcurrency);

    // Concurrency of twoPcs in reserve state. This is kept small enough so
    // the system can self heal.
    static constexpr uint64_t ReserveConcurrency = MaxNesting;
    static constexpr uint64_t ReserveMsgCount =
        MinMessages * ReserveConcurrency;
    static constexpr uint64_t ReserveNestingConcurrency =
        ReserveConcurrency * MaxNesting;
    static_assert(ReserveConcurrency / MaxNesting > 0,
                  "ReserveConcurrency/MaxNesting > 0");

    static_assert(SteadyStateConcurrency + SteadyStateNestingConcurrency +
                          ReserveConcurrency + ReserveNestingConcurrency <=
                      TwoPcMaxOutstandingCmds,
                  "Cannot exceed TwoPcMaxOutstandingCmds");

    static constexpr unsigned MsgTwoFields = 2;
    static constexpr uint64_t MsgConnectRetryTimeSec = 3600;
    static constexpr uint64_t MsgConnectSleepUSec = 50;
    static constexpr uint64_t MsgConnectRetryCount =
        MsgConnectRetryTimeSec * USecsPerSec / MsgConnectSleepUSec;

    // Receive error ACKs are processed every 1 Milli-Second.
    static constexpr size_t RecvErrorThreadSleepUsec = 1000;
    static constexpr size_t RecvErrChunkSize = 1 * KB;
    static constexpr size_t MaxOutstandingRecvErrs = 1 << 4;

    // We include swap here to compute the threshold at which we start failing
    // 2PCs, so the system can self heal by kicking transaction recovery.
    // Minimum System Free resource size in Bytes.
    static constexpr size_t MinSysFreeResourceSize = 1 * GB;

    // Invalidate cached system free resource size every second.
    static constexpr uint64_t CachedSysFreeResSizeInvalidateUsec = 1000000;

    // When last updated the cached system free resource size.
    struct timespec lastUpdatedSysFreeResSize_;

    // Cached system resource size in Bytes.
    size_t cachedSysFreeResSize_ = 0;

    // Mininum System Free resource in Percent of total system resources.
    static constexpr size_t MinSysFreeResPct = 5;

    // System Free resource threshold to start cancelling 2PCs that can be
    // failed cleanly.
    size_t sysFreeResThreshold_ = MinSysFreeResourceSize;

    // System free resource percent over system resource threshold
    // (sysFreeResThreshold_) to start self healing. This allows us to prevent
    // hysteresis.
    static constexpr size_t SysFreeResPctOverThreshToSelfHeal = 25;
    static MsgMgr *instance;
    static constexpr unsigned NumTxnLogSlots = 31;

    size_t sysFreeResHealThreshold_ =
        MinSysFreeResourceSize +
        (MinSysFreeResourceSize * SysFreeResPctOverThreshToSelfHeal) / 100;

    bool sysFreeResourceBelowThreshold_ = false;

    Spinlock txnLogLock_;
    IntHashTable<Xid,
                 TxnLog,
                 &TxnLog::hook,
                 &TxnLog::getTxnXid,
                 NumTxnLogSlots,
                 hashIdentity>
        txnLogs_;

    // Buf$ for messages
    BcHandle *bcHandleMsgNormal_ = NULL;
    BcHandle *bcHandleMsgAlt_ = NULL;
    BcHandle *bcHandleTwoPcPages_ = NULL;
    BcHandle *bcHandleAtomic64_ = NULL;

    enum class ClusterState {
        Startup,
        Shutdown,
    };
    ClusterState clusterState_ = ClusterState::Startup;

    struct AckAndFreeInfo {
        MsgMessage *msg_;
        SchedulableFsm *schedFsm_;
    };

    struct SendRecvThreadInfo {
        // Receive thread info
        pthread_t recvThreadId_;
        bool recvThreadCreated_ = false;
        Status recvThrStatus_ = StatusOk;
        int recvThreadReady_ = 0;
        MsgThreadParams recvThreadParams_;

        // Receive thread error handlers.
        enum ReceiveErrMsgState : unsigned {
            ReceiveErrMsgFree,
            ReceiveErrMsgInProgress,
            ReceiveErrMsgInUse,
        };
        MsgMessage *receiveErrorMsgs_ = NULL;
        ReceiveErrMsgState receiveErrorMsgsState_[MaxOutstandingRecvErrs] = {
            ReceiveErrMsgFree};
        pthread_t recvErrorThreadId_;
        bool recvErrorThreadCreated_ = false;
        Spinlock errRecvLock_;
        sem_t errRecvSem_;

        // Send thread info
        SocketHandle sendFd_[MaxNodes];
        pthread_t sendThreadId_[MaxNodes];
        bool sendThreadCreated_[MaxNodes] = {false};
        MsgThreadParams sendThreadParams_[MaxNodes];
        int sendThreadsReady_ = 0;
        // Serialize send messages. This goes under high contention due to the
        // amount of small ACKs we send
        pthread_mutex_t sendSerializeLock_[MaxNodes];

        Spinlock threadsReadyLock_;
        SendRecvThreadInfo()
        {
            for (unsigned ii = 0; ii < MaxNodes; ii++) {
                verify(pthread_mutex_init(&sendSerializeLock_[ii], NULL) == 0);
            }
        }

        ~SendRecvThreadInfo()
        {
            for (unsigned ii = 0; ii < MaxNodes; ii++) {
                verify(pthread_mutex_destroy(&sendSerializeLock_[ii]) == 0);
            }
        }
    };
    SendRecvThreadInfo sendRecvThdInfo_;

    // the pokeSd_ is to wake up the listener when a new connection has been
    // added in order to cause the listener to add it to the descriptor set
    SocketHandle pokeSd_ = SocketHandleInvalid;

    // Track configuration information of all nodes in the cluster.
    struct NodeLocalInfo {
        unsigned numCores;
    };
    NodeLocalInfo *nodeLocalInfo_ = NULL;
    bool symmetricalNodes_ = true;

    class BarrierInfo
    {
      public:
        BarrierInfo(BarrierType type) : barrierReached_(0), type_(type) {}

        BarrierType getType() const { return type_; }

        void recordNodeEntered(NodeId nodeId);

        Semaphore barrierReached_;
        IntHashTableHook hook;

      private:
        BarrierType type_;
        Spinlock lock_;
        bool nodeReached_[MaxNodes] = {};
    };
    typedef IntHashTable<BarrierType,
                         BarrierInfo,
                         &BarrierInfo::hook,
                         &BarrierInfo::getType,
                         2,
                         hashCastUint64<BarrierType>>
        BarrierHashTable;

    class BarrierMgr
    {
      public:
        BarrierMgr() = default;

        Status initBarrier(BarrierType type, BarrierInfo **bInfo);
        void finalizeBarrier(BarrierInfo *bInfo);

        Status getBarrierInfo(BarrierType type, BarrierInfo **bInfo);

      private:
        Spinlock lock_;
        BarrierHashTable barrierTable_;
    };

    struct BarrierMsg {
        BarrierType type;
        // XXX this should be a separate API which is exchanged earlier on
        NodeLocalInfo info;
    };

    struct TwoPcInfo {
        BarrierMgr barrier_;

        Spinlock twoPcCmdSlotLock_;
        TwoPcCmdProperty *twoPcCmdLock_ = NULL;
        Semaphore twoPcCmdSem_;
        uint32_t twoPcCmdNextAvailIndex_ = 0;

        // Basically a two dimensional array of the state of each outstanding
        // 2PC message to each node (function or function complete)
        MsgTypeId **twoPcStateCompletions_ = NULL;

        TwoPcHandle dummyTwoPcHandle_;
        Semaphore *twoPcNormalSem_ = NULL;
        Semaphore *twoPcReserveSem_ = NULL;
        Semaphore *twoPcNestedNormalSem_ = NULL;
        Semaphore *twoPcNestedReserveSem_ = NULL;

        Semaphore *twoPcAltSem_ = NULL;

        TwoPcInfo() : twoPcCmdSem_(0) {}
    };
    TwoPcInfo twoPcInfo_;

    //
    // Stats
    //
    // Stat count for message group
    static constexpr size_t StatCount = 17;
    struct {
        StatGroupId statsGrpId_;
        StatHandle recdMessagesStat_;
        StatHandle recdMessagesErrStat_;
        StatHandle sentMessagesSucceededStat_;
        StatHandle sentMessagesFailedStat_;
        StatHandle ooMessagesStat_;
        StatHandle ooPayloadStat_;
        StatHandle bigMessagesStat_;
        StatHandle ooTwoPcCmdsCumWaitCntStat_;
        StatHandle ooTwoPcCmdsCumWaitUsecStat_;
        StatHandle acksScheduled_;
        StatHandle kickedTxnAbort_;
        StatHandle outOfFiberSched_;
        StatHandle outOfFiberSchedFsmTransportPage_;
        StatHandle outOfFiberSchedMessageObject_;
        StatHandle resourceLevelLow_;

        // recv thread counter
        // number of times recvDataFromSocket is called
        StatHandle recvDataFromSocketCounter_;
        // how much time we spent in recvDataFromSocket method
        StatHandle timeSpentUsToRecvData_;
    };

    //
    // Private Methods
    //
    MustCheck Status sendVector(SocketHandle sockHandle,
                                SgArray *sgArray,
                                int flags);

    MustCheck Status buildTmpSgArray(SgArray *dstSgArray,
                                     const SgArray *srcSgArray,
                                     size_t bytesRemaining);

    static MustCheck void *sendQueuedMessages(void *threadParamsIn);

    void *issueLocalIos(void *unused);

    static MustCheck void *waitForMessagesActual(void *threadParamsIn);

    MustCheck Status allocMsg(MsgMessage **msg,
                              MsgCreator creator,
                              PayloadBufCacheType payloadBufCacheType,
                              size_t payloadLength,
                              MsgBufType msgBufType);

    MustCheck TwoPcHandle
    nextTwoPcCmdHandle(MsgTypeId msgTypeId,
                       TwoPcCmdType twoPcCmdType,
                       TwoPcNodeProperty twoPcNodeProperty,
                       int remoteNodeId);

    MustCheck Status recvDataFromSocket(SocketHandle sockHandle,
                                        SocketHandleSet masterSet);

    void initMsg(MsgMessage *msg,
                 MsgTypeId msgTypeId,
                 uint64_t key,
                 NodeId myNodeId,
                 Xid xidIn,
                 Semaphore *wakeupSem,
                 Atomic32 *outstandingMessages,
                 MsgEphemeral *ephemeral,
                 MsgSendRecvFlags msgSendRecvFlags,
                 TwoPcHandle twoPcHandle,
                 TwoPcCmdType twoPcCmdType,
                 TwoPcFastOrSlowPath twoPcFastOrSlowPath);

    MustCheck uint32_t getStatIndex(TwoPcCallId twoPcFuncIndex);

    void twoPcRecvDataCompletionFunc(SocketHandle *sockHandle, MsgMessage *msg);

    void process2PcCmdCompletePre(SocketHandle sockHandle,
                                  MsgMessage *msg,
                                  ZeroCopyType zeroCopy);

    void process2pcCmd(SocketHandle sockHandle,
                       MsgMessage *msg,
                       MsgTypeId msgTypeId);

    void process2pcCmdCompletePost(MsgMessage *msg, MsgTypeId msgTypeId);

    MustCheck Status connectToNode(MsgThreadParams *threadParams);

    MustCheck Status sendMsg(NodeId nodeId,
                             MsgMessage *msg,
                             AckDirection ackDirection);

    void updateMsgLocalTwoPcState(MsgMessage *msg, MsgTypeId state);

    void freeNodeToWaitForMsg(Atomic64 *nodeToWait);

    void freeAndEnqueueMsg(MsgMessage *msg);

    void freePayload(MsgMessage *msg);

    enum class ReceiveErrorOp {
        Ack,
        DontAck,
    };
    void handleReceiveError(SocketHandle sockHandle,
                            MsgMessage *msg,
                            size_t payloadLength,
                            Status msgStatus,
                            ReceiveErrorOp receiveErrorOp);

    enum class ResourceLevel {
        Normal,
        Reserve,
        Low,
    };
    MustCheck ResourceLevel failOnResourceShortage(MsgTypeId msgTypeId);

    void freeMsgOnly(MsgMessage *msg);

    void updateMsgTwoPcState(MsgMessage *msg);

    static MustCheck uint64_t getAckAndFreeHandlerQueueDepth(unsigned nodes);

    void doTwoPcWorkForMsg(MsgMessage *msg);

    static MustCheck void *processReceiveErrorAck(void *args);

    MustCheck MsgTypeId getMsgTypeId(MsgEphemeral *eph);

    void removeRecvFlagOnPayloadAllocFailure(MsgMessage *msg);
    void removeSendFlagOnPayloadAllocFailure(MsgMessage *msg);

    void checkPayloadLengthAndDisableSendRecvFlags(MsgMessage *msg);

    void removeRecvFlagAndReleasePayload(MsgMessage *msg);

    // Keep this private, use init instead
    MsgMgr() = default;

    // Keep this private, use destroy instead
    ~MsgMgr() = default;

    MsgMgr(const MsgMgr &) = delete;
    MsgMgr &operator=(const MsgMgr &) = delete;
};

#endif  // _MESSAGE_H_
