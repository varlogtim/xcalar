// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MESSAGETYPES_H_
#define _MESSAGETYPES_H_

#include <pthread.h>

#include "primitives/Primitives.h"
#include "scalars/Scalars.h"
#include "operators/XdfParallelDoTypes.h"
#include "operators/XcalarEvalTypes.h"
#include "config/Config.h"
#include "runtime/Txn.h"

// From LibApisCommon.h.
struct XcalarApiOutput;
struct XcalarApiSetConfigParamInput;

static constexpr unsigned MaxTxnLogSize = 4096;

enum ZeroCopyType {
    ZeroCopy = 0xa,
    DoNotZeroCopy = 0xb,
};

enum MsgDirection {
    MsgRecv = 0xa,
    MsgSend = 0xb,
};

enum MsgBufType : uint32_t {
    NormalMsg = 0x1000,
    AltMsg = 0x2000,
    CopyMsg = 0x4000,
    NormalCopyMsg = NormalMsg | CopyMsg,
    AltCopyMsg = AltMsg | CopyMsg,
};

enum TwoPcHandleEnum {
    TwoPcReturnHandle = 0xa,
    TwoPcDoNotReturnHandle = 0xb,
};

enum PayloadBufCacheType : uint8_t {
    XdbBufCacheForPayload = 0xa,
    MallocedBufForPayload = 0xb,
    CallerBufForPayload = 0xc,
    NoPayload = 0xd,
    IgnorePayloadBufCacheType = 0xe,
    InvalidPayloadBufCacheType = 0xf,
    TransportPagesForPayload = 0x10
};

// Bit flags for message send and recv. These can be OR-ed.
// Note: use one of MsgSendHdrOnly or MsgSendHdrPlusPayload, and one of
// MsgRecvHdrOnly or MsgRecvHdrPlusPayload. That is at most 2 flags can
// be OR-ed. Here are the valid combinations:
// 1. MsgSendHdrOnly | MsgRecvHdrOnly
// 2. MsgSendHdrOnly | MsgRecvHdrPlusPayload
// 3. MsgSendHdrPlusPayload | MsgRecvHdrOnly
// 4. MsgSendHdrPlusPayload | MsgRecvHdrPlusPayload
enum MsgSendRecvFlags : uint8_t {
    MsgSendHdrOnly = 0x1,
    MsgSendHdrPlusPayload = 0x2,
    MsgRecvHdrOnly = 0x4,
    MsgRecvHdrPlusPayload = 0x8
};

enum AckDirection {
    SourceToDest = 0xa,
    DestToSource = 0xb,
};

// TwoPcSlowPath calls can use small or large buffers that are not from
// XDB buf$ and will use malloc(). TwoPcFastPath pages come from XDB buf$.
enum TwoPcFastOrSlowPath : uint8_t {
    TwoPcFastPath = 0xa,
    TwoPcSlowPath = 0xb,
};

// Bufs or payload used by twoPc() can be owned by the caller or the uk.
// This determines their lifecycle.
// Note: The below flags are bitflags and can be OR-ed.
// Note: Do not specify a flag for input or
// TwoPcZeroCopyInput:  Input buf passed to 2pc will be used. uk will free it.
// TwoPcZeroCopyOutPut: Result set will directly be copied into buf pointed
//                      by caller's ephemeral.
// TwoPcMemCopyInput:   Input buf passed will be copied into uk's buf.
// TwoPcMemCopyOutput:  Result set will be copied by caller's completion
//                      function.
// Valid combinations are:
// One of these possibilities:
//     TwoPcNoInputOrOutput                                (no input or output)
//     TwoPcZeroCopyInput                                          (input only)
//     TwoPcZeroCopyOutput                              (output only, no input)
//     TwoPcMemCopyInput                                (input only, no output)
//     TwoPcMemCopyOutput                               (output only, no input)
//     TwoPcZeroCopyInput | TwoPcZeroCopyOutput                (input + outout)
//     TwoPcZeroCopyInput | TwoPcMemCopyOutput                 (input + outout)
//     TwoPcMemCopyInput | TwoPcZeroCopyOutput                 (input + outout)
//     TwoPcMemCopyInput | TwoPcMemCopyOutput                  (input + outout)
enum TwoPcBufLife : uint8_t {
    TwoPcNoInputOrOutput = 0x0,
    TwoPcZeroCopyInput = 0x1,
    TwoPcZeroCopyOutput = 0x2,
    TwoPcMemCopyInput = 0x4,
    TwoPcMemCopyOutput = 0x8
};

// Note: TwoPcSingleNode and TwoPcAllNodes are used to assign a single bit
// field and must preserve their values as 0 and 1.
enum TwoPcNodeProperty {
    TwoPcSingleNode = 0,
    TwoPcAllNodes = 1,
};

enum TwoPcPayloadProperty {
    TwoPcCopyPayload = 0xa,
    TwoPcDoNotCopyPayload = 0xb,
};

enum TwoPcClass {
    TwoPcClassNested,
    TwoPcClassNonNested,
};

enum TwoPcCmdProperty {
    TwoPcCmdLocked = 0xa,
    TwoPcCmdFree = 0xb,
};

// Note: TwoPcWeakOrder and TwoPcWeakOrder are used to assign a single bit
// field and must preserve their values as 0 and 1.
enum TwoPcOrder {
    TwoPcWeakOrder = 0,
    TwoPcStrongOrder = 1,
};

enum { TwoPcIgnoreNodeId = 0xdeadbeef };

// Parameters to twoPc that control if the cmd to be executed will be
// async. If async then the cmd is ack-ed on receipt by all the nodes.
// Each node spawns a thread for a long running job for an async cmd. On
// cmd completion twoPcState[][] is updated. The user needs to check for
// cmd completion. If the user does not check within a certain time, the
// cmd completion status will be cleaned up.
// Note: TwoPcAsyncCmd and TwoPcSyncCmd are used to assign a single bit
// field and must preserve their values as 0 and 1.
enum TwoPcCmdType : uint8_t {
    TwoPcSyncCmd = 0,
    TwoPcAsyncCmd = 1,
    TwoPcAltCmd = 2,
};

enum TwoPcCmdCompletionTimeout {
    TwoPcCmdTimeout = 2000000,
    TwoPcCmdTimeoutFortyFiveMillisecs = 45,
};

// Note: keep the enums numbered for easy debug. Also maintain
// low/high bounds for debug. Ensure all numbers are sequential!
enum MsgCreator : uint8_t {
    MsgCreatorLowBound = 99,
    MsgCreatorTwoPc = 100,
    MsgCreatorNextXid = 101,
    MsgCreatorMsgGetDataUsingFatptr = 102,
    MsgCreatorMsgRpcCaller = 103,
    MsgCreatorRecvDataFromSocket = 104,
    MsgCreatorMsgBarrierTwoPc = 105,
    MsgCreatorUsrIndexActual = 106,
    MsgCreatorFreed = 107,
    MsgCreatorHighBound = 108,
};

enum TwoPcCmdOffset {
    TwoPcCmdCompletionOffset = 1,
};

// These reasons are passed to some of the functions along the twoPC path.
// All possible reasons should be listed here even if the call to the function
// does not pass the corresponding value.
enum MsgCallReason : uint32_t {
    MsgCallReasonInvalid,
    MsgCallImmWork,
    MsgCallLocalWork,
    MsgCallLocalComp,
    MsgCallLastEnum
};

static constexpr uint64_t TwoPcAltHandle = 0xc001cafec001cafe;
union TwoPcHandle {
    struct {
        uint64_t msgTypeId : 16;
        uint64_t originNodeId : 8;  // 2pc originated on this node

        // XXX - fix, nodeId size should derive from HardMaxNodes or MaxNodes
        // Some 2pc cmds may be issued to only one node, that is, allNodes
        // is set to 1. When this happens the remoteNodeId is valid. When
        // inefficient use of 2pc is made remoteNodeId could be myNodeId.
        uint64_t remoteNodeId : 8;

        uint64_t index : 16;
        uint64_t unused : 14;
        uint64_t allNodes : 1;  // 2pc cmd issued to all nodes?
        uint64_t sync : 1;      // 2pc cmd sync?
    };

    uint64_t twoPcHandle;
};

// This is the only data that users of uk functionality have access to.
struct MsgEphemeral {
    // twoPc() caller's payload, if any.
    void *payloadToDistribute;

    // This is the "cookie" passed by the user. It will be left untouched
    // by the uk and available to the user for whetever.
    void *ephemeral;

    // semaphore provided by caller for async function
    Semaphore *sem;
    Atomic64 *nodeToWait;

    // Incoming payloadLength, if any, when making the twoPc() call.
    size_t payloadLength;

    // Note: payloadLengthTwoPcToAllocate is only set if there is no
    // payloadToDistribute and payloadLength is 0.
    // Setting payloadLengthTwoPcToAllocate means that twoPc must internally
    // allocate a payload even though there is none coming into the call.
    // It cannot exceed MsgMaxPayloadSize.
    // XXX - fix, need to get rid of this and have the user allocate
    // the payload they need. This is going to be the final step in
    // buffer management for twoPc().
    size_t payloadLengthTwoPcToAllocate;

    // Txn, if the 2PC initiator has one. Otherwise, set to XidInvalid. This
    // is propagated to the receiver and any worker threads the receiver
    // creates.
    Txn txn;

    Status status;

    // funcIndex is used to invoke potentially 4 functions:
    // 1. The first function needs to be invoked "immediate"
    //    on local nodes rather than being scheduled. It only works if
    //    twoPcNodeProperty == TwoPcSingleNode in the 2pc call. This is a
    //    do-work type of function.
    // 2. The second function to be invoked to do 2pc caller's local or
    //    remote work that is scheduled using schedOneMessage(). This is a
    //    do-work type of function.
    // 3. The 3rd function to be invoked on 2pc local cmd completion when
    //    the work is scheduled using schedOneMessage(). This is a cmd
    //    completion and wrapup kind of function.
    // 4. The 4th function is invoked on receipt of 2pc cmd remote completion.
    //    This is a cmd completion and wrapup kind of function.
    TwoPcCallId twoPcFuncIndex;

    // TwoPcFastPath calls use bufs that are from XDB buf$. TwoPcSlowPath will
    // use malloc() which offers flexibility for non-standard (small or large)
    // bufs for slow path ops.
    // Note: Do not use TwoPcSlowPath for fast path ops as malloc-ing will
    // kill performance! Break your io down into smaller pages and use
    // TwoPcFastPath. Be cognizant of the size of the io when sending
    // data to other nodes and receiving data from other nodes and learn to
    // break it down into smaller pages and ship using SgArray that is
    // supported by msgSend().
    // If sending or receiving just a few bytes of data, use TwoPcSlowPath.
    // XXX - fix, the msgHdr needs to be expanded to 1K or so to ensure that
    // we do not send a few bytes as a second message (that is hdr + payload).
    TwoPcFastOrSlowPath twoPcFastOrSlowPath;

    TwoPcBufLife twoPcBufLife;

    void setAckInfo(Status ackStatus, size_t ackLength)
    {
        this->payloadLength = ackLength;
        this->status = ackStatus;
    }
};

static constexpr size_t IpAddrStrLen = 256;

// Parameters for creating system threads
struct MsgThreadParams {
    char ipAddr[IpAddrStrLen];
    int port;
    NodeId nodeId;
};

struct MsgLogLevelSetInput {
    uint32_t logLevel;
    bool logFlush;          /* obsolete */
    uint32_t logFlushLevel; /* default: no flush */
    /*
     * flush period specified via logFlushPeriod:
     *  > 0: turn ON periodic flushing with period = logFlushPeriod secs
     *   -1: turn OFF periodic flushing
     * else: ignore any other value (i.e. a no-op)
     *
     * periodic flushing applies only for the global level
     */
    int32_t logFlushPeriod; /* -1 by default - i.e. no periodic flushing */
};

typedef XcalarApiSetConfigParamInput MsgSetConfigInput;

// XXX Move to the module.
struct MsgXdfParallelDoInput {
    ScalarGroupIter groupIter;
    XdfAggregateHandlers aggHandler;
    size_t broadcastPacketSize;
    size_t auxiliaryPacketSize;
    uint8_t auxiliaryPacket[0];
};

// XXX Move to the module.
struct MsgXdfParallelDoOutput {
    XdfAggregateAccumulators acc;
};

// XXX Move to the module.
struct MsgXcalarAggregateEvalVariableSubst {
    XdbId srcXdbId;
    XdbId scalarXdbId;
    unsigned numVariables;
    char variableNames[0][DfMaxFieldNameLen + 1];
};

// XXX Move to the module.
struct MsgXcalarAggregateEvalDistributeEvalFn {
    ScalarGroupIter groupIter;
    char fnName[XcalarEvalMaxFnNameLen + 1];
    XdbId outputScalarXdbId;
    size_t auxiliaryPacketSize;
    uint8_t auxiliaryPacket[0];
};

// XXX Move to the module.
struct MsgFatPointerInput {
    Fatptr fatptr;
    char fatptrPrefixName[DfMaxFieldNameLen + 1];
};

// XXX Move to the module.
struct MsgFatPointerOutput {
    MsgFatPointerInput input;
    size_t bytesCopied;
    char data[0];
};

// XXX Move to the module.
struct MsgFatPointerKeysInput {
    Fatptr fatptr;
    char fieldName[DfMaxFieldNameLen + 1];
    unsigned startValueNum;
    size_t valueSize;
};

// XXX Move to the module.
struct MsgFatPointerKeysOutput {
    DfFieldType recordFieldType;
    size_t valueSize;
    uint8_t value[0];
};

// XXX Move to the module.
struct MsgDestroyDatasetOutput {
    Status status;
    uint64_t numStatuses;
    Status statuses[0];
};

#endif  // _MESSAGETYPES_H_
