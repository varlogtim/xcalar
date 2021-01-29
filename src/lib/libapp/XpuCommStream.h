// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef XPU_COMM_STREAM_H
#define XPU_COMM_STREAM_H

#include "AppGroupMgr.h"

class LocalMsgRequestHandler;

class XpuCommObjectAction : public StreamObjectActions
{
    friend class AppGroup;

  public:
    XpuCommObjectAction();
    virtual ~XpuCommObjectAction();

    // Return the StreamObject that is being specializing here.
    virtual MustCheck MsgStreamMgr::StreamObject getStreamObject() const;

    // Invoked on the destination node to handle Stream start request.
    virtual MustCheck Status startStreamLocalHandler(Xid streamId,
                                                     void **retObjectContext);

    // Invoked on the destination node to handle Stream end request.
    virtual MustCheck Status endStreamLocalHandler(Xid streamId,
                                                   void *objectContext);

    // Invoked on the destination node to handle Streams received.
    virtual MustCheck Status
    streamReceiveLocalHandler(Xid streamId,
                              void *objectContext,
                              MsgStreamMgr::ProtocolDataUnit *payload);

    // Invoked on the source node.
    virtual void streamSendCompletionHandler(Xid streamId,
                                             void *objectContext,
                                             void *sendContext,
                                             Status reason);

    static MustCheck Status
    sendBufferToDstXpu(AppGroup::Id appGroupId,
                       unsigned xpuSrcId,
                       unsigned xpuDstId,
                       AppInstance::BufDesc *sendBuf,
                       size_t sendBufCount,
                       LocalMsgRequestHandler *reqHandler);

  private:
    static constexpr const char *ModuleName = "XpuCommObjectAction";

    struct XpuCommBuffer {
        static constexpr const size_t SeqNumInvalid = (size_t)(-1);
        static constexpr const size_t TotalBufsInvalid = (size_t)(-2);
        static constexpr const size_t TotalBufsSizeInvalid = (size_t)(-3);
        static constexpr const size_t OffsetInvalid = (size_t)(-4);
        static constexpr const bool IsAck = true;
        static constexpr const bool IsNotAck = false;

        // AppGroup for which this buffer is destined.
        AppGroup::Id appGroupId;

        // Assume that there could be concurrent XPU<->XPU streams for the same
        // AppGroup. So commId uniquely identifies an open XPU<->XPU
        // communication stream.
        Xid commId;

        // Since XPU<->XPU stream buffer may be transported in multiple pieces,
        // and the pieces can get reordered, this uniquely identifies a piece of
        // buffer in the stream with commId.
        size_t seqNum;

        // Total number of pieces to be transported.
        size_t totalBufs;

        // Total number of bytes to be transported.
        size_t totalBufsSize;

        // For a given buffer sequence number, we record the offset of this
        // piece of buffer in the entire stream.
        size_t offset;

        // Source XPU
        unsigned srcXpu;

        // Destination XPU
        unsigned dstXpu;

        // Set to true if this is an ACK.
        bool ack;

        // Actual buffer being transported.
        uint8_t data[0];

        XpuCommBuffer(AppGroup::Id appGroupId,
                      Xid commId,
                      size_t seqNum,
                      size_t totalBufs,
                      size_t totalBufsSize,
                      size_t offset,
                      unsigned srcXpu,
                      unsigned dstXpu,
                      bool ack)
            : appGroupId(appGroupId),
              commId(commId),
              seqNum(seqNum),
              totalBufs(totalBufs),
              totalBufsSize(totalBufsSize),
              offset(offset),
              srcXpu(srcXpu),
              dstXpu(dstXpu),
              ack(ack)
        {
        }

        ~XpuCommBuffer() = default;
    };

    MustCheck Status sendAckToSrcXpu(
        AppGroup *appGroup, AppGroup::DstXpuCommStream *dstXpuCommStream);

    void srcXpuStreamSendCompletionHandler(Xid streamId,
                                           void *objectContext,
                                           void *sendContext,
                                           Status reason);

    void dstXpuStreamSendCompletionHandler(Xid streamId,
                                           void *objectContext,
                                           void *sendContext,
                                           Status reason);

    MustCheck Status
    srcXpuStreamReceiveLocalHandler(Xid streamId,
                                    void *objectContext,
                                    MsgStreamMgr::ProtocolDataUnit *payload);

    MustCheck Status
    dstXpuStreamReceiveLocalHandler(Xid streamId,
                                    void *objectContext,
                                    MsgStreamMgr::ProtocolDataUnit *payload);
};
#endif  // XPU_COMM_STREAM_H
