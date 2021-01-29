// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_STATE_H_
#define _MSGSTREAM_STATE_H_

#include "msgstream/MsgStream.h"
#include "MsgStreamInfo.h"

class MsgStreamState final
{
  public:
    // Public methods.
    static MustCheck MsgStreamState *get();
    static MustCheck Status init();
    void destroy();

    // Public methods.
    MustCheck Status streamHashInsert(MsgStreamInfo *msgStreamInfo);

    MustCheck MsgStreamInfo *streamHashFind(Xid streamId);

    MustCheck MsgStreamInfo *streamHashRemove(Xid streamId);

    // Invoked on the destination node to handle Stream start request.
    MustCheck Status
    startStreamHandlerLocal(MsgStreamMgr::ProtocolDataUnit *pdu);

    // Invoked on the destination node to handle Stream end request.
    MustCheck Status endStreamHandlerLocal(MsgStreamMgr::ProtocolDataUnit *pdu);

    // Invoked on the destination node to receive streams.
    MustCheck Status
    streamReceiveHandlerLocal(MsgStreamMgr::ProtocolDataUnit *pdu);

    // Invoked on the source node to handle Stream start request completions.
    void startStreamCompletionHandler(MsgEphemeral *eph);

    // Invoked on the source node to handle Stream end request completions.
    void endStreamCompletionHandler(MsgEphemeral *eph);

    // Invoked on the source node to handle send stream completions.
    void streamSendCompletionHandler(MsgEphemeral *eph);

    void processSendCompletions(MsgStreamInfo *msgStreamInfo,
                                void *sendContext,
                                Status reason);

    void incStreamStart() { StatsLib::statNonAtomicIncr(cumlStreamStartStat_); }

    void incStreamStartError()
    {
        StatsLib::statNonAtomicIncr(cumlStreamStartErrStat_);
    }

    void incStreamEnd() { StatsLib::statNonAtomicIncr(cumlStreamEndStat_); }

    void incStreamEndError()
    {
        StatsLib::statNonAtomicIncr(cumlStreamEndErrStat_);
    }

    void incStreamSendPayload()
    {
        StatsLib::statNonAtomicIncr(cumlStreamSendPayloadStat_);
    }

    void incStreamSendPayloadError()
    {
        StatsLib::statNonAtomicIncr(cumlStreamSendPayloadErrStat_);
    }

  private:
    static constexpr uint64_t SlotsPerStreamHashTable = 61;
    static MsgStreamState *instance;

    // Lock to serialize access the MsgStream hash table.
    Spinlock streamHashTableLock_;

    // MsgStream hash table.
    IntHashTable<Xid,
                 MsgStreamInfo,
                 &MsgStreamInfo::hook_,
                 &MsgStreamInfo::getStreamId,
                 SlotsPerStreamHashTable,
                 hashIdentity>
        streamHashTable_;

    // Track all the Stream Object's actions.
    StreamObjectActions *streamObjectActions_[static_cast<uint8_t>(
        MsgStreamMgr::StreamObject::StreamObjectMax)];

    static constexpr size_t StatCount = 6;
    StatGroupId statsGrpId_;
    StatHandle cumlStreamStartStat_;
    StatHandle cumlStreamStartErrStat_;
    StatHandle cumlStreamEndStat_;
    StatHandle cumlStreamEndErrStat_;
    StatHandle cumlStreamSendPayloadStat_;
    StatHandle cumlStreamSendPayloadErrStat_;

    // Private methods
    MustCheck Status initStats();
    MustCheck Status initObjectActions();
    void destroyObjectActions();

    MsgStreamState() {}
    ~MsgStreamState() {}
    MsgStreamState(const MsgStreamState &) = delete;
    MsgStreamState &operator=(const MsgStreamState &) = delete;
};

#endif  // _MSGSTREAM_STATE_H_
