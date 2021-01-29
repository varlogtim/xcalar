// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "ns/LibNs.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "config/Config.h"
#include "ns/LibNsStreamActions.h"
#include "ns/LibNsTypes.h"

static constexpr char const *moduleName = "libNsStream";

// This function is called on the node receiving a twoPc request asking for
// path info.  If the receiving node is the same as the sending node then
// the path info is simply inserted into the results.
//
// If the receiving node is not the sending node then we'll use the message
// streaming infrastructure to send back the results.  The caller has
// provided path info whose size could be larger than the maximum twoPc payload
// size.  If that is the case then the path info is broken up into a chunk
// at a time and copied into an allocated buffer, with associated header info,
// and streamed across.
//
// On the receiving side, the stream receive handler, will allocate a buffer
// for the payload, copy into that buffer, and then insert it into the results.
//
// Once all results have been received they are combined into yet another
// allocated buffer and returned to the caller.

Status
LibNs::sendViaStream(NodeId destNodeId,
                     MsgEphemeral *ephIn,
                     void *pathInfo,
                     size_t infoSize)
{
    Status status = StatusOk;
    MsgStreamMgr *msgStreamMgr = MsgStreamMgr::get();
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();

    if (destNodeId == myNodeId) {
        // No need to stream.  Just insert into the results.

        status = addStreamData(ephIn->ephemeral, pathInfo, infoSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to insert local stream results: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

    } else {
        // Use the message streaming infrastructure to send the data.
        Xid streamId = XidInvalid;
        size_t sendSize = infoSize;
        size_t offsetInPathInfo = 0;
        MsgStreamMgr::StreamState streamState;
        size_t payloadLen;
        MsgStreamMgr::ProtocolDataUnit *payload = NULL;
        size_t maxChunkSize =
            (MsgMgr::getMsgMaxPayloadSize() -
             sizeof(MsgStreamMgr::ProtocolDataUnit) - sizeof(ephIn->ephemeral));
        maxChunkSize -= (maxChunkSize % sizeof(LibNsTypes::PathInfoOut));
        uint64_t sendCount =
            (sendSize / maxChunkSize) + ((sendSize % maxChunkSize) ? 1 : 0);
        uint64_t ii = 0;
        LibNsStreamObjectAction::SendSideCookie *sendSideCookie =
            (LibNsStreamObjectAction::SendSideCookie *)
                memAllocExt(sizeof(LibNsStreamObjectAction::SendSideCookie),
                            moduleName);
        if (sendSideCookie == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Unable to allocate %lu bytes for sendSideCookie",
                    sizeof(LibNsStreamObjectAction::SendSideCookie));
            status = StatusNoMem;
            goto EndStream;
        }

        // The cookie is initialized with the size of the info we're trying
        // to stream.  The sent/received accounting done in the handlers
        // must take into account there's overhead (PDU + Eph) associated
        // with each transfer.
        new (sendSideCookie) LibNsStreamObjectAction::SendSideCookie(sendCount);

        // Start the stream
        status =
            msgStreamMgr
                ->startStream(TwoPcSingleNode,
                              destNodeId,
                              MsgStreamMgr::StreamObject::LibNsPathInfoPerNode,
                              (void *) sendSideCookie,
                              &streamId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "startStream from node %u to node %u failed: %s",
                    myNodeId,
                    destNodeId,
                    strGetFromStatus(status));
            goto EndStream;
        }

        // The stream has been starting.  From this point any errors will
        // lead to error recovery (orderly teardown of the stream).

        statsIncStreamedCnt();
        statsAddStreamedBytes(sendSize);

        while (sendSize > 0) {
            ii++;

            // Figure out how much data can we send in this iteration.  Have to
            // leave room for the ephemeral and the PDU.  In addition the chunks
            // that are sent may arrive on the destination in a different order.
            // We don't care about the order but have to ensure that we don't
            // split across a record.
            assert((sendSize % sizeof(LibNsTypes::PathInfoOut)) == 0);

            size_t chunkSize =
                xcMin(sendSize,
                      MsgMgr::getMsgMaxPayloadSize() -
                          sizeof(MsgStreamMgr::ProtocolDataUnit) -
                          sizeof(ephIn->ephemeral));

            // Round it down to a record size increment
            chunkSize -= (chunkSize % sizeof(LibNsTypes::PathInfoOut));

            payloadLen = sizeof(MsgStreamMgr::ProtocolDataUnit) +
                         sizeof(ephIn->ephemeral) + chunkSize;

            // XXX Consider reusing the same payload memory for sending all
            // stream info.
            payload =
                (MsgStreamMgr::ProtocolDataUnit *) memAllocExt(payloadLen,
                                                               moduleName);
            if (payload == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Stream Id %lu Unable to allocate %lu bytes for"
                        " ProtocolDataUnit",
                        streamId,
                        payloadLen);
                status = StatusNoMem;
                break;
            }

            // The first part of the payload is for the PDU which gets filled
            // in via initPayloadToSend.  The second part is the ephemeral
            // associated witht the request that was passed to us from the
            // node originating the request.  Lastly is the data.

            void *src = (void *) &ephIn->ephemeral;
            void *dest = (void *) ((uintptr_t) payload +
                                   sizeof(MsgStreamMgr::ProtocolDataUnit));
            memcpy(dest, src, sizeof(ephIn->ephemeral));

            src = (void *) ((uintptr_t) pathInfo + offsetInPathInfo);
            dest = (void *) ((uintptr_t) payload +
                             sizeof(MsgStreamMgr::ProtocolDataUnit) +
                             sizeof(ephIn->ephemeral));
            memcpy(dest, src, chunkSize);

            offsetInPathInfo += chunkSize;
            sendSize -= chunkSize;

            status =
                msgStreamMgr->initPayloadToSend(streamId, payload, payloadLen);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Stream Id %lu error initializing stream payload: %s",
                        streamId,
                        strGetFromStatus(status));
                break;
            }

            streamState = MsgStreamMgr::StreamState::StreamInProgress;

            status =
                msgStreamMgr->sendStream(streamId, payload, streamState, NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Stream Id %lu sendstream failure from node %u to"
                        " %u: %s",
                        streamId,
                        myNodeId,
                        destNodeId,
                        strGetFromStatus(status));
                break;
            }

            assert(payload != NULL);
            memFree(payload);
            payload = NULL;
        }
        verify(ii <= sendCount);

        if (payload != NULL) {
            assert(status != StatusOk);
            memFree(payload);
            payload = NULL;
        }

        // Wait for the completions
        sendSideCookie->semDone_.semWait();
        assert(sendSideCookie->streamDone_ == true);
        assert(sendSideCookie->sendCount_ ==
               (uint64_t) atomicRead64(&sendSideCookie->receiveCount_));
        if (status == StatusOk) {
            status.fromStatusCode(
                (StatusCode) atomicRead32(&sendSideCookie->streamStatus_));
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Stream Id %lu sendstream completions failed from"
                    " node %u to %u: %s",
                    streamId,
                    myNodeId,
                    destNodeId,
                    strGetFromStatus(status));
            goto EndStream;
        }

        // Now send the stream done.  This has to be sent after the data has
        // completed going across as there are no ordering guarantees.
        payloadLen =
            sizeof(MsgStreamMgr::ProtocolDataUnit) + sizeof(ephIn->ephemeral);
        assert(payload == NULL);
        payload = (MsgStreamMgr::ProtocolDataUnit *) memAllocExt(payloadLen,
                                                                 moduleName);
        if (payload == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Stream Id %lu unable to allocate %lu bytes for"
                    " ProtocolDataUnit (Done)",
                    streamId,
                    payloadLen);
            status = StatusNoMem;
            goto EndStream;
        }

        // Reinitialize our cookie - no "real" data...just PDU + Eph
        new (sendSideCookie) LibNsStreamObjectAction::SendSideCookie(1);

        status = msgStreamMgr->initPayloadToSend(streamId, payload, payloadLen);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Stream Id %lu error initializing stream payload"
                    " (Done): %s",
                    streamId,
                    strGetFromStatus(status));
            goto EndStream;
        }

        // Send the "done" message.
        // XXX: Consider tagging the "last" chunk as done in order to avoid
        // sending another message.
        streamState = MsgStreamMgr::StreamState::StreamIsDone;

        status = msgStreamMgr->sendStream(streamId, payload, streamState, NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Stream Id %lu sendstream (Done) failure from node %u"
                    " to %u: %s",
                    streamId,
                    myNodeId,
                    destNodeId,
                    strGetFromStatus(status));
            goto EndStream;
        }
        assert(payload != NULL);
        memFree(payload);
        payload = NULL;

        // Wait for the "done" completion
        sendSideCookie->semDone_.semWait();
        assert(sendSideCookie->streamDone_ == true);
        assert(sendSideCookie->sendCount_ ==
               (uint64_t) atomicRead64(&sendSideCookie->receiveCount_));
        if (status == StatusOk) {
            status.fromStatusCode(
                (StatusCode) atomicRead32(&sendSideCookie->streamStatus_));
        }
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Stream Id %lu sendstream (Done) completions failed from"
                    " node %u to %u: %s",
                    streamId,
                    myNodeId,
                    destNodeId,
                    strGetFromStatus(status));
            goto EndStream;
        }

    EndStream:
        if (payload != NULL) {
            assert(status != StatusOk);
            memFree(payload);
            payload = NULL;
        }

        if (sendSideCookie != NULL) {
            memFree(sendSideCookie);
            sendSideCookie = NULL;
        }

        if (streamId != XidInvalid) {
            msgStreamMgr->endStream(streamId);
            streamId = 0;
        }
    }

CommonExit:

    return status;
}

//
// LibNsStreamObjectAction Implementation
//
LibNsStreamObjectAction::LibNsStreamObjectAction() {}

LibNsStreamObjectAction::~LibNsStreamObjectAction() {}

MsgStreamMgr::StreamObject
LibNsStreamObjectAction::getStreamObject() const
{
    return MsgStreamMgr::StreamObject::LibNsPathInfoPerNode;
}

// This function is called on the destination node to handle the start
// of a stream.
Status
LibNsStreamObjectAction::startStreamLocalHandler(Xid streamId,
                                                 void **retObjectContext)
{
#ifdef LIBNS_DEBUG
    xSyslog(moduleName, XlogDebug, "starting stream (ID: %lu)", streamId);
#endif

    return StatusOk;
}

// This function is called on the destination node to handle the completion
// of the stream.
Status
LibNsStreamObjectAction::endStreamLocalHandler(Xid streamId,
                                               void *retObjectContext)
{
#ifdef LIBNS_DEBUG
    xSyslog(moduleName, XlogDebug, "ending stream (ID: %lu)", streamId);
#endif

    return StatusOk;
}

// This function is called on the destination node to handle the reception
// of a payload.  Keep in mind that the sending side may be doing multiple
// sends which may be arriving in arbitrary order.  We don't care about the
// order as long as there are no boundary splits across records.
Status
LibNsStreamObjectAction::streamReceiveLocalHandler(
    Xid streamId,
    void *retObjectContext,
    MsgStreamMgr::ProtocolDataUnit *payload)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    uint32_t dataSize = payload->bufferSize_ -
                        sizeof(MsgStreamMgr::ProtocolDataUnit) -
                        sizeof(uintptr_t);

    if (dataSize == 0) {
        // No data.  This was a stream done message.
        return status;
    }

    // Handle the received payload.  The first part is the ephemeral
    // associated with this stream.  The second part is the data.

    // The ephemeral is in the first 8 bytes of the payload->buffer_.
    void *ephemeral =
        (void *) (*((uintptr_t *) ((uintptr_t) payload->buffer_)));

    // The results follow the ephemeral.
    LibNsTypes::PathInfoOut *pathInfo =
        (LibNsTypes::PathInfoOut *) ((uintptr_t) payload->buffer_ +
                                     sizeof(uintptr_t));

    status = libNs->addStreamData(ephemeral, pathInfo, dataSize);

    return status;
}

// This function is called on the source node on completion of each payload.
// When completions for all of the payloads are received we'll post that fact
// to the associated semaphore.
void
LibNsStreamObjectAction::streamSendCompletionHandler(Xid streamId,
                                                     void *retObjectContext,
                                                     void *sendContext,
                                                     Status reason)
{
    SendSideCookie *sendSideCookie = (SendSideCookie *) retObjectContext;
    if (reason != StatusOk) {
        atomicWrite32(&sendSideCookie->streamStatus_, reason.code());
    }
    if (sendSideCookie->streamDone_ == false) {
        if (sendSideCookie->sendCount_ ==
            (uint64_t) atomicInc64(&sendSideCookie->receiveCount_)) {
            sendSideCookie->streamDone_ = true;
            sendSideCookie->semDone_.post();
        }
    } else {
        assert(sendSideCookie->sendCount_ ==
               (uint64_t) atomicRead64(&sendSideCookie->receiveCount_));
        sendSideCookie->semDone_.post();
    }
}
