// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef GVM_H
#define GVM_H

#include "primitives/Primitives.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "gvm/GvmTarget.h"
#include "config/Config.h"
#include "runtime/Semaphore.h"

//
// GVM (Globally Visible Memory)
//

class TwoPcCallIdGvmBroadcast : public TwoPcAction
{
  public:
    TwoPcCallIdGvmBroadcast() {}
    virtual ~TwoPcCallIdGvmBroadcast() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcCallIdGvmBroadcast(const TwoPcCallIdGvmBroadcast &) = delete;
    TwoPcCallIdGvmBroadcast &operator=(const TwoPcCallIdGvmBroadcast &) =
        delete;
};

class Gvm final
{
    friend class TwoPcCallIdGvmDlm;
    friend class TwoPcCallIdGvmBroadcast;

  public:
    static Status init();
    void destroy();
    static Gvm *get();

    // Register targets at runtime.
    void registerTarget(GvmTarget *target);

    struct Payload {
        GvmTarget::Index index;
        uint32_t action;
        size_t inPayloadSize;
        size_t maxOutPayloadSize;
        size_t outPayloadSize;
        char buf[0];

        void init(GvmTarget::Index index, uint32_t action, size_t inputSize)
        {
            this->index = index;
            this->action = action;
            this->inPayloadSize = sizeof(Payload) + inputSize;
            this->maxOutPayloadSize = 0;
            this->outPayloadSize = 0;
        }
    };

    // Entry point to initiate GVM operation.
    MustCheck Status invoke(Payload *input, Status *nodeStatus);
    MustCheck Status invoke(Payload *input);

    MustCheck Status invokeWithOutput(Payload *input,
                                      size_t maxOutputSize,
                                      void **outputPerNode,
                                      size_t *sizePerNode,
                                      Status *nodeStatus);

  private:
    // Different types of broadcast completion.
    enum class CompletionType {
        GatherStatus,
        GatherOutput,
    };

    struct CompletionBlock {
        CompletionType type;
        Status *nodeStatus;
        void **outputPerNode;
        size_t *sizePerNode;
    };

    static Gvm *instance;
    GvmTarget *targets_[GvmTarget::Index::GvmIndexCount];

    MustCheck Status broadcast(Payload *input, Status *nodeStatus);

    Gvm();
    ~Gvm();

    // Disallow.
    Gvm(const Gvm &) = delete;
    Gvm &operator=(const Gvm &) = delete;
};

#endif  // GVM_H
