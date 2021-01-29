// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LOCALMSGTESTS_H
#define LOCALMSGTESTS_H

#include "primitives/Primitives.h"
#include "localmsg/ILocalMsgHandler.h"
#include "runtime/Schedulable.h"
#include "runtime/Semaphore.h"

class LocalMsgTests final : public ILocalMsgHandler
{
  public:
    LocalMsgTests();
    ~LocalMsgTests();

    void onRequestMsg(LocalMsgRequestHandler *reqHandler,
                      LocalConnection *connection,
                      const ProtoRequestMsg *request,
                      ProtoResponseMsg *response) override;
    void onConnectionClose(LocalConnection *connection, Status status) override;
    void refGet() override;
    void refPut() override;

    static Status testSanity();
    static Status testStress();

    static LocalMsgTests *currentInstance;

  private:
    static constexpr unsigned WorkerCount = 1000;
    static constexpr unsigned WorkerIters = 1000;

    class Worker : public Schedulable
    {
      public:
        Worker(LocalMsgTests *test, Semaphore *doneSem);
        ~Worker() override;

        Status init();

        void run() override;
        void done() override;

      private:
        LocalMsgTests *test_;
        Semaphore *doneSem_;
        LocalConnection *conn_;
    };

    void *sanity();
    void *stress();
    void stressWorker();

    Semaphore semWorkers_;

    // Actual test is run on separate thread. Used for coordination with
    // original thread.
    Status doneStatus_;
};

#endif  //  LOCALMSGTESTS_H
