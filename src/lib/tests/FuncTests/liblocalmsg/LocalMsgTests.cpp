// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <pthread.h>
#include "test/FuncTests/LocalMsgTests.h"
#include "localmsg/LocalMsg.h"
#include "config/Config.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "common/Version.h"
#include "util/System.h"

#include "test/QA.h"

LocalMsgTests *LocalMsgTests::currentInstance = NULL;

LocalMsgTests::LocalMsgTests() : semWorkers_(0), doneStatus_(StatusUnknown) {}

LocalMsgTests::~LocalMsgTests() {}

////////////////////////////////////////////////////////////////////////////////

//
// Test entry points.
//

Status  // static
LocalMsgTests::testSanity()
{
    Status status;
    pthread_t thread;

    LocalMsgTests localMsgTests;
    currentInstance = &localMsgTests;

    status = Runtime::get()->createBlockableThread(&thread,
                                                   &localMsgTests,
                                                   &LocalMsgTests::sanity);
    if (status != StatusOk) {
        return status;
    }

    verify(sysThreadJoin(thread, NULL) == 0);

    currentInstance = NULL;
    return localMsgTests.doneStatus_;
}

Status
LocalMsgTests::testStress()
{
    Status status;
    pthread_t thread;

    LocalMsgTests localMsgTests;
    currentInstance = &localMsgTests;

    status = Runtime::get()->createBlockableThread(&thread,
                                                   &localMsgTests,
                                                   &LocalMsgTests::stress);
    if (status != StatusOk) {
        return status;
    }

    verify(sysThreadJoin(thread, NULL) == 0);

    currentInstance = NULL;
    return localMsgTests.doneStatus_;
}

////////////////////////////////////////////////////////////////////////////////

//
// Actual test coordination logic.
//

void *
LocalMsgTests::sanity()
{
    Status status;
    LocalConnection *connection;
    status = LocalMsg::get()->connect(LocalMsg::get()->getSockPath(),
                                      NULL,
                                      &connection);
    assert(status == StatusOk);

    ProtoApiRequest apiRequest;
    apiRequest.set_func(ApiFuncGetVersion);
    ProtoRequestMsg request;
    request.set_target(ProtoMsgTargetApi);
    request.set_allocated_api(&apiRequest);

    LocalConnection::Response *response;
    status = connection->sendRequest(&request, &response);
    assert(status == StatusOk);
    request.release_api();

    status = response->wait(USecsPerSec);
    assert(status == StatusOk);

    const ProtoResponseMsg *responseMsg = response->get();
    assert(responseMsg->status() == StatusOk.code());
    verify(strcmp(responseMsg->api().version().c_str(), versionGetFullStr()) ==
           0);
    response->refPut();
    LocalMsg::get()->disconnect(connection);

    doneStatus_ = status;
    return NULL;
}

void *
LocalMsgTests::stress()
{
    Status status;
    unsigned ii;

    for (ii = 0; ii < WorkerCount; ii++) {
        auto worker =
            new (std::nothrow) LocalMsgTests::Worker(this, &semWorkers_);
        assert(worker != NULL);

        status = worker->init();
        assert(status == StatusOk);

        status = Runtime::get()->schedule(worker);
        assert(status == StatusOk);
    }

    for (ii = 0; ii < WorkerCount; ii++) {
        semWorkers_.semWait();
    }

    doneStatus_ = status;
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////

//
// Test workers. Many running in parallel communicating to this XCE node.
//

LocalMsgTests::Worker::Worker(LocalMsgTests *test, Semaphore *doneSem)
    : Schedulable("LocalMsgTests::Worker"),
      test_(test),
      doneSem_(doneSem),
      conn_(NULL)
{
}

LocalMsgTests::Worker::~Worker() {}

Status
LocalMsgTests::Worker::init()
{
    Status status;

    Runtime::get()->assertThreadBlockable();

    status =
        LocalMsg::get()->connect(LocalMsg::get()->getSockPath(), NULL, &conn_);
    assert(status == StatusOk);

    return status;
}

void
LocalMsgTests::Worker::run()
{
    Status status;

    for (unsigned ii = 0; ii < WorkerIters; ii++) {
        ProtoTestRequest testRequest;
        testRequest.set_testint(1);
        testRequest.set_teststring("hello");

        ProtoRequestMsg request;
        request.set_requestid(0);
        request.set_target(ProtoMsgTargetTest);
        request.set_allocated_test(&testRequest);

        LocalConnection::Response *response;
        status = conn_->sendRequest(&request, &response);
        request.release_test();
        assert(status == StatusOk);

        status = response->wait(3600 * USecsPerSec);
        assert(status == StatusOk);

        assert(response->get()->status() == 0);
        assert(strcmp(response->get()->error().c_str(), "all's good") == 0);

        response->refPut();
    }

    LocalMsg::get()->disconnect(conn_);
}

void
LocalMsgTests::Worker::done()
{
    doneSem_->post();
}

////////////////////////////////////////////////////////////////////////////////

void
LocalMsgTests::onRequestMsg(LocalMsgRequestHandler *reqHandler,
                            LocalConnection *connection,
                            const ProtoRequestMsg *request,
                            ProtoResponseMsg *response)
{
    assert(request->test().testint() == 1);
    assert(strcmp(request->test().teststring().c_str(), "hello") == 0);

    response->set_status(StatusOk.code());
    response->set_error("all's good");
}

void
LocalMsgTests::onConnectionClose(LocalConnection *connection, Status status)
{
}

void
LocalMsgTests::refGet()
{
}

void
LocalMsgTests::refPut()
{
}
