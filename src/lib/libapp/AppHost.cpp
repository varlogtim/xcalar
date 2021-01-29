// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include "app/AppHost.h"
#include "PythonApp.h"
#include "app/App.h"
#include "child/Child.h"

AppHost *AppHost::instance = NULL;

AppHost::AppHost() : pythonApp_(NULL) {}

AppHost::~AppHost()
{
    assert(pythonApp_ == NULL);
}

Status  // static
AppHost::init()
{
    instance = new (std::nothrow) AppHost;
    if (instance == NULL) {
        return StatusNoMem;
    }

    Status status;

    instance->pythonApp_ = new (std::nothrow) PythonApp;
    BailIfNull(instance->pythonApp_);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        instance->destroy();
    }
    return status;
}

void
AppHost::destroy()
{
    if (pythonApp_ != NULL) {
        pythonApp_->destroy();
        delete pythonApp_;
        pythonApp_ = NULL;
    }

    instance = NULL;
    delete this;
}

void
AppHost::dispatchStartRequest(const ChildAppStartRequest &startArgs,
                              ProtoResponseMsg *response)
{
    switch (startArgs.hosttype()) {
    case (uint32_t) App::HostType::Python2:
        pythonApp_->startAsync(startArgs, response);
        break;

    default:
        response->set_status(StatusUnimpl.code());
        break;
    }
}
