// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include <new>
#include "udf/UdfPython.h"
#include "UdfPythonImpl.h"
#include "UdfPyChild.h"

UdfPython *UdfPython::instance = NULL;  // static

UdfPython::UdfPython() : impl(NULL) {}

UdfPython::~UdfPython()
{
    assert(impl == NULL);
}

Status  // static
UdfPython::init()
{
    Status status;
    bool implInited = false;

    instance = new (std::nothrow) UdfPython;
    BailIfNull(instance);

    instance->impl = new (std::nothrow) UdfPythonImpl;
    BailIfNull(instance->impl);

    status = instance->impl->init();
    BailIfFailed(status);
    implInited = true;

    status = udfPyChildInit(instance->impl);

CommonExit:
    if (status != StatusOk) {
        if (instance != NULL) {
            if (instance->impl != NULL) {
                if (implInited) {
                    instance->impl->destroy();
                }
                delete instance->impl;
                instance->impl = NULL;
            }
            delete instance;
            instance = NULL;
        }
    }
    return status;
}

void
UdfPython::destroy()
{
    udfPyChildDestroy();
    instance->impl->destroy();
    delete instance->impl;
    instance->impl = NULL;
    delete this;
    instance = NULL;
}

UdfPython *  // static
UdfPython::get()
{
    return instance;
}
