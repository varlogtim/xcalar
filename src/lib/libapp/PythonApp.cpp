// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include "PythonApp.h"
#include "runtime/Runtime.h"
#include "UdfPythonImpl.h"
#include "util/MemTrack.h"
#include "udf/UdfPython.h"
#include "strings/String.h"
#include "child/Child.h"
#include "sys/XLog.h"

PythonApp::PythonApp() : fn_(NULL), inStr_(NULL), appName_(NULL) {}

PythonApp::~PythonApp() {}

void
PythonApp::destroy()
{
    // Try not to teardown until PythonApp resources have been cleaned up.
    unsigned ii;
    for (ii = 0; ii < 10; ii++) {
        if (fn_ == NULL) {
            break;
        }
        sysUSleep(TimeoutUSecsDestroy / 10);
    }

    if (ii == 10) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to cleanly teardown "
                "PythonApp. Tearing down with execution still in progress");
        xcalarExit(StatusBusy.code());
    }
}

//
// Methods for starting and running an App.
//

void
PythonApp::startAsync(const ChildAppStartRequest &startArgs,
                      ProtoResponseMsg *response)
{
    Status status;
    UdfError error;
    Child *child = Child::get();
    pthread_attr_t attr;
    int ret;
    UdfPythonImpl *udfPy = UdfPython::get()->getImpl();

    child->setAppContext(startArgs);

    lock_.lock();

    assert(inStr_ == NULL && "Enforced by parent");
    inStr_ = strAllocAndCopy(startArgs.instr().c_str());
    BailIfNull(inStr_);

    assert(appName_ == NULL && "Enforced by parent");
    appName_ = strAllocAndCopy(startArgs.appname().c_str());
    BailIfNull(appName_);

    status = udfPy->parseMainFunction(startArgs.exec().c_str(),
                                      startArgs.appname().c_str(),
                                      &fn_,
                                      &error);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to load app module %s: %s",
                appName_,
                error.message_);
        assert(fn_ == NULL);
        if (error.message_ != NULL) {
            try {
                response->set_error(error.message_);
            } catch (std::exception &e) {
                // At least we got the status.
                xSyslog(ModuleName,
                        XlogErr,
                        "Caught protocol buffer exception: %s",
                        e.what());
            }
        }
        goto CommonExit;
    }

    ret = pthread_attr_init(&attr);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "PythonApp::startAsync failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // A thread may either be joinable or detached.  If a thread is
    // joinable, then another thread can call pthread_join(3) to wait for
    // the thread to terminate and fetch its exit status.  Only when a
    // terminated joinable thread has been joined are the last of its
    // resources released back to the system.  When a detached thread
    // terminates, its resources are automatically released back to the
    // system: it is not possible to join with the thread in order to obtain
    // its exit status.  Making a thread detached is useful for some types
    // of daemon threads whose exit status the application does not need to
    // care about.  By default, a new thread is created in a joinable state,
    // unless attr was set to create the thread in a detached state (using
    // pthread_attr_setdetachstate(3)).
    ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "PythonApp::startAsync failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = Runtime::get()->createBlockableThread(&thread_,
                                                   &attr,
                                                   this,
                                                   &PythonApp::run);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "PythonApp::startAsync failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        done();
    }
    lock_.unlock();

    response->set_status(status.code());
}

void *
PythonApp::run()
{
    UdfPythonImpl *udfPy = UdfPython::get()->getImpl();
    ParentAppDoneRequest doneRequest;
    PyObject *pyReturn = NULL;
    bool shouldExit = false;

    UdfError error;

    // Run App. This can take a while.
    xSyslog(ModuleName, XlogInfo, "Starting Python2 App \"%s\"", appName_);
    Status status = fn_->evalApp(inStr_, &pyReturn, &error);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Finished Python2 App \"%s\". Result: %s; Error Msg: \"%s\"",
                appName_,
                strGetFromStatus(status),
                error.message_ ? error.message_ : "None");
    } else {
        xSyslog(ModuleName,
                XlogInfo,
                "Finished Python2 App \"%s\". Result: Success",
                appName_);
    }

    if (status == StatusOk && pyReturn != NULL) {
        udfPy->lockPython();

        // App succeeded. Get output.
        PyObject *pyReturnAsStr = NULL;
        if (PyUnicode_Check(pyReturn)) {
            pyReturnAsStr = pyReturn;
        } else {
            pyReturnAsStr = PyObject_Str(pyReturn);
            if (pyReturnAsStr == NULL) {
                status = StatusUdfUnsupportedType;
            }
        }

        if (pyReturnAsStr != NULL) {
            int64_t strSize;
            char *str = PyUnicode_AsUTF8AndSize(pyReturnAsStr, &strSize);
            if (str != NULL) {
                try {
                    doneRequest.set_outstr(str, strSize);
                } catch (std::exception &e) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Caught protocol buffer exception: %s",
                            e.what());
                    status = StatusNoMem;
                }
            }
        }

        udfPy->unlockPython();
    } else if (status != StatusOk && error.message_ != NULL) {
        try {
            doneRequest.set_errstr(error.message_);
        } catch (std::exception &e) {
            // Only got status.
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s error message dropped due to alloc failure: %s, %s",
                    appName_,
                    error.message_,
                    e.what());
        }
    }

    if (pyReturn != NULL) {
        udfPy->lockPython();
        Py_DECREF(pyReturn);
        pyReturn = NULL;
        udfPy->unlockPython();
    }

    lock_.lock();

    Child::get()->unsetAppContext();

    doneRequest.set_status(status.code());

    ParentAppRequest appRequest;
    ProtoParentRequest parentRequest;

    appRequest.set_allocated_done(&doneRequest);
    parentRequest.set_func(ParentFuncAppDone);
    parentRequest.set_allocated_app(&appRequest);

    status = Child::get()->sendSync(&parentRequest,
                                    LocalMsg::timeoutUSecsConfig,
                                    NULL);
    if (status != StatusOk) {
        // Failed to send "done" message to parent. Get parent's attention by
        // exiting.
        xSyslog(ModuleName,
                XlogErr,
                "App %s failed to notify parent of completion: %s. Exiting",
                appName_,
                strGetFromStatus(status));
        shouldExit = true;
    }

    appRequest.release_done();
    parentRequest.release_app();

    done();
    lock_.unlock();

    if (shouldExit) {
        xcalarExit(status.code());
    }

    return NULL;
}

void
PythonApp::done()
{
    if (inStr_ != NULL) {
        memFree(inStr_);
        inStr_ = NULL;
    }
    if (appName_ != NULL) {
        memFree(appName_);
        appName_ = NULL;
    }
    if (fn_ != NULL) {
        fn_->destroy();
        fn_ = NULL;
    }
    // clear the txn
    Txn::setTxn(Txn());
}
