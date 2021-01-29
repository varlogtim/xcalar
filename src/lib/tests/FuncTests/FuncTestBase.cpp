// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "StrlFunc.h"
#include "FuncTestBase.h"
#include "util/Atomics.h"
#include "runtime/Runtime.h"
#include "util/MemTrack.h"
#include "dag/DagTypes.h"
#include "dag/DagLib.h"
#include "usr/Users.h"
#include "sys/XLog.h"
#include "util/System.h"

#include "test/QA.h"

static XcalarApiUserId testUserId = {
    "FuncTestBase",
    0xdeadbeef,
};

FuncTestBase::FuncTestBase() : users_(NULL), userCount_(0) {}

FuncTestBase::~FuncTestBase() {}

Status
FuncTestBase::createUserThreads(const char *testName, unsigned count)
{
    Status status = StatusUnknown;
    unsigned ii, jj;

    assert(users_ == NULL);
    assert(userCount_ == 0);

    userCount_ = count;

    users_ = new (std::nothrow) User[userCount_];
    BailIfNull(users_);

    for (ii = 0; ii < userCount_; ii++) {
        users_[ii].index_ = ii;
        users_[ii].parent_ = this;

        status = users_[ii].init(testName);
        if (status != StatusOk) {
            for (jj = 0; jj < ii; jj++) {
                users_[jj].destroy();
            }
            delete[] users_;
            users_ = NULL;
            break;
        }
    }
    BailIfFailed(status);

    for (ii = 0; ii < userCount_; ii++) {
        status = Runtime::get()
                     ->createBlockableThread(&users_[ii].thread_,
                                             &users_[ii],
                                             &FuncTestBase::User::mainWrapper);
        if (status != StatusOk) {
            for (jj = 0; jj < ii; jj++) {
                users_[jj].status_ = StatusFailed;
                users_[jj].startSem_.post();
            }
            break;
        }
    }
    BailIfFailed(status);

    for (ii = 0; ii < userCount_; ii++) {
        users_[ii].startSem_.post();
    }

CommonExit:
    if (status != StatusOk) {
        if (users_ != NULL) {
            for (ii = 0; ii < userCount_; ii++) {
                users_[ii].destroy();
            }
            delete[] users_;
            users_ = NULL;
        }
        userCount_ = 0;
    }
    return status;
}

Status
FuncTestBase::joinUserThreads()
{
    Status status = StatusOk;

    assert(users_ != NULL);

    for (unsigned ii = 0; ii < userCount_; ii++) {
        verify(sysThreadJoin(users_[ii].thread_, NULL) == 0);
        if (users_[ii].status_ != StatusOk && status == StatusOk) {
            status = users_[ii].status_;
        }
        users_[ii].destroy();
    }

    userCount_ = 0;
    return status;
}

Status
FuncTestBase::User::init(const char *testName)
{
    Status status;

    status_ = StatusUnknown;

    verify((size_t) snprintf(id_.userIdName,
                             sizeof(id_.userIdName),
                             "%s%u",
                             testName,
                             index_) < sizeof(id_.userIdName));
    id_.userIdUnique = (uint32_t) hashStringFast(id_.userIdName);

    status = UserMgr::get()->getDag(&id_, NULL, &sessionGraph_);
    if (status == StatusSessionNotFound) {
        status = createSession();
        if (status == StatusSessionExists || status == StatusOk) {
            status = activateSession();
        }
        if (status == StatusOk) {
            status = UserMgr::get()->getDag(&id_, NULL, &sessionGraph_);
        }
    }
    if (status != StatusOk) {
        destroy();
    }
    return status;
}

void
FuncTestBase::User::destroy()
{
    if (sessionGraph_ != NULL) {
        SourceType srcTypes[] = {SrcTable, SrcConstant, SrcExport, SrcDataset};

        for (unsigned ii = 0; ii < ArrayLen(srcTypes); ii++) {
            XcalarApiOutput *output = NULL;
            size_t outputSize;
            Status status = sessionGraph_->bulkDropNodes("*",
                                                         &output,
                                                         &outputSize,
                                                         srcTypes[ii],
                                                         &testUserId);
            assert(status == StatusOk);
            if (output != NULL) {
                memFree(output);
                output = NULL;
            }
        }
    }
}

void *
FuncTestBase::User::mainWrapper()
{
    assert(sessionGraph_ != NULL);

    startSem_.semWait();
    if (status_ != StatusUnknown) {
        return NULL;
    }
    status_ = parent_->userMain(this);
    return NULL;
}

Status
FuncTestBase::User::createSession()
{
    XcalarApiSessionNewInput sessionNewInput;
    XcalarApiSessionNewOutput sessionNewOutput;
    Status status = StatusUnknown;
    UserMgr *userMgr = UserMgr::get();

    strlcpy(sessionNewInput.sessionName,
            id_.userIdName,
            sizeof(sessionNewInput.sessionName));
    sessionNewInput.sessionNameLength = strlen(sessionNewInput.sessionName);
    sessionNewInput.fork = false;
    sessionNewInput.forkedSessionNameLength = 0;

    status = userMgr->create(&id_, &sessionNewInput, &sessionNewOutput);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Thread %u (%s): Could not create session. Status: %s (%s)",
                sysGetTid(),
                id_.userIdName,
                strGetFromStatus(status),
                sessionNewOutput.error);
    }

    return status;
}

Status
FuncTestBase::User::activateSession()
{
    XcalarApiSessionActivateInput sessionActivateInput;
    XcalarApiSessionGenericOutput sessionActivateOutput;
    Status status = StatusUnknown;
    UserMgr *userMgr = UserMgr::get();

    strlcpy(sessionActivateInput.sessionName,
            id_.userIdName,
            sizeof(sessionActivateInput.sessionName));
    sessionActivateInput.sessionNameLength =
        strlen(sessionActivateInput.sessionName);
    sessionActivateInput.sessionId = 0;

    status =
        userMgr->activate(&id_, &sessionActivateInput, &sessionActivateOutput);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Thread %u (%s): Could not activate session. Status: %s (%s)",
                sysGetTid(),
                id_.userIdName,
                strGetFromStatus(status),
                sessionActivateOutput.errorMessage);
    }

    return status;
}
