// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#ifndef FUNCTESTBASE_H
#define FUNCTESTBASE_H

#include <pthread.h>

#include "primitives/Primitives.h"
#include "runtime/Semaphore.h"
#include "libapis/LibApisCommon.h"

class FuncTestBase
{
  public:
    struct User {
      public:
        User() : sessionGraph_(NULL), startSem_(0) {}

        Status init(const char *testName);
        void destroy();

        void *mainWrapper();

        unsigned index_;

        XcalarApiUserId id_;
        Dag *sessionGraph_;

        Status status_;
        Semaphore startSem_;
        FuncTestBase *parent_;
        pthread_t thread_;

      private:
        Status createSession();
        Status activateSession();
    };

    FuncTestBase();
    virtual ~FuncTestBase();

    Status createUserThreads(const char *testName, unsigned count);
    Status joinUserThreads();
    unsigned getUserCount() const { return userCount_; }

    virtual Status userMain(User *me) = 0;

  private:
    static constexpr const char *ModuleName = "libFuncTest";

    void userMainWrapper();
    void cleanupUser(User *user);

    User *users_;
    unsigned userCount_;
};

#endif  // FUNCTESTBASE_H
