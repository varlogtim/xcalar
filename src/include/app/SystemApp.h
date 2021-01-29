// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#if !defined(APP_SYSTEMAPP_H)
#define APP_SYSTEMAPP_H

#include "runtime/Schedulable.h"
#include "config/Config.h"

// Base class for a typical Python-based, long-lived, system app that we run
// on the xcalar infra. This component implements most of the generic logic that
// runs in usrnode to spin up a child.
class SystemApp
{
  public:
    virtual ~SystemApp();

    // Returns the name of this application.
    virtual const char *name() const = 0;
    virtual bool isAppEnabled() = 0;

  protected:
    // Called by the base classes to complete initialization and register the
    // task to be run by libruntime.
    static MustCheck Status completeInit(SystemApp *instance,
                                         const char *appInput);

  private:
    std::string *appInputBlob_ = nullptr;

    static constexpr const uint64_t AppCheckTimeoutUsecs = 5000000;

    //  This app is run for a lifetime of a cluster on each node.
    //
    // TODO(Oleg): expand this code to allow concrete apps to select a subset of
    //             nodes where the app runs.
    MustCheck Status run(AppGroup::Id *appGroupId) const;

    class Task : public Schedulable
    {
      public:
        explicit Task(SystemApp *owner);
        void run() override;
        virtual void done() override;

        Semaphore doneSem_;
        SystemApp *owner_;
    };

    Task *task_ = nullptr;

    static constexpr const char *mname_ = "SystemApp";
};

#endif  // APP_SYSTEMAPP_H
