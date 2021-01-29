// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef FUTURE_H
#define FUTURE_H

#include "runtime/Runtime.h"
#include "runtime/Semaphore.h"

template <typename T>
class Promise;

template <typename T>
struct SharedState {
    SharedState(Promise<T> *prom) : sem(0), promise(prom) {}
    ~SharedState() = default;

    // Cleans up this shared state, including its attached promise
    void clean() { promise->resultRetrieved(); }

#ifdef DEBUG
    bool resultAvailable = false;
#endif  // DEBUG
    Semaphore sem;
    Promise<T> *promise = NULL;
    T value;
};

template <typename T>
class Future
{
  public:
    Future() = default;

    explicit Future(SharedState<T> *state) : state_(state) {}

    ~Future()
    {
    // This check is actually not technically necessary. It is possible and
    // valid to do the below(pseudocode):
    //  Future<int> a = async(...);
    //  Future<int> b = a;
    //  a.wait();
    //  b.get();
    // But we disallow this for the sake of bug catching. If there is a use
    // case that warrants the above style of usage, feel free to remove
    // the below assertion.
#ifdef DEBUG
        if (waited_) {
            assert(!state_ && "We must get the value after waiting");
        }
#endif
    }

    // Wait for this future's value to become available
    void wait()
    {
        assert(state_ &&
               "Must have a corresponding Promise which will set this");
#ifdef DEBUG
        assert(!waited_ && "Callers must wait exactly once");
        waited_ = true;
#endif
        state_->sem.semWait();
    }

    // Get the underlying value for the future. This must only be called once,
    // as it frees the underlying state
    T get()
    {
        assert(state_ && "state must not be destructed");
#ifdef DEBUG
        assert(state_->resultAvailable &&
               "Promise must set result before 'get' is invoked");
        assert(waited_ && "Callers must wait exactly once before calling get");
#endif  // DEBUG

        T result = state_->value;

        state_->clean();
        state_ = NULL;

        return result;
    }

    bool valid() { return state_ != NULL; }

  private:
    // Futures are passed by value, but are only valid if they have a
    // corresponding promise attached to them.
    SharedState<T> *state_ = NULL;

    // Catch impatient callers who have not waited for the result
#ifdef DEBUG
    bool waited_ = false;
#endif  // DEBUG
};

#endif  // #FUTURE_H
