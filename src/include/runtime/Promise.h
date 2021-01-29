// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef PROMISE_H
#define PROMISE_H

// We need std for std::enable_if, otherwise we are going out of our way to
// avoid using std
#include <functional>

#include "runtime/Future.h"
#include "runtime/Schedulable.h"

// This is actually general purpose template metaprogramming stuff, but we are
// quarantining it here because it can be very confusing, leading to abuse.
// Additionally, this can drastically affect compile times if it is used too
// aggressively (like any template stuff).
// The below are inspired by
// https://eli.thegreenplace.net/2014/variadic-templates-in-c/ which is listed
// as in the public domain https://eli.thegreenplace.net/pages/about

// *** ArgTuple ***
// ArgTuple is similar to std::tuple

template <typename... Ts>
struct ArgTuple {
};

template <typename T, typename... Ts>
struct ArgTuple<T, Ts...> : ArgTuple<Ts...> {
    ArgTuple(T t, Ts... ts) : ArgTuple<Ts...>(ts...), tail(t) {}

    T tail;
};

// *** TypeHolder ***
// This is a helper to enable us to implement argTupleGet

template <size_t, typename>
struct TypeHolder;

// Base case for TypeHolder
template <typename T, typename... Ts>
struct TypeHolder<0, ArgTuple<T, Ts...>> {
    // This causes this type to stick around and be referancable later
    typedef T type;
};

// Inductive case
template <size_t k, typename T, typename... Ts>
struct TypeHolder<k, ArgTuple<T, Ts...>> {
    typedef typename TypeHolder<k - 1, ArgTuple<Ts...>>::type type;
};

// *** argTupleGet ***
// argTupleGet is similar to std::get
// It returns the value of a ArgTuple

// Base case for argTupleGet<0>()
template <size_t k, typename... Ts>
typename std::enable_if<k == 0,
                        typename TypeHolder<0, ArgTuple<Ts...>>::type&>::type
argTupleGet(ArgTuple<Ts...>& t)
{
    return t.tail;
}

// Inductive case for argTupleGet<k>()
template <size_t k, typename T, typename... Ts>
typename std::enable_if<k != 0,
                        typename TypeHolder<k, ArgTuple<T, Ts...>>::type&>::type
argTupleGet(ArgTuple<T, Ts...>& t)
{
    ArgTuple<Ts...>& base = t;
    return argTupleGet<k - 1>(base);
}

// *** IntSequence ***
// IntSequence is similar to std::integer_sequence or std::index_sequence
// This generates a sequence like IntSequence<0,1,2,3,N>
// https://stackoverflow.com/a/7858971

template <int...>
struct IntSequence {
};

// Inductive case
// GenSeq<3> is GenSeq<2,2,3>
template <int N, int... Ns>
struct GenSeq : GenSeq<N - 1, N - 1, Ns...> {
};

// The base case removed the first argument so
// GenSeq<0,0,1,2,...> is IntSequence<0,1,2,...>
template <int... S>
struct GenSeq<0, S...> {
    typedef IntSequence<S...> type;
};

// End of general purpose stuff

// This Promise interface defines how Futures and SharedStates can interact
// with a Promise. This is needed because Schedulables don't have all the
// methods that a Promise has.
template <typename Ret>
class Promise : public Schedulable
{
    // Allocate the shared state along with the promise. The contract here is
    // that the shared state will free this promise when it gets freed
    SharedState<Ret> state_ = NULL;
    Spinlock lock_;
    bool finishedRun_ = false;
    bool finishedGet_ = false;

  public:
    Promise() : Schedulable("Promise"), state_(this) {}

    ~Promise() override { assert(finishedRun_ && finishedGet_); }

    void run() override
    {
        state_.value = invoke();
#ifdef DEBUG
        state_.resultAvailable = true;
#endif
        state_.sem.post();
    }

    void done() override
    {
        lock_.lock();
        assert(finishedRun_ == false);

        finishedRun_ = true;
        bool shouldDelete = finishedGet_;
        lock_.unlock();

        if (shouldDelete) {
            delete this;
        }
    }

    // All types of Promises need to specialize their invoke function
    virtual Ret invoke() = 0;

    void resultRetrieved()
    {
        lock_.lock();
        assert(finishedGet_ == false);

        finishedGet_ = true;
        bool shouldDelete = finishedRun_;
        lock_.unlock();

        if (shouldDelete) {
            delete this;
        }
    }

    Future<Ret> getFuture() { return Future<Ret>(&state_); }
};

template <typename Ret, typename F, typename... Args>
class FuncPromise : public Promise<Ret>
{
    F func_;
    ArgTuple<Args...> args_;

  public:
    FuncPromise(F func, Args... args) : func_(func), args_(args...) {}

    ~FuncPromise() override = default;

    Ret invoke() override
    {
        return callFunc(typename GenSeq<sizeof...(Args)>::type());
    }

    // Call the provided function. This takes as an argument the IntSequence
    // like IntSequence<0,1,2,...> for as many arguments are there are.
    template <int... S>
    Ret callFunc(IntSequence<S...>)
    {
        // Unpack the arguments from the ArgTuple into the function call
        return func_(argTupleGet<S>(args_)...);
    }
};

// This is the same thing as a Promise, but for an instance method
template <typename Ret, typename I, typename M, typename... Args>
class ObjectPromise : public Promise<Ret>
{
    I* instance_;
    M method_;
    ArgTuple<Args...> args_;

  public:
    ObjectPromise(I* instance, M method, Args... args)
        : instance_(instance), method_(method), args_(args...)
    {
    }

    ~ObjectPromise() override = default;

    Ret invoke() override
    {
        return callFunc(typename GenSeq<sizeof...(Args)>::type());
    }

    // Call the provided function. This takes as an argument the IntSequence
    // like IntSequence<0,1,2,...> for as many arguments are there are.
    template <int... S>
    Ret callFunc(IntSequence<S...>)
    {
        // Unpack the arguments from the ArgTuple into the function call
        return (instance_->*(method_))(argTupleGet<S>(args_)...);
    }
};

#endif  // #PROMISE_H
