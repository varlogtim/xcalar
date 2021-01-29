// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef TXN_H
#define TXN_H

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "runtime/Runtime.h"

struct Txn {
    typedef Xid Id;
    static constexpr const Id InvalidId = XidInvalid;

    enum class Mode {
        LRQ,
        NonLRQ,
        Invalid,
    };

    Txn() = default;

    Txn(Id id, Mode mode, Runtime::SchedId rtSchedId, RuntimeType rtType)
        : id_(id), mode_(mode), rtSchedId_(rtSchedId), rtType_(rtType)
    {
    }

    bool operator==(const Txn &other)
    {
        return id_ == other.id_ && mode_ == other.mode_ &&
               rtSchedId_ == other.rtSchedId_ && rtType_ == other.rtType_;
    }

    Id id_ = InvalidId;
    Mode mode_ = Mode::Invalid;
    Runtime::SchedId rtSchedId_ = Runtime::SchedId::MaxSched;
    RuntimeType rtType_ = RuntimeTypeInvalid;

    static MustCheck Txn currentTxn();
    static MustCheck Txn newTxn(Mode mode);
    static MustCheck Txn newTxn(Mode mode, Runtime::SchedId rtSchedId);
    static MustCheck Txn newImmediateTxn();
    MustCheck bool valid() const;
    static void setTxn(Txn txn);
    static bool currentTxnImmediate();
    bool immediate();
};

#endif  // TXN_H
