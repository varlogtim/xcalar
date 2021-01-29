// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _HASH_TREE_INT_H_
#define _HASH_TREE_INT_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "xdb/HashTree.h"

struct DlmMsg {
    enum class Type {
        Invalid,
        Update,
        Max,
    };
    Type type;
    size_t msgSize;
    Status status = StatusOk;

    DlmMsg(Type typeIn, size_t msgSizeIn) : type(typeIn), msgSize(msgSizeIn) {}
    ~DlmMsg() = default;
};

struct UpdateDlmMsg {
    DlmMsg hdr;
    struct Info {
        int64_t commitBatchId;
        HashTreeRefHandle htreeHandle;
    };
    unsigned numUpdates;
    Info info[0];
};

class HashTreeRecord final : public NsObject
{
  public:
    enum class State {
        Invalid,
        Active,
        Inactive,
    };

    enum class Restore {
        Invalid,
        InProgress,
        NotInProgress,
    };

    HashTreeRecord(Xid hashTreeIdIn,
                   State activeStateIn,
                   Restore restoreStateIn,
                   int64_t oldestBatchIdIn,
                   int64_t currentBatchIdIn,
                   XcalarApiUdfContainer *sessionContainerIn)
        : NsObject(sizeof(HashTreeRecord)),
          activeState_(activeStateIn),
          hashTreeId_(hashTreeIdIn),
          restoreState_(restoreStateIn),
          oldestBatchId_(oldestBatchIdIn),
          currentBatchId_(currentBatchIdIn)
    {
        UserDefinedFunction::copyContainers(&sessionContainer_,
                                            sessionContainerIn);
    }

    HashTreeRecord()
        : NsObject(sizeof(HashTreeRecord)),
          activeState_(State::Invalid),
          hashTreeId_(XidInvalid),
          restoreState_(Restore::Invalid),
          oldestBatchId_(HashTree::InvalidBatchId),
          currentBatchId_(HashTree::InvalidBatchId)
    {
        memZero(&sessionContainer_, sizeof(XcalarApiUdfContainer));
    }

    ~HashTreeRecord() = default;

    State activeState_ = State::Invalid;
    Xid hashTreeId_ = XidInvalid;
    Restore restoreState_ = Restore::Invalid;
    int64_t oldestBatchId_ = HashTree::InvalidBatchId;
    int64_t currentBatchId_ = HashTree::InvalidBatchId;
    XcalarApiUdfContainer sessionContainer_;

  private:
    HashTreeRecord(const HashTreeRecord &) = delete;
    HashTreeRecord &operator=(const HashTreeRecord &) = delete;
};

#endif  // _HASH_TREE_INT_H_
