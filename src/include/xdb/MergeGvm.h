// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "gvm/GvmTarget.h"
#include "dag/DagTypes.h"
#include "table/TableNs.h"
#include "operators/Operators.h"

#ifndef _MERGE_GVM_H_
#define _MERGE_GVM_H_

class MergeGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        InitMerge,
        PrepareMerge,
        PostCommitMerge,
        AbortMerge,
    };

    static MergeGvm *get();
    static Status init();
    void destroy();

    Status localHandler(uint32_t action,
                        void *payload,
                        size_t *outputSizeOut) override;
    GvmTarget::Index getGvmIndex() const override;

    struct MergeTableInfo {
        char tableName[XcalarApiMaxTableNameLen + 1];
        TableNsMgr::IdHandle idHandle;
        XdbId xdbId;
        MustCheck Status init(const char *tableName,
                              TableNsMgr::IdHandle *idHandle,
                              XdbId xdbId);
    };

    struct MergeInitInput {
        MergeTableInfo tableInfo[Operators::MergeNumTables];
    };

    struct MergePrepareInput {
        MergeTableInfo tableInfo[Operators::MergeNumTables];
    };

    struct MergePostCommitInput {
        TableNsMgr::IdHandle idHandle;
    };

    struct MergeAbortInput {
        MergeTableInfo tableInfo[Operators::MergeNumTables];
    };

  private:
    static MergeGvm *instance;
    MergeGvm() {}
    ~MergeGvm() {}
    MergeGvm(const MergeGvm &) = delete;
    MergeGvm &operator=(const MergeGvm &) = delete;
};

#endif  // _MERGE_GVM_H_
