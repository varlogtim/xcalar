// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "gvm/GvmTarget.h"
#include "xdb/Xdb.h"
#include "xdb/HashTree.h"
#include "libapis/LibApisCommon.h"

#ifndef _LIB_HASHTREE_GVM_H_
#define _LIB_HASHTREE_GVM_H_

class LibHashTreeGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        InitState,
        Create,
        Update,
        Select,
        Coalesce,
        Inactivate,
        Destroy,
        Revert,
        GetSize,
        RefreshSize,
        AddIndex,
        RemoveIndex,
    };

    static LibHashTreeGvm *get();
    static Status init();
    void destroy();

    Status localHandler(uint32_t action,
                        void *payload,
                        size_t *outputSizeOut) override;
    GvmTarget::Index getGvmIndex() const override;

    struct GvmInput {
        XcalarApiTableInput srcTable;
        int64_t batchId;
        bool checkpoint;
        time_t unixTS;

        size_t size;
        size_t numRows;
        bool dropSrc;

        Xid hashTreeId = XidInvalid;
        Xid hashTreeIdUpdated = XidInvalid;

        union {
            unsigned createReason;
            unsigned updateReason;
        };
        char pubTableName[XcalarApiMaxTableNameLen + 1];
    };

    struct GvmSelectInput {
        static size_t getSize(size_t evalInputLen)
        {
            return sizeof(GvmSelectInput) + evalInputLen + 1;
        }

        XcalarApiTableInput joinTable;
        XcalarApiTableInput dstTable;

        int64_t minBatchId;
        int64_t maxBatchId;

        unsigned numColumns;
        XcalarApiRenameMap columns[TupleMaxNumValuesPerRecord];

        Xid hashTreeId = XidInvalid;
        DagTypes::DagId dagId;

        char filterString[XcalarApiMaxEvalStringLen + 1];

        uint64_t limitRows;
        size_t evalInputLen;
        char evalInput[0];
    };

    struct GvmIndexInput {
        Xid hashTreeId = XidInvalid;
        char key[XcalarApiMaxFieldNameLen + 1];
    };

  private:
    static LibHashTreeGvm *instance;
    LibHashTreeGvm() {}
    ~LibHashTreeGvm() {}
    LibHashTreeGvm(const LibHashTreeGvm &) = delete;
    LibHashTreeGvm &operator=(const LibHashTreeGvm &) = delete;
};

#endif  // _LIB_HASHTREE_GVM_H_
