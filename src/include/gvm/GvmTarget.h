// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef GVMTARGET_H
#define GVMTARGET_H

#include "primitives/Primitives.h"

//
// Libraries that expose APIs via Gvm must derive from GvmTarget.
//
// Status returned from these handlers should reflect the success/failure of the
// operation and will be propagated back to the original invoker.
//

class GvmTarget
{
  public:
    enum Index : uint32_t {
        GvmIndexInvalid = 0,
        GvmIndexUdf,
        GvmIndexDag,
        GvmIndexDagNode,
        GvmIndexAppMgr,
        GvmIndexAppGroupMgr,
        GvmIndexDataTargetMgr,
        GvmIndexXdb,
        GvmIndexSupport,
        GvmIndexDhtHash,
        GvmIndexDataset,
        GvmIndexParent,
        GvmIndexHashTree,
        GvmIndexLicense,
        GvmIndexOperators,
        GvmIndexRuntime,
        GvmIndexMergeOperator,
        GvmIndexTable,

        // Must be last.
        GvmIndexCount,
    };

    GvmTarget() {}
    virtual ~GvmTarget() {}

    virtual Index getGvmIndex() const = 0;

    // Invoked on each local node as a result of broadcast.
    virtual Status localHandler(uint32_t action,
                                void *payload,
                                size_t *outputSizeOut) = 0;
};

#endif  // GVMTARGET_H
