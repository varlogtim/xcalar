// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_UPDATE_H
#define _APIHANDLER_UPDATE_H

#include "libapis/ApiHandler.h"
#include "libapis/LibApisCommon.h"
#include "xdb/HashTree.h"
#include "table/TableNs.h"

class ApiHandlerUpdate : public ApiHandler
{
  public:
    ApiHandlerUpdate(XcalarApis api);
    virtual ~ApiHandlerUpdate();

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    // Pick some large enough number
    static constexpr const unsigned MaxNumUpdates = 1024;

    struct PublishedTableRef {
        const char *name;
        HashTreeRefHandle handle;
        bool handleInit;
        unsigned numRefs;
        int64_t newBatchId;
        bool tabUpdated;
    };

    struct ParentRef {
        bool ref;
        DagTypes::NodeId parentNodeId;
    };

    XcalarApiUpdateInput *input_;

    unsigned numParents_ = 0;
    ParentRef *parents_ = NULL;
    TableNsMgr::TableHandleTrack *handleTrack_ = NULL;

    unsigned numPublishedTableRefs_ = 0;
    PublishedTableRef *refs_ = NULL;

    static constexpr const char *moduleName = "libapis::apiHandler::Update";
    Flags getFlags() override;

    MustCheck Status issueCommit();
};

#endif  // _APIHANDLER_UPDATE_H
