// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "gvm/GvmTarget.h"
#include "table/Table.h"
#include "table/TableNs.h"
#include "dag/DagTypes.h"

#ifndef _TABLE_GVM_H_
#define _TABLE_GVM_H_

class TableGvm final : public GvmTarget
{
  public:
    enum class Action : uint32_t {
        AddObj,
        RemoveObj,
    };

    static TableGvm *get();
    static Status init();
    void destroy();

    Status localHandler(uint32_t action,
                        void *payload,
                        size_t *outputSizeOut) override;
    GvmTarget::Index getGvmIndex() const override;

    struct AddTableObj {
        TableNsMgr::TableId tableId;
        DagTypes::DagId dagId;
        DagTypes::NodeId dagNodeId;
        char fullyQualName[LibNsTypes::MaxPathNameLen];
        XcalarApiUdfContainer sessionContainer;
    };

    struct RemoveTableObj {
        TableNsMgr::TableId tableId;
    };

  private:
    static TableGvm *instance;
    TableGvm() {}
    ~TableGvm() {}
    TableGvm(const TableGvm &) = delete;
    TableGvm(const TableGvm &&) = delete;
    TableGvm &operator=(const TableGvm &) = delete;
    TableGvm &operator=(const TableGvm &&) = delete;
};

#endif  // _TABLE_GVM_H_
