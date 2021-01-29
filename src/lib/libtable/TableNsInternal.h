// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TABLE_NS_INTERNAL_H_
#define _TABLE_NS_INTERNAL_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "libapis/LibApisCommon.h"
#include "udf/UserDefinedFunction.h"
#include "table/TableNs.h"

class TableIdRecord final : public NsObject
{
  public:
    TableIdRecord(TableNsMgr::TableId tableId,
                  const XcalarApiUdfContainer *udfContainerIn,
                  uint64_t consistentVersion,
                  uint64_t nextVersion,
                  bool isGlobal,
                  bool mergePending)
        : NsObject(sizeof(TableIdRecord)),
          tableId_(tableId),
          consistentVersion_(consistentVersion),
          nextVersion_(nextVersion),
          isGlobal_(isGlobal),
          mergePending_(mergePending)
    {
        UserDefinedFunction::copyContainers(&udfContainer_, udfContainerIn);
    }

    TableNsMgr::TableId tableId_ = TableNsMgr::InvalidTableId;
    XcalarApiUdfContainer udfContainer_;
    uint64_t consistentVersion_ = TableNsMgr::InvalidVersion;
    uint64_t nextVersion_ = TableNsMgr::InvalidVersion;
    bool isGlobal_ = false;
    bool mergePending_ = false;

  private:
    TableIdRecord(const TableIdRecord &) = delete;
    TableIdRecord &operator=(const TableIdRecord &) = delete;
};

class TableNameRecord final : public NsObject
{
  public:
    TableNameRecord(TableNsMgr::TableId tableId)
        : NsObject(sizeof(TableNameRecord)), tableId_(tableId)
    {
    }

    TableNsMgr::TableId tableId_ = TableNsMgr::InvalidTableId;

  private:
    TableNameRecord(const TableNameRecord &) = delete;
    TableNameRecord &operator=(const TableNameRecord &) = delete;
};

#endif  // _TABLE_NS_INTERNAL_H_
