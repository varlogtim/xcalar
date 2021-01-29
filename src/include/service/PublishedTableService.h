// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PUBLISHEDTABLESERVICE_H_
#define _PUBLISHEDTABLESERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/PublishedTable.xcrpc.h"
#include "xcalar/compute/localtypes/PublishedTable.pb.h"

class HashTreeMgr;

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace PublishedTable
{
class PublishedTableService : public IPublishedTableService
{
  public:
    PublishedTableService();
    virtual ~PublishedTableService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status select(const SelectRequest *selectRequest,
                  SelectResponse *selectResponse) override;
    Status listTables(const ListTablesRequest *listTablesRequest,
                      ListTablesResponse *response) override;
    Status changeOwner(const ChangeOwnerRequest *req,
                       google::protobuf::Empty *empty) override;

  private:
    HashTreeMgr *htMgr_ = NULL;
    static constexpr const char *ModuleName = "publishedtableservice";
};

}  // namespace PublishedTable
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _PUBLISHEDTABLESERVICE_H_
