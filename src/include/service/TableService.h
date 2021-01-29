// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TABLE_SERVICE_H_
#define _TABLE_SERVICE_H_

#include "primitives/Primitives.h"
#include "xdb/HashTree.h"
#include "xcalar/compute/localtypes/Table.xcrpc.h"
#include "xcalar/compute/localtypes/Table.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Table
{
class TableService : public ITableService
{
  public:
    TableService() = default;
    virtual ~TableService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status addIndex(const IndexRequest *indexRequest,
                    google::protobuf::Empty *empty) override;

    Status removeIndex(const IndexRequest *indexRequest,
                       google::protobuf::Empty *empty) override;
    Status publishTable(const PublishRequest *publishRequest,
                        PublishResponse *publishResponse) override;
    Status unpublishTable(const UnpublishRequest *indexRequest,
                          google::protobuf::Empty *empty) override;
    Status listTables(const ListTablesRequest *listRequest,
                      ListTablesResponse *listResponse) override;
    Status tableMeta(const TableMetaRequest *metaRequest,
                     TableMetaResponse *metaResponse) override;

  private:
    static Status getHandleToHashTree(const char *tableName,
                                      LibNsTypes::NsOpenFlags flags,
                                      HashTreeRefHandle *refHandleOut);

    static constexpr const char *ModuleName = "TableService";
};
}  // namespace Table
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _TABLE_SERVICE_H_
