// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _KVSTORESERVICE_H_
#define _KVSTORESERVICE_H_

#include "primitives/Primitives.h"
#include "kvstore/KvStoreTypes.h"
#include "xcalar/compute/localtypes/KvStore.xcrpc.h"
#include "xcalar/compute/localtypes/KvStore.pb.h"
#include "xcalar/compute/localtypes/Workbook.xcrpc.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

class UserMgr;
class KvStoreLib;

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace KvStore
{
class KvStoreService : public IKvStoreService
{
  public:
    KvStoreService();
    virtual ~KvStoreService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status lookup(const LookupRequest *lookupRequest,
                  LookupResponse *lookupResponse) override;
    Status addOrReplace(const AddOrReplaceRequest *addOrReplaceRequest,
                        google::protobuf::Empty *empty) override;
    Status multiAddOrReplace(
        const MultiAddOrReplaceRequest *multiAddOrReplaceRequest,
        google::protobuf::Empty *empty) override;
    Status deleteKey(const DeleteKeyRequest *deleteKeyRequest,
                     google::protobuf::Empty *empty) override;
    Status append(const AppendRequest *appendRequest,
                  google::protobuf::Empty *empty) override;
    Status setIfEqual(const SetIfEqualRequest *setIfEqualRequest,
                      google::protobuf::Empty *empty) override;
    Status list(const ListRequest *listRequest,
                ListResponse *listResponse) override;

  private:
    static constexpr const char *ModuleName = "kvstoreservice";

    Status getKvStoreId(
        const xcalar::compute::localtypes::Workbook::WorkbookScope *scope,
        KvStoreId *id,
        bool *retTrackOpsToSession);

    Status unTrackOpsToSession(
        const xcalar::compute::localtypes::Workbook::WorkbookScope *scope);

    UserMgr *userMgr_ = NULL;
    KvStoreLib *kvsLib_ = NULL;
};

}  // namespace KvStore
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _KVSTORESERVICE_H_
