// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LICENSESERVICE_H_
#define _LICENSESERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/License.xcrpc.h"
#include "xcalar/compute/localtypes/License.pb.h"
#include "util/License.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace License
{
class LicenseService : public ILicenseService
{
  public:
    LicenseService();
    virtual ~LicenseService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status create(const CreateRequest *createRequest,
                  google::protobuf::Empty *empty) override;
    Status destroy(const DestroyRequest *destroyRequest,
                   google::protobuf::Empty *empty) override;
    Status get(const GetRequest *getRequest, GetResponse *getResponse) override;
    Status update(const UpdateRequest *updateRequest,
                  google::protobuf::Empty *empty) override;
    Status validate(const ValidateRequest *validateRequest,
                    ValidateResponse *validateResponse) override;

  private:
    static constexpr const char *ModuleName = "licenseservice";

    LicenseMgr *licenseMgr_ = NULL;
};

}  // namespace License
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _LICENSESERVICE_H_
