// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFSERVICE_H_
#define _UDFSERVICE_H_

#include "primitives/Primitives.h"
#include "udf/UserDefinedFunction.h"
#include "xcalar/compute/localtypes/UDF.xcrpc.h"
#include "xcalar/compute/localtypes/UDF.pb.h"
#include "xcalar/compute/localtypes/Workbook.xcrpc.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace UDF
{
class UserDefinedFunctionService : public IUserDefinedFunctionService
{
  public:
    UserDefinedFunctionService();
    virtual ~UserDefinedFunctionService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status getResolution(const GetResolutionRequest *getResolutionRequest,
                         GetResolutionResponse *getResolutionResponse) override;

  private:
    UserDefinedFunction *udf_ = NULL;
    static constexpr const char *ModuleName = "userdefinedfunctionservice";
};

}  // namespace UDF
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _UDFSERVICE_H_
