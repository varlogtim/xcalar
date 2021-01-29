// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _ECHOSERVICE_H_
#define _ECHOSERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Echo.xcrpc.h"
#include "xcalar/compute/localtypes/Service.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Echo
{
class EchoService : public IEchoService
{
  public:
    Status echoMessage(const EchoRequest *echoIn,
                       EchoResponse *echoInfo) override;
    Status echoErrorMessage(const EchoErrorRequest *errorIn,
                            google::protobuf::Empty *empty) override;
    ServiceAttributes getAttr(const char *methodName) override;
};

}  // namespace Echo
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _ECHOSERVICE_H_
