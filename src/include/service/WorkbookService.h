// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _WORKBOOKSERVICE_H_
#define _WORKBOOKSERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Workbook.xcrpc.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Workbook
{
class WorkbookService : public IWorkbookService
{
  public:
    WorkbookService();
    virtual ~WorkbookService() = default;

    Status convertKvsToQuery(
        const ConvertKvsToQueryRequest *convertKvsToQueryRequest,
        ConvertKvsToQueryResponse *convertKvsToQueryResponse) override;

  private:
    static constexpr const char *ModuleName = "workbookservice";
};

}  // namespace Workbook
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _WORKBOOKSERVICE_H_
