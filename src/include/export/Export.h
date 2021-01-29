// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _EXPORT_2_H_
#define _EXPORT_2_H_

#include "Primitives.h"
#include "dag/DagTypes.h"
#include "libapis/LibApisCommon.h"

class ExportAppHost final
{
  public:
    ExportAppHost() = default;
    ~ExportAppHost() = default;

    MustCheck Status exportTable(Dag *dag,
                                 XdbId exportNodeId,
                                 XdbId scalarTableId,
                                 const XcalarApiExportInput *exportInput,
                                 XcalarApiUserId *user);

  private:
    static constexpr const char *ModuleName = "exportHost";

    ExportAppHost(const ExportAppHost &) = delete;
    ExportAppHost &operator=(const ExportAppHost &) = delete;

    MustCheck Status runExportApp(XdbId tableId,
                                  const XcalarApiExportInput *exportInput,
                                  XcalarApiUserId *user,
                                  bool *retInternalAppError);

    MustCheck Status buildScalarTable(Dag *dag,
                                      XcalarApiUserId *user,
                                      const ExExportMeta *meta,
                                      XdbId srcXdbId,
                                      XdbId scalarTableXdbId);

    static MustCheck Status jsonifyExportInput(
        const XcalarApiExportInput *exportInput, XdbId tableId, char **inBlob);
};

#endif  // _EXPORT_2_H_
