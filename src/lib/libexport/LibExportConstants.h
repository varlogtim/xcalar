// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBEXPORTCONSTANTS_H_
#define _LIBEXPORTCONSTANTS_H_

namespace Export
{
//
// Export related
//
enum {
    MinRecordsPerExportWorkItem = 1 * KB,
};

//
// Source Format Connector and UDF Connector related
//
enum {
    MaxNumNodeDigits = 4,
    MaxNumChunkDigits = 4,
    MaxNumPartDigits = 4,
    AppendBufLength = 4096,
};

}  // namespace Export

#endif  // _LIBEXPORTCONSTANTS_H_
