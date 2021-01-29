# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# ******************************************************************
# ************** MUST BE KEPT IN SYNC WITH DagTypes.h **************
# ******************************************************************
#

include "DagRefTypeEnums.thrift"

typedef string XcalarApiXidT
const string XcalarApiXidInvalidT = ""

# This is really DagNameInput in DagTypes.h
struct XcalarApiNamedInputT {
  1: bool isTable
  2: string name // table name or dataset name
  3: XcalarApiXidT nodeId // filled in by callee
}

struct DagRefT {
  1: DagRefTypeEnums.DagRefTypeT type
  2: string name
  3: string xid
}
