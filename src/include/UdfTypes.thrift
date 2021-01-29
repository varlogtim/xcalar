# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# ******************************************************************
# ************** MUST BE KEPT IN SYNC WITH UdfTypes.h **************
# ******************************************************************
#

enum UdfTypeT {
  UdfTypePython = 1,
}

struct UdfModuleSrcT {
  1: UdfTypeT type
  2: string moduleName
  3: i64 sourceSize
  4: string source
}

