// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef UDFPYCHILD_H
#define UDFPYCHILD_H

#include "primitives/Primitives.h"
#include "app/AppInstance.h"

class UdfPythonImpl;

MustCheck Status udfPyChildInit(UdfPythonImpl *udfPythonImpl);

void udfPyChildDestroy();

#endif  // UDFPYCHILD_H
