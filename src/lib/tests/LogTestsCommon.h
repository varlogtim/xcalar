// Copyright 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "primitives/Primitives.h"

Status logTestSanity();
Status logTestSanity2();
Status logTestEdge();
Status logTestMagicNumbers();
Status logTestBigNumFiles(unsigned numFiles);
Status logTestBigFile(size_t size);
Status logTestExpandFiles();

void cleanupPriorTestFiles(const char *prefix);
