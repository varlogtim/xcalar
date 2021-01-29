// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#ifndef _RETINATYPES_H_
#define _RETINATYPES_H_

#define RetinaHandleInvalid NULL

#include "dag/DagTypes.h"
#include "export/DataTargetTypes.h"
#include "udf/UdfTypes.h"
#include "df/DataFormat.h"

struct RetinaDesc {
    char retinaName[DagTypes::MaxNameLen + 1];
};

struct RetinaDst {
    int numColumns;
    DagTypes::NamedInput target;
    ExColumnName columns[0];
};

struct RetinaInfo {
    RetinaDesc retinaDesc;
    char *queryStr;
    Dag *queryGraph;
    uint64_t numTables;
    // Array of retinaDst pointers
    RetinaDst **tableArray;
    uint64_t numUdfModules;
    // Array of udfModuleSrc pointers
    UdfModuleSrc **udfModulesArray;
    uint64_t numColumnHints;
    Column *columnHints;
};

#endif  // _RETINATYPES_H_
