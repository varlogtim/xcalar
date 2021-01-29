// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMATTYPES_H_
#define _DATAFORMATTYPES_H_

#include "df/DataFormatBaseTypes.h"
#include "xdb/TableTypes.h"
#include "util/AtomicTypes.h"
#include "udf/UdfTypes.h"
#include "runtime/Spinlock.h"
#include "util/Uuid.h"
#include "dag/DagTypes.h"
#include "CsvLoadArgsEnums.h"

class BackingData;

enum {
    // we encode fat pointers with a node-local record number and the node id..
    // so DfMaxRecords has to be bounded by the ability to encode both into
    // sizeof(Fatptr)
    DfMaxRecords =
        (1ULL << ((sizeof(Fatptr) * BitsPerUInt8) - MaxNodesBitShift)) - 1,

    DfDefaultConversionBase = BaseCanonicalForm,

    MaxSourcesPerLoad = 127,
};

static constexpr const int TupleMaxNumValuesPerRecord = 1023;

struct TupleValueDesc {
    uint64_t numValuesPerTuple;
    DfFieldType valueType[TupleMaxNumValuesPerRecord];
};

struct XdbLoadArgs {
    DagTypes::NodeId dstTableId;  // Deprecated
    XdbId dstXdbId;
    char keyName[XcalarApiMaxFieldNameLen + 1];
    int keyIndex;
    DfFieldType keyType;

    TupleValueDesc valueDesc;
    char evalString[XcalarApiMaxEvalStringLen + 1];

    unsigned fieldNamesCount;
    char fieldNames[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
};

struct DataSourceArgs {
    char targetName[XcalarApiMaxPathLen + 1];
    char path[XcalarApiMaxPathLen + 1];
    char fileNamePattern[XcalarApiMaxPathLen + 1];
    bool recursive;
};

struct ParseArgs {
    char parserFnName[UdfVersionedFQFname];
    // XXX figure out a better constant than path len
    char parserArgJson[XcalarApiMaxPathLen + 1];

    char fileNameFieldName[XcalarApiMaxFieldNameLen + 1];
    char recordNumFieldName[XcalarApiMaxFieldNameLen + 1];

    bool allowRecordErrors;
    bool allowFileErrors;

    unsigned fieldNamesCount;
    char oldNames[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
    char fieldNames[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
    DfFieldType types[TupleMaxNumValuesPerRecord];
};

struct ParseOptimizerArgs {
    unsigned numFieldsRequired;
    char fieldNamesPrefixed[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
    char fieldNames[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
    DfFieldType valueTypes[TupleMaxNumValuesPerRecord];
    char evalString[XcalarApiMaxEvalStringLen + 1];
};

// Needs PageSize alignment for Sparse copy.
struct __attribute__((aligned(PageSize))) DfLoadArgs {
    int sourceArgsListCount;
    DataSourceArgs sourceArgsList[MaxSourcesPerLoad];
    ParseArgs parseArgs;
    struct XdbLoadArgs xdbLoadArgs;
    int64_t maxSize;

    // Not to be filled by the user. Only to be filled by operatorHandler
    Uuid datasetUuid;
};

#endif  // _DATAFORMATTYPES_H_
