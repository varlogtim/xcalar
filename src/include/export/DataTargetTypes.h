// Copyright 2015-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATATARGETTYPES_H_
#define _DATATARGETTYPES_H_

#include "DataTargetEnums.h"
#include "df/DataFormatTypes.h"

enum {
    XcalarExportTargetMaxNameLen = 255,
    XcalarExportTargetMaxSQLTableNameLen = 128,
};

// ************************************************************************** //
/* *** EVERYTHING BELOW MUST BE KEPT IN SYNC WITH DataTargetTypes.thrift **** */
// __________________________________________________________________________ //

// ********************** Add Export Target *********************** //
// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExAddTargetSFInput {
    char url[XcalarApiMaxUrlLen + 1];
};

// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExAddTargetUDFInput {
    char url[XcalarApiMaxUrlLen + 1];
    char appName[XcalarApiMaxAppNameLen + 1];
};

// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
typedef union ExAddTargetSpecificInput {
    ExAddTargetSFInput sfInput;
    ExAddTargetUDFInput udfInput;
} ExAddTargetSpecificInput;

// ********************* List Export Targets ********************** //
// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExExportTargetHdr {
    ExTargetType type;
    char name[XcalarExportTargetMaxNameLen + 1];
};

// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExExportTarget {
    ExExportTargetHdr hdr;
    ExAddTargetSpecificInput specificInput;
};

// **************************** Export **************************** //
// TODO - Refactor this to be shared by import
// * Source/Format Export * //
struct ExInitExportCSVArgs {
    char fieldDelim;
    char recordDelim;
    char quoteDelim;
};

struct ExInitExportJSONArgs {
    // Exported as json array of objects if true, and as newline delimited
    // objects otherwise
    bool array;
};

struct ExInitExportSQLArgs {
    char tableName[XcalarExportTargetMaxSQLTableNameLen + 1];
    bool dropTable;
    bool createTable;
};

typedef union ExInitExportFormatSpecificArgs {
    ExInitExportCSVArgs csv;
    ExInitExportJSONArgs json;
    ExInitExportSQLArgs sql;
} ExInitExportFormatSpecificArgs;

struct ExSFFileSplitSpecific {
    uint64_t numFiles;
    uint64_t maxSize;
};

struct ExSFFileSplitRule {
    ExSFFileSplitType type;
    ExSFFileSplitSpecific spec;
};

struct ExInitExportSFInput {
    char fileName[XcalarApiMaxFileNameLen + 1];
    DfFormatType format;
    ExSFFileSplitRule splitRule;
    ExSFHeaderType headerType;
    ExInitExportFormatSpecificArgs formatArgs;
};
// * End Source/Format Export * //

// * UDF Export * //
struct ExInitExportUDFInput {
    char fileName[XcalarApiMaxFileNameLen + 1];
    DfFormatType format;
    ExSFHeaderType headerType;
    ExInitExportFormatSpecificArgs formatArgs;
};
// * End UDF Export * //

typedef union ExInitExportSpecificInput {
    ExInitExportSFInput sfInput;
    ExInitExportUDFInput udfInput;
} ExInitExportSpecificInput;

struct ExColumnName {
    char name[DfMaxFieldNameLen + 1];         // Name in Xcalar Table
    char headerAlias[DfMaxFieldNameLen + 1];  // Name in exported table
};

struct ExExportMeta {
    char driverName[XcalarApiMaxUdfModuleNameLen + 1];
    // This is an arbitrary length, but it should be long enough
    char driverParams[UdfVersionedFQFname];
    int numColumns;
    ExColumnName columns[0];
};

// This is what the export meta looked like before export 2.0
struct LegacyExportMeta {
    ExExportTargetHdr target;
    ExInitExportSpecificInput specificInput;
    ExExportCreateRule createRule;
    bool sorted;
    char driverName[XcalarApiMaxUdfModuleNameLen + 1];
    // This is an arbitrary length, but it should be long enough
    char driverParams[UdfVersionedFQFname];
    int numColumns;
    ExColumnName columns[0];
};

// __________________________________________________________________________ //
/* *** EVERYTHING ABOVE MUST BE KEPT IN SYNC WITH DataTargetTypes.thrift **** */
// ************************************************************************** //

struct ExExportBuildScalarTableInput {
    XdbId srcXdbId;
    XdbId dstXdbId;
    bool sorted;
    int numColumns;
    ExColumnName columns[0];
};

#endif  // _DATATARGETTYPES_H_
