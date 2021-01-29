# Copyright 2014 - 2015 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# ******************************************************************
# *********** MUST BE KEPT IN SYNC WITH DataTargetTypes.h ***********
# ******************************************************************
#

include "DataTargetEnums.thrift"
include "DataFormatEnums.thrift"

// ********************** Add Export Target *********************** //
// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExAddTargetSFInputT {
  1: string url
}

// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExAddTargetUDFInputT {
  1: string url
  2: string appName
}

// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
union ExAddTargetSpecificInputT {
  2: ExAddTargetSFInputT sfInput
  3: ExAddTargetUDFInputT udfInput
}

// ********************* List Export Targets ********************** //
// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExExportTargetHdrT {
  1: DataTargetEnums.ExTargetTypeT type
  2: string name
}

// IMPORTANT - THIS STRUCT GETS PERSISTED, PLEASE BUMP UP VERSION INFO
struct ExExportTargetT {
  1: ExExportTargetHdrT hdr
  2: ExAddTargetSpecificInputT specificInput
}

// **************************** Export **************************** //
// * Source/Format Export * //
struct ExInitExportCSVArgsT {
  1: string fieldDelim // Up to 255 char
  2: string recordDelim // 1 char
  3: string quoteDelim // 1 char
}

struct ExInitExportJSONArgsT {
  // Exported as json array of objects if true, and as newline delimited
  // objects otherwise
  1: bool array;
}

struct ExInitExportSQLArgsT {
  1: string tableName
  2: bool dropTable
  3: bool createTable
}

union ExInitExportFormatSpecificArgsT {
  1: ExInitExportCSVArgsT  csv
  2: ExInitExportJSONArgsT json
  3: ExInitExportSQLArgsT  sql
}

union ExSFFileSplitSpecificT {
  1: i64 numFiles;
  2: i64 maxSize;
}

struct ExSFFileSplitRuleT {
  1: DataTargetEnums.ExSFFileSplitTypeT type;
  2: ExSFFileSplitSpecificT spec;
}

struct ExInitExportSFInputT {
  1: string  fileName
  2: DataFormatEnums.DfFormatTypeT format;
  3: ExSFFileSplitRuleT splitRule;
  4: DataTargetEnums.ExSFHeaderTypeT headerType;
  5: ExInitExportFormatSpecificArgsT formatArgs;
}
// * End Source/Format Export * //

// * UDF Export * //
struct ExInitExportUDFInputT {
  1: string fileName
  2: DataFormatEnums.DfFormatTypeT format;
  3: DataTargetEnums.ExSFHeaderTypeT headerType;
  4: ExInitExportFormatSpecificArgsT formatArgs;
}
// * End UDF Export * //

union ExInitExportSpecificInputT {
  2: ExInitExportSFInputT   sfInput
  3: ExInitExportUDFInputT  udfInput
}

struct ExColumnNameT {
  1: string name
  2: string headerAlias
}

struct ExExportMetaT {
  1: ExExportTargetHdrT target
  2: ExInitExportSpecificInputT specificInput
  3: DataTargetEnums.ExExportCreateRuleT createRule
  4: bool sorted
  5: i32 numColumns
  6: list<ExColumnNameT> columns
}
