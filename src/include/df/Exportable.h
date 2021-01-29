// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _EXPORTABLE_H_
#define _EXPORTABLE_H_

#include "primitives/Primitives.h"

union ExInitExportFormatSpecificArgs;

class IRowRenderer
{
  public:
    struct Field {
        DfFieldType type;
        int entryIndex;
    };

    IRowRenderer() = default;
    virtual ~IRowRenderer() = default;

    // Borrows a reference to all arguments; must stay alive for the lifetime
    // of this object
    virtual MustCheck Status
    init(const ExInitExportFormatSpecificArgs *formatArgs,
         int numFields,
         const Field *fields) = 0;

    virtual MustCheck Status renderRow(const NewKeyValueEntry *entry,
                                       bool first,
                                       char *buf,
                                       int64_t bufSize,
                                       int64_t *bytesWritten) = 0;
};

// interface
class Exportable
{
  public:
    // Export Functions
    virtual Status requiresFullSchema(
        const ExInitExportFormatSpecificArgs *formatArgs,
        bool *requiresFullSchema) = 0;
    // This function should write the initial non-record data. This is
    // something like CSV headers or a SQL CREATE TABLE expression
    virtual Status renderColumnHeaders(
        const char *srcTableName,
        int numColumns,
        const char **headerColumns,
        const ExInitExportFormatSpecificArgs *formatArgs,
        const TupleValueDesc *valueDesc,
        char *buf,
        size_t bufSize,
        size_t *bytesWritten) = 0;
    virtual Status renderPrelude(
        const TupleValueDesc *valueDesc,
        const ExInitExportFormatSpecificArgs *formatArgs,
        char *buf,
        size_t bufSize,
        size_t *bytesWritten) = 0;
    virtual MustCheck IRowRenderer *getRowRenderer() = 0;
    virtual Status renderFooter(char *buf,
                                size_t bufSize,
                                size_t *bytesWritten) = 0;
};

#endif  // _EXPORTABLE_H_
