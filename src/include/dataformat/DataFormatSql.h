// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMATSQL_H_
#define _DATAFORMATSQL_H_

#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "df/DataFormatTypes.h"
#include "export/DataTargetTypes.h"

class SqlRowRenderer final : public IRowRenderer
{
  public:
    SqlRowRenderer() = default;

    // Borrows a reference to all arguments; must stay alive for the lifetime
    // of this object
    Status init(const ExInitExportFormatSpecificArgs *formatArgs,
                int numFields,
                const Field *fields) override;

    Status renderRow(const NewKeyValueEntry *entry,
                     bool first,
                     char *buf,
                     int64_t bufSize,
                     int64_t *bytesWritten) override;

  private:
    Status renderSQLValue(const NewKeyValueEntry *entry,
                          char *dest,
                          int64_t destSize,
                          int64_t *totalWritten) const;

    Status formatFieldVal(char *buf,
                          int64_t bufSize,
                          DfFieldType dfType,
                          const DfFieldValue *fieldValue,
                          int64_t *bytesWritten) const;

    const ExInitExportSQLArgs *args_ = NULL;
    int numFields_ = -1;
    const Field *fields_ = NULL;
};

class SqlFormatOps final : public Exportable
{
  public:
    static Status init();
    void destroy();
    static SqlFormatOps *get();

    IRowRenderer *getRowRenderer() override;

    Status requiresFullSchema(const ExInitExportFormatSpecificArgs *formatArgs,
                              bool *requiresFullSchema) override;
    Status renderColumnHeaders(const char *srcTableName,
                               int numColumns,
                               const char **headerColumns,
                               const ExInitExportFormatSpecificArgs *formatArgs,
                               const TupleValueDesc *valueDesc,
                               char *buf,
                               size_t bufSize,
                               size_t *bytesWritten) override;
    Status renderPrelude(const TupleValueDesc *valueDesc,
                         const ExInitExportFormatSpecificArgs *formatArgs,
                         char *buf,
                         size_t bufSize,
                         size_t *bytesWritten) override;
    Status renderFooter(char *buf,
                        size_t bufSize,
                        size_t *bytesWritten) override;

  private:
    static SqlFormatOps *instance;
    SqlFormatOps(){};
    ~SqlFormatOps(){};

    SqlFormatOps(const SqlFormatOps &) = delete;
    SqlFormatOps &operator=(const SqlFormatOps &) = delete;
};

#endif  // _DATAFORMATSQL_H_
