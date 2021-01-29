// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMATPARQUET_H_
#define _DATAFORMATPARQUET_H_

#include <jansson.h>
#include <google/protobuf/arena.h>
#include <parquet/api/reader.h>

#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "df/DataFormatTypes.h"
#include "datapage/MemPool.h"
#include "datapage/DataPage.h"
#include "export/DataTargetTypes.h"
#include "df/Importable.h"

class ParquetParser final : public IRecordParser
{
  public:
    struct Parameters {
        Status setFromJson(ParseArgs *parseArgs,
                           const json_t *paramJson,
                           char *errorBuf,
                           size_t errorBufLen);

        bool treatNullAsFnf = false;

        char **columns = NULL;
        int numColumns = 0;

        char **partitionKeys = NULL;
        char ***partitionValues = NULL;
        int numPartitionKeys = 0;
        int *numPartitionValues = NULL;

        ~Parameters()
        {
            int ii = 0;
            if (columns != NULL) {
                for (ii = 0; ii < numColumns; ii++) {
                    memFree(columns[ii]);
                    columns[ii] = NULL;
                }
                memFree(columns);
                columns = NULL;
            }

            if (partitionKeys != NULL) {
                for (ii = 0; ii < numPartitionKeys; ii++) {
                    memFree(partitionKeys[ii]);
                    partitionKeys[ii] = NULL;
                }
                memFree(partitionKeys);
                partitionKeys = NULL;
            }

            if (partitionValues != NULL) {
                for (ii = 0; ii < numPartitionKeys; ii++) {
                    int numPartitionValuesInKey = numPartitionValues[ii];
                    int jj;
                    for (jj = 0; jj < numPartitionValuesInKey; jj++) {
                        memFree(partitionValues[ii][jj]);
                        partitionValues[ii][jj] = NULL;
                    }
                    memFree(partitionValues[ii]);
                    partitionValues[ii] = NULL;
                }
                memFree(numPartitionValues);
                numPartitionValues = NULL;
                memFree(partitionValues);
                partitionValues = NULL;
            }

            if (numPartitionValues != NULL) {
                memFree(numPartitionValues);
                numPartitionValues = NULL;
            }
        }
    };

    ParquetParser() = default;
    ~ParquetParser();

    Status init(ParseArgs *parseArgs,
                ParseOptimizerArgs *optimizerArgs,
                DataPageWriter *writer,
                google::protobuf::Arena *arena,
                IRecordSink *pageCallback) override;

    Status getSchema(const uint8_t *dataPtr, int64_t dataSize);

    Status parseData(const uint8_t *dataPtr, int64_t dataSize);
    Status parseData(const char *fileName, IFileReader *reader) override;

  private:
    Status parseParquet(parquet::ParquetFileReader *pqReader);
    Status parsePartitionKeysFromFileName(const char *fileName);
    Status addRecord(std::shared_ptr<parquet::Scanner> scanners[],
                     const char *colNames[],
                     parquet::Type::type colTypes[],
                     int numCols);
    Status verifyParamsPartKeys();
    Status verifyParamsCols(const parquet::SchemaDescriptor *schema,
                            int numCols);

    ValueType parquetTypeToValueType(const parquet::ColumnDescriptor *colDesc);
    Status writePage();

    // Error reporting
    char errorStringBuf_[256];

    const ParseArgs *parseArgs_ = NULL;
    const ParseOptimizerArgs *optimizerArgs_ = NULL;
    google::protobuf::Arena *arena_ = NULL;
    IRecordSink *callback_ = NULL;
    DataPageWriter *writer_ = NULL;

    DataPageWriter::FieldMetaCache fieldMetaCache_;
    int64_t numRecords_ = -1;

    Parameters params_;

    char **partitionKeyNames_ = NULL;
    char **partitionKeyValues_ = NULL;
    int numPartitionKeys_ = 0;
};

class ParquetFormatOps final : public Importable
{
  public:
    static Status init();
    void destroy();
    static ParquetFormatOps *get();

    IRecordParser *getParser() override;

  private:
    static ParquetFormatOps *instance;

    ParquetFormatOps(){};
    ~ParquetFormatOps(){};

    ParquetFormatOps(const ParquetFormatOps &) = delete;
    ParquetFormatOps &operator=(const ParquetFormatOps &) = delete;
};

#endif  // _DATAFORMATPARQUET_H_
