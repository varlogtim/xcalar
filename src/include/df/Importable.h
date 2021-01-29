// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _IMPORTABLE_H_
#define _IMPORTABLE_H_

#include "primitives/Primitives.h"
#include "strings/String.h"
#include "datapage/DataPage.h"

// 'Sink' as in 'Source' vs 'Sink'
// This represents interface presented by a stream which accepts records
class IRecordSink
{
  public:
    virtual MustCheck Status writePage() = 0;
    virtual MustCheck Status err(int64_t recordNum, const char *fmt, ...)
        __attribute__((format(printf, 3, 4))) = 0;

    virtual MustCheck Status addRecord(int64_t recordNum,
                                       DataPageWriter::Record *record,
                                       DataPageWriter::PageStatus *pageStatus,
                                       int32_t *bytesOverflow) = 0;
};

class IFileReader
{
  public:
    // Allows for reading a single 'numBytes' sized chunk of the file at a time.
    // *data is only valid until the next 'readChunk' call.
    // numBytes == 0 means read the whole file at once
    virtual MustCheck Status readChunk(int64_t numBytes,
                                       const uint8_t **data,
                                       int64_t *actualBytes) = 0;
};

class IRecordParser
{
  public:
    IRecordParser() = default;
    virtual ~IRecordParser() = default;

    virtual MustCheck Status init(ParseArgs *parseArgs,
                                  ParseOptimizerArgs *optimizerArgs,
                                  DataPageWriter *writer,
                                  google::protobuf::Arena *arena,
                                  IRecordSink *pageCallback) = 0;

    virtual MustCheck Status parseData(const char *fileName,
                                       IFileReader *reader) = 0;
};

// interface
class Importable
{
  public:
    Importable() = default;
    virtual ~Importable() = default;

    // Caller should 'delete' when done
    virtual IRecordParser *getParser() = 0;
};

#endif  // _IMPORTABLE_H_
