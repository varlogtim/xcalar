// Copyright 2017-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMATCSV_H_
#define _DATAFORMATCSV_H_

#include <jansson.h>
#include <google/protobuf/arena.h>

#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "df/DataFormatTypes.h"
#include "datapage/MemPool.h"
#include "datapage/DataPage.h"
#include "export/DataTargetTypes.h"
#include "df/Importable.h"
#include "util/Heap.h"

class IFileReader;

class FileBuffer final
{
  public:
    FileBuffer() = default;
    ~FileBuffer();

    // The chunkSize is the bulk size that is copied from the reader; this is
    // the maximum amount of data that will get copied when we do a 'flush'.
    // BufferSize is the maximum datasize that needs to be contiguous in memory
    // before a flush (max field size)
    Status init(int32_t chunkSize, int32_t bufferSize, IFileReader *reader);

    MustCheck Status refill(int withhold, int peek);

    bool endOfFile() const { return endOfFile_; }

    // State of the temp buffer for dealing with chunk overflow
    uint8_t *dataBuffer_ = NULL;
    int64_t bufferPos_ = -1;
    int64_t bufferSize_ = -1;
    int64_t bufferCapacity_ = -1;

    // State of the current file chunk present elsewhere
    const uint8_t *chunkData_ = NULL;
    int64_t chunkPos_ = -1;
    int64_t chunkSize_ = -1;

  private:
    IFileReader *fileReader_ = NULL;
    bool endOfFile_ = false;

    int32_t maxChunkSize_ = -1;
};

enum class ParseEventType : int8_t {
    FieldDelim,
    RecordDelim,
    QuoteDelim,
    EscapeDelim,
    EndOfFile,

    NumEventTypes,  // must be last 2
    InvalidEventType,
};

struct ParseEvent {
    ParseEventType type;
    int64_t start;
    int64_t end;
};

// Processes a UTF8 stream generating parse events
class CsvLexer final
{
  public:
    Status init(IFileReader *reader,
                int64_t pageCapacity,
                const char *fieldDelim,
                const char *recordDelim,
                const char *quoteDelim,
                const char *escapeDelim);
    Status nextEvent(ParseEvent *event);
    void dereference(int64_t startPos, int32_t len, uint8_t *result) const;
    void setParseCursor(int64_t newPos);

  private:
    // This is the granularity at which the file buffer copies data from the
    // reader. Increasing this number means at flush time we will have to move
    // more data, but will create less temporary object (python strings)
    static constexpr const int FileChunkSize = 4 * KB;

    struct Matcher {
        ParseEventType type = ParseEventType::InvalidEventType;
        const char *str = NULL;
        int len = -1;

        int64_t pos = -1;  // current position of cursor in file
    };

    struct Match {
        Matcher *matcher = NULL;
        int64_t start = -1;
    };

    void addMatcher(const char *matchStr, ParseEventType eventType);
    // Checks against the chunk if !checkBuffer, otherwise checks the buffer
    // Returns true if a match was made
    Status updateMatcher(Matcher *matcher, bool checkBuffer, bool *matched);
    Status startNewChunk();

    int numMatchers_ = -1;
    Matcher matchers_[(int) ParseEventType::NumEventTypes];
    int longestMatcherLen_ = 0;

    // This is the last known 'position' of the parser. Bytes are not
    // accessible behind the parser position
    int64_t parserPos_ = -1;

    bool fileExhausted_ = false;
    FileBuffer fileBuffer_;

    static bool compareMatches(const Match *m1, const Match *m2)
    {
        return m1->start < m2->start;
    }
    Heap<Match, compareMatches> matchHeap_;
};

class CsvParser final : public IRecordParser
{
  public:
    struct CsvTypedColumn {
        char colName[DfMaxFieldNameLen + 1];
        DfFieldType colType;
    };

    struct Parameters {
        static Status setStringParam(char *dst,
                                     size_t dstSize,
                                     const json_t *srcJsonStr,
                                     const char *fieldName,
                                     char *errorBuf,
                                     size_t errorBufLen)
        {
            if (!json_is_string(srcJsonStr)) {
                snprintf(errorBuf,
                         errorBufLen,
                         "'%s' must be a string",
                         fieldName);
                return StatusInval;
            }
            Status status =
                strStrlcpy(dst, json_string_value(srcJsonStr), dstSize);
            if (status != StatusOk) {
                snprintf(errorBuf,
                         errorBufLen,
                         "'%s' too long (max %lu)",
                         fieldName,
                         dstSize);
                return status;
            } else {
                return StatusOk;
            }
        }
        Status setFromJson(ParseArgs *parseArgs,
                           const json_t *paramJson,
                           char *errorBuf,
                           size_t errorBufLen);
        json_t *getJson() const;
        void setXcalarSnapshotDialect();
        void setDefaultDialect();

        char recordDelim[128] = {DfCsvDefaultRecordDelimiter, '\0'};
        char quoteDelim[128] = {DfCsvDefaultQuoteDelimiter, '\0'};
        char escapeDelim[128] = {DfCsvDefaultEscapeDelimiter, '\0'};
        int linesToSkip = 0;
        char fieldDelim[DfCsvFieldDelimiterMaxLen + 1] =
            {DfCsvDefaultFieldDelimiter, '\0'};
        bool isCRLF = false;
        bool emptyAsFnf = false;
        CsvSchemaMode schemaMode = CsvSchemaModeNoneProvided;
        // The following 2 only used if csvSchemaMode is CsvSchemaUseLoadInput
        unsigned typedColumnsCount = 0;
        CsvTypedColumn typedColumns[TupleMaxNumValuesPerRecord];
        // The following is only used if csvSchemaMode is CsvSchemaUseSchemaFile
        char schemaFile[XcalarApiMaxPathLen + 1] = "";
        CsvDialect dialect = CsvDialectDefault;
    };

    CsvParser() = default;
    ~CsvParser() = default;

    Status init(ParseArgs *parseArgs,
                ParseOptimizerArgs *optimizerArgs,
                DataPageWriter *writer,
                google::protobuf::Arena *arena,
                IRecordSink *pageCallback) override;

    Status parseData(const char *fileName, IFileReader *reader) override;

  private:
    static constexpr const int HeaderBufPageSize = 4 * KB;

    struct StringRef {
        const char *str;
        int64_t strLen;
    };

    struct Header {
        const char *fieldName = NULL;
        DfFieldType fieldType = DfString;
        bool fieldDesired = false;
        int cacheIndex = -1;
    };

    // Borrows a reference to fieldName
    Status addDiscoveredHeader(const char *fieldName, DfFieldType fieldType);

    // Generates name like 'column1' to fill in when no column name is available
    Status generateFillerHeader(int index, char **newHeader);

    // Instance methods
    Status parseCsvRecords();

    Status addField();

    Status addRecord();

    Status writePage();

    Status castAndSetProto(DfFieldType type,
                           const char *value,
                           size_t strLen,
                           bool *fatalError);

    // Error reporting
    char errorStringBuf_[256];

    // Parser options
    int32_t pageCapacity_ = -1;
    const ParseArgs *parseArgs_ = NULL;
    const ParseOptimizerArgs *optimizerArgs_ = NULL;
    Parameters params_;

    // Accelerators
    google::protobuf::Arena *arena_;
    MallocAllocator headerAlloc_;
    MemPool headerMPool_;
    MallocAllocator fieldAlloc_;
    MemPool fieldMPool_;
    DataPageWriter::FieldMetaCache fieldMetaCache_;

    IRecordSink *callback_ = NULL;
    DataPageWriter *writer_;
    DataValue dValue_;

    // Parser status
    // Record field status
    CsvLexer *lexer_ = NULL;
    int64_t fieldStartPos_ = -1;
    int64_t fieldEndPos_ = -1;
    int64_t quoteStartPos_ = -1;
    int64_t quoteEndPos_ = -1;
    int64_t escapeEndPos_ = -1;

    // Record status
    int numFields_ = -1;
    StringRef *recordFields_ = NULL;

    // File status
    bool inHeader_ = false;
    int64_t numRecords_ = -1;

    // Metadata status
    int maxNumFields_ = 4 * KB;
    int numHeaders_ = -1;
    int numDesiredFound_ = -1;
    // maxNumFields_ is the length of this buffer
    // numHeaders_ is the number of valid elements
    // numDesiredFound_ is the number of headers found which will be included
    Header *headers_ = NULL;
};

class CsvRowRenderer final : public IRowRenderer
{
  public:
    CsvRowRenderer() = default;

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
    // Invariant: Either bytesWritten = entire field || bytesWritten = 0
    MustCheck Status writeField(char *buf,
                                int64_t bufSize,
                                DfFieldType dfType,
                                const DfFieldValue *fieldValue,
                                int64_t *bytesWritten) const;

    const ExInitExportCSVArgs *args_ = NULL;
    const ExInitExportFormatSpecificArgs *formatArgs_ = NULL;
    int numFields_ = -1;
    const Field *fields_ = NULL;
};

class CsvFormatOps final : public Importable, public Exportable
{
  public:
    static Status init();
    void destroy();
    static CsvFormatOps *get();

    IRecordParser *getParser() override;
    IRowRenderer *getRowRenderer() override;

    // export functions
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
    static CsvFormatOps *instance;
    CsvFormatOps(){};
    ~CsvFormatOps(){};

    CsvFormatOps(const CsvFormatOps &) = delete;
    CsvFormatOps &operator=(const CsvFormatOps &) = delete;
};

#endif  // _DATAFORMATCSV_H_
