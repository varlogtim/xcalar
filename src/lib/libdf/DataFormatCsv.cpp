// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>
#include <string.h>

#include "hash/Hash.h"
#include "df/DataFormat.h"
#include "dataformat/DataFormatCsv.h"
#include "util/MemTrack.h"
#include "constants/XcalarConfig.h"
#include "DataFormatConstants.h"
#include "dataset/BackingData.h"
#include "libapis/LibApisCommon.h"
#include "Utf8Verify.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "datapage/DataPage.h"

using namespace df;

CsvFormatOps *CsvFormatOps::instance;
static constexpr const char *moduleName = "libdf";

//
// FileBuffer
//

FileBuffer::~FileBuffer()
{
    if (dataBuffer_) {
        memFree(dataBuffer_);
        dataBuffer_ = NULL;
    }
}

Status
FileBuffer::init(int32_t chunkSize, int32_t bufferSize, IFileReader *reader)
{
    Status status = StatusOk;

    fileReader_ = reader;

    // buffer state
    bufferPos_ = 0;
    bufferSize_ = 0;
    bufferCapacity_ = bufferSize;
    dataBuffer_ = static_cast<uint8_t *>(memAlloc(bufferCapacity_));
    BailIfNull(dataBuffer_);

    // chunk state
    chunkPos_ = 0;
    chunkSize_ = 0;
    chunkData_ = NULL;

    maxChunkSize_ = chunkSize;

CommonExit:
    return status;
}

Status
FileBuffer::refill(int withhold, int peek)
{
    Status status = StatusOk;
    int64_t oldChunkSize = chunkSize_;
    assert(withhold <= chunkSize_ && "cannot withhold more than we have");

    if (withhold) {
        // we need to copy the rest of the current chunk into a temporary
        // buffer and request the next chunk
        int32_t bufRemaining = bufferCapacity_ - bufferSize_;
        int32_t bufIncrease = withhold + peek + 1;
        if (unlikely(bufIncrease > bufRemaining)) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        // Stash away the remaining bytes from this chunk
        memcpy(dataBuffer_ + bufferSize_,
               chunkData_ + chunkSize_ - withhold,
               withhold);

        if (bufferSize_ == 0) {
            assert(chunkSize_ >= withhold);
            assert(chunkPos_ >= 0);
            // The buffer was previously empty; track its position
            bufferPos_ = chunkPos_ + chunkSize_ - withhold;
        }
        bufferSize_ += withhold;
        // We want to keep this NUL terminated
        dataBuffer_[bufferSize_] = '\0';
        assert((int64_t) strlen((const char *) dataBuffer_) == bufferSize_);
    } else {
        bufferSize_ = 0;
    }

    status = fileReader_->readChunk(maxChunkSize_, &chunkData_, &chunkSize_);
    BailIfFailed(status);

    // Check some integrity constraints
    if (chunkSize_) {
        assert(chunkData_[chunkSize_] == '\0' && "nul terminated");
        assert(memchr(chunkData_, '\0', chunkSize_) == NULL && "no nuls");
    }

    chunkPos_ += oldChunkSize;

    // We didn't get as much data as we asked for; we must be at the end
    endOfFile_ = chunkSize_ < maxChunkSize_;

    if (chunkSize_ > 0 && peek) {
        int32_t peekAmount = xcMin(peek, (int32_t) chunkSize_);
        assert(bufferSize_ + peek + 1 <= bufferCapacity_ && "we checked above");
        memcpy(dataBuffer_ + bufferSize_, chunkData_, peekAmount);
        bufferSize_ += peekAmount;
        dataBuffer_[bufferSize_] = '\0';
    }

CommonExit:
    return status;
}

//
// CsvEventGen
//

Status
CsvLexer::init(IFileReader *reader,
               int64_t pageCapacity,
               const char *fieldDelim,
               const char *recordDelim,
               const char *quoteDelim,
               const char *escapeDelim)
{
    Status status = StatusOk;
    int32_t bufferCapacity;

    parserPos_ = 0;
    longestMatcherLen_ = 0;

    numMatchers_ = 0;
    addMatcher(fieldDelim, ParseEventType::FieldDelim);
    addMatcher(recordDelim, ParseEventType::RecordDelim);
    addMatcher(quoteDelim, ParseEventType::QuoteDelim);
    addMatcher(escapeDelim, ParseEventType::EscapeDelim);

    status = matchHeap_.init(numMatchers_);
    BailIfFailed(status);

    // The largest possible data in the buffer is 1 field + 2 delimiters
    // (1 on either side)
    bufferCapacity = pageCapacity + 2 * longestMatcherLen_;

    status = fileBuffer_.init(FileChunkSize, bufferCapacity, reader);
    BailIfFailed(status);

CommonExit:
    return status;
}

void
CsvLexer::addMatcher(const char *matchStr, ParseEventType eventType)
{
    if (matchStr[0] != '\0') {
        Matcher *matcher = &matchers_[numMatchers_];
        matcher->type = eventType;
        matcher->str = matchStr;
        matcher->len = strlen(matchStr);
        matcher->pos = 0;
        ++numMatchers_;

        if (matcher->len > longestMatcherLen_) {
            longestMatcherLen_ = matcher->len;
        }
    }
    assert(numMatchers_ <= static_cast<int>(ParseEventType::NumEventTypes));
}

Status
CsvLexer::updateMatcher(Matcher *matcher, bool checkBuffer, bool *matched)
{
    // Check if we have any matching room left with the data that we currently
    // have. Note that the buffer is always behind the chunk, so being before
    // the last possible chunk match includes the data buffer
    Status status = StatusOk;
    *matched = false;
    const char *data;
    int64_t dataPos;
    int64_t dataSize;
    if (checkBuffer) {
        data = (const char *) fileBuffer_.dataBuffer_;
        dataPos = fileBuffer_.bufferPos_;
        dataSize = fileBuffer_.bufferSize_;
    } else {
        data = (const char *) fileBuffer_.chunkData_;
        dataPos = fileBuffer_.chunkPos_;
        dataSize = fileBuffer_.chunkSize_;
    }
    int64_t lastPossibleMatch = dataPos + dataSize - matcher->len + 1;
    if (matcher->pos < lastPossibleMatch) {
        int64_t oldPos = matcher->pos;  // this is just for assertion purposes
        int64_t dataOffset = matcher->pos - dataPos;

        // We now have a limited view into the data
        // let's search that view for a match
        const char *view = data + dataOffset;
        int64_t viewSize = dataSize - dataOffset;
        int64_t viewPos = dataPos + dataOffset;

        assert(viewSize > 0);
        assert(viewPos >= 0);

        // Now we know what data to search, and we know what to do in the event
        // of a match. Time to actually do the matching itself.
        const char *matchStr = strstr(view, matcher->str);
        if (matchStr) {
            // We have made a match
            // Update the matcher
            int64_t matchOffset = matchStr - view;
            int64_t matchPos = viewPos + matchOffset;
            assert(matchOffset >= 0 && "we can not match before the start");
            assert(matchPos >= 0 && "we can not match before the start");

            matcher->pos = matchPos + matcher->len;

            // Our match must fall within data we've already seen
            assert(matchPos < fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_);
            assert(matchPos <= fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_);

            // Add the match to the heap
            Match m;
            m.start = matchPos;
            m.matcher = matcher;
            status = matchHeap_.insert(m);
            assert(status == StatusOk && "we should fit in the heap");
            BailIfFailed(status);
            *matched = true;
        } else {
            // A match may start in the last few bytes of the chunk.
            // If a match started (len-1) bytes from the end, we'll need to
            // recheck those bytes.
            matcher->pos = viewPos + viewSize - matcher->len + 1;
        }
        assert(matcher->pos > oldPos && "we must make progress");
    }
    assert(matcher->pos >= fileBuffer_.chunkPos_ &&
           "we always advance past the buffer");
CommonExit:
    return status;
}

Status
CsvLexer::startNewChunk()
{
    Status status = StatusOk;

    // There's 2 reasons why we would need to keep data around from a previous
    // chunk:
    //  1. We may have a delimiter which spans across chunks
    //  2. The parser isn't done processing the last chunk
    // In order to serve these 2 needs, we keep around the superset of the needs
    // for these two use cases.

    int parserWithholding =
        fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_ - parserPos_;
    int withholding = xcMax(longestMatcherLen_ - 1, parserWithholding);
    withholding = xcMin(withholding, (int) fileBuffer_.chunkSize_);

    // We want to peek into the next chunk in order to catch any delimiters
    // which straddle the two chunks.

    int peek = longestMatcherLen_ - 1;

    // Check our assumptions here
    for (int ii = 0; ii < numMatchers_; ii++) {
        Matcher *matcher = &matchers_[ii];

        assert(fileBuffer_.chunkSize_ == 0 ||
               matcher->pos + matcher->len - 1 >=
                       fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_ &&
                   "we must have consumed this whole chunk");
    }

    status = fileBuffer_.refill(withholding, peek);
    BailIfFailed(status);

    if (fileBuffer_.chunkSize_ > 0) {
        for (int ii = 0; ii < numMatchers_; ii++) {
            Matcher *matcher = &matchers_[ii];

            bool bufferMatch = false;
            if (matcher->len > 1) {
                // we need to check for delims that straddle chunks
                status = updateMatcher(matcher, true, &bufferMatch);
                assert(status == StatusOk && "updating matcher doesnt fail");
                BailIfFailed(status);
            }
            if (!bufferMatch) {
                // Match again, this time against the new chunk
                bool unused;
                status = updateMatcher(matcher, false, &unused);
                assert(status == StatusOk && "updating matcher doesnt fail");
                BailIfFailed(status);
            }
            assert(matcher->pos >= fileBuffer_.chunkPos_ &&
                   "we should be past any delimiters straddling chunks");
        }
    }

CommonExit:
    return status;
}

Status
CsvLexer::nextEvent(ParseEvent *event)
{
    Status status = StatusOk;

    while (matchHeap_.size() == 0 && !fileBuffer_.endOfFile()) {
        // refill heap
        status = startNewChunk();
        BailIfFailed(status);
    }
    if (matchHeap_.size()) {
        int oldSize = matchHeap_.size();

        Match match;
        status = matchHeap_.remove(&match);
        assert(status == StatusOk && "we just checked the size");
        BailIfFailed(status);
        event->start = match.start;
        event->end = event->start + match.matcher->len;
        event->type = match.matcher->type;

        // Our match must fall within data we've already seen
        assert(event->start < fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_);
        assert(event->end <= fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_);

        // Now we want to advance that same matcher until either the next match
        // in the chunk, or the end of the chunk
        bool matched;
        status = updateMatcher(match.matcher, false, &matched);
        assert(status == StatusOk && "updating shouldnt fail");
        BailIfFailed(status);
        if (matched) {
            // We added a new match to the heap, replacing the one we are
            // about to return to the caller
            assert(matchHeap_.size() == oldSize);
        } else {
            // We're at the end of the chunk
            assert(match.matcher->pos + match.matcher->len >
                   fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_);
        }
    } else {
        if (fileExhausted_) {
            assert(false && "parser must respect the end-of-file event");
            status = StatusFailed;
            goto CommonExit;
        }
        fileExhausted_ = true;

        int64_t lastByte = fileBuffer_.chunkPos_ + fileBuffer_.chunkSize_ - 1;
        event->start = lastByte;
        event->end = lastByte + 1;
        event->type = ParseEventType::EndOfFile;
    }

CommonExit:
    return status;
}

void
CsvLexer::dereference(int64_t startPos, int32_t len, uint8_t *result) const
{
    int64_t endPos = startPos + len;
    int64_t bufferChunkOverlap = fileBuffer_.bufferPos_ +
                                 fileBuffer_.bufferSize_ -
                                 fileBuffer_.chunkPos_;
    bufferChunkOverlap = xcMax(bufferChunkOverlap, 0l);

    int64_t bufferStartOffset = constrain(startPos - fileBuffer_.bufferPos_,
                                          0l,
                                          fileBuffer_.bufferSize_);
    int64_t bufferEndOffset =
        constrain(endPos - fileBuffer_.bufferPos_, 0l, fileBuffer_.bufferSize_);
    int64_t bufferBytes = bufferEndOffset - bufferStartOffset;
    assert(bufferBytes >= 0);

    int64_t chunkStartOffset = constrain(startPos - fileBuffer_.chunkPos_,
                                         bufferChunkOverlap,
                                         fileBuffer_.chunkSize_);
    int64_t chunkEndOffset = constrain(endPos - fileBuffer_.chunkPos_,
                                       chunkStartOffset,
                                       fileBuffer_.chunkSize_);
    int64_t chunkBytes = chunkEndOffset - chunkStartOffset;
    assert(chunkBytes >= 0);

    assert(bufferBytes + chunkBytes == len);

    if (bufferBytes) {
        assert(bufferStartOffset < fileBuffer_.bufferSize_);
        // The buffer is always behind the chunk; this should go into the
        // beginning of the result
        assert(fileBuffer_.bufferPos_ + bufferStartOffset == startPos);
        assert(fileBuffer_.bufferSize_ >= bufferBytes - bufferStartOffset);
        const uint8_t *data = fileBuffer_.dataBuffer_ + bufferStartOffset;

        memcpy(result, data, bufferBytes);
    }

    if (chunkBytes) {
        assert(chunkStartOffset < fileBuffer_.chunkSize_);
        assert(fileBuffer_.chunkSize_ >= chunkBytes - chunkStartOffset);
        const uint8_t *data = fileBuffer_.chunkData_ + chunkStartOffset;

        memcpy(result + bufferBytes, data, chunkBytes);
    }
}

void
CsvLexer::setParseCursor(int64_t newPos)
{
    assert(newPos >= parserPos_ && "we must never move backwards");
    parserPos_ = newPos;
    if (parserPos_ > fileBuffer_.bufferPos_ + fileBuffer_.bufferSize_) {
        // we can drop the buffer; its no longer used
        fileBuffer_.bufferSize_ = 0;
        fileBuffer_.bufferPos_ = -1;
    }
}

void
CsvParser::Parameters::setXcalarSnapshotDialect()
{
    this->recordDelim[0] = '\n';
    this->recordDelim[1] = '\0';

    this->fieldDelim[0] = '\t';
    this->fieldDelim[1] = '\0';

    this->quoteDelim[0] = '"';
    this->quoteDelim[1] = '\0';

    this->escapeDelim[0] = '\\';
    this->escapeDelim[1] = '\0';

    this->linesToSkip = 1;
    this->isCRLF = false;
    this->emptyAsFnf = true;
    this->schemaMode = CsvSchemaModeUseLoadInput;
}

//
// CsvParser::Parameters
//

Status
CsvParser::Parameters::setFromJson(ParseArgs *parseArgs,
                                   const json_t *paramJson,
                                   char *errorBuf,
                                   size_t errorBufLen)
{
    Status status = StatusOk;
    const json_t *recordDelimJson;
    const json_t *quoteDelimJson;
    const json_t *linesToSkipJson;
    const json_t *fieldDelimJson;
    const json_t *isCRLFJson;
    const json_t *emptyAsFnfJson;
    const json_t *schemaFileJson;
    const json_t *schemaModeJson;
    const json_t *dialectJson;

    dialectJson = json_object_get(paramJson, "dialect");
    if (dialectJson) {
        if (!json_is_string(dialectJson)) {
            snprintf(errorBuf, errorBufLen, "'dialect' must be a string");
            status = StatusInval;
            goto CommonExit;
        }
        this->dialect = strToCsvDialect(json_string_value(dialectJson));
        if (!isValidCsvDialect(this->dialect)) {
            snprintf(errorBuf,
                     errorBufLen,
                     "'%s' is not a valid dialect",
                     json_string_value(dialectJson));
            status = StatusInval;
            goto CommonExit;
        }
    } else {
        this->dialect = CsvDialectDefault;
    }

    switch (this->dialect) {
    case CsvDialectXcalarSnapshot:
        setXcalarSnapshotDialect();
        break;
    case CsvDialectDefault:
    default:
        recordDelimJson = json_object_get(paramJson, "recordDelim");
        if (recordDelimJson) {
            status = setStringParam(this->recordDelim,
                                    sizeof(this->recordDelim),
                                    recordDelimJson,
                                    "recordDelim",
                                    errorBuf,
                                    errorBufLen);
            BailIfFailed(status);
        }

        quoteDelimJson = json_object_get(paramJson, "quoteDelim");
        if (quoteDelimJson) {
            status = setStringParam(this->quoteDelim,
                                    sizeof(this->quoteDelim),
                                    quoteDelimJson,
                                    "quoteDelim",
                                    errorBuf,
                                    errorBufLen);
            BailIfFailed(status);
        }

        linesToSkipJson = json_object_get(paramJson, "linesToSkip");
        if (linesToSkipJson) {
            if (!json_is_integer(linesToSkipJson)) {
                snprintf(errorBuf,
                         errorBufLen,
                         "'linesToSkip' must be an integer");
                status = StatusInval;
                goto CommonExit;
            }
            this->linesToSkip = json_integer_value(linesToSkipJson);
        }

        fieldDelimJson = json_object_get(paramJson, "fieldDelim");
        if (fieldDelimJson) {
            status = setStringParam(this->fieldDelim,
                                    sizeof(this->fieldDelim),
                                    fieldDelimJson,
                                    "fieldDelim",
                                    errorBuf,
                                    errorBufLen);
            BailIfFailed(status);
        }

        isCRLFJson = json_object_get(paramJson, "isCRLF");
        if (isCRLFJson) {
            if (!json_is_boolean(isCRLFJson)) {
                snprintf(errorBuf, errorBufLen, "'isCRLF' must be a boolean");
                status = StatusInval;
                goto CommonExit;
            }
            this->isCRLF = json_is_true(isCRLFJson);
        }

        emptyAsFnfJson = json_object_get(paramJson, "emptyAsFnf");
        if (emptyAsFnfJson) {
            if (!json_is_boolean(emptyAsFnfJson)) {
                snprintf(errorBuf,
                         errorBufLen,
                         "'emptyAsFnf' must be a boolean");
                status = StatusInval;
                goto CommonExit;
            }
            this->emptyAsFnf = json_is_true(emptyAsFnfJson);
        }

        schemaModeJson = json_object_get(paramJson, "schemaMode");
        if (schemaModeJson) {
            if (!json_is_string(schemaModeJson)) {
                snprintf(errorBuf,
                         errorBufLen,
                         "'schemaMode' must be a string");
                status = StatusInval;
                goto CommonExit;
            }
            this->schemaMode =
                strToCsvSchemaMode(json_string_value(schemaModeJson));
            if (!isValidCsvSchemaMode(this->schemaMode)) {
                snprintf(errorBuf,
                         errorBufLen,
                         "'%s' is not a valid schemaMode",
                         json_string_value(schemaModeJson));
                status = StatusInval;
                goto CommonExit;
            }
        } else {
            this->schemaMode = CsvSchemaModeNoneProvided;
        }
        break;
    }

    switch (this->schemaMode) {
    case CsvSchemaModeUseSchemaFile:
        schemaFileJson = json_object_get(paramJson, "schemaFile");
        if (!schemaFileJson) {
            snprintf(errorBuf,
                     errorBufLen,
                     "schemaMode %s requires "
                     "schemaFile input",
                     strGetFromCsvSchemaMode(this->schemaMode));
            status = StatusInval;
            goto CommonExit;
        }

        if (!json_is_string(schemaFileJson)) {
            snprintf(errorBuf, errorBufLen, "schemaFile is not a string!");
            status = StatusInval;
            goto CommonExit;
        }

        status = strStrlcpy(this->schemaFile,
                            json_string_value(schemaFileJson),
                            sizeof(this->schemaFile));
        if (status != StatusOk) {
            snprintf(errorBuf,
                     errorBufLen,
                     "\"%s\" is too long (%lu chars). "
                     "Max is %lu chars",
                     json_string_value(schemaFileJson),
                     strlen(json_string_value(schemaFileJson)),
                     sizeof(this->schemaFile) - 1);
            goto CommonExit;
        }
        break;

    case CsvSchemaModeUseLoadInput: {
        this->typedColumnsCount = parseArgs->fieldNamesCount;

        for (unsigned ii = 0; ii < this->typedColumnsCount; ii++) {
            status = strStrlcpy(this->typedColumns[ii].colName,
                                parseArgs->fieldNames[ii],
                                sizeof(this->typedColumns[ii].colName));
            assert(status == StatusOk);

            this->typedColumns[ii].colType = parseArgs->types[ii];
            assert(isValidDfFieldType(this->typedColumns[ii].colType));
        }

        break;
    }
    case CsvSchemaModeNoneProvided:  // Fall through
    case CsvSchemaModeUseHeader:     // Fall through
    default:
        break;
    }

    // Apply CRLF argument. We'd really like to deprecate this
    if (this->isCRLF) {
        if (strcmp(this->recordDelim, "\n") != 0) {
            snprintf(errorBuf,
                     errorBufLen,
                     "isCRLF must be provided with a recordDelim of '\\n'; got "
                     "'%s' instead",
                     this->recordDelim);
            status = StatusInval;
            goto CommonExit;
        }
        // Okay we know that the given record delim is '\n', but it should be
        // '\r\n' (CRLF) instead. Let's just replace it
        this->recordDelim[0] = '\r';
        this->recordDelim[1] = '\n';
        this->recordDelim[2] = '\0';
    }

CommonExit:
    return status;
}

json_t *
CsvParser::Parameters::getJson() const
{
    Status status = StatusUnknown;
    json_t *jsonObj = NULL;
    int ret;
    json_t *schemaFileJson = NULL;

    jsonObj = json_pack(
        "{"
        "s:s#,"  // recordDelim
        "s:s#,"  // quoteDelim
        "s:i,"   // linesToSkip
        "s:s,"   // fieldDelim
        "s:b,"   // emptyAsFnf
        "s:s"    // csvSchemaMode
        "}",
        "recordDelim",
        this->recordDelim,
        1,
        "quoteDelim",
        this->quoteDelim,
        1,
        "linesToSkip",
        this->linesToSkip,
        "fieldDelim",
        this->fieldDelim,
        "emptyAsFnf",
        this->emptyAsFnf,
        "schemaMode",
        strGetFromCsvSchemaMode(this->schemaMode));
    if (jsonObj == NULL) {
        status = StatusInval;
        goto CommonExit;
    }

    switch (this->schemaMode) {
    case CsvSchemaModeUseSchemaFile:
        schemaFileJson = json_string(this->schemaFile);
        if (schemaFileJson == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error creating json string from \"%s\"",
                    this->schemaFile);
            status = StatusInval;
            goto CommonExit;
        }

        ret = json_object_set_new(jsonObj, "schemaFile", schemaFileJson);
        if (ret != 0) {
            xSyslog(moduleName, XlogErr, "Error setting schemaFile");
            status = StatusInval;
            goto CommonExit;
        }
        schemaFileJson = NULL;  // Reference stolen

        break;

    default:
        break;
    }

    status = StatusOk;
CommonExit:
    if (schemaFileJson != NULL) {
        json_decref(schemaFileJson);
        schemaFileJson = NULL;
    }
    if (status != StatusOk) {
        if (jsonObj != NULL) {
            json_decref(jsonObj);
            jsonObj = NULL;
        }
    }

    return jsonObj;
}

//
// CsvParser
//

Status
CsvParser::init(ParseArgs *parseArgs,
                ParseOptimizerArgs *optimizerArgs,
                DataPageWriter *writer,
                google::protobuf::Arena *arena,
                IRecordSink *pageCallback)
{
    Status status = StatusOk;
    json_t *parserJson = NULL;
    json_error_t jsonError;

    pageCapacity_ = writer->pageCapacity();

    parseArgs_ = parseArgs;
    optimizerArgs_ = optimizerArgs;

    callback_ = pageCallback;

    writer_ = writer;

    arena_ = arena;

    headerAlloc_.init(HeaderBufPageSize);
    headerMPool_.init(&headerAlloc_);

    // We need to be able to store the largest record, which is going to be
    // roughly the size of a record. This is assuming 0 DataPage overhead,
    // which is not necessarily true, so this is sufficient
    fieldAlloc_.init(pageCapacity_ + MemPool::PageOverhead);
    fieldMPool_.init(&fieldAlloc_);

    parserJson = json_loads(parseArgs->parserArgJson, 0, &jsonError);
    if (parserJson == NULL) {
        status = callback_->err(-1,
                                "Csv Parser argument parse error: '%s'",
                                jsonError.text);
        status = StatusInval;
        goto CommonExit;
    }

    status = params_.setFromJson(parseArgs,
                                 parserJson,
                                 errorStringBuf_,
                                 sizeof(errorStringBuf_));
    BailIfFailed(status);

CommonExit:
    if (parserJson) {
        json_decref(parserJson);
        parserJson = NULL;
    }

    return status;
}

Status
CsvParser::addField()
{
    Status status = StatusOk;
    int64_t fieldPos;
    int64_t fieldLen;
    int fieldIndex;

    fieldIndex = numFields_;

    if (unlikely(fieldIndex >= maxNumFields_)) {
        status =
            callback_->err(numRecords_,
                           "max num fields exceeded, max: %u, position %lu",
                           maxNumFields_,
                           fieldStartPos_);
        goto CommonExit;
    }

    // This field is now guaranteed to be structurally valid

    // Check if this field is greater than the number of headers we have
    // discovered. If not, we need to create a name for this field.
    assert(fieldIndex <= numHeaders_ &&
           "we must see have seen all prev fields");

    if (!inHeader_ && fieldIndex >= numHeaders_) {
        // Render a default-named column. This occurs when there are
        // more fields in a record than there are field names.
        // Keep these around for future rows
        char *newHeader;
        status = generateFillerHeader(fieldIndex, &newHeader);
        BailIfFailed(status);

        // we rendered this; it should be good
        assert(
            DataPageFieldMeta::fieldNameAllowed(newHeader, strlen(newHeader)));
        status = addDiscoveredHeader(newHeader, DfString);
        BailIfFailed(status);
        assert(fieldIndex + 1 == numHeaders_ &&
               "we just added this new header");
    }

    // We now have metadata to track fields of this index
    // Lets figure out where the actual field itself is
    if (unlikely(quoteStartPos_ != -1)) {
        fieldPos = quoteStartPos_;
        fieldLen = quoteEndPos_ - quoteStartPos_;
    } else {
        fieldPos = fieldStartPos_;
        fieldLen = fieldEndPos_ - fieldStartPos_;
    }
    assert(fieldLen >= 0);

    // Now we handle remembering the field value, if we need it
    if (unlikely(inHeader_)) {
        char *newHeader = NULL;
        // Add this field as a header
        if (unlikely(fieldLen > DfMaxFieldNameLen)) {
            char *fieldNameBuf =
                static_cast<char *>(fieldMPool_.alloc(fieldLen + 1));
            if (fieldNameBuf) {
                lexer_->dereference(fieldPos,
                                    fieldLen,
                                    (uint8_t *) fieldNameBuf);
                fieldNameBuf[fieldLen] = '\0';
            }

            status =
                callback_->err(-1,
                               "Field name %.*s is too long (%lu chars). "
                               "Max is %u chars",
                               (int) fieldLen,
                               fieldNameBuf ? fieldNameBuf : "unknown field",
                               fieldLen,
                               DfMaxFieldNameLen);
            goto CommonExit;
        }
        if (fieldLen == 0) {
            status = generateFillerHeader(fieldIndex, &newHeader);
            BailIfFailed(status);
            assert(newHeader != NULL);
            assert(DataPageFieldMeta::fieldNameAllowed(newHeader,
                                                       strlen(newHeader)));
        } else {
            newHeader = (char *) headerMPool_.alloc(fieldLen + 1);
            BailIfNull(newHeader);

            lexer_->dereference(fieldPos, fieldLen, (uint8_t *) newHeader);
            newHeader[fieldLen] = '\0';

            assert(UTF8::isUTF8((uint8_t *) newHeader, fieldLen));
            assert((int) strlen(newHeader) == fieldLen);
        }
        status = addDiscoveredHeader(newHeader, DfString);
        BailIfFailed(status);
        assert(fieldIndex + 1 == numHeaders_ &&
               "we just added this new header");
    } else if (headers_[fieldIndex].fieldDesired) {
        // Add this field as a record field
        assert(fieldLen >= 0);
        if (fieldLen > 0) {
            char *newField =
                static_cast<char *>(fieldMPool_.alloc(fieldLen + 1));
            BailIfNull(newField);

            lexer_->dereference(fieldPos, fieldLen, (uint8_t *) newField);
            newField[fieldLen] = '\0';

            assert(UTF8::isUTF8((uint8_t *) newField, fieldLen));

            char *curPos = newField;
            int newLen = fieldLen;
            if (params_.dialect == CsvDialectXcalarSnapshot) {
                // unescape the quote chars for xcalar snapshot dialect
                assert(strlen(params_.escapeDelim) == 1);
                assert(strlen(params_.quoteDelim) == 1);
                int ii = 1;
                for (; ii <= fieldLen; ii++) {
                    if (ii < fieldLen &&
                        (newField[ii - 1] == params_.escapeDelim[0] &&
                         (newField[ii] == params_.quoteDelim[0] ||
                          newField[ii] == params_.escapeDelim[0]))) {
                        *curPos = newField[ii];
                        ii++;
                    } else {
                        *curPos = newField[ii - 1];
                    }
                    curPos++;
                }
                newLen = curPos - newField;
                newField[newLen] = '\0';
            }

            recordFields_[fieldIndex].str = newField;
            recordFields_[fieldIndex].strLen = newLen;
        } else {
            if (quoteStartPos_ != -1) {
                // a zero length field with a quote is an empty string
                recordFields_[fieldIndex].str = "";
            } else {
                // a zero length field without a quotes is a null
                recordFields_[fieldIndex].str = NULL;
            }

            recordFields_[fieldIndex].strLen = 0;
        }
    }

    numFields_++;

    lexer_->setParseCursor(fieldPos + fieldLen);
    quoteStartPos_ = -1;
    quoteEndPos_ = -1;

CommonExit:
    return status;
}

Status
CsvParser::castAndSetProto(DfFieldType type,
                           const char *value,
                           size_t strLen,
                           bool *fatalError)
{
    Status status = StatusUnknown;
    *fatalError = false;

    // Only simple fieldTypes are allowed
    DfFieldValue valIn, valOut;

    valIn.stringVal.strActual = value;
    valIn.stringVal.strSize = strLen + 1;

    status = DataFormat::convertValueType(type,
                                          DfString,
                                          &valIn,
                                          &valOut,
                                          0,
                                          Base10);
    if (status != StatusOk) {
        *fatalError = false;
        goto CommonExit;
    }

    status = dValue_.setFromFieldValue(&valOut, type);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
CsvParser::addRecord()
{
    Status status = StatusOk;
    DataPageWriter::PageStatus pageStatus;
    bool triedOnce = false;

    if (unlikely(inHeader_)) {
        inHeader_ = false;
        goto CommonExit;
    }

    while (true) {
        DataPageWriter::Record *writeRecord;
        status = writer_->newRecord(&writeRecord);
        BailIfFailed(status);
        assert(writeRecord != NULL);

        writeRecord->setFieldNameCache(&fieldMetaCache_);

        // Add all the fields to this record
        for (int ii = 0; ii < numFields_; ii++) {
            assert(ii < numHeaders_);
            assert(numHeaders_ <= maxNumFields_ && "guaranteed by addField");

            if (headers_[ii].fieldDesired) {
                // skip this field if it's empty and we want empty as FNF
                if (params_.emptyAsFnf && recordFields_[ii].str == NULL) {
                    continue;
                }

                // Build the value
                const char *value =
                    recordFields_[ii].strLen == 0 ? "" : recordFields_[ii].str;

                if (headers_[ii].fieldType == DfString) {
                    dValue_.setString(value, recordFields_[ii].strLen);
                } else {
                    bool fatalError = false;
                    status = castAndSetProto(headers_[ii].fieldType,
                                             value,
                                             recordFields_[ii].strLen,
                                             &fatalError);
                    if (status != StatusOk) {
                        if (fatalError) {
                            goto CommonExit;
                        }
                    }
                }

                if (likely(status == StatusOk)) {
                    status =
                        writeRecord->addFieldByIndex(headers_[ii].cacheIndex,
                                                     &dValue_);
                    BailIfFailed(status);
                } else {
                    // reset status for the next field
                    status = StatusOk;
                }
            }
        }
        status =
            callback_->addRecord(numRecords_, writeRecord, &pageStatus, NULL);
        BailIfFailed(status);

        if (unlikely(pageStatus == DataPageWriter::PageStatus::Full)) {
            if (unlikely(triedOnce)) {
                status = callback_->err(numRecords_,
                                        "max record size exceeded; max: %u, "
                                        "position: %lu",
                                        pageCapacity_,
                                        fieldStartPos_);
                goto CommonExit;
            }

            status = writePage();
            BailIfFailed(status);

            triedOnce = true;
            continue;
        } else {
            break;
        }
    }
    fieldMPool_.resetPool();

    ++numRecords_;
CommonExit:
    numFields_ = 0;
    return status;
}

Status
CsvParser::writePage()
{
    Status status = StatusOk;

    status = callback_->writePage();
    BailIfFailed(status);

    fieldMetaCache_.clearCache();

CommonExit:
    return status;
}

Status
CsvParser::addDiscoveredHeader(const char *fieldName, DfFieldType fieldType)
{
    Status status = StatusOk;
    int fieldNameLen = strlen(fieldName);
    // Check if this field name has already been taken
    for (int ii = 0; ii < numHeaders_; ii++) {
        if (strcmp(fieldName, headers_[ii].fieldName) == 0) {
            status = callback_->err(-1,
                                    "Field '%s' in column %i has the same name "
                                    "as column %i",
                                    fieldName,
                                    ii,
                                    numHeaders_);
            goto CommonExit;
        }
    }

    if (unlikely(
            !DataPageFieldMeta::fieldNameAllowed(fieldName, fieldNameLen))) {
        status = callback_->err(numRecords_,
                                "Field header '%s' in column %i is not allowed",
                                fieldName,
                                numHeaders_);
        goto CommonExit;
    }

    headers_[numHeaders_].fieldName = fieldName;
    headers_[numHeaders_].fieldDesired = true;
    headers_[numHeaders_].fieldType = fieldType;
    if (optimizerArgs_->numFieldsRequired > 0) {
        // If the caller requested specific fields, ignore all else
        headers_[numHeaders_].fieldDesired = false;
        for (int ii = 0; ii < (int) optimizerArgs_->numFieldsRequired; ii++) {
            if (strcmp(fieldName, optimizerArgs_->fieldNames[ii]) == 0) {
                headers_[numHeaders_].fieldDesired = true;
                break;
            }
        }
    }

    if (headers_[numHeaders_].fieldDesired) {
        int desiredIndex = numDesiredFound_;
        headers_[numHeaders_].cacheIndex = desiredIndex;

        ValueType fieldType =
            dfTypeToValueType(headers_[numHeaders_].fieldType);
        fieldMetaCache_.setField(desiredIndex, fieldName, fieldType);
        numDesiredFound_++;
    }
    ++numHeaders_;
    assert(numHeaders_ <= maxNumFields_);
    assert(numDesiredFound_ <= numHeaders_);

CommonExit:
    return status;
}

Status
CsvParser::generateFillerHeader(int index, char **newHeader)
{
    Status status = StatusOk;
    *newHeader = NULL;

    int newHeaderLen = snprintf(NULL, 0, "column%d", index);
    *newHeader = (char *) headerMPool_.alloc(newHeaderLen + 1);
    BailIfNull(newHeader);
    snprintf(*newHeader, newHeaderLen + 1, "column%d", index);

CommonExit:
    if (status != StatusOk) {
        memFree(*newHeader);
        *newHeader = NULL;
    }
    return status;
}

// Parses a CSV string (buf) into a single dictionary segment. Replaces all
// delimiters (both types) in buf with \0 and notes the start of each record
// in dictSegment.
Status
CsvParser::parseCsvRecords()
{
    Status status;
    int numRecsSkipped = 0;
    enum ParseState {
        ParseStateSkipping,
        ParseStateNormal,
        ParseStateInQuote,
        ParseStateEscape,
    };
    ParseState parseState = ParseStateNormal;
    ParseState returnState = ParseStateNormal;
    int maxNumHeaders = maxNumFields_;
    StringRef recordFields[maxNumFields_];
    bool finished;
    ParseEvent event;
    numRecords_ = 0;
    numHeaders_ = 0;
    numFields_ = 0;

    // Make these stack varaibles available to called functions within the class
    recordFields_ = recordFields;

    if (params_.linesToSkip) {
        if (!params_.recordDelim) {
            status = StatusSkipRecordNeedsDelim;
            goto CommonExit;
        }
        parseState = ParseStateSkipping;
    }

    fieldStartPos_ = 0;
    inHeader_ = (params_.schemaMode == CsvSchemaModeUseHeader);

    switch (params_.schemaMode) {
    case CsvSchemaModeNoneProvided:
        inHeader_ = false;
        break;
    case CsvSchemaModeUseHeader:
        inHeader_ = true;
        break;
    case CsvSchemaModeUseSchemaFile:
        // load.py should have populated loadArgs.csv.typedColumns for us
        // from the schemaFile. So just fall through
    case CsvSchemaModeUseLoadInput:
        inHeader_ = false;
        for (unsigned ii = 0; ii < params_.typedColumnsCount; ii++) {
            char *newHeader;
            size_t colNameLen = strlen(params_.typedColumns[ii].colName);

            if (colNameLen == 0) {
                status = generateFillerHeader(ii, &newHeader);
                if (status != StatusOk) {
                    Status ignored = callback_->err(-1,
                                                    "Failed to generate filler "
                                                    "header for column %u: %s",
                                                    ii,
                                                    strGetFromStatus(status));
                    (void) ignored;
                    goto CommonExit;
                }
                assert(newHeader != NULL);
            } else {
                newHeader = (char *) headerMPool_.alloc(colNameLen + 1);
                if (newHeader == NULL) {
                    status = StatusNoMem;
                    Status ignored =
                        callback_
                            ->err(-1,
                                  "Insufficient memory to allocate for \"%s\"",
                                  params_.typedColumns[ii].colName);
                    (void) ignored;
                    goto CommonExit;
                }
                strlcpy(newHeader,
                        params_.typedColumns[ii].colName,
                        colNameLen + 1);
            }

            status = addDiscoveredHeader(newHeader,
                                         params_.typedColumns[ii].colType);
            BailIfFailed(status);
        }
        break;
    default:
        status =
            callback_->err(-1, "Invalid schemaMode %u", params_.schemaMode);
        status = StatusInval;
        assert("Invalid schemaMode" && 0);
        goto CommonExit;
    }

    // finished should be set to true once we get end-of-file
    finished = false;
    while (!finished && (status = lexer_->nextEvent(&event)) == StatusOk) {
        assert(numHeaders_ <= maxNumHeaders);
        assert(numFields_ <= maxNumFields_);

        // If this event is NOT right after an escape, then we should just
        // totally ignore the fact that there was an escape event at all
        if (unlikely(parseState == ParseStateEscape) &&
            event.start != escapeEndPos_) {
            escapeEndPos_ = -1;
            parseState = returnState;
        }

        switch (parseState) {
        case ParseStateNormal:
            // end of record
            switch (event.type) {
            case ParseEventType::FieldDelim:
                // We've reached the end of the field delimiter;
                // match made. Subtract off the matched characters so
                // they don't show up in the field value
                fieldEndPos_ = event.start;
                status = addField();
                BailIfFailed(status);
                fieldStartPos_ = event.end;
                break;

            case ParseEventType::RecordDelim:
                fieldEndPos_ = event.start;
                status = addField();
                BailIfFailed(status);

                status = addRecord();
                BailIfFailed(status);
                fieldStartPos_ = event.end;
                break;

            case ParseEventType::QuoteDelim:
                parseState = ParseStateInQuote;
                quoteStartPos_ = event.end;
                // If we've already seen a pair of quotes, discount that pair
                if (unlikely(quoteEndPos_ != -1)) {
                    quoteEndPos_ = -1;
                }
                break;

            case ParseEventType::EscapeDelim:
                returnState = parseState;
                parseState = ParseStateEscape;
                escapeEndPos_ = event.end;
                break;

            case ParseEventType::EndOfFile:
                fieldEndPos_ = event.end;

                // This constitutes a new field if it is non-zero length
                // or if we already have some fields in this row.
                if (fieldEndPos_ > fieldStartPos_ || numFields_) {
                    status = addField();
                    BailIfFailed(status);
                }

                if (numFields_) {
                    status = addRecord();
                    BailIfFailed(status);
                }
                finished = true;
                break;

            default:
                status = callback_->err(-1, "Invalid event %hhd", event.type);
                status = StatusInval;
                assert("Invalid parse event" && false);
                goto CommonExit;
            }
            break;

        case ParseStateInQuote:
            if (event.type == ParseEventType::EscapeDelim) {
                returnState = parseState;
                parseState = ParseStateEscape;
                escapeEndPos_ = event.end;
            } else if (event.type == ParseEventType::QuoteDelim) {
                quoteEndPos_ = event.start;
                parseState = ParseStateNormal;
            } else if (event.type == ParseEventType::EndOfFile) {
                status =
                    callback_->err(numRecords_,
                                   "Mismatched quote, position: %lu",
                                   quoteStartPos_ - strlen(params_.quoteDelim));
                goto CommonExit;
            }
            break;

        case ParseStateEscape:
            if (event.type == ParseEventType::EndOfFile) {
                status =
                    callback_
                        ->err(numRecords_,
                              "Unexpected end of file while escaping at: %lu",
                              event.start);
                goto CommonExit;
            }
            parseState = returnState;
            break;

        case ParseStateSkipping:
            if (event.type == ParseEventType::RecordDelim) {
                ++numRecsSkipped;
                fieldStartPos_ = event.end;
                lexer_->setParseCursor(fieldStartPos_);
                if (numRecsSkipped == params_.linesToSkip) {
                    parseState = ParseStateNormal;
                }
            } else if (event.type == ParseEventType::EndOfFile) {
                // Note that we are allowing end-of-file; this could be a
                // parse error if we so decided
                finished = true;
                goto CommonExit;
            }
            break;
        }
    }
    if (status == StatusNoBufs) {
        status = callback_->err(numRecords_,
                                "Field too long, start position: %lu. "
                                "Suspect an escaped quote, for example: "
                                "\"hello\\world\\\"",
                                fieldStartPos_);
        goto CommonExit;
    }
    BailIfFailed(status);

    assert(numFields_ <= maxNumFields_);
    assert(numHeaders_ <= maxNumHeaders);

    // The last page is left to the caller to flush

CommonExit:
    // headers will be deallocated when memPool is destructed
    recordFields_ = NULL;

    return status;
}

Status
CsvParser::parseData(const char *fileName, IFileReader *fileReader)
{
    Status status;
    XcalarConfig *config = XcalarConfig::get();

    lexer_ = new (std::nothrow) CsvLexer();

    status = lexer_->init(fileReader,
                          pageCapacity_,
                          params_.fieldDelim,
                          params_.recordDelim,
                          params_.quoteDelim,
                          params_.escapeDelim);
    BailIfFailed(status);

    // Check this so we can allow usage without Config being set up in tests
    if (config) {
        maxNumFields_ = config->dfMaxFieldsPerRecord_;
    }

    status = fieldMetaCache_.init(maxNumFields_, writer_);
    BailIfFailed(status);

    headers_ = new (std::nothrow) Header[maxNumFields_];
    BailIfNull(headers_);
    numHeaders_ = 0;
    numDesiredFound_ = 0;

    status = parseCsvRecords();
    BailIfFailed(status);

    status = StatusOk;

CommonExit:
    headerMPool_.resetPool();
    if (headers_) {
        delete[] headers_;
        headers_ = NULL;
    }
    if (lexer_) {
        delete lexer_;
        lexer_ = NULL;
    }
    return status;
}

//
// CsvFormatOps
//

Status
CsvFormatOps::init()
{
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(CsvFormatOps), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    CsvFormatOps::instance = new (ptr) CsvFormatOps();

    return StatusOk;
}

CsvFormatOps *
CsvFormatOps::get()
{
    assert(instance);
    return instance;
}

void
CsvFormatOps::destroy()
{
    CsvFormatOps *inst = CsvFormatOps::get();

    inst->~CsvFormatOps();
    memFree(inst);
    CsvFormatOps::instance = NULL;
}

IRecordParser *
CsvFormatOps::getParser()
{
    CsvParser *parser = new (std::nothrow) CsvParser();
    return parser;
}

IRowRenderer *
CsvFormatOps::getRowRenderer()
{
    CsvRowRenderer *renderer = new (std::nothrow) CsvRowRenderer();
    return renderer;
}

Status
CsvFormatOps::requiresFullSchema(
    const ExInitExportFormatSpecificArgs *formatArgs, bool *requiresFullSchema)
{
    *requiresFullSchema = false;
    return StatusOk;
}

// Note that valueDesc is ignored, because csv doesn't need type info to begin
Status
CsvFormatOps::renderColumnHeaders(
    const char *srcTableName,
    int numColumns,
    const char **headerColumns,
    const ExInitExportFormatSpecificArgs *formatArgs,
    const TupleValueDesc *valueDesc,
    char *buf,
    size_t bufSize,
    size_t *bytesWritten)
{
    Status status;
    int ii;
    size_t bufUsed;
    const ExInitExportCSVArgs *args;
    char fieldDelim;
    char recordDelim;

    assert(numColumns > 0);

    args = &(formatArgs->csv);
    fieldDelim = args->fieldDelim;
    recordDelim = args->recordDelim;

    if ((fieldDelim == '\0') || (recordDelim == '\0')) {
        status = StatusInval;
        goto CommonExit;
    }

    bufUsed = 0;
    for (ii = 0; ii < numColumns; ii++) {
        // 2 because delim + \0
        // OVERFLOW! This should not happen because the buffer was created
        // With MaxFields+1 * maxColumnNameLength (Gives space for \0)
        assert(bufSize >= bufUsed + strlen(headerColumns[ii]) + 2);
        // NOTE: This piece of code is tricky. strlcpy returns the string
        // length NOT INCLUDING THE NULL TERMINATOR
        bufUsed += strlcpy(buf + bufUsed, headerColumns[ii], bufSize - bufUsed);
        if (ii < numColumns - 1) {
            buf[bufUsed] = fieldDelim;
            bufUsed++;
        } else {
            buf[bufUsed] = recordDelim;
            bufUsed++;
        }
    }

    // Note that this does not null terminate
    assert(buf[bufUsed - 1] == recordDelim);
    assert(bufUsed <= bufSize);
    *bytesWritten = bufUsed;
    status = StatusOk;

CommonExit:
    return status;
}

Status
CsvRowRenderer::writeField(char *buf,
                           int64_t bufSize,
                           DfFieldType dfType,
                           const DfFieldValue *fieldValue,
                           int64_t *bytesWritten) const
{
    unsigned ii;
    Status status = StatusOk;
    DfFieldValue dst;
    dst.stringVal.strActual = buf;
    dst.stringValTmp = (char *) dst.stringVal.strActual;
    char escapeChar = '\\';

    const DfFieldValue *nonStringValue;

    const char *valueStr;
    int64_t valueStrLen;

    assert(dfType != DfUnknown);

    // Here we have 2 main codepaths: strings and non-strings.
    // We have another set of branches: scalars and non-scalars.
    // For all strings: extract the actual string value and size
    // For all non-strings: extract the value, then convert
    if (dfType == DfScalarObj) {
        dfType = fieldValue->scalarVal->fieldType;
        if (dfType == DfString) {
            valueStr = fieldValue->scalarVal->fieldVals.strVals[0].strActual;
            valueStrLen = fieldValue->scalarVal->fieldVals.strVals[0].strSize;
        } else {
            nonStringValue = (DfFieldValue *) &fieldValue->scalarVal->fieldVals;
        }
    } else if (dfType == DfString) {
        valueStr = fieldValue->stringVal.strActual;
        valueStrLen = fieldValue->stringVal.strSize;
    } else {
        nonStringValue = fieldValue;
    }

    if (dfType != DfString) {
        // Formats the DfFieldValue src as a string and writes into dst
        status = DataFormat::convertValueType(DfString,
                                              dfType,
                                              nonStringValue,
                                              &dst,
                                              bufSize,
                                              BaseCanonicalForm);
        if (status != StatusOk) {
            *bytesWritten = 0;
            goto CommonExit;
        }
        // strSize contains null terminated character we want to overwrite
        *bytesWritten = dst.stringVal.strSize - 1;
    } else {
        bool quoteField = false;
        int64_t numEscaped = 0;
        *bytesWritten = 0;
        // Check if we need to quote this string
        if (valueStrLen == 1) {
            assert(valueStr[0] == '\0');

            // this is the empty string, quote it
            quoteField = true;
        }

        for (ii = 0; ii < valueStrLen - 1; ii++) {
            char c = valueStr[ii];
            if (c == args_->recordDelim || c == args_->fieldDelim ||
                c == args_->quoteDelim) {
                quoteField = true;
            }
            if (c == args_->quoteDelim) {
                ++numEscaped;
            }
        }
        // -1 for \0
        int64_t formattedSize = valueStrLen - 1;
        if (quoteField) {
            // +2 for quotes, +numEscaped for escapes
            formattedSize += 2 + numEscaped;
        }
        if ((int64_t) bufSize < formattedSize) {
            *bytesWritten = 0;
            status = StatusNoBufs;
            goto CommonExit;
        }
        if (quoteField) {
            // Start quote
            buf[*bytesWritten] = args_->quoteDelim;
            (*bytesWritten)++;
        }
        // strSize contains null terminated character
        for (ii = 0; ii < valueStrLen - 1; ii++) {
            char c = valueStr[ii];
            // Here we escape quote characters.
            // We don't escape record or field delims, since if they are present
            // the whole string is quoted
            if (quoteField && c == args_->quoteDelim) {
                buf[*bytesWritten] = escapeChar;
                (*bytesWritten)++;
            }
            if (!quoteField) {
                assert(c != args_->recordDelim);
                assert(c != args_->fieldDelim);
            }
            buf[*bytesWritten] = c;
            (*bytesWritten)++;
        }
        if (quoteField) {
            // End quote
            buf[*bytesWritten] = args_->quoteDelim;
            (*bytesWritten)++;
        }
        assert((int64_t) *bytesWritten == formattedSize);
    }
    assert(*bytesWritten <= bufSize);
CommonExit:

    return status;
}

Status
CsvFormatOps::renderPrelude(const TupleValueDesc *valueDesc,
                            const ExInitExportFormatSpecificArgs *formatArgs,
                            char *buf,
                            size_t bufSize,
                            size_t *bytesWritten)
{
    *bytesWritten = 0;
    return StatusOk;
}

Status
CsvRowRenderer::init(const ExInitExportFormatSpecificArgs *formatArgs,
                     int numFields,
                     const Field *fields)
{
    Status status = StatusOk;
    // We only need csv, forget the formatArgs union
    args_ = &formatArgs->csv;
    numFields_ = numFields;
    fields_ = fields;

    assert(args_->fieldDelim);
    assert(args_->recordDelim);
    assert(args_->quoteDelim);

    return status;
}

Status
CsvRowRenderer::renderRow(const NewKeyValueEntry *entry,
                          bool first,
                          char *buf,
                          int64_t bufSize,
                          int64_t *bytesWritten)
{
    Status status;
    // 1 for field/record delims, 1 for \0)
    int64_t colBytesWritten = 0;
    int64_t bufUsed = 0;
    size_t entryNumFields = entry->kvMeta_->tupMeta_->getNumFields();

    // We want to go until we have no more columns
    for (int ii = 0; ii < numFields_; ii++) {
        int ind = fields_[ii].entryIndex;
        DfFieldType typeTmp = entry->kvMeta_->tupMeta_->getFieldType(ind);
        bool retIsValid;
        DfFieldValue valueTmp =
            entry->tuple_.get(ind, entryNumFields, typeTmp, &retIsValid);
        if (!retIsValid) {
            colBytesWritten = 0;
            status = StatusOk;
        } else {
            status = writeField(buf + bufUsed,
                                bufSize - bufUsed,
                                fields_[ii].type,
                                &valueTmp,
                                &colBytesWritten);
        }
        BailIfFailed(status);

        bufUsed += colBytesWritten;
        assert(bufUsed <= bufSize);

        if (unlikely(bufUsed + 1 > bufSize)) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        if (ii < numFields_ - 1) {
            buf[bufUsed] = args_->fieldDelim;
        } else {
            buf[bufUsed] = args_->recordDelim;
        }
        bufUsed++;
    }

    assert(bufUsed <= bufSize);
    *bytesWritten = bufUsed;

    status = StatusOk;
CommonExit:
    return status;
}

Status
CsvFormatOps::renderFooter(char *buf, size_t bufSize, size_t *bytesWritten)
{
    *bytesWritten = 0;
    return StatusOk;
}
