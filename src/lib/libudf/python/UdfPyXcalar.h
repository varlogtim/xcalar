// Copyright 2016-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef UDFPYXCALAR_H
#define UDFPYXCALAR_H

#include <Python.h>

#include "primitives/Primitives.h"

#include "datapage/DataPage.h"
#include "df/DataFormat.h"
#include "child/Child.h"
#include "util/Vector.h"
#include "df/Importable.h"
#include "PageWriter.h"

//
// Class for exposing "xcalar" Python module to UDFs. Injects a module whose
// methods live in libudfpy.
//

class UdfPyXcalar
{
  public:
    UdfPyXcalar();
    ~UdfPyXcalar();

    MustCheck Status injectModule();

    static inline uint64_t xpuComputeNumXdbPagesRequiredOnWire(
        uint64_t payloadLen, int64_t pageSize)
    {
        uint64_t payloadPlusHdrLen = payloadLen + sizeof(Child::XpuCommTag);
        return (payloadPlusHdrLen / pageSize) +
               ((payloadPlusHdrLen % pageSize) ? 1 : 0);
    }

  private:
    static constexpr const char *PyModuleName = "_xpu_host";
    static constexpr const char *PyHelpText = "Xcalar Python XPU utilities";
    static struct PyModuleDef InjectedModule;

    PyObject *module;
    static PyMethodDef methods[];

    static PyObject *initModule();

    // The following functions are accessible via python code.
    static MustCheck PyObject *getSocketPath(PyObject *self, PyObject *args);
    static MustCheck PyObject *getBufCacheBaseAddr(PyObject *self,
                                                   PyObject *args);
    static MustCheck PyObject *getXdbPageSize(PyObject *self, PyObject *args);
    static MustCheck PyObject *streamOutRecords(PyObject *self, PyObject *args);
    static MustCheck PyObject *streamRawFiles(PyObject *self, PyObject *args);
    static MustCheck PyObject *getNodeId(PyObject *self, PyObject *args);
    static MustCheck PyObject *getNodeCount(PyObject *self, PyObject *args);
    static MustCheck PyObject *getTxnId(PyObject *self, PyObject *args);
    static MustCheck PyObject *getChildId(PyObject *self, PyObject *args);
    static MustCheck PyObject *getXpuId(PyObject *self, PyObject *args);
    static MustCheck PyObject *getXpuClusterSize(PyObject *self,
                                                 PyObject *args);
    static MustCheck PyObject *getConfigPath(PyObject *self, PyObject *args);
    static MustCheck PyObject *getUserIdName(PyObject *self, PyObject *args);
    static MustCheck PyObject *parseDataPageToCsv(PyObject *self,
                                                  PyObject *args);
    static MustCheck PyObject *convertMoneyToString(PyObject *self,
                                                    PyObject *args);
    static MustCheck PyObject *castColumns(PyObject *self, PyObject *args);
    static MustCheck PyObject *xpuRecvString(PyObject *self, PyObject *args);
    static MustCheck PyObject *getXpuStartIdInNode(PyObject *self,
                                                   PyObject *args);
    static MustCheck PyObject *getXpuEndIdInNode(PyObject *self,
                                                 PyObject *args);
    static MustCheck PyObject *xpuSendStringFanout(PyObject *self,
                                                   PyObject *args);
    static MustCheck PyObject *xpuComputeNumXdbPagesRequiredOnWire(
        PyObject *self, PyObject *args);
    static MustCheck PyObject *isApp(PyObject *self, PyObject *args);
    static MustCheck PyObject *getAppName(PyObject *self, PyObject *args);
    static MustCheck PyObject *getParentPerfInfo(PyObject *self,
                                                 PyObject *args);
    static MustCheck PyObject *findSchema(PyObject *self, PyObject *args);
};

struct FileError {
    FileError() = default;
    FileError(int64_t recordNum, char *errorString)
        : errStr(errorString), recNum(recordNum)
    {
    }

    ~FileError()
    {
        if (errStr) {
            memFree(errStr);
            errStr = NULL;
        }
    }

    char *errStr = NULL;
    // recNum within the file
    int64_t recNum = -1;
};

struct FileReport {
    FileReport() = default;
    ~FileReport()
    {
        for (int64_t ii = 0; ii < errors.size(); ii++) {
            delete errors.get(ii);
        }
        if (fullPath) {
            memFree(fullPath);
            fullPath = NULL;
        }
    }
    void reset()
    {
        for (int64_t ii = 0; ii < errors.size(); ii++) {
            delete errors.get(ii);
        }
        errors.clear();

        if (fullPath) {
            memFree(fullPath);
            fullPath = NULL;
        }
        fileDataSize = 0;
        numRecordsInFile = 0;
        numTotalErrors = 0;
        numErrors = 0;
    }

    char *fullPath = NULL;
    int32_t fullPathLen = NULL;
    DataSourceArgs sourceArgs;
    size_t fileDataSize = 0;
    int64_t numRecordsInFile = 0;
    int64_t numTotalErrors = 0;
    int64_t numErrors = 0;
    Vector<FileError *> errors;
};

class IFileParser
{
  public:
    virtual ~IFileParser() = default;
    virtual Status processFile(const char *fileName, PyObject *file) = 0;
};

// Streams records from XPU to XCE
class PyRecordStreamer final : public IRecordSink
{
  public:
    PyRecordStreamer() = default;
    ~PyRecordStreamer() = default;

    // args must stay in scope for the lifetime of this instance
    // args should be tuple with members described in init
    MustCheck bool init(PyObject *xceDataset, PyObject *parseArgsObj);

    Status processFiles(PyObject *fileGen, IFileParser *parser);

    Status writePage() override;

    // Caller should abort on return != StatusOk; see reportError()
    Status err(int64_t recordNum, const char *fmt, ...) override
        __attribute__((format(printf, 3, 4)));

    // Returns non-StatusOk if the caller should abort due to this error.
    // This may be because the user has elected to fail promptly on any error,
    // even if the parser is able to continue to process records.
    // (The caller may be unable to continue after the error anyway)
    Status reportError(int64_t recordNum, const char *errStr);

    // Catches the last python exception and puts it in the error report
    Status pyExceptionToReport();

    // Raises a python exception from the last error in the error report
    void raiseExceptionFromReport();

    Status addRecord(int64_t recordNum,
                     DataPageWriter::Record *record,
                     DataPageWriter::PageStatus *pageStatus,
                     int32_t *bytesOverflow) override;

    Status addRecord(int64_t recordNum,
                     PageWriter::Record *record);

    Status castFieldValueToProto(DfFieldValue *field,
                                 DfFieldType type,
                                 ProtoFieldValue *protoValue);

    DataPageWriter *writer() { return &writer_; }

    google::protobuf::Arena *arena() { return &arena_; }

    ParseArgs *parseArgs() { return &parseArgs_; }

    ParseOptimizerArgs *optimizerArgs() { return &optimizerArgs_; }

    PageWriter &fixedSchemaWriter() { return fixedSchemaPageWriter_; }

    PyObject *xceDataset() { return xceDataset_; }
    // returns NULL in error and sets PyErr_Format appropriately
    static MustCheck uint8_t *getPageFromObject(PyObject *pageObj);

  private:
    static MustCheck bool extractStringFromDict(PyObject *dict,
                                                const char *dictName,
                                                const char *keyName,
                                                char *result,
                                                int maxResultSize);

    bool extractParseArgs(PyObject *parseArgsObj);
    bool extractParseOptimizerArgs(PyObject *parseArgsObj);
    bool extractSourceArgs(PyObject *sourceArgsObj);

    MustCheck Status dumpPage(bool isError);

    MustCheck Status reportFile();

    MustCheck Status packReport(int64_t numErrors,
                                DataPageWriter::Record **record);

    bool reportError();

    // Parsed arguments
    ParseArgs parseArgs_;

    // Arguments used for optimizing queries
    ParseOptimizerArgs optimizerArgs_;

    PageWriter fixedSchemaPageWriter_;

    bool addFileName() const { return parseArgs_.fileNameFieldName[0] != '\0'; }

    bool addRecordNum() const
    {
        return parseArgs_.recordNumFieldName[0] != '\0';
    }

    // Sink for the output stream of filled pages of data/errors
    // Source for the input stream of empty pages
    PyObject *xceDataset_ = NULL;

    // Accelerators
    google::protobuf::Arena arena_;
    google::protobuf::Arena errorArena_;
    DataPageWriter writer_;
    DataPageWriter errorWriter_;
    ProtoFieldValue pValue_;

    // Members
    int64_t numErrors_ = 0;
    char errorStringBuf_[4 * KB];
    FileReport fileReport_;
};

// Stream processor which takes in files and a sUDF and outputs a stream of
// DataPages with data and errors.
// This class does not use Status because it use PyErr to set exceptions
// whenever a failure occurs. Instead it uses bool of true to indicate success
class PyUdfProcessor final : public IFileParser
{
  public:
    PyUdfProcessor();
    virtual ~PyUdfProcessor();

    // args must stay in scope for the lifetime of this object
    MustCheck bool init(PyObject *args);

    Status processFile(const char *fileName, PyObject *file) override;

    MustCheck bool run();

  private:
    class ConvertCtx
    {
      public:
        ConvertCtx(PyObject *decimalModDec = NULL,
                   ProtoFieldValue *value = NULL)
            : decimalModDec(decimalModDec), value(value){};

        PyObject *decimalModDec;
        ProtoFieldValue *value;
    };

    static bool convertObjectToProto(PyObject *obj, ConvertCtx *ctx);

    MustCheck bool convertObjectToDataValue(PyObject *obj,
                                            TypedDataValue *value);

    // returns status != StatusOk in fatal error;
    // *record will be NULL if no valid conversion was achievable
    MustCheck Status convertDictToRecord(PyObject *dict,
                                         DataPageWriter::Record **record);
    MustCheck Status convertDictToRecord(PyObject *dict,
                                         PageWriter::Record *record);

    Status processFileWithSchema(PyObject *recordGen);
    Status processFileWithoutSchema(PyObject *recordGen);

    // Parsed arguments
    PyObject *inFileGen_ = NULL;
    PyObject *parseFile_ = NULL;

    // Streamer state
    int64_t recordNum_ = 0;

    PyObject *decimalClass_;

    PyRecordStreamer recordStream_;
};

class PyFileReader final : public IFileReader
{
  public:
    PyFileReader() = default;
    ~PyFileReader();

    MustCheck Status init(PyObject *inputFile);

    MustCheck Status readChunk(int64_t numBytes,
                               const uint8_t **data,
                               int64_t *actualBytes) override;

  private:
    PyObject *inFile_ = NULL;
    static constexpr const char *ReadFuncName = "read";
    static constexpr const char *ReadArgs = "l";

    PyObject *readString_ = NULL;
};

// Processes raw files, writing them back to XCE; mostly handles gluing the
// parsers to the calling python code
class PyRawStreamer final : public IFileParser
{
  public:
    PyRawStreamer() = default;
    ~PyRawStreamer();

    // args must stay in scope for the lifetime of this instance
    // args should be tuple with members described in init
    MustCheck bool init(PyObject *args);
    MustCheck bool run();
    Status processFile(const char *fileName, PyObject *file) override;

  private:
    bool reportError();

    // Parsed arguments
    PyObject *inFileGen_ = NULL;
    DfFormatType formatType_ = DfFormatUnknown;

    // Instance variables
    bool encounteredFatalError_ = false;
    PyRecordStreamer recordStream_;
    IRecordParser *parser_ = NULL;
};

// Sends payloads up from source XPU to XCE parent using SHM buffers, on way
// to a list of destination XPUs.
class PySendStringFanout final
{
  public:
    PySendStringFanout() = default;
    ~PySendStringFanout()
    {
        if (messages_) {
            delete[] messages_;
        }
    }

    // args must stay in scope for the lifetime of this object
    MustCheck bool init(PyObject *args);

    MustCheck bool run();

  private:
    // returns NULL in error and sets PyErr_Format appropriately
    static MustCheck uint8_t *getPageFromObject(PyObject *pageObj);

    // Parsed arguments
    int64_t pageSize_ = -1;
    PyObject *getBuf_ = NULL;
    PyObject *putBuf_ = NULL;
    PyObject *putLastBuf_ = NULL;
    PyObject *sendAllBufs_ = NULL;
    PyObject *xpuSendListPy_ = NULL;
    char *xpuCommTagStr_ = NULL;
    Child::XpuCommTag xpuCommTag_ = Child::XpuCommTag::CtagInvalid;

    // internal state
    struct SingleMessage {
        int64_t dstXpuId_ = -1;
        const char *payloadString_ = NULL;
        int payloadLength_ = NULL;
    };
    SingleMessage *messages_ = NULL;
    int nXpus_ = 0;
};

// Buffer for putting a bunch of strings together.
// XXX this might be generally useful and we could move this into libutil
class StringBuilder final
{
  public:
    StringBuilder() = default;
    ~StringBuilder() = default;

    MustCheck Status init(int64_t initialSize)
    {
        return elemBuf_.init(initialSize);
    }

    MustCheck Status append(const char *str, int64_t len)
    {
        Status status;
        assert(len >= 0);
        status = elemBuf_.growBuffer(len);
        BailIfFailed(status);

        memcpy(&elemBuf_.buf[elemBuf_.bufSize], str, len);
        elemBuf_.bufSize += len;

    CommonExit:
        return status;
    }

    const char *getString() const { return elemBuf_.buf; }

    int64_t size() const { return elemBuf_.bufSize; }

    void clear() { elemBuf_.bufSize = 0; }

  private:
    ElemBuf<char> elemBuf_;
};

class CsvBuilder
{
  public:
    CsvBuilder() = default;
    ~CsvBuilder();

    // Initialze the builder; this is called once and the object should be
    // reused for each row
    MustCheck Status init(char fieldDelim,
                          char recordDelim,
                          char quoteDelim,
                          DfFormatType formatType);

    // Add a field that has a valid value. This value will be converted to a
    // string for the purposes of going into the CSV row
    MustCheck Status addField(const DataValueReader *valueReader);

    // Add an empty field. Not all records have all fields, so we need to be
    // able to do this without a DataValueReader, but the API could probably
    // be refactored to incorporate this fact better.
    MustCheck Status addEmptyField();

    // Finalize the CSV row and get its value. This value is valid until
    // the CsvBuilder is clear()ed or destructed.
    MustCheck Status finishRow(const char **fullRow, int64_t *length);

    // Reset the internal structures for this builder, allowing it to be
    // used for a new CSV row.
    void clear();

  private:
    static constexpr int32_t InitialStringBufferSize = 1 * MB;

    // Rendering state
    bool first_ = true;
    StringBuilder string_;
    char *buf_ = NULL;

    // Parameters
    char recordDelim_ = '\0';
    char fieldDelim_ = '\0';
    char quoteDelim_ = '\0';
    char escapeDelim_ = '\\';  // hardcoded for now

    // representation could be either csv(external) or internal
    DfFormatType formatType_ = DfFormatCsv;
};

#endif  // UDFPYXCALAR_H
