// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <assert.h>
#include <fcntl.h>
#include <libgen.h>

#include "primitives/Primitives.h"
#include "dataformat/DataFormatCsv.h"
#include "util/FileUtils.h"
#include "util/Stopwatch.h"
#include "common/InitTeardown.h"
#include "test/QA.h"  // Must be last

static Status longCsvTest();

static TestCase testCases[] = {
    {"longCsvTest", longCsvTest, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass);

class CsvBenchmarker final : public IRecordSink, public IFileReader
{
  public:
    Status runTest();
    CsvBenchmarker()
    {
        size_t ret;
        ret = snprintf(this->TestFile,
                       sizeof(this->TestFile),
                       "%s/flight/airlines_2007.csv",
                       qaGetQaDir());
        assert(ret <= sizeof(this->TestFile));
    }
    ~CsvBenchmarker();

    // IRecordSink
    Status writePage() override;
    Status err(int64_t recordNum, const char *fmt, ...) override;

    Status addRecord(int64_t recordNum,
                     DataPageWriter::Record *record,
                     DataPageWriter::PageStatus *pageStatus,
                     int32_t *bytesOverflow) override;

    // IFileReader
    MustCheck Status readChunk(int64_t numBytes,
                               const uint8_t **data,
                               int64_t *actualBytes) override;

  private:
    char TestFile[QaMaxQaDirLen + 1];
    static constexpr uint64_t FileBufferSize = 4096;
    int fd_ = -1;
    int64_t bytesRead_ = 0;
    DataPageWriter writer_;
    google::protobuf::Arena arena_;
    char errorStringBuf_[256];
    // One additional as readChunk adds null terminator
    uint8_t fileBuffer_[FileBufferSize + 1];
    uint8_t *outBuf_ = NULL;
};

CsvBenchmarker::~CsvBenchmarker()
{
    if (outBuf_ != NULL) {
        memFree(outBuf_);
        outBuf_ = NULL;
    }
}

Status
CsvBenchmarker::writePage()
{
    Status status = StatusOk;

    writer_.serializeToPage(outBuf_);

    writer_.clear();
    arena_.Reset();

    return status;
}

Status
CsvBenchmarker::err(int64_t recordNum, const char *fmt, ...)
{
    assert(false);
    return StatusFailed;
}

Status
CsvBenchmarker::addRecord(int64_t numRecords,
                          DataPageWriter::Record *record,
                          DataPageWriter::PageStatus *pageStatus,
                          int32_t *bytesOverflow)
{
    Status status = StatusOk;
    status = writer_.commit(record, pageStatus, bytesOverflow);
    assert(status == StatusOk);
    return StatusOk;
}

Status
CsvBenchmarker::readChunk(int64_t numBytes,
                          const uint8_t **data,
                          int64_t *actualBytes)
{
    Status status = StatusOk;
    int requestedBytes = numBytes ? numBytes : FileBufferSize;

    assert(requestedBytes <= (int) FileBufferSize);
    status = FileUtils::convergentRead(fd_,
                                       fileBuffer_,
                                       requestedBytes,
                                       (size_t *) actualBytes);
    if (status == StatusEof) {
        status = StatusOk;
    }
    if (*actualBytes > 0) {
        fileBuffer_[*actualBytes] = '\0';
    }
    assert(status == StatusOk);
    bytesRead_ += *actualBytes;
    *data = fileBuffer_;

    return status;
}

Status
CsvBenchmarker::runTest()
{
    Status status = StatusOk;
    ParseArgs *parseArgs = new ParseArgs();                        // let throw
    ParseOptimizerArgs *optimizerArgs = new ParseOptimizerArgs();  // let throw
    optimizerArgs->numFieldsRequired = 0;

    CsvParser *parser = new CsvParser();                          // let throw
    CsvParser::Parameters *params = new CsvParser::Parameters();  // let throw
    unsigned long elapsedMSecs;
    unsigned long hours = 0;
    unsigned long minutes = 0;
    unsigned long seconds = 0;
    unsigned long millis = 0;

    // Init this object
    fd_ = open(TestFile, O_RDONLY);
    assert(fd_ != -1);
    status = writer_.init(128 * KB);
    assert(status == StatusOk);
    outBuf_ = static_cast<uint8_t *>(memAlloc(128 * KB));

    memZero(parseArgs, sizeof(*parseArgs));
    params->schemaMode = CsvSchemaModeUseHeader;
    params->isCRLF = false;
    params->fieldDelim[0] = ',';
    params->fieldDelim[1] = '\0';
    params->linesToSkip = 0;
    params->recordDelim[0] = '\n';
    params->recordDelim[1] = '\0';
    parseArgs->fieldNamesCount = 1;

    json_t *paramJson = params->getJson();
    char *paramJsonStr = json_dumps(paramJson, 0);
    json_decref(paramJson);
    paramJson = NULL;
    strlcpy(parseArgs->parserArgJson,
            paramJsonStr,
            sizeof(parseArgs->parserArgJson));
    // @SymbolCheckIgnore
    free(paramJsonStr);
    paramJsonStr = NULL;

    strlcpy(parseArgs->fieldNames[0],
            "TypeOfInformation",
            sizeof(parseArgs->fieldNames[0]));

    status = parser->init(parseArgs, optimizerArgs, &writer_, &arena_, this);
    assert(status == StatusOk);

    Stopwatch stopwatch;
    status = parser->parseData("FakeFile.txt", this);
    stopwatch.stop();
    assert(status == StatusOk);

    stopwatch.getPrintableTime(hours, minutes, seconds, millis);

    elapsedMSecs = stopwatch.getElapsedNSecs() / NSecsPerMSec;

    float coreThroughput = ((float) bytesRead_) / 1000.0 / elapsedMSecs;

    fprintf(stderr,
            "Read %ld bytes in %02f seconds, (%f MB/sec)\n"
            "Theoretical max on  8 cores: %f MB/sec\n"
            "Theoretical max on 32 cores: %f GB/sec\n",
            bytesRead_,
            ((float) elapsedMSecs) / 1000,
            coreThroughput,
            coreThroughput * 8,
            coreThroughput / 1000 * 32);

    delete parseArgs;
    delete optimizerArgs;
    delete parser;
    delete params;

    return status;
}

Status
longCsvTest()
{
    Status status = StatusOk;
    CsvBenchmarker benchmarker;

    benchmarker.runTest();

    return status;
}

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);
    Status status = StatusOk;

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    //
    // Run the actual tests
    //

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
