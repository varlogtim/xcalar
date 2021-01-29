from multiprocessing import Pool
import traceback
import pytest
import filecmp
import os
import getpass
import socket
import uuid
from shutil import rmtree

from xcalar.compute.util.Qa import buildNativePath, XcalarQaDatasetPath
from xcalar.compute.util.Qa import XcalarQaHDFSHost, DefaultTargetName

from xcalar.external.LegacyApi.Env import XcalarConfigPath
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Dataset import CsvDataset, JsonDataset, Dataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Export import Export

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT

from xcalar.external.client import Client
from xcalar.compute.util.config import build_config

testExportSessionName = "TestExport"

localExportDir = XcalarConfigPath

pytestmark = pytest.mark.skip("skipped until new export is complete")

hdfsHost = "hdfs-sanity"
if XcalarQaHDFSHost:
    hdfsHost = XcalarQaHDFSHost

udfTargetName = "pytest_udfLocal"

machineName = "{username}-{hostname}-{macaddress}".format(
    username=getpass.getuser(),
    hostname=socket.gethostname(),
    macaddress=uuid.getnode())

hdfsUdfTargetName = "pytest_hdfsUdf"
hdfsUdfTargetUrl = "hdfs://root@{}:50070/{}/exports/pytestHdfsConnect".format(
    hdfsHost, machineName)

s3UdfTargetName = "pytest_s3Udf"
s3UdfTargetPath = "exports"

nestedTargetName = "pytest_nestedTarget"

exportUdfTargetName = "pyTestUdfExport"
sourceExportUdf = """
from os.path import exists, dirname
import json
import os

def main(inStr):
    inObj = json.loads(inStr)
    contents = inObj["fileContents"]
    fullPath = inObj["filePath"]
    fn = fullPath.replace("nfs://", "").replace("file://", "").replace("localfile://", "")
    if not exists(dirname(fn)):
        try:
            os.makedirs(dirname(fn))
        except:
            pass
    with open(fn, "w") as f:
        f.write(contents)
"""

exportHdfsTargetName = "pyTestHdfsExport"
sourceHdfsExport = """
from urllib.parse import urlparse, urlunparse
import json

from pywebhdfs.webhdfs import PyWebHdfsClient

def main(inStr):
    inObj = json.loads(inStr)
    contents = inObj["fileContents"]
    url = inObj["filePath"]
    parsed = urlparse(url)
    client = PyWebHdfsClient(host=parsed.hostname, port=parsed.port, user_name=parsed.username)
    path = parsed.path.strip("/")
    # This overwrites all previously exported files, so if the directory has
    # been exported to previously, it will have files from both exports,
    # which is undesirable
    client.create_file(path, contents, overwrite=True)
"""

exportS3TargetName = "pyTestS3Export"
sourceS3Export = """
from urllib.parse import urlparse, urlunparse
import io
import json

import boto3

def main(inStr):
    inObj = json.loads(inStr)
    contents = io.BytesIO(inObj["fileContents"])
    url = inObj["filePath"]
    parsed = urlparse(url)

    clientArgs = {
        'aws_access_key_id': 'K3CC2YMWIGSZVR09Q3WO',
        'aws_secret_access_key': 'xqfitN99bb6l2nZRTcOqvsTrR66gCCSKaQssSu4Z',
        'endpoint_url': 'https://minio.int.xcalar.com',
        'region_name': 'us-east-1',
        'verify': False
    }
    bucket = "temp.dw.xcalar"
    client = boto3.client("s3", **clientArgs)

    client.upload_fileobj(contents, bucket, url)
"""


class TestExport(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        cls.operators = Operators(cls.xcalarApi)
        cls.export = Export(cls.xcalarApi)
        cls.udf = Udf(cls.xcalarApi)
        cls.session = cls.client.create_session(testExportSessionName)
        cls.xcalarApi.setSession(cls.session)

        cls.configDict = build_config(XcalarConfigPath).all_options

        cls.udf.addOrUpdate(exportUdfTargetName, sourceExportUdf)
        cls.udf.addOrUpdate(exportHdfsTargetName, sourceHdfsExport)
        cls.udf.addOrUpdate(exportS3TargetName, sourceS3Export)

    def teardown_class(cls):
        cls.operators.dropTable("*", deleteCompletely=True)
        cls.operators.dropExportNodes("*", deleteCompletely=True)
        cls.operators.dropConstants("*", deleteCompletely=True)
        Dataset.bulkDelete(cls.xcalarApi, "*", deleteCompletely=True)
        cls.client.destroy_session(testExportSessionName)
        cls.xcalarApi.setSession(None)
        cls.session = None
        cls.xcalarApi = None

    # Called once per class and shared
    @pytest.fixture(
        scope="class",
        params=[("exportTests/telecom.csv",
                 ["State", "Phone Number", "Total Day Charge"])])
    def table(self, request):
        fileName = request.param[0]
        columnNames = request.param[1]
        path = buildNativePath(fileName)
        datasetName = "{}-dataset".format(fileName)
        tableName = "{}-table".format(fileName)
        # Index by the first column listed
        keyName = columnNames[0]

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="header",
            isRecursive=False)
        dataset.load()
        self.operators.indexDataset(dataset.name, tableName, keyName, "p")
        return (tableName, columnNames)

    @pytest.mark.parametrize("headerType", [
        pytest.mark.xfail(
            "unknown", reason="invalid header type", strict=True), "none",
        "every", "separate"
    ])
    @pytest.mark.parametrize(
        "splitRule",
        [
            pytest.mark.xfail(
                ("unknown", 0), reason="invalid split type", strict=True),
            ("none", 0),
            ("single", 0),
            pytest.mark.xfail(
                ("size", 0), reason="split size must be > 0", strict=True),
            ("size", 2**10),    # 1KB
            ("size", 2**20),    # 1MB
            ("size", 2**30),    # 1GB
            ("size", 2**40),    # 1TB
        ])
    @pytest.mark.parametrize("createRule", [
        pytest.mark.xfail(
            "unknown", reason="invalid create rule type", strict=True),
        pytest.mark.mayfail(
            pytest.mark.xfail(
                "createOnly",
                reason="the suite may be run twice")), "createOrAppend",
        pytest.mark.xfail(
            "appendOnly", reason="nothing to append to", strict=True),
        "deleteAndReplace"
    ])
    @pytest.mark.parametrize("exFormat",
                             ["csv"])    # use format defaults for now
    @pytest.mark.parametrize("targetName", [
        "Default",
    ])
    def testCombinations(self, table, headerType, splitRule, createRule,
                         exFormat, targetName):
        """Tests all possible combinations of various export parameters
        Does not perform any validation of the output, only checking that the
        usrnode returns StatusOk and doesn't crash.
        """
        uniqueTestId = str(
            abs(
                hash((str(table), headerType, splitRule, createRule,
                      str(exFormat), targetName))))
        testName = "exportPytest-{}".format(uniqueTestId[:6])
        tableName = table[0]
        columnNames = table[1]
        # Index by the first column listed
        keyName = columnNames[0]
        splitType = splitRule[0]
        maxSize = splitRule[1]    # This is only used for split on size

        if headerType == "separate" and splitType == "single":
            pytest.xfail("single file and separate header are contradictory")

        if (headerType != "none" and
            (createRule == "appendOnly" or createRule == "createOrAppend")):
            pytest.xfail("cannot export separate header with appending")

        if exFormat == "csv":
            fileName = testName + ".csv"
            print("Exporting table '{}' to file '{}'".format(
                tableName, fileName))
            self.export.csv(
                tableName,
                columnNames,
                fileName=fileName,
                headerType=headerType,
                splitRule=splitType,
                maxSize=maxSize,
                createType=createRule,
                targetName=targetName)
        elif exFormat == "sql":
            fileName = testName + ".sql"
            self.export.sql(
                tableName,
                columnNames,
                fileName=fileName,
                headerType=headerType,
                splitRule=splitType,
                maxSize=maxSize,
                createType=createRule,
                targetName=targetName)
        else:
            assert False

        return

    @pytest.mark.parametrize("headerType", [
        pytest.mark.xfail(
            "unknown", reason="invalid header type", strict=True), "none",
        "every", "separate"
    ])
    @pytest.mark.parametrize("exFormat",
                             ["csv"])    # use format defaults for now
    @pytest.mark.parametrize("targetName", [udfTargetName])
    def testCombinationsUdf(self, table, headerType, exFormat, targetName):
        """Tests all possible combinations of various export parameters
        Does not perform any validation of the output, only checking that the
        usrnode returns StatusOk and doesn't crash.
        """
        tableName = table[0]
        columnNames = table[1]
        uniqueTestId = str(
            abs(hash((str(table), headerType, str(exFormat), targetName))))
        testName = "exportPytest-{}".format(uniqueTestId[:6])
        # Index by the first column listed
        keyName = columnNames[0]

        if exFormat == "csv":
            fileName = testName + ".csv"
            print("Exporting table '{}' to file '{}'".format(
                tableName, fileName))
            self.export.csv(
                tableName,
                columnNames,
                fileName=fileName,
                headerType=headerType,
                targetName=targetName,
                isUdf=True)
        elif exFormat == "sql":
            fileName = testName + ".csv"
            print("Exporting table '{}' to file '{}'".format(
                tableName, fileName))
            self.export.sql(
                tableName,
                columnNames,
                fileName=fileName,
                headerType=headerType,
                targetName=targetName,
                isUdf=True)
        else:
            assert False

        return

    def testManyRowCorrectness(self):
        filename = "exportTests/manyRow.csv"
        path = buildNativePath(filename)
        datasetName = "manyRowDs"
        tableName = "manyRowTable"
        columnNames = ["p::column0"]
        destFilename = "test-manyRowExp.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase, destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/manyRow-expected.csv")

        expectedNumRows = 1048576

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="none",
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(
            dataset.name,
            tableName,
            'column0',
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fileName=destFilename)

        # Verify the exported file
        assert filecmp.cmp(localResult, expectedFile)
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testEmptyStringVsNull(self):
        filename = "exportTests/emptyStringVsNull.csv"
        path = buildNativePath(filename)
        datasetName = "emptyStringVsNull"
        tableName = "emptyStringVsNull"
        columnNames = ["p::column0", "p::column1", "p::column2"]
        destFilename = "test-emptyStringVsNull.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase, destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/emptyStringVsNull.csv")

        expectedNumRows = 2

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="none",
            isRecursive=False,
            emptyAsFnf=True,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(
            dataset.name,
            tableName,
            'column0',
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            fieldDelim=',',
            makeSorted=True,
            fileName=destFilename)

        # Verify the exported file
        assert filecmp.cmp(localResult, expectedFile)
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    @pytest.mark.skip(
        reason="reimplement export resource cleanup on failureXc-7165")
    def testAbortedExport(self):
        filename = "exportTests/manyRow.csv"
        path = buildNativePath(filename)
        datasetName = "manyRowDsAborted"
        tableName = "manyRowTableAborted"
        columnNames = ["p::column0"]
        destFilename = "test-manyRowExpAborted.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/manyRow-expected.csv")

        expectedNumRows = 1048576

        # Remove the export destination as the test depends on it being newly
        # created.
        try:
            rmtree(localResult)
        except OSError:
            # tree might not exist
            pass

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="none",
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(
            dataset.name,
            tableName,
            'column0',
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fileName=destFilename)

        # Verify the exported directory doesn't exist (meaning Txn abort has
        # cleaned it up.
        assert not os.path.exists(localResult)
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testMinimalQuoting(self):
        filename = "exportTests/minimalQuote.csv"
        path = buildNativePath(filename)
        datasetName = "minimalQuote"
        tableName = "minQuoteTable"
        columnNames = [["p::row", "row"], ["p::firstCol", "firstCol"],
                       ["p::secondCol", "secondCol"],
                       ["p::lastCol", "lastCol"]]
        destFilename = "test-minQuote.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase, destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/minimalQuote.csv")

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            quoteDelim='\'',
            schemaMode="header",
            isRecursive=False)
        dataset.load()

        self.operators.indexDataset(
            dataset.name,
            tableName,
            'row',
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        self.export.csv(
            tableName,
            columnNames,
            headerType="every",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fileName=destFilename,
            fieldDelim=',',
            quoteDelim='\'')

        # Verify the exported file
        assert filecmp.cmp(localResult, expectedFile)
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testManyQuotes(self):
        filename = "exportTests/quoteFileIn.csv"
        path = buildNativePath(filename)
        datasetName = "manyQuote"
        tableName = "manyQuoteTable"
        columnNames = [["p::column{}".format(ii), "column{}".format(ii)]
                       for ii in range(10)]
        destFilename = "test-manyQuote.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase, destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/quoteFileOut.csv")

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim='\t',
            quoteDelim='',
            schemaMode="none",
            isRecursive=False)
        dataset.load()

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="every",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fileName=destFilename,
            fieldDelim='\t',
            quoteDelim='\"')

        # Verify the exported file
        assert filecmp.cmp(localResult, expectedFile)
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    @pytest.mark.parametrize("numColumns", [
        96,
    ])
    def testManyColCorrectness(self, numColumns):
        filename = "exportTests/manyCol.csv"
        path = buildNativePath(filename)
        datasetName = "Ds{}Cols".format(numColumns)
        tableName = "Table{}Cols".format(numColumns)
        columnNames = ["p::column{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-manyCol.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResultFile = os.path.join(localExportDir, destFileBase,
                                       destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath, filename)

        expectedNumRows = 1000

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="none",
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        # Verify the exported file
        with open(localResultFile, "r") as f:
            localResult = sorted(f.readlines())
        with open(expectedFile, "r") as f:
            expectedResult = sorted(f.readlines())
        assert localResult == expectedResult
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testManyIntColCorrectness(self):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "manyIntColsDs"
        tableName = "manyIntColsTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-manyIntCols.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResultFile = os.path.join(localExportDir, destFileBase,
                                       destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/manyIntCols-expected.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        # Verify the exported file
        with open(localResultFile, "r") as f:
            localResult = sorted(f.readlines())
        with open(expectedFile, "r") as f:
            expectedResult = sorted(f.readlines())
        assert localResult == expectedResult
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    @pytest.mark.xfail(reason="Row exceeds transport page size")
    def testHugeRecCorrectness(self):
        filename = "exportTests/hugeRecords.csv"
        path = buildNativePath(filename)
        datasetName = "hugeRecsDs"
        tableName = "hugeRecsTable"
        numColumns = 96
        columnNames = ["p::column{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-hugeRecords.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResultFile = os.path.join(localExportDir, destFileBase,
                                       destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath, filename)

        expectedNumRows = 1000

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="none",
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        # Verify the exported file
        with open(localResultFile, "r") as f:
            localResult = sorted(f.readlines())
        with open(expectedFile, "r") as f:
            expectedResult = sorted(f.readlines())
        assert localResult == expectedResult
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testHdfsUdf(self, table):
        tableName = table[0]
        columnNames = table[1]
        headerType = "separate"
        exFormat = "csv"
        targetName = hdfsUdfTargetName
        testName = "pytest-export-UDF"
        # Index by the first column listed
        keyName = columnNames[0]

        fileName = testName + ".csv"
        print("Exporting table '{}' to file '{}'".format(tableName, fileName))
        self.export.csv(
            tableName,
            columnNames,
            fileName=fileName,
            headerType=headerType,
            targetName=targetName,
            isUdf=True)
        return

    @pytest.mark.parametrize("exFormat",
                             ["csv"])    # Use format defaults for now
    def testMappedFields(self, table, exFormat):
        tableName = table[0]
        columnNames = table[1]
        headerType = "separate"
        testName = "pytest-export-mapped-Fields-{}".format(exFormat)
        # Index by the first column listed
        keyName = columnNames[0]
        groupedTable = "{}-grouped-{}".format(table[0], testName)
        evalStr = "sum(1)"
        groupField = "groupedVal"

        self.operators.groupBy(table[0], groupedTable, [evalStr], [groupField])

        if exFormat == "csv":
            fileName = testName + ".csv"
        else:
            fileName = testName + ".sql"

        print("Exporting table '{}' to file '{}'".format(tableName, fileName))
        if exFormat == "csv":
            self.export.csv(
                groupedTable, [groupField],
                fileName=fileName,
                headerType=headerType)
        else:
            self.export.sql(
                groupedTable, [groupField],
                fileName=fileName,
                headerType=headerType)
        return

    @pytest.mark.skip(reason="flaky s3")
    def testS3Udf(self, table):
        tableName = table[0]
        columnNames = table[1]
        headerType = "separate"
        exFormat = "csv"
        targetName = s3UdfTargetName
        testName = "pytest-export-UDF-s3"
        # Index by the first column listed
        keyName = columnNames[0]

        fileName = testName + ".csv"
        print("Exporting table '{}' to file '{}'".format(tableName, fileName))
        self.export.csv(
            tableName,
            columnNames,
            fileName=fileName,
            headerType=headerType,
            targetName=targetName,
            isUdf=True)
        return

    def testNestedExport(self):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "nestedExportDs"
        tableName = "nestedExportTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-manyIntCols.csv"
        destFileBase = os.path.splitext(destFilename)[0]

        localResultFile = os.path.join(self.nestedTargetPath, destFileBase,
                                       destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/manyIntCols-expected.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            targetName=nestedTargetName,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        # Verify the exported file
        with open(localResultFile, "r") as f:
            localResult = sorted(f.readlines())
        with open(expectedFile, "r") as f:
            expectedResult = sorted(f.readlines())
        assert localResult == expectedResult
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    @pytest.mark.parametrize("splitRule", ["none", "single"])
    def testExportSubdir(self, splitRule):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "subdirExportDs"
        tableName = "subdirExportTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]
        destFilePath = "testsubdir/testManyIntCols.csv"
        localFilePath = "testsubdir/testManyIntCols/testManyIntCols.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")

        localResultFile = os.path.join(localExportDir, localFilePath)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/manyIntCols-expected.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule=splitRule,
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilePath)

        # Verify the exported file
        with open(expectedFile, "r") as f:
            expectedResult = sorted(f.readlines())

        if splitRule == "none":
            exportDir = os.path.dirname(localResultFile)
            localResult = []
            for fn in os.listdir(exportDir):
                path = os.path.join(exportDir, fn)
                with open(path, "r") as f:
                    localResult.extend(sorted(f.readlines()))
        elif splitRule == "single":
            with open(localResultFile, "r") as f:
                localResult = sorted(f.readlines())
        assert localResult == expectedResult
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()
        rmtree(os.path.dirname(os.path.dirname(localResultFile)))

    def testSingleSepHeadCorrectness(self):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "singleSepHeadDs"
        tableName = "singleSepHeadTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-singleSepHead.csv"
        destHeadname = "test-singleSepHead-header.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localDataResult = os.path.join(localExportDir, destFileBase,
                                       destFilename)
        localHeadResult = os.path.join(localExportDir, destFileBase,
                                       destHeadname)
        expectedDataFile = os.path.join(
            XcalarQaDatasetPath, "exportTests/manyIntCols-expected.csv")
        expectedHeadFile = os.path.join(
            XcalarQaDatasetPath, "exportTests/manyIntCols-expected-head.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="separate",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        # Verify the exported data file
        assert filecmp.cmp(localDataResult, expectedDataFile)
        # Verify exported header file
        assert filecmp.cmp(localHeadResult, expectedHeadFile)
        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    # If you do an append on a single-export file, you cannot use the option
    # for 'header on every file', because this would mean you have to edit
    # the original file we are looking at, which isn't supported
    def testSingleAppendHeaderFail(self):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "singleAppendFailDs"
        tableName = "singleAppendFailTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-singleAppend.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResultFile = os.path.join(localExportDir, destFileBase,
                                       destFilename)
        expectedFile = os.path.join(
            XcalarQaDatasetPath, "exportTests/manyIntCols-apnd-expected.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="every",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        try:
            # This should fail, because header operations aren't allowed on append
            self.export.csv(
                tableName,
                columnNames,
                headerType="every",
                splitRule="single",
                createType="appendOnly",
                makeSorted=True,
                fieldDelim=",",
                fileName=destFilename)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusExportSFSingleHeaderConflict

        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testExportFailureErrorPropagation(self):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "testExportFailureErrorPropagation"
        tableName = "dummyTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        dummyFileName = "/dummyFileName"

        try:
            self.export.csv(
                tableName,
                columnNames,
                headerType="none",
                splitRule="single",
                createType="deleteAndReplace",
                makeSorted=True,
                fieldDelim=",",
                fileName=dummyFileName)
        except XcalarApiStatusException as e:
            assert ((dummyFileName in e.result.output.hdr.log) is True)

        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testSingleAppendCorrectness(self):
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "singleAppendDs"
        tableName = "singleAppendTable"
        numColumns = 96
        columnNames = ["p::field{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-singleAppend.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResultFile = os.path.join(localExportDir, destFileBase,
                                       destFilename)
        expectedFile = os.path.join(
            XcalarQaDatasetPath, "exportTests/manyIntCols-apnd-expected.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="appendOnly",
            makeSorted=True,
            fieldDelim=",",
            fileName=destFilename)

        # Verify the exported file
        with open(localResultFile, "r") as f:
            localResult = sorted(f.readlines())
        with open(expectedFile, "r") as f:
            expectedResult = sorted(f.readlines())
        assert localResult == expectedResult

        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()

    def testThreads(self):
        numThreads = 2
        timeoutSecs = 6000

        pool = Pool(processes=numThreads)

        threads = []
        for ii in range(0, numThreads):
            testname = "testThreads-{}".format(ii)
            t = pool.apply_async(
                invoke, (testname, "manyRowCorrectness", [self.configDict]))
            threads.append(t)

        print("threads are all running")
        for t in threads:
            assert t.get(timeoutSecs)
        print("threads have all finished")
        pool.close()
        pool.join()


def invoke(testname, fn, args):
    try:
        worker = ExportThread(testname)
        fn = getattr(worker, fn)
        return fn(*args)
    except Exception:
        traceback.print_exc()
        return None


class ExportThread():
    def __init__(self, testName):
        self.testName = testName
        self.xcalarApi = XcalarApi()
        self.operators = Operators(self.xcalarApi)
        self.export = Export(self.xcalarApi)
        self.session = self.client.create_session("{}-sess".format(testName))
        self.xcalarApi.setSession(self.session)

    def __del__(self):
        self.client.destroy_session(self.session.name)

    def manyRowCorrectness(self, configDict):
        filename = "exportTests/manyRow.csv"
        path = buildNativePath(filename)
        datasetName = "{}-DS".format(self.testName)
        tableName = "{}-table".format(self.testName)
        columnNames = ["p::column0"]
        destFilename = "{}.csv".format(self.testName)
        localExportDir = os.path.join(
            configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase, destFilename)
        expectedFile = os.path.join(XcalarQaDatasetPath,
                                    "exportTests/manyRow-expected.csv")

        expectedNumRows = 1048576

        dataset = CsvDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            fieldDelim=',',
            schemaMode="none",
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(
            dataset.name,
            tableName,
            'column0',
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        self.export.csv(
            tableName,
            columnNames,
            headerType="none",
            splitRule="single",
            createType="deleteAndReplace",
            makeSorted=True,
            fileName=destFilename)

        # Verify the exported file
        assert filecmp.cmp(localResult, expectedFile)

        self.operators.dropTable(tableName, deleteCompletely=True)
        self.operators.dropExportNodes("*", deleteCompletely=True)
        dataset.delete()
        return True
