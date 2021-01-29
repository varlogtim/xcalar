import os
import time

from xcalar.compute.util.Qa import XcalarQaScratchShare, DefaultTargetName

from xcalar.external.LegacyApi.Count import DatasetCount, TableCount
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.result_set import ResultSet
from xcalar.external.LegacyApi.Dataset import UdfDataset, CsvDataset

from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiException


class UdfTestUtils(object):
    def __init__(self, client, session, xcalarApi, udf):
        self.client = client
        self.session = session
        self.xcalarApi = xcalarApi
        self.udf = udf
        self.op = Operators(self.xcalarApi)
        self.addedUtilFunctions = False

    # Add util functions used in other methods in this class.
    def addUtilFunctions(self):
        if self.addedUtilFunctions:
            return
        utilFns = """
def verifyResultEqualsExpected(*args):
    if len(args) == 0:
        return 1
    firstArg = str(args[0])
    for arg in args:
        if str(arg) != firstArg:
            return 0
    return 1

def mirror(arg):
    return arg
"""
        self.udf.addOrUpdate("pyTestUdfUtil", utilFns)
        self.addedUtilFunctions = True

    # Generate a unique name for something.
    def uniqueName(self, entity):
        return (entity + str(time.time()) + str(os.getpid())).replace(".", "")

    def loadAsDataset(self,
                      inputRows,
                      datasetName,
                      column,
                      addHeader=True,
                      hasHeader=True,
                      udfFunction=None):
        datasetContent = ""
        if addHeader:
            datasetContent = "rowIndex,%s;" % column
        if hasHeader:
            schemaMode = "header"
        else:
            schemaMode = "none"

        # Append an index to beginning of each row and join all with ; as a delimiter.
        datasetContent += ";".join(
            ["%u,%s" % (i, inputRows[i]) for i in range(len(inputRows))])

        datasetFilePath = XcalarQaScratchShare + "/" + datasetName
        datasetFile = open(datasetFilePath, "w")
        datasetFile.write(datasetContent)
        datasetFile.close()

        if udfFunction:
            dataset = UdfDataset(self.xcalarApi, DefaultTargetName,
                                 datasetFilePath, datasetName, udfFunction)
        else:
            dataset = CsvDataset(
                self.xcalarApi,
                DefaultTargetName,
                datasetFilePath,
                datasetName,
                recordDelim=';',
                fieldDelim=',',
                schemaMode=schemaMode)
        return dataset.load()

    def onLoad(self, inputRows, udfFunction, verifyFn, hasHeader=False):
        datasetName = self.uniqueName("testExecOnLoadDataset")
        self.loadAsDataset(
            inputRows,
            datasetName,
            "ignored",
            addHeader=False,
            hasHeader=hasHeader,
            udfFunction=udfFunction)
        rs = ResultSet(self.client, dataset_name=".XcalarDS." + datasetName, session_name=self.session.name)
        rows = [row for row in rs.record_iterator()]
        verifyFn(rows)

    # Load input into table and execute moduleName:functionName on each row. Verify that matches
    # expectedOutput array.
    def executeOnArrayAndVerify(self, moduleName, functionName, input,
                                expectedOutput):
        self.addUtilFunctions()
        operators = Operators(self.xcalarApi)

        prefix = "udfexec" + moduleName + functionName + str(time.time())

        nameDatasetInput = prefix + "-input"
        nameDatasetExpected = prefix + "-output"
        nameIndexInput = prefix + "-inputIndex"
        nameIndexExpected = prefix + "-outputIndex"
        nameJoin = prefix + "-join"
        nameMap = prefix + "-exec"
        nameMapResult = prefix + "-result"
        nameMapMirror = prefix + "-mirror"
        nameMapMirrorEqual = prefix + "-mirroreq"
        nameAggregate = prefix + "-aggregate"
        nameAggregateMirror = prefix + "-mirrorag"

        # Load both input and expected output into tables (nameIndexInput, nameIndexExpected).
        self.loadAsDataset(input, nameDatasetInput, "input")
        self.loadAsDataset(expectedOutput, nameDatasetExpected, "output")
        nameDatasetInput = ".XcalarDS." + nameDatasetInput
        nameDatasetExpected = ".XcalarDS." + nameDatasetExpected
        assert DatasetCount(self.xcalarApi,
                            nameDatasetInput).total() == len(input)
        assert DatasetCount(self.xcalarApi,
                            nameDatasetExpected).total() == len(expectedOutput)

        operators.indexDataset(nameDatasetInput, nameIndexInput, "rowIndex",
                               "p")
        assert TableCount(self.xcalarApi, nameIndexInput).total() == len(input)

        operators.indexDataset(nameDatasetExpected, nameIndexExpected,
                               "rowIndex", "p1")
        assert TableCount(self.xcalarApi,
                          nameIndexExpected).total() == len(expectedOutput)

        # Execute function on input array and join to expected output.
        evalStr = "%s:%s(p::input)" % (moduleName, functionName)
        operators.map(nameIndexInput, nameMap, evalStr, "result")

        operators.join(nameMap, nameIndexExpected, nameJoin)
        assert TableCount(self.xcalarApi,
                          nameJoin).total() == len(expectedOutput)

        # Count up the rows where actual == expected.
        operators.map(
            nameJoin, nameMapResult,
            "pyTestUdfUtil:verifyResultEqualsExpected(p1::output, result)",
            "verify")
        assert TableCount(self.xcalarApi,
                          nameMapResult).total() == len(expectedOutput)

        result = operators.aggregate(nameMapResult, nameAggregate,
                                     "sum(int(verify))")
        assert int(result) == len(expectedOutput)

        # Mirror step is added to test map with immediates only.
        operators.map(nameMapResult, nameMapMirror,
                      "pyTestUdfUtil:mirror(verify)", "verifyMirror")

        # Make sure the mirrored 'verifyMirror' column equals the original 'verify' column.
        operators.map(
            nameMapMirror, nameMapMirrorEqual,
            "pyTestUdfUtil:verifyResultEqualsExpected(verify, verifyMirror)",
            "sum(int(verifyMirror))")
        result = operators.aggregate(nameMapMirrorEqual, nameAggregateMirror,
                                     "sum(int(verifyMirror))")
        assert int(result) == len(expectedOutput)

    def executeAndVerify(self, srcTable, evalStr, expected):
        dstTable = self.uniqueName("dstTable")
        dstTableSorted = dstTable + "-sort"
        dstCol = self.uniqueName("dstColumn")

        self.op.map(srcTable, dstTable, evalStr, dstCol)
        try:
            self.op.indexTable(
                dstTable,
                dstTableSorted,
                dstCol,
                ordering=XcalarOrderingT.XcalarOrderingAscending)
        except XcalarApiException as e:
            self.op.dropTable(dstTable)
            raise e

        resultSet = ResultSet(self.client, table_name=dstTableSorted, session_name=self.session.name)
        assert resultSet.record_count() == len(expected)

        ii = 0
        expected = sorted([str(exp) for exp in expected])
        try:
            for row in resultSet.record_iterator():
                assert row[dstCol] == expected[ii]
                ii += 1
        except KeyError:
            print("No results available")

        del resultSet
        self.op.dropTable(dstTableSorted)
        self.op.dropTable(dstTable)
