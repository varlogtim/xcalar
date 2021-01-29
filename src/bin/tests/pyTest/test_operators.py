# Copyright 2018 - 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
from multiprocessing import Pool
from multiprocessing import Process
import traceback
import json
import time
import random
import pytest
import tempfile
import sys
import decimal as dec
import pandas as pd

from xcalar.compute.util.Qa import (freezeRows, getFilteredDicts,
                                    XcalarQaDatasetPath, DefaultTargetName)

from xcalar.external.LegacyApi.XcalarApi import (XcalarApi,
                                                 XcalarApiStatusException)
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.Dataset import UdfDataset, Dataset, CsvDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.WorkItem import WorkItemMap, WorkItemGroupBy
from xcalar.external.result_set import ResultSet
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Export import Export
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.Retina import Retina
from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.JoinOpEnums.ttypes import JoinOperatorT
from xcalar.compute.coretypes.UnionOpEnums.ttypes import UnionOperatorT
from xcalar.compute.coretypes.OrderingEnums.ttypes import XcalarOrderingT
from xcalar.compute.coretypes.LibApisCommon.ttypes import (
    XcalarApiWorkbookScopeT, XcalarApiColumnT)
from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT

TestOpSessionName = "TestOpSession"
TestOpUserName = "TestOpUser"

datasetPrefix = ".XcalarDS."
memDsName = "testOperatorsDs"
datasetName = datasetPrefix + memDsName
NumDatasetRows = 1000

memSmallDsName = "testOpsSmallDs"
dsSmallName = datasetPrefix + memSmallDsName
NumDsRowsSmall = 100

FlakyFrequency = 10
memUdfName = "testOperators"
udfSum = memUdfName + ":sumAll"
udfCursor = memUdfName + ":cursorTest"
udfNone = memUdfName + ":noneTest"
udfTestNone = memUdfName + ":testNone"

memUdfSourcePreamble = """
import json
import random
from datetime import datetime, timedelta
import time
import decimal
import xcalar.container.parent as xce
from xcalar.container.table import Table

def noneTest():
    return None

def cursorTest(tableId):
    table = Table(tableId, [
        {"columnName": "p1-val",
         "headerAlias": "p1-val"}])

    out = 0
    for row in table.all_rows():
        out += row['p1-val']

    return out

def testNone(a):
    if a is None:
        return "Empty"
    return a

def sumAll(*cols):
    sum = 0
    for col in cols:
        sum += int(col)
    return str(sum)

def randdate():
    d1 = datetime.strptime('1/1/1980', '%m/%d/%Y')
    d2 = datetime.strptime('1/1/2017', '%m/%d/%Y')

    d3 = d1 + timedelta(
        seconds=random.randint(0, int((d2 - d1).total_seconds()))
    )

    return d3.strftime("%Y-%m-%d")
"""

memUdfSourceLoad = """
def load(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    rowGen = random.Random()
    rowGen.seed(537)
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {"val": ii, "valDiv10": ii//10, "constant": 1, "date": randdate(), "nullVal": None, "mixedType": None}
        rowDict.update({"numericType": decimal.Decimal('3.14159'), "moneyType": decimal.Decimal('nan')})
        rowDict.update({"moneyTypeAllNan": decimal.Decimal('nan'), "floatType": float('nan'), "floatTypeAllNan": float('nan')})
        if not (ii %% %d):
            rowDict['flaky'] = 1

        rowDict.update({"prn1": rowGen.randint(0, 4), "prn2": rowGen.randint(0, 4), "prn3": rowGen.randint(0, 4)})

        option = rowGen.randint(0, 2)
        if option == 0:
            rowDict['mixedType'] = ii
        elif option == 1:
            rowDict['mixedType'] = str(ii)
        else:
            rowDict['mixedType'] = float(ii)

        option2 = rowGen.randint(0, 4)
        if ii > 0: # First row must be nan to repro ENG-4996
            if option2 == 0:
                rowDict['floatType'] = float(10000.0 * ii)
                rowDict['moneyType'] = decimal.Decimal(10000.0 * ii)
            elif option2 == 1:
                rowDict['floatType'] = float('inf')
                rowDict['moneyType'] = decimal.Decimal('inf')
            elif option2 == 2:
                rowDict['floatType'] = float('nan')
                rowDict['moneyType'] = decimal.Decimal('nan')
            elif option2 == 3:
                rowDict['floatType'] = float('-nan')
                rowDict['moneyType'] = decimal.Decimal('-nan')
            else:
                rowDict['floatType'] = float('-inf')
                rowDict['moneyType'] = decimal.Decimal('-inf')

        yield rowDict
    """ % FlakyFrequency

memUdfSource = memUdfSourcePreamble + memUdfSourceLoad

memoryTargetName = "QA test_operators memory"
targets = [
    (memoryTargetName, "memory", {}),
]

joinDatasetNameLeft = datasetPrefix + "filterJoinLeft"
joinDatasetNameRight = datasetPrefix + "filterJoinRight"

# Join on left.col1 = right.col1 and left.col2 < right.col2
#
# For ones, max(left) < min(right),
# filter always return true, all rows have a match.
#
# For twos, min(left) < min(right) < max(right) < max(left),
# right rows always have a match, left rows don't.
#
# For threes, max(right) < min(left),
# filter always return false, no row has a match.
#
# For fours, min(right) < min(left) < max(left) < max(right),
# left rows always have a match, right rows don't.

joinFilterRawTextLeft = """col1, col2
1,2
1,3
1,3
2,4
2,1
2,4
2,5
3,5
3,5
3,5
4,5
4,5
5,6"""

joinFilterRawTextRight = """col1, col2
1,4
1,5
1,5
2,2
2,2
2,3
3,1
3,2
4,3
4,3
4,6
6,7"""

joinFilterInnerRes = 3 * 3 + 1 * 3 + 0 + 2 * 1
joinFilterLeftOuterRes = 3 * 3 + (1 * 3 + 3) + 3 + 2 * 1 + 1
joinFilterRightOuterRes = 3 * 3 + 1 * 3 + 2 + (2 * 1 + 2) + 1
joinFilterFullOuterRes = 3 * 3 + (1 * 3 + 3) + (3 + 2) + (2 * 1 + 2) + 1 + 1
joinFilterLeftSemiRes = 3 + 1 + 0 + 2
joinFilterLeftAntiRes = 0 + 3 + 3 + 0 + 1


class TestOperators(object):
    def setup_class(cls):
        cls.xcalarApi = XcalarApi()
        cls.client = Client()
        cls.session = Session(
            cls.xcalarApi,
            owner=None,
            username=TestOpUserName,
            sessionName=TestOpSessionName)
        cls.xcalarApi.setSession(cls.session)
        cls.session.activate()
        cls.client = Client()
        cls.client._user_name = TestOpUserName
        cls.operators = Operators(cls.xcalarApi)
        cls.export = Export(cls.xcalarApi)
        cls.retina = Retina(cls.xcalarApi)
        cls.kvStore = cls.client.global_kvstore()
        # Upload UDF used below.
        cls.udf = Udf(cls.xcalarApi)
        cls.udf.addOrUpdate(memUdfName, memUdfSource)
        cls.target = Target2(cls.xcalarApi)

        for targetName, targetType, targetArgs in targets:
            cls.target.add(targetType, targetName, targetArgs)

        path = str(NumDatasetRows)
        dataset = UdfDataset(cls.xcalarApi, memoryTargetName, path, memDsName,
                             "{}:load".format(memUdfName))
        dataset.load()

        path = str(NumDsRowsSmall)
        dataset = UdfDataset(cls.xcalarApi, memoryTargetName, path,
                             memSmallDsName, "{}:load".format(memUdfName))
        dataset.load()
        cls.datasetCols = [
            x.name for x in dataset.getInfo().getDatasetsInfoOutput.
            datasets[0].columns
        ]

        cls.operators.indexDataset(
            datasetName,
            '_dsTab',
            "val",
            fatptrPrefixName="p1",
            ordering=XcalarOrderingT.XcalarOrderingAscending)
        rs = []
        for r in ResultSet(cls.client, table_name='_dsTab', session_name=TestOpSessionName).record_iterator():
            rs.append(r)
        cls.testData = pd.DataFrame(rs)

        cls.operators.dropTable('_dsTab', deleteCompletely=True)

        # Join datasets.
        schema = [
            XcalarApiColumnT("col1", "col1", "DfInt64"),
            XcalarApiColumnT("col2", "col2", "DfInt64")
        ]
        with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
            tmpf.write(joinFilterRawTextLeft)
            tmpf.flush()

            joinDatasetLeft = CsvDataset(
                cls.xcalarApi,
                DefaultTargetName,
                tmpf.name,
                joinDatasetNameLeft,
                fieldDelim=',',
                schemaMode="loadInput",
                schema=schema,
                isRecursive=False,
                linesToSkip=1)
            joinDatasetLeft.load()

        with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
            tmpf.write(joinFilterRawTextRight)
            tmpf.flush()

            joinDatasetRight = CsvDataset(
                cls.xcalarApi,
                DefaultTargetName,
                tmpf.name,
                joinDatasetNameRight,
                fieldDelim=',',
                schemaMode="loadInput",
                schema=schema,
                isRecursive=False,
                linesToSkip=1)
            joinDatasetRight.load()

    def teardown_class(cls):
        cls.operators.dropTable("*", deleteCompletely=True)
        cls.operators.dropConstants("*", deleteCompletely=True)
        cls.operators.dropDatasets("*", deleteCompletely=True)

        Dataset.bulkDelete(cls.xcalarApi, '*', deleteCompletely=True)

        cls.session.destroy()
        cls.xcalarApi.setSession(None)
        cls.session = None
        cls.xcalarApi = None

    @pytest.mark.skip(reason="used for performance testing")
    def PerformanceTest(self):
        tables = ["1", "2", "3"]
        retina = "ret"

        self.operators.indexDataset(datasetName, tables[0], "xcalarRecordNum",
                                    "p")
        self.operators.map(tables[0], tables[1], ["add(p::val, 1)"], "a")

        self.retina.make(retina, [tables[1]], [["a"]], [tables[0]])

        self.retina.execute(retina, [], tables[2])

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def getRowCount(self, srcTable):
        output = self.operators.tableMeta(srcTable)
        srcRows = sum(
            [output.metas[ii].numRows for ii in range(output.numMetas)])
        return srcRows

    def verifyRowCount(self, srcTable, answerRows, printRows=False):
        output = self.operators.tableMeta(srcTable)
        srcRows = sum(
            [output.metas[ii].numRows for ii in range(output.numMetas)])
        printRows and print(ResultSet(self.client, table_name=srcTable, session_name=TestOpSessionName).record_iterator())
        assert (srcRows == answerRows)

    def verifyRowCountMatch(self, srcTable, answerTable):
        output = self.operators.tableMeta(srcTable)
        srcRows = sum(
            [output.metas[ii].numRows for ii in range(output.numMetas)])

        output = self.operators.tableMeta(answerTable)
        answerRows = sum(
            [output.metas[ii].numRows for ii in range(output.numMetas)])

        assert (srcRows == answerRows)

    def verifyChecksum(self, srcTable, field, answerTable, answerField):
        self.verifyRowCountMatch(srcTable, answerTable)

        evalStr = "sum(" + field + ")"
        srcSumTable = "checksum_src"
        self.operators.aggregate(srcTable, srcSumTable, evalStr)

        evalStr = "sum(" + answerField + ")"
        answerSumTable = "checksum_answer"
        self.operators.aggregate(answerTable, answerSumTable, evalStr)

        resultSet = ResultSet(self.client, table_name=srcSumTable, session_name=TestOpSessionName).record_iterator()
        srcSum = next(resultSet)
        del resultSet

        resultSet = ResultSet(self.client, table_name=answerSumTable, session_name=TestOpSessionName).record_iterator()
        answerSum = next(resultSet)
        del resultSet

        assert (srcSum == answerSum)

        self.operators.dropConstants("checksum*", deleteCompletely=True)

    def verifyChecksumValue(self, srcTable, field, answer):
        evalStr = "sum(" + field + ")"
        srcSumTable = "checksum_src"
        self.operators.aggregate(srcTable, srcSumTable, evalStr)

        resultSet = ResultSet(self.client, table_name=srcSumTable, session_name=TestOpSessionName).record_iterator()
        srcSum = next(resultSet)
        del resultSet

        assert (float(srcSum["constant"]) == float(answer))

        self.operators.dropConstants("checksum*", deleteCompletely=True)

    def verifyAggValue(self, srcTable, answer):
        resultSet = ResultSet(self.client, table_name=srcTable, session_name=TestOpSessionName).record_iterator()
        srcVal = next(resultSet)
        del resultSet

        assert (srcVal["constant"] == answer)

    def verifySchema(self, srcTable, columnsInSchema=[],
                     columnsNotInSchema=[]):
        resultSet = ResultSet(self.client, table_name=srcTable, session_name=TestOpSessionName).record_iterator()
        row = next(resultSet)

        for col in columnsInSchema:
            assert (col in row)

        for col in columnsNotInSchema:
            assert (col not in row)

        del resultSet

    def runAggregate(self, src, exp):
        dst = "__tmp-verifyAggregate"
        self.operators.aggregate(src, dst, exp)
        resultSet = ResultSet(self.client, table_name=dst, session_name=TestOpSessionName).record_iterator()
        v = next(resultSet)['constant']

        self.operators.dropTable(dst, deleteCompletely=True)
        del resultSet

        return (v)

    def verifyAggregate(self, src, exp, ans):
        v = self.runAggregate(src, exp)
        assert (v == ans)

    def removeBatchId(self, srcTableName, dstTableName):
        output = self.operators.tableMeta(srcTableName)
        allValues = output.valueAttrs
        columnsToKeep = []
        for value in allValues:
            if value.name != "XcalarBatchId":
                columnsToKeep.append(value.name)
        self.operators.project(srcTableName, dstTableName, columnsToKeep)

    def testUdfNone(self):
        t = self.operators.indexDataset(datasetName, "", "xcalarRecordNum",
                                        "p")
        t = self.operators.map(t, "", udfNone + "(p::val)", "dummy")

        rs = ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator()
        for row in rs:
            assert ("dummy" not in row)
        del rs

        self.operators.dropTable("*", deleteCompletely=True)

    def testExplodeStringConstant(self):
        t = self.operators.indexDataset(datasetName, "", "xcalarRecordNum",
                                        "p")
        t = self.operators.map(t, "", 'explodeString("30132312303","3")',
                               "dummy")
        t = self.operators.map(t, "", 'int(dummy)', "dummy")
        self.verifyChecksumValue(t, "dummy", NumDatasetRows * 15)

        self.operators.dropTable("*", deleteCompletely=True)

    def testNullUdfArg(self):
        t = self.operators.indexDataset(datasetName, "", "xcalarRecordNum",
                                        "p")
        t = self.operators.map(t, "", '{}(p::nullVal)'.format(udfTestNone),
                               "dummy")

        rs = ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator()
        row = next(rs)
        assert (row["dummy"] == "Empty")
        del rs

        t = self.operators.map(
            t, "", '{}(int(p::nullVal))'.format(udfTestNone), "dummy")

        rs = ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator()
        row = next(rs)
        assert (row["dummy"] == "Empty")
        del rs

        self.operators.dropTable("*", deleteCompletely=True)

    def doColoredAvg(self, srcTable, keyName, valName, finalTable, finalField):
        tables = [
            "index_random", "random_sum_count", "index_key", "key_sum_count"
        ]

        # Find the sum and the count, then divide
        evalSum = "sum(" + valName + ")"
        sumField = "sum_random"
        evalSum2 = "sum(" + sumField + ")"
        sumField2 = "sum_final"

        evalCount = "count(" + valName + ")"
        countField = "count_random"
        evalCount2 = "sum(" + countField + ")"
        countField2 = "count_final"

        evalAvg = "div(" + sumField2 + "," + countField2 + ")"
        avgField = "avg_final"

        self.operators.indexTable(srcTable, tables[0], keyName,
                                  "systemRandomDht")

        self.operators.groupBy(tables[0], tables[1], [evalSum, evalCount],
                               [sumField, countField])

        newKeyName = keyName.replace("::", "-")
        self.operators.indexTable(tables[1], tables[2], newKeyName)

        self.operators.groupBy(tables[2], tables[3], [evalSum2, evalCount2],
                               [sumField2, countField2])
        self.operators.map(tables[3], finalTable, evalAvg, finalField)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def doColoredCount(self, srcTable, keyName, valName, finalTable,
                       finalField):
        tables = ["index_random", "random_count", "index_key"]

        evalCount = "count(" + valName + ")"
        countField = "count_random"
        evalCount2 = "sum(" + countField + ")"

        self.operators.indexTable(srcTable, tables[0], keyName,
                                  "systemRandomDht")
        self.operators.groupBy(tables[0], tables[1], evalCount, countField)

        newKeyName = keyName.replace("::", "-")
        self.operators.indexTable(tables[1], tables[2], newKeyName)
        self.operators.groupBy(tables[2], finalTable, evalCount2, finalField)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def doColoredGenericAgg(self, srcTable, keyName, valName, finalTable,
                            finalField, func):
        tables = ["index_random", "random_" + func, "index_key"]

        eval1 = func + "(" + valName + ")"
        field = func + "_random"
        eval2 = func + "(" + field + ")"

        self.operators.indexTable(srcTable, tables[0], keyName,
                                  "systemRandomDht")
        self.operators.groupBy(tables[0], tables[1], eval1, field)

        newKeyName = keyName.replace("::", "-")
        self.operators.indexTable(tables[1], tables[2], newKeyName)
        self.operators.groupBy(tables[2], finalTable, eval2, finalField)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def doGroupByColoredTest(self, srcTable, keyName, valName):
        aggFns = ["sum", "avg", "max", "count"]
        tables = ["test_table", "index_table", "result_table"]

        testTable = tables[0]
        testField = "test_field"

        indexTable = tables[1]
        resultTable = tables[2]
        resultField = "result_field"

        for func in aggFns:
            print("testing func: " + func)
            resultEval = func + "(" + valName + ")"

            if func == "avg":
                self.doColoredAvg(srcTable, keyName, valName, testTable,
                                  testField)
            elif func == "count":
                self.doColoredCount(srcTable, keyName, valName, testTable,
                                    testField)
            else:
                self.doColoredGenericAgg(srcTable, keyName, valName, testTable,
                                         testField, func)

            self.operators.indexTable(srcTable, indexTable, keyName)
            self.operators.groupBy(indexTable, resultTable, resultEval,
                                   resultField)

            self.verifyChecksum(testTable, testField, resultTable, resultField)

            for table in tables:
                self.operators.dropTable(table, deleteCompletely=True)

    def testGroupByColored(self):
        tableName = "testGroupByColoredStart"
        fields = ["p::val", "p::valDiv10", "p::constant"]

        self.operators.indexDataset(datasetName, tableName, "xcalarRecordNum",
                                    "p")

        self.doGroupByColoredTest(tableName, "p::valDiv10", "p::val")

        for key in fields:
            for val in fields:
                self.doGroupByColoredTest(tableName, key, val)

        self.operators.dropTable(tableName, deleteCompletely=True)

    def testGroupByMultiKey(self):
        t = self.operators.indexDataset(datasetName, "",
                                        ["valDiv10", "constant"], "p")
        t = self.operators.groupBy(t, "", "count(1)", "result")
        self.verifyChecksumValue(t, "result", NumDatasetRows)

        self.operators.dropTable("*", deleteCompletely=True)

    def testListAgg(self):
        t = self.operators.indexDataset(datasetName, "",
                                        ["valDiv10", "constant"], "p")
        t = self.operators.map(t, "", "string(p-constant)", "str")

        noDelimResult = "1" * NumDatasetRows
        commaDelimResult = ("1," * NumDatasetRows)
        doubleCommaDelimResult = ("1,," * NumDatasetRows)
        valDelimResult = ("11" * NumDatasetRows)

        val = self.operators.aggregate(t, "", "listAgg(str)")
        assert (val == noDelimResult)
        val = self.operators.aggregate(t, "", "listAgg(string(p::constant))")
        assert (val == noDelimResult)

        val = self.operators.aggregate(t, "", 'listAgg(str, ",")')
        assert (val == commaDelimResult)
        val = self.operators.aggregate(t, "",
                                       'listAgg(string(p::constant), ",")')
        assert (val == commaDelimResult)

        val = self.operators.aggregate(t, "", 'listAgg(str, ",,")')
        assert (val == doubleCommaDelimResult)
        val = self.operators.aggregate(t, "",
                                       'listAgg(string(p::constant), ",,")')
        assert (val == doubleCommaDelimResult)

        val = self.operators.aggregate(t, "", 'listAgg(str, str)')
        assert (val == valDelimResult)
        val = self.operators.aggregate(
            t, "", 'listAgg(string(p::constant), string(p::constant))')
        assert (val == valDelimResult)

        # make sure invalid args don't crash
        try:
            c = self.operators.aggregate(t, "", 'listAgg(str, 1)')
        except XcalarApiStatusException:
            pass

        try:
            c = self.operators.aggregate(t, "", 'listAgg(str, p::constant)')
        except XcalarApiStatusException:
            pass

        try:
            c = self.operators.aggregate(t, "", 'listAgg(p::constant, str)')
        except XcalarApiStatusException:
            pass

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.dropConstants("*", deleteCompletely=True)

    def testGroupByAccumulator(self):
        # test all AccumulatorTypes
        fields = ["bool", "int", "string", "float", "money"]

        t = self.operators.indexDataset(datasetName, "", "val", "p")
        t = self.operators.groupBy(
            t, "", "max(p::constant)", "scalar", includeSrcTableSample=True)
        t = self.operators.indexTable(t, "", "p::valDiv10")
        t = self.operators.map(
            t, "", ["bool(1)", "int(1)", "string(1)", "float(1)", "money(1)"],
            fields)
        fields.append("scalar")

        accMap = {
            "max":
                fields,
            "min":
                fields,
            "count":
                fields,
            "avg": [
                f for f in fields if f in ['int', 'bool', 'float', 'money']
            ],
            "sum": [f for f in fields if f in ['int', 'bool', 'float']],
            "sumInteger": [f for f in fields if f in ['int', 'bool']],
            "avgNumeric": [f for f in fields if f in ['money']],
            "sumNumeric": [f for f in fields if f in ['money']]
        }

        for acc in accMap.keys():
            for field in fields:
                print(acc, field)

                gb = self.operators.groupBy(t, "", "{}({})".format(acc, field),
                                            "res")
                self.verifyRowCount(gb, NumDatasetRows / 10)

                if field not in accMap[acc]:
                    # not a valid combination, skip verification
                    continue

                rs = ResultSet(self.client, gb, session_name=TestOpSessionName).record_iterator()

                if acc in ["max", "min", "avg", "avgNumeric"]:
                    for row in rs:
                        if field != 'money' or acc == 'avg':
                            print(row)
                            assert (int(row['res']) == 1)
                        else:
                            assert (row['res'] == '1.00')
                else:
                    for row in rs:
                        if field != 'money' or acc == 'count':
                            assert (int(row['res']) == 10)
                        else:
                            assert (row['res'] == '10.00')

        self.operators.dropTable("*", deleteCompletely=True)

    def testMultipleMapEvals(self):
        tables = [
            "start", "map1", "map2", "map3", "mapMultiple", "mapMultiple_"
        ]
        constants = ["const"]
        aggStr = "max(p::constant)"

        eval1 = "add(p::val, p::valDiv10)"
        field1 = "add1"
        field1_ = "add1_"

        eval2 = "add(" + field1 + ",add(^" + constants[0] + ", p::val))"
        field2 = "add2"
        field2_ = "add2_"

        eval3 = "int({udfFunc}(^{const}, p::valDiv10, p::val))".format(
            udfFunc=udfSum, const=constants[0])
        field3 = "udf"
        field3_ = "udf_"

        self.operators.indexDataset(datasetName, tables[0], "xcalarRecordNum",
                                    "p")
        self.operators.aggregate(tables[0], constants[0], aggStr)

        self.operators.map(tables[0], tables[1], [eval1], [field1])
        self.operators.map(tables[1], tables[2], [eval2], [field2])
        self.operators.map(tables[2], tables[3], [eval3], [field3])

        # Do all three maps at once
        self.operators.map(tables[3], tables[4], [eval1, eval2, eval3],
                           [field1_, field2_, field3_])

        self.verifyChecksum(tables[4], field1, tables[4], field1_)
        self.verifyChecksum(tables[4], field2, tables[4], field2_)
        self.verifyChecksum(tables[4], field3, tables[4], field3_)

        # Do all three maps at once again, replace old fields
        self.operators.map(tables[4], tables[5], [eval1, eval2, eval3],
                           [field1, field2, field3])

        self.verifyChecksum(tables[5], field1, tables[5], field1_)
        self.verifyChecksum(tables[5], field2, tables[5], field2_)
        self.verifyChecksum(tables[5], field3, tables[5], field3_)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

        for constant in constants:
            self.operators.dropConstants(constant, deleteCompletely=True)

    def testDateXdfs(self):
        tables = [
            "start", "toUnixTS", "fromUnixTS", "check_1", "addDay", "addDay_",
            "addDay_check", "addMonth", "addMonth_", "addMonth_check",
            "addYear", "addYear_", "addYear_check"
        ]

        self.operators.indexDataset(datasetName, tables[0], "xcalarRecordNum",
                                    "p")

        # unixTS functions
        self.operators.map(tables[0], tables[1],
                           "convertToUnixTS(p::date, \"%F\")", "unixTS")
        self.operators.map(tables[1], tables[2],
                           "convertFromUnixTS(unixTS, \"%F\")", "date_")
        self.operators.filter(tables[2], tables[3], "eq(p::date, date_)")
        self.verifyRowCountMatch(tables[2], tables[3])

        # addDay
        self.operators.map(tables[3], tables[4],
                           "dateAddDay(p::date, \"%F\", 90)", "addDay")
        self.operators.map(tables[4], tables[5],
                           "dateAddDay(addDay, \"%F\", -90)", "date_")
        self.operators.filter(tables[5], tables[6], "eq(p::date, date_)")
        self.verifyRowCountMatch(tables[5], tables[6])

        # addMonth, add/sub months do not complement each other, so just
        # verify by adding months in 2 different wasy
        self.operators.map(tables[3], tables[7],
                           "dateAddMonth(p::date, \"%F\", 13)", "addMonth")
        self.operators.map(tables[7], tables[8],
                           "dateAddInterval(p::date, \"%F\", 0, 13, 0)",
                           "date_")
        self.operators.filter(tables[8], tables[9], "eq(addMonth, date_)")
        self.verifyRowCountMatch(tables[8], tables[9])

        # same with year, 2/29 causes problems
        self.operators.map(tables[3], tables[10],
                           "dateAddYear(p::date, \"%F\", 5)", "addYear")
        self.operators.map(tables[10], tables[11],
                           "dateAddInterval(p::date, \"%F\", 5, 0, 0)",
                           "date_")
        self.operators.filter(tables[11], tables[12], "eq(addYear, date_)")
        self.verifyRowCountMatch(tables[11], tables[12])

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def testLiteralGroupByEvals(self):
        tables = ["start", "groupBy1", "groupBy2"]
        evalStr1 = 'min("7")'
        evalStr2 = "min(7)"
        field = "min_7"

        self.operators.indexDataset(dsSmallName, tables[0], "val", "p")
        self.operators.groupBy(tables[0], tables[1], [evalStr1], [field])
        self.operators.groupBy(tables[0], tables[2], [evalStr2], [field])

        self.verifyRowCountMatch(tables[0], tables[1])
        self.verifyRowCountMatch(tables[0], tables[2])

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def testMultipleGroupByEvals(self):
        tables = [
            "start", "groupBy1", "groupBy2", "groupBy3", "groupByMultiple",
            "groupByMultiple2", "singleKeyIndex", "constGb",
            "gbMultipleGroupAll"
        ]
        constants = ["const"]
        aggStr = "max(p::constant)"

        eval1 = "sum(p::val)"
        field1 = "sum1"
        field1_ = "sum1_"

        eval2 = "sum(^" + constants[0] + ")"
        field2 = "sum2"
        field2_ = "sum2_"

        eval3 = "count(p::valDiv10)"
        field3 = "count"
        field3_ = "count_"

        self.operators.indexDataset(datasetName, tables[0], "valDiv10", "p")
        self.operators.aggregate(tables[0], constants[0], aggStr)

        self.operators.groupBy(tables[0], tables[1], [eval1], [field1])
        self.operators.groupBy(tables[0], tables[2], [eval2], [field2])
        self.operators.groupBy(tables[0], tables[3], [eval3], [field3])

        # Do all three groupBys at once
        self.operators.groupBy(tables[0], tables[4], [eval1, eval2, eval3],
                               [field1_, field2_, field3_])

        # Do all three groupBys at once, including sample
        self.operators.groupBy(
            tables[0],
            tables[5], [eval1, eval2, eval3], [field1_, field2_, field3_],
            includeSrcTableSample=True)

        self.verifyChecksum(tables[1], field1, tables[4], field1_)
        self.verifyChecksum(tables[2], field2, tables[4], field2_)
        self.verifyChecksum(tables[3], field3, tables[4], field3_)

        self.verifyChecksum(tables[1], field1, tables[5], field1_)
        self.verifyChecksum(tables[2], field2, tables[5], field2_)
        self.verifyChecksum(tables[3], field3, tables[5], field3_)

        # Test multi-groupBy with groupAll flag
        self.operators.indexDataset(datasetName, tables[6], "constant", "p")

        # This is a constant key index yielding one group
        self.operators.groupBy(tables[6], tables[7], [eval1, eval2, eval3],
                               [field1, field2, field3])

        self.operators.groupBy(
            tables[6],
            tables[8], [eval1, eval2, eval3], [field1_, field2_, field3_],
            groupAll=True)

        self.verifyChecksum(tables[7], field1, tables[8], field1_)
        self.verifyChecksum(tables[7], field2, tables[8], field2_)
        self.verifyChecksum(tables[7], field3, tables[8], field3_)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

        for constant in constants:
            self.operators.dropConstants(constant, deleteCompletely=True)

    def testUnionAll(self):
        tables = [
            "ds1", "ds1Map", "ds2", "ds2Map", "ds3", "ds3Map", "unionPrefix",
            "unionImm", "union1"
        ]

        self.operators.indexDataset(
            datasetName, tables[0], "val", fatptrPrefixName="p1")
        self.operators.map(tables[0], tables[1], ["add(1, 0)"], ["col1"])

        self.operators.indexDataset(
            datasetName, tables[2], "valDiv10", fatptrPrefixName="p2")
        self.operators.map(tables[2], tables[3], ["add(1, 0)"], ["col2"])

        self.operators.indexDataset(
            datasetName, tables[4], "constant", fatptrPrefixName="p3")
        self.operators.map(tables[4], tables[5], ["add(1, 0)"], ["col3"])

        # union prefixes
        columns = [
            [XcalarApiColumnT("p1", "p", "DfFatptr")],
            [XcalarApiColumnT("p2", "p", "DfFatptr")],
            [XcalarApiColumnT("p3", "p", "DfFatptr")],
        ]

        # SDK-635 Union on source tables with FatPtrs in schema are failed.
        try:
            self.operators.union([tables[1], tables[3], tables[5]], tables[6],
                                 columns)
            self.verifyRowCount(tables[6], 3 * NumDatasetRows)
            self.verifyChecksumValue(tables[6], "p::constant",
                                     3 * NumDatasetRows)
        except XcalarApiStatusException:
            pass

        # union immediates
        columns = [
            [XcalarApiColumnT("col1", "col", "DfFloat64")],
            [XcalarApiColumnT("col2", "col", "DfFloat64")],
            [XcalarApiColumnT("col3", "col", "DfFloat64")],
        ]

        self.operators.union([tables[1], tables[3], tables[5]], tables[7],
                             columns)
        self.verifyRowCount(tables[7], 3 * NumDatasetRows)
        self.verifyChecksumValue(tables[7], "col", 3 * NumDatasetRows)

        # union combos
        columns = [
            [
                XcalarApiColumnT("p1", "p", "DfFatptr"),
                XcalarApiColumnT("col1", "col", "DfFloat64")
            ],
            [
                XcalarApiColumnT("p2", "p", "DfFatptr"),
                XcalarApiColumnT("col2", "col", "DfFloat64")
            ],
            [
                XcalarApiColumnT("p3", "p", "DfFatptr"),
                XcalarApiColumnT("col3", "col", "DfFloat64")
            ],
        ]

        # SDK-635 Union on source tables with FatPtrs in schema are failed.
        try:
            self.operators.union([tables[1], tables[3], tables[5]], tables[8],
                                 columns)
            self.verifyRowCount(tables[8], 3 * NumDatasetRows)
            self.verifyChecksumValue(tables[8], "p::constant",
                                     3 * NumDatasetRows)
            self.verifyChecksumValue(tables[8], "col", 3 * NumDatasetRows)
        except XcalarApiStatusException:
            pass

        for table in tables:
            if table is "unionPrefix" or "union1":
                # SDK-635 Union on source tables with FatPtrs in schema are failed.
                try:
                    self.operators.dropTable(table, deleteCompletely=True)
                except XcalarApiStatusException:
                    pass
            else:
                self.operators.dropTable(table, deleteCompletely=True)

    def testUnionDedup(self):
        tables = [
            "ds1", "ds1Map", "ds2", "ds2Map", "ds3", "ds3Map", "index1",
            "index2", "index3", "union123", "selfUnion"
        ]

        self.operators.indexDataset(
            datasetName, tables[0], "valDiv10", fatptrPrefixName="p1")
        self.operators.map(tables[0], tables[1],
                           ["add(0, p1::valDiv10)", "add(1, 0)", "add(0, 0)"],
                           ["col2", "col1", "col3"])

        self.operators.indexDataset(
            datasetName, tables[2], "valDiv10", fatptrPrefixName="p2")
        self.operators.map(tables[2], tables[3],
                           ["add(0, p2::valDiv10)", "add(1, 0)", "add(0, 0)"],
                           ["col3", "col2", "col1"])

        self.operators.indexDataset(
            datasetName, tables[4], "valDiv10", fatptrPrefixName="p3")
        self.operators.map(
            tables[4], tables[5],
            ["add(0, p3::valDiv10)", "add(1, 0)", "add(1, p3::val)"],
            ["col1", "col2", "col3"])

        self.operators.indexTable(tables[1], tables[6],
                                  ["col2", "col1", "col3"])
        self.operators.indexTable(tables[3], tables[7],
                                  ["col3", "col2", "col1"])
        self.operators.indexTable(tables[5], tables[8],
                                  ["col1", "col2", "col3"])

        columns = [
            [
                XcalarApiColumnT("col2", "col1", "DfUnknown"),
                XcalarApiColumnT("col1", "col2", "DfFloat64"),
                XcalarApiColumnT("col3", "col3", "DfUnknown")
            ],
            [
                XcalarApiColumnT("col3", "col1", "DfUnknown"),
                XcalarApiColumnT("col2", "col2", "DfUnknown"),
                XcalarApiColumnT("col1", "col3", "DfFloat64")
            ],
            [
                XcalarApiColumnT("col1", "col1", "DfFloat64"),
                XcalarApiColumnT("col2", "col2", "DfUnknown"),
                XcalarApiColumnT("col3", "col3", "DfUnknown")
            ],
        ]

        self.operators.union([tables[6], tables[7], tables[8]],
                             tables[9],
                             columns,
                             dedup=True)
        self.verifyRowCount(tables[9], NumDatasetRows + (NumDatasetRows // 10))

        self.operators.union([tables[6]], tables[10], [columns[0]], dedup=True)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def setupSetTables(self, colTweak=[0, 0, 0], isDrop=False, verbose=False):
        tables = [
            "ds1", "ds1Map", "ds2", "ds2Map", "ds3", "ds3Map", "index1",
            "index2", "index3", "settab"
        ]

        if isDrop:
            for table in tables:
                try:
                    self.operators.dropTable(table, deleteCompletely=True)
                except XcalarApiStatusException:
                    pass
            return

        cols = ['prn1', 'prn2', 'prn3']
        colsr = ['prn1r', 'prn2r', 'prn3r']

        self.operators.indexDataset(
            datasetName, tables[0], cols[0], fatptrPrefixName="p1")
        self.operators.map(tables[0], tables[1], [
            "add(0, p1::prn1)",
            "add(%d, p1::prn2)" % colTweak[0], "add(0, p1::prn3)"
        ], cols)

        self.operators.indexDataset(
            datasetName, tables[2], cols[0], fatptrPrefixName="p2")
        self.operators.map(tables[2], tables[3], [
            "add(0, p2::prn1)",
            "add(%d, p2::prn2)" % colTweak[1], "add(0, p2::prn3)"
        ], cols)

        self.operators.indexDataset(
            datasetName, tables[4], cols[0], fatptrPrefixName="p3")

        self.operators.map(tables[4], tables[5], [
            "add(0, p3::prn1)",
            "add(%d, p3::prn2)" % colTweak[2], "add(0, p3::prn3)"
        ], cols)

        self.operators.indexTable(tables[1], tables[6], cols)
        self.operators.indexTable(tables[3], tables[7], cols)
        self.operators.indexTable(tables[5], tables[8], cols)

        if verbose:
            print(ResultSet(self.client, table_name=tables[6], session_name=TestOpSessionName).record_iterator())
            print(ResultSet(self.client, table_name=tables[7], session_name=TestOpSessionName).record_iterator())
            print(ResultSet(self.client, table_name=tables[8], session_name=TestOpSessionName).record_iterator())

        columns = [
            [
                XcalarApiColumnT(cols[0], colsr[0], "DfFloat64"),
                XcalarApiColumnT(cols[1], colsr[1], "DfFloat64"),
                XcalarApiColumnT(cols[2], colsr[2], "DfFloat64")
            ],
            [
                XcalarApiColumnT(cols[0], colsr[0], "DfFloat64"),
                XcalarApiColumnT(cols[1], colsr[1], "DfFloat64"),
                XcalarApiColumnT(cols[2], colsr[2], "DfFloat64")
            ],
            [
                XcalarApiColumnT(cols[0], colsr[0], "DfFloat64"),
                XcalarApiColumnT(cols[1], colsr[1], "DfFloat64"),
                XcalarApiColumnT(cols[2], colsr[2], "DfFloat64")
            ],
        ]

        return (tables, columns, cols, colsr)

    def testIntersectOne(self):
        (tables, columns, cols, colsr) = self.setupSetTables([0, 1, 0])
        self.operators.union([tables[6], tables[7], tables[8]],
                             tables[9],
                             columns,
                             dedup=True,
                             unionType=UnionOperatorT.UnionIntersect)

        def getSet(t):
            return freezeRows((getFilteredDicts(
                ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator(), cols, colsr)))

        t6 = getSet(tables[6])
        t7 = getSet(tables[7])
        t8 = getSet(tables[8])
        t9 = freezeRows((getFilteredDicts(
            ResultSet(self.client, table_name=tables[9], session_name=TestOpSessionName).record_iterator(), colsr)))

        assert (t9 == t6 & t7 & t8)

        self.setupSetTables(isDrop=True)

    def testIntersectAll(self):
        (tables, columns, cols, colsr) = self.setupSetTables([0, 1, 0])
        self.operators.union([tables[6], tables[7], tables[8]],
                             tables[9],
                             columns,
                             dedup=False,
                             unionType=UnionOperatorT.UnionIntersect)

        def getSet(t):
            return freezeRows(
                getFilteredDicts(
                    ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator(), cols, colsr), True)

        t6 = getSet(tables[6])
        t7 = getSet(tables[7])
        t8 = getSet(tables[8])
        t9 = freezeRows(
            getFilteredDicts(
                ResultSet(self.client, table_name=tables[9], session_name=TestOpSessionName).record_iterator(), colsr), True)

        assert (t9 == t6 & t7 & t8)

        self.setupSetTables(isDrop=True)

    def testExceptOne(self):
        (tables, columns, cols, colsr) = self.setupSetTables([0, 1, 1])
        self.operators.union([tables[6], tables[7], tables[8]],
                             tables[9],
                             columns,
                             dedup=True,
                             unionType=UnionOperatorT.UnionExcept)

        def getSet(t):
            return freezeRows(
                getFilteredDicts(
                    ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator(), cols, colsr))

        t6 = getSet(tables[6])
        t7 = getSet(tables[7])
        t8 = getSet(tables[8])
        t9 = freezeRows(
            getFilteredDicts(
                ResultSet(self.client, table_name=tables[9], session_name=TestOpSessionName).record_iterator(), colsr))

        assert (t9 == t6 - t7 - t8)

        self.setupSetTables(isDrop=True)

    def testExceptAll(self):
        (tables, columns, cols, colsr) = self.setupSetTables([0, 1, 1])
        self.operators.union([tables[6], tables[7], tables[8]],
                             tables[9],
                             columns,
                             dedup=False,
                             unionType=UnionOperatorT.UnionExcept)

        def getSet(t):
            return freezeRows(
                getFilteredDicts(
                    ResultSet(self.client, table_name=t, session_name=TestOpSessionName).record_iterator(), cols, colsr), True)

        t6 = getSet(tables[6])
        t7 = getSet(tables[7])
        t8 = getSet(tables[8])
        t9 = freezeRows(
            getFilteredDicts(
                ResultSet(self.client, table_name=tables[9], session_name=TestOpSessionName).record_iterator(), colsr), True)

        assert (t9 == t6 - t7 - t8)

        self.setupSetTables(isDrop=True)

    def testInterceptExceptUnion(self):
        t1 = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t2 = self.operators.indexTable(
            self.operators.map(t1, "", "int(add(p1-val, 1))", "val2"), "",
            "val2")
        columns = [
            [XcalarApiColumnT("p1-val", "v", "DfInt64")],
            [XcalarApiColumnT("val2", "v", "DfInt64")],
        ]

        intersect = self.operators.union(
            [t1, t2], "", columns, unionType=UnionOperatorT.UnionIntersect)
        self.verifyRowCount(intersect, NumDatasetRows - 1)

        exc = self.operators.union([t1, t2],
                                   "",
                                   columns,
                                   unionType=UnionOperatorT.UnionExcept)
        self.verifyRowCount(exc, 1)

        final = self.operators.union(
            [intersect, exc], "",
            [[], [XcalarApiColumnT("v", "v", "DfInt64")]])
        self.verifyRowCount(final, NumDatasetRows)

        self.operators.dropTable("*", deleteCompletely=True)

    def testMultiKeyIndex(self):
        tables = [
            "ds1", "ds1Map", "ds2", "ds2Map", "index1", "index2", "index3",
            "index4", "index5", "index6"
        ]

        self.operators.indexDataset(
            datasetName, tables[0], "valDiv10", fatptrPrefixName="p1")
        self.operators.map(tables[0], tables[1], [
            "add(0, p1::valDiv10)", "add(0, p1::val)", "add(0, 0)",
            "add(1, p1::val)", "add(2, p1::val)", "add(3, p1::val)"
        ], ["col2", "col1", "col3", "col4", "col5", "col6"])

        self.operators.indexDataset(
            datasetName, tables[2], "valDiv10", fatptrPrefixName="p2")
        self.operators.map(tables[2], tables[3], [
            "add(0, p2::valDiv10)", "add(1, 0)", "add(0, 0)",
            "add(1, p1::val)", "add(2, p1::val)", "add(3, p1::val)"
        ], ["col3_", "col2_", "col1_", "col4_", "col5_", "col6_"])

        self.operators.indexTable(tables[1], tables[4],
                                  ["col2", "col1", "col3"])
        self.operators.indexTable(tables[3], tables[5],
                                  ["col3_", "col2_", "col1_"])

        self.operators.indexTable(
            tables[1],
            tables[6], ["col2", "col1", "col3"],
            ordering=[
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingDescending,
                XcalarOrderingT.XcalarOrderingUnordered
            ])
        self.verifyRowCount(tables[6], NumDatasetRows)

        self.operators.indexTable(
            tables[1],
            tables[7], ["p1::val", "p1::valDiv10", "p1::constant"],
            ordering=[
                XcalarOrderingT.XcalarOrderingUnordered,
                XcalarOrderingT.XcalarOrderingDescending,
                XcalarOrderingT.XcalarOrderingUnordered
            ])
        self.verifyRowCount(tables[7], NumDatasetRows)

        self.operators.indexTable(
            tables[1],
            tables[8], ["p1::val", "col1", "p1::valDiv10"],
            ordering=[
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingDescending
            ])
        self.verifyRowCount(tables[8], NumDatasetRows)

        gbTables = ["gbCount"]

        self.operators.groupBy(tables[4], gbTables[0], "count(col1)",
                               "gbField")
        self.verifyRowCount(gbTables[0], NumDatasetRows)

        joinTables = ["innerJoin", "outerJoin"]

        self.operators.join(tables[4], tables[5], joinTables[0])
        self.verifyRowCount(joinTables[0], 10)

        self.operators.join(
            tables[4],
            tables[5],
            joinTables[1],
            joinType=JoinOperatorT.FullOuterJoin)
        self.verifyRowCount(joinTables[1], NumDatasetRows * 2 - 1)

        self.operators.indexTable(
            joinTables[0],
            tables[9], ["col1_", "p2::val", "p1::val", "col1"],
            ordering=[
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingDescending,
                XcalarOrderingT.XcalarOrderingUnordered
            ])
        self.verifyRowCount(tables[9], 10)

        for table in tables + gbTables + joinTables:
            self.operators.dropTable(table, deleteCompletely=True)

    def testJoinNullSafe(self):
        t1 = self.operators.indexDataset(
            datasetName, "", "flaky", fatptrPrefixName="p1")
        t2 = self.operators.indexDataset(
            datasetName, "", "flaky", fatptrPrefixName="p2")

        numNonFnfs = NumDatasetRows // FlakyFrequency
        numFnfs = NumDatasetRows - numNonFnfs
        innerJoin = "joinIn"
        outerJoin = "joinOut"
        nullSafeInner = "nullSafeIn"
        nullSafeOuter = "nullSafeOuter"

        query = [{
            "operation": "XcalarApiJoin",
            "args": {
                "source": [t1, t2],
                "dest": innerJoin,
                "joinType": "innerJoin",
                "columns": [[], []]
            }
        },
                 {
                     "operation": "XcalarApiJoin",
                     "args": {
                         "source": [t1, t2],
                         "dest": outerJoin,
                         "joinType": "fullOuterJoin",
                         "columns": [[], []]
                     }
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "args": {
                         "source": [t1, t2],
                         "dest": nullSafeInner,
                         "nullSafe": True,
                         "joinType": "innerJoin",
                         "columns": [[], []]
                     },
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "args": {
                         "source": [t1, t2],
                         "dest": nullSafeOuter,
                         "nullSafe": True,
                         "joinType": "fullOuterJoin",
                         "columns": [[], []]
                     }
                 }]

        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(innerJoin, numNonFnfs**2)
        self.verifyRowCount(outerJoin, numFnfs * 2 + numNonFnfs**2)

        self.verifyRowCount(nullSafeInner, numFnfs**2 + numNonFnfs**2)
        self.verifyRowCount(nullSafeOuter, numFnfs**2 + numNonFnfs**2)

        self.operators.dropTable("*", deleteCompletely=True)

    def testCrossJoin(self):
        tbl = "testCrossJoin-"

        self.operators.indexDataset(
            datasetName, tbl + "index1", "valDiv10", fatptrPrefixName="p")
        self.operators.indexTable(tbl + "index1", tbl + "index2", "p::val")

        self.operators.filter(tbl + "index2", tbl + "filter", "lt(p-val, 100)")

        self.operators.project(tbl + "filter", tbl + "project", ["p-val"])

        self.operators.join(
            tbl + "index1",
            tbl + "project",
            tbl + "join1",
            joinType=JoinOperatorT.CrossJoin)

        self.verifyRowCount(tbl + "join1", NumDatasetRows * 100)

        self.operators.join(
            tbl + "index1",
            tbl + "project",
            tbl + "join2",
            joinType=JoinOperatorT.CrossJoin,
            filterString="neq(p-valDiv10, p-val)")

        self.verifyRowCount(tbl + "join2",
                            NumDatasetRows * 100 - NumDatasetRows)

        self.operators.join(
            tbl + "index1",
            tbl + "project",
            tbl + "join3",
            joinType=JoinOperatorT.CrossJoin,
            filterString="lt(p-valDiv10, p-val)")

        self.verifyRowCount(tbl + "join3", 49500)

        emptyTbl = self.operators.filter(tbl + "project", "", "eq(1, 0)")

        t = self.operators.join(
            tbl + "index1",
            emptyTbl,
            "",
            joinType=JoinOperatorT.CrossJoin,
            filterString="lt(p-valDiv10, p-val)")
        self.verifyRowCount(t, 0)

        self.operators.dropTable("*", deleteCompletely=True)

    def testAntiAndSemiJoin(self):
        t1 = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="prefix1")
        t2 = self.operators.indexDataset(
            datasetName, "", "valDiv10", fatptrPrefixName="prefix2")
        semi = self.operators.join(
            t2, t2, "", joinType=JoinOperatorT.LeftSemiJoin)
        self.verifyRowCount(semi, NumDatasetRows)

        semi = self.operators.join(
            t2, t1, "", joinType=JoinOperatorT.LeftSemiJoin)
        self.verifyRowCount(semi, NumDatasetRows)
        self.verifySchema(semi, columnsNotInSchema=["prefix1-val"])

        anti = self.operators.join(
            t1, t2, "", joinType=JoinOperatorT.LeftAntiJoin)
        self.verifyRowCount(anti, 9 * NumDatasetRows / 10)
        self.verifySchema(anti, columnsNotInSchema=["prefix2-valDiv10"])

        anti = self.operators.join(
            t2, t1, "", joinType=JoinOperatorT.LeftAntiJoin)
        self.verifyRowCount(anti, 0)

        anti = self.operators.join(
            t2, t2, "", joinType=JoinOperatorT.LeftAntiJoin)
        self.verifyRowCount(anti, 0)

        self.operators.dropTable("*", deleteCompletely=True)

    def testAntiAndSemiJoinOnEvalCondition(self):
        t1 = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="prefix1")
        t2 = self.operators.indexDataset(
            datasetName, "", "valDiv10", fatptrPrefixName="prefix2")
        semi = self.operators.join(
            t2,
            t1,
            "",
            joinType=JoinOperatorT.LeftSemiJoin,
            filterString="lt(prefix2-valDiv10, 10)")

        self.verifyRowCount(semi, 100)
        self.verifySchema(semi, columnsNotInSchema=["prefix1-val"])

        valBegin = NumDatasetRows / 10 + 1
        valEnd = NumDatasetRows - 1
        filterString = "between(prefix1-val, " + str(valBegin) + ", " + str(
            valEnd) + ")"
        anti = self.operators.join(
            t1,
            t2,
            "",
            joinType=JoinOperatorT.LeftAntiJoin,
            filterString=filterString)
        self.verifyRowCount(anti, NumDatasetRows)
        self.verifySchema(anti, columnsNotInSchema=["prefix2-valDiv10"])

        self.operators.dropTable("*", deleteCompletely=True)

    def testOuterJoinOnEvalCondition(self):
        t1 = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="prefix1")
        t2 = self.operators.indexDataset(
            datasetName, "", "valDiv10", fatptrPrefixName="prefix2")
        self.operators.aggregate(t2, "const", "avg(prefix2-valDiv10)")

        renameColumn = []
        renameCol = XcalarApiColumnT()
        renameCol.sourceColumn = "prefix2-valDiv10"
        renameCol.destColumn = "prefix3-valDiv10"
        renameCol.columnType = "DfUnknown"
        renameColumn.append(renameCol)

        leftOuter = self.operators.join(
            t1,
            t2,
            "",
            rightColumns=renameColumn,
            joinType=JoinOperatorT.LeftOuterJoin,
            filterString="lt(prefix3-valDiv10, ^{})".format("const"))
        self.verifyRowCount(
            leftOuter, NumDatasetRows / 10 / 2 * 10 +
            (NumDatasetRows - NumDatasetRows / 10 / 2))

        rightOuter = self.operators.join(
            t2,
            t1,
            "",
            leftColumns=renameColumn,
            joinType=JoinOperatorT.RightOuterJoin,
            filterString="lt(prefix3-valDiv10, ^{})".format("const"))
        self.verifyRowCount(
            rightOuter, NumDatasetRows / 10 / 2 * 10 +
            (NumDatasetRows - NumDatasetRows / 10 / 2))

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.dropConstants("*", deleteCompletely=True)

    @pytest.mark.parametrize("joinType, joinResult", [
        (JoinOperatorT.InnerJoin, joinFilterInnerRes),
        (JoinOperatorT.LeftOuterJoin, joinFilterLeftOuterRes),
        (JoinOperatorT.RightOuterJoin, joinFilterRightOuterRes),
        (JoinOperatorT.FullOuterJoin, joinFilterFullOuterRes),
        (JoinOperatorT.LeftSemiJoin, joinFilterLeftSemiRes),
        (JoinOperatorT.LeftAntiJoin, joinFilterLeftAntiRes),
    ])
    def testJoinOnEvalCondition(self, joinType, joinResult):
        tbl = "testJoinOnEvalCondition-"

        self.operators.indexDataset(
            joinDatasetNameLeft,
            tbl + "left1",
            "col2",
            fatptrPrefixName="prefix1")
        self.operators.indexTable(tbl + "left1", tbl + "left2",
                                  "prefix1::col1")

        self.operators.indexDataset(
            joinDatasetNameRight,
            tbl + "right1",
            "col2",
            fatptrPrefixName="prefix2")
        self.operators.indexTable(tbl + "right1", tbl + "right2",
                                  "prefix2::col1")

        renameColumn = [
            XcalarApiColumnT("prefix2-col2", "prefix2-col4", "DfUnknown")
        ]

        join = self.operators.join(
            tbl + "left2",
            tbl + "right2",
            tbl + "result",
            rightColumns=renameColumn,
            joinType=joinType,
            filterString="lt(prefix1-col2, prefix2-col4)")
        self.verifyRowCount(join, joinResult)

        self.operators.dropTable(tbl + "*", deleteCompletely=True)

    def testBug10968(self):
        tbl = "bug10968-"

        self.operators.indexDataset(
            datasetName, tbl + "1", "val", fatptrPrefixName="prefix1")
        self.operators.indexDataset(
            datasetName, tbl + "2", "valDiv10", fatptrPrefixName="prefix2")

        self.operators.join(tbl + "1", tbl + "2", tbl + "3")
        self.retina.make("ret", [tbl + "3"],
                         [["prefix::val", "prefix::valDiv10"]], [tbl + "1"])

        self.operators.dropTable(tbl + "*", deleteCompletely=True)
        self.retina.delete("ret")

    def testRetinaWithSource(self):
        tables = [
            "index1", "index2", "ret1", "index3", "ret2", "ret1_dataset",
            "join", "joinIndex", "ret_join"
        ]
        retinas = ["r1", "r2", "r3", "r4"]
        fields = ["val", "valDiv10", "constant"]

        self.operators.indexDataset(
            datasetName,
            tables[0],
            "xcalarRecordNum",
            fatptrPrefixName="prefix")
        self.operators.indexTable(tables[0], tables[1], "prefix::val")

        # Test source index
        self.retina.make(retinas[0], [tables[1]],
                         [[("prefix::val", "val"),
                           ("prefix::valDiv10", "valDiv10")]], [tables[0]])

        self.retina.execute(retinas[0], [], tables[2])
        self.verifyRowCount(tables[2], NumDatasetRows)

        self.operators.indexTable(
            tables[2], tables[3], "valDiv10", keyFieldName="key1")
        self.verifyRowCount(tables[3], NumDatasetRows)

        # Test source retina
        self.retina.make(retinas[1], [tables[3]],
                         [[("val", "val_2"),
                           ("valDiv10", "valDiv10_2")]], [tables[2]])
        self.retina.execute(retinas[1], [], tables[4])
        self.verifyRowCount(tables[4], NumDatasetRows)

        # Test source dataset
        self.retina.make(retinas[2], [tables[1]],
                         [[("prefix::val", "val"),
                           ("prefix::constant", "constant")]], [datasetName])
        self.retina.execute(retinas[2], [], tables[5])
        self.verifyRowCount(tables[5], NumDatasetRows)

        # test source rename
        leftColumn = []
        renameCol = XcalarApiColumnT()
        renameCol.sourceColumn = "prefix"
        renameCol.destColumn = "prefixLeft"
        renameCol.columnType = "DfFatptr"
        leftColumn.append(renameCol)

        renameCol = XcalarApiColumnT()
        renameCol.sourceColumn = "prefix-val"
        renameCol.destColumn = "prefixLeft-val"
        renameCol.columnType = "DfUnknown"
        leftColumn.append(renameCol)

        rightColumn = []
        renameCol = XcalarApiColumnT()
        renameCol.sourceColumn = "prefix"
        renameCol.destColumn = "prefixRight"
        renameCol.columnType = "DfFatptr"
        rightColumn.append(renameCol)

        self.operators.join(
            tables[1],
            tables[1],
            tables[6],
            leftColumns=leftColumn,
            rightColumns=rightColumn)
        self.operators.indexTable(tables[6], tables[7], "prefixLeft::val")

        self.retina.make(retinas[3], [tables[7]],
                         [[("prefixLeft::val", "valLeft"),
                           ("prefixRight::val", "valRight")]], [tables[6]])
        self.retina.execute(retinas[3], [], tables[8])
        self.verifyRowCount(tables[8], NumDatasetRows)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)
        for retina in retinas:
            self.retina.delete(retina)

    def testFnfJoin(self):
        tables = ["ds1", "ds2", "ds1f", "ds2f"]
        verbose = False

        self.operators.indexDataset(
            datasetName, tables[0], "flaky", fatptrPrefixName="p1")
        self.verifyRowCount(tables[0], NumDatasetRows, verbose)
        self.operators.indexDataset(
            datasetName, tables[1], "flaky", fatptrPrefixName="p2")
        self.verifyRowCount(tables[1], NumDatasetRows, verbose)

        # verbose = True
        # Filter the rowcount down to something more reasonable for join
        numRowsToJoin = 100
        self.operators.filter(tables[0], tables[2],
                              "lt(p1::val, %d)" % numRowsToJoin)
        self.verifyRowCount(tables[2], numRowsToJoin, verbose)

        self.operators.filter(tables[1], tables[3],
                              "lt(p2::val, %d)" % numRowsToJoin)
        self.verifyRowCount(tables[3], numRowsToJoin, verbose)

        joinTables = ["innerJoin", "outerJoin", "leftJoin", "rightJoin"]
        numFlaky = numRowsToJoin // FlakyFrequency

        self.operators.join(tables[2], tables[3], joinTables[0])
        self.verifyRowCount(joinTables[0], numFlaky**2, verbose)

        self.operators.join(
            tables[2],
            tables[3],
            joinTables[1],
            joinType=JoinOperatorT.FullOuterJoin)
        self.verifyRowCount(joinTables[1],
                            numFlaky**2 + 2 * (numRowsToJoin - numFlaky),
                            verbose)

        self.operators.join(
            tables[2],
            tables[3],
            joinTables[2],
            joinType=JoinOperatorT.LeftOuterJoin)
        self.verifyRowCount(joinTables[2],
                            numFlaky**2 + numRowsToJoin - numFlaky, verbose)

        self.operators.join(
            tables[2],
            tables[3],
            joinTables[3],
            joinType=JoinOperatorT.RightOuterJoin)
        self.verifyRowCount(joinTables[3],
                            numFlaky**2 + numRowsToJoin - numFlaky, verbose)

        for table in tables + joinTables:
            self.operators.dropTable(table, deleteCompletely=True)

    def testFnfIdx(self):
        tables = [
            "ds1", "ds2", "idx1", "idx2", "tmp", "multiKeyAsc", "multiKeyDesc"
        ]
        verbose = False

        self.operators.indexDataset(
            datasetName, tables[0], "valDiv10", fatptrPrefixName="p1")
        self.verifyRowCount(tables[0], NumDatasetRows, verbose)
        self.operators.indexDataset(
            datasetName, tables[1], "flaky", fatptrPrefixName="p2")
        self.verifyRowCount(tables[1], NumDatasetRows, verbose)

        self.operators.indexTable(tables[0], tables[2], ["p1::flaky"])
        self.verifyRowCount(tables[2], NumDatasetRows, verbose)

        self.operators.indexTable(tables[1], tables[3], ["p2::valDiv10"])
        self.verifyRowCount(tables[3], NumDatasetRows, verbose)

        # Xc-11686
        self.operators.map(tables[0], tables[4], "int(1)", "ones")
        self.operators.indexTable(
            tables[4],
            tables[5], ["ones", "p1::flaky"],
            ordering=[
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingAscending
            ])
        rs = ResultSet(self.client, table_name=tables[5], session_name=TestOpSessionName).record_iterator()
        row = next(rs)

        print(row)
        assert ("p1::flaky" not in row)
        del rs

        self.operators.indexTable(
            tables[4],
            tables[6], ["ones", "p1::flaky"],
            ordering=[
                XcalarOrderingT.XcalarOrderingAscending,
                XcalarOrderingT.XcalarOrderingDescending
            ])
        rs = ResultSet(self.client, table_name=tables[6], session_name=TestOpSessionName).record_iterator()
        row = next(rs)

        print(row)
        assert ("p1::flaky" in row)
        del rs

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    @pytest.mark.skip(reason="Archive to be nuked")
    def testArchive(self):
        tables = ["ds1"]
        verbose = False

        self.operators.indexDataset(
            datasetName, tables[0], "valDiv10", fatptrPrefixName="p1")
        self.verifyRowCount(tables[0], NumDatasetRows, verbose)

        self.operators.archiveTables(tables)
        self.operators.unarchiveTables(tables)

        self.verifyRowCount(tables[0], NumDatasetRows, verbose)
        self.verifyChecksumValue(tables[0], "p1::constant", NumDatasetRows)

        self.operators.archiveTables(tables)

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def testResultSetDrop(self):
        tbl = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        rs = ResultSet(self.client, table_name=tbl, session_name=TestOpSessionName)
        self.operators.dropTable("*", deleteCompletely=True)

        # this should fail, the result set should be dropped when the table is
        # dropped
        with pytest.raises(XDPException):
            for row in rs.record_iterator():
                assert False

    def testMapTableCursor(self):
        tbl = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        projectTbl = self.operators.project(tbl, "", ["p1-val"])
        filterTbl = self.operators.filter(projectTbl, "", "lt(p1-val, 10)")

        mapTbl = self.operators.map(tbl, "", "{}(^{})".format(
            udfCursor, filterTbl), "sum")

        rs = ResultSet(self.client, table_name=mapTbl, session_name=TestOpSessionName).record_iterator()

        for row in rs:
            assert ("sum" in row and row['sum'] == '45')
        del rs

        self.operators.dropTable("*", deleteCompletely=True)

    def testRangeFilterOptimizations(self):
        tbl = self.operators.indexDataset(
            datasetName,
            "",
            "val",
            fatptrPrefixName="p1",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        filterTbl = self.operators.filter(tbl, "", "lt(p1-val, 10)")
        self.verifyRowCount(filterTbl, 10)

        filterTbl = self.operators.filter(tbl, "", "le(p1-val, 10)")
        self.verifyRowCount(filterTbl, 11)

        filterTbl = self.operators.filter(tbl, "", "gt(p1-val, 10)")
        self.verifyRowCount(filterTbl, NumDatasetRows - 11)

        filterTbl = self.operators.filter(tbl, "", "ge(p1-val, 10)")
        self.verifyRowCount(filterTbl, NumDatasetRows - 10)

        filterTbl = self.operators.filter(tbl, "", "between(p1-val, 5, 10)")
        self.verifyRowCount(filterTbl, 6)

        filterTbl = self.operators.filter(tbl, "", "eq(p1-val, 10)")
        self.verifyRowCount(filterTbl, 1)

        # Test string key
        mapTbl = self.operators.map(tbl, "", "string(p1-val)", "strVal")
        tbl = self.operators.indexTable(
            mapTbl,
            "",
            "strVal",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        filterTbl = self.operators.filter(tbl, "", 'eq(strVal, "10")')
        self.verifyRowCount(filterTbl, 1)

        filterTbl = self.operators.filter(tbl, "", 'between(strVal, "0", "1")')
        self.verifyRowCount(filterTbl, 2)

        self.operators.dropTable("*", deleteCompletely=True)

    def testPublishedTablesTxnRecovery(self):
        tables = ["table" + str(i) for i in range(100)]
        publishedTable = "a"
        publishedTable2 = "b"
        verbose = False
        ii = 0

        self.operators.indexDataset(
            datasetName, tables[ii], "valDiv10", fatptrPrefixName="p1")
        ii = ii + 1

        self.operators.getRowNum(tables[ii - 1], tables[ii], "rowNum")
        ii = ii + 1

        self.operators.project(tables[ii - 1], tables[ii], ["rowNum"])
        ii = ii + 1

        self.operators.map(tables[ii - 1], tables[ii], ["int(1)", "int(1)"],
                           ["XcalarOpCode", "XcalarRankOver"])
        ii = ii + 1

        self.operators.indexTable(tables[ii - 1], tables[ii], "rowNum")
        self.operators.publish(tables[ii], publishedTable)
        self.operators.publish(tables[ii], publishedTable2)
        ii = ii + 1

        # Create an update with all deletes
        self.operators.map(tables[ii - 1], tables[ii], ["int(0)"],
                           ["XcalarOpCode"])
        ii = ii + 1

        self.removeBatchId(tables[ii - 1], tables[ii])
        goodUpdate = tables[ii]
        ii = ii + 1

        # Create an update with all deletes except an invalid op code
        self.operators.map(tables[ii - 1], tables[ii],
                           ["ifInt(eq(rowNum, 1), div(1,0), 0)"],
                           ["XcalarOpCode"])
        badUpdate = tables[ii]
        ii = ii + 1

        try:
            # Do a good update on a table and a bad update in the same api
            # The bad update should invoke txn recovery and rollback everything
            self.operators.update([goodUpdate, badUpdate],
                                  [publishedTable, publishedTable2])
        except XcalarApiStatusException:
            pass

        # None of the deletes should have went through
        self.operators.select(publishedTable, tables[ii])
        self.verifyRowCount(tables[ii], NumDatasetRows, verbose)
        ii = ii + 1

        self.operators.select(publishedTable2, tables[ii])
        self.verifyRowCount(tables[ii], NumDatasetRows, verbose)

        self.operators.unpublish(publishedTable)
        self.operators.unpublish(publishedTable2)
        self.operators.dropTable("*", deleteCompletely=True)

    def testMultiKeyPublishedTables(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", ["val", "valDiv10"], fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "XcalarOpCode")
        t = self.operators.getRowNum(t, "", "XcalarRankOver")
        t = self.operators.project(
            t, "", ["XcalarOpCode", "XcalarRankOver", "p1-val", "p1-valDiv10"])

        self.operators.publish(t, pt)
        self.operators.update(t, pt)
        t = self.operators.select(pt, "")

        self.verifyRowCount(t, NumDatasetRows)

        self.operators.unpublish(pt)
        self.operators.dropTable("*", deleteCompletely=True)

    def testSelect(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt, dropSrc=True)
        with pytest.raises(XcalarApiStatusException):
            print(self.operators.dropTable(t, deleteCompletely=True))
            # table should already be dropped

        out = "selected"

        query = [{
            "operation": "XcalarApiSelect",
            "args": {
                "source":
                    pt,
                "dest":
                    out,
                "columns": [{
                    "sourceColumn": "rowNum1"
                }, {
                    "sourceColumn": "rowNum2"
                }, {
                    "sourceColumn": "rowNum3"
                }, {
                    "sourceColumn": "p1-val"
                }, {
                    "sourceColumn": "sum"
                }, {
                    "sourceColumn": "count"
                }, {
                    "sourceColumn": "max",
                }, {
                    "sourceColumn": "avg",
                }],
                "eval": {
                    "Filter":
                        "eq(rowNum2, rowNum1)",
                    "Maps": [{
                        "evalString": "add(rowNum1, 1)",
                        "newField": "rowNum2"
                    }, {
                        "evalString": "add(rowNum2, 1)",
                        "newField": "rowNum3"
                    }],
                    "GroupBys": [{
                        "func": "sum",
                        "arg": "rowNum2",
                        "newField": "sum"
                    }, {
                        "func": "count",
                        "arg": "1",
                        "newField": "count"
                    }, {
                        "func": "max",
                        "arg": '"string"',
                        "newField": "max"
                    }, {
                        "func": "avg",
                        "arg": "2.1",
                        "newField": "avg"
                    }],
                    "GroupByKeys": ["p1-val", "rowNum1", "rowNum2", "rowNum3"]
                }
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, NumDatasetRows)

        rs = ResultSet(self.client, table_name=out, session_name=TestOpSessionName).record_iterator()
        for row in rs:
            assert (row["rowNum1"] + 1 == row["rowNum2"]
                    and row["rowNum2"] == row["rowNum3"]
                    and row["sum"] == row["rowNum2"] and row["count"] == 1
                    and row["max"] == "string" and row["avg"] == 2.1)
        del rs

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.unpublish(pt)

    def testSelectNoRowsAggregate(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt, dropSrc=True)
        with pytest.raises(XcalarApiStatusException):
            print(self.operators.dropTable(t, deleteCompletely=True))
            # table should already be dropped

        out = "selected"

        query = [{
            "operation": "XcalarApiSelect",
            "args": {
                "source":
                    pt,
                "dest":
                    out,
                "columns": [{
                    "sourceColumn": "rowNum1"
                }, {
                    "sourceColumn": "rowNum2"
                }, {
                    "sourceColumn": "rowNum3"
                }, {
                    "sourceColumn": "p1-val"
                }, {
                    "sourceColumn": "sum"
                }, {
                    "sourceColumn": "count"
                }, {
                    "sourceColumn": "max",
                }, {
                    "sourceColumn": "avg",
                }],
                "eval": {
                    "Filter":
                        "eq(0, 1)",
                    "Maps": [{
                        "evalString": "add(rowNum1, 1)",
                        "newField": "rowNum2"
                    }, {
                        "evalString": "add(rowNum2, 1)",
                        "newField": "rowNum3"
                    }],
                    "GroupBys": [{
                        "func": "sum",
                        "arg": "rowNum2",
                        "newField": "sum"
                    }, {
                        "func": "count",
                        "arg": "1",
                        "newField": "count"
                    }, {
                        "func": "max",
                        "arg": '"string"',
                        "newField": "max"
                    }, {
                        "func": "avg",
                        "arg": "2.1",
                        "newField": "avg"
                    }],
                    "GroupByKeys": []
                }
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query))

        rs = ResultSet(self.client, table_name=out, session_name=TestOpSessionName).record_iterator()
        for row in rs:
            assert (row['count'] == 0 and 'avg' not in row and 'max' not in row
                    and 'sum' not in row)
        del rs

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.unpublish(pt)

    def testSelectLimit(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt, dropSrc=True)
        with pytest.raises(XcalarApiStatusException):
            print(self.operators.dropTable(t, deleteCompletely=True))
            # table should already be dropped

        t = self.operators.select(pt, "", limitRows=1)
        rows = self.getRowCount(t)
        assert (rows >= 1 and rows < NumDatasetRows)

        t = self.operators.select(pt, "", limitRows=10)
        rows = self.getRowCount(t)
        assert (rows >= 10 and rows < NumDatasetRows)

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.unpublish(pt)

    def testSelectReplaceKey(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt, dropSrc=True)
        with pytest.raises(XcalarApiStatusException):
            print(self.operators.dropTable(t, deleteCompletely=True))
            # table should already be dropped

        out = "selected"

        query = [{
            "operation": "XcalarApiSelect",
            "args": {
                "source":
                    pt,
                "dest":
                    out,
                "columns": [{
                    "sourceColumn": "rowNum1"
                }, {
                    "sourceColumn": "rowNum2"
                }, {
                    "sourceColumn": "p1-val"
                }],
                "eval": {
                    "Maps": [{
                        "evalString": "string(p1-val)",
                        "newField": "p1-val"
                    }]
                }
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query))

        rs = ResultSet(self.client, table_name=out, session_name=TestOpSessionName).record_iterator()
        for row in rs:
            assert (isinstance(row['p1-val'], str))
        del rs

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.unpublish(pt)

    def testSelectIndex(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt)

        self.operators.addIndex(pt, "rowNum1")
        self.operators.addIndex(pt, "rowNum2")

        out = "selected"

        query = [{
            "operation": "XcalarApiSelect",
            "args": {
                "source":
                    pt,
                "dest":
                    out,
                "columns": [{
                    "sourceColumn": "rowNum1"
                }, {
                    "sourceColumn": "rowNum2"
                }, {
                    "sourceColumn": "rowNum3"
                }, {
                    "sourceColumn": "p1-val"
                }, {
                    "sourceColumn": "sum"
                }],
                "eval": {
                    "Filter":
                        "le(rowNum2, 10)",
                    "Maps": [{
                        "evalString": "add(rowNum1, 1)",
                        "newField": "rowNum2"
                    }, {
                        "evalString": "add(rowNum2, 1)",
                        "newField": "rowNum3"
                    }],
                    "GroupBys": [{
                        "func": "sum",
                        "arg": "rowNum2",
                        "newField": "sum"
                    }],
                    "GroupByKeys": ["p1-val", "rowNum1", "rowNum2", "rowNum3"]
                }
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, 10)

        self.operators.dropTable(out, deleteCompletely=True)
        query[0]["args"]["eval"]["Filter"] = "lt(rowNum1, 10)"
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, 9)
        self.operators.dropTable(out, deleteCompletely=True)

        query[0]["args"]["eval"]["Filter"] = "ge(rowNum1, 10)"
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, NumDatasetRows - 9)
        self.operators.dropTable(out, deleteCompletely=True)

        query[0]["args"]["eval"]["Filter"] = "gt(rowNum2, 10)"
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, NumDatasetRows - 10)
        self.operators.dropTable(out, deleteCompletely=True)

        query[0]["args"]["eval"]["Filter"] = "between(rowNum1, 10, 5)"
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, 0)
        self.operators.dropTable(out, deleteCompletely=True)

        query[0]["args"]["eval"]["Filter"] = "between(rowNum1, 5, 10)"
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, 6)

        rs = ResultSet(self.client, table_name=out, session_name=TestOpSessionName).record_iterator()
        for row in rs:
            assert (row["rowNum1"] + 1 == row["rowNum2"]
                    and row["rowNum2"] == row["rowNum3"]
                    and row["sum"] == row["rowNum2"])
        del rs
        self.operators.dropTable(out, deleteCompletely=True)

        # test auto index creation
        self.operators.removeIndex(pt, "*")
        listOut = self.operators.listPublishedTables(pt)
        assert (len(listOut.tables[0].indexes) == 0)

        query[0]["args"]["eval"]["Filter"] = "ge(rowNum1, 10)"
        query[0]["args"]["createIndex"] = True
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, NumDatasetRows - 9)
        self.operators.dropTable(out, deleteCompletely=True)

        query[0]["args"]["eval"]["Filter"] = "gt(rowNum2, 10)"
        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, NumDatasetRows - 10)
        self.operators.dropTable(out, deleteCompletely=True)

        listOut = self.operators.listPublishedTables(pt)
        assert (len(listOut.tables[0].indexes) == 2)

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.unpublish(pt)

    def testSelectJoin(self):
        pt = "pub tables"

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt)

        # create a joinTable with keys < 100
        t = self.operators.filter(t, "", "lt(p1-val, 100)")

        # duplicate the table with union all to test dup keys
        columns = [
            XcalarApiColumnT("rowNum1", "rowNum1", "DfInt64"),
            XcalarApiColumnT("rowNum2", "rowNum2", "DfInt64"),
            XcalarApiColumnT("p1-val", "p1-val", "DfInt64"),
        ]
        t = self.operators.union([t, t], "", [columns, columns])
        joinTable = self.operators.indexTable(t, "", "p1-val")

        out = "selected"

        query = [{
            "operation": "XcalarApiSelect",
            "args": {
                "source":
                    pt,
                "dest":
                    out,
                "columns": [{
                    "sourceColumn": "rowNum1-Join"
                }, {
                    "sourceColumn": "rowNum2"
                }, {
                    "sourceColumn": "p1-val"
                }, {
                    "sourceColumn": "sum"
                }, {
                    "sourceColumn": "sum-join"
                }],
                "eval": {
                    "Join": {
                        "source":
                            joinTable,
                        "joinType":
                            "innerJoin",
                        "columns": [{
                            "sourceColumn": "rowNum1",
                            "destColumn": "rowNum1-Join",
                            "columnType": "DfInt64"
                        },
                                    {
                                        "sourceColumn": "rowNum2",
                                        "destColumn": "rowNum2-Join",
                                        "columnType": "DfInt64"
                                    }]
                    },
                    "Filter":
                        "eq(rowNum2-Join, rowNum2)",
                    "Maps": [{
                        "evalString": "add(rowNum1-Join, 1)",
                        "newField": "rowNum2"
                    },
                             {
                                 "evalString": "add(rowNum2, 1)",
                                 "newField": "rowNum1-Join"
                             }],
                    "GroupBys": [{
                        "func": "sum",
                        "arg": "rowNum2",
                        "newField": "sum"
                    },
                                 {
                                     "func": "sum",
                                     "arg": "rowNum2-Join",
                                     "newField": "sum-join"
                                 }],
                    "GroupByKeys": ["p1-val", "rowNum1-Join", "rowNum2"]
                }
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(out, 100)

        rs = ResultSet(self.client, table_name=out, session_name=TestOpSessionName).record_iterator()
        for row in rs:
            assert (row["rowNum1-Join"] == row["rowNum2"]
                    and row["sum"] == row["rowNum2"]
                    and row["sum-join"] == row["sum"] - 1)
        del rs

        self.operators.dropTable("*", deleteCompletely=True)
        self.operators.unpublish(pt)

    def testPublishedTablesBasic(self):
        tables = ["table" + str(i) for i in range(100)]
        publishedTable = "a"
        publishedTable2 = "b"
        ii = 0
        numUpdates = 5
        verbose = False
        dropUpdates = True

        self.operators.indexDataset(
            datasetName, tables[ii], "valDiv10", fatptrPrefixName="p1")
        ii = ii + 1

        self.operators.getRowNum(tables[ii - 1], tables[ii], "rowNum")
        ii = ii + 1

        self.operators.project(tables[ii - 1], tables[ii],
                               ["rowNum", "p1-valDiv10"])
        ii = ii + 1

        self.operators.map(tables[ii - 1], tables[ii], ["int(1)"],
                           ["XcalarOpCode"])
        ii = ii + 1

        self.operators.indexTable(tables[ii - 1], tables[ii], "rowNum")
        self.verifyRowCount(tables[ii], NumDatasetRows, verbose)
        ii = ii + 1

        self.operators.publish(tables[ii - 1], publishedTable)

        self.operators.indexTable(tables[ii - 1], tables[ii],
                                  ["rowNum", "p1-valDiv10"])
        ii = ii + 1

        self.operators.publish(tables[ii - 1], publishedTable2)

        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 2)
        assert (out.tables[0].numRowsTotal == NumDatasetRows)
        assert (out.tables[1].numRowsTotal == NumDatasetRows)

        assert (len(out.tables[0].updates) == 1)
        assert (out.tables[0].updates[0].numRows == NumDatasetRows)
        assert (out.tables[0].updates[0].numInserts == NumDatasetRows)
        assert (out.tables[0].updates[0].numUpdates == 0)
        assert (out.tables[0].updates[0].numDeletes == 0)

        assert (len(out.tables[1].updates) == 1)
        assert (out.tables[1].updates[0].numRows == NumDatasetRows)
        assert (out.tables[1].updates[0].numInserts == NumDatasetRows)
        assert (out.tables[1].updates[0].numUpdates == 0)
        assert (out.tables[1].updates[0].numDeletes == 0)

        # test add/remove indexes
        self.operators.addIndex(publishedTable, "rowNum")
        self.operators.addIndex(publishedTable, "p1-valDiv10")

        out = self.operators.listPublishedTables(publishedTable)
        assert (len(out.tables[0].indexes) == 2)
        assert (out.tables[0].indexes[0].key.name == "p1-valDiv10")
        assert (out.tables[0].indexes[0].uptimeMS > 0)
        assert (out.tables[0].indexes[0].sizeEstimate > 0)
        assert (out.tables[0].indexes[1].key.name == "rowNum")
        assert (out.tables[0].indexes[1].uptimeMS > 0)
        assert (out.tables[0].indexes[1].sizeEstimate > 0)

        self.operators.removeIndex(publishedTable, "rowNum")
        self.operators.removeIndex(publishedTable, "p1-valDiv10")

        out = self.operators.listPublishedTables(publishedTable)
        assert (len(out.tables[0].indexes) == 0)

        # Test ability to apply multiple updates
        updates = []
        dests = []
        for i in range(numUpdates):
            self.operators.map(
                tables[ii - 1], tables[ii],
                ["int(add(rowNum," + str(NumDatasetRows / 2) + "))"],
                ["rowNum"])
            ii = ii + 1

            self.operators.map(tables[ii - 1], tables[ii], ["int(1)"],
                               ["XcalarRankOver"])
            ii = ii + 1

            self.operators.map(tables[ii - 1], tables[ii], ["int(1)"],
                               ["XcalarOpCode"])
            ii = ii + 1

            self.operators.indexTable(tables[ii - 1], tables[ii], "rowNum")
            ii = ii + 1

            updates.append(tables[ii - 1])

            self.operators.indexTable(tables[ii - 1], tables[ii],
                                      ["rowNum", "p1-valDiv10"])
            ii = ii + 1
            updates.append(tables[ii - 1])

            dests.append(publishedTable)
            dests.append(publishedTable2)

        self.operators.update(updates, dests)

        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 2)
        assert (
            out.tables[0].numRowsTotal == NumDatasetRows * (1 + numUpdates))
        assert (
            out.tables[1].numRowsTotal == NumDatasetRows * (1 + numUpdates))

        if dropUpdates:
            self.operators.dropTable("*", deleteCompletely=True)

        # Test filters on select
        self.operators.select(
            publishedTable, tables[ii], filterString="le(rowNum, 10)")
        self.verifyRowCount(tables[ii], 10, verbose)
        rs = ResultSet(self.client, table_name=tables[ii], session_name=TestOpSessionName).record_iterator()
        row = next(rs)
        assert ("rowNum" in row and "p1-valDiv10" in row)
        del rs
        ii = ii + 1

        # Test fields of interest on select
        self.operators.select(
            publishedTable,
            tables[ii],
            columns=[
                XcalarApiColumnT("p1-valDiv10", "col1", "DfUnknown"),
                XcalarApiColumnT("rowNum", "rowNum1", "DfUnknown"),
            ])
        rs = ResultSet(self.client, table_name=tables[ii], session_name=TestOpSessionName).record_iterator()
        row = next(rs)
        assert ("rowNum1" in row and "col1" in row and "rowNum" not in row)
        del rs
        ii = ii + 1

        # Test fields of interest and filters on select
        self.operators.select(
            publishedTable,
            tables[ii],
            columns=[
                XcalarApiColumnT("rowNum", "rowNum1", "DfUnknown"),
            ],
            filterString="le(rowNum, 11)")
        self.verifyRowCount(tables[ii], 11, verbose)
        rs = ResultSet(self.client, table_name=tables[ii], session_name=TestOpSessionName).record_iterator()
        row = next(rs)
        assert ("rowNum1" in row and "p1-valDiv10" not in row
                and "rowNum" not in row)
        del rs
        ii = ii + 1

        # Get the original version
        self.operators.select(publishedTable, tables[ii], 0)
        ii = ii + 1

        self.operators.indexTable(tables[ii - 1], tables[ii], "rowNum")
        self.verifyRowCount(tables[ii], NumDatasetRows, verbose)
        ii = ii + 1

        # Get the latest version
        self.operators.select(publishedTable, tables[ii])
        ii = ii + 1

        self.operators.indexTable(tables[ii - 1], tables[ii], "rowNum")
        self.verifyRowCount(tables[ii],
                            NumDatasetRows + numUpdates * (NumDatasetRows / 2),
                            verbose)
        ii = ii + 1

        # Test ability to ignore FNF updates
        self.operators.map(tables[ii - 1], tables[ii], ["int(div(1,0))"],
                           ["p1-valDiv10"])
        ii = ii + 1

        self.removeBatchId(tables[ii - 1], tables[ii])
        ii = ii + 1

        self.operators.update(tables[ii - 1], publishedTable)
        self.operators.select(publishedTable, tables[ii])
        ii = ii + 1

        self.operators.filter(tables[ii - 1], tables[ii],
                              "exists(p1-valDiv10)")
        self.verifyRowCount(tables[ii],
                            NumDatasetRows + numUpdates * (NumDatasetRows / 2),
                            verbose)
        ii = ii + 1

        # Test ability to delete rows
        self.operators.map(tables[ii - 1], tables[ii],
                           ["int(if(le(rowNum, 10), 1, 0))"], ["XcalarOpCode"])
        ii = ii + 1

        self.removeBatchId(tables[ii - 1], tables[ii])
        ii = ii + 1

        self.operators.update(tables[ii - 1], publishedTable)
        self.operators.select(publishedTable, tables[ii])
        self.verifyRowCount(tables[ii], 10, verbose)
        ii = ii + 1

        # Test ranged select
        self.operators.select(
            publishedTable, tables[ii], minBatchId=0, maxBatchId=0)
        self.verifyRowCount(tables[ii], NumDatasetRows, verbose)
        ii = ii + 1

        # Test coalescing
        self.operators.coalesce(publishedTable)
        self.operators.select(publishedTable, tables[ii])
        self.verifyRowCount(tables[ii], 10, verbose)
        ii = ii + 1

        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 2)
        assert (out.tables[0].numRowsTotal == 10)

        # Test ability to make retina using a published table
        self.operators.select(
            publishedTable,
            tables[ii],
            columns=[
                XcalarApiColumnT("rowNum", "rowNum1", "DfUnknown"),
                XcalarApiColumnT("p1-valDiv10", "col1", "DfUnknown")
            ])
        ii = ii + 1
        self.operators.filter(tables[ii - 1], tables[ii], "eq(rowNum1, 1)")
        ii = ii + 1

        retina = "ret"
        self.retina.make(retina, [tables[ii - 1]], ["rowNum"], [])
        self.retina.execute(retina, [], tables[ii])
        self.verifyRowCount(tables[ii], 1, verbose)
        ii = ii + 1
        self.retina.delete(retina)

        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 2)
        assert (out.tables[0].active is True)
        assert (out.tables[0].numPersistedUpdates == len(
            out.tables[0].updates))
        assert (out.tables[1].active is True)
        assert (out.tables[1].numPersistedUpdates == len(
            out.tables[1].updates))

        # Test inactivate and drop
        self.operators.unpublish(publishedTable2, True)
        self.operators.unpublish(publishedTable2)
        out = self.operators.listPublishedTables("*")
        assert (out.tables[0].name == publishedTable)
        assert (len(out.tables) == 1)

        # Test inactivate and restore
        self.operators.unpublish(publishedTable, True)
        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 1)
        assert (out.tables[0].name == publishedTable)
        assert (out.tables[0].active is False)

        self.operators.restorePublishedTable(publishedTable)
        self.operators.publishTableChangeOwner(
            publishedTable, TestOpSessionName, TestOpUserName)
        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 1)
        assert (out.tables[0].name == publishedTable)
        assert (out.tables[0].active is True)

        self.operators.select(publishedTable, tables[ii])
        self.verifyRowCount(tables[ii], 10, verbose)

        self.operators.unpublish(publishedTable)
        out = self.operators.listPublishedTables("*")
        assert (len(out.tables) == 0)

        self.operators.dropTable("*", deleteCompletely=True)

    def restoreAndCancel(self, tableName):
        p1 = Process(
            target=self.operators.restorePublishedTable, args=(tableName, ))
        p1.start()

        time.sleep(random.randint(0, 3))

        p2 = Process(
            target=self.operators.unpublish, args=(
                tableName,
                True,
            ))
        p2.start()

        p1.join()
        p2.join()

    def testPublishedTablesScale(self):
        numThreads = 5
        timeoutSecs = 600
        pool = Pool(processes=numThreads)
        threads = []
        for ii in range(0, numThreads):
            testname = "testThreads-{}".format(ii)
            t = pool.apply_async(invoke, (testname, "ptScale", ii))
            threads.append(t)
        for t in threads:
            assert t.get(timeoutSecs)
        pool.close()
        pool.join()
        self.operators.dropTable("*", deleteCompletely=True)

    def testAddManyColumns(self):
        numMaps = 1021    # 1021 maps + 1 fatptr + 1 index key = 1023 fields
        baseTable = "base"
        self.operators.indexDataset(
            datasetName, baseTable, "val", fatptrPrefixName="p1")
        for ii in range(numMaps):
            if ii == 0:
                srcTable = baseTable
                srcCol = "p1::val"
            else:
                srcTable = "table{}".format(ii - 1)
                srcCol = "col{}".format(ii - 1)
            dstTable = "table{}".format(ii)
            newCol = "col{}".format(ii)
            self.operators.map(srcTable, dstTable,
                               ["add({}, 1)".format(srcCol)], [newCol])
            if ii > 0:
                self.operators.dropTable(
                    "table{}".format(ii - 1), deleteCompletely=True)
        self.operators.dropTable("*", deleteCompletely=True)

    def testIndexMixedType(self):
        tables = ["base", "map", "index"]
        with pytest.raises(XcalarApiStatusException) as e:
            self.operators.indexDataset(
                datasetName, tables[0], "mixedType", fatptrPrefixName="p1")
        assert (e.value.status == StatusT.StatusDfTypeMismatch)
        self.operators.indexDataset(
            datasetName, tables[0], "val", fatptrPrefixName="p1")
        self.operators.map(tables[0], tables[1],
                           ["string({})".format("mixedType")],
                           "mixedTypeString")
        self.operators.indexTable(tables[1], tables[2], "mixedTypeIndex")
        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

    def testUnionWithNoSrcTables(self):
        with pytest.raises(XcalarApiStatusException) as e:
            self.operators.union([], "unionTable", [])
        assert (e.value.status == StatusT.StatusInval)

    def testGroupByMultiKeyRename(self):
        t1 = self.operators.indexDataset(
            datasetName, "", ["val", "valDiv10"], fatptrPrefixName="p1")
        query = [
            {
                "operation": "XcalarApiGroupBy",
                "args": {
                    "source": t1,
                    "dest": "gb1",
                    "eval": [{
                        "evalString": "count(1)",
                        "newField": "gbField"
                    }]
                }
            },
            {
                "operation": "XcalarApiGroupBy",
                "args": {
                    "source":
                        t1,
                    "dest":
                        "gb2",
                    "eval": [{
                        "evalString": "count(1)",
                        "newField": "gbField"
                    }],
                    "key": [
                        {
                            "name": "p1-val",
                            "keyFieldName": "key1",
                        },
                        {
                            "name": "p1-valDiv10",
                            "keyFieldName": "key2",
                        },
                    ]
                }
            },
            {
                "operation": "XcalarApiGroupBy",
                "args": {
                    "source":
                        t1,
                    "dest":
                        "gb3",
                    "eval": [{
                        "evalString": "count(1)",
                        "newField": "gbField"
                    }],
                    "key": [{
                        "name": "p1-valDiv10",
                        "keyFieldName": "key2",
                    }, ]
                }
            },
            {
                "operation": "XcalarApiGroupBy",
                "args": {
                    "source":
                        t1,
                    "dest":
                        "gb4",
                    "eval": [{
                        "evalString": "count(1)",
                        "newField": "gbField"
                    }],
                    "key": [
                        {
                            "name": "p1-valDiv10",
                            "keyFieldName": "key1",
                        },
                        {
                            "name": "p1-val",
                            "keyFieldName": "key2",
                        },
                    ]
                }
            },
        ]

        self.xcalarApi.submitQuery(json.dumps(query))

        self.verifySchema("gb1", ["gbField", "p1-val", "p1-valDiv10"])
        self.verifySchema("gb2", ["gbField", "key1", "key2"])
        self.verifySchema("gb3", ["gbField", "p1-val", "key2"])
        self.verifySchema("gb4", ["gbField", "key1", "key2"])

        self.operators.dropTable("*", deleteCompletely=True)

    def testUDFInQueryState(self):
        myadd_source = '''
def myadd(a,b):
    return (a+b)
'''
        self.udf.addOrUpdate("myaddUdf", myadd_source)
        t1 = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        query = [{
            "operation": "XcalarApiMap",
            "args": {
                "source":
                    t1,
                "dest":
                    "map1",
                "eval": [{
                    "evalString": "myaddUdf:myadd(1,1)",
                    "newField": "mapField"
                }]
            },
            "annotations": {
                "columns": [{
                    "sourceColumn": "p1-val",
                }, {
                    "sourceColumn": "p1",
                    "columnType": "DfFatptr"
                }]
            }
        }]

        self.xcalarApi.submitQuery(
            json.dumps(query), TestOpSessionName, queryName="queryGet")

        qs = self.xcalarApi.queryState("queryGet")
        found_map = False
        for ii in range(0, qs.queryGraph.numNodes):
            node = qs.queryGraph.node[ii]
            if node.api == XcalarApisT.XcalarApiMap:
                found_map = True
                evalList = node.input.mapInput.eval
                for jj in range(0, len(evalList)):
                    print("node {} evalString[{}] is {}\n\n".format(
                        ii, jj, evalList[jj].evalString))
                    # assert that the eval string has relative UDF name
                    assert ('/workbook' not in evalList[jj].evalString)
                    assert ('/myaddUdf' not in evalList[jj].evalString)

        assert (found_map is True)
        self.operators.dropTable("*", deleteCompletely=True)

    def testQueryAnnotations(self):
        t1 = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        query = [{
            "operation": "XcalarApiMap",
            "args": {
                "source": t1,
                "dest": "map1",
                "eval": [{
                    "evalString": "add(1,1)",
                    "newField": "mapField"
                }]
            },
            "annotations": {
                "columns": [{
                    "sourceColumn": "p1-val",
                }, {
                    "sourceColumn": "p1",
                    "columnType": "DfFatptr"
                }]
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query), TestOpSessionName, "q1")

        # Test query list
        output = self.xcalarApi.listQueries("q1")
        assert (output.queries[0].name == 'q1')
        assert (output.queries[0].state == 'qrFinished')
        self.xcalarApi.deleteQuery("q1")

        output = self.xcalarApi.listQueries("q1")
        assert (len(output.queries) == 0)

        self.verifySchema(
            "map1", columnsInSchema=["p1::val", "p1-val", "mapField"])

        query = [{
            "operation": "XcalarApiIndex",
            "args": {
                "source": "map1",
                "dest": "index1",
                "key": [{
                    "name": "mapField"
                }]
            },
            "annotations": {
                "columns": [{
                    "sourceColumn": "mapField",
                }]
            }
        }]

        self.xcalarApi.submitQuery(json.dumps(query), TestOpSessionName, "q1")
        self.xcalarApi.deleteQuery("q1")

        self.verifySchema(
            "index1",
            columnsInSchema=["mapField"],
            columnsNotInSchema=["p1::val", "p1-val"])

        query = [
            {
                "operation": "XcalarApiIndex",
                "args": {
                    "source": "map1",
                    "dest": "index2",
                    "key": [{
                        "name": "mapField"
                    }]
                },
                "annotations": {
                    "columns": [{
                        "sourceColumn": "mapField",
                    }, {
                        "sourceColumn": "p1-val",
                    }]
                }
            },
            {
                "operation": "XcalarApiIndex",
                "args": {
                    "source": "map1",
                    "dest": "indexNoKey",
                    "key": [{
                        "name": "mapField"
                    }]
                },
                "annotations": {
                    "columns": [{
                        "sourceColumn": "p1-val",
                    }]
                }
            },
            {
                "operation": "XcalarApiMap",
                "args": {
                    "source":
                        "index2",
                    "dest":
                        "map2",
                    "eval": [{
                        "evalString": "add(1,1)",
                        "newField": "mapField2"
                    }]
                },
                "annotations": {
                    "columns": [{
                        "sourceColumn": "mapField",
                    }]
                }
            },
        ]

        self.xcalarApi.submitQuery(json.dumps(query), TestOpSessionName, "q1")
        self.xcalarApi.deleteQuery("q1")

        self.verifySchema(
            "map2",
            columnsInSchema=["mapField"],
            columnsNotInSchema=["p1-val"])

        # Even though we do not need the key, backend should ALWAYS add it because
        # the table needs to have the key
        self.verifySchema("indexNoKey", columnsInSchema=["p1-val", "mapField"])

        self.operators.dropTable("*", deleteCompletely=True)

    def testQueryDroppedState(self):
        t1 = self.operators.indexDataset(
            datasetName, "", ["val", "valDiv10"], fatptrPrefixName="p1")
        final = "final"
        query = [{
            "operation": "XcalarApiGroupBy",
            "state": "Dropped",
            "args": {
                "source": t1,
                "dest": "gb1",
                "eval": [{
                    "evalString": "count(1)",
                    "newField": "gbField1"
                }]
            }
        },
                 {
                     "operation": "XcalarApiGroupBy",
                     "state": "Dropped",
                     "args": {
                         "source":
                             "gb1",
                         "dest":
                             "gb2",
                         "eval": [{
                             "evalString": "count(1)",
                             "newField": "gbField2"
                         }]
                     }
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "state": "Dropped",
                     "args": {
                         "source": [t1, "gb1"],
                         "dest":
                             "join1",
                         "joinType":
                             "innerJoin",
                         "keepAllColumns":
                             False,
                         "columns": [[{
                             "sourceColumn": "p1-val"
                         }, {
                             "sourceColumn": "p1-valDiv10"
                         }], [{
                             "sourceColumn": "gbField1"
                         }]]
                     }
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "state": "Dropped",
                     "args": {
                         "source": ["join1", "gb2"],
                         "dest":
                             "join2",
                         "joinType":
                             "innerJoin",
                         "keepAllColumns":
                             False,
                         "columns": [[{
                             "sourceColumn": "p1-val"
                         }, {
                             "sourceColumn": "p1-valDiv10"
                         }, {
                             "sourceColumn": "gbField1"
                         }], [{
                             "sourceColumn": "gbField2"
                         }]]
                     }
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "state": "Dropped",
                     "args": {
                         "source": ["gb1", "gb2"],
                         "dest":
                             "join3",
                         "joinType":
                             "innerJoin",
                         "keepAllColumns":
                             False,
                         "columns": [[{
                             "sourceColumn": "p1-val"
                         }, {
                             "sourceColumn": "p1-valDiv10"
                         }, {
                             "sourceColumn": "gbField1"
                         }], [{
                             "sourceColumn": "gbField2"
                         }]]
                     }
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "state": "Dropped",
                     "args": {
                         "source": ["join1", "join3"],
                         "dest":
                             "join4",
                         "joinType":
                             "innerJoin",
                         "keepAllColumns":
                             False,
                         "columns": [[{
                             "sourceColumn": "p1-val"
                         }, {
                             "sourceColumn": "p1-valDiv10"
                         }, {
                             "sourceColumn": "gbField1"
                         }], [{
                             "sourceColumn": "gbField2"
                         }]]
                     }
                 },
                 {
                     "operation": "XcalarApiJoin",
                     "state": "Dropped",
                     "args": {
                         "source": ["join4", "join2"],
                         "dest":
                             final,
                         "joinType":
                             "innerJoin",
                         "keepAllColumns":
                             False,
                         "columns": [[{
                             "sourceColumn": "p1-val"
                         }, {
                             "sourceColumn": "p1-valDiv10"
                         }, {
                             "sourceColumn": "gbField1"
                         }, {
                             "sourceColumn": "gbField2"
                         }], []]
                     }
                 }]

        self.xcalarApi.submitQuery(json.dumps(query))
        self.verifyRowCount(final, NumDatasetRows)
        self.verifySchema(
            final,
            columnsInSchema=["p1-val", "p1-valDiv10", "gbField1", "gbField2"])

        out = self.xcalarApi.listTable("*")
        assert (out.numNodes == 2)
        assert (out.nodeInfo[0].name == final or out.nodeInfo[0].name == t1)
        assert (out.nodeInfo[1].name == final or out.nodeInfo[1].name == t1)

        self.operators.dropTable("*", deleteCompletely=True)

    def testAggregateEvals(self):
        # Test for bug 14197 : code coverage
        # customize eval for aggregate operation
        tables = ["tables", "max1", "max2"]
        constants = ["const"]

        eval_const = "max(p::valDiv10)"

        aggStr1 = "max(p::val)"
        aggStr2 = "max(add(p::val, ^{}))".format(constants[0])

        self.operators.indexDataset(datasetName, tables[0], "xcalarRecordNum",
                                    "p")
        self.operators.aggregate(tables[0], constants[0], eval_const)
        self.operators.aggregate(tables[0], tables[1], aggStr1)
        self.operators.aggregate(tables[0], tables[2], aggStr2)

        resultSet = ResultSet(self.client, table_name=constants[0], session_name=TestOpSessionName).record_iterator()
        max_valDiv10 = next(resultSet)
        del resultSet

        resultSet = ResultSet(self.client, table_name=tables[1], session_name=TestOpSessionName).record_iterator()
        max_val = next(resultSet)
        del resultSet

        resultSet = ResultSet(self.client, table_name=tables[2], session_name=TestOpSessionName).record_iterator()
        max_valAddConst = next(resultSet)
        del resultSet

        assert (float(max_valDiv10["constant"]) + float(
            max_val["constant"]) == float(max_valAddConst["constant"]))

        for table in tables:
            self.operators.dropTable(table, deleteCompletely=True)

        for constant in constants:
            self.operators.dropConstants(constant, deleteCompletely=True)

    def testMapInIcvMode(self):
        # Test for bug 14233: code coverage
        # Manully set the workItem.icv as True
        tbl = self.operators.indexDataset(
            datasetName, "", "xcalarRecordNum", fatptrPrefixName="p")
        workItem = WorkItemMap(tbl, "", "add(p::mixedType, 1)", "dummy")
        workItem.workItem.input.mapInput.icv = True    # set the icvMode
        tbl = self.xcalarApi.execute(workItem)
        rs = ResultSet(self.client, table_name=tbl, session_name=TestOpSessionName)
        if rs.record_count() > 0:
            for row in rs.record_iterator():
                assert (row["dummy"][:-1] in {
                    "XCE-00000108 Some variables are undefined during evaluation",
                    "XCE-000000F7 Operation is not supported on input type"
                })
        del rs
        self.operators.dropTable("*", deleteCompletely=True)

    def testGroupAllIcv(self):
        tbl = self.operators.indexDataset(
            datasetName, "", "xcalarRecordNum", fatptrPrefixName="p")

        workItem = WorkItemGroupBy(
            tbl,
            "", [
                "sum(p::nullVal)", "sum(p::mixedType)", "count(p::nullVal)",
                "max(p::nullVal)"
            ], ["sumNull", "sumMixed", "countNull", "maxNull"],
            groupAll=True)
        workItem.workItem.input.groupByInput.icv = True    # set the icvMode
        tbl = self.xcalarApi.execute(workItem)

        rs = ResultSet(self.client, table_name=tbl, session_name=TestOpSessionName)
        if rs.record_count() > 0:
            for row in rs.record_iterator():
                assert (row["sumNull"] ==
                        "XCE-0000011B No such field found while running aggregate"
                        and row["maxNull"] ==
                        "XCE-0000011B No such field found while running aggregate")
        del rs
        self.operators.dropTable("*", deleteCompletely=True)

    def testMoneyIdx_sql215(self):
        tables = ["table" + str(i) for i in range(100)]
        datasetName = "testMoneyIdx-partsupp"
        dataPath = XcalarQaDatasetPath + "/tpchDatasets/partsupp.tbl"
        schema = [
            XcalarApiColumnT(colName, colName, colType)
            for (colName, colType) in [
                (u"PS_PARTKEY", "DfInt64"),
                (u"PS_SUPPKEY", "DfInt64"),
                (u"PS_AVAILQTY", "DfInt64"),
                (u"PS_SUPPLYCOST", "DfMoney"),
                (u"PS_COMMENT", "DfString"),
            ]
        ]

        testDs = CsvDataset(
            self.xcalarApi,
            "Default Shared Root",
            dataPath,
            datasetName,
            fieldDelim="|",
            schemaMode="loadInput",
            schema=schema,
            isRecursive=False,
            linesToSkip=1)
        testDs.load()

        ii = 0
        self.operators.indexDataset(
            testDs.name,
            tables[ii],
            "PS_SUPPLYCOST",
            "p",
            ordering=XcalarOrderingT.XcalarOrderingDescending)

        resultSet = ResultSet(self.client, table_name=tables[ii], session_name=TestOpSessionName).record_iterator(num_rows=1000)
        prevVal = float(sys.maxsize)
        for row in resultSet:
            val = float(row['p-PS_SUPPLYCOST'])
            assert (val <= prevVal)
            prevVal = val

        del resultSet

        ii += 1
        self.operators.indexDataset(
            testDs.name,
            tables[ii],
            "PS_SUPPLYCOST",
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        resultSet = ResultSet(self.client, table_name=tables[ii], session_name=TestOpSessionName).record_iterator(num_rows=1000)
        prevVal = float(-sys.maxsize)
        for row in resultSet:
            val = float(row['p-PS_SUPPLYCOST'])
            assert (val >= prevVal)
            prevVal = val

        del resultSet

        self.operators.dropTable("*", deleteCompletely=True)
        testDs.delete()

    def testMoneyBasic(self):
        tables = ["table" + str(i) for i in range(100)]
        datasetName = "testMoneyBasic-tpchQ2-Answers"
        dataPath = XcalarQaDatasetPath + "/tpchDatasets/answers/q2.out"
        schema = [
            XcalarApiColumnT(colName, colName, colType)
            for (colName, colType) in [
                (u"S_ACCTBAL", "DfMoney"),
                (u"S_NAME", "DfString"),
                (u"N_NAME", "DfString"),
                (u"P_PARTKEY", "DfInt64"),
                (u"P_MFGR", "DfString"),
                (u"S_ADDRESS", "DfString"),
                (u"S_PHONE", "DfString"),
                (u"S_COMMENT", "DfString"),
            ]
        ]

        testDs = CsvDataset(
            self.xcalarApi,
            "Default Shared Root",
            dataPath,
            datasetName,
            fieldDelim="|",
            schemaMode="loadInput",
            schema=schema,
            isRecursive=False,
            linesToSkip=1)
        testDs.load()

        ii = 0
        self.operators.indexDataset(
            testDs.name,
            tables[ii],
            "S_ACCTBAL",
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["addNumeric('12345678.43', p::S_ACCTBAL)"],
                           ["m_addbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii], ["int(p::S_ACCTBAL)"],
                           ["m_int"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["money(money(p::S_ACCTBAL))"], ["m_multinumeric"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["eq(money('7852.45'), p::S_ACCTBAL)"], ["m_eqbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii], [
            "ifNumeric(gt(money('8012.97'), p::S_ACCTBAL), money('1.759'), money(1))"
        ], ["m_ifnumeric"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["eqNonNull(money('8231.61'), p::S_ACCTBAL)"],
                           ["m_eqnnbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["neq(money('7852.45'), p::S_ACCTBAL)"],
                           ["m_neqbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["exists(p::S_ACCTBAL)"], ["m_exists"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["isNull(p::S_ACCTBAL)"], ["m_isnull"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["subNumeric(m_addbal, '12345678.43')"],
                           ["m_subbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["multNumeric('10.5', m_subbal)"], ["m_multbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["multNumeric(money('10.3758'), p::S_ACCTBAL)"],
                           ["m_castbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["isNumeric(p::S_ACCTBAL)"], ["m_isn_acct"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["isNumeric(p::P_PARTKEY)"], ["m_isn_part"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["gt(money('7992.4'), p::S_ACCTBAL)"],
                           ["m_gt_acctbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["ge(money('7992.4'), p::S_ACCTBAL)"],
                           ["m_ge_acctbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["lt(money('7992.4'), p::S_ACCTBAL)"],
                           ["m_lt_acctbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["le(money('7992.4'), p::S_ACCTBAL)"],
                           ["m_le_acctbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["multNumeric(-1, m_multbal)"], ["m_multbal_neg"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["divNumeric(m_multbal, money('-3.14159'))"],
                           ["m_divbal"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii],
                           ["absNumeric(m_multbal_neg)"],
                           ["m_abs_multbal_neg"])
        ii += 1
        self.operators.map(tables[ii - 1], tables[ii], ["money('NaN')"],
                           ["nanval"])
        finalTabName = tables[ii]

        self.verifyAggregate(finalTabName, ["sumNumeric(m_multbal_neg)"],
                             '-9359017.29')
        self.verifyAggregate(finalTabName, ["sumNumeric(m_abs_multbal_neg)"],
                             '9359017.29')
        self.verifyAggregate(finalTabName, ["sumNumeric(m_divbal)"],
                             '-2979070.24')
        self.verifyAggregate(finalTabName, ["maxNumeric(m_multbal)"],
                             '104354.57')
        self.verifyAggregate(finalTabName, ["minNumeric(m_multbal)"],
                             '82356.96')
        self.verifyAggregate(finalTabName, ["avgNumeric(m_multbal)"],
                             '93590.17')
        self.verifyAggregate(finalTabName, ["sumNumeric(m_ifnumeric)"],
                             '109.87')
        self.verifyAggregate(finalTabName, ["sumNumeric(m_multinumeric)"],
                             '891334.98')
        self.verifyAggregate(finalTabName, ["sum(m_int)"], 891282)
        self.verifyAggregate(finalTabName, ["sum(int(m_eqbal))"], 1)
        self.verifyAggregate(finalTabName, ["sum(int(m_eqnnbal))"], 1)
        self.verifyAggregate(finalTabName, ["sum(int(m_neqbal))"], 99)
        self.verifyAggregate(finalTabName, ["sum(int(m_exists))"], 100)
        self.verifyAggregate(finalTabName, ["sum(int(m_isnull))"], 0)
        self.verifyAggregate(finalTabName, ["sum(int(m_isn_acct))"], 100)
        self.verifyAggregate(finalTabName, ["sum(int(m_isn_part))"], 0)
        self.verifyAggregate(finalTabName, ["sum(int(m_gt_acctbal))"], 12)
        self.verifyAggregate(finalTabName, ["sum(int(m_ge_acctbal))"], 13)
        self.verifyAggregate(finalTabName, ["sum(int(m_lt_acctbal))"], 87)
        self.verifyAggregate(finalTabName, ["sum(int(m_le_acctbal))"], 88)
        self.verifyAggregate(
            finalTabName, ["multNumeric(roundNumeric('3.1415', 0), '1000')"],
            '3000.00')
        self.verifyAggregate(
            finalTabName, ["multNumeric(roundNumeric('3.1415', 3), '1000')"],
            '3142.00')
        self.verifyAggregate(
            finalTabName,
            ["multNumeric(roundNumeric('3.14159265', 5), '100000')"],
            '314159.00')
        self.verifyAggregate(
            finalTabName,
            ["multNumeric(roundNumeric('3.14159265', 6), '100000')"],
            '314159.30')
        self.verifyAggregate(
            finalTabName,
            ["multNumeric(roundNumeric('3.14159265', 7), '100000')"],
            '314159.27')
        self.verifyAggregate(finalTabName, ["divNumeric('-112.21', '0')"],
                             '-Infinity')
        self.verifyAggregate(finalTabName, ["divNumeric('-112.21', '-0')"],
                             'Infinity')
        self.verifyAggregate(finalTabName, ["divNumeric('95.52', 0)"],
                             'Infinity')
        self.verifyAggregate(finalTabName, ["divNumeric(0, 0)"], 'NaN')
        self.verifyAggregate(finalTabName, ["divNumeric('0.0', '-0.0')"],
                             'NaN')

        self.operators.indexTable(finalTabName, "indexMoneyTestDsc", "nanval",
                                  "systemDescendingDht")
        self.operators.indexTable(finalTabName, "indexMoneyTestAsc", "nanval",
                                  "systemAscendingDht")
        self.operators.indexTable(finalTabName, "indexMoneyTestUn", "nanval",
                                  "systemUnorderedDht")
        self.operators.indexTable(finalTabName, "indexMoneyTestRand", "nanval",
                                  "systemRandomDht")
        self.operators.indexTable(finalTabName, "indexMoneyTestBcast",
                                  "nanval", "systemBroadcastDht")

        self.operators.dropTable("*", deleteCompletely=True)
        testDs.delete()

    def testMoneyOps(self):
        tables = ["table" + str(i) for i in range(100)]
        datasetName = "testMoneyOps-tpchQ2-Answers"
        dataPath = XcalarQaDatasetPath + "/tpchDatasets/answers/q2.out"
        schema = [
            XcalarApiColumnT(colName, colName, colType)
            for (colName, colType) in [
                (u"S_ACCTBAL", "DfMoney"),
                (u"S_NAME", "DfString"),
                (u"N_NAME", "DfString"),
                (u"P_PARTKEY", "DfInt64"),
                (u"P_MFGR", "DfString"),
                (u"S_ADDRESS", "DfString"),
                (u"S_PHONE", "DfString"),
                (u"S_COMMENT", "DfString"),
            ]
        ]

        testDs = CsvDataset(
            self.xcalarApi,
            "Default Shared Root",
            dataPath,
            datasetName,
            fieldDelim="|",
            schemaMode="loadInput",
            schema=schema,
            isRecursive=False,
            linesToSkip=1)
        testDs.load()

        ii = 0
        self.operators.indexDataset(
            testDs.name,
            tables[ii],
            "S_ACCTBAL",
            "p",
            ordering=XcalarOrderingT.XcalarOrderingAscending)

        ii += 1

        # Index with new prefix to avoid need for join rename
        self.operators.indexDataset(
            testDs.name,
            tables[ii],
            "S_ACCTBAL",
            "q",
            ordering=XcalarOrderingT.XcalarOrderingAscending)
        ii += 1
        self.operators.filter(tables[ii - 1], tables[ii],
                              "gt(q-S_ACCTBAL, money('8000'))")
        ii += 1

        # Basic self-join on DfMoney column
        self.operators.join(tables[0], tables[ii - 1], tables[ii])
        self.verifyRowCount(tables[ii], 107)
        ii += 1

        # Two instances of 9870.78 appear in q-S_ACCTBAL ==> 2x2 = 4
        self.verifyAggregate(
            tables[ii - 1], ["sum(int(eq(q-S_ACCTBAL, money('9870.78'))))"], 4)

        self.operators.dropTable("*", deleteCompletely=True)
        testDs.delete()

    def testMoneyCasts(self):
        tab = "testMoneyCasts"
        self.operators.indexDataset(datasetName, tab, "xcalarRecordNum", "p")

        # Cast to money
        self.verifyAggregate(tab, ["money(True)"], '1.00')
        self.verifyAggregate(tab, ["money(bool(27))"], '1.00')
        self.verifyAggregate(tab, ["money(False)"], '0.00')
        self.verifyAggregate(tab, ["money(bool(0))"], '0.00')
        self.verifyAggregate(tab, ["money('1.019')"], '1.02')
        self.verifyAggregate(tab, ["money(string('1.019'))"], '1.02')
        self.verifyAggregate(tab, ["money(2)"], '2.00')
        self.verifyAggregate(tab, ["money(int(2))"], '2.00')
        self.verifyAggregate(tab,
                             ["money(timestamp('2018-01-01T01:01:01.000Z'))"],
                             '1514768461000.00')
        self.verifyAggregate(tab, ["money(2.7182)"], '2.72')
        self.verifyAggregate(tab, ["money(float(2.7182))"], '2.72')
        self.verifyAggregate(tab, ["money(money(2))"], '2.00')

        # Cast from money
        self.verifyAggregate(tab, ["bool(money(True))"], True)
        self.verifyAggregate(tab, ["bool(money('2.7182'))"], True)
        self.verifyAggregate(tab, ["bool(money(False))"], False)
        self.verifyAggregate(tab, ["bool(money('0'))"], False)
        self.verifyAggregate(tab, ["string(money('2.7182'))"], '2.72')
        self.verifyAggregate(tab, ["int(money('-2.7182'))"], -2)
        self.verifyAggregate(tab, ["int(money(-5))"], -5)
        self.verifyAggregate(tab, ["timestamp(money('1514768461000'))"],
                             '2018-01-01T01:01:01.000Z')
        self.verifyAggregate(tab, ["float(money('2.7182'))"], 2.72)

        self.operators.dropTable("*", deleteCompletely=True)

    def testMoneySUDF(self):
        tab = "testMoneySUDF"
        tables = ["table" + str(i) for i in range(100)]
        i = 0
        self.operators.indexDataset(datasetName, tables[i], "xcalarRecordNum",
                                    "p")

        i += 1
        self.operators.map(tables[i - 1], tables[i],
                           ["isNumeric(p::numericType)"], ["isNumeric"])

        self.verifyAggregate(tables[i], ["sum(int(isNumeric))"],
                             NumDatasetRows)
        decAns = dec.Decimal(NumDatasetRows * 3.14159).quantize(
            dec.Decimal('0.01'), rounding=dec.ROUND_UP)
        self.verifyAggregate(tables[i], ["sumNumeric(p::numericType)"],
                             str(decAns))

    def testSelectDependency(self):
        pt = ["pt_" + str(i) for i in range(6)]

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        t = self.operators.project(t, "", ["rowNum1", "rowNum2", "p1-val"])

        self.operators.publish(t, pt[0])
        self.operators.publish(t, pt[1])
        self.operators.publish(t, pt[2])

        t0 = self.operators.select(pt[0], "")
        t1 = self.operators.select(pt[1], "")
        t2 = self.operators.select(pt[2], "")

        j1 = self.operators.join(
            t0,
            t1,
            "",
            leftColumns=[XcalarApiColumnT("p1-val", "k1", "DfUnknown")],
            rightColumns=[],
            keepAllColumns=False)

        j2 = self.operators.join(
            j1,
            t2,
            "",
            leftColumns=[XcalarApiColumnT("p1-val", "k2", "DfUnknown")],
            rightColumns=[],
            keepAllColumns=False)

        self.operators.publish(j1, pt[3])
        self.operators.publish(j2, pt[4])

        t3 = self.operators.select(pt[3], "")
        t4 = self.operators.select(pt[4], "")

        j3 = self.operators.join(
            t3,
            t4,
            "",
            leftColumns=[XcalarApiColumnT("k1", "k3", "DfUnknown")],
            rightColumns=[],
            keepAllColumns=False)
        self.operators.publish(j3, pt[5])

        # inactivate and restore all pt
        for t in pt:
            self.operators.unpublish(t, inactivateOnly=True)

        for t in pt:
            self.operators.restorePublishedTable(t)

        # verify the dependency
        dep = json.loads(self.kvStore.lookup("/sys/imd_dependencies"))

        assert (dep[pt[0]]["children"][pt[3]])
        assert (dep[pt[0]]["children"][pt[4]])

        assert (dep[pt[1]]["children"][pt[3]])
        assert (dep[pt[1]]["children"][pt[4]])

        assert (dep[pt[2]]["children"][pt[4]])

        assert (dep[pt[3]]["parents"][pt[0]])
        assert (dep[pt[3]]["parents"][pt[1]])
        assert (dep[pt[3]]["children"][pt[5]])

        assert (dep[pt[4]]["parents"][pt[0]])
        assert (dep[pt[4]]["parents"][pt[1]])
        assert (dep[pt[4]]["parents"][pt[2]])
        assert (dep[pt[4]]["children"][pt[5]])

        assert (dep[pt[5]]["parents"][pt[3]])
        assert (dep[pt[5]]["parents"][pt[4]])

        self.operators.unpublish(pt[5])
        dep = json.loads(self.kvStore.lookup("/sys/imd_dependencies"))
        assert (pt[5] not in dep)
        assert (pt[5] not in dep[pt[3]]["children"])
        assert (pt[5] not in dep[pt[4]]["children"])

        self.operators.unpublish(pt[4])
        dep = json.loads(self.kvStore.lookup("/sys/imd_dependencies"))
        assert (pt[4] not in dep)
        assert (pt[4] not in dep[pt[0]]["children"])
        assert (pt[4] not in dep[pt[1]]["children"])
        assert (pt[4] not in dep[pt[2]]["children"])

        self.operators.unpublish(pt[3])
        dep = json.loads(self.kvStore.lookup("/sys/imd_dependencies"))
        assert (pt[3] not in dep)
        assert (pt[3] not in dep[pt[0]]["children"])
        assert (pt[3] not in dep[pt[1]]["children"])

        self.operators.unpublish(pt[2])
        self.operators.unpublish(pt[1])
        self.operators.unpublish(pt[0])

        self.operators.dropTable("*", deleteCompletely=True)

    @pytest.mark.parametrize("pTableNum, tableRowNum, restoreTestNum", [
        (5, 2, 2),
        (4, 3, 3),
    ])
    def testSelectDependencyScale(self, pTableNum, tableRowNum,
                                  restoreTestNum):
        verbose = False

        # Given a table, return its parent tables.
        tableGraph = dict()

        # Store all tables
        tableSet = set()

        # Given a table, return the the published table it selects from.
        pTableMap = dict()

        # Store all published tables
        pTableSet = set()

        pTableParents = dict()
        pTableChildren = dict()

        newTable = "t{}".format(len(tableSet))
        newPTable = "pt{}".format(len(pTableSet))

        t = self.operators.indexDataset(
            datasetName, "", "val", fatptrPrefixName="p1")
        t = self.operators.getRowNum(t, "", "rowNum1")
        t = self.operators.getRowNum(t, "", "rowNum2")
        self.operators.project(t, newTable, ["rowNum1", "rowNum2", "p1-val"])

        tableSet.add(newTable)

        def updatePTableParentsAndChildren(curTable, newPTable):
            if curTable in tableGraph:
                for nextTable in tableGraph[curTable]:
                    updatePTableParentsAndChildren(nextTable, newPTable)
            elif curTable in pTableMap:
                if newPTable not in pTableParents:
                    pTableParents[newPTable] = set()
                pTableParents[newPTable].add(pTableMap[curTable])

                if pTableMap[curTable] not in pTableChildren:
                    pTableChildren[pTableMap[curTable]] = set()
                pTableChildren[pTableMap[curTable]].add(newPTable)
            else:
                # curTable is from a dataset
                pass

        def sortedPTables():
            sortedRes = []
            visited = set()
            rootPTables = [
                pTable for pTable in pTableSet if pTable not in pTableParents
            ]

            # We add some randomness to the sort result.
            random.shuffle(rootPTables)
            for curPt in rootPTables:
                topoSort(curPt, sortedRes, visited)
            sortedRes.reverse()
            return sortedRes

        def topoSort(curPt, sortedRes, visited):
            if curPt in visited:
                return
            # It's impossbile that the graph in this test forms a cycle.

            pTableChildrenList = list(
                pTableChildren[curPt]) if curPt in pTableChildren else []
            # We add some randomness to the sort result.
            random.shuffle(pTableChildrenList)
            for childPt in pTableChildrenList:
                topoSort(childPt, sortedRes, visited)
            sortedRes.append(curPt)
            visited.add(curPt)

        def jsonToGraph(depJson):
            dep = json.loads(depJson)
            parents = {
                pTable: set(dep[pTable]["parents"])
                for pTable in dep if set(dep[pTable]["parents"])
            }
            children = {
                pTable: set(dep[pTable]["children"])
                for pTable in dep if set(dep[pTable]["children"])
            }
            return parents, children

        while len(pTableSet) < pTableNum:
            if verbose:
                print("There are {} tables and {} published tables created".
                      format(len(tableSet), len(pTableSet)))

            action = random.randint(0, 2)

            # We publish a table to published table
            # We try not to create too many tables. Otherwise we run out of
            # memory before reaching the published table number expected.
            if action == 0 or len(tableSet) > len(pTableSet) * 2:
                t = random.sample(tableSet, 1)[0]
                # We cannot publish a table that's just selected.
                if t in pTableMap:
                    continue

                newPTable = "pt{}".format(len(pTableSet))
                self.operators.publish(t, newPTable)
                pTableSet.add(newPTable)
                updatePTableParentsAndChildren(t, newPTable)
                assert ((pTableParents, pTableChildren) == jsonToGraph(
                    self.kvStore.lookup("/sys/imd_dependencies")))

            # We select a table from a published table
            elif action == 1:
                if not pTableSet:
                    continue

                pt = random.sample(pTableSet, 1)[0]
                newTable = "t{}".format(len(tableSet))
                self.operators.select(pt, newTable)
                tableSet.add(newTable)
                pTableMap[newTable] = pt

            # We generate a new table from 2 tables
            elif action == 2:
                if len(tableSet) < 2:
                    continue

                newTable = "t{}".format(len(tableSet))
                ts = random.sample(tableSet, 2)

                self.operators.join(
                    ts[0],
                    ts[1],
                    newTable,
                    leftColumns=[
                        XcalarApiColumnT("p1-val", "p1-val", "DfUnknown")
                    ],
                    rightColumns=[],
                    filterString="lt(p1-val, {})".format(tableRowNum),
                    keepAllColumns=False)

                tableSet.add(newTable)
                tableGraph[newTable] = [ts[0], ts[1]]

        listOut = self.operators.listPublishedTables("pt{}".format("*"))
        assert len(listOut.tables) == pTableNum

        for i in range(restoreTestNum):
            sortedPt = sortedPTables()
            if verbose:
                print("Published table restore test {}".format(i))
                print(sortedPt)

            # inactivate and restore all pt
            for t in sortedPt:
                self.operators.unpublish(t, inactivateOnly=True)

            for t in sortedPt:
                self.operators.restorePublishedTable(t)

        sortedPt = sortedPTables()
        sortedPt.reverse()
        for pt in sortedPt:
            self.operators.unpublish(pt)

            if pt in pTableParents:
                for parentPt in pTableParents[pt]:
                    pTableChildren[parentPt].remove(pt)
                    if not pTableChildren[parentPt]:
                        del pTableChildren[parentPt]
                del pTableParents[pt]

            assert ((pTableParents, pTableChildren) == jsonToGraph(
                self.kvStore.lookup("/sys/imd_dependencies")))

        self.operators.dropTable("*", deleteCompletely=True)

    # Dump table to csv for debugging.  Empty fields generally occur due to
    # incorrect prefix, keepAllColumns false, etc
    def dbgDumpTable(self, tname, cols, path, fprefix='p1::'):
        exportCols = [(fprefix + x, x) for x in cols]
        client = Client()
        client._user_name = TestOpUserName
        sess = client.get_session(TestOpSessionName)
        tab = sess.get_table(tname)
        tab.export(exportCols, 'single_csv', {
            'file_path': path,
            "target": DefaultTargetName,
            "field_delim": ","
        })

    # Types that have special values such as +-nan/+-inf
    def testSpecialVals(self):
        tables = ["table" + str(i) for i in range(100)]

        def getSpecialAns(col, specVals, vals, negate=False, op='count()'):
            vals = [specVals[x] for x in vals]
            if negate:
                return eval('col[~col.isin(vals)].' + op)
            else:
                return eval('col[col.isin(vals)].' + op)

        # Common tests for different nan/inf supporting types
        def helperFcn(colPrefix, specVals):
            colNanPrefix = "{}AllNan".format(colPrefix)
            self.operators.indexDataset(
                datasetName,
                'baseTab',
                "val",
                fatptrPrefixName="p1",
                ordering=XcalarOrderingT.XcalarOrderingAscending)
            self.dbgDumpTable('baseTab', self.datasetCols,
                              '/tmp/basemixed.csv')

            self.operators.indexTable('baseTab', 'mixedTab',
                                      'p1::' + colPrefix)
            # self.dbgDumpTable('mixedTab', [colPrefix], '/tmp/mixedTab{}.csv'.format(colPrefix), fprefix='p1-')
            self.verifyRowCount('mixedTab', NumDatasetRows)

            numNumericRows = getSpecialAns(
                self.testData['p1::' + colPrefix], specVals,
                ['nan', '-nan', 'inf', '-inf'], True)
            numPosInfRows = getSpecialAns(self.testData['p1::' + colPrefix],
                                          specVals, ['inf'])
            numNegInfRows = getSpecialAns(self.testData['p1::' + colPrefix],
                                          specVals, ['-inf'])
            numInfRows = numPosInfRows + numNegInfRows
            numPosNanRows = getSpecialAns(self.testData['p1::' + colPrefix],
                                          specVals, ['nan'])
            numNegNanRows = getSpecialAns(self.testData['p1::' + colPrefix],
                                          specVals, ['-nan'])
            numExpecteJoinRows = numPosInfRows * numPosInfRows + numNegInfRows * numNegInfRows + numPosNanRows * numPosNanRows + numNegNanRows * numNegNanRows + numNumericRows

            self.operators.join(
                'mixedTab',
                'mixedTab',
                'joinMixed',
                keepAllColumns=False,
                joinType=JoinOperatorT.InnerJoin)
            # self.dbgDumpTable('joinMixed', [colPrefix], '/tmp/joinMixed{}.csv'.format(colPrefix), fprefix='p1-')
            self.verifyRowCount('joinMixed', numExpecteJoinRows)

            self.operators.indexTable('baseTab', 'nanTab',
                                      'p1::' + colNanPrefix)
            self.verifyRowCount('nanTab', NumDatasetRows)

            self.operators.join(
                'nanTab',
                'nanTab',
                'joinNan',
                keepAllColumns=False,
                joinType=JoinOperatorT.InnerJoin)
            self.verifyRowCount('joinNan', NumDatasetRows**2)
            # self.dbgDumpTable('joinNan', self.datasetCols, '/tmp/nan00.csv', fprefix='p1-')

            self.verifyAggregate(
                'nanTab', ["sum(int(isNan(p1-{})))".format(colNanPrefix)],
                NumDatasetRows)
            self.verifyAggregate('mixedTab',
                                 ["sum(int(isNan(p1::{})))".format(colPrefix)],
                                 numPosNanRows + numNegNanRows)
            self.verifyAggregate('mixedTab',
                                 ["sum(int(isInf(p1::{})))".format(colPrefix)],
                                 numInfRows)
            self.operators.filter('mixedTab', 'noNans',
                                  "not(isNan(p1::{}))".format(colPrefix))
            self.verifyAggregate(
                'noNans', ["count(p1::{})".format(colPrefix)],
                NumDatasetRows - (numPosNanRows + numNegNanRows))
            self.operators.filter('mixedTab', 'noInfs',
                                  "not(isInf(p1::{}))".format(colPrefix))
            self.verifyAggregate('noInfs', ["count(p1::{})".format(colPrefix)],
                                 NumDatasetRows - numInfRows)
            self.operators.filter('noNans', 'noInfsNoNans',
                                  "not(isInf(p1::{}))".format(colPrefix))
            # self.dbgDumpTable('mixedTab', [colPrefix], '/tmp/mixedtab.csv', fprefix='p1::')

        ################################################################
        # Stuff specific to floats...
        specVals = {'nan': 'nan', '-nan': '-nan', 'inf': 'inf', '-inf': '-inf'}
        helperFcn('floatType', specVals)
        # XXX: Resolve nan/-nan result issues for float
        # self.verifyAggregate('mixedTab', 'sum(p1::floatType)', '-nan')
        self.verifyAggregate('mixedTab', 'sum(p1::floatTypeAllNan)', 'nan')
        # Filter out specials then sum in pandas
        colSum = getSpecialAns(
            self.testData['p1::floatType'],
            specVals, ['nan', '-nan', 'inf', '-inf'],
            True,
            op='sum()')
        self.verifyAggregate('noInfsNoNans',
                             ["sum(p1::{})".format("floatType")], colSum)
        self.operators.dropTable("*", deleteCompletely=True)

        ################################################################
        # Stuff specific to money...
        specVals = {
            'nan': 'NaN',
            '-nan': '-NaN',
            'inf': 'Infinity',
            '-inf': '-Infinity'
        }
        helperFcn('moneyType', specVals)
        self.verifyAggregate('mixedTab', 'sumNumeric(p1::moneyType)', 'NaN')
        self.verifyAggregate('mixedTab', 'sumNumeric(p1::moneyTypeAllNan)',
                             'NaN')
        self.operators.filter('joinMixed', 'noInfsJm',
                              "not(isInf(p1-{}))".format("moneyType"))
        self.operators.filter('noInfsJm', 'noInfsNoNansJm',
                              "not(isNan(p1-{}))".format("moneyType"))

        # Filter out specials then sum in pandas
        # Pandas sum does not support decimal type so cheat and use float
        colSum = getSpecialAns(
            self.testData['p1::moneyType'].astype(float),
            specVals, ['nan', '-nan', 'inf', '-inf'],
            True,
            op='sum()')
        strAns = str(dec.Decimal(colSum).quantize(dec.Decimal('.01')))

        # XXX: Works in XD, API/dropped column issue with below aggregate, will
        # resolve in separate commit, see ENG-5196
        # self.verifyAggregate('noInfsNoNansJm',
        #                      ["sumNumeric(p1-{})".format("moneyType")], strAns)

        self.operators.dropTable("*", deleteCompletely=True)


###############################################################################
# Everything below is helper functions to facilitate the above tests
###############################################################################


def invoke(testname, fn, args):
    try:
        worker = PtThread(testname)
        fn = getattr(worker, fn)
        return fn(args)
    except Exception:
        traceback.print_exc()
        return None


class PtThread():
    def __init__(self, testName):
        self.testName = testName
        self.to = TestOperators()

    def ptScale(self, threadId):
        ii = 0
        pt = "PubTable{}".format(threadId)
        rt = "RegTable{}".format(threadId)
        verbose = False
        TotalUpdates = 2
        TotalPubTables = 2
        dropTables = []

        # Setup a table that will be published
        self.to.operators.indexDataset(
            dsSmallName, "%s%d" % (rt, ii), "valDiv10", fatptrPrefixName="p1")
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.to.operators.getRowNum("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                    "rowNum")
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.to.operators.project("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  ["rowNum", "p1-valDiv10"])
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.to.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                              ["int(1)"], ["XcalarOpCode"])
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.to.operators.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                     "rowNum")
        self.to.verifyRowCount("%s%d" % (rt, ii), NumDsRowsSmall, verbose)

        ii = ii + 1

        # Create publish tables
        for jj in range(TotalPubTables):
            self.to.operators.publish("%s%d" % (rt, ii - 1), "%s%d" % (pt, jj))

            # Test drop and restore
            self.to.operators.unpublish("%s%d" % (pt, jj), True)
            self.to.operators.restorePublishedTable("%s%d" % (pt, jj))
            self.to.operators.publishTableChangeOwner(
                "%s%d" % (pt, jj), TestOpSessionName, TestOpUserName)

        # Prepare updates; issue updates
        for kk in range(TotalUpdates):
            updates = []
            dests = []
            tmpTables = []
            self.to.operators.map(
                "%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                ["int(add(rowNum," + str(NumDsRowsSmall / 2) + "))"],
                ["rowNum"])
            tmpTables.append("%s%d" % (rt, ii - 1))
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.to.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  ["int(1)"], ["XcalarRankOver"])
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.to.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  ["int(1)"], ["XcalarOpCode"])
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.to.operators.indexTable("%s%d" % (rt, ii - 1),
                                         "%s%d" % (rt, ii), "rowNum")
            ii = ii + 1
            self.to.removeBatchId("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii))

            for jj in range(TotalPubTables):
                dests.append("%s%d" % (pt, jj))
                updates.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.to.operators.update(updates, dests)
            for dd in range(len(tmpTables)):
                self.to.operators.dropTable(
                    tmpTables[dd], deleteCompletely=True)

        for dd in range(len(dropTables)):
            self.to.operators.dropTable(dropTables[dd], deleteCompletely=True)

        # Verify all batches of each publish table
        for jj in range(TotalPubTables):
            for kk in range(TotalUpdates):
                tmpTables = []
                self.to.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii),
                                         kk)
                tmpTables.append("%s%d" % (rt, ii))
                self.to.verifyRowCount(
                    "%s%d" % (rt, ii),
                    NumDsRowsSmall + kk * (NumDsRowsSmall / 2), verbose)
                ii = ii + 1
                # Add range selects here to iterate through all ranges
                for nn in range(0, kk + 1):
                    self.to.operators.select("%s%d" % (pt, jj),
                                             "%s%d" % (rt, ii), nn, 0)
                    tmpTables.append("%s%d" % (rt, ii))
                    self.to.verifyRowCount(
                        "%s%d" % (rt, ii),
                        NumDsRowsSmall + nn * (NumDsRowsSmall / 2), verbose)
                    ii = ii + 1
                for dd in range(len(tmpTables)):
                    self.to.operators.dropTable(
                        tmpTables[dd], deleteCompletely=True)

        # Prepare deletes; issue deletes; verify publish tables
        updates = []
        dests = []
        dropTables = []
        curRowCount = 10
        for jj in range(TotalPubTables):
            self.to.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii))
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            mapStr = "int(if(le(rowNum, %d), 1, 0))" % curRowCount
            self.to.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  [mapStr], ["XcalarOpCode"])
            dropTables.append("%s%d" % (rt, ii))

            ii = ii + 1
            self.to.removeBatchId("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii))
            dropTables.append("%s%d" % (rt, ii))

            # Create dependencies between different publish tables
            for kk in range(TotalPubTables):
                if kk >= jj:
                    # Avoid cyclical dependencies; Create self dependencies.
                    updates.append("%s%d" % (rt, ii))
                    dests.append("%s%d" % (pt, kk))
            ii = ii + 1
        self.to.operators.update(updates, dests)
        for dd in range(len(dropTables)):
            self.to.operators.dropTable(dropTables[dd], deleteCompletely=True)
        dropTables = []
        for jj in range(TotalPubTables):
            self.to.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii))
            self.to.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
        for dd in range(len(dropTables)):
            self.to.operators.dropTable(dropTables[dd], deleteCompletely=True)

        # Issue coalesce; verify publish tables
        dropTables = []
        for jj in range(TotalPubTables):
            self.to.operators.coalesce("%s%d" % (pt, jj))
            self.to.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii))
            self.to.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            retinaTable = "%s%d" % (rt, ii)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            retName = "ret%d%d" % (threadId, jj)
            self.to.retina.make(retName, [retinaTable], ["rowNum"], [])
            self.to.retina.execute(retName, [], "%s%d" % (rt, ii))
            self.to.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.to.retina.delete(retName)
        for dd in range(len(dropTables)):
            self.to.operators.dropTable(dropTables[dd], deleteCompletely=True)

        # Txn clenout
        for jj in range(TotalPubTables):
            dropTables = []
            pubTab = "%s%d" % (pt, jj)

            # Create an update with all deletes
            self.to.operators.select(pubTab, "%s%d" % (rt, ii))
            self.to.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.to.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  ["int(0)"], ["XcalarOpCode"])
            dropTables.append("%s%d" % (rt, ii))

            ii = ii + 1
            self.to.removeBatchId("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii))

            goodUpdate = "%s%d" % (rt, ii)
            dropTables.append(goodUpdate)
            ii = ii + 1

            # Create an update with all deletes except an invalid op code
            self.to.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  ["ifInt(eq(rowNum, 1), div(1,0), 0)"],
                                  ["XcalarOpCode"])

            badUpdate = "%s%d" % (rt, ii)
            dropTables.append(badUpdate)
            ii = ii + 1
            try:
                # Do a good update on a table and a bad update in the same api
                # the bad update should invoke txn recovery and rollback
                # everything
                self.to.operators.update([goodUpdate, badUpdate],
                                         [pubTab, pubTab])
            except XcalarApiStatusException:
                pass

            # None of the deletes should have succeeded
            self.to.operators.select(pubTab, "%s%d" % (rt, ii))
            self.to.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            for dd in range(len(dropTables)):
                self.to.operators.dropTable(
                    dropTables[dd], deleteCompletely=True)

        # Clean up
        for jj in range(TotalPubTables):
            # Delete permanently
            self.to.operators.unpublish("%s%d" % (pt, jj))
        return True
