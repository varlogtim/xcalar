# Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import tarfile
import json
import pytest
import os
import shutil
import random

from xcalar.compute.util.config import build_config
from xcalar.compute.util.Qa import XcalarQaDatasetPath, datasetCheck
from xcalar.external.client import Client

from xcalar.external.LegacyApi.Env import XcalarConfigPath
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Dataset import Dataset, CsvDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.Export import Export
from xcalar.external.result_set import ResultSet
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.runtime import Runtime

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import (XcalarApiColumnT)

testRetinaSessionName = "TestRetina"

memoryTargetName = "QA memory"
targets = [
    (memoryTargetName, "memory", {}),
]


class TestRetina(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        cls.session = cls.client.create_session(testRetinaSessionName)
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)
        cls.retina = Retina(cls.xcalarApi)
        cls.export = Export(cls.xcalarApi)
        cls.target = Target2(cls.xcalarApi)
        cls.udf = Udf(cls.xcalarApi)

        for targetName, targetTypeId, targetArgs in targets:
            cls.target.add(targetTypeId, targetName, targetArgs)

    def teardown_class(cls):
        cls.client.destroy_session(testRetinaSessionName)

    def doRetinaTestWithSrc(self, retinaNoSynthPath, retinaPath,
                            expectedNumRows):
        # Use the retina without synthesize nodes to populate the seesion with source tables
        self.doRetinaTest(
            retinaNoSynthPath,
            "retinaNoSynthPath",
            expectedNumRows,
            None,
            True,
            primeTables=True)

        # Now run the retina with synthesize nodes
        self.doRetinaTest(retinaPath, "retinaPath", expectedNumRows, None,
                          False)

        # Clean up the session
        self.operators.dropTable("*")
        self.operators.dropConstants("*")
        Dataset.bulkDelete(self.xcalarApi, "*")

    def doRetinaTest(self,
                     retinaPath,
                     testPrefix,
                     expectedNumRows,
                     expectedAnswer=None,
                     runAsNormalQuery=True,
                     matchOrdering=False,
                     primeTables=False,
                     nestedTest=False,
                     exportToFile=False,
                     exportToFileOnly=False):
        # Look for retinainfo.json as well in case this is a older Dataflow file
        with tarfile.open(retinaPath, "r:gz") as tFile:
            try:
                infoFile = tFile.extractfile("dataflowInfo.json")
            except KeyError:
                infoFile = tFile.extractfile("retinaInfo.json")
            retinaInfo = json.loads(infoFile.read())
            # Copy retina's UDFs into current workbook sandbox. The tests
            # execute the retina's qgraph by extracting the query from the
            # retina and submitting this as a query, as opposed to executing
            # the retina directly. This means the qgraph's UDF references, must
            # be resolved against the workbook's UDF container.
            # NOTE: since all tests use the same workbook, delete the UDF first,
            # and ignore delete failures (UDF may not exist).
            tflist = tFile.getmembers()
            for tmemb in tflist:
                if tmemb.isfile() and 'udfs/' in tmemb.name:
                    udfsource = tFile.extractfile(tmemb).read()
                    udfsourcestr = udfsource.decode("utf-8")
                    moduleName = os.path.basename(tmemb.name).split('.', 1)[0]
                    if moduleName != "default":
                        self.udf.addOrUpdate(moduleName, udfsourcestr)

        datasetName = "%s-dataset" % testPrefix
        queryName = "%s-query" % testPrefix
        retinaName = "%s-retina" % testPrefix

        if isinstance(retinaInfo["query"], str):
            query = str(retinaInfo["query"])
        else:
            query = json.dumps(retinaInfo["query"])
        query = str.replace(query, "<pathToQaDatasets>", XcalarQaDatasetPath)
        query = str.replace(query, "<datasetName>", datasetName)
        query = str.replace(query, "<tablePrefix>", testPrefix)

        dstTable = retinaInfo["tables"][0]["name"]

        retinaContents = None
        with open(retinaPath, "rb") as fp:
            retinaContents = fp.read()

        try:
            self.retina.add(retinaName, retinaContents)
        except XcalarApiStatusException as e:
            if e.status == StatusT.StatusRetinaAlreadyExists:
                self.retina.delete(retinaName)
                self.retina.add(retinaName, retinaContents)
            else:
                raise e

        # Sometimes we might want to skip this step if the query is complex and
        # produces a lot of tables. This is because normal query doesn't have the
        # ability to drop tables when they're done, and so we can potentially
        # run out of memory
        if (runAsNormalQuery):
            self.xcalarApi.submitQuery(query, self.session.name, queryName)

            output = self.xcalarApi.listTable(dstTable)
            assert (output.numNodes == 1)
            assert (output.nodeInfo[0].name == dstTable)

            output = self.operators.tableMeta(dstTable)
            numRows = sum(
                [output.metas[jj].numRows for jj in range(output.numMetas)])

            assert (numRows == expectedNumRows or primeTables)

            self.operators.dropTable("*")
            self.operators.dropConstants("*")
            Dataset.bulkDelete(self.xcalarApi, "*")
            self.xcalarApi.deleteQuery(queryName, deferredDelete=False)

        # Prime the session with tables in this query
        if (primeTables):
            self.operators.dropTable("*")
            try:
                self.xcalarApi.submitQuery(query, self.session.name, queryName)
            except XcalarApiStatusException:
                pass

        # Now run the retina and verify we get the same results

        retinaParameters = [{
            "paramName": "pathToQaDatasets",
            "paramValue": XcalarQaDatasetPath
        }, {
            "paramName": "datasetName",
            "paramValue": datasetName
        }, {
            "paramName": "tablePrefix",
            "paramValue": testPrefix
        }]

        if (exportToFileOnly is False):
            retinaDstTable = "retinaTests-%s" % dstTable
            clean_job = random.choice([True, False])
            parallel_ops = random.choice([True, False])
            sched_name = random.choice(Runtime.get_dataflow_scheds())

            self.retina.execute(
                retinaName,
                retinaParameters,
                retinaDstTable,
                queryName=retinaName,
                parallel_operations=parallel_ops,
                sched_name=sched_name,
                clean_job_state=clean_job)
            try:
                self.xcalarApi.deleteQuery(retinaName, deferredDelete=False)
            except XcalarApiStatusException as e:
                assert e.status == StatusT.StatusQrQueryNotExist

        if (exportToFile):
            configDict = build_config(XcalarConfigPath).all_options

            localExportDir = os.path.join(
                configDict["Constants.XcalarRootCompletePath"], "export")

            queryGraph = json.loads(query)
            for op in queryGraph:
                if op["operation"] == "XcalarApiExport":
                    localExportPath = os.path.join(
                        localExportDir,
                        os.path.splitext(op["args"]["fileName"])[0])
                    print("Deleting {}".format(localExportPath))
                    assert ("xcalar" in localExportPath)
                    try:
                        shutil.rmtree(localExportPath)
                        print("Deleted {}".format(localExportPath))
                    except FileNotFoundError as e:
                        print("{} doesn't exist.".format(localExportPath))

            clean_job = random.choice([True, False])
            parallel_ops = random.choice([True, False])
            sched_name = random.choice(Runtime.get_dataflow_scheds())

            self.retina.execute(
                retinaName,
                retinaParameters,
                queryName=retinaName,
                parallel_operations=parallel_ops,
                sched_name=sched_name,
                clean_job_state=clean_job)
            try:
                self.xcalarApi.deleteQuery(retinaName, deferredDelete=False)
            except XcalarApiStatusException as e:
                assert e.status == StatusT.StatusQrQueryNotExist
            if (exportToFileOnly):
                return

        # Try creating a nested retina
        if (nestedTest):
            columns = list(
                map(lambda col: col["columnName"],
                    retinaInfo["tables"][0]["columns"]))

            self.retina.make("nested", [retinaDstTable], [columns])
            self.retina.delete("nested")

        output = self.xcalarApi.listTable(retinaDstTable)
        assert (output.numNodes == 1)
        assert (output.nodeInfo[0].name == retinaDstTable)

        output = self.operators.tableMeta(retinaDstTable)
        retinaNumRows = sum(
            [output.metas[ii].numRows for ii in range(output.numMetas)])
        assert (retinaNumRows == expectedNumRows or primeTables)

        if (expectedAnswer is not None):
            resultSet = ResultSet(self.client, table_name=retinaDstTable, session_name=testRetinaSessionName)
            datasetCheck(
                expectedAnswer, resultSet.record_iterator(), matchOrdering=matchOrdering)
            del resultSet

        self.retina.delete(retinaName)
        self.operators.dropTable(retinaDstTable)

    def testBasicOps(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/basic_ops_df.tar.gz",
            "retinaTests-basicOps", 999, None, False)

    def testJoins(self):
        self.doRetinaTest(XcalarQaDatasetPath + "/retinaTests/joinTest.tar.gz",
                          "retinaTests-joinTest", 70815, None, True)

    @pytest.mark.xfail(
        reason="Multi-key index specified with duplicate key name")
    def testDupKeyIndex(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/dupKeyIndex.tar.gz",
            "retinaTests-dupKeyIndex", 0, None, False)

    @pytest.mark.xfail(reason="Bad dag state specified")
    def testBadDagState(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/badDagState.tar.gz",
            "retinaTests-badDagState", 0, None, False)

    def testRetinaCrossJoin(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/crossJoin.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/crossJoin.tar.gz", 91)

    def testNestedWithDelims(self):
        # SDK-635 Union on source tables with FatPtrs in schema are failed.
        try:
            self.doRetinaTest(
                XcalarQaDatasetPath + "/retinaTests/nestedWithDelims.tar.gz",
                "retinaTets-nestedWithDelims", 1933, None, True)
        except XcalarApiStatusException:
            pass

    def testNestedWithDelims2(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/nestedWithDelims2.tar.gz",
            "retinaTets-nestedWithDelims2", 991, None, True)

    def testNestedWithDelims3(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/nestedWithDelims3.tar.gz",
            "retinaTets-nestedWithDelims3", 1000, None, True)

    def testNestedWithDelims4(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/nestedWithDelims4Orig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/nestedWithDelims4.tar.gz",
            1000000)

    def testNestedSynthesize(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/nestedSynthesizeOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/nestedSynthesize.tar.gz",
            141629)

    def testRetinaWithSrcProject(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath +
            "/retinaTests/retinaWithSrcProjectOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/retinaWithSrcProject.tar.gz",
            141623)

    def testRetinaWithSrcLoad(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/retinaWithSrcLoadOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/retinaWithSrcLoad.tar.gz",
            1000)

    def testRetinaWithEmptySrc(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/retinaWithEmptySrcOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/retinaWithEmptySrc.tar.gz", 1)

    def testUnionWithSrc(self):
        # SDK-635 Union on source tables with FatPtrs in schema are failed.
        try:
            self.doRetinaTestWithSrc(
                XcalarQaDatasetPath +
                "/retinaTests/unionFatptrDedupOrig.tar.gz",
                XcalarQaDatasetPath + "/retinaTests/unionFatptrDedup.tar.gz",
                43)
        except XcalarApiStatusException:
            pass

    def testUnionWithSrc2(self):
        # SDK-635 Union on source tables with FatPtrs in schema are failed.
        try:
            self.doRetinaTestWithSrc(
                XcalarQaDatasetPath + "/retinaTests/unionTestOrig.tar.gz",
                XcalarQaDatasetPath + "/retinaTests/unionTest.tar.gz", 141856)
        except XcalarApiStatusException:
            pass

    def testMapsWithSrc(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/mapTestOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/mapTest.tar.gz", 11337)

    def testDoubleOuterJoin(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/doubleOuterJoinOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/doubleOuterJoin.tar.gz", 70818)

    def testRetinaUnionDedup(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/unionDedup.tar.gz",
            "retinaTests-unionDedup", 11, None, True)

    def testSelfJoinRename(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/selfJoinRename.tar.gz",
            "retinaTests-selfJoinRename", 1000000, None, False)

    def testNestedSelfJoin(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/selfJoin.tar.gz",
            "retinaTests-nestedSelfJoin",
            1000,
            None,
            False,
            nestedTest=True)

    def testRightJoin(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/rightJoin.tar.gz",
            "retinaTests-rightJoin", 1000, None, False)

    def testMultiKeyIndexGroupBy(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/indexGroupBy.tar.gz",
            "retinaTests-indexGroupBy", 4140, None, True)

    def testIndexProject(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/indexProject.tar.gz",
            "retinaTests-indexProject", 1000, None, False)

    def testProjectImmediate(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/projectImmediate.tar.gz",
            "retinaTests-projectImmediate", 1000, None, False)

    def testIndexJoinProject(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/indexJoinProject.tar.gz",
            "retinaTests-indexJoinProject", 1000000, None, False)

    def testDoubleIndex(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/doubleIndex.tar.gz",
            "retinaTests-doubleIndex", 29, None, True)

    def testJoinRename(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/joinRename.tar.gz",
            "retinaTests-joinRename", 192, None, False)

    def testIndexFilter(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/indexFilter.tar.gz",
            "retinaTests-indexFilter", 79, None, False)

    def testIndexFilterPrefix(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/indexFilterPrefix.tar.gz",
            "retinaTests-indexFilterPrefix", 934, None, False)

    def testJoinDropColumns(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/joinDropColumns.tar.gz",
            "retinaTests-joinDropColumns", 1000, None, True)

    def testRetinaBug7491(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug7491.tar.gz",
            "retinaTests-retinaBug7491", 1423, None, True)

    def testRetinaBug7700(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug7700.tar.gz",
            "retinaTests-retinaBug7700", 1184, None, True)

    def testRetinaBug7862(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug7862.tar.gz",
            "retinaTests-retinaBug7862", 70817, None, True)

    @pytest.mark.skip(reason="we disallow certain field names until "
                      "xcalarEval is able to support all possible names")
    def testRetinaBug8444(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug8444.tar.gz",
            "retinaTests-retinaBug8444", 1, None, True)

    def testRetinaBug8673(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug8673.tar.gz",
            "retinaTests-retinaBug8673", 1000, None, True)

    def testRetinaBug9222(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug9222.tar.gz",
            "retinaTests-retinaBug9222", 1000, None, True)

    def testRetinaBug9765(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug9765.tar.gz",
            "retinaTests-retinaBug9765", 16306, None, False)

    def testRetinaBug10866(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/bug10866Orig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/bug10866.tar.gz", 26)

    def testRetinaBug13481(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath + "/retinaTests/bug13481Orig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/bug13481.tar.gz", 10)

    @pytest.mark.xfail(reason="Union of mixed types")
    def testRetinaBug10873(self):
        self.doRetinaTest(XcalarQaDatasetPath + "/retinaTests/bug10873.tar.gz",
                          "retinaTests-10873", 145075, None, True)

    @pytest.mark.skip(reason="Enable this once Xc-8601 is fixed")
    def testRetinaColumnDots(self):
        self.doRetinaTest(XcalarQaDatasetPath + "/retinaTests/testDots.tar.gz",
                          10, None, False)

    def testRetinaEmptyTableBug(self):
        # SDK-635 Union on source tables with FatPtrs in schema are failed.
        try:
            self.doRetinaTest(
                XcalarQaDatasetPath + "/retinaTests/emptyTableBug.tar.gz",
                "retinaTests-emptyTableBug", 141789, None, True)
        except XcalarApiStatusException:
            pass

    def testCustomer12Batch(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/customer12Batch.tar.gz",
            "retinaTests-customer12Batch", 5, None, True)

    def testTpcdsQ6(self):
        self.doRetinaTest(XcalarQaDatasetPath + "/retinaTests/tpcdsq6.tar.gz",
                          "tpcdsq6", 5, None, True)

    def testTpchQ2(self):
        datasetName = "tpchQ2-Answers"
        ansUrl = XcalarQaDatasetPath + "/tpchDatasets/answers/q2.out"
        schema = [
            XcalarApiColumnT(colName, colName, colType)
            for (colName, colType) in [
                ("S_ACCTBAL", "DfFloat64"),
                ("S_NAME", "DfString"),
                ("N_NAME", "DfString"),
                ("P_PARTKEY", "DfInt64"),
                ("P_MFGR", "DfString"),
                ("S_ADDRESS", "DfString"),
                ("S_PHONE", "DfString"),
                ("S_COMMENT", "DfString"),
            ]
        ]

        ansDs = CsvDataset(
            self.xcalarApi,
            "Default Shared Root",
            ansUrl,
            datasetName,
            fieldDelim="|",
            schemaMode="loadInput",
            schema=schema,
            isRecursive=False,
            linesToSkip=1)
        ansDs.load()
        resultSet = ResultSet(self.client, dataset_name=ansDs.name, session_name=testRetinaSessionName)
        expectedRows = []
        for row in resultSet.record_iterator():
            for col in row:
                if isinstance(row[col], str):
                    row[col] = row[col].rstrip()

            # Some of the comment from the input has trailing white space, but we can't
            # tell for sure from the answer
            row.pop("S_COMMENT", None)

            # Need to add in the key
            row["supplier__S_ACCTBAL"] = row["S_ACCTBAL"]

            expectedRows.append(row)
        del resultSet
        ansDs.delete()

        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/tpchq2.tar.gz",
            "tpchq2",
            100,
            expectedRows,
            False,
            matchOrdering=False)

    def testMultiSort(self):
        expectedAnswer = [
            {
                "Row": "4",
                "Col": "40",
                "Val": "450"
            },
            {
                "Row": "4",
                "Col": "40",
                "Val": "400"
            },
            {
                "Row": "3",
                "Col": "30",
                "Val": "300"
            },
            {
                "Row": "3",
                "Col": "35",
                "Val": "300"
            },
            {
                "Row": "2",
                "Col": "20",
                "Val": "200"
            },
            {
                "Row": "1",
                "Col": "10",
                "Val": "100"
            },
        ]

        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/test-multisort.tar.gz",
            "retinaTests-testMultiSort",
            6,
            expectedAnswer,
            False,
            matchOrdering=True)

    @pytest.mark.skip(reason="Bug is tested by other retinas and this "
                      "retina no longer has a supported Synthesize arg format")
    def testRetinaBug10952(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug10952.tar.gz",
            "retinaTests-retinaBug10952", 3132)

    def testXc11186FNFDescend(self):
        self.doRetinaTest(
            XcalarQaDatasetPath +
            "/retinaTests/retinaBug11186FNFDescend.tar.gz",
            "retinaTests-retinaBug11186FNFDescend", 5054, None, False)

    def testXc11426FNF(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug11426FNF.tar.gz",
            "retinaTests-retinaBug11426FNF", 56, None, False)

    def testLargeExport(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/testLargeExport.tar.gz",
            "retinaTests-testLargeExport",
            100,
            runAsNormalQuery=False,
            exportToFile=True)

    def testExplodeString(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/testExplode.tar.gz",
            "retinaTests-test_explode",
            25590,
            runAsNormalQuery=True)

    def testXc12479(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug12479.tar.gz",
            "retinaTests-retinaBug12479", 100, None, False)

    def testXc12493(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaBug12493.tar.gz",
            "retinaTests-retinaBug12493",
            41602,
            runAsNormalQuery=False,
            exportToFile=True,
            exportToFileOnly=True)

    def testXc12514(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/bug12514.tar.gz",
            "retinaTests-retinaBug12514",
            1,
            runAsNormalQuery=True)

    def testXc13684(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/bug13684.tar.gz",
            "retinaTests-retinaBug13684",
            1,
            runAsNormalQuery=True)

    def testXc14283(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/bug14283.tar.gz",
            "retinaTests-retinaBug14283",
            70817,
            runAsNormalQuery=True)

    def testXc14379(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/bug14379.tar.gz",
            "retinaTests-retinaBug14379",
            0,
            runAsNormalQuery=False)

    def testXc14460(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/bug14460.tar.gz",
            "retinaTests-retinaBug14460",
            2000,
            runAsNormalQuery=False)

    def testSynthesizeOptimizer(self):
        self.doRetinaTestWithSrc(
            XcalarQaDatasetPath +
            "/retinaTests/SynthesizeOptimizerOrig.tar.gz",
            XcalarQaDatasetPath + "/retinaTests/SynthesizeOptimizer.tar.gz",
            137)

    def testSdk738(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/sdk738.tar.gz",
            "retinaTests-Sdk738",
            2000000,
            runAsNormalQuery=True)

    def testRetinaEng6762NullSess(self):
        self.doRetinaTest(
            XcalarQaDatasetPath + "/retinaTests/retinaWithStreamingUDF.tar.gz",
            "retinaTests-retinaEng6762NullSess", 1307, None, True)
