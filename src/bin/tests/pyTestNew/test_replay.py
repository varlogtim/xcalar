# Copyright 2018 - 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

#
# This test contains simple infrastructure to ease testing of workbook replay.
# If you want to add a new replay test, please look at sampleReplayTest() below.
#

from multiprocessing import Pool
import traceback
import pytest

from xcalar.compute.util.utils import get_logger
from xcalar.compute.util.cluster import DevCluster
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Dataset import UdfDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.result_set import ResultSet
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.client import Client

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own clusters")

logger = get_logger("test_replay")

NumDatasetRows = 100
memUdfName = "testReplay"
memUdfSource = """
import json
def load(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {"val": ii, "valDiv10": ii//10, "constant": 1}
        yield rowDict
    """
memoryTargetName = "QA test_operators memory"
targets = [
    (memoryTargetName, "memory", {}),
]

TestUserName = "TestReplay"
TestSessionName = "PublishedTableReplay"


class ArgsPubTabScale(object):
    def __init__(self,
                 datasetName=None,
                 numDatasetRows=None,
                 totalUpdates=None,
                 totalPubTables=None):
        self.datasetName = datasetName
        self.numDatasetRows = numDatasetRows
        self.totalUpdates = totalUpdates
        self.totalPubTables = totalPubTables


def publishedTableReplayScale():
    client = Client()
    xcalarApi = XcalarApi()
    session = client.create_session(TestSessionName)
    xcalarApi.setSession(session)
    operators = Operators(xcalarApi)
    udf = Udf(xcalarApi)
    # Udf must be added to the test's workbook since replay's qgraph reference
    # to the UDF's name when replaying the dataset load, must resolve within
    # the same workbook container
    udf.addOrUpdate(memUdfName, memUdfSource)

    # Dataset load
    path = str(NumDatasetRows)
    DatasetName = ".XcalarDS." + "memDS"
    dataset = UdfDataset(xcalarApi, memoryTargetName, path, DatasetName,
                         "{}:load".format(memUdfName))
    dataset.load()

    # Spin up threads to run publish table tests that includes creates,
    # selects etc.
    NumThreads = 2
    TimeoutSecs = 900
    TotalUpdates = 2
    TotalPubTables = 6
    pool = Pool(processes=NumThreads)
    runThreads = []
    for ii in range(0, NumThreads):
        args = ArgsPubTabScale(
            datasetName=DatasetName,
            numDatasetRows=NumDatasetRows,
            totalUpdates=TotalUpdates,
            totalPubTables=int(TotalPubTables / NumThreads))
        runThreads.append(
            pool.apply_async(
                invoke,
                ("ptScale", ii, TestUserName, TestSessionName, [args])))
    for tt in runThreads:
        assert tt.get(TimeoutSecs)
    pool.close()
    pool.join()

    operators.dropTable("*")
    tables = operators.listPublishedTables("*").tables
    assert (len(tables) == TotalPubTables)
    retPubTables = []
    for ii in range(NumThreads):
        for jj in range(int(TotalPubTables / NumThreads)):
            retPubTables.append("PubTable{}{}".format(ii, jj))
    return (session, retPubTables)


def invoke(fn, threadId, owner, sesName, args):
    try:
        worker = PubTableScale(threadId, owner, sesName)
        fn = getattr(worker, fn)
        return fn(*args)
    except Exception:
        traceback.print_exc()
        return None


class PubTableScale():
    def __init__(self, threadId, userName, sessionName):
        self.client = Client()
        self.threadId = threadId
        self.userName = userName
        self.sessionName = sessionName
        self.xcalarApi = XcalarApi()
        self.session = self.client.get_session(sessionName)
        self.session._reuse()
        self.xcalarApi.setSession(self.session)
        self.operators = Operators(self.xcalarApi)
        self.retina = Retina(self.xcalarApi)

    def verifyRowCount(self, srcTable, answerRows, printRows=False):
        output = self.operators.tableMeta(srcTable)
        srcRows = sum(
            [output.metas[ii].numRows for ii in range(output.numMetas)])
        printRows and print(ResultSet(self.client, table_name=srcTable).record_iterator())
        assert (srcRows == answerRows)

    def removeBatchId(self, srcTableName, dstTableName):
        output = self.operators.tableMeta(srcTableName)
        allValues = output.valueAttrs
        columnsToKeep = []
        for value in allValues:
            if value.name != "XcalarBatchId":
                columnsToKeep.append(value.name)
        self.operators.project(srcTableName, dstTableName, columnsToKeep)

    # Here are the test cases we want to exercise.
    # (A) Use a Table created by modeling from a Dataset to issue update to a
    #     Publish Table.
    # (B) Use a Table created by select on a Publish Table to issue update to a
    #     Publish Table.
    # (C) Use a Table created by running BDF with Dataset as origin to issue
    #     update to a Publish Table.
    # (D) Use a Table created by running BDF (made with self select on the
    #     Publish Table) with Publish Table as origin to issue update to a
    #     Publish Table.
    # (E) Use a Table created by running BDF with SrcTable as origin to issue
    #     update to a Publish Table.
    # (F) Use a Table created by running BDF (made with self select on the
    #     Publish Table) with SrcTable as origin to issue update to a Publish
    #     Table.

    def ptScale(self, args):
        ii = 0
        threadId = self.threadId
        logger.info("ptScale tid={} start".format(threadId))
        pt = "PubTable{}".format(threadId)
        rt = "RegTable{}".format(threadId)
        verbose = False
        dropTables = []

        # Setup a table that will be published
        self.operators.indexDataset(
            args.datasetName,
            "%s%d" % (rt, ii),
            "valDiv10",
            fatptrPrefixName="p")
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.operators.getRowNum("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                 "rowNum")
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.operators.project("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                               ["rowNum", "p-valDiv10"])
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                           ["int(1)"], ["XcalarOpCode"])
        dropTables.append("%s%d" % (rt, ii))
        ii = ii + 1
        self.operators.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                  "rowNum")
        self.verifyRowCount("%s%d" % (rt, ii), args.numDatasetRows, verbose)
        ii = ii + 1

        # (A) Use a Table created by modeling from a Dataset to issue update
        # to a Publish Table.
        for jj in range(args.totalPubTables):
            self.operators.publish("%s%d" % (rt, ii - 1), "%s%d" % (pt, jj))

        logger.info("ptScale tid={} after publish".format(threadId))
        # Prepare updates; issue updates
        srcTableRetina = False
        for kk in range(args.totalUpdates):
            updates = []
            dests = []
            tmpTables = []
            self.operators.map(
                "%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                ["int(add(rowNum," + str(NumDatasetRows / 2) + "))"],
                ["rowNum"])
            tmpTables.append("%s%d" % (rt, ii - 1))
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                               ["int(1)"], ["XcalarRankOver"])
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                               ["int(1)"], ["XcalarOpCode"])
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            retinaSrcTable = "%s%d" % (rt, ii - 1)
            retinaDstTable = "%s%d" % (rt, ii)
            self.operators.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                                      "rowNum")
            tmpTables.append("%s%d" % (rt, ii))
            ii = ii + 1

            for jj in range(args.totalPubTables):
                retName = "ret%d%d" % (threadId, jj)
                if srcTableRetina is False:
                    # (C) Use a Table created by running BDF with Dataset as
                    # origin to issue update to a Publish Table.
                    self.retina.make(
                        retName, [retinaDstTable],
                        [["rowNum", "XcalarRankOver", "XcalarOpCode"]], [])
                else:
                    # (E) Use a Table created by running BDF with SrcTable as
                    # origin to issue  update to a Publish Table.
                    self.retina.make(
                        retName, [retinaDstTable],
                        [["rowNum", "XcalarRankOver", "XcalarOpCode"]],
                        [retinaSrcTable])
                srcTableRetina = not srcTableRetina
                self.retina.execute(retName, [], "%s%d" % (rt, ii))
                self.xcalarApi.deleteQuery(retName, deferredDelete=False)
                self.retina.delete(retName)
                dests.append("%s%d" % (pt, jj))
                ii = ii + 1
                self.removeBatchId("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii))
                dropTables.append("%s%d" % (rt, ii - 1))
                updates.append("%s%d" % (rt, ii))
                if jj + 1 < args.totalPubTables:
                    tmpTables.append("%s%d" % (rt, ii))
                elif kk + 1 == args.totalUpdates:
                    dropTables.append("%s%d" % (rt, ii))
                ii = ii + 1
            self.operators.update(updates, dests)
            for dd in range(len(tmpTables)):
                self.operators.dropTable(tmpTables[dd])

        logger.info("ptScale tid={} after update".format(threadId))
        for dd in range(len(dropTables)):
            self.operators.dropTable(dropTables[dd])
        logger.info("ptScale tid={} after drop table".format(threadId))

        # Verify all batches of each publish table
        for jj in range(args.totalPubTables):
            for kk in range(args.totalUpdates):
                tmpTables = []
                self.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii), kk)
                tmpTables.append("%s%d" % (rt, ii))
                self.verifyRowCount("%s%d" % (rt, ii),
                                    NumDatasetRows + kk * (NumDatasetRows / 2),
                                    verbose)
                ii = ii + 1
                # Add range selects here to iterate through all ranges
                for nn in range(0, kk + 1):
                    self.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii),
                                          nn, 0)
                    tmpTables.append("%s%d" % (rt, ii))
                    self.verifyRowCount(
                        "%s%d" % (rt, ii),
                        NumDatasetRows + nn * (NumDatasetRows / 2), verbose)
                    ii = ii + 1
                for dd in range(len(tmpTables)):
                    self.operators.dropTable(tmpTables[dd])

        logger.info("ptScale tid={} after verify".format(threadId))
        # Prepare deletes; issue deletes; verify publish tables
        updates = []
        dests = []
        dropTables = []
        curRowCount = 10
        option = 0
        for jj in range(args.totalPubTables):
            self.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii))
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            mapStr = "int(if(le(rowNum, %d), 1, 0))" % curRowCount
            self.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                               [mapStr], ["XcalarOpCode"])
            retinaSrcTable = "%s%d" % (rt, ii - 1)
            retinaDstTable = "%s%d" % (rt, ii)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1

            # Create dependencies between different publish tables
            for kk in range(args.totalPubTables):
                if kk >= jj:
                    # Avoid cyclical dependencies; Create self dependencies.
                    if option % 3 == 0:
                        # (B) Use a Table created by select on a Publish Table
                        #  to issue update to a Publish Table.
                        # NOOP
                        pass
                    elif option % 3 == 1:
                        # (D) Use a Table created by running BDF (made with self
                        # select on the Publish Table) with Publish Table as
                        # origin to issue update to a Publish Table.
                        retName = "ret%d%d" % (threadId, jj)
                        self.retina.make(
                            retName, [retinaDstTable],
                            [["rowNum", "XcalarRankOver", "XcalarOpCode"]], [])
                        self.retina.execute(retName, [], "%s%d" % (rt, ii))
                        self.xcalarApi.deleteQuery(
                            retName, deferredDelete=False)
                        self.retina.delete(retName)
                        dropTables.append("%s%d" % (rt, ii))
                        ii = ii + 1
                    else:
                        # (F) Use a Table created by running BDF (made with self
                        # select on the Publish Table) with SrcTable as origin
                        # to issue update to a Publish Table.
                        retName = "ret%d%d" % (threadId, jj)
                        self.retina.make(
                            retName, [retinaDstTable],
                            [["rowNum", "XcalarRankOver", "XcalarOpCode"]],
                            [retinaSrcTable])
                        self.retina.execute(retName, [], "%s%d" % (rt, ii))
                        self.xcalarApi.deleteQuery(
                            retName, deferredDelete=False)
                        self.retina.delete(retName)
                        dropTables.append("%s%d" % (rt, ii))
                        ii = ii + 1
                    option = option + 1
                    self.removeBatchId("%s%d" % (rt, ii - 1),
                                       "%s%d" % (rt, ii))
                    dropTables.append("%s%d" % (rt, ii))
                    updates.append("%s%d" % (rt, ii))
                    dests.append("%s%d" % (pt, kk))
                    ii = ii + 1
        logger.info("ptScale tid={} after delete".format(threadId))

        self.operators.update(updates, dests)
        for dd in range(len(dropTables)):
            self.operators.dropTable(dropTables[dd])
        dropTables = []
        for jj in range(args.totalPubTables):
            self.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii))
            self.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
        for dd in range(len(dropTables)):
            self.operators.dropTable(dropTables[dd])
        logger.info("ptScale tid={} after drop".format(threadId))

        # Do inactivate; restore
        for jj in range(args.totalPubTables):
            self.operators.unpublish("%s%d" % (pt, jj), True)
            self.operators.restorePublishedTable("%s%d" % (pt, jj))

        logger.info("ptScale tid={} restore".format(threadId))
        # Issue coalesce; verify publish tables
        dropTables = []
        for jj in range(args.totalPubTables):
            self.operators.coalesce("%s%d" % (pt, jj))
            self.operators.select("%s%d" % (pt, jj), "%s%d" % (rt, ii))
            self.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            retinaTable = "%s%d" % (rt, ii)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            retName = "ret%d%d" % (threadId, jj)
            self.retina.make(retName, [retinaTable], ["rowNum"], [])
            self.retina.execute(retName, [], "%s%d" % (rt, ii))
            self.xcalarApi.deleteQuery(retName, deferredDelete=False)
            self.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.retina.delete(retName)
        logger.info("ptScale tid={} after coalesce".format(threadId))
        for dd in range(len(dropTables)):
            self.operators.dropTable(dropTables[dd])

        # Txn clenout
        for jj in range(args.totalPubTables):
            dropTables = []
            pubTab = "%s%d" % (pt, jj)

            # create an update with all deletes
            self.operators.select(pubTab, "%s%d" % (rt, ii))
            self.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            self.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                               ["int(0)"], ["XcalarOpCode"])
            goodUpdate = "%s%d" % (rt, ii)
            dropTables.append(goodUpdate)
            ii = ii + 1

            # create an update with all deletes except an invalid op code
            self.operators.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
                               ["ifInt(eq(rowNum, 1), div(1,0), 0)"],
                               ["XcalarOpCode"])
            ii = ii + 1
            self.removeBatchId("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii))
            dropTables.append("%s%d" % (rt, ii - 1))
            badUpdate = "%s%d" % (rt, ii)
            dropTables.append(badUpdate)
            ii = ii + 1

            try:
                # do a good update on a table and a bad update in the same api
                # the bad update should invoke txn recovery and rollback
                # everything
                self.operators.update([goodUpdate, badUpdate],
                                      [pubTab, pubTab])
            except XcalarApiStatusException:
                pass

            # none of the deletes should have succeeded
            self.operators.select(pubTab, "%s%d" % (rt, ii))
            self.verifyRowCount("%s%d" % (rt, ii), curRowCount, verbose)
            dropTables.append("%s%d" % (rt, ii))
            ii = ii + 1
            for dd in range(len(dropTables)):
                self.operators.dropTable(dropTables[dd])
        logger.info("ptScale tid={} exiting".format(threadId))
        return True


#
# retinaReplayTest() tests the deserialization and replay of a workbook which
# has two execute-retina dag nodes in the workbook's qgraph: both nodes refer to
# the same retina-name, but with modifications to the retina between executions.
# The deserialization and replay shouldn't trigger any crashes. See Xc-10832.
#
# This only tests make and execution of retina. Retinas aren't persisted in
# dataflow 2.0, so they won't survive a cluster reboot.


def retinaReplayTest():
    client = Client()
    xcalarApi = XcalarApi()
    session = client.create_session("retinaReplayTest")
    xcalarApi.setSession(session)
    operators = Operators(xcalarApi)
    retinaObj = Retina(xcalarApi)
    udf = Udf(xcalarApi)
    path = str(NumDatasetRows)
    # Udf must be added to the test's workbook since replay's qgraph reference
    # to the UDF's name when replaying the dataset load, must resolve within
    # the same workbook container
    udf.addOrUpdate(memUdfName, memUdfSource)
    dataset = UdfDataset(xcalarApi, memoryTargetName, path,
                         "retinaReplayTestDS", "{}:load".format(memUdfName))
    dataset.load()
    table = ["tabForRetinaReplayTest"]
    tableFromRetExec = ["tabFromRetinaExecute"]
    retina = ["retinaReplayTest"]
    operators.indexDataset(".XcalarDS.retinaReplayTestDS", table[0], "val",
                           "p")

    # make a retina from operations so far which created table[0]
    retinaObj.make(retina[0], [table[0]], ["a"], [])

    # retina execution below creates a exec-retina dag node in workbook qgraph
    retinaObj.execute(retina[0], [], tableFromRetExec[0], queryName=retina[0])
    xcalarApi.deleteQuery(retina[0], deferredDelete=False)

    # Now get the created retina in Python dictionary form so it can be modified
    retPyDict = retinaObj.getDict(retina[0])

    # Now parameterize the load path replacing 'users' with '<file>'
    pelems = path.split('/')
    pelems[-1] = '<numRows>'
    paramPath = '/'.join(pelems)

    newQuery = {}
    newOpsList = []
    for opRec in retPyDict['query']:
        if opRec['operation'] == 'XcalarApiBulkLoad':
            opRec['args']['loadArgs']['sourceArgsList'][0]['path'] = paramPath
        newOpsList.append(opRec)
    newQuery['query'] = newOpsList

    # update retina with the modification to Load path
    retinaObj.update(retina[0], newQuery)

    # Execute updated retina, creating another exec-retina node in qgraph
    # Use 'reviews' for file name
    newTableFromRet = ["newTabFromRetinaWParams"]
    retinaObj.execute(
        retina[0], [{
            "paramName": "numRows",
            "paramValue": "101"
        }],
        newTableFromRet[0],
        queryName=retina[0])
    xcalarApi.deleteQuery(retina[0], deferredDelete=False)

    # Return the workbook - deserialization and replay of this workbook should
    # not crash due to the fact that it has two exec-retina nodes in the
    # workbook's qgraph, both referring to the same retina name, but with
    # a modification to the retina (e.g. second node has new param)

    return (session, retina[0])


#
# Sample test to show how multiple replay tests are invoked by testAllReplay()
# method below, and how each test needs to be written:
# 0. The test must take a xcalarApi param, used to create a workbook for test
# 1. The workbook must be created with owner as "TestReplay", but with any name
# 2. The test must return a tuple containing 1, or 2 elements
#     - element 0: instance of the workbook created
#     - element 1: name of a retina that may have been created by the test
#    The tuple MUST contain at least element 0, since this is a workbook replay
#    test, and the workbook must be returned so it can be replayed by the infra.
#    Element 1, the retina, must be returned if the test created any retina,
#    since it needs to be deleted by the infra at the end.
#
def sampleReplayTest():
    client = Client()
    wkbookNretina = ()
    xcalarApi = XcalarApi()
    session = client.create_workbook("sampleReplayTest")
    wkbookNretina = (session, )
    # if test creates a retina, add its name to above tuple too
    return (wkbookNretina)


def testAllReplay():
    wkbookList = []
    publishedTableList = []

    #
    # For first invocation of Xcalar cluster, execute all the replay tests.
    # Each test would return a workbook, and possibly a retina. Add them to
    # a list of workbooks, and retinas. Exit from the "with" clause will kill
    # the cluster, leaving the persisted workbooks and retinas in Xcalar root.
    #
    print("Before Replay")
    with DevCluster() as cluster:
        xcalarApi = XcalarApi()
        target = Target2(xcalarApi)

        for targetName, targetType, targetArgs in targets:
            target.add(targetType, targetName, targetArgs)

        print(" Run retinaReplayTest")
        workbook, _ = retinaReplayTest()
        wkbookList.append(workbook)

        print(" Run publishedTableReplayScale")
        workbook, publishedTables = publishedTableReplayScale()
        wkbookList.append(workbook)
        for pt in publishedTables:
            publishedTableList.append(pt)

        #
        # Sample invocation for the next test. Add new replay tests similarly.
        # This is all that's needed when adding new tests. The exit from this
        # clause will kill the cluster, and the second "with" clause below will
        # trigger re-launch of Xcalar and then a replay of each of the workbooks
        # returned by these tests here.
        #
        # workbook, retina = sampleReplayTest()
        # wkbookList.append(workbook)
        # retinaList.append(retina)

    #
    # Second for loop below will destroy any dataflows created and returned by
    # the tests from above - without this step, dataflows will be left in Xcalar
    # root, causing re-runs of this test to fail.
    #
    print("Kickoff Replay")
    with DevCluster() as cluster:
        client = Client()
        xcalarApi = XcalarApi()
        wkbook = Session(
            xcalarApi,
            owner=None,
            reuseExistingSession=True,
            username=TestUserName,
            sessionName=TestSessionName)
        xcalarApi.setSession(wkbook)
        wkbook.activate()
        op = Operators(xcalarApi)
        print(" Restore publish tables")
        for pt in publishedTableList:
            op.restorePublishedTable(pt)
        print(" Unpublish publish tables")
        for pt in publishedTableList:
            op.unpublish(pt)

        print(" Do session replay")
        for wkbook in wkbookList:
            client.destroy_session(wkbook.name)
