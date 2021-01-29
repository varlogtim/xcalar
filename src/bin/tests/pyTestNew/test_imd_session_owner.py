import pytest
import json
import time
from crontab import CronTab

from xcalar.compute.util.Qa import DefaultTargetName
from xcalar.compute.util.cluster import DevCluster, detect_cluster
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.app import App
from xcalar.external.LegacyApi.Dataset import UdfDataset
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.util.config import detect_config

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own clusters")
NumDatasetRows = 1000
MemUdfName = "TestImdReplay"
MemUdfSource = """
import json
def load(fullPath, inStream):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {"val": ii, "valDiv10": ii//10, "constant": 1}
        yield rowDict
    """

MemoryTargetName = "QA test_operators memory"
targets = [
    (MemoryTargetName, "memory", {}),
]

DatasetName = ".XcalarDS." + "memDS"


def ptCreate(xcalarApi, pt):
    op = Operators(xcalarApi)
    rt = "RegTable"
    ii = 0
    op.indexDataset(
        DatasetName, "%s%d" % (rt, ii), "valDiv10", fatptrPrefixName="p")

    ii = ii + 1
    op.getRowNum("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), "rowNum")

    ii = ii + 1
    op.project("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
               ["rowNum", "p-valDiv10"])

    ii = ii + 1
    op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["string(rowNum)"],
           ["rowNumStr"])

    ii = ii + 1
    op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["int(1)"],
           ["XcalarOpCode"])

    ii = ii + 1
    op.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), "rowNum")

    op.publish("%s%d" % (rt, ii), pt)

    # Cleanup tables
    op.dropTable("*")


def ptUpdate(xcalarApi, pt, numUpdates):
    op = Operators(xcalarApi)
    rt = "RegTable"
    ii = 0
    op.dropTable("*")
    op.indexDataset(
        DatasetName, "%s%d" % (rt, ii), "valDiv10", fatptrPrefixName="p")

    updates = []
    dests = []
    for jj in range(numUpdates):
        ii = ii + 1
        op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
               ["int(add(rowNum," + str(NumDatasetRows / 2) + "))"],
               ["rowNum"])

        ii = ii + 1
        op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["int(1)"],
               ["XcalarRankOver"])

        ii = ii + 1
        op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["int(1)"],
               ["XcalarOpCode"])

        ii = ii + 1
        op.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), "rowNum")

        updates.append("%s%d" % (rt, ii))
        dests.append(pt)

    op.update(updates, dests)

    # Cleanup tables
    op.dropTable("*")


def ptSelect(xcalarApi, pt, drop):
    op = Operators(xcalarApi)
    rt = "RegTable"
    ii = 0
    op.dropTable("*")
    tName = "%s%d" % (rt, ii)
    op.select(pt, tName)

    # Cleanup tables
    if drop:
        op.dropTable("*")
        return ""
    else:
        return tName


def ptInactivate(pt):
    xcalarApi = XcalarApi()
    op = Operators(xcalarApi)
    op.unpublish(pt, True)


def ptDelete(pt):
    xcalarApi = XcalarApi()
    op = Operators(xcalarApi)
    op.unpublish(pt)


def ptRestore(xcalarApi, pt):
    op = Operators(xcalarApi)
    op.restorePublishedTable(pt)


def ptChangeOwnership(pt, sessionName, userIdName):
    xcalarApi = XcalarApi()
    op = Operators(xcalarApi)
    op.publishTableChangeOwner(pt, sessionName, userIdName)


def ptCoalesce(pt):
    xcalarApi = XcalarApi()
    op = Operators(xcalarApi)
    op.coalesce(pt)


def testImdSessionOwner():
    publishedTableList = []
    wkbookList = []
    NumUpdates = 3
    updateCount = 0
    oldestBatchId = 0
    nextBatchId = 0
    KeyName = "rowNum"
    NumKeys = 1
    NumValues = 6

    with DevCluster() as cluster:
        client = Client()
        xcalarApi = XcalarApi()
        target = Target2(xcalarApi)
        client = Client()

        for targetName, targetType, targetArgs in targets:
            target.add(targetType, targetName, targetArgs)

        # Create Session "PublishedTableReplay_A"
        xcalarApi_A = XcalarApi()
        SesName_A = "PublishedTableReplay_A"
        pt_A = "PubTable_A"
        wkbook_A = client.create_workbook(SesName_A)
        wkbook_A.activate()
        xcalarApi_A.setSession(wkbook_A)
        wkbookList.append(wkbook_A)
        wkbook_A._create_or_update_udf_module(MemUdfName, MemUdfSource)

        # load dataset
        path = str(NumDatasetRows)
        dataset = UdfDataset(xcalarApi_A, MemoryTargetName, path, DatasetName,
                             "{}:load".format(MemUdfName))
        dataset.load()

        # Create Session "PublishedTableReplay_B"
        xcalarApi_B = XcalarApi()
        SesName_B = "PublishedTableReplay_B"
        pt_B = "PubTable_B"
        wkbook_B = client.create_workbook(SesName_B)
        wkbook_B.activate()
        xcalarApi_B.setSession(wkbook_B)
        wkbookList.append(wkbook_B)
        wkbook_B._create_or_update_udf_module(MemUdfName, MemUdfSource)

        # Create "PubTable_A" from Session "PublishedTableReplay_A"
        ptCreate(xcalarApi_A, pt_A)
        publishedTableList.append(pt_A)
        ptUpdate(xcalarApi_A, pt_A, NumUpdates)
        ptSelect(xcalarApi_A, pt_A, True)

        # Create "PubTable_B" from Session "PublishedTableReplay_B"
        ptCreate(xcalarApi_B, pt_B)
        publishedTableList.append(pt_B)
        ptUpdate(xcalarApi_B, pt_B, NumUpdates)
        ptSelect(xcalarApi_B, pt_B, True)

        # Session "PublishedTableReplay_A" should not be able to update "PubTable_B".
        try:
            ptUpdate(xcalarApi_A, pt_B, NumUpdates)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusPTUpdatePermDenied

        # Session "PublishedTableReplay_B" should not be able to update "PubTable_A".
        try:
            ptUpdate(xcalarApi_B, pt_A, NumUpdates)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusPTUpdatePermDenied

        # Session "PublishedTableReplay_A" be able to select "PubTable_B".
        ptSelect(xcalarApi_A, pt_B, True)

        # Session "PublishedTableReplay_B" be able to select "PubTable_A".
        ptSelect(xcalarApi_B, pt_A, True)

        # Validate list output
        updateCount += NumUpdates
        nextBatchId += (updateCount + 1)
        op = Operators(xcalarApi_A)
        listOut = op.listPublishedTables("*")
        for table in listOut.tables:
            assert (table.active is True)
            assert (table.restoring is False)
            assert (table.numPersistedUpdates == updateCount + 1)
            assert (table.oldestBatchId == oldestBatchId)
            assert (table.nextBatchId == nextBatchId)
            if table.name == pt_A:
                assert (table.sessionName == SesName_A)
            else:
                assert (table.name == pt_B)
                assert (table.sessionName == SesName_B)
            assert (table.keys[0].name == KeyName)
            assert (len(table.keys) == NumKeys)
            assert (len(table.values) == NumValues)

        # Inactivate PTs. Note that session owner does not matter for inactivation.
        ptInactivate(pt_B)
        ptInactivate(pt_A)

        # Validate list output after inactivate
        op = Operators(xcalarApi_A)
        listOut = op.listPublishedTables("*")
        for table in listOut.tables:
            print("listOut.tables {}".format(table))
            assert (table.active is False)
            assert (table.restoring is False)
            assert (table.numPersistedUpdates == updateCount + 1)
            assert (table.keys[0].name == KeyName)
            assert (len(table.keys) == NumKeys)
            assert (len(table.values) == NumValues)

        # Swap PT session ownership. Then do some updates
        ptRestore(xcalarApi_B, pt_B)
        ptChangeOwnership(pt_B, SesName_A, client.username)
        ptUpdate(xcalarApi_A, pt_B, NumUpdates)
        ptCoalesce(pt_B)
        ptRestore(xcalarApi_A, pt_A)
        ptChangeOwnership(pt_A, SesName_B, client.username)
        ptUpdate(xcalarApi_B, pt_A, NumUpdates)
        ptCoalesce(pt_A)
        updateCount += NumUpdates
        oldestBatchId = updateCount
        nextBatchId = oldestBatchId + 1

        # Session "PublishedTableReplay_A" be able to select "PubTable_A".
        ptSelect(xcalarApi_A, pt_A, True)

        # Session "PublishedTableReplay_B" be able to select "PubTable_B".
        ptSelect(xcalarApi_B, pt_B, True)

        # Session "PublishedTableReplay_A" should not be able to update "PubTable_A".
        try:
            ptUpdate(xcalarApi_A, pt_A, NumUpdates)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusPTUpdatePermDenied

        # Session "PublishedTableReplay_B" should not be able to update "PubTable_B".
        try:
            ptUpdate(xcalarApi_B, pt_B, NumUpdates)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusPTUpdatePermDenied

        # Make "PubTable_A" orphan by deleting Session "PublishedTableReplay_B"
        wkbook_B.delete()

        # Now let Session "PublishedTableReplay_A" take over "PubTable_A"
        ptChangeOwnership(pt_A, SesName_A, client.username)
        ptUpdate(xcalarApi_A, pt_A, NumUpdates)
        ptUpdate(xcalarApi_A, pt_B, NumUpdates)
        ptCoalesce(pt_A)
        ptCoalesce(pt_B)
        updateCount += NumUpdates
        oldestBatchId = updateCount
        nextBatchId = oldestBatchId + 1

        # Validate list output before delete
        op = Operators(xcalarApi_A)
        listOut = op.listPublishedTables("*")
        for table in listOut.tables:
            assert (table.active is True)
            assert (table.restoring is False)
            assert (table.numPersistedUpdates == updateCount + 1)
            assert (table.oldestBatchId == oldestBatchId)
            assert (table.nextBatchId == nextBatchId)
            if table.name == pt_A:
                assert (table.sessionName == SesName_A)
            else:
                assert (table.name == pt_B)
                assert (table.sessionName == SesName_A)
            assert (table.keys[0].name == KeyName)
            assert (len(table.keys) == NumKeys)
            assert (len(table.values) == NumValues)

        # Delete both PTs.
        ptDelete(pt_A)
        ptDelete(pt_B)

        # Delete Session "PublishedTableReplay_A"
        wkbook_A.delete()
