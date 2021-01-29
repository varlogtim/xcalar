import pytest
#import json
#import time
#from crontab import CronTab

from xcalar.compute.util.cluster import DevCluster
#from xcalar.external.LegacyApi.Session import Session
#from xcalar.external.app import App
from xcalar.external.LegacyApi.Dataset import UdfDataset
from xcalar.external.LegacyApi.Operators import Operators
#from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
#from xcalar.external.LegacyApi.KVStore import KVStore
#from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
#from xcalar.compute.coretypes.Status.ttypes import StatusT

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


def ptCreate(xcalarApi, rt):
    op = Operators(xcalarApi)
    ii = 1
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
    return ii

def ptDropAllTable(xcalarApi):
    op = Operators(xcalarApi)
    op.dropTable("*")


def testGetMemoryUsage():
    with DevCluster() as cluster:
        client = Client()
        xcalarApi = XcalarApi()
        target = Target2(xcalarApi)
        client = Client()

        for targetName, targetType, targetArgs in targets:
            target.add(targetType, targetName, targetArgs)

        # Create Session "Session_A"
        xcalarApi_A = XcalarApi()
        sesName_A = "Session_A"
        rt_A = "RegTable_A"
        wkbook_A = client.create_workbook(sesName_A)
        wkbook_A.activate()
        xcalarApi_A.setSession(wkbook_A)
        wkbook_A._create_or_update_udf_module(MemUdfName, MemUdfSource)

        # load dataset
        path = str(NumDatasetRows)
        dataset = UdfDataset(xcalarApi_A, MemoryTargetName, path, DatasetName,
                             "{}:load".format(MemUdfName))
        dataset.load()

        # Create "RegTable_A" from Session "Session_A"
        count = ptCreate(xcalarApi_A, rt_A)

        # Get memory usage to verify one session is created
        response = client.get_memory_usage(client.username)
        print(response)
        assert(response.user_memory.scope.workbook.name.username == "admin")
        assert(len(response.user_memory.session_memories) == 1)
        assert(response.user_memory.session_memories[0].session_name == sesName_A)
        assert(len(response.user_memory.session_memories[0].table_memories) == count)
        for table_memory in response.user_memory.session_memories[0].table_memories:
            assert(table_memory.table_name.startswith(rt_A))

        # Delete all tables.
        ptDropAllTable(xcalarApi_A)

        # Delete Session "Session_A"
        wkbook_A.delete()
