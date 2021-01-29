import sys

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.LegacyApi.Dataset import Dataset
from xcalar.compute.coretypes.Status.ttypes import StatusT

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'flight demo test',
    'Description': 'test flight demo',
    'ExpectedRunTime': 60,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'datasetSize': '2GB',
    'SingleThreaded': False,
    'ExclusiveSession': False,
}

globalPrepQuery = None

mainQuery = """
index --key \"xcalarRecordNum\" --dataset \".XcalarDS.xxx.flight5205\" --dsttable \"flight5205#bt0\" --prefix pairlines;
index --key \"xcalarRecordNum\" --dataset \".XcalarDS.xxx.flight.airport3614\" --dsttable \"airport3614#bt1\" --prefix pairports;
map --eval \"int(pairlines::ArrDelay)\" --srctable \"flight5205#bt0\" --fieldName \"ArrDelay_integer\" --dsttable \"flight5205#bt2\";
map --eval \"genUnique()\" --srctable \"flight5205#bt2\" --fieldName \"uniqueNum\" --dsttable \"flight5205#bt3\";
filter --srctable flight5205#bt3 --eval \"gt(ArrDelay_integer, 0)\" --dsttable \"flight5205#bt4\";
map --eval \"ymd2:ymd(pairlines::Year, pairlines::Month, pairlines::DayofMonth)\" --srctable \"flight5205#bt4\" --fieldName \"YearMonthDay\" --dsttable \"flight5205#bt5\";
index --key \"pairlines::Dest\" --srctable \"flight5205#bt5\" --dsttable \"flight5205.index#bt7\";
index --key \"pairports::iata\" --srctable \"airport3614#bt1\" --dsttable \"airport3614.index#bt8\";
join --leftTable \"flight5205.index#bt7\" --rightTable \"airport3614.index#bt8\" --joinType innerJoin --joinTable \"flight5205-airport3614#bt6\";
index --key \"pairlines::UniqueCarrier\" --srctable \"flight5205-airport3614#bt6\" --dsttable \"flight5205-airport3614.index#bt9\";
groupBy --srctable \"flight5205-airport3614.index#bt9\" --eval \"avg(ArrDelay_integer)\" --fieldName AvgDelay --dsttable \"flight5205-airport3614-GB#bt10\" --nosample;
"""

cleanupQuery = """
drop table flight*;
drop table airport*;
drop constant flight*;
"""

globalCleanupQuery = None

avgArrDelay = 31.2293

ymdFnModule = "ymd2"
ymdFnPath = "/netstore/udf/python/ymd.py"
ymdFn = "ymd"


def formatQuery(logging, TestCaseStatus, testSpec=None):
    global globalPrepQuery

    if testSpec is not None and 'datasetSize' in list(testSpec.keys()):
        datasetSize = testSpec['datasetSize']
    else:
        datasetSize = testCaseSpecifications['datasetSize']

    globalPrepQuery = """
    load --url \"nfs:///netstore/datasets/flight/airports.csv\" --format csv --size %s --name \"xxx.flight.airport3614\" --fielddelim , --crlf --header;
    load --url \"nfs:///netstore/datasets/flight/airlines\" --format csv --size %s --name \"xxx.flight5205\" --fielddelim , --crlf --header;
    """ % (datasetSize, datasetSize)

    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    output = xcInstance.xcalarApi.submitQuery(
        globalPrepQuery.replace('\n', ''),
        xcInstance.sessionName,
        "globalPrepareFlightDemoQuery",
        xcInstance.username,
        xcInstance.userIdUnique,
        isAsync=True)
    queryName = output.job_name

    logging.debug("username :%s, userId: %d" % (xcInstance.username,
                                                xcInstance.userIdUnique))
    logging.debug("queryName: %s" % queryName)

    xcInstance.xcalarApi.waitForQuery(
        queryName=queryName,
        userName=xcInstance.username,
        userIdUnique=xcInstance.userIdUnique,
        pollIntervalInSecs=xcInstance.pollIntervalInSecs)

    return TestCaseStatus.Pass


def prepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    try:
        xcInstance.xcalarApi.loadUdf(ymdFnModule, ymdFn, ymdFnPath)
    except XcalarApiStatusException as error:
        assert error.status == StatusT.StatusUdfModuleAlreadyExists

    queryName = None
    output = xcInstance.xcalarApi.submitQuery(
        mainQuery.replace('\n', ''),
        xcInstance.sessionName,
        "mainFlightDemoQuery",
        xcInstance.username,
        xcInstance.userIdUnique,
        isAsync=True)
    queryName = output.job_name

    logging.debug("username :%s, userId: %d" % (xcInstance.username,
                                                xcInstance.userIdUnique))
    logging.debug("queryName: %s" % queryName)

    xcInstance.xcalarApi.waitForQuery(
        queryName=queryName,
        userName=xcInstance.username,
        userIdUnique=xcInstance.userIdUnique,
        pollIntervalInSecs=xcInstance.pollIntervalInSecs)

    return TestCaseStatus.Pass


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):

    output = xcInstance.op.aggregate("flight5205-airport3614#bt6",
                                     "flight5205-airport3614-aggregate#bt11",
                                     "avg(ArrDelay_integer)")
    if output - avgArrDelay < 0.001:
        return TestCaseStatus.Pass

    return TestCaseStatus.Fail


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    output = xcInstance.xcalarApi.submitQuery(
        cleanupQuery.replace('\n', ''),
        xcInstance.sessionName,
        "cleanupFlightDemoQuery",
        xcInstance.username,
        xcInstance.userIdUnique,
        isAsync=True)
    queryName = output.job_name

    xcInstance.xcalarApi.waitForQuery(
        queryName=queryName,
        userName=xcInstance.username,
        userIdUnique=xcInstance.userIdUnique,
        pollIntervalInSecs=xcInstance.pollIntervalInSecs)

    return TestCaseStatus.Pass


def globalCleanUp(logging, xcInstance, TestCaseStatus):

    # Delist the datasets created by this test
    Dataset.bulkDelete(xcInstance.xcalarApi, "*flight*")

    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
