import sys

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.LegacyApi.Dataset import Dataset

from xcalar.compute.coretypes.Status.ttypes import StatusT

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'Multi join test',
    'Description': 'test multi join',
    'ExpectedRunTime': 60,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'datasetSize': '2GB',
    'SingleThreaded': False,
    'ExclusiveSession': False,
}

globalPrepQuery = None

mainQuery = """
index --key \"xcalarRecordNum\" --dataset \".XcalarDS.xxx.multiJoin5205\" --dsttable \"multiJoin5205#bt0\" --prefix pairlines;
map --eval \"int(pairlines::ArrDelay)\" --srctable \"multiJoin5205#bt0\" --fieldName \"ArrDelay_integer\" --dsttable \"multiJoin5205#bt2\";
map --eval \"genUnique()\" --srctable \"multiJoin5205#bt2\" --fieldName \"uniqueNum\" --dsttable \"multiJoin5205#bt3\";
filter --srctable multiJoin5205#bt3 --eval \"gt(ArrDelay_integer, 0)\" --dsttable \"multiJoin5205#bt4\";
map --eval \"ymd:ymd(pairlines::Year, pairlines::Month, pairlines::DayofMonth)\" --srctable \"multiJoin5205#bt4\" --fieldName \"YearMonthDay\" --dsttable \"multiJoin5205#bt5\";
index --key \"xcalarRecordNum\" --dataset \".XcalarDS.xxx.multiJoin.schedule589\" --dsttable \"multiJoinSchedule589#bt12\" --prefix pschedule;
map --eval \"default:multiJoin(pschedule::class_id, pschedule::teacher_id)\" --srctable \"multiJoinSchedule589#bt12\" --fieldName \"leftJoinCol88494\" --dsttable \"multiJoinSchedule589#bt14\";
map --eval \"default:multiJoin(pairlines::DayofMonth, pairlines::DayOfWeek)\" --srctable \"multiJoin5205#bt5\" --fieldName \"rightJoinCol83992\" --dsttable \"multiJoin5205#bt15\";
index --key \"leftJoinCol88494\" --srctable \"multiJoinSchedule589#bt14\" --dsttable \"multiJoinSchedule589.index#bt16\";
index --key \"rightJoinCol83992\" --srctable \"multiJoin5205#bt15\" --dsttable \"multiJoin5205.index#bt17\";
join --leftTable \"multiJoinSchedule589.index#bt16\" --rightTable \"multiJoin5205.index#bt17\" --joinType innerJoin --joinTable \"multiJoinSchedule589-multiJoin5205#bt13\";
map --eval \"string(pschedule::class_id)\" --srctable \"multiJoinSchedule589-multiJoin5205#bt13\" --fieldName \"newclassid_string\" --dsttable \"multiJoinNewTableName#bt18\";
map --eval \"int(pairlines::Month)\" --srctable \"multiJoinNewTableName#bt18\" --fieldName \"Month_integer\" --dsttable \"multiJoinNewTableName#bt19\";
index --key \"Month_integer\" --srctable \"multiJoinNewTableName#bt19\" --dsttable \"multiJoinNewTableName.index#bt26\";
groupBy --srctable \"multiJoinNewTableName.index#bt26\" --eval \"count(Month_integer)\" --fieldName statsGroupBy --dsttable \"multiJoinNewTableName.profile.GB27895#bt28\" --nosample;
index --key \"Month_integer\" --srctable \"multiJoinNewTableName.profile.GB27895#bt28\" --dsttable \"multiJoinNewTableName.profile.final21978#bt29\" --sorted;
drop table multiJoinNewTableName.index#bt26;
index --key \"statsGroupBy\" --srctable \"multiJoinNewTableName.profile.GB27895#bt28\" --dsttable \"multiJoinNewTableName.profile.final21978.asc#bt32\" --sorted;
"""

cleanupQuery = """
drop table multiJoin*;
drop constant multiJoin*;
"""

globalCleanupQuery = None

minMonthInt = 1
avgMonthInt = 6.506
maxMonthInt = 12
sumMonthInt = 12708
sdMonthInt = 3.711

avgTeacherId = 2.342
avgArrDelay = 29.767

joinTotalRow = countMonthInt = 1953

defaultModule = "default"
multiJoinFnPath = "/netstore/qa/udfs/default.py"
multiJoinFn = "multiJoin"

ymdFnModule = "ymd"
ymdFnPath = "/netstore/udf/python/ymd.py"
ymdFn = "ymd"


def formatQuery(logging, TestCaseStatus, testSpec=None):

    global globalPrepQuery

    if testSpec is not None and 'datasetSize' in list(testSpec.keys()):
        datasetSize = testSpec['datasetSize']
    else:
        datasetSize = testCaseSpecifications['datasetSize']

    globalPrepQuery = """
    load --url \"nfs:///netstore/datasets/flight/airlines/\" --format csv --size %s --name \"xxx.multiJoin5205\" --fielddelim , --crlf --header;
    load --url \"nfs:///netstore/datasets/indexJoin/schedule/schedule.json\" --format json --size %s --name \"xxx.multiJoin.schedule589\";
    """ % (datasetSize, datasetSize)

    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    found = False
    output = xcInstance.xcalarApi.submitQuery(
        globalPrepQuery.replace('\n', ''),
        xcInstance.sessionName,
        "globalPrepareMultiJoinQuery",
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


def prepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    try:
        xcInstance.xcalarApi.loadUdf(ymdFnModule, ymdFn, ymdFnPath,
                                     xcInstance.username,
                                     xcInstance.userIdUnique)
    except XcalarApiStatusException as error:
        assert error.status == StatusT.StatusUdfModuleAlreadyExists
    try:
        xcInstance.xcalarApi.loadUdf(defaultModule, multiJoinFn,
                                     multiJoinFnPath, xcInstance.username,
                                     xcInstance.userIdUnique)
    except XcalarApiStatusException as error:
        assert error.status == StatusT.StatusUdfModuleAlreadyExists

    queryName = None
    output = xcInstance.xcalarApi.submitQuery(
        mainQuery.replace('\n', ''),
        xcInstance.sessionName,
        "mainMultiJoinQuery",
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
    output = xcInstance.op.tableMeta("multiJoinSchedule589-multiJoin5205#bt13")
    totalRow = 0
    for ii in range(output.numMetas):
        totalRow += output.metas[ii].numRows

    logging.debug("TotalRow: %d" % totalRow)
    if totalRow != joinTotalRow:
        logging.error("join failed, totalRow:%d, expected row:%d" %
                      (totalRow, joinTotalRow))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt20",
                                     "min(Month_integer)")

    logging.debug("min(Month_integer): %d" % output)
    if output != minMonthInt:
        logging.error(
            "min(Month_integer): %d. Expected: %d" % (output, minMonthInt))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt21",
                                     "avg(Month_integer)")

    logging.debug("avg(Month_integer): %f", output)
    if abs(output - avgMonthInt) > 0.001:
        logging.error(
            "avg(Month_integer): %f. Expected: %f" % (output, avgMonthInt))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt22",
                                     "max(Month_integer)")

    logging.debug("max(Month_integer): %d" % output)
    if output != maxMonthInt:
        logging.error(
            "max(Month_integer): %d. Expected: %d" % (output, maxMonthInt))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt23",
                                     "count(Month_integer)")

    logging.debug("count(Month_integer): %d" % output)
    if output != countMonthInt:
        logging.error(
            "count(Month_integer): %d. Expected: %d" % (output, countMonthInt))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt24",
                                     "sum(Month_integer)")

    logging.debug("sum(Month_integer): %d" % output)
    if output != sumMonthInt:
        logging.error(
            "sum(Month_integer): %d. Expected: %d" % (output, sumMonthInt))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate(
        "multiJoinNewTableName#bt19", "multiJoinNewTableName#bt25",
        "sqrt(div(sum(pow(sub(Month_integer, avg(Month_integer)), 2)), 1953))")

    logging.debug("sd(Month_integer): %f" % output)
    if abs(output - sdMonthInt) > 0.001:
        logging.error(
            "sd(Month_integer): %f. Expected: %f" % (output, sdMonthInt))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt26",
                                     "avg(pschedule::teacher_id)")

    logging.debug("avg(teacher_id): %f" % output)
    if abs(output - avgTeacherId) > 0.001:
        logging.error(
            "avg(teacher_id): %f. Expected: %f" % (output, avgTeacherId))
        return TestCaseStatus.Fail

    output = xcInstance.op.aggregate("multiJoinNewTableName#bt19",
                                     "multiJoinNewTableName#bt28",
                                     "avg(ArrDelay_integer)")

    logging.debug("avg(ArrDelay_integer): %f" % output)
    if abs(output - avgArrDelay) > 0.001:
        logging.error(
            "avg(ArrDelay_integer): %f. Expected: %f" % (output, avgArrDelay))
        return TestCaseStatus.Fail

    return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    output = xcInstance.xcalarApi.submitQuery(
        cleanupQuery.replace('\n', ''),
        xcInstance.sessionName,
        "cleanupMultiJoinQuery",
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

    # Delist datasets created by this test
    Dataset.bulkDelete(xcInstance.xcalarApi, "*multiJoin*")
    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
