import sys

from xcalar.external.LegacyApi.Dataset import Dataset

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'filter test',
    'Description': 'test filter',
    'ExpectedRunTime': 600,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'datasetSize': '100MB',
    'SingleThreaded': False,
    'ExclusiveSession': False,
}

globalPrepQuery = None

mainQuery = """
    index --key step --dataset .XcalarDS.filter.logData --dsttable "logData/table1" --prefix plogdata;
    filter --srctable "logData/table1" --eval "lt(plogdata::step, 10)" --dsttable "logData/table2";
    map --srctable "logData/table1" --eval "div(plogdata::step, 2)" --dsttable "logData/table3" --fieldName step2;
    filter --srctable "logData/table3" --eval "lt(step2, 10)" --dsttable "logData/table4";
    """

cleanupQuery = """
    drop table "logData/*";
    """

globalCleanupQuery = None


def formatQuery(logging, TestCaseStatus, testSpec=None):
    global globalPrepQuery

    if testSpec is not None and 'datasetSize' in list(testSpec.keys()):
        datasetSize = testSpec['datasetSize']
    else:
        datasetSize = testCaseSpecifications['datasetSize']

    globalPrepQuery = """
    load --url "nfs:///netstore/datasets/lg_generated/lgLog.json" --format json --size %s --name "filter.logData";
    """ % datasetSize


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    output = xcInstance.xcalarApi.submitQuery(
        globalPrepQuery.replace('\n', ''),
        xcInstance.sessionName,
        "globalPrepareFilterQuery",
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


def prepare(logging, xcInstance, TestCaseStatus):
    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    print(("username :%s, userId: %d" % (xcInstance.username,
                                         xcInstance.userIdUnique)))

    output = xcInstance.xcalarApi.submitQuery(
        mainQuery.replace('\n', ''),
        xcInstance.sessionName,
        "mainFilterQuery",
        xcInstance.username,
        xcInstance.userIdUnique,
        isAsync=True)
    queryName = output.job_name

    print(("queryName:" + queryName))

    xcInstance.xcalarApi.waitForQuery(
        queryName=queryName,
        userName=xcInstance.username,
        userIdUnique=xcInstance.userIdUnique,
        pollIntervalInSecs=xcInstance.pollIntervalInSecs)

    return TestCaseStatus.Pass


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):

    output = xcInstance.op.tableMeta("logData/table2")

    totalCount = 0
    for ii in range(0, output.numMetas):
        totalCount += output.metas[ii].numRows

    expectedCount = 9
    if totalCount != expectedCount:
        logging.error(
            "total Count = %d, expected %d" % (totalCount, expectedCount))
        return TestCaseStatus.Fail

    output = xcInstance.op.tableMeta("logData/table4")

    totalCount = 0
    for ii in range(0, output.numMetas):
        totalCount += output.metas[ii].numRows

    expectedCount = 19
    if totalCount != expectedCount:
        logging.error(
            "total Count = %d, expected %d" % (totalCount, expectedCount))
        return TestCaseStatus.Fail

    return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    output = xcInstance.xcalarApi.submitQuery(
        cleanupQuery.replace('\n', ''),
        xcInstance.sessionName,
        "cleanupFilterQuery",
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
    Dataset.bulkDelete(xcInstance.xcalarApi, "*filter*")

    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
