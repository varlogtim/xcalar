import sys

from xcalar.external.LegacyApi.Dataset import Dataset

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'Simple Join Test',
    'Description': 'Simple Join test',
    'ExpectedRunTime': 600,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'datasetSize': '2GB',
    'SingleThreaded': False,
    'ExclusiveSession': False,
}

# If NumRuns is set to 1, feel free to leave globalPrepQuery blank.
# This might be useful if you just want to copy and paste a known bug into mainQuery
# globalPrepQuery is only ever invoked once per user

# This gets invoked as many times as NumRuns specified in testCaseSpecifications
mainQuery = """
index --key user_id --dataset .XcalarDS.simpleJoin-basicYelpUser --dsttable simpleJoinTest/table0 --prefix pyelpuser;
filter --srctable simpleJoinTest/table0 --eval \"eq(pyelpuser::name,'Steve')\" --dsttable simpleJoinTest/table1;
index --key user_id --dataset .XcalarDS.simpleJoin-basicYelpReviews --dsttable simpleJoinTest/table2 --prefix pyelpreviews;
join --leftTable simpleJoinTest/table2 --rightTable simpleJoinTest/table1 --joinType innerJoin --joinTable simpleJoinTest/joinTableInner;
join --leftTable simpleJoinTest/table2 --rightTable simpleJoinTest/table1 --joinType leftJoin --joinTable simpleJoinTest/joinTableLeft;
join --leftTable simpleJoinTest/table2 --rightTable simpleJoinTest/table1 --joinType rightJoin --joinTable simpleJoinTest/joinTableRight;
join --leftTable simpleJoinTest/table2 --rightTable simpleJoinTest/table1 --joinType fullOuterJoin --joinTable simpleJoinTest/joinTableOuter;
"""

cleanupQuery = """
drop table simpleJoinTest/table0;
drop table simpleJoinTest/table1;
drop table simpleJoinTest/table2;
drop table simpleJoinTest/joinTableInner;
drop table simpleJoinTest/joinTableLeft;
drop table simpleJoinTest/joinTableRight;
drop table simpleJoinTest/joinTableOuter;
"""

globalCleanUpQuery = None


def formatQuery(logging, TestCaseStatus, testSpec=None):
    global globalPrepQuery

    if testSpec is not None and 'datasetSize' in list(testSpec.keys()):
        datasetSize = testSpec['datasetSize']
    else:
        datasetSize = testCaseSpecifications['datasetSize']

    globalPrepQuery = """
        load --url nfs:///netstore/datasets/yelp/user --format json --name simpleJoin-basicYelpUser --size %s;
        load --url nfs:///netstore/datasets/yelp/reviews --format json --name simpleJoin-basicYelpReviews --size %s;
    """ % (datasetSize, datasetSize)

    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    output = xcInstance.xcalarApi.submitQuery(
        globalPrepQuery.replace('\n', ''),
        xcInstance.sessionName,
        "globalPrepareJoinQuery",
        xcInstance.username,
        xcInstance.userIdUnique,
        isAsync=True)
    queryName = output.job_name

    logging.debug("queryName:" + queryName)
    xcInstance.xcalarApi.waitForQuery(
        queryName=queryName,
        userName=xcInstance.username,
        userIdUnique=xcInstance.userIdUnique,
        pollIntervalInSecs=xcInstance.pollIntervalInSecs)
    return TestCaseStatus.Pass


def prepare(logging, xcInstance, TestCaseStatus):

    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    logging.debug("username :%s, userId: %d" % (xcInstance.username,
                                                xcInstance.userIdUnique))

    output = xcInstance.xcalarApi.submitQuery(
        mainQuery.replace('\n', ''),
        xcInstance.sessionName,
        "mainJoinQuery",
        xcInstance.username,
        xcInstance.userIdUnique,
        isAsync=True)
    queryName = output.job_name

    logging.info("queryName:" + queryName)

    xcInstance.xcalarApi.waitForQuery(
        queryName=queryName,
        userName=xcInstance.username,
        userIdUnique=xcInstance.userIdUnique,
        pollIntervalInSecs=xcInstance.pollIntervalInSecs)
    return TestCaseStatus.Pass


def verifyRows(logging, filename, xcInstance, expectedRows, TestCaseStatus):
    output = xcInstance.op.tableMeta(filename)
    totalCount = 0
    for ii in range(0, output.numMetas):
        totalCount += output.metas[ii].numRows

    if totalCount != expectedRows:
        logging.error("mismatched row count for %s: expected %d found %d" %
                      (filename, expectedRows, totalCount))
        return False

    return True


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    #
    # TODO: come up with a way to not have to use hard-coded numbers.  They were determined by
    #       running the same queries through XI.
    #
    if verifyRows(logging, "simpleJoinTest/table1", xcInstance, 426,
                  TestCaseStatus) is False:
        return TestCaseStatus.Fail

    if verifyRows(logging, "simpleJoinTest/joinTableInner", xcInstance, 1312,
                  TestCaseStatus) is False:
        return TestCaseStatus.Fail

    if verifyRows(logging, "simpleJoinTest/joinTableLeft", xcInstance, 335022,
                  TestCaseStatus) is False:
        return TestCaseStatus.Fail

    if verifyRows(logging, "simpleJoinTest/joinTableRight", xcInstance, 1312,
                  TestCaseStatus) is False:
        return TestCaseStatus.Fail

    if verifyRows(logging, "simpleJoinTest/joinTableOuter", xcInstance, 335022,
                  TestCaseStatus) is False:
        return TestCaseStatus.Fail

    return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    # debugging
    # return TestCaseStatus.Pass

    output = xcInstance.xcalarApi.submitQuery(
        cleanupQuery.replace('\n', ''),
        xcInstance.sessionName,
        "mainJoinCleanupQuery",
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
    Dataset.bulkDelete(xcInstance.xcalarApi, "*simpleJoin*")

    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
