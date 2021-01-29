import sys
from xcalar.external.LegacyApi.Dataset import Dataset
from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'Basic Test',
    'Description': 'Basic test to illustrate how to add a new test case',
    'ExpectedRunTime': 600,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'datasetSize': '2GB',
    'SingleThreaded': False,
    'ExclusiveSession': False,
}

# globalPrepQuery is only ever invoked once per user

globalPrepQuery = None

# This gets invoked as many times as NumRuns specified in testCaseSpecifications
mainQuery = """
index --key user_id --dataset .XcalarDS.basicTestsYelpUser --dsttable \"basicTests/table1\" --prefix pyelpuser;
index --key user_id --dataset .XcalarDS.basicTestsYelpReviews --dsttable \"basicTests/table2\" --prefix pyelpreviews;
join --leftTable \"basicTests/table1\" --rightTable \"basicTests/table2\" --joinTable \"basicTests/joinTable\";
"""

cleanupQuery = """
drop table \"basicTests/table1\";
drop table \"basicTests/table2\";
drop table \"basicTests/joinTable\";
"""

globalCleanUpQuery = None


def formatQuery(logging, TestCaseStatus, testSpec=None):
    global globalPrepQuery

    if testSpec is not None and 'datasetSize' in list(testSpec.keys()):
        datasetSize = testSpec['datasetSize']
    else:
        datasetSize = testCaseSpecifications['datasetSize']

    globalPrepQuery = """
    load --url nfs:///netstore/datasets/yelp/user --format json --name basicTestsYelpUser --size %s;
    load --url nfs:///netstore/datasets/yelp/reviews --format json --name basicTestsYelpReviews --size %s;
    """ % (datasetSize, datasetSize)

    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    output = xcInstance.xcalarApi.submitQuery(
        globalPrepQuery.replace('\n', ''),
        xcInstance.sessionName,
        "globalPrepareBasicQuery",
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
        "mainBasicQuery",
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


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    foundTable = False
    tables = xcInstance.listTables()
    numMatch = len([
        table for table in tables
        if table["tableName"] == "basicTests/joinTable"
        and table["status"] == DgDagStateT.DgDagStateReady
    ])
    if numMatch == 1:
        return TestCaseStatus.Pass
    elif numMatch > 1:
        logging.error([
            table["tableName"] for table in tables
            if table["tableName"] == "basicTests/joinTable"
        ])
        logging.error(
            "Multiple tables with same name (NumMatch: %d)" % numMatch)
        return TestCaseStatus.Fail
    else:
        logging.error("Could not find joinTable")
        return TestCaseStatus.Fail


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    output = xcInstance.xcalarApi.submitQuery(
        cleanupQuery.replace('\n', ''),
        xcInstance.sessionName,
        "cleanupBasicQuery",
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
    Dataset.bulkDelete(xcInstance.xcalarApi, "*basicTests*")

    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
