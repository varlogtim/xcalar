import sys

from xcalar.external.LegacyApi.WorkItem import WorkItemGetTableMeta
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.LegacyApi.Dataset import Dataset

from xcalar.compute.coretypes.Status.ttypes import StatusT

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'replay session test',
    'Description': 'replay session test',
    'ExpectedRunTime': 600,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'SingleThreaded': False,
    'ExclusiveSession': True,
}

#
# A way to make a query to add below is to build it in XD, export it to a
# retina and then copy the query portion from the retina to here.  These
# should not be hand-edited unless you know what you're doing...might be
# better to generate new ones using XD.
#
# Notes:
#   * The datasets need to be uniqueified for multiple users to run
#   * For query 3, copying from the retina didn't work as the json_loads in
#     QueryParser::jsonParse complained with
#     Failed to parse json query, source <string> line 1, column 328, position 328: '}' expected near 'recordDelim'
#     So, instead, I uploaded the batch dataflow file with a breakpoint set on
#     the json_loads and when the breakpoint hit I copied the query and added
#     newlines after each "operation".
#     The reason I went through this pain is that I believe this is something
#     specific to CSV files and the "recordDelim" fields.  I wanted to provide
#     a CSV example.
#
mainQuery = [
    # query 1
    """[
        {
            "operation": "XcalarApiBulkLoad",
            "args": {
                "dest": ".XcalarDS.stevew.40577.lgLog.UNIQUEIFY",
                "loadArgs": {
                    "size": 0,
                    "sourceArgs": {
                        "recursive": false,
                        "path": "/netstore/datasets/lg_generated/lgLog.json",
                        "targetName": "Default Shared Root",
                        "fileNamePattern": ""
                    },
                    "parseArgs": {
                        "parserFnName": "default:parseJson",
                        "parserArgJson": "{}"
                    }
                }
            }
        },
        {
            "operation": "XcalarApiIndex",
            "args": {
                "source": ".XcalarDS.stevew.40577.lgLog.UNIQUEIFY",
                "ordering": "Unordered",
                "dest": "lgLog#Bt1",
                "prefix": "lgLog",
                "key": [
                    {
                        "name": "xcalarRecordNum",
                        "keyFieldName": "lgLog-xcalarRecordNum",
                        "type": "DfInt64"
                    }
                ],
                "delaySort": false,
                "dhtName": "",
                "broadcast": false
            }
        },
        {
            "operation": "XcalarApiFilter",
            "args": {
                "source": "lgLog#Bt1",
                "dest": "lgLog#Bt2",
                "eval": [
                    {
                        "evalString": "between(lgLog::step, 20, 50)"
                    }
                ]
            }
        }
    ]""",
    # query 2
    """[
        {
            "operation": "XcalarApiBulkLoad",
            "args": {
                "dest": ".XcalarDS.stevew.23437.lgLog.UNIQUEIFY",
                "loadArgs": {
                    "size": 0,
                    "sourceArgs": {
                        "recursive": false,
                        "path": "/netstore/datasets/lg_generated/lgLog.json",
                        "targetName": "Default Shared Root",
                        "fileNamePattern": ""
                    },
                    "parseArgs": {
                        "parserFnName": "default:parseJson",
                        "parserArgJson": "{}"
                    }
                }
            }
        },
        {
            "operation": "XcalarApiIndex",
            "args": {
                "source": ".XcalarDS.stevew.23437.lgLog.UNIQUEIFY",
                "ordering": "Unordered",
                "dest": "lgLog#Vc0",
                "prefix": "lgLog",
                "key": [
                    {
                        "name": "xcalarRecordNum",
                        "keyFieldName": "lgLog-xcalarRecordNum",
                        "type": "DfInt64"
                    }
                ],
                "delaySort": false,
                "dhtName": "",
                "broadcast": false
            }
        },
        {
            "operation": "XcalarApiMap",
            "args": {
                "source": "lgLog#Vc0",
                "dest": "lgLog#Vc2",
                "eval": [
                    {
                        "evalString": "string(lgLog::operation)",
                        "newField": "operation"
                    }
                ],
                "icv": false
            }
        },
        {
            "operation": "XcalarApiIndex",
            "args": {
                "source": "lgLog#Vc2",
                "ordering": "Unordered",
                "dest": "lgLog.index#Vc3",
                "prefix": "",
                "key": [
                    {
                        "name": "operation",
                        "keyFieldName": "operation",
                        "type": "DfString"
                    }
                ],
                "delaySort": false,
                "dhtName": "",
                "broadcast": false
            }
        },
        {
            "operation": "XcalarApiGroupBy",
            "args": {
                "source": "lgLog.index#Vc3",
                "includeSample": false,
                "dest": "CountPerOperation#Vc1",
                "eval": [
                    {
                        "evalString": "count(lgLog::operation)",
                        "newField": "CountPerOperation"
                    }
                ],
                "newKeyField": "operation",
                "icv": false
            }
        },
        {
            "operation": "XcalarApiFilter",
            "args": {
                "source": "CountPerOperation#Vc1",
                "dest": "CountPerOperation#Vc4",
                "eval": [
                    {
                        "evalString": "gt(CountPerOperation, 5)"
                    }
                ]
            }
        }
    ]""",
    # query 3
    """[
        {\"operation\":\"XcalarApiBulkLoad\",\"args\":{\"dest\":\".XcalarDS.stevew.37246.airports.UNIQUEIFY\",\"loadArgs\":{\"sourceArgs\":{\"targetName\":\"Default Shared Root\",\"path\":\"/netstore/datasets/flight/airports.csv\",\"recursive\":false,\"fileNamePattern\":\"\"},\"size\":0,\"parseArgs\":{\"parserFnName\":\"default:parseCsv\",\"parserArgJson\":\"{\\\"recordDelim\\\":\\\"\\\\n\\\",\\\"fieldDelim\\\":\\\",\\\",\\\"isCRLF\\\":true,\\\"linesToSkip\\\":0,\\\"quoteDelim\\\":\\\"\\\\\\\"\\\",\\\"hasHeader\\\":true,\\\"schemaFile\\\":\\\"\\\",\\\"schemaMode\\\":\\\"header\\\"}\"}}}},
        {\"operation\":\"XcalarApiIndex\",\"args\":{\"source\":\".XcalarDS.stevew.37246.airports.UNIQUEIFY\",\"dest\":\"airports#tQ0\",\"ordering\":\"Unordered\",\"key\":[{\"name\":\"xcalarRecordNum\",\"keyFieldName\":\"airports-xcalarRecordNum\",\"type\":\"DfInt64\"}],\"prefix\":\"airports\",\"dhtName\":\"\",\"delaySort\":false,\"broadcast\":false}},
        {\"operation\":\"XcalarApiMap\",\"args\":{\"source\":\"airports#tQ0\",\"dest\":\"airports#tQ2\",\"eval\":[{\"evalString\":\"string(airports::state)\",\"newField\":\"state\"}],\"icv\":false}},
        {\"operation\":\"XcalarApiIndex\",\"args\":{\"source\":\"airports#tQ2\",\"dest\":\"airports.index#tQ3\",\"ordering\":\"Unordered\",\"key\":[{\"name\":\"state\",\"keyFieldName\":\"state\",\"type\":\"DfString\"}],\"prefix\":\"\",\"dhtName\":\"\",\"delaySort\":false,\"broadcast\":false}},
        {\"operation\":\"XcalarApiGroupBy\",\"args\":{\"source\":\"airports.index#tQ3\",\"dest\":\"AirportsPerState#tQ1\",\"includeSample\":false,\"eval\":[{\"evalString\":\"count(airports::state)\",\"newField\":\"state_count\"}],\"newKeyField\":\"state\",\"icv\":false}},
        {\"operation\":\"XcalarApiFilter\",\"args\":{\"source\":\"AirportsPerState#tQ1\",\"dest\":\"AirportsPerState#tQ8\",\"eval\":[{\"evalString\":\"gt(state_count, 75)\"}]}}
    ]""",
    # query 4
    """[
        {
            "operation": "XcalarApiBulkLoad",
            "args": {
                "dest": ".XcalarDS.stevew.86775.yelpuser.UNIQUEIFY",
                "loadArgs": {
                    "size": 10737418240,
                    "sourceArgs": {
                        "recursive": false,
                        "path": "/netstore/datasets/yelp/user",
                        "targetName": "Default Shared Root",
                        "fileNamePattern": ""
                    },
                    "parseArgs": {
                        "parserFnName": "default:parseJson",
                        "parserArgJson": "{}"
                    }
                }
            }
        },
        {
            "operation": "XcalarApiIndex",
            "args": {
                "source": ".XcalarDS.stevew.86775.yelpuser.UNIQUEIFY",
                "dest": "yelpuser#Dt0",
                "prefix": "yelpuser",
                "key": [
                    {
                        "name": "xcalarRecordNum",
                        "ordering": "Unordered",
                        "keyFieldName": "yelpuser-xcalarRecordNum",
                        "type": "DfInt64"
                    }
                ],
                "delaySort": false,
                "dhtName": "",
                "broadcast": false
            }
        },
        {
            "operation": "XcalarApiMap",
            "args": {
                "source": "yelpuser#Dt0",
                "dest": "yelpuser#Dt2",
                "eval": [
                    {
                        "evalString": "int(yelpuser::fans, 10)",
                        "newField": "fans"
                    }
                ],
                "icv": false
            }
        },
        {
            "operation": "XcalarApiIndex",
            "args": {
                "source": "yelpuser#Dt2",
                "dest": "yelpuser.index#Dt3",
                "prefix": "",
                "key": [
                    {
                        "name": "fans",
                        "ordering": "Unordered",
                        "keyFieldName": "fans",
                        "type": "DfInt64"
                    }
                ],
                "delaySort": false,
                "dhtName": "",
                "broadcast": false
            }
        },
        {
            "operation": "XcalarApiGroupBy",
            "args": {
                "groupAll": false,
                "source": "yelpuser.index#Dt3",
                "includeSample": false,
                "dest": "FansCountTable#Dt1",
                "eval": [
                    {
                        "evalString": "count(yelpuser::fans)",
                        "newField": "fans_count"
                    }
                ],
                "newKeyField": "fans",
                "icv": false
            }
        }
    ]"""
]

cleanupQuery = """drop table \"*\"; drop constant \"*\"; """

resultTableNames = [
    # result table for query 1
    ["lgLog#Bt2"],

    # result table for query 2
    ["CountPerOperation#Vc4"],

    # result table for query 3
    ["AirportsPerState#tQ8"],

    # result rable for query 4
    ["FansCountTable#Dt1"],

    # result table for query 5
    ["TransactionAmountsByInstitution#gN9"]
]

# UDFs
defaultModule = "default"
convertNewLineJsonFn = "convertNewLineJsonToArrayJson"
convertNewLineJsonFnPath = "/netstore/qa/udfs/default.py"

generatorModule = "generator"
generatorFn = "gen10ColDataset"
generatorFnPath = "/netstore/qa/udfs/generatorJson.py"


def formatQuery(logging, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec):

    try:
        xcInstance.xcalarApi.loadUdf(
            defaultModule, convertNewLineJsonFn, convertNewLineJsonFnPath,
            xcInstance.username, xcInstance.userIdUnique)
    except XcalarApiStatusException as error:
        assert error.status == StatusT.StatusUdfModuleAlreadyExists

    try:
        xcInstance.xcalarApi.loadUdf(generatorModule, generatorFn,
                                     generatorFnPath, xcInstance.username,
                                     xcInstance.userIdUnique)
    except XcalarApiStatusException as error:
        assert error.status == StatusT.StatusUdfModuleAlreadyExists

    return TestCaseStatus.Pass


def prepare(logging, xcInstance, TestCaseStatus):
    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    print(("username: {}, userId: {}, sessionName: {}".format(
        xcInstance.username, xcInstance.userIdUnique, xcInstance.sessionName)))

    # Make a copy of the resultant table list as "pop" consumes the list
    resultTable = list(resultTableNames)

    # For each query, run it, check the results, do a inact/swich to invoke session
    # replay and then check the results again.
    for query in mainQuery:

        resultTableRowCount = []

        # This results in a unique dataset name for each user.
        query = query.replace("UNIQUEIFY", xcInstance.username).replace(
            '\n', '')
        output = xcInstance.xcalarApi.submitQuery(
            query,
            xcInstance.sessionName,
            "Query-" + xcInstance.username + "-" + str(
                xcInstance.userIdUnique),
            xcInstance.username,
            xcInstance.userIdUnique,
            isAsync=True)
        queryName = output.job_name

        print(("queryName: {}".format(queryName)))

        xcInstance.xcalarApi.waitForQuery(
            queryName=queryName,
            userName=xcInstance.username,
            userIdUnique=xcInstance.userIdUnique,
            pollIntervalInSecs=xcInstance.pollIntervalInSecs)

        curResultTable = resultTable.pop(0)

        logging.debug(curResultTable)
        for table in curResultTable:
            # get the rowCount for each result table
            workItem = WorkItemGetTableMeta(True, table, False,
                                            xcInstance.username,
                                            xcInstance.userIdUnique)
            output = xcInstance.xcalarApi.execute(workItem)

            totalCount = 0
            for ii in range(0, output.numMetas):
                totalCount += output.metas[ii].numRows

            print(("Total count (before replay): {} ({})".format(
                table, totalCount)))
            resultTableRowCount.append(totalCount)

        xcInstance.xcalarApi.deleteQuery(queryName, xcInstance.username,
                                         xcInstance.userIdUnique)

        # replay session
        print(("Doing inact/activate for user '{}', session '{}'".format(
            xcInstance.username, xcInstance.sessionName)))
        xcInstance.inactSession()
        xcInstance.activateSession(xcInstance.sessionName)

        # verify replayed session
        for table in curResultTable:
            # get the rowCount for each result table
            workItem = WorkItemGetTableMeta(True, table, False,
                                            xcInstance.username,
                                            xcInstance.userIdUnique)
            output = xcInstance.xcalarApi.execute(workItem)

            totalCount = 0
            for ii in range(0, output.numMetas):
                totalCount += output.metas[ii].numRows

            print(("Total count (after replay): {} ({})".format(
                table, totalCount)))
            if totalCount != resultTableRowCount.pop(0):
                return TestCaseStatus.Fail

        # Cleanup here
        output = xcInstance.xcalarApi.submitQuery(
            cleanupQuery.replace('\n', ''),
            xcInstance.sessionName,
            "Cleanup" + xcInstance.username,
            xcInstance.username,
            xcInstance.userIdUnique,
            isAsync=True)
        queryName = output.job_name
        print(("Cleanup Query Name: {}".format(queryName)))
        xcInstance.xcalarApi.waitForQuery(
            queryName=queryName,
            userName=xcInstance.username,
            userIdUnique=xcInstance.userIdUnique,
            pollIntervalInSecs=xcInstance.pollIntervalInSecs)

        xcInstance.xcalarApi.deleteQuery(queryName, xcInstance.username,
                                         xcInstance.userIdUnique)

        # Delete this session in case the test suite runs again and reuses the session name.
        xcInstance.deleteSession()

        # create a new session for next query
        xcInstance.getNewSession(xcInstance.xcalarApi)

    return TestCaseStatus.Pass


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    return TestCaseStatus.Pass


def globalCleanUp(logging, xcInstance, TestCaseStatus):
    # Delete datasets created by this test
    Dataset.bulkDelete(xcInstance.xcalarApi, "*replay*")
    Dataset.bulkDelete(xcInstance.xcalarApi, "*lgLog*")
    Dataset.bulkDelete(xcInstance.xcalarApi, "*airports*")
    Dataset.bulkDelete(xcInstance.xcalarApi, "*yelpuser*")
    Dataset.bulkDelete(xcInstance.xcalarApi, "*Institutions*")
    Dataset.bulkDelete(xcInstance.xcalarApi, "*Transactions*")
    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
