import inspect
import os
import sys
import time
import random
import json
import tarfile

from xcalar.external.LegacyApi.Dataset import CsvDataset
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.result_set import ResultSet
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT

from xcalar.compute.util.utils import XcUtil
from xcalar.external.dataflow import Dataflow

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'Query Regression Test Suite',
    'Description': 'Query Regression Test Suite',
    'ExpectedRunTime': 600,    # In seconds for 1 run
    'NumRuns': 1,    # Number of times a given user will invoke main()
    'SingleThreaded': False,
    'ExclusiveSession': False,
    'TransientFailuresAllowed': False
}

cmdDir = os.path.realpath(
    os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmdDir not in sys.path:
    sys.path.insert(0, cmdDir)

xlrDir = os.environ["XLRDIR"]
buildDir = os.environ["BUILD_DIR"]
exportParamName = "exportFileName"

pyTestDir = os.path.join(cmdDir, "../../../../tests/pyTest")
if pyTestDir not in sys.path:
    sys.path.insert(0, pyTestDir)

DefaultTargetName = "Default Shared Root"
# This prefix is used to identify system test issued queries. When operators
# functest and systemtest run in parallel, we need to identify the system test
# issued queries, so that they do not get canceled by operators functest. If this prefix
# need to be changed, ensure that operators functest is also changed to accomodate the change.
systemtestQueryPrefix = "systemTestQuery"

# The below functions for datasetCheck are copied from test_load so as to not require the underlying QA infrastructure


def _freezeValue(value):
    if isinstance(value, dict):
        return frozenset([(k, _freezeValue(v)) for k, v in value.items()])
    elif isinstance(value, list):
        return frozenset(value)
    else:
        return value


def _freezeRow(row):
    return frozenset([(k, _freezeValue(v)) for k, v in row.items()])


def _freezeRows(rows):
    return frozenset([_freezeRow(row) for row in rows])


def datasetCheck(expectedRows, resultSet, matchOrdering=False):
    """expectedRows is an array of dicts where each dict is a record"""
    # Early detect different numbers of rows, since the output is hard to read
    # filter out the 'recordNum' pseudo-field
    pureData = []
    for row in resultSet:
        pureData.append(row)
    assert (len(expectedRows) == len(pureData))

    if (matchOrdering):
        assert expectedRows == pureData
    else:
        assert _freezeRows(expectedRows) == _freezeRows(pureData)


def verifyNumRows(logging, xcInstance, TestCaseStatus, retina,
                  retinaExportFilePath):
    resultNumRows = retina["resultNumRows"]

    # load the result back to xcalar
    datasetName = "qrts-verify-%s-%s-%d" % (
        retina["name"], xcInstance.username, int(time.time()))
    logging.debug("Trying to load from %s into dataset %s" %
                  (retinaExportFilePath, datasetName))
    dataset = CsvDataset(xcInstance.xcalarApi, DefaultTargetName,
                         retinaExportFilePath, datasetName)
    dataset.load()
    output = dataset.getMeta()

    # Subtract one for the header row, for each exported file
    totalSum = sum([x.numRows for x in output.metas])
    logging.debug("total row %d" % totalSum)

    dataset.delete()

    if (totalSum != resultNumRows):
        logging.error("FAILED! expected total row %d, but got %d instead" %
                      (resultNumRows, totalSum))
        return TestCaseStatus.Fail

    return TestCaseStatus.Pass


def verifyLgLrq(logging, xcInstance, TestCaseStatus, retina,
                retinaExportFilePath):
    resultNumRowGMT = retina["resultNumRowGMT"]
    resultNumRowPDT = retina["resultNumRowPDT"]
    resultNumRowUTC = retina["resultNumRowUTC"]

    # load the result back to xcalar
    datasetName = "qrts-verify-%s-%s-%d" % (
        retina["name"], xcInstance.username, int(time.time()))
    logging.debug("Trying to load from %s into dataset %s" %
                  (retinaExportFilePath, datasetName))
    dataset = CsvDataset(xcInstance.xcalarApi, DefaultTargetName,
                         retinaExportFilePath, datasetName)
    dataset.load()
    output = dataset.getMeta()

    # Subtract one for the header row, for each exported file
    totalSum = sum([x.numRows for x in output.metas])
    logging.debug("total row %d" % totalSum)

    dataset.delete()

    if (totalSum != resultNumRowGMT and totalSum != resultNumRowPDT
            and totalSum != resultNumRowUTC):
        logging.error("FAILED! expected total row %d or %d or %d" %
                      (resultNumRowGMT, resultNumRowPDT, resultNumRowUTC))
        return TestCaseStatus.Fail

    return TestCaseStatus.Pass


def verifyExpectedRows(logging, xcInstance, TestCaseStatus, retina,
                       retinaExportFilePath):

    # Load the result back to xcalar
    datasetName = "qrts-verify-%s-%s-%d" % (
        retina["name"], xcInstance.username, int(time.time()))
    logging.debug("Trying to load from %s into dataset %s" %
                  (retinaExportFilePath, datasetName))

    dataset = CsvDataset(
        xcInstance.xcalarApi,
        DefaultTargetName,
        retinaExportFilePath,
        datasetName,
        isRecursive=False)
    dataset.load()

    logging.debug("Verifying result of running retina %s" % retina["name"])
    resultSet = ResultSet(xcInstance.client, dataset_name=dataset.name, session_name=xcInstance.session.name)

    try:
        datasetCheck(retina["expectedAnswer"], resultSet.record_iterator())
    except AssertionError:
        output = "Expected:\n%s\n\nBut instead got:\n%s\n" % ("\n".join([
            str(row) for row in retina["expectedAnswer"]
        ]), "\n".join([str(row) for row in resultSet.record_iterator()]))
        logging.error(output)
        return TestCaseStatus.Fail
    finally:
        del resultSet
        dataset.delete()

    return TestCaseStatus.Pass


def datasetCompare(resultSetActual, resultSetCurrent):

    dataCurrent = []
    for row in resultSetCurrent:
        dataCurrent.append(row)

    dataActual = []
    for row in resultSetActual:
        dataActual.append(row)

    # detect and bail early when numbers of rows are different
    assert (len(dataActual) == len(dataCurrent))

    dataActual.sort()
    dataCurrent.sort()

    assert (dataCurrent == dataActual)


def verifyResultsFromDataset(logging, xcInstance, TestCaseStatus, retina,
                             retinaExportFilePath):

    logging.debug("Verifying result of running dataflow %s" % retina["name"])

    # Load the exported file back to xcalar
    datasetName = "qrts-verify-%s-%s-%d" % (
        retina["name"], xcInstance.username, int(time.time()))
    logging.debug("Trying to load from %s into dataset %s" %
                  (retinaExportFilePath, datasetName))
    datasetCurrent = CsvDataset(
        xcInstance.xcalarApi,
        DefaultTargetName,
        retinaExportFilePath,
        datasetName,
        schemaMode="header",
        isRecursive=False)
    datasetCurrent.load()
    resultSetCurrent = ResultSet(xcInstance.client, dataset_name=datasetCurrent.name, session_name=xcInstance.session.name)

    # Load the results dataset(source of truth) to xcalar
    URL = retina["resultsDatasetPath"]
    datasetName = "qrts-results-%s-%s-%d" % (
        retina["name"], xcInstance.username, int(time.time()))
    logging.debug(
        "Trying to load from %s into dataset %s" % (URL, datasetName))
    datasetActual = CsvDataset(
        xcInstance.xcalarApi,
        DefaultTargetName,
        URL,
        datasetName,
        schemaMode="header",
        isRecursive=False)
    datasetActual.load()
    resultSetActual = ResultSet(xcInstance.client, dataset_name=datasetActual.name, session_name=xcInstance.session.name)

    try:
        datasetCompare(resultSetActual.record_iterator(), resultSetCurrent.record_iterator())
    except AssertionError:
        logging.error(
            "verifyResultsFromDataset for %s failed" % retina["name"])
        return TestCaseStatus.Fail
    finally:
        del resultSetActual
        del resultSetCurrent
        datasetActual.delete()
        datasetCurrent.delete()

    return TestCaseStatus.Pass


verificationMap = {
    'LGLrq': verifyLgLrq,
    'Customer2BatchAcp': verifyExpectedRows,
    'Customer12Lrq': verifyExpectedRows,
    'customer13Lrq': verifyNumRows,
    'Customer9Lrq': verifyNumRows,
    'tpchq1': verifyNumRows,
    'tpchq2': verifyNumRows,
    'tpchq3': verifyNumRows,
    'tpchq4': verifyResultsFromDataset,
    'tpchq5': verifyNumRows,
    'tpchq6': verifyNumRows,
    'tpchq7': verifyNumRows,
    'tpchq8': verifyNumRows,
    'tpchq9': verifyNumRows,
    'tpchq10': verifyNumRows,
    'tpchq11': verifyNumRows,
    'tpchq12': verifyNumRows,
    'tpchq13': verifyResultsFromDataset,
    'tpchq14': verifyNumRows,
    'tpchq15': verifyNumRows,
    'tpchq16': verifyResultsFromDataset,
    'tpchq17': verifyNumRows,
    'tpchq18': verifyNumRows,
    'tpchq19': verifyResultsFromDataset,
    'tpchq20': verifyResultsFromDataset,
    'tpchq21': verifyResultsFromDataset,
    'tpchq22': verifyNumRows,
}


def formatQuery(logging, TestCaseStatus, testSpec=None):
    pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    if testSpec is None:
        logging.error("testSpec cannot be None")
        return TestCaseStatus.Fail

    return TestCaseStatus.Pass


def prepare(logging, xcInstance, TestCaseStatus):
    return TestCaseStatus.Pass


def isTransientFailure(status):
    return (status != StatusT.StatusTargetDoesntExist
            and status != StatusT.StatusExportSFFileExists)


def uploadRetinaUdfs(xcInstance, retinaFilePath):
    # Upload the retina UDFs into the session.
    with tarfile.open(retinaFilePath, "r:gz") as tFile:
        # Look for retinainfo.json as well in case this is a older Dataflow file.
        try:
            infoFile = tFile.extractfile("dataflowInfo.json")
        except KeyError:
            infoFile = tFile.extractfile("retinaInfo.json")

        tflist = tFile.getmembers()
        for tmemb in tflist:
            if tmemb.isfile() and 'udfs/' in tmemb.name:
                udfsource = tFile.extractfile(tmemb).read()
                udfsourcestr = udfsource.decode("utf-8")
                moduleName = os.path.basename(tmemb.name).split('.', 1)[0]
                if moduleName != "default":
                    xcInstance.udf.addOrUpdate(moduleName, udfsourcestr)


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    status = TestCaseStatus.Pass
    xc_codes = XcUtil.RETRY_XC_STATUS_CODES + [StatusT.StatusTimedOut]
    if testSpec is None:
        logging.error(
            "Cannot run this test without testSpec for exportFileName")
        return TestCaseStatus.Fail

    xlrRoot = xcInstance.configParams["XcalarRootCompletePath"]
    logging.debug("xlrRoot: %s" % xlrRoot)

    transientFailuresAllowed = testSpec.get("TransientFailuresAllowed", False)

    enabledRetinas = []
    for retinaConfig in testSpec["retinas"]:
        if retinaConfig["Enabled"]:
            enabledRetinas.append(retinaConfig)

    random.shuffle(enabledRetinas)

    for retinaConfig in enabledRetinas:
        # Upload the retina. Each user uploads and uses his own.
        success = False
        retinaFilePath = str.replace(retinaConfig['path'], "BUILD_DIR",
                                     buildDir)
        perUserRetinaName = "%s-%s" % (retinaConfig["name"],
                                       xcInstance.username)
        logging.info(
            "Processing retina %s at %s" % (perUserRetinaName, retinaFilePath))
        XcUtil.retryer(
            uploadRetinaUdfs,
            xcInstance,
            retinaFilePath,
            retry_exceptions=[],
            max_retries=testSpec["UploadRetinaUdfMaxRetry"],
            retry_sleep_timeout=testSpec["UploadRetinaRetryTimeoutSec"],
            retry_xc_status_codes=xc_codes)

        with open(retinaFilePath, 'rb') as f:
            data = f.read()

        try:
            xcInstance.retina.add(perUserRetinaName, data)
        except XcalarApiStatusException as e:
            if e.status == StatusT.StatusRetinaAlreadyExists:
                logging.debug("Retina %s already exists. Not overwriting" %
                              (perUserRetinaName))
                success = True

            if not success:
                if transientFailuresAllowed:
                    # Keep going
                    logging.error("Error processing retina %s: %s" %
                                  (perUserRetinaName, str(e)))
                    continue
                else:
                    logging.error("Failed processing retina %s: %s" %
                                  (perUserRetinaName, str(e)))
                    raise e
        except Exception:
            logging.error("Unexpected error:", sys.exc_info()[0])
            if transientFailuresAllowed:
                # Keep going
                continue
            else:
                raise

        # Update the export node.
        dstTable = retinaConfig["dstTable"]
        retinaOutput = xcInstance.retina.getDict(perUserRetinaName)

        for idx in range(len(retinaOutput['query'])):
            if (retinaOutput['query'][idx]['operation']) == 'XcalarApiExport':
                exportNodeExists = True
                expJson = json.loads(
                    retinaOutput['query'][idx]['args']['driverParams'])
                expJson['directory_path'] = expJson[
                    'directory_path'] + "/" + "export-%s-%s-%d" % (
                        dstTable, xcInstance.username, int(time.time()))
                retinaOutput['query'][idx]['args'][
                    'driverParams'] = json.dumps(expJson)

        if not exportNodeExists:
            logging.error(
                "Could not find export node when processing retina %s" %
                (perUserRetinaName))
            return TestCaseStatus.Fail

        columnsToExport = retinaOutput['tables'][0]['columns']

        # Update query with retina config params.
        query = json.dumps(retinaOutput['query'])
        for key in retinaConfig["retinaParams"]:
            query = query.replace("<{}>".format(key),
                                  retinaConfig["retinaParams"][key])

        queryName = "%s-%s-%s" % (systemtestQueryPrefix, retinaConfig["name"],
                                  xcInstance.username)
        transientError = False

        name = retinaConfig.get("name", None)
        if name:
            verifyNeeded = verificationMap.get(name, False)
        else:
            verifyNeeded = False

        # Execute query.
        logging.info("Submit Query: retina %s, queryName %s (%s: %s)" %
                     (perUserRetinaName, queryName, exportParamName,
                      expJson['directory_path']))
        df = Dataflow.create_dataflow_from_query_string(
            client=xcInstance.client,
            query_string=query,
            columns_to_export=columnsToExport)
        dfName = xcInstance.session.execute_dataflow(
            dataflow=df, optimized=True, is_async=True, clean_job_state=False)

        logging.info(
            "Wait for Query: retina %s, queryName %s, dfName %s (%s: %s)" %
            (perUserRetinaName, queryName, dfName, exportParamName,
             expJson['directory_path']))

        queryOutput = xcInstance.xcalarApi.waitForQuery(
            queryName=dfName,
            userName=xcInstance.username,
            userIdUnique=xcInstance.userIdUnique,
            pollIntervalInSecs=xcInstance.pollIntervalInSecs)
        queryState = queryOutput.queryState

        if verifyNeeded and queryState == QueryStateT.qrFinished:
            logging.info("Verifying retina %s, queryName %s" %
                         (perUserRetinaName, dfName))
            status = verificationMap[retinaConfig["name"]](
                logging, xcInstance, TestCaseStatus, retinaConfig,
                expJson['directory_path'])
            if status == TestCaseStatus.Fail:
                return status
        elif not verifyNeeded:
            logging.debug(
                "Not verifying retina %s, queryName %s because there is no verificationMap for this retina"
                % (perUserRetinaName, dfName))

    return status


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    return TestCaseStatus.Pass


def globalCleanUp(logging, xcInstance, TestCaseStatus):
    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
