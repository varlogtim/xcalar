#!/usr/bin/env python
"""
    Runs performance tests against a cluster, persisting data in sqlite.
    This file contains the 'data collection' half of performance regression
    testing. It achievies the following:
        1. Parse out test cases from the given test directory
        2. Spin up a cluster and run the test against it for each testcase
        3. A test case comprises of a retina and the test will run the retina
           as is(i.e, in batch/LRQ mode) and then runs each and every individual
           operation that is part of the retina(i.e, in query/modeling mode)
        4. Persist the performance data of each operation in a user specified
           sqlite database, which must already be populated with the given
           schemas.
    Related files:
        perfResults.py: For the gathering half
        dbUpdate.sql: Specifies the database schema for the outputs of this test
"""
import argparse
import sys
import os
import time
import logging
import sqlite3
import hashlib
import signal
import importlib.util
import tarfile
import json

from xcalar.external.LegacyApi.Session import Session
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.WorkItem import WorkItemUpdateRetina
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.compute.util.cluster import DevCluster

scriptDir = os.path.dirname(os.path.realpath(__file__))

xlrdir = os.environ["XLRDIR"]
cliBin = os.path.join(xlrdir, "bin/xccli")
launcherPath = os.path.join(xlrdir, "bin/launcher.sh")
buildDir = os.environ["BUILD_DIR"]
exportParamName = "exportFileName"

DefaultLoggingLevel = logging.DEBUG
LatestBuildDir = "/netstore/builds/unreleased/xcalar-latest/prod"
InstallerSuffix = "installer"


def getOneFile(dirPath, ext):
    matchingFiles = [
        os.path.splitext(fn) for fn in os.listdir(dirPath)
        if os.path.splitext(fn)[1] == ext
    ]
    if len(matchingFiles) != 1:
        errStr = "{num} {ext} files specified in {dirname}".format(
            num=len(matchingFiles), ext=ext, dirname=dirPath)
        raise ValueError(errStr)
    return os.path.join(dirPath, matchingFiles[0][0] + matchingFiles[0][1])


def renderAvgCsv(commandLogs):
    """This is a 'static' type method that aggregates commandLogs"""
    if len(commandLogs) == 0:
        return ""
    firstLog = commandLogs[0]
    if len(firstLog.commands) == 0:
        return ""
    final = ""

    for ii, cmd in enumerate(firstLog.commands):
        sumTime = 0
        for cmdLog in commandLogs:
            sumTime += cmdLog.commands[ii][1]
        avgTime = sumTime / len(commandLogs)
        thisStr = "{} - {}".format(cmd[0], avgTime)
        final = final + thisStr
    return final


def getLatestInstaller():
    matchingFiles = [
        fn for fn in os.listdir(LatestBuildDir) if fn.endswith(InstallerSuffix)
    ]
    if len(matchingFiles) > 1:
        errStr = "multiple installers in build dir '{}'".format(LatestBuildDir)
        raise ValueError(errStr)
    elif len(matchingFiles) == 0:
        errStr = "no installer in build dir '{}'".format(LatestBuildDir)
        raise ValueError(errStr)
    return os.path.join(LatestBuildDir, matchingFiles[0])


def alarmHandler(signum, frame):
    logging.error("Suite timed out")
    raise Exception("Suite time out")


class CommandLog(object):
    def __init__(self):
        self.commands = []

    def addCommand(self, commandStr, timeElapsed):
        self.commands.append((commandStr, timeElapsed))

    def writeToDb(self, conn, runid):
        with conn:
            for ii, cmd in enumerate(self.commands):
                conn.execute(
                    """
                        INSERT INTO commands
                            (commandid, runid, stepnum, cmdstring, secTaken)
                        VALUES
                            (NULL, :runid, :stepnum, :cmdstring, :secTaken)""",
                    {
                        "runid": runid,
                        "stepnum": ii,
                        "cmdstring": cmd[0],
                        "secTaken": cmd[1]
                    })


class PerfTest(object):
    def __init__(self,
                 name,
                 retinaData,
                 cfgFile,
                 installerPath,
                 numTrials,
                 remote,
                 username=None):
        self.name = name
        self.retinaSpec = retinaData
        self.cfgFile = cfgFile
        self.installerPath = installerPath
        self.numTrials = numTrials
        self.userName = "perftest"
        self.userIdUnique = 44442222

        # Hash the cliStr so if the test changes with the same name we don't
        # get confused and compare it against the old version
        # XXX Maybe include config file?
        self.testHash = hashlib.sha1(str(
            self.retinaSpec).encode('utf-8')).hexdigest()

        if remote:
            raise ValueError("remote cluster option is not supported yet")
        else:
            self.cluster = DevCluster()

        self.commandLogs = []

    def runXcQuery(self, xcQuery):
        elapsedInSecs = 0
        queryName = "perfTestQuery"
        logging.debug("queryName:" + xcQuery)
        output = self.xcalarApi.submitQuery(xcQuery, True, queryName,
                                            self.userName, self.userIdUnique)
        output = self.xcalarApi.waitForQuery(queryName, self.userName,
                                             self.userIdUnique)
        if output.queryStatus == StatusT.StatusOk:
            elapsedInSecs = output.elapsed.milliseconds / 1000
            success = True
        else:
            logging.error("Failed to execute query %s: %s" %
                          (xcQuery, output.queryStatus))
            success = False

        self.xcalarApi.deleteQuery(queryName, self.userName, self.userIdUnique)

        return success, elapsedInSecs

    def runXcRetina(self, retinaName, retinaParams):
        success = True
        logging.debug("retinaName:" + retinaName)
        try:
            retina = Retina(self.xcalarApi)
            output = retina.execute(retinaName, retinaParams)
        except XcalarApiStatusException as e:
            logging.error(
                "Failed to execute retina %s: %s" % (retinaName, str(e)))
            success = False
        except Exception:
            logging.exception("Unexpected error:")
            success = False

        return success

    def runQuery(self, commandLog, line, operationName):
        """Requires that self.cluster be open
        Returns true if the command succeeded and false otherwise
        """
        success, elapsed = self.runXcQuery(line)
        if success:
            commandLog.addCommand(operationName, elapsed)
        return success

    def runRetina(self, commandLog, retinaName, retinaParams):
        """Requires that self.cluster be open
        Returns true if the command succeeded and false otherwise
        """
        startTime = time.time()
        success = self.runXcRetina(retinaName, retinaParams)
        elapsed = time.time() - startTime
        if success:
            commandLog.addCommand(retinaName, elapsed)
        return success

    def addRetina(self, retinaName, retinaFilePath):
        with open(retinaFilePath, 'rb') as f:
            data = f.read()
        retina = Retina(self.xcalarApi)
        retina.add(retinaName, data)
        # prameterize the export file path. This is needed as we run the same retina multiple times
        # and hence need to give different export file paths for the runs
        output = retina.getDict(retinaName)
        for idx, queryItem in enumerate(output['query']):
            if queryItem['operation'] == 'XcalarApiExport':
                output['query'][idx]['args'][
                    'fileName'] = "<%s>" % exportParamName
        updateRetina = json.dumps(output)
        workItemUpdateRetina = WorkItemUpdateRetina(
            retinaName, updateRetina, self.userName, self.userIdUnique)
        output = self.xcalarApi.execute(workItemUpdateRetina)

    def deleteRetina(self, retinaName):
        retina = Retina(self.xcalarApi)
        retina.delete(retinaName)

    def getOrMakeTestId(self, conn):
        testid = None
        # We need to get this testid, it might not exist
        with conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                    SELECT testid
                    FROM perfTest
                    WHERE testhash=:testhash
                    """, {"testhash": self.testHash})
            rec = cursor.fetchone()
            if rec is not None:
                # This testcase exists, use its id
                testid = rec[0]
            else:
                # Make the new testcase
                cursor.execute(
                    """
                        INSERT INTO perfTest (testid, testname, testhash)
                        VALUES (NULL, :name, :testhash)
                        """, {
                        "name": self.name,
                        "testhash": self.testHash
                    })
                testid = cursor.lastrowid
        return testid

    def persistRunResults(self, conn, suiteid):
        """conn is an open sqlite3 db connection"""

        testid = self.getOrMakeTestId(conn)

        for ii, log in enumerate(self.commandLogs):
            runid = None
            with conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                        INSERT INTO testRun (runid, testid, suiteid)
                        VALUES (NULL, :testid, :suiteid)
                        """, {
                        "testid": testid,
                        "suiteid": suiteid
                    })
                runid = cursor.lastrowid
            # This opens its own transaction with conn
            log.writeToDb(conn, runid)

    def run(self, conn, suiteid):
        """
        Runs this test self.numTrials times and persists results through
        the sqlite3 db connection conn
        """
        # Clear the command logs
        self.commandLogs = []
        success = True
        for ii in range(0, self.numTrials):
            logging.info("{name} trial {trial}: starting".format(
                name=self.name, trial=ii))
            with self.cluster:
                # get an api object and then create a session to run the tests
                self.xcalarApi = self.cluster.get_xcalar_api()
                mySession = Session(self.xcalarApi, self.userName,
                                    self.userName, self.userIdUnique,
                                    self.name)
                mySession.activate()
                self.xcalarApi.setSession(mySession)

                commandLog = CommandLog()
                logging.info("  Running test on cluster")
                retinaList = self.retinaSpec.testCaseConfig["retinas"]
                for retina in retinaList:
                    if not retina["Enabled"]:
                        continue

                    # First, run retinas(in LRQ/batch mode)

                    retinaFilePath = retina['path'].replace(
                        "BUILD_DIR", buildDir)
                    # upload retina
                    self.addRetina(retina["name"], retinaFilePath)
                    # build retina parameters
                    retinaParameters = []
                    for key in retina["retinaParams"]:
                        retinaParameter = {
                            "paramName": key,
                            "paramValue": retina["retinaParams"][key]
                        }
                        retinaParameters.append(retinaParameter)
                    dstTable = retina["dstTable"]
                    exportFileName = "export-%s-%d.csv" % (dstTable,
                                                           int(time.time()))
                    retinaParameter = {
                        "paramName": exportParamName,
                        "paramValue": exportFileName
                    }
                    retinaParameters.append(retinaParameter)
                    # execute retina
                    success = self.runRetina(commandLog, retina["name"],
                                             retinaParameters)
                    # delete retina
                    self.deleteRetina(retina["name"])
                    if not success:
                        break

                    # Second, run the individual operations(in query/modeling mode)

                    # Upload the UDFs
                    xcUdf = Udf(self.xcalarApi)
                    with tarfile.open(retinaFilePath, "r:gz") as tFile:
                        for fp in tFile:
                            if fp.name.startswith("udfs/"):
                                udfModule = fp.name[len("udfs/"):-3]
                                udfSource = tFile.extractfile(
                                    fp.name).read().decode("utf-8")
                                logging.debug("Uploading %s as %s" %
                                              (fp.name, udfModule))
                                xcUdf.addOrUpdate(udfModule, udfSource)

                    # Extract the query from the batch dataflow
                    with tarfile.open(retinaFilePath, "r:gz") as tFile:
                        infoFile = tFile.extractfile("dataflowInfo.json")
                        retinaInfo = json.loads(infoFile.read())
                    # Loop through the individual operations
                    for operation in retinaInfo["query"]:
                        query = json.dumps(operation)
                        # check and replace parameters
                        for key in retina["retinaParams"]:
                            keyPattern = "<" + key + ">"
                            query = query.replace(keyPattern,
                                                  retina["retinaParams"][key])
                        # parameterize the export filename
                        if (operation["operation"]) == "XcalarApiExport":
                            operation['args'][
                                'fileName'] = "export-%s-%d.csv" % (
                                    dstTable, int(time.time()))
                            query = json.dumps(operation)
                        query = "[" + query + "]"
                        # run the query
                        success = self.runQuery(commandLog, query,
                                                operation["operation"])
                        if not success:
                            break

                self.commandLogs.append(commandLog)
                # tear down the session
                mySession.destroy()
                logging.info("  Finished test on cluster")

            logging.info("{name} trial {trial}: finished".format(
                name=self.name, trial=ii))

        self.persistRunResults(conn, suiteid)
        return success


class PerfTestSuite(object):
    def __init__(self,
                 commitSHA,
                 installerPath,
                 resultDb,
                 testsDir,
                 numTrials,
                 remote,
                 username=None):
        logging.info("Commit SHA: '{}'".format(commitSHA))
        logging.info("Performance Test Database: '{}'".format(resultDb))
        logging.info("Test directory: '{}'".format(testsDir))
        logging.info("Number of trials per test: '{}'".format(numTrials))
        self.resultDb = os.path.realpath(resultDb)
        self.testsDir = os.path.realpath(testsDir)
        self.commitSHA = commitSHA
        self.installerPath = installerPath
        self.numTrials = numTrials
        self.remote = remote
        self.username = username

        self.conn = sqlite3.connect(self.resultDb)
        self.conn.execute("PRAGMA foreign_keys=on")

    def makePerfTest(self, testPath):
        testname = os.path.basename(testPath)
        # Skip if there is a SKIP file
        if [fn for fn in os.listdir(testPath) if fn == "SKIP"]:
            logging.info("Skipping {} because of 'SKIP' file".format(testname))
            return None
        retinaFile = getOneFile(testPath, ".py")
        cfgFile = getOneFile(testPath, ".cfg")
        moduleName = os.path.splitext(os.path.basename(retinaFile))[0]
        spec = importlib.util.spec_from_file_location(moduleName, retinaFile)
        retinaData = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(retinaData)
        return PerfTest(
            testname,
            retinaData,
            cfgFile,
            self.installerPath,
            self.numTrials,
            self.remote,
            username=self.username)

    def getTests(self):
        ptests = []
        for testDir in [
                os.path.join(self.testsDir, t)
                for t in os.listdir(self.testsDir)
        ]:
            logging.debug("Checking " + testDir)
            if not os.path.isdir(testDir) or testDir.startswith("."):
                continue
            fullPath = os.path.join(self.testsDir, testDir)
            perfTest = self.makePerfTest(fullPath)
            if perfTest:
                ptests.append(perfTest)
        return ptests

    def makeSuiteRun(self):
        """Returns the suiteid"""
        suiteid = None
        with self.conn as c:
            cursor = c.cursor()
            cursor.execute(
                """
                    INSERT INTO suiteRun
                    VALUES(NULL, CURRENT_TIMESTAMP, :gitsha)
                    """, {"gitsha": self.commitSHA})
            suiteid = cursor.lastrowid
        return suiteid

    def runTests(self):
        tests = self.getTests()

        logging.info("Ready to run {} tests:".format(len(tests)))
        for t in tests:
            logging.info("  {}".format(t.name))

        suiteid = self.makeSuiteRun()
        success = True
        for t in tests:
            testSuccess = t.run(self.conn, suiteid)
            if not testSuccess:
                success = False
        return success


def main():
    latestInstaller = None
    try:
        latestInstaller = getLatestInstaller()
    except (OSError, ValueError):
        # We don't have access to /netstore or the name convention changed
        pass
    parser = argparse.ArgumentParser(description='Run performance tests')
    parser.add_argument(
        "-s",
        "--sha",
        default=None,
        help="Git commit SHA to run the tests against")
    parser.add_argument(
        "-p",
        "--installerPath",
        required=latestInstaller is None,
        default=latestInstaller,
        help="Path to the xcalar installer")
    parser.add_argument(
        "-r",
        "--resultDb",
        default="./perf.db",
        help="Path to store the test suite results")
    parser.add_argument(
        "-t",
        "--testDir",
        default="./perfTests",
        help="Path to the directory containing all of the tests")
    parser.add_argument(
        "-n",
        "--numTrials",
        type=int,
        default=3,
        help="Number of trials to run each test")
    parser.add_argument(
        "--remote", action="store_true", help="Run the cluster remotely")
    parser.add_argument(
        "-u",
        "--username",
        default="xctest",
        help="(REMOTE ONLY) Username to SSH into the "
        "machines for a remote cluster")
    parser.add_argument(
        "-o",
        "--timeout",
        type=int,
        default=60 * 60 * 5,    # 5 hours
        help="Timeout for the whole suite run")
    args = parser.parse_args()
    sha = args.sha
    installerPath = args.installerPath
    resultDb = args.resultDb
    testDir = args.testDir
    numTrials = args.numTrials
    remote = args.remote
    username = args.username
    timeout = args.timeout

    loggingLevel = DefaultLoggingLevel

    logging.basicConfig(
        level=loggingLevel, format='[%(asctime)s]: %(message)s')

    suite = PerfTestSuite(
        sha,
        installerPath,
        resultDb,
        testDir,
        numTrials,
        remote,
        username=username)
    signal.signal(signal.SIGALRM, alarmHandler)
    signal.alarm(timeout)
    success = suite.runTests()
    if success:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
