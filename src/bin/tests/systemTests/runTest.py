#!/usr/bin/env python
# Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# Xcalar programmers specify a bunch of workloads. System test infrastructure simulate a
# bunch of users, and each user has a bunch of terminals open. Each user will then randomly
# pick a random subset of the workload, and run them in some random order. Altogether, these
# comprise the systemTest. All this is started by invoking runTest.py.

# Class XcInstance represents a user's terminal. A user's terminal may be connected to any
# usrnode. Class User represents a simulated user.
#
# XXX - the below is no longer true, we've changes sessions such that each session is
# attached to a single node. So this test script needs to be modified, but for now I
# have hacked DefaultInstanceString below to make things work.  Note specifying multiple
# hostname:port pairs as input will still break things.
#
# 1 user might have several terminals open,
# and so a User object might have several XcInstances object. This means that over the course
# of running a workload, a user might execute a query on usrNode1, another part of the query
# on usrNode3, and verify the result on usrNode2.
#
# Not to mention that the user will be running
# several workloads at the same time. Class TestCase represent a workload specified by a Xcalar
# programmer. Each folder in the testCases subdirectory represent a test case. A user might run
# several workloads. So, a User object will have multiple TestCase objects.

from argparse import ArgumentParser
import os
import sys
import random
import multiprocessing
import logging
import hashlib
import time
import math
import inspect
import traceback
import importlib

from faker import Faker
from enum import Enum

from xcalar.external.client import Client
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.WorkItem import (
    WorkItemSessionActivate,
    WorkItemSessionDelete,
    WorkItemSessionInact,
    WorkItemSessionList,
)
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException

from xcalar.compute.coretypes.Status.ttypes import StatusT

cmdDir = os.path.realpath(
    os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
if cmdDir not in sys.path:
    sys.path.insert(0, cmdDir)

xlrDir = os.path.join(cmdDir, "../../..")
defaultXcCliDir = os.path.join(xlrDir, "bin/cli")
testCaseDir = os.path.join(cmdDir, "testCases")

# If run in serialMode, all users take turns, and every user runs their test
# cases 1 at a time. This makes it easier to run a query and verify the output
# for correctness
serialMode = False

# DefaultInstanceString = "localhost:18552,localhost:18553,localhost:18554,localhost:18555"
DefaultInstanceString = "localhost:18552"
DefaultLoggingLevel = logging.WARNING
DefaultNumUsers = 1
DefaultNodeUrl = "https://localhost:{}".format(
    os.getenv('XCE_HTTPS_PORT', '443'))
DefaultNodePort = 18552
DefaultTestConfigFile = "testConfig"
QueryPollIntervalInSecs = 5

TestCaseStatus = Enum('TestCaseStatus', 'Pass Fail Pending Timeout')


class XcInstance:
    def __init__(self,
                 username,
                 nodeUrl=DefaultNodeUrl,
                 nodePort=DefaultNodePort):
        self.nodeUrl = nodeUrl
        self.nodePort = nodePort
        self.username = username
        self.sessionName = None
        self.numSessions = 0
        self.userIdUnique = int(
            int(hashlib.md5(username.encode('utf-8')).hexdigest(), 16) % int(
                math.pow(2, 30))) + 1

        self.mgmtdUrl = "https://%s/thrift/service/XcalarApiService/" % self.nodeUrl
        self.client = Client(url=self.mgmtdUrl)
        self.xcalarApi = XcalarApi(self.mgmtdUrl, bypass_proxy=False,
                                    sdk_client=self.client)

        # XXX TODO override the client user_name because the client user_name
        # is picked up from Auth. Need a way to do this in a clean way for
        # tests.
        self.client._user_name = self.username
        self.getNewSession(self.xcalarApi)
        self.op = Operators(self.xcalarApi)
        self.retina = Retina(self.xcalarApi)
        self.udf = Udf(self.xcalarApi)
        self.configParamsRaw = self.xcalarApi.getConfigParams(
            self.username, self.userIdUnique)
        self.configParams = {}
        for param in self.configParamsRaw.parameter:
            self.configParams[param.paramName] = param.paramValue
        self.pollIntervalInSecs = QueryPollIntervalInSecs
        self.session = self.client.get_session(session_name=self.sessionName)
        self.client._legacy_xcalar_api.setSession(self.session)

    def getNewSession(self, xcalarApi):
        self.numSessions += 1
        ownerName = "gcesystemtest-%d" % (self.numSessions)
        sessionName = Session.genSessionName(self.username, ownerName,
                                             self.userIdUnique)

        sessionSet = False
        numTrials = 3
        for trial in range(numTrials):
            try:
                mySession = Session(
                    xcalarApi,
                    ownerName,
                    self.username,
                    self.userIdUnique,
                    sessionName=sessionName)
                mySession.activate()
                xcalarApi.setSession(mySession)
                sessionSet = True
                break
            except XcalarApiStatusException as e:
                logging.debug("Attempt %d at setting session failed: %s" %
                              (trial, e.message))
                if e.status == StatusT.StatusSessionExists:
                    self.deleteSession(sessionName)
                    logging.debug("Deleted %s" % sessionName)
                elif e.status == StatusT.StatusApiWouldBlock:
                    time.sleep(1)
                else:
                    raise e

        if not sessionSet:
            logging.debug("Could not set session after %d trials" % numTrials)
            raise

        logging.info("SessionName: %s, username:%s, userid:%d" %
                     (sessionName, self.username, self.userIdUnique))
        self.sessionName = sessionName

    def inactSession(self):
        workItem = WorkItemSessionInact(self.sessionName, self.username,
                                        self.userIdUnique)
        self.xcalarApi.execute(workItem)

    def activateSession(self, name):
        workItem = WorkItemSessionActivate(name, self.username,
                                           self.userIdUnique)
        self.xcalarApi.execute(workItem)

    def deleteSession(self, sessionName=None):
        if sessionName is None:
            sessionName = self.sessionName

        workItem = WorkItemSessionInact(sessionName, self.username,
                                        self.userIdUnique)
        try:
            self.xcalarApi.execute(workItem)
        except XcalarApiStatusException as e:
            if e.status == StatusT.StatusSessionNotFound:
                return

            if e.status != StatusT.StatusSessionAlreadyInact:
                raise e

        workItem = WorkItemSessionDelete(sessionName, self.username,
                                         self.userIdUnique)
        self.xcalarApi.execute(workItem)

    def listTables(self, namePattern="*"):
        tables = []
        output = self.xcalarApi.listTable(namePattern)
        if output.numNodes == 0:
            return tables
        for tableInfo in output.nodeInfo:
            table = {
                "tableName": tableInfo.name,
                "tableId": tableInfo.dagNodeId,
                "status": tableInfo.state
            }
            tables.append(table)
        return (tables)


class TestCase:
    def __init__(self, fullPath, moduleName):
        sys.path.append(fullPath)
        self.module = __import__(
            moduleName,
            globals(),
            locals(),
            fromlist=[
                "formatQuery", "globalPrepare", "prepare", "main", "verify",
                "cleanup", "globalCleanUp"
            ])
        self.name = self.module.testCaseSpecifications['Name']

        if self.name in list(testConfig.testCaseConfig.keys()):
            self.testSpec = testConfig.testCaseConfig[self.name]
        else:
            self.testSpec = None

        if self.testSpec is not None and 'ExpectedRunTime' in list(
                self.testSpec.keys()):
            self.expectedRunTime = self.testSpec['ExpectedRunTime']
        else:
            self.expectedRunTime = self.module.testCaseSpecifications[
                'ExpectedRunTime']

        if self.testSpec is not None and 'NumRuns' in list(
                self.testSpec.keys()):
            self.numRuns = self.testSpec['NumRuns']
        else:
            self.numRuns = self.module.testCaseSpecifications['NumRuns']

    def isEnabled(self):
        if self.testSpec is not None and 'Enabled' in list(
                self.testSpec.keys()):
            return self.testSpec['Enabled']
        else:
            return self.module.testCaseSpecifications['Enabled']

    def isSingleThread(self):
        if self.testSpec is not None and 'SingleThreaded' in list(
                self.testSpec.keys()):
            return self.testSpec['SingleThreaded']
        else:
            return self.module.testCaseSpecifications['SingleThreaded']

    def isExclusiveSession(self):

        if self.testSpec is not None and 'ExclusiveSession' in list(
                self.testSpec.keys()):
            return self.testSpec['ExclusiveSession']
        else:
            return self.module.testCaseSpecifications['ExclusiveSession']


class User:
    def __init__(self, name, username, xcNodes=""):
        self.name = name
        self.username = username
        self.xcNodes = xcNodes
        # Simulate where the user is connected
        self.xcInstances = []

        if xcNodes == "":
            self.xcInstances.append(XcInstance(self.username))
        else:
            xcNode = random.choice(xcNodes.split(","))
            (hostname, port) = xcNode.split(":")
            try:
                self._appendInstances(hostname, port)
            except XcalarApiStatusException as e:
                # Due to node affinity, we can't just go to any node
                if e.status != StatusT.StatusSessionUsrAlreadyExists:
                    raise e
                logging.debug("Output: %s" % str(e.output))
                self.deleteSessionOnAllNodes(xcNodes)

                # Try all nodes until we find one that works.
                # XXX API should tell us which node to go. Rather than rely on brute force
                foundHome = False
                for xcNode in xcNodes.split(","):
                    (hostname, port) = xcNode.split(":")
                    logging.debug("Trying %s" % xcNode)
                    try:
                        self._appendInstances(hostname, port)
                    except XcalarApiStatusException as e:
                        if e.status != StatusT.StatusSessionUsrAlreadyExists:
                            raise e
                        logging.debug("Nope. Still wrong")
                        continue
                    logging.debug("Yup this works")
                    foundHome = True
                    break

                if not foundHome:
                    raise e

        self.xcInstance = random.choice(self.xcInstances)
        logging.info("%s (%s) is connected to %s:%d" % (
            name, username, self.xcInstance.nodeUrl, self.xcInstance.nodePort))

        # Simulate the users workload
        self.workload = []
        self.workloadStatus = {}

    def _appendInstances(self, hostname, port):
        if 'XCE_HTTPS_PORT' in os.environ:
            self.xcInstances.append(
                XcInstance(
                    self.username, "{}:{}".format(
                        hostname, os.environ["XCE_HTTPS_PORT"]),
                    int(port)))
        else:
            self.xcInstances.append(
                XcInstance(self.username, hostname, int(port)))

    def deleteSessionOnAllNodes(self, xcNodes):
        username = self.username
        userIdUnique = int(
            int(hashlib.md5(username.encode('utf-8')).hexdigest(), 16) % int(
                math.pow(2, 30))) + 1

        logging.debug("deleteSessionOnAllNodes(%s)" % xcNodes)
        for xcNode in xcNodes.split(","):
            (hostname, port) = xcNode.split(":")
            mgmtdUrl = "https://%s/thrift/service/XcalarApiService/" % hostname
            xcApi = XcalarApi(mgmtdUrl)

            try:
                listSessionWorkItem = WorkItemSessionList(
                    "*", username, userIdUnique)
                listSessionOutput = xcApi.execute(listSessionWorkItem)
            except XcalarApiStatusException as e:
                if e.status == StatusT.StatusSessionUsrAlreadyExists or e.status == StatusT.StatusSessionNotFound:
                    continue
                else:
                    raise e

            logging.debug("Found %d sessions on %s" % (len(
                listSessionOutput.sessions), hostname))
            for session in listSessionOutput.sessions:
                logging.debug(
                    "Deleting session %s on %s" % (session.name, hostname))
                workItem = WorkItemSessionInact(session.name, username,
                                                userIdUnique)
                xcApi.execute(workItem)
                workItem = WorkItemSessionDelete(session.name, username,
                                                 userIdUnique)
                xcApi.execute(workItem)

    def releaseUserSession(self):
        for xcInstance in self.xcInstances:
            xcInstance.deleteSession()

    def addWorkload(self, testCases):
        self.workload.extend(testCases)

    def executeGlobalPrepare(self, activeTestCases):
        finalStatus = TestCaseStatus.Pass
        logging.debug("Invoking global prepare")

        for testCase in activeTestCases:
            logging.debug("Starting test case " + testCase.name)
            testCase.module.formatQuery(logging, TestCaseStatus,
                                        testCase.testSpec)
            logging.debug("Invoking global prepare for %s" % testCase.name)
            xcInstance = random.choice(self.xcInstances)
            finalStatus = testCase.module.globalPrepare(
                logging, xcInstance, TestCaseStatus, testCase.testSpec)
            if finalStatus != TestCaseStatus.Pass:
                break

        return finalStatus

    def executeGlobalCleanUp(self, activeTestCases):
        finalStatus = TestCaseStatus.Pass
        logging.debug("Invoking global cleanup")

        for testCase in activeTestCases:
            logging.debug("Invoking global cleanup for %s" % testCase.name)
            xcInstance = random.choice(self.xcInstances)
            finalStatus = testCase.module.globalCleanUp(
                logging, xcInstance, TestCaseStatus)
            if finalStatus != TestCaseStatus.Pass:
                break

        return finalStatus

    def executeTestCase(self, testCase, xcInstance, queue=None):
        finalStatus = TestCaseStatus.Pass
        for ii in range(0, testCase.numRuns):
            logging.debug("run time: %d" % ii)
            try:
                status = testCase.module.prepare(logging, xcInstance,
                                                 TestCaseStatus)
            except Exception:
                e = sys.exc_info()[:2]
                logging.error("Prepare failed with: %s %s\n%s" %
                              (e[0], e[1], traceback.format_exc()))
                status = TestCaseStatus.Fail

            if status == TestCaseStatus.Pass:
                try:
                    status = testCase.module.main(logging, ii, xcInstance,
                                                  TestCaseStatus,
                                                  testCase.testSpec)
                except Exception:
                    e = sys.exc_info()[:2]
                    logging.error("Testcase failed with: %s %s\n%s" %
                                  (e[0], e[1], traceback.format_exc()))
                    status = TestCaseStatus.Fail

            if status == TestCaseStatus.Pass:
                try:
                    status = testCase.module.verify(logging, ii, xcInstance,
                                                    TestCaseStatus,
                                                    testCase.testSpec)
                except Exception:
                    e = sys.exc_info()[:2]
                    logging.error("Testcase verify failed with: %s %s\n%s" %
                                  (e[0], e[1], traceback.format_exc()))
                    status = TestCaseStatus.Fail

            try:
                testCase.module.cleanup(logging, xcInstance, finalStatus,
                                        TestCaseStatus)
            except Exception:
                e = sys.exc_info()[:2]
                logging.error("Testcase cleanup failed with: %s %s\n%s" %
                              (e[0], e[1], traceback.format_exc()))
                status = TestCaseStatus.Fail

            if status != TestCaseStatus.Pass:
                # Just 1 fail is required to fail the entire test
                finalStatus = status
                break

        # We need this check in case we took too long
        # if self.workloadStatus[testCase.name] == TestCaseStatus.Pending:
        #    self.workloadStatus[testCase.name] = finalStatus

        # Each process has its own address space and has seperate copies of the same user instance.
        # Thus self.workloadStatus cannot be updated here as other processes will not see these
        # updates.
        # multiprocessing.queue is used to rely status information to the parent process.
        if queue is not None:
            queue.put((testCase.name, finalStatus))

    def prepare(self):
        # Drop any old tables
        dropTableOutput = self.xcInstance.op.dropTable("*")
        logging.debug("Dropped %d tables" % dropTableOutput.numNodes)
        dropConstantOutput = self.xcInstance.op.dropConstants("*")
        logging.debug("Dropped %d constants" % dropConstantOutput.numNodes)

    def executeWorkload(self, q=None):
        if serialMode or (q is None):
            for testCase in self.workload:
                logging.debug("I am going to execute \"" + testCase.name +
                              "\" now.")
                self.workloadStatus[testCase.name] = TestCaseStatus.Pending
                self.executeTestCase(testCase, self.xcInstance)
        else:
            processes = []
            queue = multiprocessing.Queue()
            for testCase in self.workload:
                logging.debug("I am going to execute \"" + testCase.name +
                              "\" now.")
                process = multiprocessing.Process(
                    name=self.name + " running " + testCase.name,
                    target=self.executeTestCase,
                    args=(testCase, self.xcInstance, queue))
                processes.append((process,
                                  testCase.expectedRunTime * testCase.numRuns))
                self.workloadStatus[testCase.name] = TestCaseStatus.Pending
                process.start()

            for (process, timeout) in processes:
                logging.debug(process.name + " should take " + str(timeout) +
                              " seconds")
                process.join(timeout)
                if (process.is_alive()):
                    logging.error(process.name + " is still running after " +
                                  str(timeout) + " seconds.")
                    self.workloadStatus[testCase.name] = TestCaseStatus.Timeout

            while not queue.empty():
                name, status = queue.get()
                self.workloadStatus[name] = status

        # {username: workloadStatus} dicts sent so that the user.workloadStatus can be updated in
        # the parent process.
        if q is not None:
            q.put((self.username, self.workloadStatus))

    def checkClusterStatus(self):
        status = TestCaseStatus.Pass
        logging.debug("Checking cluster status")
        xcInstance = random.choice(self.xcInstances)
        try:
            output = xcInstance.client.get_version()
            logging.debug(
                "Backend Version: %s\n Backend Xcalar API Version Signature: %s (%s)"
                % (output.version, output.xcrpc_version_signature_full,
                   output.xcrpc_version_signature_short))
        except XcalarApiStatusException as e:
            logging.exception("checkClusterStatus exception" + str(e))
            status = TestCaseStatus.Fail
            return status
        return status


def loadActiveTestCases():
    activeTestCases = []
    singleThreadCases = []
    for dirname in os.listdir(testCaseDir):
        fullPath = testCaseDir + "/" + dirname
        if os.path.isdir(fullPath):
            testCase = TestCase(fullPath, dirname)
            if (testCase.isEnabled()):
                if (testCase.isSingleThread()):
                    logging.debug(testCase.name + " is single-threaded")
                    singleThreadCases.append(testCase)
                else:
                    activeTestCases.append(testCase)
                logging.info("Enabling " + testCase.name)
            else:
                logging.info("Skipping " + testCase.name)
    return activeTestCases, singleThreadCases


def releaseActiveUserSession(activeUsers):
    for user in activeUsers:
        user.releaseUserSession()


def loadActiveUsers(numUsers=DefaultNumUsers, hosts=""):
    activeUsers = []
    fake = Faker()
    for ii in range(0, numUsers):
        activeUser = User(fake.name(), "user%d" % ii, hosts)
        activeUsers.append(activeUser)
    return activeUsers


def lookahead(iterable):
    it = iter(iterable)
    last = next(it)
    for val in it:
        yield last, False
        last = val
    yield last, True


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-n",
        "--numUsers",
        dest="numUsers",
        type=int,
        metavar="<numUsers>",
        default=DefaultNumUsers,
        help="Number of users to simulate (default = 1)")
    parser.add_argument(
        "-i",
        "--instances",
        dest="instances",
        type=str,
        metavar="<hosts>",
        default=DefaultInstanceString,
        help="Comma-separated list of <hostname:portnum> pairs "
        "of Xcalar nodes to connect to. No spaces please.")
    parser.add_argument(
        "-t",
        "--testConfigFile",
        dest="testConfigFile",
        type=str,
        metavar="<testConfigFile>",
        default=DefaultTestConfigFile,
        help="Test config file (default = testConfig.py)")
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help="Display INFO level information and above")
    parser.add_argument(
        "-w",
        "--veryverbose",
        dest="veryVerbose",
        action="store_true",
        help="Display DEBUG level information and above")
    parser.add_argument(
        "-s",
        "--serial",
        dest="serial",
        action="store_true",
        help="Run in serial mode")
    parser.add_argument(
        "-c",
        "--cliDir",
        dest="xcCliDir",
        type=str,
        metavar="<cliDir>",
        default=defaultXcCliDir,
        help="xccli diretory path")

    args = parser.parse_args()

    numUsers = args.numUsers
    verbose = args.verbose
    veryVerbose = args.veryVerbose
    serialMode = args.serial
    instances = args.instances
    xcCliDir = args.xcCliDir
    testConfig = importlib.import_module(args.testConfigFile)

    loggingLevel = DefaultLoggingLevel
    if veryVerbose:
        loggingLevel = logging.DEBUG
    elif verbose:
        loggingLevel = logging.INFO

    logging.basicConfig(
        level=loggingLevel,
        format='[%(asctime)s] [%(threadName)s]: %(message)s')

    activeTestCases, singleThreadCases = loadActiveTestCases()
    activeUsers = loadActiveUsers(numUsers, instances)

    activeProcesses = []
    workloadSelected = set([])

    sysAdminUser = User("Admin", "adminUser", instances)

    status = sysAdminUser.checkClusterStatus()
    if status != TestCaseStatus.Pass:
        print("Exiting the test as Cluster is not up")
        sys.exit(1)

    finalStatus = sysAdminUser.executeGlobalPrepare(activeTestCases)
    finalStatus = sysAdminUser.executeGlobalPrepare(singleThreadCases)

    if finalStatus != TestCaseStatus.Pass:
        print("Global preparation failed")
        sys.exit(1)

    for activeTestCase in activeTestCases:
        activeTestCase.expectedRunTime *= len(activeUsers)

    # Each process has its own seperate address place and thus
    # multiprocessing.queues are needed to allow communication between processes
    q = multiprocessing.Queue()
    for (activeUser, isLastUser) in lookahead(activeUsers):
        randomSubset = []
        if len(activeTestCases) > 0:
            # Give each user a random subset of test cases
            subsetSize = random.randint(1, len(activeTestCases))
            randomSubset = random.sample(activeTestCases, subsetSize)
            workloadSelected = workloadSelected.union(set(randomSubset))

        if isLastUser:
            # Last user takes everything that nobody else took
            randomSubset.extend(
                set(activeTestCases).difference(workloadSelected))
            randomSubset.extend(singleThreadCases)

        logging.debug(
            "%s has %d workItem(s)" % (activeUser.name, len(randomSubset)))
        random.shuffle(randomSubset)
        activeUser.addWorkload(randomSubset)
        activeUser.prepare()

        if (serialMode):
            activeUser.executeWorkload()
        else:
            process = multiprocessing.Process(
                name=activeUser.name,
                target=activeUser.executeWorkload,
                args=(q, ))
            activeProcesses.append(process)
            process.start()

    if not serialMode:
        for activeProcess in activeProcesses:
            activeProcess.join()

        # receives {username: workloadStatus} dicts from child processes
        user_stats = dict()
        while not q.empty():
            name, workload_stats = q.get()
            user_stats[name] = workload_stats

        for user in activeUsers:
            if (user.username in user_stats):
                user.workloadStatus = user_stats[user.username]

    releaseActiveUserSession(activeUsers)

    finalStatus = sysAdminUser.executeGlobalCleanUp(activeTestCases)
    finalStatus = sysAdminUser.executeGlobalCleanUp(singleThreadCases)

    summary = {}
    for testCaseStatus in TestCaseStatus:
        summary[testCaseStatus.name] = 0

    sysAdminUser.releaseUserSession()

    total = 0
    for user in activeUsers:
        print("=============================================")
        print(("Report from %s (%s)" % (user.name, user.username)))
        for (testCase, testCaseStatus) in list(user.workloadStatus.items()):
            print(("%s -- %s" % (testCaseStatus.name, testCase)))
            summary[testCaseStatus.name] += 1
            total += 1
        print("=============================================\n")

    print("=============================================")
    print("Summary")
    for (key, value) in list(summary.items()):
        print(("%s -- %d / %d" % (key, value, total)))
    print("=============================================")

    print("=============================================")
    print("TAP Output")
    print("1..{}".format(total))
    ii = 0
    for user in activeUsers:
        for (testCase, testCaseStatus) in list(user.workloadStatus.items()):
            if testCaseStatus == TestCaseStatus.Pass:
                status = "ok"
            else:
                status = "not ok"
            ii += 1
            print("{} {} - Test \"({}) {}\"".format(status, ii, user.username,
                                                    testCase))

    print("=============================================")

    if summary[TestCaseStatus.Pass.name] != total:
        sys.exit(1)
    else:
        sys.exit(0)
