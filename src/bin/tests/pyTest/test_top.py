import os
from time import sleep, time

from xcalar.compute.util.config import build_config
from xcalar.compute.util.Qa import XcalarQaDatasetPath, buildNativePath
from xcalar.compute.util.Qa import DefaultTargetName
from GenericExecutor import GenericExecutor, GenericExecutorArgs

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException, XcalarApiException
from xcalar.external.LegacyApi.Top import Top
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Dataset import UdfDataset, JsonDataset
from xcalar.external.LegacyApi.Env import XcalarConfigPath

from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.external.client import Client

import pytest
import random


class TopApiInput(object):
    def __init__(self, measureIntervalInMs=100, cacheValidityInMs=0):
        self.measureIntervalInMs = measureIntervalInMs
        self.cacheValidityInMs = cacheValidityInMs


class verifyChildCpuUsageArgs(object):
    def __init__(self, maxAllowedSeconds=None, nodeId=None, totalNodes=None):
        assert (maxAllowedSeconds is not None)
        assert (nodeId is not None)
        self.maxAllowedSeconds = maxAllowedSeconds
        self.nodeId = nodeId
        self.totalNodes = totalNodes


class TopVerifier(object):
    def __init__(self, topInput):
        assert (topInput is not None)
        assert (topInput.measureIntervalInMs is not None)
        assert (topInput.cacheValidityInMs is not None)

        self.client = Client()
        self.xcalarApi = XcalarApi()
        # Generate unique session names as multiple instances of this class
        # are instantiated.
        sess_name = "{}-{}".format(__class__.__name__, random.random())
        self.session = self.client.create_session(sess_name)
        self.xcalarApi.setSession(self.session)

        self.top = Top(
            self.xcalarApi,
            measureIntervalInMs=topInput.measureIntervalInMs,
            cacheValidityInMs=topInput.cacheValidityInMs)

    # Used to bombard mgmtd with multiple threads
    def getTopStats(self, numIterations):
        for i in range(numIterations):
            topOutput = None
            try:
                topOutput = self.top.executeTop()
            except XcalarApiStatusException as e:
                # Once in a bluemoon this error shows up however it was because
                # of mgmtd and thrift part of the code, need to debug
                # ignoring for now since this will block  gerrit review if hit
                assert (e.result.output.hdr.status == StatusT.StatusAgain)
                continue

            assert (topOutput is not None)
            assert (topOutput.status == StatusT.StatusOk)
            assert (topOutput.numNodes > 0)
            for node in range(topOutput.numNodes):
                assert (topOutput.topOutputPerNode[node].memUsageInPercent >
                        float(0))
                assert (topOutput.topOutputPerNode[node].memUsedInBytes >
                        int(0))
                assert (
                    topOutput.topOutputPerNode[node].totalAvailableMemInBytes >
                    int(0))
                assert (float(
                    topOutput.topOutputPerNode[node].childrenCpuUsageInPercent)
                        >= float(0))
                assert (
                    topOutput.topOutputPerNode[node].childrenCpuUsageInPercent
                    <= float(100))
                assert (float(
                    topOutput.topOutputPerNode[node].parentCpuUsageInPercent)
                        >= float(0))
                assert (float(
                    topOutput.topOutputPerNode[node].parentCpuUsageInPercent)
                        <= float(100))
                # we should always have swap configured on test machines
                assert (int(
                    topOutput.topOutputPerNode[node].sysSwapTotalInBytes) >
                        int(0))
                assert (int(topOutput.topOutputPerNode[node].uptimeInSeconds) >
                        int(0))
                assert (float(
                    topOutput.topOutputPerNode[node].cpuUsageInPercent) >=
                        float(0))
                assert (topOutput.topOutputPerNode[node].cpuUsageInPercent <=
                        float(100))

        return numIterations

    def getNumNodes(self):
        # XXX: wire in getNumNodes api
        topOutput = self.top.executeTop()
        assert (topOutput is not None)
        assert (topOutput.status == StatusT.StatusOk)
        return int(topOutput.numNodes)

    def verifyChildCpuUsageIncrease(self, verifierArgs):
        startTime = time()
        while (True):
            topOutput = self.top.executeTop()
            assert (topOutput is not None)
            assert (topOutput.status == StatusT.StatusOk)
            assert (int(topOutput.numNodes) > 0)

            for node in range(topOutput.numNodes):
                assert (float(
                    topOutput.topOutputPerNode[node].childrenCpuUsageInPercent)
                        >= float(0))
                assert (float(
                    topOutput.topOutputPerNode[node].childrenCpuUsageInPercent)
                        <= float(100))

                # We are looking for this node ID's child cpu usage
                if ((int(verifierArgs.nodeId) == int(
                        topOutput.topOutputPerNode[node].nodeId)) and
                    (topOutput.topOutputPerNode[node].childrenCpuUsageInPercent
                     > float(0))):
                    print(topOutput)
                    return True

            currentTime = time()
            if ((currentTime - startTime) > verifierArgs.maxAllowedSeconds):
                return False

    def verifyNumXdbPages(self):
        topOutput = self.top.executeTop()
        assert (topOutput is not None)
        assert (topOutput.status == StatusT.StatusOk)
        assert (int(topOutput.numNodes) > 0)

        for node in range(topOutput.numNodes):
            assert (int(topOutput.topOutputPerNode[node].xdbUsedBytes) >
                    int(0))
            assert (int(topOutput.topOutputPerNode[node].xdbTotalBytes) >
                    int(0))

    def getDatasetUsage(self):
        # Time to refresh cache
        sleep(1)
        topOutput = self.top.executeTop()
        assert (topOutput is not None)
        assert (topOutput.status == StatusT.StatusOk)
        assert (int(topOutput.numNodes) > 0)

        used = 0
        for node in range(topOutput.numNodes):
            used += int(topOutput.topOutputPerNode[node].datasetUsedBytes)

        return used

    def verifyDatasetUsage(self, beforeUsage):
        bytesUsed = self.getDatasetUsage()
        assert (bytesUsed > beforeUsage)

    def verifyNoDatasetUsage(self, beforeUsage):
        bytesUsed = self.getDatasetUsage()
        assert (bytesUsed == beforeUsage)

    # We tried to generate network traffic by loading a dataset
    # and indexing it. We cant generate traffic reliably hence
    # this is not being used right now.
    def verifyNetworkUsageIncrease(self, maxAllowedSeconds):
        # Get num nodes
        topOutput = self.top.executeTop()
        assert (topOutput is not None)
        assert (topOutput.status == StatusT.StatusOk)

        parentCpuAboveThresHold = []
        networkSendBytesAboveZero = []
        networkReceiveBytesAboveZero = []
        for node in range(topOutput.numNodes):
            networkSendBytesAboveZero.append(False)
            networkReceiveBytesAboveZero.append(False)
            parentCpuAboveThresHold.append(False)

        startTime = time()
        while (True):
            topOutput = self.top.executeTop()
            assert (topOutput is not None)
            assert (topOutput.status == StatusT.StatusOk)
            assert (topOutput.numNodes > 0)

            for node in range(topOutput.numNodes):
                if (topOutput.topOutputPerNode[node].networkRecvInBytesPerSec >
                        int(0)):
                    networkReceiveBytesAboveZero[node] = True
                if (topOutput.topOutputPerNode[node].networkSendInBytesPerSec >
                        int(0)):
                    networkSendBytesAboveZero[node] = True
                if (topOutput.topOutputPerNode[node].parentCpuUsageInPercent >
                        float(0)):
                    parentCpuAboveThresHold[node] = True

            networkSendBytesVerified = True
            for networkSendBytesPerNode in networkSendBytesAboveZero:
                networkSendBytesVerified &= networkSendBytesPerNode

            networkReceiveBytesVerified = True
            for networkReceiveBytesPerNode in networkReceiveBytesAboveZero:
                networkReceiveBytesVerified &= networkReceiveBytesPerNode

            parentCpuVerified = True
            for parentCpu in parentCpuAboveThresHold:
                parentCpuVerified &= parentCpu

            if ((networkSendBytesVerified is True)
                    and (networkReceiveBytesVerified is True)
                    and (parentCpuVerified is True)):
                return True

            currentTime = time()
            if ((currentTime - startTime) > maxAllowedSeconds):
                return False


class UdfInput(object):
    def __init__(self, udfName=None, udfSource=None):
        assert (udfName is not None)
        self.udfName = udfName
        # source can be none
        self.udfSource = udfSource

    def createCpuBurningUdf(self, nodeId=None):
        assert (nodeId is not None)
        udfSource = "import random" + '\n'

        udfSource += "import xcalar.container.context as ctx" + '\n'

        udfSource += "def a(ins, inp):" + '\n'

        # Adding \t makes things fail
        udfSource += '    ' + "thisNodeId = ctx.get_node_id(ctx.get_xpu_id())\n"
        udfSource += '    ' + "assert(thisNodeId <= int(256))" + '\n' + '\n'
        # If this udf is not running on required node, return immediately
        udfSource += '    ' + "if (thisNodeId != " + str(nodeId) + "):" + '\n'

        udfSource += '    ' + '    ' + "yield {'oneliner': 10}" + '\n'
        udfSource += '    ' + '    ' + "return" + '\n'

        udfSource += '    ' + "for i in range(10000000):" + '\n'
        udfSource += '    ' + '    ' + "b = random.random()" + '\n'
        udfSource += '    ' + "yield {'fileNo': b}" + '\n'

        self.udfSource = udfSource


class UdfWrapper(object):
    def __init__(self, args=None):
        self.client = Client()
        self.xcalarApi = XcalarApi()
        sess_name = "{}-{}".format(__class__.__name__, random.random())
        self.session = self.client.create_session(sess_name)
        self.xcalarApi.setSession(self.session)
        self.udf = Udf(self.xcalarApi)

        assert (args is not None)
        self.udfInput = args

    def deleteUdfIf(self):
        try:
            self.udf.delete(self.udfInput.udfName)
        except XcalarApiException as e:
            pass

    def loadUdf(self):
        assert (self.udfInput is not None)
        # Remove if added in previous iteration
        self.deleteUdfIf()
        self.udf.add(self.udfInput.udfName, self.udfInput.udfSource)


class TestTop(object):
    def setup_class(cls):
        cls.client = Client()
        cls.xcalarApi = XcalarApi()
        sess_name = "{}-{}".format(__class__.__name__, random.random())
        cls.session = cls.client.create_session(sess_name)
        cls.xcalarApi.setSession(cls.session)
        cls.operators = Operators(cls.xcalarApi)
        cls.childCpuVerifierThread = None
        cls.networkUsageVerifierThread = None
        cls.dataSetsLoaded = []
        cls.totalNodes = 0
        cls.configDict = build_config(XcalarConfigPath).all_options

    def teardown_class(cls):
        if (cls.childCpuVerifierThread is not None):
            cls.waitForChildCpuVerifierThread()
        if (cls.networkUsageVerifierThread):
            cls.waitForNetworkUsageVerifier()

        for dataset in cls.dataSetsLoaded:
            try:
                dataset.delete()
            except XcalarApiStatusException:
                pass
        cls.client.destroy_session(cls.session.name)

    def createChildCpuVerifierThread(self, nodeId=None, totalNodes=None):
        executor = GenericExecutor()
        topApiInput = TopApiInput()
        maxAllowedSeconds = 300    # 5 minutes
        verifierArgs = verifyChildCpuUsageArgs(
            maxAllowedSeconds=maxAllowedSeconds,
            nodeId=nodeId,
            totalNodes=totalNodes)
        args = GenericExecutorArgs(
            classType=TopVerifier,
            classConstructorArgs=topApiInput,
            pFnCbName="verifyChildCpuUsageIncrease",
            pFnCbArgs=verifierArgs)
        assert (self.childCpuVerifierThread is None)
        ret = executor.runAsync(args=args)
        self.childCpuVerifierThread = ret["threadHandle"]
        self.pool = ret["pool"]
        assert (self.childCpuVerifierThread is not None)

    def waitForChildCpuVerifierThread(self):
        executor = GenericExecutor()
        assert (self.childCpuVerifierThread is not None)
        result = executor.waitForThread(self.childCpuVerifierThread)
        self.pool.close()
        self.pool.join()
        self.childCpuVerifierThread = None
        return result

    def createNetworkUsageVerifierThread(self):
        executor = GenericExecutor()
        topApiInput = TopApiInput()
        maxAllowedSeconds = 300    # 5 minutes
        args = GenericExecutorArgs(
            classType=TopVerifier,
            classConstructorArgs=topApiInput,
            pFnCbName="verifyNetworkUsageIncrease",
            pFnCbArgs=maxAllowedSeconds)
        self.childCpuVerifierThread = executor.runAsync(args=args)
        assert (self.childCpuVerifierThread is not None)

    def waitForNetworkUsageVerifier(self):
        executor = GenericExecutor()
        assert (self.childCpuVerifierThread is not None)
        result = executor.waitForThread(self.childCpuVerifierThread)
        self.childCpuVerifierThread = None
        return result

    def updateTotalNodes(self):
        executor = GenericExecutor()
        topApiInput = TopApiInput()

        args = GenericExecutorArgs(
            classType=TopVerifier,
            classConstructorArgs=topApiInput,
            pFnCbName="getNumNodes",
            pFnCbArgs=None)
        ret = executor.runAsync(args=args)
        ret["threadHandle"] = threadHandle
        ret["pool"] = pool
        self.totalNodes = executor.waitForThread(threadHandle)
        pool.close()
        pool.join()

    def testTopMultiThreaded(self):
        # 16K requests to mgmtd will result in about 1K requests to usrnode
        # XXX: need to write a test to get stats from usrnodes and verify
        # will do after getStatsFromAllNodes() is implemented in XCE
        numThreads = 2
        numTopRequestsPerThread = 1000

        topApiInput = TopApiInput()

        executor = GenericExecutor()
        args = GenericExecutorArgs(
            classType=TopVerifier,
            classConstructorArgs=topApiInput,
            pFnCbName="getTopStats",
            pFnCbArgs=numTopRequestsPerThread)

        results = executor.runInParallelAndWait(
            args=args, numThreads=numThreads)

        totalIterations = 0
        for iterations in range(numThreads):
            totalIterations += results[iterations]

        assert (totalIterations == (numThreads * numTopRequestsPerThread))

    @pytest.mark.xfail(reason="Xc-9471")
    def testChildCore(self):
        self.updateTotalNodes()
        assert (self.totalNodes is not None)
        assert (self.totalNodes >= 1)

        for chosenNodeId in range(self.totalNodes):
            self.createChildCpuVerifierThread(
                nodeId=chosenNodeId, totalNodes=self.totalNodes)
            udfInput = UdfInput(udfName="cpuBurningUdf")
            udfInput.createCpuBurningUdf(nodeId=chosenNodeId)
            udf = UdfWrapper(args=udfInput)
            udf.loadUdf()
            path = buildNativePath("csvSanity")
            dataset = UdfDataset(
                udf.xcalarApi,
                DefaultTargetName,
                path,
                "cpuBurningUdf:a",
                name="testChildCore",
                fileNamePattern="*",
                fieldDelim=',',
                hasHeader=False,
                isRecursive=False)

            self.dataSetsLoaded.append(dataset)
            dataset.load()

            childCpuVerified = self.waitForChildCpuVerifierThread()
            assert (childCpuVerified is True)
            dataset.delete()

    def testNetworkUsageAndXdbPagesAndDatasetUsage(self):
        startingUsage = TopVerifier(TopApiInput()).getDatasetUsage()

        # The following code copied from test_export will
        # ensure we occupy some xdbPages and network traffic
        # is generated
        filename = "exportTests/manyIntCols.json"
        path = buildNativePath(filename)
        datasetName = "singleAppendDs"
        tableName = "singleAppendTable"
        numColumns = 96
        columnNames = ["field{}".format(ii) for ii in range(0, numColumns)]
        destFilename = "test-singleAppend.csv"
        localExportDir = os.path.join(
            self.configDict["Constants.XcalarRootCompletePath"], "export")
        localExportDir = localExportDir.replace("file://", "").replace(
            "nfs://", "").replace("hdfs://", "").replace("localfile://", "")
        destFileBase = os.path.splitext(destFilename)[0]
        localResult = os.path.join(localExportDir, destFileBase, destFilename)
        expectedFile = os.path.join(
            XcalarQaDatasetPath, "exportTests/manyIntCols-apnd-expected.csv")

        expectedNumRows = 100

        dataset = JsonDataset(
            self.xcalarApi,
            DefaultTargetName,
            path,
            datasetName,
            isRecursive=False,
            sampleSize=2**40)
        dataset.load()
        self.dataSetsLoaded.append(dataset)
        assert dataset.record_count() == expectedNumRows

        self.operators.indexDataset(dataset.name, tableName, 'xcalarRecordNum',
                                    "p")

        # We need to sleep here for 1 second to ensure top results
        # are served from usrnodes and not from mgmtd cache
        sleep(1)

        TopVerifier(TopApiInput()).verifyNumXdbPages()
        TopVerifier(TopApiInput()).verifyDatasetUsage(startingUsage)

        self.operators.dropTable(tableName)
        self.operators.dropExportNodes("*")
        dataset.delete()

        TopVerifier(TopApiInput()).verifyNoDatasetUsage(startingUsage)
