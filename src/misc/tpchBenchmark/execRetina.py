# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

#
# This script is a helper script for execRetinaMgr.py, to upload and execute a
# single dataflow/retina in a Xcalar cluster.
#
# See execRetinaMgr.py for a full description of steps and this script's role
#
# It uses a single workbook "BenchRunnerWkbook" for all its retina executions
#
import time
import argparse
import os
import hashlib
import random
import multiprocessing
import io

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.client import Client
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.WorkItem import WorkItemQueryState
from xcalar.external.LegacyApi.Top import Top as TopHelper

from xcalar.compute.coretypes.Status.ttypes import StatusT
import xcalar.compute.coretypes.LibApisEnums.ttypes as apiEnums
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT

class TopApiInput(object):
    def __init__(self, measureIntervalInMs = 100, cacheValidityInMs = 0):
        self.measureIntervalInMs = measureIntervalInMs
        self.cacheValidityInMs = cacheValidityInMs
 
def getNewTopHelper():
    xcalarApi = XcalarApi()
    session = Session(xcalarApi, "getNewStatsHelper")
    session.activate()
    xcalarApi.setSession(session)

    topInput = TopApiInput()
    # need to write a TopHelper()
    topHelper = TopHelper(xcalarApi,
                     measureIntervalInMs = topInput.measureIntervalInMs,
                     cacheValidityInMs = topInput.cacheValidityInMs)

    return (topHelper)

def getQueryInfo(queryName):
    workItem = WorkItemQueryState(queryName)
    return xcapi.execute(workItem)

def getVersion():
    return Client().get_version()

class QueryState:
    def __init__(self, name, resfile):
        self.ops = {}
        self.loadTime = 0.0
        self.queryTime = 0.0
        self.queryName = name
        self.resFileName = resfile

    def update(self, qg):
        if self.ops == {}:
            self.resultFile=open(self.resFileName, 'w+')
            print("Xcalar version: {}".format(getVersion().version),
                    file=self.resultFile)
            for node in qg.node:
                self.ops[node.name.name] = False

        for node in qg.node:
            if (not self.ops[node.name.name] and
                    node.state == DgDagStateT.DgDagStateReady):
                self.ops[node.name.name] = True
                opName = apiEnums.XcalarApisT._VALUES_TO_NAMES[node.api]
                elapsed = node.elapsed.milliseconds / 1000.0
                if node.api == apiEnums.XcalarApisT.XcalarApiBulkLoad:
                    self.loadTime += elapsed
                else:
                    self.queryTime += elapsed
                print("finished {} ({:<20}) in {:.2f} ({} {})".format(
                        node.name.name,
                        opName,
                        elapsed,
                        node.numRowsTotal,
                        "row" if node.numRowsTotal == 1 else "rows"),
                        file=self.resultFile)
                topHelper = getNewTopHelper()
                topOutput = topHelper.executeTop()
                for node in range(topOutput.numNodes):
                    print("Top result nodeId {} cpuUsageInPercent {} memUsageInPercent {} memUsedInBytes {} totalAvailableMemInBytes {} networkRecvInBytesPerSec {} networkSendInBytesPerSec {} xdbUsedBytes {} xdbTotalBytes {} parentCpuUsageInPercent {} childrenCpuUsageInPercent {} numCores {} sysSwapUsedInBytes {} sysSwapTotalInBytes {} datasetUsedBytes {} sysMemUsedInBytes {}".format(
                        topOutput.topOutputPerNode[node].nodeId,
                        topOutput.topOutputPerNode[node].cpuUsageInPercent,
                        topOutput.topOutputPerNode[node].memUsageInPercent,
                        topOutput.topOutputPerNode[node].memUsedInBytes,
                        topOutput.topOutputPerNode[node].totalAvailableMemInBytes,
                        topOutput.topOutputPerNode[node].networkRecvInBytesPerSec,
                        topOutput.topOutputPerNode[node].networkSendInBytesPerSec,
                        topOutput.topOutputPerNode[node].xdbUsedBytes,
                        topOutput.topOutputPerNode[node].xdbTotalBytes,
                        topOutput.topOutputPerNode[node].parentCpuUsageInPercent,
                        topOutput.topOutputPerNode[node].childrenCpuUsageInPercent,
                        topOutput.topOutputPerNode[node].numCores,
                        topOutput.topOutputPerNode[node].sysSwapUsedInBytes,
                        topOutput.topOutputPerNode[node].sysSwapTotalInBytes,
                        topOutput.topOutputPerNode[node].datasetUsedBytes,
                        topOutput.topOutputPerNode[node].sysMemUsedInBytes),
                        file=self.resultFile)

    def final(self):
        self.qprefix = self.queryName.split('-')[0]
        print("Query\tLoadTime (s)\tQueryTime (s)\tTotalTime (s)",
                file=self.resultFile)
        print("{}\t{:>7.2f}\t{:>7.2f}\t{:>7.2f}".format(self.qprefix,
            self.loadTime, self.queryTime, self.loadTime + self.queryTime),
            file=self.resultFile)
        topHelper = getNewTopHelper()
        topOutput = topHelper.executeTop()
        for node in range(topOutput.numNodes):
            print("Top result nodeId {} cpuUsageInPercent {} memUsageInPercent {} memUsedInBytes {} totalAvailableMemInBytes {} networkRecvInBytesPerSec {} networkSendInBytesPerSec {} xdbUsedBytes {} xdbTotalBytes {} parentCpuUsageInPercent {} childrenCpuUsageInPercent {} numCores {} sysSwapUsedInBytes {} sysSwapTotalInBytes {} datasetUsedBytes {} sysMemUsedInBytes {}".format(
                topOutput.topOutputPerNode[node].nodeId,
                topOutput.topOutputPerNode[node].cpuUsageInPercent,
                topOutput.topOutputPerNode[node].memUsageInPercent,
                topOutput.topOutputPerNode[node].memUsedInBytes,
                topOutput.topOutputPerNode[node].totalAvailableMemInBytes,
                topOutput.topOutputPerNode[node].networkRecvInBytesPerSec,
                topOutput.topOutputPerNode[node].networkSendInBytesPerSec,
                topOutput.topOutputPerNode[node].xdbUsedBytes,
                topOutput.topOutputPerNode[node].xdbTotalBytes,
                topOutput.topOutputPerNode[node].parentCpuUsageInPercent,
                topOutput.topOutputPerNode[node].childrenCpuUsageInPercent,
                topOutput.topOutputPerNode[node].numCores,
                topOutput.topOutputPerNode[node].sysSwapUsedInBytes,
                topOutput.topOutputPerNode[node].sysSwapTotalInBytes,
                topOutput.topOutputPerNode[node].datasetUsedBytes,
                topOutput.topOutputPerNode[node].sysMemUsedInBytes),
                file=self.resultFile)
        self.resultFile.close()
        print("{}\t{:>7.2f}\t{:>7.2f}\t{:>7.2f}".format(self.qprefix,
            self.loadTime, self.queryTime, self.loadTime + self.queryTime))

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataflow", required=True)
parser.add_argument("-s", "--datasetpath", required=True)
# boolean arg to indicate if this is a run which will produce src tables
parser.add_argument("-g", "--gensrctabs", required=True)
parser.add_argument("-r", "--resfilename", required=True)
args = parser.parse_args()

dfPath = args.dataflow
dsPath = args.datasetpath
genSrcTabs = args.gensrctabs
resFileName = args.resfilename

xcapi = XcalarApi()
sess = Session(xcapi, "BenchRunner", sessionName="BenchRunnerWkbook",
                                     reuseExistingSession=True)
try:
    sess.activate()
except:
    pass # workbook may already exist and active

xcapi.setSession(sess)
retClient = Retina(xcapi)
operatorsClient = Operators(xcapi)

# read-in and upload the retina
with io.open(dfPath, 'rb') as df:
    retinaContents = df.read()
# Use the retina hash as a uniquifier for the object
retinaHash = hashlib.sha1(retinaContents).hexdigest()
retinaBase = os.path.basename(dfPath).split('.')[0]
dfName = "{}-{}".format(retinaBase, retinaHash)

try:
    retClient.add(dfName, retinaContents)
except XcalarApiStatusException as e:
    if e.status != StatusT.StatusRetinaAlreadyExists:
        raise

# we now have the retina successfully uploaded; proceed to execute it

randNum = random.randint(0, 2**30)
queryName = "{}-{}".format(retinaBase, randNum)
tableName = "{}".format(retinaBase)

# let's parameterize this retina's dataset's dirpath
params = [{"paramName":"datasetPath", "paramValue":dsPath}]
# Use params = [] if the retinas are not parameterized and use hard-coded
# paths to the dataset.

# async execute the retina
thread = multiprocessing.Process(name="Retina executer",
                                 target=retClient.execute,
                                 args=(dfName, params),
                                 kwargs={"queryName": queryName,
                                         "newTableName": tableName})
thread.start()

# wait for query to exist
for _ in range(0, 10):
    try:
        getQueryInfo(queryName)
    except XcalarApiStatusException as e:
        if e.status == StatusT.StatusQrQueryNotExist:
            time.sleep(1)
        else:
            raise
    break

# Wait for query to finish running. After it's done, delete the retina, and
# delete the generated table unless the retina execution is part of the LOAD
# step, for which genSrcTabs would be "true", and the generated table must be
# left as a source table for the subsequent QUERY-EXECUTE phase.
qstate = QueryState(queryName, resFileName)
while True:
    queryInfo = getQueryInfo(queryName)

    if queryInfo.queryState == QueryStateT.qrProcessing:
        qstate.update(queryInfo.queryGraph)
        time.sleep(1)
        continue
    elif queryInfo.queryState == QueryStateT.qrFinished:
        qstate.update(queryInfo.queryGraph)
        qstate.final()
        retClient.delete(dfName)
        if genSrcTabs == "false":
            operatorsClient.dropTable(tableName)
        break
    else:
        raise ValueError("Query failed with {}".format(queryInfo.queryState))

thread.join()
