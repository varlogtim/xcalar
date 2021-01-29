import time
import argparse
import os
import hashlib
import random
import multiprocessing

from xcalar.compute.api.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.compute.api.Retina import Retina
from xcalar.compute.api.Session import Session
from xcalar.compute.api.WorkItem import WorkItemQueryState

from xcalar.compute.coretypes.Status.ttypes import StatusT
import xcalar.compute.coretypes.LibApisEnums.ttypes as apiEnums
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT


def getQueryInfo(queryName):
    workItem = WorkItemQueryState(queryName)
    return xcapi.execute(workItem)


class QueryState:
    def __init__(self):
        self.ops = {}
        self.loadTime = 0.0
        self.totalTime = 0.0

    def update(self, qg):
        if self.ops == {}:
            for node in qg.node:
                self.ops[node.name] = False

        for node in qg.node:
            if (not self.ops[node.name] and
                    node.state == DgDagStateT.DgDagStateReady):
                self.ops[node.name] = True
                opName = apiEnums.XcalarApisT._VALUES_TO_NAMES[node.api]
                elapsed = node.elapsed.milliseconds / 1000.0
                self.totalTime += elapsed
                if node.api == apiEnums.XcalarApisT.XcalarApiBulkLoad:
                    self.loadTime += elapsed
                print "finished {} ({:<20}) in {:.2f} ({} {})".format(
                        node.name.name,
                        opName,
                        elapsed,
                        node.numRowsTotal,
                        "row" if node.numRowsTotal == 1 else "rows")

    def final(self):
        print "Load  time: {:>7.2f}s".format(self.loadTime)
        print "Query time: {:>7.2f}s".format(self.totalTime - self.loadTime)
        print "Total time: {:>7.2f}s".format(self.totalTime)


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataflow", required=True)
parser.add_argument("-s", "--dataseturl", required=True)
args = parser.parse_args()

dfPath = args.dataflow
dsUrl = args.dataseturl

xcapi = XcalarApi()
sess = Session(xcapi, "BenchRunner")
xcapi.setSession(sess)
retClient = Retina(xcapi)

with open(dfPath) as df:
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

# we now have the retina successfully uploaded

queryName = "{}-{}".format(retinaBase, random.randint(0, 2**30))

# lets parameterized this retina's datasetUrl
params = [{"parameterName": "datasetUrl",
           "parameterValue": dsUrl}]

# async execute the retina
thread = multiprocessing.Process(name="Retina executer",
                                 target=retClient.execute,
                                 args=(dfName, params),
                                 kwargs={"queryName": queryName,
                                         "newTableName": "{}-out"})
print "Starting batch dataflow '{}'".format(retinaBase)
thread.start()

# wait for query to exist
for _ in xrange(0, 10):
    try:
        getQueryInfo(queryName)
    except XcalarApiStatusException as e:
        if e.status == StatusT.StatusQrQueryNotExist:
            time.sleep(1)
        else:
            raise
    break

# retClient.execute(dfName, [], queryName=queryName)
qstate = QueryState()
while True:
    queryInfo = getQueryInfo(queryName)

    if queryInfo.queryState == QueryStateT.qrProcessing:
        qstate.update(queryInfo.queryGraph)
        time.sleep(1)
        continue
    elif queryInfo.queryState == QueryStateT.qrFinished:
        qstate.update(queryInfo.queryGraph)
        qstate.final()
        print "done!"
        break
    else:
        raise ValueError("Query failed with {}".format(queryInfo.queryState))
thread.join()
