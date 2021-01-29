import pytest

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException
from xcalar.external.kvstore import KvStore as KvStore
from xcalar.compute.util.cluster import DevCluster

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
from multiprocessing import Pool
import os
import sys
import psutil
import time
import threading
import traceback
import optparse
import uuid


def _test_put_many(session_kvstore, key, valsize, tid, iterations):
    largeval = "x" * valsize
    for ii in range(iterations):
        session_kvstore.add_or_replace("{}-{}-{}".format(key, ii, tid), largeval, True)

def _test_get_many(session_kvstore, key, valsize, tid, iterations):
    largeval = "x" * valsize
    for ii in range(iterations):
        assert(session_kvstore.lookup("{}-{}-{}".format(key, ii, tid)) == largeval)

def _test_delete(session_kvstore, key, valsize, tid, iterations):
    largeval = "x" * valsize
    for ii in range(iterations):
        newkey = "{}-{}-{}".format(key, ii, tid)
        session_kvstore.delete(newkey)
        assert newkey not in session_kvstore.list()

def _test_put_get_many(session_kvstore, keyprefix, valsize, iterations, numThreads=1):
    startTime = time.time() * 1000
    threadList = []
    for tid in range(numThreads):
        x = threading.Thread(target=_test_put_many, args=(session_kvstore, keyprefix, valsize, tid, int(iterations/numThreads)))
        threadList.append(x)
        x.start()
    for thread in threadList:
        thread.join()
    stopTime = time.time() * 1000
    payloadMB = ((len(keyprefix)+4+valsize)*iterations)/2**20
    throughputMBPerSec = payloadMB/((stopTime-startTime)/1000)
    print("{},{},{},{},{},{},{},{},{}".format("put", keyprefix, valsize, int(iterations/numThreads), payloadMB, throughputMBPerSec, numThreads, (stopTime-startTime)/1000, (stopTime-startTime)/(1000*iterations)))

    startTime = time.time() * 1000
    threadList = []
    for tid in range(numThreads):
        x = threading.Thread(target=_test_get_many, args=(session_kvstore, keyprefix, valsize, tid, int(iterations/numThreads)))
        threadList.append(x)
        x.start()
    for thread in threadList:
        thread.join()
    stopTime = time.time() * 1000
    throughputMBPerSec = payloadMB/((stopTime-startTime)/1000)
    print("{},{},{},{},{},{},{},{},{}".format("get", keyprefix, valsize, int(iterations/numThreads), payloadMB, throughputMBPerSec, numThreads, (stopTime-startTime)/1000, (stopTime-startTime)/(1000*iterations)))

    startTime = time.time() * 1000
    threadList = []
    for tid in range(numThreads):
        x = threading.Thread(target=_test_delete, args=(session_kvstore, keyprefix, valsize, tid, int(iterations/numThreads)))
        threadList.append(x)
        x.start()
    for thread in threadList:
        thread.join()
    stopTime = time.time() * 1000
    print("{},{},{},{},{},{},{},{},{}".format("delete", keyprefix, valsize, int(iterations/numThreads), None, None, numThreads, (stopTime-startTime)/1000, (stopTime-startTime)/(1000*iterations)))

def test_kvperf(session_kvstore, iterations, numThreadsList):
    print('operation,keyprefix,valsize,iterationsPerThread,payloadMB,throughputMBPerSec,numThreads,totalTime,avgTime')
    value_sizes = [100, 1000, 10000, 20000]   # hand picked
    keyprefix = str(uuid.uuid1())
    numThreads = numThreadsList.split(',')
    for vs in value_sizes:
        for tt in numThreads:
            _test_put_get_many(session_kvstore, keyprefix, vs, iterations=iterations, numThreads=int(tt))

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', action="store", dest="url", help="Example: http://ec2-54-185-154-89.us-west-2.compute.amazonaws.com:80")
    parser.add_option('-l', '--login', action="store", dest="login", default='xdpadmin', help="user")
    parser.add_option('-P', '--passwd', action="store", dest="passwd", default='Welcome1', help="password")
    parser.add_option('-i', '--iterations', type=int, action="store", dest="iterations", default=3840, help="iterations")
    parser.add_option('-t', '--numThreadsList', action="store", dest="numThreadsList", default='2,4,6,8,10', help="numThreads")
    options, args = parser.parse_args()

    ret = 0
    try:
        # host: ec2-54-185-154-89.us-west-2.compute.amazonaws.com
        if not options.url:
            client = Client()
        else:
            URL = options.url
            client = Client(url=URL, client_secrets={'xiusername':options.login,'xipassword': options.passwd})
        workbook = client.create_workbook("TestKvStoreService{}".format(os.getpid()))
        session = workbook.activate()
        session_store = KvStore.workbook_by_name(session.client, session.username, session.name)
        test_kvperf(session_store, options.iterations, options.numThreadsList)
    except XcalarApiStatusException:
        print(traceback.format_exc())
        ret = -1
    finally:
        try:
            workbook.delete()
        except:
            print(traceback.format_exc())
            ret = -1
    sys.exit(ret)
