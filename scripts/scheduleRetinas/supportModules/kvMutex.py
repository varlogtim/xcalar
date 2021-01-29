import os
import sys
import json
import subprocess
import time
import random
import hashlib

XLRDIR = os.getenv("XLRDIR", "/opt/xcalar")

pathToScheduleApps = os.path.join(XLRDIR, "scripts", "scheduleRetinas", "apps")
pathToScheduleSupport = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                     "supportModules")
pathToScheduleCronExecute = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                         "executeFromCron")

sys.path.insert(0, pathToScheduleApps)
sys.path.insert(0, pathToScheduleSupport)
sys.path.insert(0, pathToScheduleCronExecute)

from xcalar.external.client import Client

from xcalar.external.exceptions import XDPException

from xcalar.compute.coretypes.Status.ttypes import *
from xcalar.compute.coretypes.LibApisCommon.ttypes import *

import logging

from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('KV mutex Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
           '%(asctime)s - KV mutex - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("KV mutex; hostname:{}".format(gethostname()))


class KVMutex(object):
    MaxPowerOfTwo = 10
    FrameLength = .001 # In seconds
    # Under exponential backoff, expected value number wait frames is
    # Sum i = 0 to MaxPowerOfTwo of (2^i - 1)/2, and maximum wait
    # frames is twice that.  Multiply that by FrameLength to get
    # Number of seconds waited, and then add time to communicate
    # with pyclient
    # MaxTime is a UX component, need to discuss with Jerene
    MaxTime = 9 # In seconds, 10s for user intervention - 1 for non spin time
    MutexUnlocked = "0"
    HashNumLen = 16
    lockKey = "ScheduleKVLock"

    DefaultScope = XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal

    def __init__(self, client=None, session=None, kvStore=None):
        if (client == None):
            self.client = Client(bypass_proxy=True)
        else:
            self.client = client
        if (kvStore == None):
            self.kvStore = self.client.global_kvstore()
        else:
            self.kvStore = kvStore
        self.session = session

    def cleanup(self):
        if self.session:
            self.session = None
        if self.client:
            self.client = None
        if self.kvStore:
            self.kvStore = None

    def tryToLock(self, uniqueId):
        try:
            self.kvStore.set_if_equal(True, self.lockKey, self.MutexUnlocked, uniqueId)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                print("Warning: kvmutex uninitialized.")
                self.initializeLock()
                return False
            # Must initialize main list
            elif e.statusCode == StatusT.StatusKvEntryNotEqual:
                print("tryToLock failed: " + self.kvStore.lookup(self.lockKey))
                return False
            else:
                raise
        return True

    def expBackoff(self, uniqueId):
        # Should we reset if notice that lockId is changed?
        for i in range(self.MaxPowerOfTwo):
            rangeMax = 2**i - 1
            waitFrames = random.randint(0, rangeMax)
            print(("Backoff step: " + str(i)))
            print(("Sleep for: " + str(waitFrames * self.FrameLength)))
            time.sleep(waitFrames * self.FrameLength)
            if self.tryToLock(uniqueId):
                return uniqueId
        return None

    def getLock(self, scheduleName):
        # Returns None if failure, else returns uniqueId if succ
        hashNum = self.createHashNum()
        lockStr = scheduleName + "-" + hashNum
        print("Getlock: " + lockStr)
        lockRes = self.expBackoff(lockStr)
        return lockRes

    def createHashNum(self):
        # TODO: maybe don't have to generate a new one every time?
        hashStr = str(time.time())
        m = hashlib.md5()
        m.update(hashStr.encode("utf-8"))
        return m.hexdigest()[0:self.HashNumLen]

    def releaseLock(self, uniqueId):
        print("Releasing lock id: " + uniqueId)
        try:
            self.kvStore.set_if_equal(True, self.lockKey, uniqueId, self.MutexUnlocked)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                print("Warning: kvmutex uninitialized in releaseLock.")
                self.initializeLock()
                return False
            # Must initialize main list
            elif e.statusCode == StatusT.StatusKvEntryNotEqual:
                print("releaseLock failed: " + self.kvStore.lookup(self.lockKey))
                return False
            else:
                raise
        return True

    def forceReleaseLock(self):
        self.kvStore.add_or_replace(self.lockKey, self.MutexUnlocked, True)
        # Return true to be consistent with releaseLock
        return True


    def initializeLock(self):
        print("Initializing lock")
        time.sleep(1)
        try:
            self.kvStore.lookup(self.lockKey)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                print("Warning: kvmutex uninitialized after wait.  Initializing...")
                self.forceInitializeLock()
                return False
            else:
                raise

    def forceInitializeLock(self):
        self.forceReleaseLock()

    def hasLock(self, uniqueId):
        curHolder = self.kvStore.lookup(self.lockKey)
        print(("checking " + str(uniqueId) + " against " + str(curHolder)))
        return (uniqueId == curHolder)


    def forceTakeLock(self, serviceId):
        # Do not use except for maintenance, e.g. consistency check
        self.kvStore.add_or_replace(self.lockKey, serviceId, True)
        # Return true to be consistent with releaseLock
        return serviceId


    def deleteLock(self):
        try:
            self.kvStore.delete(self.lockKey)
        except XDPException as e:
            if e.statusCode != StatusT.StatusKvEntryNotFound:
                raise
