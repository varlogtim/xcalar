# Copyright 2016-2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
Usage: Executed within the Xcalar Data Platform

This is a Xcalar Application which is responsible for performing
diagnostic validation and system calibration at startup primarily,
and during execution to a lesser degree.
"""

import sys
import os
import json
import time
import logging
import logging.handlers
from socket import gethostname

import xcalar.container.context as ctx
import xcalar.container.parent as xce
from xcalar.container.cluster import get_running_cluster

# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Verify App Logger')
logger.setLevel(logging.INFO)
# This module gets loaded multiple times. The global variables do not
# get cleared between loads. We need something in place to prevent
# multiple handlers from being added to the logger.
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    logHandler = logging.StreamHandler(sys.stdout)
    numNodes = ctx.get_node_count()

    formatter = logging.Formatter(
        '%(asctime)s - Node {} - Verify App - %(levelname)s - %(message)s'.
        format(thisNodeId))
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug(
        "Verify app initialized; nodeId:{}, numNodes:{}, hostname:{}".format(
            thisNodeId, numNodes, gethostname()))


class PathVerifier():
    def __init__(self, seed):
        """seed must be identical across all app instances"""
        self.seed = seed
        self.cluster = get_running_cluster()

    def checkPathsGlobal(self, paths):
        """Checks if given paths are on shared storage.
        Returns [Bool] formatted string
        """
        pathsGlobal = None
        try:
            if self.cluster.is_master():
                for p in paths:
                    self.makeTestFile(p)
            self.cluster.barrier()
            pathsGlobal = [self.checkTestFile(p) for p in paths]
        finally:
            self.cluster.barrier()
            if self.cluster.is_master():
                for p in paths:
                    self.cleanTestFile(p)

        outObj = {}
        for ii, p in enumerate(paths):
            outObj[p] = pathsGlobal[ii]
        return json.dumps(outObj)

    def getFullPath(self, path):
        fileName = "xcVerify-{}".format(self.seed)
        return os.path.join(path, fileName)

    def makeTestFile(self, path):
        fullPath = self.getFullPath(path)
        try:
            os.remove(fullPath)
        except Exception:
            pass
        open(fullPath, 'a')

    def checkTestFile(self, path):
        fullPath = self.getFullPath(path)
        for i in range(0, 10):
            # Check for the file a number of times with a brief nap
            # in between.  This is done to allow the file that was written
            # by the "dlm" node to get flushed out to shared storage.
            if os.path.isfile(fullPath):
                return True
            time.sleep(1)
        return False

    def cleanTestFile(self, path):
        fullPath = self.getFullPath(path)
        try:
            os.remove(fullPath)
        except Exception:
            logging.exception("Failed to cleanup {}".format(path))


class VersionVerifier():
    def __init__(self, seed):
        """seed must be identical across all app instances"""
        self.seed = seed
        self.cluster = get_running_cluster()

    def checkVersionsGlobal(self):
        """Checks if all nodes are running the same version of Xcalar. The
        dlm node sends its version to the other nodes.  Those nodes will raise
        an exception if their version doesn't match."""
        xceClient = xce.Client()
        myVersion = xceClient.get_version()
        outObj = {}
        if self.cluster.is_master():
            self.cluster.broadcast_msg(myVersion)
        else:
            requiredVersion = self.cluster.recv_msg()
            if myVersion != requiredVersion:
                errMsg = "Xcalar version mismatch.  Required: {}, running: {}".format(
                    requiredVersion, myVersion)
                raise ValueError(errMsg)

        return json.dumps(outObj)


def main(inBlob):
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    logger.debug("Received input: {}".format(inBlob))
    ret = None
    try:
        inObj = json.loads(inBlob)
        if inObj["func"] == "verifyShared":
            pv = PathVerifier(inObj["seed"])
            ret = pv.checkPathsGlobal(inObj["paths"])

        elif inObj["func"] == "verifyVersion":
            vv = VersionVerifier(inObj["seed"])
            ret = vv.checkVersionsGlobal()

        else:
            raise ValueError("Not implemented")
    except Exception:
        logger.exception("Node {} caught verify exception".format(thisNodeId))
        raise

    return ret
