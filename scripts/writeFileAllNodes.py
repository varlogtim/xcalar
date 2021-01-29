# Copyright 2016-2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
Usage: Executed within the Xcalar Data Platform

This is a Xcalar Application which is responsible for writing the
data passed to it to the specified file on all nodes.  If the file
is on shared storage then only one of the nodes does the write.
"""

import sys
import os
import json
import shutil
import logging
import logging.handlers
from socket import gethostname

import xcalar.container.context as ctx
from xcalar.container.cluster import get_running_cluster

# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('WriteFileAllNodes App Logger')
logger.setLevel(logging.INFO)
# This module gets loaded multiple times. The global variables do not
# get cleared between loads. We need something in place to prevent
# multiple handlers from being added to the logger.
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stderr
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    numNodes = ctx.get_node_count()
    logHandler = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        '%(asctime)s - Node {} - WriteFileAllNodes App - %(levelname)s - %(message)s'
        .format(thisNodeId))
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug(
        "WriteFileAllNodes app initialized; nodeId:{}, numNodes:{}, hostname:{}"
        .format(thisNodeId, numNodes, gethostname()))


class WriteFileAllNodes():
    def __init__(self, seed):
        """seed must be identical across all app instances"""

        self.seed = seed
        self.cluster = get_running_cluster()
        self.numNodes = ctx.get_node_count()
        self.thisXpuId = ctx.get_xpu_id()
        self.thisNodeId = ctx.get_node_id(self.thisXpuId)
        self.dlmNodeId = seed % self.numNodes
        self.sharedStorage = True

        logger.debug("I {} DLM using seed {}".format(
            "am" if self.cluster.is_master() else "am not", seed))

    def checkPathGlobal(self, filePath):
        """Checks if given path is on shared storage.
        Returns [Bool] formatted string
        """

        path = os.path.dirname(filePath)
        logger.debug("checkPathGlobal for {}".format(path))

        # Only the "dlm" creates the file.  All nodes then try to to "see" the file.
        if self.cluster.is_master():
            self.makeTestFile(path)

        self.cluster.barrier()

        # Create a file whose name is based on whether or not the "dlm"
        # created file is visible.

        # Communicate the results.
        if self.cluster.is_master():
            isGlobal = True
            for nodeId in range(0, self.numNodes - 1):
                nodePathVisible = self.cluster.recv_msg()
                if not nodePathVisible:
                    isGlobal = False

            # Now send the endResult to all in a fanout
            self.cluster.broadcast_msg(isGlobal)

            # set DLM node's state correctly too
            self.sharedStorage = isGlobal
            self.cleanTestFile(path)
        else:
            # assume that the app is single-instance so nodeId/xpuId
            # are inter-changeable: i.e. nodeId X has only one XPU with Id X
            # send my result to DLM node, then wait for result from DLM node
            # which collects results from all nodes, and sends out a final
            # result which is Global if all nodes sent it Global, otherwise
            # it'll send back Local
            pathVisible = self.checkTestFile(path)
            self.cluster.send_msg(self.cluster.master_xpu_id, pathVisible)

            # The master will respond with the final result
            self.sharedStorage = self.cluster.recv_msg()

        logger.debug("File {} {} on shared storage".format(
            filePath, "is" if self.sharedStorage else "is not"))

    def getFullPath(self, path):
        fileName = "xcWriteFileAllNodes-{}".format(self.seed)
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
        isFile = os.path.isfile(fullPath)
        logger.debug("checkTestFile on {} is {}".format(fullPath, isFile))
        return isFile

    def cleanTestFile(self, path):
        fullPath = self.getFullPath(path)
        try:
            os.remove(fullPath)
        except Exception:
            logging.exception("Failed to cleanup {}".format(path))

    def writeConfigFile(self, path, content):

        # Write the config file if this node is the "dlm" or the file is not on
        # shared storage (meaning it's local to each node).
        #
        if self.cluster.is_master() or (not self.sharedStorage):

            # Temp file is the one that will replace the current config file
            tempPath = "{}-{}".format(path, self.seed)

            with open(tempPath, 'w') as tempFile:
                tempFile.write(content)

            # The current config file will be kept around as <file>.backup
            # in case it's needed (e.g. primary file gets lost or corrupted).
            backupPath = "{}.backup".format(path)
            try:
                os.remove(backupPath)
            except Exception:
                pass

            if not self.sharedStorage:
                #
                # If self.sharedStorage is true, only DLM writes the content to
                # "path" which is on shared storage - the other instances exit
                # cleanly - so no need to barrier between the app instances.
                # Indeed, it'd be incorrect to do so, since the barrier
                # programming model allows only exactly N barrier participants
                # in a N-member group of app instances.
                #
                # If self.sharedStorage is false, all app instances including
                # the DLM write the content to "path" that's local to each
                # node, and hence visible only to the app instance - so all can
                # proceed concurrently - however, it'd be good to barrier here,
                # so all have had the chance to at least deposit their content
                # into their local nodes' tempPath above before proceeding to
                # attempt subsequent steps which may trigger exceptions,
                # killing the app, and therefore all app instances (before
                # each has finished writing the content to tempPath).
                #
                self.cluster.barrier()

            # Copy "current" to "backup" keeping "current" around in case bad
            # things happen.
            shutil.copy2(path, backupPath)

            logger.debug("Copied {} to {}".format(path, backupPath))

            # Rename "temp/new" to "current"
            try:
                os.rename(tempPath, path)
            except Exception:
                logger.debug("Failed to rename {} to {}".format(
                    tempPath, path))
                # Put the old file back
                try:
                    os.rename(backupPath, path)
                except Exception:
                    logger.debug("Failed to resurrect {} from {}".format(
                        path, backupPath))
                return

            logger.debug("Renamed {} to {}".format(tempPath, path))


def main(inBlob):
    logger.debug("Received input: {}".format(inBlob))
    ret = None
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)

    try:
        inObj = json.loads(inBlob)
        if inObj["func"] == "writeFileAllNodes":
            writeFile = WriteFileAllNodes(inObj["seed"])
            filePath = inObj["filePath"]
            fileContent = inObj["fileContent"]

            # filePath may be a symbolic link.  We want to use the "real"
            # path.
            if os.path.islink(filePath):
                realPath = os.path.realpath(filePath)
                logger.debug(
                    "Converting symbolic link {} to real path {}".format(
                        filePath, realPath))
            else:
                realPath = filePath

            writeFile.checkPathGlobal(realPath)

            # Determine if file is to be written and, if so, do it
            writeFile.writeConfigFile(realPath, fileContent)

        else:
            raise ValueError("Not implemented")

    except Exception:
        logger.exception(
            "Node {} caught writeFile exception".format(thisNodeId))
        raise

    return ret
