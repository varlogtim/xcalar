#!/bin/env python3.6
# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# This script uploads and executes a single retina in a Xcalar cluster. It uses
# execRetina.py as a helper script. The following describes the actions
# across both scripts.
#
# Inputs:
#  - cluster login info: node name, user, password (latter two for remote)
#  - flag indicating reomte or local - remote
#  - flag indicating if the retina execution will build a source table or not
#    (this is if the retina execution is part of the LOAD step) - gensrctabs
#  - path to the retina - dataflow
#  - path to dataset (if gensrctabs is true) - datasetpath
#  - dir in which results will be archived - resultsdir
# Process:
#  - upload retina
#  - run retina
#  - get operation breakdown from retina run
# Outputs:
#  - Summary of retina execution timing sent to standard output
#  - Operation breakdown and summary written to ./results/run-x directory
#      (where x is run number)

import subprocess
import argparse
import json
import time
import logging
import random
import os

# change level to logging.DEBUG for debug log messages
logging.basicConfig(level=logging.WARN,
                    format='[%(asctime)s|%(levelname)s]: %(message)s')


class BenchRunner():
    def __init__(self, nodename, user, password, dataflowPath, datasetPath,
            srcT, resultsDir, remote):
        self.user = user
        self.password = password
        self.nodename = nodename
        self.tmpDir = "/tmp/bench-{}".format(random.randint(0, 2**40))
        self.tmpDataflow = os.path.join(self.tmpDir,
                                           os.path.basename(dataflowPath))
        self.tmpScript = os.path.join(self.tmpDir, "execRetina.py")
        self.tmpResultsFile = os.path.join(self.tmpDir, "results.out")
        self.dataflowPath = dataflowPath
        self.datasetPath = datasetPath
        self.resultsDir = resultsDir
        self.remote = remote
        self.localResultsFile = self.resultsDir + "/" +    \
            os.path.basename(dataflowPath) + ".out"
        self.genSrcTabs = srcT

    def runCmd(self, cmd, captureOut=True):
        if self.remote == "true":
            fullCmd = [
                "sshpass", "-p", self.password,
                "ssh",
                "{user}@{node}".format(user=self.user, node=self.nodename),
            ] + cmd
        else:
            fullCmd = cmd
        logging.debug(" ".join(fullCmd))
        outType = subprocess.PIPE if captureOut else None
        pr = subprocess.Popen(fullCmd,
                              stdout=outType, stdin=subprocess.PIPE)
        time.sleep(1)
        out = pr.communicate()[0]
        if pr.returncode != 0:
            raise ValueError("failed to execute '{}'".format(" ".join(fullCmd)))
        return out

    def copy(self, localPath, tmpPath, out=True):
        if out is True:
            if self.remote == "true":
                fullCmd = [ "sshpass", "-p", self.password, "scp", localPath,
                    "{user}@{node}:{tmpPath}".format(user=self.user,
                    node=self.nodename, tmpPath=tmpPath),
                ]
            else:
                fullCmd = [ "cp", localPath, tmpPath ]
        else:
            if self.remote == "true":
                fullCmd = [ "sshpass", "-p", self.password, "scp",
                    "{user}@{node}:{tmpPath}".format(user=self.user,
                    node=self.nodename, tmpPath=tmpPath), localPath,
                ]
            else:
                fullCmd = [ "cp", tmpPath, localPath ]
        logging.debug(" ".join(fullCmd))
        pr = subprocess.Popen(fullCmd,
                              stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        time.sleep(1)
        out = pr.communicate()[0]
        if pr.returncode != 0:
            raise ValueError("failed to connect to node")
        return out

    def run(self):
        logging.info("Using directory '{}' with genSrcTabs set to '{}'".
                format(self.tmpDir, self.genSrcTabs))
        self.runCmd(["mkdir", "-p", self.tmpDir])
        self.runCmd(["whoami"])
        self.copy(self.dataflowPath, self.tmpDataflow)
        self.copy("execRetina.py", self.tmpScript)
        self.runCmd(["/opt/xcalar/bin/python3.6",
                 self.tmpScript, "-d", self.tmpDataflow, "-s", self.datasetPath,
                 "-g", self.genSrcTabs, "-r", self.tmpResultsFile],
                 captureOut=False)
        self.copy(self.localResultsFile, self.tmpResultsFile, out=False)
        # cleanup the temp files
        self.runCmd(["rm", "-rf", self.tmpDir])

def main():
    parser = argparse.ArgumentParser(
            description="Run retina on Xcalar cluster")
    parser.add_argument("-n", "--nodename",
                        required=True,
                        help="nodename of a node in the cluster")
    parser.add_argument("-u", "--user",
                        required=True,
                        help="user with which to ssh to the cluster")
    parser.add_argument("-p", "--password",
                        required=True,
                        help="password with which to ssh to the cluster")
    parser.add_argument("-b", "--dataflow",
                        required=True,
                        help="path to batch dataflow")
    parser.add_argument("-s", "--datasetpath",
                        required=True,
                        help="dirpath to dataset on cluster")
    parser.add_argument("-g", "--gensrctabs",
                        required=True,
                        help="boolean flagging if this is a run to build srctabs")
    parser.add_argument("-d", "--resultsdir",
                        required=True,
                        help="dir in which results must be archived")

    parser.add_argument("-r", "--remote",
                        required=True,
                        help="remote cluster")

    args = parser.parse_args()

    runner = BenchRunner(
            args.nodename,
            args.user,
            args.password,
            args.dataflow,
            args.datasetpath,
            args.gensrctabs,
            args.resultsdir,
            args.remote)
    runner.run()


if __name__ == "__main__":
    main()
