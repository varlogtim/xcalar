#!/usr/bin/env python3.6
# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# This program fetches performance statistics from a Xcalar cluster and writes
# them to CSV files.
#
import time
import argparse
import calendar
import logging
import os
from xcalar.compute.util.system_stats_util import GetTopStats

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-5s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--datasetdir")
parser.add_argument("--interval", nargs='?', type=int, default=1)
parser.add_argument("--nodeId", type=int, default=0)
parser.add_argument("--getFromAllNodes", action='store_true')
parser.add_argument("--numstatstofetch", nargs='?', type=int, default=100)
parser.add_argument("--filesperdir", nargs='?', type=int, default=500)
args = parser.parse_args()

if (args.datasetdir == None):
    logger.error("Must specify '--datasetdir path' to tell where to write the CSV stat files");
    exit(1);

if (not os.path.exists(args.datasetdir)):
    os.makedirs(args.datasetdir)


nodeID = args.nodeId
getFromAllNodes = args.getFromAllNodes

if (not os.path.exists(args.datasetdir)):
    os.makedirs(args.datasetdir)

fileCount = 0
dirValue = calendar.timegm(time.gmtime())
dirName = str(dirValue)
os.makedirs(args.datasetdir + "/" + dirName)

for i in range(0, args.numstatstofetch):
    wakeupTime = time.time()
    fileCount += 1
    if (fileCount == args.filesperdir):
        dirValue += 1
        dirName = str(dirValue)
        os.makedirs(args.datasetdir + "/" + dirName)
        fileCount = 0

    curTimeStr = str(calendar.timegm(time.gmtime()))
    path = args.datasetdir + "/" + dirName + "/_" + curTimeStr

    class StatsFile:
        def __enter__(self):
            self.statsFile = open(path, "w")
            return self.statsFile
        def __exit__(self, type, value, traceback):
            self.statsFile.close()

    failureMessage = None
    with StatsFile() as statsFile:
        try:
            content = GetTopStats().get_top_stats(node_id=nodeID,
                                                  get_from_all_nodes=getFromAllNodes,
                                                  is_json_format=False)
            statsFile.write(content)
            finalPath = args.datasetdir + "/" + dirName + "/" + curTimeStr
            os.rename(path, finalPath)
            logger.info("Wrote stats to file " + finalPath)
        except:
            os.remove(path)
            raise
    timeToSleep = args.interval - (time.time() - wakeupTime)
    if (timeToSleep > 0):
        time.sleep(timeToSleep)
