# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# This program reads CSV files generated by get_stats.py that contain the performance
# statistics for a Xcalar cluster and writes them to an IMD table.
#

import shutil
import argparse
import tempfile

from xcalar.compute.util.Qa import DefaultTargetName
from xcalar.compute.util.cluster import *
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Dataset import *
from xcalar.external.LegacyApi.Operators import *
from xcalar.external.external.result_set import ResultSet
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException

def loadDataset(xcalarApi, filePath, datasetName):
    dataset = CsvDataset(xcalarApi, DefaultTargetName, filePath, datasetName, fieldDelim=',', schemaMode="header")
    logger.info("Creating dataset " + datasetName + " from " + filePath)
    dataset.load()
    return dataset

def dumpImdTable(xcalarApi, imdTableName, dumpContents):
    op = Operators(xcalarApi)
    op.select(imdTableName, "T1")
    resultSet = ResultSet(client, table_name="T1")
    logger.info("%s has %d rows", imdTableName, resultSet.record_count())
    if (dumpContents):
        for rec in resultSet.record_iterator(num_rows=5000):
            logger.info(rec)
    op.dropTable("T1")

def createTable(xcalarApi, datasetName):
    rt = datasetName
    datasetName = ".XcalarDS." + datasetName
    op = Operators(xcalarApi)

    #
    # TODO Handle errors in these operations.  Not sure what we can actually do besides exit
    # the script.
    #
    ii = 0
    op.indexDataset(datasetName, "%s%d" % (rt, ii),
                    ["node", "timestamp", "name", "units", "maxName", "value"], fatptrPrefixName = "p")


    #
    # sql can't deal with columns with "p-" in the name so create new columns without the "p-".  I can't
    # do p-value because I don't know how to cast it (could be float or int)
    #
    ii = ii + 1
    op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
           ["int(p-node)", "int(p-timestamp)", "string(p-name)", "string(p-units)", "string(p-maxName)"],
           ["node", "timestamp", "name", "units", "maxName"])

    ii = ii + 1
    op.project("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["node", "timestamp", "name", "units", "maxName", "p-value"])

    ii = ii + 1
    op.map("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii),
           ["int(1)", "int(1)"], ["XcalarOpCode", "XcalarRankOver"])

    ii = ii + 1
    tableName = "%s%d" % (rt, ii)
    op.indexTable("%s%d" % (rt, ii - 1), "%s%d" % (rt, ii), ["node", "timestamp", "name"])
    return tableName

def createOrUpdateIMDTable(xcalarApi, imdTableName, dirPath, datasetName):
    dataset = loadDataset(xcalarApi, dirPath, datasetName)
    createdTableName = createTable(xcalarApi, datasetName)
    op = Operators(xcalarApi)
    try:
        op.update([createdTableName], [imdTableName])
    except XcalarApiStatusException as e:
        if (e.status != StatusT.StatusPubTableNameNotFound):
            raise e
        logger.warning("Creating IMD table %s", imdTableName)
        op.publish(createdTableName, imdTableName)

    op.dropTable("*")

    dataset.delete()

def moveFiles(srcDirPath, dstDirPath, fileList):
    for f in fileList:
        shutil.move(os.path.join(srcDirPath, f), os.path.join(dstDirPath, f))

parser = argparse.ArgumentParser()
parser.add_argument("--datasetdir")
parser.add_argument("--loadeddatasetdir")
parser.add_argument("--imdtable")
parser.add_argument("--pollinterval")
parser.add_argument("--logupdatedtablesize", action="store_true")
parser.add_argument("--datasetname")
args = parser.parse_args()

timestamp = str(int(time.time()))

if (args.datasetdir == None):
    logger.warning("Must specify '--datasetdir path' to tell where to read in the stats files")
    exit(1)

if (args.datasetname == None):
    datasetName = "StatsDS" + timestamp
else:
    datasetName = args.datasetname

if (not os.path.exists(args.datasetdir)):
    os.makedirs(args.datasetdir)

if (args.loadeddatasetdir):
    datasetLoadedDir = args.loadeddatasetdir
else:
    datasetLoadedDir = args.datasetdir + '_loaded'

if (not os.path.exists(datasetLoadedDir)):
    os.makedirs(datasetLoadedDir)

if (args.imdtable == None):
    logger.warning("Must specify '--imdtable table-name' to tell the name of the IMD table")
    exit(1)

imdTableName = args.imdtable
client = Client()
xcalarApi = XcalarApi()
#
# TODO It would be nice not to create a new session and workbook each time.  One option is to
# lists sessions and workbooks for statsFlow.py and clean them up.
#
sesName = "StatsFlow_" + timestamp
wkbook = client.create_workbook(sesName)
wkbook.activate()
xcalarApi.setSession(wkbook)

op = Operators(xcalarApi)
try:
    op.publishTableChangeOwner(imdTableName, sesName, client.username)
except XcalarApiStatusException as e:
    if (e.status != StatusT.StatusPubTableNameNotFound):
        raise e

while (True):
    statsDirList = os.listdir(args.datasetdir)
    if (len(statsDirList) > 0):
        timestamp = str(int(time.time()))
        dirIndex = 0
        for statsDirName in sorted(statsDirList):
            statsDirPath = args.datasetdir + '/' + statsDirName
            _statsFileList = sorted(os.listdir(statsDirPath))
            statsFileList = []
            for f in _statsFileList:
                if not f.startswith('_'):
                    statsFileList.append(f)
            if (len(statsFileList) > 0):
                loadedDirPath = datasetLoadedDir + '/' + statsDirName
                if (not os.path.exists(loadedDirPath)):
                    os.makedirs(loadedDirPath)
                loadedDirPath = loadedDirPath + '/' + timestamp
                os.makedirs(loadedDirPath)
                moveFiles(statsDirPath, loadedDirPath, statsFileList)
                createOrUpdateIMDTable(xcalarApi, imdTableName, loadedDirPath, datasetName)
            if (dirIndex < len(statsDirList) - 1):
                #
                # Remove old directories.  The directories should be empty but there could be an old
                # file that starts with an '_' if get_stats.py was killed in the middle of generating
                # a stats file.
                #
                shutil.rmtree(statsDirPath)
            dirIndex += 1
        if (args.logupdatedtablesize):
            dumpImdTable(xcalarApi, imdTableName, False)

    if (args.pollinterval == None):
        dumpImdTable(xcalarApi, imdTableName, True)
        exit(0)
    time.sleep(int(args.pollinterval))
