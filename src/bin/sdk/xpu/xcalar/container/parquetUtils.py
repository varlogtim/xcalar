# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
parquetUtils contains the following utilities:
1) An app that can detect the partition keys based on the directory
   layout of a parquet datasets and return the schema of the Parquet Dataset
   (https://arrow.apache.org/docs/python/parquet.html)

2) A standalone python program that can parse the output of
   parquet-tools.jar or parquet natively with pyArrow

3) A new parseParquet streaming UDF

To use it as a standalone program:
python scripts/parquetUtils/parquetUtils.py  --help
usage: parquetUtils.py [-h] --path PATH --mode {cat,schema,catJson}
                           [--numrows NUMROWS] [--usepyarrow {0,1}]

Xcalar Parquet Utils

optional arguments:
  -h, --help            show this help message and exit
  --path PATH, -p PATH  Path to Parquet file
  --mode {cat,schema,catJson}, -m {cat,schema,catJson}
  --numrows NUMROWS, -n NUMROWS
                            Number of rows to display

Please note that you need parquet-tools-1.8.2.jar available in $XLRDIR/bin
in order for the above to work.
Usage: Executed within the Xcalar Data Platform
"""

import sys
import os
import json
import logging
import logging.handlers
import argparse
import importlib
from socket import gethostname

from xcalar.container.parquetParser.base import ParseException

parquetParser = None

parquetParsers = {
    "pyArrow": ("pyArrowParquetParser", "PyArrowParquetParser"),
    "parquetTools": ("parquetToolsParser", "ParquetToolsParser")
}


def getParquetParser(parquetParserIn, logger):
    if parquetParserIn is None or parquetParserIn == "" or parquetParserIn == "default":
        parquetParserIn = "pyArrow"

    parserAttributes = parquetParsers.get(parquetParserIn, None)
    if parserAttributes is None:
        raise ValueError("No such parquet parser {}".format(parquetParserIn))

    modName = "xcalar.container.parquetParser.{}".format(parserAttributes[0])
    parserModule = importlib.import_module(modName)
    parserConstructor = getattr(parserModule, parserAttributes[1])

    parser = parserConstructor(parquetParserIn, logger)
    if parser.isAvailable():
        return parser
    else:
        raise ValueError(
            "Parquet parser {} is not available".format(parquetParserIn))


def findPartitionKeysFromPath(path):
    head, tail = os.path.split(path)
    allPartitions = {}
    while head:
        head, tail = os.path.split(head)
        key_value = tail.split("=")
        if len(key_value) == 2:
            allPartitions[key_value[0]] = key_value[1]
        else:
            break
    return allPartitions


def samePartitionKeysAsPath(partitionKeys, partitionKeysFromPath):
    for key in partitionKeys:
        if key not in partitionKeysFromPath:
            raise ParseException(
                "Partition key {} not part of file path".format(key))
        if len(partitionKeys[key]) == 0 or "*" in partitionKeys[key]:
            # Empty array means all values acceptable
            continue
        if partitionKeysFromPath[key] not in partitionKeys[key]:
            return False
    return True


ctx = None
logger = None
appInited = False


def initApp():
    global ctx, FileLister, get_target_data, build_target, File, NativeConnector, logger, appInited
    import xcalar.container.context as ctx
    from list_files_app import FileLister
    from xcalar.container.target.manage import get_target_data, build_target
    from xcalar.container.connectors.util import File
    from xcalar.container.connectors.native import NativeConnector

    logger = logging.getLogger('Parquet App Logger')
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Logging is not multi-process safe, so stick to stdout
        thisXpuId = ctx.get_xpu_id()
        thisNodeId = ctx.get_node_id(thisXpuId)
        logHandler = logging.StreamHandler(sys.stdout)
        numNodes = ctx.get_node_count()

        formatter = logging.Formatter(
            '%(asctime)s - Node {} - Parquet App - %(levelname)s - %(message)s'
            .format(thisNodeId))
        logHandler.setFormatter(formatter)

        logger.addHandler(logHandler)

        logger.debug(
            "Parquet app initialized; nodeId:{}, numNodes:{}, hostname:{}".
            format(thisNodeId, numNodes, gethostname()))

    appInited = True


try:
    import xcalar.container.context as ctx
    if ctx._is_app():
        initApp()
except (ImportError, ValueError) as e:
    ctx = None

try:
    parquetParser = getParquetParser("pyArrow", logger)
except ValueError:
    parquetParser = getParquetParser("parquetTools", logger)


def parseParquet(inputPath,
                 inputStream,
                 mode="cat",
                 columns=None,
                 capitalizeColumns=False,
                 partitionKeys=None,
                 parquetParserIn=None):

    global parquetParser

    if not appInited:
        initApp()

    if parquetParserIn is not None and (
            parquetParser is None or parquetParser.name() != parquetParserIn):
        parquetParser = getParquetParser(parquetParserIn, logger)

    appContext = ctx._get_app_context()
    currTarget = appContext.get("currTarget", None)
    parquetAppContext = appContext.get("parquetAppContext", None)
    dataset_root = None
    relPath = None

    if currTarget is not None:
        connector = getattr(currTarget, "connector", None)
        dataset_root = getattr(currTarget, "dataset_root", None)
    elif parquetAppContext is not None:
        currTarget = parquetAppContext["target"]
        connector = getattr(parquetAppContext["target"], "connector", None)
    else:
        raise ValueError("App Context not properly set")

    partitionKeysFromPath = findPartitionKeysFromPath(inputPath)
    if partitionKeys and not samePartitionKeysAsPath(partitionKeys,
                                                     partitionKeysFromPath):
        return
    else:
        # Update partitionKeys struct from what the user wants to what the
        # path actually contains. Since we already checked previously to
        # ensure that the file is what the user wants, we can remove the
        # unneeded other values for keys
        partitionKeys = partitionKeysFromPath

    if len(partitionKeys) == 0:
        partitionKeys = None

    try:
        if currTarget is None or connector is None or not isinstance(
                connector, NativeConnector):
            # Unfortunately, parseParquet is not a streaming function and hence this will cause the entire inputStream to be loaded into memory
            yield from parquetParser.parseParquet(
                inputPath=None,
                inputStream=inputStream,
                mode=mode,
                columns=columns,
                capitalizeColumns=capitalizeColumns,
                partitionKeys=partitionKeys)
        else:
            # If it's native connector, we don't pass in the stream but instead pass in the path so that we can take advantage of memory_mapped files to avoid copying the entire file into memory
            yield from parquetParser.parseParquet(
                inputPath=connector.last_file_opened,
                inputStream=None,
                mode=mode,
                columns=columns,
                capitalizeColumns=capitalizeColumns,
                partitionKeys=partitionKeys)
    except Exception as e:
        logger.error(e)
        raise


def getPossibleKeyValues(targetName,
                         path,
                         key,
                         givenKeys={},
                         parquetParserIn=None):
    global parquetParser

    if not appInited:
        initApp()

    if parquetParserIn is not None and (
            parquetParser is None or parquetParser.name() != parquetParserIn):
        parquetParser = getParquetParser(parquetParserIn, logger)

    subDirs = ""

    possibleValues = []
    dirsToSearch = []

    dirsToSearch.append(".")
    targetData = get_target_data(targetName)

    while len(dirsToSearch) > 0:
        target = build_target(targetName, targetData,
                              os.path.join(path, subDirs))
        if not target.is_global():
            raise NotImplementedError(
                "Parquet is not supported on non-global file systems")

        subDirs = dirsToSearch.pop()

        f = FileLister(targetName)
        fileObjects = f.list_files(
            path=os.path.join(path, subDirs),
            name_pattern="re:[^=]+=[^=]+",
            recursive=False,
            as_dict=True)

        for fileObj in fileObjects['files']:
            if fileObj["isDir"]:
                dirName = fileObj["name"]
                (partitionKey, value) = dirName.split("=")
                if (partitionKey == key):
                    possibleValues.append(value)
                elif partitionKey in givenKeys and value in givenKeys[
                        partitionKey]:
                    dirsToSearch.append(os.path.join(subDirs, dirName))

    return json.dumps(list(set(possibleValues)))


def getInfo(targetName, path, parquetParserIn=None):
    global parquetParser

    if not appInited:
        initApp()

    if parquetParserIn is not None and (
            parquetParser is None or parquetParser.name() != parquetParserIn):
        parquetParser = getParquetParser(parquetParserIn, logger)

    partitionKeys = []
    subDirs = ""
    target = None
    isPartitioned = False
    validParquet = False
    schema = {}

    targetData = get_target_data(targetName)

    # Might have to search more than 1 dir to find the parquet file
    dirsToSearch = []

    # Check if partitioned dataset
    while True:
        target = build_target(targetName, targetData,
                              os.path.join(path, subDirs))
        if not target.is_global():
            raise NotImplementedError(
                "Parquet is not supported on non-global file systems")

        # Here we're making the assumption that every nested directory exhibit
        # the same hierarchy of partition keys, and so we can just pick any
        f = FileLister(targetName)
        fileObjects = f.list_files(
            path=os.path.join(path, subDirs),
            name_pattern="re:[^=]+=[^=]+",
            recursive=False,
            as_dict=True)

        dirName = None
        for fileObj in fileObjects['files']:
            if fileObj["isDir"]:
                dirName = fileObj["name"]
                isPartitioned = True
                break

        if dirName is None:
            if (isPartitioned):
                dirsToSearch.append(os.path.join(path, subDirs))
            break

        partitionKey = dirName.split("=")[0]
        partitionKeys.append(partitionKey)
        subDirs = os.path.join(subDirs, dirName)

    try:
        ctx._set_app_context({"parquetAppContext": {"target": target}})
        # Check if spark
        fileObjs = target.get_files(
            os.path.join(path, ""), "_metadata", False, mode="listFiles")
        if len(fileObjs) > 0:
            with target.open(fileObjs[0].path, "rb") as fp:
                try:
                    schema = parseParquet(
                        os.path.join(path, fileObjs[0].relPath),
                        fp,
                        mode="schema").__next__()
                except parquetParser.ParseException:
                    pass
        else:
            dirsToSearch.insert(0, os.path.join(path, ""))

            while len(dirsToSearch) > 0:
                dirToSearch = dirsToSearch.pop()
                fileObjs = target.get_files(
                    dirToSearch, "", False, mode="listFiles")
                for fileObj in fileObjs:
                    if fileObj.isDir:
                        dirsToSearch.append(
                            os.path.join(dirToSearch, fileObj.relPath))
                    else:
                        with target.open(fileObj.path, "rb") as fp:
                            try:
                                schema = parseParquet(
                                    os.path.join(dirToSearch, fileObj.relPath),
                                    fp,
                                    mode="schema").__next__()
                            except parquetParser.ParseException:
                                pass

                break
    except Exception as e:
        raise
    finally:
        ctx._set_app_context(None)

    unknownFields = []
    if len(schema) > 0:
        schema = parquetParser.convertToXcalarSchema(schema, unknownFields)
        validParquet = True

    # Unfortunately, partitionKeys are not saved as part of the parquet files, and hence we need to infer its type
    # We don't do any type inference, and just default to string, similar to setting
    # spark.sql.sources.partitionColumnTypeInference.enabled = false
    schema.update({
        key: {
            "xcalarType": "DfString",
            "parquetType": "string"
        }
        for key in partitionKeys
    })

    returnArgs = {
        "partitionKeys": partitionKeys,
        "schema": schema,
        "validParquet": validParquet
    }
    return json.dumps(returnArgs)


if __name__ == "__main__":
    argParser = argparse.ArgumentParser(description="Xcalar Parquet Utils")
    argParser.add_argument(
        '--path', '-f', help="Path to Parquet file", required=True)
    argParser.add_argument(
        '--mode', '-m', choices=["cat", "schema", "catJson"], required=True)
    argParser.add_argument(
        '--numrows',
        '-n',
        help="Number of rows to display",
        required=False,
        type=int)
    argParser.add_argument(
        '--parquetParser', '-p', choices=parquetParsers.keys())

    args = argParser.parse_args()
    path = args.path
    mode = args.mode

    if args.parquetParser is not None and (
            parquetParser is None
            or parquetParser.name() != args.parquetParser):
        parquetParser = getParquetParser(args.parquetParser, logger)

    if mode == "schema":
        unknownFields = []

        schema = parquetParser.parseParquet(path, None, "schema",
                                            []).__next__()
        print(
            json.dumps(
                parquetParser.convertToXcalarSchema(schema, unknownFields)))
        print(json.dumps(unknownFields))

    else:
        if args.numrows is None:
            for line in parquetParser.parseParquet(path, None, mode):
                print(json.dumps(line))
        else:
            generator = parquetParser.parseParquet(path, None, mode)
            for _ in range(args.numrows):
                line = generator.__next__()
                print(line)
                print("\n")
