import json

from xcalar.compute.util.Qa import buildNativePath
from xcalar.compute.util.Qa import XcalarQaS3ClientArgs
from xcalar.compute.util.Qa import XcalarQaDatasetPath
from xcalar.compute.util.Qa import DefaultTargetName

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators

import pytest

testLoadSessionName = "TestLoad"

csvUdfName = "testCsvParser"
csvUdfSource = """
import csv
from codecs import getreader
Utf8Reader = getreader("utf-8")

def parse(fullName, inStream):
    utf8Stream = Utf8Reader(inStream)
    reader = csv.reader(utf8Stream, delimiter=',')
    colNames = next(reader)
    for row in reader:
        yield {colNames[ii]: v for ii,v in enumerate(row)}
"""

orderedDictUdfName = "testOrderedDictParser"
orderedDictUdfSource = """
import csv
from collections import OrderedDict
from codecs import getreader

Utf8Reader = getreader("utf-8")

def parse(fullName, inStream):
    utf8Stream = Utf8Reader(inStream)
    reader = csv.reader(utf8Stream, delimiter=',')
    colNames = next(reader)
    for row in reader:
        rowDict = OrderedDict()
        for i,v in enumerate(row):
            rowDict[colNames[i]] = v
        yield rowDict
"""

memUdfName = "testLoadMem"
memUdfSource = """
import json
def loadSeq(fullPath, inStream, addedVal):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    addedVal = int(addedVal)
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {}
        if (ii == 0):
            rowDict['flaky'] = 1
        rowDict['val'] = ii + addedVal
        yield rowDict
    """

memSchemaUdfName = "testLoadMemSchema"
memSchemaUdfSource = """
import json
import ast
def loadSeq(fullPath, inStream, schema):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    schemaDecoded = ast.literal_eval(schema)
    cols = schemaDecoded['Cols']
    addedVal = schemaDecoded['AddedVal']
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {}
        rowDict['Val'] = ii + addedVal
        for k, v in cols.items():
            if v == "int":
                rowDict[k] = int(ii)
            elif v == "float":
                rowDict[k] = float(ii)
            elif v == "string":
                rowDict[k] = str(ii)
        yield rowDict
    """

paramUdfName = "testParameterizedUdf"
paramUdfSource = """
import csv
from codecs import getreader
Utf8Reader = getreader("utf-8")

def appendStr(fullPath, inStream, appendStr=""):
    utf8Stream = Utf8Reader(inStream)
    reader = csv.reader(utf8Stream, delimiter=',')
    for row in reader:
        yield {"col{}".format(ii): v+appendStr for ii,v in enumerate(row)}
    """

retOneSource = """
def onesOnLoad(fullPath, inStream):
    yield {'column0': 1}
"""

nonUnicodeUdfName = "testNonUnicodeSUdf"
nonUnicodeUdfSource = """
def nonUniValue(inp, ins):
    yield {"key": b"bad\\x8fkey"}

def nonUniKey(inp, ins):
    yield {b"bad\\x8fkey": "asdf"}

def nonUniNestedKey(inp, ins):
    yield {
        "key": {b"bad\\x8fkey": "asdf"}
    }

def nonUniNestedValue(inp, ins):
    yield {
        "key": {"innerkey": b"bad\\x8fvalue"}
    }
"""

excUdfName = "testExcSUdf"
excUdfSource = """
def raiseExc(inp, ins):
    raise ValueError("userexception")
    yield {}
"""

recErrsUdfName = "testRecErrorsSUdf"
recErrsUdfSource = """
def oddRecordsBadFields(inp, ins):
    for ii in range(100):
        if ii % 2 == 0:
            yield {"field": ii}
        else:
            yield {"invalid": bytes(1)} # bytes is an invalid type for a record
"""

udfs = [
    ("ones", retOneSource),
    (csvUdfName, csvUdfSource),
    (orderedDictUdfName, orderedDictUdfSource),
    (memUdfName, memUdfSource),
    (memSchemaUdfName, memSchemaUdfSource),
    (paramUdfName, paramUdfSource),
    (nonUnicodeUdfName, nonUnicodeUdfSource),
    (excUdfName, excUdfSource),
    (recErrsUdfName, recErrsUdfSource),
]

s3MinioTargetName = "QA S3 minio"
memoryTargetName = "QA memory"
hdfsTargetName = "QA internal hdfs"
sharedQaTargetName = "QA shared target"
maprQaTargetName = "QA secured MapR"
unsharedSymmTargetName = "QA unshared symmetric"
unsharedSingleNodeTargetName = "QA unshared single node"
targets = [
    (s3MinioTargetName, "s3fullaccount", {
        "access_key": XcalarQaS3ClientArgs["aws_access_key_id"],
        "secret_access_key": XcalarQaS3ClientArgs["aws_secret_access_key"],
        "endpoint_url": XcalarQaS3ClientArgs["endpoint_url"],
        "region_name": XcalarQaS3ClientArgs["region_name"],
        "verify": "true" if XcalarQaS3ClientArgs["verify"] else "false",
    }),
    (memoryTargetName, "memory", {}),
    #    (hdfsTargetName,
    #     "unsecuredhdfs",
    #     {
    #         "hostname": hdfsHost,
    #         "port": "",
    #     }),
    (sharedQaTargetName, "shared", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (maprQaTargetName, "maprfullaccount", {
        "username": "dwillis",
        "password": "dwillis",
        "cluster_name": "mrtest",
    }),
    (unsharedSymmTargetName, "sharednothingsymm", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (unsharedSingleNodeTargetName, "sharednothingsingle", {}),
]


@pytest.fixture(scope="module")
def setup(client):
    for target_name, target_type_id, target_args in targets:
        client.add_data_target(target_name, target_type_id, target_args)

    xcalarApi = XcalarApi()
    workbook = client.create_workbook("load_test")
    xcalarApi.setSession(workbook)

    op = Operators(xcalarApi)

    for udfName, udfSource in udfs:
        workbook.create_udf_module(udfName, udfSource)

    yield (client, workbook, xcalarApi, op)

    # cleanup
    for target_name, target_type_id, target_args in targets:
        client.get_data_target(target_name).delete()

    workbook.delete()
    xcalarApi.setSession(None)


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
])
def test_dataset_create(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, op) = setup
    path = pathBuilder("jsonSanity/dirwithemptyfiles/")
    dataset_builder = workbook.build_dataset(
        "DatasetCreate1", targetName, path, "json", file_name_pattern='*')
    dataset_builder2 = workbook.build_dataset(
        "DatasetCreate2", targetName, path, "json", file_name_pattern='*')
    dataset_builder.dataset_create()
    dataset_builder2.dataset_create()
    dataset = dataset_builder.load()
    dataset2 = dataset_builder2.load()
    meta = dataset_builder.dataset_getMeta()
    meta2 = dataset_builder2.dataset_getMeta()
    dataset_builder.dataset_unload()
    # should fail as dataset is still loaded
    dataset_builder2.dataset_delete()
    dataset_builder2.dataset_unload()
    dataset_builder.dataset_delete()
    dataset_builder2.dataset_delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
])
def test_dataset_create_sudf(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, op) = setup
    path = pathBuilder("csvSanity/comma.csv")
    sudfArgs = {"appendStr": "appStr"}
    appStr = "appStr"
    udfObj = workbook.get_udf_module(paramUdfName)
    udfSource = udfObj._get()

    # Get the full path of the UDF in the workbook.  The full path name must be
    # specified in the load args.
    fullUdfPath = None
    udfmods = workbook.list_udf_modules("*")
    for um in udfmods:
        mxdfs = um._list("*")
        for ii in range(mxdfs.numXdfs):
            fnName = mxdfs.fnDescs[ii].fnName
            if "workbook" in fnName and paramUdfName in fnName and "appendStr" in fnName:
                fullUdfPath = fnName
                break

    assert (fullUdfPath is not None)

    dataset_builder3 = workbook.build_dataset(
        "paramedUdfDs",
        targetName,
        path,
        "udf",
        parser_name=fullUdfPath,
        parser_args=sudfArgs)
    dataset_builder3.dataset_create()

    # Delete the UDF from the workbook.  When the dataset meta is created
    # a copy of the UDF module is copied into /sharedUDFs and will be used
    # when the dataset is loaded
    udfObj.delete()

    # Get the dataset meta and ensure the parser name is relative
    meta = dataset_builder3.dataset_getMeta()
    metaJson = json.loads(meta.datasetMeta)
    assert (metaJson["args"]["loadArgs"]["parseArgs"]["parserFnName"] ==
            paramUdfName + ":appendStr")

    # Now load the dataset
    dataset3 = dataset_builder3.load()

    # Ensure the dataset was loaded
    rows = dataset3.records()
    rowCount = 0
    for row in rows:
        print(row)
        rowCount += 1
    assert (rowCount == 2)
    assert dataset3.record_count() == 2

    # Clean up
    dropResult = op.dropDatasets(".XcalarDS.paramedUdfDs")
    dataset_builder3.dataset_delete()
