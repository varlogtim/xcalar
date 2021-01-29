import os
from datetime import datetime, timezone
import time
import tempfile

from xcalar.compute.util.Qa import (datasetCheck, buildNativePath, buildS3Path,
                                    buildGcsPath, buildAzBlobPath)
from xcalar.compute.util.Qa import XcalarQaS3ClientArgs
from xcalar.compute.util.Qa import XcalarQaDatasetPath, XcalarQaHDFSHost
from xcalar.compute.util.Qa import DefaultTargetName
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiColumnT

from xcalar.external.LegacyApi.XcalarApi import (XcalarApi,
                                                 XcalarApiStatusException)
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.compute.util.config import detect_config
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.result_set import ResultSet
from xcalar.external.dataflow import Dataflow

import pytest

testLoadSessionName = "TestLoad"

hdfsHost = "hdfs-sanity"
if XcalarQaHDFSHost:
    hdfsHost = XcalarQaHDFSHost

hdfsCrimePath = "/datasets/qa/nycCrimeSmall"

csv_udf_name = "testCsvParser"
csv_udf_source = """
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

ordered_dict_udf_name = "testOrderedDictParser"
ordered_dict_udf_source = """
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

mem_udf_name = "testLoadMem"
mem_udf_source = """
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

mem_schema_udf_name = "testLoadMemSchema"
mem_schema_udf_source = """
import json
import ast
from datetime import datetime
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
            elif v == "datetime":
                rowDict[k] = datetime.fromtimestamp(int(ii))
        yield rowDict
    """

mem_schema_nested_udf_name = "testLoadMemSchemaNested"
mem_schema_nested_udf_source = """
import json
import ast
from datetime import datetime
def loadSeq(fullPath, inStream, schemaNested):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    schemaNestedDecoded = ast.literal_eval(schemaNested)
    cols = schemaNestedDecoded['Nested']['Cols']
    addedVal = schemaNestedDecoded['Nested']['AddedVal']
    for ii in range(startNum, startNum+numLocalRows):
        rowDict = {}
        rowDictNest = {}
        rowDict['Val'] = ii + addedVal
        for k, v in cols.items():
            if v == "int":
                rowDict[k] = int(ii)
            elif v == "float":
                rowDict[k] = float(ii)
            elif v == "string":
                rowDict[k] = str(ii)
            elif v == "stringEmpty":
                rowDict[k] = {}
                rowDict[k]['ColStringEmpty2'] = None
            elif v == "datetime":
                rowDict[k] = datetime.fromtimestamp(int(ii))
        rowDictNest['Nested'] = rowDict
        yield rowDictNest
    """

param_udf_name = "testParameterizedUdf"
param_udf_source = """
import csv
from codecs import getreader
Utf8Reader = getreader("utf-8")

def appendStr(fullPath, inStream, appendStr=""):
    utf8Stream = Utf8Reader(inStream)
    reader = csv.reader(utf8Stream, delimiter=',')
    for row in reader:
        yield {"col{}".format(ii): v+appendStr for ii,v in enumerate(row)}
    """

# This UDF has correct syntax but its reference to the java command is bad
# This should of course fail during execution, and generate a stack trace which
# has this module's name in the stack trace - as the test validates
bad_udf_name = "badUDF"
bad_udf_source = """
import os
import subprocess

def stream(inp, ins):
    command = ["xjava", "-jar", "", "cat", inp]
    pr = subprocess.Popen(command, stdout=subprocess.PIPE)
    for line in ins:
        yield {'dummyCol': line.decode('utf-8')}
"""

ret_one_source = """
def onesOnLoad(fullPath, inStream):
    yield {'column0': 1}
"""

non_unicode_udf_name = "testNonUnicodeSUdf"
non_unicode_udf_source = """
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

exc_udf_name = "testExcSUdf"
exc_udf_source = """
def raiseExc(inp, ins):
    raise ValueError("userexception")
    yield {}
"""

rec_errs_udf_name = "testRecErrorsSUdf"
rec_errs_udf_source = """
def oddRecordsBadFields(inp, ins):
    for ii in range(100):
        if ii % 2 == 0:
            yield {"field": ii}
        else:
            yield {"invalid": bytes(1)} # bytes is an invalid type for a record
"""

random_order_udf_name = "testRecsRandomOrder"
random_order_udf_source = """
import json
import random
from decimal import Decimal

import xcalar.container.cluster

def load_rand_fields(full_path, in_stream, col_count_multiplier=40):
    in_obj = json.loads(in_stream.read())
    num_rows = in_obj["numRows"]
    start_num = in_obj["startRow"]

    num_cols = 5 * col_count_multiplier
    header = [f'col_{x}' for x in range(1, num_cols+1)] + ['recPerFileYielded', 'filePathYielded']
    indices = list(range(len(header)))
    idx = 1
    for x in range(start_num, start_num+num_rows):
        random.shuffle(indices)
        data = [x, x*1.11, x%2==0, str(x), Decimal(x)] * col_count_multiplier + [idx, full_path]
        yield {header[ii]: data[ii] for ii in indices}
        idx += 1
"""

udfs = [
    ("ones", ret_one_source),
    (csv_udf_name, csv_udf_source),
    (ordered_dict_udf_name, ordered_dict_udf_source),
    (mem_udf_name, mem_udf_source),
    (mem_schema_udf_name, mem_schema_udf_source),
    (mem_schema_nested_udf_name, mem_schema_nested_udf_source),
    (param_udf_name, param_udf_source),
    (non_unicode_udf_name, non_unicode_udf_source),
    (exc_udf_name, exc_udf_source),
    (rec_errs_udf_name, rec_errs_udf_source),
    (bad_udf_name, bad_udf_source),
    (random_order_udf_name, random_order_udf_source)
]

shared_qa_target_name = "QA shared target"
s3_minio_target_name = "QA S3 minio"
gcs_target_name = "QA GCS"
azblob_target_name = "QA azblob"
memory_target_name = "QA memory"
hdfs_target_name = "QA internal hdfs"
mapr_qa_target_name = "QA secured MapR"
unshared_symm_target_name = "QA unshared symmetric"
unshared_single_node_target_name = "QA unshared single node"
target_infos = [
    (s3_minio_target_name, "s3fullaccount", {
        "access_key": XcalarQaS3ClientArgs["aws_access_key_id"],
        "secret_access_key": XcalarQaS3ClientArgs["aws_secret_access_key"],
        "endpoint_url": XcalarQaS3ClientArgs["endpoint_url"],
        "region_name": XcalarQaS3ClientArgs["region_name"],
        "verify": "true" if XcalarQaS3ClientArgs["verify"] else "false",
    }),
    (gcs_target_name, "gcsenviron", {}),
    (
        azblob_target_name,
        "azblobfullaccount",
        {
            "account_name": "xcdatasetswestus2",
            "sas_token": ""    # Fill this in with a sas_token to test
        }),
    (memory_target_name, "memory", {}),
    #    (hdfs_target_name,
    #     "unsecuredhdfs",
    #     {
    #         "hostname": hdfsHost,
    #         "port": "",
    #     }),
    (shared_qa_target_name, "shared", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (mapr_qa_target_name, "maprfullaccount", {
        "username": "dwillis",
        "password": "dwillis",
        "cluster_name": "mrtest",
    }),
    (unshared_symm_target_name, "sharednothingsymm", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (unshared_single_node_target_name, "sharednothingsingle", {}),
]

ENABLE_CLOUD_TESTS = False

# We're going to run the edge case tests on these targets
standard_targets = [
    (shared_qa_target_name, (lambda x: x)),
    (s3_minio_target_name, buildS3Path),
    pytest.param(
        gcs_target_name,
        buildGcsPath,
        marks=pytest.mark.skipif(
            not ENABLE_CLOUD_TESTS,
            reason="jenkins running cloud tests is expensive")),
    pytest.param(
        azblob_target_name,
        buildAzBlobPath,
        marks=pytest.mark.skipif(
            not ENABLE_CLOUD_TESTS,
            reason="jenkins running cloud tests is expensive")),
]

# csv_load_cases := (path, datasetArgs, expected_rows)
csv_load_cases = [
    (    # path to a directory, filename to a specific file
        "csvSanity/",
        {
            "fileNamePattern": "comma.csv",
            "fieldDelim": ",",
            "schemaMode": "none",
            "isRecursive": False,
        },
        [
            {
                u'column0': u'Comma',
                u'column1': u'Separated',
                u'column2': u'Columns'
            },
            {
                u'column0': u'Newline',
                u'column1': u'Separated',
                u'column2': u'Rows'
            },
        ],
    ),
    (    # Have a path ending in '/' to catch Xc-4293
        "edgeCases/recurSimple/",
        {
            "fieldDelim": ",",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'1',
                u'col2': u'2'
            },
            {
                u'col1': u'3',
                u'col2': u'4'
            },
            {
                u'col1': u'5',
                u'col2': u'6'
            },
            {
                u'col1': u'7',
                u'col2': u'8'
            },
            {
                u'col1': u'9',
                u'col2': u'10'
            },
            {
                u'col1': u'11',
                u'col2': u'12'
            },
        ],
    ),
    (    # Base case for TSV; recursive
        "edgeCases/recurTsv/",
        {
            "fileNamePattern": "*.tsv",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'A': u'1',
                u'B': u'2',
                u'C': u'3'
            },
            {
                u'A': u'18',
                u'B': u'18',
                u'C': u'18'
            },
            {
                u'A': u'two',
                u'B': u'two',
                u'C': u'two uncompressed-2'
            },
            {
                u'A': u'1',
                u'B': u'2',
                u'C': u'3'
            },
            {
                u'A': u'18',
                u'B': u'18',
                u'C': u'18-uncompressed1'
            },
            {
                u'A': u'1',
                u'B': u'2',
                u'C': u'3'
            },
            {
                u'A': u'18',
                u'B': u'18',
                u'C': u'18'
            },
            {
                u'A': u'two',
                u'B': u'two',
                u'C': u'two'
            },
            {
                u'A': u'333',
                u'B': u'333',
                u'C': u'333 uncompressed-3'
            },
            {
                u'A': u'1',
                u'B': u'2',
                u'C': u'3'
            },
            {
                u'A': u'18',
                u'B': u'18',
                u'C': u'18'
            },
            {
                u'A': u'two',
                u'B': u'two',
                u'C': u'two'
            },
            {
                u'A': u'3',
                u'B': u'3',
                u'C': u'3'
            },
            {
                u'A': u'4',
                u'B': u'4',
                u'C': u'4 uncompressed-4'
            },
        ],
    ),
    (    # Basic recursive, with filename. Should mimic `cd $path; find . -name '$pattern'`
        "edgeCases/recurFileNames/",
        {
            "fileNamePattern": "test.csv",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'0',
                u'col2': u'1'
            },
            {
                u'col1': u'8',
                u'col2': u'9'
            },
            {
                u'col1': u'14',
                u'col2': u'15'
            },
        ],
    ),
    (    # Recursive with wildcard. Should mimic `cd $path; find . -name '$pattern'`
        "edgeCases/recurFileNames/",
        {
            "fileNamePattern": "*test.csv",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'0',
                u'col2': u'1'
            },
            {
                u'col1': u'8',
                u'col2': u'9'
            },
            {
                u'col1': u'4',
                u'col2': u'5'
            },
            {
                u'col1': u'6',
                u'col2': u'7'
            },
            {
                u'col1': u'14',
                u'col2': u'15'
            },
            {
                u'col1': u'18',
                u'col2': u'19'
            },
        ],
    ),
    (    # Recursive with suffix wildcard
        "edgeCases/recurFileNames/",
        {
            "fileNamePattern": "test*",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'0',
                u'col2': u'1'
            },
            {
                u'col1': u'2',
                u'col2': u'3'
            },
            {
                u'col1': u'8',
                u'col2': u'9'
            },
            {
                u'col1': u'10',
                u'col2': u'11'
            },
            {
                u'col1': u'14',
                u'col2': u'15'
            },
            {
                u'col1': u'16',
                u'col2': u'17'
            },
        ],
    ),
    (    # Recursive with post/pre wildcard
        "edgeCases/recurFileNames/",
        {
            "fileNamePattern": "*test*",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'0',
                u'col2': u'1'
            },
            {
                u'col1': u'2',
                u'col2': u'3'
            },
            {
                u'col1': u'4',
                u'col2': u'5'
            },
            {
                u'col1': u'6',
                u'col2': u'7'
            },
            {
                u'col1': u'8',
                u'col2': u'9'
            },
            {
                u'col1': u'10',
                u'col2': u'11'
            },
            {
                u'col1': u'14',
                u'col2': u'15'
            },
            {
                u'col1': u'16',
                u'col2': u'17'
            },
            {
                u'col1': u'18',
                u'col2': u'19'
            },
        ],
    ),
    (    # Recursive with internal wildcard
        "edgeCases/recurFileNames/",
        {
            "fileNamePattern": "test.*sv",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'0',
                u'col2': u'1'
            },
            {
                u'col1': u'8',
                u'col2': u'9'
            },
            {
                u'col1': u'14',
                u'col2': u'15'
            },
        ],
    ),
    (    # Recursive with internal wildcard
        "edgeCases/recurFileNames/",
        {
            "fileNamePattern": "test.cs*",
            "schemaMode": "header",
            "isRecursive": True,
        },
        [
            {
                u'col1': u'0',
                u'col2': u'1'
            },
            {
                u'col1': u'8',
                u'col2': u'9'
            },
            {
                u'col1': u'14',
                u'col2': u'15'
            },
        ],
    ),
]


def compare_dict_lists(list_a, list_b):
    def process(l):
        return sorted([list(c.items()) for c in l])

    return process(list_a) == process(list_b)


@pytest.fixture(scope="module", autouse=True)
def targets(client):
    for target_name, target_type_id, target_args in target_infos:
        client.add_data_target(target_name, target_type_id, target_args)

    yield
    # cleanup
    for target_name, _, _ in target_infos:
        client.get_data_target(target_name).delete()


@pytest.fixture(scope="module")
def setup(client, workbook):
    xcalarApi = XcalarApi()
    xcalarApi.setSession(workbook)

    udf = Udf(xcalarApi)

    for udfName, udfSource in udfs:
        udf.addOrUpdate(udfName, udfSource)

    yield (client, workbook, xcalarApi, udf)

    xcalarApi.setSession(None)


def test_error_raising(setup):
    (client, workbook, xcalarApi, udf) = setup

    with pytest.raises(TypeError) as e:
        client.list_datasets(1)
    assert str(e.value) == "pattern must be str, not '{}'".format(type(1))

    with pytest.raises(TypeError) as e:
        client.get_dataset(1)
    assert str(e.value) == "dataset_name must be str, not '{}'".format(type(1))

    incorr_dataset = "Inexistant_dataset_error"
    with pytest.raises(ValueError) as e:
        client.get_dataset(incorr_dataset)
    assert str(e.value) == "No such dataset: '{}'".format(incorr_dataset)

    targetName, pathBuilder = DefaultTargetName, buildNativePath
    path = pathBuilder("symlink")
    pattern = "comma.csv"
    dsName_1 = "fullUrl"

    with pytest.raises(TypeError) as e:
        workbook.build_dataset("valid_name", 1, "valid_path", "csv")
    assert str(
        e.value
    ) == "data_target must be str or a DataTarget instance, not '{}'".format(
        type(1))

    with pytest.raises(TypeError) as e:
        workbook.build_dataset(1, "valid_target", "valid_path", "csv")
    assert str(e.value) == "dataset_name must be str, not '{}'".format(type(1))

    with pytest.raises(TypeError) as e:
        workbook.build_dataset("valid_name", "valid_target", 1, "csv")
    assert str(e.value) == "path must be str, not '{}'".format(type(1))

    with pytest.raises(TypeError) as e:
        workbook.build_dataset("valid_name", "valid_target", "valid_path", 1)
    assert str(
        e.value) == "import_format must be of type str, not '{}'".format(
            type(1))

    with pytest.raises(TypeError) as e:
        workbook.build_dataset(
            "valid_name", "valid_target", "valid_path", "csv", parser_name=1)
    assert str(e.value) == "parser_name must be str, not '{}'".format(type(1))

    with pytest.raises(TypeError) as e:
        workbook.build_dataset(
            "valid_name",
            "valid_target",
            "valid_path",
            "csv",
            parser_name="parser_name",
            parser_args=1)
    assert str(e.value) == "parser_args must be dict, not '{}'".format(type(1))

    with pytest.raises(ValueError) as e:
        workbook.build_dataset("valid_name", "valid_target", "valid_path",
                               ".word")
    assert str(e.value) == "Invalid import format: '{}'".format(".word")

    with pytest.raises(ValueError) as e:
        workbook.build_dataset(
            "valid_name",
            "valid_target",
            "valid_path",
            "csv",
            parser_name="parser_name")
    assert str(
        e.value) == "parser_name cannot be set for '{}' imports".format("csv")

    with pytest.raises(ValueError) as e:
        workbook.build_dataset(
            "valid_name",
            "valid_target",
            "valid_path",
            "csv",
            parser_args={"some_args": 1})
    assert str(
        e.value) == "parser_args cannot be set for '{}' imports".format("csv")

    with pytest.raises(ValueError) as e:
        workbook.build_dataset("valid_name", "valid_target", "valid_path",
                               "udf")
    assert str(
        e.value
    ) == "parser_name must be a valid str representing import udf file, not 'None'"

    dataset_builder = workbook.build_dataset(dsName_1, targetName, path, "csv",
                                             True, pattern)

    with pytest.raises(TypeError) as e:
        dataset_builder.option("allowFileErrors", 1)
    assert str(e.value) == "value for '{}' must be {}, not '{}'".format(
        "allowFileErrors", type(dataset_builder._args["allowFileErrors"]),
        type(1))

    incorr_param = "Some_param"
    with pytest.raises(ValueError) as e:
        dataset_builder.option(incorr_param, 1)
    assert str(e.value) == "No such param: '{}'".format(incorr_param)

    with pytest.raises(TypeError) as e:
        dataset_builder.option("linesToSkip", "1")
    assert str(e.value) == "value for '{}' must be {}, not '{}'".format(
        "linesToSkip", type(dataset_builder.csv_args["linesToSkip"]),
        type("1"))

    dataset_builder.option("fieldDelim", ',').option("schemaMode", "none")

    dataset = dataset_builder.load()

    dsName_2 = "fullUrl_2"
    dataset_builder2 = workbook.build_dataset(dsName_2, targetName, path,
                                              "csv", True, pattern)
    dataset_builder2.option("fieldDelim", ',').option("schemaMode", "none")

    dataset_builder2.load()

    with pytest.raises(ValueError) as e:
        client.get_dataset("*")
    assert str(e.value) == "Multiple datasets found matching: '{}'".format("*")

    client.get_dataset(dataset_builder.dataset_name).delete()
    client.get_dataset(dataset_builder2.dataset_name).delete()


# Validate that UDF execution failure reports its name and path
def test_bad_udf_report(setup):
    configDict = detect_config().all_options
    XlrRootConfig = "XcalarRootCompletePath"
    DefaultXlrRootConfig = configDict["Constants.{}".format(XlrRootConfig)]
    targetName, pathBuilder = DefaultTargetName, buildNativePath
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("jsonSanity/jsonSanity.json")
    dataset_builder = workbook.build_dataset(
        "badUdfDS",
        targetName,
        path,
        "udf",
        parser_name="{}:stream".format(bad_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    errStr = exc.value.output.loadOutput.errorString

    # validate that the load fails due to the bug in the bad UDF
    assert (
        "No such file or directory: 'xjava': 'xjava' at Traceback" in errStr)

    # validate that error string reports the name of the bad UDF module
    assert ("UDF module \"{}.py\"".format(bad_udf_name) in errStr)

    # validate that error string reports the UDF function name ("stream") and
    # the UDF on-disk directory path. Use mangled name to access workbook's ID
    udfDir = "{}/workbooks/{}/{}/udfs/python".format(
        DefaultXlrRootConfig, workbook.username, workbook._Workbook__id)
    assert ("in stream from \"{}\"".format(udfDir) in errStr)


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
    (shared_qa_target_name, (lambda x: x)),
])
def test_dataset_builder_features(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/comma.csv")
    dsName = "fullUrl"

    data_target = client.get_data_target(targetName)
    preview_dt = data_target.preview(path, num_bytes=10)
    assert preview_dt["fileName"] == "comma.csv"
    assert path in preview_dt["fullPath"]

    dataset_builder = workbook.build_dataset(dsName, data_target, path, "csv")
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "none")

    # test build_dataset features
    import_options = dataset_builder.list_import_options()

    assert import_options["fieldDelim"] == ','
    assert import_options["schemaMode"] == "none"
    assert dataset_builder.source_args[0]["recursive"] is False
    assert dataset_builder.source_args[0]["fileNamePattern"] == ''
    assert dataset_builder.source_args[0]["target_name"] == targetName

    start = time.time()
    dataset = dataset_builder.load()
    print("Load took {:.3f}".format(time.time() - start))

    assert dataset.name == dataset_builder.dataset_name
    dataset.delete()


@pytest.mark.parametrize(
    "targetName,pathBuilder",
    standard_targets + [(DefaultTargetName, buildNativePath)])
def test_full_path(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/comma.csv")
    dsName = "fullUrl"
    dataset_builder = workbook.build_dataset(
        dsName, client.get_data_target(targetName), path, "csv")
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "none")

    start = time.time()
    dataset = dataset_builder.load()
    print("Load took {:.3f}".format(time.time() - start))

    data_target = client.get_data_target(targetName)
    resp = data_target.list_files(path)
    files_list = resp['files']
    print(files_list)
    assert len(files_list) == 1
    assert files_list[0]["name"] == "comma.csv"
    assert files_list[0]["isDir"] is False
    assert files_list[0]["size"] == 47

    expected_rows = [
        {
            'column0': 'Comma',
            'column1': 'Separated',
            'column2': 'Columns'
        },
        {
            'column0': 'Newline',
            'column1': 'Separated',
            'column2': 'Rows'
        },
    ]
    iterator = dataset.records()
    assert next(iterator) == dataset.get_row(0)
    assert next(iterator) == dataset.get_row(1)
    dataset.show()

    datasetCheck(expected_rows, dataset.records())
    thisDs = client.get_dataset(dataset_builder.dataset_name)
    assert thisDs.name == dataset_builder.dataset_name
    info = thisDs.get_info()
    assert info["loadArgs"]["parseArgs"]["parserFnName"] == "default:parseCsv"

    expected_columns = [
        {
            "name": "column0",
            "type": "DfString"
        },
        {
            "name": "column1",
            "type": "DfString"
        },
        {
            "name": "column2",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 3
    assert expected_columns == info["columns"]

    thisDs.delete()


def test_symlink(setup):
    targetName, pathBuilder = DefaultTargetName, buildNativePath
    (client, workbook, xcalarApi, udf) = setup

    path = pathBuilder("symlink")
    pattern = "comma.csv"
    dsName = "fullUrl"
    incorrect_name = "Incorrect_name"
    dataset_builder = workbook.build_dataset(incorrect_name, targetName, path,
                                             "csv", True, pattern)
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "none")

    dataset_builder.dataset_name = dsName
    print(dataset_builder.dataset_name)
    start = time.time()
    dataset = dataset_builder.load()

    data_target = client.get_data_target(targetName)
    resp = data_target.list_files(path, pattern=pattern, recursive=True)
    files_list = resp['files']
    assert len(files_list) == 1
    assert files_list[0]["name"] == "csvSanity/comma.csv"
    assert files_list[0]["isDir"] is False
    assert files_list[0]["size"] == 47

    expected_rows = [
        {
            'column0': 'Comma',
            'column1': 'Separated',
            'column2': 'Columns'
        },
        {
            'column0': 'Newline',
            'column1': 'Separated',
            'column2': 'Rows'
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    assert dsName in dataset.name
    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_param_udf(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/comma.csv")
    sudfArgs = {"appendStr": "appStr"}
    appStr = "appStr"
    udfSource = udf.get(param_udf_name).source
    fullUdfPath = "/sharedUDFs/{}".format(param_udf_name)
    udf.addOrUpdate(fullUdfPath, udfSource)
    dataset_builder = workbook.build_dataset(
        "paramedUdfDs",
        targetName,
        path,
        "udf",
        parser_name="{}:appendStr".format(fullUdfPath),
        parser_args=sudfArgs)
    dataset = dataset_builder.load()
    expected_rows = [
        {
            'col0': 'Comma' + appStr,
            'col1': 'Separated' + appStr,
            'col2': 'Columns' + appStr
        },
        {
            'col0': 'Newline' + appStr,
            'col1': 'Separated' + appStr,
            'col2': 'Rows' + appStr
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    dataset.delete()
    udf.delete(fullUdfPath)


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_empty_json(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("jsonSanity/empty.json")
    dataset_builder = workbook.build_dataset(
        "dsJsonEmpty", targetName, path, "json", file_name_pattern='*')
    dataset = dataset_builder.load()

    for dt in client.list_datasets():
        print(dt.name)

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_dir_with_empty_files_json(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("jsonSanity/dirwithemptyfiles/")
    dataset_builder = workbook.build_dataset(
        "dsJsonEmptyFileInDir",
        targetName,
        path,
        "json",
        file_name_pattern='*')
    dataset = dataset_builder.load()

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_json_sanity(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("jsonSanity/jsonSanity.json")
    dataset_builder = workbook.build_dataset(
        "dsJsonBasic", targetName, path, "json", file_name_pattern='*')
    dataset = dataset_builder.load()
    expected_rows = [
        {
            'int': 1,
            'float': 1.2,
            'string': '1.23',
            'bool': True,
            'arrayInt': [1, 2],
            'arrayFloat': [1.2, 2.3],
            'object': {
                'int': 2,
                'float': 2.3,
                'string': '2.34',
                'bool': False
            }
        },
        {
            'int': 2,
            'float': 2.3,
            'string': '2.34',
            'bool': False,
            'arrayInt': [2, 3],
            'arrayFloat': [2.3, 3.4],
            'object': {
                'int': 3,
                'float': 3.4,
                'string': '3.45',
                'bool': True
            }
        },
    ]
    datasetCheck(expected_rows, dataset.records())
    assert dataset.name == dataset_builder.dataset_name
    expected_columns = [
        {
            "name": "object",
            "type": "DfObject"
        },
        {
            "name": "int",
            "type": "DfInt64"
        },
        {
            "name": "string",
            "type": "DfString"
        },
        {
            "name": "bool",
            "type": "DfBoolean"
        },
        {
            "name": "float",
            "type": "DfFloat64"
        },
        {
            "name": "arrayInt",
            "type": "DfArray"
        },
        {
            "name": "arrayFloat",
            "type": "DfArray"
        },
    ]
    info = dataset.get_info()
    assert info["numColumns"] == 7
    # column ordering isn't deterministic from one run to another
    for col in info["columns"]:
        expected_col = next(
            c for c in expected_columns if c["name"] == col["name"])
        assert expected_col == col

    dataset.delete()


@pytest.mark.parametrize(
    "targetName,pathBuilder",
    standard_targets + [(DefaultTargetName, buildNativePath)])
@pytest.mark.parametrize("testCase", csv_load_cases)
def test_basic_csv_load(setup, targetName, pathBuilder, testCase):
    (client, workbook, xcalarApi, udf) = setup
    relPath = testCase[0]
    datasetArgs = testCase[1]
    expected_rows = testCase[2]
    path = pathBuilder(relPath)

    file_name_pattern = datasetArgs.get("fileNamePattern", "")
    recursive = datasetArgs.get("isRecursive", None)
    dataset_builder = workbook.build_dataset(
        "testBasicLoadDs",
        targetName,
        path,
        "csv",
        recursive=recursive,
        file_name_pattern=file_name_pattern)
    for arg in datasetArgs:
        if (arg not in ["isRecursive", "fileNamePattern"]):
            dataset_builder.option(arg, datasetArgs[arg])

    dataset = dataset_builder.load()
    datasetCheck(expected_rows, dataset.records())

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_gzip_load(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("edgeCases/recurTsvGz/")
    dataset_builder = workbook.build_dataset(
        "testBasicLoadDs",
        targetName,
        path,
        "csv",
        recursive=True,
        file_name_pattern="*.tsv.*")
    dataset_builder.option("schemaMode", "header")
    dataset = dataset_builder.load()
    expected_rows = [
        {
            'A': '1',
            'B': '2',
            'C': '3'
        },
        {
            'A': '18',
            'B': '18',
            'C': '18'
        },
        {
            'A': 'two',
            'B': 'two',
            'C': 'two uncompressed-2'
        },
        {
            'A': '1',
            'B': '2',
            'C': '3'
        },
        {
            'A': '18',
            'B': '18',
            'C': '18-uncompressed1'
        },
        {
            'A': '1',
            'B': '2',
            'C': '3'
        },
        {
            'A': '18',
            'B': '18',
            'C': '18'
        },
        {
            'A': 'two',
            'B': 'two',
            'C': 'two'
        },
        {
            'A': '333',
            'B': '333',
            'C': '333 uncompressed-3'
        },
        {
            'A': '1',
            'B': '2',
            'C': '3'
        },
        {
            'A': '18',
            'B': '18',
            'C': '18'
        },
        {
            'A': 'two',
            'B': 'two',
            'C': 'two'
        },
        {
            'A': '3',
            'B': '3',
            'C': '3'
        },
        {
            'A': '4',
            'B': '4',
            'C': '4 uncompressed-4'
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    info = dataset.get_info()
    expected_columns = [
        {
            "name": "A",
            "type": "DfString"
        },
        {
            "name": "B",
            "type": "DfString"
        },
        {
            "name": "C",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 3
    assert expected_columns == info["columns"]

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_xml_load(setup, targetName, pathBuilder):
    if targetName == s3_minio_target_name:
        pytest.skip("xmlSanity bucket not avilable in s3 minio")
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("xmlSanity/")

    expected_rows = [
        {
            'xPath': {
                'a': '1',
                'b': '2',
                'c': '3'
            },
            'colA': [{
                'a': '1'
            }],
            'colB': [{
                'b': '2'
            }],
            'colC': [{
                'c': '3'
            }],
            'xcElementXpath': '/data/entry[1]',
            'xcMatchedXpath': '//entry'
        },
        {
            'xPath': {
                'a': '4',
                'b': '5',
                'c': '6'
            },
            'colA': [{
                'a': '4'
            }],
            'colB': [{
                'b': '5'
            }],
            'colC': [{
                'c': '6'
            }],
            'xcElementXpath': '/data/entry[2]',
            'xcMatchedXpath': '//entry'
        },
    ]
    parseArgs = {
        "allPaths": [{
            "xPath": {
                "name": "xPath",
                "value": "//entry"
            },
            "extraKeys": {
                "colA": "a",
                "colB": "b",
                "colC": "c"
            }
        }]
    }
    dataset_builder = workbook.build_dataset(
        "testXmlLoadDs",
        targetName,
        path,
        "udf",
        file_name_pattern='abc.xml',
        parser_name="/sharedUDFs/default:xmlToJsonWithExtraKeys",
        parser_args=parseArgs)
    dataset = dataset_builder.load()
    info = dataset.get_info()
    assert info["numColumns"] == 6

    datasetCheck(expected_rows, dataset.records())

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_xml_zip_with_dirs(setup, targetName, pathBuilder):
    """
        This test also tests ZIP files with directories inside.
        I had to use the XML parser because it is the only one
        that errors when you find a "file" with errors.
    """
    if targetName == s3_minio_target_name:
        pytest.skip("xmlSanity bucket not avilable in s3 minio")
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("xmlSanity/")

    expected_rows = [
        {
            'xPath': {
                'a': '1',
                'b': '2',
                'c': '3'
            },
            'colA': [{
                'a': '1'
            }],
            'colB': [{
                'b': '2'
            }],
            'colC': [{
                'c': '3'
            }],
            'xcElementXpath': '/data/entry[1]',
            'xcMatchedXpath': '//entry'
        },
        {
            'xPath': {
                'a': '4',
                'b': '5',
                'c': '6'
            },
            'colA': [{
                'a': '4'
            }],
            'colB': [{
                'b': '5'
            }],
            'colC': [{
                'c': '6'
            }],
            'xcElementXpath': '/data/entry[2]',
            'xcMatchedXpath': '//entry'
        },
    ]
    parseArgs = {
        "allPaths": [{
            "xPath": {
                "name": "xPath",
                "value": "//entry"
            },
            "extraKeys": {
                "colA": "a",
                "colB": "b",
                "colC": "c"
            }
        }]
    }

    dataset_builder = workbook.build_dataset(
        "testXmlLoadDs",
        targetName,
        path,
        "udf",
        file_name_pattern='abc.xml.zip',
        parser_name="default:xmlToJsonWithExtraKeys",
        parser_args=parseArgs)

    dataset = dataset_builder.load()
    info = dataset.get_info()
    assert info["numColumns"] == 6

    datasetCheck(expected_rows, dataset.records())
    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_tar_load(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/")
    dataset_builder = workbook.build_dataset(
        "testTarLoadDs",
        targetName,
        path,
        "csv",
        file_name_pattern="csvData.tar")

    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "header")

    dataset = dataset_builder.load()
    expected_rows = [
        {
            'Column0': 'file0row0col0',
            'Column1': 'file0row0col1',
            'Column2': 'file0row0col2'
        },
        {
            'Column0': 'file0row1col0',
            'Column1': 'file0row1col1',
            'Column2': 'file0row1col2'
        },
        {
            'Column0': 'file0row2col0',
            'Column1': 'file0row2col1',
            'Column2': 'file0row2col2'
        },
        {
            'Column0': 'file1row0col0',
            'Column1': 'file1row0col1',
            'Column2': 'file1row0col2'
        },
        {
            'Column0': 'file1row1col0',
            'Column1': 'file1row1col1',
            'Column2': 'file1row1col2'
        },
        {
            'Column0': 'file1row2col0',
            'Column1': 'file1row2col1',
            'Column2': 'file1row2col2'
        },
        {
            'Column0': 'file2row0col0',
            'Column1': 'file2row0col1',
            'Column2': 'file2row0col2'
        },
        {
            'Column0': 'file2row1col0',
            'Column1': 'file2row1col1',
            'Column2': 'file2row1col2'
        },
        {
            'Column0': 'file2row2col0',
            'Column1': 'file2row2col1',
            'Column2': 'file2row2col2'
        },
    # first file contents are repeated as there's a symlink included
        {
            'Column0': 'file0row0col0',
            'Column1': 'file0row0col1',
            'Column2': 'file0row0col2'
        },
        {
            'Column0': 'file0row1col0',
            'Column1': 'file0row1col1',
            'Column2': 'file0row1col2'
        },
        {
            'Column0': 'file0row2col0',
            'Column1': 'file0row2col1',
            'Column2': 'file0row2col2'
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    info = dataset.get_info()
    expected_columns = [
        {
            "name": "Column0",
            "type": "DfString"
        },
        {
            "name": "Column1",
            "type": "DfString"
        },
        {
            "name": "Column2",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 3
    assert expected_columns == info["columns"]

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_zip_load(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/")
    dataset_builder = workbook.build_dataset(
        "testZipLoadDs",
        targetName,
        path,
        "csv",
        file_name_pattern="csvData.zip")
    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "header")

    dataset = dataset_builder.load()
    expected_rows = [
        {
            'Column0': 'file0row0col0',
            'Column1': 'file0row0col1',
            'Column2': 'file0row0col2'
        },
        {
            'Column0': 'file0row1col0',
            'Column1': 'file0row1col1',
            'Column2': 'file0row1col2'
        },
        {
            'Column0': 'file0row2col0',
            'Column1': 'file0row2col1',
            'Column2': 'file0row2col2'
        },
        {
            'Column0': 'file1row0col0',
            'Column1': 'file1row0col1',
            'Column2': 'file1row0col2'
        },
        {
            'Column0': 'file1row1col0',
            'Column1': 'file1row1col1',
            'Column2': 'file1row1col2'
        },
        {
            'Column0': 'file1row2col0',
            'Column1': 'file1row2col1',
            'Column2': 'file1row2col2'
        },
        {
            'Column0': 'file2row0col0',
            'Column1': 'file2row0col1',
            'Column2': 'file2row0col2'
        },
        {
            'Column0': 'file2row1col0',
            'Column1': 'file2row1col1',
            'Column2': 'file2row1col2'
        },
        {
            'Column0': 'file2row2col0',
            'Column1': 'file2row2col1',
            'Column2': 'file2row2col2'
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    info = dataset.get_info()

    expected_columns = [
        {
            "name": "Column0",
            "type": "DfString"
        },
        {
            "name": "Column1",
            "type": "DfString"
        },
        {
            "name": "Column2",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 3
    assert expected_columns == info["columns"]

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_tar_gzip_load(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/")
    dataset_builder = workbook.build_dataset(
        "testTarGzipLoadDs",
        targetName,
        path,
        "csv",
        file_name_pattern="csvData.tar.gz")

    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")

    dataset = dataset_builder.load()
    expected_rows = [
        {
            'Column0': 'file0row0col0',
            'Column1': 'file0row0col1',
            'Column2': 'file0row0col2'
        },
        {
            'Column0': 'file0row1col0',
            'Column1': 'file0row1col1',
            'Column2': 'file0row1col2'
        },
        {
            'Column0': 'file0row2col0',
            'Column1': 'file0row2col1',
            'Column2': 'file0row2col2'
        },
        {
            'Column0': 'file1row0col0',
            'Column1': 'file1row0col1',
            'Column2': 'file1row0col2'
        },
        {
            'Column0': 'file1row1col0',
            'Column1': 'file1row1col1',
            'Column2': 'file1row1col2'
        },
        {
            'Column0': 'file1row2col0',
            'Column1': 'file1row2col1',
            'Column2': 'file1row2col2'
        },
        {
            'Column0': 'file2row0col0',
            'Column1': 'file2row0col1',
            'Column2': 'file2row0col2'
        },
        {
            'Column0': 'file2row1col0',
            'Column1': 'file2row1col1',
            'Column2': 'file2row1col2'
        },
        {
            'Column0': 'file2row2col0',
            'Column1': 'file2row2col1',
            'Column2': 'file2row2col2'
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    info = dataset.get_info()
    print(info)
    expected_columns = [
        {
            "name": "Column0",
            "type": "DfString"
        },
        {
            "name": "Column1",
            "type": "DfString"
        },
        {
            "name": "Column2",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 3
    assert expected_columns == info["columns"]

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_1k_files(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("edgeCases/20k/")
    dataset_builder = workbook.build_dataset(
        "ds1kfiles",
        targetName,
        path,
        "csv",
        recursive=True,
        file_name_pattern='re:part[01][0-9]/file0[0-4][0-9]')
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")
    dataset = dataset_builder.load()

    expected_rows = []
    for ii in range(0, 20):
        for jj in range(0, 50):
            row = {
                'directoryNum': '{:02}'.format(ii),
                'fileNum': '{:03}'.format(jj)
            }
            expected_rows.append(row)
    datasetCheck(expected_rows, dataset.records())

    numFiles = len(list(dataset.errors()))
    assert numFiles == 1000

    assert dataset.name == dataset_builder.dataset_name
    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_1k_files_udf(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("edgeCases/20k/")
    dataset_builder = workbook.build_dataset(
        "ds1kfilesUdf",
        targetName,
        path,
        "udf",
        recursive=True,
        file_name_pattern='re:part[01][0-9]/file0[0-4][0-9]',
        parser_name="{}:parse".format(csv_udf_name))

    dataset = dataset_builder.load()
    expected_rows = []
    for ii in range(0, 20):
        for jj in range(0, 50):
            row = {
                'directoryNum': '{:02}'.format(ii),
                'fileNum': '{:03}'.format(jj)
            }
            expected_rows.append(row)
    datasetCheck(expected_rows, dataset.records())

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_ordered_dict_udf(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/orderedDict.csv")
    dataset_builder = workbook.build_dataset(
        "orderedDictDs",
        targetName,
        path,
        "udf",
        file_name_pattern='*',
        parser_name="{}:parse".format(ordered_dict_udf_name))

    dataset = dataset_builder.load()

    info = dataset.get_info()

    expected_columns = [
        {
            "name": "column1",
            "type": "DfString"
        },
        {
            "name": "column2",
            "type": "DfString"
        },
        {
            "name": "column3",
            "type": "DfString"
        },
        {
            "name": "column4",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 4
    assert expected_columns == info["columns"]

    dataset.delete()


@pytest.mark.parametrize("sampleSize", [
    1 * 2**10,
    5 * 2**10,
    10 * 2**10,
    1 * 2**20,
])
@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_sample_files(setup, targetName, pathBuilder, sampleSize):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("edgeCases/20k/")
    fileSize = 28    # 28 bytes per file
    numFiles = 1000
    testName = "ds1kSample{}".format(sampleSize)
    datasetName = testName

    expectedNumFiles = min(sampleSize // fileSize, numFiles)
    expectedBytes = expectedNumFiles * fileSize
    downSampleExpected = expectedNumFiles < numFiles

    dataset_builder = workbook.build_dataset(
        "ds1kfilesSampled",
        targetName,
        path,
        "csv",
        recursive=True,
        file_name_pattern='re:part[01][0-9]/file0[0-4][0-9]')

    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("sampleSize", sampleSize).option(
        "schemaMode", "header")
    dataset = dataset_builder.load()

    assert dataset.name == dataset_builder.dataset_name

    info = dataset.get_info()

    assert info["downSampled"] == downSampleExpected

    dataset.delete()


@pytest.mark.skip(reason="XXX NOW hdfs doesnt work")
def test_hdfs_connect(setup):
    (client, workbook, xcalarApi, udf) = setup
    targetName = hdfs_target_name
    path = hdfsCrimePath
    dataset_builder = workbook.build_dataset("pyTestHdfsConnect", targetName,
                                             path, "csv")
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")
    dataset = dataset_builder.load()
    assert dataset.record_count() == 206638

    dataset.delete()


def test_local_files(setup):
    (client, workbook, xcalarApi, udf) = setup
    targetName = unshared_symm_target_name
    path = "csvSanity"
    # this uses unshared and thus will load duplicates of every file
    dataset_builder = workbook.build_dataset(
        "dirUrlFile", targetName, path, "csv", file_name_pattern="comma.csv")
    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "none")
    dataset = dataset_builder.load()
    rowsPerNode = [
        {
            'column0': 'Comma',
            'column1': 'Separated',
            'column2': 'Columns'
        },
        {
            'column0': 'Newline',
            'column1': 'Separated',
            'column2': 'Rows'
        },
    ]
    expected_rows = []
    for _ in range(client.get_num_nodes_in_cluster()):
        expected_rows.extend(rowsPerNode)

    datasetCheck(expected_rows, dataset.records())

    dataset.delete()


def test_local_files_single(setup):
    (client, workbook, xcalarApi, udf) = setup
    targetName = unshared_single_node_target_name
    # Only load from node 0
    path = os.path.join("0", XcalarQaDatasetPath.strip('/'), "csvSanity/")

    dataset_builder = workbook.build_dataset(
        "sharedNothing",
        targetName,
        path,
        "csv",
        file_name_pattern="comma.csv")
    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "none")
    dataset = dataset_builder.load()
    expected_rows = [
        {
            'column0': 'Comma',
            'column1': 'Separated',
            'column2': 'Columns'
        },
        {
            'column0': 'Newline',
            'column1': 'Separated',
            'column2': 'Rows'
        },
    ]

    datasetCheck(expected_rows, dataset.records())

    assert dataset.name == dataset_builder.dataset_name
    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_empty(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/empty.csv")
    dataset_builder = workbook.build_dataset("emptyDs", targetName, path,
                                             "csv")
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")
    dataset = dataset_builder.load()

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_dir_with_empty_files_csv(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/dirwithemptyfiles/")
    dataset_builder = workbook.build_dataset("emptyFileInDir", targetName,
                                             path, "csv")
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")
    dataset = dataset_builder.load()

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_empty_udf(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/empty.csv")
    dataset_builder = workbook.build_dataset(
        "emptyUdfDs", targetName, path, "udf", parser_name="ones:onesOnLoad")
    dataset = dataset_builder.load()
    expected_rows = [{
        'column0': 1,
    }]
    datasetCheck(expected_rows, dataset.records())

    info = dataset.get_info()
    # Note: parserFnName is fully qualified
    #      e.g. /workbook/swatanabe/5AFC8A942A5FADB0/udf/ones:onesOnLoad
    assert "ones:onesOnLoad" in info["loadArgs"]["parseArgs"]["parserFnName"]

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_non_utf8_udf_val(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    # This path is arbitrary
    path = pathBuilder("csvSanity/comma.csv")
    dataset_builder = workbook.build_dataset(
        "nonUnicodeUdfDs",
        targetName,
        path,
        "udf",
        parser_name="{}:nonUniValue".format(non_unicode_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    errorString = exc.value.output.loadOutput.errorString.lower()
    assert "bytes" in errorString
    assert "is not supported" in errorString
    assert "record: 0" in errorString
    assert "key" in errorString


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_non_utf8_udf_key(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    # This url is arbitrary
    path = pathBuilder("csvSanity/comma.csv")
    dataset_builder = workbook.build_dataset(
        "nonUnicodeUdfDs0",
        targetName,
        path,
        "udf",
        parser_name="{}:nonUniKey".format(non_unicode_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    errorString = exc.value.output.loadOutput.errorString.lower()
    assert "must be str, not bytes" in errorString
    assert "record: 0" in errorString


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_non_utf8_nested_key(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    # This url is arbitrary
    path = pathBuilder("csvSanity/comma.csv")
    dataset_builder = workbook.build_dataset(
        "nonUnicodeUdfDs1",
        targetName,
        path,
        "udf",
        parser_name="{}:nonUniNestedKey".format(non_unicode_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    errorString = exc.value.output.loadOutput.errorString.lower()
    assert "must be str, not bytes" in errorString
    assert "record: 0" in errorString
    assert "key" in errorString


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_non_utf8_nested_value(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    # This url is arbitrary
    path = pathBuilder("csvSanity/comma.csv")
    dataset_builder = workbook.build_dataset(
        "nonUnicodeUdfDs2",
        targetName,
        path,
        "udf",
        parser_name="{}:nonUniNestedValue".format(non_unicode_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    errorString = exc.value.output.loadOutput.errorString.lower()
    assert "bytes" in errorString
    assert "is not supported" in errorString
    assert "record: 0" in errorString
    assert "key" in errorString


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_user_exception(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    # This url is arbitrary
    path = pathBuilder("csvSanity/comma.csv")
    dataset_builder = workbook.build_dataset(
        "nonUnicodeUdf0Ds3",
        targetName,
        path,
        "udf",
        parser_name="{}:raiseExc".format(exc_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    assert "userexception" in exc.value.output.loadOutput.errorString.lower()


@pytest.mark.parametrize("targetName,pathBuilder", standard_targets)
def test_dot_dot_exception(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    # This url is arbitrary
    path = pathBuilder("csvSanity/../csvcomma.csv")
    dataset_builder = workbook.build_dataset(
        "nonUnicodeUdf0Ds3",
        targetName,
        path,
        "udf",
        parser_name="{}:raiseExc".format(exc_udf_name))
    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    assert "'..' is not allowed" in exc.value.output.loadOutput.errorString.lower(
    )


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_many_columns(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/manyColumns.csv")
    dataset_builder = workbook.build_dataset("manyColumnsDs", targetName, path,
                                             "csv")

    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")
    dataset = dataset_builder.load()

    info = dataset.get_info()

    expected_columns = [{
        "name": "col{}".format(ii),
        "type": "DfString"
    } for ii in range(200)]

    assert info["numColumns"] == 200
    assert expected_columns == info["columns"]

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_diff_schemas(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/diffSchemas/")
    dataset_builder = workbook.build_dataset("diffSchemasDs", targetName, path,
                                             "csv")
    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "header")
    dataset = dataset_builder.load()

    assert dataset.name == dataset_builder.dataset_name
    info = dataset.get_info()

    # The dataset consists of two files with different headers but getInfo
    # only returns those from one of the files.  As we don't know which
    # file we have to compare against both

    expected_columns_1 = [
        {
            "name": "columnA",
            "type": "DfString"
        },
        {
            "name": "columnB",
            "type": "DfString"
        },
        {
            "name": "columnC",
            "type": "DfString"
        },
    ]
    expected_columns_2 = [
        {
            "name": "columnD",
            "type": "DfString"
        },
        {
            "name": "columnE",
            "type": "DfString"
        },
        {
            "name": "columnF",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 6
    # Either file may have been first
    assert (info["columns"] == (expected_columns_1 + expected_columns_2)
            or info["columns"] == (expected_columns_2 + expected_columns_1))

    dataset.delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_multi_source_args(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path1 = pathBuilder("csvSanity/diffSchemas/diffSchema1.csv")
    path2 = pathBuilder("csvSanity/diffSchemas/diffSchema2.csv")
    dataset_builder = workbook.build_dataset("diffSchemasDs", targetName,
                                             path1, "csv")
    dataset_builder.add_source(targetName, path2)
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "header")
    dataset = dataset_builder.load()

    info = dataset.get_info()

    # The dataset consists of two files with different headers but getInfo
    # only returns those from one of the files.  As we don't know which
    # file we have to compare against both

    expected_columns_1 = [
        {
            "name": "columnA",
            "type": "DfString"
        },
        {
            "name": "columnB",
            "type": "DfString"
        },
        {
            "name": "columnC",
            "type": "DfString"
        },
    ]
    expected_columns_2 = [
        {
            "name": "columnD",
            "type": "DfString"
        },
        {
            "name": "columnE",
            "type": "DfString"
        },
        {
            "name": "columnF",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 6
    # Either file may have been first
    assert (info["columns"] == (expected_columns_1 + expected_columns_2)
            or info["columns"] == (expected_columns_2 + expected_columns_1))

    dataset.delete()


def test_load_mem(setup):
    (client, workbook, xcalarApi, udf) = setup
    numRows = 1000
    addedVal = 10
    path = str(numRows)
    parseArgs = {"addedVal": addedVal}
    dataset_builder = workbook.build_dataset(
        "memTestDs",
        memory_target_name,
        path,
        "udf",
        parser_name="{}:loadSeq".format(mem_udf_name),
        parser_args=parseArgs)
    dataset = dataset_builder.load()
    expected_rows = []
    for ii in range(numRows):
        rowDict = {"val": ii + addedVal}
        if (ii == 0):
            rowDict['flaky'] = 1
        expected_rows.append(rowDict)

    datasetCheck(expected_rows, dataset.records())

    assert dataset.name == dataset_builder.dataset_name
    dataset.delete()


def test_load_mem_schema(setup):
    (client, workbook, xcalarApi, udf) = setup
    numRows = 1000
    path = str(numRows)
    addedVal = 10
    schema = "{'Cols': {'ColInt': 'int', 'ColFloat': 'float', 'ColString': 'string', 'ColDateTime': 'datetime'}, 'AddedVal': " + "{}".format(
        addedVal) + "}"
    parseArgs = {"schema": schema}
    dataset_builder = workbook.build_dataset(
        "memTestDs",
        memory_target_name,
        path,
        "udf",
        parser_name="{}:loadSeq".format(mem_schema_udf_name),
        parser_args=parseArgs)
    dataset = dataset_builder.load()
    expected_rows = []
    for ii in range(numRows):
        rowDict = {}
        rowDict["Val"] = ii + addedVal
        rowDict['ColInt'] = int(ii)
        rowDict['ColFloat'] = float(ii)
        rowDict['ColString'] = str(ii)
        curDt = datetime.fromtimestamp(ii, timezone.utc)
        rowDict[
            'ColDateTime'] = "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:03d}Z".format(
                curDt.year, curDt.month, curDt.day, curDt.hour, curDt.minute,
                curDt.second, int(curDt.microsecond / 1000))
        expected_rows.append(rowDict)
    datasetCheck(expected_rows, dataset.records())
    assert dataset.name == dataset_builder.dataset_name
    dataset.delete()


def verifyChecksumValue(operators, client, session, srcTable, field, answer):
    evalStr = "sum({})".format(field)
    srcSumTable = "checksum_src"
    operators.aggregate(srcTable, srcSumTable, evalStr)
    resultSet = ResultSet(
        client, table_name=srcSumTable, session_name=session.name)
    srcSum = resultSet.get_row(0)
    del resultSet
    assert (float(srcSum["constant"]) == float(answer))
    operators.dropConstants("checksum*", deleteCompletely=True)


def test_load_mem_schema_nested(setup):
    (client, workbook, xcalarApi, udf) = setup
    operators = Operators(xcalarApi)
    session = workbook.activate()
    numRows = 1000
    path = str(numRows)
    addedVal = 10
    schemaNested = {}
    schemaNested = "{'Nested': {'Cols': {'ColInt': 'int', 'ColFloat': 'float', 'ColString': 'string', 'ColStringEmpty': 'stringEmpty', 'ColDateTime': 'datetime'}, 'AddedVal': " + "{}".format(
        addedVal) + "}}"
    parseArgs = {"schemaNested": schemaNested}

    dataset_builder = workbook.build_dataset(
        "memTestDs",
        memory_target_name,
        path,
        "udf",
        parser_name="{}:loadSeq".format(mem_schema_nested_udf_name),
        parser_args=parseArgs)
    dataset = dataset_builder.load()
    expected_rows = []
    for ii in range(numRows):
        rowDictNested = {}
        rowDictNested['Nested'] = {}
        rowDictNested['Nested']["Val"] = ii + addedVal
        rowDictNested['Nested']['ColInt'] = int(ii)
        rowDictNested['Nested']['ColFloat'] = float(ii)
        rowDictNested['Nested']['ColString'] = str(ii)
        # 3 level nesting for testing empty string case
        rowDictNested['Nested']['ColStringEmpty'] = {}
        rowDictNested['Nested']['ColStringEmpty']['ColStringEmpty2'] = None
        curDt = datetime.fromtimestamp(ii, timezone.utc)
        rowDictNested['Nested'][
            'ColDateTime'] = "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:03d}Z".format(
                curDt.year, curDt.month, curDt.day, curDt.hour, curDt.minute,
                curDt.second, int(curDt.microsecond / 1000))
        expected_rows.append(rowDictNested)
    datasetCheck(expected_rows, dataset.records())
    assert dataset.name == dataset_builder.dataset_name

    idxTableName = "IndexTable"
    operators.indexDataset(dataset.name, idxTableName, "Nested.ColInt", "p")
    mapTableName = "MapTable"
    mapColumnName = "MapColumnName"
    mult_num = 10
    operators.map(idxTableName, mapTableName,
                  ["mult(p-Nested.ColInt, {})".format(mult_num)],
                  [mapColumnName])
    aggResult = (numRows - 1) * ((numRows) / 2)
    verifyChecksumValue(operators, client, session, mapTableName,
                        "p-Nested.ColInt", aggResult)
    session.get_table(idxTableName).drop()
    session.get_table(mapTableName).drop()
    dataset.delete()


# XXX roll this into the above tests once we have QA data mirrored on MapR
@pytest.mark.skip(reason="Only works when MapR is installed & configured")
def test_mapr(setup):
    (client, workbook, xcalarApi, udf) = setup
    targetName = mapr_qa_target_name
    path = "/datasets/USDCAD-2010-11.csv"
    dsName = "maprTest"
    dataset_builder = workbook.build_dataset(dsName, targetName, path, "csv")
    dataset_builder.option("schemaMode", "none").option("fieldDelim", ',')
    dataset = dataset_builder.load()

    assert dataset.record_count() == 2320118

    dataset.delete()


@pytest.mark.parametrize("file_name_field,record_num_field", [
    ("", ""),
    ("fileName", "recordNum"),
    ("", "recordNum"),
    ("fileName", ""),
])
def test_added_fields(setup, file_name_field, record_num_field):
    (client, workbook, xcalar_api, udf) = setup
    target_name, path_builder = (DefaultTargetName, buildNativePath)
    path = path_builder("smallFiles/csv/")
    dataset_builder = workbook.build_dataset(
        "ds300files",
        target_name,
        path,
        "csv",
        recursive=True,
        file_name_pattern='file*')

    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "header")
    dataset_builder.option("fileNameField", file_name_field).option(
        "recordNumField", record_num_field)

    dataset = dataset_builder.load()
    expected_rows = []
    for ii in range(300):
        row = {'fileNum': str(ii)}
        if file_name_field:
            file_name = buildNativePath("smallFiles/csv/file{}".format(ii))
            row[file_name_field] = file_name
        if record_num_field:
            row[record_num_field] = 1    # all of these are 1 record files
        expected_rows.append(row)
    datasetCheck(expected_rows, dataset.records())

    dataset.delete()


@pytest.mark.parametrize("file_name_field,record_num_field,error_str", [
    ("dup", "dup", "record number field 'dup' present in data"),
    ("fileName", "invalid(", "'invalid(' is not allowed"),
])
def test_added_fields_errors(setup, file_name_field, record_num_field,
                             error_str):
    (client, workbook, xcalar_api, udf) = setup
    target_name, path_builder = (DefaultTargetName, buildNativePath)
    path = path_builder("smallFiles/csv/")
    dataset_builder = workbook.build_dataset(
        "ds300files",
        target_name,
        path,
        "csv",
        recursive=True,
        file_name_pattern='file*')

    dataset_builder.option("fieldDelim", ',')
    dataset_builder.option("schemaMode", "header")
    dataset_builder.option("fileNameField", file_name_field).option(
        "recordNumField", record_num_field)

    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    assert error_str in exc.value.output.loadOutput.errorString


def test_s3_bucket_name_in_error(setup):
    (client, workbook, xcalar_api, udf) = setup
    target_name, path_builder = (s3_minio_target_name, buildS3Path)
    path = "/bucketnotexists"
    dataset_builder = workbook.build_dataset("dsfail", target_name, path,
                                             "csv")

    with pytest.raises(XcalarApiStatusException) as exc:
        dataset = dataset_builder.load()
    assert "bucketnotexists" in exc.value.output.loadOutput.errorString


@pytest.mark.parametrize("allow_file_errors,allow_record_errors", [
    (True, False),
    (True, True),
])
def test_error_params(setup, allow_file_errors, allow_record_errors):
    (client, workbook, xcalar_api, udf) = setup
    target_name, path_builder = (DefaultTargetName, buildNativePath)
    path = path_builder("smallFiles/csv/")
    file_name_field = "fileName"
    record_num_field = "recNum"
    dataset_builder = workbook.build_dataset(
        "dsErrorTests",
        target_name,
        path,
        "udf",
        file_name_pattern='file*',
        parser_name="{}:oddRecordsBadFields".format(rec_errs_udf_name))

    dataset_builder.option("allowFileErrors", allow_file_errors)
    dataset_builder.option("allowRecordErrors", allow_record_errors)
    dataset_builder.option("fileNameField", file_name_field)
    dataset_builder.option("recordNumField", record_num_field)

    dataset = dataset_builder.load()
    expected_rows = []
    expected_errors = []
    expected_num_errors = 0
    for file_num in range(300):
        file_name = buildNativePath("smallFiles/csv/file{}".format(file_num))
        rows = []
        errors = []
        for ii in range(100):
            if ii % 2 == 0:
                rec_num = ii + 1
                row = {
                    'field': ii,
                    file_name_field: file_name,
                    record_num_field: rec_num
                }
                rows.append(row)
            else:
                err = {
                    "error":
                        "field 'invalid': Field type 'bytes' is not supported",
                    "recordNumber":
                        ii,
                }
                if allow_file_errors:
                    errors.append(err)
                if not allow_record_errors:
                    break

        file_report = {
            "fullPath": file_name,
            "numRecordsInFile": len(rows),
            "numTotalErrors": len(errors),
            "numErrors": len(errors),
            "errors": errors,
        # sourceArgs
            "targetName": target_name,
            "path": path,
            "fileNamePattern": 'file*',
            "recursive": False,
        }
        expected_rows.extend(rows)
        expected_errors.append(file_report)
        expected_num_errors += file_report["numTotalErrors"]
    datasetCheck(expected_rows, dataset.records())

    assert dataset.name == dataset_builder.dataset_name
    info = dataset.get_info()

    assert info["totalNumErrors"] == expected_num_errors
    datasetCheck(expected_errors, dataset.errors())

    dataset.delete()


# Tests a single, generated, 'long' file. Specifically this is testing that
# the resultant records are well ordered
def test_long_file(setup):
    (client, workbook, xcalarApi, udf) = setup
    numRecords = 10**4
    numFields = 5
    recTemplate = "{},hello,padding,fields,to make the records longer\n"

    expectedRecords = []
    with tempfile.NamedTemporaryFile('w', encoding="utf-8") as tmpf:
        for recNum in range(numRecords):
            recText = recTemplate.format(recNum)
            tmpf.write(recText)
        tmpf.flush()
        dataset_builder = workbook.build_dataset(
            "longFileDs", DefaultTargetName, tmpf.name, "csv")

        dataset_builder.option("fieldDelim", ',').option("schemaMode", "none")
        dataset = dataset_builder.load()

    for rowNum, row in enumerate(dataset.records()):
        assert row["column0"] == str(rowNum)

    client.get_dataset(dataset_builder.dataset_name).delete()


@pytest.mark.parametrize("targetName,pathBuilder", [
    (DefaultTargetName, buildNativePath),
])
def test_duplicate_dataset(setup, targetName, pathBuilder):
    (client, workbook, xcalarApi, udf) = setup
    path = pathBuilder("csvSanity/comma.csv")
    dsName = "fullUrl"
    dataset_builder = workbook.build_dataset(dsName, targetName, path, "csv")
    dataset_builder.option("fieldDelim", ',').option("schemaMode", "none")
    start = time.time()
    dataset = dataset_builder.load()
    print("Load took {:.3f}".format(time.time() - start))

    data_target = client.get_data_target(targetName)
    resp = data_target.list_files(path)
    files_list = resp['files']
    assert len(files_list) == 1
    assert files_list[0]["name"] == "comma.csv"
    assert files_list[0]["isDir"] is False
    assert files_list[0]["size"] == 47

    expected_rows = [
        {
            'column0': 'Comma',
            'column1': 'Separated',
            'column2': 'Columns'
        },
        {
            'column0': 'Newline',
            'column1': 'Separated',
            'column2': 'Rows'
        },
    ]
    datasetCheck(expected_rows, dataset.records())

    dsList = client.list_datasets()
    for ds in dsList:
        print(ds.name)
    thisDs = client.get_dataset(dataset_builder.dataset_name)
    info = thisDs.get_info()
    assert info["loadArgs"]["parseArgs"]["parserFnName"] == "default:parseCsv"

    expected_columns = [
        {
            "name": "column0",
            "type": "DfString"
        },
        {
            "name": "column1",
            "type": "DfString"
        },
        {
            "name": "column2",
            "type": "DfString"
        },
    ]

    assert info["numColumns"] == 3
    assert expected_columns == info["columns"]

    # Try to load the dateset using the same name.
    dataset2_builder = workbook.build_dataset(dsName, targetName, path, "csv")
    dataset2_builder.option("fieldDelim", ',').option("schemaMode", "none")

    with pytest.raises(XcalarApiStatusException) as exc:
        dataset2_builder.load()
    assert exc.value.message == "Dataset name already exists"

    thisDs.delete()


def test_dataset_long_name(setup):
    # Test load dataset with name length exceed limit
    # The DagTypes::MaxNameLens = 255
    (client, workbook, xcalarApi, udf) = setup
    numRows = 1000
    path = str(numRows)
    dsName = 'x' * 255
    addedVal = 10
    parseArgs = {'addedVal': addedVal}
    dataset_builder = workbook.build_dataset(
        dsName,
        memory_target_name,
        path,
        'udf',
        parser_name="{}:loadSeq".format(mem_udf_name),
        parser_args=parseArgs)

    with pytest.raises(XcalarApiStatusException) as exc:
        dataset_builder.load()
    assert exc.value.status == StatusT.StatusNoBufs


@pytest.mark.parametrize(
    "records, schema",
    [
        ([[1, "abc", 2.4], [2, "def", 3.4], [3, "hijk", 4.4]], [{
            "name": "col1",
            "type": "integer"
        }, {
            "name": "col2",
            "type": "string"
        }, {
            "name": "col3",
            "type": "float"
        }]),
        ([], [])    # empty records
    ])
def test_gen_table(setup, records, schema):
    (client, workbook, xcalarApi, udf) = setup
    target_name = "TableGen"
    outdir = "0"
    parser_args = {"records": records, "schema": schema}
    db = workbook.build_dataset(
        "test_table_gen_dataset",
        target_name,
        outdir,
        "udf",
        parser_name="default:genTableUdf",
        parser_args=parser_args)
    dataset = db.load()

    db_records = dataset.records()
    field_names = [field["name"] for field in schema]

    input_records_dict = [dict(zip(field_names, x)) for x in records]
    assert set(tuple(sorted(x.items())) for x in input_records_dict) == set(
        tuple(sorted(x.items())) for x in db_records)
    dataset.delete()


def test_fields_of_interests(setup):
    (client, workbook, xcalarApi, udf) = setup
    schema = [
        {
            "name": "idx",
            "type": "integer"
        },
        {
            "name": "nestedField.mixedField",
            "rename": "nestedField_mixedField",
            "type": "mixed"
        },
        {
            "name": "nestedField.floatField",
            "rename": "nestedField_floatField",
            "type": "float"
        },
        {
            "name": "nestedField.arrayField[1]",
            "rename": "nestedField_arrayField_1",
            "type": "integer"
        },
        {
            "name": "nestedField.arrayField[3]",
            "rename": "nestedField_arrayField_3",
            "type": "integer"
        },
        {
            "name": "arrayField[1]",
            "rename": "arrayField_1",
            "type": "integer"
        },
        {
            "name": "arrayField[3]",
            "rename": "arrayField_3",
            "type": "integer"
        },
        {
            "name": "nullField",
            "type": "null"
        },
    ]
    session = workbook.activate()
    db = workbook.build_dataset("test_foi", shared_qa_target_name,
                                "jsonSanity/foi.json", "json")
    dataset = db.load()
    df = Dataflow.create_dataflow_from_dataset(
        client, dataset, project_columns=schema)

    print("df: {}".format(df.optimized_query_string))

    out_tab_name = "test_foi_table"
    session.execute_dataflow(
        df, table_name=out_tab_name, optimized=True, is_async=False)
    res_tab = session.get_table(out_tab_name)
    dataset.delete()

    records = [
        {
            "idx": 1,
            "nestedField_mixedField": "hello",
            "nestedField_floatField": 5.12,
            "nestedField_arrayField_1": 2,
            "arrayField_1": 2,
            "arrayField_3": 4,
        },
        {
            "idx": 2,
            "nestedField_mixedField": 5,
            "nestedField_floatField": 6.2,
            "nestedField_arrayField_1": 2,
            "nestedField_arrayField_3": 4,
            "arrayField_1": 2,
    #                    "nullField": None # Whoa seems like we're treating NULL as FNF too?
        }
    ]

    # compare data
    assert len(records) == res_tab.record_count()
    result_records = list(res_tab.records())
    result_records = sorted(
        result_records,
        key=lambda x: float('Inf') if not x['idx'] else float(x['idx']))

    for input_rec, result_rec in zip(records, result_records):
        assert input_rec == result_rec

    res_tab.drop()

def test_random_field_order_load(setup):
    (client, workbook, xcalarApi, udf) = setup

    num_rows = 1000
    parser_name = "{}:load_rand_fields".format(random_order_udf_name)
    col_multiplier = 40
    parser_args = {
        'col_count_multiplier': col_multiplier
    }

    session = workbook.activate()
    db = workbook.build_dataset("random_order_fields", memory_target_name,
                                str(num_rows), "udf", parser_name=parser_name, parser_args=parser_args)
    col_types = ['DfInt64', 'DfFloat64', 'DfBoolean', 'DfString', 'DfMoney']
    num_cols = len(col_types) * col_multiplier
    header = [f'col_{x}' for x in range(1, num_cols+1)]
    schema_array = []
    for col_name, col_type in zip(header, col_types * col_multiplier):
        schema_array.append(XcalarApiColumnT(col_name, col_name, col_type))
    db.option("schema", schema_array)

    # add record num and file path as well
    db.option('fileNameField', 'fileName')
    db.option('recordNumField', 'recNumPerFile')
    dataset = db.load()

    df = Dataflow.create_dataflow_from_dataset(
        client, dataset)

    out_tab_name = "random_order_fields_tab"
    query_name = session.execute_dataflow(
        df, table_name=out_tab_name, optimized=True, is_async=False)
    res_tab = session.get_table(out_tab_name)
    # 4 => 2 meta columns and 'recPerFileYielded' and 'filePathYielded'
    assert(len(res_tab.columns) == len(schema_array)) + 4
    assert(res_tab.record_count() == num_rows)

    # check col1 values
    col1_sum = 0
    for rec in res_tab.records(batch_size=5000):
        col1_sum += rec['col_1']
        assert rec['recPerFileYielded'] == rec['recNumPerFile']
        assert rec['filePathYielded'] == rec['fileName']
    assert col1_sum == (num_rows * (num_rows - 1)) / 2
    res_tab.drop()

    dataset.delete()
