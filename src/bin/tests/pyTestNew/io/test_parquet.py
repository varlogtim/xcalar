import json
import random

import pytest

from xcalar.compute.util.Qa import buildNativePath
from xcalar.compute.util.Qa import XcalarQaHDFSHost

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.app import App
from xcalar.external.LegacyApi.Udf import Udf

hiveHdfsHost = "cloudera-hdfs-dl-kvm-01;cloudera-hdfs-dl-kvm-02"
hiveMetastoreHost = "cloudera-hdfs-dl-kvm-01"
hiveTargetName = "Xcalar Hive"

hdfsHost = "hdfs-sanity"
if XcalarQaHDFSHost:
    hdfsHost = XcalarQaHDFSHost

hdfsTargetName = "QA internal hdfs"

relativeDirTargetName = "QA Relative Dir"
sharedNothingSingleTargetName = "QA Shared Nothing Single"
sharedNothingSymTargetName = "QA Shared Nothing Symmetric"

targets = [(hdfsTargetName, "webhdfsnokerberos", {
    "namenodes": hdfsHost
}), (hiveTargetName, "webhdfsnokerberos", {
    "namenodes": hiveHdfsHost
}),
           (relativeDirTargetName, "shared", {
               "mountpoint": buildNativePath("parquet")
           }), (sharedNothingSingleTargetName, "sharednothingsingle", {}),
           (sharedNothingSymTargetName, "sharednothingsymm", {
               "mountpoint": buildNativePath("parquet")
           })]

listFilesModule = "filelisters"
listFilesSrc = """
import pyodbc
import urllib.parse
import os
import json

from xcalar.container.connectors.util import File
import xcalar.container.context as ctx

def passthrough(target, path, namePattern, recursive, **user_args):
    return (path, target.get_files(path, namePattern, recursive, **user_args))

def __addPartitionKeysToSqlClauses(orClauses, queryArgs, partitionKeys, partitionKeysOrdered, queryArgsAcc):
    if len(partitionKeysOrdered) == 0:
        orClauses.append("(PARTITIONS.PART_NAME LIKE ?)")
        queryArgs.append("/".join(queryArgsAcc))
        return

    key = partitionKeysOrdered.pop(0)
    if "*" in partitionKeys[key]:
        values = ["%"]
    else:
        values = partitionKeys[key]

    for value in values:
        queryArgsAcc.append("{}={}".format(key, value))
        __addPartitionKeysToSqlClauses(orClauses, queryArgs, partitionKeys, partitionKeysOrdered[:], queryArgsAcc)
        queryArgsAcc.pop()


def browseHive(target, path, namePattern, recursive, mode):
    fileObjs = []
    parsedUrl = urllib.parse.urlparse(path)
    path = parsedUrl.path
    dataset_root = path
    if mode == "listFiles":
        if path == "/":
            with pyodbc.connect("DSN=Hive;Uid=hiveuser;Pwd=Welcome1") as cxn:
                with cxn.cursor() as cursor:
                    for row in cursor.execute("SELECT * FROM TBLS"):
                        fileObj = File(path=row.TBL_NAME, relPath=row.TBL_NAME,
                                       isDir=False, size=0, mtime=0)
                        fileObjs.append(fileObj)
        else:
            folders = path.lstrip("/").split("/", 1)
            tableName = folders[0]
            if len(folders) > 1:
                subdirs = folders[1]

                with pyodbc.connect("DSN=Hive;Uid=hiveuser;Pwd=Welcome1") as cxn:
                    with cxn.cursor() as cursor:
                        for row in cursor.execute("SELECT SDS.LOCATION as location FROM SDS, TBLS, PARTITIONS WHERE PARTITIONS.SD_ID = SDS.SD_ID and TBLS.TBL_ID = PARTITIONS.TBL_ID AND TBLS.TBL_NAME = ?", tableName):
                            parquetPath = urllib.parse.urlparse(row.location).path
                            dataset_root = parquetPath[:parquetPath.find(tableName) + len(tableName)]
                            try:
                                fileObjs.extend(target.get_files(os.path.join(dataset_root, subdirs), namePattern, False, mode))
                            except:
                                pass
            else:
                fileObj = File(path=path, relPath=path, isDir=False, size=0, mtime=0)
                fileObjs.append(fileObj)
    else:
        tableName = path.lstrip("/")
        queryArgs = [tableName]
        sqlClauses = ["PARTITIONS.SD_ID = SDS.SD_ID", "TBLS.TBL_ID = PARTITIONS.TBL_ID", "TBLS.TBL_NAME = ?"]

        partitionKeys = {}
        partitionKeysOrdered = []
        if len(parsedUrl.query) > 0:
            queryStrs = urllib.parse.unquote_plus(parsedUrl.query).split("&")
            for queryStr in queryStrs:
                (key, value) = queryStr.split("=", 1)
                partitionKeys[key] = value.split(",")
                partitionKeysOrdered.append(key)

        if len(partitionKeys) > 0:
            orClauses = []
            queryArgsAcc = []
            __addPartitionKeysToSqlClauses(orClauses, queryArgs, partitionKeys, partitionKeysOrdered[:], queryArgsAcc)
            sqlClauses.append("(%s)" % " OR ".join(orClauses))

        sqlQuery = "SELECT SDS.LOCATION as location FROM SDS, TBLS, PARTITIONS WHERE %s" % " AND ".join(sqlClauses)

        with pyodbc.connect("DSN=Hive;Uid=hiveuser;Pwd=Welcome1") as cxn:
            with cxn.cursor() as cursor:
                for row in cursor.execute(sqlQuery, *(tuple(queryArgs))):
                    parquetPath = urllib.parse.urlparse(row.location).path
                    dataset_root = parquetPath[:parquetPath.find(tableName) + len(tableName)]
                    fileObjs.extend(target.get_files(parquetPath, namePattern, False, mode))

        if (len(fileObjs) == 0):
            raise ValueError("No files found at '{}' from '{}'".format(path, target.name()))

    return (dataset_root, fileObjs)
"""

num_nodes = 3


@pytest.fixture(scope="module")
def setup(client):
    global num_nodes
    num_nodes = client.get_num_nodes_in_cluster()
    xcalarApi = XcalarApi()
    try:
        workbook = client.create_workbook("TestParquet")
    except Exception as ex:
        workbook = client.get_workbook("TestParquet")
    xcalarApi.setSession(workbook)
    app = App(client)
    udf = Udf(xcalarApi)

    for target_name, target_type_id, target_args in targets:
        client.add_data_target(target_name, target_type_id, target_args)

    udf.addOrUpdate(listFilesModule, listFilesSrc)

    yield (client, workbook, app)

    # cleanup
    for target_name, target_type_id, target_args in targets:
        client.get_data_target(target_name).delete()

    udf.delete(listFilesModule)
    workbook.delete()


def flattenSchema(schema):
    flattenedSchema = {}
    for key in schema:
        escapedKey = key.replace(".", "\\.")
        children = schema[key].get("children", None)
        if children is not None:
            flattenedChild = flattenSchema(children)
            for childKey in flattenedChild:
                flattenedSchema["%s.%s" % (
                    escapedKey, childKey)] = flattenedChild[childKey]
        else:
            flattenedSchema[escapedKey] = schema[key]

    return flattenedSchema


def flattenRow(row):
    flattenedRow = {}
    for key in row:
        escapedKey = key.replace(".", "\\.")
        if isinstance(row[key], dict):
            flattenedChild = flattenRow(row[key])
            for childKey in flattenedChild:
                flattenedRow["%s.%s" % (escapedKey,
                                        childKey)] = flattenedChild[childKey]
        else:
            flattenedRow[escapedKey] = row[key]

    return flattenedRow


@pytest.mark.parametrize(
    "parquetParser",
    [("native"), ("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize(
    "columns, parserArgs, numRows",
    [
        (["os_family", "date", "os_major", "device_family"], {
            "partitionKeys": {
                "os_family": ["Linux", "Windows 98"],
                "date": ["2015-11-10"]
            },
        }, 193),
        (["device_family", "os_family", "date"], {
            "partitionKeys": {
                "date": ["2015-11-10", "2015-11-19"]
            },
            "columns": ["device_family"]
        }, 386),
        (["os_family", "date"], {
            "partitionKeys": {
                "date": ["2015-11-10", "2015-11-19"]
            },
            "columns": ["os_family"]
        }, 386),
        (
            ["date", "os_family", "os_major", "device_family"],
            {
                "partitionKeys": {
                    "date": []
                },
            },    # no columns == all keys
            386),
        (["date", "os_family", "os_major", "device_family"], {}, 386),
        (["date", "os_family"], {
            "columns": []
        }, 386),
        (["date", "os_family"], {
            "columns": ["date"]
        }, 386),
        (["date", "device_family", "os_family"], {
            "columns": ["date", "device_family"]
        }, 386),
        ([], {
            "partitionKeys": {
                "date": ["2021-01-01"]
            }
        }, 0),
        ([], {
            "partitionKeys": {
                "date": ["2021-01-01"]
            },
            "columns": []
        }, 0),
        ([], {
            "partitionKeys": {
                "date": ["2021-01-01"]
            },
            "columns": ["date"]
        }, 0),
        ([], {
            "partitionKeys": {
                "os_family": ["Windows"]
            },
            "columns": ["date", "device_family"]
        }, 0)
    ])
def test_load_parquet_file_with_partition(setup, parquetParser, columns,
                                          parserArgs, numRows):
    (client, workbook, app) = setup
    path = buildNativePath("parquet/partitioned")
    parserArgs["parquetParser"] = parquetParser
    targetName = "Default Shared Root"
    db = workbook.build_dataset(
        "parquetFileWithPartition",
        targetName,
        path,
        "udf",
        recursive=True,
        parser_name="default:parseParquet",
        parser_args=parserArgs)

    dataset = db.load()
    rc = dataset.record_count()
    dataset.show()
    assert (rc == numRows)
    dsInfo = dataset.get_info()
    assert (len(dsInfo["columns"]) == len(columns))
    for col in dsInfo["columns"]:
        assert (col["name"] in columns)
    dataset.delete()


@pytest.mark.parametrize(
    "parquetParser",
    [("pyArrow"), ("native"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize(
    "columns, parserArgs, numRows",
    [
        (
            ["x", "y", "x"],
            {
    # The reason we get 2 instance of "x" back is because it has DS
    # treat x as 2 types -- "DfInt64" and "DfNull", since we're not treating Null as FnF
                "columns": ["x", "y", "x"],
                "treatNullAsFnf": False
            },
            500),
        (["x", "y"], {
            "columns": ["x", "y", "x"],
            "treatNullAsFnf": True
        }, 500),
        ([], {
            "columns": []
        }, 0),
        (
            ["x", "x"],
            {
                "columns": ["x"]
    # Default behavior of treatNullAsFnf is False
            },
            500),
        (["y"], {
            "columns": ["y"]
        }, 500),
        (["y"], {
            "partitionKeys": {},
            "columns": ["y"]
        }, 500),
    ])
def test_load_parquet_no_partition(setup, parquetParser, columns, parserArgs,
                                   numRows):
    (client, workbook, app) = setup
    path = buildNativePath("parquet/raw")
    targetName = "Default Shared Root"
    parserArgs["parquetParser"] = parquetParser
    db = workbook.build_dataset(
        "parquetFileWithPartition",
        targetName,
        path,
        "udf",
        recursive=True,
        parser_name="default:parseParquet",
        parser_args=parserArgs)

    dataset = db.load()
    rc = dataset.record_count()
    assert (rc == numRows)
    dsInfo = dataset.get_info()
    assert (len(dsInfo["columns"]) == len(columns))
    print(dsInfo["columns"])
    for col in dsInfo["columns"]:
        assert (col["name"] in columns)
    dataset.delete()


@pytest.mark.parametrize(
    "parquetParser",
    [("native"), ("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize("path, parserArgs, errMsg", [
    ("parquet/partitioned", {
        "columns": ["doesNotExist1", "doesNotExist2"]
    }, "does not exist in parquet"),
    ("parquet/partitioned", {
        "columns": ["device_family", "doesNotExist1", "doesNotExist2"]
    }, "does not exist in parquet"),
    ("parquet/partitioned", {
        "partitionKeys": {
            "date": []
        },
        "columns": ["doesNotExist1", "doesNotExist2"]
    }, "does not exist in parquet"),
    ("parquet/partitioned", {
        "partitionKeys": {
            "doesNotExist": []
        },
        "columns": ["doesNotExist1", "doesNotExist2"]
    }, "not part of file path"),
    ("parquet/raw", {
        "partitionKeys": {
            "doesNotExist": []
        },
        "columns": ["x", "y"]
    }, "not part of file path"),
    ("parquet/raw", {
        "columns": ["doesNotExist", "y"]
    }, "does not exist in parquet"),
])
def test_load_parquet_exceptions(setup, parquetParser, path, parserArgs,
                                 errMsg):
    (client, workbook, app) = setup
    path = buildNativePath(path)
    targetName = "Default Shared Root"
    parserArgs["parquetParser"] = parquetParser
    db = workbook.build_dataset(
        "parquetFileWithPartition",
        targetName,
        path,
        "udf",
        recursive=True,
        parser_name="default:parseParquet",
        parser_args=parserArgs)

    try:
        db.load()
        assert False
    except XcalarApiStatusException as e:
        assert errMsg in e.output.loadOutput.errorString


@pytest.mark.parametrize(
    "parquetParser",
    [("native"), ("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize(
    "path, errorMsg",
    [(buildNativePath("recursiveSymlink/recursiveSymlink"),
      "Too many levels of symbolic links"),
     (buildNativePath("symlink/csvSanity/movies.csv"), "Invalid parquet file")]
)
def test_load_non_parquet(setup, parquetParser, path, errorMsg):
    (client, workbook, app) = setup
    sparkParquetTarget = None
    targetName = "SparkParquetPyTest"
    targetParams = {"backingTargetName": "Default Shared Root"}
    target = client.add_data_target(targetName, "parquetds", targetParams)

    parserArgs = {"parquetParser": parquetParser}

    db = workbook.build_dataset(
        "parquetDs",
        target,
        path,
        "udf",
        recursive=True,
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    try:
        db.load()
        assert False
    except XcalarApiStatusException as e:
        assert errorMsg in e.output.loadOutput.errorString

    target.delete()


@pytest.mark.parametrize(
    "parquetParser",
    [("native"), ("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize("targetName, path, numRows", [
    ("Default Shared Root",
     buildNativePath(
         "parquet/spark/vanilla.parquet/part-r-00000-5bcda034-b509-4fd6-ba8d-9c28755fc8fb.gz.parquet"
     ), 35885),
    ("Default Shared Root", buildNativePath("parquet/datasets-from-web"), 173),
    (relativeDirTargetName,
     "/spark/vanilla.parquet/part-r-00000-5bcda034-b509-4fd6-ba8d-9c28755fc8fb.gz.parquet",
     35885),
    (sharedNothingSingleTargetName, "{}/{}".format(
        random.randrange(num_nodes),
        buildNativePath(
            "parquet/spark/vanilla.parquet/part-r-00000-5bcda034-b509-4fd6-ba8d-9c28755fc8fb.gz.parquet"
        )), 35885),
    (sharedNothingSymTargetName,
     "/spark/vanilla.parquet/part-r-00000-5bcda034-b509-4fd6-ba8d-9c28755fc8fb.gz.parquet",
     35885 * num_nodes),
    pytest.param("Default Shared Root", buildNativePath("parquet/hvr"), 201)
])
def test_load_parquet_file(setup, parquetParser, targetName, path, numRows):
    (client, workbook, app) = setup
    parserArgs = {"parquetParser": parquetParser}
    db = workbook.build_dataset(
        "parquetDs",
        targetName,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    try:
        dataset = db.load()
    except XcalarApiStatusException as e:
        if (parquetParser == "native"):
            if "Complex types in Parquet not supported" in e.output.loadOutput.errorString:
                parserArgs["columns"] = [
                    "_corrupt_record", "average_stars", "yelping_since",
                    "fans", "name"
                ]
            else:
                raise

            db = workbook.build_dataset(
                "parquetDs",
                targetName,
                path,
                "udf",
                parser_name="default:parseParquet",
                parser_args=parserArgs)
            dataset = db.load()
        else:
            raise

    assert dataset.record_count() == numRows

    dsInfo = dataset.get_info()

    dataset.delete()


@pytest.mark.parametrize(
    "backingTargetName, path",
    [("Default Shared Root", buildNativePath("yelp/user")),
     (hdfsTargetName, "/datasets/qa/yelp/user")])
def test_custom_target_pass_through(setup, backingTargetName, path):
    (client, workbook, app) = setup
    targetName = "customTarget"
    # Use absolute path for UDF since the custom target will invoke the
    # getUdf API for this UDF from a XPU without any context or workbook.
    # The absolute path prefix must match that specified in
    # UserDefinedFunction::UdfWorkBookPrefix and
    # UserDefinedFunction::UdfWorkBookDir defined
    # in XLRDIR/src/include/udf/UserDefinedFunction.h. Note that the UDF
    # was added to the workbook container defined by
    # self.user/self.wkbookId so that needs to be added into the absolute
    # path to the UDF.
    targetParams = {
        "backingTargetName":
            backingTargetName,
        "listUdf":
            "/workbook/%s/%s/udf/%s:passthrough" %
            (workbook.username, workbook._workbook_id, listFilesModule)
    }
    target = client.add_data_target(targetName, "custom", targetParams)

    db = workbook.build_dataset(
        "customJsonDs", target, path, "json", file_name_pattern="*")
    dataset = db.load()
    assert dataset.record_count() == 70817
    dataset.delete()
    target.delete()


def startswith(string, prefix):
    prefixTokens = split(prefix)
    for token in split(string):
        try:
            prefixToken = prefixTokens.__next__()
        except StopIteration as e:
            return True

        if token != prefixToken:
            return False

    try:
        prefixToken = prefixTokens.__next__()
        return False
    except StopIteration as e:
        return True


def split(string):
    strlen = len(string)
    start = 0
    cursor = 0
    while cursor < strlen:
        if string[cursor] == '\\':
            if cursor + 1 >= strlen:
                yield string[start:cursor]
                return
            cursor += 1
        elif string[cursor] == '.':
            yield string[start:cursor]
            start = cursor + 1
        cursor += 1
    yield string[start:cursor]


@pytest.mark.parametrize(
    "parquetParser",
    [("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize(
    "targetTypeId, listFilesUdf, backingTargetName, path, numRows", [
        pytest.param(
            "custom",
            "%s:browseHive" % listFilesModule,
            hiveTargetName,
            "partitioned_table",
            500,
            marks=pytest.mark.odbc),
        ("parquetds", None, "Default Shared Root",
         buildNativePath("parquet/spark/vanilla.parquet"), 70819),
        ("parquetds", None, "Default Shared Root",
         buildNativePath("parquet/spark/nestedPartitions.parquet"), 70819),
        ("parquetds", None, hdfsTargetName,
         "/datasets/qa/parquet/spark/vanilla.parquet", 70819),
        ("parquetds", None, hdfsTargetName,
         "/datasets/qa/parquet/spark/nestedPartitions.parquet", 70819),
        ("parquetds", None, relativeDirTargetName, "/spark/vanilla.parquet",
         70819),
        ("parquetds", None, relativeDirTargetName,
         "/spark/nestedPartitions.parquet", 70819),
        pytest.param(
            "parquetds",
            None,
            sharedNothingSingleTargetName,
            "{}/{}".format(
                random.randrange(num_nodes),
                buildNativePath("parquet/spark/vanilla.parquet")),
            70819,
            marks=pytest.mark.xfail),
        pytest.param(
            "parquetds",
            None,
            sharedNothingSingleTargetName,
            "{}/{}".format(
                random.randrange(num_nodes),
                buildNativePath("parquet/spark/nestedPartitions.parquet")),
            70819,
            marks=pytest.mark.xfail),
        pytest.param(
            "parquetds",
            None,
            sharedNothingSymTargetName,
            "/spark/vanilla.parquet",
            70819,
            marks=pytest.mark.xfail),
        pytest.param(
            "parquetds",
            None,
            sharedNothingSymTargetName,
            "/spark/nestedPartitions.parquet",
            70819,
            marks=pytest.mark.xfail)
    ])
def test_load_parquet_ds(setup, parquetParser, targetTypeId, listFilesUdf,
                         backingTargetName, path, numRows):
    (client, workbook, app) = setup
    sparkParquetTarget = None
    if listFilesUdf is not None:
        # Make sure the hard-coded absolute path name components below match
        # those in XLRDIR/src/include/udf/UserDefinedFunction.h
        # (UdfWorkBookPrefix and UdfWorkBookDir).
        #
        # XXX: Eventually, target params should take user and wkbookId, and
        # the target App should be extended to do the full qualification for
        # UDF name
        fullyQUdf = "/workbook/%s/%s/udf/" % (
            workbook.username, workbook._workbook_id) + listFilesUdf
        targetName = "ParquetPyTest-%s-udf-%s" % (targetTypeId, str(fullyQUdf))
        targetParams = {
            "backingTargetName": backingTargetName,
            "listUdf": fullyQUdf
        }
    else:
        targetName = "ParquetPyTest-%s-udf" % (targetTypeId)
        targetParams = {"backingTargetName": backingTargetName}

    target = client.add_data_target(targetName, targetTypeId, targetParams)

    appName = "XcalarParquet"
    appArgs = {
        "func": "getInfo",
        "targetName": targetName,
        "path": path,
        "parquetParser": parquetParser
    }
    (outStr, errStr) = app.run_py_app(appName, False, json.dumps(appArgs))
    try:
        appOutput = json.loads(json.loads(outStr)[0][0])
    except Exception as ex:
        assert (outStr is False)
    assert appOutput["validParquet"]

    # First, we test a simple load
    parserArgs = {"parquetParser": parquetParser}
    db = workbook.build_dataset(
        "parquetDs",
        target,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    dataset = db.load()

    assert dataset.record_count() == numRows

    dsInfo = dataset.get_info()
    for column in dsInfo["columns"]:
        expectedCol = appOutput["schema"][column["name"]]
        assert (expectedCol["xcalarType"] == column["type"]
                or "DfNull" == column["type"])    # this might be NULL

    dataset.delete()

    # Next, let's extract some columns and fields of interest

    # Pick random columns
    flattenedSchema = flattenSchema(appOutput["schema"])
    columns = random.sample(flattenedSchema.keys(),
                            random.randrange(1,
                                             len(flattenedSchema) + 1))

    # Pick random partition keys
    partKeyValues = {}
    for key in appOutput["partitionKeys"]:
        appArgs = {
            "func": "getPossibleKeyValues",
            "key": key,
            "givenKeys": partKeyValues,
            "path": path,
            "targetName": targetName,
            "parquetParser": parquetParser
        }
        (outStr, errStr) = app.run_py_app(appName, False, json.dumps(appArgs))

        tmpOutput = json.loads(json.loads(outStr)[0][0])
        try:
            # XXX temporary fix for SDK-482, to limit the length
            # of query string, remove this once properly fixed
            partKeyValues[key] = random.sample(
                tmpOutput, random.randrange(1,
                                            min(10, len(tmpOutput)) + 1))
        except ValueError as e:
            partKeyValues[key] = ["*"]

    parserArgs = {
        "parquetParser": parquetParser,
        "columns": columns,
        "partitionKeys": partKeyValues
    }
    print("parserArgs: {}".format(parserArgs))
    db = workbook.build_dataset(
        "parquetDs",
        targetName,
    # "%s?%s" % (path, queryStr),
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    dataset = db.load()

    count = dataset.record_count()
    assert count > 0 and count <= numRows

    columns.extend(partKeyValues.keys())
    columns = list(set(columns))
    row = dataset.get_row(0)

    # Check all columns accounted for (we didn't get anything more than we asked for)
    flattenedRow = flattenRow(row)
    print("flattenedRow: %s" % json.dumps(flattenedRow))
    print("columnsOfInterest: %s" % json.dumps(columns))
    print("partKeyValues: %s" % json.dumps(partKeyValues))
    print("rowValue: %s" % json.dumps(row))
    for col in flattenedRow:
        found = False
        if flattenedRow[col] is not None:
            for colAskedFor in columns:
                if startswith(col, colAskedFor):
                    found = True
                    break
        else:
            # If we ask for compliments.cool, but compliments is null, we'll get
            # just compliments
            for colAskedFor in columns:
                if startswith(col, colAskedFor) or startswith(
                        colAskedFor, col):
                    found = True
                    break

        assert found, "Got a column we didn't ask for %s" % col

    # Now make sure we have every column
    for colAskedFor in columns:
        found = False
        for col in flattenedRow:
            if flattenedRow[col] is not None:
                if startswith(col, colAskedFor):
                    found = True
                    break
            else:
                if startswith(col, colAskedFor) or startswith(
                        colAskedFor, col):
                    found = True
                    break
        assert found, "Asked for column but could not find column %s" % colAskedFor

    for partKey in partKeyValues:
        if row[partKey] is None:
            assert "__HIVE_DEFAULT_PARTITION__" in partKeyValues[partKey]
        else:
            assert row[partKey] in partKeyValues[partKey]

    dataset.delete()
    target.delete()


@pytest.mark.parametrize(
    "parquetParser",
    [("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize(
    "targetTypeId, listFilesUdf, backingTargetName, path, numRows",
    [("parquetds", None, "Default Shared Root",
      buildNativePath("parquet/spark/nestedPartitions.parquet"), 70819)])
def test_load_only_partitionkeys(setup, parquetParser, targetTypeId,
                                 listFilesUdf, backingTargetName, path,
                                 numRows):
    (client, workbook, app) = setup
    sparkParquetTarget = None
    if listFilesUdf is not None:
        # Make sure the hard-coded absolute path name components below match
        # those in XLRDIR/src/include/udf/UserDefinedFunction.h
        # (UdfWorkBookPrefix and UdfWorkBookDir).
        #
        # XXX: Eventually, target params should take user and wkbookId, and
        # the target App should be extended to do the full qualification for
        # UDF name
        fullyQUdf = "/workbook/%s/%s/udf/" % (
            workbook.username, workbook._workbook_id) + listFilesUdf
        targetName = "ParquetPyTest-%s-udf-%s" % (targetTypeId, str(fullyQUdf))
        targetParams = {
            "backingTargetName": backingTargetName,
            "listUdf": fullyQUdf
        }
    else:
        targetName = "ParquetPyTest-%s-udf" % (targetTypeId)
        targetParams = {"backingTargetName": backingTargetName}

    target = client.add_data_target(targetName, targetTypeId, targetParams)

    appName = "XcalarParquet"
    appArgs = {
        "func": "getInfo",
        "targetName": targetName,
        "path": path,
        "parquetParser": parquetParser
    }
    (outStr, errStr) = app.run_py_app(appName, False, json.dumps(appArgs))

    appOutput = json.loads(json.loads(outStr)[0][0])
    assert appOutput["validParquet"]

    # First, we test a simple load
    parserArgs = {"parquetParser": parquetParser}
    db = workbook.build_dataset(
        "parquetDs",
        target,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    dataset = db.load()

    assert dataset.record_count() == numRows

    dsInfo = dataset.get_info()
    for column in dsInfo["columns"]:
        expectedCol = appOutput["schema"][column["name"]]
        assert (expectedCol["xcalarType"] == column["type"]
                or "DfNull" == column["type"])    # this might be NULL

    dataset.delete()

    # Pick random partition keys
    partKeyValues = {}
    for key in appOutput["partitionKeys"]:
        appArgs = {
            "func": "getPossibleKeyValues",
            "key": key,
            "givenKeys": partKeyValues,
            "path": path,
            "targetName": targetName,
            "parquetParser": parquetParser
        }
        (outStr, errStr) = app.run_py_app(appName, False, json.dumps(appArgs))

        tmpOutput = json.loads(json.loads(outStr)[0][0])
        try:
            # XXX temporary fix for SDK-482, to limit the length
            # of query string, remove this once properly fixed
            partKeyValues[key] = random.sample(
                tmpOutput, random.randrange(1,
                                            min(10, len(tmpOutput)) + 1))
        except ValueError as e:
            partKeyValues[key] = ["*"]

    # Pick columns from partition keys only
    columns = random.sample(partKeyValues.keys(),
                            random.randrange(1,
                                             len(partKeyValues) + 1))

    parserArgs = {
        "parquetParser": parquetParser,
        "columns": columns,
        "partitionKeys": partKeyValues
    }
    print("parserArgs: {}".format(parserArgs))
    db = workbook.build_dataset(
        "parquetDs",
        targetName,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    dataset = db.load()

    count = dataset.record_count()
    assert count > 0 and count <= numRows

    columns = list(partKeyValues.keys())
    row = dataset.get_row(0)

    # Check all columns accounted for (we didn't get anything more than we asked for)
    flattenedRow = flattenRow(row)
    print("flattenedRow: %s" % json.dumps(flattenedRow))
    print("columnsOfInterest: %s" % json.dumps(columns))
    print("partKeyValues: %s" % json.dumps(partKeyValues))
    print("rowValue: %s" % json.dumps(row))
    for col in flattenedRow:
        found = False
        if flattenedRow[col] is not None:
            for colAskedFor in columns:
                if startswith(col, colAskedFor):
                    found = True
                    break
        else:
            # If we ask for compliments.cool, but compliments is null, we'll get
            # just compliments
            for colAskedFor in columns:
                if startswith(col, colAskedFor) or startswith(
                        colAskedFor, col):
                    found = True
                    break

        assert found, "Got a column we didn't ask for %s" % col

    # Now make sure we have every column
    for colAskedFor in columns:
        found = False
        for col in flattenedRow:
            if flattenedRow[col] is not None:
                if startswith(col, colAskedFor):
                    found = True
                    break
            else:
                if startswith(col, colAskedFor) or startswith(
                        colAskedFor, col):
                    found = True
                    break
        assert found, "Asked for column but could not find column %s" % colAskedFor

    for partKey in partKeyValues:
        if row[partKey] is None:
            assert "__HIVE_DEFAULT_PARTITION__" in partKeyValues[partKey]
        else:
            assert row[partKey] in partKeyValues[partKey]

    dataset.delete()
    target.delete()


@pytest.mark.parametrize(
    "parquetParser",
    [("pyArrow"),
     pytest.param("parquetTools", marks=pytest.mark.parquetTools)])
@pytest.mark.parametrize(
    "targetTypeId, listFilesUdf, backingTargetName, path", [
        pytest.param(
            "custom",
            "%s:browseHive" % listFilesModule,
            hiveTargetName,
            "partitioned_table",
            marks=pytest.mark.odbc),
        ("parquetds", None, "Default Shared Root",
         buildNativePath("parquet/spark/nestedPartitions.parquet")),
        ("parquetds", None, hdfsTargetName,
         "/datasets/qa/parquet/spark/nestedPartitions.parquet")
    ])
def test_bogus_col_and_part_key(setup, parquetParser, targetTypeId,
                                listFilesUdf, backingTargetName, path):
    (client, workbook, app) = setup
    sparkParquetTarget = None
    if listFilesUdf is not None:
        # Make sure the hard-coded absolute path name components below match
        # those in XLRDIR/src/include/udf/UserDefinedFunction.h
        # (UdfWorkBookPrefix and UdfWorkBookDir).
        #
        # XXX: Eventually, target params should take user and wkbookId, and
        # the target App should be extended to do the full qualification for
        # UDF name
        fullyQUdf = "/workbook/%s/%s/udf/" % (
            workbook.username, workbook._workbook_id) + listFilesUdf
        targetName = "ParquetBogusTest-%s-%s" % (targetTypeId, str(fullyQUdf))
        targetParams = {
            "backingTargetName": backingTargetName,
            "listUdf": fullyQUdf
        }
    else:
        targetName = "ParquetBogusTest-%s" % targetTypeId
        targetParams = {"backingTargetName": backingTargetName}

    target = client.add_data_target(targetName, targetTypeId, targetParams)

    # Pick some bogus columns
    parserArgs = {"parquetParser": parquetParser, "columns": ["doesNotExist"]}
    db = workbook.build_dataset(
        "bogusParquetDs",
        targetName,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    try:
        dataset = db.load()
        assert False
    except XcalarApiStatusException as e:
        assert "does not exist" in e.output.loadOutput.errorString

    # Pick some bogus partition keys
    parserArgs = {
        "parquetParser": parquetParser,
        "partitionKeys": {
            "bogusKey": "doesNotExist"
        }
    }
    db = workbook.build_dataset(
        "bogusParquetDs",
        target,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    try:
        dataset = db.load()
        assert dataset.record_count() == 0
        dataset.delete()
    except XcalarApiStatusException as e:
        assert "No files found at" in e.output.loadOutput.errorString
        assert path in e.output.loadOutput.errorString
        assert targetName in e.output.loadOutput.errorString

    # Pick some bogus partition values
    appName = "XcalarParquet"
    appArgs = {
        "func": "getInfo",
        "targetName": targetName,
        "path": path,
        "parquetParser": parquetParser
    }
    (outStr, errStr) = app.run_py_app(appName, False, json.dumps(appArgs))

    appOutput = json.loads(json.loads(outStr)[0][0])
    assert appOutput["validParquet"]

    partKeyValues = {key: "bogus" for key in appOutput["partitionKeys"]}

    parserArgs = {
        "parquetParser": parquetParser,
        "partitionKeys": partKeyValues
    }
    db = workbook.build_dataset(
        "bogusParquetDs",
        target,
        path,
        "udf",
        parser_name="default:parseParquet",
        parser_args=parserArgs)
    try:
        dataset = db.load()
        assert dataset.record_count() == 0
        dataset.delete()
    except XcalarApiStatusException as e:
        assert "No files found at" in e.output.loadOutput.errorString
        assert path in e.output.loadOutput.errorString
        assert targetName in e.output.loadOutput.errorString

    target.delete()
