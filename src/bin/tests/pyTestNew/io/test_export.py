# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import pytest
import tempfile
import os
import re
import csv
import random
import string
import time
import subprocess
import multiprocessing
from socket import gethostname
import pandas as pd
import decimal
import math
import hashlib
import gzip

from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiColumnT
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.util.Qa import (XcalarQaDatasetPath, DefaultTargetName,
                                    buildNativePath, buildS3Path)
from xcalar.compute.util.Qa import XcalarQaS3ClientArgs
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from xcalar.external.dataflow import Dataflow

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own cluster")

simple_driver = """
import xcalar.container.driver.base as driver

@driver.register_export_driver(name="test_export csv driver")
@driver.param(name="file_path", type=driver.STRING, desc="exported file name")
#@driver.param(name="driver param 4", type=driver.TARGET,
#              desc="test driver param4",
#              optional=True, secret=True)
def driver(table, file_path):
    return

def placeholder():
    return 1 # this is needed because otherwise this UDF module just gets swallowed
"""

legacy_export_udf_name = "basicLegacy"
legacy_export_udf = """
from os.path import exists, dirname
import json
import os

def main(inStr):
    inObj = json.loads(inStr)
    contents = inObj["fileContents"]
    fullPath = inObj["filePath"]
    fn = fullPath.replace("nfs://", "").replace("file://", "").replace("localfile://", "")
    if not exists(dirname(fn)):
        try:
            os.makedirs(dirname(fn))
        except:
            pass
    with open(fn, "w") as f:
        f.write(contents)
"""

shared_qa_target_name = "QA shared target"
s3_minio_target_name = "QA S3 minio"
gcs_target_name = "QA GCS"
azblob_target_name = "QA azblob"
mapr_qa_target_name = "QA secured MapR"
gen_table_target_name = "TableGen"
target_infos = [
    (s3_minio_target_name, "s3fullaccount", {
        "access_key": XcalarQaS3ClientArgs["aws_access_key_id"],
        "secret_access_key": XcalarQaS3ClientArgs["aws_secret_access_key"],
        "endpoint_url": XcalarQaS3ClientArgs["endpoint_url"],
        "region_name": XcalarQaS3ClientArgs["region_name"],
        "verify": "true" if XcalarQaS3ClientArgs["verify"] else "false",
    }), (gcs_target_name, "gcsenviron", {}),
    (azblob_target_name, "azblobfullaccount", {
        "account_name":
            "xcdatasetswestus2",
        "sas_token":
            "?sv=2018-03-28&ss=b&srt=co&sp=rl&se=2019-12-05T04:36:26Z&st=2018-12-04T20:36:26Z&spr=https&sig=5yPWa%2BE8qPOLbhiLOGrX3aFB9eQmaQXancA9VMIFpU0%3D"
    }), (shared_qa_target_name, "shared", {
        "mountpoint": XcalarQaDatasetPath
    }),
    (mapr_qa_target_name, "maprfullaccount", {
        "username": "dwillis",
        "password": "dwillis",
        "cluster_name": "mrtest",
    })
]


# Fixtures
@pytest.fixture(scope="module")
def client():
    with DevCluster(num_nodes=1) as cluster:
        yield cluster.client()


@pytest.fixture(scope="module")
def workbook(client):
    wb = client.create_workbook("export_test")
    yield wb
    wb.delete()


@pytest.fixture(scope="module")
def udf(workbook):
    xc_api = XcalarApi()
    xc_api.setSession(workbook)

    udf_lib = Udf(xc_api)
    return udf_lib


@pytest.fixture(scope="module", autouse=True)
def drivers(udf):
    udf_name = "/sharedUDFs/exportTestUdfSimple"
    udf.addOrUpdate(udf_name, simple_driver)
    yield
    udf.delete(udf_name)


@pytest.fixture(scope="module", autouse=True)
def udfs(udf):
    udf_list = [(legacy_export_udf_name, legacy_export_udf)]

    for name, source in udf_list:
        full_udf_name = "/sharedUDFs/{}".format(name)
        udf.addOrUpdate(full_udf_name, source)
    yield
    for name, _ in udf_list:
        full_udf_name = "/sharedUDFs/{}".format(name)
        udf.delete(full_udf_name)


@pytest.fixture(scope="module", autouse=True)
def targets(client):
    for target_name, target_type_id, target_args in target_infos:
        client.add_data_target(target_name, target_type_id, target_args)

    yield
    # cleanup
    for target_name, _, _ in target_infos:
        client.get_data_target(target_name).delete()


@pytest.fixture(scope="module")
def telecom_table(workbook):
    file_path = "exportTests/telecom.csv"
    full_path = buildNativePath(file_path)
    table_name = "telecom-table"
    db = workbook.build_dataset("pytest-telecom-ds", DefaultTargetName,
                                full_path, "csv")
    db.option("fieldDelim", ",").option("schemaMode", "header")
    dataset = db.load()

    xc_api = XcalarApi()
    xc_api.setSession(workbook)

    operators = Operators(xc_api)

    operators.indexDataset(dataset.name, table_name, "State", "p")
    session = workbook.activate()
    table = session.get_table(table_name)
    yield table
    table.drop()


@pytest.fixture(scope="module")
def lineitem_table(workbook):
    """
    Dataset used to make a table of immediates to export.
    """
    file_path = "exportTests/tpch_sf1_lineitem.csv"
    full_path = buildNativePath(file_path)
    table_name = "lineitem-table"
    temp_table = "temp-lineitem"
    db = workbook.build_dataset("pytest-lineitem-ds", DefaultTargetName,
                                full_path, "csv")
    db.option("fieldDelim", "|").option("schemaMode", "header")
    dataset = db.load()

    xc_api = XcalarApi()
    xc_api.setSession(workbook)

    fpt = "fpt"
    columns = [("L_PARTKEY", "int"), ("L_SHIPDATE", "timestamp"),
               ("L_EXTENDEDPRICE", "money"), ("L_TAX", "float"),
               ("L_COMMENT", "string")]

    operators = Operators(xc_api)

    operators.indexDataset(dataset.name, temp_table, columns[0][0], fpt)
    eval_strs = ["{}({}::{})".format(c[1], fpt, c[0]) for c in columns]
    new_columns = ["{}_{}".format(c[0], c[1]) for c in columns]
    operators.map(temp_table, table_name, eval_strs, new_columns)

    session = workbook.activate()
    table = session.get_table(table_name)
    yield table
    table.drop()


@pytest.fixture(scope="module")
def wide_string_column_table(workbook):
    num_columns = 1000
    column_width = 10
    num_rows = 8 * 10**4
    num_files = multiprocessing.cpu_count()
    rows_per_file = num_rows // num_files
    column_names = ["column{}".format(ii) for ii in range(num_columns)]

    # In order to speed this up we're just going to generate a few
    # characteristic rows and then we'll repeat these until we get the
    # appropriate number of rows
    batch_size = 100
    field_delim = ","
    record_delim = "\n"
    num_batches = rows_per_file // batch_size
    num_leftovers = rows_per_file - num_batches * batch_size
    assert not num_leftovers, "num_rows should be a multiple of num_batches"

    def unique_field():
        return ''.join(
            random.choice(string.ascii_uppercase + string.digits)
            for _ in range(column_width))

    def unique_row():
        return field_delim.join(unique_field() for _ in range(num_columns))

    row_batch = record_delim.join([unique_row()
                                   for _ in range(batch_size)]) + record_delim

    with tempfile.TemporaryDirectory() as tmpdir:
        for ii in range(num_files):
            path = os.path.join(tmpdir, f"file{ii}.csv")
            with open(path, "w", buffering=2**20) as f:
                for _ in range(num_batches):
                    f.write(row_batch)
        db = workbook.build_dataset("wide dataset", DefaultTargetName, tmpdir,
                                    "csv")
        db.option("fieldDelim", field_delim).option("recordDelim",
                                                    record_delim).option(
                                                        "schemaMode", "none")
        dataset = db.load()
        assert dataset.record_count() == num_rows

    # We now need to index the dataset into a table
    table_name = "wide string table"
    synth_table_name = "wide string table synthesized"
    xc_api = XcalarApi()
    xc_api.setSession(workbook)

    operators = Operators(xc_api)
    operators.indexDataset(dataset.name, table_name, "column0", "a")
    operators.synthesize(table_name, synth_table_name, [
        XcalarApiColumnT("a::{}".format(col), col, "DfString")
        for col in column_names
    ])

    table = workbook.get_table(synth_table_name)
    schema = table.schema
    meta = table.get_meta()
    yield table
    table.drop()
    dataset.delete()


# Helpers


def du(path):
    result_str = subprocess.run(["du", "-d0", path],
                                check=True,
                                encoding="utf-8",
                                stdout=subprocess.PIPE).stdout
    size, _ = result_str.split()
    return int(size) * 2**10


# Test export from Fat Pointers


def test_single_csv(telecom_table):
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]
    with tempfile.TemporaryDirectory() as outdir:
        path = os.path.join(outdir, "basic_test_file.csv")
        telecom_table.export(columns, "single_csv", {
            "file_path": path,
            "target": DefaultTargetName,
            "field_delim": ","
        })

        with open(path) as f:
            lines = f.read().splitlines()
        header = lines[0]
        data_lines = lines[1:]
        assert header.split(",") == [c[1] for c in columns]
        assert len(data_lines) == 5000
        # we want to check that all the values we expect are here; lets sum a col
        col_sum = sum([float(line.split(",")[2]) for line in data_lines])
        assert abs(col_sum - 153248.34) < 0.0001


@pytest.mark.parametrize("target_name,pathBuilder", [
    (DefaultTargetName, buildNativePath),
    (s3_minio_target_name, buildS3Path),
])
def test_targets_single_csv(telecom_table, workbook, target_name, pathBuilder):
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]
    path = pathBuilder(
        os.path.join("targetExports", gethostname(), "single_csv_test.csv"))

    telecom_table.export(columns, "single_csv", {
        "file_path": path,
        "target": target_name
    })

    # We have exported the file; let's load it back and make sure it works
    db = workbook.build_dataset("csv_export_test", target_name, path, "csv")
    db.option("fieldDelim", "\t")
    dataset = db.load()

    source_columns_set = {column[1] for column in columns}
    dataset_columns_set = set(dataset.get_row(0).keys())

    assert source_columns_set == dataset_columns_set
    assert dataset.record_count() == 5000

    dataset.delete()


@pytest.mark.parametrize('field_delim,message',
                         [("Tim", "is not a valid single UTF-8 character")])
def test_failure_single_csv(telecom_table, field_delim, message):
    target_name = DefaultTargetName
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number")]
    try:
        with tempfile.TemporaryDirectory() as outdir:
            path = os.path.join(outdir, "failure_test_file.csv")
            telecom_table.export(
                columns, "single_csv", {
                    "file_path": path,
                    "target": DefaultTargetName,
                    "field_delim": field_delim
                })
            assert False
    except XcalarApiStatusException as e:
        assert message in e.result.output.hdr.log


@pytest.mark.parametrize(
    'header,field_delim,record_delim,quote_delim',
    [
    # Different delimiters
        (True, (',', ','), ('\n', '\n'), ('"', '"')),
        (False, (',', ','), ('\n', '\n'), ("'", "'")),
        (True, ('\t', '\t'), ('\n', '\n'), ('"', '"')),
    # Used for quote testing, float sum will fail if quotes aren't handled
        (False, ('.', '.'), ('\n', '\n'), ("'", "'")),
    # Escape sequences from UI are a string starting with back slash
        (True, ('\\t', '\t'), ('\\n', '\n'), ('\\x22', '"')),
        (False, ('\\u2566', '\u2566'), ('\\n', '\n'), ('\\x27', "'")),
    # Unicode characters can also be specified
        (True, ('\u03C0', 'π'), ('\\n', '\n'), ('"', '"')),
    ])
def test_multiple_csv(telecom_table, header, field_delim, record_delim,
                      quote_delim):
    # XXX Python csv.reader ignores lineterminator as of 3.7.3 :(
    target_name = DefaultTargetName
    file_base = "test_multiple_csv_pytest"
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]
    with tempfile.TemporaryDirectory() as outdir:
        telecom_table.export(
            columns, "multiple_csv", {
                "directory_path": outdir,
                "target": target_name,
                "file_base": file_base,
                "header": header,
                "field_delim": field_delim[0],
                "record_delim": record_delim[0],
                "quote_delim": quote_delim[0]
            })
        reader_kwargs = {
            "delimiter": field_delim[1],
            "lineterminator": record_delim[1],
            "quotechar": quote_delim[1]
        }
        data_lines = []
        for fn in os.listdir(outdir):
            assert fn.startswith(file_base)
            path = os.path.join(outdir, fn)
            with open(path) as f:
                reader = csv.reader(f, **reader_kwargs)
                if header:
                    header_line = next(reader)
                    assert header_line == [c[1] for c in columns]
                data_lines += [r for r in reader]
    assert len(data_lines) == 5000
    # we want to check that all the values we expect are here; lets sum a col
    col_sum = sum([float(line[2]) for line in data_lines])
    assert abs(col_sum - 153248.34) < 0.0001


@pytest.mark.parametrize(
    'field_delim,record_delim,quote_delim',
    [
    # Different delimiters
        ((',', ','), ('\n', '\n'), ('"', "'")),
        ((',', ','), ('\n', '\n'), ("'", "'")),
        (('\t', '\t'), ('\n', '\n'), ('"', '"')),
    # Used for quote testing, float sum will fail due to period
        (('.', '.'), ('\n', '\n'), ("'", "'")),
    # Escape sequences from UI are a string starting with back slash
        (('\\t', '\t'), ('\\n', '\n'), ('"', '"')),
        (('\\x27', "'"), ('\\n', '\n'), ('"', '"'))
    ])
def test_fast_csv(telecom_table, field_delim, record_delim, quote_delim):
    target_name = DefaultTargetName
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]
    with tempfile.TemporaryDirectory() as outdir:
        telecom_table.export(
            columns, "fast_csv", {
                "directory_path": outdir,
                "target": target_name,
                "field_delim": field_delim[0],
                "record_delim": record_delim[0],
                "quote_delim": quote_delim[0]
            })
        reader_kwargs = {
            "delimiter": field_delim[1],
            "lineterminator": record_delim[1],
            "quotechar": quote_delim[1]
        }
        data_lines = []
        for fn in os.listdir(outdir):
            path = os.path.join(outdir, fn)
            with open(path) as f:
                reader = csv.reader(f, **reader_kwargs)
                data_lines += [r for r in reader]
    # The data should be partitioned such that it results in multiple files
    assert len(data_lines) == 5000
    # we want to check that all the values we expect are here; lets sum a col
    col_sum = sum([float(line[2]) for line in data_lines])
    assert abs(col_sum - 153248.34) < 0.0001


@pytest.mark.parametrize('field_delim,message',
                         [("Tim", "is not a valid single ASCII character"),
                          ("\\u2566", "is not a valid single ASCII character"),
                          ("\xFC", "is not a valid single ASCII character")])
def test_failure_fast_csv(telecom_table, field_delim, message):
    target_name = DefaultTargetName
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number")]
    try:
        with tempfile.TemporaryDirectory() as outdir:
            telecom_table.export(
                columns, "fast_csv", {
                    "directory_path": outdir,
                    "target": target_name,
                    "field_delim": field_delim
                })
            assert False
    except XcalarApiStatusException as e:
        assert message in e.result.output.hdr.log


@pytest.mark.parametrize(
    'header,fieldDelim,recordDelim,quoteDelim',
    [
    # Different header types
        ('none', ',', '\n', '"'),
        ('separate', ',', '\n', '"'),
        ('every', ',', '\n', '"'),
    # Delimiters
        ('none', ',', '\n', '"'),
        ('none', '\t', '\n', '"'),
        ('none', ',', '\r\n', '"'),
        ('none', ',', '\n', '\''),
    ])
def test_simple_legacy_udf(telecom_table, header, fieldDelim, recordDelim,
                           quoteDelim):
    file_base = "legacy_udf_file"
    file_name = "{}.csv".format(file_base)
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]

    # https://docs.python.org/3/library/csv.html#csv.Dialect.lineterminator
    # > Note The reader is hard-coded to recognise either '\r' or '\n' as
    # > end-of-line, and ignores lineterminator. This behavior may change in
    # > the future.
    # So we cannot easily test for other line terminators
    assert recordDelim in ['\n', '\r\n']

    with tempfile.TemporaryDirectory() as outdir:
        telecom_table.export(
            columns, "legacy_udf", {
                "directory_path": outdir,
                "file_name": file_name,
                "export_udf": legacy_export_udf_name,
                "header": header,
                "fieldDelim": fieldDelim,
                "recordDelim": recordDelim,
                "quoteDelim": quoteDelim,
            })
        files = []    # list of lists of records
        dirs = os.listdir(outdir)
        assert len(dirs) == 1    # all files go into 1 subdirectory
        export_dir = os.path.join(outdir, dirs[0])

        def is_data_file(fn):
            return bool(
                re.match(
                    r'{file_base}-n[0-9]+-c[0-9]+-p[0-9]+\.csv'.format(
                        file_base=file_base), fn))

        def is_header_file(fn):
            return bool(
                re.match(
                    r'{file_base}-header\.csv'.format(file_base=file_base),
                    fn))

        dir_files = os.listdir(export_dir)
        # All files should be data files, there's no separate header file
        if header in ['none', 'every']:
            assert all([is_data_file(fn) for fn in dir_files])

        # We should have exactly 1 header file
        if header == 'separate':
            assert sum([is_header_file(fn) for fn in dir_files]) == 1
            assert all(
                [is_data_file(fn) or is_header_file(fn) for fn in dir_files])

        for fn in dir_files:
            path = os.path.join(export_dir, fn)

            if is_header_file(fn):
                with open(path) as f:
                    contents = f.read()
                expected_header = fieldDelim.join([c[1] for c in columns
                                                   ]) + recordDelim
                assert contents == expected_header

            if is_data_file(fn):
                with open(path) as f:
                    reader_args = {
                        "delimiter": fieldDelim,
                        "quotechar": '~',
                        "lineterminator": recordDelim
                    }
                    if header in ['none', 'separate']:
                        reader_args["fieldnames"] = [c[1] for c in columns]
                    reader = csv.DictReader(f, **reader_args)
                    rows = list(reader)
                files.append(rows)
    # flatten our list of lists
    all_records = [rec for each_file in files for rec in each_file]
    assert len(all_records) == 5000
    # we want to check that all the values we expect are here; lets sum a col
    col_sum = sum([float(rec['Total Day Charge']) for rec in all_records])
    assert abs(col_sum - 153248.34) < 0.0001


# Test export from immediates


def test_immediates_single_csv(lineitem_table):
    columns = [("L_EXTENDEDPRICE_money", "L_EXTENDEDPRICE"),
               ("L_TAX_float", "L_TAX"), ("L_PARTKEY_int", "L_PARTKEY"),
               ("L_SHIPDATE_timestamp", "L_SHIPDATE"),
               ("L_COMMENT_string", "L_COMMENT")]
    field_delim = ","
    expected_lines = 999
    with tempfile.TemporaryDirectory() as outdir:
        path = os.path.join(outdir, "test_immediates_single_csv.csv")
        lineitem_table.export(
            columns, "single_csv", {
                "file_path": path,
                "target": DefaultTargetName,
                "field_delim": field_delim
            })
        with open(path) as f:
            lines = f.read().splitlines()
        header = lines[0]
        data_lines = lines[1:]
        assert header.split(field_delim) == [c[1] for c in columns]
        assert len(data_lines) == expected_lines

    # Check L_EXTENDEDPRICE_money column: actual: 37593280.34
    col_sum = sum(
        [decimal.Decimal(line.split(field_delim)[0]) for line in data_lines])
    assert col_sum == decimal.Decimal('37593280.34')

    # Check L_TAX_float column: actual: 40.469999999999736
    col_sum = sum([float(line.split(field_delim)[1]) for line in data_lines])
    assert abs(col_sum - 40.47) < 0.0001

    # Check L_PARTKEY_int column: actual: 101703809
    col_sum = sum([int(line.split(field_delim)[2]) for line in data_lines])
    assert col_sum == 101703809

    # Check datatime format of random line
    datetime_fmt = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z'
    random_line = random.randint(0, expected_lines - 1)
    random_date_field = data_lines[random_line].split(field_delim)[3]
    assert re.search(datetime_fmt, random_date_field) is not None


def test_immediates_multiple_csv(lineitem_table):
    target_name = DefaultTargetName
    file_base = "test_lineitem_multi_pytest"
    columns = [("L_EXTENDEDPRICE_money", "L_EXTENDEDPRICE"),
               ("L_TAX_float", "L_TAX"), ("L_PARTKEY_int", "L_PARTKEY"),
               ("L_SHIPDATE_timestamp", "L_SHIPDATE"),
               ("L_COMMENT_string", "L_COMMENT")]
    field_delim = "\t"    # defaults field separator
    expected_lines = 999
    with tempfile.TemporaryDirectory() as outdir:
        lineitem_table.export(
            columns, "multiple_csv", {
                "directory_path": outdir,
                "target": target_name,
                "file_base": file_base
            })
        file_lines = []
        for fn in os.listdir(outdir):
            assert fn.startswith(file_base)
            path = os.path.join(outdir, fn)
            with open(path) as f:
                lines = f.readlines()
            header_line = lines[0]
            assert header_line == field_delim.join([c[1]
                                                    for c in columns]) + "\n"
            file_lines.append(lines[1:])
    # flatten our list of lists
    data_lines = [line for lines in file_lines for line in lines]
    # The data should be partitioned such that it results in multiple files
    assert len(file_lines) > 1
    assert len(data_lines) == expected_lines

    # Check L_EXTENDEDPRICE_money column: actual: 37593280.34
    col_sum = sum(
        [decimal.Decimal(line.split(field_delim)[0]) for line in data_lines])
    assert col_sum == decimal.Decimal('37593280.34')

    # Check L_TAX_float column: actual: 40.469999999999736
    col_sum = sum([float(line.split(field_delim)[1]) for line in data_lines])
    assert abs(col_sum - 40.47) < 0.0001

    # Check L_PARTKEY_int column: actual: 101703809
    col_sum = sum([int(line.split(field_delim)[2]) for line in data_lines])
    assert col_sum == 101703809

    # Check datatime format of random line
    datetime_fmt = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z'
    random_line = random.randint(0, expected_lines - 1)
    random_date_field = data_lines[random_line].split(field_delim)[3]
    assert re.search(datetime_fmt, random_date_field) is not None


def test_immediates_fast_csv(lineitem_table):
    target_name = DefaultTargetName
    columns = [("L_EXTENDEDPRICE_money", "L_EXTENDEDPRICE"),
               ("L_TAX_float", "L_TAX"), ("L_PARTKEY_int", "L_PARTKEY"),
               ("L_SHIPDATE_timestamp", "L_SHIPDATE"),
               ("L_COMMENT_string", "L_COMMENT")]
    field_delim = "\t"    # default field delim
    expected_lines = 999
    with tempfile.TemporaryDirectory() as outdir:
        lineitem_table.export(columns, "fast_csv", {
            "directory_path": outdir,
            "target": target_name
        })

        file_lines = []
        for fn in os.listdir(outdir):
            path = os.path.join(outdir, fn)
            with open(path) as f:
                lines = f.readlines()
            file_lines.append(lines)
    # flatten our list of lists
    data_lines = [line for lines in file_lines for line in lines]

    # The data should be partitioned such that it results in multiple files
    assert len(file_lines) > 1
    assert len(data_lines) == expected_lines

    # Check L_EXTENDEDPRICE_money column: actual: 37593280.34
    col_sum = sum(
        [decimal.Decimal(line.split(field_delim)[0]) for line in data_lines])
    assert col_sum == decimal.Decimal('37593280.34')

    # Check L_TAX_float column: actual: 40.469999999999736
    col_sum = sum([float(line.split(field_delim)[1]) for line in data_lines])
    assert abs(col_sum - 40.47) < 0.0001

    # Check L_PARTKEY_int column: actual: 101703809
    col_sum = sum([int(line.split(field_delim)[2]) for line in data_lines])
    assert col_sum == 101703809

    # Check datatime format of random line
    datetime_fmt = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z'
    random_line = random.randint(0, expected_lines - 1)
    random_date_field = data_lines[random_line].split(field_delim)[3]
    assert re.search(datetime_fmt, random_date_field) is not None


@pytest.mark.skip(reason="excel might not be included as a builtin")
def test_excel(telecom_table):
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]
    # with tempfile.TemporaryDirectory() as outdir:
    outdir = "/tmp/export_out"
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, "basic_test_file.xls")
    try:
        os.remove(path)
    except OSError:
        pass
    telecom_table.export(columns, "excel", {
        "file_path": path,
        "target": "example"
    })


@pytest.mark.skip(
    reason="this is slow and raises an exception. Remove skip to see export perf"
)
def test_export_perf(wide_string_column_table):
    # XXX this info is hardcoded
    num_columns = 1000
    column_names = [("column{}".format(ii), "column{}".format(ii))
                    for ii in range(num_columns)]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = "/tmp/export_perf_test"
        start = time.time()
        wide_string_column_table.export(column_names, "fast_csv", {
            "target": DefaultTargetName,
            "directory_path": temp_dir
        })
        end = time.time()
        total_size = du(temp_dir)
        print(
            f"export took {end - start}s for {total_size/10**6:.2f}MB ({total_size/(end-start)/10**6:.2f}MB/s)"
        )
        print(f"files in directory {temp_dir}")

    raise ValueError("test")


def test_multiple_parquet_telecom_prefixed(telecom_table, workbook):
    target_name = DefaultTargetName
    file_base = "test_multiple_parquet_pytest"
    columns = [("p::State", "State"), ("p::Phone Number", "Phone Number"),
               ("p::Total Day Charge", "Total Day Charge")]
    with tempfile.TemporaryDirectory() as outdir:
        telecom_table.export(
            columns, "snapshot_parquet", {
                "directory_path": outdir,
                "target": target_name,
                "file_base": file_base
            })
        assert len(os.listdir(outdir)) > 1

        parser_args = {"parquetParser": "native"}
        db = workbook.build_dataset(
            "test_parquet_export_dataset",
            target_name,
            outdir,
            "udf",
            parser_name="default:parseParquet",
            parser_args=parser_args,
            file_name_pattern="*.parquet")
        dataset = db.load()

        # convert to pandas
        telecom_pd = pd.DataFrame.from_dict(dataset.records())

        # check records, columns count
        assert len(columns) == telecom_pd.shape[1]
        assert telecom_table.record_count() == telecom_pd.shape[0]

        # check float column
        telecom_pd['Total Day Charge'] = telecom_pd['Total Day Charge'].astype(
            float)
        assert abs(telecom_pd['Total Day Charge'].sum() - 153248.34) < 0.0001

        # check other two columns
        source_data = {(rec["p::Phone Number"], rec["p::State"])
                       for rec in telecom_table.records()}
        export_data = {(rec["Phone Number"], rec["State"])
                       for rec in dataset.records()}
        assert source_data == export_data

        dataset.delete()


def test_multiple_parquet_telecom_derived(lineitem_table, workbook):
    target_name = DefaultTargetName
    file_base = "test_multiple_parquet_pytest"
    columns = [("L_EXTENDEDPRICE_money", "L_EXTENDEDPRICE"),
               ("L_TAX_float", "L_TAX"), ("L_PARTKEY_int", "L_PARTKEY"),
               ("L_SHIPDATE_timestamp", "L_SHIPDATE"),
               ("L_COMMENT_string", "L_COMMENT")]
    with tempfile.TemporaryDirectory() as outdir:
        lineitem_table.export(
            columns, "snapshot_parquet", {
                "directory_path": outdir,
                "target": target_name,
                "file_base": file_base
            })
        assert len(os.listdir(outdir)) > 1

        parser_args = {"parquetParser": "native"}
        db = workbook.build_dataset(
            "test_parquet_export_dataset_1",
            target_name,
            outdir,
            "udf",
            parser_name="default:parseParquet",
            parser_args=parser_args,
            file_name_pattern="*.parquet")
        dataset = db.load()

        # convert to pandas
        telecom_pd = pd.DataFrame.from_dict(dataset.records())

        # check records, columns count
        assert len(columns) == telecom_pd.shape[1]
        assert lineitem_table.record_count() == telecom_pd.shape[0]

        # Check L_EXTENDEDPRICE_money column: actual: 37593280.34
        assert telecom_pd['L_EXTENDEDPRICE'].apply(
            decimal.Decimal).sum() == decimal.Decimal('37593280.34')

        # Check L_TAX_float column: actual: 40.469999999999736
        telecom_pd['L_TAX'] = telecom_pd['L_TAX'].astype(float)
        assert abs(telecom_pd['L_TAX'].sum() - 40.47) < 0.0001

        # Check L_PARTKEY_int column: actual: 101703809
        telecom_pd['L_PARTKEY'] = telecom_pd['L_PARTKEY'].astype(int)
        assert telecom_pd['L_PARTKEY'].sum() == 101703809

        # Check datatime format of random record
        datetime_fmt = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z'
        random_date_field = telecom_pd.sample(n=1)['L_SHIPDATE'].iloc[0]
        assert re.search(datetime_fmt, random_date_field) is not None

        dataset.delete()


@pytest.mark.parametrize(
    "records, schema, dataset_name, records_count, column_count",
    [
    # test with FnF
        ([[1, 1, None, 1.4, True, None, "1970-01-01T00:00:00.006Z"],
          [2, None, "abc", None, False, "1.44", None],
          [
              3, 5, "aede", 1885.7339226780002, None, "100.45",
              "1990-01-01T00:00:00.006Z"
          ], [None, None, None, None, None, None, None]], [{
              "name": "idx",
              "type": "integer"
          }, {
              "name": "col1",
              "type": "integer"
          }, {
              "name": "col2",
              "type": "string"
          }, {
              "name": "col3",
              "type": "float"
          }, {
              "name": "col4",
              "type": "boolean"
          }, {
              "name": "col5",
              "type": "money"
          }, {
              "name": "col6",
              "type": "timestamp"
          }], "test_parquet_export_dataset_fnf", 4, 7),
    # test with inf/-inf/nan
        ([[1, 1.2], [2, 'inf'], [3, '-inf'], [4, 'nan'], [5, math.pi]], [{
            "name": "idx",
            "type": "integer"
        }, {
            "name": "fcol",
            "type": "float"
        }], "test_parquet_export_dataset_inf_nan", 5, 2),
    # test with None/empty string
        ([['1', 'xxx'], ['2', ''], ['3', None], ['4', 'XcEmpty'],
          ['5', '"XcEmpty"'], ['6', '\t""\n'], ['', '']], [{
              "name": "idx",
              "type": "string"
          }, {
              "name": "fcol",
              "type": "string"
          }], "test_parquet_export_dataset_none_empty", 7, 2)
    ])
def test_snapshot_parquet_with_data(client, workbook, records, schema,
                                    dataset_name, records_count, column_count):
    session = workbook.activate()
    target_name = DefaultTargetName
    file_base = "parquet_mixed"
    outdir = "0"
    parser_args = {"records": records, "schema": schema}
    db = workbook.build_dataset(
        dataset_name,
        "TableGen",
        outdir,
        "udf",
        parser_name="default:genTableUdf",
        parser_args=parser_args)
    dataset = db.load()

    df = Dataflow.create_dataflow_from_dataset(
        client, dataset, project_columns=schema)
    out_tab_name = "mixed_type_tab"
    session.execute_dataflow(df, table_name=out_tab_name, is_async=False)
    res_tab = session.get_table(out_tab_name)
    dataset.delete()

    # export the results and read in again to compare
    with tempfile.TemporaryDirectory() as outdir:
        res_tab.export(
            list(zip(res_tab.columns, res_tab.columns)), "snapshot_parquet", {
                "directory_path": outdir,
                "target": target_name,
                "file_base": file_base
            })

        assert len(os.listdir(outdir)) > 1

        parser_args = {"parquetParser": "native"}
        db = workbook.build_dataset(
            dataset_name,
            target_name,
            outdir,
            "udf",
            parser_name="default:parseParquet",
            parser_args=parser_args,
            file_name_pattern='*.parquet')
        dataset = db.load()
        df = Dataflow.create_dataflow_from_dataset(
            client, dataset, project_columns=schema)
        export_tab_name = "export_mixed_type_tab"
        session.execute_dataflow(
            df, table_name=export_tab_name, is_async=False)
        export_tab = session.get_table(export_tab_name)

    # compare data
    assert len(records) == export_tab.record_count() == records_count
    # Fill Non-exist column in table with None
    export_records = list(export_tab.records())
    columns = set([col['name'] for col in schema])
    for rec in export_records:
        for col in columns:
            if col not in rec:
                rec[col] = None
    export_records = sorted(
        export_records,
        key=lambda x: float('Inf') if not x['idx'] else float(x['idx']))

    for input_rec, export_dict in zip(records, export_records):
        assert len(input_rec) == len(export_dict) == column_count
        export_rec = [export_dict[col['name']] for col in schema]
        assert input_rec == export_rec

    res_tab.drop()
    export_tab.drop()
    dataset.delete()


@pytest.mark.parametrize(
    "records, schema, dataset_name, records_count, column_count",
    [
    # test with FnF
        ([[1, 1, None, 1.4, True, None, "1970-01-01T00:00:00.006Z"],
          [2, None, "abc", None, False, "1.44", None],
          [
              3, 5, "aede", 1885.7339226780002, None, "100.45",
              "1990-01-01T00:00:00.006Z"
          ], [None, None, None, None, None, None, None]], [{
              "name": "idx",
              "type": "integer"
          }, {
              "name": "col1",
              "type": "integer"
          }, {
              "name": "col2",
              "type": "string"
          }, {
              "name": "col3",
              "type": "float"
          }, {
              "name": "col4",
              "type": "boolean"
          }, {
              "name": "col5",
              "type": "money"
          }, {
              "name": "col6",
              "type": "timestamp"
          }], "test_table_rec_all_fnf", 4, 7),
    # test with inf/-inf/nan
        ([[1, 1.2], [2, 'inf'], [3, '-inf'], [4, 'nan'], [5, math.pi]], [{
            "name": "idx",
            "type": "integer"
        }, {
            "name": "fcol",
            "type": "float"
        }], "test_table_inf_nan", 5, 2),
    # test with None/empty string
        ([['1', ''], ['2', None], ['', '']], [{
            "name": "idx",
            "type": "string"
        }, {
            "name": "fcol",
            "type": "string"
        }], "test_table_empty_none_strings", 3, 2),
    # test with escape char in the field value
        ([['1', 'a\\bc'], ['2', 'ab"c'], ['3', None], ['4', '"abc"'],
          ['5', '\t""\n'], ['6', '"abc'], ['7', 'abc"'], ['8', 'ab"'],
          ['9', '"ab'], ['10', 'ab"c'], ['11', '"""'], ['12', '""'],
          ['13', '"\""'], ['14', '"\\""']], [{
              "name": "idx",
              "type": "string"
          }, {
              "name": "fcol",
              "type": "string"
          }], "test_table_escape_char_in_field", 14, 2),
    # test with table empty
        ([], [{
            "name": "idx",
            "type": "string"
        }, {
            "name": "fcol",
            "type": "string"
        }], "test_table_empty", 0, 2)
    ])
def test_snapshot_csv_round_trip(client, workbook, records, schema,
                                 dataset_name, records_count, column_count):
    schema_map = {
        "string": "DfString",
        "integer": "DfInt64",
        "boolean": "DfBoolean",
        "float": "DfFloat64",
        "timestamp": "DfTimespec",
        "money": "DfMoney"
    }
    session = workbook.activate()
    target_name = DefaultTargetName
    file_base = "snapshot_file"
    outdir = "0"
    parser_args = {"records": records, "schema": schema}
    db = workbook.build_dataset(
        dataset_name,
        "TableGen",
        outdir,
        "udf",
        parser_name="default:genTableUdf",
        parser_args=parser_args)
    dataset = db.load()

    # dataset won't have any columns information if it is empty
    df = Dataflow.create_dataflow_from_dataset(
        client, dataset, project_columns=schema if records_count > 0 else [])
    # add columns for empty table as part of map (hack)
    if records_count == 0:
        eval_list = [("{}(1)".format(col["type"]), col["name"])
                     for col in schema]
        df = df.map(eval_list)
    out_tab_name = "mixed_type_tab"
    session.execute_dataflow(df, table_name=out_tab_name, is_async=False)
    res_tab = session.get_table(out_tab_name)
    dataset.delete()
    res_tab.show()

    # export the results and read in again to compare
    with tempfile.TemporaryDirectory() as outdir:
        res_tab.export(
            list(zip(res_tab.columns, res_tab.columns)), "snapshot_csv", {
                "directory_path": outdir,
                "target": target_name,
                "file_base": file_base,
            })

        num_files = len(os.listdir(outdir))
        assert num_files > 1

        # test checksums are valid
        checksum_verified = 0
        for file_name in os.listdir(outdir):
            if file_name.endswith(".checksum"):
                continue
            file_checksum = None
            m = hashlib.md5()
            with gzip.GzipFile(os.path.join(outdir, file_name), 'rb') as fp:
                m.update(fp.read())
                file_checksum = m.hexdigest()
            assert file_checksum is not None
            base_name, ext = os.path.splitext(file_name)
            with open(os.path.join(outdir, base_name + ".checksum"),
                      'rb') as fp:
                assert file_checksum == fp.read().decode("utf-8")
            checksum_verified += 1
        assert checksum_verified == num_files / 2

        db = workbook.build_dataset(
            dataset_name,
            target_name,
            outdir,
            "csv",
            True,
            file_name_pattern='*.csv.gz')
        db.option("dialect", "xcalarSnapshot")
        load_schema = [
            XcalarApiColumnT(col_info["name"], col_info["name"],
                             schema_map[col_info["type"]])
            for col_info in schema
        ]
        db.option("schema", load_schema)
        dataset = db.load()

        dataset.show()
        df = Dataflow.create_dataflow_from_dataset(
            client,
            dataset,
            project_columns=schema if records_count > 0 else [])
        # add columns for empty table as part of map (hack)
        if records_count == 0:
            eval_list = [("{}(1)".format(col["type"]), col["name"])
                         for col in schema]
            df = df.map(eval_list)
        export_tab_name = "export_mixed_type_tab"
        session.execute_dataflow(
            df, table_name=export_tab_name, is_async=False)
        export_tab = session.get_table(export_tab_name)

    # compare data
    assert len(records) == export_tab.record_count() == records_count
    # Fill Non-exist column in table with None
    export_records = list(export_tab.records())
    columns = set([col['name'] for col in schema])
    for rec in export_records:
        for col in columns:
            if col not in rec:
                rec[col] = None
    export_records = sorted(
        export_records,
        key=lambda x: float('Inf') if not x['idx'] else float(x['idx']))

    for input_rec, export_dict in zip(records, export_records):
        assert len(input_rec) == len(export_dict) == column_count
        export_rec = [export_dict[col['name']] for col in schema]
        assert input_rec == export_rec

    res_tab.drop()
    export_tab.drop()
    dataset.delete()
