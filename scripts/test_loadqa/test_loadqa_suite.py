# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
"""Running Tests

 Running all tests using pytest
 ------------------------------

 This will run all tests. Note that we have intentionally xfail(ed)
 some tests as behavior is not ready or specified for SAAS v1.0.
 These will be revisited later.

 cd $XLRDIR/scripts/test_loadqa
 python -m pytest -s -v test_loadqa_suite.py::test_suite

 To run slow tests:

 python -m pytest -s -v --runslow test_loadqa_suite.py::test_suite

 Tests are listed in xc_load_test_cases.py. The actual test names are
 in test_cases/load/testcaseXX.py. Look at the key "name" in testcaseXX.py
 to know the name of the test. In the long run this will be fixed to make
 the testcase name consistent with the testcaseXX.py.

 Running Daily and Release Tests
 -------------------------------

 To run daily and release tests use the --runslow flag.

 cd $XLRDIR/scripts/test_loadqa
 python -m pytest -s -v --runslow test_loadqa_suite.py::test_suite


 Running Tests in Production Cloud
 ---------------------------------

 ssh ec2-54-191-242-142.us-west-2.compute.amazonaws.com
 sudo -s
 su - xcalar
 cd /opt/xcalar/scripts/test_loadqa

 export BUCKET="saas-test-4fucwxjl0c-s3bucket-opv9axy0aqza"

 # You will need to copy some data from our standard bucket, eg
 # Source data
 s3://xcfield/saas-load-test/Error_Handling_Test
 s3://xcfield/saas-load-test/Accuracy_Test
 # Destination
 s3://${BUCKET}/saas-load-test/Error_Handling_Test
 s3://${BUCKET}/saas-load-test/Accuracy_Test

 # install atleast two python packages (more might be required)
 /opt/xcalar/bin/python3.6 -m pip install pytest
 /opt/xcalar/bin/python3.6 -m pip install multiset

 # You could alternatively install all python packages
 # You will need to upload dev $XLRDIR/requirements.txt
 /opt/xcalar/bin/python3.6 -m pip install -r requirements.txt --user

 # For specific test:
 /opt/xcalar/bin/python3.6 -m pytest -s -v --loadtestcase test_31 --bucket ${BUCKET} test_loadqa_suite.py -k specific

 # For running all tests:
 /opt/xcalar/bin/python3.6 -m pytest -s -v --bucket ${BUCKET} test_loadqa_suite.py::test_suite


 Using PyTest.sh
 ---------------

 Follow the regular process including cmBuid qa. After that you can execute:

 ./src/bin/tests/pyTestNew/PyTest.sh -s -k test_loadqa

 ./src/bin/tests/pyTestNew/PyTest.sh -s -vv -k test_rundataflow


 Running a specific test
 -----------------------

 For eg: to run test "test_32":

 cd $XLRDIR/scripts/test_loadqa
 python -m pytest -s -v --loadtestcase test_32 test_loadqa_suite.py -k specific

 To run slow tests:

 python -m pytest -s -v --loadtestcase test_32 --runslow test_loadqa_suite.py -k specific


 Concurrency
 -----------

 To run tests concurrently, use the -n parameter. Set -n 8 of 8 CPUs.

 cd $XLRDIR/scripts/test_loadqa
 python -m pytest -s -v --runslow -n 2 test_loadqa_suite.py::test_suite

 You will need to install the package:

 pip install pytest-xdist


 Running All Perf Tests Only
 ---------------------------

 cd $XLRDIR/scripts/test_loadqa
 python -m pytest -s -v --runperf test_loadqa_suite.py::test_suite

 Test Case Fields

 name: Name of a test case as per the QA test plan
 sample_file: Path of the s3 sample file for schema detection
 dataset: Actual dataset dir or file that needs to be loaded
 input_serialization: S3Select specific input params for parsing S3 file
 num_rows: Number of rows to be sampled from sample_file
 file_name_pattern:  Glob that finds the files
 test_type: Can be "schema", "global_status", "custom" or full "load"
 recursive: Do we load files from folder recursively
 global_status: test_type is "global_status". First crucial test.
 empty: test_type is "empty". Second crucial test.
 schema: test_type is "schema", if no global_status error, test schema.
 custom: test_type is "custom", user execs supports only "resp" from preview.
 perf: test_type is "perf", used by LoadMart.
 load: The expected rows in LOAD table, in case test_type is "load"
 data: The expected rows in DATA table, in case test_type is "load"
 comp: The expected rows in ICV or Complements table, in case test_type is load

"""

import json
import uuid
import traceback
import os
import sys
import importlib
import logging
import hashlib
import boto3
import pytest
import copy
import time
import multiprocessing
import socket

from os import path
from subprocess import run as prun, PIPE
from psutil import virtual_memory
import contextlib

from xcalar.compute.util.cluster import DevCluster
from xcalar.external.client import Client
from xcalar.compute.util.Qa import datasetCheck
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from do_xcalar_load import do_load
from do_xcalar_load import preview_source
from do_xcalar_load import execute_schema_load_app
from do_xcalar_load import SESSION_NAME
from do_xcalar_load import LOAD
from do_xcalar_load import DATA
from do_xcalar_load import COMP
from do_xcalar_load import PRIV_TARGET_NAME
from gen_integrity_data import get_val
from gen_integrity_data import number_to_words
from xc_load_types import MARK, OPER
from xc_load_types import PERF, TEST_TYPE, PERF_DIR, PERF_FILE, NAME, TESTS
from xc_load_types import DATASET, INPUT_SERIALIZATION
from xc_load_types import get_perf_path

# Set up logging
logger = logging.getLogger('Load QA Suite')
logger.setLevel(logging.INFO)
if not logger.handlers:
    errorHandler = logging.StreamHandler(sys.stderr)
    formatterString = ('LOAD_QA: %(message)s')
    formatter = logging.Formatter(formatterString)
    errorHandler.setFormatter(formatter)
    logger.addHandler(errorHandler)

USER_NAME = "admin"
INPUT_SERIALIZATION = "input_serialization"
NAME = "name"
BUCKET = "bucket"
SAMPLE_FILE = "sample_file"
DATASET = "dataset"
NUM_ROWS = "num_rows"
FILE_NAME_PATTERN = "file_name_pattern"
RECURSIVE = "recursive"
COMMENT = "comment"
LOAD_TEST_CASE = "loadtestcase"
EXEC = "exec"
MULTI = "multi"
SCHEMA = "schema"
CUSTOM = "custom"
INTEGRITY = "integrity"
DTYPE = "dtype"
GLOBAL_STATUS = "global_status"
GLOBAL = "global"
EMPTY = "empty"
STATUSES = "statuses"
PERF = "perf"
SCHEMAS = "schemas"
MAX_COLUMNS = 1000
ROWS = 'rows'
COLUMNS = 'columns'
FILE_PREFIX = "file_prefix"
PATH_PREFIX = "path_prefix"
COUNT = "count"
SEED = "seed"
RESULT = "result"
LOAD_ROWS = "load_rows"
DATA_ROWS = "data_rows"
COMP_ROWS = "comp_rows"


@pytest.fixture(scope="module")
def setup():
    """Fixture for Xcalar client and session

    Yields
    ------

    client (Client): the Xcalar Client (setup[0])
    session (Session): the Xcalar Session (setup[1])

    """

    client = Client(bypass_proxy=True, user_name=USER_NAME)
    session = client.create_session(SESSION_NAME)
    yield (client, session)
    client.destroy_session(SESSION_NAME)


def get_rows(table):
    """Get rows from a table.
    We should probably use a generator here.

    Parameters:
    table (XcalarTable): Xcalar table

    Returns:
    <class 'List'>: List of rows

    """

    return [rec for rec in table.records()]


def sanitize(test_case):
    """All required fields need to be present in the testcase.

    If the required fields are not present in the testcase
    the testcase will fail.

    Parameters:
    test_case (str): The "name" field in the testcaseXX.py

    """

    assert NAME in test_case
    assert BUCKET in test_case, test_case[NAME]
    assert DATASET in test_case, test_case[NAME]
    assert INPUT_SERIALIZATION in test_case, test_case[NAME]
    assert SAMPLE_FILE in test_case, test_case[NAME]
    assert NUM_ROWS in test_case, test_case[NAME]
    assert FILE_NAME_PATTERN in test_case, test_case[NAME]
    assert RECURSIVE in test_case, test_case[NAME]
    assert TEST_TYPE in test_case, test_case[NAME]
    test_type = test_case[TEST_TYPE], test_case[NAME]
    if test_type == SCHEMA:
        assert SCHEMA in test_case, test_case[NAME]
    elif test_type == LOAD:
        assert LOAD in test_case, test_case[NAME]
        assert DATA in test_case, test_case[NAME]
        assert COMP in test_case, test_case[NAME]
        if INTEGRITY in test_case:
            assert DTYPE in test_case
    elif test_type == GLOBAL_STATUS:
        assert GLOBAL_STATUS in test_case, test_case[NAME]
    elif test_type == STATUSES:
        assert STATUSES in test_case, test_case[NAME]
    elif test_type == MULTI:
        assert FILE_PREFIX in test_case, test_case[NAME]
        assert PATH_PREFIX in test_case, test_case[NAME]
        assert SEED in test_case, test_case[NAME]
        assert COUNT in test_case, test_case[NAME]


def get_load_id(client, session):
    app_in = {
        "func": "get_load_id",
        "session_name": session.name,
    }
    app_in_json = json.dumps(app_in)
    app_out_raw = execute_schema_load_app(client, app_in_json)
    load_id = json.loads(app_out_raw.json)
    return load_id


def strip_cols(rows):
    for row in rows:
        if "XcalarRankOver" in row:
            del row["XcalarRankOver"]
        if 'XCALAR_PATH' in row:
            del row["XCALAR_PATH"]
    return rows


def integrity_check(observed, dtype):
    observed = strip_cols(observed)
    for row in observed:
        fname = row["ONE"]
        row_ind = int(row["ZERO"])
        num_cols = len(row.keys())
        for col_ind in range(2, num_cols):
            obs_val = row[number_to_words(col_ind).upper()]
            exp_val = get_val[dtype](fname, row_ind, col_ind)
            assert obs_val == exp_val, f"Value mismatch at [{fname}, {row_ind}, {col_ind}]"


def get_assert_case(test_case):
    assert_case = copy.deepcopy(test_case)
    if MARK in assert_case:
        del assert_case[MARK]
    if LOAD in assert_case:
        del assert_case[LOAD]
    if DATA in assert_case:
        del assert_case[DATA]
    if COMP in assert_case:
        del assert_case[COMP]
    if SCHEMA in assert_case:
        del assert_case[SCHEMA]
    if DTYPE in assert_case:
        del assert_case[DTYPE]

    return assert_case


def run_test_case(target_name, client, session, test_case):
    """Run the actual test case.

    Suitable for mostly cookie cutter tests which do a schema
    preview, dataflow generation and load, and finally compare
    expected with observed results. The expected results are
    stored in the testcaseXX.py file in a subdirectory
    test_cases/. Care must be taken to ensure that the tables
    are not too large! This test does a brute force comparison
    of final data on dict objects which can be very slow!

    In case of test_type == "load", we attempt to create "load",
    "data", and "comp" tables.

    There are two dataflow templates encoded as array of JSONs.

    1. Load Dataflow
    2. Data + Comp Dataflow  (One template runs twice once for
    Data and once for Comp with appropriate variable substitutions).
    The template is called final_table_df in schema_discover_dataflows.py.

    Load Dataflow Execution
    This consists of three operations. We run in optimized to
    create a load table instead of a dataset. Following in that order:

    1. XcalarApiBulkLoad
    2. XcalarApiSynthesize
    3. XcalarApiExport

    Data and Comp Dataflow Executions

    It takes a load table generated above and generates a Data table,
    which has rows without ICVs. Following operations in that order.

    1. XcalarApiSynthesize: Get rid of fat ptrs
    2. XcalarApiFilter: Checks if row is ICV
    3. XcalarApiGetRowNum: Creates XcalarRankOver (may not be required in future)
    4. XcalarApiProject: Remove/Keeps ICV specific columns depending upon Data/Comp
    5. XcalarApiIndex: Key is XcalarRankOver (DfInt64)
    6. XcalarApiExport: Export to data table

    Xcalar Load uses two dataflow templates as mentioned above to
    generate the tables. The user supplies things like source args
    (parse args, load args), input serialization etc. And the templates
    are used to generate the final dataflows dynamically. Even a UDF is
    uploaded as part of the load dataflow which contains the files and
    plan. So the part specific to the tests are the input parameters.

    Parameters:
    target_name (str): Name of the connector target
    client (Client): Xcalar client
    session (Session): Xcalar session
    test_case (<class 'dict'>): Actual dict that stores testcase.

    """

    bucket = test_case[BUCKET]
    sample_file = f"/{bucket}{test_case[SAMPLE_FILE]}"
    dataset = f"/{bucket}{test_case[DATASET]}"

    assert_case = get_assert_case(test_case)

    comment = ""
    if COMMENT in test_case:
        comment = test_case[COMMENT]

    startup_msg = f"\n\ntest_case: {test_case[NAME]}\nsample_file: {sample_file}\ndataset: {dataset}\n{comment}"

    assert_msg = f"{startup_msg}, test details: {json.dumps(assert_case, indent=2)}"
    logger.info(f"\nRunning {startup_msg}")

    input_serial_json = json.dumps(test_case[INPUT_SERIALIZATION])
    num_rows = test_case[NUM_ROWS]
    file_name_pattern = test_case[FILE_NAME_PATTERN]
    recursive = test_case[RECURSIVE]
    test_type = test_case[TEST_TYPE]
    if GLOBAL_STATUS in test_case:
        global_status = test_case[GLOBAL_STATUS]
    elif STATUSES in test_case:
        statuses = test_case[STATUSES]

    # This load id is used to link preview and load requests.
    load_id = get_load_id(client, session)

    response_msg = ""
    if test_type != OPER:
        resp = preview_source(client, target_name, sample_file,
                              input_serial_json, load_id, num_rows)

        response_msg = f"\n\n\n\nResponse from preview_source = {json.dumps(resp, indent=2)}"

        assert resp is not None, assert_msg
        # this needs to be fixed, the user may choose second schema

        if test_type == GLOBAL_STATUS:
            assert global_status == resp[
                GLOBAL_STATUS], assert_msg + response_msg
            return
        elif test_type == EMPTY:
            assert 0 == len(resp[ROWS]), assert_msg + response_msg
            return
        elif test_type == STATUSES:
            assert statuses == resp[STATUSES], assert_msg + response_msg
            return

        assert SCHEMAS in resp, assert_msg + response_msg
        assert len(resp[SCHEMAS]) > 0, assert_msg + response_msg
        first_schema = resp[SCHEMAS][0]
    else:
        assert SCHEMA in test_case, assert_msg
        first_schema = test_case[SCHEMA]

    if test_type == SCHEMA:
        assert first_schema == test_case[SCHEMA], assert_msg
        return
    # the CUSTOM evals are confined to variable "resp".
    elif test_type == CUSTOM:
        assert EXEC in test_case, assert_msg
        try:
            exec(test_case[EXEC])
        except AssertionError:
            logger.error(f"{test_case[EXEC]}")
            raise
        return
    elif test_type in [LOAD, MULTI, PERF]:
        assert len(resp[ROWS]) > 0, assert_msg

    # Specify sourceArgs and schema to get dataflows
    table_name = f"TEMP_{uuid.uuid4()}"
    data_table_name = table_name
    load_table_name = f'LOAD_{table_name}'
    comp_table_name = f'COMP_{table_name}'

    source_args = [{
        "targetName": target_name,
        "path": dataset,
        "fileNamePattern": file_name_pattern,
        "recursive": recursive
    }]
    schema_json = json.dumps(first_schema)
    source_args_json = json.dumps(source_args)

    startTime = time.time()
    try:
        tables, delay = do_load(client, session, source_args_json,
                                input_serial_json, schema_json, load_id)
    except XcalarApiStatusException as xae:
        assert False, "\nCHECK DATASET PATH ON s3!!*******" + assert_msg + response_msg

    logger.info(
        f"\nLoad Time {test_case[NAME]}, dataset {dataset}: {time.time()-startTime}"
    )

    if test_type not in [PERF, MULTI, OPER]:
        # Load Tables
        load_result = {}
        load_result[LOAD] = get_rows(tables[LOAD])
        load_result[DATA] = get_rows(tables[DATA])
        load_result[COMP] = get_rows(tables[COMP])

        # TODO: implement data integrity later
        if INTEGRITY in test_case:
            integrity_check(load_result[DATA], test_case[DTYPE])
        else:
            datasetCheck(
                strip_cols(test_case[LOAD]), strip_cols(load_result[LOAD]))
            datasetCheck(
                strip_cols(test_case[DATA]), strip_cols(load_result[DATA]))
            datasetCheck(
                strip_cols(test_case[COMP]), strip_cols(load_result[COMP]))

    load_rows = tables[LOAD].get_meta(True).total_records_count
    data_rows = tables[DATA].get_meta(True).total_records_count
    comp_rows = tables[COMP].get_meta(True).total_records_count

    tables[LOAD].drop(delete_completely=True)
    tables[DATA].drop(delete_completely=True)
    tables[COMP].drop(delete_completely=True)

    logger.info(
        f"load_rows = {load_rows}, data_rows = {data_rows}, comp_rows = {comp_rows}"
    )

    if RESULT in test_case:
        assert load_rows == test_case[RESULT][
            LOAD_ROWS], assert_msg + response_msg
        assert data_rows == test_case[RESULT][
            DATA_ROWS], assert_msg + response_msg
        assert comp_rows == test_case[RESULT][
            COMP_ROWS], assert_msg + response_msg

    update_perf_stats(client, test_case, delay)

    return load_rows, data_rows, comp_rows


def update_perf_stats(client, test_case, delay):

    perf_path = get_perf_path()

    with open(perf_path, "r+") as po:
        data = po.read()
        jj = json.loads(data)

        if GLOBAL not in jj:
            jj[GLOBAL] = {}
            jj[GLOBAL]["xcalar_version"] = client.get_version().version
            jj[GLOBAL]["cpu_count"] = multiprocessing.cpu_count()
            jj[GLOBAL]["total_memory"] = virtual_memory().total

        jj[TESTS][test_case[NAME]] = delay
        po.seek(0)
        po.write(json.dumps(jj, indent=2) + "\n")
        po.truncate()


def get_tests(name, bucket, loadtestcase, runperf):
    """Generator function to get all tests
    These are returned as a generator of test case dicts.

    Parameters:
    name (str): Name of fixture

    Returns:
    <class 'generator'>: Generator for <class 'dict'>

    """
    # Load module which contains test data
    tests_module = importlib.import_module(name)
    # Tests are to be found in the variable 'tests' of the module
    for test in tests_module.generate_tests(runperf):
        test[0][0][BUCKET] = bucket
        test[0][0][LOAD_TEST_CASE] = loadtestcase
    return [test for test in tests_module.generate_tests(runperf)]


def get_test_ids(name, runperf):
    """Function to get actual test ids.
    The test_cases being dynamic, the default ids printed by pytest
    which are an incremented index are  meaningless. What we want to
    print is the actual test_case[NAME]. In static test_case scenario
    the test_case is a pytest function name which can be meaningful.
    In case of dynamically generated test_case there is only one
    pytest function which gets invoked with pytest.mark.parametrize
    dynamically and the parameter passed contains the actual test_case
    name which needs to be printed. This is what we call the id.

    Parameters:
    name (str): Name of fixture

    Returns:
    <class 'List'>: List of ids.

    """

    # Load module which contains test data
    tests_module = importlib.import_module(name)

    # Tests are to be found in the variable 'tests' of the module
    return [
        test.values[0][NAME] if NAME in test.values[0] else "Unknown"
        for test in tests_module.generate_tests(runperf)
    ]


def pytest_generate_tests(metafunc):
    """Pytest Hook for parameterizing tests dynamically.
    Refer: https://docs.pytest.org/en/stable/parametrize.html#pytest-generate-tests
    Allows to load tests from external files. This makes
    it easy to add new test cases. This file does not have
    to be modified. We just add a test case in directory
    test_cases/. We figure out the actual ids for the test_case
    as the default test_case id is just the number signifying
    the order in which the test_cases are organized. It's real
    name is hidden as test_case[NAME] which would not get printed.
    In case of PyTest.sh, we do not need to skip the test_specific
    as we have one default specific loadtestcase set to test_32.
    This allows users to execute specific tests outside PyTest.sh
    without having to uncomment @pytest.mark.skip.

    Parameters:
    metafunc (Metafunc): Object used to request test context

    """

    bucket = metafunc.config.option.bucket
    logger.info(f"config bucket = {bucket}")
    loadtestcase = metafunc.config.option.loadtestcase
    rundataflow = metafunc.config.option.rundataflow
    runperf = metafunc.config.option.runperf

    for fixture in metafunc.fixturenames:
        if fixture.startswith('xc_load_test_cases'):
            # Load associated test data
            tests = get_tests(fixture, bucket, loadtestcase, runperf)
            metafunc.parametrize(
                fixture, tests, ids=get_test_ids(fixture, runperf))


def get_ext(test_case):
    keys = [key.lower() for key in test_case[INPUT_SERIALIZATION]]
    exts = ["csv", "json", "parquet"]
    for ext in exts:
        if ext in keys:
            return ext
    assert_msg = f"test_case: {test_case[NAME]}, dataset: {test_case[DATASET]}"
    assert False, assert_msg


def delete_folder(mybucket, exportpath):
    s3 = boto3.resource('s3')
    logger.info(f"bucket = {mybucket}, exportpath = {exportpath}")
    objects_to_delete = s3.meta.client.list_objects(
        Bucket=mybucket, Prefix=exportpath)
    delete_keys = {'Objects': []}
    delete_keys['Objects'] = [{
        'Key': k
    } for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
    assert len(delete_keys['Objects']) < 50, "Risk of deleting too many keys!"

    logger.info(f"delete_keys = {delete_keys['Objects']}")

    if len(delete_keys['Objects']) > 0:
        s3.meta.client.delete_objects(Bucket=mybucket, Delete=delete_keys)


@pytest.fixture(scope='session')
def rundataflow(request):
    return request.config.option.rundataflow


loadparams = [
    {
        "loadapp": "scripts/test_loadqa/loadapps/basic/LoadApp.xlrapp.tar.gz",
        "queryname": f"loadapp",
        "dataflow": "LoadQA",
        "bucket": "xcfield",
        "test_case_name": "oper_550",
        "params": {
            "sourcepath": "/saas-load-test/Operationalization_Test/json/",
            "pattern": "*",
            "exportpath": "export/loadqa/",
            "exportprefix": "tiny"
        }
    },
    {
        "loadapp":
            "scripts/test_loadqa/loadapps/basic/LoadQACSV.xlrapp.tar.gz",
        "queryname":
            f"loadapp",
        "dataflow":
            "LoadQACSV",
        "bucket":
            "xcfield",
        "test_case_name":
            "oper_551",
        "params": {
            "sourcepath": "/saas-load-test/Operationalization_Test/csv/",
            "pattern": "*",
            "exportpath": "export/loadqa/",
            "exportprefix": "tiny"
        }
    }
]


@pytest.mark.skip(f"ENG-10525 All tests invalid")
@pytest.mark.parametrize("loadparams,expected_rows", [(loadparams[0], 5000),
                                                      (loadparams[1], 312000)])
def test_rundataflow(setup, loadparams, expected_rows, rundataflow):

    ppath = f"{os.path.dirname(os.path.realpath(__file__))}/{rundataflow}"

    target = PRIV_TARGET_NAME
    client = setup[0]
    session = setup[1]

    loadapp = loadparams["loadapp"]
    queryname = loadparams["queryname"]
    dataflow = loadparams["dataflow"]
    params = loadparams["params"]
    bucket = loadparams["bucket"]
    test_case_name = loadparams["test_case_name"]
    logger.info(f"bucket = {bucket}")
    workbook = os.path.basename(loadapp).split('.')[0]

    sourcepath = f"sourcepath=/{bucket}{params['sourcepath']}"
    pattern = f"pattern={params['pattern']}"
    expuniq = str(uuid.uuid1())[:4]
    exportpath = f"exportpath=/{bucket}/{params['exportpath']}{expuniq}"
    exportprefix = f"exportprefix={params['exportprefix']}"

    logger.info(f"sourcepath = {sourcepath}")

    pparams = ','.join([sourcepath, pattern, exportpath, exportprefix])

    result = prun([ppath, loadapp, queryname, dataflow, pparams, workbook],
                  stdout=PIPE,
                  stderr=PIPE,
                  universal_newlines=True)
    logger.info(f"status = {result.stdout}")

    try:
        assert result.returncode == 0, result.stderr

        tests_module = importlib.import_module('xc_load_test_cases')
        test_case = tests_module.get_test_case_by_name(test_case_name)
        test_case[
            SAMPLE_FILE] = f"/{params['exportpath']}{expuniq}/{exportprefix}0.csv"
        test_case[DATASET] = f"/{params['exportpath']}{expuniq}"
        logger.info(f"TEST_CASE = {test_case}")
        load_rows, data_rows, comp_rows = run_test_case(
            target, client, session, test_case)

        assert expected_rows == load_rows, test_case[NAME]
        assert load_rows == (data_rows + comp_rows), test_case[NAME]
    except AssertionError as ae:
        raise ae
    finally:
        logger.info(f"Deleting exportpath = {exportpath}")
        delete_folder(bucket, params['exportpath'])


@pytest.mark.skip(f"ENG-10525 All tests invalid")
def test_specific(setup, xc_load_test_cases):
    """Call a specific test by name
    You can invoke a specific test by it's name, eg. "test-02".
    The names are stored as a "name" field inside the <class 'dict'>
    yielded by the get_tests() function. This function helps us
    avoid writing one parameterizable pytest per testcase. As we
    just need a single test. And the amount of metadata for the
    testcase can be cleanly stored in a separate testcase python
    file, so it does not need further parsing.

    Parameters:
    setup (Fixture): Initialize/destroy xcalar client and session
    xc_load_test_cases (<class 'dict'>): A single test case.
    loadtestcase (str): Command line getopt like option.

    """

    target_name = PRIV_TARGET_NAME
    client = setup[0]
    session = setup[1]
    test_case = xc_load_test_cases
    loadtestcase = test_case[LOAD_TEST_CASE]

    sanitize(test_case)

    if loadtestcase == test_case[NAME]:
        assert TEST_TYPE in test_case
        test_type = test_case[TEST_TYPE]
        if test_type == MULTI:
            handle_multi(client, session, test_case)
        else:
            run_test_case(target_name, client, session, test_case)
    else:
        pytest.skip()


def handle_multi(client, session, test_case):
    target_name = PRIV_TARGET_NAME
    file_prefix = test_case[FILE_PREFIX]
    path_prefix = test_case[PATH_PREFIX]
    seed = test_case[SEED]
    count = test_case[COUNT]
    dataset = test_case[DATASET]
    ext = get_ext(test_case)
    for ds_index in range(count):
        test_case[DATASET] = f"{path_prefix}{ds_index}/"
        test_case[
            SAMPLE_FILE] = f"{path_prefix}{ds_index}/{file_prefix}{seed}.{ext}"
        run_test_case(target_name, client, session, test_case)


@pytest.mark.skip(f"ENG-10525 All tests invalid")
def test_suite(setup, xc_load_test_cases):
    """Central test suite to fire all tests.

    Parameters:
    setup (Fixture): Initialize/destroy xcalar client and session
    xc_load_test_cases (<class 'dict'>): A single test case.

    """
    target_name = PRIV_TARGET_NAME
    client = setup[0]
    session = setup[1]
    test_case = xc_load_test_cases

    sanitize(test_case)

    assert TEST_TYPE in test_case
    test_type = test_case[TEST_TYPE]

    if test_type == MULTI:
        handle_multi(client, session, test_case)
    else:
        run_test_case(target_name, client, session, test_case)


@pytest.mark.skip(f"ENG-10525 All tests invalid")
def test_list(xc_load_test_cases):
    """Central test suite to fire all tests.

    Parameters:
    setup (Fixture): Initialize/destroy xcalar client and session
    xc_load_test_cases (<class 'dict'>): A single test case.

    """

    sanitize(xc_load_test_cases)
    logger.info(
        f"{xc_load_test_cases['name']} : {xc_load_test_cases['sample_file']}")
