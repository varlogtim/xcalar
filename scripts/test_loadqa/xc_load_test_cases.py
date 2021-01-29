# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import pytest
import logging
import json
from os import path

# Test cases here are as per QA test plan
# https://docs.google.com/spreadsheets/d/1MDB6s4yzbfHAhg7tf2PszotNXZbHdj2X11Llh1Hja5g/edit?usp=sharing

from auto_test_cases import testlist as auto_tests
from manual_test_cases import testlist as manual_tests
from xc_load_types import PERF, TEST_TYPE, PERF_DIR, PERF_FILE, NAME, TESTS
from xc_load_types import DATASET, INPUT_SERIALIZATION, MARK, OPER
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

tests = []


def get_perf_file():
    perf_path = get_perf_path()
    with open(perf_path, "r") as pf:
        data = pf.read()
        logger.info(f"perf_path = {perf_path}, DATA_LEN = {len(data)}")
        return json.loads(data)


def qualify_test(testcase):
    return testcase[TEST_TYPE] == PERF and (DATASET in testcase) and (
        testcase[DATASET] is
        not None) and ("100col" in testcase[DATASET]) and (
            INPUT_SERIALIZATION in testcase) and (
                testcase[INPUT_SERIALIZATION] is not None) and (
                    not "CompressionType" in testcase[INPUT_SERIALIZATION])


def generate_tests(runperf):
    global tests

    if len(tests) > 0:
        return tests

    perf_data = get_perf_file()

    for testcase in auto_tests + manual_tests:
        if runperf:
            if testcase[NAME] in perf_data[TESTS]:
                tests.append(pytest.param(testcase, marks=pytest.mark.skip))
            elif qualify_test(testcase):
                tests.append(pytest.param(testcase))
            else:
                tests.append(pytest.param(testcase, marks=pytest.mark.skip))
        elif testcase[TEST_TYPE] == OPER:
            tests.append(pytest.param(testcase, marks=pytest.mark.skip))
        else:
            if MARK in testcase:
                tests.append(pytest.param(testcase, marks=testcase[MARK]))
            else:
                tests.append(pytest.param(testcase))

    return tests


def get_test_case_by_name(name):
    for testcase in auto_tests + manual_tests:
        if testcase["name"] == name:
            return testcase
    return None
