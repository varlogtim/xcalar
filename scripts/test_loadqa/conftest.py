# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import pytest

# Required name-bound config file for pytest


def pytest_addoption(parser):
    parser.addoption("--loadtestcase", default="test_32", action="store")
    parser.addoption("--rundataflow", default="execapp.sh", action="store")
    parser.addoption("--runperf", default=False, action="store_true", help="run perf tests")
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--runprecheckin", action="store_true", default=False, help="run precheckin tests")
    parser.addoption("--runrelease", action="store_true", default=False, help="run release tests")
    parser.addoption("--bucket", action="store", default="xcfield", help="bucket")


@pytest.fixture(scope='session')
def loadtestcase(request):
    name_value = request.config.option.loadtestcase
    if name_value is None:
        pytest.skip()
    return name_value


@pytest.fixture(scope='session')
def rundataflow(request):
    name_value = request.config.option.rundataflow
    if name_value is None:
        pytest.skip()
    return name_value


@pytest.fixture(scope='session')
def runperf(request):
    return request.config.option.runperf


@pytest.fixture(scope='session')
def bucket(request):
    return request.config.option.bucket


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "precheckin: mark test as prechecking to run")
    config.addinivalue_line("markers", "release: mark test as release to run")


def pytest_collection_modifyitems(config, items):
    skip_test = pytest.mark.skip()
    if not config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_test)
