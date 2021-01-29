"""
This file contains fixtures for common use by all tests.  If you don't know
what a fixture is, see https://docs.pytest.org/en/latest/fixture.html
"""
import pytest
import shutil
import os

from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.util.config import detect_config


# The scope for this is 'session' to allow the cluster to be reused across all
# tests. 'session' in this context means 'run of pytest'. For any tests which
# require a cluster, we want them to be reused so we aren't unnecessarily
# starting and stopping the cluster.
@pytest.fixture(scope="session", autouse=True)
def cluster():
    with DevCluster() as cluster:
        yield cluster


# The client class doesn't have a ton of state, so it's not particularly
# important what the scope for this is. We choose "session" as a fine option,
# meaning this SDK Client object will get reused for all tests.
@pytest.fixture(scope="module")
def client(cluster):
    return cluster.client()


# This fixture is scoped per module so that the workbook doesn't get too big if
# the test doesn't explicitly clean up after itself.
@pytest.fixture(scope="module")
def workbook(client, request):
    # Let's name the workbook after the test file like 'test_csv'
    module_name = request.module.__name__
    workbook_name = module_name or "QA workbook pytest"
    workbook = client.create_workbook(workbook_name)
    yield workbook
    workbook.delete()


def config_backup_helper():
    # take backup of default config file
    config_path = detect_config().config_file_path
    config_bkp = config_path + ".PytestBackup"
    shutil.copyfile(config_path, config_bkp)
    yield
    # revert to prior configs
    shutil.copyfile(config_bkp, config_path)
    if os.path.exists(config_bkp):
        os.remove(config_bkp)


# We want to make sure that configs aren't changed beyond the
# scope of the individual pytest module.
# Modules need to explicitly specify when they want to use this.
@pytest.fixture(scope="module")
def config_backup():
    yield from config_backup_helper()


# Hacky way to achive the same as above, but with a
# function level scope. Pytest doesn't make it easy to
# dynamic scope fixtures from within a test run.
@pytest.fixture(scope="function")
def per_function_config_backup():
    yield from config_backup_helper()
