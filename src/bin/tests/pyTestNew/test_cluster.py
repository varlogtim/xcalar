import tempfile
import time
import random
import json
import pytest

import xcalar.compute.util.config
import xcalar.compute.util.cluster
from xcalar.compute.util.cluster import detect_cluster

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Udf import Udf
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.Target2 import Target2
from xcalar.external.LegacyApi.Dataset import Dataset

from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT

TestClusterSessionName = "TestClusterSession"
TestClusterUserName = "TestClusterUser"

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own clusters")


@pytest.mark.slow(
    reason="test_basic covers the basic sanity tests for cluster start/stop")
def test_startstop():
    num_cycles = 3
    for i in range(num_cycles):
        cluster = xcalar.compute.util.cluster.DevCluster().start()
        assert detect_cluster().is_up()
        time.sleep(random.randrange(1, 100) / 30.0)
        detect_cluster().stop()
        assert detect_cluster().is_fully_down()
        time.sleep(random.randrange(1, 100) / 30.0)


def test_basic():
    cluster = xcalar.compute.util.cluster.DevCluster()
    cluster.stop()

    assert not cluster.is_up()
    assert cluster.is_fully_down()
    assert detect_cluster().is_fully_down()

    for p_name, proc in cluster.status().items():
        assert p_name == proc["name"]
        assert proc["status"] == xcalar.compute.util.cluster.ProcessState.DOWN
        assert proc["pid"] == ""
        assert proc["uptime"] == ""

    statuses = [
        xcalar.compute.util.cluster.ProcessState.DOWN,
        xcalar.compute.util.cluster.ProcessState.STARTED,
        xcalar.compute.util.cluster.ProcessState.UP,
    ]

    cluster.start()
    assert cluster.is_up()
    assert not cluster.is_fully_down()
    assert detect_cluster().is_up()
    for p_name, proc in cluster.status().items():
        assert p_name == proc["name"]
        assert proc["status"] in statuses
        if proc["status"] == xcalar.compute.util.cluster.ProcessState.UP:
            assert proc["pid"] > 0
            assert proc["uptime"] != ""    # should be stringified timedelta
    cluster.stop()


def test_config():
    detect_cluster().stop()
    # copy config file, detect_config should get it
    with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as cfg_file:
        old_config = xcalar.compute.util.config.detect_config()
        with open(old_config.config_file_path) as old_cfg_file:
            cfg_file.write(old_cfg_file.read())
        cfg_file.flush()

        xcalar.compute.util.cluster.DevCluster(
            config_path=cfg_file.name).start()
        # we should now be able to detect this cluster, even though it has a weird config
        cluster = detect_cluster()
        assert cluster.is_up()
        assert cluster.config.config_file_path == cfg_file.name
        cluster.stop()
    assert detect_cluster().is_fully_down()


# While some operations are on-going, just ungraciously pull the rug
# Witness to ENG-6754
@pytest.mark.parametrize("num_nodes",
                         [pytest.param(1, marks=pytest.mark.slow), (3)])
def test_racy_shutdown(num_nodes):
    testPrefix = "racyShutdown"

    with xcalar.compute.util.cluster.DevCluster(
            num_nodes=num_nodes) as cluster:
        assert cluster.is_up()

        xcApi = XcalarApi()
        session = Session(
            xcApi,
            owner=None,
            username=TestClusterUserName,
            sessionName=TestClusterSessionName)
        xcApi.setSession(session)
        session.activate()

        dataTargetMgr = Target2(xcApi)
        memoryTargetName = "QA test_cluster memory"
        dataTargetMgr.add("memory", memoryTargetName, {})

        # Let's start a never-ending load
        udfMgr = Udf(xcApi)
        udfSource = """
import json
def load(inp, ins):
    ii = 0
    while True:
        yield { "x": ii }
        ii += 1
"""

        udfName = "{}-udf".format(testPrefix)
        udfMgr.addOrUpdate(udfName, udfSource)

        dsName = "{}-ds".format(testPrefix)

        query = [{
            "operation": "XcalarApiBulkLoad",
            "args": {
                "dest": dsName,
                "loadArgs": {
                    "sourceArgsList": [{
                        "targetName": memoryTargetName,
                        "path": "1",
                        "fileNamePattern": "",
                        "recursive": False,
                    }],
                    "parseArgs": {
                        "parserFnName": "{}:load".format(udfName),
                        "parserArgJson": "{}",
                        "fileNameFieldName": "",
                        "recordNumFieldName": "",
                        "allowFileErrors": False,
                        "allowRecordErrors": False
                    },
                    "size":
                        32737418240
                }
            }
        }]

        queryName = "{}-query".format(testPrefix)
        xcApi.submitQuery(json.dumps(query), queryName=queryName, isAsync=True)

        queryState = xcApi.queryState(queryName).queryState
        while queryState == QueryStateT.qrNotStarted:
            # Let's wait for the load to run for a while
            time.sleep(1)
            queryState = xcApi.queryState(queryName).queryState

    assert detect_cluster().is_fully_down()

    # Now let's clean up after ourselves
    with xcalar.compute.util.cluster.DevCluster(
            num_nodes=num_nodes) as cluster:
        assert cluster.is_up()

        xcApi = XcalarApi()
        session = Session(
            xcApi,
            owner=None,
            username=TestClusterUserName,
            sessionName=TestClusterSessionName,
            reuseExistingSession=True)
        xcApi.setSession(session)
        session.activate()

        Dataset.bulkDelete(
            xcApi, "{}-*".format(testPrefix), deleteCompletely=True)
        session.destroy()

    assert detect_cluster().is_fully_down()
