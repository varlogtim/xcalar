import time
import os
import requests
import signal

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.app import App
from xcalar.compute.util.utils import XcUtil

from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.compute.util.config import detect_config

import pytest
import json

pytestmark = pytest.mark.usefixtures("config_backup")

try:
    xce_logdir = detect_config().all_options['Constants.XcalarLogCompletePath']
except KeyError as e:
    xce_logdir = '/var/log/xcalar'


@pytest.fixture(scope="module")
def app(client):
    app = App(client)
    yield app


def test_local_error_app(app):
    app_name = 'localError'
    app_src = 'raise ValueError("failed")'
    app.set_py_app(app_name, app_src)

    try:
        app.run_py_app(app_name, False, "")
        assert False
    except XcalarApiStatusException as e:
        assert e.status == StatusT.StatusUdfModuleLoadFailed


def test_local_error_aync_app(app):
    app_name = 'localError'
    app_src = 'raise ValueError("failed")'
    app.set_py_app(app_name, app_src)
    group_id = 0
    try:
        group_id = app.run_py_app_async(app_name, False, "")
        assert False
    except XcalarApiStatusException as e:
        assert e.status == StatusT.StatusUdfModuleLoadFailed

    assert not app.is_app_alive(group_id)


def test_local_run_error_app(app):
    app_name = 'localError'
    app_src = 'def main(inBlob): raise ValueError("failed")'
    app.set_py_app(app_name, app_src)

    outStr, errStr = app.run_py_app(app_name, False, "")
    assert '[[""]]' == outStr
    assert "ValueError: failed" in errStr


def test_is_alive_check_in_error_scenario(app):
    app_name = 'localError'
    app_src = "def main(inBlob): import time; time.sleep(2); var = 1/0; time.sleep(100000)"
    app.set_py_app(app_name, app_src)

    is_global = True
    for _ in range(4):
        print("is_global = {}".format(is_global))
        group_id = app.run_py_app_async(app_name, is_global, "")

        # Allow some time for the backend to notice that the app has failed
        for _ in range(10):
            if not app.is_app_alive(group_id):
                break
            time.sleep(1)
        assert not app.is_app_alive(group_id)
        is_global = not is_global


def test_is_alive_check_after_app_kill(app):
    app_name = 'appKill'
    app_src = 'def main(inBlob): import time; time.sleep(2000000000)'
    app.set_py_app(app_name, app_src)
    is_global = True

    #
    # We can't kill grpc xpu processes.
    #
    grpc_pids = []
    for line in os.popen("grep GrpcServer " + xce_logdir + "/xpu.log"):
        start = line.find("Pid")
        if (start != -1):
            start += 4
            end = line.find(":", start)
            grpc_pids.append(int(line[start:end]))

    print("grpc_pids=", str(grpc_pids))

    for _ in range(4):
        print("is_global = {}".format(is_global))
        group_id = app.run_py_app_async(app_name, is_global, "")
        assert app.is_app_alive(group_id)

        for pid in os.popen("pgrep childnode"):
            if (not int(pid) in grpc_pids):
                with XcUtil.ignored(Exception):
                    os.kill(int(pid), signal.SIGKILL)

        # Allow some time for backend to notice that the app has been killed
        for _ in range(10):
            try:
                if not app.is_app_alive(group_id):
                    break
            except Exception as e:
                print("Exception:" + str(e))
                time.sleep(10000)
            time.sleep(1)

        assert not app.is_app_alive(group_id)
        is_global = not is_global


def test_is_alive_after_clean_exit(app):
    sleep_time = 5
    app_name = 'app'
    app_src = 'def main(inBlob): import time; time.sleep({})'.format(
        sleep_time)
    app.set_py_app(app_name, app_src)
    is_global = True
    for _ in range(2):
        print("is_global = {}".format(is_global))
        group_id = app.run_py_app_async(app_name, is_global, "")
        assert app.is_app_alive(group_id)

        for i in range(10):
            if not app.is_app_alive(group_id):
                break
            time.sleep(1)

        assert not app.is_app_alive(group_id)
        is_global = not is_global


def test_app_cancel(app):
    app_name = 'appCancel'

    # Validate cancel reaps out the App state and allows refreshing the
    # App source code.
    for ii in range(0, 4):
        if ii % 2 == 0:
            app_src = 'def main(inBlob): import time; time.sleep(1000000000)'
        else:
            app_src = 'def main(inBlob): import time; time.sleep(2000000000)'
        app.set_py_app(app_name, app_src)
        group_id = app.run_py_app_async(app_name, False, "")
        for _ in range(10):
            assert app.is_app_alive(group_id)
            time.sleep(1)
        app.cancel(group_id)
        assert not app.is_app_alive(group_id)


# Witness to ENG-6824. We want to construct a message that just barely
# tips over to 2 pages
def test_xpu_comm(app):
    app_name = "test_xpu_comm"
    app_src = """
import pickle
from xcalar.container.cluster import get_running_cluster
import xcalar.container.parent as xce
from xcalar.compute.services.Stats_xcrpc import Stats
import xcalar.compute.localtypes.Stats_pb2

def main(inBlob):
    xceClient = xce.Client()
    xdbPageSize = xceClient.get_xdb_page_size()
    cluster = get_running_cluster()
    pickleOverhead = len(pickle.dumps(b''))
    for ii in range(-10, 10):
        res = xceClient.get_libstats()
        buf = b'\x01' * (xdbPageSize - pickleOverhead + ii)
        if cluster.is_master():
            cluster.broadcast_msg(buf)
        else:
            recvBuf = cluster.recv_msg()
            assert(recvBuf == buf)
    return 0
"""
    app.set_py_app(app_name, app_src)
    (out_strs, err_strs) = app.run_py_app(app_name, True, "")
    print(err_strs)
    for node_errs in json.loads(err_strs):
        for app_err_str in node_errs:
            assert app_err_str == ""
    # When the cluster shuts down, we check for leaks


def test_system_perf_app(client):
    # Step 1: enable perf app
    perf_configs = client.get_config_params(
        param_names=["RuntimeStats", "EnableRuntimeStatsApp"])
    for config_param in perf_configs:
        client.set_config_param(
            param_name=config_param["param_name"], param_value=True)

    # Step 2: fetch perf data over HTTP.
    params = {'json': "true"}
    retries = 600    # 60 secs, very generous timeout for app to come up
    sleep_time = 0.1
    while True:
        try:
            r = requests.get(url="http://localhost:6000", params=params)
            break
        except requests.exceptions.ConnectionError as ex:
            retries -= 1
            if retries == 0:
                raise ex
            time.sleep(sleep_time)
    data = r.json()

    # Step 3: disable perf app
    client.set_config_param(
        param_name="EnableRuntimeStatsApp", param_value=False)

    with pytest.raises(requests.exceptions.ConnectionError) as e:
        retries = 600    # 60 secs
        while True:
            r = requests.get(url="http://localhost:6000", params=params)
            retries -= 1
            time.sleep(sleep_time)
            assert retries > 0

    # step 4: put back the defaults
    for config_param in perf_configs:
        client.set_config_param(
            param_name=config_param["param_name"],
            param_value=config_param["param_value"])
