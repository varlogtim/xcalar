import random
from time import sleep
from multiprocessing import Pool
import pytest

from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi as LegacyXcalarApi
from xcalar.external.LegacyApi.Top import Top as TopHelper

pytestmark = pytest.mark.usefixtures("config_backup")


# used for reset_metric test
def get_new_top_helper():
    xcalar_api = LegacyXcalarApi()
    measureIntervalInMs = 100
    cacheValidityInMs = 0
    top_helper = TopHelper(
        xcalar_api,
        measureIntervalInMs=measureIntervalInMs,
        cacheValidityInMs=cacheValidityInMs)

    return (top_helper)


# ## PYTEST


# parts of this function will be visible in the documentation
def test_snippet_get():
    # [START get_metrics]
    # from xcalar.external.client import Client

    client = Client()

    all_node_metrics = []
    num_nodes = client.get_num_nodes_in_cluster()

    for node in range(num_nodes):
        node_metrics = client.get_metrics_from_node(node)
        all_node_metrics.append(node_metrics)

    # [END get_metrics]


# parts of this function may be visible in the documentation
def test_snippet_reset():
    # [START reset_metrics]
    # from xcalar.external.client import Client

    client = Client()

    client.reset_libstats()

    # [END reset_metrics]


# parts of this function may be visible in the documentation
def test_snippet_get_and_set_config_param():
    # [START get_and_set_config_param]
    # from xcalar.external.client import Client

    client = Client()
    config_params = client.get_config_params()
    param_to_check = "SwapUsagePercent"

    for config_param in config_params:
        if (config_param["param_name"] == param_to_check):

            # if SwapUsagePercent param_value is greater than 90, set to 90
            if (int(config_param["param_value"]) > 90):
                client.set_config_param(
                    param_name=param_to_check, param_value=90)
            return
    # [END get_and_set_config_param]
    assert (0)


# helper function
def get_metrics_from_all_nodes(client):
    all_node_metrics = []

    for node in range(client.get_num_nodes_in_cluster()):
        node_metrics = client.get_metrics_from_node(node)
        all_node_metrics.append(node_metrics)

    return all_node_metrics


# helper function
def get_metric_object(node_id, group_name, metric_name, metrics):
    assert (group_name is not None)
    assert (node_id is not None)
    assert (metric_name is not None)
    assert (metrics is not None)

    for metric in metrics:
        if (group_name == metric["group_name"]
                and metric_name == metric["metric_name"]):
            return metric

    assert (0)


# helper function
# returns the number of stat requests processed
def get_metrics_from_node(node_id, num_metric_requests):
    client = Client()
    for i in range(num_metric_requests):
        node_metrics = client.get_metrics_from_node(node_id)
    return (i + 1)


def test_metrics_in_parellel(client):
    num_processes_per_node = 4
    num_metric_requests = 4
    num_nodes = client.get_num_nodes_in_cluster()
    thread_handles = []
    for node in range(num_nodes):
        pool = Pool(processes=num_processes_per_node)
        per_node_thread_handles = []
        for i in range(num_processes_per_node):
            per_node_thread_handles.append(
                pool.apply_async(get_metrics_from_node,
                                 [node, num_metric_requests]))

        thread_handles.extend(per_node_thread_handles)
        pool.close()
        pool.join()

    total_iterations = 0
    for thread_handle in thread_handles:
        total_iterations += thread_handle.get()

    assert (total_iterations == (
        num_processes_per_node * num_metric_requests * num_nodes))


# test_reset_metrics similar to old version
def test_reset_metrics(client):
    # test expects the mgmtd we are talking to is always connected to node0
    # uk.msgNumInvocationsCount's top on node0 would have increased
    # uk.msgRemoteInvocationsCount's top on other nodes' would have increased

    ukNumLocalInvocationsGroupName = "uk.msgNumInvocationsCount"
    ukNumRemoteInvocationsGroupName = "uk.msgRemoteInvocationsCount"
    topSinglemetricName = "Msg2pcXcalarApiTop"

    numTopLocalInvocationsSlice1 = []
    numTopLocalInvocationsSlice2 = []
    numTopLocalInvocationsSlice3 = []

    numTopRemoteInvocationsSlice1 = []
    numTopRemoteInvocationsSlice2 = []
    numTopRemoteInvocationsSlice3 = []

    num_nodes = client.get_num_nodes_in_cluster()

    for node_id in range(num_nodes):
        numTopLocalInvocationsSlice1.append(int(0))
        numTopLocalInvocationsSlice2.append(int(0))
        numTopLocalInvocationsSlice3.append(int(0))

        numTopRemoteInvocationsSlice1.append(int(0))
        numTopRemoteInvocationsSlice2.append(int(0))
        numTopRemoteInvocationsSlice3.append(int(0))

    top_helper = get_new_top_helper()

    first_slice = get_metrics_from_all_nodes(client)
    num_invocations = 5
    for i in range(num_invocations):
        top_helper.executeTop()
        # sleep for 1 second to hit usrnodes
        sleep(1)

    second_slice = get_metrics_from_all_nodes(client)

    client.reset_libstats()

    third_slice = get_metrics_from_all_nodes(client)

    for node_id in range(num_nodes):
        numTopLocalInvocationsSlice1[node_id] = get_metric_object(
            node_id=node_id,
            group_name=ukNumLocalInvocationsGroupName,
            metric_name=topSinglemetricName,
            metrics=first_slice[node_id])["metric_value"]

        numTopLocalInvocationsSlice2[node_id] = get_metric_object(
            node_id=node_id,
            group_name=ukNumLocalInvocationsGroupName,
            metric_name=topSinglemetricName,
            metrics=second_slice[node_id])["metric_value"]

        numTopLocalInvocationsSlice3[node_id] = get_metric_object(
            node_id=node_id,
            group_name=ukNumLocalInvocationsGroupName,
            metric_name=topSinglemetricName,
            metrics=third_slice[node_id])["metric_value"]

        numTopRemoteInvocationsSlice1[node_id] = get_metric_object(
            node_id=node_id,
            group_name=ukNumRemoteInvocationsGroupName,
            metric_name=topSinglemetricName,
            metrics=first_slice[node_id])["metric_value"]

        numTopRemoteInvocationsSlice2[node_id] = get_metric_object(
            node_id=node_id,
            group_name=ukNumRemoteInvocationsGroupName,
            metric_name=topSinglemetricName,
            metrics=second_slice[node_id])["metric_value"]

        numTopRemoteInvocationsSlice3[node_id] = get_metric_object(
            node_id=node_id,
            group_name=ukNumRemoteInvocationsGroupName,
            metric_name=topSinglemetricName,
            metrics=third_slice[node_id])["metric_value"]

    # actual test
    for node_id in range(num_nodes):
        if (node_id == 0):
            # 1 top call from here results in 2 calls from mgmtd to usrnode
            assert ((int(numTopLocalInvocationsSlice2[0]) - int(
                numTopLocalInvocationsSlice1[0])) == (num_invocations * 2))

            # this is after resetmetrics
            assert (int(numTopLocalInvocationsSlice3[0]) == 0)

            assert (int(numTopRemoteInvocationsSlice1[0]) == 0)
            assert (int(numTopRemoteInvocationsSlice2[0]) == 0)
            assert (int(numTopRemoteInvocationsSlice3[0]) == 0)
        else:
            assert ((int(numTopRemoteInvocationsSlice2[node_id]) - int(
                numTopRemoteInvocationsSlice1[node_id])) == (
                    num_invocations * 2))

            # this is after resetmetrics
            assert (int(numTopRemoteInvocationsSlice3[0]) == 0)

            assert (int(numTopLocalInvocationsSlice1[node_id]) == 0)
            assert (int(numTopLocalInvocationsSlice2[node_id]) == 0)
            assert (int(numTopLocalInvocationsSlice3[node_id]) == 0)


# helper function
def get_metrics_with_group_name(client, node_id, group_name, check_group_id):
    assert (group_name is not None)
    assert (node_id is not None)
    assert (client is not None)

    metrics = client.get_metrics_from_node(node_id)

    metrics_with_group_name = []
    group_id = None
    metrics_with_group_name = [
        metric for metric in metrics if metric["group_name"] == group_name
    ]
    group_id = metrics_with_group_name[0]["group_id"]
    assert all(
        [metric["group_id"] == group_id for metric in metrics_with_group_name])

    assert (group_id is not None and group_id == check_group_id)
    total_single_metrics_for_group = client._get_group_id_map(
        node_id)[group_id][1]

    assert (total_single_metrics_for_group == len(metrics_with_group_name))


def test_group_mapping_and_get_metrics(client):
    num_nodes = client.get_num_nodes_in_cluster()
    assert (num_nodes > 0)

    for node in range(num_nodes):
        group_mapping = client._get_group_id_map(node)
        assert (len(group_mapping) > 0)

        # repeat test for 5 random group_names
        for i in range(5):
            random_group_id = random.choice(list(group_mapping.keys()))
            group_name = group_mapping[random_group_id][0]
            get_metrics_with_group_name(client, node, group_name,
                                        random_group_id)


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def test_get_and_set_config_param(client):
    assert (client is not None)
    config_params = client.get_config_params()
    assert (len(config_params) > 0)

    # The following raise errors when set_config_param is called on them.
    # XdbSerDesMode should not be changed to default value for purpose of jenkins tests
    cannot_be_set = [
        "ClusterLogLevel", "XemHostPortNumber", "LatencyRtThreadCount",
        "ThroughputRtThreadCount", "XdbSerDesMode"
    ]

    # config params that will be set in the following tests. This could be expandied
    params_to_be_set = [
        'UdfExportBufferSize', 'ResultSetTimeoutinSecs',
        'XemStatsPushHeartBeat'
    ]

    # update config params to default_value+1 if they are changeable and do not require
    # a restart, and are in params_to_be_set
    for config_param in config_params:
        if (config_param["param_name"] in params_to_be_set):
            # The following 3 asserts should never be hit if params_to_be_set is selected correctly
            assert (config_param["changeable"]
                    and not config_param["restart_required"])
            assert is_int(config_param["default_value"])
            assert config_param["param_name"] not in cannot_be_set

            val = int(config_param["default_value"]) + 1
            print(config_param["param_name"] + " : " + str(val))
            client.set_config_param(
                param_name=config_param["param_name"], param_value=val)

    config_params = client.get_config_params()
    assert (len(config_params) > 0)
    # check changed values to make sure they are default_value+1
    for config_param in config_params:
        if (config_param["param_name"] in params_to_be_set):
            assert (config_param["changeable"]
                    and not config_param["restart_required"])
            assert is_int(config_param["default_value"])
            assert config_param["param_name"] not in cannot_be_set

            assert (int(config_param["param_value"]) == (
                int(config_param["default_value"]) + 1))

    # set selected params to default_value
    for config_param in config_params:
        if (config_param["param_name"] in params_to_be_set):
            assert (config_param["changeable"]
                    and not config_param["restart_required"])
            assert is_int(config_param["default_value"])
            assert config_param["param_name"] not in cannot_be_set

            print(config_param["param_name"] + " : " +
                  config_param["param_value"])
            client.set_config_param(
                param_name=config_param["param_name"],
                param_value="",
                set_to_default=True)

    # check that changed params are actually the default_value
    config_params = client.get_config_params()
    assert (len(config_params) > 0)
    for config_param in config_params:
        if (config_param["param_name"] in params_to_be_set):
            assert (config_param["changeable"]
                    and not config_param["restart_required"])
            assert is_int(config_param["default_value"])
            assert config_param["param_name"] not in cannot_be_set

            assert (
                config_param["param_value"] == config_param["default_value"])


def test_version(client):
    version = client.get_version()


def test_error_raising(client):
    assert client is not None

    with pytest.raises(TypeError) as e:
        client.get_metrics_from_node(False)
    assert str(e.value) == "node_id must be int, not '{}'".format(type(False))

    with pytest.raises(ValueError) as e:
        client.get_metrics_from_node(-2)
    assert str(
        e.value
    ) == "node_id must be a valid node in the cluster (0 <= node_id < num_nodes={})".format(
        client.get_num_nodes_in_cluster())

    with pytest.raises(TypeError) as e:
        client.reset_libstats(node_id=False)
    assert str(e.value) == "node_id must be int, not '{}'".format(type(False))

    with pytest.raises(ValueError) as e:
        client.reset_libstats(node_id=-2)
    assert str(
        e.value
    ) == "node_id must be a valid node in the cluster (0 <= node_id < num_nodes={})".format(
        client.get_num_nodes_in_cluster())

    with pytest.raises(TypeError) as e:
        client.set_config_param(False, "45", True)
    assert str(e.value) == "param_name must be str, not '{}'".format(
        type(False))

    with pytest.raises(TypeError) as e:
        client.set_config_param("correct", ["45"])
    assert str(
        e.value) == "param_value must be str, bool or int, not '{}'".format(
            type(["45"]))

    with pytest.raises(ValueError) as e:
        client.set_config_param("correct", ["45"], True)
    assert str(e.value) == "No such param_name: '{}'".format("correct")

    with pytest.raises(TypeError) as e:
        client.set_config_param("correct", "45", "2")
    assert str(e.value) == "set_to_default must be bool, not '{}'".format(
        type("2"))
