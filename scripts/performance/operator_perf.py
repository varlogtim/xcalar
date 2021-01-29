#!/usr/bin/env python
"""
This script is for benchmarking operator performance.
Currently, it generates a single column(int64 type) table with
random values and runs a operator and collects fiber stats.
"""
import re
import random
import sys
import argparse
import logging
import time
import os
import json
import math
import signal
from datetime import datetime
from collections import namedtuple

from xcalar.solutions.query_builder import QueryBuilder

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.app import App

from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT, XcalarOrderingTStr
from xcalar.compute.coretypes.LibApisEnums.constants import XcalarApisT, XcalarApisTStr
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.Status.ttypes import StatusT

logger = logging.getLogger("perf_work")
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s {%(filename)s:%(lineno)d} %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

results_list = []
ubmTestResults = {}
ubmTestResults["group"] = "ubmTest"

here = os.path.abspath(os.path.dirname(__file__))
collect_fiber_stats_interval = 60
scale = {"K": 2**10, "M": 2**20, "G": 2**30}

# NOTE: leave the order of actions below exactly as is - code depends on it
actions = [
    'all', 'clean', 'load-table', 'index', 'aggr', 'filter', 'map', 'sort',
    'inner-join-self'
]

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = 'false'

client = None
session = None
xc_api = None
num_nodes_in_cluster = None

script_args = None

# sum of low memory stat values, the stats
# considered in _low_mem_impacted function
low_mem_total_value = 0

udf_mod_name = "load_data"
udf_source = """
import sys
import json
import numpy.random

import xcalar.container.context as ctx

def _gen_random_integers(size):
    yield from numpy.random.randint(1, sys.maxsize, size=size)

def load_data(fullPath, inStream, seed=0):
    inObj = json.loads(inStream.read())
    seed += ctx.get_xpu_id()
    start = inObj["startRow"]
    end = inObj["startRow"] + inObj["numRows"]
    numpy.random.seed(seed)
    bulk_rand = 2 ** 20
    while start < end:
        if start + bulk_rand < end:
            num_vals = bulk_rand
        else:
            num_vals = end - start
        start += num_vals
        rand_numbers = _gen_random_integers(num_vals)
        for num in rand_numbers:
            yield {
                "intCol": num
            }

def get_one_as_string():
    return "1"
"""

df_run_args = namedtuple(
    'df_exec_args',
    ['table_name', 'optimized', 'is_async', 'clean_job_state', 'pin_results'])
df_run_args.__new__.__defaults__ = (None, True, True, False, False)
default_df_args = df_run_args()
default_pattern = "operator_perf"


def sig_clean_up(signum, frame):
    logger.info("sig_clean_up called with signal: {}".format(signum))
    clean_up()
    sys.exit()


signal.signal(signal.SIGINT, sig_clean_up)
signal.signal(signal.SIGHUP, sig_clean_up)
signal.signal(signal.SIGTERM, sig_clean_up)


def cluster_setup():
    global client, session, xc_api, num_nodes_in_cluster

    # setup cluster client and session
    url = "https://{}:{}".format(script_args.xcalarHost,
                                 script_args.xcalarPort)
    username = script_args.xcalarUser
    password = script_args.xcalarPass
    client = Client(
        url=url,
        client_secrets={
            "xiusername": username,
            "xipassword": password
        })
    num_nodes_in_cluster = client.get_num_nodes_in_cluster()
    session = None
    try:
        session = client.get_session(script_args.sessionName)
    except Exception:
        session = client.create_session(script_args.sessionName)

    xc_api = XcalarApi(auth_instance=client.auth)
    xc_api.setSession(session)

    ubmTestResults["xlrVersion"] = client.get_version().version
    ubmTestResults["startUTC"] = "{}".format(
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f"))
    ubmTestResults["notes"] = script_args.notes

    if script_args.action == 'clean':
        return

    # add import udf
    try:
        client.get_udf_module(udf_mod_name).delete()
    except Exception as ex:
        logger.warn('Failed to delete udf {}'.format(ex))
    client.create_udf_module(udf_mod_name, udf_source)

    # know current memory values before running any operator calling
    # _low_mem_impacted function will update the global value 'low_mem_value'
    if not script_args.ignoreLowMem and _low_mem_impacted():
        logger.warning('low memory detected before test run!')


#
# Helper functions
#
def record_results(op_name, execution_time, source_tables):
    result_dict = {}
    result_dict["name"] = op_name
    result_dict["time"] = execution_time
    result_dict["source-nrows"] = []
    result_dict["source-bytes"] = []
    result_dict["source-skew"] = []

    # add source table meta
    for source_table in source_tables:
        tab_meta = source_table.get_meta()
        result_dict.get("source-nrows").append(tab_meta.total_records_count)
        result_dict.get("source-bytes").append(tab_meta.total_size_in_bytes)
        result_dict.get("source-skew").append(tab_meta.node_skew())

    results_list.append(result_dict)
    logger.info("{} done in {}s!".format(op_name, execution_time))


def noex(f, *args):
    try:
        f(*args)
    except Exception as ex:
        logger.warn(f'Failed to run {f.__name__}, error: {ex}')


def _get_df_execution_times(df_id, xc_api_name):
    df_state = xc_api.queryState(df_id)
    assert (df_state.queryState == QueryStateT.qrFinished)
    min_start_time = math.inf
    max_end_time = -1
    for graph_node in df_state.queryGraph.node:
        if xc_api_name == "any" or xc_api_name == graph_node.api:
            min_start_time = min(min_start_time, graph_node.startTime)
            max_end_time = max(max_end_time, graph_node.endTime)
    return (min_start_time, max_end_time)


def _wait_for_dfs(df_ids, xc_api_name):
    min_start_time = math.inf
    max_end_time = -1
    for df_id in df_ids:
        query_output = xc_api.waitForQuery(df_id)
        if query_output.queryState == QueryStateT.qrError or query_output.queryState == QueryStateT.qrCancelled:
            raise XcalarApiStatusException(query_output.queryStatus,
                                           query_output)
        # get start and end time of the dataflow
        tmp_min_start_time, tmp_max_end_time = _get_df_execution_times(
            df_id, xc_api_name)
        min_start_time = min(min_start_time, tmp_min_start_time)
        max_end_time = max(max_end_time, tmp_max_end_time)
        # delete the dataflow state
        noex(xc_api.deleteQuery, df_id)

    return (max_end_time - min_start_time) / (10**9)


def _show_tables(table_names):
    tabs = session.list_tables()
    for tab in tabs:
        if tab.name in table_names:
            tab.show()


def _run_dfs_concurrently(op_name,
                          df_list,
                          df_args=default_df_args,
                          xc_api_name='any'):
    group_id = export_system_fiber_stats(
        op_name, dir_path=script_args.statsPath)
    df_ids = []
    tab_names = []
    success = False
    time_taken = 0
    try:
        for df in df_list:
            df_id = session.execute_dataflow(
                df,
                query_name=f"{default_pattern}_{op_name}",
                table_name=df_args.table_name,
                optimized=df_args.optimized,
                is_async=df_args.is_async,
                clean_job_state=df_args.clean_job_state,
                pin_results=df_args.pin_results)
            df_ids.append(df_id)
            tab_names.append(df.final_table_name)
        time_taken = _wait_for_dfs(df_ids, xc_api_name)
        if logger.level == logging.DEBUG:
            _show_tables(tab_names)
        success = True
    finally:
        if group_id > 0:
            logger.info("Stopping fiber stats app..")
            if success:
                # sleep to get last sample of fiber stats
                time.sleep(collect_fiber_stats_interval)
            reap_export_system_stats_app(group_id)
        _clean_tables()

    # check if slow down has occurred due to low memory
    if not script_args.ignoreLowMem and _low_mem_impacted():
        raise RuntimeError("Slowdown due to low memory detected!")
    return time_taken


def _low_mem_impacted():
    # XXX this is an approximation to tell if XC is taking
    # slow paths due to memory pressure. It only takes into
    # consideration XDB allocations, as this test should
    # make sure to keep heap allocations(like deleting dag nodes, etc) in control.
    #
    # alert if xdb allocations are from pagekvbufPageable slab.
    # alert if xdb allocations are from slowAllocs in pagekvbuf slab.
    # alert if xcalar demand paging has occured.
    #
    # group_name mapping to metric_names
    lowmem_hint_stats = {
        'xdb.pagekvbufPageable.bc': ['slowAllocs', 'fastAllocs'],
        'xdb.pagekvbuf.bc': ['slowAllocs'],
        'XdbMgr': ['numSerializedBytes']
    }
    global low_mem_total_value

    lowmem_numbers = [
        metric['metric_value'] for node_id in range(num_nodes_in_cluster)
        for metric in client.get_metrics_from_node(node_id)
        if metric['group_name'] in lowmem_hint_stats
        and metric['metric_name'] in lowmem_hint_stats[metric['group_name']]
    ]
    curr_mem_total_value = sum(lowmem_numbers)
    if curr_mem_total_value > low_mem_total_value:
        low_mem_total_value = curr_mem_total_value
        return True
    low_mem_total_value = curr_mem_total_value
    return False


# XXX ENG-8682
# weird workaround to rename table
# as current rename table api is broken
def _rename_table(old_tab_obj, new_name):
    df = Dataflow.create_dataflow_from_table(client, old_tab_obj)
    df = df.filter('eq(1, 1)')
    df._query_list[1]['args']['source'] = old_tab_obj.name
    del df._query_list[0]
    df_args = df_run_args(
        table_name=new_name,
        optimized=False,
        pin_results=old_tab_obj._get_meta().pinned)
    time_taken = _run_dfs_concurrently("filter", [df], df_args=df_args)

    # drop old table
    if old_tab_obj._get_meta().pinned:
        old_tab_obj.unpin()
    old_tab_obj.drop(True)


#
# table load,
# schema: {'intCol': 'DfInt64'}
#
def load_data(tab_name, num_rows):
    try:
        tab = session.get_table(tab_name)
        raise RuntimeError("Table already exists")
    except ValueError:
        pass
    task_name = "load"
    target_name = "TableGen"
    path = str(num_rows)
    table_info = {"schema": [{"name": "intCol", "type": "string"}]}
    schema = {"intCol": "integer"}
    parser_name = f"/sharedUDFs/{udf_mod_name}:load_data"
    parser_args = {'seed': script_args.seed}
    qb = QueryBuilder(schema)
    qb.XcalarApiBulkLoad(
        targetName=target_name,
        parserFnName=parser_name,
        parserArg=parser_args,
        path=path)
    tmp_tab_name = qb.query[-1]['args']['source']
    # delete index operator which queryBuilder puts after bulkLoad
    del qb.query[-1]
    qb.counter -= 1
    qb.XcalarApiSynthesize(
        table_in=tmp_tab_name, columns=qb.synthesizeColumns())
    query = json.dumps(qb.getQuery())
    columnsOut = [{'columnName': c, 'headerAlias': c} for c in schema]
    df = Dataflow.create_dataflow_from_query_string(
        client,
        query_string=query,
        columns_to_export=columnsOut,
        dataflow_name=task_name)
    logger.info("Loading data..")
    logger.info(json.dumps(df._query_list, indent=4))

    df_args = df_run_args(
        table_name=tab_name, optimized=True, pin_results=True)
    time_taken = _run_dfs_concurrently(task_name, [df], df_args=df_args)

    tab = session.get_table(tab_name)
    record_results(task_name, time_taken, [tab])

    logger.info("{} => {}\n".format(tab.name, tab.record_count()))


# Operators execution functions

# Filter


# Filter operation, filtering out a random value
# most likely results in zero record table
def apply_filter(tab_name):
    tab = session.get_table(tab_name)
    dfs = []
    logger.info("Running filter(s)..")
    for idx in range(1, script_args.concurrency + 1):
        df = Dataflow.create_dataflow_from_table(client, tab)
        df = df.filter('eq(intCol, {})'.format(random.randint(1, sys.maxsize)))
        df._query_list[1]['args']['source'] = tab.name
        del df._query_list[0]
        dfs.append(df)

    logger.info(json.dumps(dfs[0]._query_list, indent=4))
    df_args = df_run_args(optimized=False)
    time_taken = _run_dfs_concurrently(
        "filter",
        dfs,
        df_args=df_args,
        xc_api_name=XcalarApisT.XcalarApiFilter)

    if script_args.concurrency == 1:
        record_results("filter", time_taken, [tab])
    else:
        record_results(f"filter-conc-{script_args.concurrency}", time_taken,
                       [tab])


# Aggegate functions


# min aggregate
def apply_aggregate(tab_name):
    task_name = "aggregate-min"
    tab = session.get_table(tab_name)
    dfs = []
    logger.info("Running aggregation(s)..")
    for idx in range(1, script_args.concurrency + 1):
        df = Dataflow.create_dataflow_from_table(client, tab)
        df = df.aggregate('min', 'intCol')
        df._query_list[1]['args']['source'] = tab.name
        del df._query_list[0]
        del df._query_list[1:]
        dfs.append(df)

    logger.info(json.dumps(dfs[0]._query_list, indent=4))

    df_args = df_run_args(optimized=False)
    time_taken = _run_dfs_concurrently(
        task_name,
        dfs,
        df_args=df_args,
        xc_api_name=XcalarApisT.XcalarApiAggregate)

    if script_args.concurrency == 1:
        record_results(task_name, time_taken, [tab])
    else:
        record_results(f"{task_name}-conc-{script_args.concurrency}",
                       time_taken, [tab])


# Index fuctions


# Custom index function, helps with join/union-dedup/groupBy
def _run_custom_index(src_tab_name,
                      dest_tab_name,
                      index_args,
                      dht_name=None,
                      pin_result=False):
    task_name = f"index-{index_args[0]['name']}-{XcalarOrderingTStr[index_args[0]['ordering']]}"
    logger.info(f"Running {task_name}..")
    tab = session.get_table(src_tab_name)
    df = Dataflow.create_dataflow_from_table(client, tab)
    df = df.custom_sort(
        index_args, result_table_name=dest_tab_name, dht_name=dht_name)
    df._query_list[1]['args']['source'] = tab.name
    del df._query_list[0]
    try:
        tab = session.get_table(dest_tab_name).drop(delete_completely=True)
    except ValueError:
        pass
    logger.info(json.dumps(df._query_list, indent=4))

    df_args = df_run_args(
        table_name=dest_tab_name, optimized=False, pin_results=pin_result)
    time_taken = _run_dfs_concurrently(task_name, [df], df_args=df_args)

    if logger.level == logging.DEBUG:
        _show_tables([dest_tab_name])
    _clean_tables(src_tab_name, False)

    tab = session.get_table(dest_tab_name)
    logger.info("{} => {}\n".format(tab.name, tab.record_count()))
    return tab


# sort - Ascending on intCol column
def apply_sort(tab_name):
    apply_index(
        tab_name,
        task_name="sort",
        ordering=XcalarOrderingT.XcalarOrderingAscending)


# index - applies unordered index on intCol column
def apply_index(tab_name,
                task_name="index",
                ordering=XcalarOrderingT.XcalarOrderingUnordered):
    logger.info(f"Running {task_name}(s)..")
    tab = session.get_table(tab_name)
    index_args = {"name": "intCol", "type": "integer", "ordering": ordering}
    dfs = []
    for idx in range(1, script_args.concurrency + 1):
        df = Dataflow.create_dataflow_from_table(client, tab)
        df = df.custom_sort([index_args])
        dfs.append(df)
    logger.info(json.dumps(dfs[0]._query_list[1], indent=4))

    time_taken = _run_dfs_concurrently(
        task_name, dfs, xc_api_name=XcalarApisT.XcalarApiIndex)

    if script_args.concurrency == 1:
        record_results(task_name, time_taken, [tab])
    else:
        record_results(f"{task_name}-conc-{script_args.concurrency}",
                       time_taken, [tab])


# Map functions


# Helper function for map functions
def _apply_map(task_name, tab_name, eval_string):
    tab = session.get_table(tab_name)
    dfs = []
    logger.info(f"Running {task_name}(s)..")
    for idx in range(1, script_args.concurrency + 1):
        df = Dataflow.create_dataflow_from_table(client, tab)
        df = df.map([(eval_string, f'{task_name}-col')])
        dfs.append(df)

    logger.info(json.dumps(dfs[0]._query_list[1], indent=4))
    time_taken = _run_dfs_concurrently(
        task_name, dfs, xc_api_name=XcalarApisT.XcalarApiMap)

    if script_args.concurrency == 1:
        record_results(task_name, time_taken, [tab])
    else:
        record_results(f"{task_name}-conc-{script_args.concurrency}",
                       time_taken, [tab])


# Map function one using xdf and udf, both resulting same results
def apply_map(tab_name):
    # xdf
    _apply_map("map-xdf", tab_name, "string(1)")

    # udf
    udf_eval_str = "load_data:get_one_as_string()"
    _apply_map("map-udf", tab_name, udf_eval_str)


# Join functions


# This function appiles an inner-join on two same tables on "intCol" column.
# self inner join
def apply_inner_join_self(tab_name):
    task_name = 'inner-join-self'
    dfs = []
    dfs = []
    # prepare table to indexed for join
    pre_join_tab_name = "tab_pre_join#1"
    sort_args = {
        "name": "intCol",
        "type": "integer",
        "ordering": XcalarOrderingT.XcalarOrderingUnordered
    }
    try:
        tab = session.get_table(pre_join_tab_name)
    except:
        tab = _run_custom_index(tab_name, pre_join_tab_name, [sort_args],
                                "systemUnorderedDht", True)

    logger.info(f"Running {task_name}(s)..")
    for idx in range(1, script_args.concurrency + 1):
        df = Dataflow.create_dataflow_from_table(client, tab)
        df = df.join(df, [("intCol", "intCol")])
        source_name = df._query_list[0]['args']['dest']
        df._query_list[2]['args']['source'] = [source_name] * 2
        # delete synthesize and index operators
        del df._query_list[1]
        dfs.append(df)

    logger.info(json.dumps(dfs[0]._query_list, indent=4))
    time_taken = _run_dfs_concurrently(
        task_name, dfs, xc_api_name=XcalarApisT.XcalarApiJoin)

    if script_args.concurrency == 1:
        record_results(task_name, time_taken, [tab])
    else:
        record_results(f"{task_name}-conc-{script_args.concurrency}",
                       time_taken, [tab])

    # rename to original table
    _rename_table(tab, tab_name)


# Fiber stats collection app.
# This functions is to launches fiber stats app before the
# operator run and gets destroyed after the run.
def export_system_fiber_stats(file_name_prefix, dir_path="/tmp"):
    group_id = 0
    if script_args.noStats:
        return group_id
    app = App(client)
    app_name = "system_stats_app_perf_work"

    # check fiber stats enabled
    fiber_param_name = "RuntimeStats"
    fiber_param_value = [
        param["param_value"] for param in client.get_config_params()
        if param["param_name"] == fiber_param_name
    ][0]
    if fiber_param_value != "true":
        logger.warn("Fiber stats are disabled, enabling it")
        client.set_config_param(
            param_name=fiber_param_name, param_value="true")

    # upload app
    with open(os.path.join(here, "fiber_stats_app.py")) as fp:
        app_src = fp.read()
    app.set_py_app(app_name, app_src)

    # run app
    try:
        # input params
        input_obj = {"dir_path": dir_path, "file_prefix": file_name_prefix}
        group_id = app.run_py_app_async(app_name, True, json.dumps(input_obj))
        logger.info("Group id of fiber stats app {}".format(group_id))
    except Exception as ex:
        logger.error(f"Failed to start fiber stats app: {ex}")
    return int(group_id)


def reap_export_system_stats_app(group_id):
    app = App(client)
    try:
        app.cancel(str(group_id))
    except Exception as ex:
        logger.error(f"Error reaping stats app: {ex}")


#
# Clean up functions
#
def clean_up(pattern="*", unpinned_only=True):
    logger.info("Clean up started..")
    op_api = Operators(xc_api)
    if not session:
        return
    _clear_all_dataflows()
    op_api.dropConstants('*')
    _clean_tables(pattern, unpinned_only)
    logger.info("Done clean up!")


def _clear_all_dataflows():
    logger.info("Clearing up all dataflows..")
    df_queries = xc_api.listQueries(f'*{default_pattern}*').queries
    for df_query in df_queries:
        logger.info(f'clearing dataflow {df_query.name}..')
        noex(xc_api.cancelQuery, df_query.name)
        try:
            xc_api.waitForQuery(df_query.name)
            xc_api.deleteQuery(df_query.name)
        except XcalarApiStatusException as ex:
            if ex.status != StatusT.StatusQrQueryNotExist:
                raise


def _clean_tables(pattern="*", unpinned_only=True):
    logger.info(f"Dropping all tables with pattern '{pattern}'..")
    session.drop_tables(pattern, delete_completely=True)
    if unpinned_only:
        return
    for tab in session.list_tables(pattern=pattern):
        noex(tab.unpin)
        tab.drop(True)


#
# Input arguments parser function
#
def parse_user_args():
    global script_args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-H",
        "--xcalarHost",
        dest="xcalarHost",
        required=False,
        default="localhost",
        type=str,
        help="url of the Xcalar cluster",
    )
    parser.add_argument(
        "-a",
        "--apiPort",
        dest="xcalarPort",
        required=False,
        default=os.getenv("XCE_HTTPS_PORT", "8443"),
        type=str,
        help="url of the Xcalar cluster",
    )
    parser.add_argument(
        "-u",
        "--xcalarUser",
        dest="xcalarUser",
        help="username of xcalar",
        required=False,
        default="admin",
    )
    parser.add_argument(
        "-p",
        "--xcalarPass",
        dest="xcalarPass",
        help="password of xcalar",
        required=False,
        default="admin",
    )
    parser.add_argument(
        "-s",
        "--sessionName",
        dest="sessionName",
        help="sessionName to be generated",
        required=False,
        default="perf")
    parser.add_argument(
        "--action", dest="action", help="/".join(actions), required=True)
    parser.add_argument(
        "--size",
        dest="size",
        help=
        "data size to generate with some tax; \ncurrently it generates one column of integer type",
        required=False,
        default='15G')
    parser.add_argument(
        "--seed",
        dest="seed",
        help="seed to make random data generation deterministic",
        required=False,
        type=int,
        default=0)
    parser.add_argument(
        "--statsPath",
        dest="statsPath",
        help="stats directory path",
        required=False,
        default='/tmp')
    parser.add_argument(
        "-c",
        "--concurrency",
        dest="concurrency",
        help="number of operators to run concurrently except load",
        required=False,
        type=int,
        default=1)
    parser.add_argument(
        "--no-stats",
        dest="noStats",
        help="don't run fiber stats along with the operator runs",
        action="store_true")
    parser.add_argument(
        "-v",
        dest="verbose",
        help="will set DEBUG level logging",
        action="store_true")
    parser.add_argument(
        "--notes",
        dest="notes",
        help="comments/notes to be recorded in run results",
        required=False,
        type=str,
        default="manual run")
    parser.add_argument(
        "--results-output-dir",
        dest="outputDir",
        help="dir path where operator timings will be recorded",
        required=False,
        type=str,
        default="/tmp")
    parser.add_argument(
        "--iter-num",
        dest="iterNum",
        help="iteration number",
        required=False,
        type=int,
        default=0)
    parser.add_argument(
        "--ignore-lowmem",
        dest="ignoreLowMem",
        help=
        "Do not fail even if Xcalar is running low on memory during the run",
        action="store_true")
    script_args = parser.parse_args()


#
# Benchamrk start function
#
def runBm(action, size):
    tab_name = "tab#1"
    if action == 'load-table':
        # load data
        match = re.match(r'^([0-9]+)([KMG])?', size)
        number = int(match.group(1))
        m_scale = match.group(2)
        max_size = number * scale[m_scale] if m_scale else number
        logger.info("Generating {} size {} records".format(
            max_size, max_size // 8))
        load_data(tab_name, max_size // 8)
    elif action == 'filter':
        apply_filter(tab_name)
    elif action == 'map':
        apply_map(tab_name)
    elif action == 'aggr':
        apply_aggregate(tab_name)
    elif action == 'inner-join-self':
        apply_inner_join_self(tab_name)
    elif action == 'sort':
        apply_sort(tab_name)
    elif action == 'index':
        apply_index(tab_name)
    elif action == 'clean':
        clean_up(unpinned_only=False)
    else:
        assert False


if __name__ == '__main__':
    parse_user_args()
    logger.info("@@@ args: {}".format(script_args))

    if script_args.verbose:
        logger.setLevel(logging.DEBUG)
    random.seed(script_args.seed)
    # setup
    cluster_setup()

    action = script_args.action
    if action not in actions:
        raise ValueError(
            f'Invalid action. Choose one from {"/".join(actions)}')

    size = script_args.size
    if action != 'all':
        runBm(action, size)
    else:
        op_actions = actions
        op_actions.pop(0)    # assume first is 'all' - which isn't needed
        for a in op_actions:
            runBm(a, size)

    ubmTestResults["results"] = results_list
    ubmTestResults["endUTC"] = "{}".format(
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f"))
    print("ubmTestResults is {}".format(json.dumps(ubmTestResults, indent=4)))

    output_file_path = script_args.outputDir + "/xce-ubm-test-{}-ubm_results.json".format(
        script_args.iterNum)
    with open(output_file_path, 'wb') as rf:
        rf.write(json.dumps(ubmTestResults).encode())
