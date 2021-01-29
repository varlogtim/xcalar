# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# XXX: In future, this tool must be made flexible with respect to window size
# Currently, it mostly works with less columns (by excluding what's beyond the
# reduced geometry) but stops responding to 'quit' or re-sort keys, apparently
# due to a bug in getch.

import sys
import time
import curses
import csv
import os
from multiprocessing.pool import ThreadPool
from curses import wrapper
from datetime import datetime
import json

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT
from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT
from xcalar.compute.util.utils import get_skew
from xcalar.external.LegacyApi.WorkItem import WorkItemGetStats
from xcalar.compute.util.system_stats_util import GetTopStats

high_skew_threshold = 80
out_filename = "xcalar_top_out.json"
page_size_default = 128 * 1024

# XXX: this is a bit of a hack since we hard-code the prefix XD uses when
# executing optimized jobs...in the future, XD should use a SDK-like i/f
XD_optjob_prefix = "xcRet_DF2_"

pgm_descr_text = '''
This utility reports live stats for jobs executing in optimized mode.

For each job, the following are reported (column headings are on left):

    JOB:                    Name of job (truncated after a limit)
    SESSION:                Session-name or ID, depending on how job was invoked
    STATE:                  One of [Runnable, Running, Success, Failed, Cancelled]
    ELAPSED(s):             Elapsed time in seconds
    %PROGRESS (w/c/f):      % of graph-nodes completed, no. waiting/completed/failed
    OPERATOR (%completed):  Current running operator (% completed).
    W0RKING SET:            Current working set size of job. Takes into account the fact
                            that tables aren't dropped until all child graph nodes are done
                            using it.
    SKEW:                   Load imbalance across cluster nodes.
                            Values are from 0 (perfectly balanced) to 100 (most imbalanced).
                            Skew is across the job (including operators executed so far).
    SKEWTIME%:              % of elapsed time spent in high skew (> 80) operators.
    NETW:                   Transport page (bytes) sent between cluster-nodes.
    SIZE:                   Size (bytes) of all tables ever created by the job.
                            NOTE: some of these tables may have been dropped.
    PGDENSE%:               % page density (Xcalar buffer cache page utilization)

The tool runs like 'htop' and will quit when the 'q' key is entered. If this
doesn't work, use "ctrl-C" to quit.

Entering one of the following keys will take the specified action

"e": sort (descending) by ELAPSED(s) (this is default sort key)
"s": sort (descending) by SIZE
"d": dump a JSON snapshot of current top output into {}

For a seamless experience, the tool must run in a window with at least 170
columns and a height which accommodates all jobs in the system. If not, the tool
may error out with a failure, asking the user to resize the window.
'''.format(out_filename)

qstateToStr = {
    QueryStateT.qrNotStarted: "Runnable",
    QueryStateT.qrProcessing: "Running",
    QueryStateT.qrFinished: "Success",
    QueryStateT.qrError: "Failed",
    QueryStateT.qrCancelled: "Cancelled",
}

# X, Y co-ordinates for all the column headings
# NOTE: the user must have wide enough screen to display
# all headings

sys_metrics_header_x = 1
sys_metrics_value_x = 35

total_working_set_y = 1
xcalar_paging_rate_y = total_working_set_y + 1
xcalar_paging_diff_y = xcalar_paging_rate_y + 1
xdb_used_y = xcalar_paging_diff_y + 1
xdb_available_y = xdb_used_y + 1
swap_y = xdb_available_y + 1

job_heading_y = swap_y + 2

min_job_col_len = 20

col_x = {"JOB": 1, "SESSION": 45, "STATE": 65, "ELAPSED": 75, "NODES": 86,
            "API": 104, "WORKING_SET": 124, "SKEW": 136, "SKEWTIME": 142, "NETW": 152,
            "PGDENSE": 160, "SIZE": 170}

last_update_ts_x = 1

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

maxY, maxX = 0, 0

def addstr_colcheck(stdscr, *args):
    global maxY, maxY
    y = args[0]
    x = args[1]
    if y > maxY or x > maxX:
        return
    try:
        stdscr.addstr(*args)
    except:
        pass

def get_pagesize(xcalarApi):
    page_size = None
    try:
        configs = xcalarApi.getConfigParams()
        for param in configs.parameter:
            if param.paramName == "XdbPageSize":
                page_size = int(param.paramValue)
                break
        assert page_size is not None
        return page_size
    except Exception as e:
        # use a hard-coded default page-size
        return page_size_default


def display_heading(stdscr):
    addstr_colcheck(stdscr, total_working_set_y, sys_metrics_header_x,
                    "TOTAL WORKING SET", curses.A_REVERSE)
    addstr_colcheck(stdscr, xcalar_paging_diff_y, sys_metrics_header_x,
                    "DEMAND PAGED BYTES",
                    curses.A_REVERSE)
    addstr_colcheck(stdscr, xcalar_paging_rate_y, sys_metrics_header_x,
                    "DEMAND PAGING RATE (BYTES/SEC)", curses.A_REVERSE)
    addstr_colcheck(stdscr, xdb_used_y, sys_metrics_header_x,
                    "XDB BYTES USED", curses.A_REVERSE)
    addstr_colcheck(stdscr, xdb_available_y, sys_metrics_header_x,
                    "XDB BYTES AVAILABLE", curses.A_REVERSE)
    addstr_colcheck(stdscr, swap_y, sys_metrics_header_x,
                    "OS SWAP USED", curses.A_REVERSE)

    ## Job level headings
    addstr_colcheck(stdscr, job_heading_y, col_x["JOB"], "JOB", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["SESSION"], "SESSION", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["STATE"], "STATE", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["ELAPSED"], "ELAPSED(s)",
                    curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["NODES"], "%PROGRESS (w/c/f)",
                    curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["API"], "OPERATOR (%done)", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["WORKING_SET"], "W0RKING SET",
                    curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["SKEW"], "SKEW", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["SKEWTIME"], "SKEWTIME%",
                curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["NETW"], "NETW", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y,  col_x["PGDENSE"], "PGDENSE%", curses.A_REVERSE)
    addstr_colcheck(stdscr, job_heading_y, col_x["SIZE"], "SIZE", curses.A_REVERSE)

# Convert bytest to human readable format
def bytes_to_hr(nbytes):
    if nbytes < KB:
        return str(nbytes)
    elif nbytes < MB:
        nkb = "{:.1f}K".format(nbytes / KB)
        return nkb
    elif nbytes < GB:
        nmb = "{:.1f}M".format(nbytes / MB)
        return nmb
    else:
        ngb = "{:.1f}G".format(nbytes / GB)
        return ngb


def _build_time_sorted_node_map(nodes):
    node_dict = {}
    for node in nodes:
        # filter out nodes that haven't been executed yet
        if node.state == DgDagStateT.DgDagStateProcessing or node.state == DgDagStateT.DgDagStateReady:
            node_dict[node.dagNodeId] = (node, int(node.startTime))
    final_node_dict = {
        k: v[0]
        for k, v in sorted(node_dict.items(), key=lambda item: item[1][1])
    }
    return final_node_dict


#
# Each invocation of get_job_row runs on its own ThreadPool thread
#
def get_job_row(xcalarApi, job_name, page_size):
    # job returned in list may be gone by the time
    # queryState is called on it below - so the call
    # to queryState could fail with job not found
    # error - so protect this program by ignoring any
    # such error
    row = {}
    try:
        qstate = xcalarApi.queryState(job_name)
    except Exception as e:
        return row

    # session info extraction depends on job name following
    # a specific format:
    # <SDK_prefix>-<sessionInfo>-<user_or_dataflow_names>-<username>-<timestamp>
    #
    # XD invokes it with a different format:
    # "xcRet_DF2_<sessionID>_<long_misc_string>"
    #
    # Handle either format...
    #
    # If this fails, null the session column

    if XD_optjob_prefix in job_name:
        try:
            row["session"] = job_name.split('_')[2]
        except Exception as e:
            row["session"] = "-"
        pass
    else:
        try:
            row["session"] = job_name.split('-')[1]
        except Exception as e:
            row["session"] = "-"
        pass

    if "XcalarSDK" in job_name:
        # assume format is: "XcalarSDK*--<sessionID>-*" and so report
        # only the interesting part - the stuff after the sessionID (since this
        # is extracted separately above)
        row["job"] = "-".join(job_name.split('-')[2:])
    else:
        row["job"] = job_name
    row["state"] = qstateToStr[qstate.queryState]
    row["elapsed_sec"] = int(qstate.elapsed.milliseconds / 1000)
    q = qstate.numQueuedWorkItem
    c = qstate.numCompletedWorkItem
    f = qstate.numFailedWorkItem
    nodes_tot = q + c + f
    nodes_done = c + f
    node_pct = int((nodes_done / nodes_tot) * 100)
    row["nodes_prog"] = "{}% ({}/{}/{})".format(node_pct, q, c, f)
    curr_api = 0

    # add up sizes of all nodes' table sizes
    job_size_total = 0

    # add up sizes of all nodes' pages received
    job_pages_recv_total = 0

    # add up sizes of xdbBytes usage stats across all nodes'
    # to calculate page density (consumed/required)
    job_xdb_bytes_required = 0
    job_xdb_bytes_consumed = 0

    # this is for job skew calculation - add up each
    # graph-node's cluster-node numRows - and then calculate
    # skew on the job's numRowsPerNode. Following just inits
    # the job_num_rows_per_node list
    job_num_rows_per_node = []
    for ii in range(qstate.queryGraph.numNodes):
        job_num_rows_per_node.append(0)

    # total elapsed time spent in high-skew (> 80) nodes is
    # calculated in hskew_time
    hskew_time = 0

    running_size_total = 0
    previous_operator_dropped_tables_size = 0
    parent_children_mapping = {}
    child_parent_mapping = {}
    nodes_dict = _build_time_sorted_node_map(qstate.queryGraph.node)

    for _, n in nodes_dict.items():
        job_size_total += n.sizeTotal
        # this script may run on systems which don't yet have
        # the page density stats available in the graph-node
        # so just skip them, if so
        try:
            job_xdb_bytes_required += n.xdbBytesRequired
        except Exception as e:
            pass
        try:
            job_xdb_bytes_consumed += n.xdbBytesConsumed
        except Exception as e:
            pass

        # accumulate time spent in high skew nodes
        if get_skew(n.numRowsPerNode) >= high_skew_threshold:
            hskew_time += n.elapsed.milliseconds

        # accumulate per-node row count  to calculate
        # overall skew at the job level - this combined
        # with "skewtimepct", should be very useful:
        # if skew is high, but nodes with high skew
        # account for, say, only 17% of the total job
        # elapsed time, then this may not be worth
        # imrproving
        job_num_rows_per_node = [
            j1 + n1 for j1, n1 in zip(job_num_rows_per_node, n.numRowsPerNode)
        ]

        # An operator may have had pages received from other
        # cluster nodes - accumulate this up into
        # job_pages_recv_total
        node_rcv_tot = 0
        for rcv in n.numTransPagesReceivedPerNode:
            node_rcv_tot += rcv

        job_pages_recv_total += node_rcv_tot

        # True in memory table size (taking into account parent table memory)
        running_size_total = running_size_total + int(
            n.sizeTotal) - previous_operator_dropped_tables_size
        previous_operator_dropped_tables_size = 0
        node_id = n.dagNodeId

        if node_id in child_parent_mapping:
            # loop through all my parents, to see if I am the last child for any of them
            for parent in child_parent_mapping[node_id]:
                if len(parent_children_mapping[parent]) == 1:
                    # I am the last child, and parent table can be dropped
                    previous_operator_dropped_tables_size += nodes_dict[
                        parent].sizeTotal
                # remove myself from parent-children mapping
                parent_children_mapping[parent].remove(node_id)

            # I have looked at all my parents, so can remove myself from children-parent mapping
            del child_parent_mapping[node_id]

        if n.children:
            parent_children_mapping[node_id] = n.children
            for child in n.children:
                if child in child_parent_mapping:
                    child_parent_mapping[child].append(node_id)
                else:
                    child_parent_mapping[child] = [node_id]

        # Record currently running API, and get its % completion
        if n.state == DgDagStateT.DgDagStateProcessing:
            curr_api = n.api
            if n.numWorkTotal is not 0:
                apipct = n.numWorkCompleted / n.numWorkTotal
            else:
                apipct = 0
            # This should be the last node we will process (since nodes
            # that haven't started executing yet are filtered out in
            # _build_time_sorted_node_map and we don't support parallel
            # operations)
            break

    # Pct time spent in high skew nodes
    hskew_time_pct = (hskew_time / qstate.elapsed.milliseconds) * 100

    if curr_api is not 0:
        row["api"] = XcalarApisT._VALUES_TO_NAMES[curr_api].replace("XcalarApi", "")
        row["apipct"] = "{:0.0f}".format(apipct * 100)
    else:
        row["api"] = "-"
        row["apipct"] = "-"

    row["total_size_processed"] = job_size_total
    row["current_working_set_size"] = running_size_total

    # convert transport pages to bytes
    row["netw"] = job_pages_recv_total * page_size
    # Page density
    if job_xdb_bytes_required > 0:
        row["pgdense"] = "{:0.0f}".format(
            (job_xdb_bytes_consumed / job_xdb_bytes_required) * 100)
    else:
        row["pgdense"] = str(0)

    # Job skew and time spent in high skew nodes as % of ET
    row["skew"] = get_skew(job_num_rows_per_node)
    row["skewtimepct"] = "{:0.0f}".format(hskew_time_pct)
    return row


def get_xc_internal_stats(client, num_cluster_nodes, url, username, password,
                          prev_xc_stats):
    xc_stats = {
        "numSerializedBytes": [],
        "numDeserializedBytes": [],
        "pagedOutBytes": [],
        "XcalarPagingRate": [],
        "timestamps": [],
        "XdbUsedBytes": [],
        "SystemSwapUsed": [],
        "XdbAviableBytes": []
    }
    xcalarApi = XcalarApi(
        url=url,
        client_secrets={
            "xiusername": username,
            "xipassword": password
        })
    for node_id in range(num_cluster_nodes):
        getStatsWorkItem = WorkItemGetStats(nodeId=node_id)
        metrics = xcalarApi.execute(getStatsWorkItem)
        cur_ts = int(time.time())
        for stat in metrics:
            if stat.statName in xc_stats:
                rate = "-"
                if stat.statName == "numSerializedBytes":
                    serialized = int(stat.statValue)
                    try:
                        prev_ts = prev_xc_stats["timestamps"][node_id]
                        prev_serialized = prev_xc_stats["numSerializedBytes"][
                            node_id]
                        rate = bytes_to_hr((serialized - prev_serialized) /
                                   (cur_ts - prev_ts))
                    except Exception as e:
                        pass
                    xc_stats["XcalarPagingRate"].append(rate)
                if stat.statName == "numDeserializedBytes":
                    deserialized = int(stat.statValue)
                xc_stats[stat.statName].append(int(stat.statValue))

        paged_out = serialized - deserialized
        xc_stats["pagedOutBytes"].append(bytes_to_hr(paged_out))
        xc_stats["timestamps"].append(cur_ts)

    xc_stats["pagedOutBytes"] = " | ".join("{:6.6}".format(e) for e in xc_stats["pagedOutBytes"])
    xc_stats["XcalarPagingRate"] = " | ".join(
        "{:6.6}".format(e) for e in xc_stats["XcalarPagingRate"])
    top_stats = GetTopStats()._get_top_stats_from_backend(client, get_from_all_nodes=True, is_json_format=True)
    for node in sorted(top_stats, key = lambda i: i['cluster_node']):
        used = int(node["XdbUsedBytes"])
        total = int(node["XdbTotalBytes"])
        xc_stats["XdbUsedBytes"].append(bytes_to_hr(used))
        xc_stats["SystemSwapUsed"].append(bytes_to_hr(int(node["SystemSwapUsed"])))
        xc_stats["XdbAviableBytes"].append(bytes_to_hr(total - used))

    xc_stats["XdbUsedBytes"] = " | ".join("{:6.6}".format(e) for e in xc_stats["XdbUsedBytes"])
    xc_stats["SystemSwapUsed"] = " | ".join("{:6.6}".format(e) for e in xc_stats["SystemSwapUsed"])
    xc_stats["XdbAviableBytes"] = " | ".join("{:6.6}".format(e) for e in xc_stats["XdbAviableBytes"])
    return xc_stats


def dotop(stdscr, client, url, username, password, interval, parallel_procs, verbose, iters):
    # set initial screen size in globals maxY, maxX
    global maxY
    maxY = curses.LINES - 1
    global maxX
    maxX = curses.COLS - 1
    global col_x

    curses.noecho()
    display_heading(stdscr)
    stdscr.nodelay(True)
    stdscr.refresh()    # displays heading
    xcalarApi = XcalarApi(
        url=url,
        client_secrets={
            "xiusername": username,
            "xipassword": password
        })
    num_cluster_nodes = client.get_num_nodes_in_cluster()
    page_size = get_pagesize(xcalarApi)
    sortkey = "e"    # default is elpased time
    pool = ThreadPool(processes=parallel_procs)
    xc_stats = {}
    count = 0
    for _ in range(iters):
        start_time = int(time.time())
        curr_job = 0
        jobs = {}
        sess = client.list_sessions()
        sess_num = 0
        xc_internal_stats_async_result = pool.apply_async(
            get_xc_internal_stats,
            # client object not being used after this point, so okay to pass it into
            # async routine.
            (client, num_cluster_nodes, url, username, password, xc_stats))
        if len(sess) is not 0:
            while True:
                try:
                    xcalarApi.setSession(sess[sess_num])
                    jobs = xcalarApi.listQueries('*').queries
                    if len(jobs) is not 0:
                        break
                    else:
                        sess_num = sess_num + 1
                    if sess_num >= len(sess):
                        break
                    stdscr.refresh()
                except Exception as e:
                    sess_num = sess_num + 1
                    if sess_num >= len(sess):
                        break
                    pass
        # Now check if there are jobs and if so, collect stats for
        # each job, and report them
        final_jobs = []
        if len(jobs) is not 0:
            if count == 0:
                # display "Loading..." only first time
                addstr_colcheck(stdscr, job_heading_y+1, col_x["JOB"],
                                    "Loading...")
                count += 1
            rows = []
            job_async_result = []
            curr_job = 0
            xcApis = []
            # Invoke get_job_row for each job using ThreadPool for
            # concurrency (upto num_parallel_procs will run in parallel).
            # Each invocation needs its own XcalarApi object (the same
            # object can't be invoked concurrently from two different
            # ThreadPool threads)
            for job in jobs:
                # We only care about jobs that are currently running
                if job.state != "qrProcessing":
                    continue
                else:
                    final_jobs.append(job)
                xcApis.append(
                    XcalarApi(
                        url=url,
                        client_secrets={
                            "xiusername": username,
                            "xipassword": password
                        }))
                xcApis[curr_job].setSession(sess[sess_num])
                # XXX: cache and skip the get of completed jobs' status
                job_async_result.append(
                    pool.apply_async(get_job_row,
                                     (xcApis[curr_job], job.name, page_size)))
                curr_job += 1
            assert (len(final_jobs) == curr_job)

            # Now get results from each of the get_job_row() invocations to
            # yield a row per job
            max_job_id_len = min_job_col_len
            for ii in range(len(final_jobs)):
                row = job_async_result[ii].get()
                if len(row) != 0:
                    rows.append(row)
                    max_job_id_len = max(max_job_id_len, len(row["job"]))

            # sort the rows by elapsed time or size (table sizes)
            # XXX: add more here if needed
            if sortkey == "e":
                sortedrows = sorted(
                    rows, key=lambda k: k['elapsed_sec'], reverse=True)
            elif sortkey == "s":
                sortedrows = sorted(
                    rows, key=lambda k: k['total_size_processed'], reverse=True)

            # Now display the sorted rows
            y_coord = job_heading_y + 1
            total_working_set_size = 0
            if verbose:
                # display full job name in verbose mode
                # if job name doesn't fit space before next column starts
                # ("SESSION"), then compute x_delta by which all
                # subsequent columns need to shift their x-co-ordinate
                next_x = col_x["JOB"] + max_job_id_len + 1
                delta = next_x - col_x["SESSION"]
                if delta != 0:
                    # for each column from curr_col + 1 to end,
                    # update its x co-ord by x_delta
                    for col in col_x:
                        if col != "JOB":
                            col_x[col] += delta

            stdscr.clear()
            display_heading(stdscr)
            for row in sortedrows:
                if not verbose:
                    addstr_colcheck(stdscr, y_coord, col_x["JOB"],
                                    '{:43.43}'.format(row["job"]))
                else:
                    addstr_colcheck(stdscr, y_coord, col_x["JOB"], row["job"])
                addstr_colcheck(stdscr, y_coord, col_x["SESSION"],
                                '{:19.19}'.format(row["session"]))
                addstr_colcheck(stdscr, y_coord, col_x["STATE"],
                                '{:10.10}'.format(row["state"]))
                addstr_colcheck(stdscr, y_coord, col_x["ELAPSED"],
                                "{:0.0f}".format(row["elapsed_sec"]))
                addstr_colcheck(stdscr, y_coord, col_x["NODES"],
                                row["nodes_prog"])
                addstr_colcheck(stdscr, y_coord, col_x["API"],
                                '{:12.12}({}%)'.format(row["api"], row["apipct"]))
                addstr_colcheck(stdscr, y_coord, col_x["WORKING_SET"],
                                bytes_to_hr(row["current_working_set_size"]))
                addstr_colcheck(stdscr, y_coord, col_x["SKEW"], str(row["skew"]))
                addstr_colcheck(stdscr, y_coord, col_x["SKEWTIME"],
                            row["skewtimepct"])
                addstr_colcheck(stdscr, y_coord, col_x["NETW"],
                            bytes_to_hr(row["netw"]))
                addstr_colcheck(stdscr, y_coord, col_x["PGDENSE"], str(row["pgdense"]))
                addstr_colcheck(stdscr, y_coord, col_x["SIZE"],
                            bytes_to_hr(row["total_size_processed"]))

                total_working_set_size += row["current_working_set_size"]
                # get new Y co-ord
                y_coord += 1
            addstr_colcheck(stdscr, total_working_set_y, sys_metrics_value_x,
                            bytes_to_hr(total_working_set_size))
        else:
            y_coord = job_heading_y + 1
            stdscr.clear()
            display_heading(stdscr)

        ## System level metrics
        xc_stats = xc_internal_stats_async_result.get()
        addstr_colcheck(stdscr, xcalar_paging_diff_y, sys_metrics_value_x,
                        xc_stats["pagedOutBytes"])
        addstr_colcheck(stdscr, xcalar_paging_rate_y, sys_metrics_value_x,
                        xc_stats["XcalarPagingRate"])
        addstr_colcheck(stdscr, xdb_used_y, sys_metrics_value_x,
                        xc_stats["XdbUsedBytes"])
        addstr_colcheck(stdscr, xdb_available_y, sys_metrics_value_x,
                        xc_stats["XdbAviableBytes"])
        addstr_colcheck(stdscr, swap_y, sys_metrics_value_x,
                        xc_stats["SystemSwapUsed"])
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        addstr_colcheck(stdscr, y_coord + 2, last_update_ts_x,
                        "[Last Update: {}]".format(dt_string))
        stdscr.refresh()

        # Check every sec for quit or dump input, and every interval
        # secs for sort key. Recompute max co-ordinates if window size changes
        nsecs = 0
        uinput = 0
        end_time = int(time.time())
        wait_time =  interval - (end_time - start_time)
        while wait_time > 0 and nsecs < wait_time:
            nsecs += 1
            time.sleep(1)
            uinput = stdscr.getch()
            if uinput == ord('q') or uinput == ord('d'):
                break
            elif uinput == curses.KEY_RESIZE:
                maxY, maxX = stdscr.getmaxyx()
                curses.resizeterm(maxY, maxX)
        if uinput == ord('q'):
            break
        elif uinput == ord('s'):    # sort by size
            sortkey = "s"
        elif uinput == ord('e'):    # sort by elapsed time
            sortkey = "e"
        elif uinput == ord('d'):
            out_json = {}
            out_json["memory_profile"] = xc_stats
            out_json["job_stats"] = sortedrows
            try:
                with open(out_filename, 'w') as f:
                    f.write(json.dumps(out_json, indent=2))
            except IOError:
                print("I/O error when trying to dump to json file")
                break


def get_program_description():
    return pgm_descr_text


def xcalar_top_main(client, url, username, password, interval, parallel_procs, verbose, iters):
    wrapper(dotop, client, url, username, password, interval, parallel_procs, verbose, iters)
