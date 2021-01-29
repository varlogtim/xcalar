# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

# This app is run for a lifetime of a cluster. It periodically gathers system 
# stats and dumps this out to the filesystem.
# Stats rotation is carried out every 24 hours to manage the space footprint of stats on disk

import gzip
import json
import os
import subprocess
from datetime import datetime
import shutil
import hashlib
import psutil
import time
import sys
from multiprocessing.pool import ThreadPool
import multiprocessing
import threading
import jsonpickle

from xcalar.container.cluster import get_running_cluster
import xcalar.container.cgroups.config as cgroups_config_mgr
import xcalar.container.cgroups.base as cgroups_base_mgr
import xcalar.container.context as ctx

from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT
from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT
from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.compute.util.system_stats_util import GetTopStats
from xcalar.compute.util.utils import thrift_to_json
from xcalar.compute.util.config import get_cluster_node_list

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.session import Session

from xcalar.container.parent import Client as XpuClient

nano_sec_in_micro_sec = 1000
millisecs_in_sec = 1000

# Dynamic Constants
num_cores = multiprocessing.cpu_count()
xcalar_paging_size = None
total_ram_size = psutil.virtual_memory().total
total_swap_size = psutil.swap_memory().total
version = None
num_nodes = None

# The current hardcoded stats rotation policy:
#  - Files older than 30 days are automatically deleted
#  - If stats space utilization is above min(5% of disk space, 10GB), we remove stats
#    one day at a time until utilization is below min(5% of disk space, 10GB).
#  - Stats younger than 7 days will never be deleted

# time based policy in days
rot_time_policy_oldest = 30
rot_time_policy_protected_age = 7
rot_space_policy_percent = 5
rot_spce_policy_absolute = 10737418240 # 10 GB
secs_per_day = 24*60*60

# Check and Update cluster configs every minute
cluster_config_check_period = 60

# Check configs every 10 secs
stats_config_check_period = 10

# Interval to sleep if stats are off
stats_off_check_interval = 10

# Set up logging
logger = ctx.get_logger()

node_list = get_cluster_node_list()
port = os.getenv("XCE_HTTPS_PORT", '8443')

# Config params
COLLECT_STATS = "CollectStats"
COLLECT_SYSTEM_STATS = "CollectSystemStats"
STATS_COLLECTION_INTERVAL = "StatsCollectionInterval"
COLLECT_PER_PROC_INFO = "IncludePerProcStats"
COLLECT_MPTSTAT = "IncludePerCpuStats"
COLLECT_CGROUP_STATS = "IncludeCgroupStats"
COLLECT_LIBSTATS = "IncludeLibstats"
COLLECT_TOP_STATS = "IncludeTopStats"
COLLECT_IO_STATS = "IncludeIOStats"
COLLECT_CONFIG_STATS = "CollectConfigStats"
STATS_WRITE_INTERVAL = "StatsWriteInterval"
CGROUPS_CONFIG = "Cgroups"

important_libstat_stats = ["numXdbPageableSlabOom",
                           "numOsPageableSlabOom",
                           "numPilePagesAlloced",
                           "numPilePagesFreed",
                           "numDagNodesAlloced",
                            "numDagNodesFreed",
                           "numSerializedPages",
                           "numDeserializedPages",
                           "numSerDesDroppedBatches",
                           "cumlTransPagesShipped",
                           "cumlTransPagesReceived",
                           "numRunnableSchedObjs",
                           "numSuspendedSchedObjs",
                           "timeSuspendedSchedObjs"]

important_libstat_groups = ["XdbMgr", "xdb.pagekvbuf.bc"]


def move_job_stats_to_shared_root(job_meta_path):
    with gzip.open(job_meta_path, "rb") as f:
        data = f.read()
        in_obj = json.loads(data)
    os.remove(job_meta_path)

    qs_location = in_obj["qsOutputLocation"]
    with gzip.open(qs_location, 'rb') as f:
        data = f.read().decode()
        qs = jsonpickle.decode(data)
    final_output = {}

   # XXX: Need to dataflow name in the future
    # final_output["dataflow_name"] = dataflow_name
    final_output["job_id"] = in_obj["jobName"]
    final_output["session_name"] = in_obj["sessionName"]
    final_output["user_id_name"] = in_obj["userIdName"]
    final_output["job_status"] = in_obj["jobStatus"]
    final_output["job_state"] = in_obj["jobState"]
    final_output["job_start_timestamp_microsecs"] = in_obj[
        "startTime"] // nano_sec_in_micro_sec
    final_output["job_end_timestamp_microsecs"] = in_obj[
        "endTime"] // nano_sec_in_micro_sec
    final_output["total_time_elapsed_millisecs"] = qs.elapsed.milliseconds
    final_output["number_completed_operations"] = qs.numCompletedWorkItem
    final_output["number_failed_operations"] = qs.numFailedWorkItem

    for node in qs.queryGraph.node:
        record = {}
        record["graph_node_name"] = node.name.name
        record["graph_node_id"] = node.dagNodeId
        record["operator_name"] = XcalarApisT._VALUES_TO_NAMES[node.api]
        record["operator_state"] = DgDagStateT._VALUES_TO_NAMES[node.state]
        record["operator_status"] = StatusT._VALUES_TO_NAMES[node.status]
        record[
            "graph_node_start_timestamp_microsecs"] = node.startTime // nano_sec_in_micro_sec
        record["graph_node_time_elapsed_millisecs"] = node.elapsed.milliseconds
        record[
            "graph_node_end_timestamp_microsecs"] = node.endTime // nano_sec_in_micro_sec
        record["input_parameters"] = thrift_to_json(node.input)
        record["input_size"] = node.inputSize
        record["size_total"] = node.sizeTotal
        record["size_per_cluster_node"] = ":".join(str(e) for e in node.sizePerNode)
        record["num_trans_pages_received_per_cluster_node"] = ":".join(str(e) for e in node.numTransPagesReceivedPerNode)
        record["xdb_bytes_required"] = node.xdbBytesRequired  # for page density
        record["xdb_bytes_consumed"] = node.xdbBytesConsumed  # for page density
        record["input_graph_nodes"] = ":".join(node.parents)
        record["output_graph_nodes"] = ":".join(node.children)
        record["total_row_count_output_table"] = node.numRowsTotal
        record["rows_per_cluster_node"] = ":".join(
            str(e) for e in node.numRowsPerNode)
        record["hash_slot_skew_per_cluster_node"] = ":".join(
            str(e) for e in node.hashSlotSkewPerNode)
        record["tag"] = node.tag
        record['comment'] = node.comment
        # XXX: Need to add this in the future once libstats are integrated with the queryState output
        # record["libstats"] = json.loads(node.statsJsonStringPerNode)["libstats"]
        if "nodes" not in final_output:
            final_output["nodes"] = []
        final_output["nodes"].append(record)

    file_path = os.path.join(in_obj["statsDirPath"], "job_stats.json.gz")
    with gzip.open(file_path, 'wb') as f:
        f.write(json.dumps(final_output).encode())
        logger.info("Successfully moved {} job stats to: {}".format(in_obj["jobName"], file_path))
    os.remove(qs_location)

def job_stats_routine(xcalar_logs_path):
    tmp_dir_root = os.path.join(xcalar_logs_path, ".statsStagingDir")
    logger.info("Starting routine to move job stats from {} to shared root".format(tmp_dir_root))
    os.makedirs(tmp_dir_root, exist_ok=True)
    tmp_dir_job_meta = os.path.join(tmp_dir_root, "jobMeta")
    os.makedirs(tmp_dir_job_meta, exist_ok=True)
    while (True):
        for f in os.listdir(tmp_dir_job_meta):
            try:
                job_meta_path = os.path.join(tmp_dir_job_meta, f)
                move_job_stats_to_shared_root(job_meta_path)
            except Exception as e:
                logger.error("Moving stats to shared root for {} failed with error: {}".format(f, e))
        time.sleep(1)

def get_dir_size(dir):
    excluded = set()
    total_size = os.stat(dir, follow_symlinks=False).st_size
    for root, dirs, files in os.walk(dir):
        for name in files + dirs:
            p = os.path.join(root, name)
            fstat = os.stat(p, follow_symlinks=False)
            # Avoid double counting of hard links
            if fstat.st_nlink > 1:
                pair = (fstat.st_ino, fstat.st_dev)
                if pair in excluded:
                    continue
                excluded.add(pair)
            total_size += fstat.st_size
    return total_size

def remove(path):
    """
    Remove the file or directory
    """
    # XXX need config parameter for "DataflowStatsHistory"
    if os.path.isdir(path) and "DataflowStatsHistory" in path:
        try:
            shutil.rmtree(path)
            logger.info("Stats rotation - deleted stats in directory: {}".format(path))
        except Exception as e:
            raise Exception("During stats file rotation, unable to "
                            "remove folder: {} due to the following error: {}".format(path, str(e)))

def delete_old_dirs(old_age_threshold_days, top_level_path):
    """
    Removes files from the passed in path that are older than or equal
    to the number_of_days
    """
    if not os.path.exists(top_level_path):
        logger.info("Stats rotation - path {} does not exist".format(top_level_path))
        return {}
    logger.info("Stats rotation - Deleting stats that are {} days or "
                "older in path: {}".format(old_age_threshold_days, top_level_path))
    cur_time = int(time.time())
    old_age_threshold_secs = cur_time - (old_age_threshold_days * secs_per_day)
    potential_deletes_cache = {}
    for dir in os.listdir(top_level_path):
        full_path = os.path.join(top_level_path, dir)
        try:
            ts = datetime.strptime(dir, "%Y-%m-%d").timestamp()
            if ts <= old_age_threshold_secs:
                remove(full_path)
            else:
                days_old = (cur_time - ts) // (secs_per_day)
                if (days_old > rot_time_policy_protected_age):
                    potential_deletes_cache[days_old] = full_path
        except Exception as e:
            logger.error(str(e))
            continue
    return potential_deletes_cache

def requires_space_rotation(top_level_path):
    total, used, free = shutil.disk_usage(top_level_path)
    dir_size = get_dir_size(top_level_path)
    logger.info("Stats rotation - stats directory size: {}".format(dir_size))
    max_policy = int(min(rot_spce_policy_absolute, rot_space_policy_percent/100 * total))
    if (dir_size > max_policy):
        logger.info("Stats rotation - stats dir is using {} of disk space, which exceeds "
                    "the {} bytes cap".format(dir_size, max_policy))
        return True
    else:
        return False

def delete_files_routine(top_level_path):
    """
    First, we delete all files that are older than 30 days old.
    potential_<>_stat_deletes_cache contains all the directories that
    are younger than 30 days, but are candidates for deletion if space
    is an issue. We will delete one directory at a time until the space
    restriction is met.
    """
    potential_system_stat_deletes_cache = delete_old_dirs(rot_time_policy_oldest, os.path.join(top_level_path, "systemStats"))
    potential_job_stat_deletes_cache = delete_old_dirs(rot_time_policy_oldest, os.path.join(top_level_path, "jobStats"))
    if requires_space_rotation(top_level_path):
        for num_days in range(rot_time_policy_oldest-1,
                          rot_time_policy_protected_age, -1):
            file_deleted = False
            if num_days in potential_system_stat_deletes_cache:
                try:
                    remove(potential_system_stat_deletes_cache[num_days])
                    file_deleted = True
                except Exception as e:
                    logger.error(str(e))

            if num_days in potential_job_stat_deletes_cache:
                try:
                    remove(potential_job_stat_deletes_cache[num_days])
                    file_deleted = True
                except Exception as e:
                    logger.error(str(e))

            if file_deleted:
                if not requires_space_rotation(top_level_path):
                    return
        if requires_space_rotation(top_level_path):
            logger.info("Stats rotation - space ceiling is exceeded due to the need to protect stat files"
                    " younger than {} days old".format(rot_time_policy_protected_age))

def no_fail_delete_files_routine(top_level_path):
    while True:
        try:
            logger.info("Stats rotation - starting.")
            delete_files_routine(top_level_path)
            logger.info("Stats rotation - completed.")
        except Exception as e:
            logger.error("Stats file rotation failed with error: {}".format(str(e)))
        time.sleep(secs_per_day)

def _number(text):
    if text.isdigit():
        return int(text)
    try:
        return float(text)
    except ValueError:
        return text

def process_line(input, ts, nodeId, type):
    headers_to_ignore = ["irq", "soft", "guest", "gnice"]
    stat = input.splitlines()
    ret = []
    header = stat[0].split()
    for line in stat[1:]:
        if not line:
            break
        comps = line.split()
        cur ={}
        if (type == "iostat"):
            cur["Device"] = comps[0]
            comps = comps[1:]
        elif (type == "io_cpu"):
            cur["CPU"] = "all"
        else:
            comps = comps[1:]
        cur["timestamp"] = ts
        cur["cluster_node"] = nodeId
        for metric, value in zip(header[1:], comps):
            metric = metric.replace("/", "_per_").replace("%", "")
            metric = metric.replace("user", "usr").replace("system", "sys")
            if metric not in headers_to_ignore:
                cur[metric] = _number(value)
        ret.append(cur)
    return ret

def get_io_stats(my_node_id, interval_in_secs, get_cpu_stats):
    # XXX this will be moved to C code in the future. Until then, we need
    # to live with the Popen here
    p = subprocess.Popen(["iostat", str(interval_in_secs), "2"],
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    stdout, sterr = p.communicate()
    retcode = p.returncode
    if retcode != 0:
        raise Exception("iostat failed with the following error: {}".format(sterr))
    ts = int(time.time() * millisecs_in_sec)
    out = stdout.decode().split("\n\n")
    ret = process_line(out[4], ts, my_node_id, "iostat")
    if get_cpu_stats:
        cpustats = process_line(out[3], ts, my_node_id, "io_cpu")
    else:
        cpustats = None
    return ret, cpustats

def get_cpu_stats(my_node_id, interval_in_secs):
    # XXX this will be moved to C code in the future. Until then, we need
    # to live with the Popen here
    p = subprocess.Popen(["mpstat", "-P", "ALL", str(interval_in_secs), "1"],
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    stdout, sterr = p.communicate()
    retcode = p.returncode
    if retcode != 0:
        raise Exception("mpstat failed with the following error: {}".format(sterr))
    ts = int(time.time() * millisecs_in_sec)
    out = stdout.decode().split("\n\n")
    return process_line(out[2], ts, my_node_id, "cpustat")


def get_libstats(node_id, map_group_id_name, client):
    rest_of_libstats = {}
    important_libstats = {}
    libstats = client.get_libstats()
    cur_time = int(time.time() * millisecs_in_sec)
    rest_of_libstats["timestamp"] = cur_time
    rest_of_libstats["cluster_node"] = node_id
    important_libstats["timestamp"] = cur_time
    important_libstats["cluster_node"] = node_id
    for stat in libstats.libstatNodes:
        group_id = stat.groupId
        if (group_id in map_group_id_name):
            group_name = map_group_id_name[group_id][0]
        else:
            group_name = ""
        key = group_name + "_" + str(stat.statType) + "_" + stat.statName
        key = key.replace("::", "_").replace(".", "_")
        field = stat.statValue.WhichOneof("StatValue")
        value = None
        if field == "vInt":
            value = stat.statValue.vInt
        elif field == "vDouble":
            value = stat.statValue.vDouble

        if stat.statName in important_libstat_stats or \
                group_name in important_libstat_groups or \
                "merge" in key.lower():
            important_libstats[key] = value
        else:
            rest_of_libstats[key] = value
    return [important_libstats], [rest_of_libstats]

def get_cgroup_stats(cg_mgr):
    if cg_mgr == None:
        return []
    cg_stats = cg_mgr.list_all_cgroups(kvstore_lookup=False)
    return cg_stats

def get_proc_stats():
    p = subprocess.Popen(["ps -Ao pid | grep -v PID | xargs -IF cat /proc/F/status"],
                        shell=True,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    stdout, sterr = p.communicate()
    retcode = p.returncode
    ts = int(time.time() * millisecs_in_sec)
    res = []
    cur_pid = {}
    for line in stdout.decode().splitlines():
        key, val = line.split(":")
        if key in cur_pid:
            cur_pid["timestamp"] = ts
            res.append(cur_pid)
            cur_pid = {}
        cur_pid[key] = val
    cur_pid["timestamp"] = ts
    res.append(cur_pid)
    return res

def get_group_id_map(client, node_id):
    try:
        map_group_id_name = client._get_group_id_map(node_id)
        return map_group_id_name
    except Exception as e:
        logger.error("Failed to get group id map with error: {}".format(e))
        return None

def list_cgroup_params():
    cgroup_params_list = list(cgroups_config_mgr.list_cgroup_params())
    return cgroup_params_list

def checksum_differs(new_checksum, old_checksum_file_path):
    if not os.path.exists(old_checksum_file_path):
        return True
    with open(old_checksum_file_path, 'r') as f:
        try:
            old_checksum = f.read()
        except:
            return True

    return new_checksum != old_checksum

def get_checksum(data):
    return hashlib.md5(data).hexdigest()

def add_cgroup_configs(node_id, cgroup_configs_dir, staging_dir):
    checksum_file_path = os.path.join(staging_dir, "cgroupConfigs{}.checksum".format(node_id))
    cg_config = json.dumps(list_cgroup_params(), sort_keys=True).encode()
    checksum = get_checksum(cg_config)

    # We only want to write out the current cgroup configs if they have been updated.
    # To detect any changes in configs, we use a checksum
    if checksum_differs(checksum, checksum_file_path):
        cg_config_file_path = os.path.join(cgroup_configs_dir,
                                           "cgroup_configs_node_{}_{}.json.gz".format(node_id, int(time.time())))
        logger.info("Detected change in cgroup configs. Writing latest configs to: {}".format(cg_config_file_path))
        with gzip.open(cg_config_file_path, 'wb') as f:
            f.write(cg_config)

        with open(checksum_file_path, "w") as f:
            f.write(checksum)


def add_cluster_configs(client, node_id, configs, cluster_configs_dir, staging_dir):
    if configs == None:
        return
    global xcalar_paging_size
    global version
    global num_nodes
    checksum_file_path = os.path.join(staging_dir, "clusterConfigs{}.checksum".format(node_id))
    cluster_info = {}
    cluster_info["node_id"] = node_id
    if version is None:
        version = client.get_version()
    if num_nodes is None:
        num_nodes = client.get_num_nodes_in_cluster()
    cluster_info["xcalar_version"] = version
    cluster_info["num_nodes_in_cluster"] = num_nodes
    cluster_info["num_cores"] = num_cores
    cluster_info["memory"] = total_ram_size
    cluster_info["swap_size"] = total_swap_size
    cluster_info["config_params"] = thrift_to_json(configs)
    if xcalar_paging_size is None:
        for param in json.loads(cluster_info["config_params"])["parameter"]:
            if param["paramName"] == "XdbLocalSerDesPath":
                if os.path.isdir(param["paramValue"]):
                    xcalar_paging_size = shutil.disk_usage(param["paramValue"]).total
                else:
                    xcalar_paging_size = -1
                break
    cluster_info["xcalar_paging_size"] = xcalar_paging_size
    cluster_info_encoded = json.dumps(cluster_info, sort_keys=True).encode()
    checksum = get_checksum(cluster_info_encoded)
    # We only want to write out the current cluster configs if they have been updated.
    # To detect any changes in configs, we use a checksum
    if checksum_differs(checksum, checksum_file_path):
        configs_file_path = os.path.join(cluster_configs_dir,
                                           "cluster_configs_node_{}_{}.json.gz".format(node_id, int(time.time())))
        logger.info("Detected change in cluster configs. Writing latest configs to: {}".format(configs_file_path))
        with gzip.open(configs_file_path, 'wb') as f:
            f.write(cluster_info_encoded)

        with open(checksum_file_path, "w") as f:
            f.write(str(checksum))


def get_configs(node_id):
    xcalar_api = XcalarApi("https://{}:{}".format(node_list[str(node_id)], port), bypass_proxy=True)
    configs_dict = {
        COLLECT_STATS: False,
        COLLECT_SYSTEM_STATS: None,
        STATS_COLLECTION_INTERVAL: None,
        COLLECT_PER_PROC_INFO:  None,
        COLLECT_MPTSTAT: None,
        COLLECT_CGROUP_STATS: None,
        CGROUPS_CONFIG: None,
        COLLECT_LIBSTATS: None,
        COLLECT_TOP_STATS: None,
        COLLECT_IO_STATS: None,
        COLLECT_CONFIG_STATS: None,
        STATS_WRITE_INTERVAL: None
    }
    try:
        configs = xcalar_api.getConfigParams()
        for param in configs.parameter:
            if param.paramName in configs_dict:
                if param.paramName == STATS_COLLECTION_INTERVAL or param.paramName == STATS_WRITE_INTERVAL:
                   configs_dict[param.paramName] = int(param.paramValue)
                else:
                    configs_dict[param.paramName] = (param.paramValue == "true")

        return (configs_dict, configs)
    except Exception as e:
        logger.error("Unable to lookup config params due to error: {}".format(str(e)))
        return configs_dict, None

def run_loop(cluster, stats_dir_path):
    # .statsMeta is hidden to the user, but contains important info such as
    # checksum files. Should not be deleted.
    staging_dir = os.path.join(stats_dir_path, ".statsMeta")
    os.makedirs(staging_dir, exist_ok=True)
    tmp_dir = os.getenv('TMPDIR', '/tmp')
    stats_tmp_dir = os.path.join(tmp_dir, ".StatsTmp")
    os.makedirs(stats_tmp_dir, exist_ok=True)
    tmp_path = os.path.join(stats_tmp_dir, "{}_node{}_stats.json".format("TMP", cluster.my_node_id))
    try:
        # old state should be cleared out
        os.remove(tmp_path)
    except Exception:
        pass
    configs_dir = os.path.join(stats_dir_path, "configs")
    cluster_configs_dir = os.path.join(configs_dir, "clusterConfigs")
    cgroups_config_dir = os.path.join(configs_dir, "cgroupConfigs")
    os.makedirs(cluster_configs_dir, exist_ok=True)
    os.makedirs(cgroups_config_dir, exist_ok=True)
    logger.info("Logging system stats to: {}".format(stats_dir_path))
    pool = ThreadPool(processes=2)
    map_group_id_name = None
    last_cluster_config_check_ts = int(time.time())
    last_stats_config_check_ts = int(time.time())
    first_iter = True
    cg_mgr = None
    configs = None
    configs_dict = {
        COLLECT_STATS: True,
        COLLECT_SYSTEM_STATS: True,
        STATS_COLLECTION_INTERVAL: 5,
        COLLECT_PER_PROC_INFO:  False,
        COLLECT_MPTSTAT: True,
        COLLECT_CGROUP_STATS: True,
        CGROUPS_CONFIG: True,
        COLLECT_LIBSTATS: True,
        COLLECT_TOP_STATS: True,
        COLLECT_IO_STATS: True,
        COLLECT_CONFIG_STATS: True,
        # We need to use a reasonable value for the stats write interval
        # since we don't want the memory payload of stats app getting
        # too large, but at the same time we want the number of files to be small.
        STATS_WRITE_INTERVAL:30
    }

    cur_sys_stats_filepath = None
    last_write_ts = int(time.time())

    client = XpuClient()
    while True:
        time_before = time.time()

        # CONFIG STATS
        # Check the stats configs every 10 secs to see if there has been any changes
        if first_iter or (int(time.time()) - last_stats_config_check_ts > stats_config_check_period):
            configs_dict_new, configs = get_configs(cluster.my_node_id)
            for key in configs_dict.keys():
                if configs_dict_new[key] != None and configs_dict_new[key] != configs_dict[key]:
                    logger.info("{} has been changed to {}".format(key, configs_dict_new[key]))
                    configs_dict[key] = configs_dict_new[key]
            last_stats_config_check_ts = int(time.time())

        # Update the cluster configs every minute if they have been changed
        if first_iter or (int(time.time()) - last_cluster_config_check_ts > cluster_config_check_period):
            if configs_dict[COLLECT_STATS] and configs_dict[COLLECT_CONFIG_STATS]:
                try:
                    add_cluster_configs(client, cluster.my_node_id, configs, cluster_configs_dir, staging_dir)
                except Exception as e:
                    logger.error("Add cluster configs failed with error: {}".format(e))

                if configs_dict[CGROUPS_CONFIG]:
                    try:
                        add_cgroup_configs(cluster.my_node_id, cgroups_config_dir, staging_dir)
                    except Exception as e:
                        logger.error("Add cgroup configs failed with error: {}".format(e))
            last_cluster_config_check_ts = int(time.time())

        # SYSTEM STATS
        if not configs_dict[COLLECT_STATS] or not configs_dict[COLLECT_SYSTEM_STATS]:
            # Sleep 10 seconds before checking stats configs again
            time.sleep(stats_off_check_interval)
            continue

        iostat_async_result = None
        cpu_stats_async = None
        if configs_dict[COLLECT_IO_STATS]:
            # We want to get cpu stats from iostats call if mpstats per-core stats collection has been switched off
            iostat_async_result = pool.apply_async(get_io_stats, (cluster.my_node_id,
                                                                  configs_dict[STATS_COLLECTION_INTERVAL],
                                                                  not configs_dict[COLLECT_MPTSTAT]))
        if configs_dict[COLLECT_MPTSTAT]:
            cpu_stats_async = pool.apply_async(get_cpu_stats, (cluster.my_node_id,
                                                               configs_dict[STATS_COLLECTION_INTERVAL], ))

        top_stats = []
        cg_stats = {}
        important_libstats, rest_of_libstats = [], []
        proc_stats = []

        if configs_dict[COLLECT_TOP_STATS]:
            try:
                top_stats = GetTopStats()._get_top_stats_from_backend(client, get_from_all_nodes=False, is_json_format=True)
            except Exception as e:
                logger.error("get_stats failed with error: {}".format(e))

        if configs_dict[COLLECT_LIBSTATS]:
            if map_group_id_name is None:
                map_group_id_name = get_group_id_map(client, cluster.my_node_id)

            try:
                important_libstats, rest_of_libstats = get_libstats(cluster.my_node_id, map_group_id_name, client)
            except Exception as e:
                logger.error("get_libstats failed with error: {}".format(e))

        if  configs_dict[CGROUPS_CONFIG] and configs_dict[COLLECT_CGROUP_STATS]:
            if cg_mgr is None:
                try:
                    cg_mgr = cgroups_base_mgr.CgroupMgr()
                except Exception as e:
                    logger.error("Failed to initialize CgroupMgr. Error: {}".format(str(e)))
            try:
                cg_stats = get_cgroup_stats(cg_mgr)
            except Exception as e:
                logger.error("get_cgroup_stats failed with error: {}".format(e))

        if configs_dict[COLLECT_PER_PROC_INFO]:
            try:
                proc_stats = get_proc_stats()
            except Exception as e:
                logger.error("get_proc_stats failed with error: {}".format(e))

        cpu_stats = []
        iostats = []
        if configs_dict[COLLECT_IO_STATS]:
            iostats, cpu_stats = iostat_async_result.get()
        if configs_dict[COLLECT_MPTSTAT]:
            cpu_stats = cpu_stats_async.get()

        sys_stats = {}
        sys_stats["cpustats"] = cpu_stats
        sys_stats["xcalar_internal_stats"] = important_libstats
        sys_stats["system_stats"] = top_stats
        sys_stats["cgroup_stats"] = cg_stats
        sys_stats["iostats"] = iostats
        sys_stats["libstats"] = rest_of_libstats
        sys_stats["proc_stats"] = proc_stats

        ts = int(time.time())

        # Update stats in jsonline format in a temporary file every loop iteration
        # This is so that we don't have the system stats app in-memory payload go beyond a single iteration worth of stats
        with open(tmp_path, 'a+') as f:
            json_data = json.dumps(sys_stats)
            f.write(json_data + "\n")

        sys_stats = {}

        # Every STATS_WRITE_INTERVAL secs, we compress and write file out to permenant shared root location
        # By combining multiple iterations of stats in one file, we will have a few large files instead of
        # multiple small files, which leads to space saving and greater ease with post processing.
        if ts - last_write_ts > configs_dict[STATS_WRITE_INTERVAL]:
            today = datetime.fromtimestamp(ts)
            directory = os.path.join(stats_dir_path, "systemStats/", "{}-{}-{}/".format(today.year,
                        today.month, today.day), "{}/".format(today.hour))
            os.makedirs(directory, exist_ok=True)

            final_sys_stats_filepath = os.path.join(directory, "{}_node{}_stats.json.gz".format(ts, cluster.my_node_id))

            staging_path = os.path.join(staging_dir, "{}_node{}_stats.json.gz".format("TMP", cluster.my_node_id))

            # Create temp gzip file and do an atomic "replace" to move this over to the target stats_dir_path location.
            # This avoids empty or partially written files being created in the target stats_dir_path if the app is
            # shot down in the middle of a write to the temporary file.
            with open(tmp_path, "rb") as f:
                data = f.read()
            os.remove(tmp_path)
            with gzip.open(staging_path, "wb") as f:
                f.write(data)

            os.replace(staging_path, final_sys_stats_filepath)
            data = None
            last_write_ts = ts

        first_iter = False

        time_taken = (time.time() - time_before)
        if (time_taken > configs_dict[STATS_COLLECTION_INTERVAL] + 10):
            logger.info("WARNING - A single iteration of system stats app took {} secs, which "
                        "is much longer than the expected {} secs.".format(time_taken, configs_dict[STATS_COLLECTION_INTERVAL]))
        time_to_sleep = configs_dict[STATS_COLLECTION_INTERVAL] - time_taken
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)

def main(in_blob):
    in_obj = json.loads(in_blob)
    stats_dir_path = in_obj["stats_dir_path"]
    cluster = get_running_cluster()

    if cluster.is_local_master():
        if cluster.my_node_id == 0:
            # spin of async thread that moves job stats files from node local storage to shared root
            job_stats_thread = threading.Thread(target=job_stats_routine, daemon=True, args=((in_obj["log_path"], )))
            job_stats_thread.start()

            # spin of async thread that handles stats rotation
            stats_rotation_thread = threading.Thread(target=no_fail_delete_files_routine, daemon=True, args=((stats_dir_path, )))
            stats_rotation_thread.start()

        logger.info("Sucessfully started system stats app with "
                    "groupId: {} and pid: {}".format(cluster.unique_run_id, os.getpid()))
        ret = run_loop(cluster, stats_dir_path)
        return ret
