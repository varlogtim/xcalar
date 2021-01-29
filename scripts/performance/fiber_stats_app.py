import json
import os
import time
import csv
import codecs
from google.protobuf.json_format import MessageToDict

import xcalar.container.context as ctx
import xcalar.container.xpu_host as xpu_host
from xcalar.container.cluster import get_running_cluster

# Set up logging
logger = ctx.get_logger()

Utf8Encoder = codecs.getwriter("utf-8")
Collect_fiber_stats_interval = 59

cluster = get_running_cluster()


def write_system_fiber_stats(fp=None):
    idx = 1
    while True:
        histo_proto = xpu_host.fetch_parent_perf_info()
        dict_obj = MessageToDict(histo_proto)
        for hist in dict_obj['histograms']:
            for item in hist['items']:
                item["run"] = idx
                item["node_id"] = cluster.my_node_id
                yield item
        idx += 1
        if fp:
            fp.flush()
        time.sleep(Collect_fiber_stats_interval)


def export_sys_perf_stats(input_dict):
    column_map = {
        "run": "run",
        "node_id": "node_id",
        "name": "schedulable_name",
        "count": "num_fibers",
        "meanDurationUs": "mean_duration_usecs",
        "durationStddev": "duration_std_deviation_usecs",
        "duration95thUs": "duration_95th_usecs",
        "duration99thUs": "duration_99th_usecs",
        "meanSuspensions": "mean_suspensions",
        "meanSuspendedTimeUs": "mean_suspended_time_usecs"
    }
    if not cluster.is_local_master():
        return
    dir_path = input_dict.get("dir_path", "/tmp")
    file_name = input_dict.get(
        "file_prefix",
        "") + f"perf_stats_{cluster.my_node_id}_{ctx.get_txn_id()}.csv"
    file_path = os.path.join(dir_path, file_name)
    logger.info(f"Writing stats to {file_path}")
    with open(os.path.join(dir_path, file_name), 'wb') as fp:
        utf8_file = Utf8Encoder(fp)
        writer = csv.DictWriter(
            utf8_file,
            column_map.keys(),
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore")

        writer.writerow(column_map)
        writer.writerows(write_system_fiber_stats(utf8_file))
    logger.info("Done exporting histograms of runtime stats")


def main(input_json_str):
    input_json = json.loads(input_json_str)
    export_sys_perf_stats(input_json)
