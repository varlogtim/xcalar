# Copyright 2019-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# This job is launched at the end of a dataflow and dumps job stats to the file system

import json
import gzip
import os
import sys
import logging
import jsonpickle
import time

from xcalar.external.client import Client
import xcalar.container.context as ctx

from socket import gethostname
import multiprocessing

# Dynamic Constants
num_cores = multiprocessing.cpu_count()

# Set up logging
logger = logging.getLogger("xcalar")
logger.setLevel(logging.INFO)
# This module gets loaded multiple times. The global variables do not
# get cleared between loads. We need something in place to prevent
# multiple handlers from being added to the logger.
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stderr
    log_handler = logging.StreamHandler(sys.stderr)
    this_xpu_id = ctx.get_xpu_id()
    this_node_id = ctx.get_node_id(this_xpu_id)
    num_nodes = ctx.get_node_count()

    formatter = logging.Formatter(
        '%(asctime)s - Pid {} Node {} - Dataflow Stats App - %(levelname)s - %(message)s'
        .format(os.getpid(), this_node_id))
    log_handler.setFormatter(formatter)

    logger.addHandler(log_handler)

    logger.debug(
        "Dataflow Stats App initialized; nodeId:{}, numNodes:{}, hostname:{}, numCores:{}"
        .format(this_node_id, num_nodes, gethostname(), num_cores))

def update_history(client, in_obj):
    xcalar_logs_path = in_obj["logPath"]
    tmp_dir_root = os.path.join(xcalar_logs_path, ".statsStagingDir")
    os.makedirs(tmp_dir_root, exist_ok=True)
    tmp_dir_staging = os.path.join(tmp_dir_root, "tmpJobMeta")
    os.makedirs(tmp_dir_staging, exist_ok=True)
    tmp_dir_qs_output = os.path.join(tmp_dir_root, "queryStateOutput")
    os.makedirs(tmp_dir_qs_output, exist_ok=True)
    tmp_dir_job_meta = os.path.join(tmp_dir_root, "jobMeta")
    os.makedirs(tmp_dir_job_meta, exist_ok=True)

    job_name = in_obj["jobName"]
    file_name = "{}_{}.json.gz".format(int(time.time()), job_name)
    qs = client._legacy_xcalar_api.queryState(in_obj["jobName"])
    qs_output_path = os.path.join(tmp_dir_qs_output, file_name)
    with gzip.open(qs_output_path, 'wb') as f:
        f.write(jsonpickle.encode(qs).encode())

    in_obj["qsOutputLocation"] = qs_output_path

    staging_path = os.path.join(tmp_dir_staging, file_name)
    with gzip.open(staging_path, 'wb') as f:
        f.write(json.dumps(in_obj).encode())

    final_path = os.path.join(tmp_dir_job_meta, file_name)
    os.rename(staging_path, final_path)

def main(in_blob):
    in_obj = json.loads(in_blob)
    client = Client(bypass_proxy=True)
    logger.info("Starting node local write for job: {}".format(in_obj["jobName"]))
    update_history(client, in_obj)
    logger.info("Finished node local write for job: {}".format(in_obj["jobName"]))
    return None
