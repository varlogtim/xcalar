# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import gzip
import json
import subprocess
import tempfile
import time
import uuid
import random
import sys
import shutil

import pytest
import os
import signal

from xcalar.compute.util.Qa import XcalarQaDatasetPath
from xcalar.compute.util.config import detect_config
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.compute.util.tests_util import upload_stats_test_util
from xcalar.compute.util.utils import XcUtil
from datetime import datetime
import tarfile


@pytest.fixture(scope="module")
def stats_tmp_dir():
    tmp_dir = os.getenv('TMPDIR', '/tmp')
    stats_tmp_dir = tempfile.TemporaryDirectory(
        prefix="StatsTest-", dir=tmp_dir)
    yield stats_tmp_dir.name
    stats_tmp_dir.cleanup()


@pytest.fixture(scope="module", autouse=True)
def setup(client):
    client.set_config_param("StatsWriteInterval", 1, False)
    client.set_config_param("StatsCollectionInterval", 1, False)
    # Clean out the stats dir before the test runs
    stats_root_dir = os.path.join(detect_config().xcalar_root_path,
                                "DataflowStatsHistory")
    for dir_type in ["systemStats", "jobStats"]:
        # This will cause the system stats app to die because it will fail to write
        # files to the deleted dir. But this is fine because the app should get
        # re-launched straight away.
        shutil.rmtree(os.path.join(stats_root_dir, dir_type), ignore_errors=True)
    # To be conservative, allow some extra time for system stats app to relaunch
    time.sleep(10)
    yield
    client.set_config_param("StatsWriteInterval", 30, True)
    client.set_config_param("StatsCollectionInterval", 5, True)


def extract_system_stats(system_stats_path):
    final_stats = {
        "cpustats": [],
        "xcalar_internal_stats": [],
        "system_stats": [],
        "iostats": [],
        "libstats": [],
        "cgroup_stats": []
    }
    for r, d, f in os.walk(system_stats_path):
        for file in f:
            stats_file = os.path.join(r, file)
            with gzip.open(stats_file, "rb") as f:
                data = f.read()
                for line in data.splitlines():
                    stats = json.loads(line)
                    final_stats["system_stats"].extend(stats["system_stats"])
                    final_stats["iostats"].extend(stats["iostats"])
                    final_stats["cpustats"].extend(stats["cpustats"])
                    final_stats["libstats"].extend(stats["libstats"])
                    final_stats["cgroup_stats"].extend(stats["cgroup_stats"])
                    final_stats["xcalar_internal_stats"].extend(
                        stats["xcalar_internal_stats"])
            os.remove(stats_file)

    return final_stats


def verify_gather_stats(query_name, job_dir, date, stats_tmp_dir):
    # sleep for 10 seconds to give time for some system stats to be written out
    # This is just to be conservative to avoid test failures.
    time.sleep(10)
    tar_file_name = str(uuid.uuid4())
    cmd = "{} {}/scripts/gather_stats.py".format(sys.executable, os.environ["XLRDIR"])
    cmd += " --startDate " + date
    cmd += " --jobNamePattern " + query_name
    cmd += " --tarFileName " + tar_file_name
    resp = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, sterr = resp.stdout, resp.stderr
    retcode = resp.returncode
    if retcode != 0:
        raise Exception(
            "gather stats failed with the following error: {}".format(sterr))

    tarfile_name = '{}.tar.gz'.format(tar_file_name)
    tmp_dir = tempfile.TemporaryDirectory(dir=stats_tmp_dir)

    tar = tarfile.open(tarfile_name)
    tar.extractall(path=tmp_dir.name)
    tar.close()

    job_stats_output_file = os.path.join(tmp_dir.name, tar_file_name,
                                         "jobStats", date,
                                         "{}.json.gz".format(job_dir))
    with gzip.open(job_stats_output_file, "rb") as f:
        data = f.read()
        stats = json.loads(data)

        # just some ssanity checks
        assert stats["job_id"] == query_name
        time_elapsed = stats["total_time_elapsed_millisecs"] // 1000
        num_samples = time_elapsed

    system_stats_dir = os.path.join(tmp_dir.name, tar_file_name, "systemStats")
    assert len(os.listdir(system_stats_dir)) >= 1

    final_stats = extract_system_stats(system_stats_dir)

    for key, val in final_stats.items():
        assert len(val) > num_samples

    # remove the un-tarred directory from /tmp/statsTests
    tmp_dir.cleanup()
    # remove the tarball file that was also created
    os.remove("{}.tar.gz".format(tar_file_name))


@pytest.mark.parametrize(
    "file_path, dataflow_name, expected_rows, stats_config",
    [
    # Simple dataflows in the same workbook - first two end with linkout
    # optimized
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF1", 34, True),
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF2", 34, True),
    # this one ends with export optimized
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF3", 34, True),
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF1", 34, False),
    ])
def test_dataflow_stats(client, file_path, dataflow_name, expected_rows,
                        stats_config, stats_tmp_dir):
    workbook_path = XcalarQaDatasetPath + file_path
    test_wb_name = "TestExecuteWorkbook_{}".format(int(time.time()))
    test_final_table = "TestExecuteFinalTable"
    query_name = "TestExecuteQuery_{}_{}".format(dataflow_name,
                                                 int(time.time()))
    client.set_config_param("CollectStats", stats_config, False)

    with open(workbook_path, 'rb') as wb_file:
        wb_content = wb_file.read()

        workbook = client.upload_workbook(test_wb_name, wb_content)
        dataflow = workbook.get_dataflow(dataflow_name=dataflow_name)
        dates_to_check = []
        now = datetime.now()
        date_before = now.strftime("%Y-%m-%d")
        dates_to_check.append(date_before)

        sess = workbook.activate()
        # Dataflows can only be executed optimized as there are no loaded
        # datasets for the unoptimized version to find.
        qname = sess.execute_dataflow(
            dataflow,
            optimized=True,
            table_name=test_final_table,
            query_name=query_name,
            is_async=False,
            parallel_operations=random.choice([True, False]))

        now = datetime.now()
        date_after = now.strftime("%Y-%m-%d")

        check_both_days = False
        if date_after != date_before:
            dates_to_check.append(date_after)

        table = sess.get_table(table_name=test_final_table)
        rows = len(list(table.records()))
        assert rows == expected_rows

        stat_dir = os.path.join(detect_config().xcalar_root_path,
                                "DataflowStatsHistory")

        num_retries = 10
        for _ in range(num_retries):
            file_seen = False
            for date in dates_to_check:
                if file_seen:
                    break
                job_stats_dir = os.path.join(stat_dir, "jobStats", date)
                for root, dir_names, file in os.walk(job_stats_dir):
                    for job_dir in dir_names:
                        if file_seen:
                            break
                        if qname not in job_dir:
                            continue

                        file_path = os.path.join(root, job_dir,
                                                 "job_stats.json.gz")
                        for retry in range(num_retries):
                            try:
                                with gzip.open(file_path, "rb") as f:
                                    data = f.read()
                                    job_stats = json.loads(data)
                                    file_seen = True
                                    break
                            except Exception as e:
                                print("Failed read {} due to: {}. Retrying..".format(file_path, str(e)))
                                # Sleep and retry since the job stats took some time to be written out
                                time.sleep(2)

                        if file_seen:
                            assert job_stats["job_id"] == qname
                            assert job_stats[
                                "session_name"] == test_wb_name
                            assert job_stats[
                                "number_completed_operations"] > 2
                            assert len(job_stats["nodes"]) > 1
                            verify_gather_stats(
                                qname, job_dir, date, stats_tmp_dir)
                            break

            if file_seen or not stats_config:
                break
            else:
                # Sleep and retry (just in case the job stats took some time to be written out)
                time.sleep(1)

        if not stats_config:
            assert not file_seen
        else:
            assert file_seen

        table.drop()

        # clean up
        workbook.inactivate()
        workbook.delete()
        client.set_config_param("CollectStats", True, True)

@pytest.mark.parametrize(
    "file_path, dataflow_name",
    [
    # Simple dataflows in the same workbook - first two end with linkout
    # optimized
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF1"),
    ])
def test_upload_stats(client, file_path, dataflow_name):
    workbook_path = XcalarQaDatasetPath + file_path
    xcalar_api = XcalarApi()
    upload_stats_test_util(client, xcalar_api, workbook_path, dataflow_name, shell=None)

def verify_gather_system_stats(client, date, start_time, end_time, ts_before,
                               stats_tmp_dir):
    # We will try 20 times, with a 20 second wait in between. This is because
    # sometimes it takes longer than expected for the system stats app to begin
    # writing the stats out, and we don't want the test to fail for this reason.
    num_retries = 20
    for retry in range(num_retries):
        test_passed = False
        tar_file_name = str(uuid.uuid4())
        print(tar_file_name)
        cmd = "{} {}/scripts/gather_stats.py".format(sys.executable, os.environ["XLRDIR"])
        cmd += " --systemStatsOnly "
        cmd += " --startDate " + date
        cmd += " --startTime " + start_time
        cmd += " --endTime " + end_time
        cmd += " --tarFileName " + tar_file_name
        resp = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, sterr = resp.stdout, resp.stderr
        retcode = resp.returncode
        if retcode != 0:
            raise Exception("gather stats failed with the following error: {}".
                            format(sterr))

        tarfile_name = '{}.tar.gz'.format(tar_file_name)
        tmp_dir = tempfile.TemporaryDirectory(dir=stats_tmp_dir)
        tar = tarfile.open(tarfile_name)
        tar.extractall(path=tmp_dir.name)
        tar.close()

        system_stats_dir = os.path.join(tmp_dir.name, tar_file_name,
                                        "systemStats")
        assert len(os.listdir(system_stats_dir)) >= 1
        final_stats = extract_system_stats(system_stats_dir)

        # just some sanity checks
        files_seen_since_ts_before = 0
        for cur_stat in final_stats["cpustats"]:
            ts = cur_stat["timestamp"]
            if (ts > ts_before):
                files_seen_since_ts_before += 1

        # We expect at least num_nodes files to be writen since ts_before.
        # This is because there is one stats daemon per node.
        # However, to make sure the app did not die after the first write,
        # we will use 2*num_nodes as the sanity check
        num_nodes = client.get_num_nodes_in_cluster()
        num_samples = int(2 * num_nodes)
        # max_files is the max number of files that could be gathered
        num_samples_max = 2 * 3600 * num_nodes

        test_passed = (len(final_stats["cpustats"]) >= num_samples
                       and len(final_stats["system_stats"]) <= num_samples_max
                       and
                       len(final_stats["xcalar_internal_stats"]) >= num_samples
                       and len(final_stats["iostats"]) >= num_samples
                       and len(final_stats["system_stats"]) > num_samples
                       and len(final_stats["libstats"]) > num_samples
                       and len(final_stats["cgroup_stats"]) > num_samples
                       and files_seen_since_ts_before >= num_samples)

        cgs_on = None
        if 'container' in os.environ:
            cgs_on = False
        else:
            configs = client.get_config_params(param_names=["Cgroups"])
            cgs_on = (configs[0]["param_value"] == "true")

        assert cgs_on is not None
        if cgs_on:
            test_passed = (test_passed
                            and len(final_stats["cgroup_stats"]["xpu_cgroups"]["memory"]) > num_samples
                            and len(final_stats["cgroup_stats"]["xpu_cgroups"]["cpu"]) > num_samples
                            and len(final_stats["cgroup_stats"]["xpu_cgroups"]["cpuacct"]) > num_samples
                            and len(final_stats["cgroup_stats"]["usrnode_cgroups"]["memory"]) > num_samples
                            and len(final_stats["cgroup_stats"]["usrnode_cgroups"]["cpu"]) > num_samples
                            and len(final_stats["cgroup_stats"]["usrnode_cgroups"]["cpuacct"]) > num_samples)

        # remove the un-tarred directory from /tmp/statTests/
        tmp_dir.cleanup()
        # remove the tarball file that was also created
        os.remove("{}.tar.gz".format(tar_file_name))
        if test_passed:
            break
        if retry < num_retries - 1:
            print("Test failed. Will re-try after waiting 20 seconds.")
            time.sleep(20)
    assert test_passed


# This must be the last pytest to run in this file.
def test_system_stats_app_relaunch(client, stats_tmp_dir):
    past_pids = []
    # The loop count is 6 below to force app re-launch code to go through
    # a sleep which kicks in only after 5 retries
    for i in range(6):
        current_pids = []
        print("Killing all childnodes")
        for pid in os.popen("pgrep childnode"):
            assert pid not in past_pids
            with XcUtil.ignored(Exception):
                os.kill(int(pid), signal.SIGKILL)
            current_pids.append(pid)
        past_pids = current_pids

        ts_before = int(time.time())

        # allow some time for system stats app to be relaunched
        time.sleep(10)

        now = datetime.fromtimestamp(ts_before)
        date = "{}-{}-{}".format(now.year, now.month, now.day)
        start_time = "{}:{}".format(now.hour, now.minute)
        # We add the 420 below because in the verify_gather_stats() we loop up to
        # 20 times with a 20 sec gap in between (so we need to take into account
        # those potential 400 extra seconds in the end_time)
        ts_after_8_min = ts_before + (60 + 420)
        now_plus_8_min = datetime.fromtimestamp(ts_after_8_min)
        date_plus_8_min = "{}-{}-{}".format(
            now_plus_8_min.year, now_plus_8_min.month, now_plus_8_min.day)
        end_time = "{}:{}".format(now_plus_8_min.hour, now.minute)

        # currently, gather_stats script cannot take multiple dates. So just skip if that is the case
        if date == date_plus_8_min:
            verify_gather_system_stats(client, date, start_time, end_time,
                                       ts_before, stats_tmp_dir)
