#!/usr/bin/env python3.6
# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import sys
from datetime import datetime, timedelta
import argparse
import os
import gzip
import json
import time
import tarfile
import xcalar.compute.util.config as config
from xcalar.compute.util.utils import module_exists
try:
    import tqdm
except ImportError:
    pass

nanosecsInSecs = 1000000000
microsecsInSecs = 1000000

parser = argparse.ArgumentParser()
parser.add_argument(
    "--startDate",
    required=True,
    help="Date (in GMT) to start gathering stats from."
    " Must be of form YYYY-MM-DD")
parser.add_argument(
    "--numDays",
    default=0,
    help="Number of days prior to startDate to gather stats from."
    " Default is 0 days (which means only stats from startDate"
    " will be collected).")

# Use this flag if you want to only get system stats between a certain time period on the startDate.
# startTime and endTime must be provided
# FOR INTERNAL USE ONLY
parser.add_argument(
    "--systemStatsOnly", action='store_true', help=argparse.SUPPRESS)

# Start time to collect system stats. Only applicable if systemStatsOnly flag is used.
# Must be of the form HH:MM.
# FOR INTERNAL USE ONLY
parser.add_argument("--startTime", default=None, help=argparse.SUPPRESS)

# End time to stop collecting system stats. Only applicable if systemStatsOnly flag is used.
# Must be of the form HH:MM.
# FOR INTERNAL USE ONLY
parser.add_argument("--endTime", default=None, help=argparse.SUPPRESS)

# FOR INTERNAL TESTING USE ONLY. DO NOT USE
parser.add_argument("--tarFileName", default=None, help=argparse.SUPPRESS)
# FOR INTERNAL USE ONLY
parser.add_argument("--jobNamePattern", default="", help=argparse.SUPPRESS)
args = parser.parse_args()

def parse_file_and_set_xce_config(file_path):
    with open(file_path, 'r') as default_file:
        for line in default_file:
            if line.startswith("#") or line.startswith("//"):
                continue
            try:
                env_var, val = line.split("=")
            except:
                # The split may fail if there is no "=" in the line.
                # So just continue if this is the case since we don't care about that line anyways.
                continue
            if env_var == "XCE_CONFIG":
                os.environ[env_var] = val.strip('\n')
                print("XCE_CONFIG path set to: {}".format(val.strip('\n')))


# Set the XCE_CONFIG environment variable if not already set
if os.environ.get('XCE_CONFIG') is None:
    real_path = os.path.dirname(os.path.realpath(__file__))
    install_path = os.path.dirname(os.path.dirname(os.path.dirname(real_path)))
    path_to_check = os.path.join(install_path, 'etc/default/xcalar')
    if os.access('/etc/default/xcalar', os.R_OK):
        parse_file_and_set_xce_config('/etc/default/xcalar')
    elif os.access(path_to_check, os.R_OK):
        parse_file_and_set_xce_config(path_to_check)

xcalar_root = config.detect_config().xcalar_root_path

stats_dir_path = os.path.join(xcalar_root, "DataflowStatsHistory")
job_name_pattern = args.jobNamePattern
start_date = args.startDate
num_days = int(args.numDays)
sys_start_time = args.startTime
sys_end_time = args.endTime
job_stats_folder = "jobStats"
system_stats_folder = "systemStats"
configs_folder = "configs"
tqdm_exists = module_exists("tqdm")
if not tqdm_exists:
    sys.stderr.write("WARNING!!! Tqdm does not exist! Please Install tqdm for progress bars. "
          "Continuing without tqdm progress bars...\n")
    sys.stderr.flush()

def get_job_start_and_end_time(file_path):
    with gzip.open(file_path, "rb") as f:
        data = f.read()
        t = json.loads(data)
    start_time = t["job_start_timestamp_microsecs"] // microsecsInSecs
    end_time = t["job_end_timestamp_microsecs"] // microsecsInSecs
    return (t, start_time, end_time)


def tar_single_hour_of_stats(ts, final_tarball, tar_root):
    # A hours worth of stats is being tarred up and added to
    # the final tarball.
    tm = datetime.fromtimestamp(ts * 3600)
    tm_dir = os.path.join("{}-{}-{}/".format(tm.year, tm.month, tm.day), "{}/".format(tm.hour))
    directory = os.path.join(stats_dir_path, system_stats_folder, tm_dir)
    if not os.path.isdir(directory): return
    final_tarball.add(directory, arcname=os.path.join(tar_root, system_stats_folder, tm_dir))

def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def get_system_stats_between(start_time_secs, end_time_secs, final_tarball, tar_root):
    """
    Given the start and end time, this function searches the systemStats directory to get the relevant
    system stats files in this time period.
    """
    start_time_hours = start_time_secs // 3600
    end_time_hours = end_time_secs // 3600
    if tqdm_exists:
        pbar = tqdm.tqdm(total=end_time_hours - start_time_hours)
    else:
        progress_count = 0
        progress_total = end_time_hours - start_time_hours + 1
    for ts in range(start_time_hours, end_time_hours + 1):
        tar_single_hour_of_stats(ts, final_tarball, tar_root)
        if tqdm_exists:
            pbar.update(1)
        else:
            progress_count += 1
            progress(progress_count, progress_total, status='Adding system stats files')

    if not tqdm_exists:
        # Printing a newline here to avoid the next print from overriding progress bar
        print()


def create_tarball(root_folder, start, end):
    # We are adding all the jobStats and systemStat files into a single compressed tarball.
    if args.tarFileName:
        tar_filename = args.tarFileName
    else:
        tar_filename = 'xcalarGatherStats{}-{}'.format(start, end)
    file_path = "{}.tar.gz".format(tar_filename)
    print("Creating tarball...")
    with tarfile.open(file_path, "w:gz") as tar:
        tar.add(root_folder.name, arcname=os.path.basename(tar_filename))
    root_folder.cleanup()
    print("tmp dir {} successfully deleted.".format(root_folder.name))
    print("All compressed files added to single tarball: {}".format(file_path))


def collect_job_stats():
    """
    This function searches the jobStats directory to find all the job stats files corresponding to
    the date and jobNamePattern provided.
    Given the start time of the first job and end time of the last job for the given dates, it then 
    searches the systemStats directory to get the relevant system stats files and dumps this into 
    the systemStats dir.
    The script dumps the all the job stats and system stats files into one tarball.
    """
    if args.tarFileName:
        tar_root = args.tarFileName
    else:
        tar_root = 'xcalarGatherStats_{}_{}'.format(start_date, num_days)
    tar_filename = "{}.tar.gz".format(tar_root)
    final_tarball = tarfile.open(tar_filename, "w:gz")

    config_directory = os.path.join(stats_dir_path, configs_folder)
    if os.path.isdir(config_directory):
        final_tarball.add(config_directory, arcname=os.path.join(tar_root, configs_folder))

    dt_object_start = datetime.strptime(start_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    for i in range(num_days + 1):
        min_start = sys.maxsize
        max_end = 0
        date_string = dt_object_start.strftime("%Y-%m-%d")
        directory = os.path.join(stats_dir_path, "jobStats", date_string)
        if not os.path.isdir(directory):
            print("Stats directory {} does not exist".format(directory))
            dt_object_start -= delta
            continue
        print("Gathering job stats from date: {}".format(date_string))
        size_of_dir = sum([len(files) for r, d, files in os.walk(directory)])
        if tqdm_exists:
            pbar = tqdm.tqdm(total=size_of_dir)
        else:
            progress_count = 0
        for r, d, f in os.walk(directory):
            for job_dir in d:
                if tqdm_exists:
                    pbar.update(1)
                else:
                    progress_count += 1
                    progress(progress_count, size_of_dir, status='Adding job stats files')
                if job_name_pattern in job_dir:
                    file_path = os.path.join(r, job_dir, "job_stats.json.gz")
                    if os.path.exists(file_path):
                        # extract the qs_output and job_start_time + job_end_time
                        try:
                            qs_output, start_time_secs, end_time_secs = get_job_start_and_end_time(
                                file_path)
                        except Exception as e:
                            sys.stderr.write("Failed to add {}. Error: {}\n".format(
                                os.path.join(r, job_dir, "job_stats.json.gz"),
                                str(e)))
                            sys.stderr.flush()
                            continue

                        min_start = min(min_start, start_time_secs)
                        max_end = max(max_end, end_time_secs)
                        final_tarball.add(file_path, arcname=os.path.join(
                            tar_root,
                            job_stats_folder,
                            date_string,
                            "{}.json.gz".format(job_dir)))
        if tqdm_exists:
            pbar.close()
        else:
            # Printing a newline here to avoid the next print from overriding progress bar
            print()
        dt_object_start -= delta

        print("Collecting relevant system stats for jobs run on {}...".format(date_string))
        get_system_stats_between(min_start, max_end, final_tarball, tar_root)
    final_tarball.close()
    print("All compressed files added to single tarball: {}".format(tar_filename))

def collect_system_stats():
    """
    This function writes the system stats for a specific time period into one file.
    """
    if (sys_start_time == None):
        raise Exception("startTime must be provided")
    if (sys_end_time == None):
        raise Exception("endTime must be provided")

    dt_start_string = "{} {}".format(start_date, sys_start_time)
    dt_end_string = "{} {}".format(start_date, sys_end_time)

    dt_object_start = datetime.strptime(dt_start_string, "%Y-%m-%d %H:%M")
    dt_object_end = datetime.strptime(dt_end_string, "%Y-%m-%d %H:%M")

    start_time = int(time.mktime(dt_object_start.timetuple()))
    end_time = int(time.mktime(dt_object_end.timetuple()))

    if args.tarFileName:
        tar_root = args.tarFileName
    else:
        tar_root = 'xcalarGatherStats_{}_{}_{}'.format(start_date, start_time, end_time)
    tar_filename = "{}.tar.gz".format(tar_root)
    final_tarball = tarfile.open(tar_filename, "w:gz")

    print("Collecting relevant system stats...")
    get_system_stats_between(start_time, end_time, final_tarball, tar_root)
    final_tarball.close()
    print("All compressed files added to single tarball: {}".format(tar_filename))

if not args.systemStatsOnly:
    collect_job_stats()
else:
    before = time.time()
    collect_system_stats()
    time_taken = time.time() - before
    print("Time taken to gather system stats: {}".format(time_taken))
