#!/usr/bin/env python3

# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

description = """
This tool creates workbook snapshots. An admin can add this script to crontab
for periodic execution on a schedule or it can be executed as a one-time
operation. Old snapshots will be removed automatically based on user settings.

After the snapshots are created, one can list available snapshots and
restore the selected workbook using this tool.

You always need to run this tool with Python3 provided by Xcalar. The path is
$XCALAR_INSTALL_DIR/opt/xcalar/bin/python3, in case your environmental python3
is not here. You need to be user xcalar to run this python3.

Examples:
Create a snapshot immediately, any snapshot older than the most recent 20
snapshots will be deleted immediately.
python3 WorkbookSnapshot.py -n 20

List available snapshots for user user1
python3 WorkbookSnapshot.py -l user1

sample output:
0: Tue Jul 24 2018 13:45:22
1: Tue Jul 24 2018 13:45:24
2: Tue Jul 24 2018 13:45:25
3: Tue Jul 24 2018 13:45:26
4: Tue Jul 24 2018 13:45:27

Restore the third oldest snapshot of workbook wb1 for user user1
python3 WorkbookSnapshot.py -r user1 wb1 2

Then it will restore snapshot "2: 2018-07-19T17:13:51"

Instruction for crontab:
You should add crontab for user xcalar. You need to use full path to Python3
provided by Xcalar. The full path is "$XCALAR_INSTALL_DIR/opt/xcalar/bin/python3".
An example of crontab entry to create snapshot every 15 minutes:
*/15 * * * * $XCALAR_INSTALL_DIR/opt/xcalar/bin/python3 /path/to/WorkbookSnapshot.py

If there are many users, with many workbooks, the crontab frequency should not
be too high. You can monitor the time it takes to run the tool from logs.

The log file is in the snapshots' directory. You may consider using logrotate
for the log file in case it grows too fast.
"""

# The functionality in this script should eventually be integrated into XD so
# that the same functionality can be leveraged by a XD admin using the UI.

import sys
import os
import argparse
import re
import time
import shutil
import datetime
import signal
import logging

new_sdk = True
try:
    from xcalar.external.client import Client
    from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
except ModuleNotFoundError:
    from xcalar.compute.api.XcalarApi import XcalarApi
    from xcalar.compute.api.Session import Session
    from xcalar.compute.api.XcalarApi import XcalarApiStatusException
    new_sdk = False


def get_path():
    if os.path.isdir("/var/opt/xcalar"):
        return "/var/opt/xcalar"

    root_dir = '/'.join(os.__file__.split('/')[:-5])
    xce_config = root_dir + "/etc/xcalar/default.cfg"
    with open(xce_config, 'r') as f:
        re_pattern = "Constants\.XcalarRootCompletePath=(.+)\n"
        path_xcalar = re.search(re_pattern, f.read()).group(1)
    return path_xcalar


max_num_snapshot = 30
path_xcalar = get_path()
path_snapshot = path_xcalar + "/workbook_snapshots/"
path_log = path_snapshot + "WorkbookSnapshot.log"
default_timeout = 60


class WorkbookSnapshot:
    def __init__(self, args):
        os.makedirs(path_snapshot, exist_ok=True)
        self.logger = self.init_logger()
        self.num_snapshot = args.n if args.n is not None else max_num_snapshot
        if args.l is not None:
            self.list_snapshot(args.l)
        elif args.r is not None:
            self.restore_snapshot(args.r[0], args.r[1], int(args.r[2]))
        else:
            self.save_snapshot()

    def init_logger(self):
        logger = logging.getLogger('workbook_snapshot')
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(path_log)
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def save_snapshot(self):
        start_clock = time.time()
        start_time = self.time_to_readable(start_clock)
        self.logger.info("Start creating snapshot")

        user_list = self.get_user_list()
        path_snapshot_time = os.path.join(path_snapshot, start_time)
        os.makedirs(path_snapshot_time, exist_ok=True)
        for username in user_list:
            self.logger.info("Creating snapshot for user {}".format(username))
            path_snapshot_user = os.path.join(path_snapshot_time, username)
            os.makedirs(path_snapshot_user, exist_ok=True)
            for wb, wb_bytes in self.get_workbooks(username):
                self.logger.info("Creating snapshot of workbook {}".format(wb))
                path_snapshot_wb = os.path.join(path_snapshot_user,
                                                wb + ".tar.gz")
                with open(path_snapshot_wb, "wb") as f:
                    f.write(wb_bytes)
        self.rotate_snapshot()

        end_clock = time.time()
        self.logger.info(
            "Finished creating snapshot. Total duration is {}s".format(
                end_clock - start_clock))

    def get_user_list(self):
        path_xcalar_session = os.path.join(path_xcalar, "sessions")
        file_list = os.listdir(path_xcalar_session) if os.path.isdir(
            path_xcalar_session) else []
        re_pattern = "Xcalar\.session\.(.+)\..*[0-1]"
        res_list = set(
            re.search(re_pattern, fn).group(1) for fn in file_list
            if re.search(re_pattern, fn))
        res_list = [fn for fn in res_list if fn != "XcalarInternal"]

        return res_list

    def get_workbooks(self, username):
        workbook_list = self.get_workbook_list(username)
        if not workbook_list:
            return
        if new_sdk:
            client = Client(username=username)
            # XXX New SDK should be enhanced to add a timeout feature.
            # See Xc-12942.
            for wb in workbook_list:
                self.logger.info("Downloading workbook {}".format(wb))
                yield wb, client.get_workbook(wb).download()
        else:
            with ClientOld(username) as client:
                with Timeout():
                    for wb in workbook_list:
                        self.logger.info("Downloading workbook {}".format(wb))
                        yield wb, client.download(wb, None)

    def get_workbook_list(self, username):
        workbook_list = []
        try:
            if new_sdk:
                client = Client(username=username)
                workbook_list = client.list_workbooks()
                workbook_list = [
                    workbook_list[i].name for i in range(len(workbook_list))
                ]
            else:
                with ClientOld(username) as client:
                    workbook_list = client.list()
                    workbook_list = [
                        workbook_list.sessions[i].name
                        for i in range(workbook_list.numSessions)
                    ]
                    workbook_list.remove(client.name)
        except XcalarApiStatusException as e:
            self.logger.error("Error getting workbook list. {}".format(str(e)))
        return workbook_list

    def rotate_snapshot(self):
        time_list = [
            d for d in os.listdir(path_snapshot)
            if os.path.isdir(os.path.join(path_snapshot, d))
        ] if os.path.isdir(path_snapshot) else []
        time_list.sort(key=self.readable_to_time)
        while len(time_list) > self.num_snapshot:
            shutil.rmtree(os.path.join(path_snapshot, time_list.pop(0)))

    def list_snapshot(self, username):
        time_list = [
            d for d in os.listdir(path_snapshot)
            if os.path.isdir(os.path.join(path_snapshot, d))
        ] if os.path.isdir(path_snapshot) else []
        time_list.sort(key=self.readable_to_time)
        time_list = [
            f for f in time_list if os.path.isdir(
                os.path.join(os.path.join(path_snapshot, f), username))
        ]
        for i, f in enumerate(time_list):
            print("{}: {}".format(
                i,
                datetime.datetime.fromtimestamp(int(self.readable_to_time(f)))
                .strftime("%a %b %d %Y %H:%M:%S")))

    def restore_snapshot(self, username, wb_name, num):
        time_list = [
            d for d in os.listdir(path_snapshot)
            if os.path.isdir(os.path.join(path_snapshot, d))
        ] if os.path.isdir(path_snapshot) else []
        time_list.sort(key=self.readable_to_time)
        time_list = [
            f for f in time_list if os.path.isdir(
                os.path.join(os.path.join(path_snapshot, f), username))
        ]
        if num >= len(time_list):
            print("No snapshot for the user at this time", file=sys.stderr)
            return
        file = os.path.join(path_snapshot, time_list[num])
        file = os.path.join(file, username)
        file = os.path.join(file, wb_name + ".tar.gz")

        if not os.path.isfile(file):
            print("No snapshot of the workbook at this time", file=sys.stderr)
            return

        with open(file, "rb") as f:
            try:
                self.upload_workbook(username,
                                     wb_name + "_restored_" + time_list[num],
                                     f.read())
            except XcalarApiStatusException as e:
                if str(e) == "Session already exists":
                    print("Workbook already exists", file=sys.stderr)
                    return
                else:
                    raise
        print("Workbook restored")

    def upload_workbook(self, username, workbook_name, workbook_bytes):
        if new_sdk:
            client = Client(username=username)
            client.upload_workbook(workbook_name, workbook_bytes)
        else:
            with ClientOld(username) as client:
                client.upload(workbook_name, workbook_bytes, None)

    # Can take str, int, float.
    @staticmethod
    def time_to_readable(timestamp):
        return datetime.datetime.fromtimestamp(
            int(timestamp)).isoformat().replace(':', '-')

    # Return str.
    @staticmethod
    def readable_to_time(readable):
        readable_date, readable_time = readable.split('T')
        readable_time = readable_time.replace('-', ':')
        readable = 'T'.join([readable_date, readable_time])
        if sys.version_info >= (3, 7):
            timestamp = str(
                int(datetime.datetime.fromisoformat(readable).timestamp()))
        else:
            timestamp = str(
                int(
                    datetime.datetime.strptime(
                        readable, "%Y-%m-%dT%H:%M:%S").timestamp()))
        return timestamp


class ClientOld:
    def __init__(self, username, sessionName=None):
        self.logger = logging.getLogger('workbook_snapshot')
        self.xcalarApi = XcalarApi()
        self.session = Session(
            self.xcalarApi,
            "XcalarWbSs",
            username=username,
            reuseExistingSession=True,
            sessionName=sessionName)
        self.xcalarApi.setSession(self.session)
        self.session.activate()

    def __enter__(self):
        return self.session

    # If an error is raised in __init__ or __enter__
    # the code block will not be executed and __exit__ will not be called.
    def __exit__(self, e_type, e_value, traceback):
        expected_exception = False
        # This is the exception from socket when you time out it using signal.
        if isinstance(e_value, IndexError):
            self.logger.error("Error downloading workbook. Timeout")
            expected_exception = True
        elif isinstance(e_value, TimeoutError):
            self.logger.error("Error downloading workbook. Timeout")
            expected_exception = True
        elif isinstance(e_value, XcalarApiStatusException):
            self.logger.error("Error downloading workbook. {}".format(
                str(e_value)))
            expected_exception = True
        # Catch all exceptions from code block so the tool will not quit.
        # Return True to prevent the context manager from raising the exception
        # again.
        elif e_value is not None:
            self.logger.error("Error downloading workbook. {}".format(
                str(e_value)))
            expected_exception = True

        try:
            self.session.destroy()
        except ConnectionRefusedError:
            self.logger.error(
                "Error destroying snapshot session. Connection refused")
        except XcalarApiStatusException as e:
            self.logger.error("Error destroying snapshot session. {}".format(
                str(e)))
        except:
            self.logger.error("Error destroying snapshot session. {}".format(
                str(sys.exc_info()[1])))

        # If no exception from code block in context manager, will return
        # False but there will be no exception to raise.
        return expected_exception


class Timeout:
    def __init__(self, seconds=default_timeout):
        self.seconds = seconds

    def handle_timeout(self, sig_num, frame):
        raise TimeoutError

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, e_type, e_value, traceback):
        signal.alarm(0)


def get_args(argv):
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "-n",
        type=lambda x: int(x) if int(x) >= 0 else max_num_snapshot,
        help="Max number of snapshots")
    parser.add_argument("-l", type=str, help="List all snapshots")
    parser.add_argument(
        "-r", nargs=3, type=str, help="Restore selected snapshots")

    args = parser.parse_args(argv)

    return args


def main():
    args = get_args(sys.argv[1:])
    WorkbookSnapshot(args)


if __name__ == "__main__":
    main()