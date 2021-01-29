#!/usr/bin/env python3.6
# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import sys
import argparse
import os

import xcalar.compute.util.config as config
from xcalar.compute.util.utils import tar_cz
from xcalar.compute.util.upload_stats_util import UploadHelper
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client

port = os.getenv("XCE_HTTPS_PORT", '8443')
parser = argparse.ArgumentParser()

parser.add_argument(
    "-d",
    "--date",
    required=False,
    help="Date to upload stats from. Must be of form YYYY-MM-DD"
)
parser.add_argument(
    "-n",
    "--additional_days_prior",
    required=False,
    default=0
)
parser.add_argument(
    "-a",
    "--upload_all_dates",
    action='store_true',
    help="Upload all days of stats from the stats root.",
    required=False
)
parser.add_argument(
    "-r",
    "--stats_root",
    required=False,
    help="Defaults to use the default stats root of the cluster you connect to.",
    default=""
)
parser.add_argument(
    "-f",
    "--postfix",
    help="Postfix for workbook and published tables names.",
    required=False,
    default=""
)
parser.add_argument(
    '-l',
    "--xcalar_host",
    required=False,
    default="localhost:{}".format(port),
    type=str,
    help='Xcalar cluster hostname and port (as hostname:port)')
parser.add_argument(
    '-u',
    "--xcalar_user",
    help="username of xcalar",
    required=False,
    default="admin")
parser.add_argument(
    '-p',
    "--xcalar_pass",
    help="password of xcalar",
    required=False,
    default="admin")
parser.add_argument(
    "-w",
    "--workbook_prefix",
    help="Prefix of workbook to upload. Must be StatsMart or StatsMart_Internal. Defaults to use StatsMart.",
    required=False,
    default="StatsMart"
)
parser.add_argument(
    "--ignore_cgroup_stats",
    dest="ignore_cgroup_stats",
    action='store_true',
    required=False
)
parser.add_argument(
    "--detailed_cgroup_stats",
    dest="detailed_cgroup_stats",
    help="Include detailed memory cgroup stats (includes details such as total swap per cgroup)",
    action='store_true',
    required=False
)

args = parser.parse_args()

# setup cluster client and session
url = 'https://{}'.format(args.xcalar_host)
username = args.xcalar_user
password = args.xcalar_pass

workbook_name = "{}{}".format(args.workbook_prefix, args.postfix)

client = Client(
    url=url,
    client_secrets={
	    "xiusername": username,
	    "xipassword": password
})

xcalar_api = XcalarApi(
    url=url,
    client_secrets={
        "xiusername": username,
        "xipassword": password
})

stats_root = args.stats_root
if not stats_root:
    for param in client.get_config_params():
        if param['param_name'] == 'XcalarRootCompletePath':
            xcalar_root = param['param_value']
    stats_root = os.path.join(xcalar_root, 'DataflowStatsHistory')

try:
    workbook = client.get_workbook(workbook_name)
except Exception as e:
    if "No such workbook" in str(e):
        path = os.path.join(os.getenv("XLRDIR", "/opt/xcalar/"), "scripts/exampleWorkbooks/{}/workbook".format(args.workbook_prefix))
        workbook = client.upload_workbook(workbook_name, tar_cz(path))
        print("Successfully Uploaded Workbook with name: {}".format(workbook.name))
    else:
        raise e

sess = workbook.activate()
xcalar_api.setSession(sess)
upload_helper = UploadHelper(client, xcalar_api, sess, stats_root)
include_cgroup_stats = not args.ignore_cgroup_stats
if not args.upload_all_dates:
    if not args.date:
        raise Exception("Date must be provided. If you load stats from all dates in stats root, please use the '--upload_all_dates' flag.")
    upload_helper.upload_stats(args.date, int(args.additional_days_prior),
                postfix=args.postfix, load_cg_stats=include_cgroup_stats, load_iostats=True, detailed_mem_cg_stats=args.detailed_cgroup_stats)
else:
    if args.date:
        print("WARNING: Since --upload_all_dates flag was provided, date provided will be ignored") 
    upload_helper.upload_all_stats(postfix=args.postfix,
                load_cg_stats=include_cgroup_stats, load_iostats=True, detailed_mem_cg_stats=args.detailed_cgroup_stats)
