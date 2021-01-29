#!/usr/bin/env python3.6
# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

#
# This program provides a 'htop'-like experience for Xcalar jobs state and some
# select stats for jobs. See the 'pgm_description' below for details.

import argparse
import textwrap
import os
import multiprocessing

from xcalar.compute.util.xcalar_top_util import xcalar_top_main
from xcalar.compute.util.xcalar_top_util import get_program_description
from xcalar.external.client import Client

port = os.getenv("XCE_HTTPS_PORT", '8443')
default_refresh_interval = 5
default_num_parallel_procs = multiprocessing.cpu_count()
default_iters = 100

def parse_user_args():
    parser = argparse.ArgumentParser(
        prog='PROG',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(get_program_description()))
    parser.add_argument(
        '-l',
        '--xcalarHost',
        dest='xcalarHost',
        required=False,
        default="localhost:{}".format(port),
        type=str,
        help='Xcalar cluster hostname and port (as hostname:port)')
    parser.add_argument(
        '-u',
        '--xcalarUser',
        dest="xcalarUser",
        help="username of xcalar",
        required=False,
        default="admin")
    parser.add_argument(
        '-p',
        '--xcalarPassword',
        dest="xcalarPass",
        help="password of xcalar",
        required=False,
        default="admin")
    parser.add_argument(
        '-i',
        '--interval',
        dest="interval",
        help="Refresh Interval",
        required=False,
        default=default_refresh_interval
    )
    parser.add_argument(
        '-c',
        '--num_parallel_procs',
        dest="num_parallel_procs",
        help="Parallel procs to use for processing.",
        required=False,
        default=default_num_parallel_procs
    )
    parser.add_argument(
        '-v',
        '--verbose',
        dest="verbose",
        action='store_true',
        required=False
    )
    parser.add_argument(
        '-n',
        '--iters',
        dest="iters",
        help="Number of Iterations",
        required=False,
        default=default_iters
    )
    return parser.parse_args()


args = parse_user_args()

# setup cluster client and session
url = 'https://{}'.format(args.xcalarHost)
username = args.xcalarUser
password = args.xcalarPass

# Create a Xcalar Client object
client = Client(
    url=url, client_secrets={
        "xiusername": username,
        "xipassword": password
    })

xcalar_top_main(client,
                url,
                username,
                password,
                int(args.interval),
                int(args.num_parallel_procs),
                args.verbose,
                int(args.iters))
