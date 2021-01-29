# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import socket
import os
import json
from os import path

PERF = "perf"
TEST_TYPE = "test_type"
PERF_DIR = "/netstore/datasets/loadqa"
PERF_FILE = "perfout"
NAME = "name"
TESTS = "tests"
DATASET = "dataset"
INPUT_SERIALIZATION = "input_serialization"
COMPRESSION_TYPE = "CompressionType"
MARK = "mark"
TEST_TYPE = "test_type"
OPER = "oper"


def get_perf_path():
    os.makedirs(PERF_DIR, exist_ok=True)
    perf_file = f"{PERF_FILE}_{socket.gethostname()}.json"
    perf_path = path.join(PERF_DIR, perf_file)
    if not path.exists(perf_path):
        with open(perf_path, "w") as pf:
            pf.write(json.dumps({f"{TESTS}" : {} }) + "\n")
    return perf_path
