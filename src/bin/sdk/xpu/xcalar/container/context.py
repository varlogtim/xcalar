# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

#
# Interfaces to return run-time, context information for Apps and UDFs
#
import logging
import multiprocessing
import os
import sys
from socket import gethostname

import xcalar.container.xpu_host as xpu_host    # Injected by XPU process.

# Dynamic Constants
num_cores = multiprocessing.cpu_count()


# internal i/f to distinguish b/w App (returns true) and UDF (returns false)
def _is_app():
    return bool(xpu_host.is_app())


#
# Functions returning contextual info for Apps
#
def get_app_name():
    if not _is_app():
        return None
    return str(xpu_host.get_app_name())


def get_xpu_id():
    if not _is_app():
        raise ValueError("Not running as an App")
    return int(xpu_host.get_xpu_id())


def get_xpu_cluster_size():
    if not _is_app():
        raise ValueError("Not running as an App")
    return int(xpu_host.get_xpu_cluster_size())


def get_node_id(xpu_id):
    if not _is_app():
        raise ValueError("Not running as an App")
    return int(xpu_host.get_node_id(xpu_id))


def get_num_xpus_per_node():
    if not _is_app():
        raise ValueError("Not running as an App")
    return [
        xpu_host.get_xpu_end_id_in_node(node_id) -
        xpu_host.get_xpu_start_id_in_node(node_id) + 1
        for node_id in range(get_node_count())
    ]


# Convert a global xpu_id (globally unique logical XPU id) to a local xpu_id (a
# logical XPU id that's unique only to the local XCE node).
#
def get_local_xpu_id(xpu_id):
    if not _is_app():
        raise ValueError("Not running as an App")
    return xpu_id - xpu_host.get_xpu_start_id_in_node(get_node_id(xpu_id))


#
# Functions returning contextual info for Apps or UDFs
#


def get_node_count():
    return int(xpu_host.get_node_count())


def get_txn_id():
    return int(xpu_host.get_txn_id())


def get_user_id_name():
    return str(xpu_host.get_user_id_name())


app_ctx = None


def _set_app_context(app_ctx_in):
    global app_ctx
    app_ctx = app_ctx_in


def _get_app_context():
    return app_ctx


# xpu logger
def get_logger(log_level=logging.INFO):
    logger = logging.getLogger("xcalar")

    # setting a log filter to add transaction id
    # dynamically to the log formatter
    class AppLogFilter(logging.Filter):
        def filter(self, record):
            record.txn_id = get_txn_id()
            app_name = get_app_name()
            if app_name:
                record.app_name = " - {} App - ".format(app_name)
            else:
                record.app_name = ""
            return True

    logger.setLevel(log_level)
    # This module gets loaded multiple times. The global variables do not
    # get cleared between loads. We need something in place to prevent
    # multiple handlers from being added to the logger.
    if not logger.handlers and _is_app():
        # Logging is not multi-process safe, so stick to stderr
        log_handler = logging.StreamHandler(sys.stderr)
        this_xpu_id = get_xpu_id()
        this_node_id = get_node_id(this_xpu_id)
        num_nodes = get_node_count()

        formatter = logging.Formatter(
            '%(asctime)s - Pid {} Node {} Txn %(txn_id)s%(app_name)s%(levelname)s - %(message)s'
            .format(os.getpid(), this_node_id))
        log_handler.setFormatter(formatter)

        logger.addHandler(log_handler)
        logger.addFilter(AppLogFilter())

        logger.debug(
            "{} App initialized; nodeId:{}, numNodes:{}, hostname:{}, numCores:{}"
            .format(get_app_name(), this_node_id, num_nodes, gethostname(),
                    num_cores))
    return logger
