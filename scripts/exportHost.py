# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import json
import multiprocessing

import xcalar.container.context as ctx
import xcalar.container.driver.manage as driver_manager
import xcalar.container.driver.base
import xcalar.container.target.manage as target_manager
import xcalar.container.table

# Dynamic Constants
numCores = multiprocessing.cpu_count()

# Set up logging
logger = ctx.get_logger()


def _check_params(driver, given_params):
    required = set([p.name for p in driver.params.values() if not p.optional])
    given = set([p.descriptor.name for p in given_params])
    if not required.issubset(given):
        missing = required - given
        raise ValueError("Missing parameters for Driver '{}': {}".format(
            driver.name, ", ".join(missing)))


class Param:
    def __init__(self, descriptor, value):
        self.descriptor = descriptor
        self.value = value


def _run_export(xdb_id, columns, driver_name, raw_driver_params):
    driver = driver_manager.get_driver(driver_name)
    if not driver:
        raise ValueError("No such driver '{}'".format(driver_name))
    table = xcalar.container.table.Table(xdb_id, columns)

    params = [
        Param(driver.params[name], value)
        for name, value in raw_driver_params.items()
    ]

    _check_params(driver, params)

    # Prepare params for the driver
    driver_args = {}
    for p in params:
        if p.descriptor.type == xcalar.container.driver.base.TARGET:
            target_name = p.value
            target_data = target_manager.get_target_data(target_name)
            # The path arg here is only used by the memory target, which isn't
            # a real target anyway and it doesn't really make sense to export
            # to it, so this is fine
            target = target_manager.build_target(
                target_name,
                target_data,
                None,
                num_nodes=ctx.get_node_count(),
                node_id=ctx.get_node_id(ctx.get_xpu_id()),
                user_name=ctx.get_user_id_name(),
                num_xpus_per_node=ctx.get_num_xpus_per_node())
            arg_value = target
        else:
            arg_value = p.value
        # We don't allow 'None' as a valid driver parameter, so this represents
        # an unset parameter.
        if arg_value is not None:
            driver_args[p.descriptor.name] = arg_value

    driver(table, **driver_args)


def main(inBlob):
    ret = None
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    try:
        logger.debug("got export input {}".format(inBlob))
        inObj = json.loads(inBlob)
        if inObj["func"] == "launchDriver":
            raw_driver_params = json.loads(inObj["driverParams"])
            ret = _run_export(inObj["xdbId"], inObj["columns"],
                              inObj["driverName"], raw_driver_params)
        else:
            raise ValueError("Function {} not implemented".format(
                inObj["func"]))
    except Exception:
        logger.exception("Node {} caught export exception".format(thisNodeId))
        raise
    finally:
        ctx._set_app_context(None)

    return ret
