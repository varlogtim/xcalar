import sys
import logging
import json

import xcalar.container.driver.manage as driver_manager

# Set up logging
logger = logging.getLogger('xpu')
logger.setLevel(logging.INFO)
# This module gets loaded multiple times. The global variables do not
# get cleared between loads. We need something in place to prevent
# multiple handlers from being added to the logger.
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stderr
    log_handler = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        '%(asctime)s - Driver App - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)

    logger.addHandler(log_handler)

    logger.debug("Driver Mgr App initialized")


def main(in_blob):
    logger.debug("Received input: {}".format(in_blob))
    ret = None
    try:
        in_obj = json.loads(in_blob)
        if in_obj["func"] == "listDrivers":
            drivers = list(driver_manager.list_driver_dicts())
            ret = json.dumps(drivers)

        else:
            raise ValueError("Not implemented")
    except Exception:
        logger.exception("Caught DriverMgr exception")
        raise

    return ret
