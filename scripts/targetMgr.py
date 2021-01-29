import sys
import logging
import json

import xcalar.container.target.manage as target_manager

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
            '%(asctime)s - Target App - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)

    logger.addHandler(log_handler)

    logger.debug("Target Mgr App initialized")

def main(in_blob):
    logger.debug("Received input: {}".format(in_blob))
    ret = None
    try:
        in_obj = json.loads(in_blob)
        if in_obj["func"] == "addTarget":
            target_manager.add_target(in_obj["targetTypeId"],
                             in_obj["targetName"],
                             in_obj["targetParams"])
            ret = json.dumps({"status": "success"})

        elif in_obj["func"] == "deleteTarget":
            target_manager.delete_target(in_obj["targetName"])
            ret = json.dumps({"status": "success"})

        elif in_obj["func"] == "listTargets":
            targets = list(target_manager.list_targets())
            ret = json.dumps(targets)

        elif in_obj["func"] == "listTypes":
            types = list(target_manager.list_target_types())
            ret = json.dumps(types)

        else:
            raise ValueError("Not implemented")
    except Exception:
        logger.exception("Caught TargetMgr exception")
        raise

    return ret
