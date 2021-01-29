# Copyright 2018-2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
"""
Usage: Launched within the Xcalar Data Platform to serve as a bridge for XCE to
send a legacy, pre-DF2.0 query string to the expServer for upgrade to DF2.0 key
value pairs.

This Xcalar Application sends the query string supplied in "inBlob" to the
expServer's "upgradeQuery" REST endpoint and receives the upgraded dataflows
in the return string (r.text below), which contains key value pairs
representing the corresponding DF2.0 dataflows. The launcher of the application
must then parse the json string and enter each k-v pair in the appropriate
k-v store for the upgrade to be complete

"""
import sys
import logging
import logging.handlers
import requests
import tempfile
import json
import os
from socket import gethostname

import xcalar.container.context as ctx

logger = logging.getLogger('query to DF2 upgrade app Logger')
logger.setLevel(logging.INFO)

if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    thisXpuId = ctx.get_xpu_id()
    thisNodeId = ctx.get_node_id(thisXpuId)
    logHandler = logging.StreamHandler(sys.stdout)
    numNodes = ctx.get_node_count()

    formatter = logging.Formatter(
       '%(asctime)s - Node {} - QueryToDF2 Upgrade App - %(levelname)s - %(message)s'.
       format(thisNodeId))
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("queryToDF2 upgrade app initialized; nodeId:{}, numNodes:{}, hostname:{}".format(
        thisNodeId,
        numNodes,
        gethostname()
        ))

# Write the query string to a temp file, send the filename to expServer. The
# "upgradeQuery" REST endpoint in expServer will read and convert the query
# string to the new DF2.0 dataflows and write the K-V pairs to the temp file
# also provided by the python app. The python app will read the result from the
# temp file upon getting the http status ok from expserver.

def main(inBlob):
    headers = {'Content-Type': 'application/json'}
    address = os.getenv('XCE_API_URL', 'http://localhost:12124')
    URL = '{}/service/upgradeQuery'.format(address)

    with tempfile.NamedTemporaryFile(mode="rt", encoding="utf-8") as queryFileIn:
        with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as queryFile:
            queryFile.write(inBlob)
            queryFile.flush()
            data = {'filename': queryFile.name, 'filenameOut': queryFileIn.name}
            requests.post(
                URL, headers=headers, data=json.dumps(data), verify=False)
        res = queryFileIn.read()
    return res
