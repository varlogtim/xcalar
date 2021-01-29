# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import json
import xcalar.container.parquetUtils as parquetUtils
import logging

logger = logging.getLogger("xcalar")
logger.setLevel(logging.INFO)

def main(inBlob):
    try:
        inObj = json.loads(inBlob)
        if inObj["func"] == "getInfo":
            ret = parquetUtils.getInfo(inObj["targetName"], inObj["path"], parquetParserIn = inObj.get("parquetParser", None))
        elif inObj["func"] == "getPossibleKeyValues":
            ret = parquetUtils.getPossibleKeyValues(inObj["targetName"], inObj["path"], key = inObj["key"], givenKeys = inObj.get("givenKeys", {}), parquetParserIn = inObj.get("parquetParser", None))
        else:
            raise ValueError("Not implemented")
    except:
        logger.exception("Parquet tools exception")
        raise

    return ret

