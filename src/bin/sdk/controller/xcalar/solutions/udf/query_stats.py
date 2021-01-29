# PLEASE TAKE NOTE:

# UDFs can only support
# return values of
# type String.

# Function names that
# start with __ are
# considered private
# functions and will not
# be directly invokable.

from xcalar.external.LegacyApi.XcalarApi import XcalarApi

import json
import jsonpickle
import os
import time
import gzip
import datetime


def main(inBlob):
    params = json.loads(inBlob)
    client_secrets = {
        "xiusername": params["username"],
        "xipassword": params["password"]
    }
    statsFilePath = params["statsFilePath"]
    xcApi = XcalarApi(client_secrets=client_secrets)
    qs = xcApi.queryState(params["dfName"])

    today = datetime.datetime.now()
    directory = os.path.join(
        statsFilePath, "jobStats/", "{}-{}-{}/".format(today.year, today.month,
                                                       today.day))

    os.makedirs(directory, exist_ok=True)
    with gzip.open(
            os.path.join(
                directory, "job_{}_{}.json.gz".format(params["dfName"],
                                                      int(time.time()))),
            "wb") as fp:
        fp.write(jsonpickle.encode(qs).encode())
