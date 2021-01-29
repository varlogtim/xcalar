# Runtime tune script

import os
import os.path
import shutil
import psutil
import json
import time
import argparse
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.WorkItem import WorkItem
from xcalar.external.client import Client
from xcalar.compute.util.config import build_config
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.LegacyApi.Env import XcalarConfigPath
from xcalar.external.LegacyApi.RuntimeParams import RuntimeParams
from xcalar.compute.coretypes.LibApisCommon.ttypes import (
    RuntimeTypeT,
    XcalarApiRuntimeSetParamInputT,
    XcalarApiSchedParamT,
)

# Lower the CPU count for enabling mixed mode
coreCount = psutil.cpu_count()
xcalarApi = XcalarApi(bypass_proxy=True)
xcalarApi.setConfigParam("RuntimeMixedModeMinCores", "%d" % coreCount)

parser = argparse.ArgumentParser()
parser.add_argument("--sched0", dest='sched0', required=False, type=int, help='Resource group Sched0 percent', default=-1)
parser.add_argument("--sched1", dest='sched1', required=False, type=int, help='Resource group Sched1 percent', default=-1)
parser.add_argument("--sched2", dest='sched2', required=False, type=int, help='Resource group Sched2 percent', default=-1)
parser.add_argument("--immediate", dest='immediate', required=False, type=int, help='Resource group Immediate', default=-1)
parser.add_argument("--getParams", dest='getParams', required=False, help='Get Resource group config params', action='store_true')
parser.add_argument("--setParams", dest='setParams', required=False, help='Set Resource group config params', action='store_true')
args = parser.parse_args()

# Runtime settings
runtimeParams = RuntimeParams(xcalarApi)
nameSc = ["Scheduler-0", "Scheduler-1", "Scheduler-2", 'Immediate']
rtTypeSc = [
    RuntimeTypeT.Throughput, RuntimeTypeT.Latency, RuntimeTypeT.Latency, RuntimeTypeT.Immediate
]

def getCurParams():
    getRtParams = runtimeParams.get()
    for ii in getRtParams.schedParams:
        print(" Runtime scheduler params: {}".format(ii))
    return getRtParams

# Get runtime params
print("Runtime default params!")
getRtParams = getCurParams()

if args.setParams:
    # Set new runtime params
    rtSetParams = XcalarApiRuntimeSetParamInputT()
    rtSetParams.schedParams = []
    if args.sched0 is None and args.sched1 is None and  args.sched2 is None and args.immediate is None:
        print("Atleast one of the Resouce groups sched0, sched1, sched2 or immediate needs to be set!")
        exit(-1)

    for ii in getRtParams.schedParams:
        schedParam = XcalarApiSchedParamT()
        schedParam.schedName = ii.schedName
        if ii.schedName == nameSc[0]:
            if args.sched0 is -1:
                schedParam.cpusReservedInPercent = getRtParams.schedParams[0].cpusReservedInPercent
            else:
                schedParam.cpusReservedInPercent = args.sched0
            schedParam.runtimeType = rtTypeSc[0]
        elif ii.schedName == nameSc[1]:
            if args.sched1 is -1:
                schedParam.cpusReservedInPercent = getRtParams.schedParams[1].cpusReservedInPercent
            else:
                schedParam.cpusReservedInPercent = args.sched1
            schedParam.runtimeType = rtTypeSc[1]
        elif ii.schedName == nameSc[2]:
            if args.sched2 is -1:
                schedParam.cpusReservedInPercent = getRtParams.schedParams[2].cpusReservedInPercent
            else:
                schedParam.cpusReservedInPercent = args.sched2
            schedParam.runtimeType = rtTypeSc[2]
        elif ii.schedName == nameSc[3]:
            if args.immediate is -1:
                schedParam.cpusReservedInPercent = getRtParams.schedParams[3].cpusReservedInPercent
            else:
                schedParam.cpusReservedInPercent = args.immediate
            schedParam.runtimeType = rtTypeSc[3]
        else:
            assert (False)
        rtSetParams.schedParams.append(schedParam)

    # Issue set request
    runtimeParams.set(rtSetParams)

    # Get runtime params
    print("Runtime params after set!")
    getCurParams()
