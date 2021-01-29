import os
import os.path
import psutil
import json
import time
import pytest

from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.LegacyApi.RuntimeParams import RuntimeParams
from xcalar.compute.coretypes.LibApisCommon.ttypes import (
    RuntimeTypeT,
    XcalarApiRuntimeSetParamInputT,
    XcalarApiSchedParamT,
)
from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException
from xcalar.compute.services.Cgroup_xcrpc import Cgroup, CgRequest

pytestmark = [
    pytest.mark.last(
        "Execute this test as late as possible since it manages its own cluster"
    ),
    # Back up the original config file and remember the default
    # XrRootCompletePath, so we can revert.
    pytest.mark.usefixtures("config_backup")
]


def listCgroupParams():
    client = Client()
    cgserv = Cgroup(client)
    inputObj = {}
    inputObj["func"] = "listCgroupsConfigs"
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)
    return json.loads(cgres.jsonOutput)


def waitForCgroupMgrInit(cgserv):
    # Hang in there until cgroup sub-system gets inited
    cgroupMgrInited = False
    while cgroupMgrInited is False:
        try:
            listCgroupParams()
        except XDPException as exc:
            assert (exc.statusCode == StatusT.StatusAgain)
            time.sleep(0.25)
            continue
        cgroupMgrInited = True

@pytest.mark.skipif(
    'container' in os.environ, reason='cgroups not supported in containers')
def testMixedModeRuntime():
    coreCount = psutil.cpu_count()
    with DevCluster() as cluster:
        client = Client()
        cgserv = Cgroup(client)
        waitForCgroupMgrInit(cgserv)

        xcalarApi = XcalarApi()
        runtimeParams = RuntimeParams(xcalarApi)
        defaultCpuPctSc = []
        nameSc = ["Scheduler-0", "Scheduler-1", "Scheduler-2", "Immediate"]

        rtTypeSc = [
            RuntimeTypeT.Throughput, RuntimeTypeT.Latency, RuntimeTypeT.Latency,
            RuntimeTypeT.Immediate
        ]

        # Validate default runtime params
        getRtParams = runtimeParams.get()
        for ii in getRtParams.schedParams:
            defaultCpuPctSc.append(ii.cpusReservedInPercent)
            if ii.schedName == nameSc[0]:
                assert (ii.runtimeType == RuntimeTypeT.Throughput)
                assert (ii.cpusReservedInPercent == 100)
            elif ii.schedName == nameSc[1]:
                assert (ii.runtimeType == RuntimeTypeT.Throughput
                        or ii.runtimeType == RuntimeTypeT.Latency)
                assert (ii.cpusReservedInPercent == 100)
            elif ii.schedName == nameSc[2]:
                assert (ii.runtimeType == RuntimeTypeT.Latency)
                assert (ii.cpusReservedInPercent == 100)
            elif ii.schedName == nameSc[3]:
                assert (ii.runtimeType == RuntimeTypeT.Immediate)
            else:
                assert (False)
        print("getRtParams-1 {}".format(getRtParams))

        # Set new runtime params
        xcalarApi.setConfigParam("RuntimeMixedModeMinCores", "%d" % coreCount)
        rtSetParams = XcalarApiRuntimeSetParamInputT()
        rtSetParams.schedParams = []
        cpuPctSc = [185, 190, 80, defaultCpuPctSc[3] + 100]
        defaultCpuPctSc[3] = cpuPctSc[3]
        for ii in getRtParams.schedParams:
            schedParam = XcalarApiSchedParamT()
            schedParam.schedName = ii.schedName
            if ii.schedName == nameSc[0]:
                schedParam.cpusReservedInPercent = cpuPctSc[0]
                schedParam.runtimeType = rtTypeSc[0]
            elif ii.schedName == nameSc[1]:
                schedParam.cpusReservedInPercent = cpuPctSc[1]
                schedParam.runtimeType = rtTypeSc[1]
            elif ii.schedName == nameSc[2]:
                schedParam.cpusReservedInPercent = cpuPctSc[2]
                schedParam.runtimeType = rtTypeSc[2]
            else:
                assert (False)
            rtSetParams.schedParams.append(schedParam)

        # Validate the runtime params set
        runtimeParams.set(rtSetParams)

        getRtParams = runtimeParams.get()
        for ii in getRtParams.schedParams:
            if ii.schedName == nameSc[0]:
                assert (ii.runtimeType == rtTypeSc[0])
                assert (ii.cpusReservedInPercent == int((int(
                    (coreCount * cpuPctSc[0]) / 100) * 100) / coreCount))
            elif ii.schedName == nameSc[1]:
                assert (ii.runtimeType == rtTypeSc[1])
                assert (ii.cpusReservedInPercent == int((int(
                    (coreCount * cpuPctSc[1]) / 100) * 100) / coreCount))
            elif ii.schedName == nameSc[2]:
                assert (ii.runtimeType == rtTypeSc[2])
                assert (ii.cpusReservedInPercent == int((int(
                    (coreCount * cpuPctSc[2]) / 100) * 100) / coreCount))
            elif ii.schedName == nameSc[3]:
                assert (ii.runtimeType == rtTypeSc[3])
                assert (ii.cpusReservedInPercent == int((int(
                    (coreCount * cpuPctSc[3]) / 100) * 100) / coreCount))
            else:
                assert (False)
        print("getRtParams-2 {}".format(getRtParams))

        # Try setting a bogus runtime setting
        rtSetParams = XcalarApiRuntimeSetParamInputT()
        rtSetParams.schedParams = []
        for ii in getRtParams.schedParams:
            schedParam = XcalarApiSchedParamT()
            schedParam.schedName = ii.schedName
            if ii.schedName == nameSc[0]:
                schedParam.cpusReservedInPercent = cpuPctSc[0]
                schedParam.runtimeType = rtTypeSc[1]    # Invalid param
            elif ii.schedName == nameSc[1]:
                schedParam.cpusReservedInPercent = cpuPctSc[1]
                schedParam.runtimeType = rtTypeSc[1]
            elif ii.schedName == nameSc[2]:
                schedParam.cpusReservedInPercent = cpuPctSc[2]
                schedParam.runtimeType = rtTypeSc[2]
            elif ii.schedName == nameSc[3]:
                schedParam.cpusReservedInPercent = cpuPctSc[3]
                schedParam.runtimeType = rtTypeSc[3]
            else:
                assert (False)
            rtSetParams.schedParams.append(schedParam)
        try:
            runtimeParams.set(rtSetParams)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusRuntimeSetParamInvalid

        xcalarApi.setConfigParam("RuntimeMixedModeMinCores", "%d" % 32)
        try:
            runtimeParams.set(rtSetParams)
        except XcalarApiStatusException as e:
            assert e.status == StatusT.StatusRuntimeSetParamNotSupported

        # Restore previous runtime settings
        xcalarApi.setConfigParam("RuntimeMixedModeMinCores", "%d" % coreCount)
        rtSetParams = XcalarApiRuntimeSetParamInputT()
        rtSetParams.schedParams = []
        for (ii, schedName) in enumerate(nameSc):
            schedParam = XcalarApiSchedParamT()
            schedParam.cpusReservedInPercent = defaultCpuPctSc[ii]
            schedParam.schedName = schedName
            schedParam.runtimeType = rtTypeSc[ii]
            rtSetParams.schedParams.append(schedParam)
        runtimeParams.set(rtSetParams)
