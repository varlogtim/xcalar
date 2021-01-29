# Cgroup tune script
# Example:
# python tuneCgroups.py --cgName "usr_xpus-sched2.scope" --cgController "memory" --memory.limit_in_bytes 40466547916 --memory.memsw.limit_in_bytes 49056482508 --memory.soft_limit_in_bytes 36419893125

import os
import os.path
import shutil
import psutil
import json
import time
import argparse
import sys

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.WorkItem import WorkItem
from xcalar.compute.util.config import build_config
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.LegacyApi.Env import XcalarConfigPath
from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException
from xcalar.compute.services.Cgroup_xcrpc import Cgroup, CgRequest
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException


# Args parsing
parser = argparse.ArgumentParser()
parser.add_argument("--cgName", type=str, help="Cgroup name", dest='cgname')
parser.add_argument("--cgController", type=str, help="Cgroup controller name", dest='controller')
parser.add_argument("--memory.limit_in_bytes", type=int, help="Cgroup controller memory.limit_in_bytes", dest='limit_in_bytes')
parser.add_argument("--memory.memsw.limit_in_bytes", type=int, help="Cgroup controller memory.memsw.limit_in_bytes", dest='swlimit_in_bytes')
parser.add_argument("--memory.soft_limit_in_bytes", type=int, help="Cgroup controller memory.soft_limit_in_bytes", dest='soft_limit_in_bytes')
args = parser.parse_args()


def listCgroupParams():
    client = Client(bypass_proxy=True)
    cgserv = Cgroup(client)
    inputObj = {}
    inputObj["func"] = "listCgroupsConfigs"
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)
    return json.loads(cgres.jsonOutput)


def getCgroupParams(cgName, cgController):
    client = Client(bypass_proxy=True)
    cgserv = Cgroup(client)
    inputObj = {}
    inputObj["func"] = "getCgroup"
    inputObj["cgroupName"] = cgName
    inputObj["cgroupController"] = cgController
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)
    cgParamDict = json.loads(json.loads(
        cgres.jsonOutput)[0][0])[cgController][0]
    for k, v in cgParamDict.items():
        cgParamDict[k] = str(v).rstrip()
    print("\n\nGet cgName: {}, cgController: {}, Params {}".format(cgName, cgController, cgParamDict))
    return cgParamDict


def setCgroupParams(cgName, cgController, cgParams):
    client = Client(bypass_proxy=True)
    cgserv = Cgroup(client)
    inputObj = {}
    inputObj["func"] = "setCgroup"
    inputObj["cgroupName"] = cgName
    inputObj["cgroupController"] = cgController
    inputObj["cgroupParams"] = cgParams
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)
    print("\n\nSet cgName: {}, cgController: {}, Params {}".format(cgName, cgController, cgParams))


# List Cgroup Params
listOut = listCgroupParams()
print("***\n\t\tList Cgroup Params: {}".format(listOut))

# Get all XPU Cgroup params
print("***\n\t\tGet XPUs current config")
xpuMemSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "memory")
xpuCpuSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "cpu")
#xpuCpusetSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "cpuset")
xpuCpuacctSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "cpuacct")

xpuMemSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "memory")
xpuCpuSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "cpu")
#xpuCpusetSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "cpuset")
xpuCpuacctSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "cpuacct")

xpuMemSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "memory")
xpuCpuSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "cpu")
#xpuCpusetSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "cpuset")
xpuCpuacctSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "cpuacct")

xpuMemUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "memory")
xpuCpuUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "cpu")
#xpuCpusetUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "cpuset")
xpuCpuacctUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "cpuacct")

xpuMemUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "memory")
xpuCpuUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "cpu")
#xpuCpusetUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "cpuset")
xpuCpuacctUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "cpuacct")

xpuMemUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "memory")
xpuCpuUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "cpu")
#xpuCpusetUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "cpuset")
xpuCpuacctUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "cpuacct")


# Set given config
print("\n\t\t***Setting Config")
if ("xpus" not in args.cgname or args.controller != "memory" or args.limit_in_bytes == 0 or args.swlimit_in_bytes == 0 or args.soft_limit_in_bytes == 0):
    print("Invalid input params")
    sys.exit(-1)
curParams = getCgroupParams(args.cgname, args.controller)
newCgParams = {}
newCgParams["memory.limit_in_bytes"] = args.limit_in_bytes
newCgParams["memory.memsw.limit_in_bytes"] = args.swlimit_in_bytes
newCgParams["memory.soft_limit_in_bytes"] = args.soft_limit_in_bytes
setCgroupParams(args.cgname, args.controller, newCgParams)
print("\n\t\t***Getting Config")
getCgroupParams(args.cgname, args.controller)
