import pytest
import json
import time
import os
import getpass

from ctypes import (cdll)
from multiprocessing import Process, Pipe
from xcalar.external.client import Client
from xcalar.external.exceptions import XDPException
from xcalar.compute.services.Cgroup_xcrpc import CgRequest
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.app import App
from xcalar.compute.coretypes.Status.ttypes import StatusT

import pyTestNew.test_oom_app as appOom

OOM_APP_NAME = "OOMApp"
OOM_EVENT_STRING = "Saw OOM"
CUR_USER = getpass.getuser()
SYS_CGROUP_PATH = "/sys/fs/cgroup"
CGROUP_CTRL_MEMORY = "memory"
CGROUP_XCALAR_USR_XPUS = "usr_xpus"
CGROUP_XCALAR_SLICE = "xcalar.slice"
CGROUP_XCALAR_SERVICE = "xcalar-usrnode.service"


def listCgroupParams():
    client = Client()
    cgserv = client._cgroup_service
    inputObj = {}
    inputObj["func"] = "listCgroupsConfigs"
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)
    return json.loads(cgres.jsonOutput)


def getCgroupParams(cgName, cgController):
    client = Client()
    cgserv = client._cgroup_service
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
    return cgParamDict


def setCgroupParams(cgName, cgController, cgParams):
    client = Client()
    cgserv = client._cgroup_service
    inputObj = {}
    inputObj["func"] = "setCgroup"
    inputObj["cgroupName"] = cgName
    inputObj["cgroupController"] = cgController
    inputObj["cgroupParams"] = cgParams
    cgreq = CgRequest()
    cgreq.jsonInput = json.dumps(inputObj)
    cgres = cgserv.process(cgreq)


def cgroupMgrInited(cgserv):
    # Hang in there until cgroup sub-system gets inited
    cgroupMgrInited = False
    while cgroupMgrInited is False:
        try:
            listCgroupParams()
        except XDPException as exc:
            if exc.statusCode == StatusT.StatusAgain:
                time.sleep(0.25)
                continue
            elif exc.statusCode == StatusT.StatusCgroupsDisabled:
                return False
            else:
                assert (0)
        cgroupMgrInited = True
    return True


def testCgroups(client):
    cgserv = client._cgroup_service
    if cgroupMgrInited(cgserv) is False:
        return

    # Unknown "func"
    with pytest.raises(XDPException) as ex:
        inputObj = {}
        inputObj["func"] = "foo"
        cgreq = CgRequest()
        cgreq.jsonInput = json.dumps(inputObj)
        cgres = cgserv.process(cgreq)
        assert ex.statusCode == StatusT.StatusUdfExecuteFailed

    #
    # Invalid "getCgroup" requests
    #

    # Unknown cgroup name for "getCgroup"
    with pytest.raises(XDPException) as ex:
        inputObj = {}
        inputObj["func"] = "getCgroup"
        inputObj["cgroupName"] = "foo"
        cgreq = CgRequest()
        cgreq.jsonInput = json.dumps(inputObj)
        cgres = cgserv.process(cgreq)
        assert ex.statusCode == StatusT.StatusUdfExecuteFailed

    # Unknown cgroup controller for "getCgroup"
    with pytest.raises(XDPException) as ex:
        inputObj = {}
        inputObj["func"] = "getCgroup"
        inputObj["cgroupName"] = "usr_xpus-sched0.scope"
        inputObj["cgroupController"] = "bar"
        cgreq = CgRequest()
        cgreq.jsonInput = json.dumps(inputObj)
        cgres = cgserv.process(cgreq)
        assert ex.statusCode == StatusT.StatusUdfExecuteFailed

    # Unsupported cgroup controller for "getCgroup"
    with pytest.raises(XDPException) as ex:
        inputObj = {}
        inputObj["func"] = "getCgroup"
        inputObj["cgroupName"] = "usr_xpus-sched0.scope"
        inputObj["cgroupController"] = "blkio"
        cgreq = CgRequest()
        cgreq.jsonInput = json.dumps(inputObj)
        cgres = cgserv.process(cgreq)
        assert ex.statusCode == StatusT.StatusUdfExecuteFailed

    #
    # Try out valid "getCgroup" requests
    #
    xpuMemSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "memory")
    xpuMemSysCgSched0["memory.limit_in_bytes"] = -1
    xpuCpuSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "cpu")
    xpuCpusetSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "cpuset")
    xpuCpuacctSysCgSched0 = getCgroupParams("sys_xpus-sched0.scope", "cpuacct")

    xpuMemSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "memory")
    xpuMemSysCgSched1["memory.limit_in_bytes"] = -1
    xpuCpuSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "cpu")
    xpuCpusetSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "cpuset")
    xpuCpuacctSysCgSched1 = getCgroupParams("sys_xpus-sched1.scope", "cpuacct")

    xpuMemSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "memory")
    xpuMemSysCgSched2["memory.limit_in_bytes"] = -1
    xpuCpuSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "cpu")
    xpuCpusetSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "cpuset")
    xpuCpuacctSysCgSched2 = getCgroupParams("sys_xpus-sched2.scope", "cpuacct")

    xpuMemUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "memory")
    xpuMemUsrCgSched0["memory.limit_in_bytes"] = -1
    xpuCpuUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "cpu")
    xpuCpusetUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "cpuset")
    xpuCpuacctUsrCgSched0 = getCgroupParams("usr_xpus-sched0.scope", "cpuacct")

    xpuMemUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "memory")
    xpuMemUsrCgSched1["memory.limit_in_bytes"] = -1
    xpuCpuUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "cpu")
    xpuCpusetUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "cpuset")
    xpuCpuacctUsrCgSched1 = getCgroupParams("usr_xpus-sched1.scope", "cpuacct")

    xpuMemUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "memory")
    xpuMemUsrCgSched2["memory.limit_in_bytes"] = -1
    xpuCpuUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "cpu")
    xpuCpusetUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "cpuset")
    xpuCpuacctUsrCgSched2 = getCgroupParams("usr_xpus-sched2.scope", "cpuacct")

    # Try list cgroup request
    listCgroupParams()

    #
    # Invalid "setCgroup" requests
    #

    # Unknown params to set for a given cgroup name
    with pytest.raises(XDPException) as ex:
        inputObj = {}
        inputObj["func"] = "setCgroup"
        inputObj["cgroupName"] = "usr_xpus-sched0.scope"
        inputObj["cgroupController"] = "memory"
        inputObj["foo"] = "bar"
        cgreq = CgRequest()
        cgreq.jsonInput = json.dumps(inputObj)
        cgres = cgserv.process(cgreq)
        assert ex.statusCode == StatusT.StatusUdfExecuteFailed

    #
    # Try all valid "setCgroup" requests
    #

    [
        xpuMemSysCgSched0.pop(k) for k in list(xpuMemSysCgSched0.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched0.scope", "memory", xpuMemSysCgSched0)
    [
        xpuCpuSysCgSched0.pop(k) for k in list(xpuCpuSysCgSched0.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched0.scope", "cpu", xpuCpuSysCgSched0)
    [
        xpuCpusetSysCgSched0.pop(k) for k in list(xpuCpusetSysCgSched0.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched0.scope", "cpuset", xpuCpusetSysCgSched0)
    [
        xpuCpuacctSysCgSched0.pop(k)
        for k in list(xpuCpuacctSysCgSched0.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched0.scope", "cpuacct", xpuCpuacctSysCgSched0)

    [
        xpuMemSysCgSched1.pop(k) for k in list(xpuMemSysCgSched1.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched1.scope", "memory", xpuMemSysCgSched1)
    [
        xpuCpuSysCgSched1.pop(k) for k in list(xpuCpuSysCgSched1.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched1.scope", "cpu", xpuCpuSysCgSched1)
    [
        xpuCpusetSysCgSched1.pop(k) for k in list(xpuCpusetSysCgSched1.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched1.scope", "cpuset", xpuCpusetSysCgSched1)
    [
        xpuCpuacctSysCgSched1.pop(k)
        for k in list(xpuCpuacctSysCgSched1.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched1.scope", "cpuacct", xpuCpuacctSysCgSched1)

    [
        xpuMemSysCgSched2.pop(k) for k in list(xpuMemSysCgSched2.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched2.scope", "memory", xpuMemSysCgSched2)
    [
        xpuCpuSysCgSched2.pop(k) for k in list(xpuCpuSysCgSched2.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched2.scope", "cpu", xpuCpuSysCgSched2)
    [
        xpuCpusetSysCgSched2.pop(k) for k in list(xpuCpusetSysCgSched2.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched2.scope", "cpuset", xpuCpusetSysCgSched2)
    [
        xpuCpuacctSysCgSched2.pop(k)
        for k in list(xpuCpuacctSysCgSched2.keys())
        if k != "memory.limit_in_bytes"
    ]
    setCgroupParams("sys_xpus-sched2.scope", "cpuacct", xpuCpuacctSysCgSched2)


def getModuleSrc(moduleName):
    src = None
    with open(moduleName.__file__) as f:
        src = f.read()
    return src


def eventFd(initVal, flags):
    libc = cdll.LoadLibrary("libc.so.6")
    return libc.eventfd(initVal, flags)


def listenEventFd():
    efd = eventFd(0, 0)
    eventControl = os.path.join(SYS_CGROUP_PATH, CGROUP_CTRL_MEMORY,
                                CGROUP_XCALAR_SLICE, CGROUP_XCALAR_SERVICE,
                                CGROUP_XCALAR_USR_XPUS + "-sched0.scope",
                                "cgroup.event_control")
    cfd = os.open(eventControl, os.O_WRONLY)
    oomControl = os.path.join(SYS_CGROUP_PATH, CGROUP_CTRL_MEMORY,
                              CGROUP_XCALAR_SLICE, CGROUP_XCALAR_SERVICE,
                              CGROUP_XCALAR_USR_XPUS + "-sched0.scope",
                              "memory.oom_control")
    ofd = os.open(oomControl, os.O_RDWR)
    os.write(cfd, ('%d %d' % (efd, ofd)).encode())
    return efd


def watchEventOOM(conn):
    print("Listen for OOM event")
    efd = listenEventFd()
    # Block on receiving an event
    print("Block on OOM event")
    os.read(efd, 8)
    print("Received OOM event")
    conn.send(OOM_EVENT_STRING)
    conn.close()


#
# Run OOM App and validate that XPU cgroup limited it's memory usage.
#
@pytest.mark.skip("OOM event does not work for RHEL6")
def testCgroupOOM(client):
    client = Client()
    cgserv = client._cgroup_service
    if cgroupMgrInited(cgserv) is False:
        return

    xpuUsrCg = getCgroupParams("usr_xpus-sched0.scope", "memory")

    # Start process to watch for OOM events
    parentConn, childConn = Pipe()
    proc = Process(target=watchEventOOM, args=(childConn, ))
    proc.start()

    # Lower the "xcalar_usr_xpus" cgroup memory limit.
    oomAppSrc = getModuleSrc(appOom)
    cgParams = {}
    cgParams["memory.limit_in_bytes"] = 1 * 1024 * 1024 * 1024
    cgParams["memory.soft_limit_in_bytes"] = 512 * 1024 * 1024
    cgParams["memory.swappiness"] = 0
    cgParams["memory.oom_control"] = 0
    setCgroupParams("usr_xpus-sched0.scope", "memory", cgParams)

    app = App(client)
    app.set_py_app(OOM_APP_NAME, oomAppSrc)
    try:
        app.run_py_app(OOM_APP_NAME, False, "")
    except XcalarApiStatusException as e:
        assert e.status == StatusT.StatusChildTerminated

    # Reap process kicked up for watching OOM event.
    assert (parentConn.recv() == OOM_EVENT_STRING)
    proc.join()

    # Revert defaults
    cgParams = {}
    cgParams["memory.limit_in_bytes"] = -1
    cgParams["memory.soft_limit_in_bytes"] = -1
    cgParams["memory.swappiness"] = xpuUsrCg["memory.swappiness"]
    cgParams["memory.oom_control"] = 0
    setCgroupParams("usr_xpus-sched0.scope", "memory", cgParams)
