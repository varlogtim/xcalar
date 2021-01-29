import time
import pytest
import psutil
import random

from xcalar.external.LegacyApi.WorkItem import WorkItemShutdown
from xcalar.external.LegacyApi.XcalarApi import XcalarApi


def getPids(procNameOrNames):
    if isinstance(procNameOrNames, list):
        procNames = procNameOrNames
    else:
        procNames = [procNameOrNames]
    for p in psutil.process_iter():
        try:
            name = p.name()
            if name in procNames and p.is_running():
                yield p
        except psutil.NoSuchProcess:
            continue


@pytest.mark.last
class TestShutdown(object):
    def setup_class(cls):
        cls.xcalarApi = XcalarApi()

    @pytest.mark.last
    def testShutdown(self):
        # See if xcmonitor exists. If it does, use xcmonitor to trigger shutdown
        xcmonitorPids = list(getPids("xcmonitor"))
        if len(xcmonitorPids) > 0:
            # Any xcmonitor can take the shutdown call
            randomMonitor = random.choice(xcmonitorPids)
            randomMonitor.terminate()
        else:
            workItem = WorkItemShutdown()
            # Send fake userId to leaking session.
            workItem.workItem.userIdUnique = 1
            workItem.workItem.userId = "xcalar"
            self.xcalarApi.execute(workItem)

        timeToWait = 60 * 5
        for ii in range(0, timeToWait):
            try:
                xceProcs = list(getPids(["usrnode", "childnode"]))
                gone, alive = psutil.wait_procs(xceProcs, timeout=3)
                if not len(alive):
                    break
                print(f'Still alive {alive}')
                time.sleep(5)
            except psutil.NoSuchProcess as e:
                print(f'psutil: NoSuchProcess {e}. Retrying.')
                time.sleep(5)
        alive = list(getPids(["usrnode", "childnode"]))
        print(alive)
        assert not alive
