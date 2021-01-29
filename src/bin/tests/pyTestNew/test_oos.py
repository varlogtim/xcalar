import os
import os.path
import subprocess
import pytest

from xcalar.compute.util.config import detect_config
from ctypes import (cdll, c_int, c_longlong)
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.app import App
from xcalar.compute.util.cluster import DevCluster

from xcalar.external.client import Client

testOosSessionName = "TestOos"

pytestmark = [
    pytest.mark.last(
        "Execute this test as late as possible since it manages its own clusters"
    ),
    pytest.mark.usefixtures("config_backup")
]

XcalarConfigPath = detect_config().config_file_path


# Update Xcalar Config file with new XlrRoot
def updateConfig(XlrRootConfig, defaultXlrRootConfig, newXlrRoot):
    os.path.exists(newXlrRoot)
    oldConfig = "Constants." + XlrRootConfig + "=" + defaultXlrRootConfig
    newConfig = "Constants." + XlrRootConfig + "=" + newXlrRoot

    with open(XcalarConfigPath) as f:
        newText = f.read().replace(oldConfig, newConfig)

    with open(XcalarConfigPath, "w") as f:
        f.write(newText)


# Create new XlrRoot
def createNewXlrRoot(tFile):
    newXlrRoot = "/mnt/newXlrRootDisk"
    assert (os.path.exists(tFile) is True)
    mkfsCmd = "yes | sudo mkfs.ext4 " + tFile
    print("{}".format(mkfsCmd))
    subprocess.call(mkfsCmd, shell=True)

    mkdirCmd = "sudo mkdir -p " + newXlrRoot
    print("{}".format(mkdirCmd))
    subprocess.call(mkdirCmd, shell=True)
    mountCmd = "sudo mount -t ext4 " + tFile + " " + newXlrRoot
    print("{}".format(mountCmd))
    subprocess.call(mountCmd, shell=True)

    chmodCmd = "sudo chmod 777 " + newXlrRoot
    subprocess.call(chmodCmd, shell=True)
    return newXlrRoot


# Return App source that does fallocate on XlrRoot
def getAppSrc():
    appSrc = """
import ctypes, tempfile
import os
from ctypes import *
def main(inBlob):
    libc=cdll.LoadLibrary("libc.so.6")
    tmpDir=inBlob
    tmpFilePrefix="temp_file_"
    tfile=tempfile.NamedTemporaryFile(delete=True, dir=tmpDir,
        prefix=tmpFilePrefix)
    statvfs = os.statvfs(tmpDir)
    libc.fallocate(tfile.fileno(), c_int(0), c_longlong(0),
        c_longlong(statvfs.f_frsize * statvfs.f_bavail))
    os.close(tfile)
"""
    return appSrc


@pytest.mark.skip(reason="Needs to be tested in a dockerized env")
def testExhaustFsUnderXlrRoot():
    configDict = None
    defaultXlrRootConfig = None
    XlrRootConfig = "XcalarRootCompletePath"

    #
    # Create a new mount for XlrRoot. This is the one that will be exhausted
    # with fallocate.
    #
    cwd = os.getcwd()
    statvfs = os.statvfs(cwd)
    FallocSize = 104857600
    if statvfs.f_frsize * statvfs.f_bavail < FallocSize:
        print("Not enough storage in the {} for this test".format(cwd))
        return

    tFileName = cwd + "testExhaustFsUnderXlrRoot"
    if os.path.exists(tFileName):
        os.unlink(tFileName)
    tfile = open(tFileName, "w+")
    libc = cdll.LoadLibrary("libc.so.6")
    libc.fallocate(tfile.fileno(), c_int(0), c_longlong(0),
                   c_longlong(FallocSize))
    assert (os.path.exists(tfile.name) is True)

    newXlrRoot = createNewXlrRoot(tfile.name)

    #
    # Prepare the App that will be launched in XPU that will do the fallocate
    # of a large file in XlrRoot.
    #
    appSrc = getAppSrc()
    appName = 'appExhaustFsUnderTmp'

    configDict = detect_config().all_options
    defaultXlrRootConfig = configDict["Constants.{}".format(XlrRootConfig)]

    updateConfig(XlrRootConfig, defaultXlrRootConfig, newXlrRoot)

    #
    # Start the cluster and run XPU app that will exhaust the newly mounted
    # XlrRoot
    #
    with DevCluster() as cluster:
        client = Client()
        xcalarApi = XcalarApi()
        session = client.create_session(testOosSessionName)
        xcalarApi.setSession(session)

        app = App(client)
        app.set_py_app(appName, appSrc)
        app.run_py_app(appName, False, newXlrRoot)

        session.destroy()
        xcalarApi.setSession(None)
        session = None
        xcalarApi = None

    #
    # Clean out the XlrRoot mounted
    #
    tfile.close()
    os.unlink(tFileName)
