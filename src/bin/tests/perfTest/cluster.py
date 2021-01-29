import os
import logging
import subprocess
import shutil
import shlex
import time
import tempfile
import math
from distutils.dir_util import copy_tree
from xcalar.compute.util.config import build_config

scriptDir = os.path.dirname(os.path.realpath(__file__))

cliBin = os.path.abspath(os.path.join(scriptDir, "../../../../bin/xccli"))
launcherPath = os.path.abspath(
    os.path.join(scriptDir, "../../usrnode/launcher.sh"))
dockerPkgPath = os.path.abspath(
    os.path.join(scriptDir, "../../../../pkg/docker"))

# Constants
ipAddrKeyFmt = "Node.{nodeId}.IpAddr"
apiPortKeyFmt = "Node.{nodeId}.ApiPort"
numNodesKey = "Node.NumNodes"


def execute(cmd):
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = process.communicate()
    retCode = process.returncode
    return (retCode, stdout, stderr)


class Cluster(object):
    def __init__(self, logging, cfgFile, sessionName):
        self.cfgFile = cfgFile
        self.configDict = build_config(cfgFile).all_options
        self.sessionName = sessionName
        self.logging = logging

        self.url = self.configDict[ipAddrKeyFmt.format(nodeId=0)]
        self.port = self.configDict[apiPortKeyFmt.format(nodeId=0)]

        self.containerName = "perftest_xcservice"
        # These can be arbitrary since we are the only user on this cluster
        self.userName = "perftest"
        self.userIdUnique = 44442222

        self.clusterWaitTimeoutInSec = 600

        self.cliBin = cliBin
        self.logging.info("Node Url: '{}:{}'".format(self.url, self.port))

    def __enter__(self):
        try:
            self.startCluster()

            newSessionCmd = "session --new --name {}".format(self.sessionName)
            self.runXcCmd(newSessionCmd)
        except Exception:
            self.stopCluster()
            raise
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        self.stopCluster()

    def waitForClusterUp(self):
        self.logging.info(
            "Waiting for cluster to come up... ({}sec timeout)".format(
                self.clusterWaitTimeoutInSec))
        retryWait = 10
        timeoutMessages = 10
        numRetries = math.ceil(self.clusterWaitTimeoutInSec / retryWait)

        timeoutsPrinted = 0
        success = False
        for tryNum in range(0, numRetries):
            (retCode, retString, retErr) = self.runXcCmd("version")
            if b'Error' not in retString:
                success = True
                break

            # Print still-waiting message
            progress = tryNum / numRetries
            if float(timeoutsPrinted) / timeoutMessages < progress:
                self.logging.info("({:2.2f}%) Still waiting...".format(
                    100 * progress))
                timeoutsPrinted = math.ceil(progress * timeoutMessages)
            time.sleep(retryWait)

        if success:
            self.logging.info("Cluster up")
        else:
            errStr = "Failed to bring up cluster after {} seconds".format(
                self.clusterWaitTimeoutInSec)
            self.logging.error(errStr)
            raise ValueError(errStr)
        return

    def runXcCmd(self, xcCmd):
        escapedCmd = xcCmd.replace('\\', '\\\\').replace('"', '\\"')
        cliBin = ""
        cmd = "{cli} --noprettyprint -i \"{url}\" -o {port} -u {username} -k {userIdUnique} -c \"{cmd}\"".format(
            cli=self.cliBin,
            url=self.url,
            port=self.port,
            username=self.userName,
            userIdUnique=self.userIdUnique,
            cmd=escapedCmd)
        (retCode, retString, retErr) = execute(shlex.split(cmd))
        self.logging.debug(
            "Output for command '{}'(ret={}):\nstdout:\n{}\nstderr:\n{}'".
            format(cmd, retCode, retString, retErr))
        return (retCode, retString, retErr)


class LocalCluster(Cluster):
    def __init__(self, logging, cfgFile, installerPath, sessionName):
        super(LocalCluster, self).__init__(logging, cfgFile, sessionName)
        # Override the config file
        self.url = "localhost"
        self.port = "18552"

    def startCluster(self):
        self.logging.debug("Starting cluster")
        xcalardir = "/var/opt/xcalar"

        for fileObj in [
                os.path.join(xcalardir, t) for t in os.listdir(xcalardir)
        ]:
            if os.path.isdir(fileObj):
                shutil.rmtree(fileObj)
            else:
                os.remove(fileObj)

        launcherCmd = "{launcher} 3 daemon".format(launcher=launcherPath)
        self.logging.debug(execute(launcherCmd.split()))
        # wait for cluster to start
        self.waitForClusterUp()
        return

    def stopCluster(self):
        self.logging.debug("Stopping cluster")
        murderCmd = "killall -TERM usrnode xcmgmtd childnode"
        execute(murderCmd.split())
        return


class RemoteNode(object):
    def __init__(self, logging, username, hostname):
        self.logging = logging
        self.username = username
        self.hostname = hostname

        self.remoteTmpDir = "/tmp/perfTest/"
        self.usrnodeUp = False
        self.hadUsrnode = None
        self.hadApache = None

        self.timeoutInSec = 600

    def execute(self, cmd):
        cmd = [
            "timeout", "{}s".format(self.timeoutInSec), "ssh",
            "-oBatchMode=yes", "-oStrictHostKeyChecking=no", "{}@{}".format(
                self.username, self.hostname), cmd
        ]
        self.logging.debug("Executing on {}:\n$ {}".format(
            self.hostname, " ".join(cmd)))
        (retCode, retString, retErr) = execute(cmd)
        self.logging.debug(retString)
        if retCode != 0:
            self.logging.debug(("Remote command failed on {user}@{host}:\n"
                                "command='{cmd}'\n\n"
                                "stdout='{output}'").format(
                                    user=self.username,
                                    host=self.hostname,
                                    cmd=" ".join(cmd),
                                    output=retString))
        return (retCode, retString, retErr)

    def copy(self, localDir):
        cmd = [
            "scp", "-oBatchMode=yes", "-oStrictHostKeyChecking=no", "-r",
            localDir, "{user}@{hostname}:{tempDir}".format(
                user=self.username,
                hostname=self.hostname,
                tempDir=self.remoteTmpDir)
        ]
        self.logging.debug("Executing on {}:\n$ {}".format(
            self.hostname, " ".join(cmd)))
        execute(cmd)


class RemoteCluster(Cluster):
    def __init__(self, logging, cfgFile, installerPath, username, sessionName):
        """Assumes that username is globally valid to ssh into the cluster"""
        super(RemoteCluster, self).__init__(logging, cfgFile, sessionName)

        self.installerPath = installerPath

        self.remoteConnections = []
        numNodes = int(self.configDict[numNodesKey])
        for nodeId in range(0, numNodes):
            hostname = self.configDict[ipAddrKeyFmt.format(nodeId=nodeId)]
            self.logging.debug("Node {} hostname: '{}'".format(
                nodeId, hostname))
            conn = RemoteNode(self.logging, username, hostname)
            self.remoteConnections.append(conn)

        self.logging.debug("Installer Path: '{}'".format(self.installerPath))

    def prepareNode(self, conn):
        # Clean up a previous run of this test
        logging.info("   Cleaning up any previous perfTest runs")
        self.cleanEnvironment(conn)

        # We need to stop apache to reclaim port 80
        apacheTestScript = """
            if [ -e /etc/init.d/apache2 ]; then
                /etc/init.d/apache2 status
            else
                exit 1
            fi
        """
        retTuple = conn.execute(apacheTestScript)
        conn.hadApache = retTuple[0] == 0
        if conn.hadApache:
            logging.info("   Stopping apache")
            apacheStopScript = """
                sudo service apache2 stop
            """
            retTuple = conn.execute(apacheStopScript)
            if retTuple[0] != 0:
                self.logging.error("Unable to stop apache on {host}".format(
                    host=conn.hostname))
                raise ValueError("Unable to stop apache")

        sentinalVal = 123
        usrnodeTestScript = """
            if [ -e /etc/init.d/xcalar ]; then
                /etc/init.d/xcalar status
            else
                exit {}
            fi
        """.format(sentinalVal)
        retTuple = conn.execute(usrnodeTestScript)
        # This is highly dependent on the implementation of /etc/init.d/xcalar
        conn.hadUsrnode = retTuple[0] != sentinalVal and retTuple[0] != 27
        if conn.hadUsrnode:
            self.logging.info("   Stopping usrnode")
            usrnodeStopScript = """
                sudo service xcalar stop
            """
            retTuple = conn.execute(usrnodeStopScript)
            if retTuple[0] != 0:
                self.logging.error(
                    "Unable to stop usrnode on {host}:{stderr}".format(
                        host=conn.hostname, stderr=retTuple[2]))
                raise ValueError("Unable to stop usrnode")

    def startCluster(self):
        dirpath = tempfile.mkdtemp()
        try:
            configFilename = "perfTest.cfg"
            installerFilename = "xcalarInstaller"
            configPath = os.path.join(dirpath, configFilename)
            installerPath = os.path.join(dirpath, installerFilename)

            for ii, conn in enumerate(self.remoteConnections):
                self.logging.info("  Node {} ({}) pre-install prep:".format(
                    ii, conn.hostname))
                self.prepareNode(conn)
                self.logging.info("    Done")

            for ii, conn in enumerate(self.remoteConnections):
                self.logging.info("  Node {} ({}):".format(ii, conn.hostname))
                # Copy over all necessary files
                copy_tree(dockerPkgPath, dirpath)
                self.logging.debug("installer path is " + self.installerPath)
                shutil.copyfile(self.installerPath, installerPath)

                shutil.copyfile(self.cfgFile, configPath)

                self.logging.info("    uploading installer")
                conn.copy(dirpath)

                # We can now start the node
                self.logging.info("    starting node")

                startClusterScript = """
                    cd {tmpDir};
                    export CONTAINER_NAME="{containerName}"
                    export INSTALLER_PATH="{installerPath}"
                    export DEFAULT_CFG_PATH="{cfgPath}"
                    make runWithCfg
                """.format(
                    tmpDir=conn.remoteTmpDir,
                    installerPath=os.path.join(conn.remoteTmpDir,
                                               installerFilename),
                    cfgPath=os.path.join(conn.remoteTmpDir, configFilename),
                    containerName=self.containerName)
                retTuple = conn.execute(startClusterScript)
                if retTuple[0] != 0:
                    self.logging.error("Unable to start containerized "
                                       "usrnode on {host}:\n{stderr}".format(
                                           host=conn.hostname,
                                           stderr=retTuple[2]))
                    raise ValueError("Unable to start usrnode")
                conn.usrnodeUp = True
            self.waitForClusterUp()
        finally:
            shutil.rmtree(dirpath)

        return

    def cleanEnvironment(self, conn):
        scpCleanStr = "rm -rf {tempDir}".format(tempDir=conn.remoteTmpDir)
        conn.execute(scpCleanStr)
        self.logging.debug("  Stopping usrnode")
        dockerStopStr = "docker stop {containerName}".format(
            containerName=self.containerName)
        conn.execute(dockerStopStr)

    def stopCluster(self):
        self.logging.info("Safely stopping cluster...")
        for ii, conn in enumerate(self.remoteConnections):
            self.logging.info("  Node {} ({}) clean environment:".format(
                ii, conn.hostname))
            scpCleanStr = "rm -rf {tempDir}".format(tempDir=conn.remoteTmpDir)
            conn.execute(scpCleanStr)
            if conn.usrnodeUp:
                self.logging.debug("  Stopping usrnode")
                dockerStopStr = "docker stop {containerName}".format(
                    containerName=self.containerName)
                conn.execute(dockerStopStr)
                conn.usrnodeUp = False

            if conn.hadApache:
                apacheStartScript = """
                    sudo service apache2 start
                """
                logging.info("  Restarting original apache")
                conn.execute(apacheStartScript)
            self.logging.info("    Done")

        for ii, conn in enumerate(self.remoteConnections):
            # Do this in bulk last, to avoid cross-cluster-confusion
            if conn.hadUsrnode:
                usrnodeStartScript = """
                    sudo service xcalar start
                """
                self.logging.info(
                    "  Node {} ({}) restarting host usrnode:".format(
                        ii, conn.hostname))
                conn.execute(usrnodeStartScript)
                self.logging.info("    Done")
        self.logging.info("Cluster stopped")
        return
