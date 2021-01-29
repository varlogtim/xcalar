#!/usr/bin/env python
#
# This script should use the system python, because it may be called outside of
# a functioning Xcalar environment.
#
from __future__ import print_function

import sys
import os
import shutil
import tempfile
import glob
import errno
import time
import traceback
import subprocess
from subprocess import Popen, PIPE
from datetime import datetime

secondsInHour = 3600

# Xcalar config file as a dictionary
xcalarConfig = None

# Xcalar binaries to collect core dumps for.
xcalarDumpBins = ['usrnode', 'xcmgmtd', 'childnode', 'xcmonitor']

# Useful subdirectories of xcalar root.
xcalarRootDirs = ['kvs', 'published', 'workbooks']

# Stats subdirectory in xcalar root.
xcalarStatsDir = 'DataflowStatsHistory'

# Number of hours of system stats that will be added to the support bundle
numHoursStats = 24

xcalarLogs = ['/var/log/syslog*', '/var/log/messages*']

xcalarRelConfigFiles = [
    'etc/xcalar/*', 'etc/hdfs/*', 'etc/*-release', 'etc/*/xcalar'
]

procFiles = ['/proc/meminfo', '/proc/cpuinfo']

# Path to this system's core dump files.
# XXX Handle non-ubuntu OS.
sysDumpPaths = [
    '/core.{0}.*', '/cores/core.{0}.*', '/var/crash/core.{0}.*',
    '%s/core.{0}.*' % os.getcwd(),
    '%s/core.{0}.*' % os.getenv('XCE_WORKDIR', '/var/tmp/xcalar-root')
]
configStatsPath = ''
scriptDir = os.path.dirname(os.path.realpath(__file__))
xcasup = os.path.join(scriptDir, 'xcasup.sh')
systemInfo = os.path.join(scriptDir, 'system-info.sh')

# Let the user override the standard temp dir via TMPDIR [1]
# We don't hardcode this to /tmp, because /tmp is often backed
# by tmpfs (RAM) and is limited in size. Since we'll potentially be
# staging very large tar files with core dumps in them, we have to
# use a saner default (/var/tmp) and let the user override it. It's
# also desirable to use /var/tmp to prevent a reboot from wiping out
# any failed support bundles.
#
# [1] http://pubs.opengroup.org/onlinepubs/009695399/basedefs/xbd_chap08.html
if 'XCE_ASUP_TMPDIR' in os.environ:
    tmpDir = os.path.join(os.getenv('XCE_ASUP_TMPDIR'), 'xcalar')
else:
    tmpDir = os.path.join(os.getenv('TMPDIR', '/var/tmp'), 'xcalar')


def wrapper_exception(fn):
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            print("Function Args: {}".format(args))
            print("Function Keyword Args: {}".format(kwargs))
            print(traceback.print_tb(sys.exc_info()[2], 10), file=sys.stderr)

    return inner


@wrapper_exception
def scopy(src, dst):
    shutil.copy2(src, dst)


@wrapper_exception
def smove(src, dst):
    shutil.move(src, dst)


@wrapper_exception
def scopytree(src, dst):
    if os.path.exists(src):
        shutil.copytree(src, dst)


@wrapper_exception
def srmtree(d):
    shutil.rmtree(d)


def mkdirp(d):
    try:
        os.makedirs(d)
    except OSError as e:
        # May have multiple nodes trying to create this at once.
        if e.errno != errno.EEXIST:
            print(traceback.print_tb(sys.exc_info()[2], 10), file=sys.stderr)
            raise


# List of tuples [(src, dst), (src, dst), ...]
supportBundleFilePaths = []
supportBundleDirPaths = []
supportBundleStatsDirPaths = []
supportBundleCorePaths = []


def findFreeSpaceOnTemp(tmpDir):
    """
    Walk up dirs till we find the mount point of the tmp dir then
    find the free space for that mount.
    """
    path = os.path.abspath(tmpDir)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    total, used, free = shutil.disk_usage(path)
    return free


def findSizeOfXcalarCreatedDir(xcalarCreatedDir):
    """
    Find the size of a Xcalar created directory. The distinction is that
    we know the Xcalar created directories are of a finite size.
    """
    duCmd = 'du -sb {}'.format(xcalarCreatedDir)
    duCmd_P = subprocess.run(duCmd, shell=True, stdout=PIPE, stderr=PIPE)
    if duCmd_P.returncode == 0:
        try:
            return int(duCmd_P.stdout.decode('utf-8').split()[0])
        except ValueError:
            return 1    # Possible that we can't cast to int
    else:
        print("Could not determine size of {}. ERR: {}".format(
            xcalarCreatedDir, duCmd_P.stderr.decode('utf-8'), file=sys.stderr))
        # If we couldn't find the size, say it's 1 byte.
        return 1


@wrapper_exception
def findSizeOfFile(filePath):
    return os.path.getsize(filePath)


def estimateSpaceNeededForBundle(filePaths, dirPaths, corePaths):
    totalSize = 0
    for fp in filePaths:
        fileSize = findSizeOfFile(fp[0])
        if fileSize is not None:
            totalSize += fileSize

    for dp in dirPaths:
        totalSize += findSizeOfXcalarCreatedDir(dp[0])

    for cp in corePaths:
        coreSize = findSizeOfFile(cp[0])
        if fileSize is not None:
            totalSize += coreSize

    return totalSize * 2    # assume 0% compression

# XXX: For future reference
#
# def backtick(cmd):
#     args = ['/bin/bash','-c']
#     if type(cmd) == type([]):
#         args.extend(cmd)
#     else:
#         args.append(cmd)
#     proc = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     stdout, stderr = proc.communicate()
#     return proc.returncode, stdout, stderr
#
# Execute a command and return stdout as a string if the command succeeded
# eg, configStatsDir = bconeline([r"sed -n -E -e 's/^\s*Constants.XcalarStatsPath=(.*)$/\1/p' %s" % configFilePath])
#
# def bconeline(cmd):
#     res, stdout, _ = backtick(cmd)
#     if res == 0 and stdout:
#         return stdout.rstrip('\n')
#     return ''


# Load xcalar config file into a dict of dicts
# eg, configStatsDir = conf['Constants']['XcalarStatsPath']
class XcalarConfig(object):
    def __init__(self, configFile):
        self.confdict = dict()
        try:
            for line in open(configFile, 'rt').read().split('\n'):
                if line.startswith('//'):
                    continue
                kv = line.rstrip(' ').split('=')
                if len(kv) == 2:
                    keys = kv[0].split('.')
                    if len(keys) < 2:
                        continue
                    kvdict = self.confdict
                    for k in keys[0:-1]:
                        if k not in kvdict:
                            kvdict[k] = dict()
                        kvdict = kvdict[k]
                    kvdict[keys[-1]] = kv[1]
        except Exception:
            pass

    def get(self, configKey):
        keys = configKey.split('.')
        if len(keys) < 2:
            return None
        kvdict = self.confdict
        for k in keys[0:-1]:
            if k not in kvdict:
                return None
            kvdict = kvdict[k]
        return kvdict.get(keys[-1], None)


def flatten(l):
    return [item for sublist in l for item in sublist]


def fglob(afg):
    return flatten([glob.glob(fg) for fg in afg])


# Collect config, log, and core dump files. Put in 'payload' directory.
def collect(payloadDir, configFilePath, xcalarRootPath, xcalarLogPath, nodeId,
            version, generateMiniBundle, allNodeIds):
    with open(os.path.join(payloadDir, 'version'), 'w') as versionFile:
        versionFile.write(version)

    # generateMiniBundle is used by the mgmtd test and all it expects is that
    # the path has been created.  So just bail out here.
    if generateMiniBundle:
        print("Generating mini support bundle")
        return

    # System info
    procDir = os.path.join(payloadDir, 'proc')
    os.mkdir(procDir)
    os.system(
        '%s > %s' % (systemInfo, os.path.join(procDir, 'system-info.txt')))
    os.system('uname -a > %s' % os.path.join(procDir, 'uname_a.txt'))
    os.system('mount > %s 2>&1' % os.path.join(procDir, 'mounts.txt'))
    dateFormat = '+%FT%T%t%z'
    os.system('date %s > %s' % (dateFormat, os.path.join(procDir, 'date.txt')))
    for procFile in fglob(procFiles):
        supportBundleFilePaths.append((procFile, procDir))
    # Syslog logs
    syslogDir = os.path.join(payloadDir, 'syslog')
    os.mkdir(syslogDir)

    # Flush in-core stdio(3) buffers for local nodes
    # There can be multiple node-ids only if usrnodes are launched manually
    # In prod / installer based builds, only one node-id will be running

    numNodes = len(allNodeIds)
    for nIx in range(0, numNodes):
        nId = allNodeIds[nIx]
        gP = Popen(
            ['grep', '^Node.' + str(nId) + '.ApiPort', configFilePath],
            stdout=PIPE,
            stderr=PIPE,
            close_fds=True)
        gPout = gP.communicate()
        if gP.returncode != 0:
            print(
                " -> Log flush error (node %s has no ApiPort); check "
                " config file; exit code %d " % (str(nId), gP.returncode),
                file=sys.stderr)
        else:
            gPout = gPout[0].decode("utf-8").strip()    # remove newlines
            apipLn = gPout.split('=')
            apiPort = apipLn[1]
            # set-up xccli cmd for flushing nId's logs
            # send xcclicmd to nId by connecting to its apiPort
            xcclicmd = 'connect localhost ' + str(
                apiPort) + '; loglevelset NoChange FlushLocal'
            xP = Popen(['xccli', '-c', xcclicmd],
                       shell=False,
                       stdout=PIPE,
                       stderr=PIPE,
                       close_fds=True)
            xPout = xP.communicate()
            if "Connection refused" in xPout[0].decode("utf-8") or                        \
               "Connection refused" in xPout[1].decode("utf-8") or xP.returncode != 0:
                print(
                    " -> Log flush error (node %s inaccessible) "
                    " xccli exit code %d" % (str(nId), xP.returncode),
                    file=sys.stderr)

    xcalarLogs.append(xcalarLogPath + "/*")
    for logFile in fglob(xcalarLogs):
        if (os.access(logFile, os.R_OK)):
            supportBundleFilePaths.append((logFile, syslogDir))

    # Capture syslogs via journalctl. Try running as root first, so that if
    # passwordless sudo is configured, we capture all system errors, such as
    # OOM kills. Ideally, we would check for passwordless sudo first, but we
    # haven't found a straightforward way to do this yet. "sudo -l" did not
    # behave as expected.
    print("Attempting to run journalctl as root")
    if (os.system(
            "command -v journalctl && sudo -n journalctl -b > %s/journalctl.log"
            % syslogDir) == 0):
        print("Succeeded!")
    else:
        print(
            "Passwordless sudo is not configured -- running journalctl as xcalar user"
        )
        os.system("command -v journalctl && journalctl -b > %s/journalctl.log"
                  % syslogDir)

    # Copy installed binaries
    binDir = os.path.join(payloadDir, 'bin')
    os.mkdir(binDir)
    for binFile in xcalarDumpBins:
        supportBundleFilePaths.append((os.path.join(
            os.getenv('XLRDIR', '/opt/xcalar'), 'bin', binFile), binDir))

    # Config file
    configDir = os.path.join(payloadDir, 'config')
    os.mkdir(configDir)
    # Copy the given config file to a specially named one, in case /etc/xcalar/default.cfg
    # points to a symlink (like /mnt/xcalar/default.cfg)
    if (os.access(os.path.join(configDir, 'xcalar.cfg'), os.R_OK)):
        supportBundleFilePaths.append((configFilePath,
                                       os.path.join(configDir, 'xcalar.cfg')))

    xcalarConfigFiles = [os.path.join('/', s) for s in xcalarRelConfigFiles]
    localXcalarConfigFiles = [
        os.path.join(os.getenv('XLRDIR', '/opt/xcalar'), s)
        for s in xcalarRelConfigFiles
    ]
    xcalarConfigFiles += localXcalarConfigFiles
    for confFile in fglob(xcalarConfigFiles):
        if (os.access(confFile, os.R_OK)):
            relPath = os.path.relpath(confFile, start="/")
            fullPath = os.path.join(configDir, relPath)
            if (os.path.isfile(confFile)):
                supportBundleFilePaths.append((confFile, fullPath))
            if (os.path.isdir(confFile)):
                supportBundleDirPaths.append((confFile, fullPath))

    # Stats
    if configStatsPath and os.path.exists(configStatsPath):
        statsDir = os.path.join(payloadDir, 'stats')
        supportBundleDirPaths.append((configStatsPath, statsDir))

    # Collect core dumps
    coreDumpDir = os.path.join(payloadDir, 'coreDumps')
    mkdirp(coreDumpDir)
    for xcalarDumpBin in xcalarDumpBins:
        for sysDumpPath in sysDumpPaths:
            for dumpFile in glob.glob(sysDumpPath.format(xcalarDumpBin)):
                supportBundleCorePaths.append((dumpFile, coreDumpDir))

    if int(nodeId) != 0:
        return

    # Xcalar root directory
    xcalarDir = os.path.join(payloadDir, 'xcalarRoot')

    for subdir in xcalarRootDirs:
        supportBundleDirPaths.append((os.path.join(xcalarRootPath, subdir),
                                      os.path.join(xcalarDir, subdir)))

    statsDirPath = os.path.join(xcalarRootPath, xcalarStatsDir)
    cur_ts = int(time.time())

    # add numHoursStats hours of system and dataflow stats.
    for stat_type in ["jobStats", "systemStats"]:
        temp_ts = cur_ts
        for _ in range(numHoursStats):
            tm = datetime.fromtimestamp(temp_ts)
            if stat_type == "systemStats":
                statsDirectory = os.path.join("systemStats",
                                        "{}-{}-{}/".format(tm.year, tm.month, tm.day),
                                        "{}/".format(tm.hour))
            else:
                statsDirectory = os.path.join("jobStats",
                                        "{}-{}-{}/".format(tm.year, str(tm.month).zfill(2), str(tm.day).zfill(2)),
                                        "{}/".format(str(tm.hour).zfill(2)))
            fullPath = os.path.join(statsDirPath, statsDirectory)
            if not os.path.isdir(fullPath):
                temp_ts -= secondsInHour
                continue
            supportBundleStatsDirPaths.append((fullPath, os.path.join(xcalarDir, statsDirectory)))
            temp_ts -= secondsInHour

    # add cluster configs folder
    fullPath = os.path.join(statsDirPath, "configs")
    supportBundleStatsDirPaths.append((fullPath, os.path.join(xcalarDir, "configs")))


# Pack collected files in tar.gz. The 'S' flag to tar ensures
# that sparse coredumps are stored properly.
def pack(destination, source):
    if os.system("tar Sczf %s %s" % (destination, source)) != 0:
        print("Error creating tar archive")
        raise


# Send supportPack to Xcalar.
def send(packFilePath, supportCaseId):
    return os.system('%s %s %s' % (xcasup, packFilePath, supportCaseId))


def main(argv):
    if len(argv) != 10:
        raise ValueError("Incorrect number of arguments.")

    supportId = argv[1]
    nodeId = argv[2]
    configFilePath = argv[3]
    xcalarRootPath = argv[4]
    xcalarLogPath = argv[5]
    version = argv[6]
    generateMiniBundle = False
    if argv[7] == 'true':
        generateMiniBundle = True
    supportCaseId = argv[8]
    allNodeIds = argv[9].split(" ")

    global xcalarConfig
    global configStatsPath
    xcalarConfig = XcalarConfig(configFilePath)
    configStatsPath = xcalarConfig.get('Constants.XcalarStatsPath')
    # Currently no stats so don't spam the user
    # if not configStatsPath:
    #    print >>sys.stderr,'No Constants.XcalarStatsPath defined'

    sharedRoot = False
    if xcalarRootPath.startswith("file://"):
        xcalarRootPath = xcalarRootPath[len("file://"):]

    if xcalarRootPath.startswith("nfs://"):
        xcalarRootPath = xcalarRootPath[len("nfs://"):]
        sharedRoot = True

    sendSupportBundle = False
    if xcalarConfig.get('Constants.SendSupportBundle') == 'true':
        sendSupportBundle = True

    result = 1
    send_result = 1
    mkdirp(tmpDir)
    workingDir = tempfile.mkdtemp(prefix='xcasup', dir=tmpDir)

    try:
        os.chdir(workingDir)
        payloadDir = os.path.join(workingDir, 'supportData')
        mkdirp(payloadDir)

        # Print supportId to file
        with open(os.path.join(payloadDir, 'supportId'), 'w') as supportIdFile:
            supportIdFile.write(supportId)

        # Include Support Case Id in a file
        with open(os.path.join(payloadDir, 'supportCaseId'),
                  'w') as supportCaseIdFile:
            supportCaseIdFile.write(supportCaseId)

        collect(payloadDir, configFilePath, xcalarRootPath, xcalarLogPath,
                nodeId, version, generateMiniBundle, allNodeIds)

        tmpDirFreeSpace = findFreeSpaceOnTemp(tmpDir)
        spaceNeeded = estimateSpaceNeededForBundle(supportBundleFilePaths,
                                                   supportBundleDirPaths + supportBundleStatsDirPaths,
                                                   supportBundleCorePaths)

        if tmpDirFreeSpace <= spaceNeeded:
            raise RuntimeError(
                "Not enough free space to generate bundle. Free tmp ({}) space: {}, "
                "estimated space needed: {}".format(tmpDir, tmpDirFreeSpace, spaceNeeded))

        for fp in supportBundleFilePaths:
            parentDir = os.path.dirname(fp[1])
            if not os.path.exists(parentDir):
                os.makedirs(parentDir, exist_ok=True)
            scopy(fp[0], fp[1])

        for dp in supportBundleDirPaths:
            parentDir = os.path.dirname(dp[1])
            if not os.path.exists(parentDir):
                os.makedirs(parentDir, exist_ok=True)
            scopytree(dp[0], dp[1])

        for cp in supportBundleCorePaths:
            smove(cp[0], cp[1])

        for dp in supportBundleStatsDirPaths:
            scopytree(dp[0], dp[1])

        packFilePath = os.path.join(workingDir,
                                    'supportPack' + supportId + '.tar.gz')
        pack(packFilePath, 'supportData')
        if sendSupportBundle:
            send_result = send(packFilePath, supportCaseId)
            if send_result == 0:
                # supportGenerateInternal scans for this message
                print(
                    "Successfully uploaded support bundle: %s" % packFilePath)
        else:
            print("Sending support bundle is disabled %s" % packFilePath)

        # Create directory to backup support files locally.
        supportBackupRoot = os.path.join(xcalarRootPath, 'support')
        mkdirp(supportBackupRoot)
        supportBackupSupportId = os.path.join(supportBackupRoot, supportId)
        mkdirp(supportBackupSupportId)
        supportBackupNode = os.path.join(supportBackupSupportId, nodeId)
        mkdirp(supportBackupNode)
        shutil.copy(packFilePath, supportBackupNode)

        result = 0
    except OSError as e:
        # 28: Out of disk space. So, this can occur from the os.mkdir calls
        # in collect() when the system-info.sh fills up the disk. This usually
        # only occurs when we have a very small amount of free space. Rather
        # than have a static size lets just catch and clean up.
        if e.errno == 28:
            print("Drive filled. Cleaning up working dir", file=sys.stderr)
            print(traceback.print_tb(sys.exc_info()[2], 10), file=sys.stderr)
            os.chdir('/tmp')
            srmtree(workingDir)
        result = 1
    except Exception as e:
        result = 1
        print("Unexpected error:", str(sys.exc_info()), file=sys.stderr)
        print(traceback.print_tb(sys.exc_info()[2], 10), file=sys.stderr)
    if result == 0:
        # supportGenerateInternal scans for this message
        print("Successfully generated support bundle.")
        os.chdir('/tmp')
        srmtree(workingDir)
    else:
        print("Support bundle generation failed with error code: " +
              str(result))
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv))
