import sys
import os
import subprocess
import shlex
import shutil

testCaseSpecifications = {
    'Enabled': False,
    'Name': 'XcalarSim',
    'Description': 'Use Xcalar Sim to run random queries',
    'ExpectedRunTime': 100,    # In seconds for 1 run
    'NumRuns': 10,    # Number of times a given user will invoke main()
    'SingleThreaded': False,
    'ExclusiveSession': False,
}

myDir = os.path.dirname(os.path.abspath(__file__))
xlrDir = myDir + "/../../../../../.."
jsonGenDir = xlrDir + "/src/misc/jsongenondisk"
xcalarSimDir = xlrDir + "/src/misc/xcalarsim"

xcalarSimOutputDirPrefix = "/tmp/xcalarsim"


def formatQuery(logging, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def globalPrepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    return TestCaseStatus.Pass


def prepare(logging, xcInstance, TestCaseStatus, testSpec=None):
    xcalarSimOutputDir = xcalarSimOutputDirPrefix + "/" + xcInstance.username
    if not os.path.exists(xcalarSimOutputDir):
        os.makedirs(xcalarSimOutputDir)
    xcalarSimDatasetDir = xcalarSimOutputDir + "/dataset"
    if not os.path.exists(xcalarSimDatasetDir):
        os.makedirs(xcalarSimDatasetDir)
    jsonGenCmd = "%s/jsongenondisk -o %s -z 1MB --mute" % (jsonGenDir,
                                                           xcalarSimDatasetDir)
    logging.debug(jsonGenCmd)
    args = shlex.split(jsonGenCmd)
    try:
        output = subprocess.check_output(args)
        logging.debug(output)
    except Exception:
        logging.error("Failed to invoke:\n%s" % jsonGenCmd)
        raise

    return TestCaseStatus.Pass


def main(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    xcalarSimOutputDir = xcalarSimOutputDirPrefix + "/" + xcInstance.username
    xcalarSimDatasetDir = xcalarSimOutputDir + "/dataset"
    xcSimCmd = "%s/xcalarsim --dontspawnnodes -d %s -p %d -n %s -k %d -o %s -g %s/src/bin/usrnode/test-config.cfg -j %s/outputFile0.json -s %s/schemaFile.cfg --mute" % (
        xcalarSimDir, xcInstance.nodeUrl, xcInstance.nodePort,
        xcInstance.username, xcInstance.userIdUnique, xcalarSimOutputDir,
        xlrDir, xcalarSimDatasetDir, xcalarSimDatasetDir)
    logging.debug(xcSimCmd)
    args = shlex.split(xcSimCmd)
    try:
        output = subprocess.check_output(args, stderr=subprocess.STDOUT)
        logging.info(output)
    except Exception:
        logging.error("Failed to invoke:\n%s" % xcSimCmd)
        raise
    return TestCaseStatus.Pass


def verify(logging, runNum, xcInstance, TestCaseStatus, testSpec=None):
    query = "drop table \"xcalarSim/*\""
    xcInstance.runQuery(query)
    return TestCaseStatus.Pass


def cleanup(logging, xcInstance, finalStatus, TestCaseStatus):
    query = "drop dataset \".XcalarDS./xcalarSim/*\""
    xcInstance.runQuery(query)
    xcalarSimOutputDir = xcalarSimOutputDirPrefix + "/" + xcInstance.username
    if finalStatus == TestCaseStatus.Pass:
        try:
            shutil.rmtree(xcalarSimOutputDir)
        except Exception:
            logging.error("Could not delete %s\n" % xcalarSimOutputDir)
            raise


def globalCleanUp(logging, xcInstance, TestCaseStatus):
    return TestCaseStatus.Pass


if __name__ == "__main__":
    print("This test case should not be executed as a standalone")
    sys.exit(0)
