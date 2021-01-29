#!/usr/bin/env python3.6
"""
    Runs the unit test suite specified by the .tst files.
"""

import sys
import os
import subprocess
import signal
import tempfile
import re
import shutil
import io
import getopt
import json
from collections import namedtuple

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, flush=True, **kwargs)

colorAvailable = False
try:
    from termcolor import colored
    colorAvailable = True
except:
    pass

# status can be "pass" "fail" "skip" or None (for a message)
TestResult = namedtuple('TestResult', ['status', 'testnum', 'message'])

# get a mapping between signal numbers and names https://stackoverflow.com/a/2549950
sigDict = dict((k, v) for v, k in reversed(sorted(signal.__dict__.items()))
                        if v.startswith('SIG') and not v.startswith('SIG_'))

istty = sys.stderr.isatty()

def colorize(string, color):
    if istty and colorAvailable:
        return colored(string, color)
    else:
        return string

def tapParser(stream, outfile):
    numTestsRe = "^(\d+)\.\.(\d+)"
    okRe = "^ok\s+(\d+)?\s*?(.*)"
    notOkRe = "^not ok\s+(\d+)?\s*?(.*)"
    directiveRe = "^#"

    numTestsComp = re.compile(numTestsRe)
    okComp = re.compile(okRe)
    notOkComp = re.compile(notOkRe)
    directiveComp = re.compile(directiveRe)

    testNum = 0
    numTests = None

    for line in stream:
        outfile.write(line)

        result = None

        # XXX the below pattern runs the regex twice unnecessarily
        if numTestsComp.match(line):
            m = numTestsComp.match(line)
            groups = m.groups()
            numTests = groups[1]
        if okComp.match(line):
            m = okComp.match(line)
            groups = m.groups()
            message = groups[1]
            if "# skip" in message.lower():
                result = TestResult(status="skip", message=message, testnum=testNum)
            else:
                result = TestResult(status="pass", message=message, testnum=testNum)
            testNum += 1
        elif notOkComp.match(line):
            m = notOkComp.match(line)
            groups = m.groups()
            result = TestResult(status="fail", message=groups[1], testnum=testNum)
            testNum += 1
        elif directiveComp.match(line):
            m = directiveComp.match(line)
            result = TestResult(status=None, message=line, testnum=None)

        if result:
            yield result

class TestRunner():
    def __init__(self, dryRun=False, group='all'):
        self.topDir = "."
        self.dryRun = dryRun
        self.group = group
        self.curDir = os.path.abspath(self.topDir)
        self.tests = []
        self.collectTests(self.curDir)
        return

    def runSuite(self):
        eprint("TEST_HARNESS_START_PROCESS_TESTS")
        rtn = self.processTests(self.topDir)
        eprint("TEST_HARNESS_END_PROCESS_TESTS")
        return rtn

    def collectTests(self, dirPath):
        """Consumes a .tst file and adds all tests discovered
        to self.tests if they match the specified group or the
        group is 'all'"""

        self.curDir = dirPath
        testPath = os.path.join(dirPath, "sanity.tst")
        with open(testPath, "r") as f:
            try:
                tests, subdirs = self.parseConfig(f)
                if self.group == 'all':
                    self.tests += tests
                else:
                    self.tests += [t for t in tests if self.group == t['group']]
            except:
                eprint("Failed to parse {}".format(testPath))
                raise

        for d in subdirs:
            newDir = os.path.join(dirPath, d)
            if not self.collectTests(newDir):
                return False

        return True

    def processTests(self, dirPath):
        """Executes all enabled tests
        Returns True on success; False on some failure
        """

        for test in self.tests:
            if test.get("enabled", True):
                if not self.executeTest(test):
                    return False
            else:
                self.skipTest(test["name"], "", "Disabled in sanity.tst")

        return True

    def _invokeTapTest(self, test):
        """Returns True for a passed test"""
        command = test["command"]
        with tempfile.NamedTemporaryFile('w+t', encoding="utf-8") as errfile, tempfile.NamedTemporaryFile('w+t', buffering=1, encoding="utf-8") as outfile:
            pasteableCommand = '(cd "{}" && {})'.format(
                    self.curDir,
                    " ".join(command)
                    )
            eprint(pasteableCommand)
            eprint("stderr -> {}".format(errfile.name))
            failingResult = None
            pr = subprocess.Popen(command,
                                  cwd=self.curDir,
                                  stdout=subprocess.PIPE,
                                  stderr=errfile,
                                  bufsize=1,
                                  encoding="utf-8")
            for testResult in tapParser(pr.stdout, outfile):
                self.printTestResult(test, testResult)
                if testResult.status == "fail":
                    failingResult = testResult

            pr.communicate()

            failed = bool(failingResult or pr.returncode != 0)
            if pr.returncode != 0:
                if pr.returncode < 0:
                    signalName = sigDict[-pr.returncode]
                    message = "failed with {}".format(signalName)
                else:
                    message = "returned {}".format(pr.returncode)
                self.failTest(test["name"], "", message)

            if failed:
                outfile.seek(0)
                errfile.seek(0)

                eprint("{} stdout:".format(test["name"]))
                shutil.copyfileobj(outfile, sys.stderr)
                eprint("\n{}\n".format("-*" * 40))
                eprint("{} stderr:".format(test["name"]))
                shutil.copyfileobj(errfile, sys.stderr)
                eprint("\n{}\n".format("-*" * 40))
                return False
        return True

    def _invokeNonTapTest(self, test):
        res = subprocess.run(test["command"], cwd=self.curDir)
        success = res.returncode == 0
        if not success:
            eprint("Test '{}' failed".format(test["name"]))
        return success

    def executeTest(self, test):
        """Returns True for a passed test"""

        self.curDir = test["path"]
        if test["tap"]:
            return self._invokeTapTest(test)
        else:
            return self._invokeNonTapTest(test)

    def printTestResult(self, test, result):
        if result.status == "pass":
            self.passTest(test["name"], result.testnum, result.message)
        elif result.status == "fail":
            self.failTest(test["name"], result.testnum, result.message)
        elif result.status == "skip":
            self.skipTest(test["name"], result.testnum, result.message)
        elif result.status is None:
            eprint(result.message)

    def skipTest(self, testname, testnum, message):
        status = "SKIP"
        eprint("{}: {} {}{}".format(
                colorize(status, "yellow"),
                testname,
                testnum,
                message))

    def passTest(self, testname, testnum, message):
        status = "PASS"
        eprint("{}: {} {}{}".format(
                colorize(status, "green"),
                testname,
                testnum,
                message))

    def failTest(self, testname, testnum, message):
        status = "FAIL"
        eprint("{}: {} {}{}".format(
                colorize(status, "red"),
                testname,
                testnum,
                message))

    def listGroups(self):
        eprint("TEST_HARNESS_START_LIST_GROUPS")
        print(' '.join(set([t['group'] for t in self.tests])))
        eprint("TEST_HARNESS_END_LIST_GROUPS")
        return True

    def listTests(self):
        eprint("TEST_HARNESS_START_LIST_TESTS")
        for t in self.tests:
            print(t)
        eprint("TEST_HARNESS_END_LIST_TESTS")
        return True

    def getGroupCmd(self, ts):
        """ Returns group name and command. the group is
        stripped of the leading @"""
        if ts[0].startswith("@"):
            return (ts[0][1:], ts[1:])
        else:
            return ('default',ts[0:])

    def parseConfig(self, cfg):
        tests = []
        subdirs = []
        for line in cfg:
            if not line or line.startswith("#"):
                continue
            tokens = line.split()
            cmd = tokens[0]
            if cmd == "addtaptest":
                test = {}
                group, testCmd = self.getGroupCmd(tokens[1:])
                test["name"] = os.path.basename(testCmd[0])
                test["command"] = testCmd
                test["group"] = group
                test["path"] = self.curDir
                test["tap"] = True
                tests.append(test)
            elif cmd == "addtest":
                test = {}
                group, testCmd = self.getGroupCmd(tokens[1:])
                test["name"] = os.path.basename(testCmd[0])
                test["command"] = testCmd
                test["group"] = group
                test["path"] = self.curDir
                test["tap"] = False
                tests.append(test)
            elif cmd == "addskip":
                test = {}
                group, testCmd = self.getGroupCmd(tokens[1:])
                test["name"] = os.path.basename(testCmd)
                test["command"] = testCmd
                test["enabled"] = False
                test["path"] = self.curDir
                test["group"] = group
                tests.append(test)
            elif cmd == "subdir":
                subdir = tokens[1]
                subdirs.append(subdir)
            else:
                raise ValueError("test command {} not recognized".format(cmd))

        return tests, subdirs

def main(argv):
    # fix up any http proxy which may be set up
    # This sometimes intercepts http thrift APIs, returning an error message.
    os.environ["http_proxy"] = ""
    os.environ["https_proxy"] = ""
    group = os.environ.get('SANITY_GROUP', 'all')
    dryRun = False
    groupNames = False
    ret = 0

    try:
        opts, args = getopt.getopt(argv[1:],"hlng:")
    except getopt.GetoptError:
        print ('%s -hnj' % argv[0] )
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('test-harness.py' )
            print ('  -h help')
            print ('  -l list all tests or only those selected by -g')
            print ('  -n outputs all group names')
            print ('  -g <group> includes only tests labeled @group (note the @)')
            return 0
        elif opt == '-g':
            group = arg
        elif opt == '-l':
            dryRun = True
        elif opt == '-n':
            groupNames = True

    runner = TestRunner(group=group)

    if dryRun:
        ret = runner.listTests()
    elif groupNames:
        ret = runner.listGroups()
    else:
        ret = runner.runSuite()

    if ret:
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv))
