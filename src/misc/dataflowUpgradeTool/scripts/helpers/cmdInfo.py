# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

class CmdInfo(object):
    def __init__(self, exitCode = 1, stdOut = [], stdErr = []):
        self.exitCode = exitCode
        self.stdOut = stdOut
        self.stdErr = stdErr

    def getExitCode(self):
        return (self.exitCode)

    def getStdout(self):
        return (self.stdOut)

    def getStderr(self):
        return (self.stdErr)

    def setStderr(self, exitCode = 1, stdErr = None):
        self.exitCode = exitCode
        self.stdErr = stdErr
        assert(self.stdErr is not None)

    def setExitCodeStdOutAndErr(self, exitCode = None, stdOut = None, stdErr = None):
        assert(exitCode is not None)
        assert(stdOut is not None)
        assert(stdErr is not None)

        self.exitCode = exitCode
        self.stdOut = stdOut
        self.stdErr = stdErr
