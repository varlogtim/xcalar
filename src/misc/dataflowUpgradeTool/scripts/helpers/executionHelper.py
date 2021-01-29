# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import subprocess

from helpers import cmdInfo as CmdInfo
from helpers import tempHelper as TempHelper

class ExecutionHelper(object):

    def __init__(self, verbose = False):
        self.verbose = verbose

    def executeCommandAndReturnCmdInfo(self, command,
            cwd = None, bufsize = 0, encoding = "utf-8"):

        cmdInfo = CmdInfo.CmdInfo()

        if (self.verbose is True):
            print("Executing: " + command)

        tempHelper = TempHelper.TempHelper(verbose = self.verbose)
        stdOut = tempHelper.createNamedTempFile()
        stdErr = tempHelper.createNamedTempFile()
        if ((stdOut is None) or (stdErr is None)):
            print("Unable to execute: " + command)

            cmdInfo.setStderr(
                stdErr = ['Unable to create temp files to get stdout/stderr'])
            return (cmdInfo)

        assert(isinstance(command, str))

        try:
            p = subprocess.Popen(command,
                             shell = True,
                             cwd = cwd,
                             stdout = stdOut,
                             stderr = stdErr,
                             bufsize = bufsize)
            exitCode = p.wait()
        except Exception as e:
            message = "Unable to execute " + command + ": " + str(e)
            print(message)
            cmdInfo.setStderr(stdErr = message)
            tempHelper.closeAndDeleteFile(stdOut)
            tempHelper.closeAndDeleteFile(stdErr)
            return (cmdInfo)

        cmdInfo.setExitCodeStdOutAndErr(exitCode = exitCode,
            stdOut = tempHelper.getAllContents(stdOut, encoding=encoding),
            stdErr = tempHelper.getAllContents(stdErr, encoding=encoding))

        if (self.verbose is True):
            print(command)
            print("Stdout: " + ''.join(tempHelper.getAllContents(stdOut, encoding=encoding)))
            print("Stderr: " + ''.join(tempHelper.getAllContents(stdErr, encoding=encoding)))

        tempHelper.closeAndDeleteFile(stdOut)
        tempHelper.closeAndDeleteFile(stdErr)

        return (cmdInfo)
