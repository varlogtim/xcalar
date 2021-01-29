# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from helpers import executionHelper as ExecutionHelper

class Lib2to3Helper(object):

    def __init__(self, verbose = False):
        self.verbose = verbose

    def does2To3ExistOnThisMachine(self):
        executor = ExecutionHelper.ExecutionHelper(verbose = self.verbose)

        command = "2to3 --help"
        cmdInfo = executor.executeCommandAndReturnCmdInfo(command = command)
        if (cmdInfo.getExitCode() != 0):
            print("2to3 does not exist on this machine!")
            return (False)

        return (True) # success

    # caller needs to ensure pathToUdf exists
    def run2to3OnThisUdf(self, pathToUdf):
        executor = ExecutionHelper.ExecutionHelper(verbose = self.verbose)

        command = "2to3 -w " + pathToUdf
        cmdInfo = executor.executeCommandAndReturnCmdInfo(command = command)
        if (cmdInfo.getExitCode() != 0):
            # 2to3 does not provide non zero exit code
            # and there is no doc on return code either
            print("Unable to run 2to3 on: " + pathToUdf)

        return (cmdInfo)
