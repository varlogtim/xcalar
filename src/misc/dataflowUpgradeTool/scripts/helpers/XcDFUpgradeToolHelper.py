# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from helpers import executionHelper as ExecutionHelper

class XcDFUpgradeToolHelper(object):

    # caller needs to ensure the both files exists for the duration
    # of this object, and both needs to be absolute paths
    def __init__(self, verbose = False,
            pathToUpgradeToolBinary = "", pathToXcCfgFile = ""):

        self.verbose = verbose
        self.pathToUpgradeToolBinary = pathToUpgradeToolBinary
        self.pathToXcCfgFile = pathToXcCfgFile

        assert(len(self.pathToUpgradeToolBinary) > 0)
        assert(len(self.pathToXcCfgFile) > 0)

    def convertQueryTo13(self,
            dataflowInputFile = "", dataflowOutputFile = ""):

        executor = ExecutionHelper.ExecutionHelper(verbose = self.verbose)

        command = "%s --cfg \"%s\" --dataflowInputFile \"%s\" --dataflowOutputFile \"%s\""\
            % (self.pathToUpgradeToolBinary, self.pathToXcCfgFile,
                dataflowInputFile, dataflowOutputFile)

        if (self.verbose is True):
            print("Executing: %s" % command)

        return (executor.executeCommandAndReturnCmdInfo(command = command))
