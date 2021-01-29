# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from helpers import tempHelper as TempHelper

class XcConfigHelper(object):

    def __init__(self, verbose = False):
        self.verbose = verbose
        self.tempHelper = TempHelper.TempHelper(verbose = verbose)

        self.cfgFileHandle = None
        self.logDirectory = None
        self.xcalarRoot = None

    def createConfigFile(self, args = None):

        tempDirectory = self.tempHelper.createTempDirectory()
        if (tempDirectory is None):
            return (None)

        cfgFile = self.tempHelper.createNamedTempFile()
        if (cfgFile is None):
            self.tempHelper.deleteDirectory(tempDirectory)
            return (None)

        assert(self.cfgFileHandle is None)
        assert(self.logDirectory is None)
        assert(self.xcalarRoot is None)

        if (args is None):
            # Use some bogus value that nobody is using
            thriftPort = 1234
            portNumber = 1235
            apiPortNumber = 1236
            monitorPortNumber = 1237

        cfgParams = ""
        cfgParams += "Constants.XcalarRootCompletePath=%s\n" % tempDirectory
        cfgParams += "Constants.XcalarLogCompletePath=%s\n" % tempDirectory
        cfgParams += "Constants.XdbSerDesMode=2\n"
        cfgParams += "Constants.MaxInteractiveDataSize=1073741824\n"

        cfgParams += "Thrift.Port=%s\n" % thriftPort
        cfgParams += "Node.NumNodes=1\n"
        cfgParams += "Node.0.IpAddr=127.0.0.1\n"
        cfgParams += "Node.0.Port=%s\n" % portNumber
        cfgParams += "Node.0.ApiPort=%s\n" % apiPortNumber
        cfgParams += "Node.0.MonitorPort=%s\n" %monitorPortNumber

        if (self.verbose is True):
            print("Config params for upgradeTool:\n %s" % cfgParams)

        success = self.tempHelper.writeToTempFile(
            tempFile = cfgFile, contents = cfgParams)
        if (success is False):
            print("Unable to write contents of upgrade tool config file")
            self.tempHelper.closeAndDeleteFile(cfgFile)
            self.tempHelper.deleteDirectory(tempDirectory)
            return (None)

        success = self.tempHelper.flushTempFileContents(cfgFile)
        if (success is False):
            print("Unable to write contents of upgrade tool config file")
            self.tempHelper.closeAndDeleteFile(cfgFile)
            self.tempHelper.deleteDirectory(tempDirectory)
            return (None)

        if (self.verbose is True):
            print("Successfully created cfgFile: %s" % cfgFile.name)

        self.cfgFileHandle = cfgFile
        self.logDirectory = self.xcalarRoot = tempDirectory
        return (cfgFile)

    def cleanUp(self):
        self.tempHelper.cleanUp()
        # all these are tracked in tempHelper it'll cleanup
        self.cfgFileHandle = self.logDirectory = self.xcalarRoot = None
