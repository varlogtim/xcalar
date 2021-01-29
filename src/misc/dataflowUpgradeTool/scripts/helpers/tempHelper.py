# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import tempfile
import shutil
import os

class TempHelper(object):

    def __init__(self, verbose = False):
        self.verbose = verbose
        self.listOfOpenTempFileHandles = []
        self.listOfNewTempDirNames = []

    def __del__(self):
        self.cleanUp()

    def cleanUp(self):
        listOfOpenTempFileHandles = self.listOfOpenTempFileHandles[:]
        for openTempFile in listOfOpenTempFileHandles:
            self.closeAndDeleteFile(openTempFile)

        listOfTempDirNames = self.listOfNewTempDirNames[:]
        for openNewTempDir in listOfTempDirNames:
            self.deleteDirectory(openNewTempDir)

    def createNamedTempFile(self):

        try:
            tmpFile = tempfile.NamedTemporaryFile('w+t',
                        encoding="utf-8", delete = False)
            if (self.verbose is True):
                print("created temp file: " + tmpFile.name)

            self.listOfOpenTempFileHandles.append(tmpFile)

            return tmpFile

        except:
            print("unable to create a temp file")
            return None

    def writeToTempFile(self, tempFile = None, contents = None):
        try:
            tempFile.write(contents)

            # most callers to this are giving this file path to a different
            # process, so its important to flush the writes
            return (self.flushTempFileContents(tempFile))
        except:
            return False

    def flushTempFileContents(self, tmpFile):
        try:
            tmpFile.flush()
            return True
        except:
            return False

    def closeFile(self, tmpFile):
        if (self.verbose is True):
            print("Closing: %s" % tmpFile.name)

        try:
            tmpFile.close()
            return (True)
        except:
            print("unable to close the file: %s" % tmpFile.name)
            return (False)

    def closeAndDeleteFile(self, tmpFile):
        assert(tmpFile is not None)

        if (self.verbose is True):
            print("deleting tmpFile: " + tmpFile.name)

        try:
            self.closeFile(tmpFile)

            os.unlink(tmpFile.name)
            self.listOfOpenTempFileHandles.remove(tmpFile)

        except:
            pass

    def getAllContents(self, tmpFile, encoding):
        try:
            tmpFile.seek(0)
            return (tmpFile.readlines())

        except:
            message = "unable to read lines from " + tmpFile.name
            print(message)
            return [message]

    def createTempDirectory(self):
        try:
            directoryName = tempfile.mkdtemp()
            self.listOfNewTempDirNames.append(directoryName)

            if (self.verbose is True):
                print("new temp directory created: " + directoryName)

            return directoryName

        except:
            print("unable to create a temp directory")
            return None

    # be careful if anyone else uses this since this will wipe the directory
    def deleteDirectory(self, path):
        if (self.verbose is True):
            print("deleting directory " + path)

        try:
            shutil.rmtree(path, ignore_errors=False, onerror=None)

            self.listOfNewTempDirNames.remove(path)

        except:
            print("Unable to delete directory: " + path)
            pass
