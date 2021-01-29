# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import tarfile
import os

from helpers import fileHelper

class TarFileHelper(object):
    def __init__(self, verbose = False):
        self.verbose = verbose

        self.inputTarFileHandle = None
        self.inputTarFileExtractLocation = None

        self.listOfExtractedFiles = []

        self.listOfRealUdfs = []
        self.pathToInputJsonFile = None

        # this is for the directory to put converted files into
        self.locationToZipFrom = None

    def __del__(self):
        if (self.inputTarFileHandle is not None):
            self.closeInputTarFile()

    def __openTarFile(self, pathToTarFile, openMode):
        try:
            if (self.verbose is True):
                print("Opening tar file: " + pathToTarFile)

            openTarFileHandle = tarfile.open(pathToTarFile, openMode)

            if (self.verbose is True):
                print("tar file: " + openTarFileHandle.name + " opened")

            return (openTarFileHandle)

        except:
            print("Unable to open tar file: " + pathToTarFile)
            return None

    def openInputTarFileAndSaveHandle(self, pathToInputTarFile):

        assert(self.inputTarFileHandle is None)

        self.inputTarFileHandle =\
            self.__openTarFile(pathToInputTarFile, "r:gz")

        if (self.inputTarFileHandle is None):
            return (1)

        return (0)

    def __extractTarFile(self, tarFileHandle, pathToExtractTo):

        if (self.verbose is True):
            print("Extracting tar file: %s to %s"
                % (tarFileHandle.name, pathToExtractTo))

        try:
            tarFileHandle.extractall(pathToExtractTo)

            if (self.verbose is True):
                print("Successfully extracted tar file: %s to %s"
                    % (tarFileHandle.name, pathToExtractTo))

            return (0)

        except:
            print("Unable to extract tar file: %s to %s"
                %(tarFileHandle.name, pathToExtractTo))
            return (1)

    def closeInputTarFile(self):
        try:
            if (self.verbose is True):
                print("closing tar file: " + self.inputTarFileHandle.name)

            self.inputTarFileHandle.close()

            if (self.verbose is True):
                print("tar file: " + self.inputTarFileHandle.name + " closed")

            self.inputTarFileHandle = None

        except:
            print("unable to close tar file:" + self.inputTarFileHandle.name)

    def extractTarFileAndUpdateFileNames(self, pathToExtractTo):
        assert(len(self.listOfExtractedFiles) == 0)
        assert(self.inputTarFileHandle is not None)
        assert(self.inputTarFileExtractLocation is None)

        ret = self.__extractTarFile(
            self.inputTarFileHandle, pathToExtractTo)
        if (ret != 0):
            return (ret)

        self.listOfExtractedFiles = fileHelper.listAllFiles(pathToExtractTo)
        if (len(self.listOfExtractedFiles) == 0):
            return (1)

        for currentFile in self.listOfExtractedFiles:
            if (currentFile.strip().lower().endswith(".json")):
                if (self.pathToInputJsonFile is not None):
                    # we have only one json file
                    print("Second json file detected! ")
                    print("First: " + self.pathToInputJsonFile)
                    print("second: " + currentFile)
                    return (1)

                self.pathToInputJsonFile = currentFile

            elif (currentFile.strip().lower().endswith(".py")):
                self.listOfRealUdfs.append(currentFile)

        if (self.verbose is True):
            print("Extracted file which have Xcalar's interest:")
            print("Xcalar Dataflow file name: " + self.pathToInputJsonFile)
            for idx, udf in enumerate(self.listOfRealUdfs):
                print("Udf :" + str(idx) + ": " + self.listOfRealUdfs[idx])

        self.inputTarFileExtractLocation = pathToExtractTo

        return (0)

    def getAllOrigUdfsPaths(self):
        return list(self.listOfRealUdfs)

    def moveThisUdfToLocation(self, udfFile, pathToMove):

        destPath = None

        # get the name of the UDF so that we can copy the file to dst
        udfFileName = udfFile.strip().split("/")[-1]
        assert(udfFileName.lower().endswith(".py"))

        if (not pathToMove.strip().rstrip("/").lower().endswith("udfs")):
            destPath = os.path.join(pathToMove, "udfs")
            ret = fileHelper.createDirectoryIf(destPath)
            if (ret != 0):
                return (ret)
        else:
            destPath = pathToMove

        if (self.verbose is True):
            print("Moving udf %s to %s " % (udfFile, destPath))

        # move these to file helper, doesnt belong here
        dstUdfFullPath = os.path.join(destPath, udfFileName)
        ret = fileHelper.copyFile(udfFile, dstUdfFullPath)
        if (ret != 0):
            print("File copy failed src: " + udfFile +
                " dst: " + dstUdfFullPath)
            return (ret)

        return (0)

    def moveUdfsToDestLocation(self):
        assert(self.locationToZipFrom is not None)

        for udfFile in self.listOfRealUdfs:
            ret = self.moveThisUdfToLocation(udfFile,
                                            self.locationToZipFrom)
            if (ret != 0):
                return (ret)

        return (0)

    # convertedJsonFileName need not be of the name dataflowInfo.json
    def moveDataflowFileToDestLocation(self, convertedJsonFileName):
        assert(self.locationToZipFrom is not None)
        assert (self.pathToInputJsonFile is not None)

        dataFlowFileName = self.pathToInputJsonFile.strip().split("/")[-1]
        assert(dataFlowFileName.lower().endswith(".json"))
        if (self.verbose is True):
            print("found an older version of dataflow")
        dataFlowFileName = "dataflowInfo.json"

        dataFlowFileNameToZip = os.path.join(self.locationToZipFrom,
                                                dataFlowFileName)

        ret = fileHelper.copyFile(convertedJsonFileName,
                    dataFlowFileNameToZip)
        if (ret != 0):
            print("File copy failed src: " + convertedJsonFileName +
                    " dst: " + dataFlowFileNameToZip)
            return (ret)

        return (0)

    # saving the path where udfs and dataflow file will be copied to
    def setDstPathToZipFrom(self, pathToZipFrom):
        assert(self.locationToZipFrom is None)
        self.locationToZipFrom = pathToZipFrom

    def gzipUdfAndDataflowFile(self, dstDirectory):
        assert(self.locationToZipFrom is not None)
        assert(self.inputTarFileHandle is not None)

        tarFileName = self.inputTarFileHandle.name.strip().split("/")[-1]
        dstTarFilePath = os.path.join(dstDirectory, tarFileName)

        if (fileHelper.doesThisFileExist(dstTarFilePath) is True):
            print("File: " + dstTarFilePath + " exists!, can not overwrite")
            assert(0) # this is checked for when the script starts
            return (1)

        dstTarFileHandle = self.__openTarFile(dstTarFilePath, "w|gz")
        if (dstTarFileHandle is None):
            print("Unable to open tar file to write to: %s" % dstTarFilePath)
            return (1)

        listOfFilesToZip = fileHelper.listAllFiles(self.locationToZipFrom)

        ret = 0
        for currentFile in listOfFilesToZip:
            if (currentFile.strip().endswith(".py") or
                    currentFile.strip().lower().endswith(".json")):
                if (self.verbose is True):
                    print("Adding File: " + currentFile + " to archive: " +
                        dstTarFilePath)

            # this is the name to tar, for the file we are about to archive
            archiveName = None
            if (currentFile.strip().endswith(".py")):
                currentUdfFileName = currentFile.strip().split("/")[-1]
                archiveName = "udfs/" + currentUdfFileName
            elif (currentFile.strip().lower().endswith(".json")):
                archiveName = currentFile.strip().split("/")[-1]

            if (archiveName is not None):
                try:
                    dstTarFileHandle.add(currentFile,
                        arcname = archiveName, recursive = False)

                except:
                    print("Unable to add: " + currentFile +
                        "to archive: " + dstTarFilePath)
                    ret = 1
                    break

        try:
            dstTarFileHandle.close()
        except:
            print("Unable to close  archive file! " + dstTarFilePath)
            ret = 1

        if (ret == 0):
            print("Upgraded dataflow file can be found here: " + dstTarFilePath)

        return (ret)

def isTarFile(pathToFile):
    try:
        return (tarfile.is_tarfile(pathToFile))
    except:
        return (False)

def isXcalarDataflowFile(pathToFile, verbose = False):
    dataFlowFileFound = False
    try:
        tarFileObject = tarfile.open(pathToFile, "r:gz")
    except:
        print("Unable to open the tar file!")
        return (False)

    memberNames = tarFileObject.getnames()

    for member in memberNames:
        # XXX: is this name always constant?
        # move this name to a constants.py file
        if ((member.lower() == "dataflowinfo.json".lower()) or
                (member.lower() == "retinaInfo.json".lower())):

            if (verbose is True):
                print("Found dataflow file: " + member + " in " + pathToFile)

            dataFlowFileFound = True
            break

    tarFileObject.close()

    return (dataFlowFileFound)

