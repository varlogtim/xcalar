# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

from helpers import argumentParser as ArgumentParser
from helpers import fileHelper as FileHelper
from helpers import tarHelper as TarHelper
from helpers import tempHelper as TempHelper
from helpers import lib2to3Helper as Lib2to3Helper
from helpers import XcEvalStringHelper
from helpers import XcJsonHelper
from helpers import XcConfigHelper
from helpers import XcDFUpgradeToolHelper
from helpers import XcParamReplaceHelper

def getUserConsent(question):
    listOfYes = ['yes', 'y', 'ye']
    listOfNo = ['no', 'n']

    while True:
        choice = input(question).lower()
        if (not choice):
            print("Please respond with 'yes' or 'no'")
            continue
        if (choice in listOfYes):
            return (True)
        elif (choice in listOfNo):
            return (False)
        else:
            print("Please respond with 'yes' or 'no'")
            continue

def doArgSanityCheck(args):
    if (ArgumentParser.sanityCheckArgsAndWarn(args) is False):
        print("required arguments not present")
        return (1)

    if (FileHelper.doesThisFileExist(args.dataflowtoupgrade) is False):
        print("dataflow file '{}' does not exist".format(args.dataflowtoupgrade))
        return (1)

    if (TarHelper.isTarFile(args.dataflowtoupgrade) is False):
        print("dataflow file '{}' is not of type tar.gz".format(args.dataflowtoupgrade))
        return (1)

    if (TarHelper.isXcalarDataflowFile(args.dataflowtoupgrade,
            verbose = args.verbose) is False):
        print("tar file '{}' is not a Xcalar dataflow file".format(args.dataflowtoupgrade))
        return (1)

    if (FileHelper.doesThisDirExist(args.destdataflowdir) is False):
        print("Dir '{}' does not exist".format(args.destdataflowdir))
        return (1)

    # check if dest dir has a file same as args.dataflowtoupgrade, warn and exit
    tarFileName = args.dataflowtoupgrade.strip().split("/")[-1]
    dstTarFile = os.path.join(args.destdataflowdir, tarFileName)
    if (FileHelper.doesThisFileExist(dstTarFile)):
        print("Destination file '{}' already exists and cannot be overwritten!".
                format(dstTarFile))
        return (1)

    return (0)

def extractInputDataflowTarFile(args, tempHelper, tarHelper):

    newTempDirectory = tempHelper.createTempDirectory()

    if (newTempDirectory is None):
        print("Unable to create temp directory")
        return (1)

    ret = tarHelper.openInputTarFileAndSaveHandle(args.dataflowtoupgrade)
    if (ret != 0):
        return (ret)

    ret = tarHelper.extractTarFileAndUpdateFileNames(newTempDirectory)
    if (ret != 0):
        return (ret)

    print("Dataflow file is extracted to: " + newTempDirectory)

    return (0)

def createAndSaveTmpDstDirectory(args, tempHelper, tarHelper):
    # we create a temp directory to move the udf after
    # customer has made the modifications
    dirToCopyFilesTo = tempHelper.createTempDirectory()
    if (dirToCopyFilesTo is None):
        print("unable to create temp directory")
        return (1)

    tarHelper.setDstPathToZipFrom(dirToCopyFilesTo)

    return (0)

def run2to3OnTheseUdfs(args, listOfUdfsInOrigDataflow):
    lib2to3Helper = Lib2to3Helper.Lib2to3Helper(verbose = args.verbose)

    if (lib2to3Helper.does2To3ExistOnThisMachine() is False):
        print("2to3 utility is not found on this machine, please install and retry")
        return (1)

    for udf in listOfUdfsInOrigDataflow:
        cmdInfo = lib2to3Helper.run2to3OnThisUdf(udf)
        if (cmdInfo.getExitCode() != 0):
            print("Unable to run 2to3 on: " + udf)
            print("Stderr: " + ''.join(cmdInfo.getStdout()))
            print("Stdout: " + ''.join(cmdInfo.getStderr()))
            return (cmdInfo.getExitCode())

    return 0

def makeUdfsPythonCompatibleWithUserIntervention(args, listOfUdfsInOrigDataflow):
    print("Python 2.x source is not fully compatible with python 3.6")
    print("Optionally, on all udfs, this script can run 2to3\n"
            "(from standard python library which applies a specific set of fixers per:"
            "\nhttps://docs.python.org/3.6/library/2to3.html)\n")
    print("Please ensure the udf source files are not open by any other program before you continue")
    proceed = getUserConsent("Would you like to run 2to3 on udfs? [no/yes]: ")
    if (proceed is True):
        ret = run2to3OnTheseUdfs(args, listOfUdfsInOrigDataflow)
        if (ret != 0):
            print("Unable to run 2to3 on udfs")

        return (ret)

    else: # user does not want to run 2to3 on it, check if he wants to do it manually
        print("Without making the udf[s] compatible with python 3.6")
        print("uploading the dataflow will fail")
        proceed = getUserConsent("Would you like to manually fix them? [no/yes]: ")
        if (proceed is True):
            return (0) # caller needs to wait for user to edit the udfs

        else: # user does not want to manually fix them
            print("Archiving these udfs as is to upgraded version of dataflow")
            print("will cause errors during upload of dataflow to Xcalar cluster")
            return (0) # user might want to fix them later, lets fix json for now and archive

def fixUdfAndMoveToDstDir(args, tempHelper, tarHelper):

    listOfUdfsInOrigDataflow = tarHelper.getAllOrigUdfsPaths()

    if (len(listOfUdfsInOrigDataflow) == 0):
        return (0)

    print("\nFound following udfs:")

    for udf in listOfUdfsInOrigDataflow:
        print(udf)
    print("")

    ret = makeUdfsPythonCompatibleWithUserIntervention(args, listOfUdfsInOrigDataflow)
    if (ret != 0):
        return (ret)

    print("Please examine the following UDFs and make changes where necessary")
    for udf in listOfUdfsInOrigDataflow:
        print(udf)
    print("")

    message = "Once complete, please enter 'yes' to continue, 'no' to quit: "
    proceed = getUserConsent(message)
    if (proceed is False):
        print("Quitting dataflow upgrade process")
        return (1)

    if (args.verbose is True):
        print("Continuing to move final udf files to dest directory")

    ret = tarHelper.moveUdfsToDestLocation()

    return (ret)


# FIXME: modularize this
def fixAndMoveDataflowToDstDir(args, tempHelper, tarHelper):

    inputJsonfile = tarHelper.pathToInputJsonFile

    listOfUdfModuleNames = []
    udfFileNames = tarHelper.getAllOrigUdfsPaths()
    for udfFile in udfFileNames:
        udfModuleName = udfFile.strip().split("/")[-1].strip(".py")
        listOfUdfModuleNames.append(udfModuleName)

    evalStringHelper = XcEvalStringHelper.XcEvalStringHelper(
            verbose = args.verbose, listOfUdfNames = listOfUdfModuleNames)
    jsonHelper = XcJsonHelper.XcJsonHelper(verbose = args.verbose)

    jsonObjToManipulate = jsonHelper.load(inputJsonfile)
    if (jsonObjToManipulate is None):
        print("Unable to read from %s" % inputJsonfile)
        return (1)

    inputQuery = jsonHelper.getValue('query', jsonObjToManipulate)
    if (inputQuery is None):
        print("Unable to retrieve 1.2.3 query from %s" % inputJsonfile)
        return (1)

    paramReplaceHelper =\
        XcParamReplaceHelper.XcParamReplaceHelper(verbose = args.verbose)

    inputQuery = paramReplaceHelper.replaceParameters(inputQuery)

    queryWithDummyEvalStrings = evalStringHelper.insertDummyStrings(
            origDataFlowFileContents = inputQuery)

    inputFileForUpgradeTool = tempHelper.createNamedTempFile()
    if (inputFileForUpgradeTool is None):
        print("Unable to create temp file to invoke upgradeTool")
        return (1)

    outputFileForUpgradeTool = tempHelper.createNamedTempFile()
    if (outputFileForUpgradeTool is None):
        print("Unable to create temp file to invoke upgradeTool")
        return (1)

    success = tempHelper.writeToTempFile(
        tempFile = inputFileForUpgradeTool,
        contents = queryWithDummyEvalStrings)
    if (success is False):
        print("Unable to write to temp file to invoke upgradeTool")
        return (1)

    cfgFileHelper = XcConfigHelper.XcConfigHelper(verbose = args.verbose)
    cfgFileHandle = cfgFileHelper.createConfigFile()
    if (cfgFileHandle is None):
        print("Unable to create config file for upgradeTool")
        return (1)

    upgradeToolHelper = XcDFUpgradeToolHelper.XcDFUpgradeToolHelper(
        pathToUpgradeToolBinary = args.pathToUpgradeTool,
        pathToXcCfgFile = cfgFileHandle.name)

    if (args.verbose is True):
        print("path to bin: {}".format(args.pathToUpgradeTool))
        print("path to cfg: {}".format(cfgFileHandle.name))

    cmdInfo = upgradeToolHelper.convertQueryTo13(
                dataflowInputFile = inputFileForUpgradeTool.name,
                dataflowOutputFile = outputFileForUpgradeTool.name)
    if (cmdInfo.getExitCode() != 0):
        print("Unable to convert the Xcalar query to 1.3 version")
        print(cmdInfo.getStderr())
        print("\n\n")
        print(cmdInfo.getStdout())
        return (cmdInfo.getExitCode())

    jsonObjFromUpgradeTool = jsonHelper.load(outputFileForUpgradeTool.name)
    if (jsonObjFromUpgradeTool is None):
        print("Unable to retrieve converted query from %s"
            % outputFileForUpgradeTool.name)
        return (1)

    queryFromUpgradeTool = jsonHelper.getValue('query', jsonObjFromUpgradeTool)
    if (queryFromUpgradeTool is None):
        print("Unable to retrieve 1.3 query from %s"
            % outputFileForUpgradeTool.name)
        return (1)

    updatedDict = evalStringHelper.revertDummyEvalStringsToOriginal(
        queryFromUpgradeTool)
    if (updatedDict is None):
        print("Unable to update eval strings in final Xcalar query")
        return (1)

    jsonObjFromUpgradeTool["tables"] = jsonObjToManipulate["tables"]
    evalStringHelper.fixupSchemaViaInference(jsonObjFromUpgradeTool)

    evalStringHelper.updateQueryAndVersionsFromOutput(
        jsonObjToManipulate, jsonObjFromUpgradeTool)

    finalJsonDataflowFileHandle = tempHelper.createNamedTempFile()
    if (finalJsonDataflowFileHandle is None):
        print("unable to create a temp file")
        return (1)

    ret = jsonHelper.dump(finalJsonDataflowFileHandle, jsonObjToManipulate)
    if (ret != 0):
        print("Unable to create final dataflow")
        return (ret)

    success = tempHelper.flushTempFileContents(finalJsonDataflowFileHandle)
    if (success is False):
        print("Unable to flush the contents of %s"
            % finalJsonDataflowFileHandle.name)
        return (1)

    success = tempHelper.closeFile(finalJsonDataflowFileHandle)
    if (success is False):
        print("Unable to close the file %s" % finalJsonDataflowFileHandle.name)
        return (1)

    ret = tarHelper.moveDataflowFileToDestLocation(finalJsonDataflowFileHandle.name)
    if (ret != 0):
        print("Unable to create final dataflow file")
        return (ret)

    return (0)

def gzipUpgradedDataflow(args, tempHelper, tarHelper):

    return(tarHelper.gzipUdfAndDataflowFile(args.destdataflowdir))

def upgradeDataflowInt(args, tempHelper, tarHelper):

    ret = extractInputDataflowTarFile(args, tempHelper, tarHelper)
    if (ret != 0):
        return (ret)

    ret = createAndSaveTmpDstDirectory(args, tempHelper, tarHelper)
    if (ret != 0):
        return (ret)

    ret = fixUdfAndMoveToDstDir(args, tempHelper, tarHelper)
    if (ret != 0):
        return (ret)

    ret = fixAndMoveDataflowToDstDir(args, tempHelper, tarHelper)
    if (ret != 0):
        return (ret)

    ret = gzipUpgradedDataflow(args, tempHelper, tarHelper)
    if (ret != 0):
        return (ret)

    return (0)

# only entry point to the script
def upgradeDataflow():
    args = ArgumentParser.parseCmdArguments()

    ret = doArgSanityCheck(args)
    if (ret != 0):
        return (ret)

    tempHelper = TempHelper.TempHelper(verbose = args.verbose)
    tarHelper = TarHelper.TarFileHelper(verbose = args.verbose)

    ret = upgradeDataflowInt(args, tempHelper, tarHelper)

    print("Cleaning all temporary files created during the upgrade....")
    tarHelper.closeInputTarFile()
    tempHelper.cleanUp()

    return (ret)

if (__name__ == "__main__"):

    exit(upgradeDataflow())
