# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import argparse

from helpers import fileHelper

# update arg object iff the value is not set in arg
def readFromCfgFileAndUpdateArgIf(args):
    pathToUpgradeTool = None
    dataflowToUpgrade = None
    directoryToWriteConvertedDf = None

    if (fileHelper.doesThisFileExist(args.pathToConfigFile) is False):
        if args.verbose is True:
            print("File %s does not exist, will check from command line"
                    % args.pathToConfigFile)
        return (False)

    cfgFileContents = fileHelper.getContents(args.pathToConfigFile)
    if (cfgFileContents is None):
        print("Unable to read from %s" % args.pathToConfigFile)
        return (False)

    for line in cfgFileContents:
        if (line.strip().startswith("#") or (len(line.strip()) == 0)):
            continue

        elif (line.strip().startswith("PATH_TO_UPGRADE_TOOL=")):
            pathToUpgradeTool =\
                line.strip().strip("PATH_TO_UPGRADE_TOOL=").strip("\"")
            if (not (len(pathToUpgradeTool) > 0)):
                pathToUpgradeTool = None

        elif (line.strip().startswith("DATAFLOW_TO_UPGRADE=")):
            dataflowToUpgrade =\
                line.strip().strip("DATAFLOW_TO_UPGRADE=").strip("\"")
            if (not (len(dataflowToUpgrade) > 0)):
                dataflowToUpgrade = None

        elif (line.strip().startswith("DEST_DIR_TO_WRITE_UPGRADED_DF=")):
            directoryToWriteConvertedDf =\
                line.strip().strip("DEST_DIR_TO_WRITE_UPGRADED_DF=").strip("\"")
            if (not (len(directoryToWriteConvertedDf) > 0)):
                directoryToWriteConvertedDf = None

        else:
            print("Invalid keyword: %s found in %s. Ignoring"
                    % (line, args.pathToConfigFile))

    if (args.dataflowtoupgrade is None):
        args.dataflowtoupgrade = dataflowToUpgrade

    if (args.destdataflowdir is None):
        args.destdataflowdir = directoryToWriteConvertedDf

    if (args.pathToUpgradeTool is None):
        args.pathToUpgradeTool = pathToUpgradeTool

    return (True)

def parseCmdArguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--pathToConfigFile', '-c', type = str,
        help = 'absolute path to upgradeConfig.cfg')

    parser.add_argument('--dataflowtoupgrade', '-f', type = str,
        help = 'absolute path to dataflow file(.tar.gz)')

    parser.add_argument('--destdataflowdir', '-d', type = str,
        help = 'absolute path to a directory where upgraded'
                'dataflows are to be copied to')

    parser.add_argument('--verbose', '-v', type = int, default = 0)

    parser.add_argument('--pathToUpgradeTool', '-p', type = str,
        help = 'enable to run in verbose mode')

    args = parser.parse_args()

    return (args)

def sanityCheckArgsAndWarn(args):

    readFromCfgFileAndUpdateArgIf(args)

    if (args.dataflowtoupgrade is None):
        print("--dataflowtoupgrade not set")
        return (False)

    if (args.destdataflowdir is None):
        print("--destdataflowdir not set")
        return (False)

    if (args.pathToUpgradeTool is None):
        print("--pathToUpgradeTool not set")
        return (False)

    print("Using '%s' as input dataflow file"
        % args.dataflowtoupgrade)
    print("Using '%s' as output directory to write final dataflow\n"
        % args.destdataflowdir)

    return (True)
