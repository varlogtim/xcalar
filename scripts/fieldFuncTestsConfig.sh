#!/bin/bash
# See ./fieldFuncTestsRun.sh for more information about this file, and its place
# in the overall flow of steps in order to run func tests in the field.
#
# This file automates the installation of config for the field func tests, stops
# Xcalar after getting a confirmation from the user, and starts Xcalar at
# the end with a new config for the field functional tests.
#
# It creates a new Xcalar rootdir (a subdir under the Xcalar root), and updates
# the config with this rootdir, so that on a restart, the new Xcalar root will
# be used by the func tests. After the run, the fieldFuncTestsClean.sh script
# must be run, which will blow-away the func test rootdir, and revert the config
# to the original state.
#
# XCE_CONFIG is modified to point to the new config file before starting
# Xcalar here.
#
# Note that the script takes a "-y" parameter: the script will skip interactive
# mode if this param is specified (which prompts user before proceeding to stop
# Xcalar).
#
DIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"

INSTALL_DIR="$(dirname $(dirname $(dirname $DIR) ) )"

# Change defaults for your installation in the following file
TARBALL_INSTALL="0"
if [ -r "/etc/default/xcalar" ]; then
    . /etc/default/xcalar
elif [ -r "$INSTALL_DIR/etc/default/xcalar" ]; then
    export XCE_DEFAULTS=${INSTALL_DIR}/etc/default/xcalar
    . "$XCE_DEFAULTS"
    TARBALL_INSTALL="1"
fi

printf "\nRunning this script will stop Xcalar and require you to re-start\n\n"
if [[ $# -eq 0 ]] || [[ $1 != "-y" ]] ; then
    read -p "Continue? 'Y' or 'y' for yes, any other char for No: " contresp
    if ! [[ $contresp =~ ^[Yy] ]] ; then
        printf "Not proceeding with this script - as requested\n"
        exit 1
    fi
fi

export XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"
export XLRDIR="${XLRDIR:-/opt/xcalar}"
xcalarRootPath="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG)"
if [ -z $xcalarRootPath ]; then
    printf "Xcalar Root undefined. Check Xcalar config: $XCE_CONFIG. Exiting.\n"
    exit 1
fi
XCE_CONFIG_DIR=$(dirname $XCE_CONFIG)
XCE_DEFAULTS_DIR=$(dirname $XCE_DEFAULTS)
funcTestConfParams=$XCE_CONFIG_DIR/fieldFuncTests.cfg
funcTestConf="$xcalarRootPath/fieldFuncTestsCombo.cfg"
funcTestDefaults="$XCE_DEFAULTS_DIR/fieldFuncTests"
funcTestRoot="$xcalarRootPath/rootForFieldFuncTests"
sysConf=$XCE_CONFIG

#
# Messages for diagnostics; uncomment if needed
#
printf "\nInstalling functional test params in config file...\n\n"
#printf "The files in $xcalarRootPath are:\n\n"
#ls $xcalarRootPath
#printf "\n"
#printf "\nThe current Xcalar config file is $sysConf\n"
#printf "The func test param config is in $funcTestConfParams\n"
#printf "The new Xcalar config file will be $funcTestConf\n"
#printf "Xcalar root for func tests will be $funcTestRoot\n"

nodeId=`${XLRDIR}/bin/xcalarctl node-id | tail -1`

#
# If the script is run multiple times before the admin cleans up, this is
# detected here, and the script aborted. Otherwise it'd be hard to keep track of
# config changes, and issues due to possible blend of manual updates to the
# config file and this automation.
#
if [ -e $funcTestConf ]; then
    if [ $nodeId == "0" ]; then
        printf "\n\nYou're in middle of a functest config change or run\n"
        printf "To reset, first abort fieldFuncTests.Run.sh if running\n"
        printf "and then run fieldFuncTestsClean.sh\n\n"
        exit 1
    fi
else
    if [ $nodeId != "0" ]; then
        printf "\nWARNING: functest config not available. Run first on node0\n"
        exit 1
    fi
fi

if [ -d $funcTestRoot ]; then
    if [ $nodeId == "0" ]; then
        printf "\n\nWARNING: a func test root from prior run not cleaned up"
        printf "\nTo reset, first abort fieldFuncTests.Run.sh if running\n"
        printf "\nand then run fieldFuncTestsClean.sh on each node\n\n"
        exit 1
    fi
else
    if [ $nodeId != "0" ]; then
        printf "\nWARNING: functest rootdir not available. Run first on node0\n"
        exit 1
    fi
fi

#printf "Now stopping Xcalar..\n\n"

#
# On nodes > 0, the stop isn't really needed, since node0's stop should've
# stopped Xcalar on all nodes but doesn't hurt to issue the stop. This is
# just if someone runs Config.sh on a >0 node before runnig on node 0.
#
${XLRDIR}/bin/xcalarctl stop-supervisor
rc=$?

if [ $rc -ne 0 ]; then
    printf "ERROR($rc): Failed to stop Xcalar\n"
    exit $rc
fi

printf "Succeeded in stopping Xcalar.\n"

# do tarball install defaults setup
if [ "$TARBALL_INSTALL" == "1" ]; then
    cp -f $XCE_DEFAULTS $funcTestDefaults
    sed -i -e "s:^XCE_CONFIG=.*:XCE_CONFIG=$funcTestConf:g" $funcTestDefaults
    export XCE_DEFAULTS=$funcTestDefaults
fi

if [ $nodeId == "0" ]; then
    printf "Installing new config for func tests\n\n"
    #
    # Create the new Xcalar Root sub-directory
    #

    mkdir -p $funcTestRoot

    #
    # Then cat the modified default.cfg with the field func test config. The
    # new config should specify the new Xcalar Root, which was created above.
    # Use "sed" to modify the new config accordingly.
    #
    cat $sysConf $funcTestConfParams > $funcTestConf


    #
    # Comment out XcalarRoot in the new func test config file
    #

    sed -i -e 's/^Constants.XcalarRootCompletePath*/#&/' $funcTestConf

    #
    # Append new line with the new XcalarRoot specified after the commented-out
    # line with the previous root
    #

    sed -i "/^#Constants.XcalarRootCompletePath/a \
        Constants.XcalarRootCompletePath=$funcTestRoot" $funcTestConf

    #
    # if it's a tarball install fix up data paths
    #
    if [ "$TARBALL_INSTALL" == "1" ]; then
        sed -i -e "s:/opt/xcalar:$XLRDIR:g" $funcTestConf
    fi

    #
    # Some explanatory messages for diagnostics; uncomment if needed
    #
    #printf "After config install, the files in $xcalarRootPath are:\n\n"
    #ls $xcalarRootPath

    printf "Now please run fieldFuncTestsConfig.sh on other nodes\n\n"
fi

#
# Now export XCE_CONFIG for Xcalar start to use the new config

export XCE_CONFIG=$funcTestConf

printf "Starting Xcalar with the new field func test config\n\n"

if [ $nodeId == "0" ]; then
    printf "\nXcalar start waits for fieldFuncTestsConfig.sh on other nodes\n\n"
fi

${XLRDIR}/bin/xcalarctl start
rc=$?

if [ $rc -ne 0 ]; then
    printf "ERROR($rc): Failed to start Xcalar\n"
    exit $rc
else
    printf "Now, you can run fieldFuncTestsRun.sh on any one node\n"
fi
