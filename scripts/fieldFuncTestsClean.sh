#!/bin/bash
# See ./fieldFuncTestsRun.sh for more information about this file
# This file is used to cleanup any state left by a run of the field func tests.
# It needs to be run only on node 0 of the Xcalar cluster. It takes a parameter,
# "-y": if specified, it will skip interactive mode (which prompts the user
# for confirmation, before proceeding to stop Xcalar).
#
# Change defaults for your installation in the following file

DIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"

INSTALL_DIR="$(dirname $(dirname $(dirname $DIR) ) )"

# Change defaults for your installation in the following file
TARBALL_INSTALL="0"
if [ -r "/etc/default/xcalar" ]; then
    . /etc/default/xcalar
elif [ -r "$INSTALL_DIR/etc/default/fieldFuncTests" ]; then
    export XCE_DEFAULTS=${INSTALL_DIR}/etc/default/fieldFuncTests
    . "$XCE_DEFAULTS"
    TARBALL_INSTALL="1"
else
    printf "No configured defaults file found"
    exit 1
fi

export XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"
export XLRDIR="${XLRDIR:-/opt/xcalar}"

nodeId=`${XLRDIR}/bin/xcalarctl node-id | tail -1`
if [ "$nodeId" != "0" ]; then
        printf "\nNo need to run this on node $nodeId; only run on node 0\n"
        exit 0
fi

if [[ $XCE_CONFIG == *"fieldFuncTestsCombo.cfg"* ]]; then
    xcalarRootPath="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG)"
    funcTestConf=$XCE_CONFIG
    funcTestRoot=$xcalarRootPath
    xcalarRootPath=$(dirname $funcTestRoot)
    unset XCE_CONFIG
else
    xcalarRootPath="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG)"
    funcTestConf="$xcalarRootPath/fieldFuncTestsCombo.cfg"
    funcTestRoot="$xcalarRootPath/rootForFieldFuncTests"
fi

#
# Now print messages for diagnostics before reverting the func test config
# changes done when fieldFuncTestsConfig.sh was executed. Uncomment if needed
#
#printf "Reverting functest param changes to config file...\n\n"
#printf "The files in $xcalarRootPath are:\n\n"
#ls $xcalarRootPath
#printf "\n"

if [ -e $funcTestConf ];
then
    printf "\nNOTE: reverting to normal config: this will stop Xcalar\n\n"
    if [[ $# -eq 0 ]] || [[ $1 != "-y" ]] ; then
        read -p "Continue? 'Y' or 'y' for yes, any other char for No: " contresp
        if ! [[ $contresp =~ ^[Yy] ]] ; then
            printf "Not proceeding as requested\n"
            exit 1
        fi
    fi
    ${XLRDIR}/bin/xcalarctl stop-supervisor
    rc=$?
    if [ $rc -ne 0 ]; then
        printf "ERROR($rc): Failed to stop Xcalar.\n"
        exit $rc
    fi
    if [ -d $funcTestRoot ]; then
        printf "Cleaning and removing Func Test Root Dir $funcTestRoot ...\n"
        rm -rf $funcTestRoot
        printf "Done\n\n"
    fi
    printf "Removing Func Test combo config $funcTestConf...\n"
    rm $funcTestConf
    [ "$TARBALL_INSTALL" == "1" ] && rm $XCE_DEFAULTS
    printf "\nDone. Please re-start Xcalar now.\n\n"
else
    # There could be some scenarios in which the config's normal, but somehow
    # references the fieldfuncTests rootdir (the cleanup was run, the
    # config cleaned up, but someone manually edited the config) - this is
    # fatal - don't want to remove the only Xcalar rootdir via this script!
    #
    # Another scenario is that although the config's clean, the test rootdir
    # didn't get deleted somehow - so the dir exists! - if so, remove it. This
    # is harmless since the normal config has a valid xcalar root, and this one
    # is clearly useless now.

    if [[ $xcalarRootPath == *"rootForFieldFuncTests"* ]]; then
        printf "FATAL: normal config references field func test rootdir\n"
        printf "This needs manual intervention. Aborted\n\n"
        exit 1
    fi
    printf "There's no functest config state to cleanup\n\n"
    if [ -d $funcTestRoot ]; then
        printf "WARNING: config's clean but func test rootdir exists\n\n"
        printf "Removing $funcTestRoot...\n"
        rm -rf $funcTestRoot
    fi
fi
