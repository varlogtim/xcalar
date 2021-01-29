#!/bin/bash

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

. $DIR/jenkinsUtils.sh

source $XLRDIR/doc/env/xc_aliases

ScriptPath="/netstore/users/xma/dashboard/startFuncTests.py"
configFile="$XLRDIR/src/data/test.cfg"
export XCE_LICENSEDIR=$XLRDIR/src/data
export XCE_LICENSEFILE=${XCE_LICENSEDIR}/XcalarLic.key
export ExpServerd="true"
export XLRGUIDIR=$PWD/xcalar-gui
export XLRINFRADIR=$PWD/xcalar-infra
export GuardRailsArgs
trap "postReviewToGerrit" EXIT


sudo pkill -9 gdbserver || true
sudo pkill -9 usrnode || true
sudo pkill -9 childnode || true
sudo pkill -9 xcmgmtd || true
sudo pkill -9 xcmonitor || true
pgrep -f 'python*'$ScriptPath'.*' | xargs -r sudo kill -9

rm -rf /var/tmp/xcalar-`id -un`/*
rm -rf /var/opt/xcalar/*

mkdir -p /var/tmp/xcalar-`id -un`/sessions

set +e
xclean

grep "^Constants.CtxTracesMode=" $configFile &> /dev/null
if [ $? == 1 ]; then
	if [ $CtxTracesMode != 0 ]; then
		echo "Constants.CtxTracesMode=$CtxTracesMode" | cat >> $configFile
	fi
else
	sudo sed -i -e "s'Constants\.CtxTracesMode=.*'Constants\.CtxTracesMode=$Ct  xTracesMode'" $configFile
fi

set -e

if [ "${BUILD_ONCE}" != "1" ]; then
    gitCheckoutIDL
    git submodule update --force

    cmBuild clean

    echo >&2 "Configuring for build type '${XCE_BUILD_TYPE}'"
    cmBuild config "${XCE_BUILD_TYPE}"
    cmBuild qa

    # Build xcalar-gui so that expServer will run
    (cd $XLRGUIDIR && make dev)

else
    echo >&2 "BUILD_ONCE set! Skipping building and configuring"
fi

if [ "$GuardRailsArgs" != "" ]
then
    echo "BUILD_DEBUG: Running Guardrails: $GuardRailsArgs"
    # Increase the mmap map count for GuardRails to work
    echo 100000000 | sudo tee /proc/sys/vm/max_map_count
    # set this config param for GuardRails
    echo "Constants.NoChildLDPreload=true" >> $configFile

    # Build GuardRails
    make -C xcalar-infra/GuardRails clean
    make -C xcalar-infra/GuardRails deps
    make -C xcalar-infra/GuardRails
fi

# XXX: Temp fix to catch a corrupt db hit during pyTest coverage (ENG-9404)
# If pytest trips over a corrupt coverage db, it'll generate an internal
# error which shows up as exit code 3 - see
# https://docs.pytest.org/en/latest/usage.html#possible-exit-codes
#
# if exit code 3 is detected, then sleep for a while, so the slave is
# available for someone to trouble shoot.
#
# Once sleep is over, the job will exit, releasing the slave, but the setting
# of PYTESTINTERROR will trigger a copy of the corrupt db
#
#

set +e
# The disabling of 'set -e' is needed here - otherwise 'cmBuild sanity''s
# failure will cause this script to exit, and the code below it will not be
# reached

cmBuild sanity
rc=$?
set -e

if [ $rc -eq 3 ]; then
    export PYTESTINTERROR=1
    sleep 24h
fi

exit $rc
