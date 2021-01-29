#!/bin/bash
# ----------------------------------------------------
# This is for Customer4 container tests
# Build the environment
# ----------------------------------------------------
set -e
set -x

say () {
    echo >&2 "$*"
}

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"
. $DIR/jenkinsUtils.sh

onExit() {
    local retval=$?
    set +e

    if [[ $retval != 0 ]]
    then
        genBuildArtifacts
        say "Build artifacts copied to ${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
    fi

    exit $retval
}

START_AT=`date`

# Common set up
export WORKSPACE=${WORKSPACE}
echo "(Setting WORKSPACE to ${WORKSPACE})"

trap onExit EXIT

cd $XLRDIR
source doc/env/xc_aliases

cmBuild clean
cmBuild config prod
cmBuild xce

READY_AT=`date`

echo "Everything is set up and ready to test solutions repo"
echo "(setup started at ${START_AT} and completed at ${READY_AT}";

