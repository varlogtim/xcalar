#!/bin/bash
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

set +e
xclean
set -e

cmBuild clean
cmBuild config prod
cmBuild xce

cd $XLRGUIDIR
make dev
cd ..

xclean
xc2 cluster start --num-nodes 3
READY_AT=`date`

echo "Everything is set up and ready to test solutions repo"
echo "(setup started at ${START_AT} and completed at ${READY_AT}";

# Run test command here
$XLRDIR/src/bin/tests/controllerTest/test_runner.sh

xc2 cluster stop