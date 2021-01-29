#!/bin/bash -x
# Params:
#   DATA_SIZE - size of data loaded before running rest of the UBM operators
#   ARCHIVE_STATS_AND_LOGS - Whether to archive stats and logs in a successful run
#   CLUSTER_LAUNCH_TIMEOUT - timeout for cluster launch

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

source $XLRDIR/doc/env/xc_aliases
export XLRGUIDIR="${XLRGUIDIR:-$XLRDIR/xcalar-gui}"
export XCE_NEWCONFIG=/tmp/test_ubm_perf.cfg
export NETSTORE_JENKINS="${NETSTORE_JENKINS:-/netstore/qa/jenkins}"
CLUSTER_LAUNCH_TIMEOUT="${CLUSTER_LAUNCH_TIMEOUT:-120}"

RESULTS_PATH="${NETSTORE_JENKINS}/${JOB_NAME}/${BUILD_ID}"
mkdir -p "$RESULTS_PATH"

. $DIR/jenkinsUtils.sh

ARCHIVE_STATS_AND_LOGS=${ARCHIVE_STATS_AND_LOGS:-false}

onExit() {
    local retval=$?
    set +e
    if [ $retval = 0 ]; then
        if [ $ARCHIVE_STATS_AND_LOGS = true ]; then
            # We want to archive logs and stats from this run
            genBuildArtifacts true
            echo "Stats and Logs from run copied to ${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
        fi
        exit 0
    else
        genBuildArtifacts
        echo "Build artifacts copied to ${NETSTORE}/${JOB_NAME}/${BUILD_ID}"
    fi
    exit $retval
}

trap onExit EXIT SIGINT SIGTERM

set +e

# Clean up existing running cluster if any
sudo pkill -9 usrnode || true
sudo pkill -9 childnode || true
sudo pkill -9 xcmonitor || true
sudo pkill -9 xcmgmtd || true
xclean

# build XCE now

echo "Building XCE"
cd $XLRDIR
cmBuild clean
cmBuild config prod
cmBuild xce

# Build xcalar-gui so that expServer will run
echo "Building XD"
(cd $XLRGUIDIR && make dev)

# then, launch 2-node cluster
# eventually, 'dcc' should be invoked (each node in its own  container)

export XCE_CONFIG="${XCE_CONFIG:-$XLRDIR/src/data/ubm-perf-test.cfg}"

# This is a perf eval test, so num-nodes=1 wouldn't be sufficient to cover
# the inter-node paths, and no point in having more than 2 nodes fighting
# for resources on the one-host multi-node cluster.

# XXX: Eventually, we should configure/allow multi-host clusters in the
# test automation infra

# XXX: In addition, the timeout is higher than default since lazy mlock is off
# which means the usrnode takes longer to come-up. Once ENG-7204 is fixed to
# parallelize mlock, it should be ok to remove the longer timeout here
xc2 cluster start --num-nodes 2 --timeout $CLUSTER_LAUNCH_TIMEOUT
exitCode=$?
if [ $exitCode -ne 0 ]; then
    echo "failed to start the cluster"
    exit $exitCode
fi

# 0. The --no-stats turns off fiber stats app launch and kill - which takes too
#    long and more importantly, isn't necessary
# 1. The --action=all cycles through all operators and reports timing for each
# 2. The iter-num is a placeholder for the scenario when this command is invoked
#    multiple times with the iter-num bumped each time to record results for
#    different iterations

exitCode=1
$XLRDIR/scripts/performance/operator_perf.py --action=all --no-stats --size=$DATA_SIZE --notes="jenkins run" --results-output-dir=$RESULTS_PATH --iter-num=0
exitCode=$?

xc2 cluster stop

exit $exitCode
