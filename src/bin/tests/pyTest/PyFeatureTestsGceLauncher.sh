#!/bin/bash

#
# Creates GCE cluster and installs Xcalar. Runs pyTests on cluster by uploading
# test scripts and invoking py.test.
#

DEFAULT_NUM_NODES=5

DIR=`dirname ${BASH_SOURCE[0]}`
XLRROOT=$DIR/../../../..
printUsage=1

function usage
{
    echo >&2 ""
    echo >&2 "Usage:"
    echo >&2 "$0 --installer/-i <installerPath> [--numNodes/-n <numNodes> --leaveOnFailure]"
    echo >&2 "    --leaveOnFailure Leave cluster running on failure for debug. Requires manual deletion of cluster."
    echo >&2 ""
}

function fail
{
    if [ $printUsage -eq 1 ]; then
        usage
    fi
    echo >&2 "$*"
    exit 1
}

function cleanup
{
    echo "Cleaning up $clusterName"
    $XLRINFRADIR/gce/gce-cluster-delete.sh "$clusterName" || true
}

# Output of this script is difficult to dig through. Help user figure out what's
# going on.
function header
{
    local message="$1"
    echo ""
    echo ""
    echo "================================================================================"
    echo " ${message}"
    echo "================================================================================"
    echo ""
}


header "Preparing to launch cluster..."

if [ -z "$XLRINFRADIR" ]; then
    XLRINFRADIR="$XLRROOT/../xcalar-infra"
    echo "Guessing xcalar-infra is at '${XLRINFRADIR}'."
    echo "Set \$XLRINFRADIR if incorrect."
fi

# Parse arguments.
numNodes=$DEFAULT_NUM_NODES
leaveOnFailure=0
while [ $# -gt 0 ]; do
    opt="$1"
    shift
    case "$opt" in
        -n|--numNodes)
            test -z "$1" && fail "Need to specify numNodes."
            numNodes="$1"
            shift
            ;;
        -i|--installer)
            installerPath="$1"
            shift
            ;;
        --leaveOnFailure)
            leaveOnFailure="1"
            ;;
        --)
            # Pass everything after -- along to py.test.
            break
            ;;
        *)
            fail "Unknown option: $opt."
            ;;
    esac
done

if [ -z "$installerPath" ]; then
    fail
fi
if [ $numNodes -eq 4 ]; then
    # This is to prevent automated tests from using 4 nodes. Please do not delete.
    # Instead, if you need to verify something on 4 nodes, comment this out.
    fail "Please stop testing everything on 4 nodes"
fi
printUsage=0

header "Launching cluster..."

# Create a cluster.
clusterName="${USER}-pytest-$(date +%Y-%m-%d-%H-%M-%S)"
$XLRINFRADIR/gce/gce-cluster-delete.sh "$clusterName" || true
ret=$($XLRINFRADIR/gce/gce-cluster.sh "$installerPath" "$numNodes" "$clusterName")

ips=($(awk '/RUNNING/ {print $6}' <<< "$ret"))

header "Waiting for cluster to be ready..."

timeout=$(( $(date "+%s") + 600 ))
sleep 8 # Yeah, this takes a while.
until $XLRINFRADIR/gce/gce-cluster-health.sh "$numNodes" "$clusterName"; do
    if [ $(date "+%s") -ge $timeout ]; then
        cleanup
        fail "Timed out waiting for cluster."
    fi
    echo "Waiting for $clusterName with $XcalarQaNumNodes nodes to come up..."
    sleep 4
done


header "Preparing cluster..."

# Upload test scripts to cluster and install dependencies.
gcloud compute ssh --command "mkdir --parent /tmp/pyTest" "${clusterName}-1"
if [ $? -ne 0 ]; then
    cleanup
    fail "Failed to create test directory on node 0."
fi
gcloud compute copy-files "$XLRROOT/src/bin/pyClient" "$XLRROOT/src/bin/tests/pyTest" "${clusterName}-1:/tmp/pyTest/"
if [ $? -ne 0 ]; then
    cleanup
    fail "Failed to copy test files to node 0."
fi


header "Executing tests..."
gcloud compute ssh --command "ls;/tmp/pyTest/pyTest/PyFeatureTestsGce.sh" "${clusterName}-1"
ret=$?

if [ $ret -ne 0 ]; then
    header "One or more tests failed"
    if [ $leaveOnFailure -eq 1 ]; then
        echo "As requested, cluster will not be cleaned up."
        echo "Run '$XLRINFRADIR/gce/gce-cluster-delete.sh ${clusterName}' once finished."
    else
        cleanup
    fi
else
    header "Success! \(^-^)/"
    cleanup
fi
exit $ret
