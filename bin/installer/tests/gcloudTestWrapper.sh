#!/bin/bash

# Copyright 2015 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

PROJECT="angular-expanse-99923"
ZONE='us-central1-f'
NETWORK='default'
MAINTENANCE='MIGRATE'
IMAGE='ubuntu-14-04' # XXX Make param.
SCOPES="https://www.googleapis.com/auth/cloud.useraccounts.readonly","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write"

VM_PREFIX='xc-test-inst'
NFS_INSTANCE='xc-test-nfs'
HELPER_INSTANCE='xc-test-helper'
NOW=$(date +%s)


function usage
{
    echo "usage: gcloudTestWrapper.sh -i <installer_path> -n <node_count>" >&2
}

function fail
{
    # Print error message and/or usage then exit.

    local message=$1
    local skipUsage=$2

    echo ""

    if [ "$message" != "" ]; then
        # Print $message in red.
        echo -e "\e[31m${message}\e[0m" >&2
        echo "" >&2
    fi

    if [ "$skipUsage" = false -o "$skipUsage" = "" ]; then
        usage
        echo ""
    fi

    exit 1
}

function gcloudCreateVM
{
    # Creates gcloud instance (VM) with the given size and name and default
    # Xcalar parameters.

    instanceName=$1
    size=$2

    gcloud compute instances create "$instanceName" \
        --project $PROJECT                       \
        --zone $ZONE                             \
        --network $NETWORK                       \
        --maintenance-policy $MAINTENANCE        \
        --image $IMAGE                           \
        --machine-type "$size"                   \
        --scopes $SCOPES
}

function gcloudDeleteVMs
{
    # Deletes all gcloud instances (VMs) in the configured Xcalar project that
    # match the given pattern.

    pattern=$1

    for instance in $(gcloud compute instances list | grep "$pattern" | \
            awk '{print $1}'); do
        yes y | gcloud compute instances delete "$instance" --delete-disks all \
            --zone "$ZONE" &
    done
    wait
}

function teardownVms
{
    gcloudDeleteVMs "${VM_PREFIX}-${USER}"
}

function teardownNfs
{
    gcloudDeleteVMs "${NFS_INSTANCE}-${USER}"
}

function teardownHelper
{
    gcloudDeleteVMs "${HELPER_INSTANCE}-${USER}"
}

function testTeardown
{
    teardownVms &
    teardownNfs &
    teardownHelper &
    wait
}

function cleanFail
{
    testTeardown
    fail "$@"
}

function instanceGetIp
{
    local instanceName=$1

    output=$(gcloud compute instances describe "$instanceName" --format yaml | \
            grep natIP | tr -d '[[:space:]]' | cut -d: -f2)
}

function setupVms
{
    if [ -z "$nodeCount" ]; then
        fail "setupVms called without node count"
    fi

    # Create VMs.
    for i in $(seq -f "%5g" 0 $(( nodeCount - 1 ))); do
        gcloudCreateVM "${VM_PREFIX}-${USER}-${NOW}-${i}" "n1-highmem-8" &
    done
    wait # For instance creation jobs to complete.
    gcloud compute instances list
}

function setupNfs
{
    instanceName="${NFS_INSTANCE}-${USER}-${NOW}"

    gcloudCreateVM "$instanceName" "g1-small"

    # Allow node to become SSHable.
    sleep 16

    # Configure NFS on this instance.
    # XXX This is a security hole.
    gcloud compute ssh "$instanceName" --ssh-key-file ~/.ssh/id_rsa --command '
    sudo apt-get update > /dev/null
    sudo apt-get -y upgrade > /dev/null
    sudo apt-get -y install nfs-kernel-server
    sudo mkdir /public
    sudo chmod 0777 /public
    sudo sh -c "echo \"/public *(rw,sync,no_subtree_check)\" >> /etc/exports"
    sudo exportfs -ra
    sudo service rpcbind restart
    sudo service nfs-kernel-server restart
    '
}

function setupHelper
{
    # Creates a "helper" VM that orchestrates the test (so that everything is
    # local to gcloud and avoids communication between the nodes and host).

    gcloudCreateVM "${HELPER_INSTANCE}-${USER}-${NOW}" "g1-small"
}

function testSetup
{
    setupNfs &
    setupVms &
    setupHelper &
    wait
}


#
# Parse and validate arguments.
#

while getopts ":i:n:" opt; do
    case $opt in
    i)
        installerPath="$OPTARG"
        ;;
    n)
        nodeCount="$OPTARG"
        ;;
    :)
        fail "-${OPTARG} requires and argument."
        ;;
    \?)
        fail "Invalid option -${OPTARG}."
        ;;
    esac
done

if [ -z "$installerPath" ]; then
    fail "-i (installer path) required."
fi
if [ -z "$nodeCount" ]; then
    fail "-n (node count) required."
fi
if [[ ! $nodeCount =~ ^[0-9]+$ ]]; then
    fail "Node count ($nodeCount) must be an integer."
fi


#
# Prepare test environment in gcloud.
#

#testTeardown # Cleanup leftover state.

# Don't want VMs to keep running on error. Trap and clean up.
trap 'cleanFail "Fatal error occurred." true' ERR

testSetup

# Allow nodes to become SSHable.
sleep 16

nodes=""
for instance in $(gcloud compute instances list | \
        grep "${VM_PREFIX}" | awk '{print $1}'); do
    gcloud compute ssh "$instance" --ssh-key-file ~/.ssh/id_rsa \
        --command 'sudo apt-get -y install nfs-common'

    nodes="$nodes $instance"
done

#
# Launch cluster installer from helper VM.
#

helperInstance="${HELPER_INSTANCE}-${USER}-${NOW}"
nfsInstance="${NFS_INSTANCE}-${USER}-${NOW}"
gcloud compute copy-files --ssh-key-file ~/.ssh/id_rsa \
    "$XLRDIR"/bin/installer/clusterInstall.sh "$XLRDIR"/bin/installer/clusterInstallSsh.exp \
    "$XLRDIR"/bin/installer/clusterInstallScp.exp "$installerPath" "$helperInstance":/var/tmp

installerName=$(basename "$installerPath")
instanceGetIp "$helperInstance"
helperIp="$output"

ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oForwardAgent=yes \
    "${helperIp}" "sudo apt-get -y install nfs-common expect; /var/tmp/clusterInstall.sh -i '/var/tmp/$installerName' -h '$nfsInstance' -a -c '/public' $nodes"

echo "\nInstances created:"
echo "$nodes $helperInstance $nfsInstance"
echo "Be sure to delete once finished."
