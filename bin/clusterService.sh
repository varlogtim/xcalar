#!/bin/bash

# Copyright 2015 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

function usage
{
    echo "usage: clusterService.sh [-u <username>] <action> <node0> [<node1> ... <nodeN>]" >&2
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

function stopNodes
{
    local nodes="$1"

    for node in $nodes; do
        ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -t \
            ${username}@${node} "sudo service xcalar stop"
    done
}

function startNodes
{
    local nodes="$1"

    for node in $nodes; do
        ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -t \
            ${username}@${node} "sudo service xcalar start"
    done
}


#
# Parse and validate arguments.
#

while getopts ":u:" opt; do
    case $opt in
        u)
            username="$OPTARG"
            ;;
        :)
            fail "-${OPTARG} requires an argument."
            ;;
        \?)
            fail "Invalid option -${OPTARG}."
            ;;
    esac
done

if [ -z "$username" ]; then
    username="$USER"
fi

shift $(( $OPTIND - 1 ))
action="$1"
shift 1
nodes="$@"

if [ -z "$nodes" ]; then
    fail "Must specify at least one node."
fi

case "$action" in
    start)
        startNodes "$nodes"
        ;;
    stop)
        stopNodes "$nodes"
        ;;
    restart)
        stopNodes "$nodes"
        startNodes "$nodes"
        ;;
    *)
        fail "Unknown action '$action'."
        ;;
esac
