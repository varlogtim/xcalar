#!/bin/bash

DIR=`dirname ${BASH_SOURCE[0]}`
NUM_NODES=3

function fail
{
    echo >&2 "$*"
    exit 1
}

if ! xc2 -vv cluster start --num-nodes $NUM_NODES; then
    fail
fi

# This test will directly connect to the above launched cluster

if ! "$DIR/libapisTestDriver"; then
    fail
fi

xc2 cluster stop

exit 0
