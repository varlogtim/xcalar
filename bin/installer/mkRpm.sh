#!/bin/bash

# This script uses the docker-rpm container to produce the Xcalar RPM.

function abort
{
    local message="$1"

    if [ ! -z "$message" ]; then
        echo "$message"
    else
        echo "Error occurred."
    fi

    exit 1
}

trap 'abort' ERR

if [ -z "$XLRDIR" ]; then
    abort '$XLRDIR must be set.'
fi

if [ -z "$1" ]; then
    abort 'Usage: ./mkRpm.sh <xcalar_version>'
fi
XLRVERSION="$1"

# Build container. If done previously, this will be fast.
#docker build --tag=docker-rpm "$XLRDIR/bin/installer/docker-rpm"

if [[ ! "$(git status | tail -1)" =~ "nothing to commit, working directory clean" ]]; then
    echo "Warning: uncommitted changes will not be included in RPM."
fi

# XXX Pass in signing details.

if [ ! -e "$XLRDIR/build" ]; then
    mkdir "$XLRDIR/build"
fi

# Create an instance of the container to execute the actual RPM creation.
#docker run --rm -v $XLRDIR:/xcalar -e BUILD_NUMBER=${BUILD_NUMBER:-1} docker-rpm $XLRVERSION
crun el7-build $XLRVERSION
