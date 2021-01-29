#!/bin/bash

# Tests CentOS installer within a container. This script creates the container
# and executes a script within it.

docker run --rm --privileged -v $XLRDIR:/xcalar centos:7 /bin/bash \
       /xcalar/bin/installer/tests/testInstallDocker.sh
