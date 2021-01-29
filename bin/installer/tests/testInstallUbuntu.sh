#!/bin/bash

# Tests ubuntu installer within a container. This portion of the test creates
# the container and executes a script within it.

docker run --rm --privileged -v $XLRDIR:/xcalar ubuntu:14.04 /bin/bash \
           /xcalar/bin/installer/tests/testInstallDocker.sh
