#!/bin/bash
#
# Generate the same XLRVERSION as configure.ac
# Useful for calling some build scripts (eg build-rpm.sh)
# directly without having to invoke the entire build.

set -e
VERSION=$(sed -r -n 's/^\s*\[xcalar-([0-9\.]*)\].*$/\1/p' $XLRDIR/configure.ac)
echo "${VERSION}"
