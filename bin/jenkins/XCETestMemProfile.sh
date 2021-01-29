#!/bin/bash

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

. $DIR/jenkinsUtils.sh

source $XLRDIR/doc/env/xc_aliases

configFile="$XLRDIR/src/data/test.cfg"
export XCE_LICENSEDIR=$XLRDIR/src/data
export XCE_LICENSEFILE=${XCE_LICENSEDIR}/XcalarLic.key
export ExpServerd="true"
export XLRGUIDIR=$PWD/xcalar-gui
export XLRINFRADIR=$PWD/xcalar-infra
export GuardRailsArgs

# SDK-478 - build sanity now requires jupyter to run test_sqlmagic.py
JUPYTER_DIR=~/.jupyter
test -e $JUPYTER_DIR -a ! -h $JUPYTER_DIR && rm -rf $JUPYTER_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/jupyter $JUPYTER_DIR
IPYTHON_DIR=~/.ipython
test -e $IPYTHON_DIR -a ! -h $IPYTHON_DIR && rm -rf $IPYTHON_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/ipython $IPYTHON_DIR

trap "postReviewToGerrit" EXIT

gitCheckoutIDL

rm -rf /var/tmp/xcalar-`id -un`/*
rm -rf /var/opt/xcalar/*

mkdir -p /var/tmp/xcalar-`id -un`/sessions

echo "BUILD_DEBUG: xclean"
set +e
xclean
set -e
echo "BUILD_DEBUG: cmBuild clean"
cmBuild clean

echo >&2 "Configuring for build type '${XCE_BUILD_TYPE}'"
# Enable Buffer Cache poison and tracing
# XXX TODO Enable -DUB_SANITIZER along with -DADDRESS_SANITIZER when jenkins
# slaves get more resources.
export BUFCACHEPOSION="true"
export BUFCACHETRACE="true"
cmBuild config "${XCE_BUILD_TYPE}" -DADDRESS_SANITIZER=$ADDRESS_SANITIZER_FLAG

if [ "$ADDRESS_SANITIZER_FLAG" = "OFF" ]
then
    echo "BUILD_DEBUG: Running Guardrails: $GuardRailsArgs"

    # Note that puppet should already take care of this and we need to avoid
    # sudo privileges.

    # Increase the mmap map count for GuardRails to work.
    map_count_var=$(cat /proc/sys/vm/max_map_count)
    if [ "$map_count_var" != 100000000 ]
    then
        echo 100000000 | sudo tee /proc/sys/vm/max_map_count
    fi

    # set this config param for GuardRails
    echo "Constants.NoChildLDPreload=true" >> $configFile

    # Build GuardRails
    make -C xcalar-infra/GuardRails clean
    make -C xcalar-infra/GuardRails deps
    make -C xcalar-infra/GuardRails
else
    echo "BUILD_DEBUG: Running ASAN"
    unset GuardRailsArgs

    # Set this config param for ASAN
    echo "Constants.EnforceVALimit=false" >> $configFile

    # Add ASAN and LSAN suppression options
    ASAN_OPTIONS="$ASAN_OPTIONS:suppressions=$XLRDIR/bin/ASan.supp"
    echo "BUILD_DEBUG: ASAN_OPTIONS: $ASAN_OPTIONS"

    if [ -z "$LSAN_OPTIONS" ]
    then
        LSAN_OPTIONS="suppressions=$XLRDIR/bin/LSan.supp"
    else
        LSAN_OPTIONS="$LSAN_OPTIONS:suppressions=$XLRDIR/bin/LSan.supp"
    fi
    echo "BUILD_DEBUG: LSAN_OPTIONS: $LSAN_OPTIONS"

    if [ -z "$UBSAN_OPTIONS" ]
    then
        UBSAN_OPTIONS="suppressions=$XLRDIR/bin/UBSan.supp"
    else
        UBSAN_OPTIONS="$UBSAN_OPTIONS:suppressions=$XLRDIR/bin/UBSan.supp"
    fi
fi

cmBuild xce
cmBuild qa

# Build xcalar-gui so that expServer will run
(cd $XLRGUIDIR && make dev)

# Run build sanity
cmBuild sanity

xclean
