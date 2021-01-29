#!/bin/bash
DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
DIR="$(readlink -f $DIR)"

. $DIR/jenkinsUtils.sh

export ExpServerd="false"
export CCACHE_BASEDIR=$PWD
ScriptPath="/netstore/users/xma/dashboard/startFuncTests.py"
export XLRGUIDIR=$XLRDIR/xcalar-gui

rpm -q xcalar && sudo yum remove -y xcalar

sudo rm -rf /opt/xcalar/scripts

rm -rf /var/tmp/xcalar-`id -un`/* /var/tmp/xcalar-`id -un`/*
mkdir -p /var/tmp/xcalar-`id -un`/sessions /var/tmp/xcalar-`id -un`/sessions

source $XLRDIR/doc/env/xc_aliases

# SDK-478 - build sanity now requires jupyter to run test_sqlmagic.py
JUPYTER_DIR=~/.jupyter
test -e $JUPYTER_DIR -a ! -h $JUPYTER_DIR && rm -rf $JUPYTER_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/jupyter $JUPYTER_DIR
IPYTHON_DIR=~/.ipython
test -e $IPYTHON_DIR -a ! -h $IPYTHON_DIR && rm -rf $IPYTHON_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/ipython $IPYTHON_DIR

trap "postReviewToGerrit" EXIT

gitCheckoutIDL

sudo pkill -9 gdbserver || true
sudo pkill -9 usrnode || true
sudo pkill -9 childnode || true
sudo pkill -9 xcmgmtd || true
sudo pkill -9 xcmonitor || true
pgrep -f 'python*'$ScriptPath'.*' | xargs -r sudo kill -9

# Debug build
set +e
sudo rm -rf /opt/xcalar/scripts
xclean
set -e
ccache -s
cmBuild clean
cmBuild config debug
ccache -s
cmBuild
cmBuild qa

# Build xcalar-gui so that expServer will run
(cd $XLRGUIDIR && make dev)

cmBuild sanity

# Prod build
set +e
xclean
set -e
cmBuild clean
cmBuild config prod
ccache -s
cmBuild sanity
