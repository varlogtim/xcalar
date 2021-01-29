#!/bin/bash

set +e
#docker stop $(docker ps -aq)
#docker rm $(docker ps -aq)
#docker rmi $(docker images -aq)
set -e

export XLRDIR=`pwd`
export XLRGUIDIR=$XLRDIR/xcalar-gui
export XCE_LICENSEDIR=$XLRDIR/src/data
export XCE_LICENSEFILE=$XCE_LICENSEDIR/XcalarLic.key
export ExpServerd="false"
#export MYSQL_PASSWORD="i0turb1ne!"
export PATH="$XLRDIR/bin:/opt/xcalar/bin:$PATH"

export XLRGUIDIR=$PWD/xcalar-gui

# SDK-478 - build sanity now requires jupyter to run test_sqlmagic.py
JUPYTER_DIR=~/.jupyter
test -e $JUPYTER_DIR -a ! -h $JUPYTER_DIR && rm -rf $JUPYTER_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/jupyter $JUPYTER_DIR
IPYTHON_DIR=~/.ipython
test -e $IPYTHON_DIR -a ! -h $IPYTHON_DIR && rm -rf $IPYTHON_DIR
ln -sfn ${XLRGUIDIR}/xcalar-gui/assets/jupyter/ipython $IPYTHON_DIR

# Set this for pytest to be able to find the correct cfg file
pgrep -u `whoami` childnode | xargs -r kill -9
pgrep -u `whoami` usrnode | xargs -r kill -9
pgrep -u `whoami` xcmgmtd | xargs -r kill -9
# Nuke as soon as possible
#ipcs -m | cut -d \  -f 2 | xargs -iid ipcrm -mid || true
#rm /tmp/xcalarSharedHeapXX* || true
rm -rf /var/tmp/xcalar-jenkins/*
mkdir -p /var/tmp/xcalar-jenkins/sessions
rm -rf /var/opt/xcalar/*
git clean -fxd
#git submodule init
#git submodule update

. doc/env/xc_aliases


sudo pkill -9 gdbserver || true
sudo pkill -9 python || true
sudo pkill -9 usrnode || true
sudo pkill -9 childnode || true

# Debug build
set +e
xclean
set -e
cmBuild clean
cmBuild config debug

cmBuild xce

# Build xcalar-gui so that expServer will run
(cd $XLRGUIDIR && make dev)

cmBuild sanity

# dwillis(10/16/17) commenting this out since it doesn't seem to work
# mkdir -p $BUILD_DIRECTORY/qa/coverage
# bin/coverageReport.sh --output $BUILD_DIRECTORY/qa/coverage --type html

# Prod build
set +e
xclean
set -e
cmBuild config prod
cmBuild sanity
