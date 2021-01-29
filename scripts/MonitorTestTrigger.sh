#!/bin/bash

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "usage: $0 <configFile> <git remote> <git branch> <numTest> <scriptPath>" >&2
    exit 1
fi

configFile=${1:-$XLRDIR/src/data/monitor.cfg}
remote=${2:-gerrit}
branch=${3:-trunk}
numTest=${4:-1}
scriptPath=${5:-$XLRDIR/src/bin/tests/monitorTest.sh}

cd $XLRDIR


git checkout $branch
git pull $remote $branch
build clean
build config
if [ $? -ne 0 ]; then
    echo "build config failed"
    exit 1
fi

build

if [ $? -ne 0 ]; then
    echo "build config failed"
    exit 1
fi


$scriptPath $configFile $numTest
