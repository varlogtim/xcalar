#!/bin/bash

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "usage: $0 <configFilePath> <numTest> <XLRDIR> <monitorPath> " >&2
    exit 1
fi

is_localhost () {
    if [ "$1" == "127.0.0.1" ]|| [ "$1" == localhost ] || \
       [ "$1" == "$(hostname -s)" ] || [ "$1" == "$(hostname -f)" ]; then
        return 0
    fi
    return 1
}

_ssh () {
    local host="$1"
    shift
    if is_localhost "$host"; then
        eval "$@"
    else
        echo "This test can only run on the local host"
        exit 1
    fi
}

DIR=`dirname ${BASH_SOURCE[0]}`
DIR=$PWD/$DIR
. nodes.sh

XLRROOT=$DIR/../../..
# Don't start usrnodes
export XCMONITOR_NOUSRNODES=1

configPathDefault="${1:-$XLRDIR/src/data/monitorBig.cfg}"
numTest="${2:-1}"
XLRDIR="${3:-$XLRDIR}"
monitorPath="${4:-xcmonitor}"

. qa.sh
. $XLRDIR/doc/env/xc_aliases

monitorTest()
{
    configPath="${1:-$configPathDefault}"
    numNode=`sed  -n -Ee 's/^Node.NumNodes=(.*)/\1/p' $configPath`
    echo "total number nodes:$numNode"

    for ii in $(seq 1 $numTest)
    do
        echo "Test number $ii"
        murderNodes

        # Start up the xcmonitors
        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            _ssh $host "rm -rf /tmp/xcmonitor$jj"
            echo "start monitor $jj on host $host"
            _ssh $host "xcmonitor  -n $jj -c $configPath &> /tmp/xcmonitor$jj &"
        done

        maxRetries=600
        retryCount=0
        foundMaster=false

        echo "Waiting for monitor 0 to become master"

        while true
        do
            monitorLog="`_ssh $host "cat /tmp/xcmonitor0"`"
            echo "$monitorLog" | grep "STATE CHANGE:" | grep -q "=> Master"
            ret=$?
            if [ "$ret" = "0" ]; then
                echo "Monitor 0 became master.  Retry count: $retryCount"
                foundMaster=true
                break
            fi
            sleep 1
            ((retryCount++))
            if [ "$retryCount" -gt "$maxRetries" ];  then
                echo "Monitor 0 failed to become master"
                return 1
            fi
        done
    done

    echo "Clean up"
    murderNodes
    rm -rf /tmp/xcmonitor*

    return 0
}

manyNodeMonitorTest()
{
    monitorTest "$configPathDefaults"
}

addTestCase "manyNode monitor test" manyNodeMonitorTest $TestCaseEnable ""

runTestSuite

echo "1..$currentTestNumber"

xc2 cluster stop
tmpExitCode=$?
if [ "$tmpExitCode" != "0" ]; then
    echo "Failing sessionReplayTest because of unclean shutdown" 1>&2
    exit $tmpExitCode
fi

echo "*** monitorBigTest.sh ENDING ON `/bin/date`"
