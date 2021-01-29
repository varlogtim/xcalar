#!/bin/bash

# This script is leveraged from monitorTest.sh and is used to help understand xcmonitor.  It starts
# numNodes number of xcmonitors (as specified in monitorBig.cfg) each in a xterm window.
#
# To stop the instances requires "pkill -9 xterm" (careful if you have other things running in xterm)
#
# To restart instance:
#   xterm -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c 'xcmonitor -n 0 -c ../../data/monitorBig.cfg ' &

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
        ssh  -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no -oLogLevel=error ${host} "$@"
    fi
}

DIR=`dirname ${BASH_SOURCE[0]}`
DIR=$PWD/$DIR
XLRROOT=$DIR/../../..

configPathDefault="${1:-$XLRDIR/src/data/monitorBig.cfg}"
numTest="${2:-1}"
XLRDIR="${3:-$XLRDIR}"
monitorPath="${4:-xcmonitor}"

TERMINAL=${TERMINAL:-xterm}

. qa.sh

. $XLRDIR/doc/env/xc_aliases

forcefulExit()
{
    exitCode=$1
    murderNodes
    for filn in `/bin/ls -1 $TMP1`
    do
        grep -q -i assert $TMP1/$filn
        if [ "$?" = "0" ]
        then
            echo "*** assertion failure detected $filn: `grep -i assert $TMP1/$filn`"
        fi
    done
    echo "*** Leaving cliTest output in $TMP1 for your examination"
    echo "1..$currentTestNumber"
    echo "*** If one of your usrnodes crashed, find the core file in $XLRROOT/src/bin/tests/"
    exit $exitCode
}

monitorTest()
{
    configPath="${1:-$configPathDefault}"
    numNode=`sed  -n -Ee 's/^Node.NumNodes=(.*)/\1/p' $configPath`
    echo "total number nodes:$numNode"

    for ii in $(seq 1 $numTest)
    do
        echo "Test number $ii"
        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            # echo "kill usrnode, monitor $jj on host $host"

            _ssh $host "sudo pkill -9 usrnode > /dev/null "
            _ssh $host "sudo pkill -9 xcmonitor > /dev/null "
            _ssh $host "sudo pkill -9 childnode > /dev/null "

            local monitorKillTimeout=60
            local monitorKillCount=0
            while true; do
                local found=false

                _ssh $host "pgrep usrnode"
                if [ $? -eq 0 ]; then
                    found=true
                fi

                _ssh $host "pgrep xcmonitor"
                if [ $? -eq 0 ]; then
                    found=true
                fi

                _ssh $host "pgrep childnode"
                if [ $? -eq 0 ]; then
                    found=true
                fi

                if [ "$found" = "true" ]; then
                    if [ $monitorKillCount -gt $monitorKillTimeout ]; then
                        echo "failed to kill usrnode"
                        return 1
                    fi
                    sleep 1s
                    ((monitorKillCount++))
                else
                    break
                fi
            done
        done

        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            echo "start monitor $jj on host $host"

            monitorString="$monitorPath  -n $jj -c $configPath"
            if [ "$TERMINAL" = "gnome-terminal" ]; then
                $TERMINAL --tab --profile $Profile -e "$monitorString" &
            elif [ "$TERMINAL" = "lxterminal" ]; then
                $TERMINAL --title="node $host $jj" --command="$monitorString 2>&1 " &
            elif [ "$TERMINAL" = "noxterm" ]; then
                # This option is used when testing large number of nodes (e.g. > 185) where
                # xterm limitations don't allow that many windows
                $monitorString 2> /tmp/xcmonitor$jj &
            else
                # This option is used when you want the output to go to the
                # xterm window
                # Uncomment below if you want old style terminal; replacing
                # below with larger geometry and different position to make
                # 2-node cluster windows convenient....
                # $TERMINAL -title "node $host $jj" -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c "$monitorString" &
                if [ `expr $jj % 2` == 0 ]; then
                    $TERMINAL -geometry 120x32+0+0 -fg white -bg black -title "node $host $jj" -hold -sb -sl 1000000 -e /bin/bash -l -c "$monitorString" &
                else
                    $TERMINAL -geometry 120x32+1200+0 -fg white -bg black -title "node $host $jj" -hold -sb -sl 1000000 -e /bin/bash -l -c "$monitorString" &
                fi

                # This option is used when you want to enable the "Wait for monitors to elect a master".  Note
                # the output doesn't go to the xterm window.
                # $TERMINAL -title "node $host" -hold -sb -sl 1000000 -bg white -fg black -e /bin/bash -l -c "$monitorString > /tmp/xcmonitor$jj" &
            fi

            # sleep .1s
        done

    done

    return 0
}

manyNodeMonitorTest()
{
    monitorTest "$configPathDefaults"
}

addTestCase "manyNode monitor test" manyNodeMonitorTest $TestCaseEnable ""

runTestSuite

echo "1..$currentTestNumber"
echo "*** monitorBig.sh ENDING ON `/bin/date`"
