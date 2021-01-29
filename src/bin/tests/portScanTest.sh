#!/bin/bash
# This test scans the ports used by Xcalar pids: monitor, usrnode, mgmtd, and
# sends these ports randomly generated data - large and small, a random number
# of times.

#if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    #echo "usage: $0 <configFilePath> <numTest> <XLRDIR> <monitorPath> <usernodePath> <cliPath> <XCE_LICENSEDIR>" >&2
    #exit 1
#fi

#set -x

. nodes.sh
. qa.sh

# since we're using launcher.sh to launch the cluster, there's only the one
# config file launcher.sh uses:
configPath="test-config.cfg"
nIters="${1:-3}"
nMaxAttacks="${2:-3}"
nMinAttacks="${3:-1}"
nAttacksRange=$(($nMaxAttacks - $nMinAttacks))
numNode=3

echo "This test iterates $nIters times"
echo "Total number nodes:$numNode"
echo "Attack range:" $nAttacksRange "from " $nMinAttacks " to " $nMaxAttacks
echo ""

source $XLRDIR/doc/env/xc_aliases

forcefulExit()
{
    exitCode=$1
    murderNodes
    echo "1..$currentTestNumber"
    echo "*** If one of your usrnodes crashed, find the core file in $XLRDIR/src/bin/tests/"
    exit $exitCode
}

startCluster()
{
    xc2 -vv cluster start --num-nodes ${numNode}

    usrnodeTimeOut=300
    usrnodeCount=0
    while true; do
        xccli -c "version" | grep "Backend Version"
        if [ $? -eq 0 ];then
            break
        fi
        sleep 1
        ((usrnodeCount++))
        if [ $usrnodeCount -gt $usrnodeTimeOut ]; then
            echo "usrnode failed to start"
            return 1;
        fi
    done

    if [ $? -ne 0 ]; then
        echo "usrnode failed to start"
        return 1
    else
        echo ""
        echo "Cluster is up!"
        echo ""
        return 0
    fi
}

doPortScanUsrnode()
{
    for ii in $(seq 1 $nIters)
    do
        # pick a random host and the host's usrnode port to attack
        let "randNode = $RANDOM % $numNode"
        host=`sed -n -Ee 's/^Node.'"$randNode"'.IpAddr=(.*)/\1/p' $configPath`
        port=`sed -n -Ee 's/^Node.'"$randNode"'.Port=(.*)/\1/p' $configPath`

        let "randNatt = $(($RANDOM % $nAttacksRange + $nMinAttacks))"
        echo "attacking usrnode $host:$port, $randNatt times"
        for jj in $(seq 1 $randNatt)
        do
            echo "$jj: attack $host:$port"
            nc $host $port < /dev/urandom            # send large streaming data
            perl -e 'print "a"x5' | nc $host $port  # send small no. of bytes
        done
    done
}

doPortScanApi()
{
    for ii in $(seq 1 $nIters)
    do
        # pick a random host and the host's usrnode port to attack
        let "randNode = $RANDOM % $numNode"
        host=`sed -n -Ee 's/^Node.'"$randNode"'.IpAddr=(.*)/\1/p' $configPath`
        port=`sed -n -Ee 's/^Node.'"$randNode"'.ApiPort=(.*)/\1/p' $configPath`

        let "randNatt = $(($RANDOM % $nAttacksRange + $nMinAttacks))"
        echo "attacking apiport $host:$port, $randNatt times"
        for jj in $(seq 1 $randNatt)
        do
            echo "$jj: attack $host:$port"
            nc $host $port < /dev/urandom            # send large streaming data
            perl -e 'print "a"x5' | nc $host $port  # send small no. of bytes
        done
    done
}

doPortScanMonitor()
{
    for ii in $(seq 1 $nIters)
    do
        # pick a random host and the host's monitor port to attack
        let "randNode = $RANDOM % $numNode"
        host=`sed -n -Ee 's/^Node.'"$randNode"'.IpAddr=(.*)/\1/p' $configPath`
        port=`sed -n -Ee 's/^Node.'"$randNode"'.MonitorPort=(.*)/\1/p' $configPath`

        let "randNatt = $(($RANDOM % $nAttacksRange + $nMinAttacks))"
        echo "attacking monitor port $host:$port, $randNatt times"
        for jj in $(seq 1 $randNatt)
        do
            echo "$jj: attack $host:$port"
            nc $host $port < /dev/urandom           # send large streaming data
            perl -e 'print "a"x5' | nc $host $port # send small no. of bytes
        done
    done

}

doPortScan()
{
    startCluster

    if [ $? -ne 0 ]; then
        echo "Couldn't start cluster! Exiting"
        return 1
    fi

    doPortScanMonitor
    doPortScanUsrnode
    doPortScanApi
}

addTestCase "port scan test" doPortScan $TestCaseEnable ""

runTestSuite

murderNodes

echo "1..3"
echo "*** portScanTest.sh ENDING ON `/bin/date`"
