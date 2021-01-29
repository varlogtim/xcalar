#!/bin/bash

# XXX:the script should be changed to use xcalar start script if running on a installer installed machine
if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "usage: $0 <XLRDIR> <numTest>" >&2
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
. nodes.sh

XLRDIR="${XLRDIR:-$1}"
configPathDefault="$XLRDIR/src/data/monitor.cfg"
numTest="${2:-1}"
configPath1Node="$XLRDIR/src/data/monitor-1Node.cfg"
configPathStress="$XLRDIR/src/data/monitorStress.cfg"
configPathElection="$XLRDIR/src/data/monitorTimeout.cfg"
xcalarStartCmd="/opt/xcalar/bin/xcalarctl start"
xcalarStopCmd="/opt/xcalar/bin/xcalarctl stop-supervisor"

sleepTimeForUsrNodeStart=3
maxExtraSleepTime=5
killTimeOut=60

# see comments for killTimeOutStress in multiNodeMonitorStressTest() below
killTimeOutStress=360 # 60 + monitorHeartBeatIntvl

if [ -z "${container+x}" ]; then
    export XCE_CHILDNODE_PATHS="/sys/fs/cgroup/cpu,cpuacct/xcalar.slice/xcalar-usrnode.service:/sys/fs/cgroup/memory/xcalar.slice/xcalar-usrnode.service"
    export XCE_CGROUP_CONTROLLER_MAP="memory%/sys/fs/cgroup/memory:cpu%/sys/fs/cgroup/cpu,cpuacct:cpuacct%/sys/fs/cgroup/cpu,cpuacct:cpuset%/sys/fs/cgroup/cpuset"
    export XCE_CGROUP_CONTROLLERS="memory cpu cpuacct cpuset"
    export XCE_CGROUP_UNIT_PATH="xcalar.slice/xcalar-usrnode.service"
    export XCE_CHILDNODE_SCOPES="sys_xpus-sched0 sys_xpus-sched1 sys_xpus-sched2 usr_xpus-sched0 usr_xpus-sched1 usr_xpus-sched2"
fi

. qa.sh
. $XLRDIR/doc/env/xc_aliases

forcefulExit()
{
    exitCode=$1
    murderNodes
    echo "1..$currentTestNumber"
    echo "*** If one of your usrnodes crashed, find the core file in $XLRDIR/src/bin/tests/"
    exit $exitCode
}

killExistingProcesses()
{
    numNode=$1

    for jj in $(seq 0 $(($numNode - 1)))
    do
        # Delete any /tmp files associated with the instance as the tests
        # grep for content and we don't want stale content
        echo "Removing /tmp/xcmonitor$jj"
        sudo rm -f /tmp/xcmonitor$jj
    done

    xc2 cluster stop
}

monitorTest()
{
    configPath="${1:-$configPathDefault}"
    numNode=`sed  -n -Ee 's/^Node.NumNodes=(.*)/\1/p' $configPath`
    echo "total number nodes:$numNode"

    # Get rid of any existing processes

    for ii in $(seq 1 $numTest)
    do
        echo "Test number $ii"

        killExistingProcesses $numNode
        if [ $? != "0" ]; then
            echo "Unable to kill existing processes"
            exit 1;
        fi

        # For each node, start xcmonitor, sleeping a bit between hosts

        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            echo "start monitor $jj on host $host"
            echo "xcmonitor will start usrnode $jj on host $host "
            _ssh $host "xcmonitor  -n $jj -c $configPath &> /tmp/xcmonitor$jj &"
            # Add sleep so different monitors come up at different times
            sleep 1
        done

        # Wait for monitors to elect a master

        monitorTimeout=600
        monitorCount=0
        foundMaster=false

        while true; do
            for jj in $(seq 0 $(($numNode - 1)))
            do
                monitorLog="`_ssh $host "cat /tmp/xcmonitor$jj"`"
                echo "$monitorLog" | grep "STATE CHANGE:" | grep -q "=> Master"
                ret=$?
                if [ "$ret" = "0" ]; then
                    echo "Monitor $jj became master"
                    echo "$monitorLog" | grep "STATE CHANGE"
                    foundMaster=true
                    break
                fi

                echo "$monitorLog" | grep -q "ERR:"
                ret=$?
                if [ "$ret" = "0" ]; then
                    echo "Monitor $jj encountered an error"
                    echo "$monitorLog" | grep "ERR:"
                    return 1
                fi
            done
            if [ "$foundMaster" = "true" ]; then
                break
            fi
            sleep 1
            ((monitorCount++))
            if [ "$monitorCount" -gt "$monitorTimeout" ]; then
                echo "Monitor failed to become master"
                return 1
            fi
        done

        usrnodeTimeOut=1200
        usrnodeCount=0

        # Ensure the cluster is operational (successfully returns the
        # version)

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
        fi

        # Make sure monitors are still alive

        if [ `pgrep xcmonitor | wc -l` != "$numNode" ]; then
            echo "1 or more xcmonitor died before test even began"
            return 1
        fi

        # Sleep for a random amount of time...not sure why...we've already
        # ensured the cluster is operational.

        let "sleepTime = $RANDOM % $maxExtraSleepTime + $sleepTimeForUsrNodeStart"

        sleep $sleepTime
        echo "wakeup"

        # Decide if we're going to kill one or two usrnodes and then do it

        let "killTwoUsrNode = $RANDOM % 2"
        if [ $killTwoUsrNode -eq 1 -a $numNode -ge 2 ]; then
            host=`sed  -n -Ee 's/^Node.0.IpAddr=(.*)/\1/p' $configPath`
            _ssh $host "pkill -SIGKILL -f \"usrnode --nodeId 0\"" &

            host=`sed  -n -Ee 's/^Node.1.IpAddr=(.*)/\1/p' $configPath`
            _ssh $host "pkill -SIGKILL -f \"usrnode --nodeId 1\"" &
        else
            let "nodeIdToKill = $RANDOM % $numNode"
            echo "kill node $nodeIdToKill"

            host=`sed  -n -Ee 's/^Node.'"$nodeIdToKill"'.IpAddr=(.*)/\1/p' $configPath`
            _ssh $host "pkill -SIGKILL -f \"usrnode --nodeId $nodeIdToKill\""

        fi

        # Killing the usrnode(s) should result in the remaining usrnode
        # getting killed by its xcmonitor.

        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            echo "verify usrnode $jj killed on host $host "

            count=0
            while true; do
                _ssh $host "pgrep usrnode" > /dev/null
                if [ $? -ne 0 ];then
                    break
                fi
                sleep 1
                ((count++))
                if [ $count -gt $killTimeOut ]; then
                    echo "monitor failed to bring down the node $jj"
                    return 1;
                fi
            done

            count=0
            while true; do
                _ssh $host "pgrep xcmonitor" > /dev/null
                if [ $? -ne 0 ];then
                    break
                fi
                sleep 1
                ((count++))
                if [ $count -gt $killTimeOut ]; then
                    echo "monitor $jj failed to self-terminate"
                    return 1;
                fi
            done

            echo "usrnode $jj killed"
        done
    done

    return 0
}

monitorStressTest()
{
    configPath="${1:-$configPathDefault}"
    scope="${2:-local}"
    nIters="${3:-5}"
    numNode=`sed  -n -Ee 's/^Node.NumNodes=(.*)/\1/p' $configPath`
    echo "monitorStressTest $configPath $scope $nIters starting"
    echo "total number nodes:$numNode"

    for ii in $(seq 1 $nIters)
    do
        echo "Test number $ii"
        if [ $ii -eq 1 ]; then

            # Get rid of any existing processes only for the very first test iteration.
            # Subsequent iterations continue only if all usrnodes/xcmonitors are dead
            # so no need to check on every test iteration
            killExistingProcesses $numNode
            if [ $? != "0" ]; then
                echo "Unable to kill existing processes"
                exit 1;
            fi
        fi

        echo "all nodes clean and down: starting cluster for next iter..."

        sleep 5 # let sockets drain from prior xcmonitors since this is a loop

        # For each node, start xcmonitor

        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            if [ $scope == "local" ]; then
                echo "start monitor $jj on host $host"
                _ssh $host "xcmonitor  -n $jj -c $configPath &> /tmp/xcmonitor$jj &"
                echo "xcmonitor will start usrnode $jj on host $host "
            else
                echo "issue xcalarctl start for node $jj on host $host"
                _ssh $host "$xcalarStartCmd &> /tmp/xcmonitor$jj &"
                echo "xcalarctl will start xcmonitor which will start usrnode"
            fi
            sleep 1
        done

        # This test is not about master election; this is assumed to work
        # So just wait for whole cluster to be up

        usrnodeTimeOut=1200
        usrnodeCount=0

        # Ensure the cluster is operational (successfully returns the
        # version)

        if [ $scope == "global" ]; then
            # just get one host's ip address for cluster: might as well be 0
            host=`sed -n -Ee 's/^Node.0.IpAddr=(.*)/\1/p' $configPath`
        fi
        while true; do
            if [ $scope == "local" ]; then
                xccli -c "version" | grep "Backend Version"
            else
                _ssh $host "xccli -c \"version\" | grep \"Backend Version\""
            fi
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

        echo "cluster is up"

        # Make sure monitors are still alive
        # in global case, checking all nodes for xcmonitors being alive
        # seems like overkill- skip it for now

        if [ $scope == "local" ]; then
            if [ `pgrep xcmonitor | wc -l` != "$numNode" ]; then
                echo "1 or more xcmonitor died before test even began"
                return 1
            fi
        fi

        # kill some random usrnodes - eventually, replace SIGKILL with another
        # signal (say SIGUSR2) which can be handled inside usrnode so that it
        # can randomly pick different ways of committing suicide.

        let "killNumUsrNode = $(($RANDOM % ($numNode + 1)))"
        if [ $killNumUsrNode -ne 0 ]; then
            for kk in $(seq 1 $killNumUsrNode)
            do
                nodeNum=$(($kk - 1))
                echo "kill usrnode $nodeNum"
                host=`sed -n -Ee 's/^Node.'"$nodeNum"'.IpAddr=(.*)/\1/p' $configPath`
                _ssh $host "pkill -SIGKILL -f \"usrnode --nodeId $nodeNum\" &> /tmp/xcmonitor$nodeNum &"
            done
        else
            echo "not killing any usrnodes"
        fi

        # repeat loop to kill some xcmonitor nodes

        let "killNumXcmon = $(($RANDOM % ($numNode + 1)))"
        #
        # if random selection picks 0 usrnodes and 0 xcmons to kill,
        # then kill at least one xcmonitor
        #
        if [ $killNumUsrNode -eq 0 -a $killNumXcmon -eq 0 ]; then
            echo "0 nodes were selected to be killed; pick at least 1 xcmon"
            killNumXcmon=1
        fi
        if [ $killNumXcmon -ne 0 ]; then
            for kk in $(seq 1 $killNumXcmon)
            do
                nodeNum=$(($kk - 1))
                echo "kill xcmonitor $nodeNum"
                host=`sed -n -Ee 's/^Node.'"$nodeNum"'.IpAddr=(.*)/\1/p' $configPath`
                _ssh $host "pkill -SIGKILL -f \"xcmonitor -n $nodeNum\"  &> /tmp/xcmonitor$nodeNum &" 2> /dev/null
            done
        else
            echo "not killing any xcmonitor nodes"
        fi

        for jj in $(seq 0 $(($numNode - 1)))
        do
            host=`sed  -n -Ee 's/^Node.'"$jj"'.IpAddr=(.*)/\1/p' $configPath`
            echo "verify usrnode and xcmonitor $jj killed on host $host "

            count=0
            while true; do
                _ssh $host "pgrep usrnode" > /dev/null
                if [ $? -ne 0 ];then
                    break
                fi
                sleep 1
                ((count++))
                if [ $count -gt $killTimeOutStress ]; then
                    echo "failure to bring down usrnode $jj"
                    echo ""
                    echo "TEST $ii FAILED!"
                    echo ""
                    return 1;
                fi
            done

            count=0
            while true; do
                _ssh $host "pgrep xcmonitor" > /dev/null
                if [ $? -ne 0 ];then
                    break
                fi
                sleep 1
                ((count++))
                if [ $count -gt $killTimeOut ]; then
                    echo "monitor $jj failed to self-terminate"
                    echo ""
                    echo "TEST $ii FAILED!"
                    echo ""
                    return 1;
                fi
            done
            echo "usrnode and xcmonitor $jj killed"
        done
        echo ""
        echo "TEST $ii of multinode stress PASSED"
        echo ""
    done
    return 0
}

multiNodeMonitorStressTest()
{
    # Start many nodes; pick random subset of both usrnodes and xcmonitors to
    # kill, and then verify that whole cluster is brought down in some time
    #
    # Use a timeout of killTimeOutStress for usrnode death (if a usrnode's
    # monitor parent dies, it may wait upto monitorHeartBeatIntvl seconds to
    # detect that the monitor died - so the test must wait longer: so
    # killTimeOutStress must be larger than monitorHeartBeatIntvl). The timeout
    # for a monitor to die is the same as for the other tests (killTimeOut).
    #
    # The first arg is configfile: use a different config file so no. of nodes
    # for this test can be changed later, independent of the other tests. For
    # now, use 4 nodes.
    #
    # The second arg is scope: local or global: for now use local (=localhost).
    # global scope (remote hosts) is separated out since start/stop etc. are a
    # little different. This may not be needed.
    #
    # The third arg is no. of iterations: for sanity, use 2; crank it up later

    monitorStressTest "$configPathStress" "local" "2"
}

electionTimeoutTest()
{
    configPath="${1:-$configPathDefault}"
    skipNode="${2:-0}"

    numNode=`sed  -n -Ee 's/^Node.NumNodes=(.*)/\1/p' $configPath`
    echo "electionTimeoutTest $configPath starting"
    echo "total number of nodes:$numNode"

    killExistingProcesses $numNode

    # For each node, except for the node being skipped, start xcmonitor,
    # sleeping a bit between hosts

    for ii in $(seq 0 $(($numNode - 1)))
    do
        if [  $ii -ne $skipNode ]; then
            host=`sed  -n -Ee 's/^Node.'"$ii"'.IpAddr=(.*)/\1/p' $configPath`
            echo "start monitor $ii on host $host"
            _ssh $host "xcmonitor  -n $ii -c $configPath &> /tmp/xcmonitor$ii &"
            # Sleep so different monitors come up at different times
            sleep 1
        fi
    done

    # Wait for monitors to timeout the election

    maxRetries=600
    retryCount=0
    electionTimedOut=false

    while true; do
        # Check the xcmonitor logs on each node until one of them times out
        # the election or the retry count is exceeded.
        for ii in $(seq 0 $(($numNode - 1)))
        do
            if [  $ii -ne $skipNode ]; then
                monitorLog="`_ssh $host "cat /tmp/xcmonitor$ii"`"
                echo "$monitorLog" | grep "Timed out waiting for candidate message from master"
                ret=$?
                if [ "$ret" = "0" ]; then
                    echo "Node $ii timed out the election"
                    electionTimedOut=true
                    break
                fi
                echo "$monitorLog" | grep "Failed to complete election"
                ret=$?
                if [ "$ret" = "0" ]; then
                    echo "Node $ii timed out the election"
                    electionTimedOut=true
                    break
                fi
            fi
        done
        if [ "$electionTimedOut" = "true" ]; then
            break
        fi
        sleep 1
        ((retryCount++))
        if [ "$retryCount" -gt "$maxRetries" ]; then
            echo "Election failed to timeout"
            return 1
        fi
    done

    return 0
}

threeNodesMonitorTest()
{
    # Start three usrnodes and then kill either one or two of them

    monitorTest "$configPathDefault"
}

oneNodeMonitorTest()
{
    # Start one usernode and kill it.  The xcmonitor should detect and die.

    monitorTest "$configPath1Node"
}

threeNodesMasterTimeoutTest()
{
    # Don't start the master (node 0).  The other two nodes should timeout
    # the election.

    electionTimeoutTest "$configPathElection" 0
}

threeNodesSlaveTimeoutTest()
{
    # Don't start one of the slaves (node 2).  The master and other slave
    # should timeout the election.

    electionTimeoutTest "$configPathElection" 2
}

addTestCase "threeNodes monitor test" threeNodesMonitorTest $TestCaseEnable ""
addTestCase "oneNode monitor test" oneNodeMonitorTest $TestCaseEnable ""
addTestCase "multinode monstress test" multiNodeMonitorStressTest $TestCaseEnable ""
addTestCase "threeNodes master timeout" threeNodesMasterTimeoutTest $TestCaseEnable ""
addTestCase "threeNodes slave timeout" threeNodesSlaveTimeoutTest $TestCaseEnable ""

runTestSuite

echo "1..$currentTestNumber"

killExistingProcesses
tmpExitCode=$?
if [ "$tmpExitCode" != "0" ]; then
    echo "Failing monitorTest because of unclean shutdown" 1>&2
    exit $tmpExitCode
fi

echo "*** monitorTest.sh ENDING ON `/bin/date`"
