#!/bin/bash

# Library of node-related bash functions

. utils.sh
XLRINFRADIR="${XLRINFRADIR:-$XLRDIR/xcalar-infra}"
GUARD_RAILS_PATH=${GUARD_RAILS_PATH:-"$XLRINFRADIR/GuardRails/libguardrails.so.0.0"}
SQLDF_READ_TIMEOUT_MS=${SQLDF_READ_TIMEOUT_MS:-180000}

cgexecWrap () {
    local group="$1"
    shift
    CGROUPS_ENABLED=$(grep '^Constants.Cgroups' "$XCE_CONFIG" | tail -1 | cut -d '=' -f 2)
    [ -n "${container}" ] && CGROUPS_ENABLED="false"
    if [ "$CGROUPS_ENABLED" != "false" ]; then
        cgexec -g cpu,cpuacct,memory:${group} --sticky "$@"
    else
        "$@"
    fi
}

userBreakpoint()
{
    msg=$1
    if [ "$interactiveMode" != "" -a "$interactiveMode" = "1" ]; then
        # $XLRROOT/bin/gdball.sh
        echo "$msg Press enter to continue"
        read
    fi
}

# Wait for usrnode shutdown (defined as usrnodes going away)
waitForUserNodeShutdown()
{
    local whoami=`whoami`
    local numRetries=25
    local procPattern='usrnode'

    while pkill -0 -u $whoami $procPattern 2>/dev/null &&
            [ "$numRetries" != "0" ]; do
        echo "Waiting for processes: $(pgrep -u $whoami $procPattern)" 1>&2
        sleep 1
        numRetries=$(( numRetries - 1 ))
    done

    if pkill -0 -u $whoami $procPattern 2>/dev/null; then
        printInColor "red" "All XCE processes did not shutdown"
        return 1
    fi

    return 0
}

# If all else fails
murderNodes()
{
    xc2 cluster stop
}

guardRailsCheck() {
    local configFile=$1
    local cfgStr="Constants.NoChildLDPreload=true"

    if [ ! -e "$GUARD_RAILS_PATH" ]; then
        printInColor "red" "Error: cannot find guard rails at $GUARD_RAILS_PATH"
        printInColor "red" "Checkout xcalar-infra and build xcalar-infra/GuardRails"
        exit 1
    fi

    if ! grep -q "^$cfgStr" "$configFile"; then
        printInColor "red" "Error: please add $cfgStr to your config file at $configFile"
        exit 1
    fi

    local vmMapMax=$(cat /proc/sys/vm/max_map_count)

    if [ $vmMapMax -lt 1000000 ]; then
        printInColor "red" "Error: VM max map value $vmMapMax too low, please increase, eg:"
        printInColor "red" "    echo 10000000 | sudo tee /proc/sys/vm/max_map_count"
        exit 1
    fi
}

spawnOneNode()
{
    local nodeId=$1
    local numNodes=$2
    local outputdir=$3
    local memDebugger=$4
    local licenseFile=$5
    local valgrindCmd=""
    local valgrindCmdCommon="valgrind --log-file=$outputdir/valgrind.${nodeId}.out --error-limit=no -v --gen-suppressions=all --suppressions=$XLRROOT/src/data/debug/valgrind/usrnode.supp"

    case "$memDebugger" in
        valgrind)
            valgrindCmd="$valgrindCmdCommon"
            ;;
        valgrindtrack)
            printInColor "yellow" "Warning: valgrind tracking substantially increases memory overhead"
            valgrindCmd="$valgrindCmdCommon --track-origins=yes"
            ;;
        guardrails)
            guardRailsCheck "$configFile"
            LD_PRELOAD_TMP="$GUARD_RAILS_PATH"
            ;;
        *)
            ;;
    esac

    LD_PRELOAD="$LD_PRELOAD_TMP" \
    stdbuf -i0 -o0 -e0 $valgrindCmd usrnode --nodeId $nodeId --numNodes \
        $numNodes --configFile "$configFile" --licenseFile "$licenseFile" 1> "$outputdir/node.${nodeId}.out" 2> "$outputdir/node.${nodeId}.err"  &

    nodePids[$nodeId]=$!
    nodeJids[$nodeId]=`jobs -l | grep ${nodePids[$nodeId]} | cut -d \  -f 1 | sed 's/\[\([0-9]\+\)\]+/\1/'`
    echo "Spawning node $nodeId (pid: ${nodePids[$nodeId]} jid: ${nodeJids[$nodeId]})"
}

spawnNodes()
{
    local numNodes=$1
    local outputdir=$2
    local memDebugger=$3
    local licenseFile=$4
    local clusWaitCountMax=300
    local clusWaitCount=0

    if ! hash usrnode; then
        printInColor "red" "usrnode not found!"
        return 2
    fi

    if [ "$outputdir" = "" ]; then
        printInColor "red" "outputdir not specified!"
        return 3
    fi

    for ii in `seq 0 $(( $numNodes - 1 ))`; do
        spawnOneNode "$ii" "$numNodes" "$outputdir" "$memDebugger" "$licenseFile"
    done

    echo "Spawned $numNodes nodes"

    echo "Now wait (max $clusWaitCountMax secs) for cluster boot..."
    until xccli -c "version"; do
        sleep 1
        ((clusWaitCount++))
        echo "Waited $clusWaitCount secs for cluster boot..."
        if [ $clusWaitCount -gt $clusWaitCountMax ]; then
            printInColor "red" "cluster failed to start!"
            cat "Leaving $outputdir for further investigation" 1>&2
            return 5
        fi
    done
    return 0
}

spawnMgmtd()
{
    local XLRROOT=$1
    local OUTPUT=$2

    if ! hash xcmgmtd; then
        echo "xcmgmtd not found!\n"
        return 0
    fi

    echo "Killing any existing xcmgmtd processes"
    pkill -9 -u "$(whoami)" xcmgmtd
    killall -9 xcmgmtd

    echo "Launching xcmgmtd..."
    cgexecWrap xcalar_middleware_${USER} xcmgmtd "$XCE_CONFIG" 0<&- >> $OUTPUT 2>&1 &
    local MGMTPID=$!

    echo "xcmgmtd pid is $MGMTPID"

    output="$MGMTPID"

    kill -0 $MGMTPID

    return $?
}

spawnXcMonitor()
{
    local nodeId=$1
    local numNodes=$2
    local outputdir=$3
    local memDebugger=$4
    local licenseFile=$5

    local memDebuggerArg=""

    if ! hash xcmonitor; then
        echo "xcmonitor not found!\n"
        return 0
    fi

    case "$memDebugger" in
        valgrind*)
            printInColor "red" "Error: valgrind not supported with monitor (use Guard Rails) "
            exit 1
            ;;
        guardrails)
            guardRailsCheck "$XCE_CONFIG"
            memDebuggerArg="-g $GUARD_RAILS_PATH"
            ;;
        *)
            ;;
    esac

    local OUTPUT="$outputdir/xcmonitor.$nodeId"
    echo "Launching xcmonitor $nodeId ($OUTPUT)..."
    cgexecWrap xcalar_xce_${USER} xcmonitor -n $nodeId -m $numNodes -c "$XCE_CONFIG" -k "$licenseFile" $memDebuggerArg > $OUTPUT 2>&1 &
    local XCMONITORPID=$!

    echo "xcmonitor pid is $XCMONITORPID"

    kill -0 $XCMONITORPID

    return $?
}

spawnXcMonitors()
{
    local numNodes=$1
    local outputdir=$2
    local memDebugger=$3
    local licenseFile=$4

    local memDebuggerArg=""

    if ! hash xcmonitor; then
        echo "xcmonitor not found!\n"
        return 0
    fi

    case "$memDebugger" in
        valgrind*)
            printInColor "red" "Error: valgrind not supported with monitor (use Guard Rails) "
            exit 1
            ;;
        guardrails)
            guardRailsCheck "$XCE_CONFIG"
            memDebuggerArg="-g $GUARD_RAILS_PATH"
            ;;
        *)
            ;;
    esac

    for ii in `seq 0 $(( $numNodes - 1 ))`; do
        spawnXcMonitor "$ii" "$numNodes" "$outputdir" "$memDebugger" "$licenseFile"
    done

    return $?
}

spawnExpServer()
{
    local XLRGUIROOT=$1
    local OUTPUT=$2
    local EXPSVRDIR=$XLRGUIROOT/xcalar-gui/services/expServer

    if [ ! -e "$EXPSVRDIR/expServer.js" ]; then
        echo "$EXPSVRDIR/expServer.js not found\n"
        return 0
    fi

    echo "Killing any existing expServer processes"
    pgrep -u `whoami` -f expServer -l | grep "node" | cut -d\  -f1 | xargs -r kill -9

    echo "Launching expServer..."
    cgexecWrap xcalar_middleware_${USER} npm start --prefix $EXPSVRDIR 0<&- >> $OUTPUT 2>&1 &
    local EXPSVRPID=$!

    echo "expServer pid is $EXPSVRPID"

    output="$EXPSVRPID"

    kill -0 $EXPSVRPID

    return $?
}

spawnJupyter()
{
    local XCALARGUIROOT=$1
    local OUTPUT=$2

    jupyter_cmd=$(command -v 'jupyter-notebook')
    rc=$?

    if [ "$rc" != "0" ]; then
        echo "jupyter not found\n"
        return 0
    fi

    echo "Killing any existing Jupyter processes"
    pgrep -u `whoami` -f jupyter-notebook | xargs -r kill -9

    echo "Launching Jupyter..."
    cgexecWrap xcalar_middleware_${USER} python3.6 "$jupyter_cmd" 0<&- >> $OUTPUT 2>&1 &
    local JUPYTERPID=$!

    echo "jupyter pid is $JUPYTERPID"

    output="$JUPYTERPID"

    kill -0 $JUPYTERPID

    return $?
}

spawnSqldf()
{
    local whoami=`whoami`
    local SQLDF="$XLRDIR/src/sqldf/sbt/target/xcalar-sqldf.jar"
    local OUTPUT=$1
    local tmpDir="${TMP1:-/tmp}/tmpsqldf"

    if [ ! -e "$SQLDF" ]; then
        printInColor "yellow" "sqldf build not found at $SQLDF, falling back to installed version"
        SQLDF="/opt/xcalar/lib/xcalar-sqldf.jar"
        if [ ! -e "$SQLDF" ]; then
            printInColor "yellow" "sqldf build not found at $SQLDF, running without SQL/JDBC support"
            return 1
        fi
    fi

    echo "Killing any existing xcalar-sqldf processes"
    pkill -9 -fu $whoami 'xcalar-sqldf.jar'

    echo "Launching xcalar-sqldf with read timeout: $SQLDF_READ_TIMEOUT_MS"
    cgexecWrap xcalar_middleware_${USER} /bin/bash -c "java -Djava.io.tmpdir=\"$tmpDir\" -jar \"$SQLDF\" -r $SQLDF_READ_TIMEOUT_MS -jPn -R 10000" <&- >> $OUTPUT 2>&1 &
    local SQLDFPID=$!

    echo "xcalar-sqldf pid is $SQLDFPID"

    output="$SQLDFPID"

    kill -0 $SQLDFPID

    return $?
}

waitSqldf() {
    local jdbc=${1-false}
    local port=27000 # Default to plan server wait
    local maxWaitSecs=${2-60}
    local waitSecs=0

    $jdbc && port=10000 # Wait for JDBC server

    # -z option not supported in all nc versions
    local ckCmd="eval nc -w1 localhost $port < /dev/null"

    while sleep 1 && ! $ckCmd && [[ $waitSecs -lt $maxWaitSecs ]]
    do
        let waitSecs+=1
        echo "Waited $waitSecs seconds for sqldf on port $port"
    done

    if $ckCmd
    then
        printInColor "green" "Detected xcalar-sqldf running on port $port"
        return 0
    else
        printInColor "red" "Failed to detect xcalar-sqldf running on port $port"
        return 1
    fi
}

waitForNodes()
{
    for ii in `seq 0 $(( $numNodes - 1 ))`; do
        local nodePid=${nodePids[$ii]}
        wait $nodePid
        ret=$?
        if [ $ret -ne 0 ]; then
            printInColor "red" "Node $ii (pid: $nodePid) died with exitCode $ret"
            return $ret
        fi
        echo "Node $ii (pid: $nodePid) exited cleanly"
    done

    return 0
}
