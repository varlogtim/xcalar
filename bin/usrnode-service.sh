#!/bin/bash

echo $$ > "$XCE_WORKDIR"/xcalar.pid

PATH=/opt/xcalar/bin:/opt/xcalar/scripts:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$XCE_USRNODE_PATH:$PATH
LD_LIBRARY_PATH=/opt/xcalar/lib:/opt/xcalar/lib64:$XCE_USRNODE_LD_LIB_PATH:$LD_LIBRARY_PATH

if [ -n "$XLRDIR" ]; then
    PATH="$XLRDIR"/bin:"$XLRDIR"/scripts:$PATH
fi

export PATH LD_LIBRARY_PATH

test -n "$KRB5_CONFIG" && export KRB5_CONFIG
test -n "$KRB5_KDC_PROFILE" && export KRB5_KDC_PROFILE
test -n "$KRB5_KTNAME" && export KRB5_KTNAME
test -n "$KRB5CCNAME" && export KRB5CCNAME
test -n "$KPROP_PORT" && export KPROP_PORT
test -n "$KRB5RCACHETYPE" && export KRB5RCACHETYPE
test -n "$KRB5RCACHEDIR" && export KRB5RCACHEDIR
test -n "$LD_PRELOAD" && export LD_PRELOAD

CGROUPS_ENABLED=$(grep '^Constants.Cgroups' "$XCE_CONFIG" | tail -1 | cut -d '=' -f 2)
[ -n "${container}" ] && CGROUPS_ENABLED="false"
declare -a CGROUPS_V1_CONTROLLERS=("memory" "cpu" "cpuacct")
declare -a CREATED_CONTROLLER_PATHS=()
declare -a CREATED_CHILDNODE_PATHS=()
declare -a CHILDNODE_SCOPES="sys_xpus-sched0 sys_xpus-sched1 sys_xpus-sched2 usr_xpus-sched0 usr_xpus-sched1 usr_xpus-sched2"
CGROUPS_MODE="1"

INSTANCE_ID="${1:--1}"

. determineNodeId.sh

. cgroupMgmtUtils.sh

setupLogDir ()
{
    XCE_LOGDIR="$(awk -F'=' '/^Constants.XcalarLogCompletePath/ {a=$2} END{print a}' $XCE_CONFIG)"
    XCE_LOGDIR="${XCE_LOGDIR:-/var/log/xcalar}"
    export XCE_LOGDIR
}

setupMain()
{
    if [ ! -r "$XCE_CONFIG" ]; then
        echo >&2 "Failed to read $XCE_CONFIG"
        exit 1
    fi

    # Parse config file to determine which nodes are on this host.
    determineNodeId
    if [ -z "$nodeId" ]; then
        echo >&2 "Could not determine node ID for this host. Please check your config file (${XCE_CONFIG})."
        exit 1
    fi

    # nodeId is a string containing the list of all the node numbers in the .cfg file
    # running on the local node
    # this converts that string to an array, and uses the first entry to find the
    # ApiPort for one usrnode process running on the local node and the monitor port
    # if we are running one usrnode process per host
    nodeIdArr=($nodeId)

    echo "Node ID: ${nodeIdArr[0]}"

    XCE_MONITORPORT="$(grep "^Node.${nodeIdArr[0]}.MonitorPort" $XCE_CONFIG | awk -F'=' '{print $2}')"
    export XCE_MONITORPORT

    if [ -z "$XCE_MONITORPORT" ]; then
        echo >&2 "Node.${nodeIdArr[0]}.MonitorPort not found. Possibly outdated or corrupt config file (${XCE_CONFIG})."
        exit 1
    fi

    apiPortNum=$(grep "^Node.${nodeIdArr[0]}.ApiPort" "$XCE_CONFIG" | cut -d '=' -f 2)

    if [ -z "$apiPortNum" ]; then
        echo >&2 "Node.${nodeIdArr[0]}.ApiPort value not found.  Possibly outdated or corrupt config file (${XCE_CONFIG})."
        exit 1
    fi

    numNodes=$(grep -v '^//' "$XCE_CONFIG" | grep 'NumNodes' | cut -d '=' -f 2)
    if [ -z "$numNodes" ] ||  [ "$numNodes" -ne "$numNodes" ] 2>/dev/null; then
       echo >&2 "Failed to determine number of nodes. Possibly corrupt config file (${XCE_CONFIG})."
       exit 1
    fi

    # Count the usrnode instances configured for this host
    numLocalNodes=0
    for ii in $nodeId; do
        nodeIdVerify "$ii"
        ((numLocalNodes++))
    done

    read THPG_DEFRAG < /sys/kernel/mm/transparent_hugepage/defrag
    read KHPGD_DEFRAG < /sys/kernel/mm/transparent_hugepage/khugepaged/defrag

    if [[ $THPG_DEFRAG =~ never$ ]] || [ "$KHPGD_DEFRAG" == "1" ] || [ "$KHPGD_DEFRAG" == "yes" ]; then
        echo >&2 "WARNING: transparent hugepage defragmentation is active"
        echo >&2 "WARNING: this may result in reduced performance for Xcalar"
        echo >&2 "/sys/kernel/mm/transparent_hugepage/defrag = $THPG_DEFRAG"
        echo >&2 "/sys/kernel/mm/transparent_hugepage/khugepaged/defrag = $KHPGD_DEFRAG"
    fi

}

joinBy() { local IFS="$1"; shift; echo "$*"; }

cgroupSetup() {
    case "$CGROUPS_MODE" in
        "1")
            cgroupFindCgroupUnitPath "$XCE_USRNODE_UNIT"

            mkdir -p "${CGROUPS_SYSTEMD_MOUNT}/${CGROUP_UNIT_PATH}/${XCE_USRNODE_SCOPE}"

            echo "$$" > "${CGROUPS_SYSTEMD_MOUNT}/${CGROUP_UNIT_PATH}/${XCE_USRNODE_SCOPE}/cgroup.procs"

            for controller_mount in "${CGROUPS_V1_CONTROLLER_MOUNTS[@]}"; do
                local controller_path="${controller_mount}/${CGROUP_UNIT_PATH}"

                sudo $XCE_CGCTRL_CMD create "$XCE_USER" "$XCE_GROUP" "$controller_path"
                mkdir -p "${controller_path}/${XCE_USRNODE_SCOPE}"
                echo "$$" > "${controller_path}/${XCE_USRNODE_SCOPE}/cgroup.procs"

                CREATED_CONTROLLER_PATHS+=("$controller_path")

                if [[ "$controller_mount" =~ .*memory.* ]]; then
                    echo "$XCE_USRNODE_SWAPPINESS" > "${controller_path}/${XCE_USRNODE_SCOPE}/memory.swappiness"
                    echo "$XCE_USRNODE_OOM_CONTROL" > "${controller_path}/${XCE_USRNODE_SCOPE}/memory.oom_control"
                fi
            done

            CREATED_CHILDNODE_PATHS=("${CREATED_CONTROLLER_PATHS[@]}")
            CREATED_CHILDNODE_PATHS+=("${CGROUPS_SYSTEMD_MOUNT}/${CGROUP_UNIT_PATH}")
            # take care of any redundant controller paths
            CREATED_CHILDNODE_PATHS=($(echo "${CREATED_CHILDNODE_PATHS[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

            for scope in $CHILDNODE_SCOPES; do
                mkdir -p "${CGROUPS_SYSTEMD_MOUNT}/${CGROUP_UNIT_PATH}/${scope}.scope"

                for controller_mount in "${CGROUPS_V1_CONTROLLER_MOUNTS[@]}"; do
                    mkdir -p "${controller_mount}/${CGROUP_UNIT_PATH}/${scope}.scope"
                done
            done

            XCE_CGROUP_CONTROLLERS=$(joinBy ' ' "${CGROUPS_V1_CONTROLLERS[@]}")
            XCE_CHILDNODE_SCOPES="$CHILDNODE_SCOPES"
            XCE_CGROUP_UNIT_PATH="$CGROUP_UNIT_PATH"
            XCE_CGROUP_CONTROLLER_MAP=$(joinBy ':' "${CGROUPS_V1_CONTROLLER_MAP[@]}")
            XCE_CHILDNODE_PATHS=$(joinBy ':' "${CREATED_CHILDNODE_PATHS[@]}")
            export XCE_CGROUP_CONTROLLERS XCE_CHILDNODE_SCOPES XCE_CGROUP_UNIT_PATH XCE_CGROUP_CONTROLLER_MAP XCE_CHILDNODE_PATHS
        ;;
        "2")
            echo >&2 "Cgroups v2 are not currently supported"
            exit 1
        ;;
        *)
            echo >&2 "Illegal cgroup mode $CGROUPS_MODE"
            exit 1
        ;;
    esac
}

setupMain

if [ "$CGROUPS_ENABLED" != "false" ]; then
    cgroupMode
    cgroupSetup
fi

umask $XCE_UMASK

# this script expects to be called in two ways:
# if INSTANCE_ID is -1, start a single process using the first (and only) detected node number
# if INSTANCE_ID > -1, verify that the number is in nodeId and start that number
if [ "$INSTANCE_ID" == "-1" ]; then
    if [ "$numLocalNodes" != "1" ]; then
        echo >&2 "Unit expects one node id for this host; found $numLocalNodes"
        exit 1
    fi

    NODE_NUMBER="${nodeIdArr[0]}"
elif [ "$INSTANCE_ID" -gt "-1" ]; then
    NODE_NUMBER="-1"
    for ii in $nodeId; do
        if [ "$ii" == "$INSTANCE_ID" ]; then
            NODE_NUMBER="$INSTANCE_ID"
        fi
    done

    if [ "$NODE_NUMBER" == "-1" ]; then
        echo >&2 "INSTANCE_ID $INSTANCE_ID is not configured for this host"
        exit 1
    fi
fi

USRNODE_ARGS="${USRNODE_ARGS:--n $NODE_NUMBER -m $numNodes -c "$XCE_CONFIG" -k "$XCE_LICENSEFILE"}"
XCE_GUARDRAILS_LIB="${XCE_GUARDRAILS_LIB:-/usr/local/lib64/GuardRails/libguardrails.so.0.0}"

if [ "$XCE_USE_GUARDRAILS" = "1" ] && [ -f "$XCE_GUARDRAILS_LIB" ]; then
    USRNODE_ARGS="$USRNODE_ARGS -g $XCE_GUARDRAILS_LIB"
fi

exec xcmonitor $USRNODE_ARGS 1>> "$XCE_LOGDIR/xcmonitor.out" 2>&1 < /dev/null


