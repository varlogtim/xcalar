#!/bin/bash

declare -a FOUND_V1_CONTROLLERS=()
declare -a CGROUPS_V1_CONTROLLER_MOUNTS=()
declare -a CGROUPS_V1_CONTROLLER_MAP=()

cgroupV1ControllerMountCheck() {
    local controller=$1
    local mount=$2
    local rc=1

    case "$controller" in
        cpu)
            if [[ $mount == *$controller* ]]; then
                OLDIFS="$IFS"
                IFS=',' read -ra FIELD <<< "$(basename $mount)"
                IFS="$OLDIFS"
                for ii in "${FIELD[@]}"; do
                    if [ "$ii" == "$controller" ]; then
                        rc=0
                    fi
                done
            fi
            ;;
        *)
            if [[ $mount == *$controller* ]]; then
                rc=0
            fi
            ;;
    esac

    return $rc
}

cgroupFindV1ControllerPath() {
    local controller=$1

    #just to have a default
    CGROUP_CONTAINER_PATH="$(cat /proc/1/cgroup | grep systemd | cut -d ':' -f3)"
    potential_paths=($(grep "$controller" /proc/1/cgroup))

    for path in "${potential_paths[@]}"; do
        local c_list="$(echo "$path" | cut -d: -f2)"
        local c_path="$(echo "$path" | cut -d: -f3)"

        case "$controller" in
            cpu)
                OLDIFS="$IFS"
                IFS=',' read -ra FIELD <<< "$c_list"
                IFS="$OLDIFS"
                for ii in "${FIELD[@]}"; do
                    if [ "$ii" = "$controller" ]; then
                        CGROUP_CONTAINER_PATH="$c_path"
                    fi
                done
                ;;
            *)
                CGROUP_CONTAINER_PATH="$c_path"
                ;;
        esac
    done
}

cgroupFindV1ControllerMounts() {
    for v1_mount in "$@"; do
        for v1_controller in ${CGROUPS_V1_CONTROLLERS[@]}; do
            cgroupFindV1ControllerPath "$v1_controller"
            v1_cgroup_path="$CGROUP_CONTAINER_PATH"

            if cgroupV1ControllerMountCheck "$v1_controller" "$v1_mount"; then
                FOUND_V1_CONTROLLERS+=("$v1_controller")
                if [ "$v1_cgroup_path" != "/" ]; then
                    CGROUPS_V1_CONTROLLER_MOUNTS+=("${v1_mount}${v1_cgroup_path}")
                    CGROUPS_V1_CONTROLLER_MAP+=("${v1_controller}%${v1_mount}${v1_cgroup_path}")
                else
                    CGROUPS_V1_CONTROLLER_MOUNTS+=("$v1_mount")
                    CGROUPS_V1_CONTROLLER_MAP+=("${v1_controller}%${v1_mount}")
                fi
            fi
        done
    done
}

cgroupMode()  {
    # philosphy here is to check what cgroup and cgroup2 filesystems are mounted
    # if (cgroup > 0) && (cgroup2 == 0), cgroup1 mode
    # if (cgroup > 0) && (cgroup2 > 0), hybrid mode (but we may be able to treat it like cgroup1)
    # if (cgroup == 0) && (cgroup2 > 0), cgroup2 mode

    v1_mounts=( $(mount -t cgroup | awk '{ print $3 }') )
    v2_mounts=( $(mount -t cgroup2 | awk '{ print $3 }') )
    v1_count="${#v1_mounts[@]}"
    v2_count="${#v2_mounts[@]}"

    if [ "$v1_count" == "0" ] && [ "$v2_count" == "0" ]; then
        echo >&2 "No cgroup file systems are mounted"
        exit 1;
    fi

    if [ "$v1_count" -gt 0 ] && [ "$v2_count" == 0 ]; then
        cgroupFindV1ControllerMounts ${v1_mounts[@]}

        if [ "${#FOUND_V1_CONTROLLERS[@]}" != "${#CGROUPS_V1_CONTROLLERS[@]}" ]; then
            echo >&2 "Cgroup controller error: ${CGROUPS_V1_CONTROLLERS[@]} V1 wanted, ${FOUND_V1_CONTROLLERS[@]} V1 found, V2 not mounted"
            exit 1
        fi
        CGROUPS_MODE="1"

    elif [ "$v1_count" == 0 ] && [ "$v2_count" -gt 0 ]; then
        CGROUPS_MODE="2"

    elif [ "$v1_count" -gt 0 ] && [ "$v2_count" -gt 0 ]; then
        cgroupFindV1ControllerMounts ${v1_mounts[@]}

        # if all v1 controllers wanted are found, it's version 1
        # if none, then it's version 2
        if [ "${#FOUND_V1_CONTROLLERS[@]}" == "${#CGROUPS_V1_CONTROLLERS[@]}" ]; then
            CGROUPS_MODE="1"
        elif [ "${#FOUND_V1_CONTROLLERS[@]}" == "0" ] && [ "$v2_count" -gt 0 ]; then
            CGROUPS_MODE="2"
        else
            echo >&2 "Cgroup controller error: ${CGROUPS_V1_CONTROLLERS[@]} V1 wanted, ${FOUND_V1_CONTROLLERS[@]} V1 found, $v2_count V2 mounted"
            exit 1
        fi
    fi
}

cgroupFindCgroupUnitPath () {
    local my_unit_name="$1"

    cgroupFindV1ControllerPath "${$}" 
    CURRENT_UNIT_PATH="$(cat /proc/${$}/cgroup | grep name=systemd | cut -d ':' -f3)"
    SYSTEMD_V1_UNIT_PATH="$(cat /proc/1/cgroup | grep name=systemd | cut -d ':' -f3)"
    SYSTEMD_V2_UNIT_PATH="$(cat /proc/1/cgroup | grep 0:: | cut -d ':' -f3)"
    CGROUPS_SYSTEMD_MOUNT="$(mount -t cgroup | awk '{ print $3 }' | grep systemd)"

    local unit_path="$CURRENT_UNIT_PATH"

    # on Ubuntu host systems init/systemd process V1_UNIT_PATH and V2_UNIT_PATH
    # are the same and not "/"
    if [ "$SYSTEMD_V1_UNIT_PATH" != "$SYSTEMD_V2_UNIT_PATH" ] &&
           [ "$SYSTEMD_V1_UNIT_PATH" != "/" ]; then
        unit_path="${CURRENT_UNIT_PATH:${#SYSTEMD_V1_UNIT_PATH}}"
        CGROUPS_SYSTEMD_MOUNT="${CGROUPS_SYSTEMD_MOUNT}${SYSTEMD_V1_UNIT_PATH}"
    fi

    while true; do
        local unit_name=$(basename "$unit_path")

        if [ "$unit_name" = "$my_unit_name" ]; then
            break;
        fi

        local new_unit_path=$(dirname "$unit_path")

        if [ "$new_unit_path" = '/' ] || [ "$new_unit_path" = '.' ]; then
            echo >&2 "Unit name $my_unit_name not found in $CURRENT_UNIT_PATH"
            exit 1
        fi

        unit_path="$new_unit_path"
    done

    CGROUP_UNIT_PATH="${unit_path#/}"
}

cgroupMwMemoryCalc () {
    local memFile="${MEMINFO:-/proc/meminfo}"
    local memtotal="$(grep MemTotal "$memFile" | awk '{ print $2 }')"
    local memtotal=$(( memtotal * 1024 )) # /proc/meminfo lists it in KB

    local mintotalmem=$1
    local softmempct=$2
    local mempct=$3
    local numsvcs=$4

    amountbypct=$(( (mempct * memtotal) / 100 ))
    if (( mintotalmem > amountbypct )); then
        memamt=$mintotalmem
    else
        memamt=$amountbypct
    fi

    softmemamt=$(( (memamt * softmempct) / 100 ))
}

cgroupMwSetup() {
    case "$CGROUPS_MODE" in
        "1")
            cgroupFindCgroupUnitPath "$SYSD_UNIT_NAME"

            cgroupMwMemoryCalc "$MIN_TOTAL_MEM" "$SOFT_MEM_PCT" "$MEM_PCT" "$SVC_CNT"

            for controller_mount in "${CGROUPS_V1_CONTROLLER_MOUNTS[@]}"; do
                if [[ "$controller_mount" =~ .*memory.* ]]; then
                    local controller_path="${controller_mount}/${CGROUP_UNIT_PATH}"

                    sudo $XCE_CGCTRL_CMD modify "$XCE_USER" "$XCE_GROUP" "$controller_path"

                    echo "$memamt" > "${controller_path}/memory.limit_in_bytes"
                    echo "$softmemamt" > "${controller_path}/memory.soft_limit_in_bytes"
                    echo "$SWAPPINESS" > "${controller_path}/memory.swappiness"
                    echo "$OOM_CONTROL" > "${controller_path}/memory.oom_control"
                fi
            done
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
