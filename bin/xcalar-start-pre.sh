#!/bin/bash
#
# These are commands that are optionally run before the
# Xcalar *service* starts. This means this script will
# run with root services, or not at all.

DIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"

if [ $(id -u) -ne 0 ]; then
    echo >&2 "ERROR: Must be root to run ${BASH_SOURCE[0]}"
    exit 1
fi

if [ -r /etc/default/xcalar ]; then
    . /etc/default/xcalar
fi

XCE_USER=${XCE_USER:-xcalar}
XCE_GROUP=${XCE_GROUP:-xcalar}

say () {
    echo >&2 "$1"
}

xce_cgroups_setup() {
    local user="${1:-$XCE_USER}" group="$2" conf
    if [ -n "$3" ]; then
        conf="$3"
    elif test -d /etc/cgconfig.d; then
        conf=/etc/cgconfig.d/xcalar-${user}.conf
    elif test -e /etc/cgconfig.conf; then
        conf=/etc/cgconfig.conf
    fi

    if [ -z "$group" ]; then
        group="$user"
    fi

    ${DIR}/cgconfig-setup.sh $user $group $conf
    return $?
}

xce_disable_thpg() {
    say "### Disabling transparent hugepage defragmentation"

    local thpg_path="/sys/kernel/mm/transparent_hugepage"
    read thpg_defrag < ${thpg_path}/defrag
    read khpgd_defrag < ${thpg_path}/khugepaged/defrag

    if [ -n "$(grep ^sysfs /proc/mounts | grep ro)" ]; then
        mount -o rw,remount /sys
    fi

    if [[ "$thpg_defrag" =~ never$ ]]; then
        echo "never" > ${thpg_path}/defrag
        local uname=$(uname -r)
        local kmaj=${uname%%.*}

        if [ $kmaj -eq 2 ]; then
            [[ "$khpgd_defrag" =~ no$ ]] && echo "no" > ${thpg_path}/khugepaged/defrag
        elif [ $kmaj -ge 3 ]; then
            [ "$khpgd_defrag" != "0" ] && echo 0 > ${thpg_path}/khugepaged/defrag
        else
            echo >&2 "WARNING: Don't know how to deal with UNAME=$uname"
        fi
    fi
}

xce_ephemeral_disk() {
    if command -v ephemeral-disk >/dev/null; then
        if ephemeral-disk && test -d /ephemeral/data; then
            if ! test -e /ephemeral/data/serdes; then
                mkdir -p /ephemeral/data/serdes
                chmod 0777 /ephemeral/data
                chown $XCE_USER:$XCE_GROUP /ephemeral/data/serdes
            fi
        fi
    fi
}

if [ $# -eq 0 ]; then
    xce_ephemeral_disk
    xce_cgroups_setup $XCE_USER $XCE_GROUP
    xce_disable_thpg
else
    while [ $# -gt 0 ]; do
        cmd="$1"
        shift
        case "$cmd" in
            --ephemeral-disk) xce_ephemeral_disk;;
            --cgcreate) xce_cgroups_setup "$1" "$2"; shift 2;;
            --disable-thpg) xce_disable_thpg;;
            *) say "usage $0: [--ephemeral-disk] [--cgcreate <user> <group>] [--disable-thpg]"; exit 1;;
        esac
    done
fi

exit 0
