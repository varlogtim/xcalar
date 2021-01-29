#!/bin/bash
set -e

# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

#####################################
# To modify inputs here, uncomment the line and set
# the approprate value: (e.g. XCE_USER=someuser)
#
# Partial list of inputs are:
# XCE_USER - name of the Xcalar process owner, if not
#            provided, xcalar will be used
# XCE_GROUP - name of the primary group for the Xcalar process
#             owner, defaults to XCE_USER value (optional)
# XCE_USER_HOME - home directory of the Xcalar process owner, if account
#            created during install  (optional)
# XCE_ROOTDIR - directory where xcalar software will be installed
# XCE_KERBEROS_ENABLE - set to 0 to disable install Kerberos and
#                       SASL libraries (optional)
#
#XCE_USER=
#XCE_GROUP=
#XCE_USER_HOME=
#XCE_ROOTDIR=
#XCE_INSTALLDIR=
#XCE_KERBEROS_ENABLE=
#
# Do not edit below this line.
#####################################

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

if [ -f "$DIR/setup.txt" ]; then
    . "$DIR/setup.txt"
fi

XCE_USER="${XCE_USER:-xcalar}"
XCE_GROUP="${XCE_GROUP:-$XCE_USER}"
XCE_USER_HOME="${XCE_USER_HOME:-/home/xcalar}"
XCE_ROOTDIR="${XCE_ROOTDIR:-/mnt/xcalar}"
XCE_INSTALLDIR="${XCE_INSTALLDIR:-/opt/xcalar}"
XCE_KERBEROS_ENABLE="${XCE_KERBEROS_ENABLE:-1}"
XCE_SQLDF_ENABLE="${XCE_SQLDF_ENABLE:-1}"
XCE_JAVA_HOME="${XCE_JAVA_HOME:-}"

XCE_SYSTEM_DEPENDENCIES=%SYSTEM_PACKAGES%
XCE_XCALAR_DEPENDENCIES=%XCALAR_PACKAGES%
XCE_KERBEROS_DEPENDENCIES=%KRB_PACKAGES%
XCE_JAVA_DEPENDENCIES=%JAVA_PACKAGES%

XCE_LIMIT_COUNT=$(( %XCE_LIMIT_COUNT% - 1 ))
XCE_LIMIT_FLAGS=(%XCE_LIMIT_FLAGS%)
XCE_LIMIT_VALUES=(%XCE_LIMIT_VALUES%)

XCE_SYSCTL_COUNT=$(( %XCE_SYSCTL_COUNT% - 1 ))
XCE_SYSCTL_TOKENS=(%XCE_SYSCTL_TOKENS%)
XCE_SYSCTL_VALUES=(%XCE_SYSCTL_VALUES%)

XCE_VERIFY_RESULT="PASS"

die () {
    echo >&2 "ERROR: $*"
    exit 1
}

command_exists () {
    command -v "$@" >/dev/null 2>&1
}

have_package () {
    case "$VERSTRING" in
        rhel*|el*)
            rpm -q "$1" &>/dev/null
            ;;
        ub*)
            dpkg -L "$1" &>/dev/null
            ;;
    esac
}

service_cmd () {
    local svc="$1" cmd="$2"
    case "$VERSTRING" in
        rhel6|el6|ub14)
            service $svc $cmd
            ;;
        rhel7|el7|ub16)
            systemctl daemon-reload
            systemctl $cmd $svc
            ;;
        *)
            echo >&2 "$svc $cmd: Unknown service command for $VERSTRING"
            return 1
            ;;
    esac
}

autostart () {
    local svc="$1" on="$2"
    case "$VERSTRING" in
        rhel6|el6)
            offon=(off on)
            chkconfig $svc ${offon[$on]}
            ;;
        rhel7|el7|ub16)
            offon=(disable enable)
            systemctl ${offon[$on]} $svc
            ;;
        ub14)
            offon=(remove defaults)
            update-rc.d $svc ${offon[$on]}
            ;;
        *)
            echo >&2 "$svc $on: Unknown autostart command for $VERSTRING"
            return 1
            ;;
    esac
}

write_bit_set () {
    case "$1" in
        2|3|6|7)
            return 0
            ;;
    esac

    return 1
}

TOTAL_DEPENDENCIES="$XCE_SYTSTEM_DEPENDENCIES"

test "$XCE_KERBEROS_ENABLE" = 1 && \
    TOTAL_DEPENDENCIES="$TOTAL_DEPENDENCIES $XCE_KERBEROS_DEPENDENCIES"

test -n "$XCE_JAVA_HOME" -a "$XCE_SQLDF_ENABLE" = "1" && \
    TOTAL_DEPENDENCIES="$TOTAL_DEPENDENCIES $XCE_JAVA_DEPENDENCIES"

if [ -f /etc/os-release ]; then
    . /etc/os-release
    case "$ID" in
        rhel|ol)
            ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
            VERSTRING="rhel${ELVERSION}"
            ;;
        centos)
            ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
            VERSTRING="el${ELVERSION}"
            ;;
        ubuntu)
            UBVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
            VERSTRING="ub${UBVERSION}"
            ;;
        *)
            echo >&2 "Unknown OS version: $PRETTY_NAME ($VERSION)"
            ;;
    esac
fi

if [ -z "$VERSTRING" ] && [ -e /etc/redhat-release ]; then
    ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/redhat-release | cut -d'.' -f1)"
    if grep -q 'Red Hat' /etc/redhat-release; then
        VERSTRING="rhel${ELVERSION}"
    elif grep -q CentOS /etc/redhat-release; then
        VERSTRING="el${ELVERSION}"
    else
        cat >&2 /etc/*-release
        die "Unrecognized EL OS version. Please set VERSTRING to rhel6, el6, rhel7 or el7 manually before running this script"
    fi
fi

if pgrep -f usrnode > /dev/null 2>&1; then
    echo "######"
    echo "# WARNING!!!"
    echo "# One or more Xcalar usrnodes appear to be"
    echo "# running on this system"
    echo "# These tests should be run with Xcalar shut down"
    echo "# for the greatest accuracy"
    echo "######"
fi

echo -n "Platform Support Test: "

case "$VERSTRING" in
    rhel*|el*|ub14)
        echo "PASS"
        ;;
    *)
        echo "FAIL"
        XCE_VERIFY_RESULT="FAIL"
        ;;
esac

echo -n "xcalar package is installed: "
if have_package xcalar; then
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
else
    echo "PASS"
fi

echo -n "User $XCE_USER exists: "
XCE_USER_EXISTS=1
if getent passwd "$XCE_USER" >/dev/null ; then
    echo "PASS"
else
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
    XCE_USER_EXISTS=0
fi

echo -n "Group $XCE_GROUP exists: "
if getent group "$XCE_GROUP" >/dev/null ; then
    echo "PASS"
else
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
fi

if [ "$XCE_USER_EXISTS" = "1" ]; then
    XCE_HOMEDIR_RESULT="PASS"
    echo "User $XCE_USER home directory tests"

    USER_HOMEDIR="$(getent passwd "$XCE_USER" | cut -d ':' -f 6)"
    echo -n " * User $XCE_USER home directory $USER_HOMEDIR exists: "

    if [ -d "$USER_HOMEDIR" ]; then
        echo "PASS"
        USER_HOMEDIR_STAT="$(stat -c '%a' "$USER_HOMEDIR")"
        W_PERM=$(( $USER_HOMEDIR_STAT % 10 ))
        G_PERM=$(( (($USER_HOMEDIR_STAT % 100) - $W_PERM) / 10 ))

        echo -n " * Group write bit not set on $USER_HOMEDIR: "
        if write_bit_set "$G_PERM"; then
            echo "FAIL"
            XCE_VERIFY_RESULT="FAIL"
            XCE_HOMEDIR_RESULT="FAIL"
        else
            echo "PASS"
        fi

        echo -n " * World write bit not set on $USER_HOMEDIR: "
        if write_bit_set "$W_PERM"; then
            echo "FAIL"
            XCE_VERIFY_RESULT="FAIL"
            XCE_HOMEDIR_RESULT="FAIL"
        else
            echo "PASS"
        fi

        echo -n " * User $XCE_USER directory ${USER_HOMEDIR}/.ssh exists: "
        if [ -d "$USER_HOMEDIR/.ssh" ]; then
            echo "PASS"
            USER_DOTSSH_STAT="$(stat -c '%a' "${USER_HOMEDIR}/.ssh")"
            W_PERM=$(( $USER_DOTSSH_STAT % 10 ))
            G_PERM=$(( (($USER_DOTSSH_STAT % 100) - $W_PERM) / 10 ))

            echo -n " * Group write bit not set on ${USER_HOMEDIR}/.ssh: "
            if write_bit_set "$G_PERM"; then
                echo "FAIL"
                XCE_VERIFY_RESULT="FAIL"
                XCE_HOMEDIR_RESULT="FAIL"
            else
                echo "PASS"
            fi

            echo -n " * World write bit not set on ${USER_HOMEDIR}/.ssh: "
            if write_bit_set "$W_PERM"; then
                echo "FAIL"
                XCE_VERIFY_RESULT="FAIL"
                XCE_HOMEDIR_RESULT="FAIL"
            else
                echo "PASS"
            fi

            echo -n " * User $XCE_USER file ${USER_HOMEDIR}/.ssh/authorized_keys exists: "
            if [ -f "$USER_HOMEDIR/.ssh/authorized_keys" ]; then
                echo "PASS"
                USER_AUTHKEY_STAT="$(stat -c '%a' "${USER_HOMEDIR}/.ssh/authorized_keys")"
                W_PERM=$(( $USER_AUTHKEY_STAT % 10 ))
                G_PERM=$(( (($USER_AUTHKEY_STAT % 100) - $W_PERM) / 10 ))

                echo -n " * Group write bit not set on ${USER_HOMEDIR}/.ssh/authorized_keys: "
                if write_bit_set "$G_PERM"; then
                    echo "FAIL"
                    XCE_VERIFY_RESULT="FAIL"
                    XCE_HOMEDIR_RESULT="FAIL"
                else
                    echo "PASS"
                fi

                echo -n " * World write bit not set on ${USER_HOMEDIR}/.ssh/authorized_keys: "
                if write_bit_set "$W_PERM"; then
                    echo "FAIL"
                    XCE_VERIFY_RESULT="FAIL"
                    XCE_HOMEDIR_RESULT="FAIL"
                else
                    echo "PASS"
                fi

            else
                echo "NOT PRESENT"
            fi #${USER_HOMEDIR}/.ssh/authorized_keys

        else
            echo "NOT PRESENT"
        fi #${USER_HOMEDIR}/.ssh
    else
        echo "FAIL"
        XCE_VERIFY_RESULT="FAIL"
        XCE_HOMEDIR_RESULT="FAIL"
    fi #${USER_HOMEDIR}
    echo "User $XCE_USER home directory test results: $XCE_HOMEDIR_RESULT"
fi

XCE_INSTALL_DIR_RESULT="PASS"
echo "Install directory tests"
echo -n " * Xcalar install directory exists: "

if [ -d "$XCE_INSTALLDIR" ]; then
    echo "PASS"
else
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
    XCE_INSTALL_DIR_RESULT="FAIL"
fi

if [ "$XCE_INSTALL_DIR_RESULT" = "PASS" ]; then
    echo -n " * Install directory owner is $XCE_USER: "
    dir_user="$(stat -c '%U' $XCE_INSTALLDIR)"
    if [ "$dir_user" != "$XCE_USER" ]; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_INSTALL_DIR_RESULT="FAIL"
        echo "FAIL"
    else
        echo "PASS"
    fi

    echo -n " * Install directory group is $XCE_GROUP: "
    dir_group="$(stat -c '%G' $XCE_INSTALLDIR)"
    if [ "$dir_group" != "$XCE_GROUP" ]; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_INSTALL_DIR_RESULT="FAIL"
        echo "FAIL"
    else
        echo "PASS"
    fi
fi

echo "Xcalar install directory test results: $XCE_INSTALL_DIR_RESULT"

XCE_ROOT_DIR_RESULT="PASS"
echo "Shared data directory tests"
echo -n " * Xcalar shared data directory exists: "

if [ -d "$XCE_ROOTDIR" ]; then
    echo "PASS"
else
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
    XCE_ROOT_DIR_RESULT="FAIL"
fi

if [ "$XCE_ROOT_DIR_RESULT" = "PASS" ]; then
    echo -n " * Data directory owner is $XCE_USER: "
    dir_user="$(stat -c '%U' $XCE_ROOTDIR)"
    if [ "$dir_user" != "$XCE_USER" ]; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_ROOT_DIR_RESULT="FAIL"
        echo "FAIL"
    else
        echo "PASS"
    fi

    echo -n " * Data directory group is $XCE_GROUP: "
    dir_group="$(stat -c '%G' $XCE_ROOTDIR)"
    if [ "$dir_group" != "$XCE_GROUP" ]; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_ROOT_DIR_RESULT="FAIL"
        echo "FAIL"
    else
        echo "PASS"
    fi

    echo -n " * Data directory size ( >= 1TB ): "
    df_lines="$(df -k "$XCE_ROOTDIR" | wc -l)"
    case "$df_lines" in
        2)
            data_dir_size=$(df -k "$XCE_ROOTDIR" | tail -1 | awk '{ print $2 }')
            ;;
        3)
            data_dir_size=$(df -k "$XCE_ROOTDIR" | tail -1 | awk '{ print $1 }')
            ;;
        *)
            echo "Unknown df output during XCE_ROOTDIR check"
            exit 1
    esac
    if [ "$data_dir_size" -ge "1073741824" ]; then
        echo "PASS"
    elif [ "$data_dir_size" -ge "209715200" ]; then
        echo "PASS (with reservations, more space would be better)"
    else
        echo "!WARNING! Xcalar may not function or perform correctly!"
        XCE_VERIFY_RESULT="FAIL"
        XCE_ROOT_DIR_RESULT="FAIL"
    fi
fi

echo "Xcalar shared directory test results: $XCE_ROOT_DIR_RESULT"

if [ -e "/tmp/xcalar" ] || [ -e "/tmp/${XCE_USER}" ] || \
    [ -e "/tmp/xcalar-sock" ]; then
    echo "/tmp directory tests"
    XCE_TMP_DIR_RESULT="PASS"

    if [ -e "/tmp/xcalar" ]; then
        echo -n " * /tmp/xcalar owner is $XCE_USER: "
        dir_user="$(stat -c '%U' /tmp/xcalar)"
        if [ "$dir_user" != "$XCE_USER" ]; then
            XCE_VERIFY_RESULT="FAIL"
            XCE_TMP_DIR_RESULT="FAIL"
            echo "FAIL"
        else
            echo "PASS"
        fi
    fi

    if [ -e "/tmp/${XCE_USER}" ]; then
        echo -n " * /tmp/${XCE_USER} owner is $XCE_USER: "
        dir_user="$(stat -c '%U' "/tmp/${XCE_USER}")"
        if [ "$dir_user" != "$XCE_USER" ]; then
            XCE_VERIFY_RESULT="FAIL"
            XCE_TMP_DIR_RESULT="FAIL"
            echo "FAIL"
        else
            echo "PASS"
        fi
    fi

    if [ -e "/tmp/xcalar_sock" ]; then
        echo -n " * /tmp/xcalar_sock owner is $XCE_USER: "
        dir_user="$(stat -c '%U' "/tmp/xcalar_sock")"
        if [ "$dir_user" != "$XCE_USER" ]; then
            XCE_VERIFY_RESULT="FAIL"
            XCE_TMP_DIR_RESULT="FAIL"
            echo "FAIL"
        else
            echo "PASS"
        fi
    fi
fi

echo -n "System RPM dependencies: "

XCE_SYS_PKG_RESULT="PASS"
XCE_SYS_MISSING_PKG=""
for pkg in $TOTAL_DEPENDENCIES; do
    if ! have_package $pkg; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_SYS_PKG_RESULT="FAIL"
        XCE_SYS_MISSING_PKG="$pkg $XCE_SYS_MISSING_PKG"
    fi
done

echo "$XCE_SYS_PKG_RESULT"
[ "$XCE_SYS_PKG_RESULT" = "FAIL" ] && echo "Missing packages: $XCE_SYS_MISSING_PKG"

echo -n "Xcalar RPM dependencies: "

XCE_XLR_PKG_RESULT="PASS"
XCE_XLR_MISSING_PKG=""
for pkg in $XCE_XCALAR_DEPENDENCIES; do
    if ! have_package $pkg; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_XLR_PKG_RESULT="FAIL"
        XCE_XLR_MISSING_PKG="$pkg $XCE_XLR_MISSING_PKG"
    fi
done

echo "$XCE_XLR_PKG_RESULT"
[ "$XCE_XLR_PKG_RESULT" = "FAIL" ] && echo "Missing packages: $XCE_XLR_MISSING_PKG"

echo "Xcalar limits tests: "

XCE_LIMIT_RESULT="PASS"
for idx1 in $(seq 0 $XCE_LIMIT_COUNT); do
    value=$(ulimit ${XCE_LIMIT_FLAGS[$idx1]})
    result="PASS"

    if [ "${XCE_LIMIT_VALUES[$idx1]}" = "unlimited" ]; then
        [ "$value" != "unlimited" ] && result="FAIL"
    elif [ "$value" != "unlimited" ]; then
        ## XCE_LIMIT_VALUE is not unlimited
        [ "$value" -lt "${XCE_LIMIT_VALUES[$idx1]}" ] && result="FAIL"
    fi
    # if we're here, XCE_LIMIT_VALUE is not unlimited, but value is,
    # therefore: not FAIL

    echo " * Test of ${XCE_LIMIT_FLAGS[$idx1]}: $result"
    if [ "$result" = "FAIL" ]; then
        XCE_VERIFY_RESULT="FAIL"
        XCE_LIMIT_RESULT="FAIL"
    fi
done

echo "Xcalar limit test results: $XCE_LIMIT_RESULT"

echo "Xcalar sysctl tests: "

XCE_SYSCTL_RESULT="PASS"
for idx1 in $(seq 0 $XCE_SYSCTL_COUNT); do
    value=$(sysctl ${XCE_SYSCTL_TOKENS[$idx1]} | cut -d ' ' -f 3)
    result="PASS"

    if [ "$value" != "${XCE_SYSCTL_VALUES[$idx1]}" ]; then
        result="FAIL"
        XCE_VERIFY_RESULT="FAIL"
        XCE_SYSCTL_RESULT="FAIL"
    fi
    echo " * Test of ${XCE_SYSCTL_TOKENS[$idx1]}: $result"
done

echo "Xcalar sysctl test results: $XCE_SYSCTL_RESULT"

if command_exists getenforce; then
    echo -n "SELinux mode is PERMISSIVE: "
    if [ "$(getenforce)" = "Enforcing" ]; then
        XCE_VERIFY_RESULT="FAIL"
        echo "FAIL"
    else
        echo "PASS"
    fi
fi

thpg_path="/sys/kernel/mm/transparent_hugepage"
read thpg_defrag < ${thpg_path}/defrag
read khpgd_defrag < ${thpg_path}/khugepaged/defrag

echo -n "Testing transparent hugepage defrag: "

if [[ "$thpg_defrag" =~ \[never\]$ ]]; then
    echo "PASS"
else
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
fi

echo -n "Testing transparent hugepage khugepaged defrag: "

if [[ "$khpgd_defrag" =~ \[no\]$ ]] || [ "$khpgd_defrag" = "0" ]; then
    echo "PASS"
else
    echo "FAIL"
    XCE_VERIFY_RESULT="FAIL"
fi

XCE_MEM_STATS="$(grep -E "MemTotal:|MemFree:|Buffers:|^Cached:|Shmem:" /proc/meminfo)"

XCE_MEM_TOTAL="$(echo "$XCE_MEM_STATS" | grep "MemTotal:" | awk '{print $2}')"
XCE_MEM_FREE="$(echo "$XCE_MEM_STATS" | grep "MemFree:" | awk '{print $2}')"
XCE_MEM_BUF="$(echo "$XCE_MEM_STATS" | grep "Buffers:" | awk '{print $2}')"
XCE_MEM_CACHE="$(echo "$XCE_MEM_STATS" | grep "Cached:" | awk '{print $2}')"
XCE_MEM_SHARED="$(echo "$XCE_MEM_STATS" | grep "Shmem:" | awk '{print $2}')"

XCE_FREE_TOTAL=$(( $XCE_MEM_FREE + $XCE_MEM_BUF + $XCE_MEM_CACHE - $XCE_MEM_SHARED ))
XCE_BUF_CACHE=$(( $XCE_MEM_TOTAL * 7 / 10 ))

echo -n "Testing total amount of free RAM: "

if [ "$XCE_BUF_CACHE" -ge "$XCE_FREE_TOTAL" ]; then
    echo "FAIL -- $XCE_BUF_CACHE KB needed, $XCE_FREE_TOTAL KB available"
    XCE_VERIFY_RESULT="FAIL"
else
    echo "PASS"
fi

echo "Testing space on /dev/shm:"
XCE_SHM_RESULT="PASS"
if ! [ -e "/dev/shm" ]; then
    echo "FAIL -- /dev/shm does not exist"
    XCE_SHM_RESULT="FAIL"
    XCE_VERIFY_RESULT="FAIL"
else
    XCE_DF_SHM=$(df -k /dev/shm | grep shm)
    XCE_DF_TOTAL=$(echo "$XCE_DF_SHM" | awk '{print $2}')
    XCE_DF_FREE=$(echo "$XCE_DF_SHM" | awk '{print $4}')

    echo -n " * Total amount of /dev/shm space: "
    if [ "$XCE_DF_TOTAL" -lt "$XCE_BUF_CACHE" ]; then
        echo "FAIL - $XCE_BUF_CACHE KB needed, $XCE_DF_TOTAL KB available"
        XCE_SHM_RESULT="FAIL"
        XCE_VERIFY_RESULT="FAIL"
    else
        echo "PASS"
    fi

    echo -n " * Amount of free /dev/shm space: "
    if [ "$XCE_DF_FREE" -lt "$XCE_BUF_CACHE" ]; then
        echo "FAIL - $XCE_BUF_CACHE KB needed, $XCE_DF_FREE KB free"
        XCE_SHM_RESULT="FAIL"
        XCE_VERIFY_RESULT="FAIL"
    else
        echo "PASS"
    fi
fi
echo "Xce /dev/shm test results: $XCE_SHM_RESULT"

echo -n "Testing swap size (>= amount of RAM):"

XCE_SWAP_SPACE=$(free | grep Swap: | awk '{ print $2 }')

if [ $XCE_SWAP_SPACE -ge $XCE_MEM_TOTAL ]; then
    echo "PASS"
else
    echo "!WARNING! Xcalar may not function or perform correctly!"
    XCE_VERIFY_RESULT="FAIL"
fi

echo "*"
echo "* Xce final test results: $XCE_VERIFY_RESULT"
echo "*"

[ "$XCE_VERIFY_RESULT" = "FAIL" ] && exit 1

exit 0
