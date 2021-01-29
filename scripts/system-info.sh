#!/bin/bash
# Copyright 2016-2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# This script collects system and Xcalar specific information
# to stdout. To save it to a file, redirect the output of the program:
#
#   $ ./system-info.sh > host1.txt
#
# In case these get large, pipe into gzip first:
#
#   $ ./system-info.sh | gzip > host1.txt.gz
#

set +e

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin:$PATH"

osid () {
	(
	unset ELVERSION UBVERSION VERSTRING VERSION_ID ID PRETTY_NAME VERSION
	if [ -f /etc/os-release ]; then
		. /etc/os-release
		case "$ID" in
			rhel|ol|centos)
				ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
				;;
			ubuntu)
				UBVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
				VERSTRING="ub${UBVERSION}"
				;;
			*)
				echo >&2 "Unknown OS version: $PRETTY_NAME ($VERSION)"
				;;
		esac
		if [ -n "$ELVERSION" ]; then
			if [ "$ELVERSION" = "6" ]; then
				VERSTRING="rhel${ELVERSION}"
			else
				VERSTRING="el${ELVERSION}"
			fi
		fi
	fi

	if [ -z "$VERSTRING" ] && [ -e /etc/redhat-release ]; then
		ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/redhat-release | cut -d'.' -f1)"
		if grep -q 'Red Hat' /etc/redhat-release; then
			VERSTRING="rhel${ELVERSION}"
		elif grep -q CentOS /etc/redhat-release; then
			VERSTRING="el${ELVERSION}"
		else
			cat >&2 /etc/*-release
			echo >&2 "Unrecognized EL OS version. Please set VERSTRING to rhel6, el6, rhel7 or el7 manually before running this script"
			exit 1
		fi
	fi

	if [ -z "$VERSTRING" ]; then
		cat >&2 /etc/*-release
		echo >&2 "Unknown OS version"
		exit 1
	fi
	[ $# -eq 0 ] && set -- -f
    for cmd in "$@"; do
        case "$cmd" in
            -e|--env)
                test -n "$ELVERSION" && echo "ELVERSION=${ELVERSION}"
                test -n "$UBVERSION" && echo "UBVERSION=${UBVERSION}"
                test -n "$VERSTRING" && echo "VERSTRING=${VERSTRING}"

                ;;
            -f|--full) echo "$VERSTRING" ;;
            -s|--split) echo "$VERSTRING" | sed -E -e 's/([a-z]+)([0-9]+)/\1 \2/g' ;;
            -p|--package)
                case "$VERSTRING" in
                    ub*) echo "deb";;
                    el*|rhel*) echo "rpm";;
                    el*) echo "rpm";;
                esac
                ;;
            -*) echo >&2 "Unknown option $cmd"; exit 1;;
        esac
    done
	)
}


header () {
    TITLE="$1"
    TITLE_MARKER="$(title "$TITLE")"

    echo >&2 " -> ${TITLE}"

    echo "############################################"
    printf '# %-40s #\n' "${TITLE}"
    echo "############################################"
    echo "#__BEGIN__${TITLE_MARKER}__"
}

footer () {
    echo "#__END__${TITLE_MARKER}__"
    echo
    TITLE='' TITLE_MARKER=''
}

cmd_with_title () {
    header "$1"
    shift
    case "$(type -t $1)" in
        function) "$@" 2>&1;;
        alias) "$@" 2>&1;;
        *) eval timeout --kill-after=5s 31s "$@" 2>&1;;
    esac

    footer
}

cmd_simple () {
    cmd_with_title "$*" "$@"
}

try_sudo () {
    if [ "$(id -u)" = 0 ]; then
        "$@"
    elif $TRY_SUDO; then
        /usr/bin/sudo -n "$@"
    else
        return 1
    fi
}

file_with_title () {
    header "$1"
    shift
    if [ -f "$1" ]; then
        if [ -r "$1" ]; then
            cat "$1"
        else
            try_sudo cat "$1"
        fi
    elif [ -d "$1" ]; then
        if [ -r "$1" ]; then
            ls -altr "$1"
        else
            try_sudo ls -altr "$1"
        fi
    fi
    footer
}

file_simple () {
    if [ -f "$1" ]; then
        file_with_title "$1" "$1"
    elif [ -d "$1" ]; then
        file_with_title "$1" "$1"
    else
        file_with_title "$1" "$1"
    fi
}

have_command () {
    command -v "$1" &>/dev/null
}

title () {
    echo "$1" | tr -d '`$' | tr '[:lower:]' '[:upper:]' | tr ', %-\.!@/|' '_'
}

system_info_extract () {
    local TITLE="$(title "$1")"
    sed -n "/^#__BEGIN__${TITLE}__/,/^#__END__${TITLE}__/p"
}

system_info_usage () {
    cat <<EOF
    usage: $0 [-s <try to use sudo for commands that need permission>] [-o outputfile] [-x 'title to extract']

    $0 gathers system level information and dumps it to stdout. Redirect to a file to
    save the output.

      \$ $0 > output.txt
      \$ $0 | gzip > output.txt.gz
      \$ $0 -o output.txt
      \$ $0 -o output.txt.gz
      \$ $0 -x 'ulimit -Sa' < output.txt
      \$ $0 2>/dev/null | $0 -x 'ulimit -Sa'

EOF
    exit 1
}

system_info_main () {
    if [ -r /etc/default/xcalar ]; then
        . /etc/default/xcalar
    fi

    echo "# DATE: $(date --rfc-3339=seconds)"
    echo "# HOSTNAME: $(hostname)"
    echo
    # HW info
    cmd_with_title 'cpuinfo' "sort -u /proc/cpuinfo | uniq | grep -Ev '^(processor|apicid|initial|physical|power)'; printf 'processors : '; grep -c processor /proc/cpuinfo; echo"
    for hw_info in /proc/meminfo /proc/slabinfo /proc/filesystems /proc/stat /proc/pagetypeinfo; do
        test -e "$hw_info" && file_simple "$hw_info"
    done
    cmd_with_title 'free' 'free -lht'
    have_command slabtop && cmd_with_title 'slabtop' try_sudo slabtop -o

    # OS info
    cmd_with_title 'uname' 'uname -a'
    cmd_with_title 'env' 'env | sort'
    for os_file in /proc/sys/kernel/core_pattern /etc/redhat-release /etc/os-release; do
        test -e "$os_file" && file_simple "$os_file"
    done
    cmd_simple 'ldconfig -p'
    have_command getenforce && cmd_simple 'getenforce'
    have_command rpm && cmd_with_title 'rpm' 'rpm -qa | sort'
    have_command dpkg && cmd_with_title 'deb' 'dpkg -l'
    have_command chkconfig && cmd_simple 'chkconfig'
    have_command systemctl && cmd_simple 'systemctl list-units --no-pager'
    have_command virt-what && cmd_with_title 'virt-what' try_sudo -n virt-what
    have_command iostat && cmd_simple 'iostat'
    have_command vmstat && cmd_simple 'vmstat'
    if have_command mpstat; then
        # If this script is being run interactively (from a terminal), then don't
        # wait for 30s of mpstat
        if [ -t 0 ]; then
            cmd_with_title 'mpstat' 'mpstat -P ALL'
        else
            cmd_with_title 'mpstat' 'mpstat -P ALL 5 6'
        fi
    fi

    # User
    cmd_with_title 'id' "id; id -u; id -g; id -un; id -gn; getent passwd `id -u`"
    cmd_simple 'bash -c "ulimit -Sa"'
    cmd_simple 'bash -c "ulimit -a"'
    cmd_simple 'bash -c "ulimit -Ha"'
    cmd_simple 'sysctl -a'
    cmd_simple 'lsmod'
    file_simple $HOME
    test "$HOME" != "$PWD" && file_simple $PWD

    # Current users/processes
    cmd_simple 'w'
    cmd_simple 'whoami'
    cmd_simple 'who -a --lookup'
    cmd_simple 'ps faux'
    cmd_simple 'top -b -n1'

    # Disk
    file_simple '/etc/fstab'
    file_simple '/proc/mounts'
    cmd_simple 'mount'
    cmd_simple 'lsblk'
    cmd_simple 'df -Th'
    cmd_simple 'swapon -s'

    #
    # Networking
    file_simple '/etc/hosts'
    file_simple '/etc/resolv.conf'
    cmd_simple 'iptables-save'
    have_command netstat && cmd_simple 'netstat -ltupn'
    have_command ss && cmd_simple 'ss -ltupn'
    have_command ss && cmd_simple 'ss -mop'
    have_command ifconfig && cmd_simple 'ifconfig -a'
    have_command ip && cmd_simple 'ip addr show'
    have_command ip && cmd_simple 'ip route'
    cmd_simple 'route'
    have_command ip && cmd_simple 'ip route get 8.8.8.8'
    cmd_with_title 'hostname' 'hostname -f; hostname -s; hostname'
    cmd_with_title 'host' "host $(hostname); host $(hostname -f)"
    if ip_addr=$(set -o pipefail; host $(hostname) | awk '{print $(NF)}') && [ -n "$ip_addr" ]; then
        cmd_simple "host $ip_addr"
    fi

    # Logs
    for system_file in /proc/ /var/log/messages /var/log/syslog /dev/shm /tmp /var/tmp; do
        test -e "$system_file" && file_simple "$system_file"
    done
    have_command journalctl && cmd_simple 'journalctl -b --no-pager'

    # Xcalar
    have_command rpm && cmd_with_title 'Xcalar RPM package info' 'rpm -qlRi xcalar'
    have_command rpm && cmd_with_title 'Xcalar RPM package verification' 'rpm -V xcalar'
    have_command dpkg && cmd_with_title 'Xcalar DEB package info' 'dpkg -s xcalar; dpkg -L xcalar'
    XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"
    if XCE_HOME="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG 2>/dev/null)" && [ "$XCE_HOME" != "" ]; then
        if [ "$XCE_HOME" != "/var/opt/xcalar" ]; then
            file_simple "$XCE_HOME"
        fi
        file_simple "$XCE_HOME/config/ldapConfig.json"
    fi
    for xcalar_file in /var/opt/xcalar $XCE_CONFIG /etc/default/xcalar $(ls /var/log/Xcalar*.log 2>/dev/null); do
        file_simple $xcalar_file
    done
}

TRY_SUDO=false
OUTPUT=
while getopts "hso:x:" opt "$@"; do
    case "$opt" in
        o) OUTPUT="$OPTARG";;
        s) TRY_SUDO=true;;
        h) system_info_usage;;
        x) system_info_extract "$OPTARG" | head -n-1 | tail -n+2; exit $?;;
        \?) echo >&2 "Invalid option -$OPTARG"; exit 1;;
        :) echo >&2 "Option -$OPTARG requires an argument."; exit 1;;
    esac
done

shift $((OPTIND-1))

if [ -z "$OUTPUT" ]; then
    system_info_main "$@"
else
    if [[ "$OUTPUT" =~ \.gz$ ]]; then
        system_info_main "$@" | gzip > "$OUTPUT"
    else
        system_info_main "$@" > "$OUTPUT"
    fi
fi

