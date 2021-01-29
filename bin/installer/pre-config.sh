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
# XCE_INSTALLDIR - directory where xcalar software will be installed
# XCE_FIREWALL_CONFIG - set to 1 to perform simple modification of iptables/
#                       firewalld to open ports required by Xcalar (optional)
# XCE_KERBEROS_ENABLE - set to 0 to disable install Kerberos and
#                       SASL libraries (optional)
# XLRROOT - absolute path where Xcalar cluster shared storage is located
#
#XCE_USER=
#XCE_GROUP=
#XCE_USER_HOME=
#XCE_INSTALLDIR=
#XCE_FIREWALL_CONFIG=
#XCE_KERBEROS_ENABLE=
#XLRROOT=
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
XCE_INSTALLDIR="${XCE_INSTALLDIR:-/opt/xcalar}"
XCE_FIREWALL_CONFIG="${XCE_FIREWALL_CONFIG:-0}"
XCE_KERBEROS_ENABLE="${XCE_KERBEROS_ENABLE:-1}"
XCE_SQLDF_ENABLE="${XCE_SQLDF_ENABLE:-1}"
XCE_JAVA_HOME="${XCE_JAVA_HOME:-}"
XLRROOT="${XLRROOT:-/var/opt/xcalar}"

XCE_PACKAGE_DEPENDENCIES=%PACKAGES%
XCE_KERBEROS_DEPENDENCIES=%KRB_PACKAGES%
XCE_JAVA_DEPENDENCIES=%JAVA_PACKAGES%

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

version_compare () {
    local pkg="$1"
    local installed="$2"
    local sorted_versions=($(printf '%s\n' "$pkg" "$installed" | sort -V))
    # if pkg = installed, no error
    [ "$pkg" = "$installed" ] && return 1
    # if pkg is lower than installed, no error
    [ "${sorted_versions[0]}" = "$1" ] && return 1
    # if pkg is higher than installed, error
    return 0
}

have_package_file_version () {
    local pkg_file="$1"
    local pkg_name="" pkg_ver="" installed_ver=""

    if ! [ -f "$pkg_file" ]; then
        return 1
    fi

    case "$VERSTRING" in
        rhel6|el6|rhel7|el7)
            pkg_name="$(rpm -qp --qf '%{NAME}' "$pkg_file")"
            pkg_ver="$(rpm -qp --qf '%{VERSION}' "$pkg_file")"
            ;;
        ub14|ub16)
            pkg_name="$(dpkg-deb -I "$pkg_file" | grep "Package:" | cut -d ' ' -f3)"
            pkg_ver="$(dpkg-deb -I "$pkg_file" | grep "Version:" | cut -d ' ' -f3)"
            ;;
    esac

    if ! have_package "$pkg_name"; then
        return 1
    fi

    case "$VERSTRING" in
        rhel6|el6|rhel7|el7)
            installed_ver="$(rpm -q --qf '%{VERSION}' "$pkg_name")"
            if version_compare "$pkg_ver" "$installed_ver"; then
                return 1
            fi
            ;;
        ub14|ub16)
            installed_ver="$(dpkg-query -W "$pkg_name" | awk '{ print $2 }')"
            installed_state="$(dpkg-query -s "$pkg_name" | grep 'Status' | cut -d ' ' -f4)"
            if [ "$installed_state" != "installed" ] || version_compare "$pkg_ver" "$installed_ver"; then
                return 1
            fi
            ;;
    esac

    return 0
}

test_missing_deps () {
    MISSING_DEP_PACKAGES=""
    for dep in $TOTAL_DEPENDENCIES; do
        if ! have_package "$dep"; then
            MISSING_DEP_PACKAGES="$dep $MISSING_DEP_PACKAGES"
        fi
    done

    if [ -n "$MISSING_DEP_PACKAGES" ]; then
        echo >&2 "One or more dependencies are not installed: $MISSING_DEP_PACKAGES"
        echo >&2 "Node is not configured."
        exit 1
    fi
}

test_missing_xlr_deps () {
    MISSING_XLR_PACKAGES=""
    XLR_DEP_FILES=""
    case "$VERSTRING" in
        rhel*|el*)
            XLR_DEP_FILES="$DIR/*.rpm"
            ;;
        ub14|ub16)
            XLR_DEP_FILES="$DIR/*.deb"
            ;;
    esac

    for dep in $XLR_DEP_FILES; do
        dep="$(echo "$dep" | tr -d '\n' | tr -d '\r')"
        if ! have_package_file_version "$dep"; then
            MISSING_XLR_PACKAGES="$dep $MISSING_XLR_PACKAGES"
        fi
    done

    if [ -n "$MISSING_XLR_PACKAGES" ]; then
        echo >&2 "One or more Xcalar dependencies are not installed: $MISSING_XLR_PACKAGES"
        echo >&2 "Node is not configured."
        exit 1
    fi
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

TOTAL_DEPENDENCIES="$XCE_PACKAGE_DEPENDENCIES"

test "$XCE_KERBEROS_ENABLE" = "1" && \
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

if [ -z "$VERSTRING" ]; then
    cat >&2 /etc/*-release
    die "Unknown OS version"
fi

if ! getent group "$XCE_GROUP" >/dev/null ; then
    groupadd -f "$XCE_GROUP" || exit 1
fi
if ! getent passwd "$XCE_USER" >/dev/null ; then
    useradd -g "$XCE_GROUP" -d "$XCE_USER_HOME" -m -s /bin/bash -c "Xcalar software owner" "$XCE_USER" || exit 1
fi

if ! [ -d "$XCE_INSTALLDIR" ]; then
    mkdir -p "$XCE_INSTALLDIR"
    chown "$XCE_USER":"$XCE_GROUP" "$XCE_INSTALLDIR"
fi

case "$VERSTRING" in
    rhel*|el*)
        yum -y install $TOTAL_DEPENDENCIES || true
        if ls -l $DIR/*.rpm >/dev/null 2>&1; then
            yum -y install $DIR/*.rpm || true
            test_missing_xlr_deps
        fi
        test_missing_deps
        ;;
    ub14|ub16)
        DEBIAN_FRONTEND=noninteractive apt-get update -yq && \
            DEBIAN_FRONTEND=noninteractive apt-get upgrade -yq && \
            DEBIAN_FRONTEND=noninteractive apt-get install -yq $TOTAL_DEPENDENCIES || true
        if ls -l install $DIR/*.deb >/dev/null 2>&1; then
            dpkg -i $DIR/*.deb || true
            test_missing_xlr_deps
        fi
        test_missing_deps
        ;;
esac


# amzn1 is still not using systemd and depends on the old xcalarctl
# so skipping the new systemd/cgroups stuff
case "$VERSTRING" in
    amzn2|rhel7|el7|ub16)
        mkdir -p ${XCE_INSTALLDIR}/bin
        chown "$XCE_USER":"$XCE_GROUP" "${XCE_INSTALLDIR}/bin"
        sed --follow-symlinks -i "s#/opt/xcalar/#${XCE_INSTALLDIR}#g" $DIR/cgroupControllerUtil.sh
        install -m 700 -o root -g root $DIR/cgroupControllerUtil.sh ${XCE_INSTALLDIR}/bin/cgroupControllerUtil.sh
        sed --follow-symlinks -i "s#/opt/xcalar/#${XCE_INSTALLDIR}#g" $DIR/xcalar-sudo.conf
        if [ "$XCE_USER" != "xcalar" ]; then
            sed --follow-symlinks -i "s#^xcalar ALL=NOPASSWD:#${XCE_USER} ALL=NOPASSWD:#g" $DIR/xcalar-sudo.conf
        fi
        install -m 644 -o root -g root $DIR/xcalar-sudo.conf /etc/sudoers.d/xcalar
        sed -i "s#/opt/xcalar/etc#${XCE_INSTALLDIR}/opt/xcalar/etc#g" $DIR/*.service
        sed -i "s#=-/etc/default/xcalar#=-${XCE_INSTALLDIR}/etc/default/xcalar#g" $DIR/*.service
        sed -i "s#/opt/xcalar/bin#${XCE_INSTALLDIR}/opt/xcalar/bin#g" $DIR/*.service
        sed -i "s#/var/log/xcalar#${XCE_INSTALLDIR}/var/log/xcalar#g" $DIR/*.service
        sed -i "s#/var/opt/xcalar#${XCE_INSTALLDIR}/var/opt/xcalar#g" $DIR/*.service
        # Modify existing ReadOnlyDirectories statements
        # 1. Make the XCE_INSTALLDIR read only
        sed -i 's:^ReadOnlyDirectories=/opt/xcalar$:ReadOnlyDirectories='"${XCE_INSTALLDIR}"'/opt/xcalar:g' $DIR/terminal.service
        # 2. If XcalarRootCompletePath is not set to the default location
        #    a. The default location no longer needs to be read only
        #    b. Add the new location after the '# ReadOnlyDirectories=XcalarRoot' line
        #       if it is not already present
        if [ "$XLRROOT" != "/var/opt/xcalar" ]; then
            sed -i '\:^ReadOnlyDirectories=/var/opt/xcalar$:d' $DIR/terminal.service
            if ! grep -qE '^ReadOnlyDirectories='"${XLRROOT}"'$' $DIR/terminal.service >/dev/null 2&>1; then
                sed -i '\:^# ReadOnlyDirectories=XcalarRoot$:a ReadOnlyDirectories='"${XLRROOT}" $DIR/terminal.service
            fi
        fi
        sed -i "s#/var/tmp/xcalar-root#${XCE_INSTALLDIR}/var/tmp/xcalar-root#g" $DIR/*.service
        if [ "$XCE_USER" != "xcalar" ]; then
            sed -i "s#/var/tmp/xcalar-root#/var/tmp/${XCE_USER}-root#g" $DIR/*.service
        fi
        sed -i "s#User=xcalar#User=${XCE_USER}#g" $DIR/*.service
        sed -i "s#Group=xcalar#User=${XCE_GROUP}#g" $DIR/*.service
        install -m 644 -o root -g root $DIR/caddy.service /usr/lib/systemd/system/xcalar-caddy.service
        # if there is a non-template expserver target, delete it during upgrade
        rm -f /usr/lib/systemd/system/xcalar-expserver.service
        install -m 644 -o root -g root $DIR/expserver.service /usr/lib/systemd/system/xcalar-expserver@.service
        install -m 644 -o root -g root $DIR/jupyter.service /usr/lib/systemd/system/xcalar-jupyter.service
        install -m 644 -o root -g root $DIR/sqldf.service /usr/lib/systemd/system/xcalar-sqldf.service
        install -m 644 -o root -g root $DIR/support-asup.service /usr/lib/systemd/system/xcalar-support-asup.service
        install -m 644 -o root -g root $DIR/terminal.service /usr/lib/systemd/system/xcalar-terminal.service
        install -m 644 -o root -g root $DIR/usrnode.service /usr/lib/systemd/system/xcalar-usrnode.service
        install -m 644 -o root -g root $DIR/xcmgmtd.service /usr/lib/systemd/system/xcalar-xcmgmtd.service
        rm -f /usr/lib/systemd/system/xcalar-services.target
        install -m 644 -o root -g root $DIR/xcalar.service /usr/lib/systemd/system/
        install -m 644 -o root -g root $DIR/xcalar.slice /usr/lib/systemd/system/xcalar.slice
        systemctl daemon-reload
        sed -i "s#/var/log/xcalar#${XCE_INSTALLDIR}/var/log/xcalar#g" $DIR/xcalar-rsyslog.conf
        sed -i "s# xcalar# ${XCE_USER}#g" $DIR/xcalar-rsyslog.conf
        install -m 644 -o root -g root $DIR/xcalar-rsyslog.conf /etc/rsyslog.d/90-xcrsyslog.conf
        systemctl restart rsyslog.service
        ;;
esac

install -m 444 -o root -g root $DIR/90-xcsysctl.conf /etc/sysctl.d
install -m 444 -o root -g root $DIR/90-xclimits.conf /etc/security/limits.d
sed --follow-symlinks -i "s/xce_user/$XCE_USER/g" $DIR/xcalar.cron
sed --follow-symlinks -i "s:/opt/xcalar/scripts:$XCE_INSTALLDIR/opt/xcalar/scripts:g" $DIR/xcalar.cron
install -m 444 -o root -g root $DIR/xcalar.cron /etc/cron.d/xcalar
sed --follow-symlinks -i "s:/opt/xcalar:$XCE_INSTALLDIR/opt/xcalar:g" $DIR/xcalar.ld.so.conf
install -m 444 -o root -g root $DIR/xcalar.ld.so.conf /etc/ld.so.conf.d/xcalar.conf
ldconfig

if have_package logrotate && [ -d "/etc/logrotate.d" ]; then
    install -m 444 -o root -g root $DIR/xclogrotate /etc/logrotate.d
    XCE_LOGROTATE="/etc/logrotate.d/xclogrotate"
    sed --follow-symlinks -i -e "s/create 0644 xcalar xcalar/create 0644 $XCE_USER $XCE_GROUP/g" "$XCE_LOGROTATE"
    sed --follow-symlinks -i -e "s:/var/log/xcalar:$XCE_INSTALLDIR/var/log/xcalar:g" "$XCE_LOGROTATE"
fi

case "$VERSTRING" in
    rhel*|el*)
        if have_package abrt; then
            service_cmd abrtd stop
            autostart abrtd 0
        fi
        ;;
    ub14)
        if have_package apport; then
            service_cmd apport stop 2>/dev/null || true
            echo 'enabled=0' > /etc/default/apport
        fi
        ;;
esac

if command_exists getenforce; then
    # FIXME: This is the wrong way to do it ... We can't ship an installer
    # that disables all of SELinux. For POCs it's ok.
    if [ "$(getenforce)" = "Enforcing" ]; then
        echo >&2 "WARNING: Setting SELinux to permissive"
        setenforce permissive
    fi
    # This is the right way to do it
    if command_exists getsebool; then
        if getsebool -a | grep -q 'daemons_dump_core'; then
            echo >&2 "Setting SELinux to allow daemons_dump_core"
            setsebool -P daemons_dump_core 1 || true
        fi
    fi
fi
test -e /etc/sysconfig/selinux && sed --follow-symlinks -i 's/^SELINUX=enforcing/SELINUX=permissive/g' /etc/sysconfig/selinux


! /is_container 2>/dev/null || exit 0 # anything below shouldn't run in a container

test -f /etc/sysctl.d/90-xcsysctl.conf && sysctl -p /etc/sysctl.d/90-xcsysctl.conf

chmod 755 /etc/rc.local

case "$VERSTRING" in
    amzn1|rhel6|el6|ub14)
        sed --follow-symlinks -i '/^## Xcalar CGroup Start/,/^## Xcalar CGroup End/d' /etc/rc.local
        ${DIR}/cgconfig-setup.sh $XCE_USER $XCE_GROUP
        ;;
    amzn2|rhel7|el7|ub16)
        command_exists systemctl && systemctl enable rc-local.service
        ;;
    *)
        echo >&2 "Unknown cgroup creation command for $VERSTRING"
        ;;
esac

# disable transparent hugepage defragmentation
thpg_path="/sys/kernel/mm/transparent_hugepage"
read thpg_defrag < ${thpg_path}/defrag
read khpgd_defrag < ${thpg_path}/khugepaged/defrag

if [ -n "$(grep ^sysfs /proc/mounts | grep ro)" ]; then
    mount -o rw,remount /sys
fi

if [[ $thpg_defrag =~ never$ ]]; then
    echo "never" > ${thpg_path}/defrag
    cat <<EOF >> /etc/rc.local

if test -f ${thpg_path}/defrag; then
    echo "never" > ${thpg_path}/defrag
fi
EOF
    case "$VERSTRING" in
        rhel6|el6)
            [[ "$khpgd_defrag" =~ no$ ]] && echo "no" > ${thpg_path}/khugepaged/defrag
            cat <<EOF >> /etc/rc.local

if test -f ${thpg_path}/khugepaged/defrag; then
    echo "no" > ${thpg_path}/khugepaged/defrag
fi
EOF
            ;;
        rhel7|el7|ub14|ub16)
            [ "$khpgd_defrag" != "0" ] && echo 0 > ${thpg_path}/khugepaged/defrag
            cat <<EOF >> /etc/rc.local

if test -f ${thpg_path}/khugepaged/defrag; then
    echo 0 > ${thpg_path}/khugepaged/defrag
fi
EOF
            command_exists systemctl && systemctl enable rc-local.service
            ;;
        *)
            echo >&2 "Unknown hugepage defragmentation disable command for $VERSTRING"
            ;;
    esac

    case "$VERSTRING" in
        ub14|ub16)
            sed --follow-symlinks -i '/exit 0/d' /etc/rc.local
            echo 'exit 0' >> /etc/rc.local
            ;;
    esac
fi

# Set up the tmpfs size as a percent of physical memory
sed --follow-symlinks --in-place '/\/dev\/shm/d' /etc/fstab
tmpFsPctOfPhysicalMem=95
echo "none      /dev/shm        tmpfs   defaults,size=${tmpFsPctOfPhysicalMem}%        0 0" >> /etc/fstab
mount -o remount /dev/shm

if [ "$XCE_FIREWALL_CONFIG" = "1" ]; then
    # enable ports
    if command_exists firewall-cmd; then
        fw=$(firewall-cmd --state &>/dev/null; echo $?)
        if [ $fw -eq 0 ]; then
            firewall-cmd --permanent --add-port=8443/tcp
            firewall-cmd --permanent --add-port=9090/tcp
            firewall-cmd --permanent --add-port=18552/tcp
            firewall-cmd --permanent --add-port=5000/tcp
            firewall-cmd --permanent --add-port=8000/tcp
            firewall-cmd --permanent --add-port=8000/udp
            firewall-cmd --permanent --add-port=8889/tcp
            firewall-cmd --permanent --add-service=nfs
            firewall-cmd --permanent --add-service=mountd
            firewall-cmd --permanent --add-service=rpc-bind
            firewall-cmd --reload
        fi
    elif command_exists iptables; then
        for port in 8443 9090 18552 5000 8000 8889; do
            if ! iptables -C INPUT -m state --state NEW -m tcp -p tcp --dport $port -j ACCEPT &>/dev/null; then
                iptables -I INPUT 4 -m state --state NEW -m tcp -p tcp --dport $port -j ACCEPT || true
            fi
        done

        iptables-save > /etc/sysconfig/iptables || true
    fi
fi
