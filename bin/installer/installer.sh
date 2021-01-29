#!/bin/bash

# Copyright 2015-2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
# The idea is for this script to drive the installation.
# All the files required for installation are in a gzip tarball
# appended right at the end of this script, so that this script
# is a self-contained installation.

# Run xcalar/bin/installer/mkInstaller.sh
# to create the self-contained installer

# UB support has been removed, however conditionals and wrapper functions have
# been preserved in case we wanted to add different platforms later on

# shellcheck disable=SC1091,SC2086

export PATH=/usr/sbin:/usr/bin:/sbin:/bin

USAGE="$0 [--stop] [--nostart] [--start] [--startonboot] [--nostartonboot] [-x path]"

# ON EL system it's customary to have the admin
# start the service, not the packaging system
if [ -e /etc/system-release ]; then
    startNode=0
else
    startNode=1
fi
stopNode=1
startOnBoot=
daemonReload=1
runningSystemd=0
waitAfterStart=0


if [ -r "/etc/default/xcalar" ]; then
    . /etc/default/xcalar
fi

XCE_USER="${XCE_USER:-xcalar}"
XCE_GROUP="${XCE_GROUP:-$XCE_USER}"
XCE_HOME="${XCE_HOME:-/var/opt/xcalar}"
XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"
PREFIX="${PREFIX:-/opt/xcalar}"
XCE_CADDYFILE="${XCE_CADDYFILE:-/etc/xcalar/Caddyfile}"
XCE_LOCAL_ENV="${XCE_LOCAL_ENV:-/etc/default/xcalar}"
XCE_DEFAULT_ENV="${XCE_DEFAULT_ENV:-/opt/xcalar/etc/default/xcalar}"

## Needed for microsoft mssql support packages
export ACCEPT_EULA=Y


say () {
    echo >&2 "$*"
}

die () {
    echo >&2 "ERROR: $*"
    exit 1
}

command_exists () {
    command -v "$@" >/dev/null 2>&1
}

have_package () {
    case "$VERSTRING" in
        amzn*|rhel*|el*)
            rpm -q "$1" &>/dev/null
            ;;
    esac
}

in_container() {
    [ -z "$container" ] || return 0
    local cid
    if /is_container 2>/dev/null ; then
        return 0
    fi
    return 1
}

have_package_file_version () {
    local pkg_file="$1"
    local pkg_name="" pkg_ver="" installed_ver=""

    if ! [ -f "$pkg_file" ]; then
        return 1
    fi

    case "$VERSTRING" in
        amzn*|rhel7|el7)
            pkg_name="$(rpm -qp --qf '%{NAME}' "$pkg_file")"
            pkg_ver="$(rpm -qp --qf '%{VERSION}' "$pkg_file")"
            ;;
    esac

    if ! have_package "$pkg_name"; then
        return 1
    fi

    case "$VERSTRING" in
        amzn*|rhel7|el7)
            installed_ver="$(rpm -q --qf '%{VERSION}' "$pkg_name")"
            if [ "$pkg_ver" != "$installed_ver" ]; then
                return 1
            fi
            ;;
    esac

    return 0
}

service_cmd () {
    local svc="$1" cmd="$2"
    case "$VERSTRING" in
        amzn1)
            service $svc $cmd
            ;;
        amzn2|rhel7|el7)
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
        amzn1)
            offon=(off on)
            chkconfig $svc ${offon[$on]}
            ;;
        amzn2|rhel7|el7)
            offon=(disable enable)
            systemctl ${offon[$on]} $svc
            ;;
        *)
            echo >&2 "$svc $on: Unknown autostart command for $VERSTRING"
            return 1
            ;;
    esac
}

extract () {
    sed -n '/^__XCALAR_TARBALL__/,$p' "$1" | tail -n+2
}

extract_top () {
    sed -n '1,/^__XCALAR_TARBALL__$/p' "$1"
}

update_local_config () {
    local setting="$1"
    local key="$(echo "$setting" | cut -d = -f1)"
    local last_key="$(grep -E "^${key}=" "$XCE_LOCAL_ENV" | tail -1)"

    if [ "$last_key" != "$setting" ]; then
        echo "$setting" >> "$XCE_LOCAL_ENV"
    fi
}

say "### Parsing input flags"

myPath="$0"

while [ $# -gt 0 ]; do
    opt="$1"
    shift
    case "$opt" in
        --*httpd*)
            echo >&2 "WARNING: $opt and other httpd settings have been deprecated!! Please remove them from your scripts"
            echo >&2 "WARNING: Sleeping for 30s to encourage fixing without breaking existing scripts"
            sleep 30
            ;;
        --caddy)
            echo >&2 "WARNING: caddy is enabled by default now!! Please remove --$opt from your scripts"
            echo >&2 "WARNING: Sleeping for 5s to encourage fixing without breaking existing scripts"
            sleep 5
            ;;
        -x)
            EXTRACTDIR="$1"
            shift
            test -n "$EXTRACTDIR" || { echo >&2 "Must specify dir to extract to"; exit 1; }
            mkdir -p "${EXTRACTDIR}/xcalar-install" || { echo >&2 "Unable to create "; exit 1; }
            extract "$myPath" | tee "${EXTRACTDIR}/xcalar-install.tar.gz" | tar zixf - -C "$EXTRACTDIR/xcalar-install" &&
            extract_top "$myPath" > "${EXTRACTDIR}/installer.sh"
            exit $?
            ;;
        --start[Oo]n[Bb]oot)
            startOnBoot=1
            ;;
        --no[sS]tart[Oo]n[Bb]oot)
            startOnBoot=0
            ;;
        --start)
            startNode=1
            ;;
        --no[sS]tart)
            startNode=0
            ;;
        --stop)
            stopNode=1
            ;;
        --nostop)
            stopNode=0
            ;;
        --wait)
            waitAfterStart=1
            ;;
        -h|--help)
            echo "Usage: $USAGE"
            exit 0
            ;;
        --osid)
            export VERSTRING="$1"
            shift
            # Remove the version numbers
            NAME="${VERSTRING%%[0-9]*}"
            ELVERSION="${VERSTRING#$NAME}"
            ;;
        *)
            echo "Usage: $USAGE"
            die "Unknown argument: $opt"
            ;;
    esac
done

if [ "$(id -u)" != "0" ]; then
    echo "The installer requires root privileges. Please use sudo, or run it as root."
    exit 1
fi


if [ -z "$VERSTRING" ]; then
    say "### Checking environment and operating system version"

    if [ -f /etc/os-release ]; then
        . /etc/os-release
        case "$NAME" in
            Alibaba\ Group\ Enterprise\ Linux\ Server)
                ELVERSION="${VERSION%%.*}"    # Only seen 7
                VERSTRING="el${ELVERSION}"
                ;;
            *)
                case "$ID" in
                    alinux)
                        ELVERSION=7
                        VERSTRING="el${ELVERSION}"
                        ;;
                    rhel|ol)
                        ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
                        VERSTRING="rhel${ELVERSION}"
                        ;;
                    centos)
                        ELVERSION="$(echo $VERSION_ID | cut -d'.' -f1)"
                        VERSTRING="el${ELVERSION}"
                        ;;
                    amzn)
                        if [ "$NAME" = "Amazon Linux AMI" ]; then
                            AMZNVERSION="1"
                            VERSTRING="amzn1"
                            ELVERSION=6
                        elif [ "$NAME" = "Amazon Linux" ]; then
                            AMZNVERSION="2"
                            VERSTRING="amzn2"
                            ELVERSION=7
                        fi
                        ;;
                    *)
                        echo >&2 "Unknown OS version: $PRETTY_NAME ($VERSION)"
                        ;;
                esac
        esac
    fi
else
    say "### OSID specified as ${VERSTRING}. Skipping detection"
fi

if [ -z "$VERSTRING" ] && [ -e /etc/system-release ]; then
    ELVERSION="$(grep -Eow '([0-9\.]+)' /etc/system-release | cut -d'.' -f1)"
    if grep -q 'Red Hat' /etc/system-release; then
        VERSTRING="rhel${ELVERSION}"
    elif grep -q CentOS /etc/system-release; then
        VERSTRING="el${ELVERSION}"
    elif grep -q 'Alibaba' /etc/system-release; then
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

say "### Operating system is $VERSTRING"

# Do online install by default on EL
case "$VERSTRING" in
    amzn*|rhel*|el*)
        OFFLINE="${OFFLINE:-0}"
        CENTOS=1
        ;;
    *)
        die "Unsuported OS $VERSTRING"
        ;;
esac

say "### Extract installer payload"

tmpdir=$(mktemp -d --tmpdir xcalarInstall.XXXXX)
if [ "$SAVETMP" = "1" ]; then
    echo >&2 "Temporary files are in $tmpdir"
else
    trap "rm -rf $tmpdir" EXIT
fi

if [ "$CENTOS" = "1" ]; then
    # These don't come standard in container versions of some images
    DEPS="sudo tar gzip"

    # First call to yum, take this opportunity to clean out old data
    yum clean --enablerepo='*' all
    rm -rf /var/cache/yum/*
    yum install -y $DEPS
fi

set -o pipefail
if ! extract "$myPath" | tar zixf - -C "$tmpdir"; then
    echo >&2 "Failed to extract payload from $myPath."
    exit 1
fi
set +o pipefail

cd "$tmpdir"

if [ "$CENTOS" = "1" ]; then
    cd "${VERSTRING#rh}" || die "Installer package for ${VERSTRING} not found"
fi

say "### Install Xcalar dependencies"

if [ "$OFFLINE" != "1" ]; then
    echo "Installing dependencies"
    if [ "$CENTOS" = "1" ]; then
        rpm --import ../*-GPG-*
        if [ "$VERSTRING" = "amzn2" ]; then
            amazon-linux-extras install -y epel
        fi
        if ! have_package epel-release; then
            echo "Attempting to install epel-release from local repos ..."
            if ! yum install -y epel-release; then
                EPELRPM="https://dl.fedoraproject.org/pub/epel/epel-release-latest-${ELVERSION}.noarch.rpm"
                echo "Attempting to install epel-release from Fedora ($EPELRPM) ..."
                if ! yum install -y "${EPELRPM}"; then
                    echo "*** Failed to install epel-release from your existing repos and from Fedora. Xcalar may not install properly ***."
                fi
            fi
        fi
    fi
fi

# Are we running under a systemd booted system?
if [ -d '/run/systemd/system' ] && \
   [[ "$(stat /proc/1/exe | grep 'File:')" =~ .*systemd.* ]]; then
    runningSystemd=1
else
    update_local_config "XCE_USE_SYSTEMD=0"
fi

# There two versions of the xcalar unit file, one that
# calls xcalarctl and one that does not.  If xcalar is
# currently running using the older mechanism, make
# sure that it is stopped using the old mechanism and
# restarted under the new one.
USRNODE_UNIT="/lib/systemd/system/xcalar-usrnode.service"
TARGET_UNIT="/lib/systemd/system/xcalar-services.target"
if [ "$runningSystemd" = "1" ] && \
       have_package xcalar && \
       ! [ -f "$USRNODE_UNIT" ] && \
       service_cmd xcalar status >/dev/null 2>&1; then
    stopNode=1
fi

# This handles any intermediate development builds (rare)
if [ "$runningSystemd" = "1" ] && \
       have_package xcalar && \
       [ -f "$TARGET_UNIT" ] && \
       service_cmd xcalar-services.target status >/dev/null 2>&1; then
    service_cmd xcalar-services.target stop
fi

if [ "$stopNode" = "1" ] && have_package xcalar; then
    say "### Shut down any running instances of Xcalar"
    service_cmd xcalar stop
fi

say "### Installing Xcalar Compute Engine"
rc=0
if [ "$CENTOS" = "1" ]; then
    say "### Modify SELinux configuration"

    if command_exists getenforce; then
        # FIXME: This is the wrong way to do it ... We can't ship an installer
        # that disables all of SELinux. For POCs it's ok.
        if [ "$(getenforce)" = "Enforcing" ]; then
            echo >&2 "WARNING: Setting SELinux to permissive"
            setenforce permissive
        fi
        # This is the right way to do it
        if command_exists getsebool; then
            if getsebool -a | grep -q 'httpd_can_network_connect --> off'; then
                echo >&2 "Setting SELinux to allow httpd_can_network_connect"
                setsebool -P httpd_can_network_connect on
            fi
            if getsebool -a | grep -q 'daemons_dump_core'; then
                echo >&2 "Setting SELinux to allow daemons_dump_core"
                setsebool -P daemons_dump_core 1
            fi
        fi
    fi
    test -e /etc/sysconfig/selinux && sed --follow-symlinks -i 's/^SELINUX=enforcing/SELINUX=permissive/g' /etc/sysconfig/selinux

    say "### Install xcalar-provided packages"

    XLR_PKG="$(find ../ -name 'xcalar-[0-9]*.rpm' -print)"
    if [ "$OFFLINE" = "1" ]; then
        yum -y install --disableplugin=priorities ./*.rpm ../unibuild/*.rpm || \
        rpm -f --oldpackage --replacefiles --replacepkgs --nodeps --nofiles -Uvh ./*.rpm ../unibuild/*.rpm
        install_rc=$?
        rc=$(( rc + install_rc ))
    else
        yum -y install --disableplugin=priorities ./*.rpm ../unibuild/*.rpm
        install_rc=$?
        rc=$(( rc + install_rc ))
    fi

    if ! have_package_file_version "$XLR_PKG"; then
        echo >&2 "Xcalar package file $XLR_PKG is not installed.  Installation failed with return code $rc."
        exit 1
    fi
fi

install_pip_bundle() {
    pip_bundle_tmpdir=$(mktemp -d -t pip_bundle.XXXXXX)
    if tar zxf "$tmpdir"/pip-bundle*.tar.gz -C "$pip_bundle_tmpdir" 2>/dev/null; then
        (cd "$pip_bundle_tmpdir" && unset PS4 && set +x && bash install.sh --python $PREFIX/bin/python3) || return 1
    fi
    rm -rf "$pip_bundle_tmpdir"
    return 0
}

install_pip_bundle

if [ -e ../post-install.sh ]; then
    (cd .. && bash post-install.sh)
fi

# when rerunning the node installer on an installed system, caddy is still loaded into
# memory and the 'cp -p' fails. Safely replace the file by removing it first. mv would
# be better
rm -f "${PREFIX}/bin/caddy"
if ! cp -p ${tmpdir}/caddy "${PREFIX}/bin"; then
    rc=$(( rc + 1 ))
else
    # Allow non-root to bind to 443/80
    if setcap cap_net_bind_service=+ep "${PREFIX}/bin/caddy"; then
        XCE_HTTPS_PORT=443
    else
        XCE_HTTPS_PORT=8443
        sed --follow-symlinks -i -r 's/^[#]?XCE_HTTPS_PORT=.*$/XCE_HTTPS_PORT='$XCE_HTTPS_PORT'/g' /etc/default/xcalar
    fi
    sed --follow-symlinks -i "s:redirect /assets/htmlFiles/login.html:redirect {\$XCE_ACCESS_URL}:g" "$XCE_CADDYFILE"
    sed --follow-symlinks -i "s:except /assets/htmlFiles/login.html: except {\$XCE_LOGIN_PAGE}:g" "$XCE_CADDYFILE"
fi


say "### Install java parquet support"
if ! cp ${tmpdir}/parquet-tools-1.8.2.jar ${PREFIX}/bin/; then
    rc=$(( $rc + 1 ))
fi

say "### Install sqldf support"
if ! cp ${tmpdir}/xcalar-sqldf.jar ${PREFIX}/lib/; then
    rc=$(( $rc + 1 ))
fi

say "### Check for any installation failures"

if [ $rc -ne 0 ]; then
    echo >&2 "Failed to install or remove packages"
    exit $rc
fi

say "### Installing GUI files"
cd ..
test -e /opt/xcalar/xcalar-gui && mv /opt/xcalar/xcalar-gui /opt/xcalar/xcalar-gui-$$
case "$VERSTRING" in
    amzn*|el7)
        gui_distro="$VERSTRING"
        ;;
    rhel7)
        gui_distro="el7"
        ;;
esac
tar xzf gui/xcalar-gui.tar.gz -C /opt/xcalar
tar xzf gui/node-modules.tar.gz -C /opt/xcalar/xcalar-gui/services/expServer

cp /opt/xcalar/xcalar-gui/NOTICE /opt/xcalar/share/NOTICE.XD
chmod 444 /opt/xcalar/share/NOTICE.XD
# change the owner of extensions tree so non-root user can download/admin extensions
chown "$XCE_USER:$XCE_GROUP" /opt/xcalar/xcalar-gui/assets/extensions/*


mkdir -p /var/www/
ln -sfn /opt/xcalar/xcalar-gui /var/www/xcalar-gui
ln -sfn /opt/xcalar/xcalar-gui /var/www/xcalar-design
chown "$XCE_USER:$XCE_GROUP" /var/www/xcalar-gui /var/www/xcalar-design
rm -rf /opt/xcalar/xcalar-gui-$$

say "### Updating GUI config based on hostname"

cd /opt/xcalar/xcalar-gui
if [ -f assets/js/sample-config.js ] && [ ! -f assets/js/config.js ]; then
    say "### Updating GUI config based on hostname"
    cp assets/js/sample-config.js assets/js/config.js
fi
cd -

say "### Setting up logging and firewall"

mkdir -p /etc/xcalar
mkdir -p /var/log/xcalar
chown "$XCE_USER:$XCE_GROUP" /var/log/xcalar /var/opt/xcalar
chmod 755 /var/log/xcalar
cd "$tmpdir"

if [ "$CENTOS" = "1" ] && ! in_container; then
    # enable ports
    if command_exists firewall-cmd; then
        firewall-cmd --state &>/dev/null
        fw=$?
        if [ $fw -eq 0 ]; then
            firewall-cmd --permanent --add-port=${XCE_HTTPS_PORT}/tcp
            firewall-cmd --permanent --add-port=18552/tcp
            firewall-cmd --permanent --add-port=5000/tcp
            firewall-cmd --permanent --add-port=8000/tcp
            firewall-cmd --permanent --add-port=8000/udp
            firewall-cmd --permanent --add-service=nfs
            firewall-cmd --permanent --add-service=mountd
            firewall-cmd --permanent --add-service=rpc-bind
            firewall-cmd --reload
        fi
    elif command_exists iptables; then
        for port in ${XCE_HTTPS_PORT} 18552 5000 8000; do
            if ! iptables -C INPUT -m state --state NEW -m tcp -p tcp --dport $port -j ACCEPT &>/dev/null; then
                iptables -I INPUT -m state --state NEW -m tcp -p tcp --dport $port -j ACCEPT
            fi
        done
        for port in 8000; do
            if ! iptables -C INPUT -m state --state NEW -m udp -p udp --dport $port -j ACCEPT &>/dev/null; then
                iptables -I INPUT -m state --state NEW -m udp -p udp --dport $port -j ACCEPT
            fi
        done
        iptables-save > /etc/sysconfig/iptables
    fi
fi

case "$VERSTRING" in
    amzn*|rhel*|el*)
        if have_package abrt; then
            service_cmd abrtd stop
            autostart abrtd 0
        fi
        ;;
esac

if test -f /etc/sysctl.d/90-xcsysctl.conf && ! in_container; then
    sysctl -p /etc/sysctl.d/90-xcsysctl.conf
fi

# Create directories for, and populate, jupyter assets
mkdir -p "$XCE_HOME"/.jupyter
mkdir -p "$XCE_HOME"/.ipython
cp -r /opt/xcalar/xcalar-gui/assets/jupyter/jupyter/* "$XCE_HOME"/.jupyter
cp -r /opt/xcalar/xcalar-gui/assets/jupyter/ipython/* "$XCE_HOME"/.ipython
chown -R "$XCE_USER:$XCE_GROUP" "$XCE_HOME"/.jupyter "$XCE_HOME"/.ipython

say "### Populate dataset needed by the XD TestSuite"
# done with the rpm above now, but still creating the symlink
ln -sfn /opt/xcalar/test_data /var/tmp/qa
# Tell xcalar-gui the location of our test data.
sed --follow-symlinks -i "s,/netstore/datasets/,/opt/xcalar/test_data/," /opt/xcalar/xcalar-gui/assets/js/shared/setup.js

if [ -e "$XCE_CONFIG" ]; then
    echo >&2 "WARNING: $XCE_CONFIG already exists. Not regenerating."
    XLRROOT="$(awk -F'=' '/^Constants.XcalarRootCompletePath/{print $2}' $XCE_CONFIG)"
    if in_container ; then
        grep -q '^Constants.Cgroups' "$XCE_CONFIG" && sed -i 's/^Constants.Cgroups.*/Constants.Cgroups=false/' "$XCE_CONFIG" || echo 'Constants.Cgroups=false' >> "$XCE_CONFIG"
    fi
    say "### Blocking terminal read access to XcalarRootCompletePath"
    xlrRootReadOnly="ReadOnlyDirectories=${XLRROOT}"
    terminalUnitFile="/lib/systemd/system/xcalar-terminal.service"
    if ! grep -qE '^'"${xlrRootReadOnly}"'$' "${terminalUnitFile}" >/dev/null 2&>1; then
        sed -i "/^# ReadOnlyDirectories=XcalarRoot$/a $xlrRootReadOnly" "$terminalUnitFile"
    fi
else
    say "### Generating Xcalar default.cfg"
    /opt/xcalar/scripts/genConfig.sh /etc/xcalar/template.cfg - localhost > $XCE_CONFIG
    chown $XCE_USER:$XCE_GROUP $XCE_CONFIG
    # xcalar-terminal.service has ReadOnlyDirectories=/var/opt/xcalar by default
fi

say "### Updating ldap config files"
LDAP_CONFIG_FILE="${XLRROOT}/config/ldapConfig.json"
if [ -n "$XLRROOT" -a -d "$XLRROOT" ]; then
    if [ -f "$LDAP_CONFIG_FILE" ]; then
        sed --follow-symlinks -i.bak 's/"useTLS" *: *"\([truefals]\{4,\}\)"/"useTLS":\1/g' $LDAP_CONFIG_FILE
        sed --follow-symlinks -i.bak 's/"activeDir" *: *"\([truefals]\{4,\}\)"/"activeDir":\1/g' $LDAP_CONFIG_FILE
    else
        mkdir -p "${XLRROOT}/config"
        chown "$XCE_USER:$XCE_GROUP" "${XLRROOT}/config"
        echo '{"ldapConfigEnabled": false}' > "$LDAP_CONFIG_FILE"
        chown "$XCE_USER:$XCE_GROUP" "$LDAP_CONFIG_FILE"
    fi
    say "### Creating JupyterNotebook folder"
    mkdir -p "${XLRROOT}/jupyterNotebooks"
    chown -R "$XCE_USER:$XCE_GROUP" "${XLRROOT}/jupyterNotebooks"
fi

if [ -e "$XCE_HOME"/jupyterNotebooks ]; then
    echo >&2 "WARNING: Jupyter folder exists but will be moved by upgrade tool"
fi


if ! in_container; then
    $PREFIX/bin/xcalar-start-pre.sh
    case "$VERSTRING" in
        amzn1)
            ;;
        amzn2|rhel7|el7)
            systemctl enable rc-local.service
            ;;
        *)
            echo >&2 "Unknown cgroup creation command for $VERSTRING"
            ;;
    esac
    chmod 755 /etc/rc.local

    # Remove previously added code
    sed --follow-symlinks -i '/^## Xcalar CGroup Start/,/^## Xcalar CGroup End/d' /etc/rc.local

    # Set up the tmpfs size as a percent of physical memory
    sed --follow-symlinks --in-place '/\/dev\/shm/d' /etc/fstab
    tmpFsPctOfPhysicalMem=95
    echo "none      /dev/shm        tmpfs   defaults,size=${tmpFsPctOfPhysicalMem}%        0 0" >> /etc/fstab

    mount -o remount /dev/shm
fi

if have_package logrotate; then
    XCE_LOGROTATE="/etc/logrotate.d/xclogrotate"
    if [ -f $XCE_LOGROTATE ]; then
        sed --follow-symlinks -i -e "s/create 0644 xcalar xcalar/create 0644 $XCE_USER $XCE_GROUP/g" "$XCE_LOGROTATE"
    fi
fi

say "### Creating empty license file"
test -e /etc/xcalar/XcalarLic.key || \
    touch /etc/xcalar/XcalarLic.key

lic_owner="$(stat -c '%U' /etc/xcalar/XcalarLic.key)"
lic_group="$(stat -c '%G' /etc/xcalar/XcalarLic.key)"
test "$lic_owner" != "$XCE_USER" -o "$lic_group" != "$XCE_GROUP" && \
    chown $XCE_USER:$XCE_GROUP /etc/xcalar/XcalarLic.key

if have_package rsyslog; then
    service_cmd rsyslog restart
fi


say "### Enabling system service autostart"
autostart rsyslog 1
autostart rpcbind 1

# This needs to be called regardless of whether we're starting now or not
# When building images, we usually don't want it started during install,
# only on next boot and going forward
if [ -n "$startOnBoot" ]; then
    autostart xcalar $startOnBoot
fi

if [ "$startNode" = "1" ] && [ "$stopNode" = "1" ]; then
    say "### Starting Xcalar"
    service_cmd xcalar start

    if [ "$runningSystemd" = "1" ] && \
           [ "$waitAfterStart" = "1" ]; then
        $(set -a; \
          source /opt/xcalar/etc/default/xcalar; \
          source /etc/default/xcalar; \
          set +a; \
          /opt/xcalar/bin/usrnode-service-responding.sh)
    fi
fi

exit $?
__XCALAR_TARBALL__
