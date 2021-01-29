Name:           xcalar
License:        Proprietary
Group:          Applications/Databases
Summary:        Xcalar Platform Core. Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
Vendor:         Xcalar, Inc.
Version:        %{_version}
Release:        %{_build_number}
URL:            http://www.xcalar.com
Packager:       Xcalar, Inc. <support@xcalar.com>
Source0:        %{name}-%{version}.tar
Buildroot:      %{_tmppath}/%{name}-root
Prefix:         %prefix

Requires:       xcalar-python36 xcalar-node10 xcalar-antlr xcalar-arrow-libs xcalar-caddy xcalar-jre8
Requires(pre):  xcalar-platform

%description
Xcalar Data Platform Core Package

%prep
%setup -q -n %{name}-%{version}

%build
bin/cmBuild clean
bin/cmBuild config $BUILD_TYPE -DCOVERAGE=$COVERAGE
bin/cmBuild installedObjects

%install
DESTDIR=%{buildroot} bin/cmBuild install
TAR_INSTALLER=%{_xlrdir}/build/xce/%{name}-%{_version}-%{_build_number}.tar.gz
mkdir -p %{_xlrdir}/build/xce
fakeroot tar czf "$TAR_INSTALLER" -C %{buildroot} opt etc

# XXX: Example of installing something 'manually' without automake
#install -d $RPM_BUILD_ROOT/%{_bindir}
#install -p -m 755 src/bin/usrnode/usrnode $RPM_BUILD_ROOT/%{_bindir}/usrnode

%clean
rm -rf $RPM_BUILD_ROOT

# See https://fedoraproject.org/wiki/Packaging:Scriptlets
%pre

test -e /usr/bin/python2 || ln -s /usr/bin/python2.7 /usr/bin/python2

if [ "$1" = "1" ]; then
    #  initial install
    true
elif [ "$1" = "2" ]; then
    # upgrade
    systemctl stop xcalar || true
fi
exit 0

%post
ldconfig

# Xc-7811 - we do user checks on both install and upgrade
if [ -r "/etc/default/xcalar" ]; then
    . /etc/default/xcalar
fi

XCE_USER="${XCE_USER:-xcalar}"
XCE_GROUP="${XCE_GROUP:-$XCE_USER}"
XCE_HOME="${XCE_HOME:-/var/opt/xcalar}"
XCE_USER_HOME="${XCE_USER_HOME:-/home/xcalar}"
XCE_WORKDIR="${XCE_WORKDIR:-/var/tmp/xcalar-root}"
XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"
XCE_LOGDIR="${XCE_LOGDIR:-/var/log/xcalar}"
XLRDIR="${XLRDIR:-/opt/xcalar}"
xce_config_dir="$(dirname $XCE_CONFIG)"

if ! getent group "$XCE_GROUP" >/dev/null ; then
    groupadd -f "$XCE_GROUP" || exit 1
fi
if ! getent passwd "$XCE_USER" >/dev/null ; then
    useradd -g "$XCE_GROUP" -d "$XCE_USER_HOME" -s /bin/bash -c "Xcalar software owner" "$XCE_USER" || exit 1
fi

mkdir -p "$XCE_LOGDIR" /var/opt/xcalar/stats "$XCE_WORKDIR"
chown "${XCE_USER}:${XCE_GROUP}" "$XCE_LOGDIR" /var/opt/xcalar /var/opt/xcalar/stats "$XCE_WORKDIR"
chmod u+w "$XCE_WORKDIR"

sed -i "s/xce_user/$XCE_USER/g" /etc/cron.d/xcalar

# initial install

if [ "$1" = "1" ]; then
    #TODO: figure out if we need to do more here
    hostname_f="$(hostname -f 2>/dev/null)"
    if [ $? -ne 0 ] || [ -z "$hostname_f" ]; then
        hostname_f="$(hostname 2>/dev/null)"
    fi

    if ! test -e $XCE_CONFIG; then
        $XLRDIR/scripts/genConfig.sh /etc/xcalar/template.cfg $XCE_CONFIG localhost
    fi
elif [ "$1" = "2" ]; then
    # upgrade
    true
fi

# ENG-7119 Fix for THP
if test -x /usr/sbin/tuned-adm; then
    tuned-adm profile xcalar || true
fi

chown $XCE_USER:$XCE_GROUP $xce_config_dir $XCE_CONFIG
exit 0

%preun

if [ "$1" = "0" ]; then
    if [ -r "/etc/default/xcalar" ]; then
        . /etc/default/xcalar
    fi
    # ENG-7119 Fix for THP
    if test -x /usr/sbin/tuned-adm; then
        tuned-adm profile `tuned-adm recommend` || true
    fi
    set +e
    XCE_CONFIG="${XCE_CONFIG:-/etc/xcalar/default.cfg}"

    # Undo everything, being mindful that some of the operations might
    # have failed. We have to undo as much as possible without crippling
    # the customer. Leave XcalarRootCompletePath alone.
    systemctl stop xcalar
    rm -f /var/www/xcalar-gui
    rm -f /var/www/xcalar-design
    rm -f /opt/xcalar/share/NOTICE.XD
    if test -e "$XCE_CONFIG"; then
        xcalar_root=$(awk -F'=' '$1 == "Constants.XcalarRootCompletePath" {print $2}' "$XCE_CONFIG")
        xcalar_root="${xcalar_root##*://}"
        if test -n "$xcalar_root"; then
            xcalar_created=$(grep '#xcalar_mnt' /etc/fstab 2>/dev/null | grep "$xcalar_root" >/dev/null 2>&1; echo $?)
            if test "$xcalar_created" = "0" && mountpoint -q $xcalar_root; then
                umount -f $xcalar_root
                sed -i -e '\@'$xcalar_root'@d' /etc/fstab
                rmdir $xcalar_root || true
            fi
        fi
    fi
fi
exit 0

%postun
if [ "$1" = "0" ]; then
    # uninstall fully
    ldconfig
fi
exit 0

%files
%defattr(-,root,root,-)
/opt/xcalar/*
/etc/xcalar/hdfs-default.xml
/etc/xcalar/template.cfg
/etc/xcalar/supervisor.conf
/etc/xcalar/EcdsaPub.key
/etc/xcalar/fieldFuncTests.cfg
/etc/init.d/xcalar
/etc/logrotate.d/xclogrotate
/etc/rsyslog.d/90-xcrsyslog.conf
/etc/security/limits.d/90-xclimits.conf
/etc/sysctl.d/90-xcsysctl.conf
/etc/ld.so.conf.d/xcalar.conf
/etc/cron.d/xcalar
/etc/sudoers.d/xcalar
/usr/lib/systemd/system/xcalar.service
/usr/lib/systemd/system/xcalar.slice
/usr/lib/systemd/system/xcalar-caddy.service
/usr/lib/systemd/system/xcalar-expserver@.service
/usr/lib/systemd/system/xcalar-jupyter.service
/usr/lib/systemd/system/xcalar-sqldf.service
/usr/lib/systemd/system/xcalar-support-asup.service
/usr/lib/systemd/system/xcalar-terminal.service
/usr/lib/systemd/system/xcalar-usrnode.service
/usr/lib/systemd/system/xcalar-xcmgmtd.service
/usr/lib/tuned/xcalar/tuned.conf

%config(noreplace) %attr(0644,root,root) /etc/default/xcalar
%config(noreplace) %attr(0644,root,root) /etc/xcalar/Caddyfile
%config(noreplace) %attr(0644,root,root) /etc/xcalar/hdfs-client.xml

%changelog
