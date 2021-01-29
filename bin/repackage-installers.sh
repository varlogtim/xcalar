#!/bin/bash
#
# This script removes packages from the standard RHEL and EPEL
# repos to which the customer should have access to (via www or a local
# mirror). The reasons for this are that a) we're not allowed to redist
# RHEL binary packages, b) customers would rather we use local mirror
# packages that match whatever versions they have locked down c) our
# package could have older packages in it than what is available.
#
# $ repackage-installer.sh <xcalar-1.x.y.z_SHA1-installer>
#
# Produces 4 new installers suffixed with -el6, el7, -deb, and -all
# containing  platform specific installers, or -all containing all
# platforms.
#
set -e
XCALAR_GPG=$XLRDIR/src/data/RPM-GPG-KEY-Xcalar
IUS_GPG=$XLRDIR/src/data/IUS-COMMUNITY-GPG-KEY
XCALAR_SIG="faf9342363a342a9"
IUS_SIG='da221cdf9cd4953f'

if [ -z "$1" ]; then
	echo >&2 "Usage: $0 xcalar-full-installer"
	exit 1
fi

rpmsig () {
    rpm -qp "$1" --qf '%{NAME}-%{VERSION}-%{RELEASE} %{SIGPGP:pgpsig} %{SIGGPG:pgpsig}\n'
}

JOB_NAME="${JOB_NAME:-local}"
TMPDIR="${TMPDIR:-/tmp/`id -un`}/${JOB_NAME}/repackage-installers"
rm -rf "$TMPDIR"
mkdir -p "$TMPDIR/xcalar-installer"
sed -n '/^__XCALAR_TARBALL__/,$p' "$1" | tail -n+2 > "$TMPDIR/xcalar-installer.tar.gz"

WORKDIR="$(pwd)"
cd "$TMPDIR/xcalar-installer"
tar zxif ../xcalar-installer.tar.gz
if ! rpm --addsign el*/xcalar-*.rpm; then
    echo >&2 "Failed to sign rpm packages. Skipping"
fi

cp $XLRDIR/src/data/*-GPG-* .

# Delete any files that are not from our repo or from the IUS repo.
for elv in el6 el7; do
    for rpm in `ls ${elv}/*.rpm`; do
        if ! rpmsig "${rpm}" 2>/dev/null | grep -q "$XCALAR_SIG"; then
            if ! rpmsig "${rpm}" 2>/dev/null | grep -q "$IUS_SIG"; then
                rm -fv "${rpm}"
            fi
        fi
    done
    for rpm in `ls ${elv}/*.rpm`; do
        rpmsig "${rpm}"
    done
done
cd ..
for pkg in el6 el7 deb all; do
    pkg_tar="$(pwd)/xcalar-installer-${pkg}.tar.gz"
    if [ "$pkg" = "all" ]; then
        (cd xcalar-installer && tar czf "${pkg_tar}" .)
    else
        (cd xcalar-installer && tar czf "${pkg_tar}" .)
    fi
    pkg_installer="${WORKDIR}/${1}-${pkg}"
    echo "Writing ${pkg_installer} ..."
    cat "$XLRDIR/bin/installer/installer.sh" "${pkg_tar}" > "${pkg_installer}"
    chmod 0755 "${pkg_installer}"
done

