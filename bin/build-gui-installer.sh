#!/bin/bash
# SUMARY:
# builds the xcalar GUI user installer . includes bits built from build-gui-installer-os.sh, build-rpms and more
# it used to also be the GUI installer, but that's deprecated. This is only the usermode type.
# OUTPUT:
# - shar (self-extract bash archive) build/xcalar-gui-userinstaller-${XLRVERSION}-${BUILD_NUMBER}.sh

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"

say () {
    echo >&2 "$*"
}

die () {
    res=$1
    shift
    echo >&2 "ERROR:$res: $*"
    exit $res
}

export XLRDIR=${XLRDIR:-$PWD}
export XLRVERSION="$(cat $XLRDIR/VERSION | tr -d '\n')"
export BUILD_NUMBER="${BUILD_NUMBER:-1}"

if [ $# -eq 0 ]; then
    set -- amzn1 el7
fi
PLATFORMS="$@"

if [ "$XLRGUIDIR" = "" ]
then
    if [ -e "$XLRDIR/xcalar-gui" ]; then
        export XLRGUIDIR=$XLRDIR/xcalar-gui
    elif [ -e "$XLRDIR/../xcalar-gui" ]; then
        export XLRGUIDIR=$XLRDIR/../xcalar-gui
    fi
fi
if [ ! -e "$XLRGUIDIR" ]
then
    echo "Could not find xcalar-gui at $XLRGUIDIR; set XLRGUIDIR env var" 1>&2
    exit 1
fi

# Build a few things
export PATH=$DIR:$PATH
. osid > /dev/null
VERSTRING="$(_osid --full)"
TMPDIR="${TMPDIR:-/tmp/$(id -un)}/gui-installer/$$"
trap "rm -rf $TMPDIR" EXIT
DESTDIR="${TMPDIR}/rootfs"
rm -rf "$DESTDIR" "$TMPDIR"
mkdir -p "$DESTDIR" "$TMPDIR"

NODE_VERSION="${NODE_VERSION:-0.9.5}"
SSHPASS_VERSION="${SSHPASS_VERSION:-1.06}"
SSHPASS_REVISION="${SSHPASS_REVISION:-6}"
NETCAT_VERSION="${NETCAT_VERSION:-7.50}"
NETCAT_REVISION="${NETCAT_REVISION:-6}"

# Unibuild platform
UNIPLAT="el7"
cd $DESTDIR

mkdir -p opt/xcalar/bin opt/xcalar/installer opt/xcalar/etc/caddy opt/xcalar/var opt/xcalar/config opt/xcalar/tmp

curl -sSL http://repo.xcalar.net/deps/caddy_official.gz | gzip -dc > opt/xcalar/bin/caddy && chmod +x opt/xcalar/bin/caddy
curl -sSL http://repo.xcalar.net/deps/jq_1.5_linux_amd64.gz | gzip -dc > opt/xcalar/bin/jq && chmod +x opt/xcalar/bin/jq

cp -p $XLRDIR/bin/osid opt/xcalar/bin

cp ${XLRDIR}/build/gui/gui-installer-${XLRVERSION}-${BUILD_NUMBER}.tar.gz gui-installer.tar.gz
cp ${XLRDIR}/build/gui/node-modules-${XLRVERSION}-${BUILD_NUMBER}.tar.gz node-modules.tar.gz

curl -sSL http://repo.xcalar.net/deps/sshpass-${SSHPASS_VERSION}-${SSHPASS_REVISION}.${UNIPLAT}.tar.gz -o sshpass.tar.gz
curl -sSL http://repo.xcalar.net/deps/xcalar-netcat-${NETCAT_VERSION}-${NETCAT_REVISION}.${UNIPLAT}.tar.gz -o netcat.tar.gz

grep -v "root" $XLRDIR/pkg/gui-installer/Caddyfile > opt/xcalar/etc/caddy/Caddyfile
sed -i -e 's@http://localhost:12124@http://localhost:12224@g' opt/xcalar/etc/caddy/Caddyfile
cp $XLRDIR/bin/genConfig.sh opt/xcalar/installer/
cp $XLRDIR/src/data/template.cfg opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/nfs-init.sh opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/nfs-test.sh opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/ldap-client-install.sh opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/installer-sh-lib opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/cluster-install-tarball.sh opt/xcalar/installer/cluster-install.sh
cp $XLRDIR/pkg/gui-installer/report-config.py opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/default-admin.py opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/xd-ext-copy.sh opt/xcalar/installer/
cp $XLRDIR/pkg/gui-installer/pip-update.sh opt/xcalar/installer/
cp $XLRDIR/scripts/stringSeed.py opt/xcalar/installer/
ECDSA_PROD_PUB_KEYS=($(ls -r $XLRDIR/src/data/EcdsaPub.key.official.*))
if [ "$IS_RC" = "true" ]; then
    cp "${ECDSA_PROD_PUB_KEYS[0]}" opt/xcalar/installer/EcdsaPub.key
else
    cp $XLRDIR/src/data/EcdsaPub.key opt/xcalar/installer/
fi
# This is a shell script so we can grab from any .tar.gz
tar xzf $(ls $XLRDIR/build/xce/xcalar-${XLRVERSION}-${BUILD_NUMBER}.tar.gz | head -1) opt/xcalar/scripts/SetXceDefaults.sh -O > opt/xcalar/installer/SetXceDefaults.sh
# Using the redirect does not preserve permissions
chmod 755 opt/xcalar/installer/SetXceDefaults.sh

cd $DESTDIR/opt/xcalar

XLRGUI_TAR=`find ${XLRDIR}/build/gui -name "xcalar-gui-${XLRVERSION}-${BUILD_NUMBER}.tar.gz"| head -1`
echo "XLRGUI_TAR is $XLRGUI_TAR"
tar xzf "$XLRGUI_TAR"

cd $DESTDIR/opt/xcalar/var
ln -s ../xcalar-gui www

cd $DESTDIR/opt/xcalar/xcalar-gui
rm -f index.html && ln -s install.html index.html

cd $DESTDIR/opt/xcalar/xcalar-gui/services/expServer
sed -i -Ee "s@12124,@12124,'localhost',@g" expServer.js

cd $DESTDIR/opt/xcalar/installer
for ii in 01-license-check.sh 02-cluster-config.sh 03-deploy-installer.sh 04-xcalar-install.sh 05-nfs-setup.sh 06-nfs-write-test.sh 07-cluster-start.sh cluster-uninstall.sh cluster-upgrade.sh cluster-discover.sh; do ln -sfn cluster-install.sh $ii; done

cd $DESTDIR
cp $XLRDIR/bin/osid opt/xcalar/installer/
cp $XLRDIR/build/xcalar-*-userinstaller opt/xcalar/installer/

cd $DESTDIR/opt/xcalar/xcalar-gui
[ -e install-tarball.html ] && rm -f index.html && ln -s install-tarball.html index.html

cd $XLRDIR
INSTALLER_TAR=${XLRDIR}/build/gui-user-installer-${XLRVERSION}-${BUILD_NUMBER}.tar.gz
fakeroot tar czf "$INSTALLER_TAR" -C "$DESTDIR" .

INSTALLER_NAME="build/xcalar-${XLRVERSION}-${BUILD_NUMBER}-gui-userinstaller"
bin/mkshar.sh "$INSTALLER_TAR" "pkg/gui-installer/user-gui-installer.sh" > "$INSTALLER_NAME"
chmod 755  "$INSTALLER_NAME"
