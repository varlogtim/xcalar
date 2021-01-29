#!/bin/bash
#
# SUMMARY
# Builds distro dependent packages such as node and openldap that are used in gui installs
# Originally part of build-gui-installer.sh and broken off, it does things such as unpacking stuff that's used
# back in build-gui-installer.sh
# INPUT:
# - path to xcalar code
# - XCALAR code version
# - BUILD Number
# - xcalar node install and modules (http://repo.xcalar.net/deps/xcalar-node-10.15.1-2.tar.gz)
# - openldap install (http://repo.xcalar.net/deps/xcalar-openldap-2.4.44-6.el7.tar.gz)
# OUPUT:
# - build/gui/xcalar-gui-${XLRVERSION}-${BUILD_NUMBER}.tar.gz
#     (renamed from xcalar-gui.tar.gz, created by grunt, contains make installer of XD and XI)
# - build/gui/node-modules-${XLRVERSION}-${BUILD_NUMBER}.tar.gz
#     (renamed from node-modules.tar.gz, created by grunt, contains xcalar-gui/services/expServer and node_modules )
# - build/gui/gui-installer-${XLRVERSION}-${BUILD_NUMBER}.tar.gz
#     (contains the xcalar-node-* (node itself and some basic modules) and the ldap + licenseCheck utils per untarring below)

set -e

NODEJS_VERSION="10.15.1"
NODEJS_RELEASE="2"
OPENLDAP_VERSION="2.4.44"
OPENLDAP_RELEASE="6"

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

# we need the el6 g++ compiler for npm module build
if ls /etc/profile.d/scl-* &>/dev/null; then
    for prof in /etc/profile.d/scl-*; do
        . $prof
    done
fi

export XLRDIR=${XLRDIR:-$PWD}
export XLRVERSION="$(cat $XLRDIR/VERSION | tr -d '\n')"
export BUILD_NUMBER="${BUILD_NUMBER:-1}"
export BUILD_TYPE=${BUILD_TYPE:-prod}
if [ -z "$XLRDIR" ] || [ -z "$XLRVERSION" ] || [ -z "$BUILD_NUMBER" ]; then
    die 1 "Usage: $0 <xlrdir> <xlrversion> <build number>"
fi

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
    die 1 "Could not find xcalar-gui at $XLRGUIDIR; set XLRGUIDIR env var" 1>&2
fi

# Build a few things
export PATH=$DIR:$PATH
. osid > /dev/null
VERSTRING="$(_osid --full)"

## This stuff is not used here but picked up and copied in build-gui-installer.sh
TMPDIR="${TMPDIR:-/tmp/$(id -un)}/gui-installer/$$"
trap "rm -rf $TMPDIR" EXIT

DESTDIR="${TMPDIR}/rootfs"
rm -rf "$DESTDIR" "$TMPDIR"
mkdir -p "$DESTDIR" "$TMPDIR"

NODEJS_TAR="xcalar-node-${NODEJS_VERSION}-${NODEJS_RELEASE}.tar.gz"
if ! curl -fsSL http://repo.xcalar.net/deps/${NODEJS_TAR} -o ./${NODEJS_TAR} ; then die 1 "Unable to locate nodejs tar file" ; fi

OPENLDAP_TAR="xcalar-openldap-${OPENLDAP_VERSION}-${OPENLDAP_RELEASE}.${OSID_NAME}${OSID_VERSION}.tar.gz"
if ! curl -fsSL http://repo.xcalar.net/deps/${OPENLDAP_TAR} -o ./${OPENLDAP_TAR} ; then die 1 "Unable to locate openldap tar file" ; fi

cd "$DESTDIR"
tar xzf $XLRDIR/${NODEJS_TAR}
tar xzf $XLRDIR/${OPENLDAP_TAR} opt/xcalar/bin/ldapwhoami

mkdir -p opt/xcalar/installer
tar xzf $XLRDIR/build/xce/xcalar-${XLRVERSION}-${BUILD_NUMBER}.tar.gz opt/xcalar/bin/licenseCheck
mv opt/xcalar/bin/licenseCheck opt/xcalar/installer
cd -

rm -f ${NODEJS_TAR} ${OPENLDAP_TAR}
####

mkdir -p ${XLRDIR}/build/gui

cd $XLRDIR/pkg/gui-installer
PATH=$DESTDIR/opt/xcalar/bin:/opt/xcalar/bin:/usr/local/bin:$PATH
rm -f xcalar-gui.tar.gz
MAKELOG=$XLRDIR/build-gui-installer-os.log
source $XLRDIR/doc/env/xc_aliases && xcEnvEnter
case "$BUILD_TYPE" in
    debug) GUI_BUILD_TARGET=debug;;
    prod)  GUI_BUILD_TARGET=installer;;
    *) die 1 "Unknown build type $BUILD_TYPE";;
esac

echo >&2 "Running: make GUI_BUILD_TARGET=$GUI_BUILD_TARGET gui-build"

# disable npm/grunt color output if not running from terminal
[ -t 1 ] || export NPM_GRUNT_COLOR='--no-color'

make GUI_BUILD_TARGET=$GUI_BUILD_TARGET gui-build 2>&1 | tee $MAKELOG

test ${PIPESTATUS[0]} -eq 0 ||  die 1 "*** Failed to run make GUI_BUILD_TARGET=$GUI_BUILD_TARGET gui-build (see $MAKELOG)"

xcEnvLeave
XLRGUI_TAR=${XLRDIR}/build/gui/xcalar-gui-${XLRVERSION}-${BUILD_NUMBER}.tar.gz
NPMMOD_TAR=${XLRDIR}/build/gui/node-modules-${XLRVERSION}-${BUILD_NUMBER}.tar.gz
mv xcalar-gui.tar.gz ${XLRGUI_TAR}
mv node-modules.tar.gz ${NPMMOD_TAR}
cd -

INSTALLER_TAR=${XLRDIR}/build/gui/gui-installer-${XLRVERSION}-${BUILD_NUMBER}.tar.gz

fakeroot tar czf "$INSTALLER_TAR" -C "$DESTDIR" .
