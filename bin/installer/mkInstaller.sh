#!/bin/bash
#
#
# # SUMARY:
# Builds the rpm based node installer (non gui)
# INPUT:
# - PLATFORMS
# - SQLDF_VERSION
# - BUILD_NUMBER
# NEEDED:
# - node-modules-${XLRVERSION}-${BUILD_NUMBER}.${plat}.tar.gz
# - xcalar-gui-${XLRVERSION}-${BUILD_NUMBER}.${plat}.tar.gz
# - Caddy
# - Parquet
# - SQLDF
# OUPUT:
# - INSTALLER="$XLRDIR/build/xcalar-$XLRVERSION-$BUILD_NUMBER-installer"
#

set -e

if [ -z "$XLRDIR" ]; then
    echo >&2 "XLRDIR not defined"
    exit 1
fi

if [ -z "$XLRGUIDIR" ] || [ ! -e "$XLRGUIDIR" ]; then
    echo "Could not find xcalar-gui at $XLRGUIDIR; set XLRGUIDIR env var" 1>&2
    exit 1
fi

cd $XLRDIR
export XLRVERSION="$(cat $XLRDIR/VERSION | tr -d '\n')"
source $XLRDIR/pkg/VERSIONS
export SQLDF_VERSION=${SQLDF_VERSION:-1.0}

[ $# -gt 0 ] || set -- el7 amzn2
PLATFORMS="$@"
BUILD_NUMBER="${BUILD_NUMBER:-1}"
BRANCH_NAME="$(git rev-parse --abbrev-ref HEAD)"

PIP_BUNDLER=pip-bundler-${XLRVERSION}-${XCALAR_PYTHON_VERSION}.tar.gz
PIP_BUNDLER_URL="http://repo.xcalar.net/deps/pip-bundler/${PIP_BUNDLER}"

VERSION="$XLRVERSION-$BUILD_NUMBER"
INSTALLER="$XLRDIR/build/xcalar-$VERSION-installer"

if [ -e "$INSTALLER" ]; then
    echo "$INSTALLER already made" 1>&2
    exit 0
fi

cd $XLRDIR
mkdir -p $XLRDIR/build

#sudo chown -R "$USER" $XLRDIR/build
TMPDIR="${TMPDIR:-/tmp/$(id -u)}"
INSTALL_PACKAGE="$TMPDIR"/install_package-$(date +%s)
mkdir -p $INSTALL_PACKAGE

mkdir -p $INSTALL_PACKAGE/gui
cp ${XLRDIR}/build/gui/node-modules-${XLRVERSION}-${BUILD_NUMBER}.tar.gz ${INSTALL_PACKAGE}/gui/node-modules.tar.gz
cp ${XLRDIR}/build/gui/xcalar-gui-${XLRVERSION}-${BUILD_NUMBER}.tar.gz ${INSTALL_PACKAGE}/gui/xcalar-gui.tar.gz

${XLRDIR}/bin/download-caddy.sh ${INSTALL_PACKAGE}/caddy && chmod 755 ${INSTALL_PACKAGE}/caddy

curl -fsSL http://repo.xcalar.net/deps/parquet-tools-1.8.2.jar -o ${INSTALL_PACKAGE}/parquet-tools-1.8.2.jar
if ! curl -fsL "$PIP_BUNDLER_URL" -o ${INSTALL_PACKAGE}/${PIP_BUNDLER}; then
    echo >&2 "No custom pip-bundler package found here: $PIP_BUNDLER_URL"
fi

${XLRDIR}/bin/download-sqldf.sh "$INSTALL_PACKAGE"/xcalar-sqldf.jar
# needs to be 644 so non-root user can read root-owned file
chmod 644 "$INSTALL_PACKAGE"/xcalar-sqldf.jar

cd $XLRDIR
mkdir $INSTALL_PACKAGE/conf
## Hack: XXX: Fixme. We end up with two different copies of python-meld3
#rm -f build/el7/python-meld3-0.*.rpm

# copy main unibuild packages
cp -r build/unibuild/ $INSTALL_PACKAGE/

# copy per plat dependencies
for plat in $PLATFORMS; do
    test -e build/$plat && cp -a build/$plat $INSTALL_PACKAGE/ || echo >&2 "Missing package for $plat - skipping"
done

if [ -z "$SIGN_KEYFILE_PUB" ]; then
    SIGN_KEYFILE_PUB=$XLRDIR/src/data/RPM-GPG-KEY-Xcalar
fi
cp $XLRDIR/src/data/*-GPG-* "$INSTALL_PACKAGE/"
cp $XLRDIR/pkg/requirements.in $INSTALL_PACKAGE/conf/
cp $XLRDIR/pkg/requirements.txt $INSTALL_PACKAGE/conf/
cd $INSTALL_PACKAGE
## Remove dupes
$XLRDIR/bin/dedupe.sh
cat $XLRDIR/bin/installer/installer.sh >"$INSTALLER.tmp"
tar -zcf - --owner=0 --group=0 --ignore-failed-read *-GPG-* MD5SUMS* pip-bundle* parquet-tools-1.8.2.jar caddy bymd5 unibuild $PLATFORMS gui xcalar-sqldf.jar conf >>"$INSTALLER.tmp"
chmod 755 "$INSTALLER.tmp"
mv "$INSTALLER.tmp" "$INSTALLER"
cd -
rm -rf "$INSTALL_PACKAGE"

set +x

echo "Successfully built $INSTALLER"

exit 0
