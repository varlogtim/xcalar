#!/bin/bash
#
# # SUMARY:
# Builds tar based xcalar installer that can be installed as non-root except for some pre-script.sh work that requires root
# OUPUT:
# - shar (self-extracting bash archive) of xcalar-${XLRVERSION}-${BUILD_NUMBER}-userinstaller

set -e

NODEJS_VERSION="10.15.1"
NODEJS_RELEASE="2"

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

export XLRVERSION="$(cat $XLRDIR/VERSION | tr -d '\n')"
export XLRDIR=${XLRDIR:-$PWD}
export SQLDF_VERSION=${SQLDF_VERSION:-1.0}
export BUILD_NUMBER="${BUILD_NUMBER:-1}"
source $XLRDIR/pkg/VERSIONS
PIP_BUNDLER=pip-bundler-${XLRVERSION}-${XCALAR_PYTHON_VERSION}.tar.gz
PIP_BUNDLER_URL="http://repo.xcalar.net/deps/pip-bundler/${PIP_BUNDLER}"

if [ $# -eq 0 ]; then
    set -- el7
fi
PLATFORMS="$@"

TMPDIR="${TMPDIR:-/tmp/$(id -un)}/user-installer/$$"
trap "rm -rf $TMPDIR" EXIT
DESTDIR="${TMPDIR}/rootfs"
rm -rf "$DESTDIR" "$TMPDIR"
mkdir -p "$DESTDIR" "$TMPDIR"
export PATH=$DIR:$PATH
INSTALLER_SCRIPT="$XLRDIR/bin/installer/user-installer.sh"

START_CWD="$(pwd)"
cd $DESTDIR

curl -sSL http://repo.xcalar.net/deps/parquet-tools-1.8.2.jar -o "$TMPDIR"/parquet-tools-1.8.2.jar
${XLRDIR}/bin/download-sqldf.sh "$TMPDIR"/xcalar-sqldf.jar


INSTALL_TAR="$XLRDIR/build/xce/xcalar-${XLRVERSION}-${BUILD_NUMBER}.tar.gz"
mkdir installer config

tar xzf ${INSTALL_TAR} -C installer
for pkg in xcalar-python36 xcalar-node10 xcalar-antlr xcalar-arrow-libs xcalar-caddy xcalar-jre8; do
    tar xzf ${XLRDIR}/build/xce/${pkg}.tar.gz -C installer
done
PYTHON=$(pwd)/installer/opt/xcalar/bin/python3
mkdir -p "$TMPDIR"/pip
if curl -fsSL "$PIP_BUNDLER_URL" | tar zxf - -C "$TMPDIR"/pip; then
    (cd "$TMPDIR"/pip && unset PS4 && set +x && bash install.sh --python $PYTHON)
fi
rm -rf "$TMPDIR"/pip
tar xzf ${XLRDIR}/build/gui/xcalar-gui-${XLRVERSION}-${BUILD_NUMBER}.tar.gz -C installer/opt/xcalar
tar xzf ${XLRDIR}/build/gui/node-modules-${XLRVERSION}-${BUILD_NUMBER}.tar.gz -C installer/opt/xcalar/xcalar-gui/services/expServer
cp "$TMPDIR"/xcalar-sqldf.jar installer/opt/xcalar/lib/xcalar-sqldf.jar
chmod 644 installer/opt/xcalar/lib/xcalar-sqldf.jar
mkdir -p installer/opt/xcalar/test_data

# we don't really to install the old defaults file anymore
rm installer/etc/default/xcalar

# HACK we want to ship cgroupControllerUtil.sh owned by root in
# $INSTALL_DIR/bin and not $XLRDIR so removing it from the user installer tar
rm installer/opt/xcalar/bin/cgroupControllerUtil.sh

echo "xcalar-${XLRVERSION}-${BUILD_NUMBER}" > installer/etc/xcalar/VERSION

cp "$TMPDIR"/parquet-tools-1.8.2.jar installer/opt/xcalar/bin/parquet-tools-1.8.2.jar

cd $DESTDIR
mkdir -p installer/var/www
cd installer/var/www
ln -sfn ../../opt/xcalar/xcalar-gui xcalar-gui
ln -sfn ../../opt/xcalar/xcalar-gui xcalar-design
chown "$XCE_USER:$XCE_GROUP" xcalar-gui xcalar-design

cd $DESTDIR
fakeroot tar czf user-installer.tar.gz -C installer .

cp ${XLRDIR}/bin/osid config
cp ${XLRDIR}/bin/installer/pre-config.sh config
cp ${XLRDIR}/bin/cgconfig-setup.sh config
cp ${XLRDIR}/bin/installer/verify.sh config
cp ${XLRDIR}/scripts/system-info.sh config
cp ${XLRDIR}/conf/*.service config
cp ${XLRDIR}/conf/xcalar.service config
cp ${XLRDIR}/conf/xcalar.slice config
cp ${XLRDIR}/conf/xcalar-rsyslog.conf config
tar xzf "${INSTALL_TAR}" etc/security/limits.d/90-xclimits.conf -O > config/90-xclimits.conf
tar xzf "${INSTALL_TAR}" etc/sysctl.d/90-xcsysctl.conf -O > config/90-xcsysctl.conf
tar xzf "${INSTALL_TAR}" etc/logrotate.d/xclogrotate -O > config/xclogrotate
tar xzf "${INSTALL_TAR}" etc/cron.d/xcalar -O > config/xcalar.cron
tar xzf "${INSTALL_TAR}" etc/ld.so.conf.d/xcalar.conf -O > config/xcalar.ld.so.conf
tar xzf "${INSTALL_TAR}" etc/sudoers.d/xcalar -O > config/xcalar-sudo.conf
tar xzf "${INSTALL_TAR}" opt/xcalar/bin/cgroupControllerUtil.sh -O > config/cgroupControllerUtil.sh


# there's probably a bug if we have more platforms than one(el7) because the installer script
# may not know how to make a difference and account for shared packages in unibuild and more per arch.
for distro in $PLATFORMS; do
    case "${distro}" in
        el7)
            cp ${XLRDIR}/build/${distro}/*.rpm config

            XCALAR_RPM_DEPS=""
            if XCALAR_RPM_FILES="$(ls -1 config/*.rpm 2>/dev/null)"; then
                for file in $XCALAR_RPM_FILES; do
                    rpm_name="$(rpm -qip $file 2>/dev/null | grep Name | cut -c 15-)"
                    XCALAR_RPM_DEPS="$rpm_name $XCALAR_RPM_DEPS"
                done
            fi

            echo "$XCALAR_RPM_DEPS" > ${XLRDIR}/build/${distro}-config/xcalar-rpm-deps.${distro}.txt
            SYSTEM_PKG_DEPS="$(cat ${XLRDIR}/build/${distro}-config/system-rpm-deps.${distro}.txt)"
            KERBEROS_PKG_DEPS="$(cat ${XLRDIR}/build/${distro}-config/krb-rpm-deps.${distro}.txt)"
            JAVA_PKG_DEPS="$(cat ${XLRDIR}/build/${distro}-config/java-rpm-deps.${distro}.txt)"
            XCALAR_PKG_DEPS="$XCALAR_RPM_DEPS"
            ;;
    esac

    XCE_LIMIT_COUNT="$(jq '.count' ${XLRDIR}/conf/xcalar-limits.json | tr '\n' ' ' | tr '\r' ' ')"
    XCE_LIMIT_FLAGS="$(jq '.data[].flag' ${XLRDIR}/conf/xcalar-limits.json | tr '\n' ' ' | tr '\r' ' ')"
    XCE_LIMIT_VALUES="$(jq '.data[].soft' ${XLRDIR}/conf/xcalar-limits.json | tr '\n' ' ' | tr '\r' ' ')"
    XCE_SYSCTL_COUNT="$(jq '.count' ${XLRDIR}/conf/xcalar-sysctl.json | tr '\n' ' ' | tr '\r' ' ')"
    XCE_SYSCTL_TOKENS="$(jq '.data[].token' ${XLRDIR}/conf/xcalar-sysctl.json | tr '\n' ' ' | tr '\r' ' ')"
    XCE_SYSCTL_VALUES="$(jq '.data[].value' ${XLRDIR}/conf/xcalar-sysctl.json | tr '\n' ' ' | tr '\r' ' ')"

    sed -i -e "s/%JAVA_PACKAGES%/\"${JAVA_PKG_DEPS}\"/g" config/pre-config.sh
    sed -i -e "s/%KRB_PACKAGES%/\"${KERBEROS_PKG_DEPS}\"/g" config/pre-config.sh
    sed -i -e "s/%PACKAGES%/\"${SYSTEM_PKG_DEPS}\"/g" config/pre-config.sh
    sed -i -e "s/%JAVA_PACKAGES%/\"${JAVA_PKG_DEPS}\"/g" config/verify.sh
    sed -i -e "s/%KRB_PACKAGES%/\"${KERBEROS_PKG_DEPS}\"/g" config/verify.sh
    sed -i -e "s/%SYSTEM_PACKAGES%/\"${SYSTEM_PKG_DEPS}\"/g" config/verify.sh
    sed -i -e "s/%XCALAR_PACKAGES%/\"${XCALAR_PKG_DEPS}\"/g" config/verify.sh
    sed -i -e "s/%XCE_LIMIT_COUNT%/$XCE_LIMIT_COUNT/g" config/verify.sh
    sed -i -e "s/%XCE_LIMIT_FLAGS%/$XCE_LIMIT_FLAGS/g" config/verify.sh
    sed -i -e "s/%XCE_LIMIT_VALUES%/$XCE_LIMIT_VALUES/g" config/verify.sh
    sed -i -e "s/%XCE_SYSCTL_COUNT%/$XCE_SYSCTL_COUNT/g" config/verify.sh
    sed -i -e "s/%XCE_SYSCTL_TOKENS%/$XCE_SYSCTL_TOKENS/g" config/verify.sh
    sed -i -e "s/%XCE_SYSCTL_VALUES%/$XCE_SYSCTL_VALUES/g" config/verify.sh

    tar czf configuration_tarball-${XLRVERSION}-${BUILD_NUMBER}.${distro}.tar.gz config
    cp configuration_tarball-${XLRVERSION}-${BUILD_NUMBER}.${distro}.tar.gz ${XLRDIR}/build/${distro}-config
done

curl -fsSL http://repo.xcalar.net/deps/xcalar-testdata/xcalar-testdata-${XCALAR_TESTDATA_VERSION}.tar.gz -o test-data.tar.gz
cp ${XLRDIR}/bin/osid .
tar czf user-installer-bundle.tar.gz --owner=0 --group=0 user-installer.tar.gz configuration_tarball-*.tar.gz test-data.tar.gz osid

INSTALLER=${XLRDIR}/build/xcalar-${XLRVERSION}-${BUILD_NUMBER}-userinstaller
$XLRDIR/bin/mkshar.sh user-installer-bundle.tar.gz "$INSTALLER_SCRIPT" > "$INSTALLER"
chmod 775 "$INSTALLER"

rm -rf bin user-installer*.tar.gz test_data test-data.tar.gz

cd $START_CWD
