#!/bin/bash
#
# SUMMARY:
# Downloads all dependencies for the specified xcalar rpm
# INPUT:
# - XLRVERSION
# OUPUT:
# - build/xcalar-dependencies-<DISTRO>.txt
# - build/<DISTRO>-config/system-rpm-deps.<DISTRO>.txt
# - build/<DISTRO>-config/krb-rpm-deps.<DISTRO>.txt
# - build/<DISTRO>-config/java-rpm-deps.<DISTRO>.txt
# - all pkgs xcalar depends on downloaded to ${ELV}.tar and extracted into build/
# - the tar version of the rpm pkgs for xcalar-python36

set -e

say () {
    echo >&2 "$*"
}

export BUILD_NUMBER="${BUILD_NUMBER:-1}"
PREFIX=${PREFIX:-/opt/xcalar}

MYXLRDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ "$MYXLRDIR" != "$XLRDIR" ]; then
    echo >&2 "WARNING: Mismatched XLRDIR=$XLRDIR, but this script is under $MYXLRDIR"
    echo >&2 "WARNING: Setting XLRDIR=$MYXLRDIR"
    export XLRDIR="$MYXLRDIR"
fi

MY_XCE_LICENSEDIR="$MYXLRDIR/src/data"
if [ "$MY_XCE_LICENSEDIR" != "$XCE_LICENSEDIR" ]; then
    echo >&2 "WARNING: Mismatched license paths - XCE_LICENSEDIR=$XCE_LICENSEDIR but testing will look in $MY_XCE_LICENSEDIR"
    echo >&2 "WARNING: setting XCE_LICENSEDIR=$MY_XCE_LICENSEDIR"
    export XCE_LICENSEDIR=$MY_XCE_LICENSEDIR
fi
# Make sure we're talking to the right tools, you'd be surprised how often I see this
# warning now. When modifying the PATH, one should clear the builtin PATH cache. The
# shell maintains a hash of command => path values to optimize command lookup.
# 'help hash' for more info.
export PATH="$XLRDIR/bin:$PATH"
hash -r
export XLRVERSION="$(tr -d '\n' < $XLRDIR/VERSION)"

VERSIONID="$XLRVERSION-$BUILD_NUMBER"

mkdir -p $XLRDIR/build

### UNIBUILD ###
UNIPLAT="el7" #platform used to build unibuild stuff on
UNIDIR="unibuild"
source $XLRDIR/pkg/VERSIONS
mkdir -p build/${UNIDIR}
RPMSUFFIX="${VERSIONID}.$(uname -p).rpm"
# build core rpm only once on el7. -plaform rpm built below per platform
crun ${UNIPLAT}-build bin/build-rpm.sh
# COPY_DIST="true" copies xcalar-python36 from the /dist directory in the container
CONTAINER_USER=root CONTAINER_UID=0 crun -e COPY_DIST="true" ${UNIPLAT}-offline /yum-download.sh build/xcalar-${RPMSUFFIX}
tar xif ${UNIPLAT}.tar -C build/
rm -f ${UNIPLAT}.tar
# build the gui bits (nodejs modules and xcalar-gui)
crun ${UNIPLAT}-build bin/build-gui-installer-os.sh
crun ${UNIPLAT}-build yumdownloader --enablerepo='xcalar-deps-common' xcalar-testdata-${XCALAR_TESTDATA_VERSION}
mv build/xcalar-${RPMSUFFIX} xcalar-testdata-${XCALAR_TESTDATA_VERSION}*.rpm build/${UNIPLAT}/* build/${UNIDIR}/

cd build/xce
for pkg in xcalar-python36 xcalar-node10 xcalar-antlr xcalar-arrow-libs xcalar-caddy xcalar-jre8; do
    PKG_RPM="$(find ../unibuild -name ${pkg}\*.rpm -print)"
    # Repackage as tar.gz with files owned by root/root
    pkg2tar "$PKG_RPM" "${pkg}.tar.gz"
    if [[ $pkg =~ xcalar-python36 ]]; then
        reprefix.sh -i ${pkg}.tar.gz -o ${pkg}.tar.gz --prefix ${PREFIX} --newprefix ${NEWPREFIX:-$PREFIX$PREFIX}
    fi
done
cd -

#### PER PLATFORM DEPENDENCIES PACKAGES ####
# by default build for all supported platforms
# SUPPORTED PLATS should be defined some place global, not here
test $# -eq 0 && set -- amzn2 el7

for ELV in "$@"; do
    SUFFIX="platform-${VERSIONID}.${ELV}"
    RPMSUFFIX="${SUFFIX}.$(uname -p).rpm"
    rm -rf build/xcalar-platform-* build/${ELV} build/xcalar-debuginfo-* build/xcalar-offline-*
    mkdir -p build/${ELV} build/${ELV}-config
    crun ${ELV}-offline bin/build-rpm.sh --xplat
    CONTAINER_USER=root CONTAINER_UID=0 crun ${ELV}-offline /yum-download.sh build/xcalar-${RPMSUFFIX}
    res=$?
    if [ $res != 0 ]; then
        say "***************************************************"
        say "****** FAILED TO BUILD $ELV (xcalar-$SUFFIX) with code ($res)"
        say "****** Check output above for more information"
        say "***************************************************"
        exit $res
    fi
    tar xif ${ELV}.tar -C build/ && \
    mv build/xcalar-${RPMSUFFIX} build/${ELV}

    ## Don't distribute the debug info. Until GA we will do POCs with
    ## unstripped elfs.
    #cp build/xcalar-debuginfo-${RPMSUFFIX} build/${ELV}
    rm -f ${ELV}.tar
done
