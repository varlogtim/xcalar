#!/bin/bash
#
# SUMMARY:
# builds the xcalar rpm using rpmbuild. If called with -c builds the core pkg \
# (default) and if with -p the -plaform dependent pkg. The platform is automatically
# selected based on the host running the script (generally a container)
# INPUT:
# - -p : builds the platform dependent pkg
# - -t : builds test_data pkg
# - -a : builds test data and platform dep pkgs
# OUPUT:
# - rpm file of build dir build/xcalar-<VERSION>.<ARCH>.rpm
# - tar file of build dir build/xce/xcalar-<VERSION>.<ARCH>.rpm
# - rpm file of platform dependent package dependencies build/xcalar-platform-<VERSION>.<PLAT>.<ARCH>.rpm
# - rpm file of test_data build/xcalar-testdata-<VERSION>.<ARCH>.rpm

set -e

usage() {
cat << EOF_USAGE >&2
Usage: $0 [-h -a --xplat --test-data]
By default builds the core platform package with the xcalar code
If --xplat, -a or --test-data are specified it builds those packages ONLY.

-h show usage"
-a builds all packages"
--test-data builds test_data package"
--xplat builds platform dependent package"
EOF_USAGE
}

while [ $# -gt 0 ]; do
    opt="$1"
    case "$opt" in
        -h|--help)
            usage
            exit 0
            ;;
        -a)
            BUILD_PLATFORM=1
            BUILD_TESTDATA=1
            shift
            ;;
        --xplat)
            BUILD_PLATFORM=1
            shift
            ;;
        --test-data)
            BUILD_TESTDATA=1
            shift
            ;;
        *)
            usage
            exit 2
            ;;
    esac
done

if ls /etc/profile.d/scl-* &>/dev/null; then
    for prof in /etc/profile.d/scl-*; do
        . $prof
    done
fi

export XLRVERSION="$(cat $XLRDIR/VERSION | tr -d '\n')"
export BUILD_NUMBER="${BUILD_NUMBER:-1}"
export BUILD_TYPE="${BUILD_TYPE:-debug}"
export COVERAGE=${COVERAGE:-OFF}

# EL=Enterprise Linux (aka CentOS/RedHat)
# ELVERSION=Major version (eg, 6 or 7 currently)
DIST="$(osid)"
rm -rf ~/rpmbuild/BUILD ~/rpmbuild/BUILDROOT
mkdir -p ~/rpmbuild/SOURCES ~/rpmbuild/BUILD ~/rpmbuild/BUILDROOT ~/rpmbuild/RPMS
mkdir -p $XLRDIR/build
cp $XLRDIR/pkg/rpm/rpmmacros ~/.rpmmacros

set +e

if [ -z "$BUILD_PLATFORM" -a -z "$BUILD_TESTDATA" ]; then
    $XLRDIR/bin/git-archiver --prefix=xcalar-$XLRVERSION/ HEAD > ~/rpmbuild/SOURCES/xcalar-$XLRVERSION.tar && \
    GIT_DIR="$XLRDIR/.git" rpmbuild -bb $XLRDIR/pkg/rpm/xcalar.spec \
        --define "_build_number $BUILD_NUMBER" \
        --define "_version $XLRVERSION" \
        --define "_xlrdir $XLRDIR"
    rc=$?
else
    if [ -n "$BUILD_PLATFORM" ]; then
        rpmbuild -bb $XLRDIR/pkg/rpm/xcalar-platform.spec \
            --define "_build_number $BUILD_NUMBER" \
            --define "_version $XLRVERSION" \
            --define "_xlrdir $XLRDIR" \
            --define "dist .${DIST}"
        rc=$?
    fi
    if [ -n "$BUILD_TESTDATA" ]; then
        rpmbuild -bb $XLRDIR/pkg/rpm/xcalar-testdata.spec \
            --define "_build_number $BUILD_NUMBER" \
            --define "_version $XLRVERSION" \
            --define "_xlrdir $XLRDIR"
        rc=$?
    fi

fi
if [ $rc -ne 0 ]; then
    echo >&2 "ERROR($rc): $0 failed. spec file is in /tmp/xcalar-$$.spec"
    exit $rc
fi

set -e
mv ~/rpmbuild/RPMS/x86_64/xcalar-*.x86_64.rpm $XLRDIR/build/
