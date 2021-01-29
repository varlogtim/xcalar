#!/bin/bash
set -e

NAME=node
VERSION=${1:-10.15.1}
VMAJ=$(echo $VERSION | tr '.' '\n' | head -1)
PKGNAME="xcalar-${NAME}"
if [ -n "$2" ]; then
    ITERATION=$2
else
    ITERATION="${BUILD_NUMBER:-2}"
fi

# Binary tarball if it ends with linux-x64.tar.gz
TARBALL=${NAME}-v${VERSION}-linux-x64.tar.xz
URL=https://nodejs.org/dist/v${VERSION}/${TARBALL}
REPLACES=()
if [ $VMAJ -gt 6 ]; then
  REPLACES+=(--conflicts xcalar-node)
fi
if [ $VMAJ -gt 8 ]; then
  REPLACES+=(--conflicts xcalar-node8)
fi

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH
. osid > /dev/null
VERSTRING="$(_osid --full)"

PREFIX=/opt/xcalar

TMPDIR="${TMPDIR:-/tmp/$(id -un)}/${NAME}/$$"
SRCDIR="${TMPDIR}/src"
DESTDIR="${TMPDIR}/rootfs"
rm -rf "$TMPDIR" "$SRCDIR" "$DESTDIR"
mkdir -p "$TMPDIR" "$SRCDIR" "$DESTDIR"


# Prebuilt tar ball of node for generic linux-x64, or source install
# We used to build from source, but that makes that ends up linking
# to distribution/version specific libraries. Prefer to use prebuilt
# in this case to avoid the extra dependencies.
if [[ $URL =~ linux-x64 ]]; then
    test -e $TARBALL || curl -sSL "$URL" -O -f
    mkdir -p ${DESTDIR}/opt/xcalar
    tar Jxf ${TARBALL} --strip-components=1 -C "${DESTDIR}/opt/xcalar"
else
    test -e $TARBALL || curl -sSL "$URL" -O -f
    tar Jxf ${TARBALL} --strip-components=1 -C "$SRCDIR"
    curl -sSL "$URL" | tar Jxf - --strip-components=1 -C "$SRCDIR"
    cd "$SRCDIR"
    export CC='ccache clang'
    export CXX='ccache clang++'
    export CCACHE_BASEDIR=$PWD
    ./configure --prefix=${PREFIX} --partly-static
    make V=0 -j`nproc`
    make install "DESTDIR=$DESTDIR"
    cd -
fi


fakeroot tar czf "${PKGNAME}-${VERSION}-${ITERATION}.tar.gz" -C "$DESTDIR" "${PREFIX#/}"

FPM_FLAGS=(--url 'https://nodejs.org' --description 'As an asynchronous event driven JavaScript runtime, Node is designed to build scalable network applications.' --license 'Node.js' --vendor 'Xcalar, Inc')
FPM_FLAGS+=(-s dir -a native -n ${PKGNAME}${VMAJ} -v ${VERSION} --iteration ${ITERATION} "${REPLACES[@]}" --prefix ${PREFIX} -f -C ${DESTDIR}${PREFIX})

fpm -t deb "${FPM_FLAGS[@]}"
fpm -t rpm "${FPM_FLAGS[@]}"

rm -rf "$SRCDIR" "$TMPDIR"
