#!/bin/bash
NAME=openldap
VERSION=${VERSION:-2.4.44}
ITERATION="${ITERATION:-6}"
URL=ftp://ftp.openldap.org/pub/OpenLDAP/openldap-release/openldap-${VERSION}.tgz

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH
. osid > /dev/null
VERSTRING="$(_osid --full)"

PREFIX=/opt/xcalar

TMPDIR="${TMPDIR:-/tmp/$(id -un)}/${NAME}/$$"
SRCDIR="$(pwd)/${NAME}-${VERSION}"
DESTDIR="${TMPDIR}/rootfs"
rm -rf "$TMPDIR" "$SRCDIR" "$DESTDIR"
mkdir -p "$TMPDIR" "$SRCDIR" "$DESTDIR"

curl -sSL "$URL" | tar zxf - -C "$SRCDIR/.."

cd "$SRCDIR"
./configure --prefix=${PREFIX} --enable-static=yes --enable-dynamic=no
make depend
make
make install "DESTDIR=$DESTDIR"
cd -

PKGNAME="xcalar-${NAME}"
FPM_FLAGS="-n ${PKGNAME} -v ${VERSION} --iteration ${ITERATION} -f -C ${DESTDIR} ${PREFIX#/}"
tar czf "${PKGNAME}-${VERSION}-${ITERATION}.${OSID_NAME}${OSID_VERSION}.tar.gz" -C "$DESTDIR" "${PREFIX#/}"

if test -x /usr/local/bin/fpm; then
    case "$VERSTRING" in
        ub14)
            fpm -s dir -t deb -a amd64 $FPM_FLAGS
            ;;
        el6|rhel6)
            fpm -s dir -t rpm -a x86_64 --rpm-dist el6 --rpm-autoreqprov $FPM_FLAGS
            ;;
        el7|rhel7)
            fpm -s dir -t rpm -a x86_64 --rpm-dist el7 --rpm-autoreqprov $FPM_FLAGS
            ;;
        *)
            echo >&2 "Unknown platform $OSID_NAME $OSID_VERSION"
            exit 2
            ;;
    esac
fi

rm -rf "$SRCDIR"
