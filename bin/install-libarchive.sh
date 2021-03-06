#!/bin/bash
NAME=libarchive
VERSION=${VERSION:-3.3.1}
ITERATION="${ITERATION:-6}"
URL=http://www.libarchive.org/downloads/${NAME}-${VERSION}.tar.gz

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH
. osid >/dev/null
VERSTRING="$(_osid --full)"

TMPDIR="${TMPDIR:-/tmp/$(id -un)}/${NAME}/$$"
SRCDIR="$(pwd)/${NAME}-${VERSION}"
DESTDIR="${TMPDIR}/rootfs"
rm -rf "$TMPDIR" "$SRCDIR" "$DESTDIR"
mkdir -p "$TMPDIR" "$SRCDIR" "$DESTDIR"

curl -sSL "$URL" | tar zxf - -C "$SRCDIR/.."

FPM_FLAGS="-n libarchive-static -v ${VERSION} --iteration ${ITERATION} -f -C ${DESTDIR} usr/lib64/libarchive.a usr/lib64/pkgconfig usr/include"

cd "$SRCDIR"
if [[ "$VERSTRING" =~ el6 ]]; then
    extra_args='--program-suffix=3'
fi
./configure --disable-bsdcat --disable-bsdtar --disable-bsdcpio --libdir=/usr/lib64 --prefix=/usr --with-pic --enable-static --enable-silent-rules $extra_args
make V=0 -s -j`nproc`
make install "DESTDIR=$DESTDIR"
cd -
case "$VERSTRING" in
    ub*)
        fpm -s dir -t deb --replaces libarchive-dev -a amd64 $FPM_FLAGS
        ;;
    rhel6|el6)
        fpm -s dir -t rpm --replaces libarchive3-devel -a x86_64 --rpm-dist el6 --rpm-autoreqprov $FPM_FLAGS
        ;;
    rhel7|el7)
        fpm -s dir -t rpm --replaces libarchive-devel  -a x86_64 --rpm-dist el7 --rpm-autoreqprov $FPM_FLAGS
        ;;
    *)
        echo >&2 "Unknown OS ${VERSTRING}"
        exit 1
        ;;
esac
