#!/bin/bash
NAME=xtables-addons
VERSION=${VERSION:-2.10}
ITERATION="${ITERATION:-1}"
URL=http://downloads.sourceforge.net/project/xtables-addons/Xtables-addons/xtables-addons-${VERSION}.tar.xz

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH
. osid >/dev/null
VERSTRING="$(_osid --full)"

if [[ "$VERSTRING" =~ ^ub ]]; then
    echo >&2 "ERROR: This module only compiles for EL systems"
    exit 1
fi

TMPDIR="${TMPDIR:-/tmp/$(id -un)}/${NAME}/$$"
SRCDIR="$(pwd)/${NAME}-${VERSION}"
DESTDIR="${TMPDIR}/rootfs"
rm -rf "$TMPDIR" "$SRCDIR" "$DESTDIR"
mkdir -p "$TMPDIR" "$SRCDIR" "$DESTDIR"

curl -sSL "$URL" | tar Jxf - -C "$SRCDIR/.."

FPM_FLAGS="-n xtables-addons -v ${VERSION} --iteration ${ITERATION} -f -C ${DESTDIR} usr"

cd "$SRCDIR"
if [[ "$VERSTRING" =~ el6 ]]; then
    extra_args='--program-suffix=3'
fi
if [[ $VERSTRING =~ ^el ]]; then
	sudo yum install -y gcc-c++ make automake kernel-devel-`uname -r` wget unzip iptables-devel perl-Text-CSV_XS
fi
./configure #--disable-bsdcat --disable-bsdtar --disable-bsdcpio --libdir=/usr/lib64 --prefix=/usr --with-pic --enable-static --enable-silent-rules $extra_args
make V=0 -s -j`nproc`
make install "DESTDIR=$DESTDIR"
cd -
case "$VERSTRING" in
    rhel6|el6)
        fpm -s dir -t rpm -a x86_64 --rpm-dist el6 --rpm-autoreqprov $FPM_FLAGS
        ;;
    rhel7|el7)
        fpm -s dir -t rpm -a x86_64 --rpm-dist el7 --rpm-autoreqprov $FPM_FLAGS
        ;;
    *)
        echo >&2 "Unknown OS ${VERSTRING}"
        exit 1
        ;;
esac
