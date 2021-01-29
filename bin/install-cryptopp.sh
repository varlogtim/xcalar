#!/bin/bash
#
# To compile with clang and libc++ use
# CXX=clang++-3.9 CXXFLAGS=-stdlib=libc++

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH
. osid >/dev/null
VERSTRING="$(_osid --full)"
NAME=cryptopp
VERSION=${VERSION:-5.6.4}
ITERATION="${ITERATION:-7}"

TMPDIR=${TMPDIR:-/tmp}/`id -un`/fpm
mkdir -p "$TMPDIR"

SRCDIR="${SRCDIR:-$(pwd)/${NAME}-${VERSION}}"
DESTDIR=${TMPDIR}/${NAME}-${VERSION}-install

rm -rf ${TMPDIR} ${SRCDIR} ${DESTDIR}
mkdir -p ${TMPDIR} ${SRCDIR} ${DESTDIR}

curl -sSL http://repo.xcalar.net/deps/${NAME}-${VERSION}.tar.gz | tar zxf - -C $SRCDIR/..
cd $SRCDIR
sed -i 's/march=native/march=corei7/g' GNUmakefile
make -j
make -j DESTDIR=$DESTDIR PREFIX=/usr install
make distclean
rm -f libcryptopp.a
hash -r
make -j`nproc` CXX=clang++ CC=clang CXXFLAGS=-stdlib=libstdc++ static
cp libcryptopp.a $DESTDIR/usr/lib/libcryptopp_clang.a
cd -
FPM_FLAGS="-n ${NAME}-static -v ${VERSION} --iteration ${ITERATION} -f --url https://www.cryptopp.com -C ${DESTDIR} usr/lib/libcryptopp_clang.a usr/lib/libcryptopp.a usr/include"
case "$VERSTRING" in
    ub*)
        fpm -s dir -t deb --replaces libcrypto++-dev $FPM_FLAGS
        ;;
    rhel6|el6)
        fpm -s dir -t rpm --replaces cryptopp-devel --rpm-dist el6 --rpm-autoreqprov $FPM_FLAGS
        ;;
    rhel7|el7)
        fpm -s dir -t rpm --replaces cryptopp-devel --rpm-dist el7 --rpm-autoreqprov $FPM_FLAGS
        ;;
    amzn*)
        fpm -s dir -t rpm --replaces cryptopp-devel --rpm-dist $VERSTRING --rpm-autoreqprov $FPM_FLAGS
        ;;
    *)
        echo >&2 "Unknown OS ${VERSTRING}"
        exit 1
        ;;
esac
