#!/bin/bash

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH
. osid >/dev/null
VERSTRING="$(_osid --full)"
NAME=ccache
VERSION=${VERSION:-3.3.4}
ITERATION="${ITERATION:-5}"

TMPDIR=${TMPDIR:-/tmp}/`id -un`/fpm
mkdir -p "$TMPDIR"

SRCDIR=/usr/src/${NAME}-${VERSION}
DESTDIR=${TMPDIR}/${NAME}-${VERSION}-install

sudo rm -rf ${SRCDIR} ${DESTDIR}
sudo mkdir -p ${SRCDIR} ${DESTDIR}
sudo chown `id -u`:`id -g` ${SRCDIR} ${DESTDIR}

curl -sSL https://www.samba.org/ftp/$NAME/${NAME}-${VERSION}.tar.bz2 | tar jx -C $SRCDIR/..
cd $SRCDIR
./configure --prefix=/usr --sysconfdir=/etc
make -j`nproc` install DESTDIR=$DESTDIR
cd -
case "$VERSTRING" in
    ub*)
        fpm -s dir -t deb -n ${NAME} -v ${VERSION} --iteration ${ITERATION} --url "http://ccache.samba.org" --description 'Compiler cache for fast recompilation of C/C++' -a amd64 -d libc6 -d zlib1g -f -C ${DESTDIR} usr
        ;;
    rhel6|el6)
        fpm -s dir -t rpm --rpm-dist el6 -n ${NAME} -v ${VERSION} --iteration ${ITERATION} --url "http://ccache.samba.org" --description 'Compiler cache for fast recompilation of C/C++' -d zlib -f -C ${DESTDIR} usr
        ;;
    rhel7|el7)
        fpm -s dir -t rpm --rpm-dist el7 -n ${NAME} -v ${VERSION} --iteration ${ITERATION} --url "http://ccache.samba.org" --description 'Compiler cache for fast recompilation of C/C++' -d zlib -f -C ${DESTDIR} usr
        ;;
    amzn*)
        fpm -s dir -t rpm --rpm-dist ${VERSTRING} -n ${NAME} -v ${VERSION} --iteration ${ITERATION} --url "http://ccache.samba.org" --description 'Compiler cache for fast recompilation of C/C++' -d zlib -f -C ${DESTDIR} usr
        ;;
    *)
        echo >&2 "Unknown OS ${VERSTRING}"
        exit 1
        ;;
esac
