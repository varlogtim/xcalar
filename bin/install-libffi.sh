#!/bin/bash


NAME=libffi
VERSION="${1}"
export VERSION="${VERSION:-3.2.1}"
URL=https://github.com/$NAME/$NAME/archive/v${VERSION}.tar.gz
ITERATION="${ITERATION:-2}"
TMPDIR="${TMPDIR:-/tmp/$(id -u)}/$(basename $0 .sh)"
SRCDIR="${TMPDIR}/src"
DESTDIR="${TMPDIR}/rootfs"
PREFIX=/usr
INSTALL=${DESTDIR}${PREFIX}

set -e

OSID=$(osid)

case "$OSID" in
  el*) rpm -q texinfo || sudo yum install -y texinfo;;
esac


mkdir -p $TMPDIR
rm -rf $DESTDIR $SRCDIR
mkdir -p $DESTDIR $SRCDIR
curl -fL "$URL" | tar zxvf - --strip-components=1 -C $SRCDIR
cd $SRCDIR
./autogen.sh
./configure --enable-static --disable-share --prefix=${PREFIX} --enable-silent-rules
make -j
make install DESTDIR=$DESTDIR
rm -f ${DESTDIR}/usr/share/info/dir
cd -
FPMFLAGS=(--version $VERSION --iteration $ITERATION -f -C $DESTDIR)
fpm -s dir -t rpm --name ${NAME}-static --replaces ${NAME}-devel ${FPMFLAGS[@]} usr/lib usr/lib64/libffi.a usr/share
fpm -s dir -t rpm --name ${NAME}6 ${FPMFLAGS[@]} usr/lib64/libffi.so.6 usr/lib64/libffi.so.6.0.4
