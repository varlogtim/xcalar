#!/bin/bash
set -e

NAME=dnsmasq
PKGNAME=dnsmasq
VERSION=2.78
TMPDIR="${TMPDIR:-/tmp}/$(id -u)/$NAME"
SRCDIR=$TMPDIR/dnsmasq-${VERSION}
PREFIX=/usr
DESTDIR=$TMPDIR/rootfs
CURDIR=$(pwd)

rm -rf $TMPDIR
mkdir -p $TMPDIR

git clone git://thekelleys.org.uk/dnsmasq.git  $SRCDIR
cd $SRCDIR
git checkout -f v${VERSION}

if command -v apt-get >/dev/null; then
    # dpkg-source needs the source archive as a .tar.gz
    # in the parent dir of the source
    git archive --format=tar HEAD -- . | gzip > ../${NAME}-${VERSION}.tar.gz
    sudo apt-get install -y libidn11-dev libnetfilter-conntrack-dev libdbus-1-dev libidn-dev build-essentials
    debian/rules clean
    dpkg-source -b .
    debian/rules build
    fakeroot debian/rules binary
    cd -
    cp $TMPDIR/dnsmasq*.deb .
else
    make PREFIX=$PREFIX -j
    make PREFIX=$PREFIX DESTDIR=$DESTDIR install
    ELVERSION=$(rpm -q $(rpm -qf /etc/redhat-release) --qf '%{VERSION}\n')
    ELV="${ELVERSION:0:1}"
    cd -
    fpm -s dir -t rpm -v ${VERSION} --iteration ${BUILD_NUMBER:-1} --name ${PKGNAME} --rpm-dist el${ELV} --rpm-autoreqprov -f -C $DESTDIR
fi
