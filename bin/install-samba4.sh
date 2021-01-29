#!/bin/bash

set -e

NAME=samba
VERSION=4.6.5
ITERATION=2
URL=https://download.samba.org/pub/samba/stable/samba-${VERSION}.tar.gz
DESTDIR=/tmp/`id -un`/${NAME}-${VERSION}
SRCDIR=$(pwd)/${NAME}-${VERSION}

if ! RPM="$(rpm -qf /etc/redhat-release)"; then
    echo >&2 "This script only supports EL7"
    exit 1
fi
# This will return either 6,7 on RHEL clones or 6Server, 7Server on RHEL
ELVERSION="$(rpm -q $RPM --qf '%{VERSION}')"
ELVERSION="${ELVERSION:0:1}"
if [ "${ELVERSION}" != "7" ]; then
    echo >&2 "This script only supports EL7"
    exit 1
fi
VERSTRING=el${ELVERSION}

yum groupinstall -y 'Development tools'
yum install -y perl gcc libacl-devel libblkid-devel gnutls-devel \
                readline-devel python-devel gdb pkgconfig \
                krb5-workstation zlib-devel setroubleshoot-server libaio-devel \
                setroubleshoot-pluginspolicycoreutils-python libsemanage-python setools-libs-python \
                setools-libs popt-devel libpcap-devel sqlite-devel libidn-devel libxml2-devel \
                libacl-devel libsepol-devel libattr-devel keyutils-libs-develcyrus-sasl-devel \
                cups-devel bind-utils libxslt docbook-style-xsl openldap-devel pam-devel bzip2 ncurses-devel

mkdir -p ${DESTDIR}
rm -rf ${SRCDIR} ${DESTDIR}
curl -sSL ${URL} | tar zxf -
cd ${NAME}-${VERSION}

CC='ccache clang' CXX='ccache clang++' ./configure --enable-debug --enable-selftest --with-ads --with-systemd --with-winbind --download --prefix=/usr --sysconfdir=/etc --localstatedir=/var --enable-fhs -j `nproc`
make -j`nproc` -s V=0 CC='ccache clang' CXX='ccache clang++'
make DESTDIR=${DESTDIR} install
REPLACES="--replaces samba-common --replaces samba-common-libs --replaces samba-client-libs --replaces libsmbclient"

fpm -s dir -t rpm -n ${NAME}-client -v ${VERSION} --iteration ${ITERATION} --rpm-dist ${VERSTRING} --rpm-autoreqprov $REPLACES -C ${DESTDIR} .
