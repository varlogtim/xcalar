#!/bin/bash
VERSION=5.5.0
ITERATION=1
VERSTRING="$(osid)"
curl -fsSL https://collectd.org/files/collectd-${VERSION}.tar.gz | tar xz
cd collectd-${VERSION}/
yum install -y yajl yajl-devel lvm2-devel curl-devel iptables-devel mysql-devel rrdtool-devel libvirt-devel python-devel
./configure --prefix=/usr --sysconfdir=/etc --localstatedir=/var --libdir=/usr/lib64 --mandir=/usr/share/man --enable-python --enable-java
make

DESTDIR="${TMPDIR:-/tmp/`id -un`}"/collectd-${VERSION}
rm -rf "$DESTDIR"
mkdir -p "$DESTDIR"

make DESTDIR=$DESTDIR install
mkdir -p "${DESTDIR}/etc/init.d"
cp contrib/redhat/init.d-collectd  "${DESTDIR}/etc/init.d/collectd"
chmod +x "${DESTDIR}/etc/init.d/collectd"
find "${DESTDIR}" -name '*.la' -delete

export PATH=/opt/rh/ruby193/root/usr/local/bin:$PATH
cd ..
fpm -s dir -t rpm -f --rpm-dist ${VERSTRING} --iteration ${ITERATION} -v ${VERSION} -n collectd \
    --rpm-autoreqprov --config-files /etc/collectd.conf -d libtool-ltdl \
    --after-install <(echo ldconfig) --after-remove <(echo ldconfig) \
    -C "${DESTDIR}" usr etc
