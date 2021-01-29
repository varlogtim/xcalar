#!/bin/bash

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:$PATH

export PATH=$DIR:$PATH
. osid >/dev/null
VERSTRING="$(_osid --full)"
NAME=libevent
VERSION=${VERSION:-2.0.22}
BUILD_NUMBER=${BUILD_NUMBER:-1}
ITERATION="${ITERATION:-$BUILD_NUMBER}"

TMPDIR=${TMPDIR:-/tmp}/`id -un`/fpm
mkdir -p "$TMPDIR"

SRCDIR=/usr/src/${NAME}-${VERSION}-stable
DESTDIR=${TMPDIR}/${NAME}-${VERSION}-install

sudo rm -rf ${SRCDIR} ${DESTDIR}
sudo mkdir -p ${SRCDIR} ${DESTDIR}
sudo chown `id -u`:`id -g` ${SRCDIR} ${DESTDIR}

case "$VERSTRING" in
    ub*) sudo apt-get install -yqq openssl-dev;;
    *el[67]) sudo yum install -y openssl-devel;;
    *) ;;
esac

curl -sSL https://github.com/libevent/libevent/releases/download/release-${VERSION}-stable/libevent-${VERSION}-stable.tar.gz | tar zxf - -C $SRCDIR/..
cd $SRCDIR
./configure --prefix=/usr --enable-shared=yes --enable-static=yes
make -j`nproc`
make DESTDIR=$DESTDIR install
cat > /tmp/ldconfig.sh<<EOF
#!/bin/sh
ldconfig
EOF
chmod +x /tmp/ldconfig.sh

cd -

case "$VERSTRING" in
    ub14)
        fpm -s dir -t deb --iteration ${ITERATION} --version ${VERSION} --name ${NAME}-dev -C $DESTDIR usr/include  usr/lib/pkgconfig  usr/lib/libevent{,_extra,_core,_pthreads,_openssl}.a
        fpm -s dir -t deb --iteration ${ITERATION} --version ${VERSION} --name ${NAME} --after-install /tmp/ldconfig.sh --rpm-autoreqprov -C $DESTDIR $(cd $DESTDIR && find usr -name 'libevent*.so' -o -name 'libevent*.so.*')
        ;;
    *el6)
        fpm -s dir -t rpm --rpm-dist el6 --iteration ${ITERATION} --version ${VERSION} --name ${NAME}-devel --rpm-autoreqprov -C  $DESTDIR usr/include  usr/lib/pkgconfig  usr/lib/libevent{,_extra,_core,_pthreads,_openssl}.a
        fpm -s dir -t rpm --rpm-dist el6 --iteration ${ITERATION} --version ${VERSION} --name ${NAME} --after-install /tmp/ldconfig.sh --rpm-autoreqprov -C $DESTDIR $(cd $DESTDIR && find usr -name 'libevent*.so' -o -name 'libevent*.so.*')
        ;;
    *el7)
        fpm -s dir -t rpm --rpm-dist el7 --iteration ${ITERATION} --version ${VERSION} --name ${NAME}-devel --rpm-autoreqprov -C  $DESTDIR usr/include  usr/lib/pkgconfig  usr/lib/libevent{,_extra,_core,_pthreads,_openssl}.a
        fpm -s dir -t rpm --rpm-dist el7 --iteration ${ITERATION} --version ${VERSION} --name ${NAME} --after-install /tmp/ldconfig.sh --rpm-autoreqprov -C $DESTDIR $(cd $DESTDIR && find usr -name 'libevent*.so' -o -name 'libevent*.so.*')
        ;;
    *)
        echo >&2 "Unknown OS $VERSTRING"
        ;;
esac

