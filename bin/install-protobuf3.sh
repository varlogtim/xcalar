#!/bin/bash
set -e

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
export PATH="$DIR:$PATH"

. osid >/dev/null

OSID=$(osid)
VERSION=3.6.1
ITERATION=${ITERATION:-6}
NAME=protobuf
TMPDIR=${TMPDIR:-/tmp}/`id -un`/$NAME
DESTDIR=${TMPDIR}/${NAME}-${VERSION}/rootfs
SRCDIR=${TMPDIR}/${NAME}-${VERSION}/${NAME}-${VERSION}

prefix=usr
for libdir in $prefix/lib/x86_64-linux-gnu $prefix/lib64 $prefix/lib; do
    test -e /$libdir && break
done

rm -rf $DESTDIR $SRCDIR
mkdir -p $DESTDIR $SRCDIR

curl -fsSL http://repo.xcalar.net/deps/protobuf-cpp-$VERSION.tar.gz | tar zxf - -C $SRCDIR/.. --no-same-owner
curl -fsSL http://repo.xcalar.net/deps/protobuf-js-$VERSION.tar.gz | tar zxf - -C $SRCDIR/.. --no-same-owner
curl -fsSL http://repo.xcalar.net/deps/protobuf-python-$VERSION.tar.gz | tar zxf - -C $SRCDIR/.. --no-same-owner

cd $SRCDIR

./autogen.sh
CC='clang' CXX='clang++' ./configure --prefix=/$prefix --libdir=/$libdir --with-zlib --enable-silent-rules --enable-shared=no --enable-static=yes
make -j -s V=0 CC='ccache clang' CXX='ccache clang++'
make DESTDIR=$DESTDIR install

cd -

SCRIPTS=

if command -v fpm &>/dev/null; then
    find $DESTDIR -name '*.la' -delete
    case "$OSID" in
        ub*)
            ITERATION="${ITERATION}.${OSID}"
            VERIT="${VERSION}-${ITERATION}"
            fpm -s dir -t deb -v ${VERSION} --iteration ${ITERATION} --name libprotoc-dev -d 'zlib1g-dev' -d "libprotobuf-dev (= $VERIT)" $FPMEXTRA -f -C $DESTDIR $libdir/libprotoc.a usr/include/google/protobuf/compiler
            (cd $DESTDIR && rm -rf usr/include/google/protobuf/compiler)
            fpm -s dir -t deb -v ${VERSION} --iteration ${ITERATION} --name libprotobuf-dev -d 'zlib1g-dev' $FPMEXTRA -f -C $DESTDIR $libdir/pkgconfig/protobuf{,-lite}.pc $libdir/libprotobuf{,-lite}.a usr/include/google/protobuf
            fpm -s dir -t deb -v ${VERSION} --iteration ${ITERATION} --name protobuf-compiler $FPMEXTRA -f -C $DESTDIR usr/bin/protoc
            ;;
        el*|rhel*)
            VERIT="${VERSION}-${ITERATION}.el${OSID_VERSION}"
            FPMEXTRA="--rpm-dist el${OSID_VERSION} --rpm-autoreqprov"
            find $DESTDIR -name '*.la' -delete
            fpm -s dir -t rpm -v ${VERSION} --iteration ${ITERATION} --name protoc-devel -d "protobuf-compiler = $VERIT" -d "protobuf-devel = $VERIT" $FPMEXTRA -f -C $DESTDIR $libdir/libprotoc.a usr/include/google/protobuf/compiler
            (cd $DESTDIR && rm -rf usr/include/google/protobuf/compiler)
            fpm -s dir -t rpm -v ${VERSION} --iteration ${ITERATION} --name protobuf-devel $FPMEXTRA -f -C $DESTDIR $libdir/pkgconfig/protobuf{,-lite}.pc $libdir/libprotobuf{,-lite}.a usr/include/google/protobuf
            fpm -s dir -t rpm -v ${VERSION} --iteration ${ITERATION} --name protobuf-compiler $FPMEXTRA -f -C $DESTDIR usr/bin/protoc
            ;;
        amzn*)
            VERIT="${VERSION}-${ITERATION}.${OSID_NAME}${OSID_VERSION}"
            FPMEXTRA="--rpm-dist ${OSID_NAME}${OSID_VERSION} --rpm-autoreqprov"
            find $DESTDIR -name '*.la' -delete
            fpm -s dir -t rpm -v ${VERSION} --iteration ${ITERATION} --name protoc-devel -d "protobuf-compiler = $VERIT" -d "protobuf-devel = $VERIT" $FPMEXTRA -f -C $DESTDIR $libdir/libprotoc.a usr/include/google/protobuf/compiler
            (cd $DESTDIR && rm -rf usr/include/google/protobuf/compiler)
            fpm -s dir -t rpm -v ${VERSION} --iteration ${ITERATION} --name protobuf-devel $FPMEXTRA -f -C $DESTDIR $libdir/pkgconfig/protobuf{,-lite}.pc $libdir/libprotobuf{,-lite}.a usr/include/google/protobuf
            fpm -s dir -t rpm -v ${VERSION} --iteration ${ITERATION} --name protobuf-compiler $FPMEXTRA -f -C $DESTDIR usr/bin/protoc
            ;;
        *)
            echo >&2 "Unknown platform $OSID"
            exit 2
            ;;
    esac

    if [ `id -u` -eq 0 ]; then
        case "$OSID" in
            ub*)
                dpkg -i libproto*.deb protobuf*.deb
                ;;
            el*|rhel*|amzn*)
                yum install -y proto*-${VERIT}.x86_64.rpm
                ;;
        esac
    else
        case "$OSID" in
            ub*)
                echo >&2 "${NAME} packages: `ls libproto*.deb protobuf*.deb`"
                ;;
            el*|rhel*|amzn*)
                echo >&2 "${NAME} packages: `ls proto*-${VERIT}.x86_64.rpm`"
                ;;
        esac
    fi

fi
