#!/bin/bash

set -e

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
PATH="$DIR:$PATH" . osid '' >/dev/null

NAME=xcalar-jemalloc
VERSION=5.2.1
ITERATION="${ITERATION:-3}"
TMPDIR="`pwd`/tmp/${NAME}-${VERSION}"
DESTDIR="${TMPDIR}/rootfs"
SRCDIR="${TMPDIR}/src"

prefix=opt/xcalar
libdir=opt/xcalar/lib

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR" "$DESTDIR" "$SRCDIR"

curl -sSL https://github.com/jemalloc/jemalloc/releases/download/$VERSION/jemalloc-${VERSION}.tar.bz2 | tar jxf - -C "$SRCDIR" --strip-components=1

cd "$SRCDIR"
./configure --prefix=/$prefix --libdir=/$libdir --with-rpath=/$libdir --enable-stats "$@"
make -j`nproc`
make DESTDIR=$DESTDIR install
cd -

# If fpm is installed, build deb/rpm packages and prefer to install those
DEVFILES="$prefix/bin/jemalloc-config $prefix/include/jemalloc/jemalloc.h $libdir/libjemalloc.a $libdir/pkgconfig/jemalloc.pc $libdir/libjemalloc_pic.a $prefix/share/man/man3/jemalloc.3"
LIBFILES="$prefix/bin/jemalloc.sh $prefix/bin/jeprof $libdir/libjemalloc.so.2"
case "$OSID_NAME" in
    ub)
        export DEBIAN_FRONTEND=noninteractive
        PKG="deb"
        INSTALLCMD="dpkg -i"
        REMOVECMD="apt-get purge -y"
        ;;
    el|rhel)
        PKG="rpm"
        INSTALLCMD="yum localinstall -y"
        REMOVECMD="yum remove -y"
        ;;
    *)
        echo >&2 "Unknown platform $OSID_NAME $OSID_VERSION"
        exit 2
        ;;
esac
FPMEXTRA=(--description 'jemalloc is a general purpose malloc(3) implementation that emphasizes fragmentation avoidance and scalable concurrency support.'
          --url 'jemalloc.net')

if command -v fpm &>/dev/null; then
    cat > "$TMPDIR/ldconfig.sh" <<-XEOF
	#!/bin/sh
	ldconfig
	XEOF

    SCRIPTS="--after-install $TMPDIR/ldconfig.sh --after-remove $TMPDIR/ldconfig.sh"
    fpm -s dir -t $PKG -v $VERSION --iteration $ITERATION --name $NAME "${FPMEXTRA[@]}" $SCRIPTS -f -C "$DESTDIR" $LIBFILES $DEVFILES
    sudo $REMOVECMD $NAME
    sudo $INSTALLCMD ${NAME}*.${PKG}
else
    cd "$SRCDIR"
    sudo $REMOVECMD $NAME || true
    sudo make install
    cd -
fi
