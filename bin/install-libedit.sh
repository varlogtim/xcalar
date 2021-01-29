#!/bin/bash

set -e

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
PATH="$DIR:$PATH" . osid >/dev/null

NAME=libedit

VERSION=3.1
ITERATION="${ITERATION:-3}"
TMPDIR="${TMPDIR:-/tmp/`id -u`}/${NAME}-${VERSION}_${ITERATION}"
DESTDIR="${TMPDIR}/rootfs"
SRCDIR=/usr/src/${NAME}-20160903-${VERSION}

prefix=usr
for libdir in $prefix/lib/x86_64-linux-gnu $prefix/lib64 $prefix/lib; do
    test -e /$libdir && break
done

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR" "$DESTDIR"
rm -rf "$SRCDIR"
mkdir -p "$SRCDIR"

curl -sSL http://repo.xcalar.net/deps/libedit-20160903-${VERSION}.tar.gz | tar zxf - -C "$SRCDIR/.."

cd "$SRCDIR"
./configure --prefix=/$prefix --libdir=/$libdir CFLAGS=-fPIC LDFLAGS=-fPIC
make -j`nproc`
make DESTDIR=$DESTDIR install
rm -f $DESTDIR/$libdir/libedit.so $DESTDIR/$libdir/libedit.la
cd -

# If fpm is installed, build deb/rpm packages and prefer to install those
if command -v fpm &>/dev/null; then
    cat > "$TMPDIR/ldconfig.sh" <<-XEOF
	#!/bin/sh
	ldconfig
	XEOF

    case "$OSID_NAME" in
        ub)
            PKG="deb"
            DEVNAME="${NAME}"
            INSTALLCMD="dpkg -i"
            ;;
        el|rhel)
            PKG="rpm"
            DEVNAME="${NAME}"
            INSTALLCMD="rpm -Uvh"
            FPMEXTRA="--rpm-dist el${OSID_VERSION}"
            ;;
        *)
            echo >&2 "Unknown platform $OSID_NAME $OSID_VERSION"
            exit 2
            ;;
    esac
    SCRIPTS="--after-install $TMPDIR/ldconfig.sh --after-remove $TMPDIR/ldconfig.sh"
    fpm -s dir -t $PKG -v $VERSION --iteration $ITERATION --name $DEVNAME -f $FPMEXTRA -C "$DESTDIR" usr
    sudo $INSTALLCMD ${DEVNAME}*.${PKG}
else
    cd "$SRCDIR"
    sudo make install
    cd / && sudo rm -f $LIBFILES
    cd $SRCDIR
fi
