#!/bin/bash
NAME=CastXML
VER=0.1
ITERATION="${ITERATION:-6}"
TMPDIR=${TMPDIR:-/tmp}/`id -un`/castxml
DESTDIR=$TMPDIR/rootfs
SRCDIR=/usr/src/${NAME}
set -e
rm -rf $TMPDIR
mkdir -p $DESTDIR
sudo rm -rf $SRCDIR
sudo mkdir -p $SRCDIR
sudo chown `id -u`:`id -g` $SRCDIR

export PATH="/opt/rh/ruby193/root/usr/local/bin:/usr/local/bin:$PATH"

osid="$(bash osid)"
case "$osid" in
    *el6|*el7)
        CMAKECC=cmake3
        ;;
    ub14)
        CMAKECC=cmake
        ;;
    *)
        echo >&2 "Unknown OS: $osid"
        ;;
esac

curl -sSL http://repo.xcalar.net/deps/castxml-${VER}.tar.gz | tar zxf - -C "$SRCDIR/.."
cd $SRCDIR

${CMAKECC} -DCMAKE_INSTALL_PREFIX:PATH=/usr . && make && make install DESTDIR=${DESTDIR}

cd -

PKGNAME="castxml"
case "$osid" in
    *el6)
        fmt=rpm
        dist=el6
        pkg_install="yum localinstall -y"
        ;;
    *el7)
        fmt=rpm
        dist=el7
        pkg_install="yum localinstall -y"
        ;;
    ub14)
        fmt=deb
        pkg_install="dpkg -i"
        ;;
    *)
        echo >&2 "Unknown OS: $osid"
        ;;
esac

if [ "$fmt" = rpm ]; then
    FPMEXTRA="--rpm-dist $dist --rpm-autoreqprov"
fi
if command -v fpm &>/dev/null; then
    # Use fpm to build a native .rpm or .deb package called optclang
    fpm -s dir -t $fmt -n ${PKGNAME} -v $VER --iteration $ITERATION $FPMEXTRA -f $FPMEXTRA -C $DESTDIR usr
    if [ `id -u` -eq 0 ]; then
        $pkg_install ${PKGNAME}*.${fmt}
    else
        echo >&2 "${NAME} packages: `ls ${PKGNAME}*.${fmt}` "
    fi
else
    # if fpm isn't around, then copy $DESTDIR to /
    tar cf - -C $DESTDIR usr | tar xf - -C /
fi
