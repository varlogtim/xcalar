#!/bin/bash

PKGNAME=xcalar-boost
NAME=boost
VER=1.65.1
PREFIX=/opt/xcalar
ITERATION="${ITERATION:-4}"
# Due to the way containers work with tmpfs, we need to resort to using
# a local directory under $XLRDIR. This is purely to get mapping volumes
# into the fpm container to work.
TMPDIR=$XLRDIR/tmp/`id -un`/${NAME}
DESTDIR=$TMPDIR/rootfs
SRCDIR=$TMPDIR/${NAME}_${VER//./_}
rm -rf $TMPDIR
mkdir -p $SRCDIR ${DESTDIR}/${PREFIX}/{lib,bin,include}
# replace version dots with underscore so 1.2.3 -> 1_2_3
srcUrl="http://repo.xcalar.net/deps/boost_${VER//./_}.tar.gz"
curl -sSL "$srcUrl" | tar zxf - -C "$SRCDIR/.."
cd $SRCDIR
export PATH=/usr/lib64/ccache:$PATH

./bootstrap.sh --prefix=$PREFIX --without-icu --with-python=/opt/xcalar/bin/python3.6m --with-python-version=3.6m --with-toolset=clang
./b2 clean
./b2 -j`nproc` \
     variant=release \
     debug-symbols=off \
     threading=multi \
     runtime-link=static \
     link=static \
     cflags="${CPPFLAGS} ${CFLAGS} -fPIC -O3" \
     cxxflags="${CPPFLAGS} ${CXXFLAGS} -stdlib=libstdc++ -std=gnu++11 -fPIC -O3" \
     linkflags="${LDFLAGS} -stdlib=libstdc++" \
     --layout=system \
     --prefix="${DESTDIR}${PREFIX}" \
     install

printf '#!/bin/sh\nldconfig' > $DESTDIR/ldconfig.sh && chmod +x $DESTDIR/ldconfig.sh
cd -

osid="$(bash osid)"
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
    ub*)
        fmt=deb
        pkg_install="dpkg -i"
        ITERATION="${ITERATION}.${osid}"
        ;;
    *)
        echo >&2 "Unknown OS: $osid"
        ;;
esac
if [ "$fmt" = rpm ]; then
    :
elif [ "$fmt" = deb ]; then
    :
fi

if command -v fpm &>/dev/null; then
    # Use fpm to build a native .rpm or .deb package
    fpm -s dir -t $fmt -n ${PKGNAME} -v $VER --iteration $ITERATION $FPMEXTRA -a native \
        --after-install $DESTDIR/ldconfig.sh --prefix $PREFIX -f -C ${DESTDIR}${PREFIX}
fi
# if fpm isn't around, then copy $DESTDIR to /
tar czf ${PKGNAME}.tar.gz -C $DESTDIR "${PREFIX#/}"

