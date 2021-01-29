#!/bin/bash
set -e

NAME=thrift
PKGNAME=${PKGNAME:-xcalar-thrift}
VERSION=${1:-0.10.0}
ITERATION="${2:-28}"
# Unfortunately, we can't run fpm in a container when building in /tmp
TMPDIR=${TMPDIR:-$XLRDIR/tmp/$(id -un)-$(id -u)}/${NAME}-${VERSION}
DESTDIR=$TMPDIR/rootfs
SRCDIR=$TMPDIR/src
PREFIX=${PREFIX:-/opt/xcalar}
export PY_PREFIX=${PY_PREFIX:-/opt/xcalar}
export PYTHON=${PY_PREFIX}/bin/python3.6m
ENABLE_SHARED=${ENABLE_SHARED:-no}
ENABLE_STATIC=${ENABLE_STATIC:-yes}
TARBALL="${NAME}-${VERSION}.tar.gz"
TARBALL_PATH="$TMPDIR"/$TARBALL
URL="http://repo.xcalar.net/deps/${TARBALL}"
CCACHE=0
CLANG=1
CMAKE=1

rm -rf $TMPDIR
mkdir -p $DESTDIR $SRCDIR
if ! curl -fsSL "$URL" -o "$TARBALL_PATH"; then
    echo "ERROR: Unable to download ${NAME} ${VERSION}" >&2
    echo "Download it from https://www.apache.org/dist/${NAME}/${VERSION}/${TARBALL}" >&2
    exit 1
fi

tar zxf "$TARBALL_PATH" --strip-components=1 -C "$SRCDIR" || exit 1

OSID="$(bash osid)"
case "$OSID" in
    *el6)
        fmt=rpm
        dist=el6
        pkg_install="yum localinstall -y"
        sudo yum install --enablerepo='xcalar*' -y bison flex libevent2-devel
        FPMEXTRA="--rpm-dist $OSID"
        ;;
    *el7)
        fmt=rpm
        dist=el7
        pkg_install="yum localinstall -y"
        sudo yum install --enablerepo='xcalar*' -y bison flex libevent2-devel
        FPMEXTRA="--rpm-dist $OSID"
        ;;
    amzn*)
        fmt=rpm
        dist=$OSID
        pkg_install="yum localinstall -y"
        FPMEXTRA="--rpm-dist $OSID"
        sudo yum install --enablerepo='xcalar* epel' --disableplugin=priority -y bison flex libevent-devel
        ;;
    ub*)
        fmt=deb
        dist=$OSID
        pkg_install="dpkg -i"
        ;;
    *)
        echo >&2 "Unknown OS: $OSID"
        ;;
esac

if [ "$fmt" = rpm ]; then
    [ "$PREFIX" == /usr ] && libdir="${PREFIX}/lib64" || libdir="${PREFIX}/lib"
elif [ "$fmt" = deb ]; then
    libdir="${PREFIX}/lib"
fi

export CXXFLAGS="-O2 -g -std=gnu++11"
export CFLAGS="-O2 -g"

## These need to be the same flags as used in the xcalar repo's CMakeFiles.txt
export CXXFLAGS="$CXXFLAGS -D_THRIFT_USING_CLANG_LIBCXX=1 -D_THRIFT_USING_MICROSOFT_STDLIB=0 -DT_GLOBAL_DEBUG_VIRTUAL=0"
export CXXFLAGS="$CXXFLAGS -DUSE_BOOST_THREAD=0 -DUSE_STDTHREADS=0 -DUSE_STD_THREAD=1"

if ((CLANG)); then
    export CXX=clang++
    export CC=clang
else
    export CXX=g++
    export CC=gcc
fi

if ((CCACHE)); then
    if command -v ccache >/dev/null; then
        export PATH=/usr/lib64/ccache:$PATH
    fi
fi

if ((CMAKE)); then
    mkdir $SRCDIR/output
    cd $SRCDIR/output
    cmake -DBUILD_COMPILER=ON -DBUILD_LIBRARIES=ON -DBUILD_TESTING=OFF -DBUILD_EXAMPLES=OFF -DBUILD_CPP=ON -DWITH_STATIC_LIB=ON \
        -DBUILD_C_GLIB=OFF -DBUILD_PYTHON=ON -DWITH_BOOST_STATIC=ON \
        -DCMAKE_INSTALL_PREFIX=${PREFIX} -DCMAKE_CXX_COMPILER=$CXX -DCMAKE_C_COMPILER=$CC \
        -DCMAKE_CXX_FLAGS="$CXXFLAGS" -DCMAKE_C_FLAGS="$CFLAGS" \
        -DCMAKE_INSTALL_RPATH=${PREFIX}/lib -DWITH_STDTHREADS=OFF -DBOOST_ROOT=${PREFIX} \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_SHARED_LIB=OFF -DBUILD_TUTORIALS=OFF -DWITH_PYTHON=ON -G Ninja ..
    DESTDIR=$DESTDIR ninja install
    cd -
else
    cd $SRCDIR
    ./bootstrap.sh --prefix=$PREFIX
    PYTHON=${PY_PREFIX}/bin/python3 PY_PREFIX=$PY_PREFIX
    ./configure --prefix=$PREFIX --libdir=$libdir --enable-static=${ENABLE_STATIC} \
        --with-cpp --with-boost=${PREFIX} \
        --enable-shared=${ENABLE_SHARED} --without-python --without-c_glib --without-lua --without-ruby --without-csharp --without-java \
        --without-tutorials --without-examples --disable-examples --disable-tests --disable-tutorials \
        --without-nodejs --without-perl --without-php --without-php_extension \
        --without-haskell --without-go --without-d --enable-tests=no
    sed -i -Ee 's,^(#define [r]?[e]?[m]?alloc.*)$,//\1,g' lib/cpp/src/thrift/config.h
    make -s -j V=0 CXXFLAGS="$CXXFLAGS" CFLAGS="$CFLAGS" CC=$CC CXX=$CXX
    make -s install DESTDIR=$DESTDIR
    libtool --finish ${DESTDIR}$libdir
    cd -
fi

DEVEL=(
    .${PREFIX}/include
    .$libdir/libthrift.a
    .$libdir/libthriftc.a
    .$libdir/libthriftnb.a
    .$libdir/libthriftz.a)

LIBS=(.${PREFIX}/bin)

if [ "$ENABLE_SHARED" = yes ]; then
    DEVEL+=(
        .$libdir/libthrift.so
        .$libdir/libthriftc.so
        .$libdir/libthriftnb.so
        .$libdir/libthriftz.so)
    LIBS+=(
        .$libdir/libthrift-${VERSION}.so
        .$libdir/libthriftc.so.0
        .$libdir/libthriftc.so.0.0.0
        .$libdir/libthriftnb-${VERSION}.so
        .$libdir/libthriftz-${VERSION}.so)
fi

fpm=/usr/local/bin/fpm
if ! test -x $fpm; then
    fpm=$(command -v fpm) || exit 1
fi

if test -x $fpm; then
    printf '#!/bin/sh\nldconfig' >$DESTDIR/ldconfig.sh && chmod +x $DESTDIR/ldconfig.sh
    # Use fpm to build a native .rpm or .deb package
    if ! $fpm -s dir -t $fmt -n ${PKGNAME}-devel -v $VERSION --iteration "${ITERATION}" $FPMEXTRA -d ${PKGNAME} -f -C $DESTDIR "${DEVEL[@]}"; then
        tar czvf ${PKGNAME}-devel-${VERSION}-${ITERATION}-${OSID}.tar.gz -C $DESTDIR "${DEVEL[@]}"
    fi
    if ! $fpm -s dir -t $fmt -n ${PKGNAME} -v $VERSION --iteration "${ITERATION}" $FPMEXTRA --after-install $DESTDIR/ldconfig.sh -f -C $DESTDIR "${LIBS[@]}"; then
        tar czf ${PKGNAME}-${VERSION}-${ITERATION}-${OSID}.tar.gz -C $DESTDIR "${LIBS[@]}"
    fi
    if test -d ${DESTDIR}${PY_PREFIX}/lib/python3.6; then
        if ! $fpm -s dir -t $fmt -n xcalar-python36-${NAME} -v $VERSION --iteration "${ITERATION}" $FPMEXTRA -d xcalar-python36 --after-install $DESTDIR/ldconfig.sh -f -C $DESTDIR ".${PY_PREFIX}/lib/python3.6"; then
            tar czf xcalar-python36-${PKGNAME}-${VERSION}-${ITERATION}-${OSID}.tar.gz -C $DESTDIR ".${PY_PREFIX}/lib/python3.6"
        fi
    fi
    if [ $(id -u) -eq 0 ]; then
        $pkg_install ${PKGNAME}*.${fmt}
    else
        echo >&2 "Thrift packages: $(ls ${PKGNAME}*.${fmt}) "
    fi
else
    # if fpm isn't around, then copy $DESTDIR to /
    tar cf ${PKGNAME}-${VERSION}-${ITERATION}.tar.gz -C $DESTDIR .${PREFIX}
fi
