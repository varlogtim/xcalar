#!/bin/bash
NAME=Parquet-cpp
VER=1.3.0
ITERATION="${ITERATION:-6}"
TMPDIR=${TMPDIR:-/tmp}/`id -un`/arrow
DESTDIR=$TMPDIR/rootfs
SRCDIR=/usr/src/apache-parquet-cpp-${VER}
set -e
rm -rf $TMPDIR
mkdir -p $DESTDIR
sudo rm -rf $SRCDIR
sudo mkdir -p $SRCDIR
sudo chown `id -u`:`id -g` $SRCDIR

export PATH="/opt/rh/ruby193/root/usr/local/bin:$PATH"
export LD_LIBRARY_PATH=/opt/xcalar/lib64:${LD_LIBRARY_PATH}

# these force parquet to use its own internally built versions
# that are statically linked with fPIC
#export ARROW_HOME=/opt/xcalar
export THRIFT_HOME=$SRCDIR/null
export SNAPPY_HOME=$SRCDIR/null
export ZLIB_HOME=$SRCDIR/null

osid="$(bash osid)"
case "$osid" in
    *el6)
        CMAKECC=cmake3
        export PKG_CONFIG_PATH=/opt/xcalar/lib64/pkgconfig:${PKG_CONFIG_PATH}
        ;;
    *el7)
        CMAKECC=cmake3
        export PKG_CONFIG_PATH=/opt/xcalar/lib64/pkgconfig:${PKG_CONFIG_PATH}
        ;;
    ub14)
        CMAKECC=cmake
        export PKG_CONFIG_PATH=/opt/xcalar/lib/pkgconfig:${PKG_CONFIG_PATH}
        ;;
    *)
        echo >&2 "Unknown OS: $osid"c
        ;;
esac

curl -sSL https://dist.apache.org/repos/dist/release/parquet/apache-parquet-cpp-${VER}/apache-parquet-cpp-${VER}.tar.gz | tar zxf - -C "$SRCDIR/.."

mkdir $SRCDIR/release
mkdir $SRCDIR/null

cd $SRCDIR/release

${CMAKECC} .. -DPARQUET_BUILD_BENCHMARKS=off -DPARQUET_BUILD_EXECUTABLES=off \
    -DPARQUET_BUILD_STATIC=on -DPARQUET_ARROW=on -DPARQUET_BUILD_TESTS=off \
    -DCMAKE_INSTALL_PREFIX:PATH=/opt/xcalar -DCMAKE_BUILD_TYPE=release && make -j4 && make install DESTDIR=${DESTDIR}

cd -

PKGNAME="xcalar-parquet"
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
    fpm -s dir -t $fmt -n ${PKGNAME} -v $VER --iteration $ITERATION $FPMEXTRA -f $FPMEXTRA -C $DESTDIR opt
    if [ `id -u` -eq 0 ]; then
        $pkg_install ${PKGNAME}*.${fmt}
    else
        echo >&2 "${NAME} packages: `ls ${PKGNAME}*.${fmt}` "
    fi
else
    # if fpm isn't around, then copy $DESTDIR to /
    tar cf - -C $DESTDIR usr | tar xf - -C /
fi
