#!/bin/bash
NAME=pyarrow
VER=0.7.1
ITERATION="${ITERATION:-6}"
TMPDIR=${TMPDIR:-/tmp}/`id -un`/arrow
DESTDIR=$TMPDIR/rootfs
SRCDIR=/usr/src/apache-arrow-${VER}
set -e
rm -rf $TMPDIR
mkdir -p $DESTDIR
sudo rm -rf $SRCDIR
sudo mkdir -p $SRCDIR
sudo chown `id -u`:`id -g` $SRCDIR
sudo ldconfig

if [ -e /usr/bin/cmake3 ]; then
    sudo ln -sfn /usr/bin/cmake3 /usr/local/bin/cmake
    PATH="/usr/local/bin:$PATH"
fi

export PATH="/opt/xcalar/bin:/opt/rh/ruby193/root/usr/local/bin:$PATH"
#export ARROW_HOME=/opt/xcalar
#export PARQUET_HOME=/opt/xcalar

osid="$(bash osid)"
case "$osid" in
    *el6)
        CMAKECC=cmake3
        cmake_args="-DPYTHON_LIBRARY=/opt/xcalar/lib/libpython2.7.a
                    -DPYTHON_INCLUDE_DIR=/opt/xcalar/include/python2.7/
                    -DPYTHON_EXECUTABLE=/opt/xcalar/bin/python2.7"
        export LD_LIBRARY_PATH=/opt/xcalar/lib64:${LD_LIBRARY_PATH}
        export PKG_CONFIG_PATH=/opt/xcalar/lib64/pkgconfig:${PKG_CONFIG_PATH}
        PYTHON_CMD=python2.7
        ;;
    *el7)
        CMAKECC=cmake3
        cmake_args="-DPYTHON_LIBRARY=/opt/xcalar/lib/libpython2.7.a
                    -DPYTHON_INCLUDE_DIR=/opt/xcalar/include/python2.7/
                    -DPYTHON_EXECUTABLE=/opt/xcalar/bin/python2.7"
        export LD_LIBRARY_PATH=/opt/xcalar/lib64:${LD_LIBRARY_PATH}
        export PKG_CONFIG_PATH=/opt/xcalar/lib64/pkgconfig:${PKG_CONFIG_PATH}
        PYTHON_CMD=python2.7
        ;;
    ub14)
        CMAKECC=cmake
        export LD_LIBRARY_PATH=/opt/xcalar/lib:${LD_LIBRARY_PATH}
        export PKG_CONFIG_PATH=/opt/xcalar/lib/pkgconfig:${PKG_CONFIG_PATH}
        PYTHON_CMD=python2.7
        ;;
    *)
        echo >&2 "Unknown OS: $osid"
        ;;
esac

curl -sSL https://dist.apache.org/repos/dist/release/arrow/arrow-${VER}/apache-arrow-${VER}.tar.gz | tar zxf - -C "$SRCDIR/.."

export PYARROW_BUILD_TYPE=release
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_JEMALLOC=0
export PYARROW_BUNDLE_ARROW_CPP=0

PKGNAME="xcalar-pyarrow"
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
    FPMEXTRA="$FPMEXTRA --rpm-dist $dist --rpm-autoreqprov"
elif [ "$fmt" = deb ]; then
    FPMEXTRA="$FPMEXTRA --no-auto-depends"
fi
if command -v fpm &>/dev/null; then
    # Use fpm to build a native .rpm or .deb package called optclang
    fpm -s python -t $fmt -n ${PKGNAME} --python-bin /opt/xcalar/bin/python2.7 --python-pip /opt/xcalar/bin/pip2.7 --no-python-dependencies ${FPMEXTRA} --iteration 6 --version ${VER} -f $SRCDIR/python/setup.py

    if [ `id -u` -eq 0 ]; then
        $pkg_install ${PKGNAME}*.${fmt}
    else
        echo >&2 "${NAME} packages: `ls ${PKGNAME}*.${fmt}` "
    fi
else
    # if fpm isn't around, then copy $DESTDIR to /
    tar cf - -C $DESTDIR usr | tar xf - -C /
fi
