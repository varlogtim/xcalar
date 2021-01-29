#!/bin/bash

set -e

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
export PATH="$DIR:$PATH"
. osid '' >/dev/null
VERSTRING="$(osid)"

NAME=sqlite
VERSION="${1}"
VERSION="${VERSION:-3.2.1}"
VER="3210000"
PKGNAME="xcalar-${NAME}${VER}"
ITERATION="${ITERATION:-1}"
TMPDIR="${TMPDIR:-/tmp/$(id -u)}/${NAME}-${VERSION}/${VERSTRING}"
DESTDIR="${TMPDIR}/rootfs"
SRCDIR="${TMPDIR}/${NAME}-${VERSION}"
PREFIX=/usr
INSTALL=${DESTDIR}${PREFIX}
AFTER_INSTALL="python-after-install.sh"
URL="https://www.sqlite.org/2017/sqlite-autoconf-${VER}.tar.gz"
unset CCACHE_BASEDIR

rm -rf "$TMPDIR"
mkdir -p "$TMPDIR" $DESTDIR
rm -rf "$SRCDIR"
mkdir -p "$SRCDIR"

export PATH=/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/sbin:/bin

if test -f /opt/rh/devtoolset-7/enable; then
    . /opt/rh/devtoolset-7/enable
fi
for cc in gfortran cc gcc g++; do
    sudo ln -sfn $(which ccache) /usr/local/bin/${cc}
done
export PATH="/usr/lib64/ccache:/usr/local/bin:$PATH"

safe_curl() {
    curl -4 --location --retry 20 --retry-delay 3 --retry-max-time 60 "$@"
}

install_deps() {
    return
    # Make sure we have editline for python
    case "$OSID_NAME" in
        ub)
            PKG="deb"
            INSTALLCMD="dpkg -i"
            DISTPKG="${PKGNAME}_${VERSION}-${ITERATION}_amd64.deb"
            sudo apt-get purge -y libsqlite0-dev
            ;;
        el | rhel)
            PKG="rpm"
            INSTALLCMD="yum localinstall -y"
            FPMDIST="--rpm-dist el${OSID_VERSION}"
            FPMEXTRA="${FPMDIST} -d glibc -d libxslt -d libxml2 -d openssl -d ncurses-libs -d zlib -d bzip2-libs -d unixODBC -d openblas"
            FPMPROVIDES=("--provides" "python(abi) = ${VER}")
            DISTPKG="${PKGNAME}-${VERSION}-${ITERATION}.${OSID_NAME}${OSID_VERSION}.x86_64.rpm"

            if [ "$OSID_VERSION" = 6 ]; then
                EXTRA_PACKAGES="devtoolset-7-gcc-gfortran"
            fi
            sudo yum install -y editline-devel sqlite-devel unixODBC-devel blas64-static openblas-static libffi-devel $EXTRA_PACKAGES
            sudo yum remove -y python-devel python27-devel || true
            ;;
        *)
            echo >&2 "Unknown platform $OSID_NAME $OSID_VERSION"
            exit 2
            ;;
    esac
}

install_sqlite() {
    safe_curl "$URL" | tar zxvf - -C "$SRCDIR" --strip-components=1
}

install_deps
install_sqlite

cd $SRCDIR
./configure --prefix=$PREFIX --disable-static --enable-fts5 --enable-json1 CFLAGS="-g -O2 -DSQLITE_ENABLE_FTS3=1 -DSQLITE_ENABLE_FTS4=1 -DSQLITE_ENABLE_RTREE=1 -fPIC"
make -j
make install DESTDIR=$DESTDIR
fpm -s dir -t deb -n $NAME -v $VERSION --iteration $ITERATION -C $DESTDIR usr
