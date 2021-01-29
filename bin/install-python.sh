#!/bin/bash
#
# This script builds a full python distribution in 'Omnibus'
# mode, away from the OS.
#
set -e

DIR="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"
export PATH="$DIR:$PATH"
. osid '' >/dev/null
OSID="$(osid)"

NAME=python
UPNAME=Python
VERSION="${1}"
export VERSION="${VERSION:-3.6.4}"
VMAJ="${VERSION:0:1}"
VER="${VERSION:0:3}"                # eg, 3.6
NVER="${VERSION:0:1}${VERSION:2:1}" # eg, 36
PKGNAME="xcalar-${NAME}${NVER}"
ITERATION="${ITERATION:-86}"
TMPDIR="${TMPDIR:-/tmp/$(id -u)}/${NAME}-${VERSION}/${OSID}"
DESTDIR=""
SRCDIR="${TMPDIR}/${UPNAME}-${VERSION}"
PIPBUILD="${TMPDIR}/pip"
PREFIX=/opt/xcalar
INSTALL=${DESTDIR}${PREFIX}
PYTOOLS_VERSIONED="${PYTOOLS_VERSIONED:-0}"
AFTER_INSTALL="python-after-install.sh"
PYDOWNLOADS=${PWD}/python${VERSION}/${OSID}/packages
PYWHEELS=${PWD}/python${VERSION}/${OSID}/wheels
PYREQ=${TMPDIR}/requirements.txt
unset CCACHE_BASEDIR

if [ $(id -u) -ne 0 ]; then
    sudo=/usr/bin/sudo
    if ! test -x $sudo; then
        echo >&2 "You need to install sudo or run as root"
        exit 1
    fi
fi


rm -rf "$TMPDIR"
mkdir -p "$TMPDIR" $DESTDIR
rm -rf "$SRCDIR"
mkdir -p "$SRCDIR" "$PIPBUILD"

export PATH=/usr/lib64/ccache:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/sbin:/bin

#if test -f /opt/rh/devtoolset-7/enable; then
#    . /opt/rh/devtoolset-7/enable
#fi
#for cc in gfortran cc gcc g++; do
#    sudo ln -sfn $(which ccache) /usr/local/bin/${cc}
#done
#export PATH="/usr/lib64/ccache:/usr/local/bin:$PATH"

safe_curl() {
    curl -4 --location --retry 20 --retry-delay 3 --retry-max-time 60 "$@"
}

pip() {
    python${VER} -m pip "$@"
}

pip_install() {
    pip install --build $PIPBUILD "$@"
}

install_sqlite() {
    local SQLITE_VER="3210000"
    local SQLITE_URL="https://www.sqlite.org/2017/sqlite-autoconf-${SQLITE_VER}.tar.gz"
    mkdir -p $SRCDIR/../sqlite-autoconf-${SQLITE_VER}
    cd $SRCDIR/../sqlite-autoconf-${SQLITE_VER}

    safe_curl "$SQLITE_URL" | tar zxvf - -C "$SRCDIR/../sqlite-autoconf-${SQLITE_VER}" --strip-components=1
    ./configure --prefix=$PREFIX --disable-static --enable-fts5 --enable-json1 CFLAGS="-g -O2 -DSQLITE_ENABLE_FTS3=1 -DSQLITE_ENABLE_FTS4=1 -DSQLITE_ENABLE_RTREE=1 -fPIC"
    make -j
    make install DESTDIR=${DESTDIR:-/}
    cd -
    tar czvf - -C "${DESTDIR:-/}" "${PREFIX#/}" >sqlite-autoconf-${SQLITE_VER}.tar.gz
    export PKG_CONFIG_PATH=${PREFIX}/lib/pkgconfig
}

install_deps() {
    # Make sure we have editline for python
    case "$OSID" in
        ub*)
            PKG="deb"
            INSTALLCMD="dpkg -i"
            DISTPKG="${PKGNAME}_${VERSION}-${ITERATION}_amd64.deb"
            FPMDEPS=(-d libodbc1 -d unixodbc -d libsybdb5 -d libopenblas-base -d freetds-common)
            $sudo apt-get update
            $sudo apt-get install -y libeditline-dev unixodbc-dev gfortran freetds-dev freetds-common libopenblas-dev
            $sudo apt-get purge -y python-dev || true
            ;;
        amzn* | el* | rhel*)
            PKG="rpm"
            INSTALLCMD="yum localinstall -y"
            FPMDIST="--rpm-dist ${OSID}"
            FPMDEPS=(-d glibc -d libxslt -d libxml2 -d openssl -d ncurses-libs -d zlib -d bzip2-libs -d openblas -d 'unixODBC >= 2.3.1' -d freetds)
            FPMDEPS+=(-d mysql-libs -d mysql-connector-odbc -d postgresql-odbc -d postgresql-libs)
            FPMPROVIDES=("--provides" "python(abi) = ${VER}")
            DISTPKG="${PKGNAME}-${VERSION}-${ITERATION}.${OSID_NAME}${OSID_VERSION}.x86_64.rpm"

            case "$OSID" in
                el6) EXTRA_PACKAGES="devtoolset-7-gcc-gfortran";;
                amzn1) EXTRA_PACKAGES="gcc72 gcc72-c++";;
            esac
            $sudo yum install --enablerepo='epel' --enablerepo='xcalar-*' --disableplugin=priorities -y \
                editline-devel unixODBC-devel blas64-static openblas-static libffi-static \
                mysql-devel libxml2-static libxml2-devel freetds-devel \
                libxslt-devel xmlsec1-devel $EXTRA_PACKAGES
            $sudo yum remove -y python-devel python27-devel || true
            ;;
        *)
            echo >&2 "Unknown platform $OSID_NAME $OSID_VERSION"
            exit 2
            ;;
    esac

    DISTTAR="${PKGNAME}-${VERSION}-${ITERATION}.${OSID_NAME}${OSID_VERSION}.tar"

    if [ "$VMAJ" = 2 ]; then
        FPMREPLACES="--replaces xcalar-python27-lxml"
    elif [ "$VMAJ" = 3 ]; then
        FPMREPLACES="--replaces xcalar-python27 --replaces xcalar-python27-faker --replaces xcalar-python27-pyquery --replaces xcalar-python27-cssselect --replaces xcalar-python27-lxml --replaces xcalar-pyarrow --replaces xcalar-parquet"
        FPMREPLACES="${FPMREPLACES} --replaces xcalar-python36-cx_Oracle --replaces xcalar-python36-mysql-connector-python --replaces xcalar-python36-mysqlclient --replaces xcalar-python36-pymssql --replaces xcalar-python36-pymysql --replaces xcalar-python36-teradata"
    else
        echo >&2 "ERROR: VMAJ=$VMAJ is impossible"
        exit 1
    fi
}

install_pyodbc() {
    pip download $PYODBC
    tar zxf pyodbc* -C "$TMPDIR"
    pushd ${TMPDIR}/pyodbc*
    patch -p1 <${TMPDIR}/pyodbc-pip-patch-0001.diff
    $INSTALL/bin/python${VER} setup.py install
    popd
}

unset PYTHONHOME
unset PYTHONPATH
# Make sure we're in a safe environment because we have to nuke /opt/xcalar for the
# pip binaries to have proper paths
if [ "$container" != docker ]; then
    echo >&2 "WARNING: You're not running inside a container."
    if [ -e "$PREFIX/" ]; then
        echo >&2 "This script will remove your $PREFIX. Press Ctrl-C to cancel"
        echo >&2 "Sleeping for 10 seconds ..."
        sleep 10
    fi
fi

restore_prefix() {
    if ! [ -d "${PREFIX}.$$" ]; then
        return
    fi
    if [ -d "$PREFIX" ]; then
        $sudo mv "$PREFIX" "${PREFIX}.$$-build"
    fi
    $sudo mv "${PREFIX}.$$" "$PREFIX"
}

if test -e $PREFIX; then
    $sudo mv $PREFIX ${PREFIX}.$$
    trap "restore_prefix" EXIT
fi
$sudo mkdir -p $PREFIX
$sudo chown $(id -u):$(id -g) $PREFIX

install_deps
install_sqlite

if [ "$VERSION" = 3.7.0 ]; then
    # Because ... that link is different.
    if ! test -e ${UPNAME}-${VERSION}a1.tgz; then
        safe_curl -fsS https://www.python.org/ftp/${NAME}/${VERSION}/${UPNAME}-${VERSION}a1.tgz | tar zxf - -C "$SRCDIR" --strip-components=1
    else
        tar zxf ${UPNAME}-${VERSION}a1.tgz -C "$SRCDIR" --strip-components=1
    fi
else
    if ! test -e ${UPNAME}-${VERSION}.tgz; then
        safe_curl -fsS https://www.python.org/ftp/${NAME}/${VERSION}/${UPNAME}-${VERSION}.tgz | tar zxf - -C "$SRCDIR" --strip-components=1
    else
        tar zxf ${UPNAME}-${VERSION}.tgz -C "$SRCDIR" --strip-components=1
    fi
fi

if [ "$VMAJ" = 3 ]; then
    # Have to build from master of supervisor for Py3 support. Do this until we've migrated off of supervisor
    sed 's|^supervisor.*$|git+https://github.com/Supervisor/supervisor@master|g' ${XLRDIR}/pkg/requirements.txt >"${PYREQ}"
    # pyodbc fails on a compiler warning with Python 3.7, so we'll special case iit
    if [ "$VERSION" = 3.7.0 ]; then
        PYODBC="$(grep ^pyodbc ${PYREQ})"
        sed -i '/^pyodbc/d' "${PYREQ}"
        cp "${XLRDIR}/src/3rd/pyodbc/pyodbc-pip-patch-0001.diff" "${TMPDIR}"
    fi
else
    cp "${XLRDIR}/pkg/requirements.txt" "${PYREQ}"
fi

pushd "$SRCDIR"
./configure --enable-unicode=ucs4 --with-ensurepip=install --prefix=$PREFIX --with-threads --enable-loadable-sqlite-extensions \
    --with-system-ffi --with-tsc --enable-optimizations

# We have to disable the use of tmpnam, tmpnam_r and tempnam from libc or we get tons of linker warnings in our code
sed -i '/HAVE_TMPNAM /d; /HAVE_TMPNAM_R /d; /HAVE_TEMPNAM /d' pyconfig.h
ccache -s
if ! mkdir -p $INSTALL; then
    $sudo mkdir -p $INSTALL
    $sudo chown $(id -u):$(id -g) $INSTALL
fi
make -s V=0 DESTDIR=$DESTDIR altinstall
ccache -s

# Remove docs
rm -rf $INSTALL/share/doc

# Install all our required python packes into /opt/xcalar
export PATH=$INSTALL/bin:$PATH
hash -r

# Install pip unless we have it
if [ $VMAJ -lt 3 ]; then
    safe_curl -fsSL https://bootstrap.pypa.io/get-pip.py | $INSTALL/bin/python${VER}
fi

# Manually upgrade/install these package for various reasons. Mostly because we don't
# the packages as listed in our requirements.txt are out of date with what the python
# source ships with (wheel, for example)
pip install --compile -U pip setuptools wheel
sed -i '/^wheel/d' $PYREQ
echo virtualenv >> $PYREQ
echo pipenv >> $PYREQ

if [ "$VMAJ" = 3 ]; then
    if [ "$VERSION" = 3.7.0 ]; then
        install_pyodbc
    fi
fi

# `pip install -r` is unable to install our requirements.txt ina clean environment.
# Work around this issue by downloading the packages first, building wheels and installing
# those. Don't ask
mkdir -p $PYDOWNLOADS $PYWHEELS
pip download -r $PYREQ -d $PYDOWNLOADS
pip wheel -r $PYREQ -w $PYWHEELS --no-index --find-links file://${PYDOWNLOADS}
pip install -r $PYREQ --no-index --find-links file://${PYWHEELS}
# This is the wrong license
rm -f $INSTALL/LICENSE
# Copy the real python license to /opt/xcalar/share/doc/python-$version/
mkdir -p $INSTALL/share/doc/${NAME}-${VERSION}
cp $SRCDIR/LICENSE $INSTALL/share/doc/${NAME}-${VERSION}/
popd

pushd "$INSTALL/bin"
# This jp file somehow gets installed with a shbang line of #!/Users/jim/bin/...
rm -f jp

if [ "$VMAJ" = 3 ]; then
    if [ "$PYTOOLS_VERSIONED" = 1 ]; then
        rm -f python pip easy_install
        for tool in $(find . -executable -type f -mindepth 1 -maxdepth 1 | sed -e 's|^\./||g' | grep -v -- ${VER}); do
            basetool="$(basename $tool)"
            basetool="${basetool%%.*}"
            ext="$(basename $tool)"
            ext="${tool##*.}"
            newname="${basetool}-${VER}"
            if [ -n "$ext" ] && [ "$ext" != "$basetool" ]; then
                newname="${newname}.${ext}"
            fi
            if ! test -e "$newname"; then
                mv "$tool" "$newname"
            else
                rm -fv "$tool"
            fi
        done
    fi
    # Remove empty python2 kernel dir so it doesn't show up in menu
    rm -rf $INSTALL/share/jupyter/kernels/python2
    # Add symlinks for pip3 and python3 to point to the correct versioned binary
    # (eg, python3.6)
    test -e python${VMAJ} || ln -sfn python${VER} python${VMAJ}
    test -e pip${VMAJ} || ln -sfn pip${VER} pip${VMAJ}
fi
popd

# These files conflicts
rm -f $INSTALL/share/man/man1/ipython.1.gz
rm -f $INSTALL/etc/bash_completion.d/snakebite-completion.bash

# Remove tests, saving ~100mb and removing ~24k of files
rm -rvf $INSTALL/lib/python${VER}/test

# Move the license from the rootdir
mv -v $INSTALL/cx_Oracle-doc/*  $INSTALL/lib/python3.6/site-packages/cx_Oracle-*.dist-info/
rm -rvf $INSTALL/cx_Oracle-doc/

find $INSTALL/lib/python${VER}/site-packages -type d -name tests -print0 | xargs -r -0 -n 1 -I{} rm -rfv "{}"

# Precompile *.py to pyc so the package manager can keep track of and remove these
# during upgrade/uninstall
$INSTALL/bin/python${VER} -m compileall -x 'test' $INSTALL/bin

# Install supervisord 'binaries' into bin. These aren't explicitly part of
# 'python' but it's a part of the xcalarPython package
SUPERVISORD_VERSION="${SUPERVISORD_VERSION:-3.3.3-1}"
for supervisorBin in supervisord supervisorctl; do
    url="http://repo.xcalar.net/deps/${supervisorBin}-${SUPERVISORD_VERSION}.pex"
    curl -sL $url >$INSTALL/bin/${supervisorBin}
    chmod +x $INSTALL/bin/${supervisorBin}
done

# If fpm is installed, build deb/rpm packages and prefer to install those
if command -v fpm &>/dev/null; then
    sed -e "s,@PREFIX@,$PREFIX,g" "${DIR}/${AFTER_INSTALL}.in" >"${TMPDIR}/${AFTER_INSTALL}"
    fpm -s dir -t $PKG -v $VERSION --iteration $ITERATION --name $PKGNAME "${FPMDEPS[@]}" $FPMREPLACES -f \
        --license "Python" --url "https://www.python.org" $FPMDIST "${FPMPROVIDES[@]}" \
        --description "An interpreted, interactive, object-oriented programming language" \
        --after-install "${TMPDIR}/${AFTER_INSTALL}" --after-upgrade "${TMPDIR}/${AFTER_INSTALL}" \
        --prefix ${PREFIX} -C "${DESTDIR}${PREFIX}"
else
    fakeroot tar czf "${PKGNAME}-${VERSION}-${ITERATION}.${OSID_NAME}${OSID_VERSION}.tar.gz" -C "${DESTDIR:-/}" "${PREFIX#/}"
fi

if test -e "${PREFIX}.$$"; then
    $sudo mv ${PREFIX} ${PREFIX}.new
    $sudo mv ${PREFIX}.$$ ${PREFIX}
fi
