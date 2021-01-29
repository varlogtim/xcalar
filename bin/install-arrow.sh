#!/bin/bash
#
# shellcheck disable=SC2086,SC2206
set -eu

NAME=apache-arrow
PKGNAME="${PKGNAME:-xcalar-arrow}"
VERSION=${VERSION:-0.13.0}
ITERATION="${ITERATION:-8}"
TMPDIR=$XLRDIR/tmp/$PKGNAME
DESTDIR=$TMPDIR/rootfs
SRCDIR=$TMPDIR/src
PREFIX=${PREFIX:-/opt/xcalar}
OSID=$(osid)
URL=http://repo.xcalar.net/deps/${NAME}-${VERSION}.tar.gz

rm -rf $TMPDIR
mkdir -p $DESTDIR $SRCDIR

fmt=$(osid -p)

case "$fmt" in
    rpm)
        LIBDIR=lib
        FPMEXTRA=(--rpm-autoreqprov)
        sudo yum install -y bison flex libev-devel
        ;;
    deb)
        LIBDIR=lib
        # Add a dummy argument because setting FPMEXTRA=() causes set -u to trip up
        FPMEXTRA=(--no-deb-generate-changes)
        sudo apt-get update -y
        sudo apt-get install -y bison flex libevent-dev
        ;;
    *)
        echo >&2 "ERROR: Unknown packaging format: $fmt for OS $(osid)"
        exit 1
        ;;
esac

curl -sSL "$URL" | tar zxf - --strip-components=1 -C "$SRCDIR"

mkdir $SRCDIR/cpp/release

cd $SRCDIR/cpp/release

# Used by cmake to determine where to look for pkg-config and cmake files
export PKG_CONFIG_PATH=${PREFIX}/${LIBDIR}/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}

cmake -DARROW_BOOST_VENDORED=ON -DARROW_BOOST_USE_SHARED=OFF -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_PARQUET=ON -DBUILD_SHARED=OFF -DARROW_BUILD_STATIC=ON -DARROW_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX -DCMAKE_INSTALL_LIBDIR:PATH=$LIBDIR -DCMAKE_BUILD_TYPE=Release -G Ninja ..
ninja
DESTDIR=$DESTDIR ninja install
mkdir -p $DESTDIR/etc/ld.so.conf.d/
cat >$DESTDIR/etc/ld.so.conf.d/xcalar-arrow.conf  <<EOF
$PREFIX/$LIBDIR
EOF
cat >$DESTDIR/ldconfig.sh  <<EOF
#!/bin/sh
ldconfig
EOF

cd -

FPMCOMMON=(-s dir -t $fmt -v $VERSION --iteration $ITERATION --after-install $DESTDIR/ldconfig.sh --after-remove $DESTDIR/ldconfig.sh --after-upgrade $DESTDIR/ldconfig.sh -f)
FPMCOMMON+=(--url https://arrow.apache.org/ --description "Apache Arrow is a cross-language development platform for in-memory data" --license Apache2)

tar zcf ${PKGNAME}-${VERSION}-${ITERATION}.${OSID}.tar.gz -C $DESTDIR ${PREFIX#/} etc
if command -v fpm &>/dev/null; then
    libdir=${PREFIX#/}/${LIBDIR}
    libs=($libdir/libarrow.so $libdir/libarrow.so.13 $libdir/libarrow.so.13.0.0 $libdir/libparquet.so $libdir/libparquet.so.13 $libdir/libparquet.so.13.0.0)
    fpm -n ${PKGNAME}-libs "${FPMCOMMON[@]}" "${FPMEXTRA[@]}" -C ${DESTDIR} "${libs[@]}" etc/ld.so.conf.d
    (cd ${DESTDIR} && rm -fv "${libs[@]}" && rm -rfv etc/ld.so.conf.d)
    fpm -n ${PKGNAME} "${FPMCOMMON[@]}" "${FPMEXTRA[@]}" --prefix $PREFIX -C ${DESTDIR}${PREFIX}
fi
