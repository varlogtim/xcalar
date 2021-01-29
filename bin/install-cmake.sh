#!/bin/bash
NAME=cmake
VER=${1:-3.10.3}
ITERATION="${BUILD_NUMBER:-7}"
TMPDIR=${TMPDIR:-/tmp}/`id -un`/${NAME}
DESTDIR=$TMPDIR/rootfs
PKGNAME=optcmake3
PREFIX=/opt/cmake3
URL=http://repo.xcalar.net/deps/${NAME}-${VER}-Linux-x86_64.tar.gz

set -e

rm -rf $DESTDIR $TMPDIR
mkdir -p $DESTDIR $TMPDIR

curl -L "${URL}" | tar zxf - -C $DESTDIR

PROGS='cmake ccmake cpack ctest'

cat > "$TMPDIR/after-remove.sh"<<EOF
#!/bin/bash
cd /usr/local/bin &&
for prog in $PROGS; do
  rm -f /usr/local/bin/\$prog
done
EOF
cat > "$TMPDIR/after-install.sh"<<EOF
#!/bin/bash
test -d /usr/local/bin || mkdir -p /usr/local/bin
for prog in $PROGS; do
  ln -sfn /opt/cmake3/bin/\$prog /usr/local/bin/
done
EOF

case "$(osid)" in
    ub1[46])
        fpm -s dir -t deb --prefix $PREFIX -n $PKGNAME --version $VER --iteration ${ITERATION}.ub --after-remove $TMPDIR/after-remove.sh --after-install $TMPDIR/after-install.sh -a amd64 --description 'cross-platform, open-source make system' -d procps -d 'libarchive13' -d 'libc6 (>= 2.15)' -d 'libcurl3 (>= 7.16.2)' -d 'libexpat1 (>= 2.0.1)' -d 'libgcc1 (>= 1:3.0)' -d 'zlib1g (>= 1:1.2.3.3)' -f -C $DESTDIR/${NAME}-${VER}-Linux-x86_64
        ;;
    ub18)
        fpm -s dir -t deb --prefix $PREFIX -n $PKGNAME --version $VER --iteration ${ITERATION}.ub18 --after-remove $TMPDIR/after-remove.sh --after-install $TMPDIR/after-install.sh -a amd64 --description 'cross-platform, open-source make system' -d procps -d 'libarchive13' -d 'libc6 (>= 2.15)' -d 'libcurl4 (>= 7.58.0)' -d 'libexpat1 (>= 2.0.1)' -d 'libgcc1 (>= 1:3.0)' -d 'zlib1g (>= 1:1.2.3.3)' -f -C $DESTDIR/${NAME}-${VER}-Linux-x86_64
        ;;
    amzn[12]|*el6|*el7)
        fpm -s dir -t rpm --prefix $PREFIX -n $PKGNAME --version $VER --iteration $ITERATION --after-remove $TMPDIR/after-remove.sh --after-install $TMPDIR/after-install.sh -a x86_64 --description 'cross-platform, open-source make system' --rpm-autoreqprov -f -C $DESTDIR/${NAME}-${VER}-Linux-x86_64
        ;;
    *)
        echo >&2 "Unknown OS: $osid"
        exit 1
esac

rm -rf $TMPDIR
exit
