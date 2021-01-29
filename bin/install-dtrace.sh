#!/bin/bash

if [ `id -u` -ne 0 ]; then
    exec sudo "$0" "$@"
fi

VERSION=20160613
NAME=dtrace
SRCDIR=/usr/src/$NAME-$VERSION


rm -rf "$SRCDIR"
curl -sSL ftp://crispeditor.co.uk/pub/release/website/dtrace/dtrace-${VERSION}.tar.bz2 | tar jxf - -C "$SRCDIR/.."
cd "$SRCDIR"
sed -i -e 's/sudo apt-get install/sudo DEBIAN_FRONTEND=noninteractive apt-get install -yqq/g' tools/get-deps.pl  > /tmp/get-deps.sh
bash -x /tmp/get-deps.sh
cat > dkms.conf<<EOF
MAKE="make all install load"
CLEAN="make clean"
BUILT_MODULE_NAME=dtracedrv
BUILT_MODULE_LOCATION=./build/driver/
DEST_MODULE_LOCATION=/extra
PACKAGE_NAME=$NAME
PACKAGE_VERSION=$VERSION
REMAKE_INITRD=yes
AUTOINSTALL=yes
EOF
dkms add -m $NAME -v $VERSION
dkms build -m $NAME -v $VERSION
