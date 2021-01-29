#!/bin/bash

set -e
ITERATION=2
VERSION=0.24
VERSTRING="$(osid)"
NAME=btparser

test -e /opt/rh/ruby193/root/usr/local/bin && export PATH="/opt/rh/ruby193/root/usr/local/bin:$PATH"

case "$VERSTRING" in
    el[67]) true ;;
    *) echo "$VERSTRING not supported for $NAME"; exit 1;;
esac

TMPDIR="${TMPDIR:-/tmp/`id -un`}/$NAME"
SRCDIR="$TMPDIR/$NAME"
DESTDIR="$TMPDIR/${NAME}-install"
rm -rf "$SRCDIR" "$DESTDIR"
mkdir -p "$SRCDIR" "$DESTDIR"

git clone git://git.fedorahosted.org/git/${NAME}.git "$SRCDIR"
(
cd "$SRCDIR"
./configure --prefix=/usr --enable-static
make -j`nproc`
make DESTDIR="$DESTDIR" install
)

fpm -s dir -t rpm --rpm-dist ${VERSTRING} --iteration ${ITERATION} --version ${VERSION} --name $NAME --rpm-autoreqprov --after-install <(echo ldconfig) -f -C ${DESTDIR} usr
