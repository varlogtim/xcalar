#!/bin/bash

set -e

DIR="$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)"
export PATH=$DIR:/opt/rh/ruby193/root/usr/local/bin:$PATH

VERSTRING="$(osid)"
NAME=tmux
PACKAGE_NAME=tmux
VERSION=${VERSION:-2.6}
ITERATION="${ITERATION:-2}"

TMPDIR=${TMPDIR:-/tmp/`id -un`}/tmux2/$$
mkdir -p "$TMPDIR"

DESTDIR=${TMPDIR}/rootfs
SRCDIR=${NAME}-${VERSION}

rm -rf ${SRCDIR} ${DESTDIR}
mkdir -p ${SRCDIR} ${DESTDIR}

case "$VERSTRING" in
    ub*) sudo apt-get install -yqq libssl-dev libutempter-dev libutf8proc-dev;;
    *el6) sudo yum install -y openssl-devel libevent2-devel libutempter-devel ncurses-devel ;;
    *el7) sudo yum install -y openssl-devel libevent2-devel libutempter-devel ncurses-devel utf8proc-devel;;
    *) ;;
esac

curl -sSL https://github.com/tmux/tmux/releases/download/${VERSION}/${NAME}-${VERSION}.tar.gz | tar zxvf - -C ${SRCDIR}/..
pushd $SRCDIR
if ! [[ $VERSTRING =~ el6 ]]; then
	./configure --prefix=/usr --enable-silent-rules --enable-utempter LIBNCURSES_LIBS="`pkg-config --libs --static ncurses`"
else
	./configure --prefix=/usr --enable-silent-rules --enable-utempter LIBNCURSES_LIBS="`pkg-config --libs --static ncurses`"
fi
make all V=0
make DESTDIR=$DESTDIR install

curl -sSL http://repo.xcalar.net/deps/bash_completion_tmux.sh --create-dirs -o $DESTDIR/etc/bash_completion.d/tmux.sh
popd

case "$VERSTRING" in
    ub14)
        fpm -s dir -t deb --iteration ${ITERATION} --version ${VERSION} --name ${PACKAGE_NAME} --deb-ignore-iteration-in-dependencies -d libutf8proc1 -d ncurses-term -d libutempter0 -d libevent-2.0-5 -d libtinfo5  -d libc6 -d libstdc++6 -f -C $DESTDIR usr etc
        ;;
    *el6)
        fpm -s dir -t rpm --rpm-dist el6 --iteration ${ITERATION} --version ${VERSION} --name ${PACKAGE_NAME} -d ncurses-term --rpm-autoreqprov -f -C $DESTDIR usr etc
        ;;
    *el7)
        fpm -s dir -t rpm --rpm-dist el7 --iteration ${ITERATION} --version ${VERSION} --name ${PACKAGE_NAME} -d ncurses-term --rpm-autoreqprov -f -C $DESTDIR usr etc
        ;;
    *)
        echo >&2 "Unknown OS $VERSTRING"
        exit 1
        ;;
esac
rm -rf "$TMPDIR"
